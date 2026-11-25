"""Collecteur Docker des résultats de coupons réels, sans base ni Firebase.

Configuration explicitement injectée ; aucun chargement de .env. Le serveur
revalide et règle les preuves par VERITAS, puis notifie les coupons concernés.
"""
import argparse
import datetime as dt
import json
import os
import signal
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import Counter
from dataclasses import dataclass
from pathlib import Path

from veritas import starz_results

HEARTBEAT = Path("/tmp/coupon-results-heartbeat")


@dataclass(frozen=True)
class Config:
    server_url: str
    cron_secret: str
    interval: int = 60
    window_offset: int = -1200

    @classmethod
    def from_env(cls):
        base = os.environ.get("COUPON_RESULTS_SERVER_URL", "").strip().rstrip("/")
        parsed = urllib.parse.urlsplit(base)
        if (parsed.scheme != "https" or not parsed.hostname or parsed.username or
                parsed.password or parsed.query or parsed.fragment or
                parsed.path not in ("", "/")):
            raise ValueError("COUPON_RESULTS_SERVER_URL doit être une origine HTTPS sans identifiants")
        secret = os.environ.get("TRACKING_CRON_SECRET", "").strip()
        if not secret:
            filename = os.environ.get("TRACKING_CRON_SECRET_FILE", "").strip()
            if not filename:
                raise ValueError("Secret cron absent (variable ou fichier requis)")
            secret = Path(filename).read_text(encoding="utf-8").strip()
        if len(secret) < 32 or any(marker in secret.lower() for marker in (
                "change-this", "replace-me", "your-secret", "placeholder")):
            raise ValueError("Secret cron invalide")
        interval = int(os.environ.get("COUPON_RESULTS_INTERVAL", "60"))
        if not 10 <= interval <= 3600:
            raise ValueError("Cadence hors limites (10 à 3600 secondes)")
        offset = int(os.environ.get("COUPON_RESULTS_WINDOW_OFFSET", "-1200"))
        if not -86400 < offset < 86400:
            raise ValueError("Décalage de fenêtre hors limites")
        return cls(base, secret, interval, offset)


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        # Un changement d'hôte ne doit jamais emporter le secret cron.
        raise RuntimeError("Redirection du serveur refusée")


class Backend:
    def __init__(self, config):
        self.config = config
        self.opener = urllib.request.build_opener(_NoRedirect())

    def request(self, path, payload=None):
        body = None if payload is None else json.dumps(
            payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        req = urllib.request.Request(self.config.server_url + path, data=body, headers={
            "X-Cron-Secret": self.config.cron_secret,
            "Accept": "application/json", "Content-Type": "application/json",
            "User-Agent": "ORACXPRED-Coupon-Results/1.0"})
        with self.opener.open(req, timeout=15) as response:
            raw = response.read(512 * 1024 + 1)
            if len(raw) > 512 * 1024:
                raise ValueError("Réponse serveur trop volumineuse")
            data = json.loads(raw.decode("utf-8"))
        if not isinstance(data, dict):
            raise ValueError("Réponse serveur invalide")
        return data


class Worker:
    def __init__(self, config, backend=None, client=None):
        self.config = config
        self.backend = backend or Backend(config)
        self.client = client or starz_results.Client(window_offset_seconds=config.window_offset)
        self.cursor = 0

    def tick(self):
        # Relire l'état avant chaque nouvelle écriture, y compris après un
        # timeout dont l'issue du POST précédent était inconnue.
        data = self.backend.request(f"/cron/coupon-results/refs?after={self.cursor}")
        refs = data.get("items")
        cursor = data.get("next_cursor")
        if (not isinstance(refs, list) or len(refs) > 100 or
                any(not isinstance(ref, dict) for ref in refs) or
                type(cursor) is not int or cursor < 0):
            raise ValueError("Références serveur invalides")
        summary = {"event": "coupon_results", "refs": len(refs), "rows": 0,
                   "settled": 0, "busy": False, "diagnostics": [], "rejected_reasons": {}}
        if refs:
            now = dt.datetime.now(dt.timezone.utc).replace(tzinfo=None)
            report = self.client.collect_refs(refs, now=now,
                budget=min(20.0, max(1.0, self.config.interval - 5.0)))
            summary.update(rows=len(report.rows), rejected=report.rejected,
                           attempted=report.attempted,
                           budget_exceeded=report.budget_exceeded,
                           source_error=report.error or "",
                           diagnostics=report.diagnostics,
                           rejected_reasons=dict(Counter(
                               item["reason"] for item in report.diagnostics)))
            if report.rows:
                reply = self.backend.request("/cron/coupon-results/ingest",
                                             {"rows": report.rows})
                if (type(reply.get("busy")) is not bool or
                        type(reply.get("settled")) is not int or
                        reply["settled"] < 0):
                    raise ValueError("Accusé de règlement invalide")
                summary.update(settled=reply["settled"], busy=reply["busy"],
                               review=reply.get("review", 0),
                               lock_cleanup_failed=bool(reply.get("lock_cleanup_failed")),
                               notification_dispatch_failed=bool(
                                   reply.get("notification_dispatch_failed")))
        self.cursor = cursor
        return summary


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--once", action="store_true", help="Un passage, puis arrêt")
    args = parser.parse_args()
    try:
        config = Config.from_env()
    except (ValueError, OSError):
        print(json.dumps({"event": "configuration_error",
                          "message": "URL HTTPS, secret et cadence valides requis"}), flush=True)
        return 2
    worker = Worker(config)
    stop = threading.Event()
    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, lambda *_: stop.set())
    while not stop.is_set():
        started = time.monotonic()
        try:
            summary = worker.tick()
            HEARTBEAT.write_text(str(time.time()), encoding="ascii")
            diagnostics = summary.pop("diagnostics", [])
            print(json.dumps(summary, ensure_ascii=False), flush=True)
            for item in diagnostics:
                print(json.dumps({"event": "coupon_result_rejected", **item},
                                 ensure_ascii=False), flush=True)
        except Exception as exc:
            # Jamais d'URL contenant un secret, ni de paramètres SQL dans les logs.
            summary = {"event": "pass_failed", "error_type": type(exc).__name__}
            if isinstance(exc, urllib.error.HTTPError):
                summary["http_status"] = exc.code
            print(json.dumps(summary), flush=True)
            if args.once:
                return 1
        if args.once:
            return 0
        stop.wait(max(0.1, config.interval - (time.monotonic() - started)))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
