"""Collecte des états de match depuis 888Starz, sans jamais deviner un résultat.

Règles non négociables appliquées ici :
- un match n'est déclaré terminé que sur un marqueur explicite du flux
  (`F: true`, `SC.CPS: "Jeu terminé"`, `SC.I: "Match terminé"`) ;
- une minute théorique écoulée ne prouve rien ;
- un score incomplet n'est jamais complété par une valeur inventée ; il est
  simplement déclaré inexploitable (`scores_ready = False`) ;
- chaque observation conserve la preuve brute (empreinte SHA-256 du JSON reçu).
"""

import datetime as dt
import hashlib
import json
import time
import urllib.error
import urllib.request
from dataclasses import dataclass, field

from .resolver import ResolutionError, normalise_name, resolve

FEED_URL = (
    "https://starz-relay-dash.lovable.app/api/888/live"
    "?sports=85&count=200&lng=fr&country=96"
)
SOURCE_NAME = "888Starz"
MAX_GOALS = 99
LIVE_PERIODS = {"1ere mi temps", "2eme mi temps"}
FINISH_STATUS = {"jeu termine", "match termine"}


@dataclass(frozen=True)
class FeedObservation:
    match_id: str
    home: str
    away: str
    league: str
    kickoff: dt.datetime | None
    home_score: int | None
    away_score: int | None
    started: bool
    finished: bool
    finish_evidence: tuple = ()
    status_text: str = ""
    period_home: int | None = None
    period_away: int | None = None
    period_count: int = 0
    raw_hash: str = ""
    source: str = SOURCE_NAME
    observed_at: dt.datetime | None = None
    warnings: tuple = ()

    @property
    def scores_ready(self) -> bool:
        return self.home_score is not None and self.away_score is not None

    @property
    def periods_consistent(self) -> bool:
        if not self.period_count or not self.scores_ready:
            return True
        return (self.period_home, self.period_away) == (self.home_score, self.away_score)


@dataclass
class CollectionReport:
    observed_at: dt.datetime
    payload_hash: str
    fixtures: int = 0
    observations: list = field(default_factory=list)
    finished: list = field(default_factory=list)
    rejected: list = field(default_factory=list)
    warnings: list = field(default_factory=list)


def utcnow() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc).replace(tzinfo=None)


def fetch_feed(url=FEED_URL, timeout=25, attempts=3, user_agent="ORACXPRED-VERITAS/1.0"):
    """Télécharge le flux une fois et renvoie (fixtures, empreinte du payload)."""
    last_error = None
    for attempt in range(attempts):
        try:
            request = urllib.request.Request(url, headers={
                "User-Agent": user_agent,
                "Accept": "application/json, text/plain, */*",
            })
            with urllib.request.urlopen(request, timeout=timeout) as response:
                raw = response.read()
            payload = json.loads(raw.decode("utf-8", "replace"))
        except (urllib.error.URLError, TimeoutError, OSError, ValueError) as exc:
            last_error = exc
            if attempt + 1 < attempts:
                time.sleep(2 * (attempt + 1))
            continue
        fixtures = payload.get("Value") if isinstance(payload, dict) else payload
        if not isinstance(fixtures, list):
            raise ValueError("payload sans liste de matchs (Value)")
        return fixtures, hashlib.sha256(raw).hexdigest()
    raise ValueError(f"flux injoignable : {last_error}")


def _as_int(value, label, warnings):
    if value is None or value == "":
        return None
    try:
        number = int(value)
    except (TypeError, ValueError):
        warnings.append(f"{label} illisible : {value!r}")
        return None
    if number < 0 or number > MAX_GOALS:
        warnings.append(f"{label} hors limites : {number}")
        return None
    return number


def _status_parts(match):
    sc = match.get("SC") if isinstance(match.get("SC"), dict) else {}
    return {
        "CPS": str(sc.get("CPS") or "").strip(),
        "I": str(sc.get("I") or "").strip(),
        "SLS": str(sc.get("SLS") or "").strip(),
        "HS": match.get("HS"),
        "F": match.get("F"),
        "TS": sc.get("TS"),
        "TR": sc.get("TR"),
    }


def finish_evidence(match) -> tuple:
    """Preuves explicites de fin de match, dans l'ordre de fiabilité."""
    status = _status_parts(match)
    evidence = []
    if status["F"] is True:
        evidence.append("F=True")
    if normalise_name(status["CPS"]) in FINISH_STATUS:
        evidence.append(f"SC.CPS={status['CPS']}")
    if normalise_name(status["I"]) in FINISH_STATUS:
        evidence.append(f"SC.I={status['I']}")
    return tuple(evidence)


def _is_pre_start(sc) -> bool:
    """Le flux annonce explicitement un match pas encore commencé.

    `SC.SLS` prend la forme « Début dans … » et `SC.I` « Paris avant… » tant
    que le coup d'envoi n'a pas eu lieu — y compris quand l'heure théorique
    est déjà passée. Un score présent sur une telle ligne est celui d'un tour
    précédent, pas du match à venir : il ne faut jamais le lire comme du direct.
    """
    sls = normalise_name(sc.get("SLS"))
    inner = normalise_name(sc.get("I"))
    return sls.startswith("debut dans") or "avant" in inner


def is_started(match) -> bool:
    """Un match n'est « commencé » que si le flux le documente.

    Preuves retenues, par ordre de fiabilité :
    - `F=True` ou un libellé de phase `SC.CPS` vivant/terminé ;
    - du jeu documenté : un `SC.FS` non vide (un but marqué prouve le coup
      d'envoi) ou des périodes `SC.PS` jouées — sauf si la ligne dit
      explicitement « pas encore commencé » (score d'un tour précédent).

    Certaines ligues (Rush, Penalty) omettent `SC.CPS` pendant tout le match :
    sans ce filet, elles restaient figées « à venir » avec leur score caché.
    `SC.TS` ressemble à un temps de jeu mais c'est un compte à rebours : il est
    non nul avant le coup d'envoi. Il ne sert donc jamais de preuve.
    """
    status = _status_parts(match)
    if status["F"] is True:
        return True
    if normalise_name(status["CPS"]) in LIVE_PERIODS | FINISH_STATUS:
        return True
    sc = match.get("SC") if isinstance(match.get("SC"), dict) else {}
    if _is_pre_start(sc):
        return False
    full = sc.get("FS")
    if isinstance(full, dict) and full:
        return True
    periods = sc.get("PS")
    if isinstance(periods, list) and periods:
        return True
    return False


def _scores(match, trusted_started, warnings, periods=None, finished=False):
    """Score exploitable uniquement si le flux le documente.

    Le flux omet les zéros : une équipe absente de `SC.FS` n'a pas marqué. Un
    `SC.FS` vide n'est reconnu comme 0-0 que si les périodes (`SC.PS`) le
    confirment — une période documentée sans but est une période à zéro. Aucun
    score n'est jamais inventé : sans confirmation il reste inconnu (`None`).
    """
    sc = match.get("SC") if isinstance(match.get("SC"), dict) else {}
    full = sc.get("FS")
    if not trusted_started:
        if isinstance(full, dict) and full:
            warnings.append("score antérieur au coup d'envoi ignoré")
        return None, None
    if full is not None and not isinstance(full, dict):
        warnings.append("SC.FS de forme inattendue")
        return None, None
    if isinstance(full, dict) and ("S1" in full or "S2" in full):
        home = _as_int(full.get("S1"), "score domicile", warnings)
        away = _as_int(full.get("S2"), "score extérieur", warnings)
        if home is None and "S1" in full:
            return None, None
        if away is None and "S2" in full:
            return None, None
        return (0 if home is None else home), (0 if away is None else away)
    period_home, period_away, period_count = periods or (None, None, 0)
    # Une rencontre se joue en deux périodes : une seule période documentée à la
    # fin ne suffit pas à clore un 0-0.
    if period_count >= (2 if finished else 1):
        warnings.append("score reconstruit à partir des périodes")
        return period_home, period_away
    warnings.append("score absent du flux")
    return None, None


def _periods(match, warnings):
    """Périodes documentées et buts qui y sont inscrits.

    `SC.PS` liste les périodes jouées ; une entrée sans `S1`/`S2` est une
    période sans but, ce qui compte comme une preuve de zéro et non comme une
    absence d'information.
    """
    sc = match.get("SC") if isinstance(match.get("SC"), dict) else {}
    periods = sc.get("PS")
    if not isinstance(periods, list) or not periods:
        return None, None, 0
    home = away = 0
    count = 0
    for period in periods:
        value = period.get("Value") if isinstance(period, dict) else None
        if not isinstance(value, dict):
            continue
        count += 1
        first = _as_int(value.get("S1"), "score période domicile", warnings)
        second = _as_int(value.get("S2"), "score période extérieur", warnings)
        home += first or 0
        away += second or 0
    if not count:
        return None, None, 0
    return home, away, count


def scores(match) -> tuple:
    """Score exploitable d'un match du flux, `(None, None)` s'il n'est pas prouvé.

    Point d'entrée unique pour le site et l'app : la même règle que VERITAS,
    sans résolution d'identité (un nom inconnu ne doit pas masquer un score).
    """
    warnings = []
    finished = bool(finish_evidence(match))
    period_home, period_away, period_count = _periods(match, warnings)
    return _scores(match, is_started(match) or finished, warnings,
                   (period_home, period_away, period_count), finished=finished)


def raw_fingerprint(match) -> str:
    try:
        canonical = json.dumps(match, sort_keys=True, ensure_ascii=False, default=str)
    except (TypeError, ValueError):
        canonical = repr(match)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def extract(match, observed_at=None) -> FeedObservation | None:
    """Transforme un match du flux en observation. None si l'identité est invalide."""
    try:
        identity = resolve(match)
    except ResolutionError:
        return None
    warnings = []
    started = is_started(match)
    evidence = finish_evidence(match)
    finished = bool(evidence)
    period_home, period_away, period_count = _periods(match, warnings)
    home_score, away_score = _scores(match, started or finished, warnings,
                                     (period_home, period_away, period_count),
                                     finished=finished)
    if finished and (home_score is None or away_score is None):
        warnings.append("match terminé sans score exploitable")
    status = _status_parts(match)
    return FeedObservation(
        match_id=identity.match_id,
        home=identity.home,
        away=identity.away,
        league=identity.league,
        kickoff=identity.kickoff,
        home_score=home_score,
        away_score=away_score,
        started=started,
        finished=finished,
        finish_evidence=evidence,
        status_text=" · ".join(part for part in (status["CPS"], status["I"], status["SLS"]) if part),
        period_home=period_home,
        period_away=period_away,
        period_count=period_count,
        raw_hash=raw_fingerprint(match),
        observed_at=observed_at or utcnow(),
        warnings=tuple(warnings),
    )


def collect(fixtures, observed_at=None, payload_hash="") -> CollectionReport:
    now = observed_at or utcnow()
    report = CollectionReport(observed_at=now, payload_hash=payload_hash, fixtures=len(fixtures or []))
    seen = set()
    for match in fixtures or []:
        observation = extract(match, observed_at=now)
        if observation is None:
            report.rejected.append(match.get("I") if isinstance(match, dict) else match)
            continue
        if observation.match_id in seen:
            report.rejected.append(observation.match_id)
            report.warnings.append(f"match {observation.match_id} présent deux fois dans le flux")
            continue
        seen.add(observation.match_id)
        report.observations.append(observation)
        if observation.finished:
            report.finished.append(observation)
        report.warnings.extend(f"{observation.match_id}: {warning}" for warning in observation.warnings)
    return report
