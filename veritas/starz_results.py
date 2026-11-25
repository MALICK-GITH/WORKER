"""Anonymous 888Starz result reads, restricted to exact real-football references.

Complete periods published by v3/games are result-source evidence, not an API
finished flag. Only the first two periods are used for the 90-minute result.
This module uses the stdlib and the safe VERITAS identity/observation types only.
"""

import datetime as dt
import hashlib
import http.client
import json
import math
import re
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import OrderedDict
from dataclasses import dataclass, field

from .collector import FeedObservation
from .resolver import normalise_name, parse_kickoff

SOURCE_NAME = "888starz-results"
LABEL = "888Starz résultats"
BASE_URL = "https://888starz.bet/service-api/result/web/api"
MAX_GOALS = 99
WINDOW_SECONDS = 86400
KICKOFF_TOLERANCE_SECONDS = 900
CACHE_SECONDS = 30
CACHE_ENTRIES = 256
MAX_RESPONSE_BYTES = 4 * 1024 * 1024
MAX_CACHE_BYTES = 8 * 1024 * 1024

_NON_REAL = re.compile(
    r"\b(?:fifa|fc)\s*\d+\b|\be\s*sports?\b|\befootball\b|\bpes\b|"
    r"\bpro evolution soccer\b|\bcyber\w*\b|\bvirt(?:ual|uel)\w*\b|"
    r"\bpenalt\w*\b"
)
_PHASE = re.compile(
    r"\bhalf\b|\bmi temps\b|\b(?:period|periode)\b|\bextra time\b|"
    r"\btemps supplementaire\b|\bprolongations?\b|\bpenalt\w*\b|\btirs au but\b"
)
_SCORE = re.compile(
    r"([0-9]+):([0-9]+)(?:\s*\(([0-9]+:[0-9]+(?:\s*,\s*[0-9]+:[0-9]+)*)\))?"
)
# Noms exacts normalisés observés en français et en anglais. Les alias
# désignent la même phase pour ne pas autoriser deux copies d'une prolongation.
_EXTRA_TITLES = {
    "temps supplementaire": "extra_time",
    "extra time": "extra_time",
    "tirs au but": "shootout",
    "penalty shootout": "shootout",
}

REJECTION_MESSAGES = {
    "reference_invalid": "Référence de match mal formée.",
    "reference_id_invalid": "Identifiant de référence absent ou non numérique.",
    "reference_kickoff_invalid": "Heure de référence absente ou invalide.",
    "reference_not_started": "Le coup d'envoi de référence est encore dans le futur.",
    "reference_teams_missing": "Nom d'équipe manquant dans la référence.",
    "reference_league_invalid": "Championnat de référence absent ou hors football réel.",
    "observation_invalid": "Ligne de résultat ou date d'observation invalide.",
    "id_mismatch": "L'identifiant du résultat diffère de la référence.",
    "sport_not_real": "Le résultat n'est pas identifié comme du football réel.",
    "championship_id_invalid": "Identifiant de championnat absent ou invalide.",
    "kickoff_invalid": "Heure du résultat absente, invalide ou encore dans le futur.",
    "kickoff_mismatch": "Écart de coup d'envoi supérieur à 15 minutes.",
    "teams_mismatch": "Les équipes ou leur ordre diffèrent de la référence.",
    "league_mismatch": "Le championnat du résultat diffère de la référence réelle.",
    "administrative_info": "Le résultat contient une mention administrative non vide.",
    "combined_match": "Identifiants d'équipes multiples ou mal formés : match agrégé possible.",
    "partial_match": "La ligne décrit une période ou un sous-match, ou son type est invalide.",
    "subgames_invalid": "Sous-matchs, scores annexes ou indicateurs incohérents.",
    "score_invalid": "Score absent, mal formé ou hors limites.",
    "periods_incomplete": "Les deux périodes nécessaires au score à 90 minutes manquent.",
    "periods_inconsistent": "Les périodes ne permettent pas de confirmer le score à 90 minutes.",
    "raw_payload_invalid": "La preuve brute n'est pas un objet JSON valide.",
    "duplicate_id": "Plusieurs lignes portent le même identifiant dans une réponse.",
    "conflicting_rows": "Des versions différentes du même résultat ont été reçues.",
    "endpoint_championship_mismatch": "La ligne ne porte pas le championnat demandé à l'API.",
    "championship_not_found": "Aucun championnat réel de même nom normalisé dans les fenêtres consultées.",
    "match_id_not_found": "Identifiant du match absent des championnats correspondants consultés.",
    "budget_exceeded": "Collecte incomplète : budget de temps atteint, absence non confirmée.",
    "source_error": "Collecte incomplète : erreur de la source, absence non confirmée.",
    "invalid_collection_config": "Date d'observation ou budget de collecte invalide.",
}


def _reject(reasons, reason):
    """Instrumente un refus existant, sans changer son verdict."""
    if reasons is not None:
        reasons.append(reason)
    return None


def _diagnostic(ref, index, reason, championship_ids=()):
    # Liste blanche : jamais de réponse brute, d'en-tête ni de configuration.
    ref = ref if isinstance(ref, dict) else {}
    kickoff = _timestamp(ref.get("kickoff_ts"))
    league = ref.get("league")
    league = " ".join(league.split())[:160] if isinstance(league, str) else ""
    return {
        "ref_index": index,
        "match_id": _identifier(ref.get("match_id")),
        "league": league,
        "kickoff_utc": kickoff.isoformat() + "Z" if kickoff else None,
        "reason": reason,
        "message": REJECTION_MESSAGES[reason],
        "checked_championship_ids": sorted(championship_ids)[:10],
    }


@dataclass
class Report:
    """Raw accepted rows; attempted counts HTTP requests, rejected counts refs.

    Cache hits are not attempts. Missing, invalid and unprocessed references
    count as rejected; duplicate accepted references produce just one raw row.
    """

    rows: list = field(default_factory=list)
    attempted: int = 0
    rejected: int = 0
    budget_exceeded: bool = False
    error: str | None = None
    diagnostics: list = field(default_factory=list)


class _DeadlineReached(Exception):
    pass


def _identifier(value):
    # Never truncate floats, accept booleans, or match through team homonyms.
    if type(value) is int:
        return value if value > 0 else None
    if isinstance(value, str) and re.fullmatch(r"[0-9]+", value.strip()):
        try:
            number = int(value.strip())
            return number if number > 0 else None
        except ValueError:
            pass
    return None


def _timestamp(value):
    if isinstance(value, bool) or not isinstance(value, (str, int, float)):
        return None
    try:
        number = float(value)
    except (ValueError, OverflowError):
        return None
    if not math.isfinite(number) or number <= 0:
        return None
    return parse_kickoff(number)


def _now(value):
    if value is None:
        value = time.time()
    if isinstance(value, dt.datetime):
        if value.tzinfo is not None:
            value = value.astimezone(dt.timezone.utc).replace(tzinfo=None)
        return value if value > dt.datetime(1970, 1, 1) else None
    return _timestamp(value)


def _name(value):
    return normalise_name(value) if isinstance(value, str) else ""


def _real_league(value):
    name = _name(value)
    return bool(name) and not _NON_REAL.search(name)


def _real_sport(row):
    return (type(row.get("sportId")) is int and row["sportId"] == 1
            and ("isCyber" not in row or row["isCyber"] is False))


def _reference(ref, now, rejection_reasons=None):
    if not isinstance(ref, dict):
        return _reject(rejection_reasons, "reference_invalid")
    match_id = _identifier(ref.get("match_id"))
    kickoff = _timestamp(ref.get("kickoff_ts"))
    if match_id is None:
        return _reject(rejection_reasons, "reference_id_invalid")
    if kickoff is None:
        return _reject(rejection_reasons, "reference_kickoff_invalid")
    if kickoff > now:
        return _reject(rejection_reasons, "reference_not_started")
    if not _name(ref.get("home")) or not _name(ref.get("away")):
        return _reject(rejection_reasons, "reference_teams_missing")
    if not _real_league(ref.get("league")):
        return _reject(rejection_reasons, "reference_league_invalid")
    return match_id, kickoff, _name(ref["league"])


def _score(value):
    if not isinstance(value, str):
        return None
    matched = _SCORE.fullmatch(value.strip())
    if matched is None:
        return None
    try:
        principal = (int(matched[1]), int(matched[2]))
        periods = [tuple(int(goal) for goal in part.split(":"))
                   for part in matched[3].split(",")] if matched[3] else []
    except ValueError:
        return None
    if any(goal > MAX_GOALS for pair in [principal, *periods] for goal in pair):
        return None
    return principal, periods


def _extra_labels(row):
    """Reject malformed subgames; statistical subgame scores are not goals."""
    games = row.get("subGame", [])
    if not isinstance(games, list):
        return None
    if "countSubGame" in row:
        count = row["countSubGame"]
        if type(count) is not int or count != len(games):
            return None
    if "hasSubGame" in row:
        flag = row["hasSubGame"]
        if type(flag) is not bool or flag != bool(games):
            return None
    labels = set()
    for game in games:
        if not isinstance(game, dict):
            return None
        title = _name(game.get("title"))
        score = game.get("score")
        if not title or not isinstance(score, str) or not score.strip():
            return None
        phase = _EXTRA_TITLES.get(title)
        if phase is not None:
            parsed = _score(score)
            if parsed is None or phase in labels:
                return None
            principal, periods = parsed
            if periods and tuple(sum(pair[i] for pair in periods) for i in (0, 1)) != principal:
                return None
            labels.add(phase)
    return labels


def observation(row, ref, observed_at=None, *, rejection_reasons=None):
    """Return a proven 90-minute observation, or None on any identity doubt.

    Missing isCyber is allowed (the observed real-football schema omits it);
    any present value other than the literal boolean False is rejected.
    Datetimes are normalized to naive UTC, as required by VERITAS.
    """
    now = _now(observed_at)
    if now is None or not isinstance(row, dict):
        return _reject(rejection_reasons, "observation_invalid")
    identity = _reference(ref, now, rejection_reasons)
    if identity is None:
        return None
    match_id, ref_kickoff, league = identity
    kickoff = _timestamp(row.get("dateStart"))
    champ_id = _identifier(row.get("champId"))
    if _identifier(row.get("id")) != match_id:
        return _reject(rejection_reasons, "id_mismatch")
    if not _real_sport(row):
        return _reject(rejection_reasons, "sport_not_real")
    if champ_id is None:
        return _reject(rejection_reasons, "championship_id_invalid")
    if kickoff is None or kickoff > now:
        return _reject(rejection_reasons, "kickoff_invalid")
    if abs((kickoff - ref_kickoff).total_seconds()) > KICKOFF_TOLERANCE_SECONDS:
        return _reject(rejection_reasons, "kickoff_mismatch")
    if (_name(row.get("opp1")) != _name(ref["home"])
            or _name(row.get("opp2")) != _name(ref["away"])):
        return _reject(rejection_reasons, "teams_mismatch")
    if not _real_league(row.get("champName")) or _name(row["champName"]) != league:
        return _reject(rejection_reasons, "league_mismatch")
    if row.get("dopInfo") not in (None, ""):
        return _reject(rejection_reasons, "administrative_info")
    for key in ("opp1Ids", "opp2Ids"):
        ids = row.get(key, [])
        if (not isinstance(ids, list) or len(ids) > 1
                or any(_identifier(item) is None for item in ids)):
            return _reject(rejection_reasons, "combined_match")
    for key in ("gameTypeName", "gameVidName"):
        text = row.get(key, "")
        if not isinstance(text, str) or _PHASE.search(_name(text)):
            return _reject(rejection_reasons, "partial_match")
    labels = _extra_labels(row)
    parsed = _score(row.get("score"))
    if labels is None:
        return _reject(rejection_reasons, "subgames_invalid")
    if parsed is None:
        return _reject(rejection_reasons, "score_invalid")
    principal, periods = parsed
    if len(periods) < 2:
        return _reject(rejection_reasons, "periods_incomplete")
    goals = tuple(periods[0][i] + periods[1][i] for i in (0, 1))
    if any(goal > MAX_GOALS for goal in goals):
        return _reject(rejection_reasons, "periods_inconsistent")
    if len(periods) == 2:
        if goals != principal:
            return _reject(rejection_reasons, "periods_inconsistent")
    elif not labels or any(principal[i] < goals[i] for i in (0, 1)):
        return _reject(rejection_reasons, "periods_inconsistent")
    try:
        canonical = json.dumps(row, sort_keys=True, ensure_ascii=False,
                               separators=(",", ":"), allow_nan=False)
    except (TypeError, ValueError, OverflowError):
        return _reject(rejection_reasons, "raw_payload_invalid")
    return FeedObservation(
        match_id=str(match_id), home=row["opp1"].strip(), away=row["opp2"].strip(),
        league=row["champName"].strip(), kickoff=kickoff,
        home_score=goals[0], away_score=goals[1], started=True, finished=True,
        finish_evidence=("endpoint=v3/games", f"champId={champ_id}", "basis=90min"),
        status_text=LABEL, period_home=goals[0], period_away=goals[1],
        period_count=2, raw_hash=hashlib.sha256(canonical.encode("utf-8")).hexdigest(),
        source=SOURCE_NAME, observed_at=now,
    )


def _envelope(payload):
    if not isinstance(payload, dict):
        raise ValueError("result envelope must be an object")
    count = payload.get("count")
    if type(count) is not int or count < 0:
        raise ValueError("result envelope requires a nonnegative integer count")
    items = payload.get("items", [] if count == 0 else None)
    if (not isinstance(items, list) or len(items) != count
            or any(not isinstance(item, dict) for item in items)):
        raise ValueError("result envelope count/items mismatch or invalid items")
    return items


class Client:
    def __init__(self, window_offset_seconds=-1200):
        if type(window_offset_seconds) is not int:
            raise ValueError("window_offset_seconds must be an integer")
        self.window_offset_seconds = window_offset_seconds
        self._cache = OrderedDict()

    def window_for(self, kickoff):
        """Enclosing [start, end) window, aligned to shifted UTC midnight."""
        value = _timestamp(kickoff)
        if value is None:
            raise ValueError("invalid kickoff timestamp")
        seconds = (value - dt.datetime(1970, 1, 1)).total_seconds()
        offset = self.window_offset_seconds
        start = math.floor((seconds - offset) / WINDOW_SECONDS) * WINDOW_SECONDS + offset
        return start, start + WINDOW_SECONDS

    @staticmethod
    def _url(endpoint, window, champ_id=None):
        params = [] if champ_id is None else [("champId", champ_id)]
        # Les références CALIBRE enregistrent le championnat anglais (LE).
        # Utiliser la même langue aux deux étapes, sans traduire la preuve brute.
        params.extend([("dateFrom", window[0]), ("dateTo", window[1]),
                       ("lng", "en"), ("ref", 233)])
        if champ_id is None:
            params.append(("sportIds", 1))
        return f"{BASE_URL}/{endpoint}?{urllib.parse.urlencode(params)}"

    @staticmethod
    def _check_deadline(deadline):
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise _DeadlineReached
        return remaining

    def _read(self, url, deadline, report):
        self._check_deadline(deadline)
        cached = self._cache.get(url)
        clock = time.monotonic()
        if cached is not None:
            if clock < cached[0]:
                self._cache.move_to_end(url)
                return _envelope(json.loads(cached[1].decode("utf-8")))
            del self._cache[url]
        request = urllib.request.Request(url, headers={"Accept": "application/json"})
        remaining = self._check_deadline(deadline)
        report.attempted += 1
        with urllib.request.urlopen(request, timeout=min(6.0, remaining)) as response:
            self._check_deadline(deadline)
            raw = response.read(MAX_RESPONSE_BYTES + 1)
        self._check_deadline(deadline)
        if len(raw) > MAX_RESPONSE_BYTES:
            raise ValueError("réponse de résultats trop volumineuse")
        items = _envelope(json.loads(raw.decode("utf-8")))
        if url.startswith(f"{BASE_URL}/v2/champs?"):
            for champ in items:
                if (_identifier(champ.get("id")) is None or not _name(champ.get("name"))
                        or type(champ.get("sportId")) is not int):
                    raise ValueError("invalid championship item schema")
        self._check_deadline(deadline)
        # Cache les octets, pas des centaines de graphes d'objets dupliqués.
        self._cache[url] = (time.monotonic() + CACHE_SECONDS, raw)
        self._cache.move_to_end(url)
        while (len(self._cache) > CACHE_ENTRIES or
               sum(len(entry[1]) for entry in self._cache.values()) > MAX_CACHE_BYTES):
            self._cache.popitem(last=False)
        return items

    def collect_refs(self, refs, now=None, budget=20.0):
        """Fetch once per window/league/champ, stopping on the first failure.

        Successful reads (including empty envelopes) are cached for 30 seconds.
        There are no retries, sleeps, cookies, authentication, or API keys.
        """
        report = Report()
        refs = list(refs)
        report.rejected = len(refs)
        observed_at = _now(now)
        if (observed_at is None or isinstance(budget, bool)
                or not isinstance(budget, (int, float)) or not math.isfinite(budget)
                or budget < 0):
            report.error = "invalid now or budget"
            report.diagnostics = [_diagnostic(ref, index, "invalid_collection_config")
                                  for index, ref in enumerate(refs)]
            return report
        deadline = time.monotonic() + budget
        windows = OrderedDict()
        pending = {}
        reasons = {index: [] for index in range(len(refs))}
        checked_champs = {index: set() for index in range(len(refs))}
        matched_champs = set()
        for index, ref in enumerate(refs):
            identity = _reference(ref, observed_at, reasons[index])
            if identity is not None:
                match_id, kickoff, league = identity
                seconds = (kickoff - dt.datetime(1970, 1, 1)).total_seconds()
                # La tolérance d'identité peut traverser la frontière du jour.
                # Garde les fenêtres de 24 h observées dans le contrat de l'API.
                required = dict.fromkeys(self.window_for(max(1, seconds + drift))
                    for drift in (0, -KICKOFF_TOLERANCE_SECONDS, KICKOFF_TOLERANCE_SECONDS))
                pending[index] = {(window, None) for window in required}
                for window in required:
                    windows.setdefault(window, []).append((index, ref, match_id, league))
        found = {}
        ambiguous = set()
        try:
            for window, candidates in windows.items():
                champs = self._read(self._url("v2/champs", window), deadline, report)
                leagues = {candidate[3] for candidate in candidates}
                selected = OrderedDict()
                for champ in champs:
                    champ_id = _identifier(champ["id"])
                    league = _name(champ["name"])
                    if _real_sport(champ) and _real_league(champ["name"]) and league in leagues:
                        selected.setdefault(champ_id, set()).add(league)
                for index, _, _, league in candidates:
                    pending[index].remove((window, None))
                    pending[index].update((window, champ_id)
                        for champ_id, names in selected.items() if league in names)
                    if any(league in names for names in selected.values()):
                        matched_champs.add(index)
                for champ_id, names in selected.items():
                    games = self._read(self._url("v3/games", window, champ_id), deadline, report)
                    by_id = {}
                    for row in games:
                        row_id = _identifier(row.get("id"))
                        if row_id is not None:
                            by_id.setdefault(row_id, []).append(row)
                    for index, ref, match_id, league in candidates:
                        self._check_deadline(deadline)
                        if league not in names:
                            continue
                        checked_champs[index].add(champ_id)
                        matches = by_id.get(match_id, [])
                        if len(matches) > 1:
                            ambiguous.add(index)
                            reasons[index].append("duplicate_id")
                        elif matches:
                            row = matches[0]
                            row_reasons = []
                            proof = observation(row, ref, observed_at,
                                                rejection_reasons=row_reasons)
                            if _identifier(row.get("champId")) != champ_id or proof is None:
                                ambiguous.add(index)
                                reasons[index].append("endpoint_championship_mismatch"
                                    if _identifier(row.get("champId")) != champ_id
                                    else row_reasons[0])
                            elif index not in found:
                                found[index] = (match_id, row, proof.raw_hash)
                            elif found[index][2] != proof.raw_hash:
                                # Même ID dans deux championnats ou deux versions
                                # différentes : jamais retenir la première ligne.
                                ambiguous.add(index)
                                reasons[index].append("conflicting_rows")
                        pending[index].remove((window, champ_id))
        except _DeadlineReached:
            report.budget_exceeded = True
        except (urllib.error.URLError, http.client.HTTPException, OSError,
                ValueError, OverflowError, RecursionError) as exc:
            report.error = f"{type(exc).__name__}: {exc}"
            report.budget_exceeded = time.monotonic() >= deadline
        # Une panne conserve les preuves entièrement examinées, pas celles dont
        # un championnat homonyme ou une fenêtre voisine reste à interroger.
        accepted = OrderedDict()
        accepted_indices = set()
        for index in sorted(found):
            if not pending[index] and index not in ambiguous:
                match_id, row, fingerprint = found[index]
                accepted.setdefault(match_id, []).append((row, fingerprint, index))
        for matches in accepted.values():
            if len({fingerprint for _, fingerprint, _ in matches}) == 1:
                report.rows.append(matches[0][0])
                report.rejected -= len(matches)
                accepted_indices.update(index for _, _, index in matches)
            else:
                for _, _, index in matches:
                    reasons[index].append("conflicting_rows")
        for index, ref in enumerate(refs):
            if index in accepted_indices:
                continue
            if reasons[index]:
                reason = reasons[index][0]
            elif pending.get(index):
                reason = "budget_exceeded" if report.budget_exceeded else "source_error"
            elif index not in matched_champs:
                reason = "championship_not_found"
            else:
                reason = "match_id_not_found"
            report.diagnostics.append(_diagnostic(ref, index, reason, checked_champs[index]))
        return report
