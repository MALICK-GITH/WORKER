"""Résolution d'identité des matchs : une identité canonique par rencontre.

Le flux 888Starz fournit un identifiant numérique (`I`), des identifiants
stables d'équipes (`O1I`, `O2I`), un identifiant de compétition (`LI`) et
l'heure de coup d'envoi (`S`). L'identité retenue est `I` ; les autres champs
servent uniquement à détecter les incohérences (identifiant réutilisé, match
réaffecté), jamais à fusionner deux rencontres distinctes.
"""

import datetime as dt
import re
import unicodedata
from dataclasses import dataclass

DEFAULT_LEAGUE = "Football virtuel"
MAX_KICKOFF_DRIFT = dt.timedelta(hours=12)


class ResolutionError(ValueError):
    """Le match ne peut pas recevoir d'identité fiable."""


@dataclass(frozen=True)
class MatchIdentity:
    match_id: str
    home: str
    away: str
    league: str
    kickoff: dt.datetime | None
    home_id: str | None = None
    away_id: str | None = None
    league_id: str | None = None

    @property
    def pair_key(self):
        return (normalise_name(self.home), normalise_name(self.away))

    @property
    def team_ids(self):
        if self.home_id is None or self.away_id is None:
            return None
        return (self.home_id, self.away_id)

    @property
    def fingerprint(self) -> str:
        kickoff = self.kickoff.strftime("%Y%m%d%H%M") if self.kickoff else "?"
        return f"{kickoff}|{self.pair_key[0]}|{self.pair_key[1]}"


def _text(value):
    if value is None:
        return ""
    text = str(value).strip()
    return "" if text.lower() in ("none", "null", "nan") else text


def _identifier(value):
    """Les identifiants du flux sont numériques ; tout le reste est rejeté."""
    text = _text(value)
    if not text:
        return None
    try:
        number = int(float(text))
    except (TypeError, ValueError):
        return None
    return str(number) if number > 0 else None


def normalise_name(value) -> str:
    text = unicodedata.normalize("NFKD", _text(value))
    text = "".join(char for char in text if not unicodedata.combining(char))
    text = re.sub(r"[^0-9a-z]+", " ", text.lower())
    return " ".join(text.split())


def parse_kickoff(value) -> dt.datetime | None:
    text = _text(value)
    if not text:
        return None
    try:
        timestamp = float(text)
    except (TypeError, ValueError):
        return None
    try:
        return dt.datetime.fromtimestamp(timestamp, dt.timezone.utc).replace(tzinfo=None)
    except (OSError, OverflowError, ValueError):
        return None


def resolve(match, default_league=DEFAULT_LEAGUE) -> MatchIdentity:
    if not isinstance(match, dict):
        raise ResolutionError("match illisible : objet JSON attendu")
    match_id = _identifier(match.get("I"))
    if not match_id:
        raise ResolutionError("identifiant de match absent (I)")
    home = _text(match.get("O1"))
    away = _text(match.get("O2"))
    if not home or not away:
        raise ResolutionError(f"match {match_id} sans deux équipes nommées")
    league = _text(match.get("LE")) or _text(match.get("L")) or default_league
    return MatchIdentity(
        match_id=match_id,
        home=home,
        away=away,
        league=league,
        kickoff=parse_kickoff(match.get("S")),
        home_id=_identifier(match.get("O1I")),
        away_id=_identifier(match.get("O2I")),
        league_id=_identifier(match.get("LI")),
    )


def conflicts(identity: MatchIdentity, known: "MatchIdentity") -> tuple:
    """Liste les raisons pour lesquelles deux identités du même identifiant divergent."""
    reasons = []
    if identity.team_ids and known.team_ids and identity.team_ids != known.team_ids:
        reasons.append("identifiants d'équipes différents")
    elif identity.pair_key != known.pair_key:
        reasons.append("noms d'équipes différents")
    if identity.kickoff and known.kickoff:
        drift = abs(identity.kickoff - known.kickoff)
        if drift > MAX_KICKOFF_DRIFT:
            reasons.append(f"coup d'envoi décalé de {int(drift.total_seconds() // 3600)} h")
    return tuple(reasons)


def same_fixture(left: MatchIdentity, right: MatchIdentity) -> bool:
    if left.match_id == right.match_id:
        return not conflicts(left, right)
    return left.fingerprint == right.fingerprint and left.fingerprint != "?||"
