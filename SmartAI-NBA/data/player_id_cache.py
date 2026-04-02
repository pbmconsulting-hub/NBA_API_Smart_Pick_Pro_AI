"""
Complete player ID management system with fuzzy matching and auto-discovery.
Handles two-way players and G-League call-ups not in NBA database.
"""

import json
import re
import unicodedata
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, Any, List, Tuple

try:
    from thefuzz import fuzz
    _FUZZ_AVAILABLE = True
except ImportError:
    _FUZZ_AVAILABLE = False

from utils.logger import get_logger

logger = get_logger(__name__)


class PlayerIDCache:
    """Manages player IDs with fuzzy matching and fallback strategies."""

    FUZZY_MATCH_THRESHOLD = 70  # minimum token_set_ratio score (0-100)

    def __init__(self):
        self.cache_file = Path(__file__).parent / "player_id_overrides.json"
        self.cache: Dict[str, Any] = self._load_cache()
        self._nba_player_names: Dict[str, Dict] = {}  # Populated from API
        self._team_map: Dict[str, str] = self._get_team_map()

    # ── static helpers ───────────────────────────────────────

    @staticmethod
    def _get_team_map() -> Dict[str, str]:
        """Map team abbreviations to full names."""
        return {
            "ATL": "Hawks", "BOS": "Celtics", "BKN": "Nets", "CHA": "Hornets",
            "CHI": "Bulls", "CLE": "Cavaliers", "DAL": "Mavericks", "DEN": "Nuggets",
            "DET": "Pistons", "GSW": "Warriors", "HOU": "Rockets", "IND": "Pacers",
            "LAC": "Clippers", "LAL": "Lakers", "MEM": "Grizzlies", "MIA": "Heat",
            "MIL": "Bucks", "MIN": "Timberwolves", "NOP": "Pelicans", "NYK": "Knicks",
            "OKC": "Thunder", "ORL": "Magic", "PHI": "76ers", "PHX": "Suns",
            "POR": "Trail Blazers", "SAC": "Kings", "SAS": "Spurs", "TOR": "Raptors",
            "UTA": "Jazz", "WAS": "Wizards",
        }

    # ── persistence ──────────────────────────────────────────

    def _load_cache(self) -> Dict[str, Any]:
        """Load player ID overrides from JSON file."""
        if self.cache_file.exists():
            try:
                with open(self.cache_file, "r", encoding="utf-8") as f:
                    return json.load(f)
            except (json.JSONDecodeError, OSError) as exc:
                logger.error("Failed to parse %s: %s", self.cache_file, exc)
                return {}
        return {}

    def _save_cache(self):
        """Save cache to JSON file."""
        try:
            self.cache_file.parent.mkdir(parents=True, exist_ok=True)
            with open(self.cache_file, "w", encoding="utf-8") as f:
                json.dump(self.cache, f, indent=2)
        except OSError as exc:
            logger.error("Failed to save player ID cache: %s", exc)

    # ── NBA player list ingestion ────────────────────────────

    def load_nba_player_names(self, players_list: List[Dict]):
        """Load official NBA player names for matching."""
        for player in players_list:
            name = player.get("full_name", player.get("name", ""))
            player_id = player.get("id")
            team = player.get("team_abbreviation", player.get("team", ""))
            if name and player_id:
                self._nba_player_names[name] = {
                    "id": player_id,
                    "team": team,
                }
        logger.debug("Loaded %d NBA players for matching", len(self._nba_player_names))

    # ── name normalisation ───────────────────────────────────

    @staticmethod
    def _normalize_name(name: str) -> str:
        """Normalize player name for matching."""
        # Remove suffixes like Jr., Sr., III
        name = re.sub(r"\s+(Jr\.|Sr\.|II|III|IV)$", "", name, flags=re.IGNORECASE)
        # Remove periods and convert to lowercase
        name = name.replace(".", "").lower().strip()
        # Remove accents
        name = unicodedata.normalize("NFKD", name).encode("ASCII", "ignore").decode("ASCII")
        return name

    # ── fuzzy matching ───────────────────────────────────────

    def _fuzzy_match(self, name: str, team: Optional[str] = None) -> Optional[Tuple[str, int]]:
        """Fuzzy match player name against known players."""
        normalized = self._normalize_name(name)

        # Try exact match on normalized names first
        for known_name, data in self._nba_player_names.items():
            if self._normalize_name(known_name) == normalized:
                return (known_name, data["id"])

        if not _FUZZ_AVAILABLE:
            return None

        # Build candidate list (optionally filtered by team)
        candidates: List[str] = []
        if team:
            for known_name, data in self._nba_player_names.items():
                if data.get("team") == team or data.get("team") == self._team_map.get(team, ""):
                    candidates.append(known_name)
        if not candidates:
            candidates = list(self._nba_player_names.keys())

        # Fuzzy match using token_set_ratio for better name matching
        best_match: Optional[str] = None
        best_score = 0

        for candidate in candidates:
            score = fuzz.token_set_ratio(normalized, self._normalize_name(candidate))
            if score > best_score and score > self.FUZZY_MATCH_THRESHOLD:
                best_score = score
                best_match = candidate

        if best_match:
            return (best_match, self._nba_player_names[best_match]["id"])

        return None

    # ── public API ───────────────────────────────────────────

    def get_player_id(self, player_name: str, team: Optional[str] = None) -> Optional[int]:
        """
        Get player ID using multiple strategies:
        1. Exact match in overrides
        2. Fuzzy match against NBA database
        3. Name variants (without suffix, last-name-only)
        """
        # Strategy 1: exact match in overrides
        if player_name in self.cache:
            cached = self.cache[player_name]
            pid = cached["id"] if isinstance(cached, dict) else cached
            logger.debug("Override match: %s -> %s", player_name, pid)
            return pid

        # Strategy 2: fuzzy match against NBA database
        match = self._fuzzy_match(player_name, team)
        if match:
            logger.debug("Fuzzy match: %s -> %s (%s)", player_name, match[0], match[1])
            return match[1]

        # Strategy 3: name variants
        name_variants = [
            player_name.replace(" Jr.", ""),
            player_name.replace(" Sr.", ""),
        ]
        parts = player_name.split()
        if parts:
            name_variants.append(parts[-1])  # last name only

        for variant in name_variants:
            if variant in self.cache:
                cached_v = self.cache[variant]
                return cached_v["id"] if isinstance(cached_v, dict) else cached_v

        logger.warning("No ID found for %s (team: %s)", player_name, team)
        return None

    def add_override(self, player_name: str, player_id: int, team: str, source: str = "manual"):
        """Add a player ID override."""
        self.cache[player_name] = {
            "id": player_id,
            "team": team,
            "source": source,
            "added_at": datetime.now().isoformat(),
        }
        self._save_cache()
        logger.info("Added override for %s (%s): %s", player_name, team, player_id)

    def list_overrides(self) -> Dict[str, Any]:
        """Return all overrides for debugging."""
        return self.cache

    def get_missing_players(self, target_players: List[str]) -> List[str]:
        """Return list of players missing from cache and NBA player list."""
        missing: List[str] = []
        for player in target_players:
            if player not in self.cache and player not in self._nba_player_names:
                missing.append(player)
        return missing
