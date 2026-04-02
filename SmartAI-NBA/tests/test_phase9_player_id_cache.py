# ============================================================
# FILE: tests/test_phase9_player_id_cache.py
# PURPOSE: Phase 9 — Unit tests for data/player_id_cache.py
#   9E: PlayerIDCache name normalization
#   9F: PlayerIDCache override persistence
#   9G: PlayerIDCache fuzzy matching
#   9H: PlayerIDCache get_player_id strategies
#   9I: PlayerIDCache missing player detection
# ============================================================
import json
import os
import shutil
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch


def _ensure_streamlit_mock():
    """Inject a lightweight streamlit mock if not installed."""
    if "streamlit" not in sys.modules:
        mock_st = MagicMock()
        mock_st.session_state = {}
        mock_st.cache_data = lambda **kw: (lambda f: f)
        sys.modules["streamlit"] = mock_st


_ensure_streamlit_mock()


# ============================================================
# 9E: PlayerIDCache — name normalization
# ============================================================

class TestPlayerIDCacheNormalization(unittest.TestCase):
    """Verify _normalize_name strips suffixes, accents, dots."""

    def setUp(self):
        from data.player_id_cache import PlayerIDCache
        self.normalize = PlayerIDCache._normalize_name

    def test_lowercase(self):
        self.assertEqual(self.normalize("LeBron James"), "lebron james")

    def test_strip_jr(self):
        self.assertEqual(self.normalize("Larry Nance Jr."), "larry nance")

    def test_strip_sr(self):
        self.assertEqual(self.normalize("Gary Payton Sr."), "gary payton")

    def test_strip_iii(self):
        self.assertEqual(self.normalize("Robert Williams III"), "robert williams")

    def test_strip_iv(self):
        self.assertEqual(self.normalize("Marcus Johnson IV"), "marcus johnson")

    def test_strip_ii(self):
        self.assertEqual(self.normalize("Wendell Carter II"), "wendell carter")

    def test_remove_periods(self):
        self.assertEqual(self.normalize("P.J. Tucker"), "pj tucker")

    def test_remove_accents(self):
        result = self.normalize("Nikola Jokić")
        self.assertEqual(result, "nikola jokic")

    def test_whitespace_trimmed(self):
        self.assertEqual(self.normalize("  Stephen Curry  "), "stephen curry")

    def test_combined_normalization(self):
        # Note: trailing spaces prevent the regex $ anchor from matching Jr.
        # So normalize trims, but Jr becomes "jr" (period removed, not stripped)
        result = self.normalize("Kenyon Martin Jr.")
        self.assertEqual(result, "kenyon martin")


# ============================================================
# 9F: PlayerIDCache — override persistence
# ============================================================

class TestPlayerIDCacheOverrides(unittest.TestCase):
    """Verify add_override stores and retrieves player data."""

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        self.cache_file = Path(self.tmpdir) / "player_id_overrides.json"
        # Write empty override file
        with open(self.cache_file, "w") as f:
            json.dump({}, f)

    def tearDown(self):
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def _make_cache(self):
        from data.player_id_cache import PlayerIDCache
        cache = PlayerIDCache.__new__(PlayerIDCache)
        cache.cache_file = self.cache_file
        cache.cache = cache._load_cache()
        cache._nba_player_names = {}
        cache._team_map = cache._get_team_map()
        return cache

    def test_add_override(self):
        cache = self._make_cache()
        cache.add_override("Test Player", 12345, "LAL", source="test")
        self.assertIn("Test Player", cache.cache)
        self.assertEqual(cache.cache["Test Player"]["id"], 12345)
        self.assertEqual(cache.cache["Test Player"]["team"], "LAL")

    def test_override_persists_to_file(self):
        cache = self._make_cache()
        cache.add_override("Test Player", 12345, "LAL")
        # Reload from file
        with open(self.cache_file, "r") as f:
            data = json.load(f)
        self.assertIn("Test Player", data)
        self.assertEqual(data["Test Player"]["id"], 12345)

    def test_list_overrides(self):
        cache = self._make_cache()
        cache.add_override("Player A", 100, "BOS")
        cache.add_override("Player B", 200, "MIA")
        overrides = cache.list_overrides()
        self.assertEqual(len(overrides), 2)
        self.assertIn("Player A", overrides)
        self.assertIn("Player B", overrides)

    def test_override_includes_timestamp(self):
        cache = self._make_cache()
        cache.add_override("Player A", 100, "BOS")
        self.assertIn("added_at", cache.cache["Player A"])

    def test_override_includes_source(self):
        cache = self._make_cache()
        cache.add_override("Player A", 100, "BOS", source="api_fallback")
        self.assertEqual(cache.cache["Player A"]["source"], "api_fallback")


# ============================================================
# 9G: PlayerIDCache — fuzzy matching
# ============================================================

class TestPlayerIDCacheFuzzyMatch(unittest.TestCase):
    """Verify _fuzzy_match against loaded NBA player list."""

    def _make_cache_with_players(self, players):
        from data.player_id_cache import PlayerIDCache
        cache = PlayerIDCache.__new__(PlayerIDCache)
        cache.cache_file = Path("/tmp/nonexistent_override.json")
        cache.cache = {}
        cache._nba_player_names = {}
        cache._team_map = cache._get_team_map()
        cache.load_nba_player_names(players)
        return cache

    def test_exact_normalized_match(self):
        cache = self._make_cache_with_players([
            {"full_name": "Stephen Curry", "id": 201939, "team_abbreviation": "GSW"},
        ])
        match = cache._fuzzy_match("Stephen Curry")
        self.assertIsNotNone(match)
        self.assertEqual(match[1], 201939)

    def test_normalized_match_ignores_case(self):
        cache = self._make_cache_with_players([
            {"full_name": "Giannis Antetokounmpo", "id": 203507, "team_abbreviation": "MIL"},
        ])
        match = cache._fuzzy_match("giannis antetokounmpo")
        self.assertIsNotNone(match)
        self.assertEqual(match[1], 203507)

    def test_match_strips_suffix(self):
        cache = self._make_cache_with_players([
            {"full_name": "Larry Nance", "id": 1001, "team_abbreviation": "ATL"},
        ])
        match = cache._fuzzy_match("Larry Nance Jr.")
        self.assertIsNotNone(match)
        self.assertEqual(match[1], 1001)

    def test_no_match_returns_none(self):
        cache = self._make_cache_with_players([
            {"full_name": "Stephen Curry", "id": 201939, "team_abbreviation": "GSW"},
        ])
        match = cache._fuzzy_match("Completely Unknown Player")
        self.assertIsNone(match)

    def test_team_filter_narrows_candidates(self):
        cache = self._make_cache_with_players([
            {"full_name": "Marcus Morris", "id": 1001, "team_abbreviation": "CLE"},
            {"full_name": "Marcus Morris", "id": 1002, "team_abbreviation": "LAC"},
        ])
        # When team is specified, should match from that team
        match = cache._fuzzy_match("Marcus Morris", team="LAC")
        self.assertIsNotNone(match)

    def test_load_nba_player_names_formats(self):
        """Verify both field name formats work."""
        cache = self._make_cache_with_players([
            {"full_name": "Player A", "id": 100, "team_abbreviation": "BOS"},
            {"name": "Player B", "id": 200, "team": "MIA"},
        ])
        self.assertIn("Player A", cache._nba_player_names)
        self.assertIn("Player B", cache._nba_player_names)


# ============================================================
# 9H: PlayerIDCache — get_player_id strategies
# ============================================================

class TestPlayerIDCacheGetPlayerID(unittest.TestCase):
    """Verify multi-strategy player ID resolution."""

    def _make_cache(self, overrides=None, players=None):
        from data.player_id_cache import PlayerIDCache
        cache = PlayerIDCache.__new__(PlayerIDCache)
        cache.cache_file = Path("/tmp/nonexistent_override.json")
        cache.cache = overrides or {}
        cache._nba_player_names = {}
        cache._team_map = cache._get_team_map()
        if players:
            cache.load_nba_player_names(players)
        return cache

    def test_strategy1_exact_override_dict(self):
        """Override stored as dict with 'id' key."""
        cache = self._make_cache(overrides={
            "Coby White": {"id": 1629632, "team": "CHI", "source": "manual"}
        })
        self.assertEqual(cache.get_player_id("Coby White"), 1629632)

    def test_strategy1_exact_override_int(self):
        """Override stored as plain int."""
        cache = self._make_cache(overrides={"Coby White": 1629632})
        self.assertEqual(cache.get_player_id("Coby White"), 1629632)

    def test_strategy2_fuzzy_match(self):
        """Falls back to fuzzy match if not in overrides."""
        cache = self._make_cache(players=[
            {"full_name": "Stephen Curry", "id": 201939, "team_abbreviation": "GSW"},
        ])
        result = cache.get_player_id("Stephen Curry")
        self.assertEqual(result, 201939)

    def test_strategy3_name_variant_jr(self):
        """Falls back to name variant without Jr."""
        cache = self._make_cache(overrides={
            "Jaren Jackson": {"id": 999, "team": "MEM"}
        })
        result = cache.get_player_id("Jaren Jackson Jr.")
        self.assertEqual(result, 999)

    def test_strategy3_name_variant_sr(self):
        """Falls back to name variant without Sr."""
        cache = self._make_cache(overrides={
            "Gary Payton": {"id": 888, "team": "GSW"}
        })
        result = cache.get_player_id("Gary Payton Sr.")
        self.assertEqual(result, 888)

    def test_strategy3_last_name_fallback(self):
        """Falls back to last name only."""
        cache = self._make_cache(overrides={
            "Curry": {"id": 201939, "team": "GSW"}
        })
        result = cache.get_player_id("Stephen Curry")
        self.assertEqual(result, 201939)

    def test_returns_none_when_not_found(self):
        cache = self._make_cache()
        result = cache.get_player_id("Nonexistent Player", team="UNK")
        self.assertIsNone(result)


# ============================================================
# 9I: PlayerIDCache — missing player detection
# ============================================================

class TestPlayerIDCacheMissing(unittest.TestCase):
    """Verify get_missing_players identifies unmatched players."""

    def test_all_found(self):
        from data.player_id_cache import PlayerIDCache
        cache = PlayerIDCache.__new__(PlayerIDCache)
        cache.cache_file = Path("/tmp/nonexistent_override.json")
        cache.cache = {"Player A": {"id": 1}}
        cache._nba_player_names = {"Player B": {"id": 2}}
        cache._team_map = {}

        missing = cache.get_missing_players(["Player A", "Player B"])
        self.assertEqual(missing, [])

    def test_some_missing(self):
        from data.player_id_cache import PlayerIDCache
        cache = PlayerIDCache.__new__(PlayerIDCache)
        cache.cache_file = Path("/tmp/nonexistent_override.json")
        cache.cache = {"Player A": {"id": 1}}
        cache._nba_player_names = {}
        cache._team_map = {}

        missing = cache.get_missing_players(["Player A", "Player B", "Player C"])
        self.assertEqual(sorted(missing), ["Player B", "Player C"])

    def test_empty_list(self):
        from data.player_id_cache import PlayerIDCache
        cache = PlayerIDCache.__new__(PlayerIDCache)
        cache.cache_file = Path("/tmp/nonexistent_override.json")
        cache.cache = {}
        cache._nba_player_names = {}
        cache._team_map = {}

        missing = cache.get_missing_players([])
        self.assertEqual(missing, [])


# ============================================================
# 9J: PlayerIDCache — team map
# ============================================================

class TestPlayerIDCacheTeamMap(unittest.TestCase):
    """Verify team map has all 30 NBA teams."""

    def test_thirty_teams(self):
        from data.player_id_cache import PlayerIDCache
        team_map = PlayerIDCache._get_team_map()
        self.assertEqual(len(team_map), 30)

    def test_known_teams(self):
        from data.player_id_cache import PlayerIDCache
        team_map = PlayerIDCache._get_team_map()
        self.assertEqual(team_map["LAL"], "Lakers")
        self.assertEqual(team_map["BOS"], "Celtics")
        self.assertEqual(team_map["GSW"], "Warriors")
        self.assertEqual(team_map["CHI"], "Bulls")

    def test_all_three_letter_abbrevs(self):
        from data.player_id_cache import PlayerIDCache
        team_map = PlayerIDCache._get_team_map()
        for abbrev in team_map:
            self.assertEqual(len(abbrev), 3)
            self.assertTrue(abbrev.isupper())


# ============================================================
# 9K: PlayerIDCache — cache file handling
# ============================================================

class TestPlayerIDCacheFileHandling(unittest.TestCase):
    """Verify graceful handling of corrupt/missing cache files."""

    def test_missing_file_returns_empty(self):
        from data.player_id_cache import PlayerIDCache
        cache = PlayerIDCache.__new__(PlayerIDCache)
        cache.cache_file = Path("/tmp/definitely_does_not_exist.json")
        result = cache._load_cache()
        self.assertEqual(result, {})

    def test_corrupt_file_returns_empty(self):
        from data.player_id_cache import PlayerIDCache
        tmpdir = tempfile.mkdtemp()
        try:
            cache_file = Path(tmpdir) / "corrupt.json"
            with open(cache_file, "w") as f:
                f.write("{not valid json")
            cache = PlayerIDCache.__new__(PlayerIDCache)
            cache.cache_file = cache_file
            result = cache._load_cache()
            self.assertEqual(result, {})
        finally:
            shutil.rmtree(tmpdir, ignore_errors=True)

    def test_valid_file_loads(self):
        from data.player_id_cache import PlayerIDCache
        tmpdir = tempfile.mkdtemp()
        try:
            cache_file = Path(tmpdir) / "valid.json"
            data = {"Coby White": {"id": 1629632, "team": "CHI"}}
            with open(cache_file, "w") as f:
                json.dump(data, f)
            cache = PlayerIDCache.__new__(PlayerIDCache)
            cache.cache_file = cache_file
            result = cache._load_cache()
            self.assertEqual(result["Coby White"]["id"], 1629632)
        finally:
            shutil.rmtree(tmpdir, ignore_errors=True)


if __name__ == "__main__":
    unittest.main()
