# ============================================================
# FILE: tests/test_phase1_retry_and_index.py
# PURPOSE: Tests for Phase 1 improvements:
#          1) Sync get functions use _request_with_retry (exponential backoff)
#          2) Pre-indexed player lookup for O(1) fuzzy matching
# ============================================================

import pathlib
import sys
import unittest
from unittest.mock import MagicMock


def _ensure_streamlit_mock():
    """Inject a lightweight streamlit mock if not installed."""
    if "streamlit" not in sys.modules:
        mock_st = MagicMock()
        mock_st.session_state = {}
        mock_st.cache_data = lambda **kw: (lambda f: f)
        sys.modules["streamlit"] = mock_st


# ============================================================
# Section 2: Pre-Indexed Player Fuzzy Lookup
# ============================================================

class TestPlayerIndexedLookup(unittest.TestCase):
    """Verify that find_player_by_name_fuzzy uses pre-computed indices
    for O(1) exact, alias, and normalized lookups."""

    def setUp(self):
        _ensure_streamlit_mock()
        from data.data_manager import (
            find_player_by_name_fuzzy,
            normalize_player_name,
            _build_player_index,
            _player_index_cache,
        )
        self.fuzzy = find_player_by_name_fuzzy
        self.normalize = normalize_player_name
        self.build_index = _build_player_index
        self.index_cache = _player_index_cache

        self.players = [
            {"name": "LeBron James", "team": "LAL", "points_avg": "25.0"},
            {"name": "Stephen Curry", "team": "GSW", "points_avg": "28.0"},
            {"name": "Nikola Jokić", "team": "DEN", "points_avg": "26.0"},
            {"name": "Nicolas Claxton", "team": "BKN", "points_avg": "10.0"},
            {"name": "Jaren Jackson Jr.", "team": "MEM", "points_avg": "22.0"},
            {"name": "Shai Gilgeous-Alexander", "team": "OKC", "points_avg": "31.0"},
        ]

    def test_build_index_returns_three_dicts(self):
        """_build_player_index returns (lower_index, alias_index, normalized_index)."""
        lower_idx, alias_idx, norm_idx = self.build_index(self.players)
        self.assertIsInstance(lower_idx, dict)
        self.assertIsInstance(alias_idx, dict)
        self.assertIsInstance(norm_idx, dict)

    def test_lower_index_keys(self):
        """Lower index should contain lowered player names."""
        lower_idx, _, _ = self.build_index(self.players)
        self.assertIn("lebron james", lower_idx)
        self.assertIn("stephen curry", lower_idx)

    def test_alias_index_resolves_nicknames(self):
        """Alias index should resolve known nicknames to player dicts."""
        _, alias_idx, _ = self.build_index(self.players)
        # "steph" → "stephen curry" alias
        self.assertIn("steph", alias_idx)
        self.assertEqual(alias_idx["steph"]["name"], "Stephen Curry")

    def test_normalized_index_handles_unicode(self):
        """Normalized index should map unicode-stripped names to players."""
        _, _, norm_idx = self.build_index(self.players)
        # "Nikola Jokić" → normalized "nikola jokic"
        self.assertIn("nikola jokic", norm_idx)

    def test_exact_match(self):
        """Pass 1: exact case-insensitive match (O(1))."""
        result = self.fuzzy(self.players, "LeBron James")
        self.assertIsNotNone(result)
        self.assertEqual(result["name"], "LeBron James")

    def test_alias_match(self):
        """Pass 2: alias lookup for common nicknames (O(1))."""
        result = self.fuzzy(self.players, "steph")
        self.assertIsNotNone(result)
        self.assertEqual(result["name"], "Stephen Curry")

    def test_sga_alias(self):
        """Alias 'sga' should resolve to Shai Gilgeous-Alexander."""
        result = self.fuzzy(self.players, "sga")
        self.assertIsNotNone(result)
        self.assertEqual(result["name"], "Shai Gilgeous-Alexander")

    def test_normalized_unicode_match(self):
        """Pass 3: normalized match handles unicode accents."""
        result = self.fuzzy(self.players, "Nikola Jokic")  # no accent
        self.assertIsNotNone(result)
        self.assertEqual(result["team"], "DEN")

    def test_nic_claxton_alias(self):
        """'Nic Claxton' alias should resolve to 'Nicolas Claxton'."""
        result = self.fuzzy(self.players, "Nic Claxton")
        self.assertIsNotNone(result)
        self.assertEqual(result["name"], "Nicolas Claxton")

    def test_suffix_stripped_match(self):
        """Pass 3: suffix stripping should match 'Jaren Jackson' to 'Jaren Jackson Jr.'"""
        result = self.fuzzy(self.players, "Jaren Jackson")
        self.assertIsNotNone(result)
        self.assertEqual(result["team"], "MEM")

    def test_none_player_name(self):
        """None or empty player_name should return None."""
        self.assertIsNone(self.fuzzy(self.players, None))
        self.assertIsNone(self.fuzzy(self.players, ""))

    def test_not_found(self):
        """Unknown player should return None."""
        result = self.fuzzy(self.players, "Unknown Player XYZ")
        self.assertIsNone(result)

    def test_index_cache_reuse(self):
        """Index cache should be reused for the same players_list object."""
        # First call builds the cache
        self.fuzzy(self.players, "LeBron James")
        cached_id = self.index_cache["list_id"]
        self.assertEqual(cached_id, id(self.players))

        # Second call should reuse the cache (same list object)
        self.fuzzy(self.players, "Stephen Curry")
        self.assertEqual(self.index_cache["list_id"], cached_id)

    def test_index_cache_invalidated_on_new_list(self):
        """Index cache should rebuild when a different players_list is passed."""
        self.fuzzy(self.players, "LeBron James")
        old_id = self.index_cache["list_id"]

        # New list object → cache should rebuild
        new_players = list(self.players)  # shallow copy = different id()
        self.fuzzy(new_players, "LeBron James")
        self.assertNotEqual(self.index_cache["list_id"], old_id)

    def test_partial_substring_match(self):
        """Pass 4: partial/substring fallback should work for partial names."""
        result = self.fuzzy(self.players, "Gilgeous-Alexander")
        self.assertIsNotNone(result)
        self.assertEqual(result["team"], "OKC")


if __name__ == "__main__":
    unittest.main()
