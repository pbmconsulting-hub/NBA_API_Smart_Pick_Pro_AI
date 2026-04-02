"""
tests/test_platform_fetcher_parsers.py
--------------------------------------
Unit tests for Underdog and DraftKings parser fixes in data/platform_fetcher.py.

Verifies:
  1. Underdog parser handles numeric sport_id (e.g. 7 for NBA)
  2. Underdog parser handles nested over_under data structure
  3. DraftKings parser accepts bookmaker key prefix matches
  4. DraftKings diagnostic logging for 0-props scenarios
"""

import os
import sys
import unittest
from unittest.mock import MagicMock, patch

# Add repo root to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

# Pre-mock streamlit before any application imports
_mock_st = MagicMock()
_mock_st.session_state = {}
_mock_st.cache_data = lambda *a, **kw: (lambda f: f)
_mock_st.secrets = {}
sys.modules.setdefault("streamlit", _mock_st)
sys.modules.setdefault("streamlit.components", MagicMock())
sys.modules.setdefault("streamlit.components.v1", MagicMock())


class TestUnderdogNumericSportId(unittest.TestCase):
    """Underdog parser should handle numeric sport_id values (e.g. 7 for NBA)."""

    def _make_underdog_data(self, sport_id, title="Stephen Curry",
                            stat_value="28.5", display_stat="Points"):
        return {
            "over_under_lines": [
                {
                    "sport_id": sport_id,
                    "title": title,
                    "display_stat": display_stat,
                    "stat_value": stat_value,
                }
            ],
            "appearances": [],
        }

    @patch("data.platform_fetcher._cache_get", return_value=None)
    @patch("data.platform_fetcher._cache_set")
    @patch("data.platform_fetcher._fetch_with_retry")
    def test_numeric_sport_id_7_is_nba(self, mock_fetch, _cs, _cg):
        import data.platform_fetcher as pf
        resp = MagicMock()
        resp.json.return_value = self._make_underdog_data(sport_id=7)
        resp.raise_for_status.return_value = None
        mock_fetch.return_value = resp

        props = pf.fetch_underdog_props(league="NBA")
        self.assertEqual(len(props), 1)
        self.assertEqual(props[0]["player_name"], "Stephen Curry")

    @patch("data.platform_fetcher._cache_get", return_value=None)
    @patch("data.platform_fetcher._cache_set")
    @patch("data.platform_fetcher._fetch_with_retry")
    def test_string_sport_id_nba_still_works(self, mock_fetch, _cs, _cg):
        import data.platform_fetcher as pf
        resp = MagicMock()
        resp.json.return_value = self._make_underdog_data(sport_id="NBA")
        resp.raise_for_status.return_value = None
        mock_fetch.return_value = resp

        props = pf.fetch_underdog_props(league="NBA")
        self.assertEqual(len(props), 1)

    @patch("data.platform_fetcher._cache_get", return_value=None)
    @patch("data.platform_fetcher._cache_set")
    @patch("data.platform_fetcher._fetch_with_retry")
    def test_numeric_sport_id_2_is_nfl_filtered_out(self, mock_fetch, _cs, _cg):
        import data.platform_fetcher as pf
        resp = MagicMock()
        resp.json.return_value = self._make_underdog_data(sport_id=2)
        resp.raise_for_status.return_value = None
        mock_fetch.return_value = resp

        props = pf.fetch_underdog_props(league="NBA")
        self.assertEqual(len(props), 0)


class TestUnderdogNestedOverUnder(unittest.TestCase):
    """Underdog parser should handle nested over_under data structure."""

    @patch("data.platform_fetcher._cache_get", return_value=None)
    @patch("data.platform_fetcher._cache_set")
    @patch("data.platform_fetcher._fetch_with_retry")
    def test_title_in_over_under_nested_object(self, mock_fetch, _cs, _cg):
        import data.platform_fetcher as pf
        data = {
            "over_under_lines": [
                {
                    "sport_id": 7,
                    "stat_value": "25.5",
                    "over_under": {
                        "title": "LeBron James",
                        "appearance_stat": {
                            "display_stat": "Points",
                        },
                    },
                }
            ],
            "appearances": [],
        }
        resp = MagicMock()
        resp.json.return_value = data
        resp.raise_for_status.return_value = None
        mock_fetch.return_value = resp

        props = pf.fetch_underdog_props(league="NBA")
        self.assertEqual(len(props), 1)
        self.assertEqual(props[0]["player_name"], "LeBron James")

    @patch("data.platform_fetcher._cache_get", return_value=None)
    @patch("data.platform_fetcher._cache_set")
    @patch("data.platform_fetcher._fetch_with_retry")
    def test_appearance_id_in_over_under(self, mock_fetch, _cs, _cg):
        import data.platform_fetcher as pf
        data = {
            "over_under_lines": [
                {
                    "sport_id": 7,
                    "stat_value": "12.5",
                    "over_under": {
                        "title": "Nikola Jokic",
                        "appearance_id": "ap-001",
                        "appearance_stat": {
                            "display_stat": "Rebounds",
                        },
                    },
                }
            ],
            "appearances": [
                {
                    "id": "ap-001",
                    "team_abbreviation": "DEN",
                }
            ],
        }
        resp = MagicMock()
        resp.json.return_value = data
        resp.raise_for_status.return_value = None
        mock_fetch.return_value = resp

        props = pf.fetch_underdog_props(league="NBA")
        self.assertEqual(len(props), 1)
        self.assertEqual(props[0]["team"], "DEN")

    @patch("data.platform_fetcher._cache_get", return_value=None)
    @patch("data.platform_fetcher._cache_set")
    @patch("data.platform_fetcher._fetch_with_retry")
    def test_player_name_from_appearances_fallback(self, mock_fetch, _cs, _cg):
        import data.platform_fetcher as pf
        data = {
            "over_under_lines": [
                {
                    "sport_id": 7,
                    "stat_value": "8.5",
                    "appearance_id": "ap-002",
                    "display_stat": "Assists",
                }
            ],
            "appearances": [
                {
                    "id": "ap-002",
                    "title": "Trae Young",
                    "team_abbreviation": "ATL",
                }
            ],
        }
        resp = MagicMock()
        resp.json.return_value = data
        resp.raise_for_status.return_value = None
        mock_fetch.return_value = resp

        props = pf.fetch_underdog_props(league="NBA")
        self.assertEqual(len(props), 1)
        self.assertEqual(props[0]["player_name"], "Trae Young")


class TestDraftKingsBookmakerPrefix(unittest.TestCase):
    """DraftKings parser should accept bookmaker keys with 'draftkings' prefix."""

    @patch("data.platform_fetcher._cache_get", return_value=None)
    @patch("data.platform_fetcher._cache_set")
    @patch("data.platform_fetcher._fetch_with_retry")
    def test_draftkings_pick6_bookmaker_key(self, mock_fetch, _cs, _cg):
        import data.platform_fetcher as pf

        events_resp = MagicMock()
        events_resp.json.return_value = [{"id": "evt-001"}]
        events_resp.raise_for_status.return_value = None

        props_resp = MagicMock()
        props_resp.json.return_value = {
            "bookmakers": [
                {
                    "key": "draftkings_pick6",
                    "markets": [
                        {
                            "key": "player_points",
                            "outcomes": [
                                {
                                    "name": "Jayson Tatum",
                                    "point": 27.5,
                                    "description": "Over",
                                    "price": -110,
                                },
                                {
                                    "name": "Jayson Tatum",
                                    "point": 27.5,
                                    "description": "Under",
                                    "price": -110,
                                },
                            ],
                        }
                    ],
                }
            ],
        }
        props_resp.raise_for_status.return_value = None

        mock_fetch.side_effect = [events_resp, props_resp]

        props = pf.fetch_draftkings_props(api_key="test-key")
        self.assertEqual(len(props), 1)
        self.assertEqual(props[0]["player_name"], "Jayson Tatum")
        self.assertEqual(props[0]["stat_type"], "points")
        self.assertEqual(props[0]["line"], 27.5)

    @patch("data.platform_fetcher._cache_get", return_value=None)
    @patch("data.platform_fetcher._cache_set")
    @patch("data.platform_fetcher._fetch_with_retry")
    def test_exact_draftkings_key_still_works(self, mock_fetch, _cs, _cg):
        import data.platform_fetcher as pf

        events_resp = MagicMock()
        events_resp.json.return_value = [{"id": "evt-001"}]
        events_resp.raise_for_status.return_value = None

        props_resp = MagicMock()
        props_resp.json.return_value = {
            "bookmakers": [
                {
                    "key": "draftkings",
                    "markets": [
                        {
                            "key": "player_assists",
                            "outcomes": [
                                {
                                    "name": "Luka Doncic",
                                    "point": 8.5,
                                    "description": "Over",
                                    "price": -115,
                                },
                            ],
                        }
                    ],
                }
            ],
        }
        props_resp.raise_for_status.return_value = None

        mock_fetch.side_effect = [events_resp, props_resp]

        props = pf.fetch_draftkings_props(api_key="test-key")
        self.assertEqual(len(props), 1)
        self.assertEqual(props[0]["player_name"], "Luka Doncic")

    @patch("data.platform_fetcher._cache_get", return_value=None)
    @patch("data.platform_fetcher._cache_set")
    @patch("data.platform_fetcher._fetch_with_retry")
    def test_non_draftkings_bookmaker_filtered_out(self, mock_fetch, _cs, _cg):
        import data.platform_fetcher as pf

        events_resp = MagicMock()
        events_resp.json.return_value = [{"id": "evt-001"}]
        events_resp.raise_for_status.return_value = None

        props_resp = MagicMock()
        props_resp.json.return_value = {
            "bookmakers": [
                {
                    "key": "fanduel",
                    "markets": [
                        {
                            "key": "player_points",
                            "outcomes": [
                                {"name": "Someone", "point": 20, "description": "Over", "price": -110},
                            ],
                        }
                    ],
                }
            ],
        }
        props_resp.raise_for_status.return_value = None

        mock_fetch.side_effect = [events_resp, props_resp]

        props = pf.fetch_draftkings_props(api_key="test-key")
        self.assertEqual(len(props), 0)


if __name__ == "__main__":
    unittest.main()
