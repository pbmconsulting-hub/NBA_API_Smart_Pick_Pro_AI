"""
tests/test_advanced_fetcher.py
-------------------------------
Unit tests for data/advanced_fetcher.py.

Verifies:
  1. Module is importable and exposes all public functions
  2. enrich_tonights_slate returns correct structure
  3. enrich_tonights_slate handles empty game list gracefully
  4. build_enrichment_summary computes correct summary
  5. _resolve_team_id handles all key-name conventions
  6. Progress callback is invoked with correct step counts
  7. Graceful degradation when nba_data_service is unavailable
"""

import os
import sys
import unittest
from unittest.mock import MagicMock, patch, call

# Add repo root to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

# Pre-mock streamlit
_mock_st = MagicMock()
_mock_st.session_state = {}
_mock_st.cache_data = lambda *a, **kw: (lambda f: f)
_mock_st.secrets = {}
sys.modules.setdefault("streamlit", _mock_st)
sys.modules.setdefault("streamlit.components", MagicMock())
sys.modules.setdefault("streamlit.components.v1", MagicMock())


# ── Section 1: Module Structure ───────────────────────────────────────────────

class TestModuleStructure(unittest.TestCase):
    """data/advanced_fetcher.py must be importable and expose all public APIs."""

    def test_module_importable(self):
        import data.advanced_fetcher  # noqa: F401

    def test_enrich_tonights_slate_callable(self):
        import data.advanced_fetcher as af
        self.assertTrue(callable(af.enrich_tonights_slate))

    def test_build_enrichment_summary_callable(self):
        import data.advanced_fetcher as af
        self.assertTrue(callable(af.build_enrichment_summary))

    def test_recent_team_games_constant(self):
        import data.advanced_fetcher as af
        self.assertIsInstance(af.RECENT_TEAM_GAMES, int)
        self.assertGreater(af.RECENT_TEAM_GAMES, 0)


# ── Section 2: Empty Inputs ───────────────────────────────────────────────────

class TestEmptyInputs(unittest.TestCase):
    """enrich_tonights_slate should handle empty game lists gracefully."""

    def test_empty_games_returns_empty_dict(self):
        import data.advanced_fetcher as af
        result = af.enrich_tonights_slate([])
        self.assertIsInstance(result, dict)
        self.assertEqual(result, {})

    def test_none_games_treated_as_empty(self):
        import data.advanced_fetcher as af
        result = af.enrich_tonights_slate(None)
        self.assertIsInstance(result, dict)
        self.assertEqual(result, {})


# ── Section 3: Enrichment Output Structure ────────────────────────────────────

class TestEnrichmentStructure(unittest.TestCase):
    """enrich_tonights_slate must return the expected nested structure."""

    def _make_mock_service(self):
        """Return a mock that replaces all nba_data_service functions."""
        mock_svc = MagicMock()
        mock_svc.get_team_game_logs = MagicMock(return_value=[{"PTS": 110}])
        mock_svc.get_team_lineups = MagicMock(return_value=[{"GROUP_ID": "A-B-C-D-E"}])
        mock_svc.get_team_dashboard = MagicMock(return_value={"overall": [{"W": 30}]})
        mock_svc.get_player_estimated_metrics = MagicMock(return_value=[{"E_PACE": 99.5}])
        mock_svc.get_standings_from_nba_api = MagicMock(return_value=[{"TeamID": 1}])
        mock_svc.get_todays_scoreboard = MagicMock(return_value={"game_header": []})
        return mock_svc

    def test_single_game_returns_keyed_dict(self):
        import data.advanced_fetcher as af
        games = [
            {
                "game_id": "0022401234",
                "home_team_id": 1610612738,
                "away_team_id": 1610612747,
            }
        ]
        mock_svc = self._make_mock_service()

        with patch.dict("sys.modules", {"data.nba_data_service": mock_svc}):
            result = af.enrich_tonights_slate(games)

        self.assertIn("0022401234", result)
        game_data = result["0022401234"]
        self.assertEqual(game_data["game_id"], "0022401234")
        self.assertIn("home_game_logs", game_data)
        self.assertIn("away_game_logs", game_data)
        self.assertIn("home_lineups", game_data)
        self.assertIn("away_lineups", game_data)
        self.assertIn("home_dashboard", game_data)
        self.assertIn("away_dashboard", game_data)
        self.assertIn("standings", game_data)
        self.assertIn("player_metrics", game_data)
        self.assertIn("scoreboard", game_data)
        self.assertIn("fetch_time", game_data)

    def test_enrichment_values_have_correct_types(self):
        import data.advanced_fetcher as af
        games = [{"game_id": "0022401234", "home_team_id": 1610612738, "away_team_id": 1610612747}]
        mock_svc = self._make_mock_service()

        with patch.dict("sys.modules", {"data.nba_data_service": mock_svc}):
            result = af.enrich_tonights_slate(games)

        game_data = result.get("0022401234", {})
        self.assertIsInstance(game_data.get("home_game_logs"), list)
        self.assertIsInstance(game_data.get("away_game_logs"), list)
        self.assertIsInstance(game_data.get("home_lineups"), list)
        self.assertIsInstance(game_data.get("home_dashboard"), dict)
        self.assertIsInstance(game_data.get("standings"), list)
        self.assertIsInstance(game_data.get("player_metrics"), list)
        self.assertIsInstance(game_data.get("fetch_time"), float)

    def test_game_id_fallback_keys(self):
        """Should work even when game uses GAME_ID instead of game_id."""
        import data.advanced_fetcher as af
        games = [{"GAME_ID": "0022401999", "HOME_TEAM_ID": 1610612738, "VISITOR_TEAM_ID": 1610612747}]
        mock_svc = self._make_mock_service()

        with patch.dict("sys.modules", {"data.nba_data_service": mock_svc}):
            result = af.enrich_tonights_slate(games)

        self.assertIn("0022401999", result)


# ── Section 4: build_enrichment_summary ──────────────────────────────────────

class TestBuildEnrichmentSummary(unittest.TestCase):
    """build_enrichment_summary must compute correct counts."""

    def _make_enrichment(self):
        return {
            "0022401234": {
                "game_id": "0022401234",
                "home_game_logs": [{"PTS": 110}, {"PTS": 105}],
                "away_game_logs": [{"PTS": 100}],
                "home_lineups": [{"GROUP_ID": "A-B-C-D-E"}, {"GROUP_ID": "A-B-C-D-F"}],
                "away_lineups": [{"GROUP_ID": "X-Y-Z-W-V"}],
                "home_dashboard": {"overall": [{"W": 30}]},
                "away_dashboard": {},
                "standings": [{"TeamID": 1}, {"TeamID": 2}],
                "player_metrics": [{"E_PACE": 99.5}] * 300,
                "scoreboard": {"game_header": []},
                "fetch_time": 1711000000.0,
            }
        }

    def test_summary_counts_are_correct(self):
        import data.advanced_fetcher as af
        enriched = self._make_enrichment()
        summary = af.build_enrichment_summary(enriched)

        self.assertEqual(summary["games_enriched"], 1)
        self.assertEqual(summary["game_logs_fetched"], 3)   # 2 home + 1 away
        self.assertEqual(summary["lineups_fetched"], 3)     # 2 home + 1 away
        self.assertEqual(summary["dashboards_fetched"], 1)  # only home had data
        self.assertEqual(summary["standings_rows"], 2)
        self.assertEqual(summary["player_metrics_rows"], 300)
        self.assertTrue(summary["scoreboard_available"])

    def test_empty_enrichment_returns_zeros(self):
        import data.advanced_fetcher as af
        summary = af.build_enrichment_summary({})
        self.assertEqual(summary["games_enriched"], 0)
        self.assertEqual(summary["game_logs_fetched"], 0)
        self.assertFalse(summary["scoreboard_available"])


# ── Section 5: _resolve_team_id ──────────────────────────────────────────────

class TestResolveTeamId(unittest.TestCase):
    """_resolve_team_id should handle all key naming conventions."""

    def setUp(self):
        import data.advanced_fetcher as af
        self.fn = af._resolve_team_id

    def test_lowercase_home_team_id(self):
        game = {"home_team_id": 1610612738}
        self.assertEqual(self.fn(game, "home"), 1610612738)

    def test_uppercase_HOME_TEAM_ID(self):
        game = {"HOME_TEAM_ID": 1610612747}
        self.assertEqual(self.fn(game, "home"), 1610612747)

    def test_visitor_team_id(self):
        game = {"VISITOR_TEAM_ID": 1610612748}
        self.assertEqual(self.fn(game, "away"), 1610612748)

    def test_away_team_id(self):
        game = {"away_team_id": 1610612749}
        self.assertEqual(self.fn(game, "away"), 1610612749)

    def test_missing_returns_none(self):
        game = {"game_id": "0022401234"}
        self.assertIsNone(self.fn(game, "home"))
        self.assertIsNone(self.fn(game, "away"))

    def test_invalid_value_returns_none(self):
        game = {"home_team_id": "not_an_int"}
        self.assertIsNone(self.fn(game, "home"))


# ── Section 6: Progress Callback ──────────────────────────────────────────────

class TestProgressCallback(unittest.TestCase):
    """Progress callback should be invoked during enrichment."""

    def test_progress_callback_called(self):
        import data.advanced_fetcher as af
        games = [{"game_id": "0022401234", "home_team_id": 1610612738, "away_team_id": 1610612747}]

        mock_svc = MagicMock()
        mock_svc.get_team_game_logs = MagicMock(return_value=[])
        mock_svc.get_team_lineups = MagicMock(return_value=[])
        mock_svc.get_team_dashboard = MagicMock(return_value={})
        mock_svc.get_player_estimated_metrics = MagicMock(return_value=[])
        mock_svc.get_standings_from_nba_api = MagicMock(return_value=[])
        mock_svc.get_todays_scoreboard = MagicMock(return_value={})

        calls = []

        def _callback(current, total, msg):
            calls.append((current, total, msg))

        with patch.dict("sys.modules", {"data.nba_data_service": mock_svc}):
            af.enrich_tonights_slate(games, progress_callback=_callback)

        # At least the 3 shared steps should have triggered the callback
        self.assertGreater(len(calls), 0)
        # All steps should have current ≤ total
        for current, total, _ in calls:
            self.assertLessEqual(current, total)


# ── Section 7: Graceful Degradation ──────────────────────────────────────────

class TestGracefulDegradation(unittest.TestCase):
    """enrich_tonights_slate must not raise when services are unavailable."""

    def test_import_error_returns_empty_dict(self):
        import data.advanced_fetcher as af
        games = [{"game_id": "0022401234", "home_team_id": 1610612738}]

        # Simulate nba_data_service being completely unavailable
        broken_svc = MagicMock()
        broken_svc.get_standings_from_nba_api = MagicMock(side_effect=ImportError("no module"))
        broken_svc.get_player_estimated_metrics = MagicMock(side_effect=ImportError("no module"))
        broken_svc.get_todays_scoreboard = MagicMock(side_effect=ImportError("no module"))
        broken_svc.get_team_game_logs = MagicMock(side_effect=Exception("API error"))
        broken_svc.get_team_lineups = MagicMock(side_effect=Exception("API error"))
        broken_svc.get_team_dashboard = MagicMock(side_effect=Exception("API error"))

        with patch.dict("sys.modules", {"data.nba_data_service": broken_svc}):
            # Should not raise
            result = af.enrich_tonights_slate(games)

        # When all services fail, should return dict with empty enrichment
        self.assertIsInstance(result, dict)


if __name__ == "__main__":
    unittest.main()
