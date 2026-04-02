"""
tests/test_nba_stats_service.py
--------------------------------
Unit tests for data/nba_stats_service.py.

Verifies:
  1. Module structure and public API surface
  2. Caching logic (TTL-based)
  3. Rate-limit / circuit-breaker bypass returns empty
  4. Each public function returns expected schema on mock success
  5. Each public function returns safe defaults on failure
  6. Season string normalisation helper
  7. Player intelligence integration functions
  8. nba_data_service integration functions
  9. Simulation advanced-stats enrichment function
"""

import os
import sys
import unittest
from unittest.mock import MagicMock, patch, PropertyMock

# Add repo root to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

# Pre-mock streamlit before importing application modules
_mock_st = MagicMock()
_mock_st.session_state = {}
_mock_st.cache_data = lambda *a, **kw: (lambda f: f)
_mock_st.secrets = {}
sys.modules.setdefault("streamlit", _mock_st)
sys.modules.setdefault("streamlit.components", MagicMock())
sys.modules.setdefault("streamlit.components.v1", MagicMock())

# Check whether nba_api and pandas are available. Tests that patch live
# nba_api endpoints cannot function without the real package installed.
try:
    import nba_api  # noqa: F401
    _HAS_NBA_API = True
except ModuleNotFoundError:
    _HAS_NBA_API = False

try:
    import pandas  # noqa: F401
    _HAS_PANDAS = True
except ModuleNotFoundError:
    _HAS_PANDAS = False


# ── Section 1: Module structure ───────────────────────────────────────────────

class TestModuleStructure(unittest.TestCase):
    """data/nba_stats_service.py must be importable and expose all public APIs."""

    def test_module_importable(self):
        import data.nba_stats_service  # noqa: F401

    def _assert_callable(self, name):
        import data.nba_stats_service as svc
        fn = getattr(svc, name, None)
        self.assertIsNotNone(fn, f"{name} not found")
        self.assertTrue(callable(fn), f"{name} is not callable")

    def test_get_all_players(self):
        self._assert_callable("get_all_players")

    def test_get_player_info(self):
        self._assert_callable("get_player_info")

    def test_get_player_game_logs(self):
        self._assert_callable("get_player_game_logs")

    def test_get_player_career_stats(self):
        self._assert_callable("get_player_career_stats")

    def test_get_player_splits(self):
        self._assert_callable("get_player_splits")

    def test_get_advanced_box_score(self):
        self._assert_callable("get_advanced_box_score")

    def test_get_player_tracking(self):
        self._assert_callable("get_player_tracking_box_score")

    def test_get_hustle_box_score(self):
        self._assert_callable("get_hustle_box_score")

    def test_get_defensive_matchup(self):
        self._assert_callable("get_defensive_matchup_data")

    def test_get_shot_chart(self):
        self._assert_callable("get_shot_chart")

    def test_get_lineup_stats(self):
        self._assert_callable("get_lineup_stats")

    def test_get_league_standings(self):
        self._assert_callable("get_league_standings")

    def test_find_games(self):
        self._assert_callable("find_games")

    def test_get_play_by_play(self):
        self._assert_callable("get_play_by_play")

    def test_get_team_roster(self):
        self._assert_callable("get_team_roster")

    def test_live_ttl_exists(self):
        from data.nba_stats_service import LIVE_TTL
        self.assertIsInstance(LIVE_TTL, int)
        self.assertGreater(LIVE_TTL, 0)

    def test_hist_ttl_exists(self):
        from data.nba_stats_service import HIST_TTL, LIVE_TTL
        self.assertIsInstance(HIST_TTL, int)
        self.assertGreaterEqual(HIST_TTL, LIVE_TTL)


# ── Section 2: Season helper ──────────────────────────────────────────────────

class TestResolveSeasonHelper(unittest.TestCase):
    """_resolve_season normalises season strings."""

    def _resolve(self, s):
        from data.nba_stats_service import _resolve_season
        return _resolve_season(s)

    def test_none_returns_current_season(self):
        from data.nba_stats_service import _current_season
        self.assertEqual(self._resolve(None), _current_season())

    def test_already_formatted(self):
        self.assertEqual(self._resolve("2024-25"), "2024-25")

    def test_year_string(self):
        self.assertEqual(self._resolve("2024"), "2024-25")

    def test_year_int(self):
        self.assertEqual(self._resolve(2023), "2023-24")

    def test_unknown_passthrough(self):
        self.assertEqual(self._resolve("xyz"), "xyz")


# ── Section 3: Cache helpers ──────────────────────────────────────────────────

class TestCacheHelpers(unittest.TestCase):
    """_cache_get / _cache_set operate correctly."""

    def setUp(self):
        import data.nba_stats_service as svc
        svc._CACHE.clear()

    def test_cache_miss_returns_none(self):
        from data.nba_stats_service import _cache_get
        self.assertIsNone(_cache_get("missing_key", 300))

    def test_cache_hit_returns_payload(self):
        from data.nba_stats_service import _cache_get, _cache_set
        _cache_set("k", [1, 2, 3])
        self.assertEqual(_cache_get("k", 300), [1, 2, 3])

    def test_expired_entry_returns_none(self):
        import time
        from data.nba_stats_service import _cache_get, _cache_set, _CACHE
        _cache_set("expired", {"x": 1})
        # Manually backdate the timestamp
        _CACHE["expired"] = (_CACHE["expired"][0], time.time() - 400)
        self.assertIsNone(_cache_get("expired", 300))


# ── Section 4: get_all_players ────────────────────────────────────────────────

@unittest.skipUnless(_HAS_NBA_API, "nba_api not installed")
class TestGetAllPlayers(unittest.TestCase):

    def setUp(self):
        import data.nba_stats_service as svc
        svc._CACHE.clear()

    def _mock_endpoint(self, rows):
        """Return a mock that looks like a CommonAllPlayers endpoint."""
        mock = MagicMock()
        mock.get_normalized_dict.return_value = {"CommonAllPlayers": rows}
        return mock

    @patch("data.nba_stats_service._check_rate_limit", return_value=True)
    @patch("data.nba_stats_service._NBA_API_AVAILABLE", True)
    @patch("data.nba_stats_service.time.sleep")
    @patch("data.nba_stats_service.time.monotonic", return_value=0.0)
    def test_returns_player_list(self, _mono, _sleep, _rate):

        rows = [
            {
                "PERSON_ID": 2544,
                "DISPLAY_FIRST_LAST": "LeBron James",
                "DISPLAY_LAST_COMMA_FIRST": "James, LeBron",
                "ROSTERSTATUS": 1,
                "TEAM_ID": 1610612747,
                "TEAM_ABBREVIATION": "LAL",
            }
        ]
        mock_ep = self._mock_endpoint(rows)

        with patch("nba_api.stats.endpoints.commonallplayers.CommonAllPlayers",
                   return_value=mock_ep):
            from data.nba_stats_service import get_all_players
            result = get_all_players(active_only=True)

        self.assertIsInstance(result, list)
        self.assertEqual(len(result), 1)
        p = result[0]
        self.assertEqual(p["id"], 2544)
        self.assertEqual(p["full_name"], "LeBron James")
        self.assertEqual(p["team_abbreviation"], "LAL")
        self.assertIn("is_active", p)

    @patch("data.nba_stats_service._check_rate_limit", return_value=True)
    @patch("data.nba_stats_service._NBA_API_AVAILABLE", True)
    def test_returns_empty_on_exception(self, _rate):
        with patch("nba_api.stats.endpoints.commonallplayers.CommonAllPlayers",
                   side_effect=Exception("boom")):
            from data.nba_stats_service import get_all_players
            import data.nba_stats_service as svc
            svc._CACHE.clear()
            result = get_all_players()
        self.assertEqual(result, [])

    @patch("data.nba_stats_service._check_rate_limit", return_value=False)
    @patch("data.nba_stats_service._NBA_API_AVAILABLE", True)
    def test_returns_empty_when_rate_limited(self, _rate):
        from data.nba_stats_service import get_all_players
        import data.nba_stats_service as svc
        svc._CACHE.clear()
        result = get_all_players()
        self.assertEqual(result, [])

    @patch("data.nba_stats_service._NBA_API_AVAILABLE", False)
    def test_returns_empty_when_nba_api_unavailable(self):
        from data.nba_stats_service import get_all_players
        import data.nba_stats_service as svc
        svc._CACHE.clear()
        result = get_all_players()
        self.assertEqual(result, [])

    @patch("data.nba_stats_service._check_rate_limit", return_value=True)
    @patch("data.nba_stats_service._NBA_API_AVAILABLE", True)
    @patch("data.nba_stats_service.time.sleep")
    @patch("data.nba_stats_service.time.monotonic", return_value=0.0)
    def test_caching_prevents_duplicate_calls(self, _mono, _sleep, _rate):
        rows = [{"PERSON_ID": 1, "DISPLAY_FIRST_LAST": "Test Player",
                 "DISPLAY_LAST_COMMA_FIRST": "Player, Test",
                 "ROSTERSTATUS": 1, "TEAM_ID": 1, "TEAM_ABBREVIATION": "TST"}]
        mock_ep = self._mock_endpoint(rows)

        import data.nba_stats_service as svc
        svc._CACHE.clear()

        with patch("nba_api.stats.endpoints.commonallplayers.CommonAllPlayers",
                   return_value=mock_ep) as mock_cls:
            from data.nba_stats_service import get_all_players
            get_all_players(active_only=True)
            get_all_players(active_only=True)  # second call hits cache
            self.assertEqual(mock_cls.call_count, 1)


# ── Section 5: get_player_info ────────────────────────────────────────────────

@unittest.skipUnless(_HAS_NBA_API, "nba_api not installed")
class TestGetPlayerInfo(unittest.TestCase):

    def setUp(self):
        import data.nba_stats_service as svc
        svc._CACHE.clear()

    @patch("data.nba_stats_service._check_rate_limit", return_value=True)
    @patch("data.nba_stats_service._NBA_API_AVAILABLE", True)
    @patch("data.nba_stats_service.time.sleep")
    @patch("data.nba_stats_service.time.monotonic", return_value=0.0)
    def test_returns_player_dict(self, _mono, _sleep, _rate):

        rows = [{
            "DISPLAY_FIRST_LAST": "LeBron James",
            "POSITION": "F",
            "HEIGHT": "6-9",
            "WEIGHT": "250",
            "COUNTRY": "USA",
            "BIRTHDATE": "1984-12-30T00:00:00",
            "DRAFT_YEAR": "2003",
            "DRAFT_ROUND": "1",
            "DRAFT_NUMBER": "1",
            "SCHOOL": "N/A",
            "TEAM_ID": 1610612747,
            "TEAM_ABBREVIATION": "LAL",
            "JERSEY": "23",
            "FROM_YEAR": "2003",
            "TO_YEAR": "2025",
        }]
        mock_ep = MagicMock()
        mock_ep.get_normalized_dict.return_value = {"CommonPlayerInfo": rows}

        with patch("nba_api.stats.endpoints.commonplayerinfo.CommonPlayerInfo",
                   return_value=mock_ep):
            from data.nba_stats_service import get_player_info
            result = get_player_info(2544)

        self.assertEqual(result["id"], 2544)
        self.assertEqual(result["full_name"], "LeBron James")
        self.assertEqual(result["position"], "F")
        self.assertEqual(result["height"], "6-9")
        self.assertEqual(result["country"], "USA")
        self.assertEqual(result["jersey"], "23")

    @patch("data.nba_stats_service._check_rate_limit", return_value=True)
    @patch("data.nba_stats_service._NBA_API_AVAILABLE", True)
    def test_returns_empty_dict_on_failure(self, _rate):
        with patch("nba_api.stats.endpoints.commonplayerinfo.CommonPlayerInfo",
                   side_effect=Exception("fail")):
            from data.nba_stats_service import get_player_info
            import data.nba_stats_service as svc
            svc._CACHE.clear()
            result = get_player_info(9999)
        self.assertEqual(result, {})

    @patch("data.nba_stats_service._NBA_API_AVAILABLE", False)
    def test_returns_empty_dict_when_unavailable(self):
        from data.nba_stats_service import get_player_info
        import data.nba_stats_service as svc
        svc._CACHE.clear()
        result = get_player_info(2544)
        self.assertEqual(result, {})


# ── Section 6: get_player_game_logs ──────────────────────────────────────────

@unittest.skipUnless(_HAS_NBA_API and _HAS_PANDAS, "nba_api or pandas not installed")
class TestGetPlayerGameLogs(unittest.TestCase):

    def setUp(self):
        import data.nba_stats_service as svc
        svc._CACHE.clear()

    @patch("data.nba_stats_service._check_rate_limit", return_value=True)
    @patch("data.nba_stats_service._NBA_API_AVAILABLE", True)
    @patch("data.nba_stats_service.time.sleep")
    @patch("data.nba_stats_service.time.monotonic", return_value=0.0)
    def test_returns_game_log_list(self, _mono, _sleep, _rate):

        import pandas as pd
        mock_df = pd.DataFrame([
            {"GAME_DATE": "2024-11-01", "PTS": 30, "REB": 8, "AST": 6, "MATCHUP": "LAL vs BOS", "WL": "W"},
            {"GAME_DATE": "2024-10-28", "PTS": 25, "REB": 7, "AST": 9, "MATCHUP": "LAL @ GSW", "WL": "L"},
        ])
        mock_ep = MagicMock()
        mock_ep.get_data_frames.return_value = [mock_df]

        with patch("nba_api.stats.endpoints.playergamelog.PlayerGameLog",
                   return_value=mock_ep):
            from data.nba_stats_service import get_player_game_logs
            result = get_player_game_logs(2544, season="2024-25")

        self.assertIsInstance(result, list)
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]["PTS"], 30)
        self.assertEqual(result[0]["GAME_DATE"], "2024-11-01")

    @patch("data.nba_stats_service._check_rate_limit", return_value=True)
    @patch("data.nba_stats_service._NBA_API_AVAILABLE", True)
    def test_returns_empty_on_failure(self, _rate):
        with patch("nba_api.stats.endpoints.playergamelog.PlayerGameLog",
                   side_effect=Exception("fail")):
            from data.nba_stats_service import get_player_game_logs
            import data.nba_stats_service as svc
            svc._CACHE.clear()
            result = get_player_game_logs(2544)
        self.assertEqual(result, [])


# ── Section 7: get_league_standings ──────────────────────────────────────────

@unittest.skipUnless(_HAS_NBA_API and _HAS_PANDAS, "nba_api or pandas not installed")
class TestGetLeagueStandings(unittest.TestCase):

    def setUp(self):
        import data.nba_stats_service as svc
        svc._CACHE.clear()

    @patch("data.nba_stats_service._check_rate_limit", return_value=True)
    @patch("data.nba_stats_service._NBA_API_AVAILABLE", True)
    @patch("data.nba_stats_service.time.sleep")
    @patch("data.nba_stats_service.time.monotonic", return_value=0.0)
    def test_returns_standings_list(self, _mono, _sleep, _rate):

        import pandas as pd
        mock_df = pd.DataFrame([{
            "TeamAbbreviation": "BOS",
            "TeamCity": "Boston",
            "TeamName": "Celtics",
            "Conference": "East",
            "PlayoffRank": 1,
            "WINS": 60,
            "LOSSES": 22,
            "WinPCT": 0.732,
            "strCurrentStreak": "W5",
            "L10": "8-2",
        }])
        mock_ep = MagicMock()
        mock_ep.get_data_frames.return_value = [mock_df]

        with patch("nba_api.stats.endpoints.leaguestandingsv3.LeagueStandingsV3",
                   return_value=mock_ep):
            from data.nba_stats_service import get_league_standings
            result = get_league_standings(season="2024-25")

        self.assertIsInstance(result, list)
        self.assertEqual(len(result), 1)
        s = result[0]
        self.assertEqual(s["team_abbreviation"], "BOS")
        self.assertEqual(s["wins"], 60)
        self.assertAlmostEqual(s["win_pct"], 0.732)
        self.assertEqual(s["streak"], "W5")

    @patch("data.nba_stats_service._check_rate_limit", return_value=True)
    @patch("data.nba_stats_service._NBA_API_AVAILABLE", True)
    def test_returns_empty_on_failure(self, _rate):
        with patch("nba_api.stats.endpoints.leaguestandingsv3.LeagueStandingsV3",
                   side_effect=Exception("fail")):
            from data.nba_stats_service import get_league_standings
            import data.nba_stats_service as svc
            svc._CACHE.clear()
            result = get_league_standings()
        self.assertEqual(result, [])


# ── Section 8: get_advanced_box_score ────────────────────────────────────────

@unittest.skipUnless(_HAS_NBA_API, "nba_api not installed")
class TestGetAdvancedBoxScore(unittest.TestCase):

    def setUp(self):
        import data.nba_stats_service as svc
        svc._CACHE.clear()

    @patch("data.nba_stats_service._check_rate_limit", return_value=True)
    @patch("data.nba_stats_service._NBA_API_AVAILABLE", True)
    @patch("data.nba_stats_service.time.sleep")
    @patch("data.nba_stats_service.time.monotonic", return_value=0.0)
    def test_returns_box_score_dict(self, _mono, _sleep, _rate):

        player_row = {"PLAYER_NAME": "LeBron James", "EFG_PCT": 0.55, "TS_PCT": 0.61, "USG_PCT": 0.32}
        team_row = {"TEAM_ABBREVIATION": "LAL", "PACE": 102.5}
        mock_ep = MagicMock()
        mock_ep.get_normalized_dict.return_value = {
            "PlayerStats": [player_row],
            "TeamStats": [team_row],
        }

        with patch("nba_api.stats.endpoints.boxscoreadvancedv3.BoxScoreAdvancedV3",
                   return_value=mock_ep):
            from data.nba_stats_service import get_advanced_box_score
            result = get_advanced_box_score("0022400001")

        self.assertIn("player_stats", result)
        self.assertIn("team_stats", result)
        self.assertEqual(len(result["player_stats"]), 1)
        self.assertAlmostEqual(result["player_stats"][0]["EFG_PCT"], 0.55)

    @patch("data.nba_stats_service._check_rate_limit", return_value=True)
    @patch("data.nba_stats_service._NBA_API_AVAILABLE", True)
    def test_returns_empty_on_failure(self, _rate):
        with patch("nba_api.stats.endpoints.boxscoreadvancedv3.BoxScoreAdvancedV3",
                   side_effect=Exception("fail")):
            from data.nba_stats_service import get_advanced_box_score
            import data.nba_stats_service as svc
            svc._CACHE.clear()
            result = get_advanced_box_score("bad_id")
        self.assertEqual(result, {})


# ── Section 9: get_team_roster ────────────────────────────────────────────────

@unittest.skipUnless(_HAS_NBA_API, "nba_api not installed")
class TestGetTeamRoster(unittest.TestCase):

    def setUp(self):
        import data.nba_stats_service as svc
        svc._CACHE.clear()

    @patch("data.nba_stats_service._check_rate_limit", return_value=True)
    @patch("data.nba_stats_service._NBA_API_AVAILABLE", True)
    @patch("data.nba_stats_service.time.sleep")
    @patch("data.nba_stats_service.time.monotonic", return_value=0.0)
    def test_returns_roster_list(self, _mono, _sleep, _rate):

        rows = [{"PLAYER_ID": 2544, "PLAYER": "LeBron James", "POSITION": "F",
                 "NUM": "23", "HEIGHT": "6-9", "WEIGHT": "250",
                 "BIRTH_DATE": "1984-12-30", "EXP": 22}]
        mock_ep = MagicMock()
        mock_ep.get_normalized_dict.return_value = {"CommonTeamRoster": rows}

        with patch("nba_api.stats.endpoints.commonteamroster.CommonTeamRoster",
                   return_value=mock_ep):
            from data.nba_stats_service import get_team_roster
            result = get_team_roster(1610612747, season="2024-25")

        self.assertIsInstance(result, list)
        self.assertEqual(len(result), 1)
        p = result[0]
        self.assertEqual(p["player_id"], 2544)
        self.assertEqual(p["player_name"], "LeBron James")
        self.assertEqual(p["jersey"], "23")

    @patch("data.nba_stats_service._check_rate_limit", return_value=True)
    @patch("data.nba_stats_service._NBA_API_AVAILABLE", True)
    def test_returns_empty_on_failure(self, _rate):
        with patch("nba_api.stats.endpoints.commonteamroster.CommonTeamRoster",
                   side_effect=Exception("fail")):
            from data.nba_stats_service import get_team_roster
            import data.nba_stats_service as svc
            svc._CACHE.clear()
            result = get_team_roster(9999)
        self.assertEqual(result, [])


# ── Section 10: get_defensive_matchup_data ────────────────────────────────────

@unittest.skipUnless(_HAS_NBA_API, "nba_api not installed")
class TestGetDefensiveMatchupData(unittest.TestCase):

    def setUp(self):
        import data.nba_stats_service as svc
        svc._CACHE.clear()

    @patch("data.nba_stats_service._check_rate_limit", return_value=True)
    @patch("data.nba_stats_service._NBA_API_AVAILABLE", True)
    @patch("data.nba_stats_service.time.sleep")
    @patch("data.nba_stats_service.time.monotonic", return_value=0.0)
    def test_returns_rows(self, _mono, _sleep, _rate):

        rows = [{"TEAM_ABBREVIATION": "BOS", "DEFENSE_CATEGORY": "Overall", "D_PTS": 95.2}]
        mock_ep = MagicMock()
        mock_ep.get_normalized_dict.return_value = {"LeagueDashPtDefend": rows}

        with patch("nba_api.stats.endpoints.leaguedashptdefend.LeagueDashPtDefend",
                   return_value=mock_ep):
            from data.nba_stats_service import get_defensive_matchup_data
            result = get_defensive_matchup_data(season="2024-25")

        self.assertIsInstance(result, list)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["TEAM_ABBREVIATION"], "BOS")

    @patch("data.nba_stats_service._check_rate_limit", return_value=True)
    @patch("data.nba_stats_service._NBA_API_AVAILABLE", True)
    def test_returns_empty_on_failure(self, _rate):
        with patch("nba_api.stats.endpoints.leaguedashptdefend.LeagueDashPtDefend",
                   side_effect=Exception("fail")):
            from data.nba_stats_service import get_defensive_matchup_data
            import data.nba_stats_service as svc
            svc._CACHE.clear()
            result = get_defensive_matchup_data()
        self.assertEqual(result, [])


# ── Section 11: player_intelligence integration ───────────────────────────────

class TestPlayerIntelligenceIntegration(unittest.TestCase):

    def test_get_player_game_logs_from_service_returns_list(self):
        from engine.player_intelligence import get_player_game_logs_from_service
        with patch("data.nba_data_service.get_player_game_log", return_value=[{"PTS": 25}]):
            result = get_player_game_logs_from_service(2544)
        self.assertIsInstance(result, list)

    def test_get_player_game_logs_from_service_returns_empty_on_error(self):
        from engine.player_intelligence import get_player_game_logs_from_service
        with patch("data.nba_data_service.get_player_game_log", side_effect=Exception("oops")):
            result = get_player_game_logs_from_service(2544)
        self.assertEqual(result, [])

    def test_get_player_matchup_grade_returns_na_when_no_data(self):
        from engine.player_intelligence import get_player_matchup_grade
        with patch("data.nba_data_service.get_defensive_ratings", return_value=[]):
            result = get_player_matchup_grade(2544, "BOS", "points")
        self.assertEqual(result["grade"], "N/A")

    def test_get_player_matchup_grade_returns_grade_with_data(self):
        from engine.player_intelligence import get_player_matchup_grade
        rows = [
            {"TEAM_ABBREVIATION": "BOS", "DEFENSE_CATEGORY": "Overall", "D_PTS": 95.0},
            {"TEAM_ABBREVIATION": "LAL", "DEFENSE_CATEGORY": "Overall", "D_PTS": 115.0},
            {"TEAM_ABBREVIATION": "PHX", "DEFENSE_CATEGORY": "Overall", "D_PTS": 105.0},
        ]
        with patch("data.nba_data_service.get_defensive_ratings", return_value=rows):
            result = get_player_matchup_grade(2544, "LAL", "points")
        self.assertIn(result["grade"], ("A", "B", "C", "D", "N/A"))
        self.assertIn("label", result)
        self.assertEqual(result["source"], "nba_stats_service")

    def test_get_player_home_away_splits_returns_zeros_when_no_data(self):
        from engine.player_intelligence import get_player_home_away_splits
        with patch("data.nba_data_service.get_player_shooting_splits", return_value={}):
            result = get_player_home_away_splits(2544)
        self.assertEqual(result["home_pts"], 0.0)
        self.assertEqual(result["away_pts"], 0.0)
        self.assertIn("last5_pts", result)
        self.assertIn("last10_pts", result)

    def test_get_player_home_away_splits_returns_data(self):
        from engine.player_intelligence import get_player_home_away_splits
        splits_data = {
            "home": [{"PTS": 28.5, "REB": 7.2, "AST": 8.1}],
            "away": [{"PTS": 24.0, "REB": 6.5, "AST": 7.5}],
            "last_5_games": [{"PTS": 30.0, "REB": 8.0, "AST": 9.0}],
            "last_10_games": [{"PTS": 27.0, "REB": 7.5, "AST": 8.3}],
        }
        with patch("data.nba_data_service.get_player_shooting_splits", return_value=splits_data):
            result = get_player_home_away_splits(2544)
        self.assertAlmostEqual(result["home_pts"], 28.5)
        self.assertAlmostEqual(result["away_pts"], 24.0)
        self.assertAlmostEqual(result["last5_pts"], 30.0)
        self.assertAlmostEqual(result["last10_pts"], 27.0)


# ── Section 12: nba_data_service integration ──────────────────────────────────

class TestNbaDataServiceIntegration(unittest.TestCase):

    def test_get_standings_from_nba_api_returns_list(self):
        from data.nba_data_service import get_standings_from_nba_api
        standings = [{"team_abbreviation": "BOS", "wins": 60}]
        with patch("data.etl_data_service.get_standings", return_value=standings):
            result = get_standings_from_nba_api()
        self.assertEqual(result, standings)

    def test_get_standings_from_nba_api_returns_empty_on_error(self):
        from data.nba_data_service import get_standings_from_nba_api
        with patch("data.etl_data_service.get_standings", side_effect=Exception("fail")):
            result = get_standings_from_nba_api()
        self.assertEqual(result, [])

    def test_get_game_logs_from_nba_api_resolves_player(self):
        from data.nba_data_service import get_game_logs_from_nba_api
        logs = [{"PTS": 30, "REB": 8}]
        player = {"player_id": 2544, "first_name": "LeBron", "last_name": "James"}
        with patch("data.etl_data_service.get_player_by_name", return_value=player):
            with patch("data.etl_data_service.get_player_game_logs", return_value=logs):
                result = get_game_logs_from_nba_api("LeBron James")
        self.assertEqual(result, logs)

    def test_get_game_logs_from_nba_api_returns_empty_when_no_id(self):
        from data.nba_data_service import get_game_logs_from_nba_api
        with patch("data.etl_data_service.get_player_by_name", return_value=None):
            result = get_game_logs_from_nba_api("Unknown Player")
        self.assertEqual(result, [])


# ── Section 13: simulation advanced stats enrichment ─────────────────────────

class TestSimulationAdvancedStats(unittest.TestCase):

    def test_enrich_returns_default_when_no_data(self):
        from engine.simulation import enrich_simulation_with_advanced_stats
        with patch("data.nba_data_service.get_box_score_advanced", return_value={}):
            result = enrich_simulation_with_advanced_stats("bad_id", "LeBron James")
        self.assertFalse(result["available"])
        self.assertEqual(result["pace_factor"], 1.0)

    def test_enrich_returns_stats_when_data_available(self):
        from engine.simulation import enrich_simulation_with_advanced_stats
        box = {
            "player_stats": [
                {"PLAYER_NAME": "LeBron James", "EFG_PCT": 0.55, "TS_PCT": 0.62, "USG_PCT": 0.31}
            ],
            "team_stats": [
                {"TEAM_ABBREVIATION": "LAL", "PACE": 102.0}
            ],
        }
        with patch("data.nba_data_service.get_box_score_advanced", return_value=box):
            result = enrich_simulation_with_advanced_stats("0022400001", "LeBron James")
        self.assertTrue(result["available"])
        self.assertAlmostEqual(result["efg_pct"], 0.55)
        self.assertAlmostEqual(result["ts_pct"], 0.62)
        self.assertAlmostEqual(result["usage_pct"], 0.31)
        self.assertGreater(result["pace_factor"], 0.0)

    def test_enrich_returns_default_when_service_unavailable(self):
        from engine.simulation import enrich_simulation_with_advanced_stats
        with patch("data.nba_data_service.get_box_score_advanced",
                   side_effect=Exception("service down")):
            result = enrich_simulation_with_advanced_stats("bad_id", "Any Player")
        self.assertFalse(result["available"])
        self.assertEqual(result["pace_factor"], 1.0)

    def test_enrich_player_not_in_box_score(self):
        from engine.simulation import enrich_simulation_with_advanced_stats
        box = {
            "player_stats": [
                {"PLAYER_NAME": "Jayson Tatum", "EFG_PCT": 0.52, "TS_PCT": 0.58, "USG_PCT": 0.30}
            ],
            "team_stats": [],
        }
        with patch("data.nba_data_service.get_box_score_advanced", return_value=box):
            result = enrich_simulation_with_advanced_stats("0022400001", "LeBron James")
        self.assertFalse(result["available"])


# ── Section 14: player_profile_service enrichment ────────────────────────────

class TestPlayerProfileServiceEnrichment(unittest.TestCase):

    def test_get_player_bio_returns_dict_with_keys(self):
        from data.player_profile_service import get_player_bio
        mock_info = {
            "position": "F",
            "height": "6-9",
            "weight": "250",
            "country": "USA",
            "birthdate": "1984-12-30T00:00:00",
            "draft_year": "2003",
            "draft_round": "1",
            "draft_number": "1",
            "school": "N/A",
            "jersey": "23",
        }
        with patch("data.player_profile_service.get_player_id", return_value=2544):
            with patch("data.nba_stats_service.get_player_info", return_value=mock_info):
                result = get_player_bio("LeBron James")
        self.assertEqual(result["position"], "F")
        self.assertEqual(result["height"], "6-9")
        self.assertEqual(result["country"], "USA")
        self.assertEqual(result["jersey"], "23")

    def test_get_player_bio_returns_empty_when_no_player_id(self):
        from data.player_profile_service import get_player_bio
        with patch("data.player_profile_service.get_player_id", return_value=None):
            result = get_player_bio("Unknown Player")
        self.assertEqual(result, {})

    def test_build_dynamic_player_lookup_returns_dict(self):
        from data.player_profile_service import _build_dynamic_player_lookup
        players = [{"id": 2544, "full_name": "LeBron James"}, {"id": 1629029, "full_name": "Luka Doncic"}]
        with patch("data.nba_stats_service.get_all_players", return_value=players):
            result = _build_dynamic_player_lookup()
        self.assertEqual(result.get("lebron james"), 2544)
        self.assertEqual(result.get("luka doncic"), 1629029)

    def test_build_dynamic_player_lookup_returns_empty_on_error(self):
        from data.player_profile_service import _build_dynamic_player_lookup
        with patch("data.nba_stats_service.get_all_players", side_effect=Exception("fail")):
            result = _build_dynamic_player_lookup()
        self.assertEqual(result, {})


# ── Section 15: find_games and play_by_play ───────────────────────────────────

@unittest.skipUnless(_HAS_NBA_API, "nba_api not installed")
class TestFindGamesAndPlayByPlay(unittest.TestCase):

    def setUp(self):
        import data.nba_stats_service as svc
        svc._CACHE.clear()

    @patch("data.nba_stats_service._check_rate_limit", return_value=True)
    @patch("data.nba_stats_service._NBA_API_AVAILABLE", True)
    @patch("data.nba_stats_service.time.sleep")
    @patch("data.nba_stats_service.time.monotonic", return_value=0.0)
    def test_find_games_returns_list(self, _mono, _sleep, _rate):

        rows = [{"GAME_ID": "0022400001", "TEAM_ABBREVIATION": "LAL"}]
        mock_ep = MagicMock()
        mock_ep.get_normalized_dict.return_value = {"LeagueGameFinderResults": rows}

        with patch("nba_api.stats.endpoints.leaguegamefinder.LeagueGameFinder",
                   return_value=mock_ep):
            from data.nba_stats_service import find_games
            result = find_games(season="2024-25")

        self.assertIsInstance(result, list)
        self.assertEqual(result[0]["GAME_ID"], "0022400001")

    @patch("data.nba_stats_service._check_rate_limit", return_value=True)
    @patch("data.nba_stats_service._NBA_API_AVAILABLE", True)
    @patch("data.nba_stats_service.time.sleep")
    @patch("data.nba_stats_service.time.monotonic", return_value=0.0)
    def test_get_play_by_play_returns_list(self, _mono, _sleep, _rate):

        rows = [{"EVENTNUM": 1, "HOMEDESCRIPTION": "Tatum 3PT"}]
        mock_ep = MagicMock()
        mock_ep.get_normalized_dict.return_value = {"PlayByPlay": rows}

        with patch("nba_api.stats.endpoints.playbyplayv3.PlayByPlayV3",
                   return_value=mock_ep):
            from data.nba_stats_service import get_play_by_play
            result = get_play_by_play("0022400001")

        self.assertIsInstance(result, list)
        self.assertEqual(len(result), 1)


# ── Section 16: get_recent_form_vs_line auto-fetch ────────────────────────────

class TestGetRecentFormAutoFetch(unittest.TestCase):
    """get_recent_form_vs_line auto-fetches logs when player_id is supplied."""

    def test_autofetch_when_no_logs_and_player_id_given(self):
        """When game_logs is empty and player_id is provided, logs are fetched."""
        from engine.player_intelligence import get_recent_form_vs_line
        fetched_logs = [
            {"PTS": 30, "REB": 7, "AST": 6, "GAME_DATE": "2024-11-01", "MATCHUP": "LAL vs BOS"},
            {"PTS": 28, "REB": 5, "AST": 8, "GAME_DATE": "2024-10-28", "MATCHUP": "LAL @ GSW"},
            {"PTS": 32, "REB": 9, "AST": 5, "GAME_DATE": "2024-10-25", "MATCHUP": "LAL vs PHX"},
        ]
        with patch("engine.player_intelligence.get_player_game_logs_from_service",
                   return_value=fetched_logs) as mock_fetch:
            result = get_recent_form_vs_line(
                [], "points", 25.0, player_id=2544
            )
        mock_fetch.assert_called_once_with(2544, season=None)
        self.assertEqual(result["hits"], 3)
        self.assertAlmostEqual(result["hit_rate"], 1.0)
        self.assertTrue(result["sufficient_data"])

    def test_autofetch_passes_season(self):
        """Season is forwarded when provided."""
        from engine.player_intelligence import get_recent_form_vs_line
        with patch("engine.player_intelligence.get_player_game_logs_from_service",
                   return_value=[]) as mock_fetch:
            get_recent_form_vs_line([], "points", 25.0, player_id=2544, season="2024-25")
        mock_fetch.assert_called_once_with(2544, season="2024-25")

    def test_no_autofetch_when_logs_provided(self):
        """Existing logs are used as-is — no service call made."""
        from engine.player_intelligence import get_recent_form_vs_line
        logs = [{"PTS": 25, "REB": 7, "GAME_DATE": "2024-11-01", "MATCHUP": "LAL vs BOS"}]
        with patch("engine.player_intelligence.get_player_game_logs_from_service") as mock_fetch:
            result = get_recent_form_vs_line(logs, "points", 20.0, player_id=2544)
        mock_fetch.assert_not_called()
        self.assertEqual(result["hits"], 1)

    def test_no_autofetch_when_no_player_id(self):
        """Without player_id, empty logs return the usual empty result."""
        from engine.player_intelligence import get_recent_form_vs_line
        with patch("engine.player_intelligence.get_player_game_logs_from_service") as mock_fetch:
            result = get_recent_form_vs_line([], "points", 25.0)
        mock_fetch.assert_not_called()
        self.assertEqual(result["form_label"], "No Data")

    def test_original_callers_unaffected(self):
        """Existing callers that pass game_logs positionally still work."""
        from engine.player_intelligence import get_recent_form_vs_line
        logs = [
            {"PTS": 20, "REB": 7, "GAME_DATE": "2024-11-01", "MATCHUP": "LAL vs BOS"},
            {"PTS": 18, "REB": 5, "GAME_DATE": "2024-10-28", "MATCHUP": "LAL @ GSW"},
            {"PTS": 22, "REB": 9, "GAME_DATE": "2024-10-25", "MATCHUP": "LAL vs PHX"},
        ]
        result = get_recent_form_vs_line(logs, "points", 25.0)
        self.assertEqual(result["hits"], 0)
        self.assertEqual(result["hit_rate"], 0.0)
        self.assertEqual(result["form_label"], "Cold 🧊")


# ── Section 17: dynamic NBA_API_ABBREV_TO_OURS ───────────────────────────────

class TestDynamicNbaApiAbbrevMap(unittest.TestCase):
    """NBA_API_ABBREV_TO_OURS is built dynamically from nba_api static teams."""

    def test_map_is_a_dict(self):
        from data.live_data_fetcher import NBA_API_ABBREV_TO_OURS
        self.assertIsInstance(NBA_API_ABBREV_TO_OURS, dict)

    def test_fallback_const_exists(self):
        from data.live_data_fetcher import _NBA_API_ABBREV_TO_OURS_FALLBACK
        self.assertIsInstance(_NBA_API_ABBREV_TO_OURS_FALLBACK, dict)
        self.assertIn("GS", _NBA_API_ABBREV_TO_OURS_FALLBACK)

    def test_known_aliases_present(self):
        """Short-form aliases used by some nba_api endpoints still resolve."""
        from data.live_data_fetcher import NBA_API_ABBREV_TO_OURS
        self.assertEqual(NBA_API_ABBREV_TO_OURS.get("GS"), "GSW")
        self.assertEqual(NBA_API_ABBREV_TO_OURS.get("NY"), "NYK")
        self.assertEqual(NBA_API_ABBREV_TO_OURS.get("NO"), "NOP")
        self.assertEqual(NBA_API_ABBREV_TO_OURS.get("SA"), "SAS")

    def test_standard_abbreviations_are_identity(self):
        """When nba_api is available, full standard abbreviations map to themselves."""
        from data.live_data_fetcher import NBA_API_ABBREV_TO_OURS
        for abbrev in ("LAL", "BOS", "GSW", "MIA", "PHX"):
            self.assertEqual(
                NBA_API_ABBREV_TO_OURS.get(abbrev, abbrev),
                abbrev,
                f"{abbrev} should map to itself",
            )

    @unittest.skipUnless(_HAS_NBA_API, "nba_api not installed")
    def test_build_nba_abbrev_map_falls_back_on_error(self):
        """When nba_api.stats.static.teams is unavailable, fallback is used."""
        from data.live_data_fetcher import _build_nba_abbrev_map, _NBA_API_ABBREV_TO_OURS_FALLBACK
        with patch("nba_api.stats.static.teams.get_teams", side_effect=Exception("fail")):
            result = _build_nba_abbrev_map()
        self.assertEqual(result, _NBA_API_ABBREV_TO_OURS_FALLBACK)

    @unittest.skipUnless(_HAS_NBA_API, "nba_api not installed")
    def test_build_nba_abbrev_map_includes_all_teams(self):
        """Dynamic build includes identity entries for all nba_api static teams."""
        mock_teams = [
            {"abbreviation": "LAL", "full_name": "Los Angeles Lakers"},
            {"abbreviation": "BOS", "full_name": "Boston Celtics"},
            {"abbreviation": "GSW", "full_name": "Golden State Warriors"},
        ]
        with patch("nba_api.stats.static.teams.get_teams", return_value=mock_teams):
            from data.live_data_fetcher import _build_nba_abbrev_map
            result = _build_nba_abbrev_map()
        self.assertEqual(result["LAL"], "LAL")
        self.assertEqual(result["BOS"], "BOS")
        self.assertEqual(result["GSW"], "GSW")
        # Hardcoded aliases must still be present
        self.assertEqual(result["GS"], "GSW")
        self.assertEqual(result["NY"], "NYK")


# ── Section 17: nba_stats_service game-ID validation guards ──────────────────

class TestNbaStatsServiceGameIdGuards(unittest.TestCase):
    """Verify that nba_stats_service functions reject non-numeric game IDs."""

    def test_is_nba_game_id_accepts_numeric(self):
        from data.nba_stats_service import _is_nba_game_id
        self.assertTrue(_is_nba_game_id("0022401234"))

    def test_is_nba_game_id_rejects_synthetic(self):
        from data.nba_stats_service import _is_nba_game_id
        self.assertFalse(_is_nba_game_id("DET_vs_NOP"))

    def test_is_nba_game_id_rejects_empty(self):
        from data.nba_stats_service import _is_nba_game_id
        self.assertFalse(_is_nba_game_id(""))

    def test_advanced_box_score_rejects_synthetic_id(self):
        import data.nba_stats_service as svc
        svc._CACHE.clear()
        with patch.object(svc, "_NBA_API_AVAILABLE", True):
            result = svc.get_advanced_box_score("DET_vs_NOP")
        self.assertEqual(result, {})

    def test_tracking_box_score_rejects_synthetic_id(self):
        import data.nba_stats_service as svc
        svc._CACHE.clear()
        with patch.object(svc, "_NBA_API_AVAILABLE", True):
            result = svc.get_player_tracking_box_score("DET_vs_NOP")
        self.assertEqual(result, {})

    def test_hustle_box_score_rejects_synthetic_id(self):
        import data.nba_stats_service as svc
        svc._CACHE.clear()
        with patch.object(svc, "_NBA_API_AVAILABLE", True):
            result = svc.get_hustle_box_score("DET_vs_NOP")
        self.assertEqual(result, {})

    def test_play_by_play_rejects_synthetic_id(self):
        import data.nba_stats_service as svc
        svc._CACHE.clear()
        with patch.object(svc, "_NBA_API_AVAILABLE", True):
            result = svc.get_play_by_play("DET_vs_NOP")
        self.assertEqual(result, [])


class TestSimulationEnrichGuard(unittest.TestCase):
    """enrich_simulation_with_advanced_stats should reject synthetic game IDs."""

    def test_rejects_synthetic_game_id(self):
        from engine.simulation import enrich_simulation_with_advanced_stats
        result = enrich_simulation_with_advanced_stats("DET_vs_NOP", "LeBron James")
        self.assertFalse(result["available"])
        self.assertEqual(result["pace_factor"], 1.0)

    def test_rejects_empty_game_id(self):
        from engine.simulation import enrich_simulation_with_advanced_stats
        result = enrich_simulation_with_advanced_stats("", "LeBron James")
        self.assertFalse(result["available"])


# ── Section 13: None-norm guard ───────────────────────────────────────────────

@unittest.skipUnless(_HAS_NBA_API, "nba_api not installed")
class TestNoneNormGuard(unittest.TestCase):
    """Verify that nba_stats_service functions handle get_normalized_dict() returning None."""

    def setUp(self):
        import data.nba_stats_service as svc
        svc._CACHE.clear()

    def _make_none_ep(self):
        mock_ep = MagicMock()
        mock_ep.get_normalized_dict.return_value = None
        return mock_ep

    @patch("data.nba_stats_service._check_rate_limit", return_value=True)
    @patch("data.nba_stats_service._NBA_API_AVAILABLE", True)
    @patch("data.nba_stats_service.time.sleep")
    @patch("data.nba_stats_service.time.monotonic", return_value=0.0)
    def test_advanced_box_score_none_norm(self, _mono, _sleep, _rate):
        with patch("nba_api.stats.endpoints.boxscoreadvancedv3.BoxScoreAdvancedV3",
                   return_value=self._make_none_ep()):
            from data.nba_stats_service import get_advanced_box_score
            import data.nba_stats_service as svc
            svc._CACHE.clear()
            result = get_advanced_box_score("0022501066")
        self.assertIsInstance(result, dict)
        self.assertEqual(result.get("player_stats"), [])
        self.assertEqual(result.get("team_stats"), [])

    @patch("data.nba_stats_service._check_rate_limit", return_value=True)
    @patch("data.nba_stats_service._NBA_API_AVAILABLE", True)
    @patch("data.nba_stats_service.time.sleep")
    @patch("data.nba_stats_service.time.monotonic", return_value=0.0)
    def test_player_tracking_box_score_none_norm(self, _mono, _sleep, _rate):
        with patch("nba_api.stats.endpoints.boxscoreplayertrackv3.BoxScorePlayerTrackV3",
                   return_value=self._make_none_ep()):
            from data.nba_stats_service import get_player_tracking_box_score
            import data.nba_stats_service as svc
            svc._CACHE.clear()
            result = get_player_tracking_box_score("0022501066")
        self.assertIsInstance(result, dict)
        self.assertEqual(result.get("player_stats"), [])

    @patch("data.nba_stats_service._check_rate_limit", return_value=True)
    @patch("data.nba_stats_service._NBA_API_AVAILABLE", True)
    @patch("data.nba_stats_service.time.sleep")
    @patch("data.nba_stats_service.time.monotonic", return_value=0.0)
    def test_hustle_box_score_none_norm(self, _mono, _sleep, _rate):
        with patch("nba_api.stats.endpoints.boxscorehustlev2.BoxScoreHustleV2",
                   return_value=self._make_none_ep()):
            from data.nba_stats_service import get_hustle_box_score
            import data.nba_stats_service as svc
            svc._CACHE.clear()
            result = get_hustle_box_score("0022501066")
        self.assertIsInstance(result, dict)
        self.assertEqual(result.get("player_stats"), [])


# ── Section 18: Diagnostic logging verification ───────────────────────────────

class TestNbaStatsServiceDiagnosticLogging(unittest.TestCase):
    """Verify diagnostic logging in nba_stats_service at every silent-return point."""

    def setUp(self):
        import data.nba_stats_service as svc
        svc._CACHE.clear()

    def _make_none_ep(self):
        mock_ep = MagicMock()
        mock_ep.get_normalized_dict.return_value = None
        return mock_ep

    def test_debug_logged_for_advanced_box_score_synthetic_id(self):
        import data.nba_stats_service as svc
        svc._CACHE.clear()
        with patch.object(svc, "_logger") as mock_log:
            svc.get_advanced_box_score("LAL_vs_BOS")
        mock_log.debug.assert_called_once()
        args = mock_log.debug.call_args[0]
        self.assertIn("rejected non-numeric", args[0])
        self.assertIn("LAL_vs_BOS", str(args))

    def test_debug_logged_for_hustle_box_score_synthetic_id(self):
        import data.nba_stats_service as svc
        svc._CACHE.clear()
        with patch.object(svc, "_logger") as mock_log:
            svc.get_hustle_box_score("DET_vs_NOP")
        mock_log.debug.assert_called_once()
        args = mock_log.debug.call_args[0]
        self.assertIn("rejected non-numeric", args[0])

    def test_debug_logged_for_play_by_play_synthetic_id(self):
        import data.nba_stats_service as svc
        svc._CACHE.clear()
        with patch.object(svc, "_logger") as mock_log:
            svc.get_play_by_play("GSW_vs_MIA")
        mock_log.debug.assert_called_once()
        args = mock_log.debug.call_args[0]
        self.assertIn("rejected non-numeric", args[0])

    @patch("data.nba_stats_service._check_rate_limit", return_value=False)
    @patch("data.nba_stats_service._NBA_API_AVAILABLE", True)
    def test_warning_logged_when_rate_limit_blocks_advanced(self, _rate):
        import data.nba_stats_service as svc
        svc._CACHE.clear()
        with patch.object(svc, "_logger") as mock_log:
            svc.get_advanced_box_score("0022501066")
        mock_log.warning.assert_called()
        args = mock_log.warning.call_args[0]
        self.assertIn("blocked", args[0])

    @patch("data.nba_stats_service._NBA_API_AVAILABLE", False)
    def test_warning_logged_when_nba_api_unavailable_hustle(self):
        import data.nba_stats_service as svc
        svc._CACHE.clear()
        with patch.object(svc, "_logger") as mock_log:
            svc.get_hustle_box_score("0022501066")
        mock_log.warning.assert_called()
        args = mock_log.warning.call_args[0]
        self.assertIn("blocked", args[0])

    @unittest.skipUnless(_HAS_NBA_API, "nba_api not installed")
    @patch("data.nba_stats_service._check_rate_limit", return_value=True)
    @patch("data.nba_stats_service._NBA_API_AVAILABLE", True)
    @patch("data.nba_stats_service.time.sleep")
    @patch("data.nba_stats_service.time.monotonic", return_value=0.0)
    def test_warning_logged_advanced_none_norm(self, _mono, _sleep, _rate):
        import data.nba_stats_service as svc
        svc._CACHE.clear()
        with patch("nba_api.stats.endpoints.boxscoreadvancedv3.BoxScoreAdvancedV3",
                   return_value=self._make_none_ep()):
            with patch.object(svc, "_logger") as mock_log:
                svc.get_advanced_box_score("0022501066")
        warning_calls = [str(c) for c in mock_log.warning.call_args_list]
        self.assertTrue(
            any("get_normalized_dict" in c for c in warning_calls),
            f"Expected get_normalized_dict warning, got: {warning_calls}",
        )

    @unittest.skipUnless(_HAS_NBA_API, "nba_api not installed")
    @patch("data.nba_stats_service._check_rate_limit", return_value=True)
    @patch("data.nba_stats_service._NBA_API_AVAILABLE", True)
    @patch("data.nba_stats_service.time.sleep")
    @patch("data.nba_stats_service.time.monotonic", return_value=0.0)
    def test_warning_logged_hustle_none_norm(self, _mono, _sleep, _rate):
        import data.nba_stats_service as svc
        svc._CACHE.clear()
        with patch("nba_api.stats.endpoints.boxscorehustlev2.BoxScoreHustleV2",
                   return_value=self._make_none_ep()):
            with patch.object(svc, "_logger") as mock_log:
                svc.get_hustle_box_score("0022501066")
        warning_calls = [str(c) for c in mock_log.warning.call_args_list]
        self.assertTrue(
            any("get_normalized_dict" in c for c in warning_calls),
            f"Expected get_normalized_dict warning, got: {warning_calls}",
        )


if __name__ == "__main__":
    unittest.main()
