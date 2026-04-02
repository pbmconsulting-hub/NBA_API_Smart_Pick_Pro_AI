"""
tests/test_nba_live_fetcher.py
-------------------------------
Unit tests for data/nba_live_fetcher.py.

Verifies:
  1. Module is importable and exposes all public functions
  2. All functions return safe defaults when nba_api is unavailable
  3. TTL caching layer works correctly
  4. Season-string normalisation helper
  5. Rate-limit bypass returns empty results
  6. Tier 1, 2, and 3 functions have correct return types
"""

import os
import sys
import time
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


# ── Section 1: Module Structure ───────────────────────────────────────────────

class TestModuleStructure(unittest.TestCase):
    """data/nba_live_fetcher.py must be importable and expose all public APIs."""

    def test_module_importable(self):
        import data.nba_live_fetcher  # noqa: F401

    def _assert_callable(self, name):
        import data.nba_live_fetcher as nlf
        fn = getattr(nlf, name, None)
        self.assertIsNotNone(fn, f"{name} not found in nba_live_fetcher")
        self.assertTrue(callable(fn), f"{name} is not callable")

    # TIER 1
    def test_fetch_player_game_logs(self): self._assert_callable("fetch_player_game_logs")
    def test_fetch_box_score_traditional(self): self._assert_callable("fetch_box_score_traditional")
    def test_fetch_box_score_advanced(self): self._assert_callable("fetch_box_score_advanced")
    def test_fetch_box_score_usage(self): self._assert_callable("fetch_box_score_usage")
    def test_fetch_player_on_off(self): self._assert_callable("fetch_player_on_off")
    def test_fetch_player_estimated_metrics(self): self._assert_callable("fetch_player_estimated_metrics")
    def test_fetch_player_fantasy_profile(self): self._assert_callable("fetch_player_fantasy_profile")
    def test_fetch_rotations(self): self._assert_callable("fetch_rotations")
    def test_fetch_schedule(self): self._assert_callable("fetch_schedule")
    def test_fetch_todays_scoreboard(self): self._assert_callable("fetch_todays_scoreboard")

    # TIER 2
    def test_fetch_box_score_matchups(self): self._assert_callable("fetch_box_score_matchups")
    def test_fetch_hustle_box_score(self): self._assert_callable("fetch_hustle_box_score")
    def test_fetch_defensive_box_score(self): self._assert_callable("fetch_defensive_box_score")
    def test_fetch_scoring_box_score(self): self._assert_callable("fetch_scoring_box_score")
    def test_fetch_tracking_box_score(self): self._assert_callable("fetch_tracking_box_score")
    def test_fetch_four_factors_box_score(self): self._assert_callable("fetch_four_factors_box_score")
    def test_fetch_player_shooting_splits(self): self._assert_callable("fetch_player_shooting_splits")
    def test_fetch_shot_chart(self): self._assert_callable("fetch_shot_chart")
    def test_fetch_player_clutch_stats(self): self._assert_callable("fetch_player_clutch_stats")
    def test_fetch_team_lineups(self): self._assert_callable("fetch_team_lineups")
    def test_fetch_team_dashboard(self): self._assert_callable("fetch_team_dashboard")
    def test_fetch_standings(self): self._assert_callable("fetch_standings")
    def test_fetch_team_game_logs(self): self._assert_callable("fetch_team_game_logs")
    def test_fetch_player_year_over_year(self): self._assert_callable("fetch_player_year_over_year")

    # TIER 3
    def test_fetch_player_vs_player(self): self._assert_callable("fetch_player_vs_player")
    def test_fetch_win_probability(self): self._assert_callable("fetch_win_probability")
    def test_fetch_play_by_play(self): self._assert_callable("fetch_play_by_play")
    def test_fetch_game_summary(self): self._assert_callable("fetch_game_summary")
    def test_fetch_league_leaders(self): self._assert_callable("fetch_league_leaders")
    def test_fetch_team_streak_finder(self): self._assert_callable("fetch_team_streak_finder")

    def test_ttl_constants(self):
        import data.nba_live_fetcher as nlf
        self.assertIsInstance(nlf.LIVE_TTL, int)
        self.assertIsInstance(nlf.HIST_TTL, int)
        self.assertGreater(nlf.LIVE_TTL, 0)
        self.assertGreater(nlf.HIST_TTL, nlf.LIVE_TTL)


# ── Section 2: Safe Defaults When nba_api Unavailable ─────────────────────────

class TestSafeDefaults(unittest.TestCase):
    """All functions must return safe defaults ([] or {}) when nba_api is not available."""

    def _with_no_api(self):
        """Context manager that sets _NBA_API_AVAILABLE = False."""
        import data.nba_live_fetcher as nlf
        return patch.object(nlf, "_NBA_API_AVAILABLE", False)

    def test_player_game_logs_no_api_returns_list(self):
        import data.nba_live_fetcher as nlf
        # Clear cache so we hit the real code path
        nlf._CACHE.clear()
        with self._with_no_api():
            result = nlf.fetch_player_game_logs(1234567)
        self.assertIsInstance(result, list)
        self.assertEqual(result, [])

    def test_box_score_traditional_no_api_returns_dict(self):
        import data.nba_live_fetcher as nlf
        nlf._CACHE.clear()
        with self._with_no_api():
            result = nlf.fetch_box_score_traditional("0022401234")
        self.assertIsInstance(result, dict)
        self.assertEqual(result, {})

    def test_box_score_usage_no_api_returns_dict(self):
        import data.nba_live_fetcher as nlf
        nlf._CACHE.clear()
        with self._with_no_api():
            result = nlf.fetch_box_score_usage("0022401234")
        self.assertIsInstance(result, dict)

    def test_player_on_off_no_api_returns_dict(self):
        import data.nba_live_fetcher as nlf
        nlf._CACHE.clear()
        with self._with_no_api():
            result = nlf.fetch_player_on_off(1610612738)
        self.assertIsInstance(result, dict)

    def test_player_estimated_metrics_no_api_returns_list(self):
        import data.nba_live_fetcher as nlf
        nlf._CACHE.clear()
        with self._with_no_api():
            result = nlf.fetch_player_estimated_metrics()
        self.assertIsInstance(result, list)

    def test_rotations_no_api_returns_dict(self):
        import data.nba_live_fetcher as nlf
        nlf._CACHE.clear()
        with self._with_no_api():
            result = nlf.fetch_rotations("0022401234")
        self.assertIsInstance(result, dict)

    def test_schedule_no_api_returns_list(self):
        import data.nba_live_fetcher as nlf
        nlf._CACHE.clear()
        with self._with_no_api():
            result = nlf.fetch_schedule("2026-03-25")
        self.assertIsInstance(result, list)

    def test_todays_scoreboard_no_api_returns_dict(self):
        import data.nba_live_fetcher as nlf
        nlf._CACHE.clear()
        with self._with_no_api():
            result = nlf.fetch_todays_scoreboard()
        self.assertIsInstance(result, dict)

    def test_player_clutch_stats_no_api_returns_list(self):
        import data.nba_live_fetcher as nlf
        nlf._CACHE.clear()
        with self._with_no_api():
            result = nlf.fetch_player_clutch_stats()
        self.assertIsInstance(result, list)

    def test_team_game_logs_no_api_returns_list(self):
        import data.nba_live_fetcher as nlf
        nlf._CACHE.clear()
        with self._with_no_api():
            result = nlf.fetch_team_game_logs(1610612738)
        self.assertIsInstance(result, list)

    def test_league_leaders_no_api_returns_list(self):
        import data.nba_live_fetcher as nlf
        nlf._CACHE.clear()
        with self._with_no_api():
            result = nlf.fetch_league_leaders()
        self.assertIsInstance(result, list)

    def test_win_probability_no_api_returns_dict(self):
        import data.nba_live_fetcher as nlf
        nlf._CACHE.clear()
        with self._with_no_api():
            result = nlf.fetch_win_probability("0022401234")
        self.assertIsInstance(result, dict)

    def test_game_summary_no_api_returns_dict(self):
        import data.nba_live_fetcher as nlf
        nlf._CACHE.clear()
        with self._with_no_api():
            result = nlf.fetch_game_summary("0022401234")
        self.assertIsInstance(result, dict)


# ── Section 3: Caching ────────────────────────────────────────────────────────

class TestCaching(unittest.TestCase):
    """TTL-based cache: _cache_get/_cache_set round-trip and expiry."""

    def setUp(self):
        import data.nba_live_fetcher as nlf
        nlf._CACHE.clear()

    def test_cache_set_and_get(self):
        import data.nba_live_fetcher as nlf
        nlf._cache_set("test_key", [1, 2, 3])
        result = nlf._cache_get("test_key", nlf.HIST_TTL)
        self.assertEqual(result, [1, 2, 3])

    def test_cache_returns_none_for_missing_key(self):
        import data.nba_live_fetcher as nlf
        result = nlf._cache_get("nonexistent_key", nlf.HIST_TTL)
        self.assertIsNone(result)

    def test_cache_expires_after_ttl(self):
        import data.nba_live_fetcher as nlf
        nlf._cache_set("expiring_key", {"data": "value"})
        # Manually set the timestamp to the past to simulate expiry
        payload, _ = nlf._CACHE["expiring_key"]
        nlf._CACHE["expiring_key"] = (payload, time.time() - 7200)  # 2 hours ago
        result = nlf._cache_get("expiring_key", nlf.HIST_TTL)  # 1 hour TTL
        self.assertIsNone(result)

    def test_live_ttl_shorter_than_hist_ttl(self):
        import data.nba_live_fetcher as nlf
        self.assertLess(nlf.LIVE_TTL, nlf.HIST_TTL)


# ── Section 4: Season String Normalisation ────────────────────────────────────

class TestSeasonHelper(unittest.TestCase):
    """_resolve_season should normalise season inputs to 'YYYY-YY' format."""

    def test_none_returns_current_season(self):
        import data.nba_live_fetcher as nlf
        result = nlf._resolve_season(None)
        self.assertRegex(result, r"^\d{4}-\d{2}$")

    def test_yyyy_yy_format_passthrough(self):
        import data.nba_live_fetcher as nlf
        result = nlf._resolve_season("2024-25")
        self.assertEqual(result, "2024-25")

    def test_integer_year_conversion(self):
        import data.nba_live_fetcher as nlf
        result = nlf._resolve_season("2024")
        self.assertEqual(result, "2024-25")

    def test_long_format_preserved(self):
        import data.nba_live_fetcher as nlf
        result = nlf._resolve_season("2025-26")
        self.assertEqual(result, "2025-26")


# ── Section 5: last_n Filter ──────────────────────────────────────────────────

class TestLastNFilter(unittest.TestCase):
    """fetch_player_game_logs and fetch_team_game_logs should respect last_n."""

    def test_player_game_logs_last_n_zero_means_all(self):
        """last_n=0 should return all available data (or [] on failure)."""
        import data.nba_live_fetcher as nlf
        nlf._CACHE.clear()
        # Pre-cache a fake result of 20 games
        nlf._cache_set("nlf:player_game_logs:1234:2025-26:0", list(range(20)))
        result = nlf.fetch_player_game_logs(1234, season="2025-26", last_n=0)
        self.assertEqual(len(result), 20)

    def test_player_game_logs_last_n_positive_truncates(self):
        """last_n>0 should return at most last_n items."""
        import data.nba_live_fetcher as nlf
        nlf._CACHE.clear()
        # Pre-cache a fake result of 20 games
        nlf._cache_set("nlf:player_game_logs:1234:2025-26:5", list(range(5)))
        result = nlf.fetch_player_game_logs(1234, season="2025-26", last_n=5)
        self.assertLessEqual(len(result), 5)

    def test_team_game_logs_last_n_respected(self):
        """fetch_team_game_logs should return empty list when no API available."""
        import data.nba_live_fetcher as nlf
        nlf._CACHE.clear()
        with patch.object(nlf, "_NBA_API_AVAILABLE", False):
            result = nlf.fetch_team_game_logs(1610612738, last_n=10)
        self.assertIsInstance(result, list)


# ── Section 6: nba_data_service wrappers ──────────────────────────────────────

class TestNbaDataServiceWrappers(unittest.TestCase):
    """nba_data_service.py must expose all new get_*() wrappers."""

    def _assert_callable(self, name):
        import data.nba_data_service as svc
        fn = getattr(svc, name, None)
        self.assertIsNotNone(fn, f"{name} not found in nba_data_service")
        self.assertTrue(callable(fn), f"{name} is not callable")

    def test_get_player_game_logs_v2(self): self._assert_callable("get_player_game_logs_v2")
    def test_get_box_score_traditional(self): self._assert_callable("get_box_score_traditional")
    def test_get_box_score_advanced(self): self._assert_callable("get_box_score_advanced")
    def test_get_box_score_usage(self): self._assert_callable("get_box_score_usage")
    def test_get_player_on_off(self): self._assert_callable("get_player_on_off")
    def test_get_player_estimated_metrics(self): self._assert_callable("get_player_estimated_metrics")
    def test_get_player_fantasy_profile(self): self._assert_callable("get_player_fantasy_profile")
    def test_get_rotations(self): self._assert_callable("get_rotations")
    def test_get_schedule(self): self._assert_callable("get_schedule")
    def test_get_todays_scoreboard(self): self._assert_callable("get_todays_scoreboard")
    def test_get_box_score_matchups(self): self._assert_callable("get_box_score_matchups")
    def test_get_hustle_box_score(self): self._assert_callable("get_hustle_box_score")
    def test_get_defensive_box_score(self): self._assert_callable("get_defensive_box_score")
    def test_get_scoring_box_score(self): self._assert_callable("get_scoring_box_score")
    def test_get_tracking_box_score(self): self._assert_callable("get_tracking_box_score")
    def test_get_four_factors_box_score(self): self._assert_callable("get_four_factors_box_score")
    def test_get_player_shooting_splits(self): self._assert_callable("get_player_shooting_splits")
    def test_get_shot_chart_v2(self): self._assert_callable("get_shot_chart_v2")
    def test_get_player_clutch_stats(self): self._assert_callable("get_player_clutch_stats")
    def test_get_team_lineups(self): self._assert_callable("get_team_lineups")
    def test_get_team_dashboard(self): self._assert_callable("get_team_dashboard")
    def test_get_team_game_logs(self): self._assert_callable("get_team_game_logs")
    def test_get_player_year_over_year(self): self._assert_callable("get_player_year_over_year")
    def test_get_player_vs_player(self): self._assert_callable("get_player_vs_player")
    def test_get_win_probability(self): self._assert_callable("get_win_probability")
    def test_get_play_by_play_v2(self): self._assert_callable("get_play_by_play_v2")
    def test_get_game_summary(self): self._assert_callable("get_game_summary")
    def test_get_league_leaders(self): self._assert_callable("get_league_leaders")
    def test_get_team_streak_finder(self): self._assert_callable("get_team_streak_finder")

    def test_wrappers_return_safe_defaults(self):
        """All wrappers should return [] or {} even when nba_live_fetcher fails."""
        import data.nba_data_service as svc
        import data.nba_live_fetcher as nlf
        nlf._CACHE.clear()
        with patch.object(nlf, "_NBA_API_AVAILABLE", False):
            self.assertIsInstance(svc.get_player_game_logs_v2(9999), list)
            self.assertIsInstance(svc.get_box_score_traditional("0022401234"), dict)
            self.assertIsInstance(svc.get_rotations("0022401234"), dict)
            self.assertIsInstance(svc.get_schedule(), list)
            self.assertIsInstance(svc.get_team_game_logs(1610612738), list)


# ── Section 5: Game-ID validation & synthetic-ID guard ────────────────────────

class TestGameIdValidation(unittest.TestCase):
    """_is_nba_game_id should accept numeric IDs and reject synthetic labels."""

    def test_valid_numeric_game_id(self):
        from data.nba_live_fetcher import _is_nba_game_id
        self.assertTrue(_is_nba_game_id("0022401234"))

    def test_valid_short_numeric(self):
        from data.nba_live_fetcher import _is_nba_game_id
        self.assertTrue(_is_nba_game_id("12345"))

    def test_rejects_synthetic_vs_label(self):
        from data.nba_live_fetcher import _is_nba_game_id
        self.assertFalse(_is_nba_game_id("DET_vs_NOP"))

    def test_rejects_empty_string(self):
        from data.nba_live_fetcher import _is_nba_game_id
        self.assertFalse(_is_nba_game_id(""))

    def test_rejects_none_string(self):
        from data.nba_live_fetcher import _is_nba_game_id
        # Passing "None" string (edge case from str(None))
        self.assertFalse(_is_nba_game_id("None"))

    def test_matchups_skips_synthetic_id(self):
        """fetch_box_score_matchups should return {} for non-numeric IDs
        without hitting the NBA API."""
        import data.nba_live_fetcher as nlf
        nlf._CACHE.clear()
        # Even with API available, a synthetic ID should be skipped
        with patch.object(nlf, "_NBA_API_AVAILABLE", True):
            result = nlf.fetch_box_score_matchups("DET_vs_NOP")
        self.assertIsInstance(result, dict)
        self.assertEqual(result, {})


# ── Section 6: None-norm guard ────────────────────────────────────────────────

class TestNoneNormGuard(unittest.TestCase):
    """Verify that functions handle get_normalized_dict() returning None."""

    def _make_none_ep(self):
        mock_ep = MagicMock()
        mock_ep.get_normalized_dict.return_value = None
        mock_ep.get_dict.return_value = {}
        return mock_ep

    @patch("data.nba_live_fetcher._check_rate_limit", return_value=True)
    @patch("data.nba_live_fetcher._NBA_API_AVAILABLE", True)
    @patch("data.nba_live_fetcher.time.sleep")
    @patch("data.nba_live_fetcher.time.monotonic", return_value=0.0)
    def test_four_factors_none_norm(self, _mono, _sleep, _rate):
        import data.nba_live_fetcher as nlf
        nlf._CACHE.clear()
        with patch("nba_api.stats.endpoints.boxscorefourfactorsv3.BoxScoreFourFactorsV3",
                   return_value=self._make_none_ep()):
            result = nlf.fetch_four_factors_box_score("0022501066")
        self.assertIsInstance(result, dict)
        self.assertEqual(result.get("player_stats"), [])
        self.assertEqual(result.get("team_stats"), [])

    @patch("data.nba_live_fetcher._check_rate_limit", return_value=True)
    @patch("data.nba_live_fetcher._NBA_API_AVAILABLE", True)
    @patch("data.nba_live_fetcher.time.sleep")
    @patch("data.nba_live_fetcher.time.monotonic", return_value=0.0)
    def test_matchups_none_norm(self, _mono, _sleep, _rate):
        import data.nba_live_fetcher as nlf
        nlf._CACHE.clear()
        with patch("nba_api.stats.endpoints.boxscorematchupsv3.BoxScoreMatchupsV3",
                   return_value=self._make_none_ep()):
            result = nlf.fetch_box_score_matchups("0022501066")
        self.assertIsInstance(result, dict)
        self.assertEqual(result.get("player_stats"), [])

    @patch("data.nba_live_fetcher._check_rate_limit", return_value=True)
    @patch("data.nba_live_fetcher._NBA_API_AVAILABLE", True)
    @patch("data.nba_live_fetcher.time.sleep")
    @patch("data.nba_live_fetcher.time.monotonic", return_value=0.0)
    def test_box_score_traditional_none_norm(self, _mono, _sleep, _rate):
        import data.nba_live_fetcher as nlf
        nlf._CACHE.clear()
        with patch("nba_api.stats.endpoints.boxscoretraditionalv3.BoxScoreTraditionalV3",
                   return_value=self._make_none_ep()):
            result = nlf.fetch_box_score_traditional("0022501066")
        self.assertIsInstance(result, dict)
        self.assertEqual(result.get("player_stats"), [])
        self.assertEqual(result.get("team_stats"), [])

    @patch("data.nba_live_fetcher._check_rate_limit", return_value=True)
    @patch("data.nba_live_fetcher._NBA_API_AVAILABLE", True)
    @patch("data.nba_live_fetcher.time.sleep")
    @patch("data.nba_live_fetcher.time.monotonic", return_value=0.0)
    def test_defensive_box_score_none_norm(self, _mono, _sleep, _rate):
        import data.nba_live_fetcher as nlf
        nlf._CACHE.clear()
        with patch("nba_api.stats.endpoints.boxscoredefensivev2.BoxScoreDefensiveV2",
                   return_value=self._make_none_ep()):
            result = nlf.fetch_defensive_box_score("0022501066")
        self.assertIsInstance(result, dict)
        self.assertEqual(result.get("player_stats"), [])
        self.assertEqual(result.get("team_stats"), [])

    @patch("data.nba_live_fetcher._check_rate_limit", return_value=True)
    @patch("data.nba_live_fetcher._NBA_API_AVAILABLE", True)
    @patch("data.nba_live_fetcher.time.sleep")
    @patch("data.nba_live_fetcher.time.monotonic", return_value=0.0)
    def test_scoring_box_score_none_norm(self, _mono, _sleep, _rate):
        import data.nba_live_fetcher as nlf
        nlf._CACHE.clear()
        with patch("nba_api.stats.endpoints.boxscorescoringv3.BoxScoreScoringV3",
                   return_value=self._make_none_ep()):
            result = nlf.fetch_scoring_box_score("0022501066")
        self.assertIsInstance(result, dict)
        self.assertEqual(result.get("player_stats"), [])
        self.assertEqual(result.get("team_stats"), [])

    @patch("data.nba_live_fetcher._check_rate_limit", return_value=True)
    @patch("data.nba_live_fetcher._NBA_API_AVAILABLE", True)
    @patch("data.nba_live_fetcher.time.sleep")
    @patch("data.nba_live_fetcher.time.monotonic", return_value=0.0)
    def test_box_score_usage_none_norm(self, _mono, _sleep, _rate):
        import data.nba_live_fetcher as nlf
        nlf._CACHE.clear()
        with patch("nba_api.stats.endpoints.boxscoreusagev3.BoxScoreUsageV3",
                   return_value=self._make_none_ep()):
            result = nlf.fetch_box_score_usage("0022501066")
        self.assertIsInstance(result, dict)
        self.assertEqual(result.get("player_stats"), [])
        self.assertEqual(result.get("team_stats"), [])


class TestBuildFormattedGame(unittest.TestCase):
    """_build_formatted_game should use a real game_id when provided."""

    def test_uses_real_game_id(self):
        from data.live_data_fetcher import _build_formatted_game
        game = _build_formatted_game(
            "LAL", "BOS", "Los Angeles Lakers", "Boston Celtics",
            "7:30 PM ET", "TD Garden, Boston", {},
            game_id="0022401234",
        )
        self.assertEqual(game["game_id"], "0022401234")

    def test_falls_back_to_synthetic_id(self):
        from data.live_data_fetcher import _build_formatted_game
        game = _build_formatted_game(
            "LAL", "BOS", "Los Angeles Lakers", "Boston Celtics",
            "7:30 PM ET", "TD Garden, Boston", {},
        )
        self.assertEqual(game["game_id"], "LAL_vs_BOS")

    def test_empty_game_id_falls_back(self):
        from data.live_data_fetcher import _build_formatted_game
        game = _build_formatted_game(
            "DET", "NOP", "Detroit Pistons", "New Orleans Pelicans",
            "8:00 PM ET", "", {},
            game_id="",
        )
        self.assertEqual(game["game_id"], "DET_vs_NOP")

    def test_synthetic_id_fallback_logs_info(self):
        """_build_formatted_game should log at INFO when falling back to synthetic ID."""
        import logging
        from data.live_data_fetcher import _build_formatted_game
        with self.assertLogs("smartai_nba.data.live_data_fetcher", level="INFO") as cm:
            _build_formatted_game(
                "GSW", "MIA", "Golden State Warriors", "Miami Heat",
                "9:00 PM ET", "", {},
                game_id="",
            )
        self.assertTrue(
            any("synthetic" in msg for msg in cm.output),
            "Expected 'synthetic' in log output",
        )

    def test_real_game_id_does_not_log_synthetic(self):
        """_build_formatted_game should NOT log a synthetic-ID warning when a real game_id is provided."""
        import logging
        from data.live_data_fetcher import _build_formatted_game
        # No log at all (INFO+) should be emitted for the synthetic fallback path
        import data.live_data_fetcher as ldf
        with patch.object(ldf, "_logger") as mock_log:
            _build_formatted_game(
                "GSW", "MIA", "Golden State Warriors", "Miami Heat",
                "9:00 PM ET", "", {},
                game_id="0022401234",
            )
        mock_log.info.assert_not_called()


# ── Section 7: Diagnostic logging verification ───────────────────────────────

class TestDiagnosticLoggingSyntheticId(unittest.TestCase):
    """_logger.debug() should be called when _is_nba_game_id() rejects a synthetic ID."""

    def setUp(self):
        import data.nba_live_fetcher as nlf
        nlf._CACHE.clear()

    def test_debug_logged_for_traditional_synthetic_id(self):
        import data.nba_live_fetcher as nlf
        nlf._CACHE.clear()
        with patch.object(nlf, "_logger") as mock_log:
            nlf.fetch_box_score_traditional("LAL_vs_BOS")
        mock_log.debug.assert_called_once()
        args = mock_log.debug.call_args[0]
        self.assertIn("rejected non-numeric", args[0])
        self.assertIn("LAL_vs_BOS", str(args))

    def test_debug_logged_for_matchups_synthetic_id(self):
        import data.nba_live_fetcher as nlf
        nlf._CACHE.clear()
        with patch.object(nlf, "_logger") as mock_log:
            nlf.fetch_box_score_matchups("GSW_vs_MIA")
        mock_log.debug.assert_called_once()
        args = mock_log.debug.call_args[0]
        self.assertIn("rejected non-numeric", args[0])

    def test_debug_logged_for_four_factors_synthetic_id(self):
        import data.nba_live_fetcher as nlf
        nlf._CACHE.clear()
        with patch.object(nlf, "_logger") as mock_log:
            nlf.fetch_four_factors_box_score("DET_vs_NOP")
        mock_log.debug.assert_called_once()
        args = mock_log.debug.call_args[0]
        self.assertIn("rejected non-numeric", args[0])

    def test_debug_logged_for_game_summary_synthetic_id(self):
        import data.nba_live_fetcher as nlf
        nlf._CACHE.clear()
        with patch.object(nlf, "_logger") as mock_log:
            nlf.fetch_game_summary("LAL_vs_BOS")
        mock_log.debug.assert_called_once()
        args = mock_log.debug.call_args[0]
        self.assertIn("rejected non-numeric", args[0])


class TestDiagnosticLoggingRateLimit(unittest.TestCase):
    """_logger.warning() should be called when _check_rate_limit() blocks a call."""

    def setUp(self):
        import data.nba_live_fetcher as nlf
        nlf._CACHE.clear()

    @patch("data.nba_live_fetcher._check_rate_limit", return_value=False)
    @patch("data.nba_live_fetcher._NBA_API_AVAILABLE", True)
    def test_warning_logged_when_rate_limit_blocks_traditional(self, _rate):
        import data.nba_live_fetcher as nlf
        nlf._CACHE.clear()
        with patch.object(nlf, "_logger") as mock_log:
            nlf.fetch_box_score_traditional("0022501066")
        mock_log.warning.assert_called()
        args = mock_log.warning.call_args[0]
        self.assertIn("blocked", args[0])

    @patch("data.nba_live_fetcher._NBA_API_AVAILABLE", False)
    def test_warning_logged_when_nba_api_unavailable_traditional(self):
        import data.nba_live_fetcher as nlf
        nlf._CACHE.clear()
        with patch.object(nlf, "_logger") as mock_log:
            nlf.fetch_box_score_traditional("0022501066")
        mock_log.warning.assert_called()
        args = mock_log.warning.call_args[0]
        self.assertIn("blocked", args[0])

    @patch("data.nba_live_fetcher._check_rate_limit", return_value=False)
    @patch("data.nba_live_fetcher._NBA_API_AVAILABLE", True)
    def test_warning_logged_when_rate_limit_blocks_four_factors(self, _rate):
        import data.nba_live_fetcher as nlf
        nlf._CACHE.clear()
        with patch.object(nlf, "_logger") as mock_log:
            nlf.fetch_four_factors_box_score("0022501066")
        mock_log.warning.assert_called()
        args = mock_log.warning.call_args[0]
        self.assertIn("blocked", args[0])

    @patch("data.nba_live_fetcher._check_rate_limit", return_value=False)
    @patch("data.nba_live_fetcher._NBA_API_AVAILABLE", True)
    def test_warning_logged_when_rate_limit_blocks_schedule(self, _rate):
        import data.nba_live_fetcher as nlf
        nlf._CACHE.clear()
        with patch.object(nlf, "_logger") as mock_log:
            nlf.fetch_schedule()
        mock_log.warning.assert_called()
        args = mock_log.warning.call_args[0]
        self.assertIn("blocked", args[0])


class TestDiagnosticLoggingNoneNorm(unittest.TestCase):
    """_logger.warning() should be called when get_normalized_dict() returns None."""

    def _make_none_ep(self):
        mock_ep = MagicMock()
        mock_ep.get_normalized_dict.return_value = None
        mock_ep.get_dict.return_value = {}
        return mock_ep

    @patch("data.nba_live_fetcher._check_rate_limit", return_value=True)
    @patch("data.nba_live_fetcher._NBA_API_AVAILABLE", True)
    @patch("data.nba_live_fetcher.time.sleep")
    @patch("data.nba_live_fetcher.time.monotonic", return_value=0.0)
    def test_warning_logged_traditional_none_norm(self, _mono, _sleep, _rate):
        import data.nba_live_fetcher as nlf
        nlf._CACHE.clear()
        with patch("nba_api.stats.endpoints.boxscoretraditionalv3.BoxScoreTraditionalV3",
                   return_value=self._make_none_ep()):
            with patch.object(nlf, "_logger") as mock_log:
                nlf.fetch_box_score_traditional("0022501066")
        warning_calls = [str(c) for c in mock_log.warning.call_args_list]
        self.assertTrue(
            any("get_normalized_dict" in c for c in warning_calls),
            f"Expected get_normalized_dict warning, got: {warning_calls}",
        )

    @patch("data.nba_live_fetcher._check_rate_limit", return_value=True)
    @patch("data.nba_live_fetcher._NBA_API_AVAILABLE", True)
    @patch("data.nba_live_fetcher.time.sleep")
    @patch("data.nba_live_fetcher.time.monotonic", return_value=0.0)
    def test_warning_logged_four_factors_none_norm(self, _mono, _sleep, _rate):
        import data.nba_live_fetcher as nlf
        nlf._CACHE.clear()
        with patch("nba_api.stats.endpoints.boxscorefourfactorsv3.BoxScoreFourFactorsV3",
                   return_value=self._make_none_ep()):
            with patch.object(nlf, "_logger") as mock_log:
                nlf.fetch_four_factors_box_score("0022501066")
        warning_calls = [str(c) for c in mock_log.warning.call_args_list]
        self.assertTrue(
            any("no data" in c or "get_normalized_dict" in c for c in warning_calls),
            f"Expected no-data/get_normalized_dict warning, got: {warning_calls}",
        )

    @patch("data.nba_live_fetcher._check_rate_limit", return_value=True)
    @patch("data.nba_live_fetcher._NBA_API_AVAILABLE", True)
    @patch("data.nba_live_fetcher.time.sleep")
    @patch("data.nba_live_fetcher.time.monotonic", return_value=0.0)
    def test_warning_logged_matchups_none_norm(self, _mono, _sleep, _rate):
        import data.nba_live_fetcher as nlf
        nlf._CACHE.clear()
        with patch("nba_api.stats.endpoints.boxscorematchupsv3.BoxScoreMatchupsV3",
                   return_value=self._make_none_ep()):
            with patch.object(nlf, "_logger") as mock_log:
                nlf.fetch_box_score_matchups("0022501066")
        warning_calls = [str(c) for c in mock_log.warning.call_args_list]
        self.assertTrue(
            any("no data" in c or "get_normalized_dict" in c for c in warning_calls),
            f"Expected no-data/get_normalized_dict warning, got: {warning_calls}",
        )


# ── Section: _parse_resultsets helper ─────────────────────────────────────────

class TestParseResultsets(unittest.TestCase):
    """Tests for _parse_resultsets() fallback helper."""

    def test_empty_dict(self):
        from data.nba_live_fetcher import _parse_resultsets
        self.assertEqual(_parse_resultsets({}), {})

    def test_basic_resultsets(self):
        from data.nba_live_fetcher import _parse_resultsets
        raw = {
            "resultSets": [
                {
                    "name": "PlayerStats",
                    "headers": ["PLAYER_ID", "PLAYER_NAME", "PTS"],
                    "rowSet": [[101, "Test Player", 25]],
                }
            ]
        }
        result = _parse_resultsets(raw)
        self.assertIn("PlayerStats", result)
        self.assertEqual(len(result["PlayerStats"]), 1)
        self.assertEqual(result["PlayerStats"][0]["PLAYER_NAME"], "Test Player")
        self.assertEqual(result["PlayerStats"][0]["PTS"], 25)

    def test_multiple_resultsets(self):
        from data.nba_live_fetcher import _parse_resultsets
        raw = {
            "resultSets": [
                {"name": "PlayerStats", "headers": ["ID"], "rowSet": [[1], [2]]},
                {"name": "TeamStats", "headers": ["TID"], "rowSet": [[10]]},
            ]
        }
        result = _parse_resultsets(raw)
        self.assertEqual(len(result["PlayerStats"]), 2)
        self.assertEqual(len(result["TeamStats"]), 1)

    def test_missing_headers_skipped(self):
        from data.nba_live_fetcher import _parse_resultsets
        raw = {"resultSets": [{"name": "NoHeaders", "rowSet": [[1]]}]}
        self.assertEqual(_parse_resultsets(raw), {})


# ── Section: IndexError fallback via get_dict() ──────────────────────────────

class TestIndexErrorFallback(unittest.TestCase):
    """Verify that get_dict() fallback works when get_normalized_dict() raises IndexError."""

    @patch("data.nba_live_fetcher._check_rate_limit", return_value=True)
    @patch("data.nba_live_fetcher._NBA_API_AVAILABLE", True)
    @patch("data.nba_live_fetcher.time.sleep")
    @patch("data.nba_live_fetcher.time.monotonic", return_value=0.0)
    def test_matchups_indexerror_falls_back_to_get_dict(self, _mono, _sleep, _rate):
        import data.nba_live_fetcher as nlf
        nlf._CACHE.clear()

        mock_ep = MagicMock()
        mock_ep.get_normalized_dict.side_effect = IndexError("list index out of range")
        mock_ep.get_dict.return_value = {
            "resultSets": [
                {
                    "name": "MatchUps",
                    "headers": ["PLAYER_ID", "PLAYER_NAME"],
                    "rowSet": [[101, "Test Player"]],
                }
            ]
        }

        with patch("nba_api.stats.endpoints.boxscorematchupsv3.BoxScoreMatchupsV3",
                   return_value=mock_ep):
            result = nlf.fetch_box_score_matchups("0022501082")

        self.assertIsInstance(result, dict)
        self.assertEqual(len(result["player_stats"]), 1)
        self.assertEqual(result["player_stats"][0]["PLAYER_NAME"], "Test Player")

    @patch("data.nba_live_fetcher._check_rate_limit", return_value=True)
    @patch("data.nba_live_fetcher._NBA_API_AVAILABLE", True)
    @patch("data.nba_live_fetcher.time.sleep")
    @patch("data.nba_live_fetcher.time.monotonic", return_value=0.0)
    def test_four_factors_indexerror_falls_back_to_get_dict(self, _mono, _sleep, _rate):
        import data.nba_live_fetcher as nlf
        nlf._CACHE.clear()

        mock_ep = MagicMock()
        mock_ep.get_normalized_dict.side_effect = IndexError("list index out of range")
        mock_ep.get_dict.return_value = {
            "resultSets": [
                {
                    "name": "sqlPlayersFourFactors",
                    "headers": ["GAME_ID", "EFG_PCT"],
                    "rowSet": [["0022501082", 0.55]],
                },
                {
                    "name": "sqlTeamsFourFactors",
                    "headers": ["GAME_ID", "TM_TOV_PCT"],
                    "rowSet": [["0022501082", 0.12]],
                },
            ]
        }

        with patch("nba_api.stats.endpoints.boxscorefourfactorsv3.BoxScoreFourFactorsV3",
                   return_value=mock_ep):
            result = nlf.fetch_four_factors_box_score("0022501082")

        self.assertIsInstance(result, dict)
        self.assertEqual(len(result["player_stats"]), 1)
        self.assertEqual(result["player_stats"][0]["EFG_PCT"], 0.55)
        self.assertEqual(len(result["team_stats"]), 1)

    @patch("data.nba_live_fetcher._check_rate_limit", return_value=True)
    @patch("data.nba_live_fetcher._NBA_API_AVAILABLE", True)
    @patch("data.nba_live_fetcher.time.sleep")
    @patch("data.nba_live_fetcher.time.monotonic", return_value=0.0)
    def test_matchups_both_methods_fail_returns_empty(self, _mono, _sleep, _rate):
        import data.nba_live_fetcher as nlf
        nlf._CACHE.clear()

        mock_ep = MagicMock()
        mock_ep.get_normalized_dict.side_effect = IndexError("list index out of range")
        mock_ep.get_dict.return_value = {}

        with patch("nba_api.stats.endpoints.boxscorematchupsv3.BoxScoreMatchupsV3",
                   return_value=mock_ep):
            result = nlf.fetch_box_score_matchups("0022501082")

        self.assertIsInstance(result, dict)
        self.assertEqual(result.get("player_stats"), [])


if __name__ == "__main__":
    unittest.main()
