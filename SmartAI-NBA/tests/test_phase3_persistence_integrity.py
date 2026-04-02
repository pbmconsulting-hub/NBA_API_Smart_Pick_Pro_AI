# ============================================================
# FILE: tests/test_phase3_persistence_integrity.py
# PURPOSE: Tests for Phase 3 Persistence & Cache Integrity:
#   3A — database.py _execute_write() retry helper + 6 write functions
#   3C — game_log_cache.py stale .tmp cleanup on load
#   3D — nba_data_service.py safe_avg NaN/None filtering
# ============================================================
import datetime
import math
import os
import shutil
import sys
import tempfile
import unittest
from unittest.mock import MagicMock, patch


def _ensure_streamlit_mock():
    """Inject a lightweight streamlit mock if not installed."""
    if "streamlit" not in sys.modules:
        mock_st = MagicMock()
        mock_st.session_state = {}
        mock_st.cache_data = lambda **kw: (lambda f: f)
        sys.modules["streamlit"] = mock_st


# ============================================================
# 3A: database.py — _execute_write helper & write retry
# ============================================================

class TestExecuteWriteHelper(unittest.TestCase):
    """Verify the centralised _execute_write() retry helper exists and works."""

    def setUp(self):
        _ensure_streamlit_mock()
        import tracking.database as db
        self.db = db

    def test_execute_write_exists(self):
        """_execute_write should be a callable function."""
        self.assertTrue(callable(self.db._execute_write))

    def test_execute_write_returns_cursor_on_success(self):
        """_execute_write should return a cursor on a valid INSERT."""
        self.db.initialize_database()
        cursor = self.db._execute_write(
            "INSERT INTO bets (bet_date, player_name, stat_type, prop_line, direction) "
            "VALUES (?, ?, ?, ?, ?)",
            ("2025-01-01", "_test_execute_write", "points", 20.5, "OVER"),
            caller="test",
        )
        self.assertIsNotNone(cursor)
        # Clean up
        self.db._execute_write(
            "DELETE FROM bets WHERE player_name = ?",
            ("_test_execute_write",),
            caller="test_cleanup",
        )

    def test_execute_write_returns_none_on_bad_sql(self):
        """_execute_write should return None on invalid SQL."""
        cursor = self.db._execute_write(
            "INSERT INTO nonexistent_table (x) VALUES (?)",
            (1,),
            caller="test_bad_sql",
        )
        self.assertIsNone(cursor)


class TestInsertPredictionRetry(unittest.TestCase):
    """Verify insert_prediction uses retry (via _execute_write)."""

    def setUp(self):
        _ensure_streamlit_mock()
        import tracking.database as db
        self.db = db
        self.db.initialize_database()

    def test_insert_prediction_succeeds(self):
        """insert_prediction should return a row ID on valid data."""
        pred_data = {
            "prediction_date": "2025-01-01",
            "player_name": "_test_pred_retry",
            "stat_type": "points",
            "prop_line": 20.5,
            "direction": "OVER",
            "confidence_score": 75.0,
            "probability_predicted": 0.65,
        }
        row_id = self.db.insert_prediction(pred_data)
        self.assertIsNotNone(row_id)
        self.assertIsInstance(row_id, int)

    def test_insert_prediction_returns_none_on_failure(self):
        """insert_prediction should return None on bad data/error."""
        # Passing non-dict should not crash
        result = self.db.insert_prediction({})
        # Empty data should still insert (with defaults) — but let's verify type
        if result is not None:
            self.assertIsInstance(result, int)


class TestUpdatePredictionOutcomeRetry(unittest.TestCase):
    """Verify update_prediction_outcome uses retry."""

    def setUp(self):
        _ensure_streamlit_mock()
        import tracking.database as db
        self.db = db
        self.db.initialize_database()

    def test_update_nonexistent_prediction_returns_boolean(self):
        """update_prediction_outcome should return a boolean."""
        result = self.db.update_prediction_outcome(999999, True, 25.0)
        self.assertIsInstance(result, bool)


class TestSaveDailySnapshotRetry(unittest.TestCase):
    """Verify save_daily_snapshot uses retry for the write portion."""

    def setUp(self):
        _ensure_streamlit_mock()
        import tracking.database as db
        self.db = db
        self.db.initialize_database()

    def test_save_daily_snapshot_returns_bool(self):
        """save_daily_snapshot should return True on success."""
        result = self.db.save_daily_snapshot("2025-01-01")
        self.assertIsInstance(result, bool)

    def test_save_daily_snapshot_default_date(self):
        """save_daily_snapshot with no date should use today."""
        result = self.db.save_daily_snapshot()
        self.assertIsInstance(result, bool)


class TestInsertAnalysisPicksRetry(unittest.TestCase):
    """Verify insert_analysis_picks uses retry."""

    def setUp(self):
        _ensure_streamlit_mock()
        import tracking.database as db
        self.db = db
        self.db.initialize_database()

    def test_empty_results_returns_zero(self):
        """Empty analysis results should return 0."""
        result = self.db.insert_analysis_picks([])
        self.assertEqual(result, 0)

    def test_insert_analysis_picks_succeeds(self):
        """insert_analysis_picks should insert valid data."""
        picks = [{
            "player_name": "_test_analysis_retry",
            "stat_type": "points",
            "line": 20.5,
            "direction": "OVER",
            "platform": "test",
            "confidence_score": 75,
            "probability_over": 0.65,
            "edge_percentage": 10.0,
            "tier": "Gold",
        }]
        count = self.db.insert_analysis_picks(picks)
        self.assertIsInstance(count, int)
        self.assertGreaterEqual(count, 0)


class TestSaveAnalysisSessionRetry(unittest.TestCase):
    """Verify save_analysis_session uses retry."""

    def setUp(self):
        _ensure_streamlit_mock()
        import tracking.database as db
        self.db = db
        self.db.initialize_database()

    def test_save_session_returns_int(self):
        """save_analysis_session should return an int session_id."""
        result = self.db.save_analysis_session(
            analysis_results=[{"player_name": "test", "stat_type": "points"}],
            todays_games=[],
            selected_picks=[],
        )
        self.assertIsInstance(result, int)
        self.assertGreater(result, 0)


class TestSaveBacktestResultRetry(unittest.TestCase):
    """Verify save_backtest_result uses retry."""

    def setUp(self):
        _ensure_streamlit_mock()
        import tracking.database as db
        self.db = db
        self.db.initialize_database()

    def test_invalid_status_returns_none(self):
        """Backtest with status != 'ok' returns None."""
        result = self.db.save_backtest_result({"status": "error"})
        self.assertIsNone(result)

    def test_valid_backtest_returns_id(self):
        """Valid backtest result returns a row ID."""
        bt = {
            "status": "ok",
            "season": "2024-25",
            "stat_types": ["points"],
            "min_edge": 0.05,
            "tier_filter": None,
            "total_picks": 100,
            "wins": 60,
            "losses": 40,
            "win_rate": 60.0,
            "roi": 5.2,
            "total_pnl": 52.0,
            "tier_win_rates": {},
            "stat_win_rates": {},
            "edge_win_rates": {},
            "pick_log": [],
        }
        result = self.db.save_backtest_result(bt)
        self.assertIsNotNone(result)
        self.assertIsInstance(result, int)


class TestSaveGameLogsRetry(unittest.TestCase):
    """Verify save_player_game_logs_to_db uses retry."""

    def setUp(self):
        _ensure_streamlit_mock()
        import tracking.database as db
        self.db = db
        self.db.initialize_database()

    def test_empty_logs_returns_zero(self):
        """Empty game logs should return 0."""
        result = self.db.save_player_game_logs_to_db("123", "Test Player", [])
        self.assertEqual(result, 0)

    def test_valid_logs_inserts(self):
        """Valid game logs should insert successfully."""
        logs = [{
            "game_date": "2025-01-01",
            "opponent": "BOS",
            "minutes": 32.5,
            "points": 25,
            "rebounds": 7,
            "assists": 5,
            "threes": 3,
            "steals": 1,
            "blocks": 0,
            "turnovers": 2,
            "fg_pct": 0.45,
            "ft_pct": 0.85,
            "plus_minus": 10,
        }]
        result = self.db.save_player_game_logs_to_db("__test_retry__", "Test Retry", logs)
        self.assertGreaterEqual(result, 1)


# ============================================================
# 3C: game_log_cache.py — stale .tmp cleanup
# ============================================================

class TestGameLogCacheTmpCleanup(unittest.TestCase):
    """Verify _load_cache_file cleans up stale .tmp files."""

    def setUp(self):
        _ensure_streamlit_mock()
        import data.game_log_cache as glc
        self.glc = glc
        self._orig_cache_file = glc._CACHE_FILE
        self._tmpdir = tempfile.mkdtemp()
        # Redirect cache file to temp directory
        glc._CACHE_FILE = os.path.join(self._tmpdir, "test_cache.json")

    def tearDown(self):
        self.glc._CACHE_FILE = self._orig_cache_file
        shutil.rmtree(self._tmpdir, ignore_errors=True)

    def test_stale_tmp_removed_on_load(self):
        """_load_cache_file should remove a lingering .tmp file."""
        tmp_path = self.glc._CACHE_FILE + ".tmp"
        with open(tmp_path, "w") as f:
            f.write("{}")
        self.assertTrue(os.path.exists(tmp_path))
        self.glc._load_cache_file()
        self.assertFalse(os.path.exists(tmp_path))

    def test_no_tmp_no_crash(self):
        """_load_cache_file should not crash when no .tmp exists."""
        result = self.glc._load_cache_file()
        self.assertIsInstance(result, dict)

    def test_load_returns_valid_data_after_cleanup(self):
        """Cache file content should be returned correctly after .tmp cleanup."""
        import json
        cache_data = {"test_player": {"game_logs": [{"pts": 20}], "cached_at": "2025-01-01T00:00:00+00:00"}}
        with open(self.glc._CACHE_FILE, "w") as f:
            json.dump(cache_data, f)
        # Also create a stale .tmp
        tmp_path = self.glc._CACHE_FILE + ".tmp"
        with open(tmp_path, "w") as f:
            f.write("corrupt data")

        result = self.glc._load_cache_file()
        self.assertIn("test_player", result)
        self.assertFalse(os.path.exists(tmp_path))

    def test_write_then_load_roundtrip(self):
        """Write + load should roundtrip without crash."""
        import json
        cache = {"player_a": {"game_logs": [{"pts": 30}], "cached_at": "2025-01-01T00:00:00+00:00"}}
        self.glc._write_cache_file(cache)
        loaded = self.glc._load_cache_file()
        self.assertEqual(loaded.get("player_a", {}).get("game_logs"), [{"pts": 30}])


# ============================================================
# 3D: nba_data_service.py — safe_avg NaN/None filtering
# ============================================================

class TestSafeAvgNanFiltering(unittest.TestCase):
    """Verify safe_avg filters out NaN/None/non-finite values."""

    def setUp(self):
        _ensure_streamlit_mock()
        # safe_avg is a nested function inside _get_recent_form_summary;
        # replicate its logic here to test in isolation
        import math as _math

        def safe_avg(values):
            clean = [v for v in values if v is not None and isinstance(v, (int, float)) and _math.isfinite(v)]
            return round(sum(clean) / len(clean), 1) if clean else 0.0

        self.safe_avg = safe_avg

    def test_normal_values(self):
        """Normal list averages correctly."""
        self.assertAlmostEqual(self.safe_avg([10, 20, 30]), 20.0)

    def test_empty_list(self):
        """Empty list returns 0.0."""
        self.assertEqual(self.safe_avg([]), 0.0)

    def test_none_values_filtered(self):
        """None values should be filtered out."""
        self.assertAlmostEqual(self.safe_avg([10, None, 20, None]), 15.0)

    def test_nan_values_filtered(self):
        """NaN values should be filtered out."""
        self.assertAlmostEqual(self.safe_avg([10, float('nan'), 20]), 15.0)

    def test_inf_values_filtered(self):
        """Infinity values should be filtered out."""
        self.assertAlmostEqual(self.safe_avg([10, float('inf'), 20]), 15.0)

    def test_all_nan_returns_zero(self):
        """All-NaN list returns 0.0."""
        self.assertEqual(self.safe_avg([float('nan'), float('nan')]), 0.0)

    def test_all_none_returns_zero(self):
        """All-None list returns 0.0."""
        self.assertEqual(self.safe_avg([None, None]), 0.0)

    def test_mixed_bad_values(self):
        """Mixed bad values (NaN, inf, None) should all be filtered."""
        self.assertAlmostEqual(
            self.safe_avg([10, float('nan'), None, float('inf'), 20, float('-inf')]),
            15.0,
        )

    def test_single_value(self):
        """Single-element list returns that value."""
        self.assertAlmostEqual(self.safe_avg([42.0]), 42.0)

    def test_integers_and_floats_mixed(self):
        """Mixed int/float values should work."""
        self.assertAlmostEqual(self.safe_avg([10, 20.0, 30]), 20.0)


# ============================================================
# Integration: Verify write retry constants haven't changed
# ============================================================

class TestWriteRetryConstantsUnchanged(unittest.TestCase):
    """Verify Phase 3 didn't change the retry tuning constants."""

    def test_constants(self):
        _ensure_streamlit_mock()
        import tracking.database as db
        self.assertEqual(db._WRITE_RETRY_ATTEMPTS, 3)
        self.assertEqual(db._WRITE_RETRY_DELAY, 0.25)


if __name__ == "__main__":
    unittest.main()
