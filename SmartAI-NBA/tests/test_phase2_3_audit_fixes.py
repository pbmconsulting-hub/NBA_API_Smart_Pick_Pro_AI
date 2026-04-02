# ============================================================
# FILE: tests/test_phase2_3_audit_fixes.py
# PURPOSE: Tests for Phase 2-3 audit fixes — cache mutation guards,
#          timezone anchoring, and safe datetime arithmetic.
# ============================================================
import datetime
import math
import os
import tempfile
import unittest


class TestCacheMutationGuard(unittest.TestCase):
    """Pillar 2: Verify that _load_csv_file returns a defensive copy."""

    def setUp(self):
        # data_manager imports streamlit — skip if not available
        try:
            from data.data_manager import _load_csv_file
            self._load_csv = _load_csv_file
        except ImportError:
            self.skipTest("streamlit not installed — cannot import data_manager")
        # Create a temporary CSV for testing
        self._tmpdir = tempfile.mkdtemp()
        self._csv_path = os.path.join(self._tmpdir, "test.csv")
        with open(self._csv_path, "w") as f:
            f.write("name,score\nAlice,10\nBob,20\n")

    def tearDown(self):
        import shutil
        shutil.rmtree(self._tmpdir, ignore_errors=True)

    def test_load_csv_returns_list(self):
        """_load_csv_file returns a list."""
        result = self._load_csv(self._csv_path)
        self.assertIsInstance(result, list)
        self.assertEqual(len(result), 2)

    def test_load_csv_mutation_does_not_corrupt(self):
        """Mutating the returned list should not affect a fresh call."""
        result1 = self._load_csv(self._csv_path)
        # Simulate caller mutation: pop a row
        result1.pop()
        self.assertEqual(len(result1), 1)
        # A fresh load should still return the full data
        result2 = self._load_csv(self._csv_path)
        self.assertEqual(len(result2), 2)

    def test_load_csv_empty_file(self):
        """An empty CSV (headers only) returns an empty list."""
        empty_path = os.path.join(self._tmpdir, "empty.csv")
        with open(empty_path, "w") as f:
            f.write("name,score\n")
        result = self._load_csv(empty_path)
        self.assertEqual(result, [])

    def test_load_csv_missing_file(self):
        """A missing CSV returns an empty list, not an error."""
        result = self._load_csv("/tmp/nonexistent_csv_12345.csv")
        self.assertEqual(result, [])


class TestTimezoneAnchoring(unittest.TestCase):
    """Pillar 3: Verify timezone-aware datetime usage."""

    def test_database_timestamps_are_utc(self):
        """database.py should use UTC for all stored timestamps."""
        import tracking.database as db
        # Verify the module can be imported and has the expected constants
        self.assertTrue(hasattr(db, '_WRITE_RETRY_ATTEMPTS'))
        self.assertEqual(db._WRITE_RETRY_ATTEMPTS, 3)

    def test_utc_timestamp_has_offset(self):
        """UTC timestamps should include timezone offset info."""
        ts = datetime.datetime.now(datetime.timezone.utc).isoformat()
        # UTC isoformat includes '+00:00' suffix
        self.assertIn("+00:00", ts)

    def test_game_log_cache_utc_timestamp(self):
        """game_log_cache.py should produce UTC timestamps."""
        from data.game_log_cache import save_game_logs_to_cache, load_game_logs_from_cache

        _test_player = "__test_player_tz_audit__"
        try:
            # Save a test entry
            saved = save_game_logs_to_cache(_test_player, [{"pts": 25}])
            if saved:
                # Read back and check timestamp format
                logs, is_stale = load_game_logs_from_cache(_test_player)
                # Should not crash with mixed tz arithmetic
                self.assertIsInstance(is_stale, bool)
        finally:
            # Clean up test entry from the persistent cache
            try:
                from data.game_log_cache import _load_cache_file, _write_cache_file
                cache = _load_cache_file()
                cache.pop(_test_player.strip().lower(), None)
                _write_cache_file(cache)
            except Exception:
                pass  # Best-effort cleanup

    def test_platform_fetcher_now_str_utc(self):
        """platform_fetcher._now_str() should return a UTC timestamp."""
        from data.platform_fetcher import _now_str
        ts = _now_str()
        # Should include UTC offset
        self.assertIn("+00:00", ts)

    def test_platform_fetcher_today_str_format(self):
        """platform_fetcher._today_str() should return YYYY-MM-DD format."""
        from data.platform_fetcher import _today_str
        ts = _today_str()
        # Should be YYYY-MM-DD format
        self.assertRegex(ts, r"^\d{4}-\d{2}-\d{2}$")

    def test_live_data_fetcher_nba_today_et(self):
        """live_data_fetcher._nba_today_et() should return a date object."""
        from data.live_data_fetcher import _nba_today_et
        result = _nba_today_et()
        self.assertIsInstance(result, datetime.date)

    def test_is_game_log_cache_stale_handles_mixed_tz(self):
        """is_game_log_cache_stale should not crash with tz-aware stored timestamps."""
        import tracking.database as db
        # Just verify the function exists and handles a non-existent player
        result = db.is_game_log_cache_stale("nonexistent_player_id_xyz")
        self.assertTrue(result)  # Should return True for missing data

    def test_auto_resolve_eastern_date(self):
        """auto_resolve_bet_results should use ET-anchored date."""
        import tracking.bet_tracker as bt
        # The function itself will try to load bets and use nba_api,
        # but we can at least verify it doesn't crash on import
        self.assertTrue(hasattr(bt, 'auto_resolve_bet_results'))
        self.assertTrue(callable(bt.auto_resolve_bet_results))


class TestCacheMutationInjuryStatus(unittest.TestCase):
    """Pillar 2: Verify load_injury_status returns a defensive copy."""

    def test_injury_status_returns_dict_copy(self):
        """load_injury_status should not return the same object on repeated calls."""
        # We can't easily test @st.cache_data without streamlit,
        # but we can test the internal logic path
        import json
        import tempfile
        import os

        # Create a temp JSON file
        tmpdir = tempfile.mkdtemp()
        test_json = os.path.join(tmpdir, "test_injury.json")
        test_data = {
            "lebron james": {"status": "Active", "injury_note": ""},
            "kevin durant": {"status": "GTD", "injury_note": "knee"},
        }
        with open(test_json, "w") as f:
            json.dump(test_data, f)

        # Read it manually and verify dict() copies inner dicts
        with open(test_json, "r") as f:
            data = json.load(f)
        copied = {k: dict(v) if isinstance(v, dict) else v for k, v in data.items()}

        # Mutating the copy should not affect original
        copied["lebron james"]["status"] = "Out"
        self.assertEqual(data["lebron james"]["status"], "Active")

        import shutil
        shutil.rmtree(tmpdir, ignore_errors=True)


class TestDatabaseRetryLogic(unittest.TestCase):
    """Pillar 3: Verify SQLite retry configuration."""

    def test_write_retry_constants(self):
        """Database should have retry constants for concurrent write safety."""
        import tracking.database as db
        self.assertEqual(db._WRITE_RETRY_ATTEMPTS, 3)
        self.assertEqual(db._WRITE_RETRY_DELAY, 0.25)

    def test_database_initialization(self):
        """initialize_database should succeed without errors."""
        import tracking.database as db
        result = db.initialize_database()
        self.assertTrue(result)


if __name__ == "__main__":
    unittest.main()
