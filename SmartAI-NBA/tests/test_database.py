"""
tests/test_database.py
----------------------
Tests for tracking/database.py — SQLite database operations.
"""

import sys
import os
import pathlib
import unittest
import sqlite3
import tempfile

# ── Ensure repo root is on sys.path ──────────────────────────────
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

_DB_SRC = pathlib.Path(__file__).parent.parent / "tracking" / "database.py"


class TestDatabaseSourceLevel(unittest.TestCase):
    """Source-level checks for tracking/database.py."""

    def test_file_exists(self):
        self.assertTrue(_DB_SRC.exists(), "tracking/database.py must exist")

    def test_has_initialize_database(self):
        src = _DB_SRC.read_text(encoding="utf-8")
        self.assertIn("def initialize_database(", src)

    def test_has_get_database_connection(self):
        src = _DB_SRC.read_text(encoding="utf-8")
        self.assertIn("def get_database_connection(", src)

    def test_has_insert_bet(self):
        src = _DB_SRC.read_text(encoding="utf-8")
        self.assertIn("def insert_bet(", src)

    def test_has_load_all_bets(self):
        src = _DB_SRC.read_text(encoding="utf-8")
        self.assertIn("def load_all_bets(", src)

    def test_wal_mode_enabled(self):
        """WAL mode must be set in get_database_connection."""
        src = _DB_SRC.read_text(encoding="utf-8")
        self.assertIn("PRAGMA journal_mode=WAL", src)

    def test_timeout_set(self):
        """Connection timeout should be set."""
        src = _DB_SRC.read_text(encoding="utf-8")
        self.assertIn("timeout=30", src)

    def test_integrity_check(self):
        """Database initialization should include integrity check."""
        src = _DB_SRC.read_text(encoding="utf-8")
        self.assertIn("PRAGMA integrity_check", src)


class TestDatabaseInitialization(unittest.TestCase):
    """Test that initialize_database creates all required tables."""

    def setUp(self):
        """Set up a temporary database for testing."""
        self.temp_dir = tempfile.mkdtemp()
        self.original_db_dir = None
        self.original_db_path = None
        # Patch database path to use temp dir
        import tracking.database as db_mod
        self.original_db_dir = db_mod.DB_DIRECTORY
        self.original_db_path = db_mod.DB_FILE_PATH
        db_mod.DB_DIRECTORY = pathlib.Path(self.temp_dir)
        db_mod.DB_FILE_PATH = pathlib.Path(self.temp_dir) / "test.db"

    def tearDown(self):
        """Restore original database paths."""
        import tracking.database as db_mod
        db_mod.DB_DIRECTORY = self.original_db_dir
        db_mod.DB_FILE_PATH = self.original_db_path
        # Clean up temp dir
        import shutil
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_initialize_creates_bets_table(self):
        from tracking.database import initialize_database
        result = initialize_database()
        self.assertTrue(result)
        # Verify bets table exists
        db_path = os.path.join(self.temp_dir, "test.db")
        conn = sqlite3.connect(db_path)
        cursor = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='bets'")
        self.assertIsNotNone(cursor.fetchone())
        conn.close()

    def test_initialize_creates_subscriptions_table(self):
        from tracking.database import initialize_database
        initialize_database()
        db_path = os.path.join(self.temp_dir, "test.db")
        conn = sqlite3.connect(db_path)
        cursor = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='subscriptions'")
        self.assertIsNotNone(cursor.fetchone())
        conn.close()

    def test_initialize_is_idempotent(self):
        """Calling initialize_database twice should not error."""
        from tracking.database import initialize_database
        self.assertTrue(initialize_database())
        self.assertTrue(initialize_database())


class TestInsertAndLoadBets(unittest.TestCase):
    """Test insert_bet and load_all_bets round-trip."""

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        import tracking.database as db_mod
        self.original_db_dir = db_mod.DB_DIRECTORY
        self.original_db_path = db_mod.DB_FILE_PATH
        db_mod.DB_DIRECTORY = pathlib.Path(self.temp_dir)
        db_mod.DB_FILE_PATH = pathlib.Path(self.temp_dir) / "test.db"
        from tracking.database import initialize_database
        initialize_database()

    def tearDown(self):
        import tracking.database as db_mod
        db_mod.DB_DIRECTORY = self.original_db_dir
        db_mod.DB_FILE_PATH = self.original_db_path
        import shutil
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_insert_and_load_round_trip(self):
        from tracking.database import insert_bet, load_all_bets
        bet_data = {
            "bet_date": "2026-03-24",
            "player_name": "LeBron James",
            "team": "LAL",
            "stat_type": "points",
            "prop_line": 24.5,
            "direction": "OVER",
            "platform": "PrizePicks",
            "confidence_score": 82.0,
            "probability_over": 0.65,
            "edge_percentage": 12.0,
            "tier": "Gold",
        }
        bet_id = insert_bet(bet_data)
        self.assertIsNotNone(bet_id)
        self.assertIsInstance(bet_id, int)

        bets = load_all_bets(limit=10)
        self.assertGreater(len(bets), 0)
        loaded_bet = bets[0]
        self.assertEqual(loaded_bet["player_name"], "LeBron James")
        self.assertEqual(loaded_bet["stat_type"], "points")


if __name__ == "__main__":
    unittest.main()
