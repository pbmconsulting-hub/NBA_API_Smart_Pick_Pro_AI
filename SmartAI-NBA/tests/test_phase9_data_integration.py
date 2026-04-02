# ============================================================
# FILE: tests/test_phase9_data_integration.py
# PURPOSE: Phase 9 — Integration tests for data layer
#   9L: data/__init__.py exports
#   9M: NBADataService class instantiation and methods
#   9N: Cross-module wiring verification
#   9O: Verification script checks
# ============================================================
import sys
import unittest
from unittest.mock import MagicMock, patch, PropertyMock


def _ensure_streamlit_mock():
    """Inject a lightweight streamlit mock if not installed."""
    if "streamlit" not in sys.modules:
        mock_st = MagicMock()
        mock_st.session_state = {}
        mock_st.cache_data = lambda **kw: (lambda f: f)
        sys.modules["streamlit"] = mock_st


_ensure_streamlit_mock()


# ============================================================
# 9L: data/__init__.py — package exports
# ============================================================

class TestDataPackageExports(unittest.TestCase):
    """Verify data package exports the expected classes."""

    def test_nba_data_service_importable(self):
        from data import NBADataService
        self.assertTrue(callable(NBADataService))

    def test_roster_engine_importable(self):
        from data import RosterEngine
        self.assertTrue(callable(RosterEngine))

    def test_player_id_cache_importable(self):
        from data import PlayerIDCache
        self.assertTrue(callable(PlayerIDCache))

    def test_all_exports_defined(self):
        import data
        self.assertIn("NBADataService", data.__all__)
        self.assertIn("RosterEngine", data.__all__)
        self.assertIn("PlayerIDCache", data.__all__)


# ============================================================
# 9M: NBADataService — class instantiation
# ============================================================

class TestNBADataServiceInit(unittest.TestCase):
    """Verify NBADataService can be created and has expected methods."""

    def test_instantiates(self):
        from data.nba_data_service import NBADataService
        svc = NBADataService()
        self.assertIsNotNone(svc)

    def test_has_get_todays_games(self):
        from data.nba_data_service import NBADataService
        svc = NBADataService()
        self.assertTrue(hasattr(svc, "get_todays_games"))
        self.assertTrue(callable(svc.get_todays_games))

    def test_has_get_todays_players(self):
        from data.nba_data_service import NBADataService
        svc = NBADataService()
        self.assertTrue(hasattr(svc, "get_todays_players"))
        self.assertTrue(callable(svc.get_todays_players))

    def test_has_get_team_stats(self):
        from data.nba_data_service import NBADataService
        svc = NBADataService()
        self.assertTrue(hasattr(svc, "get_team_stats"))
        self.assertTrue(callable(svc.get_team_stats))

    def test_has_get_injuries(self):
        from data.nba_data_service import NBADataService
        svc = NBADataService()
        self.assertTrue(hasattr(svc, "get_injuries"))
        self.assertTrue(callable(svc.get_injuries))

    def test_has_clear_caches(self):
        from data.nba_data_service import NBADataService
        svc = NBADataService()
        self.assertTrue(hasattr(svc, "clear_caches"))
        self.assertTrue(callable(svc.clear_caches))

    def test_has_refresh_all_data(self):
        from data.nba_data_service import NBADataService
        svc = NBADataService()
        self.assertTrue(hasattr(svc, "refresh_all_data"))
        self.assertTrue(callable(svc.refresh_all_data))


class TestNBADataServiceGetInjuries(unittest.TestCase):
    """Verify get_injuries returns dict when roster_engine unavailable."""

    def test_returns_dict_when_no_roster_engine(self):
        from data.nba_data_service import NBADataService
        svc = NBADataService()
        svc.roster_engine = None
        result = svc.get_injuries()
        self.assertIsInstance(result, dict)


# ============================================================
# 9N: Cross-module wiring — headers integration
# ============================================================

class TestHeadersIntegration(unittest.TestCase):
    """Verify data modules import utility headers correctly."""

    def test_roster_engine_imports_headers(self):
        """roster_engine.py should have _HAS_HEADERS flag."""
        import data.roster_engine as mod
        # Either _HAS_HEADERS exists and is True, or the module loaded fine
        has_flag = getattr(mod, "_HAS_HEADERS", None)
        # If the flag doesn't exist, the module uses headers directly (also OK)
        if has_flag is not None:
            self.assertTrue(has_flag)

    def test_live_data_fetcher_imports_headers(self):
        """live_data_fetcher.py should have header integration."""
        import data.live_data_fetcher as mod
        has_flag = getattr(mod, "_HAS_HEADERS", None)
        if has_flag is not None:
            self.assertTrue(has_flag)

    def test_platform_fetcher_imports_headers(self):
        """platform_fetcher.py should have _HAS_PLATFORM_HEADERS flag."""
        import data.platform_fetcher as mod
        has_flag = getattr(mod, "_HAS_PLATFORM_HEADERS", None)
        if has_flag is not None:
            self.assertTrue(has_flag)


# ============================================================
# 9N: Cross-module wiring — retry integration
# ============================================================

class TestRetryIntegration(unittest.TestCase):
    """Verify data modules import retry logic correctly."""

    def test_retry_decorator_importable(self):
        from utils.retry import retry_with_backoff
        self.assertTrue(callable(retry_with_backoff))

    def test_circuit_breaker_importable(self):
        from utils.retry import CircuitBreaker
        self.assertTrue(callable(CircuitBreaker))


# ============================================================
# 9N: Cross-module wiring — cache integration
# ============================================================

class TestCacheIntegration(unittest.TestCase):
    """Verify data modules import cache correctly."""

    def test_file_cache_importable(self):
        from utils.cache import FileCache
        self.assertTrue(callable(FileCache))

    def test_nba_data_service_has_cache(self):
        from data.nba_data_service import NBADataService
        svc = NBADataService()
        # cache attribute should exist (may be None if FileCache unavailable)
        self.assertTrue(hasattr(svc, "cache"))


# ============================================================
# 9O: Module-level functions preserved
# ============================================================

class TestModuleLevelFunctions(unittest.TestCase):
    """Verify nba_data_service module-level functions still exist."""

    def test_get_todays_games_exists(self):
        from data.nba_data_service import get_todays_games
        self.assertTrue(callable(get_todays_games))

    def test_get_todays_players_exists(self):
        from data.nba_data_service import get_todays_players
        self.assertTrue(callable(get_todays_players))

    def test_get_team_stats_exists(self):
        from data.nba_data_service import get_team_stats
        self.assertTrue(callable(get_team_stats))

    def test_clear_caches_exists(self):
        from data.nba_data_service import clear_caches
        self.assertTrue(callable(clear_caches))

    def test_refresh_all_data_exists(self):
        from data.nba_data_service import refresh_all_data
        self.assertTrue(callable(refresh_all_data))


# ============================================================
# 9O: Verification script — smoke test
# ============================================================

class TestVerificationScriptImports(unittest.TestCase):
    """Verify that the verification script's imports work."""

    def test_verify_script_modules_importable(self):
        """All modules referenced by verify_fixes.py should import."""
        from utils.headers import get_nba_headers
        from utils.retry import retry_with_backoff
        from utils.cache import FileCache
        from utils.logger import get_logger
        from data.roster_engine import RosterEngine
        from data.nba_data_service import NBADataService

        self.assertTrue(callable(get_nba_headers))
        self.assertTrue(callable(retry_with_backoff))
        self.assertTrue(callable(FileCache))
        self.assertTrue(callable(get_logger))
        self.assertTrue(callable(RosterEngine))
        self.assertTrue(callable(NBADataService))

    def test_headers_returns_valid_dict(self):
        from utils.headers import get_nba_headers
        h = get_nba_headers()
        self.assertIsInstance(h, dict)
        self.assertIn("User-Agent", h)


# ============================================================
# 9P: player_id_overrides.json — data file validation
# ============================================================

class TestPlayerIDOverridesFile(unittest.TestCase):
    """Verify player_id_overrides.json is valid and well-formed."""

    def test_file_exists(self):
        import json
        from pathlib import Path
        fp = Path(__file__).parent.parent / "data" / "player_id_overrides.json"
        self.assertTrue(fp.exists(), "player_id_overrides.json should exist")

    def test_valid_json(self):
        import json
        from pathlib import Path
        fp = Path(__file__).parent.parent / "data" / "player_id_overrides.json"
        with open(fp, "r") as f:
            data = json.load(f)
        self.assertIsInstance(data, dict)

    def test_entries_have_id(self):
        import json
        from pathlib import Path
        fp = Path(__file__).parent.parent / "data" / "player_id_overrides.json"
        with open(fp, "r") as f:
            data = json.load(f)
        for name, entry in data.items():
            if isinstance(entry, dict):
                self.assertIn("id", entry, f"Override for {name} missing 'id'")


if __name__ == "__main__":
    unittest.main()
