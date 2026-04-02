# ============================================================
# FILE: tests/test_apex_optimization.py
# PURPOSE: Tests for the APEX Optimization Directive:
#          1) True Line extraction & crash prevention
#          2) Async bet resolver (ThreadPoolExecutor)
#          3) CSS logo sizing overrides
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
        sys.modules["streamlit"] = mock_st


# ============================================================
# Pillar 1: True Line Extraction & Crash Prevention
# ============================================================

# ============================================================
# Pillar 2: Async Bet Tracker (ThreadPoolExecutor)
# ============================================================

class TestAsyncBetResolver(unittest.TestCase):
    """Verify bet resolver uses ThreadPoolExecutor for parallel retrieval."""

    def test_threadpoolexecutor_in_auto_resolve(self):
        """auto_resolve_bet_results should import ThreadPoolExecutor."""
        src = pathlib.Path(__file__).parent.parent / "tracking" / "bet_tracker.py"
        content = src.read_text(encoding="utf-8")
        self.assertIn("ThreadPoolExecutor", content,
                       "auto_resolve_bet_results should use ThreadPoolExecutor")

    def test_as_completed_in_auto_resolve(self):
        """auto_resolve_bet_results should use as_completed for future gathering."""
        src = pathlib.Path(__file__).parent.parent / "tracking" / "bet_tracker.py"
        content = src.read_text(encoding="utf-8")
        self.assertIn("as_completed", content,
                       "Should use as_completed from concurrent.futures")

    def test_game_log_cache_in_resolve(self):
        """Resolver should pre-cache game logs before processing bets."""
        src = pathlib.Path(__file__).parent.parent / "tracking" / "bet_tracker.py"
        content = src.read_text(encoding="utf-8")
        self.assertIn("_game_log_cache", content,
                       "Resolver should maintain a _game_log_cache for parallel get results")

    def test_parallel_get_helper_exists(self):
        """A _get_player_log helper should exist for the ThreadPoolExecutor."""
        src = pathlib.Path(__file__).parent.parent / "tracking" / "bet_tracker.py"
        content = src.read_text(encoding="utf-8")
        self.assertIn("def _get_player_log(", content,
                       "_get_player_log helper should be defined for parallel retrieval")

    def test_max_workers_capped(self):
        """ThreadPoolExecutor max_workers should be capped at 8."""
        src = pathlib.Path(__file__).parent.parent / "tracking" / "bet_tracker.py"
        content = src.read_text(encoding="utf-8")
        self.assertIn("max_workers=min(8", content,
                       "ThreadPoolExecutor should cap max_workers at 8")

    def test_resolve_todays_uses_threadpool(self):
        """resolve_todays_bets should also use ThreadPoolExecutor."""
        src = pathlib.Path(__file__).parent.parent / "tracking" / "bet_tracker.py"
        content = src.read_text(encoding="utf-8")
        # Count ThreadPoolExecutor references — should appear in both resolve functions
        count = content.count("ThreadPoolExecutor")
        self.assertGreaterEqual(count, 2,
                                f"Expected ThreadPoolExecutor in ≥2 resolve functions, got {count}")


# ============================================================
# Pillar 3: CSS Logo Sizing Overrides
# ============================================================

class TestCSSLogoOverrides(unittest.TestCase):
    """Verify CSS overrides for hero banner and sidebar logos."""

    def test_hero_logo_width_250px(self):
        """Hero banner logo should have width: 88px !important (65% smaller than original 250px)."""
        src = pathlib.Path(__file__).parent.parent / "app.py"
        content = src.read_text(encoding="utf-8")
        self.assertIn("width: 88px !important", content,
                       "Hero logo should have strict 88px width (65% reduction)")

    def test_hero_logo_object_fit(self):
        """Hero banner logo should use object-fit: contain."""
        src = pathlib.Path(__file__).parent.parent / "app.py"
        content = src.read_text(encoding="utf-8")
        self.assertIn("object-fit: contain", content,
                       "Hero logo should use object-fit: contain")

    def test_sidebar_logo_max_width_220px(self):
        """Sidebar logo should have max-width: 220px !important."""
        src = pathlib.Path(__file__).parent.parent / "styles" / "theme.py"
        content = src.read_text(encoding="utf-8")
        self.assertIn("max-width: 220px !important", content,
                       "Sidebar logo should have 220px max-width override")

    def test_sidebar_logo_scale_transform(self):
        """Sidebar logo should use transform: scale(1.2)."""
        src = pathlib.Path(__file__).parent.parent / "styles" / "theme.py"
        content = src.read_text(encoding="utf-8")
        self.assertIn("transform: scale(1.2)", content,
                       "Sidebar logo should have scale(1.2) transform")

    def test_sidebar_logo_margin_offset(self):
        """Sidebar logo should have margin-left: -5px."""
        src = pathlib.Path(__file__).parent.parent / "styles" / "theme.py"
        content = src.read_text(encoding="utf-8")
        self.assertIn("margin-left: -5px", content,
                       "Sidebar logo should have -5px left margin offset")

    def test_sidebar_targets_st_logo_and_header(self):
        """CSS should target both stLogo and stSidebarHeader selectors."""
        src = pathlib.Path(__file__).parent.parent / "styles" / "theme.py"
        content = src.read_text(encoding="utf-8")
        self.assertIn('[data-testid="stSidebarHeader"]', content,
                       "CSS should target stSidebarHeader for sidebar logo")
        self.assertIn('[data-testid="stLogo"]', content,
                       "CSS should target stLogo for sidebar logo")

    def test_hero_logo_responsive_keeps_250px(self):
        """Mobile responsive CSS should maintain 88px width for hero logo."""
        src = pathlib.Path(__file__).parent.parent / "app.py"
        content = src.read_text(encoding="utf-8")
        # Check that the responsive media query also uses 88px
        self.assertIn(".spp-hero-logo { width: 88px !important", content,
                       "Responsive hero logo should maintain 88px width")


if __name__ == "__main__":
    unittest.main()
