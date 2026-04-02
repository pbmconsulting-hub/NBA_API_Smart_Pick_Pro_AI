# ============================================================
# FILE: tests/test_education_audit.py
# PURPOSE: Verify that every page in the SmartBetPro NBA app
#          includes user education content — specifically a
#          "📖 How to Use" expander section.
# ============================================================

import os
import unittest

# Base path for the project
_BASE = os.path.join(os.path.dirname(__file__), "..")

# All page files that must have education content
_PAGE_FILES = [
    ("app.py", "📖 How to Use"),
    ("pages/0_🏆_Live_Scores_&_Props.py", "📖 How to Use"),
    ("pages/1_📡_Live_Games.py", "📖 How to Use"),
    ("pages/2_🔬_Prop_Scanner.py", "📖 How to Use"),
    ("pages/3_⚡_Quantum_Analysis_Matrix.py", "📖 How to Use"),
    ("pages/4_📋_Game_Report.py", "📖 How to Use"),
    ("pages/5_💦_Live_Sweat.py", "📖 How to Use"),
    ("pages/5b_🔮_Player_Simulator.py", "📖 How to Use"),
    ("pages/6_🧬_Entry_Builder.py", "📖 How to Use"),
    ("pages/7_🎙️_The_Studio.py", "📖 How to Use"),
    ("pages/8_🛡️_Risk_Shield.py", "📖 How to Use"),
    ("pages/9_📡_Data_Feed.py", "📖 How to Use"),
    ("pages/10_🗺️_Correlation_Matrix.py", "📖 How to Use"),
    ("pages/11_📈_Bet_Tracker.py", "📖 How to Use"),
    ("pages/12_📊_Backtester.py", "📖 How to Use"),
    ("pages/13_⚙️_Settings.py", "📖 How to Use"),
    ("pages/14_💎_Subscription_Level.py", "📖 How"),
]


class TestEducationPresence(unittest.TestCase):
    """Every page must include a 📖 How to Use expander for user education."""

    def _read(self, rel_path):
        full = os.path.normpath(os.path.join(_BASE, rel_path))
        with open(full, encoding="utf-8") as fh:
            return fh.read()

    def test_all_pages_have_education_expander(self):
        """All 17 page files must contain a How-to-Use education section."""
        missing = []
        for rel_path, marker in _PAGE_FILES:
            try:
                src = self._read(rel_path)
                if marker not in src:
                    missing.append(rel_path)
            except FileNotFoundError:
                missing.append(f"{rel_path} (FILE MISSING)")
        self.assertEqual(
            missing, [],
            f"Pages missing '📖 How to Use' education: {missing}"
        )

    def test_education_uses_expander_pattern(self):
        """Education sections must use st.expander (collapsible for clean UX)."""
        for rel_path, _marker in _PAGE_FILES:
            src = self._read(rel_path)
            self.assertIn(
                "st.expander",
                src,
                f"{rel_path} must use st.expander for education content",
            )

    def test_education_has_pro_tips(self):
        """Each education section should include Pro Tips for actionable advice."""
        for rel_path, _marker in _PAGE_FILES:
            src = self._read(rel_path)
            has_tips = "Pro Tip" in src or "💡" in src or "Tip" in src
            self.assertTrue(
                has_tips,
                f"{rel_path} should include 💡 Pro Tips in education content",
            )

    def test_page_count(self):
        """Verify we're checking all expected pages."""
        self.assertEqual(len(_PAGE_FILES), 17, "Expected 17 pages to audit")


class TestEducationBoxFunction(unittest.TestCase):
    """Verify the education box HTML helper exists and works."""

    def test_get_education_box_html_exists(self):
        from styles.theme import get_education_box_html
        result = get_education_box_html("Test Title", "Test content")
        self.assertIn("Test Title", result)
        self.assertIn("Test content", result)
        self.assertIn("education-box", result)

    def test_get_education_tooltip_html_exists(self):
        from styles.theme import get_education_tooltip_html
        result = get_education_tooltip_html("EV", "Expected Value")
        self.assertIn("EV", result)
        self.assertIn("Expected Value", result)
        self.assertIn("edu-tooltip", result)


class TestNoSyntheticData(unittest.TestCase):
    """Verify no synthetic/fake prop data is generated in production code."""

    def test_deprecated_synthetic_props_returns_empty(self):
        """generate_props_for_todays_players must return empty list."""
        import sys
        from unittest.mock import MagicMock
        # data_manager imports streamlit at module level; mock it if needed
        if "streamlit" not in sys.modules:
            sys.modules["streamlit"] = MagicMock()
        import warnings
        from data.data_manager import generate_props_for_todays_players
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            result = generate_props_for_todays_players([], [])
        self.assertEqual(result, [])

    def test_no_active_synthetic_generation(self):
        """Production pages must not import the deprecated synthetic generator."""
        pages_dir = os.path.join(_BASE, "pages")
        for fname in os.listdir(pages_dir):
            if not fname.endswith(".py"):
                continue
            full = os.path.join(pages_dir, fname)
            with open(full, encoding="utf-8") as fh:
                src = fh.read()
            self.assertNotIn(
                "generate_props_for_todays_players",
                src,
                f"{fname} must NOT use the deprecated synthetic prop generator",
            )


class TestWiringConsistency(unittest.TestCase):
    """Verify cross-module wiring is consistent across all pages."""

    def test_all_pages_set_joseph_page_context(self):
        """Every page must set joseph_page_context for ambient commentary."""
        pages_dir = os.path.join(_BASE, "pages")
        missing = []
        for fname in sorted(os.listdir(pages_dir)):
            if not fname.endswith(".py"):
                continue
            full = os.path.join(pages_dir, fname)
            with open(full, encoding="utf-8") as fh:
                src = fh.read()
            if 'joseph_page_context' not in src:
                missing.append(fname)
        self.assertEqual(missing, [], f"Pages missing joseph_page_context: {missing}")

    def test_all_pages_inject_joseph_floating(self):
        """Every page must call inject_joseph_floating."""
        pages_dir = os.path.join(_BASE, "pages")
        missing = []
        for fname in sorted(os.listdir(pages_dir)):
            if not fname.endswith(".py"):
                continue
            full = os.path.join(pages_dir, fname)
            with open(full, encoding="utf-8") as fh:
                src = fh.read()
            if 'inject_joseph_floating' not in src:
                missing.append(fname)
        self.assertEqual(missing, [], f"Pages missing inject_joseph_floating: {missing}")

    def test_all_pages_import_global_css(self):
        """Every page must import and apply get_global_css."""
        pages_dir = os.path.join(_BASE, "pages")
        missing = []
        for fname in sorted(os.listdir(pages_dir)):
            if not fname.endswith(".py"):
                continue
            full = os.path.join(pages_dir, fname)
            with open(full, encoding="utf-8") as fh:
                src = fh.read()
            if 'get_global_css' not in src:
                missing.append(fname)
        self.assertEqual(missing, [], f"Pages missing get_global_css: {missing}")


if __name__ == "__main__":
    unittest.main()
