# ============================================================
# FILE: tests/test_3_pillar_upgrade.py
# PURPOSE: Tests for the 3-Pillar Upgrade:
#          1) Engine Rebrand (JM5 → Quantum Matrix Engine 5.6)
#          2) Global Settings component (render_global_settings)
#          3) Pre-Analysis Prop Funnel (smart_filter_props wiring)
# ============================================================

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
# Pillar 1: Engine Rebrand
# ============================================================

class TestEngineRebrand(unittest.TestCase):
    """Verify all JM5 references have been replaced with Quantum Matrix Engine 5.6."""

    def test_no_jm5_in_theme(self):
        """styles/theme.py should contain no 'JM5' references."""
        import pathlib
        theme_path = pathlib.Path(__file__).parent.parent / "styles" / "theme.py"
        content = theme_path.read_text(encoding="utf-8")
        self.assertNotIn("JM5", content, "Found residual 'JM5' reference in styles/theme.py")

    def test_no_jm5_in_neural_analysis(self):
        """pages/3_⚡_Quantum_Analysis_Matrix.py should contain no 'JM5' references."""
        import pathlib
        na_path = pathlib.Path(__file__).parent.parent / "pages" / "3_⚡_Quantum_Analysis_Matrix.py"
        content = na_path.read_text(encoding="utf-8")
        self.assertNotIn("JM5", content, "Found residual 'JM5' reference in Neural Analysis page")

    def test_quantum_matrix_in_theme(self):
        """styles/theme.py should mention 'Quantum Matrix Engine 5.6'."""
        import pathlib
        theme_path = pathlib.Path(__file__).parent.parent / "styles" / "theme.py"
        content = theme_path.read_text(encoding="utf-8")
        self.assertIn("Quantum Matrix Engine 5.6", content)

    def test_quantum_matrix_in_neural_analysis(self):
        """pages/3_⚡_Quantum_Analysis_Matrix.py should mention 'Quantum Matrix Engine 5.6'."""
        import pathlib
        na_path = pathlib.Path(__file__).parent.parent / "pages" / "3_⚡_Quantum_Analysis_Matrix.py"
        content = na_path.read_text(encoding="utf-8")
        self.assertIn("Quantum Matrix Engine 5.6", content)


# ============================================================
# Pillar 2: Global Settings Component
# ============================================================

class TestGlobalSettingsComponent(unittest.TestCase):
    """Verify the render_global_settings utility exists and is importable."""

    def test_component_module_exists(self):
        """utils/components.py should be importable."""
        import pathlib
        comp_path = pathlib.Path(__file__).parent.parent / "utils" / "components.py"
        self.assertTrue(comp_path.exists(), "utils/components.py does not exist")

    def test_render_global_settings_defined(self):
        """render_global_settings should be a callable in utils/components."""
        import pathlib
        comp_path = pathlib.Path(__file__).parent.parent / "utils" / "components.py"
        source = comp_path.read_text(encoding="utf-8")
        self.assertIn("def render_global_settings()", source)

    def test_sync_callbacks_defined(self):
        """on_change callback functions should be defined in utils/components.py."""
        import pathlib
        comp_path = pathlib.Path(__file__).parent.parent / "utils" / "components.py"
        source = comp_path.read_text(encoding="utf-8")
        self.assertIn("def _sync_sim_depth()", source)
        self.assertIn("def _sync_edge_threshold()", source)

    def test_settings_injected_in_app(self):
        """app.py should import and call render_global_settings."""
        import pathlib
        app_path = pathlib.Path(__file__).parent.parent / "app.py"
        source = app_path.read_text(encoding="utf-8")
        self.assertIn("from utils.components import render_global_settings", source)
        self.assertIn("render_global_settings()", source)

    def test_settings_injected_in_neural_analysis(self):
        """Neural Analysis page should import and call render_global_settings."""
        import pathlib
        na_path = pathlib.Path(__file__).parent.parent / "pages" / "3_⚡_Quantum_Analysis_Matrix.py"
        source = na_path.read_text(encoding="utf-8")
        self.assertIn("from utils.components import render_global_settings", source)
        self.assertIn("render_global_settings()", source)

    def test_settings_injected_in_entry_builder(self):
        """Entry Builder page should import and call render_global_settings."""
        import pathlib
        eb_path = pathlib.Path(__file__).parent.parent / "pages" / "6_🧬_Entry_Builder.py"
        source = eb_path.read_text(encoding="utf-8")
        self.assertIn("from utils.components import render_global_settings", source)
        self.assertIn("render_global_settings()", source)


# ============================================================
# Pillar 3: Pre-Analysis Prop Funnel
# ============================================================

class TestPreAnalysisFunnel(unittest.TestCase):
    """Verify the pre-analysis filter funnel has been removed from Neural Analysis.

    The prop funnel and stat-type filter UI was removed so the engine
    analyzes ALL available props without user-facing stat-type filtering.
    """

    def test_funnel_expander_removed(self):
        """Neural Analysis page should NOT have a Market Filters expander."""
        import pathlib
        na_path = pathlib.Path(__file__).parent.parent / "pages" / "3_⚡_Quantum_Analysis_Matrix.py"
        source = na_path.read_text(encoding="utf-8")
        self.assertNotIn('Market Filters', source)

    def test_funnel_stat_multiselect_removed(self):
        """Neural Analysis page should NOT have a stat type multiselect widget."""
        import pathlib
        na_path = pathlib.Path(__file__).parent.parent / "pages" / "3_⚡_Quantum_Analysis_Matrix.py"
        source = na_path.read_text(encoding="utf-8")
        self.assertNotIn("_STAT_TYPE_OPTIONS", source)

    def test_funnel_max_per_player_removed(self):
        """Neural Analysis page should NOT have a user-facing max-props-per-player control."""
        import pathlib
        na_path = pathlib.Path(__file__).parent.parent / "pages" / "3_⚡_Quantum_Analysis_Matrix.py"
        source = na_path.read_text(encoding="utf-8")
        self.assertNotIn("Max Props per Player", source)
        self.assertNotIn("funnel_max_per_player", source)

    def test_funnel_absolute_max_removed(self):
        """Neural Analysis page should NOT have an absolute-max-props control."""
        import pathlib
        na_path = pathlib.Path(__file__).parent.parent / "pages" / "3_⚡_Quantum_Analysis_Matrix.py"
        source = na_path.read_text(encoding="utf-8")
        self.assertNotIn("Absolute Max Props", source)
        self.assertNotIn("funnel_absolute_max", source)

    def test_output_quota_removed(self):
        """_QME_MIN_OUTPUT_BETS should no longer be present — all props analysed."""
        import pathlib
        na_path = pathlib.Path(__file__).parent.parent / "pages" / "3_⚡_Quantum_Analysis_Matrix.py"
        source = na_path.read_text(encoding="utf-8")
        self.assertNotIn("_QME_MIN_OUTPUT_BETS", source)

    def test_all_props_passed_directly(self):
        """Neural Analysis should pass all current_props directly as final_props."""
        import pathlib
        na_path = pathlib.Path(__file__).parent.parent / "pages" / "3_⚡_Quantum_Analysis_Matrix.py"
        source = na_path.read_text(encoding="utf-8")
        self.assertIn("final_props = list(current_props)", source)

    def test_smart_filter_wired_in_runner(self):
        """The analysis runner should call smart_filter_props without stat_type restriction."""
        import pathlib
        na_path = pathlib.Path(__file__).parent.parent / "pages" / "3_⚡_Quantum_Analysis_Matrix.py"
        source = na_path.read_text(encoding="utf-8")
        self.assertIn("smart_filter_props", source)
        self.assertIn("stat_types=None", source)

    def test_no_per_player_cap_in_neural_analysis(self):
        """Neural Analysis should pass max_props_per_player=None (no per-player cap)."""
        import pathlib
        na_path = pathlib.Path(__file__).parent.parent / "pages" / "3_⚡_Quantum_Analysis_Matrix.py"
        source = na_path.read_text(encoding="utf-8")
        self.assertIn("max_props_per_player=None", source)


class TestSmartFilterPropsIntegration(unittest.TestCase):
    """Unit tests for smart_filter_props with funnel parameters."""

    def setUp(self):
        _ensure_streamlit_mock()
        from data.sportsbook_service import smart_filter_props
        self.filter_fn = smart_filter_props

    def test_stat_type_filtering(self):
        """Only props with matching stat types should survive."""
        props = [
            {"player_name": "Player A", "stat_type": "points", "team": "", "line": 20},
            {"player_name": "Player A", "stat_type": "blocks", "team": "", "line": 1.5},
            {"player_name": "Player B", "stat_type": "rebounds", "team": "", "line": 8},
        ]
        filtered, summary = self.filter_fn(
            all_props=props,
            stat_types={"points", "rebounds"},
            max_props_per_player=5,
        )
        stat_types = {p["stat_type"] for p in filtered}
        self.assertNotIn("blocks", stat_types)
        self.assertIn("points", stat_types)
        self.assertIn("rebounds", stat_types)

    def test_per_player_cap(self):
        """Props per player should be capped at max_props_per_player."""
        props = [
            {"player_name": "LeBron", "stat_type": "points", "team": "", "line": 25},
            {"player_name": "LeBron", "stat_type": "rebounds", "team": "", "line": 8},
            {"player_name": "LeBron", "stat_type": "assists", "team": "", "line": 7},
            {"player_name": "LeBron", "stat_type": "threes", "team": "", "line": 2},
        ]
        filtered, summary = self.filter_fn(
            all_props=props,
            max_props_per_player=2,
        )
        lebron_props = [p for p in filtered if p["player_name"] == "LeBron"]
        self.assertLessEqual(len(lebron_props), 2)

    def test_no_per_player_cap_when_none(self):
        """Passing max_props_per_player=None should skip the per-player cap."""
        props = [
            {"player_name": "LeBron", "stat_type": "points", "team": "", "line": 25},
            {"player_name": "LeBron", "stat_type": "rebounds", "team": "", "line": 8},
            {"player_name": "LeBron", "stat_type": "assists", "team": "", "line": 7},
            {"player_name": "LeBron", "stat_type": "threes", "team": "", "line": 2},
            {"player_name": "LeBron", "stat_type": "steals", "team": "", "line": 1},
            {"player_name": "LeBron", "stat_type": "blocks", "team": "", "line": 1},
            {"player_name": "LeBron", "stat_type": "turnovers", "team": "", "line": 3},
        ]
        filtered, summary = self.filter_fn(
            all_props=props,
            max_props_per_player=None,
        )
        lebron_props = [p for p in filtered if p["player_name"] == "LeBron"]
        self.assertEqual(len(lebron_props), 7)

    def test_empty_props(self):
        """Empty prop list should return empty result."""
        filtered, summary = self.filter_fn(all_props=[])
        self.assertEqual(len(filtered), 0)
        self.assertEqual(summary["original_count"], 0)

    def test_summary_has_expected_keys(self):
        """Filter summary should contain all expected step counts."""
        props = [
            {"player_name": "Test", "stat_type": "points", "team": "", "line": 20},
        ]
        _, summary = self.filter_fn(all_props=props)
        for key in ("original_count", "after_team_filter", "after_injury_filter",
                     "after_dedup", "after_stat_filter", "after_per_player_cap",
                     "final_count", "reduction_pct"):
            self.assertIn(key, summary, f"Missing key '{key}' in filter summary")


# ============================================================
# Joseph Analytics Wiring on QAM
# ============================================================

class TestJosephQamWiring(unittest.TestCase):
    """Verify that the Quantum Analysis Matrix page correctly wires
    Joseph's analytics so his independent analysis runs and his
    bets are logged after each analysis cycle."""

    @classmethod
    def setUpClass(cls):
        import pathlib
        na_path = pathlib.Path(__file__).parent.parent / "pages" / "3_⚡_Quantum_Analysis_Matrix.py"
        cls.source = na_path.read_text(encoding="utf-8")

    def test_joseph_results_cleared_before_analysis(self):
        """Stale joseph_results must be cleared when a new analysis run begins
        so that fresh results are generated by the Live Desk."""
        self.assertIn('pop("joseph_results"', self.source,
                      "QAM must clear joseph_results before re-running analysis")

    def test_joseph_bets_logged_reset(self):
        """joseph_bets_logged flag must be reset before re-running analysis."""
        # The flag reset should appear near the analysis start, not just in the
        # Joseph desk section.
        idx_run = self.source.find("if run_analysis:")
        idx_reset = self.source.find('"joseph_bets_logged"] = False')
        self.assertGreater(idx_reset, idx_run,
                           "joseph_bets_logged reset must come after run_analysis check")
        # The reset should appear before the main analysis loop
        idx_loop = self.source.find("for prop_index, prop in enumerate(props_to_analyze):")
        self.assertLess(idx_reset, idx_loop,
                        "joseph_bets_logged reset must come before the analysis loop")

    def test_inline_commentary_uses_joseph_results(self):
        """inject_joseph_inline_commentary must receive joseph_results (with
        verdicts) rather than only raw analysis_results."""
        # Find the actual function call (not the import statement).
        # The import line contains 'import inject_joseph_inline_commentary'
        # while the call has 'inject_joseph_inline_commentary(' with arguments.
        import_context_chars = 80  # chars to check for nearby "import" keyword
        call_site = self.source.find("inject_joseph_inline_commentary(")
        while call_site != -1 and "import" in self.source[max(0, call_site - import_context_chars):call_site]:
            call_site = self.source.find("inject_joseph_inline_commentary(", call_site + 1)
        self.assertNotEqual(call_site, -1, "inject_joseph_inline_commentary call must exist in QAM")
        snippet = self.source[call_site:call_site + 300]
        self.assertIn("joseph_results", snippet,
                      "inject_joseph_inline_commentary should use joseph_results when available")

    def test_live_desk_renders_with_analysis_results(self):
        """render_joseph_live_desk must be called with analysis_results."""
        self.assertIn("render_joseph_live_desk(", self.source)
        idx = self.source.find("render_joseph_live_desk(")
        snippet = self.source[idx:idx + 300]
        self.assertIn("analysis_results=analysis_results", snippet)

    def test_auto_log_bets_called(self):
        """joseph_auto_log_bets must be called with joseph_results."""
        self.assertIn("joseph_auto_log_bets(", self.source)


if __name__ == "__main__":
    unittest.main()
