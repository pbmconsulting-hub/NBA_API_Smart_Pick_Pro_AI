# ============================================================
# FILE: tests/test_phase3_dfs_ui_surface.py
# PURPOSE: Tests for Phase 3 — DFS Metrics UI Surface Layer
#          Validates that DFS breakeven, parlay EV, Kelly sizing,
#          and target-line probability are properly rendered in
#          the prop cards, summary dashboard, and entry builder.
# ============================================================

import math
import sys
import os
import types
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


def _ensure_streamlit_mock():
    """Mock streamlit if not available (for CI/headless environments)."""
    if "streamlit" not in sys.modules:
        mock_st = types.ModuleType("streamlit")
        mock_st.cache_data = lambda *a, **kw: (lambda f: f)
        mock_st.cache_resource = lambda *a, **kw: (lambda f: f)

        class _MockSessionState(dict):
            def __getattr__(self, name):
                return self.get(name)
            def __setattr__(self, name, value):
                self[name] = value

        mock_st.session_state = _MockSessionState()
        sys.modules["streamlit"] = mock_st

    # Ensure streamlit.components.v1 exists (needed by neural_analysis_helpers)
    if "streamlit.components" not in sys.modules:
        mock_components = types.ModuleType("streamlit.components")
        mock_v1 = types.ModuleType("streamlit.components.v1")
        mock_v1.html = lambda *a, **kw: None
        mock_components.v1 = mock_v1
        sys.modules["streamlit.components"] = mock_components
        sys.modules["streamlit.components.v1"] = mock_v1


_ensure_streamlit_mock()


def _make_result_with_dfs(
    player="Test Player",
    stat="points",
    prob=0.62,
    edge=8.0,
    platform="PrizePicks",
    target_line=24.5,
    prob_target=0.62,
):
    """Build a mock analysis result dict with DFS Phase 2 metrics."""
    from engine.odds_engine import calculate_dfs_parlay_ev_from_sim
    parlay_ev = calculate_dfs_parlay_ev_from_sim(prob_target, platform, "OVER")
    breakevens = {}
    from engine.odds_engine import calculate_dfs_breakeven_probability
    for n in (3, 4, 5, 6):
        be = calculate_dfs_breakeven_probability(platform, n)
        breakevens[n] = be.get("breakeven_per_leg", 0.5)
    return {
        "player_name": player,
        "stat_type": stat,
        "line": target_line,
        "platform": platform,
        "direction": "OVER",
        "probability_over": prob,
        "edge_percentage": edge,
        "confidence_score": 75,
        "tier": "Gold",
        "tier_emoji": "🥇",
        "adjusted_projection": 26.0,
        "simulated_mean": 25.5,
        "simulated_std": 5.5,
        "percentile_10": 18.0,
        "percentile_50": 25.5,
        "percentile_90": 33.0,
        "over_odds": -110,
        "under_odds": -110,
        "should_avoid": False,
        "player_is_out": False,
        "forces": {"over_forces": [], "under_forces": []},
        "score_breakdown": {},
        "prop_target_line": target_line,
        "probability_over_target": prob_target,
        "dfs_breakevens": breakevens,
        "dfs_parlay_ev": parlay_ev,
        "dfs_platform": platform,
    }


def _make_result_without_dfs(player="No DFS Player", prob=0.55):
    """Build a mock analysis result dict WITHOUT DFS metrics."""
    return {
        "player_name": player,
        "stat_type": "rebounds",
        "line": 8.5,
        "platform": "",
        "direction": "OVER",
        "probability_over": prob,
        "edge_percentage": 5.0,
        "confidence_score": 60,
        "tier": "Silver",
        "tier_emoji": "🥈",
        "adjusted_projection": 9.0,
        "simulated_mean": 9.1,
        "simulated_std": 3.0,
        "percentile_10": 5.0,
        "percentile_50": 9.0,
        "percentile_90": 13.0,
        "over_odds": -110,
        "under_odds": -110,
        "should_avoid": False,
        "player_is_out": False,
        "forces": {"over_forces": [], "under_forces": []},
        "score_breakdown": {},
    }


# ============================================================
# Test _build_dfs_metrics_html helper
# ============================================================

class TestBuildDfsMetricsHtml(unittest.TestCase):
    """Tests for neural_analysis_helpers._build_dfs_metrics_html."""

    def setUp(self):
        _ensure_streamlit_mock()
        from pages.helpers.neural_analysis_helpers import _build_dfs_metrics_html
        self.build = _build_dfs_metrics_html

    def test_returns_html_with_dfs_data(self):
        result = _make_result_with_dfs()
        html = self.build(result)
        self.assertIsInstance(html, str)
        self.assertGreater(len(html), 0)

    def test_contains_dfs_flex_ev_label(self):
        result = _make_result_with_dfs()
        html = self.build(result)
        self.assertIn("DFS FLEX EV", html)

    def test_contains_tier_pills(self):
        result = _make_result_with_dfs()
        html = self.build(result)
        self.assertIn("3-Pick", html)
        self.assertIn("4-Pick", html)
        self.assertIn("5-Pick", html)
        self.assertIn("6-Pick", html)

    def test_best_tier_highlighted(self):
        result = _make_result_with_dfs()
        html = self.build(result)
        # Best tier gets a star
        self.assertIn("★", html)

    def test_shows_platform(self):
        result = _make_result_with_dfs(platform="Underdog")
        html = self.build(result)
        self.assertIn("Underdog", html)

    def test_shows_target_probability(self):
        result = _make_result_with_dfs(target_line=24.5, prob_target=0.62)
        html = self.build(result)
        self.assertIn("24.5", html)

    def test_shows_kelly_fraction(self):
        result = _make_result_with_dfs(prob_target=0.65)
        html = self.build(result)
        # Should contain Kelly info if best_tier exists
        parlay = result["dfs_parlay_ev"]
        if parlay.get("best_tier") and parlay.get("kelly_fraction", 0) > 0:
            self.assertIn("Kelly", html)

    def test_returns_empty_without_dfs(self):
        result = _make_result_without_dfs()
        html = self.build(result)
        self.assertEqual(html, "")

    def test_handles_empty_tiers(self):
        result = _make_result_with_dfs()
        result["dfs_parlay_ev"]["tiers"] = {}
        html = self.build(result)
        self.assertEqual(html, "")

    def test_no_dfs_parlay_ev_key(self):
        result = _make_result_without_dfs()
        result.pop("dfs_parlay_ev", None)
        html = self.build(result)
        self.assertEqual(html, "")

    def test_edge_values_in_html(self):
        result = _make_result_with_dfs(prob_target=0.62)
        html = self.build(result)
        # Edge values should be formatted as +X.X%
        self.assertIn("%", html)

    def test_breakeven_label(self):
        result = _make_result_with_dfs()
        html = self.build(result)
        self.assertIn("BE", html)


# ============================================================
# Test _build_dfs_metrics_html is wired into prop card
# ============================================================

class TestDfsMetricsInPropCard(unittest.TestCase):
    """Verify _build_dfs_metrics_html is called in display_prop_analysis_card_qds."""

    def test_prop_card_contains_dfs_call(self):
        """The prop card display function should reference _build_dfs_metrics_html."""
        helper_path = os.path.join(
            os.path.dirname(__file__), "..",
            "pages", "helpers", "neural_analysis_helpers.py",
        )
        with open(helper_path, "r") as f:
            content = f.read()
        self.assertIn("_build_dfs_metrics_html", content)

    def test_prop_card_contains_dfs_strip_rendering(self):
        """The display function should render the DFS strip with st.markdown."""
        helper_path = os.path.join(
            os.path.dirname(__file__), "..",
            "pages", "helpers", "neural_analysis_helpers.py",
        )
        with open(helper_path, "r") as f:
            content = f.read()
        self.assertIn("_dfs_strip", content)


# ============================================================
# Test Neural Analysis summary DFS metrics
# ============================================================

class TestNeuralAnalysisSummaryDfs(unittest.TestCase):
    """Verify DFS aggregate metrics are present in Neural Analysis page."""

    def test_summary_has_dfs_flex_edge(self):
        page_path = os.path.join(
            os.path.dirname(__file__), "..",
            "pages", "3_⚡_Quantum_Analysis_Matrix.py",
        )
        with open(page_path, "r") as f:
            content = f.read()
        self.assertIn("DFS FLEX EDGE", content)

    def test_summary_counts_beats_breakeven(self):
        page_path = os.path.join(
            os.path.dirname(__file__), "..",
            "pages", "3_⚡_Quantum_Analysis_Matrix.py",
        )
        with open(page_path, "r") as f:
            content = f.read()
        self.assertIn("legs beat breakeven", content)

    def test_summary_uses_dfs_parlay_ev(self):
        page_path = os.path.join(
            os.path.dirname(__file__), "..",
            "pages", "3_⚡_Quantum_Analysis_Matrix.py",
        )
        with open(page_path, "r") as f:
            content = f.read()
        self.assertIn("dfs_parlay_ev", content)


# ============================================================
# Test Entry Builder DFS breakeven display
# ============================================================

class TestEntryBuilderDfsBreakeven(unittest.TestCase):
    """Verify DFS breakeven thresholds in Entry Builder."""

    def test_entry_builder_has_dfs_breakeven(self):
        eb_path = os.path.join(
            os.path.dirname(__file__), "..",
            "pages", "6_🧬_Entry_Builder.py",
        )
        with open(eb_path, "r") as f:
            content = f.read()
        self.assertIn("DFS", content)
        self.assertIn("Breakeven", content)

    def test_entry_builder_imports_breakeven(self):
        eb_path = os.path.join(
            os.path.dirname(__file__), "..",
            "pages", "6_🧬_Entry_Builder.py",
        )
        with open(eb_path, "r") as f:
            content = f.read()
        self.assertIn("calculate_dfs_breakeven_probability", content)

    def test_entry_builder_shows_avg_leg_vs_breakeven(self):
        eb_path = os.path.join(
            os.path.dirname(__file__), "..",
            "pages", "6_🧬_Entry_Builder.py",
        )
        with open(eb_path, "r") as f:
            content = f.read()
        self.assertIn("Avg leg", content)

    def test_entry_builder_shows_payout_multiplier(self):
        eb_path = os.path.join(
            os.path.dirname(__file__), "..",
            "pages", "6_🧬_Entry_Builder.py",
        )
        with open(eb_path, "r") as f:
            content = f.read()
        self.assertIn("payout", content)


# ============================================================
# Test DFS metrics helper with all platforms
# ============================================================

class TestDfsMetricsAllPlatforms(unittest.TestCase):
    """Verify DFS metrics render for all supported platforms."""

    def setUp(self):
        _ensure_streamlit_mock()
        from pages.helpers.neural_analysis_helpers import _build_dfs_metrics_html
        self.build = _build_dfs_metrics_html

    def test_prizepicks(self):
        result = _make_result_with_dfs(platform="PrizePicks")
        html = self.build(result)
        self.assertIn("PrizePicks", html)
        self.assertGreater(len(html), 50)

    def test_underdog(self):
        result = _make_result_with_dfs(platform="Underdog")
        html = self.build(result)
        self.assertIn("Underdog", html)
        self.assertGreater(len(html), 50)

    def test_draftkings(self):
        result = _make_result_with_dfs(platform="DraftKings")
        html = self.build(result)
        self.assertIn("DraftKings", html)
        self.assertGreater(len(html), 50)


# ============================================================
# Test edge case: low probability (below all breakevens)
# ============================================================

class TestDfsMetricsLowProb(unittest.TestCase):
    """DFS metrics for picks that don't beat any breakeven."""

    def setUp(self):
        _ensure_streamlit_mock()
        from pages.helpers.neural_analysis_helpers import _build_dfs_metrics_html
        self.build = _build_dfs_metrics_html

    def test_low_prob_no_star(self):
        result = _make_result_with_dfs(prob_target=0.40)
        html = self.build(result)
        # best_tier may be None for low prob — should still render pills
        if result["dfs_parlay_ev"].get("best_tier") is None:
            self.assertNotIn("★", html)

    def test_low_prob_still_renders(self):
        result = _make_result_with_dfs(prob_target=0.40)
        html = self.build(result)
        # Still shows tier pills with negative edges
        self.assertIn("3-Pick", html)


# ============================================================
# Test calculate_dfs_breakeven_probability used by Entry Builder
# ============================================================

class TestDfsBreakevenForEntryBuilder(unittest.TestCase):
    """Verify calculate_dfs_breakeven_probability returns usable data."""

    def test_3_pick_breakeven(self):
        from engine.odds_engine import calculate_dfs_breakeven_probability
        be = calculate_dfs_breakeven_probability("PrizePicks", 3)
        self.assertGreater(be["breakeven_per_leg"], 0.4)
        self.assertLess(be["breakeven_per_leg"], 0.8)

    def test_6_pick_breakeven(self):
        from engine.odds_engine import calculate_dfs_breakeven_probability
        be = calculate_dfs_breakeven_probability("PrizePicks", 6)
        self.assertGreater(be["breakeven_per_leg"], 0.4)
        self.assertLess(be["breakeven_per_leg"], 0.8)

    def test_all_hit_payout_increases_with_picks(self):
        from engine.odds_engine import calculate_dfs_breakeven_probability
        p3 = calculate_dfs_breakeven_probability("PrizePicks", 3)
        p6 = calculate_dfs_breakeven_probability("PrizePicks", 6)
        self.assertGreater(p6["all_hit_payout"], p3["all_hit_payout"])


if __name__ == "__main__":
    unittest.main()
