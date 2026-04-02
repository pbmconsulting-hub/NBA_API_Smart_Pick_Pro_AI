# ============================================================
# FILE: tests/test_phase4_dfs_entry_builder.py
# PURPOSE: Tests for Phase 4 — DFS-Aware Entry Builder
#          Validates that generate_optimal_slip() embeds per-leg
#          DFS breakeven edges and slip-level DFS aggregates,
#          and that the Entry Builder UI surfaces these metrics.
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

    if "streamlit.components" not in sys.modules:
        mock_components = types.ModuleType("streamlit.components")
        mock_v1 = types.ModuleType("streamlit.components.v1")
        mock_v1.html = lambda *a, **kw: None
        mock_components.v1 = mock_v1
        sys.modules["streamlit.components"] = mock_components
        sys.modules["streamlit.components.v1"] = mock_v1


_ensure_streamlit_mock()

from engine.odds_engine import (
    generate_optimal_slip,
    calculate_dfs_parlay_ev_from_sim,
)


# ── Helper to build props with DFS Phase 2 metrics ──────────────────

def _make_props_with_dfs(n, platform="PrizePicks"):
    """Generate n mock props with per-leg DFS parlay EV from Phase 2."""
    props = []
    for i in range(n):
        prob = 0.58 + i * 0.02
        parlay_ev = calculate_dfs_parlay_ev_from_sim(prob, platform, "OVER")
        props.append({
            "player_name": f"DFSPlayer_{i}",
            "stat_type": "points",
            "probability_over": prob,
            "direction": "OVER",
            "player_team": f"TEAM{i % 5}",
            "opponent": f"OPP{(i + 1) % 5}",
            "edge_percentage": 5.0 + i * 0.5,
            "confidence_score": 60 + i,
            "dfs_parlay_ev": parlay_ev,
            "dfs_platform": platform,
        })
    return props


def _make_props_without_dfs(n):
    """Generate n mock props WITHOUT DFS Phase 2 metrics."""
    props = []
    for i in range(n):
        props.append({
            "player_name": f"NoDFS_{i}",
            "stat_type": "rebounds",
            "probability_over": 0.55 + i * 0.02,
            "direction": "OVER",
            "player_team": f"TEAM{i % 5}",
            "opponent": f"OPP{(i + 1) % 5}",
            "edge_percentage": 4.0 + i,
            "confidence_score": 55 + i,
        })
    return props


# ============================================================
# Test: generate_optimal_slip returns new DFS keys
# ============================================================

class TestSlipDfsKeys(unittest.TestCase):
    """generate_optimal_slip() must return dfs_leg_edges, dfs_legs_beat_breakeven, dfs_avg_edge."""

    def test_slip_contains_dfs_keys(self):
        props = _make_props_with_dfs(4)
        slips = generate_optimal_slip(props, platform="PrizePicks")
        self.assertGreater(len(slips), 0)
        slip = slips[0]
        for key in ("dfs_leg_edges", "dfs_legs_beat_breakeven", "dfs_avg_edge"):
            self.assertIn(key, slip, f"Missing key: {key}")

    def test_dfs_keys_present_without_dfs_data(self):
        """Even without DFS data, the keys should exist (with defaults)."""
        props = _make_props_without_dfs(4)
        slips = generate_optimal_slip(props, platform="PrizePicks")
        self.assertGreater(len(slips), 0)
        slip = slips[0]
        self.assertIn("dfs_leg_edges", slip)
        self.assertIn("dfs_legs_beat_breakeven", slip)
        self.assertIn("dfs_avg_edge", slip)

    def test_dfs_leg_edges_length_matches_slip_size(self):
        props = _make_props_with_dfs(5)
        slips = generate_optimal_slip(props, platform="PrizePicks")
        for slip in slips:
            self.assertEqual(len(slip["dfs_leg_edges"]), slip["slip_size"])

    def test_dfs_leg_edges_all_none_without_data(self):
        props = _make_props_without_dfs(4)
        slips = generate_optimal_slip(props, platform="PrizePicks")
        for slip in slips:
            for edge in slip["dfs_leg_edges"]:
                self.assertIsNone(edge)

    def test_dfs_legs_beat_breakeven_zero_without_data(self):
        props = _make_props_without_dfs(4)
        slips = generate_optimal_slip(props, platform="PrizePicks")
        for slip in slips:
            self.assertEqual(slip["dfs_legs_beat_breakeven"], 0)

    def test_dfs_avg_edge_zero_without_data(self):
        props = _make_props_without_dfs(4)
        slips = generate_optimal_slip(props, platform="PrizePicks")
        for slip in slips:
            self.assertEqual(slip["dfs_avg_edge"], 0.0)


# ============================================================
# Test: DFS per-leg edge data is accurate
# ============================================================

class TestSlipDfsLegEdge(unittest.TestCase):
    """Per-leg DFS edge in slips should reflect the Phase 2 data on each pick."""

    def test_leg_edge_has_correct_keys(self):
        props = _make_props_with_dfs(3)
        slips = generate_optimal_slip(props, platform="PrizePicks")
        for slip in slips:
            for edge in slip["dfs_leg_edges"]:
                if edge is not None:
                    for k in ("beats_breakeven", "edge_vs_breakeven", "breakeven", "probability"):
                        self.assertIn(k, edge, f"Missing key: {k}")

    def test_leg_edge_values_are_finite(self):
        props = _make_props_with_dfs(5)
        slips = generate_optimal_slip(props, platform="PrizePicks")
        for slip in slips:
            for edge in slip["dfs_leg_edges"]:
                if edge is not None:
                    self.assertTrue(math.isfinite(edge["edge_vs_breakeven"]))
                    self.assertTrue(math.isfinite(edge["breakeven"]))
                    self.assertTrue(math.isfinite(edge["probability"]))

    def test_beats_breakeven_is_bool(self):
        props = _make_props_with_dfs(4)
        slips = generate_optimal_slip(props, platform="PrizePicks")
        for slip in slips:
            for edge in slip["dfs_leg_edges"]:
                if edge is not None:
                    self.assertIsInstance(edge["beats_breakeven"], bool)

    def test_high_prob_legs_beat_breakeven(self):
        """A player with 0.72 probability should beat DFS breakeven for any tier."""
        props = _make_props_with_dfs(3, platform="PrizePicks")
        # Override first player to have very high prob
        props[0]["probability_over"] = 0.72
        props[0]["dfs_parlay_ev"] = calculate_dfs_parlay_ev_from_sim(0.72, "PrizePicks", "OVER")
        slips = generate_optimal_slip(props, platform="PrizePicks")
        # Find a slip that includes Player_0
        found = False
        for slip in slips:
            for i, pk in enumerate(slip["picks"]):
                if pk["player_name"] == "DFSPlayer_0":
                    edge = slip["dfs_leg_edges"][i]
                    if edge is not None:
                        self.assertTrue(edge["beats_breakeven"],
                                        "0.72 prob should beat breakeven")
                        found = True
        self.assertTrue(found, "Should find DFSPlayer_0 in at least one slip")


# ============================================================
# Test: Slip-level DFS aggregates
# ============================================================

class TestSlipDfsAggregate(unittest.TestCase):
    """Slip-level dfs_legs_beat_breakeven and dfs_avg_edge must be correct."""

    def test_legs_beat_breakeven_count(self):
        props = _make_props_with_dfs(5)
        slips = generate_optimal_slip(props, platform="PrizePicks")
        for slip in slips:
            valid_edges = [e for e in slip["dfs_leg_edges"] if e is not None]
            expected = sum(1 for e in valid_edges if e["beats_breakeven"])
            self.assertEqual(slip["dfs_legs_beat_breakeven"], expected)

    def test_avg_edge_is_mean_of_leg_edges(self):
        props = _make_props_with_dfs(5)
        slips = generate_optimal_slip(props, platform="PrizePicks")
        for slip in slips:
            valid_edges = [e for e in slip["dfs_leg_edges"] if e is not None]
            if valid_edges:
                expected_avg = sum(e["edge_vs_breakeven"] for e in valid_edges) / len(valid_edges)
                self.assertAlmostEqual(slip["dfs_avg_edge"], expected_avg, places=5)

    def test_dfs_avg_edge_is_finite(self):
        props = _make_props_with_dfs(6)
        slips = generate_optimal_slip(props, platform="PrizePicks")
        for slip in slips:
            self.assertTrue(math.isfinite(slip["dfs_avg_edge"]))


# ============================================================
# Test: All platforms produce DFS data in slips
# ============================================================

class TestSlipDfsPlatforms(unittest.TestCase):
    """All three platforms should produce DFS edge data."""

    def test_prizepicks(self):
        props = _make_props_with_dfs(4, platform="PrizePicks")
        slips = generate_optimal_slip(props, platform="PrizePicks")
        self.assertGreater(len(slips), 0)
        self.assertIn("dfs_leg_edges", slips[0])

    def test_underdog(self):
        props = _make_props_with_dfs(4, platform="Underdog")
        slips = generate_optimal_slip(props, platform="Underdog")
        self.assertGreater(len(slips), 0)
        self.assertIn("dfs_leg_edges", slips[0])

    def test_draftkings(self):
        props = _make_props_with_dfs(4, platform="DraftKings")
        slips = generate_optimal_slip(props, platform="DraftKings")
        self.assertGreater(len(slips), 0)
        self.assertIn("dfs_leg_edges", slips[0])


# ============================================================
# Test: Backward compatibility — existing keys unchanged
# ============================================================

class TestSlipBackwardCompat(unittest.TestCase):
    """Existing slip keys must remain intact and unchanged."""

    def test_original_keys_still_present(self):
        props = _make_props_with_dfs(4)
        slips = generate_optimal_slip(props, platform="PrizePicks")
        for slip in slips:
            for key in ("slip_size", "picks", "cumulative_ev",
                        "combined_probability", "correlation_penalty", "fair_odds"):
                self.assertIn(key, slip)

    def test_sorting_still_by_ev(self):
        props = _make_props_with_dfs(6)
        slips = generate_optimal_slip(props, platform="PrizePicks")
        evs = [s["cumulative_ev"] for s in slips]
        self.assertEqual(evs, sorted(evs, reverse=True))

    def test_max_10_results(self):
        props = _make_props_with_dfs(10)
        slips = generate_optimal_slip(props, platform="PrizePicks")
        self.assertLessEqual(len(slips), 10)


# ============================================================
# Test: Entry Builder UI contains DFS badge markup
# ============================================================

class TestEntryBuilderDfsBadges(unittest.TestCase):
    """Entry Builder page source must contain Phase 4 DFS badge markup."""

    def setUp(self):
        self._eb_path = os.path.join(
            os.path.dirname(__file__), "..",
            "pages", "6_🧬_Entry_Builder.py",
        )
        with open(self._eb_path, "r") as f:
            self._content = f.read()

    def test_dfs_badge_markup_present(self):
        """Per-leg DFS breakeven badge should be in the ticket."""
        self.assertIn("BE+", self._content)
        self.assertIn("beats_breakeven", self._content)

    def test_dfs_legs_breakeven_summary(self):
        """Slip-level breakeven summary must be rendered."""
        self.assertIn("LEGS vs", self._content)
        self.assertIn("PICK BREAKEVEN", self._content)

    def test_dfs_edge_in_summary_bar(self):
        """DFS Edge metric should appear in the summary statistics bar."""
        self.assertIn("DFS Edge", self._content)

    def test_dfs_avg_edge_computed(self):
        """avg_dfs_edge should be computed from slip data."""
        self.assertIn("_avg_dfs_edge", self._content)


# ============================================================
# Test: Mixed DFS and non-DFS props
# ============================================================

class TestMixedDfsNonDfsProps(unittest.TestCase):
    """Slips with a mix of DFS and non-DFS props should gracefully handle Nones."""

    def test_mixed_props_produce_slips(self):
        dfs_props = _make_props_with_dfs(2)
        plain_props = _make_props_without_dfs(2)
        # Rename plain props to avoid player name collision
        for i, p in enumerate(plain_props):
            p["player_name"] = f"PlainPlayer_{i}"
        mixed = dfs_props + plain_props
        slips = generate_optimal_slip(mixed, platform="PrizePicks")
        self.assertGreater(len(slips), 0)

    def test_mixed_props_leg_edges_partial_none(self):
        dfs_props = _make_props_with_dfs(2)
        plain_props = _make_props_without_dfs(2)
        for i, p in enumerate(plain_props):
            p["player_name"] = f"PlainPlayer_{i}"
        mixed = dfs_props + plain_props
        slips = generate_optimal_slip(mixed, platform="PrizePicks")
        # Across all slips, we expect to see both None edges (plain props)
        # and dict edges (DFS props), since the combinator pairs them
        has_none = False
        has_dict = False
        for slip in slips:
            for edge in slip["dfs_leg_edges"]:
                if edge is None:
                    has_none = True
                else:
                    has_dict = True
        # With 2 DFS + 2 non-DFS, combinator creates mixed slips
        self.assertTrue(has_none and has_dict,
                        "Mixed props should produce both None and dict edge entries")


if __name__ == "__main__":
    unittest.main()
