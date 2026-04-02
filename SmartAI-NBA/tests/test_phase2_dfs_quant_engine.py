# ============================================================
# FILE: tests/test_phase2_dfs_quant_engine.py
# PURPOSE: Tests for Phase 2 — Fixed-Payout Quant Engine
#          Validates DFS parlay EV from sim, simulation DFS
#          metric embedding, and prop_target_line integration.
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


_ensure_streamlit_mock()


# ============================================================
# Tests for calculate_dfs_parlay_ev_from_sim
# ============================================================

class TestDfsParlayEvFromSim(unittest.TestCase):
    """Tests for engine/odds_engine.py calculate_dfs_parlay_ev_from_sim()."""

    def test_basic_return_structure(self):
        """Function returns expected dict keys."""
        from engine.odds_engine import calculate_dfs_parlay_ev_from_sim
        result = calculate_dfs_parlay_ev_from_sim(0.60)
        self.assertIn("model_probability", result)
        self.assertIn("platform", result)
        self.assertIn("direction", result)
        self.assertIn("tiers", result)
        self.assertIn("best_tier", result)
        self.assertIn("kelly_fraction", result)

    def test_tiers_contain_3_through_6(self):
        """Result contains breakeven data for 3, 4, 5, 6 pick tiers."""
        from engine.odds_engine import calculate_dfs_parlay_ev_from_sim
        result = calculate_dfs_parlay_ev_from_sim(0.60)
        for n in (3, 4, 5, 6):
            self.assertIn(n, result["tiers"])
            tier = result["tiers"][n]
            self.assertIn("breakeven", tier)
            self.assertIn("beats_breakeven", tier)
            self.assertIn("edge_vs_breakeven", tier)
            self.assertIn("all_hit_payout", tier)

    def test_high_prob_beats_breakeven(self):
        """A 65% per-leg probability should beat at least some breakeven thresholds."""
        from engine.odds_engine import calculate_dfs_parlay_ev_from_sim
        result = calculate_dfs_parlay_ev_from_sim(0.65, "PrizePicks")
        any_beats = any(
            result["tiers"][n]["beats_breakeven"] for n in (3, 4, 5, 6)
        )
        self.assertTrue(any_beats)

    def test_low_prob_below_breakeven(self):
        """A 40% per-leg probability should NOT beat any breakeven threshold."""
        from engine.odds_engine import calculate_dfs_parlay_ev_from_sim
        result = calculate_dfs_parlay_ev_from_sim(0.40, "PrizePicks")
        any_beats = any(
            result["tiers"][n]["beats_breakeven"] for n in (3, 4, 5, 6)
        )
        self.assertFalse(any_beats)
        self.assertIsNone(result["best_tier"])
        self.assertEqual(result["kelly_fraction"], 0.0)

    def test_best_tier_is_valid(self):
        """best_tier should be in {3, 4, 5, 6} when player has edge."""
        from engine.odds_engine import calculate_dfs_parlay_ev_from_sim
        result = calculate_dfs_parlay_ev_from_sim(0.62, "PrizePicks")
        if result["best_tier"] is not None:
            self.assertIn(result["best_tier"], {3, 4, 5, 6})

    def test_kelly_fraction_positive_with_edge(self):
        """Kelly fraction should be positive when beating breakeven."""
        from engine.odds_engine import calculate_dfs_parlay_ev_from_sim
        result = calculate_dfs_parlay_ev_from_sim(0.65, "PrizePicks")
        if result["best_tier"] is not None:
            self.assertGreater(result["kelly_fraction"], 0.0)

    def test_all_platforms(self):
        """Function works for all supported platforms."""
        from engine.odds_engine import calculate_dfs_parlay_ev_from_sim
        for platform in ("PrizePicks", "Underdog", "DraftKings"):
            result = calculate_dfs_parlay_ev_from_sim(0.60, platform)
            self.assertEqual(result["platform"], platform)
            self.assertIn(3, result["tiers"])

    def test_direction_label(self):
        """Direction is preserved in output."""
        from engine.odds_engine import calculate_dfs_parlay_ev_from_sim
        result = calculate_dfs_parlay_ev_from_sim(0.60, direction="UNDER")
        self.assertEqual(result["direction"], "UNDER")

    def test_edge_values_finite(self):
        """All edge values should be finite floats."""
        from engine.odds_engine import calculate_dfs_parlay_ev_from_sim
        result = calculate_dfs_parlay_ev_from_sim(0.60)
        for n in (3, 4, 5, 6):
            self.assertTrue(math.isfinite(result["tiers"][n]["edge_vs_breakeven"]))
            self.assertTrue(math.isfinite(result["tiers"][n]["breakeven"]))

    def test_invalid_probability_clamps(self):
        """Invalid probabilities are clamped to valid range."""
        from engine.odds_engine import calculate_dfs_parlay_ev_from_sim
        # Above 1
        result = calculate_dfs_parlay_ev_from_sim(1.5)
        self.assertLessEqual(result["model_probability"], 1.0)
        # Below 0
        result = calculate_dfs_parlay_ev_from_sim(-0.5)
        self.assertGreaterEqual(result["model_probability"], 0.0)

    def test_non_numeric_falls_back(self):
        """Non-numeric probability falls back to 0.5."""
        from engine.odds_engine import calculate_dfs_parlay_ev_from_sim
        result = calculate_dfs_parlay_ev_from_sim("not_a_number")
        self.assertAlmostEqual(result["model_probability"], 0.5, places=2)


# ============================================================
# Tests for simulation DFS metric embedding
# ============================================================

class TestSimulationDfsMetrics(unittest.TestCase):
    """Tests for engine/simulation.py DFS metric embedding."""

    def setUp(self):
        _ensure_streamlit_mock()
        from engine.simulation import run_quantum_matrix_simulation
        self.run_sim = run_quantum_matrix_simulation

    def test_sim_without_target_line_no_dfs_keys(self):
        """Without prop_target_line, no DFS keys should appear."""
        result = self.run_sim(
            projected_stat_average=25.0,
            stat_standard_deviation=6.0,
            prop_line=24.5,
            number_of_simulations=100,
            blowout_risk_factor=0.15,
            pace_adjustment_factor=1.0,
            matchup_adjustment_factor=1.0,
            home_away_adjustment=0.0,
            rest_adjustment_factor=1.0,
        )
        self.assertNotIn("dfs_breakevens", result)
        self.assertNotIn("dfs_parlay_ev", result)
        self.assertNotIn("prop_target_line", result)

    def test_sim_with_target_line_has_dfs_keys(self):
        """With prop_target_line, DFS keys should appear."""
        result = self.run_sim(
            projected_stat_average=25.0,
            stat_standard_deviation=6.0,
            prop_line=24.5,
            number_of_simulations=100,
            blowout_risk_factor=0.15,
            pace_adjustment_factor=1.0,
            matchup_adjustment_factor=1.0,
            home_away_adjustment=0.0,
            rest_adjustment_factor=1.0,
            prop_target_line=24.5,
            platform="PrizePicks",
        )
        self.assertIn("prop_target_line", result)
        self.assertAlmostEqual(result["prop_target_line"], 24.5, places=1)
        self.assertIn("probability_over_target", result)
        self.assertIn("dfs_breakevens", result)
        self.assertIn("dfs_parlay_ev", result)
        self.assertIn("dfs_platform", result)

    def test_dfs_breakevens_has_all_tiers(self):
        """DFS breakeven dict should have 3, 4, 5, 6 pick tiers."""
        result = self.run_sim(
            projected_stat_average=25.0,
            stat_standard_deviation=6.0,
            prop_line=24.5,
            number_of_simulations=100,
            blowout_risk_factor=0.15,
            pace_adjustment_factor=1.0,
            matchup_adjustment_factor=1.0,
            home_away_adjustment=0.0,
            rest_adjustment_factor=1.0,
            prop_target_line=24.5,
            platform="PrizePicks",
        )
        for n in (3, 4, 5, 6):
            self.assertIn(n, result["dfs_breakevens"])
            self.assertGreater(result["dfs_breakevens"][n], 0.0)
            self.assertLess(result["dfs_breakevens"][n], 1.0)

    def test_probability_over_target_valid_range(self):
        """probability_over_target should be between 0.01 and 0.99."""
        result = self.run_sim(
            projected_stat_average=25.0,
            stat_standard_deviation=6.0,
            prop_line=24.5,
            number_of_simulations=200,
            blowout_risk_factor=0.15,
            pace_adjustment_factor=1.0,
            matchup_adjustment_factor=1.0,
            home_away_adjustment=0.0,
            rest_adjustment_factor=1.0,
            prop_target_line=24.5,
            platform="PrizePicks",
        )
        prob = result["probability_over_target"]
        self.assertGreaterEqual(prob, 0.01)
        self.assertLessEqual(prob, 0.99)

    def test_dfs_parlay_ev_structure(self):
        """dfs_parlay_ev should contain expected keys."""
        result = self.run_sim(
            projected_stat_average=25.0,
            stat_standard_deviation=6.0,
            prop_line=24.5,
            number_of_simulations=100,
            blowout_risk_factor=0.15,
            pace_adjustment_factor=1.0,
            matchup_adjustment_factor=1.0,
            home_away_adjustment=0.0,
            rest_adjustment_factor=1.0,
            prop_target_line=24.5,
            platform="PrizePicks",
        )
        parlay = result.get("dfs_parlay_ev", {})
        self.assertIn("tiers", parlay)
        self.assertIn("best_tier", parlay)
        self.assertIn("kelly_fraction", parlay)

    def test_invalid_target_line_ignored(self):
        """An invalid prop_target_line (e.g., 0 or None-like) should be ignored."""
        result = self.run_sim(
            projected_stat_average=25.0,
            stat_standard_deviation=6.0,
            prop_line=24.5,
            number_of_simulations=100,
            blowout_risk_factor=0.15,
            pace_adjustment_factor=1.0,
            matchup_adjustment_factor=1.0,
            home_away_adjustment=0.0,
            rest_adjustment_factor=1.0,
            prop_target_line=0,
        )
        self.assertNotIn("dfs_breakevens", result)

    def test_negative_target_line_ignored(self):
        """A negative prop_target_line should be ignored."""
        result = self.run_sim(
            projected_stat_average=25.0,
            stat_standard_deviation=6.0,
            prop_line=24.5,
            number_of_simulations=100,
            blowout_risk_factor=0.15,
            pace_adjustment_factor=1.0,
            matchup_adjustment_factor=1.0,
            home_away_adjustment=0.0,
            rest_adjustment_factor=1.0,
            prop_target_line=-5,
        )
        self.assertNotIn("dfs_breakevens", result)

    def test_string_target_line_parsed(self):
        """A string target line should be parsed as float."""
        result = self.run_sim(
            projected_stat_average=25.0,
            stat_standard_deviation=6.0,
            prop_line=24.5,
            number_of_simulations=100,
            blowout_risk_factor=0.15,
            pace_adjustment_factor=1.0,
            matchup_adjustment_factor=1.0,
            home_away_adjustment=0.0,
            rest_adjustment_factor=1.0,
            prop_target_line="24.5",
            platform="PrizePicks",
        )
        self.assertIn("prop_target_line", result)
        self.assertAlmostEqual(result["prop_target_line"], 24.5, places=1)

    def test_all_platforms_in_sim(self):
        """DFS metrics should work for all platform names."""
        for platform in ("PrizePicks", "Underdog", "DraftKings"):
            result = self.run_sim(
                projected_stat_average=25.0,
                stat_standard_deviation=6.0,
                prop_line=24.5,
                number_of_simulations=100,
                blowout_risk_factor=0.15,
                pace_adjustment_factor=1.0,
                matchup_adjustment_factor=1.0,
                home_away_adjustment=0.0,
                rest_adjustment_factor=1.0,
                prop_target_line=24.5,
                platform=platform,
            )
            self.assertEqual(result.get("dfs_platform"), platform)


# ============================================================
# Tests for backward compatibility
# ============================================================

class TestBackwardCompatibility(unittest.TestCase):
    """Ensure existing simulation API is not broken by Phase 2 additions."""

    def setUp(self):
        _ensure_streamlit_mock()
        from engine.simulation import run_quantum_matrix_simulation
        self.run_sim = run_quantum_matrix_simulation

    def test_original_signature_works(self):
        """Calling with only original params should work."""
        result = self.run_sim(
            projected_stat_average=25.0,
            stat_standard_deviation=6.0,
            prop_line=24.5,
            number_of_simulations=100,
            blowout_risk_factor=0.15,
            pace_adjustment_factor=1.0,
            matchup_adjustment_factor=1.0,
            home_away_adjustment=0.0,
            rest_adjustment_factor=1.0,
        )
        self.assertIn("probability_over", result)
        self.assertIn("simulated_mean", result)
        self.assertIn("simulated_std", result)
        self.assertIn("simulations_run", result)

    def test_probability_over_unchanged_without_target(self):
        """probability_over should be a valid probability without target line."""
        result = self.run_sim(
            projected_stat_average=25.0,
            stat_standard_deviation=6.0,
            prop_line=24.5,
            number_of_simulations=500,
            blowout_risk_factor=0.15,
            pace_adjustment_factor=1.0,
            matchup_adjustment_factor=1.0,
            home_away_adjustment=0.0,
            rest_adjustment_factor=1.0,
            random_seed=42,
        )
        # Valid probability between 0 and 1
        self.assertGreaterEqual(result["probability_over"], 0.0)
        self.assertLessEqual(result["probability_over"], 1.0)

    def test_percentile_keys_still_present(self):
        """All percentile keys should still be in the result."""
        result = self.run_sim(
            projected_stat_average=25.0,
            stat_standard_deviation=6.0,
            prop_line=24.5,
            number_of_simulations=100,
            blowout_risk_factor=0.15,
            pace_adjustment_factor=1.0,
            matchup_adjustment_factor=1.0,
            home_away_adjustment=0.0,
            rest_adjustment_factor=1.0,
        )
        for key in ("percentile_10", "percentile_25", "percentile_50",
                     "percentile_75", "percentile_90"):
            self.assertIn(key, result)

    def test_calculate_fractional_kelly_still_works(self):
        """Existing Kelly function should not be affected."""
        from engine.odds_engine import calculate_fractional_kelly
        result = calculate_fractional_kelly(0.60, -110, 0.25)
        self.assertIn("kelly_fraction", result)
        self.assertIn("fractional_kelly", result)
        self.assertGreater(result["fractional_kelly"], 0)


# ============================================================
# Integration Tests
# ============================================================

class TestIntegration(unittest.TestCase):
    """End-to-end tests combining quarantine, sim, and DFS EV."""

    def test_quarantine_to_sim_pipeline(self):
        """Quarantined prop_target_line flows into simulation correctly."""
        _ensure_streamlit_mock()
        from data.sportsbook_service import quarantine_props
        from engine.simulation import run_quantum_matrix_simulation

        # Create a prop set with one good line
        props = [{
            "player_name": "Test Player",
            "stat_type": "points",
            "line": 24.5,
            "over_odds": -110,
            "under_odds": -110,
            "platform": "PrizePicks",
        }]
        quarantined, _ = quarantine_props(props)
        self.assertEqual(len(quarantined), 1)
        target = quarantined[0]["prop_target_line"]
        self.assertEqual(target, 24.5)

        # Run simulation with the quarantined target line
        result = run_quantum_matrix_simulation(
            projected_stat_average=25.0,
            stat_standard_deviation=6.0,
            prop_line=24.5,
            number_of_simulations=200,
            blowout_risk_factor=0.15,
            pace_adjustment_factor=1.0,
            matchup_adjustment_factor=1.0,
            home_away_adjustment=0.0,
            rest_adjustment_factor=1.0,
            prop_target_line=target,
            platform="PrizePicks",
        )
        self.assertIn("prop_target_line", result)
        self.assertIn("dfs_parlay_ev", result)
        self.assertIn("dfs_breakevens", result)


if __name__ == "__main__":
    unittest.main()
