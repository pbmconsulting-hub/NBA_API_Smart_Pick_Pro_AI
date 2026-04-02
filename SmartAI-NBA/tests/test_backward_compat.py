# ============================================================
# FILE: tests/test_backward_compat.py
# PURPOSE: Ensure backward compatibility for deprecated
#          functions and old caller signatures.
# ============================================================
import unittest
import warnings


class TestDeprecatedAlias(unittest.TestCase):
    """Ensure run_monte_carlo_simulation still works (deprecation wrapper)."""

    def setUp(self):
        from engine.simulation import (
            run_monte_carlo_simulation,
            run_quantum_matrix_simulation,
        )
        self.old_fn = run_monte_carlo_simulation
        self.new_fn = run_quantum_matrix_simulation

    def _base_kwargs(self):
        return dict(
            projected_stat_average=20.0,
            stat_standard_deviation=4.0,
            prop_line=19.5,
            number_of_simulations=300,
            blowout_risk_factor=0.15,
            pace_adjustment_factor=1.0,
            matchup_adjustment_factor=1.0,
            home_away_adjustment=0.0,
            rest_adjustment_factor=1.0,
        )

    def test_old_function_still_callable(self):
        """run_monte_carlo_simulation must not raise an exception."""
        result = self.old_fn(**self._base_kwargs())
        self.assertIsInstance(result, dict)

    def test_old_function_returns_same_keys(self):
        """Old function result must have same keys as new function result."""
        old_result = self.old_fn(**self._base_kwargs(), random_seed=99)
        new_result = self.new_fn(**self._base_kwargs(), random_seed=99)
        self.assertEqual(set(old_result.keys()), set(new_result.keys()))

    def test_old_function_emits_deprecation_log(self):
        """run_monte_carlo_simulation should log a deprecation warning."""
        import logging
        with self.assertLogs("engine.simulation", level="WARNING") as log_ctx:
            self.old_fn(**self._base_kwargs())
        self.assertTrue(
            any("deprecated" in msg.lower() for msg in log_ctx.output),
            "Expected a deprecation warning in the log output.",
        )

    def test_old_function_result_probability_in_range(self):
        result = self.old_fn(**self._base_kwargs())
        prob = result.get("probability_over", -1)
        self.assertGreater(prob, 0.0)
        self.assertLess(prob, 1.0)

    def test_new_function_imported_from_old_callers(self):
        """Verify that pages import run_quantum_matrix_simulation after update."""
        # This just checks the import is available
        from engine.simulation import run_quantum_matrix_simulation
        self.assertTrue(callable(run_quantum_matrix_simulation))


class TestThresholdsBackwardCompat(unittest.TestCase):
    """Confidence thresholds must still work from their original location."""

    def test_confidence_module_still_exports_thresholds(self):
        from engine.confidence import (
            PLATINUM_TIER_MINIMUM_SCORE,
            GOLD_TIER_MINIMUM_SCORE,
            SILVER_TIER_MINIMUM_SCORE,
            DO_NOT_BET_SCORE_THRESHOLD,
        )
        self.assertEqual(PLATINUM_TIER_MINIMUM_SCORE, 84)
        self.assertEqual(GOLD_TIER_MINIMUM_SCORE, 65)
        self.assertEqual(SILVER_TIER_MINIMUM_SCORE, 57)
        self.assertEqual(DO_NOT_BET_SCORE_THRESHOLD, 35)

    def test_config_thresholds_importable(self):
        from config.thresholds import (
            PLATINUM_THRESHOLD, GOLD_THRESHOLD,
            SILVER_THRESHOLD, BRONZE_THRESHOLD,
        )
        self.assertEqual(PLATINUM_THRESHOLD, 84)
        self.assertEqual(GOLD_THRESHOLD, 65)
        self.assertEqual(SILVER_THRESHOLD, 57)
        self.assertEqual(BRONZE_THRESHOLD, 35)

    def test_uncertain_thresholds_importable(self):
        """UNCERTAIN_* threshold names must be importable from config.thresholds."""
        from config.thresholds import (
            UNCERTAIN_CONFLICT_RATIO_THRESHOLD,
            UNCERTAIN_HIGH_VAR_MAX_EDGE,
            UNCERTAIN_BLOWOUT_SPREAD_THRESHOLD,
            UNCERTAIN_HOT_STREAK_RATIO,
        )
        self.assertGreater(UNCERTAIN_CONFLICT_RATIO_THRESHOLD, 0)
        self.assertGreater(UNCERTAIN_HIGH_VAR_MAX_EDGE, 0)
        self.assertGreater(UNCERTAIN_BLOWOUT_SPREAD_THRESHOLD, 0)
        self.assertGreater(UNCERTAIN_HOT_STREAK_RATIO, 0)

    def test_uncertain_thresholds_from_edge_detection(self):
        """UNCERTAIN_* names must be importable from engine.edge_detection."""
        from engine.edge_detection import (
            UNCERTAIN_CONFLICT_RATIO_THRESHOLD,
            UNCERTAIN_HIGH_VAR_MAX_EDGE,
            UNCERTAIN_HIGH_VAR_STATS,
            UNCERTAIN_BLOWOUT_SPREAD_THRESHOLD,
            UNCERTAIN_HOT_STREAK_RATIO,
        )
        self.assertGreater(UNCERTAIN_CONFLICT_RATIO_THRESHOLD, 0)
        self.assertIsInstance(UNCERTAIN_HIGH_VAR_STATS, set)

    def test_goblin_demon_logo_paths_importable(self):
        """GOBLIN_LOGO_PATH and DEMON_LOGO_PATH must be importable and non-empty."""
        from styles.theme import GOBLIN_LOGO_PATH, DEMON_LOGO_PATH
        self.assertTrue(bool(GOBLIN_LOGO_PATH), "GOBLIN_LOGO_PATH must not be empty")
        self.assertTrue(bool(DEMON_LOGO_PATH),  "DEMON_LOGO_PATH must not be empty")
        self.assertIn("Goblin", GOBLIN_LOGO_PATH)
        self.assertIn("Demon",  DEMON_LOGO_PATH)


class TestEdgeDetectionBackwardCompat(unittest.TestCase):
    """Edge detection classify_bet_type must still work with old signatures."""

    def _base_classify_kwargs(self):
        return dict(
            probability_over=0.72,
            edge_percentage=22.0,
            stat_standard_deviation=3.0,
            projected_stat=22.0,
            prop_line=20.5,
            stat_type="points",
            directional_forces_result={"over_strength": 45.0, "under_strength": 15.0},
            season_average=20.0,
        )

    def test_classify_normal_bet(self):
        from engine.edge_detection import classify_bet_type
        result = classify_bet_type(**self._base_classify_kwargs())
        self.assertIn("bet_type", result)
        self.assertEqual(result["bet_type"], "standard")

    def test_classify_returns_reasons_list(self):
        from engine.edge_detection import classify_bet_type
        result = classify_bet_type(**self._base_classify_kwargs())
        self.assertIn("reasons", result)
        self.assertIsInstance(result["reasons"], list)

    def test_classify_goblin_requires_high_std_devs(self):
        from engine.edge_detection import classify_bet_type
        kw = self._base_classify_kwargs()
        kw["projected_stat"] = 26.0  # 2.5 std devs above line of 20.5
        kw["probability_over"]   = 0.88
        kw["edge_percentage"]    = 35.0
        kw["line_source"]        = "prizepicks"  # real line
        result = classify_bet_type(**kw)
        # Goblin tier removed — always returns "standard"
        self.assertEqual(result["bet_type"], "standard")

    def test_classify_synthetic_line_never_goblin(self):
        from engine.edge_detection import classify_bet_type
        kw = self._base_classify_kwargs()
        kw["projected_stat"] = 26.0
        kw["probability_over"]   = 0.88
        kw["edge_percentage"]    = 35.0
        kw["line_source"]        = "synthetic"  # synthetic line
        result = classify_bet_type(**kw)
        self.assertNotEqual(result["bet_type"], "goblin",
                            "Synthetic lines must never be classified as Goblin")

    def test_classify_estimated_line_never_goblin(self):
        from engine.edge_detection import classify_bet_type
        kw = self._base_classify_kwargs()
        kw["projected_stat"] = 26.0
        kw["probability_over"]   = 0.88
        kw["edge_percentage"]    = 35.0
        kw["line_source"]        = "estimated"
        result = classify_bet_type(**kw)
        self.assertNotEqual(result["bet_type"], "goblin",
                            "Estimated lines must never be classified as Goblin")


class TestSensitivityAnalysisBackwardCompat(unittest.TestCase):
    """Sensitivity analysis must work correctly."""

    def test_sensitivity_callable(self):
        from engine.simulation import run_sensitivity_analysis
        self.assertTrue(callable(run_sensitivity_analysis))

    def test_sensitivity_base_probability_matches_qme(self):
        from engine.simulation import run_sensitivity_analysis, run_quantum_matrix_simulation
        kw = dict(
            projected_stat_average=20.0,
            stat_standard_deviation=4.0,
            prop_line=19.5,
            number_of_simulations=300,
            blowout_risk_factor=0.15,
            pace_adjustment_factor=1.0,
            matchup_adjustment_factor=1.0,
            home_away_adjustment=0.0,
            rest_adjustment_factor=1.0,
        )
        sens_result = run_sensitivity_analysis(**kw)
        qme_result  = run_quantum_matrix_simulation(**kw, random_seed=42)
        # Base probability should be close (both use seed=42 internally)
        self.assertAlmostEqual(
            sens_result["base_probability"],
            qme_result["probability_over"],
            delta=0.05,
        )


class TestBetTrackerHelpersBackwardCompat(unittest.TestCase):
    """Bet tracker helper functions must be importable and callable."""

    def test_classify_uncertain_subtype_importable(self):
        """classify_uncertain_subtype() must be importable."""
        from pages.helpers.bet_tracker_helpers import classify_uncertain_subtype
        self.assertTrue(callable(classify_uncertain_subtype))

    def test_classify_uncertain_subtype_returns_string(self):
        from pages.helpers.bet_tracker_helpers import classify_uncertain_subtype
        result = classify_uncertain_subtype("conflict ratio 85% overlap")
        self.assertIsInstance(result, str)
        self.assertEqual(result, "Conflict")

    def test_get_uncertain_subtype_counts_importable(self):
        """get_uncertain_subtype_counts() must be importable."""
        from pages.helpers.bet_tracker_helpers import get_uncertain_subtype_counts
        self.assertTrue(callable(get_uncertain_subtype_counts))


if __name__ == "__main__":
    unittest.main()
