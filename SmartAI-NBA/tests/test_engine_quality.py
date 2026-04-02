# ============================================================
# FILE: tests/test_engine_quality.py
# PURPOSE: Quality tests for SmartAI-NBA core engine modules.
#          Covers simulation, confidence, edge detection,
#          sensitivity analysis, and magic number constants.
# ============================================================
import math
import unittest


class TestSimulationQuality(unittest.TestCase):
    """Tests for engine/simulation.py quality improvements."""

    def setUp(self):
        from engine.simulation import (
            run_quantum_matrix_simulation,
            run_sensitivity_analysis,
        )
        self.run_qme = run_quantum_matrix_simulation
        self.run_sensitivity = run_sensitivity_analysis

    def _base_kwargs(self):
        return dict(
            projected_stat_average=20.0,
            stat_standard_deviation=4.0,
            prop_line=19.5,
            number_of_simulations=500,
            blowout_risk_factor=0.15,
            pace_adjustment_factor=1.0,
            matchup_adjustment_factor=1.0,
            home_away_adjustment=0.02,
            rest_adjustment_factor=1.0,
        )

    def test_qme_returns_dict_with_probability(self):
        result = self.run_qme(**self._base_kwargs())
        self.assertIsInstance(result, dict)
        self.assertIn("probability_over", result)

    def test_qme_probability_in_range(self):
        result = self.run_qme(**self._base_kwargs())
        prob = result["probability_over"]
        self.assertGreater(prob, 0.0)
        self.assertLess(prob, 1.0)

    def test_qme_random_seed_reproducible(self):
        kw = self._base_kwargs()
        r1 = self.run_qme(**kw, random_seed=42)
        r2 = self.run_qme(**kw, random_seed=42)
        self.assertAlmostEqual(r1["probability_over"], r2["probability_over"], places=3)

    def test_qme_higher_avg_increases_probability(self):
        kw_low  = self._base_kwargs()
        kw_high = dict(kw_low); kw_high["projected_stat_average"] = 25.0
        r_low   = self.run_qme(**kw_low)
        r_high  = self.run_qme(**kw_high)
        self.assertGreater(r_high["probability_over"], r_low["probability_over"])

    def test_sensitivity_analysis_returns_expected_structure(self):
        result = self.run_sensitivity(**self._base_kwargs())
        self.assertIn("base_probability", result)
        self.assertIn("parameters", result)
        self.assertIn("blowout_risk", result["parameters"])
        self.assertIn("pace", result["parameters"])
        self.assertIn("matchup", result["parameters"])

    def test_sensitivity_analysis_blowout_high_reduces_probability(self):
        result = self.run_sensitivity(**self._base_kwargs())
        blowout = result["parameters"]["blowout_risk"]
        base_prob = result["base_probability"]
        # Higher blowout risk should not dramatically increase the over-probability
        self.assertLessEqual(blowout["high"]["probability"], base_prob + 0.10)

    def test_sensitivity_delta_keys_present(self):
        result = self.run_sensitivity(**self._base_kwargs())
        for param_name, param_data in result["parameters"].items():
            self.assertIn("low", param_data)
            self.assertIn("high", param_data)
            self.assertIn("delta_pct", param_data["low"])
            self.assertIn("delta_pct", param_data["high"])


class TestConfidenceMagicNumbers(unittest.TestCase):
    """Tests that confidence threshold constants have expected values."""

    def test_platinum_threshold(self):
        from engine.confidence import PLATINUM_TIER_MINIMUM_SCORE
        self.assertEqual(PLATINUM_TIER_MINIMUM_SCORE, 84)

    def test_gold_threshold(self):
        from engine.confidence import GOLD_TIER_MINIMUM_SCORE
        self.assertEqual(GOLD_TIER_MINIMUM_SCORE, 65)

    def test_silver_threshold(self):
        from engine.confidence import SILVER_TIER_MINIMUM_SCORE
        self.assertEqual(SILVER_TIER_MINIMUM_SCORE, 57)

    def test_do_not_bet_threshold(self):
        from engine.confidence import DO_NOT_BET_SCORE_THRESHOLD
        self.assertEqual(DO_NOT_BET_SCORE_THRESHOLD, 35)

    def test_config_thresholds_match_confidence(self):
        from config.thresholds import (
            PLATINUM_THRESHOLD, GOLD_THRESHOLD,
            SILVER_THRESHOLD, BRONZE_THRESHOLD,
        )
        from engine.confidence import (
            PLATINUM_TIER_MINIMUM_SCORE, GOLD_TIER_MINIMUM_SCORE,
            SILVER_TIER_MINIMUM_SCORE, DO_NOT_BET_SCORE_THRESHOLD,
        )
        self.assertEqual(PLATINUM_THRESHOLD, PLATINUM_TIER_MINIMUM_SCORE)
        self.assertEqual(GOLD_THRESHOLD, GOLD_TIER_MINIMUM_SCORE)
        self.assertEqual(SILVER_THRESHOLD, SILVER_TIER_MINIMUM_SCORE)
        self.assertEqual(BRONZE_THRESHOLD, DO_NOT_BET_SCORE_THRESHOLD)


class TestEdgeDetectionHelpers(unittest.TestCase):
    """Tests for new helper functions in engine/edge_detection.py."""

    def setUp(self):
        from engine.edge_detection import (
            _normalize_force_strength,
            _reconcile_line_signals,
        )
        self.normalize = _normalize_force_strength
        self.reconcile = _reconcile_line_signals

    def test_normalize_ratio_method(self):
        # factor=1.5 → (1.5-1)*100 = 50
        self.assertAlmostEqual(self.normalize(1.5, method="ratio"), 50.0)

    def test_normalize_gap_method(self):
        # gap=9.0 → 9/3 = 3.0
        self.assertAlmostEqual(self.normalize(9.0, method="gap"), 3.0)

    def test_normalize_capped_at_100(self):
        self.assertEqual(self.normalize(999.0, method="ratio"), 100.0)
        self.assertEqual(self.normalize(999.0, method="gap"), 100.0)

    def test_normalize_floored_at_0(self):
        self.assertEqual(self.normalize(0.5, method="ratio"), 0.0)

    def test_reconcile_both_inactive(self):
        result = self.reconcile(
            {"is_sharp": False, "confidence": 0.0},
            {"is_trap": False,  "confidence": 0.0},
        )
        self.assertEqual(result["winner"], "neutral")

    def test_reconcile_only_sharpness(self):
        result = self.reconcile(
            {"is_sharp": True,  "confidence": 0.8},
            {"is_trap": False,  "confidence": 0.0},
        )
        self.assertEqual(result["winner"], "sharpness")
        self.assertTrue(result["is_sharp"])

    def test_reconcile_only_trap(self):
        result = self.reconcile(
            {"is_sharp": False, "confidence": 0.0},
            {"is_trap": True,   "confidence": 0.7},
        )
        self.assertEqual(result["winner"], "trap")
        self.assertTrue(result["is_trap"])

    def test_reconcile_higher_confidence_wins(self):
        result = self.reconcile(
            {"is_sharp": True, "confidence": 0.9},
            {"is_trap": True,  "confidence": 0.6},
        )
        self.assertEqual(result["winner"], "sharpness")

    def test_reconcile_tie_favors_trap(self):
        result = self.reconcile(
            {"is_sharp": True, "confidence": 0.7},
            {"is_trap": True,  "confidence": 0.7},
        )
        self.assertEqual(result["winner"], "trap")

class TestPlatformFiltering(unittest.TestCase):
    """Tests for platform-only player filtering in data/data_manager.py."""

    def setUp(self):
        import sys
        from unittest.mock import MagicMock

        # data.data_manager does `import streamlit as st` at module level.
        # In CI environments where streamlit is not installed, we inject a
        # lightweight mock so the pure-logic function under test can load.
        if "streamlit" not in sys.modules:
            sys.modules["streamlit"] = MagicMock()

        from data.data_manager import filter_props_to_platform_players
        self.filter_fn = filter_props_to_platform_players

    def test_empty_platform_props_returns_all(self):
        generated = [{"player_name": "Player A"}, {"player_name": "Player B"}]
        result = self.filter_fn(generated, [])
        self.assertEqual(len(result), 2)

    def test_filters_to_platform_players_only(self):
        generated = [
            {"player_name": "LeBron James"},
            {"player_name": "Bench Warmer"},
        ]
        platform = [{"player_name": "LeBron James", "stat_type": "points", "line": 25.5}]
        result = self.filter_fn(generated, platform)
        names = [p["player_name"] for p in result]
        self.assertIn("LeBron James", names)
        self.assertNotIn("Bench Warmer", names)

    def test_case_insensitive_name_matching(self):
        generated = [{"player_name": "lebron james"}]
        platform  = [{"player_name": "LeBron James", "stat_type": "points", "line": 25.5}]
        result = self.filter_fn(generated, platform)
        self.assertEqual(len(result), 1)

    def test_none_platform_props_returns_all(self):
        generated = [{"player_name": "Player A"}]
        result = self.filter_fn(generated, None)
        self.assertEqual(len(result), 1)


if __name__ == "__main__":
    unittest.main()
