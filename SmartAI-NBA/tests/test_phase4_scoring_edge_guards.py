# ============================================================
# FILE: tests/test_phase4_scoring_edge_guards.py
# PURPOSE: Phase 4 — Scoring & Edge Engine NaN/inf Guards
#   4A: engine/confidence.py _safe_float + return dict guards
#   4B: engine/edge_detection.py _safe_float + return dict guards
#   4C: engine/bankroll.py _safe_float + return dict guards
#   4D: engine/calibration.py _safe_float + return dict guards
# ============================================================
import math
import unittest


# ============================================================
# 4A: engine/confidence.py
# ============================================================

class TestConfidenceSafeFloat(unittest.TestCase):
    """Verify _safe_float helper exists and behaves correctly."""

    def setUp(self):
        from engine.confidence import _safe_float
        self.sf = _safe_float

    def test_finite_passes(self):
        self.assertEqual(self.sf(42.0), 42.0)

    def test_nan_returns_fallback(self):
        self.assertEqual(self.sf(float("nan"), 0.0), 0.0)

    def test_inf_returns_fallback(self):
        self.assertEqual(self.sf(float("inf"), 1.0), 1.0)

    def test_neg_inf_returns_fallback(self):
        self.assertEqual(self.sf(float("-inf"), -1.0), -1.0)

    def test_none_returns_fallback(self):
        self.assertEqual(self.sf(None, 5.0), 5.0)

    def test_string_returns_fallback(self):
        self.assertEqual(self.sf("abc", 0.0), 0.0)

    def test_zero_passes(self):
        self.assertEqual(self.sf(0.0), 0.0)


class TestConfidenceOutputGuards(unittest.TestCase):
    """Verify calculate_confidence_score return dict has finite scalars."""

    def setUp(self):
        from engine.confidence import calculate_confidence_score
        self.calc = calculate_confidence_score

    def _basic_result(self, **overrides):
        kwargs = dict(
            probability_over=0.62,
            edge_percentage=8.0,
            directional_forces={
                "over_count": 3, "under_count": 1,
                "over_strength": 4.0, "under_strength": 1.0,
            },
            defense_factor=1.05,
            stat_standard_deviation=5.0,
            stat_average=24.0,
            simulation_results=[],
            games_played=40,
        )
        kwargs.update(overrides)
        return self.calc(**kwargs)

    def test_standard_result_all_finite(self):
        r = self._basic_result()
        self.assertTrue(math.isfinite(r["confidence_score"]))
        self.assertTrue(math.isfinite(r["sample_size_discount"]))
        self.assertTrue(math.isfinite(r["probability_agreement_bonus"]))
        self.assertTrue(math.isfinite(r["streak_adjustment"]))
        for k, v in r["score_breakdown"].items():
            self.assertTrue(math.isfinite(v), f"score_breakdown['{k}'] not finite: {v}")

    def test_zero_average_finite(self):
        """Zero stat_average should produce finite results (no ZeroDivision)."""
        r = self._basic_result(stat_average=0.0)
        self.assertTrue(math.isfinite(r["confidence_score"]))

    def test_extreme_edge_finite(self):
        """Very large edge should still produce finite results."""
        r = self._basic_result(edge_percentage=99.9)
        self.assertTrue(math.isfinite(r["confidence_score"]))

    def test_low_probability_finite(self):
        """Very low probability should produce finite results."""
        r = self._basic_result(probability_over=0.01)
        self.assertTrue(math.isfinite(r["confidence_score"]))


class TestRiskScoreGuards(unittest.TestCase):
    """Verify calculate_risk_score output has finite scalars."""

    def test_standard_risk_finite(self):
        from engine.confidence import calculate_risk_score
        conf = {"confidence_score": 65.0, "tier": "Gold"}
        result = calculate_risk_score(conf, 8.0, 0.35)
        self.assertTrue(math.isfinite(result["risk_score"]))


# ============================================================
# 4B: engine/edge_detection.py
# ============================================================

class TestEdgeDetectionSafeFloat(unittest.TestCase):
    """Verify _safe_float helper exists in edge_detection."""

    def setUp(self):
        from engine.edge_detection import _safe_float
        self.sf = _safe_float

    def test_nan_returns_fallback(self):
        self.assertEqual(self.sf(float("nan"), 0.0), 0.0)

    def test_finite_passes(self):
        self.assertEqual(self.sf(3.14), 3.14)

    def test_inf_returns_fallback(self):
        self.assertEqual(self.sf(float("inf"), 99.0), 99.0)


class TestAnalyzeDirectionalForcesGuards(unittest.TestCase):
    """Verify analyze_directional_forces return dict has finite scalars."""

    def test_standard_result_finite(self):
        from engine.edge_detection import analyze_directional_forces
        player = {"games_played": 40, "points_avg": 24.0, "points_std": 5.0}
        proj = {"projected_points": 25.0, "defense_factor": 1.05,
                "pace_factor": 1.03, "blowout_risk": 0.15, "rest_factor": 1.0}
        ctx = {"is_home": True, "vegas_spread": -3.0, "game_total": 225.0}
        result = analyze_directional_forces(player, 22.5, "points", proj, ctx)
        for k in ("over_strength", "under_strength", "net_strength", "conflict_severity"):
            self.assertTrue(math.isfinite(result[k]), f"'{k}' not finite: {result[k]}")

    def test_empty_forces_finite(self):
        """No forces should still produce finite results."""
        from engine.edge_detection import analyze_directional_forces
        player = {"games_played": 40, "points_avg": 24.5, "points_std": 5.0}
        proj = {"projected_points": 24.5, "defense_factor": 1.0,
                "pace_factor": 1.0, "blowout_risk": 0.15, "rest_factor": 1.0}
        ctx = {"is_home": True, "vegas_spread": 0, "game_total": 220.0}
        result = analyze_directional_forces(player, 24.5, "points", proj, ctx)
        self.assertTrue(math.isfinite(result["conflict_severity"]))


class TestEstimateClosingLineValueGuards(unittest.TestCase):
    """Verify CLV estimate return dict has finite scalars."""

    def test_standard_clv_finite(self):
        from engine.edge_detection import estimate_closing_line_value
        result = estimate_closing_line_value(24.5, 27.0, hours_to_game=6.0)
        self.assertTrue(math.isfinite(result["estimated_closing_line"]))
        self.assertTrue(math.isfinite(result["clv_edge"]))

    def test_zero_line_finite(self):
        from engine.edge_detection import estimate_closing_line_value
        result = estimate_closing_line_value(0.0, 27.0)
        self.assertTrue(math.isfinite(result["clv_edge"]))


class TestDetectCoinFlipGuards(unittest.TestCase):
    """Verify detect_coin_flip return dict has finite scalars."""

    def test_coin_flip_detected_finite(self):
        from engine.edge_detection import detect_coin_flip
        result = detect_coin_flip(24.8, 24.5, 6.0, "points")
        self.assertTrue(math.isfinite(result["std_devs_from_line"]))

    def test_not_coin_flip_finite(self):
        from engine.edge_detection import detect_coin_flip
        result = detect_coin_flip(30.0, 24.5, 5.0, "points")
        self.assertTrue(math.isfinite(result["std_devs_from_line"]))

    def test_zero_std_finite(self):
        from engine.edge_detection import detect_coin_flip
        result = detect_coin_flip(24.8, 24.5, 0.0, "points")
        self.assertTrue(math.isfinite(result["std_devs_from_line"]))


class TestCalculateWeightedNetForceGuards(unittest.TestCase):
    """Verify calculate_weighted_net_force output is finite."""

    def test_standard_forces_finite(self):
        from engine.edge_detection import calculate_weighted_net_force
        forces = {
            "over_forces": [{"strength": 2.5}, {"strength": 0.8}],
            "under_forces": [{"strength": 1.2}],
        }
        result = calculate_weighted_net_force(forces)
        for k in ("weighted_over_score", "weighted_under_score", "weighted_net"):
            self.assertTrue(math.isfinite(result[k]), f"'{k}' not finite")

    def test_empty_forces_finite(self):
        from engine.edge_detection import calculate_weighted_net_force
        result = calculate_weighted_net_force({"over_forces": [], "under_forces": []})
        self.assertEqual(result["weighted_net"], 0.0)


class TestConfidenceAdjustedEdgeGuards(unittest.TestCase):
    """Verify calculate_confidence_adjusted_edge returns finite."""

    def test_standard_case(self):
        from engine.edge_detection import calculate_confidence_adjusted_edge
        r = calculate_confidence_adjusted_edge(15.0, 60.0)
        self.assertTrue(math.isfinite(r))

    def test_zero_confidence(self):
        from engine.edge_detection import calculate_confidence_adjusted_edge
        self.assertEqual(calculate_confidence_adjusted_edge(15.0, 0.0), 0.0)


# ============================================================
# 4C: engine/bankroll.py
# ============================================================

class TestBankrollSafeFloat(unittest.TestCase):
    """Verify _safe_float helper exists in bankroll."""

    def setUp(self):
        from engine.bankroll import _safe_float
        self.sf = _safe_float

    def test_nan_returns_fallback(self):
        self.assertEqual(self.sf(float("nan"), 0.0), 0.0)

    def test_finite_passes(self):
        self.assertEqual(self.sf(42.5), 42.5)

    def test_inf_returns_fallback(self):
        self.assertEqual(self.sf(float("inf")), 0.0)


class TestKellyFractionGuards(unittest.TestCase):
    """Verify calculate_kelly_fraction returns finite."""

    def test_standard_case_finite(self):
        from engine.bankroll import calculate_kelly_fraction
        result = calculate_kelly_fraction(0.62, 3.0, "quarter")
        self.assertTrue(math.isfinite(result))
        self.assertGreater(result, 0.0)

    def test_no_edge_returns_zero(self):
        from engine.bankroll import calculate_kelly_fraction
        result = calculate_kelly_fraction(0.3, 2.0, "quarter")
        self.assertEqual(result, 0.0)

    def test_degenerate_inputs_safe(self):
        from engine.bankroll import calculate_kelly_fraction
        self.assertEqual(calculate_kelly_fraction(0.0, 3.0), 0.0)
        self.assertEqual(calculate_kelly_fraction(1.0, 3.0), 0.0)
        self.assertEqual(calculate_kelly_fraction(0.5, 1.0), 0.0)


class TestBankrollAllocationGuards(unittest.TestCase):
    """Verify get_bankroll_allocation enriches entries with finite values."""

    def test_standard_allocation_finite(self):
        from engine.bankroll import get_bankroll_allocation
        entries = [
            {"win_probability": 0.62, "payout_multiplier": 3.0},
            {"win_probability": 0.55, "payout_multiplier": 2.5},
        ]
        result = get_bankroll_allocation(entries, bankroll=1000.0)
        for e in result:
            self.assertTrue(math.isfinite(e["recommended_bet"]))
            self.assertTrue(math.isfinite(e["kelly_fraction"]))
            self.assertTrue(math.isfinite(e["expected_profit"]))


class TestSessionRiskSummaryGuards(unittest.TestCase):
    """Verify get_session_risk_summary returns finite scalars."""

    def test_standard_summary_finite(self):
        from engine.bankroll import get_session_risk_summary
        entries = [
            {"win_probability": 0.62, "payout_multiplier": 3.0, "recommended_bet": 25.0},
        ]
        result = get_session_risk_summary(entries, bankroll=1000.0)
        for k in ("total_at_risk", "total_at_risk_pct", "expected_profit",
                   "prob_positive_session", "worst_case_loss", "best_case_gain",
                   "risk_of_ruin_estimate"):
            self.assertTrue(math.isfinite(result[k]), f"'{k}' not finite: {result[k]}")

    def test_zero_bankroll_safe(self):
        from engine.bankroll import get_session_risk_summary
        result = get_session_risk_summary([], bankroll=0.0)
        self.assertEqual(result["total_at_risk"], 0.0)


# ============================================================
# 4D: engine/calibration.py
# ============================================================

class TestCalibrationSafeFloat(unittest.TestCase):
    """Verify _safe_float helper exists in calibration."""

    def setUp(self):
        from engine.calibration import _safe_float
        self.sf = _safe_float

    def test_nan_returns_fallback(self):
        self.assertEqual(self.sf(float("nan"), 0.0), 0.0)

    def test_finite_passes(self):
        self.assertEqual(self.sf(3.14), 3.14)

    def test_inf_returns_fallback(self):
        self.assertEqual(self.sf(float("inf")), 0.0)


class TestCalibrationAdjustmentGuards(unittest.TestCase):
    """Verify get_calibration_adjustment returns finite."""

    def test_cold_start_returns_zero(self):
        from engine.calibration import get_calibration_adjustment
        result = get_calibration_adjustment(0.65, days=1)
        self.assertTrue(math.isfinite(result))

    def test_standard_prob_finite(self):
        from engine.calibration import get_calibration_adjustment
        result = get_calibration_adjustment(0.70, days=90)
        self.assertTrue(math.isfinite(result))


class TestCalibrationSummaryGuards(unittest.TestCase):
    """Verify get_calibration_summary returns well-formed dict."""

    def test_summary_shape(self):
        from engine.calibration import get_calibration_summary
        result = get_calibration_summary(days=1)
        self.assertIn("has_data", result)
        self.assertIn("total_bets", result)
        # If overall_accuracy is not None, it should be finite
        if result["overall_accuracy"] is not None:
            self.assertTrue(math.isfinite(result["overall_accuracy"]))


class TestIsotonicCurveGuards(unittest.TestCase):
    """Verify get_isotonic_calibration_curve returns well-formed dict."""

    def test_isotonic_curve_shape(self):
        from engine.calibration import get_isotonic_calibration_curve
        result = get_isotonic_calibration_curve(days=1)
        self.assertIn("has_data", result)
        self.assertIn("curve", result)
        # If there are curve points, all should be finite
        for pt in result.get("curve", []):
            self.assertTrue(math.isfinite(pt["predicted"]))
            self.assertTrue(math.isfinite(pt["actual"]))
            self.assertTrue(math.isfinite(pt["gap"]))


if __name__ == "__main__":
    unittest.main()
