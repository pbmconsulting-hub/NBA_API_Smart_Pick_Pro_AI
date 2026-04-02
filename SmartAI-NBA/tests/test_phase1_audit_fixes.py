# ============================================================
# FILE: tests/test_phase1_audit_fixes.py
# PURPOSE: Tests for Phase 1 audit fixes — zero-boundary guards,
#          float precision safeguards, and extreme-odds handling
#          in the quantitative engine.
# ============================================================
import math
import unittest


class TestOddsEngineZeroBoundary(unittest.TestCase):
    """Tests for odds_engine.py zero-boundary and extreme-value guards."""

    def setUp(self):
        from engine.odds_engine import (
            american_odds_to_implied_probability,
            odds_to_payout_multiplier,
            calculate_expected_value_with_odds,
            calculate_half_kelly_ev,
            devig_probabilities,
        )
        self.implied_prob = american_odds_to_implied_probability
        self.payout_mult = odds_to_payout_multiplier
        self.calc_ev = calculate_expected_value_with_odds
        self.kelly = calculate_half_kelly_ev
        self.devig = devig_probabilities

    # --- american_odds_to_implied_probability ---

    def test_odds_zero_returns_default(self):
        """Odds of 0 are invalid American odds; must not return 1.0 (certainty)."""
        prob = self.implied_prob(0)
        self.assertNotAlmostEqual(prob, 1.0, places=2,
                                  msg="Odds=0 should NOT return 1.0 (certainty)")
        self.assertAlmostEqual(prob, 0.5238, places=3)

    def test_odds_standard_negative(self):
        prob = self.implied_prob(-110)
        self.assertAlmostEqual(prob, 0.5238, places=3)

    def test_odds_standard_positive(self):
        prob = self.implied_prob(150)
        self.assertAlmostEqual(prob, 0.4, places=3)

    def test_odds_extreme_positive(self):
        """Extreme positive odds like +99900 should not overflow."""
        prob = self.implied_prob(99900)
        self.assertGreater(prob, 0.0)
        self.assertLess(prob, 0.01)

    def test_odds_extreme_negative(self):
        """Extreme negative odds like -99900 should be near 1.0 but not exactly 1.0."""
        prob = self.implied_prob(-99900)
        self.assertGreater(prob, 0.99)
        self.assertLessEqual(prob, 1.0)

    def test_odds_non_numeric_returns_default(self):
        prob = self.implied_prob("abc")
        self.assertAlmostEqual(prob, 0.5238, places=3)

    def test_odds_none_returns_default(self):
        prob = self.implied_prob(None)
        self.assertAlmostEqual(prob, 0.5238, places=3)

    # --- odds_to_payout_multiplier ---

    def test_payout_zero_returns_default(self):
        """Odds of 0 must not return 1.0 (break-even payout)."""
        payout = self.payout_mult(0)
        self.assertAlmostEqual(payout, 1.9091, places=3)

    def test_payout_standard_negative(self):
        payout = self.payout_mult(-110)
        self.assertAlmostEqual(payout, 1.9091, places=3)

    def test_payout_standard_positive(self):
        payout = self.payout_mult(150)
        self.assertAlmostEqual(payout, 2.5, places=3)

    def test_payout_extreme_positive(self):
        """Extreme +99900 should return a large but finite multiplier."""
        payout = self.payout_mult(99900)
        self.assertTrue(math.isfinite(payout))
        self.assertGreater(payout, 100.0)

    # --- calculate_expected_value_with_odds ---

    def test_ev_with_zero_odds(self):
        """EV with odds=0 should not crash or return NaN."""
        ev = self.calc_ev(0.6, 0)
        self.assertTrue(math.isfinite(ev))

    def test_ev_with_extreme_odds(self):
        """EV with extreme +99900 odds should be finite."""
        ev = self.calc_ev(0.02, 99900, stake=100)
        self.assertTrue(math.isfinite(ev))

    def test_ev_clamps_probability(self):
        """Probability > 1.0 should be clamped, not cause overflow."""
        ev = self.calc_ev(1.5, -110)
        self.assertTrue(math.isfinite(ev))

    def test_ev_negative_probability_clamped(self):
        """Negative probability should be clamped to 0."""
        ev = self.calc_ev(-0.5, -110)
        self.assertTrue(math.isfinite(ev))
        self.assertLessEqual(ev, 0.0)

    def test_ev_none_returns_zero(self):
        ev = self.calc_ev(None, -110)
        self.assertEqual(ev, 0.0)

    # --- devig_probabilities ---

    def test_devig_extreme_positive_odds(self):
        """Extreme +10000 / +10000 should not crash or produce NaN."""
        fair_over, fair_under = self.devig(10000, 10000)
        self.assertTrue(math.isfinite(fair_over))
        self.assertTrue(math.isfinite(fair_under))
        self.assertAlmostEqual(fair_over + fair_under, 1.0, places=4)

    def test_devig_standard_market(self):
        fair_over, fair_under = self.devig(-110, -110)
        self.assertAlmostEqual(fair_over, 0.5, places=2)
        self.assertAlmostEqual(fair_under, 0.5, places=2)

    def test_devig_zero_odds_both_sides(self):
        """Both sides at zero odds should return safe defaults."""
        fair_over, fair_under = self.devig(0, 0)
        self.assertTrue(math.isfinite(fair_over))
        self.assertTrue(math.isfinite(fair_under))

    # --- calculate_half_kelly_ev ---

    def test_kelly_zero_odds(self):
        """Kelly with odds=0 should not crash."""
        result = self.kelly(0.6, 0)
        self.assertIsInstance(result, dict)
        self.assertTrue(math.isfinite(result["kelly_fraction"]))

    def test_kelly_extreme_odds(self):
        """Kelly with extreme +99900 should not overflow."""
        result = self.kelly(0.6, 99900)
        self.assertTrue(math.isfinite(result["kelly_fraction"]))
        self.assertTrue(math.isfinite(result["half_kelly_stake"]))


class TestSimulationZeroBoundary(unittest.TestCase):
    """Tests for simulation.py zero-boundary and edge case guards."""

    def setUp(self):
        from engine.simulation import (
            run_quantum_matrix_simulation,
            build_histogram_from_results,
        )
        self.run_qme = run_quantum_matrix_simulation
        self.build_hist = build_histogram_from_results

    def _base_kwargs(self):
        return dict(
            projected_stat_average=20.0,
            stat_standard_deviation=4.0,
            prop_line=19.5,
            number_of_simulations=200,
            blowout_risk_factor=0.15,
            pace_adjustment_factor=1.0,
            matchup_adjustment_factor=1.0,
            home_away_adjustment=0.02,
            rest_adjustment_factor=1.0,
        )

    def test_zero_projected_stat_average(self):
        """Bench player with 0 projected stat should not crash."""
        kw = self._base_kwargs()
        kw["projected_stat_average"] = 0.0
        kw["stat_standard_deviation"] = 0.0
        result = self.run_qme(**kw)
        self.assertIsInstance(result, dict)
        self.assertTrue(math.isfinite(result["probability_over"]))

    def test_zero_std_dev(self):
        """Zero standard deviation should not cause NaN or crash."""
        kw = self._base_kwargs()
        kw["stat_standard_deviation"] = 0.0
        result = self.run_qme(**kw)
        self.assertTrue(math.isfinite(result["probability_over"]))

    def test_zero_projected_minutes(self):
        """projected_minutes=0 should not cause division by zero."""
        kw = self._base_kwargs()
        kw["projected_minutes"] = 0.0
        result = self.run_qme(**kw)
        self.assertTrue(math.isfinite(result["probability_over"]))

    def test_empty_game_logs(self):
        """Empty recent_game_logs should not crash."""
        kw = self._base_kwargs()
        kw["recent_game_logs"] = []
        result = self.run_qme(**kw)
        self.assertIsInstance(result, dict)

    def test_all_zero_game_logs(self):
        """All-zero game logs should not cause division by zero in KDE scaling."""
        kw = self._base_kwargs()
        kw["recent_game_logs"] = [0.0] * 20
        result = self.run_qme(**kw)
        self.assertIsInstance(result, dict)
        self.assertTrue(math.isfinite(result["probability_over"]))

    def test_negative_combined_adjustment(self):
        """Extreme negative home_away_adjustment should not produce negative projection."""
        kw = self._base_kwargs()
        kw["home_away_adjustment"] = -1.5  # Would make (1 + -1.5) = -0.5
        result = self.run_qme(**kw)
        self.assertIsInstance(result, dict)
        self.assertGreaterEqual(result["adjusted_projection"], 0.0)

    # --- build_histogram_from_results ---

    def test_histogram_zero_buckets(self):
        """number_of_buckets=0 should not cause ZeroDivisionError."""
        data = [10.0, 15.0, 20.0, 25.0, 30.0]
        hist = self.build_hist(data, 20.0, number_of_buckets=0)
        self.assertIsInstance(hist, list)
        self.assertGreater(len(hist), 0)

    def test_histogram_negative_buckets(self):
        """Negative number_of_buckets should be clamped to 1."""
        data = [10.0, 15.0, 20.0, 25.0, 30.0]
        hist = self.build_hist(data, 20.0, number_of_buckets=-5)
        self.assertIsInstance(hist, list)
        self.assertGreater(len(hist), 0)

    def test_histogram_empty_results(self):
        """Empty results should return empty list."""
        hist = self.build_hist([], 20.0)
        self.assertEqual(hist, [])

    def test_histogram_uniform_results(self):
        """All identical values should return a single bucket."""
        data = [15.0] * 100
        hist = self.build_hist(data, 20.0)
        self.assertEqual(len(hist), 1)
        self.assertEqual(hist[0]["count"], 100)


class TestMathHelpersCDFEdgeCases(unittest.TestCase):
    """Tests for math_helpers.py CDF/PDF edge cases."""

    def setUp(self):
        from engine.math_helpers import (
            calculate_normal_cdf,
            calculate_probability_over_line,
            clamp_probability,
            sample_skew_normal,
            sample_poisson_like,
            sample_zero_inflated,
            estimate_zero_probability,
        )
        self.cdf = calculate_normal_cdf
        self.prob_over = calculate_probability_over_line
        self.clamp = clamp_probability
        self.skew = sample_skew_normal
        self.poisson = sample_poisson_like
        self.zero_inf = sample_zero_inflated
        self.est_zero = estimate_zero_probability

    def test_cdf_zero_std(self):
        """CDF with std=0 should return deterministic 0 or 1, not NaN."""
        self.assertEqual(self.cdf(25.0, 20.0, 0.0), 1.0)
        self.assertEqual(self.cdf(15.0, 20.0, 0.0), 0.0)

    def test_cdf_negative_std(self):
        """CDF with negative std should return deterministic."""
        result = self.cdf(25.0, 20.0, -1.0)
        self.assertIn(result, [0.0, 1.0])

    def test_cdf_extreme_z_score(self):
        """Extreme z-scores should not produce NaN."""
        result = self.cdf(1000.0, 0.0, 0.001)
        self.assertTrue(math.isfinite(result))
        self.assertAlmostEqual(result, 1.0, places=4)

    def test_prob_over_zero_std(self):
        """Probability over with std=0 should be deterministic."""
        result = self.prob_over(20.0, 0.0, 25.0)
        self.assertIn(result, [0.0, 1.0])

    def test_clamp_nan_input(self):
        """Clamp with NaN: Python max(0.01, NaN) returns 0.01, min(0.99, NaN) returns 0.99,
        so NaN is clamped to 0.99.  This test documents the actual Python behavior."""
        result = self.clamp(float('nan'))
        # In CPython, max(0.01, NaN)=0.01 and min(0.99, NaN)=0.99, so the
        # overall result is max(0.01, min(0.99, NaN)) = max(0.01, 0.99) = 0.99.
        self.assertAlmostEqual(result, 0.99, places=2)

    def test_skew_normal_zero_std(self):
        """Skew-normal with std=0 should return the mean, not NaN."""
        result = self.skew(20.0, 0.0, 0.8)
        self.assertEqual(result, 20.0)

    def test_poisson_like_zero_mean(self):
        """Poisson sampler with mean=0 should return >= 0, not crash."""
        result = self.poisson(0.0)
        self.assertGreaterEqual(result, 0.0)

    def test_zero_inflated_zero_mean(self):
        """Zero-inflated with mean=0 should not crash."""
        result = self.zero_inf(0.0, 1.0, 0.5)
        self.assertGreaterEqual(result, 0.0)

    def test_estimate_zero_prob_empty_logs(self):
        """Empty game logs should return a stat-type default."""
        result = self.est_zero([], "threes")
        self.assertGreater(result, 0.0)
        self.assertLessEqual(result, 1.0)


if __name__ == "__main__":
    unittest.main()
