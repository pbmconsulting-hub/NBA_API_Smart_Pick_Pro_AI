# ============================================================
# FILE: tests/test_composite_win_score.py
# PURPOSE: Unit tests for calculate_composite_win_score() in
#          engine/edge_detection.py — the composite 0-100 score
#          that ranks picks by expected win probability.
# ============================================================

import unittest


class TestCompositeWinScore(unittest.TestCase):
    """Tests for calculate_composite_win_score in engine/edge_detection.py."""

    def setUp(self):
        from engine.edge_detection import calculate_composite_win_score
        self.calc = calculate_composite_win_score

    # ── Return shape ──────────────────────────────────────────────

    def test_returns_dict_with_required_keys(self):
        result = self.calc(0.60, 60, 8.0)
        self.assertIsInstance(result, dict)
        for key in ("composite_win_score", "grade", "grade_label", "components"):
            self.assertIn(key, result)

    def test_components_has_all_sub_scores(self):
        result = self.calc(0.65, 70, 10.0)
        comps = result["components"]
        for key in (
            "probability_score", "confidence_score", "edge_score",
            "force_alignment_score", "streak_score", "risk_score",
        ):
            self.assertIn(key, comps)

    # ── Score range ───────────────────────────────────────────────

    def test_score_in_0_to_100(self):
        """Score should always be in [0, 100]."""
        for prob in (0.50, 0.55, 0.70, 0.90, 1.0):
            for conf in (0, 30, 60, 90, 100):
                for edge in (0, 5, 15, 30):
                    r = self.calc(prob, conf, edge)
                    self.assertGreaterEqual(r["composite_win_score"], 0.0)
                    self.assertLessEqual(r["composite_win_score"], 100.0)

    def test_minimum_score_is_zero(self):
        """Worst-case inputs should produce a very low score."""
        r = self.calc(0.50, 0, 0.0)
        self.assertLessEqual(r["composite_win_score"], 30.0)

    # ── Grade assignment ──────────────────────────────────────────

    def test_elite_pick_grades_A_or_higher(self):
        r = self.calc(0.80, 90, 18.0,
                      directional_forces_result={
                          "over_forces": [{"s": 1}] * 5,
                          "under_forces": [],
                      },
                      streak_multiplier=1.05,
                      risk_score=1.5)
        self.assertIn(r["grade"], ("A+", "A"))

    def test_weak_pick_grades_D_or_F(self):
        r = self.calc(0.52, 30, 2.0, risk_score=8.0)
        self.assertIn(r["grade"], ("D", "F"))

    # ── Monotonicity: higher inputs → higher scores ───────────────

    def test_higher_probability_increases_score(self):
        r_low = self.calc(0.55, 70, 10.0)
        r_high = self.calc(0.75, 70, 10.0)
        self.assertGreater(
            r_high["composite_win_score"],
            r_low["composite_win_score"],
        )

    def test_higher_confidence_increases_score(self):
        r_low = self.calc(0.65, 40, 10.0)
        r_high = self.calc(0.65, 90, 10.0)
        self.assertGreater(
            r_high["composite_win_score"],
            r_low["composite_win_score"],
        )

    def test_higher_edge_increases_score(self):
        r_low = self.calc(0.65, 70, 3.0)
        r_high = self.calc(0.65, 70, 18.0)
        self.assertGreater(
            r_high["composite_win_score"],
            r_low["composite_win_score"],
        )

    def test_lower_risk_increases_score(self):
        r_risky = self.calc(0.65, 70, 10.0, risk_score=9.0)
        r_safe = self.calc(0.65, 70, 10.0, risk_score=2.0)
        self.assertGreater(
            r_safe["composite_win_score"],
            r_risky["composite_win_score"],
        )

    # ── Coin flip hard cap ────────────────────────────────────────

    def test_coin_flip_caps_at_25(self):
        r = self.calc(0.70, 80, 12.0, is_coin_flip=True)
        self.assertLessEqual(r["composite_win_score"], 25.0)

    def test_coin_flip_grade_is_F(self):
        r = self.calc(0.70, 80, 12.0, is_coin_flip=True)
        self.assertEqual(r["grade"], "F")

    # ── Avoid hard cap ────────────────────────────────────────────

    def test_avoid_caps_at_15(self):
        r = self.calc(0.70, 80, 12.0, should_avoid=True)
        self.assertLessEqual(r["composite_win_score"], 15.0)

    def test_avoid_grade_is_F(self):
        r = self.calc(0.70, 80, 12.0, should_avoid=True)
        self.assertEqual(r["grade"], "F")

    # ── Streak impact ─────────────────────────────────────────────

    def test_hot_streak_boosts_score(self):
        r_neutral = self.calc(0.65, 70, 10.0, streak_multiplier=1.0)
        r_hot = self.calc(0.65, 70, 10.0, streak_multiplier=1.05)
        self.assertGreater(
            r_hot["composite_win_score"],
            r_neutral["composite_win_score"],
        )

    def test_cold_streak_reduces_score(self):
        r_neutral = self.calc(0.65, 70, 10.0, streak_multiplier=1.0)
        r_cold = self.calc(0.65, 70, 10.0, streak_multiplier=0.95)
        self.assertLess(
            r_cold["composite_win_score"],
            r_neutral["composite_win_score"],
        )

    # ── Force alignment ───────────────────────────────────────────

    def test_unanimous_forces_boost_score(self):
        r_split = self.calc(0.65, 70, 10.0,
                            directional_forces_result={
                                "over_forces": [{"s": 1}] * 2,
                                "under_forces": [{"s": 1}] * 2,
                            })
        r_unanimous = self.calc(0.65, 70, 10.0,
                                directional_forces_result={
                                    "over_forces": [{"s": 1}] * 4,
                                    "under_forces": [],
                                })
        self.assertGreater(
            r_unanimous["composite_win_score"],
            r_split["composite_win_score"],
        )

    # ── Default arguments ─────────────────────────────────────────

    def test_default_directional_forces_uses_neutral(self):
        r = self.calc(0.65, 70, 10.0)
        self.assertEqual(r["components"]["force_alignment_score"], 50.0)

    def test_default_streak_is_neutral(self):
        r = self.calc(0.65, 70, 10.0)
        self.assertEqual(r["components"]["streak_score"], 50.0)

    # ── Edge cases ────────────────────────────────────────────────

    def test_probability_below_50_yields_zero_prob_score(self):
        r = self.calc(0.45, 50, 5.0)
        self.assertEqual(r["components"]["probability_score"], 0.0)

    def test_negative_confidence_treated_as_zero(self):
        r = self.calc(0.65, -10, 10.0)
        self.assertEqual(r["components"]["confidence_score"], 0.0)

    def test_extreme_edge_capped(self):
        """Edge above _CWS_MAX_EDGE_PCT should cap the edge sub-score at 100."""
        r = self.calc(0.65, 70, 50.0)
        self.assertEqual(r["components"]["edge_score"], 100.0)

    def test_both_avoid_and_coin_flip_uses_avoid_cap(self):
        """When both should_avoid and is_coin_flip are True, avoid cap (15) wins."""
        r = self.calc(0.70, 80, 12.0, should_avoid=True, is_coin_flip=True)
        self.assertLessEqual(r["composite_win_score"], 15.0)


class TestCompositeWinScoreImport(unittest.TestCase):
    """Verify the function is importable from the module."""

    def test_import_from_edge_detection(self):
        from engine.edge_detection import calculate_composite_win_score
        self.assertTrue(callable(calculate_composite_win_score))

    def test_constants_importable(self):
        from engine.edge_detection import (
            _CWS_W_PROBABILITY,
            _CWS_W_CONFIDENCE,
            _CWS_W_EDGE,
            _CWS_W_FORCES,
            _CWS_W_STREAK,
            _CWS_W_RISK,
        )
        # Verify weights sum to 1.0
        total = (
            _CWS_W_PROBABILITY + _CWS_W_CONFIDENCE + _CWS_W_EDGE
            + _CWS_W_FORCES + _CWS_W_STREAK + _CWS_W_RISK
        )
        self.assertAlmostEqual(total, 1.0, places=6)


if __name__ == "__main__":
    unittest.main()

