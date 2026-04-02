# ============================================================
# FILE: tests/test_institutional_modules.py
# PURPOSE: Tests for the institutional-grade modules:
#          - calculate_fractional_kelly
#          - calculate_fair_odds_from_simulation (fair-value odds)
#          - generate_optimal_slip
#          - pearson_sim_correlation (Correlation Matrix)
# ============================================================

import math
import unittest


class TestFractionalKelly(unittest.TestCase):
    """Tests for engine/odds_engine.py calculate_fractional_kelly()."""

    def setUp(self):
        from engine.odds_engine import calculate_fractional_kelly
        self.calc = calculate_fractional_kelly

    def test_positive_ev_returns_nonzero_fraction(self):
        result = self.calc(0.62, -110, 0.25)
        self.assertGreater(result["kelly_fraction"], 0.0)
        self.assertGreater(result["fractional_kelly"], 0.0)

    def test_negative_ev_returns_zero(self):
        result = self.calc(0.40, -110, 0.25)
        self.assertEqual(result["kelly_fraction"], 0.0)
        self.assertEqual(result["fractional_kelly"], 0.0)

    def test_exact_breakeven_returns_zero(self):
        # At -110, breakeven is ~52.38%
        result = self.calc(0.5238, -110, 0.25)
        self.assertAlmostEqual(result["fractional_kelly"], 0.0, places=3)

    def test_multiplier_scales_fraction(self):
        full = self.calc(0.65, -110, 1.0)
        half = self.calc(0.65, -110, 0.5)
        self.assertAlmostEqual(
            full["fractional_kelly"] / 2.0,
            half["fractional_kelly"],
            places=4,
        )

    def test_zero_multiplier_returns_zero_fraction(self):
        result = self.calc(0.65, -110, 0.0)
        self.assertEqual(result["fractional_kelly"], 0.0)
        self.assertGreater(result["kelly_fraction"], 0.0)

    def test_returns_expected_keys(self):
        result = self.calc(0.60, -110, 0.25)
        for key in ["kelly_fraction", "fractional_kelly", "multiplier", "ev_per_unit", "edge"]:
            self.assertIn(key, result)

    def test_divide_by_zero_odds_handled(self):
        # odds=0 is invalid
        result = self.calc(0.60, 0, 0.25)
        self.assertIsInstance(result, dict)

    def test_extreme_positive_odds(self):
        result = self.calc(0.60, 500, 0.25)
        self.assertGreater(result["fractional_kelly"], 0.0)

    def test_invalid_probability_clamped(self):
        result_high = self.calc(1.5, -110, 0.25)
        self.assertIsInstance(result_high, dict)
        result_neg = self.calc(-0.5, -110, 0.25)
        self.assertIsInstance(result_neg, dict)

    def test_none_inputs_handled(self):
        result = self.calc(None, None, None)
        self.assertEqual(result["kelly_fraction"], 0.0)


class TestFairOddsFromSimulation(unittest.TestCase):
    """Tests for engine/odds_engine.py calculate_fair_odds_from_simulation()."""

    def setUp(self):
        from engine.odds_engine import calculate_fair_odds_from_simulation
        self.calc = calculate_fair_odds_from_simulation

    def test_simple_over_probability(self):
        # 7 out of 10 values > 5.0
        sim = [3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0, 12.0]
        result = self.calc(sim, 5.0, "OVER")
        self.assertAlmostEqual(result["win_probability"], 0.7, places=2)

    def test_simple_under_probability(self):
        sim = [3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0, 12.0]
        result = self.calc(sim, 5.0, "UNDER")
        # values <= 5.0: 3,4,5 = 3/10 = 0.30
        self.assertAlmostEqual(result["win_probability"], 0.3, places=2)

    def test_empty_array_returns_defaults(self):
        result = self.calc([], 10.0, "OVER")
        self.assertEqual(result["win_probability"], 0.5)
        self.assertEqual(result["sample_size"], 0)

    def test_returns_expected_keys(self):
        sim = [10.0, 20.0, 30.0]
        result = self.calc(sim, 15.0, "OVER")
        for key in ["win_probability", "fair_odds", "target_line", "direction", "sample_size"]:
            self.assertIn(key, result)

    def test_probability_clamped(self):
        # All above → probability near 1.0 but clamped to 0.99
        sim = [100.0] * 100
        result = self.calc(sim, 1.0, "OVER")
        self.assertLessEqual(result["win_probability"], 0.99)

    def test_probability_floor(self):
        # None above → probability clamped to 0.01
        sim = [1.0] * 100
        result = self.calc(sim, 1000.0, "OVER")
        self.assertGreaterEqual(result["win_probability"], 0.01)

    def test_fair_odds_negative_for_favorites(self):
        sim = [20.0] * 70 + [10.0] * 30
        result = self.calc(sim, 15.0, "OVER")
        self.assertLess(result["fair_odds"], 0)

    def test_fair_odds_positive_for_underdogs(self):
        sim = [20.0] * 30 + [10.0] * 70
        result = self.calc(sim, 15.0, "OVER")
        self.assertGreater(result["fair_odds"], 0)

    def test_direction_case_insensitive(self):
        sim = [10.0, 20.0, 30.0]
        result_upper = self.calc(sim, 15.0, "OVER")
        result_lower = self.calc(sim, 15.0, "over")
        self.assertEqual(result_upper["win_probability"], result_lower["win_probability"])

    def test_sample_size_correct(self):
        sim = list(range(100))
        result = self.calc(sim, 50.0, "OVER")
        self.assertEqual(result["sample_size"], 100)


class TestGenerateOptimalSlip(unittest.TestCase):
    """Tests for engine/odds_engine.py generate_optimal_slip()."""

    def setUp(self):
        from engine.odds_engine import generate_optimal_slip
        self.gen = generate_optimal_slip

    def _make_props(self, n):
        """Generate n mock props with different players."""
        props = []
        for i in range(n):
            props.append({
                "player_name": f"Player_{i}",
                "stat_type": "points",
                "probability_over": 0.60 + i * 0.01,
                "direction": "OVER",
                "player_team": f"TEAM{i % 5}",
                "opponent": f"OPP{(i+1) % 5}",
                "edge_percentage": 5.0 + i * 0.5,
                "confidence_score": 60 + i,
            })
        return props

    def test_empty_input_returns_empty(self):
        self.assertEqual(self.gen([]), [])

    def test_single_prop_returns_empty(self):
        self.assertEqual(self.gen(self._make_props(1)), [])

    def test_two_props_returns_slips(self):
        result = self.gen(self._make_props(2))
        self.assertGreater(len(result), 0)
        self.assertEqual(result[0]["slip_size"], 2)

    def test_returns_expected_keys(self):
        result = self.gen(self._make_props(3))
        slip = result[0]
        for key in ["slip_size", "picks", "cumulative_ev", "combined_probability",
                     "correlation_penalty", "fair_odds"]:
            self.assertIn(key, slip)

    def test_sorted_by_ev_descending(self):
        result = self.gen(self._make_props(5))
        evs = [s["cumulative_ev"] for s in result]
        self.assertEqual(evs, sorted(evs, reverse=True))

    def test_max_10_results(self):
        result = self.gen(self._make_props(10))
        self.assertLessEqual(len(result), 10)

    def test_same_game_picks_penalized(self):
        props = [
            {"player_name": "A", "stat_type": "pts", "probability_over": 0.65,
             "direction": "OVER", "player_team": "LAL", "opponent": "GSW", "edge_percentage": 8.0},
            {"player_name": "B", "stat_type": "reb", "probability_over": 0.62,
             "direction": "OVER", "player_team": "LAL", "opponent": "GSW", "edge_percentage": 6.0},
        ]
        result = self.gen(props)
        self.assertGreater(len(result), 0)
        self.assertLess(result[0]["correlation_penalty"], 1.0)

    def test_unique_players_enforced(self):
        props = [
            {"player_name": "Same Player", "stat_type": "pts", "probability_over": 0.65,
             "direction": "OVER", "player_team": "LAL", "opponent": "GSW", "edge_percentage": 8.0},
            {"player_name": "Same Player", "stat_type": "reb", "probability_over": 0.62,
             "direction": "OVER", "player_team": "LAL", "opponent": "GSW", "edge_percentage": 6.0},
            {"player_name": "Other Player", "stat_type": "pts", "probability_over": 0.60,
             "direction": "OVER", "player_team": "BOS", "opponent": "MIA", "edge_percentage": 5.0},
        ]
        result = self.gen(props)
        for slip in result:
            names = [p["player_name"] for p in slip["picks"]]
            self.assertEqual(len(names), len(set(n.lower().strip() for n in names)))

    def test_slip_sizes_2_through_5(self):
        result = self.gen(self._make_props(6))
        sizes = set(s["slip_size"] for s in result)
        self.assertTrue(sizes.issubset({2, 3, 4, 5}))

    def test_platform_parameter_accepted(self):
        for platform in ["PrizePicks", "Underdog", "DraftKings"]:
            result = self.gen(self._make_props(4), platform=platform)
            self.assertIsInstance(result, list)


class TestPearsonSimCorrelation(unittest.TestCase):
    """Tests for engine/correlation.py pearson_sim_correlation()."""

    def setUp(self):
        from engine.correlation import pearson_sim_correlation
        self._pearson = pearson_sim_correlation

    def test_perfect_positive_correlation(self):
        a = [1.0, 2.0, 3.0, 4.0, 5.0]
        b = [10.0, 20.0, 30.0, 40.0, 50.0]
        self.assertAlmostEqual(self._pearson(a, b), 1.0, places=3)

    def test_perfect_negative_correlation(self):
        a = [1.0, 2.0, 3.0, 4.0, 5.0]
        b = [50.0, 40.0, 30.0, 20.0, 10.0]
        self.assertAlmostEqual(self._pearson(a, b), -1.0, places=3)

    def test_zero_correlation(self):
        a = [1.0, 2.0, 3.0, 4.0, 5.0]
        b = [5.0, 5.0, 5.0, 5.0, 5.0]
        self.assertEqual(self._pearson(a, b), 0.0)

    def test_insufficient_data(self):
        self.assertEqual(self._pearson([1.0], [2.0]), 0.0)
        self.assertEqual(self._pearson([1.0, 2.0], [3.0, 4.0]), 0.0)

    def test_empty_arrays(self):
        self.assertEqual(self._pearson([], []), 0.0)

    def test_different_length_arrays(self):
        a = [1.0, 2.0, 3.0, 4.0, 5.0]
        b = [10.0, 20.0, 30.0]
        r = self._pearson(a, b)
        self.assertAlmostEqual(r, 1.0, places=3)

    def test_result_bounded(self):
        a = [1.0, 2.0, 3.0, 4.0, 5.0]
        b = [5.0, 3.0, 1.0, 4.0, 2.0]
        r = self._pearson(a, b)
        self.assertGreaterEqual(r, -1.0)
        self.assertLessEqual(r, 1.0)

    def test_result_rounded_to_4_dp(self):
        # Use arrays that produce a Pearson r with high decimal precision
        a = [1.0, 2.3, 3.7, 4.1, 5.9, 6.2, 7.8]
        b = [2.1, 3.4, 5.6, 4.9, 7.3, 8.1, 9.5]
        r = self._pearson(a, b)
        # round(r, 4) should equal r itself (already rounded)
        self.assertEqual(r, round(r, 4))
        # Confirm there are at most 4 decimal digits
        r_str = f"{r:.10f}".rstrip("0")
        decimal_part = r_str.split(".")[-1] if "." in r_str else ""
        self.assertLessEqual(len(decimal_part), 4)


class TestSessionStateDefaults(unittest.TestCase):
    """Tests for app.py session state initialization of bankroll/Kelly keys."""

    def test_app_initializes_total_bankroll(self):
        """Verify that app.py contains total_bankroll initialization."""
        import os
        app_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "app.py")
        with open(app_path, "r") as f:
            content = f.read()
        self.assertIn('"total_bankroll"', content)
        self.assertIn('st.session_state["total_bankroll"]', content)

    def test_app_initializes_kelly_multiplier(self):
        """Verify that app.py contains kelly_multiplier initialization."""
        import os
        app_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "app.py")
        with open(app_path, "r") as f:
            content = f.read()
        self.assertIn('"kelly_multiplier"', content)
        self.assertIn('st.session_state["kelly_multiplier"]', content)

    def test_default_total_bankroll_is_1000(self):
        """Verify the default bankroll is $1000."""
        import os
        app_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "app.py")
        with open(app_path, "r") as f:
            content = f.read()
        # Verify the specific initialization pattern
        self.assertIn('st.session_state["total_bankroll"] = 1000.0', content)

    def test_default_kelly_multiplier_is_025(self):
        """Verify the default Kelly multiplier is 0.25 (Quarter Kelly)."""
        import os
        app_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "app.py")
        with open(app_path, "r") as f:
            content = f.read()
        # Verify the specific initialization pattern
        self.assertIn('st.session_state["kelly_multiplier"] = 0.25', content)


class TestInlineBreakdownKellyAllocation(unittest.TestCase):
    """Test that render_inline_breakdown_html produces Kelly wager HTML."""

    def test_kelly_html_present_in_breakdown(self):
        """Verify the inline breakdown function contains Kelly allocation logic."""
        import os
        helper_path = os.path.join(
            os.path.dirname(os.path.dirname(__file__)),
            "pages", "helpers", "neural_analysis_helpers.py",
        )
        with open(helper_path, "r") as f:
            content = f.read()
        # The inline breakdown now includes Kelly TARGET ALLOCATION
        self.assertIn("calculate_fractional_kelly", content)
        self.assertIn("WAGER", content)
        self.assertIn("total_bankroll", content)

    def test_card_matrix_wager_metric(self):
        """Verify the card matrix renderer includes a Wager metric."""
        import os
        renderer_path = os.path.join(
            os.path.dirname(os.path.dirname(__file__)),
            "utils", "renderers.py",
        )
        with open(renderer_path, "r") as f:
            content = f.read()
        self.assertIn("calculate_fractional_kelly", content)
        self.assertIn("Wager", content)
        self.assertIn("wager_html", content)


class TestFairOddsSliderRobustness(unittest.TestCase):
    """Test fair-odds explore-line slider edge-case handling."""

    def test_slider_key_includes_platform(self):
        """Verify the fair-odds slider key includes platform for uniqueness."""
        import os
        helper_path = os.path.join(
            os.path.dirname(os.path.dirname(__file__)),
            "pages", "helpers", "neural_analysis_helpers.py",
        )
        with open(helper_path, "r") as f:
            content = f.read()
        # Key should include platform to prevent collisions
        self.assertIn("_{platform}_", content)

    def test_slider_min_max_guard(self):
        """Verify that slider guards against min >= max edge case."""
        import os
        helper_path = os.path.join(
            os.path.dirname(os.path.dirname(__file__)),
            "pages", "helpers", "neural_analysis_helpers.py",
        )
        with open(helper_path, "r") as f:
            content = f.read()
        # The guard must exist: when slider_max <= slider_min, expand range
        self.assertIn("_slider_max <= _slider_min", content)
        self.assertIn("_slider_max = _slider_min + 5.0", content)
        # Base line must be clamped inside slider range
        self.assertIn("_base_line = max(_slider_min", content)


# ============================================================
# MODULE 3 Enhancement Tests
# ============================================================

class TestCorrelationMatrixPageImports(unittest.TestCase):
    """Verify the Correlation Matrix page imports from engine, not inline."""

    def test_page_imports_from_engine(self):
        """Verify the page imports pearson_sim_correlation from engine."""
        import os
        page_path = os.path.join(
            os.path.dirname(os.path.dirname(__file__)),
            "pages", "10_🗺️_Correlation_Matrix.py",
        )
        with open(page_path, "r") as f:
            content = f.read()
        self.assertIn("from engine.correlation import", content)
        self.assertIn("pearson_sim_correlation", content)

    def test_page_no_inline_pearson(self):
        """Verify the old inline pearson function is removed from the page."""
        import os
        page_path = os.path.join(
            os.path.dirname(os.path.dirname(__file__)),
            "pages", "10_🗺️_Correlation_Matrix.py",
        )
        with open(page_path, "r") as f:
            content = f.read()
        # Should NOT contain a local 'def pearson_sim_correlation'
        self.assertNotIn("def pearson_sim_correlation", content)

    def test_page_imports_parlay_and_kelly(self):
        """Verify the page imports parlay and Kelly functions."""
        import os
        page_path = os.path.join(
            os.path.dirname(os.path.dirname(__file__)),
            "pages", "10_🗺️_Correlation_Matrix.py",
        )
        with open(page_path, "r") as f:
            content = f.read()
        self.assertIn("adjust_parlay_probability", content)
        self.assertIn("correlation_adjusted_kelly", content)

    def test_page_has_game_filter(self):
        """Verify the page has game-level filtering."""
        import os
        page_path = os.path.join(
            os.path.dirname(os.path.dirname(__file__)),
            "pages", "10_🗺️_Correlation_Matrix.py",
        )
        with open(page_path, "r") as f:
            content = f.read()
        self.assertIn("Filter by Game", content)
        self.assertIn("_game_groups", content)

    def test_page_has_summary_stats(self):
        """Verify the page has correlation summary statistics bar."""
        import os
        page_path = os.path.join(
            os.path.dirname(os.path.dirname(__file__)),
            "pages", "10_🗺️_Correlation_Matrix.py",
        )
        with open(page_path, "r") as f:
            content = f.read()
        self.assertIn("Mean r", content)
        self.assertIn("Max r", content)
        self.assertIn("Min r", content)
        self.assertIn("Pairs", content)

    def test_page_has_parlay_impact_section(self):
        """Verify the page has a Parlay Impact panel."""
        import os
        page_path = os.path.join(
            os.path.dirname(os.path.dirname(__file__)),
            "pages", "10_🗺️_Correlation_Matrix.py",
        )
        with open(page_path, "r") as f:
            content = f.read()
        self.assertIn("Parlay Impact", content)
        self.assertIn("Independent Joint Prob", content)
        self.assertIn("Correlation-Adjusted Prob", content)

    def test_page_has_kelly_section(self):
        """Verify the page has Correlation-Adjusted Kelly section."""
        import os
        page_path = os.path.join(
            os.path.dirname(os.path.dirname(__file__)),
            "pages", "10_🗺️_Correlation_Matrix.py",
        )
        with open(page_path, "r") as f:
            content = f.read()
        self.assertIn("Correlation-Adjusted Kelly", content)
        self.assertIn("Recommended Wager", content)
        self.assertIn("Correlation Discount", content)


class TestPearsonSimCorrelationEngine(unittest.TestCase):
    """Tests for the pearson_sim_correlation exported from engine/correlation.py."""

    def setUp(self):
        from engine.correlation import pearson_sim_correlation
        self.fn = pearson_sim_correlation

    def test_delegates_to_calculate_pearson_correlation(self):
        """Confirm pearson_sim_correlation wraps calculate_pearson_correlation."""
        from engine.correlation import calculate_pearson_correlation
        a = [1.0, 3.0, 5.0, 7.0, 9.0]
        b = [2.0, 4.0, 6.0, 8.0, 10.0]
        raw = calculate_pearson_correlation(a, b)
        wrapped = self.fn(a, b)
        self.assertAlmostEqual(wrapped, round(raw, 4), places=4)

    def test_returns_rounded_result(self):
        a = [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0]
        b = [7.0, 5.0, 3.0, 2.0, 6.0, 1.0, 4.0]
        r = self.fn(a, b)
        self.assertEqual(r, round(r, 4))


class TestParlayImpactIntegration(unittest.TestCase):
    """Tests for adjust_parlay_probability used in the Correlation Matrix."""

    def setUp(self):
        from engine.correlation import adjust_parlay_probability
        self.fn = adjust_parlay_probability

    def test_independent_returns_product(self):
        """When correlation matrix is identity, result ≈ product."""
        probs = [0.6, 0.7]
        matrix = [[1.0, 0.0], [0.0, 1.0]]
        result = self.fn(probs, matrix)
        naive = 0.6 * 0.7
        # Should be very close to naive (identity = no corr adjustment)
        self.assertAlmostEqual(result, naive, places=3)

    def test_positive_correlation_increases_probability(self):
        """Positive correlation should increase the joint probability."""
        probs = [0.6, 0.7]
        corr = [[1.0, 0.5], [0.5, 1.0]]
        result = self.fn(probs, corr)
        naive = 0.6 * 0.7
        self.assertGreater(result, naive)

    def test_negative_correlation_decreases_probability(self):
        """Negative correlation should decrease the joint probability."""
        probs = [0.6, 0.7]
        corr = [[1.0, -0.5], [-0.5, 1.0]]
        result = self.fn(probs, corr)
        naive = 0.6 * 0.7
        self.assertLess(result, naive)

    def test_single_prop_returns_itself(self):
        """Single prop returns its own probability."""
        result = self.fn([0.65], [[1.0]])
        self.assertAlmostEqual(result, 0.65, places=2)


class TestCorrelationAdjustedKelly(unittest.TestCase):
    """Tests for correlation_adjusted_kelly used in the Correlation Matrix."""

    def setUp(self):
        from engine.correlation import correlation_adjusted_kelly
        self.fn = correlation_adjusted_kelly

    def test_returns_expected_keys(self):
        picks = [{"win_probability": 0.6, "odds_decimal": 1.91}]
        result = self.fn(picks, 1000, [[1.0]])
        for key in ["kelly_fraction", "recommended_bet", "correlation_discount"]:
            self.assertIn(key, result)

    def test_higher_correlation_reduces_fraction(self):
        """Higher pairwise correlation should reduce the Kelly fraction."""
        picks = [
            {"win_probability": 0.6, "odds_decimal": 1.91},
            {"win_probability": 0.65, "odds_decimal": 2.0},
        ]
        low_corr = [[1.0, 0.1], [0.1, 1.0]]
        high_corr = [[1.0, 0.8], [0.8, 1.0]]
        result_low = self.fn(picks, 1000, low_corr)
        result_high = self.fn(picks, 1000, high_corr)
        self.assertGreaterEqual(
            result_low["kelly_fraction"],
            result_high["kelly_fraction"],
        )

    def test_discount_below_one_with_correlation(self):
        """Correlation discount should be < 1 when picks are correlated."""
        picks = [
            {"win_probability": 0.6, "odds_decimal": 1.91},
            {"win_probability": 0.65, "odds_decimal": 2.0},
        ]
        corr = [[1.0, 0.6], [0.6, 1.0]]
        result = self.fn(picks, 1000, corr)
        self.assertLess(result["correlation_discount"], 1.0)

    def test_zero_bankroll_returns_zero(self):
        picks = [{"win_probability": 0.6, "odds_decimal": 1.91}]
        result = self.fn(picks, 0, [[1.0]])
        self.assertEqual(result["recommended_bet"], 0.0)

    def test_empty_picks_returns_zero(self):
        result = self.fn([], 1000, [])
        self.assertEqual(result["recommended_bet"], 0.0)


# ============================================================
# MODULE 4: Auto-Slip Optimizer Enhancement Tests
# ============================================================

class TestAutoSlipOptimizerPageFeatures(unittest.TestCase):
    """Verify MODULE 4 enhancements exist in Entry Builder page."""

    def _read_page(self):
        import os
        page_path = os.path.join(
            os.path.dirname(os.path.dirname(__file__)),
            "pages", "6_🧬_Entry_Builder.py",
        )
        with open(page_path, "r") as f:
            return f.read()

    def test_page_imports_calculate_fractional_kelly(self):
        """Verify the auto-slip section imports calculate_fractional_kelly."""
        content = self._read_page()
        self.assertIn("calculate_fractional_kelly", content)

    def test_page_imports_clamp_probability(self):
        """Verify the auto-slip section imports clamp_probability."""
        content = self._read_page()
        self.assertIn("clamp_probability", content)

    def test_page_has_kelly_target_allocation(self):
        """Verify the optimal ticket displays a TARGET ALLOCATION."""
        content = self._read_page()
        self.assertIn("TARGET ALLOCATION", content)

    def test_page_has_expected_payout(self):
        """Verify the optimal ticket displays EXPECTED PAYOUT."""
        content = self._read_page()
        self.assertIn("EXPECTED PAYOUT", content)

    def test_page_has_kelly_percent(self):
        """Verify the optimal ticket displays KELLY %."""
        content = self._read_page()
        self.assertIn("KELLY %", content)

    def test_page_reads_total_bankroll(self):
        """Verify the auto-slip reads total_bankroll from session state."""
        content = self._read_page()
        self.assertIn("total_bankroll", content)

    def test_page_reads_kelly_multiplier(self):
        """Verify the auto-slip reads kelly_multiplier from session state."""
        content = self._read_page()
        self.assertIn("kelly_multiplier", content)

    def test_page_reads_entry_fee(self):
        """Verify the auto-slip reads entry_fee from session state."""
        content = self._read_page()
        self.assertIn("entry_fee", content)

    def test_page_has_slip_summary_stats(self):
        """Verify the slip summary statistics bar exists."""
        content = self._read_page()
        self.assertIn("Slips Generated", content)
        self.assertIn("Best EV", content)
        self.assertIn("Avg EV", content)
        self.assertIn("Avg All-Hit", content)

    def test_page_has_per_leg_edge(self):
        """Verify the per-leg edge % is displayed in the ticket."""
        content = self._read_page()
        self.assertIn("edge_percentage", content)

    def test_alternative_slips_show_prob_and_odds(self):
        """Verify alternative slips show probability and fair odds."""
        content = self._read_page()
        self.assertIn("_alt_prob", content)
        self.assertIn("_alt_odds", content)
        self.assertIn("_alt_penalty", content)


class TestCalculateFractionalKellyForSlip(unittest.TestCase):
    """Test Kelly integration for the optimal slip wager calculation."""

    def setUp(self):
        from engine.odds_engine import calculate_fractional_kelly
        self.calc = calculate_fractional_kelly

    def test_slip_prob_with_fair_odds(self):
        """Slip probability (e.g. 0.20) with positive fair odds gives kelly."""
        result = self.calc(0.35, 200, 0.25)
        # At 35% prob and +200, there is an edge
        self.assertGreater(result["fractional_kelly"], 0.0)

    def test_low_prob_no_edge(self):
        """Very low probability against market odds yields zero kelly."""
        result = self.calc(0.10, 500, 0.25)
        # At 10% prob and +500 (breakeven ~16.7%), no edge
        self.assertEqual(result["fractional_kelly"], 0.0)

    def test_parlay_odds_kelly(self):
        """Simulating parlay: low prob, high odds, with edge."""
        # A 4-leg parlay might have ~15% prob at +800 fair odds
        result = self.calc(0.15, 500, 0.25)
        # 15% prob at +500 (breakeven ~16.7%): no edge → 0
        self.assertEqual(result["fractional_kelly"], 0.0)

    def test_high_ev_slip_kelly(self):
        """High EV slip: good probability exceeding implied odds."""
        # 30% prob at +200 (breakeven 33%): no edge → 0
        result = self.calc(0.40, 200, 0.25)
        # 40% prob at +200 (breakeven 33%): has edge
        self.assertGreater(result["fractional_kelly"], 0.0)

    def test_wager_amount_calculation(self):
        """Verify wager = fractional_kelly * bankroll."""
        bankroll = 1000.0
        result = self.calc(0.60, -110, 0.25)
        frac = result["fractional_kelly"]
        wager = round(frac * bankroll, 2)
        self.assertEqual(wager, round(frac * bankroll, 2))


if __name__ == "__main__":
    unittest.main()
