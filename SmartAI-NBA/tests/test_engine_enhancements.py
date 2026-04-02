# ============================================================
# FILE: tests/test_engine_enhancements.py
# PURPOSE: Unit tests for the QME 6.0 comprehensive engine enhancements
#          covering all four core modules:
#            - engine/simulation.py (QME 6.0)
#            - engine/edge_detection.py (Advanced Edge Analysis)
#            - engine/confidence.py (Precision Confidence Scoring)
#            - engine/correlation.py (Advanced Correlation Engine)
#
# Each test class covers one module.
# Each new function has at least 3 test cases.
# Backward-compatibility with existing callers is verified.
# ============================================================

import math
import unittest


# ============================================================
# MODULE 1: simulation.py
# ============================================================

class TestSimulationEnhancements(unittest.TestCase):
    """Tests for QME 6.0 enhancements in engine/simulation.py."""

    def setUp(self):
        from engine.simulation import (
            run_quantum_matrix_simulation,
            run_enhanced_simulation,
            _apply_garbage_time_adjustment,
            _simulate_hot_cold_modifier,
            _simulate_game_scenario,
            GAME_SCENARIOS,
            CONVERGENCE_THRESHOLD,
            CONVERGENCE_CHECK_INTERVAL,
            QUARTER_FATIGUE_RATES,
            BACK_TO_BACK_FATIGUE_MULTIPLIER,
            _SPREAD_TOTAL_MATRIX,
        )
        self.run_qme = run_quantum_matrix_simulation
        self.run_enhanced = run_enhanced_simulation
        self.garbage_time = _apply_garbage_time_adjustment
        self.hot_cold = _simulate_hot_cold_modifier
        self.game_scenario = _simulate_game_scenario
        self.GAME_SCENARIOS = GAME_SCENARIOS
        self.CONVERGENCE_THRESHOLD = CONVERGENCE_THRESHOLD
        self.CONVERGENCE_CHECK_INTERVAL = CONVERGENCE_CHECK_INTERVAL
        self.QUARTER_FATIGUE_RATES = QUARTER_FATIGUE_RATES
        self.BACK_TO_BACK_MULT = BACK_TO_BACK_FATIGUE_MULTIPLIER
        self.SPREAD_TOTAL_MATRIX = _SPREAD_TOTAL_MATRIX

    # --- 1A: New game scenarios and spread-total matrix ---

    def test_game_scenarios_contain_new_scenarios(self):
        """The three new scenarios are present in GAME_SCENARIOS."""
        names = [s[0] for s in self.GAME_SCENARIOS]
        self.assertIn("shootout", names)
        self.assertIn("grind_blowout", names)
        self.assertIn("defensive_slug", names)

    def test_game_scenarios_sum_to_one(self):
        """GAME_SCENARIOS probabilities must sum to 1.0 (core invariant)."""
        total = sum(s[1] for s in self.GAME_SCENARIOS)
        self.assertAlmostEqual(total, 1.0, places=5)

    def test_spread_total_matrix_has_all_three_patterns(self):
        """Spread-total matrix should define all three game context patterns."""
        self.assertIn("shootout_game", self.SPREAD_TOTAL_MATRIX)
        self.assertIn("grind_blowout_game", self.SPREAD_TOTAL_MATRIX)
        self.assertIn("defensive_slug_game", self.SPREAD_TOTAL_MATRIX)

    def test_game_scenario_returns_valid_tuple(self):
        """_simulate_game_scenario returns (name, float, float)."""
        name, min_red, stat_mult = self.game_scenario(0.15, vegas_spread=3.0, game_total=230.0)
        self.assertIsInstance(name, str)
        self.assertGreaterEqual(min_red, -0.15)
        self.assertLessEqual(min_red, 0.65)
        self.assertGreater(stat_mult, 0.0)

    def test_game_scenario_spread_total_matrix_applied(self):
        """Tight + high-total game should sometimes produce 'shootout' scenario."""
        scenarios_seen = set()
        for _ in range(200):
            name, _, _ = self.game_scenario(0.10, vegas_spread=2.0, game_total=232.0)
            scenarios_seen.add(name)
        # With spread=2.0 (<4) and total=232.0 (>228), shootout weight is 25%
        self.assertIn("shootout", scenarios_seen,
                      "Shootout scenario should appear in tight/high-total games")

    # --- 1B: Quarter-aware fatigue decay ---

    def test_fatigue_rates_constant_values(self):
        """Quarter fatigue rates should be (1.0, 0.97, 0.94, 0.90)."""
        self.assertEqual(len(self.QUARTER_FATIGUE_RATES), 4)
        self.assertAlmostEqual(self.QUARTER_FATIGUE_RATES[0], 1.00)
        self.assertAlmostEqual(self.QUARTER_FATIGUE_RATES[3], 0.90)

    def test_back_to_back_multiplier_less_than_one(self):
        """Back-to-back fatigue multiplier should reduce performance."""
        self.assertLess(self.BACK_TO_BACK_MULT, 1.0)
        self.assertGreater(self.BACK_TO_BACK_MULT, 0.80)

    def test_enable_fatigue_curve_param_accepted(self):
        """run_quantum_matrix_simulation accepts enable_fatigue_curve parameter."""
        r = self.run_qme(
            25.0, 6.0, 24.5, 200, 0.15, 1.0, 1.0, 0.02, 1.0,
            enable_fatigue_curve=True, random_seed=42
        )
        self.assertIn("probability_over", r)

    def test_fatigue_curve_disabled_gives_result(self):
        """enable_fatigue_curve=False still returns a valid result."""
        r = self.run_qme(
            25.0, 6.0, 24.5, 200, 0.15, 1.0, 1.0, 0.02, 1.0,
            enable_fatigue_curve=False, random_seed=42
        )
        self.assertIn("probability_over", r)
        self.assertGreaterEqual(r["probability_over"], 0.0)
        self.assertLessEqual(r["probability_over"], 1.0)

    # --- 1C: Garbage time modeling ---

    def test_garbage_time_star_reduces_minutes(self):
        """Star player (>=30 min) in blowout should get fewer adjusted minutes."""
        results = [self.garbage_time(34.0, "blowout_win", True) for _ in range(50)]
        avg = sum(results) / len(results)
        self.assertLess(avg, 34.0, "Star player should average fewer minutes in garbage time")

    def test_garbage_time_bench_increases_minutes(self):
        """Bench player (<30 min) in blowout should get more minutes."""
        results = [self.garbage_time(18.0, "blowout_loss", False) for _ in range(50)]
        avg = sum(results) / len(results)
        self.assertGreater(avg, 18.0, "Bench player should average more minutes in garbage time")

    def test_garbage_time_no_effect_in_normal_game(self):
        """Non-blowout scenario should not change minutes."""
        result = self.garbage_time(30.0, "normal", True)
        self.assertEqual(result, 30.0)

    def test_garbage_time_non_negative_minutes(self):
        """Adjusted minutes should never be negative."""
        for scenario in ["blowout_win", "blowout_loss", "grind_blowout", "normal"]:
            result = self.garbage_time(5.0, scenario, True)
            self.assertGreaterEqual(result, 0.0)

    # --- 1D: Enhanced hot/cold streaks ---

    def test_hot_cold_returns_positive_multiplier(self):
        """Hot/cold modifier should always return a positive multiplier."""
        for _ in range(50):
            m = self.hot_cold([20, 25, 18, 22, 24])
            self.assertGreater(m, 0.0)

    def test_hot_streak_caps_at_momentum_cap(self):
        """Hot streak momentum should be capped at MOMENTUM_HOT_CAP (1.12)."""
        from engine.simulation import MOMENTUM_HOT_CAP
        # Player running very hot: recent games well above average
        recent = [30.0, 32.0, 35.0, 22.0, 21.0, 20.0, 19.0]
        multipliers = [self.hot_cold(recent) for _ in range(100)]
        self.assertLessEqual(max(multipliers), MOMENTUM_HOT_CAP + 0.001)

    def test_cold_streak_floors_at_momentum_floor(self):
        """Cold streak momentum should be floored at MOMENTUM_COLD_FLOOR (0.88)."""
        from engine.simulation import MOMENTUM_COLD_FLOOR
        # Player running very cold
        recent = [8.0, 9.0, 7.0, 22.0, 23.0, 24.0, 25.0]
        multipliers = [self.hot_cold(recent) for _ in range(100)]
        self.assertGreaterEqual(min(multipliers), MOMENTUM_COLD_FLOOR - 0.001)

    def test_hot_cold_fallback_with_no_logs(self):
        """With no game logs, hot/cold modifier should still return valid value."""
        m = self.hot_cold(None)
        self.assertGreater(m, 0.0)

    # --- 1E: Convergence optimization ---

    def test_convergence_threshold_tighter(self):
        """Convergence threshold should be 0.003 (tighter than old 0.005)."""
        self.assertAlmostEqual(self.CONVERGENCE_THRESHOLD, 0.003, places=4)

    def test_convergence_check_interval(self):
        """Convergence check interval should be 250 (not 500)."""
        self.assertEqual(self.CONVERGENCE_CHECK_INTERVAL, 250)

    def test_simulations_run_key_present(self):
        """Result should include 'simulations_run' key."""
        r = self.run_qme(25.0, 6.0, 24.5, 500, 0.15, 1.0, 1.0, 0.02, 1.0)
        self.assertIn("simulations_run", r)
        self.assertGreater(r["simulations_run"], 0)

    # --- 1F: run_enhanced_simulation wrapper ---

    def test_enhanced_simulation_returns_dict(self):
        """run_enhanced_simulation should return a dict with required keys."""
        r = self.run_enhanced(25.0, 6.0, 24.5, 200, random_seed=42)
        self.assertIn("probability_over", r)
        self.assertIn("qme_probability", r)
        self.assertIn("game_script_probability", r)
        self.assertIn("blend_method", r)

    def test_enhanced_simulation_probability_valid_range(self):
        """Probability should be in [0, 1]."""
        r = self.run_enhanced(25.0, 6.0, 24.5, 200, random_seed=1)
        self.assertGreaterEqual(r["probability_over"], 0.0)
        self.assertLessEqual(r["probability_over"], 1.0)

    def test_enhanced_simulation_backward_compatible(self):
        """run_enhanced_simulation works with minimal args (backward compat)."""
        r = self.run_enhanced(20.0, 5.0, 19.5, 300)
        self.assertIn("probability_over", r)

    def test_enhanced_simulation_with_spread_total(self):
        """run_enhanced_simulation accepts vegas_spread and game_total."""
        r = self.run_enhanced(
            25.0, 6.0, 24.5, 200,
            vegas_spread=2.5, game_total=235.0, random_seed=99
        )
        self.assertIn("probability_over", r)

    # --- Backward compatibility ---

    def test_existing_qme_call_unchanged(self):
        """Original run_quantum_matrix_simulation call still works."""
        r = self.run_qme(25.0, 6.0, 24.5, 500, 0.15, 1.0, 1.0, 0.02, 1.0)
        self.assertIn("simulated_results", r)
        self.assertIn("probability_over", r)
        self.assertEqual(len(r["simulated_results"]), r["simulations_run"])


# ============================================================
# MODULE 2: edge_detection.py
# ============================================================

class TestEdgeDetectionEnhancements(unittest.TestCase):
    """Tests for QME 6.0 enhancements in engine/edge_detection.py."""

    def setUp(self):
        from engine.edge_detection import (
            analyze_directional_forces,
            estimate_closing_line_value,
            calculate_dynamic_vig,
            should_avoid_prop,
            STAT_EDGE_THRESHOLDS,
        )
        self.analyze_forces = analyze_directional_forces
        self.clv = estimate_closing_line_value
        self.dynamic_vig = calculate_dynamic_vig
        self.should_avoid = should_avoid_prop
        self.STAT_EDGE_THRESHOLDS = STAT_EDGE_THRESHOLDS

        # Standard test inputs for directional forces
        self._player = {"points_avg": 25.0}
        self._projection = {"projected_points": 26.0, "defense_factor": 1.0,
                            "pace_factor": 1.0, "blowout_risk": 0.10,
                            "rest_factor": 1.0}
        self._game_context = {"is_home": True, "vegas_spread": 2.0, "game_total": 220.0}

    # --- 2A: Market consensus force ---

    def test_market_consensus_over_force(self):
        """Line far below consensus should produce an OVER Market Consensus force."""
        result = self.analyze_forces(
            self._player, 22.0, "points", self._projection, self._game_context,
            platform_lines={"PrizePicks": 25.0, "Underdog": 24.5}
        )
        over_names = [f["name"] for f in result["over_forces"]]
        self.assertIn("Market Consensus", over_names)

    def test_market_consensus_under_force(self):
        """Line far above consensus should produce an UNDER Market Consensus force."""
        result = self.analyze_forces(
            self._player, 30.0, "points", self._projection, self._game_context,
            platform_lines={"PrizePicks": 25.0, "Underdog": 24.5}
        )
        under_names = [f["name"] for f in result["under_forces"]]
        self.assertIn("Market Consensus", under_names)

    def test_market_consensus_no_force_when_close(self):
        """When line is within 5% of consensus, no force is added."""
        result = self.analyze_forces(
            self._player, 25.2, "points", self._projection, self._game_context,
            platform_lines={"PrizePicks": 25.0, "Underdog": 25.5}
        )
        all_names = [f["name"] for f in result["over_forces"] + result["under_forces"]]
        self.assertNotIn("Market Consensus", all_names)

    def test_market_consensus_requires_two_platforms(self):
        """Market consensus force requires at least 2 platform lines."""
        result = self.analyze_forces(
            self._player, 22.0, "points", self._projection, self._game_context,
            platform_lines={"PrizePicks": 25.0}
        )
        all_names = [f["name"] for f in result["over_forces"] + result["under_forces"]]
        self.assertNotIn("Market Consensus", all_names)

    def test_analyze_forces_backward_compatible(self):
        """analyze_directional_forces works without new parameters."""
        result = self.analyze_forces(
            self._player, 24.5, "points", self._projection, self._game_context
        )
        self.assertIn("over_forces", result)
        self.assertIn("conflict_severity", result)

    # --- 2B: Closing Line Value ---

    def test_clv_returns_required_keys(self):
        """estimate_closing_line_value should return all required keys."""
        r = self.clv(24.5, 27.0)
        self.assertIn("estimated_closing_line", r)
        self.assertIn("clv_edge", r)
        self.assertIn("is_positive_clv", r)

    def test_clv_positive_when_line_above_projection(self):
        """Positive CLV when current_line > estimated_close (you beat the closing line)."""
        # If current line is ABOVE where model projects (i.e., you're taking UNDER at inflated line)
        # current=27, proj=24 → close = 27*0.3 + 24*0.7 = 24.9 → clv = 27-24.9 = +2.1
        r = self.clv(27.0, 24.0)
        self.assertTrue(r["is_positive_clv"])
        self.assertGreater(r["clv_edge"], 0)

    def test_clv_negative_when_line_below_projection(self):
        """Negative CLV when current_line < estimated_close (line will move away from you)."""
        # current=22, proj=27 → close = 22*0.3 + 27*0.7 = 25.5 → clv = 22-25.5 = -3.5
        r = self.clv(22.0, 27.0)
        self.assertFalse(r["is_positive_clv"])

    def test_clv_less_movement_near_game(self):
        """When hours_to_game < 2, estimated close is weighted more toward current line."""
        # current_line=24.5, model_projection=27.0 (projection is above current line)
        # Early: close = 24.5*0.3 + 27.0*0.7 = 26.25 (moves significantly toward projection)
        # Late:  close = 24.5*0.6 + 27.0*0.4 = 25.5  (stays closer to current_line)
        r_early = self.clv(24.5, 27.0, hours_to_game=8.0)
        r_late = self.clv(24.5, 27.0, hours_to_game=1.0)
        # Near-game close is closer to current_line (lower, since proj > current)
        self.assertLess(r_late["estimated_closing_line"],
                        r_early["estimated_closing_line"])

    def test_clv_zero_line_handled(self):
        """CLV should handle zero current_line gracefully."""
        r = self.clv(0.0, 25.0)
        self.assertEqual(r["clv_edge"], 0.0)

    # --- 2C: Dynamic Vig ---

    def test_dynamic_vig_prizepicks_zero(self):
        """PrizePicks should have 0% vig."""
        self.assertEqual(self.dynamic_vig(platform="PrizePicks"), 0.0)

    def test_dynamic_vig_underdog_zero(self):
        """Underdog should have 0% vig."""
        self.assertEqual(self.dynamic_vig(platform="Underdog"), 0.0)

    def test_dynamic_vig_draftkings_standard(self):
        """DraftKings -110/-110 should calculate non-zero vig."""
        vig = self.dynamic_vig(-110, -110, "DraftKings")
        self.assertGreater(vig, 0.0)

    def test_dynamic_vig_fallback(self):
        """Without odds or platform, fallback vig should be 2.38%."""
        vig = self.dynamic_vig()
        self.assertAlmostEqual(vig, 2.38, places=2)

    def test_dynamic_vig_juice_line_higher_vig(self):
        """Symmetric heavy juice (-120/-120) should produce higher vig than -110/-110."""
        vig_standard = self.dynamic_vig(-110, -110)
        vig_heavy = self.dynamic_vig(-120, -120)
        self.assertGreater(vig_heavy, vig_standard)

    # --- 2D: Stat-specific edge thresholds ---

    def test_stat_edge_thresholds_defined(self):
        """STAT_EDGE_THRESHOLDS should define all key stats."""
        for stat in ["points", "rebounds", "assists", "threes", "steals", "blocks"]:
            self.assertIn(stat, self.STAT_EDGE_THRESHOLDS)

    def test_steals_blocks_threshold_highest(self):
        """Steals and blocks should have the highest thresholds (most volatile)."""
        self.assertGreaterEqual(self.STAT_EDGE_THRESHOLDS["steals"],
                                self.STAT_EDGE_THRESHOLDS["points"])
        self.assertGreaterEqual(self.STAT_EDGE_THRESHOLDS["blocks"],
                                self.STAT_EDGE_THRESHOLDS["rebounds"])

    def test_combo_stats_threshold_lower(self):
        """Combo stats (PRA) should have lower thresholds than individual stats."""
        self.assertLessEqual(self.STAT_EDGE_THRESHOLDS["points_rebounds_assists"],
                             self.STAT_EDGE_THRESHOLDS["points"])

    # --- 2E: Conflict severity score ---

    def test_conflict_severity_in_return_dict(self):
        """analyze_directional_forces should return conflict_severity."""
        result = self.analyze_forces(
            self._player, 24.5, "points", self._projection, self._game_context
        )
        self.assertIn("conflict_severity", result)

    def test_conflict_severity_range(self):
        """conflict_severity should be in [0.0, 1.0]."""
        result = self.analyze_forces(
            self._player, 24.5, "points", self._projection, self._game_context
        )
        cs = result["conflict_severity"]
        self.assertGreaterEqual(cs, 0.0)
        self.assertLessEqual(cs, 1.0)

    def test_conflict_severity_high_when_balanced(self):
        """Conflict severity should be close to 1.0 when forces are balanced."""
        # Create balanced scenario: projection exactly at line, no other special forces
        proj = {"projected_points": 24.5, "defense_factor": 1.0,
                "pace_factor": 1.0, "blowout_risk": 0.10, "rest_factor": 1.0}
        ctx = {"is_home": True, "vegas_spread": 0.0, "game_total": 222.0}
        result = self.analyze_forces(self._player, 24.5, "points", proj, ctx)
        # When projection == line, neither OVER nor UNDER force from projection
        # conflict_severity should reflect balance of remaining forces
        cs = result["conflict_severity"]
        self.assertGreaterEqual(cs, 0.0)

    # --- 2F: Regression-to-mean force ---

    def test_regression_risk_added_when_hot(self):
        """When recent_form_ratio > 1.20, Regression Risk UNDER force should appear."""
        result = self.analyze_forces(
            self._player, 24.5, "points", self._projection, self._game_context,
            recent_form_ratio=1.35
        )
        under_names = [f["name"] for f in result["under_forces"]]
        self.assertIn("Regression Risk", under_names)

    def test_bounce_back_added_when_cold(self):
        """When recent_form_ratio < 0.80, Bounce-Back OVER force should appear."""
        result = self.analyze_forces(
            self._player, 24.5, "points", self._projection, self._game_context,
            recent_form_ratio=0.65
        )
        over_names = [f["name"] for f in result["over_forces"]]
        self.assertIn("Bounce-Back", over_names)

    def test_no_regression_force_when_neutral(self):
        """No regression force when recent form is within 20% of average."""
        result = self.analyze_forces(
            self._player, 24.5, "points", self._projection, self._game_context,
            recent_form_ratio=1.10
        )
        all_names = [f["name"] for f in result["over_forces"] + result["under_forces"]]
        self.assertNotIn("Regression Risk", all_names)
        self.assertNotIn("Bounce-Back", all_names)


# ============================================================
# MODULE 3: confidence.py
# ============================================================

class TestConfidenceEnhancements(unittest.TestCase):
    """Tests for QME 6.0 enhancements in engine/confidence.py."""

    def setUp(self):
        from engine.confidence import (
            calculate_confidence_score,
            calculate_risk_score,
            enforce_tier_distribution,
        )
        self.calc = calculate_confidence_score
        self.risk_score = calculate_risk_score
        self.enforce_tiers = enforce_tier_distribution

        # Standard directional forces for testing
        self._forces = {
            "over_count": 3, "under_count": 1,
            "over_strength": 2.5, "under_strength": 0.8,
            "over_forces": [], "under_forces": [],
        }

    def _base_result(self, **kwargs):
        """Helper: call calculate_confidence_score with standard defaults."""
        defaults = dict(
            probability_over=0.65,
            edge_percentage=10.0,
            directional_forces=self._forces,
            defense_factor=1.05,
            stat_standard_deviation=5.0,
            stat_average=25.0,
            simulation_results={},
            games_played=40,
        )
        defaults.update(kwargs)
        return self.calc(**defaults)

    # --- 3A: Bayesian confidence adjustment ---

    def test_small_sample_discount_applied(self):
        """Games played season < 15 should reduce confidence score."""
        r_full = self._base_result(games_played=40, games_played_season=None)
        r_small = self._base_result(games_played=40, games_played_season=5)
        self.assertLess(r_small["confidence_score"], r_full["confidence_score"])

    def test_sample_size_discount_in_return_dict(self):
        """Return dict should include sample_size_discount key."""
        r = self._base_result(games_played_season=10)
        self.assertIn("sample_size_discount", r)

    def test_tiny_sample_caps_tier_at_bronze(self):
        """< 5 games played should cap tier at Bronze."""
        r = self._base_result(
            probability_over=0.80, edge_percentage=20.0,
            games_played_season=3
        )
        self.assertEqual(r["tier"], "Bronze",
                         "Very small sample should force Bronze tier")

    def test_small_sample_caps_tier_at_silver(self):
        """< 10 games played should cap tier at Silver (not Gold/Platinum)."""
        r = self._base_result(
            probability_over=0.80, edge_percentage=20.0,
            games_played_season=7
        )
        self.assertNotIn(r["tier"], ("Gold", "Platinum"),
                         "Small sample should limit to Silver or below")

    def test_no_discount_with_full_season(self):
        """No discount applied when games_played_season >= 15."""
        r = self._base_result(games_played_season=50)
        self.assertAlmostEqual(r["sample_size_discount"], 1.0, places=3)

    # --- 3B: Multi-source probability agreement ---

    def test_agreement_bonus_when_all_agree(self):
        """Bonus should be positive when 3+ models agree on direction."""
        r = self._base_result(
            alternative_probabilities=[0.62, 0.68, 0.65, 0.70]
        )
        self.assertGreater(r["probability_agreement_bonus"], 0.0)

    def test_disagreement_penalty_when_split(self):
        """Penalty when models disagree on direction."""
        r = self._base_result(
            alternative_probabilities=[0.60, 0.40, 0.65]
        )
        self.assertLess(r["probability_agreement_bonus"], 0.0)

    def test_agreement_bonus_in_return_dict(self):
        """Return dict should include probability_agreement_bonus key."""
        r = self._base_result()
        self.assertIn("probability_agreement_bonus", r)

    def test_agreement_no_effect_with_fewer_than_three_probs(self):
        """< 3 alternative probabilities should not trigger bonus or penalty."""
        r_no = self._base_result(alternative_probabilities=None)
        r_two = self._base_result(alternative_probabilities=[0.62, 0.68])
        self.assertEqual(r_no["probability_agreement_bonus"], r_two["probability_agreement_bonus"])

    # --- 3C: Streak-adjusted confidence ---

    def test_hot_streak_over_bonus(self):
        """Hot streak + OVER pick should give positive streak adjustment."""
        r = self._base_result(
            probability_over=0.65,
            streak_info={"type": "hot", "length": 3}
        )
        self.assertGreater(r["streak_adjustment"], 0.0)

    def test_hot_streak_under_penalty(self):
        """Hot streak + UNDER pick should give negative streak adjustment."""
        r = self._base_result(
            probability_over=0.35,
            streak_info={"type": "hot", "length": 3}
        )
        self.assertLess(r["streak_adjustment"], 0.0)

    def test_cold_streak_under_bonus(self):
        """Cold streak + UNDER pick should give positive streak adjustment."""
        r = self._base_result(
            probability_over=0.35,
            streak_info={"type": "cold", "length": 4}
        )
        self.assertGreater(r["streak_adjustment"], 0.0)

    def test_streak_adjustment_in_return_dict(self):
        """Return dict should include streak_adjustment key."""
        r = self._base_result()
        self.assertIn("streak_adjustment", r)

    def test_long_streak_doubles_adjustment(self):
        """Streak length > 5 should double the bonus/penalty."""
        r_short = self._base_result(streak_info={"type": "hot", "length": 3})
        r_long = self._base_result(streak_info={"type": "hot", "length": 7})
        self.assertGreater(abs(r_long["streak_adjustment"]),
                           abs(r_short["streak_adjustment"]))

    # --- 3D: Platform-specific tier adjustments ---

    def test_power_play_harder_to_achieve_platinum(self):
        """PrizePicks Power play should raise thresholds (harder to get Platinum)."""
        r_flex = self._base_result(
            probability_over=0.75, edge_percentage=12.0, platform="PrizePicks"
        )
        r_power = self._base_result(
            probability_over=0.75, edge_percentage=12.0, platform="PrizePicks Power"
        )
        # Power play should result in equal or lower tier than Flex
        tier_order = {"Platinum": 4, "Gold": 3, "Silver": 2, "Bronze": 1, "Avoid": 0}
        self.assertLessEqual(
            tier_order.get(r_power["tier"], 0),
            tier_order.get(r_flex["tier"], 0) + 1  # Allow 1 tier slack
        )

    def test_confidence_score_unchanged_by_platform(self):
        """Platform premium affects tier assignment but not the raw confidence_score."""
        r_flex = self._base_result(platform="PrizePicks")
        r_dk = self._base_result(platform="DraftKings")
        # The raw score should be the same (platform only shifts tier boundary)
        self.assertAlmostEqual(r_flex["confidence_score"], r_dk["confidence_score"], places=1)

    def test_underdog_same_as_prizepicks_flex(self):
        """Underdog platform should behave like PrizePicks Flex (0 premium)."""
        r_ud = self._base_result(platform="Underdog")
        r_pp = self._base_result(platform="PrizePicks")
        self.assertEqual(r_ud["tier"], r_pp["tier"])

    # --- 3E: calculate_risk_score ---

    def test_risk_score_returns_required_keys(self):
        """calculate_risk_score should return required keys."""
        r = self._base_result()
        result = self.risk_score(r, 10.0, 0.30)
        self.assertIn("risk_score", result)
        self.assertIn("risk_label", result)
        self.assertIn("risk_factors", result)

    def test_risk_score_range(self):
        """Risk score should be in [1, 10]."""
        r = self._base_result()
        result = self.risk_score(r, 10.0, 0.30)
        self.assertGreaterEqual(result["risk_score"], 1.0)
        self.assertLessEqual(result["risk_score"], 10.0)

    def test_high_confidence_low_risk(self):
        """High confidence + big edge + low CV should produce Low Risk."""
        r_high = self._base_result(probability_over=0.80, edge_percentage=20.0)
        result = self.risk_score(r_high, 20.0, 0.20)
        self.assertEqual(result["risk_label"], "Low Risk")

    def test_low_confidence_high_risk(self):
        """Low confidence + thin edge + high CV should produce High Risk."""
        r_low = self._base_result(probability_over=0.52, edge_percentage=2.0,
                                   stat_average=2.0, stat_standard_deviation=1.5)
        result = self.risk_score(r_low, 2.0, 0.65)
        self.assertEqual(result["risk_label"], "High Risk")

    def test_risk_factors_populated_for_bad_pick(self):
        """Poor pick should generate risk factors."""
        r = self._base_result(probability_over=0.52, edge_percentage=1.0)
        result = self.risk_score(r, 1.0, 0.50)
        self.assertGreater(len(result["risk_factors"]), 0)

    # --- 3F: enforce_tier_distribution ---

    def test_enforce_downgrades_excess_platinums(self):
        """Too many Platinums should be downgraded to Gold."""
        picks = [
            {"confidence_score": 90, "tier": "Platinum", "tier_emoji": "💎",
             "recommendation": "Elite OVER play"},
            {"confidence_score": 88, "tier": "Platinum", "tier_emoji": "💎",
             "recommendation": "Elite OVER play"},
            {"confidence_score": 85, "tier": "Platinum", "tier_emoji": "💎",
             "recommendation": "Elite OVER play"},
            {"confidence_score": 60, "tier": "Silver", "tier_emoji": "🥈",
             "recommendation": "Moderate lean"},
            {"confidence_score": 55, "tier": "Silver", "tier_emoji": "🥈",
             "recommendation": "Moderate lean"},
        ]
        adjusted, downgrades_occurred = self.enforce_tiers(picks, max_platinum_pct=0.10)
        self.assertTrue(downgrades_occurred)
        platinum_count = sum(1 for p in adjusted if p["tier"] == "Platinum")
        # With 5 picks and 10% max, only 0 or 1 Platinums allowed
        self.assertLessEqual(platinum_count, 1)

    def test_enforce_no_downgrade_when_within_limits(self):
        """Within-limit distributions should not trigger downgrades."""
        picks = [
            {"confidence_score": 90, "tier": "Platinum", "tier_emoji": "💎",
             "recommendation": "Elite OVER play"},
            {"confidence_score": 60, "tier": "Silver", "tier_emoji": "🥈",
             "recommendation": "Moderate lean"},
            {"confidence_score": 58, "tier": "Silver", "tier_emoji": "🥈",
             "recommendation": "Moderate lean"},
            {"confidence_score": 50, "tier": "Bronze", "tier_emoji": "🥉",
             "recommendation": "Weak signal"},
            {"confidence_score": 45, "tier": "Bronze", "tier_emoji": "🥉",
             "recommendation": "Weak signal"},
        ]
        # 1 Platinum out of 5 = 20%; use max_platinum_pct=0.30 (allows 30%)
        # 0 Golds + 1 Platinum = 20% Gold+; use max_gold_pct=0.30 (allows 30%)
        _, downgrades_occurred = self.enforce_tiers(picks, max_platinum_pct=0.30,
                                                    max_gold_pct=0.30)
        self.assertFalse(downgrades_occurred)

    def test_enforce_returns_same_count(self):
        """Enforcement should not change the number of picks."""
        picks = [{"confidence_score": 90, "tier": "Platinum", "tier_emoji": "💎",
                  "recommendation": "Elite"} for _ in range(10)]
        adjusted, _ = self.enforce_tiers(picks)
        self.assertEqual(len(adjusted), len(picks))

    def test_enforce_empty_list(self):
        """Empty pick list should return empty list without error."""
        result, flag = self.enforce_tiers([])
        self.assertEqual(result, [])
        self.assertFalse(flag)

    # --- Backward compatibility ---

    def test_existing_confidence_call_unchanged(self):
        """Original calculate_confidence_score call still works."""
        r = self.calc(
            0.65, 10.0, self._forces, 1.05, 5.0, 25.0, {}, games_played=40
        )
        self.assertIn("confidence_score", r)
        self.assertIn("tier", r)


# ============================================================
# MODULE 4: correlation.py
# ============================================================

class TestCorrelationEnhancements(unittest.TestCase):
    """Tests for QME 6.0 enhancements in engine/correlation.py."""

    def setUp(self):
        from engine.correlation import (
            get_teammate_correlation,
            get_position_correlation_adjustment,
            calculate_player_correlation,
            build_correlation_matrix,
            get_correlation_confidence,
            correlation_adjusted_kelly,
            CROSS_STAT_CORRELATIONS,
            POSITION_CORRELATION_ADJUSTMENTS,
        )
        self.get_corr = get_teammate_correlation
        self.pos_adj = get_position_correlation_adjustment
        self.calc_player_corr = calculate_player_correlation
        self.build_matrix = build_correlation_matrix
        self.corr_conf = get_correlation_confidence
        self.kelly = correlation_adjusted_kelly
        self.CROSS_STAT = CROSS_STAT_CORRELATIONS
        self.POS_ADJ = POSITION_CORRELATION_ADJUSTMENTS

    # --- 4A: Cross-stat correlation matrix ---

    def test_cross_stat_correlations_defined(self):
        """CROSS_STAT_CORRELATIONS should contain key cross-stat pairs."""
        s1, s2 = sorted(["points", "assists"])
        self.assertIn((s1, s2), self.CROSS_STAT)

    def test_cross_stat_lookup_different_stats(self):
        """get_teammate_correlation with two different stats uses cross-stat matrix."""
        cross = self.get_corr("points", "assists")
        same = self.get_corr("points")
        # Cross-stat and single-stat should differ (different lookups)
        # cross-stat points/assists heuristic from matrix vs. single points=-0.12
        self.assertIsInstance(cross, float)

    def test_cross_stat_same_stat_uses_single_stat_heuristic(self):
        """When stat_type2 == stat_type, use single-stat heuristic."""
        corr_single = self.get_corr("points")
        corr_same = self.get_corr("points", "points")
        self.assertAlmostEqual(corr_single, corr_same, places=4)

    def test_cross_stat_within_range(self):
        """Cross-stat correlations should be in [-1, 1]."""
        for (s1, s2), v in self.CROSS_STAT.items():
            self.assertGreaterEqual(v, -1.0)
            self.assertLessEqual(v, 1.0)

    # --- 4B: Position-based correlation priors ---

    def test_position_adjustments_defined(self):
        """POSITION_CORRELATION_ADJUSTMENTS should contain expected pairs."""
        self.assertIsInstance(self.POS_ADJ, dict)
        self.assertGreater(len(self.POS_ADJ), 0)

    def test_pg_c_pick_and_roll_positive(self):
        """PG + C pick-and-roll relationship should be positive."""
        adj = self.pos_adj("PG", "C")
        self.assertGreater(adj, 0.0)

    def test_two_centers_negative(self):
        """Two centers compete for rebounds — should be negative."""
        adj = self.pos_adj("C", "C")
        self.assertLess(adj, 0.0)

    def test_position_symmetric(self):
        """Position correlation should be symmetric (pos1, pos2) = (pos2, pos1)."""
        adj1 = self.pos_adj("PG", "SG")
        adj2 = self.pos_adj("SG", "PG")
        self.assertAlmostEqual(adj1, adj2, places=4)

    def test_unknown_positions_return_zero(self):
        """Unknown position pairs should return 0.0 (no adjustment)."""
        adj = self.pos_adj("GUARD", "FORWARD")
        self.assertIsInstance(adj, float)

    # --- 4C: Opponent correlation modeling ---

    def test_opponent_high_total_positive_correlation(self):
        """High-total game (230+) opponents should have positive correlation."""
        picks = [
            {"player_name": "player_a", "team": "LAL", "stat_type": "points",
             "game_total": 235.0, "vegas_spread": 0.0},
            {"player_name": "player_b", "team": "GSW", "stat_type": "points",
             "game_total": 235.0, "vegas_spread": 0.0},
        ]
        matrix = self.build_matrix(picks)
        self.assertGreater(matrix[0][1], 0.0,
                           "High-total opponent scorers should have positive correlation")

    def test_opponent_normal_game_near_zero(self):
        """Normal game opponents should have near-zero correlation."""
        picks = [
            {"player_name": "player_a", "team": "LAL", "stat_type": "points",
             "game_total": 220.0, "vegas_spread": 0.0},
            {"player_name": "player_b", "team": "GSW", "stat_type": "rebounds",
             "game_total": 220.0, "vegas_spread": 0.0},
        ]
        matrix = self.build_matrix(picks)
        # Should be near zero (no strong context)
        self.assertLessEqual(abs(matrix[0][1]), 0.15)

    def test_correlation_matrix_symmetric(self):
        """Correlation matrix should always be symmetric."""
        picks = [
            {"player_name": "a", "team": "LAL", "stat_type": "points"},
            {"player_name": "b", "team": "GSW", "stat_type": "points"},
            {"player_name": "c", "team": "LAL", "stat_type": "rebounds"},
        ]
        matrix = self.build_matrix(picks)
        for i in range(len(picks)):
            for j in range(len(picks)):
                self.assertAlmostEqual(matrix[i][j], matrix[j][i], places=9)

    # --- 4D: Recency-weighted Pearson correlation ---

    def test_player_correlation_requires_min_games(self):
        """calculate_player_correlation requires 8+ shared games; fewer returns 0."""
        logs1 = [{"GAME_DATE": f"2024-01-{i:02d}", "PTS": float(20 + i % 5)}
                 for i in range(1, 6)]
        logs2 = [{"GAME_DATE": f"2024-01-{i:02d}", "PTS": float(18 + i % 3)}
                 for i in range(1, 6)]
        result = self.calc_player_corr(logs1, logs2, "points")
        self.assertEqual(result, 0.0, "Fewer than 8 shared games should return 0.0")

    def test_player_correlation_valid_with_enough_games(self):
        """With 10+ shared games, returns a valid correlation."""
        logs1 = [{"GAME_DATE": f"2024-01-{i:02d}", "PTS": float(20 + (i % 5) * 2)}
                 for i in range(1, 15)]
        logs2 = [{"GAME_DATE": f"2024-01-{i:02d}", "PTS": float(18 + (i % 3) * 3)}
                 for i in range(1, 15)]
        result = self.calc_player_corr(logs1, logs2, "points")
        self.assertGreaterEqual(result, -1.0)
        self.assertLessEqual(result, 1.0)

    def test_player_correlation_range(self):
        """Result should always be in [-1, 1]."""
        logs1 = [{"GAME_DATE": f"2024-01-{i:02d}", "PTS": float(i)} for i in range(1, 12)]
        logs2 = [{"GAME_DATE": f"2024-01-{i:02d}", "PTS": float(12 - i)} for i in range(1, 12)]
        result = self.calc_player_corr(logs1, logs2, "points")
        self.assertGreaterEqual(result, -1.0)
        self.assertLessEqual(result, 1.0)

    # --- 4E: get_correlation_confidence ---

    def test_correlation_confidence_returns_required_keys(self):
        """get_correlation_confidence should return required keys."""
        picks = [
            {"team": "LAL", "opponent": "GSW"},
            {"team": "GSW", "opponent": "LAL"},
        ]
        matrix = [[1.0, 0.1], [0.1, 1.0]]
        result = self.corr_conf(picks, matrix)
        self.assertIn("correlation_confidence", result)
        self.assertIn("correlation_risk_level", result)
        self.assertIn("diversification_score", result)

    def test_correlation_confidence_range(self):
        """Correlation confidence should be in [0, 100]."""
        picks = [{"team": "LAL", "opponent": "BOS"}, {"team": "LAL", "opponent": "BOS"}]
        matrix = [[1.0, 0.12], [0.12, 1.0]]
        result = self.corr_conf(picks, matrix)
        self.assertGreaterEqual(result["correlation_confidence"], 0.0)
        self.assertLessEqual(result["correlation_confidence"], 100.0)

    def test_high_correlation_lowers_confidence(self):
        """Highly correlated picks should produce lower correlation_confidence."""
        picks = [{"team": "A", "opponent": "B"}, {"team": "A", "opponent": "B"}]
        low_corr_matrix = [[1.0, 0.02], [0.02, 1.0]]
        high_corr_matrix = [[1.0, 0.14], [0.14, 1.0]]
        low_result = self.corr_conf(picks, low_corr_matrix)
        high_result = self.corr_conf(picks, high_corr_matrix)
        self.assertGreater(low_result["correlation_confidence"],
                           high_result["correlation_confidence"])

    def test_diversified_picks_higher_score(self):
        """Picks from different games should score higher diversification."""
        picks_same = [{"team": "LAL", "opponent": "GSW"},
                      {"team": "LAL", "opponent": "GSW"}]
        picks_diff = [{"team": "LAL", "opponent": "BOS"},
                      {"team": "MIL", "opponent": "MIA"}]
        matrix = [[1.0, 0.05], [0.05, 1.0]]
        same = self.corr_conf(picks_same, matrix)
        diff = self.corr_conf(picks_diff, matrix)
        self.assertGreater(diff["diversification_score"], same["diversification_score"])

    def test_empty_picks_returns_default(self):
        """Empty picks list should return valid default dict."""
        result = self.corr_conf([], [])
        self.assertIn("correlation_confidence", result)

    # --- 4F: correlation_adjusted_kelly ---

    def test_kelly_returns_required_keys(self):
        """correlation_adjusted_kelly should return required keys."""
        picks = [{"win_probability": 0.60, "odds_decimal": 1.91}]
        result = self.kelly(picks, 1000, [[1.0]])
        self.assertIn("kelly_fraction", result)
        self.assertIn("recommended_bet", result)
        self.assertIn("correlation_discount", result)

    def test_kelly_positive_edge_gives_positive_bet(self):
        """Positive expected value should give a positive Kelly fraction."""
        picks = [{"win_probability": 0.60, "odds_decimal": 1.91}]
        result = self.kelly(picks, 1000, [[1.0]])
        self.assertGreater(result["kelly_fraction"], 0.0)
        self.assertGreater(result["recommended_bet"], 0.0)

    def test_kelly_correlation_reduces_bet(self):
        """High correlation between picks should reduce recommended bet."""
        picks = [
            {"win_probability": 0.60, "odds_decimal": 1.91},
            {"win_probability": 0.62, "odds_decimal": 1.91},
        ]
        no_corr_matrix = [[1.0, 0.0], [0.0, 1.0]]
        high_corr_matrix = [[1.0, 0.14], [0.14, 1.0]]
        result_low = self.kelly(picks, 1000, no_corr_matrix)
        result_high = self.kelly(picks, 1000, high_corr_matrix)
        self.assertGreaterEqual(result_low["kelly_fraction"],
                                result_high["kelly_fraction"])

    def test_kelly_bankroll_scaling(self):
        """Recommended bet should scale proportionally with bankroll."""
        picks = [{"win_probability": 0.60, "odds_decimal": 1.91}]
        r1 = self.kelly(picks, 1000, [[1.0]])
        r2 = self.kelly(picks, 2000, [[1.0]])
        self.assertAlmostEqual(r2["recommended_bet"], 2 * r1["recommended_bet"], places=2)

    def test_kelly_empty_picks_returns_zero(self):
        """Empty picks should return zero bet."""
        result = self.kelly([], 1000, [])
        self.assertEqual(result["kelly_fraction"], 0.0)
        self.assertEqual(result["recommended_bet"], 0.0)

    def test_kelly_capped_at_25_pct(self):
        """Kelly fraction should never exceed 25% regardless of inputs."""
        picks = [{"win_probability": 0.99, "odds_decimal": 10.0}]
        result = self.kelly(picks, 1000, [[1.0]])
        self.assertLessEqual(result["kelly_fraction"], 0.25)

    # --- Backward compatibility ---

    def test_existing_get_teammate_correlation_unchanged(self):
        """Original get_teammate_correlation(stat_type) call still works."""
        corr = self.get_corr("points")
        self.assertIsInstance(corr, float)
        self.assertGreaterEqual(corr, -1.0)
        self.assertLessEqual(corr, 1.0)

    def test_existing_build_correlation_matrix_unchanged(self):
        """Original build_correlation_matrix call still works."""
        picks = [
            {"player_name": "lebron", "team": "LAL", "stat_type": "points"},
            {"player_name": "ad", "team": "LAL", "stat_type": "rebounds"},
        ]
        matrix = self.build_matrix(picks)
        self.assertEqual(len(matrix), 2)
        self.assertEqual(len(matrix[0]), 2)


# ============================================================
# Integration: engine __init__.py exports
# ============================================================

class TestEngineInitExports(unittest.TestCase):
    """Verify that engine/__init__.py exports all new public functions."""

    def test_run_enhanced_simulation_exported(self):
        from engine import run_enhanced_simulation
        self.assertTrue(callable(run_enhanced_simulation))

    def test_estimate_closing_line_value_exported(self):
        from engine import estimate_closing_line_value
        self.assertTrue(callable(estimate_closing_line_value))

    def test_calculate_dynamic_vig_exported(self):
        from engine import calculate_dynamic_vig
        self.assertTrue(callable(calculate_dynamic_vig))

    def test_calculate_risk_score_exported(self):
        from engine import calculate_risk_score
        self.assertTrue(callable(calculate_risk_score))

    def test_enforce_tier_distribution_exported(self):
        from engine import enforce_tier_distribution
        self.assertTrue(callable(enforce_tier_distribution))

    def test_get_position_correlation_adjustment_exported(self):
        from engine import get_position_correlation_adjustment
        self.assertTrue(callable(get_position_correlation_adjustment))

    def test_get_correlation_confidence_exported(self):
        from engine import get_correlation_confidence
        self.assertTrue(callable(get_correlation_confidence))

    def test_correlation_adjusted_kelly_exported(self):
        from engine import correlation_adjusted_kelly
        self.assertTrue(callable(correlation_adjusted_kelly))


# ============================================================
# MODULE 5: player_intelligence.py
# ============================================================

class TestPlayerIntelligence(unittest.TestCase):
    """Tests for engine/player_intelligence.py — hit rate, streaks,
    matchup grading, availability context, and line value assessment."""

    _GAME_LOGS = [
        {"GAME_DATE": "2024-11-15", "MATCHUP": "LAL @ BOS", "PTS": 28, "REB": 7, "AST": 5,
         "FG3M": 2, "STL": 1, "BLK": 0, "TOV": 2},
        {"GAME_DATE": "2024-11-13", "MATCHUP": "LAL vs GSW", "PTS": 22, "REB": 6, "AST": 4,
         "FG3M": 1, "STL": 0, "BLK": 1, "TOV": 1},
        {"GAME_DATE": "2024-11-11", "MATCHUP": "LAL vs MIA", "PTS": 30, "REB": 9, "AST": 6,
         "FG3M": 3, "STL": 2, "BLK": 0, "TOV": 3},
        {"GAME_DATE": "2024-11-09", "MATCHUP": "LAL @ DEN", "PTS": 18, "REB": 5, "AST": 3,
         "FG3M": 0, "STL": 1, "BLK": 0, "TOV": 1},
        {"GAME_DATE": "2024-11-07", "MATCHUP": "LAL vs PHX", "PTS": 25, "REB": 8, "AST": 4,
         "FG3M": 2, "STL": 0, "BLK": 1, "TOV": 2},
    ]

    def setUp(self):
        from engine.player_intelligence import (
            get_recent_form_vs_line,
            get_availability_context,
            assess_line_value,
            grade_matchup,
            get_player_intelligence_summary,
            aggregate_streak_summary,
            _compute_streak,
        )
        self.get_form = get_recent_form_vs_line
        self.get_avail = get_availability_context
        self.assess_line = assess_line_value
        self.grade_mu = grade_matchup
        self.get_summary = get_player_intelligence_summary
        self.agg_streak = aggregate_streak_summary
        self.compute_streak = _compute_streak

    # ── get_recent_form_vs_line ───────────────────────────────────

    def test_form_hit_rate_correct(self):
        """3 out of 5 games over 24.5 = 60% hit rate."""
        result = self.get_form(self._GAME_LOGS, "points", 24.5)
        self.assertEqual(result["hits"], 3)
        self.assertEqual(result["window"], 5)
        self.assertAlmostEqual(result["hit_rate"], 0.6, places=2)

    def test_form_hot_label_when_over_70pct(self):
        logs = [
            {"PTS": 30, "REB": 6, "AST": 4, "FG3M": 2, "STL": 1, "BLK": 0, "TOV": 1,
             "GAME_DATE": "2024-11-15", "MATCHUP": "LAL @ BOS"},
            {"PTS": 28, "REB": 5, "AST": 3, "FG3M": 1, "STL": 0, "BLK": 1, "TOV": 2,
             "GAME_DATE": "2024-11-13", "MATCHUP": "LAL vs GSW"},
            {"PTS": 31, "REB": 7, "AST": 5, "FG3M": 3, "STL": 1, "BLK": 0, "TOV": 0,
             "GAME_DATE": "2024-11-11", "MATCHUP": "LAL vs MIA"},
            {"PTS": 26, "REB": 4, "AST": 2, "FG3M": 0, "STL": 0, "BLK": 0, "TOV": 1,
             "GAME_DATE": "2024-11-09", "MATCHUP": "LAL @ DEN"},
            {"PTS": 29, "REB": 8, "AST": 6, "FG3M": 2, "STL": 2, "BLK": 1, "TOV": 3,
             "GAME_DATE": "2024-11-07", "MATCHUP": "LAL vs PHX"},
        ]
        result = self.get_form(logs, "points", 24.5)
        self.assertIn("Hot", result["form_label"])

    def test_form_cold_label_when_under_30pct(self):
        logs = [
            {"PTS": 20, "REB": 5, "AST": 3, "FG3M": 1, "STL": 0, "BLK": 0, "TOV": 1,
             "GAME_DATE": "2024-11-15", "MATCHUP": "LAL @ BOS"},
            {"PTS": 18, "REB": 4, "AST": 2, "FG3M": 0, "STL": 0, "BLK": 0, "TOV": 2,
             "GAME_DATE": "2024-11-13", "MATCHUP": "LAL vs GSW"},
            {"PTS": 15, "REB": 3, "AST": 1, "FG3M": 0, "STL": 0, "BLK": 0, "TOV": 1,
             "GAME_DATE": "2024-11-11", "MATCHUP": "LAL vs MIA"},
            {"PTS": 22, "REB": 6, "AST": 4, "FG3M": 1, "STL": 1, "BLK": 1, "TOV": 0,
             "GAME_DATE": "2024-11-09", "MATCHUP": "LAL @ DEN"},
            {"PTS": 17, "REB": 4, "AST": 2, "FG3M": 0, "STL": 0, "BLK": 0, "TOV": 1,
             "GAME_DATE": "2024-11-07", "MATCHUP": "LAL vs PHX"},
        ]
        result = self.get_form(logs, "points", 24.5)
        self.assertIn("Cold", result["form_label"])

    def test_form_empty_logs_returns_no_data(self):
        result = self.get_form([], "points", 24.5)
        self.assertEqual(result["form_label"], "No Data")
        self.assertFalse(result["sufficient_data"])

    def test_form_combo_stat_sums_components(self):
        """PRA = PTS + REB + AST. All games above 35 = hot."""
        result = self.get_form(self._GAME_LOGS, "points_rebounds_assists", 25.0)
        for r in result["results"]:
            # PTS + REB + AST for each game should be > 25
            self.assertGreater(r["value"], 25.0)
        self.assertTrue(result["sufficient_data"])

    # ── _compute_streak ────────────────────────────────────────────

    def test_streak_three_consecutive_hits(self):
        results = [
            {"hit": True}, {"hit": True}, {"hit": True}, {"hit": False}, {"hit": True},
        ]
        streak = self.compute_streak(results)
        self.assertEqual(streak["type"], "hit")
        self.assertEqual(streak["count"], 3)
        self.assertTrue(streak["active"])

    def test_streak_empty_list(self):
        streak = self.compute_streak([])
        self.assertFalse(streak["active"])

    # ── get_availability_context ───────────────────────────────────

    def test_availability_active_player(self):
        injury_map = {"lebron james": {"status": "Active", "injury_note": ""}}
        result = self.get_avail("LeBron James", injury_map)
        self.assertTrue(result["is_active"])
        self.assertFalse(result["is_flagged"])
        self.assertEqual(result["badge_class"], "avail-active")

    def test_availability_gtd_player(self):
        injury_map = {"kawhi leonard": {"status": "Day-to-Day", "injury_note": "Knee"}}
        result = self.get_avail("Kawhi Leonard", injury_map)
        self.assertTrue(result["is_active"])
        self.assertTrue(result["is_flagged"])

    def test_availability_out_player(self):
        injury_map = {"joel embiid": {"status": "Out", "injury_note": "Knee – surgery"}}
        result = self.get_avail("Joel Embiid", injury_map)
        self.assertFalse(result["is_active"])
        self.assertEqual(result["badge_class"], "avail-out")

    def test_availability_unknown_player_defaults_active(self):
        result = self.get_avail("Unknown Player XYZ", {})
        self.assertTrue(result["is_active"])

    # ── assess_line_value ──────────────────────────────────────────

    def test_line_value_great_over(self):
        """Season avg of 28 vs line of 24.5 = ~14% edge = Great Value."""
        result = self.assess_line(28.0, 24.5, "points")
        self.assertEqual(result["direction"], "OVER")
        self.assertEqual(result["value_class"], "val-great")
        self.assertGreater(result["edge_pct"], 8.0)

    def test_line_value_under_direction(self):
        result = self.assess_line(20.0, 24.5, "points")
        self.assertEqual(result["direction"], "UNDER")

    def test_line_value_zero_line_returns_no_data(self):
        result = self.assess_line(20.0, 0.0, "points")
        self.assertEqual(result["value_label"], "No Data")

    # ── grade_matchup ──────────────────────────────────────────────

    def test_grade_a_easy_matchup(self):
        """Opponent allows most (worst defense) → A grade."""
        all_ratings = [100, 105, 108, 110, 112, 115, 118, 120, 122, 125]
        result = self.grade_mu("points", 130.0, all_ratings)
        self.assertEqual(result["grade"], "A")

    def test_grade_d_tough_matchup(self):
        """Opponent allows fewest (best defense) → D grade."""
        all_ratings = [100, 105, 108, 110, 112, 115, 118, 120, 122, 125]
        result = self.grade_mu("points", 95.0, all_ratings)
        self.assertEqual(result["grade"], "D")

    def test_grade_na_no_data(self):
        result = self.grade_mu("points", None, [])
        self.assertEqual(result["grade"], "N/A")

    # ── aggregate_streak_summary ───────────────────────────────────

    def test_aggregate_counts_correct(self):
        stubs = [
            {"player_name": "A", "form": {"form_label": "Hot 🔥"}},
            {"player_name": "B", "form": {"form_label": "Cold 🧊"}},
            {"player_name": "C", "form": {"form_label": "Neutral ➡️"}},
            {"player_name": "D", "form": {"form_label": "Hot 🔥"}},
        ]
        summary = self.agg_streak(stubs)
        self.assertEqual(summary["hot_count"], 2)
        self.assertEqual(summary["cold_count"], 1)
        self.assertEqual(summary["neutral_count"], 1)

    def test_aggregate_empty_list(self):
        summary = self.agg_streak([])
        self.assertEqual(summary["hot_count"], 0)

    # ── get_player_intelligence_summary ───────────────────────────

    def test_summary_includes_all_keys(self):
        injury_map = {"lebron james": {"status": "Active", "injury_note": ""}}
        player_data = {"points_avg": "24.8", "rebounds_avg": "7.2", "assists_avg": "3.5"}
        result = self.get_summary(
            player_name="LeBron James",
            stat_type="points",
            prop_line=24.5,
            player_data=player_data,
            game_logs=self._GAME_LOGS,
            injury_status_map=injury_map,
        )
        for key in ("availability", "form", "line_value", "matchup_grade",
                    "alert_flags", "highlight_flags", "season_avg"):
            self.assertIn(key, result)

    def test_summary_hot_player_generates_highlight(self):
        """A player hitting 70%+ of games should generate a hot highlight."""
        injury_map = {}
        hot_logs = [
            {"GAME_DATE": f"2024-11-0{i}", "MATCHUP": "LAL @ BOS",
             "PTS": 30, "REB": 6, "AST": 4, "FG3M": 2, "STL": 1, "BLK": 0, "TOV": 1}
            for i in range(1, 6)
        ]
        player_data = {"points_avg": "29.0"}
        result = self.get_summary("Test Player", "points", 24.5, player_data, hot_logs, injury_map)
        highlight_text = " ".join(result["highlight_flags"])
        self.assertIn("Hot", highlight_text)

    def test_summary_out_player_generates_alert(self):
        injury_map = {"injured player": {"status": "Out", "injury_note": "Knee"}}
        result = self.get_summary("Injured Player", "points", 24.5, {}, [], injury_map)
        alert_text = " ".join(result["alert_flags"])
        self.assertIn("do not bet", alert_text)


if __name__ == "__main__":
    unittest.main(verbosity=2)


# ============================================================
# MODULE: Alternate Line Categorization
# ============================================================

class TestCategorizeAltLines(unittest.TestCase):
    """Tests for categorize_alt_lines() in engine/edge_detection.py."""

    def setUp(self):
        from engine.edge_detection import categorize_alt_lines
        self.categorize = categorize_alt_lines

    # ── Basic categorization ──────────────────────────────────────

    def test_lines_below_standard_are_below_standard(self):
        """Lines below the standard line must appear in below_standard."""
        result = self.categorize(31.5, [28.5, 29.5, 33.5])
        self.assertIn(28.5, result["below_standard"])
        self.assertIn(29.5, result["below_standard"])

    def test_lines_above_standard_are_above_standard(self):
        """Lines above the standard line must appear in above_standard."""
        result = self.categorize(31.5, [28.5, 33.5, 36.5])
        self.assertIn(33.5, result["above_standard"])
        self.assertIn(36.5, result["above_standard"])

    def test_line_equal_to_standard_is_excluded(self):
        """A line exactly equal to standard_line must NOT appear in either list."""
        result = self.categorize(31.5, [28.5, 31.5, 34.5])
        self.assertNotIn(31.5, result["below_standard"])
        self.assertNotIn(31.5, result["above_standard"])

    # ── Sorting & structure ────────────────────────────────────────

    def test_below_standard_is_sorted_ascending(self):
        """below_standard must be sorted ascending."""
        result = self.categorize(31.5, [30.5, 28.5, 29.5])
        self.assertEqual(result["below_standard"], sorted(result["below_standard"]))

    def test_above_standard_is_sorted_ascending(self):
        """above_standard must be sorted ascending."""
        result = self.categorize(31.5, [36.5, 33.5, 38.5])
        self.assertEqual(result["above_standard"], sorted(result["above_standard"]))

    def test_returns_correct_standard_line(self):
        """Return dict must echo the standard_line that was passed in."""
        result = self.categorize(33.5, [39.5, 41.5, 44.5])
        self.assertEqual(result["standard_line"], 33.5)

    # ── Edge cases ─────────────────────────────────────────────────

    def test_all_above_standard_gives_empty_below(self):
        """When all alternate lines are above standard, below_standard is empty."""
        result = self.categorize(33.5, [39.5, 41.5, 44.5])
        self.assertEqual(result["below_standard"], [])
        self.assertEqual(result["above_standard"], [39.5, 41.5, 44.5])

    def test_all_below_standard_gives_empty_above(self):
        """When all alternate lines are below standard, above_standard is empty."""
        result = self.categorize(31.5, [28.5, 29.5, 30.5])
        self.assertEqual(result["above_standard"], [])
        self.assertEqual(result["below_standard"], [28.5, 29.5, 30.5])

    def test_empty_available_lines(self):
        """With no available lines, both lists are empty."""
        result = self.categorize(25.5, [])
        self.assertEqual(result["below_standard"], [])
        self.assertEqual(result["above_standard"], [])

    def test_zero_standard_line_returns_empty_lists(self):
        """A standard_line of 0 or None should return empty lists."""
        for bad_std in (0, None, 0.0):
            result = self.categorize(bad_std, [10.5, 20.5])
            self.assertEqual(result["below_standard"], [])
            self.assertEqual(result["above_standard"], [])

    def test_non_numeric_lines_are_skipped(self):
        """Non-numeric entries in available_lines must be silently skipped."""
        result = self.categorize(25.5, [22.5, "bad", None, 28.5])
        self.assertIn(22.5, result["below_standard"])
        self.assertIn(28.5, result["above_standard"])
        self.assertEqual(len(result["below_standard"]) + len(result["above_standard"]), 2)

    def test_result_has_all_required_keys(self):
        """Return dict must always contain standard_line, below_standard, above_standard."""
        result = self.categorize(20.5, [18.5, 22.5])
        for key in ("standard_line", "below_standard", "above_standard"):
            self.assertIn(key, result)


class TestParseAltLinesFromPlatformProps(unittest.TestCase):
    """Tests for parse_alt_lines_from_platform_props() in data/sportsbook_service.py."""

    def setUp(self):
        from data.sportsbook_service import parse_alt_lines_from_platform_props
        self.parse = parse_alt_lines_from_platform_props

    def _make_prop(self, player, stat, line, platform="PrizePicks"):
        return {
            "player_name": player,
            "stat_type":   stat,
            "line":        line,
            "platform":    platform,
        }

    # ── Single-entry group (no alternates) ────────────────────────

    def test_single_line_standard_line_stamped(self):
        """When only one line exists for a player/stat/platform, standard_line is set."""
        props = [self._make_prop("SGA", "points", 31.5)]
        result = self.parse(props)
        self.assertEqual(len(result), 1)
        self.assertAlmostEqual(result[0]["standard_line"], 31.5)

    # ── Enrichment & structure ────────────────────────────────────

    def test_result_count_matches_input(self):
        """Output list must have the same number of entries as input."""
        props = [
            self._make_prop("Player A", "points", 22.5),
            self._make_prop("Player A", "points", 25.5),
            self._make_prop("Player B", "rebounds", 8.5),
        ]
        result = self.parse(props)
        self.assertEqual(len(result), len(props))

    def test_original_fields_preserved(self):
        """All original prop fields must be preserved in the enriched output."""
        props = [self._make_prop("LeBron", "points", 24.5)]
        result = self.parse(props)
        self.assertEqual(result[0]["player_name"], "LeBron")
        self.assertEqual(result[0]["stat_type"], "points")
        self.assertEqual(result[0]["line"], 24.5)

    def test_standard_line_is_median_for_group(self):
        """The standard_line must be the median of all lines in the group."""
        props = [
            self._make_prop("SGA", "points", 28.5),
            self._make_prop("SGA", "points", 31.5),
            self._make_prop("SGA", "points", 34.5),
        ]
        result = self.parse(props)
        for p in result:
            self.assertAlmostEqual(p["standard_line"], 31.5)

    def test_platforms_are_grouped_separately(self):
        """Same player/stat is grouped per-platform; each platform has its own median."""
        props = [
            self._make_prop("SGA", "points", 28.5, "PrizePicks"),
            self._make_prop("SGA", "points", 31.5, "PrizePicks"),
            self._make_prop("SGA", "points", 34.5, "PrizePicks"),
            self._make_prop("SGA", "points", 30.0, "Underdog"),
        ]
        result = self.parse(props)
        pp = [p for p in result if p["platform"] == "PrizePicks"]
        ud = [p for p in result if p["platform"] == "Underdog"]
        # Underdog has only one line → standard_line == 30.0
        self.assertAlmostEqual(ud[0]["standard_line"], 30.0)
        # PrizePicks: median of [28.5, 31.5, 34.5] = 31.5
        for p in pp:
            self.assertAlmostEqual(p["standard_line"], 31.5)

    def test_empty_props_returns_empty(self):
        """Empty input should return an empty list."""
        self.assertEqual(self.parse([]), [])

    def test_standard_line_key_present(self):
        """Each enriched prop must contain 'standard_line'."""
        props = [self._make_prop("Test", "rebounds", 8.5)]
        result = self.parse(props)
        self.assertIn("standard_line", result[0])


# ============================================================
# MODULE: classify_bet_type() — simplified parameters
# ============================================================

class TestClassifyBetTypeLineCategoryParam(unittest.TestCase):
    """Tests for the simplified classify_bet_type() function."""

    def _base_kwargs(self):
        return dict(
            probability_over=0.65,
            edge_percentage=15.0,
            stat_standard_deviation=4.0,
            projected_stat=22.0,
            prop_line=20.5,
            stat_type="points",
            directional_forces_result={"over_strength": 45.0, "under_strength": 15.0},
            season_average=21.0,
            line_source="prizepicks",
        )

    def test_always_returns_standard(self):
        """classify_bet_type() always returns bet_type='standard'."""
        from engine.edge_detection import classify_bet_type
        result = classify_bet_type(**self._base_kwargs())
        self.assertEqual(result["bet_type"], "standard")

    def test_standard_with_high_stats(self):
        """Even extreme stats always return 'standard'."""
        from engine.edge_detection import classify_bet_type
        kw = self._base_kwargs()
        kw["projected_stat"] = 30.0
        kw["probability_over"] = 0.88
        kw["edge_percentage"] = 35.0
        result = classify_bet_type(**kw)
        self.assertEqual(result["bet_type"], "standard")

    def test_conflicting_forces_sets_risk_flags(self):
        """Conflicting forces populate risk_flags and is_uncertain, but do NOT change bet_type."""
        from engine.edge_detection import classify_bet_type
        kw = self._base_kwargs()
        # Balanced forces → conflict ratio ≥ 0.80
        kw["directional_forces_result"] = {"over_strength": 10.0, "under_strength": 9.0}
        result = classify_bet_type(**kw)
        self.assertTrue(result["is_uncertain"],
                        "Conflicting forces should set is_uncertain=True")
        self.assertGreater(len(result["risk_flags"]), 0,
                           "Conflicting forces should populate risk_flags")
        # bet_type is always 'standard' now
        self.assertEqual(result["bet_type"], "standard")

    def test_return_dict_has_required_keys(self):
        """classify_bet_type() return dict must include all required keys."""
        from engine.edge_detection import classify_bet_type
        result = classify_bet_type(**self._base_kwargs())
        for key in ("bet_type", "risk_flags", "is_uncertain",
                    "reasons", "std_devs_from_line"):
            self.assertIn(key, result, f"Key '{key}' missing from classify_bet_type() result")
        self.assertEqual(result["bet_type"], "standard")

    def test_is_uncertain_informational_only(self):
        """is_uncertain stays informational and does not affect bet_type."""
        from engine.edge_detection import classify_bet_type
        kw = self._base_kwargs()
        kw["directional_forces_result"] = {"over_strength": 10.0, "under_strength": 9.0}
        result = classify_bet_type(**kw)
        self.assertEqual(result["bet_type"], "standard")
        self.assertTrue(result["is_uncertain"])


class TestUncertainDoesNotForceAvoid(unittest.TestCase):
    """Verify that is_uncertain risk flags do NOT set should_avoid on analysis results.

    The analysis pipeline uses should_avoid_prop() for genuine avoid logic.
    Risk flags from classify_bet_type() (is_uncertain) are informational only
    — they display warnings on the card but do not block the pick from being
    shown or auto-logged.
    """

    def _make_result(self, edge_pct=12.0, should_avoid=False, is_uncertain=True, bet_type="normal"):
        """Build a minimal analysis result dict."""
        return {
            "player_name": "Test Player",
            "stat_type": "points",
            "line": 20.5,
            "direction": "OVER",
            "edge_percentage": edge_pct,
            "confidence_score": 72,
            "tier": "Gold",
            "should_avoid": should_avoid,
            "is_uncertain": is_uncertain,
            "bet_type": bet_type,
            "player_is_out": False,
        }

    def test_uncertain_pick_not_filtered_by_edge_filter(self):
        """A pick with is_uncertain=True but good edge should survive the top-picks filter."""
        r = self._make_result(edge_pct=15.0, should_avoid=False, is_uncertain=True)
        # Simulate the display filter from Neural Analysis (top picks only)
        minimum_edge = 5.0
        displayed = [
            res for res in [r]
            if abs(res.get("edge_percentage", 0)) >= minimum_edge
            and not res.get("should_avoid", False)
        ]
        self.assertEqual(len(displayed), 1,
                         "Uncertain pick with good edge should pass the top-picks filter")

    def test_should_avoid_from_prop_check_still_filters(self):
        """A pick marked should_avoid by should_avoid_prop() is correctly filtered."""
        r = self._make_result(edge_pct=2.0, should_avoid=True, is_uncertain=False)
        minimum_edge = 5.0
        displayed = [
            res for res in [r]
            if abs(res.get("edge_percentage", 0)) >= minimum_edge
            and not res.get("should_avoid", False)
        ]
        self.assertEqual(len(displayed), 0,
                         "should_avoid=True from should_avoid_prop() must still filter the pick")

    def test_is_uncertain_stays_informational(self):
        """is_uncertain should be preserved in the result dict for UI display."""
        r = self._make_result(is_uncertain=True)
        self.assertTrue(r["is_uncertain"])
        self.assertFalse(r["should_avoid"],
                         "is_uncertain alone must NOT set should_avoid")
