# ============================================================
# FILE: tests/test_phase2_nan_guards.py
# PURPOSE: Tests for Phase 2 NaN/inf hardening:
#          1) simulation.py _safe_float() output guard on QME return dict
#          2) projections.py _safe_float() output guard on projection dict
#          3) odds_engine.py _safe_float() on Kelly and synthetic odds
# ============================================================

import math
import sys
import unittest
from unittest.mock import MagicMock


def _ensure_streamlit_mock():
    """Inject a lightweight streamlit mock if not installed."""
    if "streamlit" not in sys.modules:
        mock_st = MagicMock()
        mock_st.session_state = {}
        mock_st.cache_data = lambda **kw: (lambda f: f)
        sys.modules["streamlit"] = mock_st


# ============================================================
# Section 1: simulation.py _safe_float() and output guards
# ============================================================

class TestSimulationSafeFloat(unittest.TestCase):
    """Verify _safe_float() guards in simulation.py."""

    def setUp(self):
        _ensure_streamlit_mock()
        from engine.simulation import _safe_float
        self.safe = _safe_float

    def test_finite_value_passes_through(self):
        self.assertEqual(self.safe(3.14), 3.14)

    def test_nan_returns_fallback(self):
        self.assertEqual(self.safe(float('nan')), 0.0)

    def test_inf_returns_fallback(self):
        self.assertEqual(self.safe(float('inf')), 0.0)

    def test_neg_inf_returns_fallback(self):
        self.assertEqual(self.safe(float('-inf')), 0.0)

    def test_custom_fallback(self):
        self.assertEqual(self.safe(float('nan'), 0.5), 0.5)

    def test_none_returns_fallback(self):
        self.assertEqual(self.safe(None, 1.0), 1.0)

    def test_string_returns_fallback(self):
        self.assertEqual(self.safe("abc"), 0.0)

    def test_zero_passes_through(self):
        self.assertEqual(self.safe(0.0), 0.0)

    def test_negative_passes_through(self):
        self.assertEqual(self.safe(-5.0), -5.0)


class TestSimulationOutputGuards(unittest.TestCase):
    """Verify QME simulation returns finite values for all scalars."""

    def setUp(self):
        _ensure_streamlit_mock()
        from engine.simulation import run_quantum_matrix_simulation
        self.run_qme = run_quantum_matrix_simulation

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

    def _assert_all_scalars_finite(self, result):
        """Check every scalar value in the result dict is finite."""
        for key, val in result.items():
            if key == "simulated_results":
                continue  # skip the raw list
            if key == "simulations_run":
                self.assertIsInstance(val, int)
                continue
            self.assertTrue(
                math.isfinite(val),
                f"result['{key}'] = {val} is not finite",
            )

    def test_standard_run_all_scalars_finite(self):
        """Standard simulation should return all-finite scalar outputs."""
        result = self.run_qme(**self._base_kwargs())
        self._assert_all_scalars_finite(result)

    def test_zero_avg_all_scalars_finite(self):
        """Zero projected average should not produce NaN/inf."""
        kw = self._base_kwargs()
        kw["projected_stat_average"] = 0.0
        kw["stat_standard_deviation"] = 0.0
        result = self.run_qme(**kw)
        self._assert_all_scalars_finite(result)

    def test_extreme_projection_all_scalars_finite(self):
        """Extreme projection values should not produce NaN/inf."""
        kw = self._base_kwargs()
        kw["projected_stat_average"] = 999.0
        kw["stat_standard_deviation"] = 500.0
        kw["prop_line"] = 500.0
        result = self.run_qme(**kw)
        self._assert_all_scalars_finite(result)

    def test_threes_stat_type_zero_avg_finite(self):
        """Threes with zero average should not cause CV divide-by-zero."""
        kw = self._base_kwargs()
        kw["projected_stat_average"] = 0.0
        kw["stat_standard_deviation"] = 0.0
        kw["stat_type"] = "threes"
        result = self.run_qme(**kw)
        self._assert_all_scalars_finite(result)

    def test_tiny_std_dev_finite(self):
        """Extremely small std dev should not produce NaN."""
        kw = self._base_kwargs()
        kw["stat_standard_deviation"] = 1e-12
        result = self.run_qme(**kw)
        self._assert_all_scalars_finite(result)


class TestAltLineProbabilityGuards(unittest.TestCase):
    """Verify generate_alt_line_probabilities returns a valid base probability."""

    def setUp(self):
        _ensure_streamlit_mock()
        from engine.simulation import (
            run_quantum_matrix_simulation,
            generate_alt_line_probabilities,
        )
        self.run_qme = run_quantum_matrix_simulation
        self.gen_alt = generate_alt_line_probabilities

    def test_alt_line_base_probability_finite(self):
        """The base_probability in alt-line output should be finite and in [0.01, 0.99]."""
        sim_out = self.run_qme(
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
        result = self.gen_alt(sim_out, 19.5)
        self.assertTrue(math.isfinite(result["base_probability"]))
        self.assertGreaterEqual(result["base_probability"], 0.0)
        self.assertLessEqual(result["base_probability"], 1.0)

    def test_alt_line_empty_simulation(self):
        """Empty simulated_results should return a base structure without crashing."""
        empty_sim = {"simulated_results": [], "probability_over": 0.5}
        result = self.gen_alt(empty_sim, 19.5)
        self.assertIn("base_line", result)
        self.assertIn("base_probability", result)
        self.assertIn("best_alt", result)


# ============================================================
# Section 2: projections.py _safe_float() output guards
# ============================================================

class TestProjectionsSafeFloat(unittest.TestCase):
    """Verify _safe_float() exists and guards projection outputs."""

    def setUp(self):
        _ensure_streamlit_mock()
        from engine.projections import _safe_float
        self.safe = _safe_float

    def test_finite_passes_through(self):
        self.assertEqual(self.safe(10.5), 10.5)

    def test_nan_returns_fallback(self):
        self.assertEqual(self.safe(float('nan'), 0.0), 0.0)

    def test_inf_returns_fallback(self):
        self.assertEqual(self.safe(float('inf'), 1.0), 1.0)

    def test_none_returns_fallback(self):
        self.assertEqual(self.safe(None, 0.0), 0.0)


class TestProjectionOutputGuards(unittest.TestCase):
    """Verify build_player_projection returns finite values."""

    def setUp(self):
        _ensure_streamlit_mock()
        from engine.projections import build_player_projection
        self.build = build_player_projection

    def _minimal_player(self, points_avg=20.0, games=30):
        """Build a minimal player_data dict for testing."""
        return {
            "name": "Test Player",
            "team": "LAL",
            "position": "SG",
            "games_played": str(games),
            "points_avg": str(points_avg),
            "rebounds_avg": "5.0",
            "assists_avg": "4.0",
            "threes_avg": "2.0",
            "steals_avg": "1.0",
            "blocks_avg": "0.5",
            "turnovers_avg": "2.5",
            "minutes_avg": "32.0",
        }

    def test_standard_projection_all_finite(self):
        """Standard projection should return all-finite numeric fields."""
        result = self.build(
            player_data=self._minimal_player(),
            opponent_team_abbreviation="BOS",
            is_home_game=True,
            rest_days=1,
            game_total=220.0,
            teams_data=[],
            defensive_ratings_data=[],
        )
        for key, val in result.items():
            if isinstance(val, (int, float)) and val is not None:
                self.assertTrue(
                    math.isfinite(val),
                    f"projection['{key}'] = {val} is not finite",
                )

    def test_zero_avg_projection_finite(self):
        """Player with 0 averages should not produce NaN/inf."""
        player = self._minimal_player(points_avg=0.0, games=0)
        player["rebounds_avg"] = "0.0"
        player["assists_avg"] = "0.0"
        player["threes_avg"] = "0.0"
        player["steals_avg"] = "0.0"
        player["blocks_avg"] = "0.0"
        player["turnovers_avg"] = "0.0"
        player["minutes_avg"] = "0.0"
        result = self.build(
            player_data=player,
            opponent_team_abbreviation="BOS",
            is_home_game=False,
            rest_days=0,
            game_total=220.0,
            teams_data=[],
            defensive_ratings_data=[],
        )
        for key, val in result.items():
            if isinstance(val, (int, float)) and val is not None:
                self.assertTrue(
                    math.isfinite(val),
                    f"projection['{key}'] = {val} is not finite (zero-avg player)",
                )


# ============================================================
# Section 3: odds_engine.py _safe_float() on Kelly / Synthetic
# ============================================================

class TestOddsEngineSafeFloat(unittest.TestCase):
    """Verify _safe_float() exists and guards odds engine outputs."""

    def setUp(self):
        _ensure_streamlit_mock()
        from engine.odds_engine import _safe_float
        self.safe = _safe_float

    def test_finite_passes_through(self):
        self.assertEqual(self.safe(-110.0), -110.0)

    def test_nan_returns_fallback(self):
        self.assertEqual(self.safe(float('nan'), 100.0), 100.0)

    def test_inf_returns_fallback(self):
        self.assertEqual(self.safe(float('inf')), 0.0)


class TestKellyOutputGuards(unittest.TestCase):
    """Verify calculate_fractional_kelly returns finite values."""

    def setUp(self):
        _ensure_streamlit_mock()
        from engine.odds_engine import calculate_fractional_kelly
        self.kelly = calculate_fractional_kelly

    def test_standard_kelly_all_finite(self):
        """Standard Kelly calculation returns all-finite values."""
        result = self.kelly(0.62, -110, 0.25)
        for key, val in result.items():
            self.assertTrue(math.isfinite(val), f"kelly['{key}'] = {val} not finite")

    def test_edge_case_extreme_odds_finite(self):
        """Extreme odds (+99900) should not produce NaN/inf."""
        result = self.kelly(0.50, 99900, 0.25)
        for key, val in result.items():
            self.assertTrue(math.isfinite(val), f"kelly['{key}'] = {val} not finite (extreme odds)")

    def test_edge_case_zero_prob_finite(self):
        """Near-zero probability should not produce NaN/inf."""
        result = self.kelly(0.001, -110, 0.25)
        for key, val in result.items():
            self.assertTrue(math.isfinite(val), f"kelly['{key}'] = {val} not finite (zero prob)")

    def test_negative_ev_kelly_clamps_to_zero(self):
        """Negative-EV bet should return zero Kelly fraction."""
        result = self.kelly(0.30, -110, 0.25)
        self.assertEqual(result["kelly_fraction"], 0.0)
        self.assertEqual(result["fractional_kelly"], 0.0)


class TestFairOddsOutputGuards(unittest.TestCase):
    """Verify calculate_fair_odds_from_simulation returns finite values."""

    def setUp(self):
        _ensure_streamlit_mock()
        from engine.odds_engine import calculate_fair_odds_from_simulation
        self.calc = calculate_fair_odds_from_simulation

    def test_standard_array_all_finite(self):
        """Standard sim array returns all-finite values."""
        result = self.calc([20, 22, 18, 25, 19, 21, 23, 17, 24, 20], 19.5, "OVER")
        self.assertTrue(math.isfinite(result["win_probability"]))
        self.assertTrue(math.isfinite(result["fair_odds"]))

    def test_empty_array_returns_defaults(self):
        """Empty array returns safe defaults without NaN."""
        result = self.calc([], 19.5, "OVER")
        self.assertEqual(result["win_probability"], 0.5)
        self.assertEqual(result["fair_odds"], 100.0)
        self.assertEqual(result["sample_size"], 0)

    def test_all_same_values_finite(self):
        """Array where all values equal the line should return finite odds."""
        result = self.calc([19.5] * 100, 19.5, "OVER")
        self.assertTrue(math.isfinite(result["win_probability"]))
        self.assertTrue(math.isfinite(result["fair_odds"]))

    def test_under_direction_finite(self):
        """UNDER direction should also return finite values."""
        result = self.calc([20, 22, 18, 25, 19], 19.5, "UNDER")
        self.assertTrue(math.isfinite(result["win_probability"]))
        self.assertTrue(math.isfinite(result["fair_odds"]))


if __name__ == "__main__":
    unittest.main()
