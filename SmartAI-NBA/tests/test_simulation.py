"""
tests/test_simulation.py
------------------------
Tests for engine/simulation.py — Quantum Matrix Engine 5.6.
"""

import sys
import os
import pathlib
import unittest

# ── Ensure repo root is on sys.path ──────────────────────────────
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

_SIM_SRC = pathlib.Path(__file__).parent.parent / "engine" / "simulation.py"


class TestSimulationSourceLevel(unittest.TestCase):
    """Source-level checks for engine/simulation.py."""

    def test_file_exists(self):
        self.assertTrue(_SIM_SRC.exists(), "engine/simulation.py must exist")

    def test_has_run_quantum_matrix_simulation(self):
        src = _SIM_SRC.read_text(encoding="utf-8")
        self.assertIn("def run_quantum_matrix_simulation(", src)

    def test_has_safe_float_guard(self):
        src = _SIM_SRC.read_text(encoding="utf-8")
        self.assertIn("def _safe_float(", src)


class TestRunQuantumMatrixSimulation(unittest.TestCase):
    """Runtime tests for run_quantum_matrix_simulation."""

    def setUp(self):
        from engine.simulation import run_quantum_matrix_simulation
        self.simulate = run_quantum_matrix_simulation

    def test_basic_result_structure(self):
        """Simulation returns a dict with probability_over in [0, 1]."""
        result = self.simulate(
            projected_stat_average=25.0,
            stat_standard_deviation=5.0,
            prop_line=24.5,
            number_of_simulations=500,
            blowout_risk_factor=0.1,
            pace_adjustment_factor=1.0,
            matchup_adjustment_factor=1.0,
            home_away_adjustment=0.0,
            rest_adjustment_factor=1.0,
        )
        self.assertIsInstance(result, dict)
        prob = result.get("probability_over", -1)
        self.assertGreaterEqual(prob, 0.0)
        self.assertLessEqual(prob, 1.0)

    def test_high_projection_gives_high_probability(self):
        """When projection >> line, probability_over should be > 0.5."""
        result = self.simulate(
            projected_stat_average=30.0,
            stat_standard_deviation=3.0,
            prop_line=20.0,
            number_of_simulations=1000,
            blowout_risk_factor=0.0,
            pace_adjustment_factor=1.0,
            matchup_adjustment_factor=1.0,
            home_away_adjustment=0.0,
            rest_adjustment_factor=1.0,
        )
        self.assertGreater(result.get("probability_over", 0), 0.5)

    def test_low_projection_gives_low_probability(self):
        """When projection << line, probability_over should be < 0.5."""
        result = self.simulate(
            projected_stat_average=15.0,
            stat_standard_deviation=3.0,
            prop_line=25.0,
            number_of_simulations=1000,
            blowout_risk_factor=0.0,
            pace_adjustment_factor=1.0,
            matchup_adjustment_factor=1.0,
            home_away_adjustment=0.0,
            rest_adjustment_factor=1.0,
        )
        self.assertLess(result.get("probability_over", 1), 0.5)

    def test_zero_simulations_returns_valid_result(self):
        """Edge case: 0 simulations should still return a valid dict."""
        result = self.simulate(
            projected_stat_average=20.0,
            stat_standard_deviation=5.0,
            prop_line=20.0,
            number_of_simulations=0,
            blowout_risk_factor=0.0,
            pace_adjustment_factor=1.0,
            matchup_adjustment_factor=1.0,
            home_away_adjustment=0.0,
            rest_adjustment_factor=1.0,
        )
        self.assertIsInstance(result, dict)
        prob = result.get("probability_over", -1)
        self.assertGreaterEqual(prob, 0.0)
        self.assertLessEqual(prob, 1.0)

    def test_zero_projection_and_line(self):
        """Edge case: projection=0, line=0 should still return valid result."""
        result = self.simulate(
            projected_stat_average=0.0,
            stat_standard_deviation=1.0,
            prop_line=0.0,
            number_of_simulations=500,
            blowout_risk_factor=0.0,
            pace_adjustment_factor=1.0,
            matchup_adjustment_factor=1.0,
            home_away_adjustment=0.0,
            rest_adjustment_factor=1.0,
        )
        self.assertIsInstance(result, dict)
        prob = result.get("probability_over", -1)
        self.assertGreaterEqual(prob, 0.0)
        self.assertLessEqual(prob, 1.0)


if __name__ == "__main__":
    unittest.main()
