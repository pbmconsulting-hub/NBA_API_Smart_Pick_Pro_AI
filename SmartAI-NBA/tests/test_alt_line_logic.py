# ============================================================
# FILE: tests/test_alt_line_logic.py
# PURPOSE: Tests for the Alt-Line logic — engine layer and formatters.
# ============================================================

import unittest


class TestGenerateAltLineProbabilities(unittest.TestCase):
    """Tests for engine/simulation.py generate_alt_line_probabilities()."""

    def setUp(self):
        from engine.simulation import generate_alt_line_probabilities
        self.gen = generate_alt_line_probabilities

    def _mock_sim_output(self, results, prob_over=0.5):
        return {
            "simulated_results": results,
            "probability_over": prob_over,
        }

    def test_returns_base_line(self):
        sim = self._mock_sim_output([float(x) for x in range(5, 15)], 0.6)
        result = self.gen(sim, 7.5)
        self.assertEqual(result["base_line"], 7.5)

    def test_returns_base_probability(self):
        sim = self._mock_sim_output([10.0] * 100, 0.8)
        result = self.gen(sim, 10.0)
        self.assertAlmostEqual(result["base_probability"], 0.8, places=3)

    def test_best_alt_type_is_base(self):
        sim = self._mock_sim_output([float(x) for x in range(5, 15)], 0.6)
        result = self.gen(sim, 7.5)
        self.assertEqual(result["best_alt"]["type"], "base")

    def test_best_alt_prediction_is_empty(self):
        sim = self._mock_sim_output([float(x) for x in range(5, 15)], 0.6)
        result = self.gen(sim, 7.5)
        self.assertEqual(result["best_alt"]["prediction"], "")

    def test_empty_simulation_returns_base_structure(self):
        sim = self._mock_sim_output([], 0.5)
        result = self.gen(sim, 10.0)
        self.assertEqual(result["best_alt"]["type"], "base")
        self.assertIn("base_line", result)
        self.assertIn("base_probability", result)

    def test_best_alt_line_equals_base_line(self):
        sim = self._mock_sim_output([10.0] * 50, 0.5)
        result = self.gen(sim, 10.0)
        self.assertEqual(result["best_alt"]["line"], 10.0)

    def test_result_has_required_keys(self):
        sim = self._mock_sim_output([10.0] * 10, 0.65)
        result = self.gen(sim, 10.0)
        for key in ("base_line", "base_probability", "best_alt"):
            self.assertIn(key, result)


if __name__ == "__main__":
    unittest.main()
