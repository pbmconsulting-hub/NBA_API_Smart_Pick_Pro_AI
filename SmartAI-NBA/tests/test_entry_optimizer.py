"""
tests/test_entry_optimizer.py
-----------------------------
Tests for engine/entry_optimizer.py — optimal entry builder.
"""

import sys
import os
import pathlib
import unittest

# ── Ensure repo root is on sys.path ──────────────────────────────
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

_OPT_SRC = pathlib.Path(__file__).parent.parent / "engine" / "entry_optimizer.py"


class TestEntryOptimizerSourceLevel(unittest.TestCase):
    """Source-level checks for engine/entry_optimizer.py."""

    def test_file_exists(self):
        self.assertTrue(_OPT_SRC.exists(), "engine/entry_optimizer.py must exist")

    def test_has_build_optimal_entries(self):
        src = _OPT_SRC.read_text(encoding="utf-8")
        self.assertIn("def build_optimal_entries(", src)

    def test_has_calculate_entry_expected_value(self):
        src = _OPT_SRC.read_text(encoding="utf-8")
        self.assertIn("def calculate_entry_expected_value(", src)

    def test_has_calculate_correlation_risk(self):
        src = _OPT_SRC.read_text(encoding="utf-8")
        self.assertIn("def calculate_correlation_risk(", src)


class TestCalculateEntryExpectedValue(unittest.TestCase):
    """Runtime tests for calculate_entry_expected_value."""

    def setUp(self):
        from engine.entry_optimizer import calculate_entry_expected_value
        self.calc_ev = calculate_entry_expected_value

    def test_empty_picks(self):
        """0 picks should return zero EV."""
        result = self.calc_ev([], {}, 10.0)
        self.assertEqual(result["expected_value_dollars"], 0.0)

    def test_single_pick_power(self):
        """Single pick in a power-play payout table."""
        # 2-pick power play: hit both = 3x payout
        result = self.calc_ev(
            [0.7, 0.7],
            {2: 3.0, 1: 0.0, 0: 0.0},
            10.0,
        )
        self.assertIsInstance(result, dict)
        self.assertIn("expected_value_dollars", result)
        self.assertIn("return_on_investment", result)

    def test_three_pick_flex(self):
        """3-pick flex entry with known probabilities."""
        from engine.entry_optimizer import PRIZEPICKS_FLEX_PAYOUT_TABLE
        payout = PRIZEPICKS_FLEX_PAYOUT_TABLE.get(3, {})
        result = self.calc_ev([0.6, 0.6, 0.6], payout, 10.0)
        self.assertIsInstance(result["expected_value_dollars"], float)


class TestBuildOptimalEntries(unittest.TestCase):
    """Runtime tests for build_optimal_entries."""

    def setUp(self):
        from engine.entry_optimizer import build_optimal_entries
        self.build = build_optimal_entries

    def test_zero_picks(self):
        """0 qualifying picks should return empty list."""
        result = self.build([], "PrizePicks", 3, 10.0, 5)
        self.assertEqual(result, [])

    def test_one_pick_not_enough_for_3(self):
        """1 pick can't form a 3-pick entry — should return only 1-pick entries."""
        picks = [{
            "player_name": "LeBron",
            "stat_type": "points",
            "line": 24.5,
            "probability_over": 0.65,
            "direction": "OVER",
            "confidence_score": 80.0,
            "edge_percentage": 10.0,
        }]
        result = self.build(picks, "PrizePicks", 3, 10.0, 5)
        # With only 1 pick, no 3-pick combo is possible;
        # the optimizer may still return 1-pick entries
        self.assertIsInstance(result, list)
        for entry in result:
            self.assertLessEqual(len(entry.get("picks", [])), 1)

    def test_five_picks_builds_entries(self):
        """5+ qualifying picks should produce entries for 3-pick combos."""
        picks = [
            {
                "player_name": f"Player{i}",
                "stat_type": "points",
                "line": 20.0 + i,
                "probability_over": 0.60 + i * 0.02,
                "direction": "OVER",
                "confidence_score": 75.0,
                "edge_percentage": 8.0 + i,
                "player_team": f"Team{i}",
                "opponent": f"Opp{i}",
            }
            for i in range(5)
        ]
        result = self.build(picks, "PrizePicks", 3, 10.0, 5)
        self.assertIsInstance(result, list)
        self.assertGreater(len(result), 0)


class TestCalculateCorrelationRisk(unittest.TestCase):
    """Runtime tests for calculate_correlation_risk."""

    def setUp(self):
        from engine.entry_optimizer import calculate_correlation_risk
        self.calc_risk = calculate_correlation_risk

    def test_no_picks(self):
        """Empty picks list should return no discount."""
        result = self.calc_risk([])
        self.assertEqual(result["discount_multiplier"], 1.0)

    def test_different_games_no_discount(self):
        """Picks from different games should have no correlation discount."""
        picks = [
            {"player_name": "A", "player_team": "LAL", "opponent": "GSW"},
            {"player_name": "B", "player_team": "BOS", "opponent": "MIA"},
        ]
        result = self.calc_risk(picks)
        self.assertEqual(result["discount_multiplier"], 1.0)

    def test_same_game_high_correlation(self):
        """3+ picks from same game should get a high correlation discount."""
        picks = [
            {"player_name": "A", "player_team": "LAL", "opponent": "GSW"},
            {"player_name": "B", "player_team": "LAL", "opponent": "GSW"},
            {"player_name": "C", "player_team": "GSW", "opponent": "LAL"},
        ]
        result = self.calc_risk(picks)
        self.assertLess(result["discount_multiplier"], 1.0)
        self.assertEqual(result["correlation_level"], "high")


if __name__ == "__main__":
    unittest.main()
