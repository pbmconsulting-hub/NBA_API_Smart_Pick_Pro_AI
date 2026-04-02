# ============================================================
# FILE: tests/test_line_movement.py
# PURPOSE: Tests for the detect_line_movement() function in
#          engine/line_movement_mirror.py
# ============================================================
import unittest


class TestDetectLineMovement(unittest.TestCase):
    """Tests for detect_line_movement()."""

    def setUp(self):
        try:
            from engine.line_movement_mirror import detect_line_movement
            self._detect = detect_line_movement
        except ImportError as exc:
            self.skipTest(f"line_movement_mirror not importable: {exc}")

    def _make_flat_prop(self, player="LeBron James", stat="points", line=25.5,
                        odds_type="standard"):
        return {
            "player_name": player,
            "stat_type": stat,
            "line": line,
            "odds_type": odds_type,
        }

    def _make_hier_prop(self, player="LeBron James", stat="points", line=25.5):
        return {
            "player_name": player,
            "stat_type": stat,
            "line": line,
        }

    def test_detects_movement_at_exactly_0_5(self):
        flat = [self._make_flat_prop(line=26.0)]
        hier = [self._make_hier_prop(line=25.5)]
        result = self._detect(flat, hier)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["delta"], 0.5)

    def test_detects_movement_above_0_5(self):
        flat = [self._make_flat_prop(line=27.5)]
        hier = [self._make_hier_prop(line=25.5)]
        result = self._detect(flat, hier)
        self.assertEqual(len(result), 1)

    def test_ignores_movement_below_0_5(self):
        flat = [self._make_flat_prop(line=25.7)]
        hier = [self._make_hier_prop(line=25.5)]
        result = self._detect(flat, hier)
        self.assertEqual(len(result), 0)

    def test_ignores_zero_movement(self):
        flat = [self._make_flat_prop(line=25.5)]
        hier = [self._make_hier_prop(line=25.5)]
        result = self._detect(flat, hier)
        self.assertEqual(len(result), 0)

    def test_skips_goblin_odds_type(self):
        flat = [self._make_flat_prop(line=27.0, odds_type="goblin")]
        hier = [self._make_hier_prop(line=25.5)]
        result = self._detect(flat, hier)
        self.assertEqual(len(result), 0)

    def test_skips_demon_odds_type(self):
        flat = [self._make_flat_prop(line=27.0, odds_type="demon")]
        hier = [self._make_hier_prop(line=25.5)]
        result = self._detect(flat, hier)
        self.assertEqual(len(result), 0)

    def test_direction_up_when_flat_above_hierarchy(self):
        flat = [self._make_flat_prop(line=26.5)]
        hier = [self._make_hier_prop(line=25.5)]
        result = self._detect(flat, hier)
        self.assertEqual(result[0]["direction"], "UP")

    def test_direction_down_when_flat_below_hierarchy(self):
        flat = [self._make_flat_prop(line=24.5)]
        hier = [self._make_hier_prop(line=25.5)]
        result = self._detect(flat, hier)
        self.assertEqual(result[0]["direction"], "DOWN")

    def test_sorted_by_largest_delta_first(self):
        flat = [
            self._make_flat_prop(player="Player A", line=28.0),
            self._make_flat_prop(player="Player B", stat="rebounds", line=12.0),
        ]
        hier = [
            self._make_hier_prop(player="Player A", line=25.5),   # delta = 2.5
            self._make_hier_prop(player="Player B", stat="rebounds", line=11.0),  # delta = 1.0
        ]
        result = self._detect(flat, hier)
        self.assertEqual(len(result), 2)
        self.assertGreaterEqual(abs(result[0]["delta"]), abs(result[1]["delta"]))

    def test_returns_correct_fields(self):
        flat = [self._make_flat_prop(line=27.0)]
        hier = [self._make_hier_prop(line=25.5)]
        result = self._detect(flat, hier)
        self.assertEqual(len(result), 1)
        r = result[0]
        self.assertIn("player_name", r)
        self.assertIn("stat_type", r)
        self.assertIn("flat_line", r)
        self.assertIn("hierarchy_line", r)
        self.assertIn("delta", r)
        self.assertIn("direction", r)

    def test_returns_empty_list_when_no_movements(self):
        flat = [self._make_flat_prop(line=25.5)]
        hier = [self._make_hier_prop(line=25.5)]
        result = self._detect(flat, hier)
        self.assertEqual(result, [])

    def test_handles_empty_flat_props(self):
        result = self._detect([], [self._make_hier_prop()])
        self.assertEqual(result, [])

    def test_handles_empty_hierarchy_props(self):
        result = self._detect([self._make_flat_prop()], [])
        self.assertEqual(result, [])

    def test_handles_both_empty(self):
        result = self._detect([], [])
        self.assertEqual(result, [])

    def test_case_insensitive_player_name_matching(self):
        flat = [self._make_flat_prop(player="lebron james", line=27.0)]
        hier = [self._make_hier_prop(player="LeBron James", line=25.5)]
        result = self._detect(flat, hier)
        self.assertEqual(len(result), 1)

    def test_delta_value_is_correct(self):
        flat = [self._make_flat_prop(line=27.5)]
        hier = [self._make_hier_prop(line=25.5)]
        result = self._detect(flat, hier)
        self.assertEqual(result[0]["delta"], 2.0)

    def test_flat_line_and_hierarchy_line_in_result(self):
        flat = [self._make_flat_prop(line=27.0)]
        hier = [self._make_hier_prop(line=25.5)]
        result = self._detect(flat, hier)
        self.assertEqual(result[0]["flat_line"], 27.0)
        self.assertEqual(result[0]["hierarchy_line"], 25.5)


if __name__ == "__main__":
    unittest.main()
