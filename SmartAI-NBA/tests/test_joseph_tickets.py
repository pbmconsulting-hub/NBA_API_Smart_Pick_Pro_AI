"""
tests/test_joseph_tickets.py
Unit-tests for engine/joseph_tickets.py — Layer 5 ticket builder.
"""

import sys
import os
import unittest

# Ensure repo root is on the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


# ── Imports ─────────────────────────────────────────────────


class TestJosephTicketsImport(unittest.TestCase):
    """Verify all public functions and constants are importable."""

    def test_build_joseph_ticket_importable(self):
        from engine.joseph_tickets import build_joseph_ticket
        self.assertTrue(callable(build_joseph_ticket))

    def test_validate_ticket_correlation_importable(self):
        from engine.joseph_tickets import validate_ticket_correlation
        self.assertTrue(callable(validate_ticket_correlation))

    def test_generate_ticket_pitch_importable(self):
        from engine.joseph_tickets import generate_ticket_pitch
        self.assertTrue(callable(generate_ticket_pitch))

    def test_get_alternative_tickets_importable(self):
        from engine.joseph_tickets import get_alternative_tickets
        self.assertTrue(callable(get_alternative_tickets))

    def test_ticket_pitch_pools_importable(self):
        from engine.joseph_tickets import TICKET_PITCH_POOLS
        self.assertIsInstance(TICKET_PITCH_POOLS, dict)

    def test_ticket_names_available(self):
        from engine.joseph_tickets import TICKET_NAMES
        self.assertEqual(TICKET_NAMES[2], "POWER PLAY")
        self.assertEqual(TICKET_NAMES[3], "TRIPLE THREAT")
        self.assertEqual(TICKET_NAMES[4], "THE QUAD")
        self.assertEqual(TICKET_NAMES[5], "HIGH FIVE")
        self.assertEqual(TICKET_NAMES[6], "THE FULL SEND")


# ── TICKET_PITCH_POOLS structure ───────────────────────────


class TestTicketPitchPools(unittest.TestCase):
    def setUp(self):
        from engine.joseph_tickets import TICKET_PITCH_POOLS
        self.pools = TICKET_PITCH_POOLS

    def test_has_all_leg_counts(self):
        for n in (2, 3, 4, 5, 6):
            self.assertIn(n, self.pools, f"Missing pitch pool for {n} legs")

    def test_each_pool_has_at_least_5_templates(self):
        for n, pool in self.pools.items():
            self.assertGreaterEqual(len(pool), 5,
                                    f"Leg count {n} has fewer than 5 pitches")

    def test_all_templates_are_strings(self):
        for n, pool in self.pools.items():
            for template in pool:
                self.assertIsInstance(template, str)
                self.assertTrue(len(template) > 10)


# ── build_joseph_ticket ────────────────────────────────────


class TestBuildJosephTicket(unittest.TestCase):
    def _make_results(self, count=6):
        results = []
        for i in range(count):
            results.append({
                "player_name": f"Player_{i}",
                "stat_type": "points",
                "line": 20 + i,
                "direction": "OVER",
                "verdict": "SMASH" if i < 3 else "LEAN",
                "joseph_edge": 10.0 - i,
                "joseph_probability": 65.0 - i,
                "confidence": 75.0,
                "game_id": f"game_{i % 4}",
                "player_team": f"Team_{i}",
                "narrative_tags": [],
            })
        return results

    def test_returns_dict(self):
        from engine.joseph_tickets import build_joseph_ticket
        result = build_joseph_ticket(3, self._make_results(), [])
        self.assertIsInstance(result, dict)

    def test_has_required_keys(self):
        from engine.joseph_tickets import build_joseph_ticket
        result = build_joseph_ticket(3, self._make_results(), [])
        for key in ("ticket_name", "legs", "combined_probability",
                     "expected_value", "synergy_score", "joseph_confidence",
                     "joseph_pitch", "why_these_legs", "risk_disclaimer",
                     "nerd_stats"):
            self.assertIn(key, result, f"Missing key: {key}")

    def test_ticket_name_matches_leg_count(self):
        from engine.joseph_tickets import build_joseph_ticket
        for n in (2, 3, 4, 5, 6):
            result = build_joseph_ticket(n, self._make_results(), [])
            expected = {2: "POWER PLAY", 3: "TRIPLE THREAT", 4: "THE QUAD",
                        5: "HIGH FIVE", 6: "THE FULL SEND"}
            self.assertEqual(result["ticket_name"], expected[n])

    def test_correct_leg_count(self):
        from engine.joseph_tickets import build_joseph_ticket
        results = self._make_results(8)
        for n in (2, 3, 4, 5, 6):
            result = build_joseph_ticket(n, results, [])
            if result["legs"]:
                self.assertEqual(len(result["legs"]), n)

    def test_empty_results_returns_empty_ticket(self):
        from engine.joseph_tickets import build_joseph_ticket
        result = build_joseph_ticket(3, [], [])
        self.assertEqual(result["legs"], [])
        self.assertTrue(len(result["joseph_pitch"]) > 0)

    def test_legs_have_required_fields(self):
        from engine.joseph_tickets import build_joseph_ticket
        result = build_joseph_ticket(3, self._make_results(), [])
        for leg in result["legs"]:
            for key in ("player", "stat", "line", "direction",
                        "verdict", "joseph_edge"):
                self.assertIn(key, leg, f"Leg missing key: {key}")

    def test_combined_probability_is_float(self):
        from engine.joseph_tickets import build_joseph_ticket
        result = build_joseph_ticket(3, self._make_results(), [])
        self.assertIsInstance(result["combined_probability"], float)

    def test_nerd_stats_is_dict(self):
        from engine.joseph_tickets import build_joseph_ticket
        result = build_joseph_ticket(3, self._make_results(), [])
        self.assertIsInstance(result["nerd_stats"], dict)

    def test_no_stay_away_in_legs(self):
        from engine.joseph_tickets import build_joseph_ticket
        results = self._make_results()
        results.append({
            "player_name": "BadPlayer",
            "verdict": "STAY_AWAY",
            "joseph_edge": -2.0,
            "game_id": "game_99",
            "narrative_tags": [],
        })
        result = build_joseph_ticket(3, results, [])
        for leg in result["legs"]:
            self.assertNotEqual(leg.get("verdict"), "STAY_AWAY")


# ── validate_ticket_correlation ────────────────────────────


class TestValidateTicketCorrelation(unittest.TestCase):
    def test_returns_dict(self):
        from engine.joseph_tickets import validate_ticket_correlation
        result = validate_ticket_correlation([])
        self.assertIsInstance(result, dict)

    def test_has_required_keys(self):
        from engine.joseph_tickets import validate_ticket_correlation
        result = validate_ticket_correlation([{"player_name": "A"}, {"player_name": "B"}])
        self.assertIn("valid", result)
        self.assertIn("worst_pair", result)
        self.assertIn("correlation_score", result)

    def test_empty_legs_valid(self):
        from engine.joseph_tickets import validate_ticket_correlation
        result = validate_ticket_correlation([])
        self.assertTrue(result["valid"])

    def test_single_leg_valid(self):
        from engine.joseph_tickets import validate_ticket_correlation
        result = validate_ticket_correlation([{"player_name": "A"}])
        self.assertTrue(result["valid"])


# ── generate_ticket_pitch ──────────────────────────────────


class TestGenerateTicketPitch(unittest.TestCase):
    def test_returns_string(self):
        from engine.joseph_tickets import generate_ticket_pitch
        result = generate_ticket_pitch({"ticket_name": "TRIPLE THREAT", "legs": []}, 3)
        self.assertIsInstance(result, str)
        self.assertTrue(len(result) > 10)

    def test_unique_per_call(self):
        from engine.joseph_tickets import generate_ticket_pitch, _used_pitches
        _used_pitches.clear()
        pitches = set()
        for _ in range(5):
            p = generate_ticket_pitch({"ticket_name": "TRIPLE THREAT", "legs": []}, 3)
            pitches.add(p)
        # With 5 templates + closer, should get at least 3 unique
        self.assertGreaterEqual(len(pitches), 3)

    def test_all_leg_counts(self):
        from engine.joseph_tickets import generate_ticket_pitch
        for n in (2, 3, 4, 5, 6):
            result = generate_ticket_pitch({"ticket_name": "TEST", "legs": []}, n)
            self.assertIsInstance(result, str)
            self.assertTrue(len(result) > 5)


# ── get_alternative_tickets ────────────────────────────────


class TestGetAlternativeTickets(unittest.TestCase):
    def _make_results(self, count=10):
        results = []
        for i in range(count):
            results.append({
                "player_name": f"Player_{i}",
                "stat_type": "points",
                "line": 20 + i,
                "direction": "OVER",
                "verdict": "SMASH" if i < 5 else "LEAN",
                "joseph_edge": 10.0 - i * 0.5,
                "joseph_probability": 65.0 - i,
                "game_id": f"game_{i % 5}",
                "narrative_tags": [],
            })
        return results

    def test_returns_list(self):
        from engine.joseph_tickets import get_alternative_tickets
        result = get_alternative_tickets(3, self._make_results())
        self.assertIsInstance(result, list)

    def test_returns_up_to_top_n(self):
        from engine.joseph_tickets import get_alternative_tickets
        result = get_alternative_tickets(3, self._make_results(), top_n=3)
        self.assertLessEqual(len(result), 3)

    def test_each_alt_has_required_keys(self):
        from engine.joseph_tickets import get_alternative_tickets
        result = get_alternative_tickets(3, self._make_results())
        for alt in result:
            self.assertIn("ticket_name", alt)
            self.assertIn("legs", alt)
            self.assertIn("total_edge", alt)

    def test_empty_results_returns_empty(self):
        from engine.joseph_tickets import get_alternative_tickets
        result = get_alternative_tickets(3, [])
        self.assertEqual(result, [])

    def test_too_few_results_returns_empty(self):
        from engine.joseph_tickets import get_alternative_tickets
        result = get_alternative_tickets(3, [{"verdict": "SMASH", "joseph_edge": 5.0}])
        self.assertEqual(result, [])


# ── Graceful error handling ────────────────────────────────


class TestGracefulErrorHandling(unittest.TestCase):
    def test_build_ticket_with_bad_input(self):
        from engine.joseph_tickets import build_joseph_ticket
        result = build_joseph_ticket(3, [{"bad": "data"}], [])
        self.assertIsInstance(result, dict)
        self.assertIn("ticket_name", result)

    def test_validate_correlation_with_bad_input(self):
        from engine.joseph_tickets import validate_ticket_correlation
        result = validate_ticket_correlation([{"bad": "data"}])
        self.assertIsInstance(result, dict)

    def test_generate_pitch_with_bad_input(self):
        from engine.joseph_tickets import generate_ticket_pitch
        result = generate_ticket_pitch({}, 99)
        self.assertIsInstance(result, str)

    def test_get_alternatives_with_bad_input(self):
        from engine.joseph_tickets import get_alternative_tickets
        result = get_alternative_tickets(3, [{"bad": "data"}])
        self.assertIsInstance(result, list)


if __name__ == "__main__":
    unittest.main()
