# ============================================================
# FILE: tests/test_joseph_foundation.py
# PURPOSE: Tests for Joseph Foundation modules
#   - data.advanced_metrics  (normalize, classify, narrative, enrich)
#   - engine.joseph_eval     (letter_grade, gravity, switchability, grade, compare)
#   - engine.joseph_strategy (scheme detection, mismatch rules, game strategy)
# ============================================================

import sys
import os
import math
import unittest

# Ensure repo root on path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


# ============================================================
# SECTION 1: data.advanced_metrics — normalize
# ============================================================

class TestAdvancedMetricsNormalize(unittest.TestCase):
    def setUp(self):
        from data.advanced_metrics import normalize
        self.normalize = normalize

    def test_normalize_basic(self):
        """normalize(5, 0, 10, 0, 100) == 50"""
        result = self.normalize(5, 0, 10, 0, 100)
        self.assertAlmostEqual(result, 50.0)

    def test_normalize_clamp_above(self):
        """normalize(15, 0, 10, 0, 100) == 100 (clamped above max)"""
        result = self.normalize(15, 0, 10, 0, 100)
        self.assertAlmostEqual(result, 100.0)

    def test_normalize_clamp_below(self):
        """normalize(-5, 0, 10, 0, 100) == 0 (clamped below min)"""
        result = self.normalize(-5, 0, 10, 0, 100)
        self.assertAlmostEqual(result, 0.0)

    def test_normalize_equal_bounds(self):
        """normalize(5, 5, 5, 0, 100) == 0 (out_min when range is zero)"""
        result = self.normalize(5, 5, 5, 0, 100)
        self.assertAlmostEqual(result, 0.0)


# ============================================================
# SECTION 2: data.advanced_metrics — classify_player_archetype
# ============================================================

class TestAdvancedMetricsArchetype(unittest.TestCase):
    def setUp(self):
        from data.advanced_metrics import classify_player_archetype
        self.classify = classify_player_archetype

    def test_archetype_alpha_scorer(self):
        """pts > 25 and high usage → 'Alpha Scorer'"""
        player = {
            "points_avg": 30,
            "assists_avg": 5,
            "rebounds_avg": 7,
            "steals_avg": 1.0,
            "blocks_avg": 0.5,
            "turnovers_avg": 3,
            "fg3_pct": 0.37,
            "fg3a": 8,
            "fga": 22,
            "fta": 8,
            "minutes_avg": 36,
            "defensive_rebounds_avg": 5,
            "position": "SF",
            "starter": True,
        }
        self.assertEqual(self.classify(player), "Alpha Scorer")

    def test_archetype_floor_general(self):
        """ast > 7.5 and good ast/tov ratio → 'Floor General'"""
        player = {
            "points_avg": 12,
            "assists_avg": 10,
            "rebounds_avg": 4,
            "steals_avg": 1.0,
            "blocks_avg": 0.2,
            "turnovers_avg": 3,
            "fg3_pct": 0.34,
            "fg3a": 3,
            "fga": 10,
            "fta": 3,
            "minutes_avg": 32,
            "defensive_rebounds_avg": 3,
            "position": "PG",
            "starter": True,
        }
        self.assertEqual(self.classify(player), "Floor General")

    def test_archetype_stretch_big(self):
        """C position and fg3a > 2.5 → 'Stretch Big'"""
        player = {
            "points_avg": 14,
            "assists_avg": 2,
            "rebounds_avg": 8,
            "steals_avg": 0.5,
            "blocks_avg": 1.0,
            "turnovers_avg": 1.5,
            "fg3_pct": 0.36,
            "fg3a": 4.0,
            "fga": 11,
            "fta": 2,
            "minutes_avg": 28,
            "defensive_rebounds_avg": 6,
            "position": "C",
            "starter": True,
        }
        self.assertEqual(self.classify(player), "Stretch Big")

    def test_archetype_rim_protector(self):
        """C position and blk > 1.5 → 'Rim Protector'"""
        player = {
            "points_avg": 10,
            "assists_avg": 1.5,
            "rebounds_avg": 9,
            "steals_avg": 0.5,
            "blocks_avg": 2.5,
            "turnovers_avg": 1.5,
            "fg3_pct": 0.10,
            "fg3a": 0.3,
            "fga": 7,
            "fta": 3,
            "minutes_avg": 28,
            "defensive_rebounds_avg": 7,
            "position": "C",
            "starter": True,
        }
        self.assertEqual(self.classify(player), "Rim Protector")

    def test_archetype_role_player_fallback(self):
        """Minimal stats → 'Role Player'"""
        player = {
            "points_avg": 4,
            "assists_avg": 1,
            "rebounds_avg": 2,
            "steals_avg": 0.3,
            "blocks_avg": 0.1,
            "turnovers_avg": 0.5,
            "fg3_pct": 0.30,
            "fg3a": 1.0,
            "fga": 4,
            "fta": 1,
            "minutes_avg": 14,
            "defensive_rebounds_avg": 1,
            "position": "SG",
            "starter": True,
        }
        self.assertEqual(self.classify(player), "Role Player")


# ============================================================
# SECTION 3: data.advanced_metrics — detect_narrative_tags
# ============================================================

class TestAdvancedMetricsNarrative(unittest.TestCase):
    def setUp(self):
        from data.advanced_metrics import detect_narrative_tags
        self.detect = detect_narrative_tags

    def test_narrative_revenge_game(self):
        """LeBron vs CLE → has 'revenge_game'"""
        player = {"name": "LeBron James", "team": "LAL"}
        game = {"home_team": "LAL", "away_team": "CLE"}
        teams = {}
        tags = self.detect(player, game, teams)
        self.assertIn("revenge_game", tags)

    def test_narrative_contract_year(self):
        """Jimmy Butler → has 'contract_year'"""
        player = {"name": "Jimmy Butler", "team": "MIA"}
        game = {"home_team": "MIA", "away_team": "NYK"}
        teams = {}
        tags = self.detect(player, game, teams)
        self.assertIn("contract_year", tags)

    def test_narrative_nationally_televised(self):
        """broadcast='ESPN' → has 'nationally_televised'"""
        player = {"name": "Giannis Antetokounmpo", "team": "MIL"}
        game = {"home_team": "MIL", "away_team": "PHI", "broadcast": "ESPN"}
        teams = {}
        tags = self.detect(player, game, teams)
        self.assertIn("nationally_televised", tags)

    def test_narrative_rivalry(self):
        """BOS vs LAL → has 'rivalry'"""
        player = {"name": "Jayson Tatum", "team": "BOS"}
        game = {"home_team": "BOS", "away_team": "LAL"}
        teams = {}
        tags = self.detect(player, game, teams)
        self.assertIn("rivalry", tags)


# ============================================================
# SECTION 4: data.advanced_metrics — enrich_player_god_mode
# ============================================================

class TestAdvancedMetricsEnrich(unittest.TestCase):
    def setUp(self):
        from data.advanced_metrics import enrich_player_god_mode
        self.enrich = enrich_player_god_mode

    def _base_player(self, line=0):
        """Return a realistic player dict for enrichment tests."""
        return {
            "name": "Test Player",
            "team": "LAL",
            "position": "SF",
            "points_avg": 20,
            "assists_avg": 5,
            "rebounds_avg": 7,
            "steals_avg": 1.2,
            "blocks_avg": 0.5,
            "turnovers_avg": 2.5,
            "fg3_pct": 0.37,
            "fg3a": 5,
            "fga": 16,
            "fta": 5,
            "minutes_avg": 34,
            "defensive_rebounds_avg": 5,
            "starter": True,
            "prop": {"line": line},
        }

    def test_enrich_kill_switch_no_line(self):
        """No prop line → enriched=False"""
        player = self._base_player(line=0)
        player.pop("prop", None)
        result = self.enrich(player, [], {})
        self.assertFalse(result.get("enriched", True))

    def test_enrich_kill_switch_zero_line(self):
        """line=0 → enriched=False"""
        player = self._base_player(line=0)
        result = self.enrich(player, [], {})
        self.assertFalse(result.get("enriched", True))

    def test_enrich_with_valid_line(self):
        """line=24.5 → enriched=True and all key metrics present"""
        player = self._base_player(line=24.5)
        game = {"home_team": "LAL", "away_team": "BOS"}
        teams = {"LAL": {"pace": 100}, "BOS": {"pace": 98}}
        result = self.enrich(player, [game], teams)
        self.assertTrue(result.get("enriched", False))
        # Offensive metrics
        self.assertIn("usage_rate_est", result)
        self.assertIn("true_shooting_pct", result)
        self.assertIn("points_per_possession_est", result)
        self.assertIn("assist_to_turnover", result)
        self.assertIn("three_point_volume", result)
        self.assertIn("free_throw_rate", result)
        self.assertIn("gravity_score", result)
        # Defensive proxies
        self.assertIn("stocks", result)
        self.assertIn("defensive_activity", result)
        self.assertIn("switchability_score", result)
        # Contextual
        self.assertIn("archetype", result)
        self.assertIn("narrative_tags", result)


# ============================================================
# SECTION 5: data.advanced_metrics — Constants
# ============================================================

class TestAdvancedMetricsConstants(unittest.TestCase):
    def setUp(self):
        from data.advanced_metrics import (
            REVENGE_MATCHUPS,
            EXPIRING_CONTRACTS_2026,
            RIVALRY_PAIRS,
            TIMEZONE_MAP,
        )
        self.REVENGE_MATCHUPS = REVENGE_MATCHUPS
        self.EXPIRING_CONTRACTS_2026 = EXPIRING_CONTRACTS_2026
        self.RIVALRY_PAIRS = RIVALRY_PAIRS
        self.TIMEZONE_MAP = TIMEZONE_MAP

    def test_constants_revenge_matchups(self):
        """REVENGE_MATCHUPS contains >= 40 entries"""
        self.assertGreaterEqual(len(self.REVENGE_MATCHUPS), 40)

    def test_constants_expiring_contracts(self):
        """EXPIRING_CONTRACTS_2026 contains >= 30 entries"""
        self.assertGreaterEqual(len(self.EXPIRING_CONTRACTS_2026), 30)

    def test_constants_rivalry_pairs(self):
        """RIVALRY_PAIRS contains >= 10 entries"""
        self.assertGreaterEqual(len(self.RIVALRY_PAIRS), 10)

    def test_constants_timezone_map(self):
        """TIMEZONE_MAP contains all 30 NBA teams"""
        self.assertEqual(len(self.TIMEZONE_MAP), 30)


# ============================================================
# SECTION 6: engine.joseph_eval — letter_grade
# ============================================================

class TestJosephEvalLetterGrade(unittest.TestCase):
    def setUp(self):
        from engine.joseph_eval import letter_grade
        self.letter_grade = letter_grade

    def test_letter_grade_a_plus(self):
        """>= 95 → 'A+'"""
        self.assertEqual(self.letter_grade(95), "A+")
        self.assertEqual(self.letter_grade(100), "A+")

    def test_letter_grade_f(self):
        """< 40 → 'F'"""
        self.assertEqual(self.letter_grade(30), "F")
        self.assertEqual(self.letter_grade(0), "F")
        self.assertEqual(self.letter_grade(39), "F")

    def test_letter_grade_b(self):
        """75-79 → 'B'"""
        self.assertEqual(self.letter_grade(75), "B")
        self.assertEqual(self.letter_grade(79), "B")


# ============================================================
# SECTION 7: engine.joseph_eval — gravity & switchability
# ============================================================

class TestJosephEvalScores(unittest.TestCase):
    def setUp(self):
        from engine.joseph_eval import calculate_gravity_score, calculate_switchability
        self.gravity = calculate_gravity_score
        self.switchability = calculate_switchability

    def _star_player(self):
        return {
            "fg3a": 8, "fg3_pct": 0.38, "fta": 7, "fga": 20,
            "turnovers_avg": 3, "points_avg": 28, "minutes_avg": 36,
            "steals_avg": 1.3, "blocks_avg": 0.7, "rebounds_avg": 7,
        }

    def test_gravity_score_range(self):
        """Gravity score is between 0 and 100"""
        score = self.gravity(self._star_player())
        self.assertGreaterEqual(score, 0.0)
        self.assertLessEqual(score, 100.0)

    def test_switchability_range(self):
        """Switchability score is between 0 and 100"""
        score = self.switchability(self._star_player())
        self.assertGreaterEqual(score, 0.0)
        self.assertLessEqual(score, 100.0)


# ============================================================
# SECTION 8: engine.joseph_eval — joseph_grade_player
# ============================================================

class TestJosephEvalGradePlayer(unittest.TestCase):
    def setUp(self):
        from engine.joseph_eval import joseph_grade_player
        self.grade = joseph_grade_player
        self.player = {
            "name": "Test Star",
            "position": "SF",
            "points_avg": 25, "assists_avg": 6, "rebounds_avg": 8,
            "steals_avg": 1.2, "blocks_avg": 0.6, "turnovers_avg": 3,
            "minutes_avg": 35, "fg3a": 6, "fg3_pct": 0.37,
            "fga": 18, "fta": 6, "defensive_rebounds_avg": 5.5,
        }
        self.context = {
            "opponent_team": "BOS",
            "opponent_def_rating": 108,
            "narrative_tags": ["rivalry"],
            "scheme": {"primary_scheme": "switch"},
            "spread": -3.5,
            "total": 220,
            "rest_days": 2,
        }

    def test_joseph_grade_returns_dict(self):
        """Result has overall_grade, offense_grade, defense_grade, etc."""
        result = self.grade(self.player, self.context)
        self.assertIsInstance(result, dict)
        for key in ("overall_grade", "offense_grade", "defense_grade",
                     "impact_grade", "matchup_grade"):
            self.assertIn(key, result)

    def test_joseph_grade_overall_is_letter(self):
        """overall_grade is a valid letter grade"""
        valid_grades = {
            "A+", "A", "A-", "B+", "B", "B-",
            "C+", "C", "C-", "D+", "D", "D-", "F",
        }
        result = self.grade(self.player, self.context)
        self.assertIn(result["overall_grade"], valid_grades)

    def test_joseph_grade_profiles(self):
        """Result has offensive_profile and defensive_profile dicts"""
        result = self.grade(self.player, self.context)
        self.assertIn("offensive_profile", result)
        self.assertIn("defensive_profile", result)
        self.assertIsInstance(result["offensive_profile"], dict)
        self.assertIsInstance(result["defensive_profile"], dict)
        # Check sub-keys
        for key in ("scoring_volume", "scoring_efficiency",
                     "creation_burden", "gravity", "free_throw_drawing"):
            self.assertIn(key, result["offensive_profile"])
        for key in ("rim_protection", "perimeter_disruption",
                     "rebounding_impact", "switchability", "hustle_index"):
            self.assertIn(key, result["defensive_profile"])

    def test_joseph_grade_tonight_factors(self):
        """Result tonight_factors has fatigue_risk, etc."""
        result = self.grade(self.player, self.context)
        self.assertIn("tonight_factors", result)
        factors = result["tonight_factors"]
        for key in ("fatigue_risk", "motivation_boost",
                     "ceiling_percentile", "floor_percentile",
                     "matchup_advantage", "scheme_fit"):
            self.assertIn(key, factors)


# ============================================================
# SECTION 9: engine.joseph_eval — joseph_compare_players
# ============================================================

class TestJosephEvalCompare(unittest.TestCase):
    def setUp(self):
        from engine.joseph_eval import joseph_compare_players
        self.compare = joseph_compare_players

    def test_joseph_compare_returns_winner(self):
        """Comparison result has 'winner' key"""
        player_a = {
            "name": "Star A", "position": "SG",
            "points_avg": 28, "assists_avg": 5, "rebounds_avg": 6,
            "steals_avg": 1.5, "blocks_avg": 0.3, "turnovers_avg": 3,
            "minutes_avg": 36, "fg3a": 7, "fg3_pct": 0.39,
            "fga": 20, "fta": 7, "defensive_rebounds_avg": 4,
        }
        player_b = {
            "name": "Star B", "position": "PG",
            "points_avg": 18, "assists_avg": 9, "rebounds_avg": 4,
            "steals_avg": 1.8, "blocks_avg": 0.2, "turnovers_avg": 2.5,
            "minutes_avg": 34, "fg3a": 4, "fg3_pct": 0.36,
            "fga": 14, "fta": 4, "defensive_rebounds_avg": 3,
        }
        result = self.compare(player_a, player_b)
        self.assertIsInstance(result, dict)
        self.assertIn("winner", result)
        self.assertIn(result["winner"], ("Star A", "Star B"))
        self.assertIn("advantage_margin", result)
        self.assertIn("offense_edge", result)
        self.assertIn("defense_edge", result)
        self.assertIn("impact_edge", result)
        self.assertIn("matchup_edge", result)
        self.assertIn("joseph_comparison_take", result)


# ============================================================
# SECTION 10: engine.joseph_strategy — detect_defensive_scheme
# ============================================================

class TestJosephStrategyScheme(unittest.TestCase):
    def setUp(self):
        from engine.joseph_strategy import detect_defensive_scheme
        self.detect = detect_defensive_scheme

    def test_detect_scheme_switch(self):
        """opp_fg3_pct < 0.34 and opp_paint_fg_pct > 0.52 → 'switch'"""
        team = {
            "pace": 99, "def_rating": 110, "off_rating": 112,
            "opp_fg3_pct": 0.32, "opp_paint_fg_pct": 0.55,
        }
        result = self.detect(team)
        self.assertEqual(result["primary_scheme"], "switch")

    def test_detect_scheme_drop(self):
        """opp_paint_fg_pct < 0.48 and opp_fg3_pct > 0.36 → 'drop'"""
        team = {
            "pace": 99, "def_rating": 110, "off_rating": 112,
            "opp_fg3_pct": 0.38, "opp_paint_fg_pct": 0.45,
        }
        result = self.detect(team)
        self.assertEqual(result["primary_scheme"], "drop")

    def test_detect_scheme_zone_default(self):
        """Default / fallback conditions → 'zone'"""
        team = {
            "pace": 98, "def_rating": 112, "off_rating": 110,
            "opp_fg3_pct": 0.355, "opp_paint_fg_pct": 0.50,
        }
        result = self.detect(team)
        self.assertEqual(result["primary_scheme"], "zone")


# ============================================================
# SECTION 11: engine.joseph_strategy — MISMATCH_RULES
# ============================================================

class TestJosephStrategyMismatchRules(unittest.TestCase):
    def setUp(self):
        from engine.joseph_strategy import MISMATCH_RULES, apply_mismatch_rules
        self.MISMATCH_RULES = MISMATCH_RULES
        self.apply = apply_mismatch_rules

    def test_mismatch_rules_count(self):
        """There are exactly 12 mismatch rules"""
        self.assertEqual(len(self.MISMATCH_RULES), 12)

    def test_apply_mismatch_stretch_big_vs_drop(self):
        """Stretch Big archetype vs drop scheme triggers the rule"""
        player = {
            "player_name": "Brook Lopez",
            "team": "MIL",
            "opp_team": "OPP",
            "archetype": "Stretch Big",
        }
        opp_scheme = {"primary_scheme": "drop"}
        game_strategy = {
            "pace_label": "average",
            "blowout_probability": 0.10,
        }
        triggered = self.apply(player, opp_scheme, game_strategy, [])
        names = [m["name"] for m in triggered]
        self.assertIn("Stretch Big vs Drop", names)

    def test_apply_mismatch_returns_rant(self):
        """Each triggered mismatch has a rant string"""
        player = {
            "player_name": "Brook Lopez",
            "team": "MIL",
            "opp_team": "OPP",
            "archetype": "Stretch Big",
        }
        opp_scheme = {"primary_scheme": "drop"}
        game_strategy = {
            "pace_label": "average",
            "blowout_probability": 0.10,
        }
        triggered = self.apply(player, opp_scheme, game_strategy, [])
        for mismatch in triggered:
            self.assertIn("rant", mismatch)
            self.assertIsInstance(mismatch["rant"], str)
            self.assertGreater(len(mismatch["rant"]), 0)


# ============================================================
# SECTION 12: engine.joseph_strategy — analyze_game_strategy
# ============================================================

class TestJosephStrategyAnalyze(unittest.TestCase):
    def setUp(self):
        from engine.joseph_strategy import analyze_game_strategy
        self.analyze = analyze_game_strategy
        self.teams_data = [
            {
                "abbreviation": "LAL", "team": "LAL",
                "pace": 100.5, "off_rating": 114.0, "def_rating": 112.0,
                "opp_fg3_pct": 0.355, "opp_paint_fg_pct": 0.50,
            },
            {
                "abbreviation": "BOS", "team": "BOS",
                "pace": 98.5, "off_rating": 116.0, "def_rating": 107.0,
                "opp_fg3_pct": 0.33, "opp_paint_fg_pct": 0.48,
            },
        ]

    def _result(self):
        return self.analyze("LAL", "BOS", {}, self.teams_data)

    def test_analyze_game_strategy_keys(self):
        """Result has all expected top-level keys"""
        result = self._result()
        expected_keys = [
            "pace_projection", "pace_label", "game_total_est", "spread_est",
            "blowout_probability", "overtime_probability",
            "garbage_time_minutes_est", "home_scheme", "away_scheme",
            "scheme_matchups", "key_player_matchups",
            "game_narrative", "betting_angle",
        ]
        for key in expected_keys:
            self.assertIn(key, result)

    def test_analyze_game_pace_label(self):
        """pace_label is one of the valid labels"""
        valid_labels = {"shootout", "uptempo", "average", "grind", "rock_fight"}
        result = self._result()
        self.assertIn(result["pace_label"], valid_labels)

    def test_analyze_game_total_reasonable(self):
        """game_total_est is between 180 and 260"""
        result = self._result()
        total = result["game_total_est"]
        self.assertGreaterEqual(total, 180)
        self.assertLessEqual(total, 260)

    def test_analyze_game_has_schemes(self):
        """home_scheme and away_scheme are present and are dicts"""
        result = self._result()
        self.assertIsInstance(result["home_scheme"], dict)
        self.assertIsInstance(result["away_scheme"], dict)
        self.assertIn("primary_scheme", result["home_scheme"])
        self.assertIn("primary_scheme", result["away_scheme"])


# ============================================================
# Runner
# ============================================================

if __name__ == "__main__":
    unittest.main()
