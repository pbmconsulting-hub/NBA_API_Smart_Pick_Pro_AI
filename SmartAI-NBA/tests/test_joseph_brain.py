"""
tests/test_joseph_brain.py
Unit-tests for engine/joseph_brain.py — Layer 4, Part A (data pools & stubs).
"""

import sys
import os
import unittest

# Ensure repo root is on the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


# ── Section A: Fragment Pool structure & counts ──────────────


class TestOpenerPool(unittest.TestCase):
    def setUp(self):
        from engine.joseph_brain import OPENER_POOL
        self.pool = OPENER_POOL

    def test_count(self):
        """OPENER_POOL has exactly 15 entries."""
        self.assertEqual(len(self.pool), 15)

    def test_ids_sequential(self):
        """IDs are opener_01 through opener_15."""
        expected = [f"opener_{i:02d}" for i in range(1, 16)]
        self.assertEqual([e["id"] for e in self.pool], expected)

    def test_all_have_text(self):
        """Every entry has a non-empty 'text' field."""
        for entry in self.pool:
            self.assertIn("text", entry)
            self.assertIsInstance(entry["text"], str)
            self.assertTrue(len(entry["text"]) > 0)


class TestPivotPool(unittest.TestCase):
    def setUp(self):
        from engine.joseph_brain import PIVOT_POOL
        self.pool = PIVOT_POOL

    def test_count(self):
        """PIVOT_POOL has exactly 10 entries."""
        self.assertEqual(len(self.pool), 10)

    def test_ids_sequential(self):
        expected = [f"pivot_{i:02d}" for i in range(1, 11)]
        self.assertEqual([e["id"] for e in self.pool], expected)


class TestCloserPool(unittest.TestCase):
    def setUp(self):
        from engine.joseph_brain import CLOSER_POOL
        self.pool = CLOSER_POOL

    def test_count(self):
        """CLOSER_POOL has exactly 10 entries."""
        self.assertEqual(len(self.pool), 10)

    def test_ids_sequential(self):
        expected = [f"closer_{i:02d}" for i in range(1, 11)]
        self.assertEqual([e["id"] for e in self.pool], expected)


class TestCatchphrasePool(unittest.TestCase):
    def setUp(self):
        from engine.joseph_brain import CATCHPHRASE_POOL
        self.pool = CATCHPHRASE_POOL

    def test_count(self):
        """CATCHPHRASE_POOL has exactly 13 entries."""
        self.assertEqual(len(self.pool), 13)

    def test_ids_sequential(self):
        expected = [f"catch_{i:02d}" for i in range(1, 14)]
        self.assertEqual([e["id"] for e in self.pool], expected)


# ── Body Templates ───────────────────────────────────────────


class TestBodyTemplates(unittest.TestCase):
    def setUp(self):
        from engine.joseph_brain import BODY_TEMPLATES
        self.templates = BODY_TEMPLATES

    def test_verdict_keys(self):
        """BODY_TEMPLATES contains exactly the 5 verdict keys."""
        expected = {"SMASH", "LEAN", "FADE", "STAY_AWAY", "OVERRIDE"}
        self.assertEqual(set(self.templates.keys()), expected)

    def test_five_templates_per_verdict(self):
        """Each verdict has exactly 5 template strings."""
        for verdict, tpls in self.templates.items():
            self.assertEqual(len(tpls), 5, f"{verdict} has {len(tpls)} templates")

    def test_placeholders_present(self):
        """Every template contains at least {player}."""
        for verdict, tpls in self.templates.items():
            for t in tpls:
                self.assertIn("{player}", t, f"Missing {{player}} in {verdict}")

    def test_templates_are_strings(self):
        for verdict, tpls in self.templates.items():
            for t in tpls:
                self.assertIsInstance(t, str)


# ── Section B: Ambient Colour Pools ─────────────────────────


class TestAmbientContextPool(unittest.TestCase):
    def setUp(self):
        from engine.joseph_brain import AMBIENT_CONTEXT_POOL
        self.pool = AMBIENT_CONTEXT_POOL

    def test_has_required_contexts(self):
        """Pool covers the essential context keys."""
        for key in ("high_stakes", "rivalry", "blowout_risk", "back_to_back", "neutral"):
            self.assertIn(key, self.pool)

    def test_each_context_has_entries(self):
        for key, lines in self.pool.items():
            self.assertGreaterEqual(len(lines), 5, f"{key} has too few lines")


# ── Section C: Stat Commentary Pools ────────────────────────


class TestStatCommentaryPool(unittest.TestCase):
    def setUp(self):
        from engine.joseph_brain import STAT_COMMENTARY_POOL
        self.pool = STAT_COMMENTARY_POOL

    def test_has_core_stats(self):
        for stat in ("points", "rebounds", "assists", "threes", "steals", "blocks"):
            self.assertIn(stat, self.pool)

    def test_each_stat_has_entries(self):
        for stat, lines in self.pool.items():
            self.assertGreaterEqual(len(lines), 5, f"{stat} has too few lines")


# ── Section D: Verdict Thresholds & Config ──────────────────


class TestVerdictThresholds(unittest.TestCase):
    def setUp(self):
        from engine.joseph_brain import VERDICT_THRESHOLDS
        self.thresholds = VERDICT_THRESHOLDS

    def test_all_verdicts_present(self):
        for v in ("SMASH", "LEAN", "FADE", "STAY_AWAY", "OVERRIDE"):
            self.assertIn(v, self.thresholds)

    def test_smash_requires_high_edge(self):
        self.assertGreaterEqual(self.thresholds["SMASH"]["min_edge"], 6.0)


class TestJosephConfig(unittest.TestCase):
    def setUp(self):
        from engine.joseph_brain import JOSEPH_CONFIG
        self.cfg = JOSEPH_CONFIG

    def test_has_max_picks(self):
        self.assertIn("max_picks_per_slate", self.cfg)
        self.assertIsInstance(self.cfg["max_picks_per_slate"], int)

    def test_has_min_edge(self):
        self.assertIn("min_edge_threshold", self.cfg)


# ── Anti-repetition state ───────────────────────────────────


class TestAntiRepetitionState(unittest.TestCase):
    def test_initial_state_empty(self):
        from engine.joseph_brain import (
            _used_fragments, _used_ambient, _used_commentary, reset_fragment_state
        )
        reset_fragment_state()
        self.assertEqual(len(_used_fragments), 0)
        self.assertEqual(len(_used_ambient), 0)
        self.assertEqual(len(_used_commentary), 0)


# ── Helper functions (implemented) ──────────────────────────


class TestPickFragment(unittest.TestCase):
    def setUp(self):
        from engine.joseph_brain import (
            _pick_fragment, OPENER_POOL, reset_fragment_state
        )
        self._pick = _pick_fragment
        self.pool = OPENER_POOL
        reset_fragment_state()

    def test_returns_string(self):
        result = self._pick(self.pool, "opener")
        self.assertIsInstance(result, str)
        self.assertTrue(len(result) > 0)

    def test_no_immediate_repeat(self):
        """Two consecutive picks from a large pool should differ (probabilistically)."""
        results = {self._pick(self.pool, "opener") for _ in range(15)}
        self.assertEqual(len(results), 15, "All 15 openers should be used before repeating")

    def test_empty_pool_returns_empty(self):
        result = self._pick([], "empty")
        self.assertEqual(result, "")


class TestPickAmbient(unittest.TestCase):
    def setUp(self):
        from engine.joseph_brain import _pick_ambient, reset_fragment_state
        self._pick = _pick_ambient
        reset_fragment_state()

    def test_known_context(self):
        result = self._pick("rivalry")
        self.assertIsInstance(result, str)
        self.assertTrue(len(result) > 0)

    def test_unknown_context(self):
        result = self._pick("nonexistent_context")
        self.assertEqual(result, "")


class TestPickCommentary(unittest.TestCase):
    def setUp(self):
        from engine.joseph_brain import _pick_commentary, reset_fragment_state
        self._pick = _pick_commentary
        reset_fragment_state()

    def test_known_stat(self):
        result = self._pick("points")
        self.assertIsInstance(result, str)
        self.assertTrue(len(result) > 0)

    def test_unknown_stat(self):
        result = self._pick("nonexistent_stat")
        self.assertEqual(result, "")


# ── Function stubs return safe defaults ─────────────────────


class TestDetermineVerdictStub(unittest.TestCase):
    def test_returns_string(self):
        from engine.joseph_brain import determine_verdict
        v = determine_verdict(5.0, 60.0)
        self.assertIsInstance(v, str)
        self.assertIn(v, {"SMASH", "LEAN", "FADE", "STAY_AWAY", "OVERRIDE"})

    def test_avoid_returns_stay_away(self):
        from engine.joseph_brain import determine_verdict
        v = determine_verdict(10.0, 80.0, avoid=True)
        self.assertEqual(v, "STAY_AWAY")


class TestBuildRantStub(unittest.TestCase):
    def test_returns_string(self):
        from engine.joseph_brain import build_rant
        r = build_rant("SMASH", player="LeBron", stat="points")
        self.assertIsInstance(r, str)


class TestJosephAnalyzePickStub(unittest.TestCase):
    def test_returns_dict_with_required_keys(self):
        from engine.joseph_brain import joseph_analyze_pick
        result = joseph_analyze_pick({}, 24.5, "points", {})
        self.assertIsInstance(result, dict)
        for key in ("verdict", "edge", "confidence", "rant", "explanation",
                     "grade", "strategy"):
            self.assertIn(key, result)


class TestJosephRankPicksStub(unittest.TestCase):
    def test_returns_list(self):
        from engine.joseph_brain import joseph_rank_picks
        result = joseph_rank_picks([])
        self.assertIsInstance(result, list)


class TestJosephEvaluateParlayStub(unittest.TestCase):
    def test_returns_dict_with_required_keys(self):
        from engine.joseph_brain import joseph_evaluate_parlay
        result = joseph_evaluate_parlay([])
        self.assertIsInstance(result, dict)
        for key in ("expected_value", "correlation_matrix",
                     "adjusted_probability", "rant"):
            self.assertIn(key, result)


class TestJosephFullSlateStub(unittest.TestCase):
    def test_returns_dict_with_required_keys(self):
        from engine.joseph_brain import joseph_generate_full_slate_analysis
        result = joseph_generate_full_slate_analysis([], [], {})
        self.assertIsInstance(result, dict)
        for key in ("picks", "parlays", "top_plays", "summary_rant"):
            self.assertIn(key, result)


class TestJosephCommentaryStub(unittest.TestCase):
    def test_returns_string(self):
        from engine.joseph_brain import joseph_commentary_for_stat
        result = joseph_commentary_for_stat("LeBron", "points")
        self.assertIsInstance(result, str)


class TestJosephBlowoutWarningStub(unittest.TestCase):
    def test_returns_string(self):
        from engine.joseph_brain import joseph_blowout_warning
        result = joseph_blowout_warning(-12.5, 215.0)
        self.assertIsInstance(result, str)


class TestResetFragmentState(unittest.TestCase):
    def test_clears_state(self):
        from engine.joseph_brain import (
            _pick_fragment, OPENER_POOL, reset_fragment_state,
            _used_fragments
        )
        _pick_fragment(OPENER_POOL, "opener")
        self.assertIn("opener", _used_fragments)
        reset_fragment_state()
        self.assertEqual(len(_used_fragments), 0)


# ── Import fallbacks ────────────────────────────────────────


class TestImportFallbacks(unittest.TestCase):
    """Verify the module exposes all expected names even if deps are missing."""

    def test_all_pools_importable(self):
        from engine.joseph_brain import (
            OPENER_POOL, PIVOT_POOL, CLOSER_POOL,
            CATCHPHRASE_POOL, BODY_TEMPLATES,
            AMBIENT_CONTEXT_POOL, STAT_COMMENTARY_POOL,
            VERDICT_THRESHOLDS, JOSEPH_CONFIG,
        )
        self.assertIsInstance(OPENER_POOL, list)
        self.assertIsInstance(PIVOT_POOL, list)
        self.assertIsInstance(CLOSER_POOL, list)
        self.assertIsInstance(CATCHPHRASE_POOL, list)
        self.assertIsInstance(BODY_TEMPLATES, dict)
        self.assertIsInstance(AMBIENT_CONTEXT_POOL, dict)
        self.assertIsInstance(STAT_COMMENTARY_POOL, dict)
        self.assertIsInstance(VERDICT_THRESHOLDS, dict)
        self.assertIsInstance(JOSEPH_CONFIG, dict)

    def test_all_functions_importable(self):
        from engine.joseph_brain import (
            _pick_fragment, _pick_ambient, _pick_commentary,
            determine_verdict, build_rant,
            joseph_analyze_pick, joseph_rank_picks,
            joseph_evaluate_parlay, joseph_generate_full_slate_analysis,
            joseph_generate_independent_picks,
            joseph_commentary_for_stat, joseph_blowout_warning,
            reset_fragment_state,
        )
        self.assertTrue(callable(_pick_fragment))
        self.assertTrue(callable(determine_verdict))
        self.assertTrue(callable(joseph_analyze_pick))
        self.assertTrue(callable(joseph_generate_independent_picks))
        self.assertTrue(callable(reset_fragment_state))

    def test_blowout_constants_importable(self):
        from engine.joseph_brain import (
            BLOWOUT_DIFFERENTIAL_MILD, BLOWOUT_DIFFERENTIAL_HEAVY
        )
        self.assertEqual(BLOWOUT_DIFFERENTIAL_MILD, 12)
        self.assertEqual(BLOWOUT_DIFFERENTIAL_HEAVY, 20)


# ── Section B new: AMBIENT_POOLS (6 contexts × 15 lines) ───


class TestAmbientPools(unittest.TestCase):
    def setUp(self):
        from engine.joseph_brain import AMBIENT_POOLS
        self.pool = AMBIENT_POOLS

    def test_is_dict(self):
        self.assertIsInstance(self.pool, dict)

    def test_has_all_six_contexts(self):
        expected_original = {"idle", "games_loaded", "analysis_complete",
                            "entry_built", "premium_pitch", "commentary_on_results"}
        self.assertTrue(expected_original.issubset(set(self.pool.keys())))

    def test_has_all_page_contexts(self):
        """Each page should have a dedicated ambient pool."""
        page_keys = {
            "page_home", "page_live_scores", "page_live_games",
            "page_prop_scanner", "page_analysis", "page_game_report",
            "page_live_sweat", "page_simulator", "page_entry_builder",
            "page_studio", "page_risk_shield", "page_data_feed",
            "page_correlation", "page_bet_tracker", "page_backtester",
            "page_settings", "page_premium", "page_vegas_vault",
        }
        self.assertTrue(page_keys.issubset(set(self.pool.keys())))

    def test_total_pool_count(self):
        """AMBIENT_POOLS should have 24 pools (6 original + 18 page)."""
        self.assertEqual(len(self.pool), 24)

    def test_each_context_has_15_lines(self):
        for key, lines in self.pool.items():
            self.assertEqual(len(lines), 15, f"{key} has {len(lines)} lines, expected 15")

    def test_all_strings(self):
        for key, lines in self.pool.items():
            for line in lines:
                self.assertIsInstance(line, str)
                self.assertTrue(len(line) > 0, f"Empty string in {key}")

    def test_idle_no_placeholders(self):
        """Idle lines are static (no format placeholders)."""
        for line in self.pool["idle"]:
            self.assertNotIn("{", line, f"Unexpected placeholder in idle: {line}")

    def test_games_loaded_has_n_placeholder(self):
        """At least some games_loaded lines use {n}."""
        n_lines = [l for l in self.pool["games_loaded"] if "{n}" in l]
        self.assertGreater(len(n_lines), 0)

    def test_analysis_complete_has_placeholders(self):
        """analysis_complete lines reference {smash_count} or {total}."""
        all_text = " ".join(self.pool["analysis_complete"])
        self.assertIn("{smash_count}", all_text)
        self.assertIn("{total}", all_text)

    def test_premium_pitch_no_player_placeholders(self):
        """premium_pitch lines are generic upsell — no {player}."""
        for line in self.pool["premium_pitch"]:
            self.assertNotIn("{player}", line)

    def test_commentary_on_results_has_player(self):
        """At least some commentary_on_results lines use {player}."""
        p_lines = [l for l in self.pool["commentary_on_results"] if "{player}" in l]
        self.assertGreater(len(p_lines), 0)

    def test_page_pools_no_format_placeholders(self):
        """Page-specific pools should be static (no format placeholders)."""
        page_keys = [k for k in self.pool if k.startswith("page_")]
        for key in page_keys:
            for line in self.pool[key]:
                self.assertNotIn("{", line,
                                 f"Unexpected placeholder in {key}: {line}")


# ── Section C new: COMMENTARY_OPENER_POOL ───────────────────


class TestCommentaryOpenerPool(unittest.TestCase):
    def setUp(self):
        from engine.joseph_brain import COMMENTARY_OPENER_POOL
        self.pool = COMMENTARY_OPENER_POOL

    def test_is_dict(self):
        self.assertIsInstance(self.pool, dict)

    def test_has_all_four_context_types(self):
        expected = {"analysis_results", "entry_built", "optimal_slip",
                    "ticket_generated"}
        self.assertEqual(set(self.pool.keys()), expected)

    def test_each_context_has_5_templates(self):
        for key, templates in self.pool.items():
            self.assertEqual(len(templates), 5, f"{key} has {len(templates)}, expected 5")

    def test_all_strings(self):
        for key, templates in self.pool.items():
            for t in templates:
                self.assertIsInstance(t, str)
                self.assertTrue(len(t) > 0)


# ── Section D new: JOSEPH_COMPS_DATABASE ────────────────────


class TestJosephCompsDatabase(unittest.TestCase):
    def setUp(self):
        from engine.joseph_brain import JOSEPH_COMPS_DATABASE
        self.db = JOSEPH_COMPS_DATABASE

    def test_is_list(self):
        self.assertIsInstance(self.db, list)

    def test_at_least_50_entries(self):
        self.assertGreaterEqual(len(self.db), 50)

    def test_required_keys(self):
        """Every entry has all six required keys."""
        required_keys = {"name", "archetype", "stat_context", "tier",
                         "narrative", "template"}
        for i, entry in enumerate(self.db):
            self.assertEqual(set(entry.keys()), required_keys,
                             f"Entry {i} ({entry.get('name', '?')}) missing keys")

    def test_all_values_are_strings(self):
        for entry in self.db:
            for k, v in entry.items():
                self.assertIsInstance(v, str, f"{entry['name']}.{k} not a string")
                self.assertTrue(len(v) > 0, f"{entry['name']}.{k} is empty")

    def test_all_13_archetypes_present(self):
        archetypes = {e["archetype"] for e in self.db}
        expected = {"Alpha Scorer", "Floor General", "Glass Cleaner",
                    "3-and-D Wing", "Stretch Big", "Rim Protector",
                    "Sixth Man Spark", "Two-Way Wing", "Pick-and-Roll Big",
                    "Shot Creator", "Playmaking Wing", "Defensive Anchor",
                    "High-Usage Ball Handler"}
        self.assertEqual(archetypes, expected)

    def test_each_archetype_at_least_3(self):
        from collections import Counter
        counts = Counter(e["archetype"] for e in self.db)
        for arch, cnt in counts.items():
            self.assertGreaterEqual(cnt, 3, f"{arch} has only {cnt} entries")

    def test_required_players_referenced(self):
        all_text = " ".join(e["name"] + " " + e["template"] for e in self.db)
        required = ["Curry", "LeBron", "Jordan", "Kobe", "KD", "Giannis",
                     "Jokic", "Embiid", "Harden", "Luka", "Tatum",
                     "Iverson", "Nash", "Stockton", "Duncan", "Garnett",
                     "Magic", "Bird", "Shaq", "Hakeem", "Wade", "CP3",
                     "Kawhi", "PG13", "Dirk"]
        for player in required:
            self.assertIn(player, all_text, f"Player {player} not referenced")

    def test_tier_values_valid(self):
        valid_tiers = {"Platinum", "Gold", "Silver"}
        for entry in self.db:
            self.assertIn(entry["tier"], valid_tiers,
                          f"{entry['name']} has invalid tier {entry['tier']}")

    def test_templates_have_reason_placeholder(self):
        """Every template includes {reason}."""
        for entry in self.db:
            self.assertIn("{reason}", entry["template"],
                          f"{entry['name']} template missing {{reason}}")

    def test_unique_names(self):
        names = [e["name"] for e in self.db]
        self.assertEqual(len(names), len(set(names)), "Duplicate names found")


# ── Section E: Constants ────────────────────────────────────


class TestDawgFactorTable(unittest.TestCase):
    def setUp(self):
        from engine.joseph_brain import DAWG_FACTOR_TABLE
        self.table = DAWG_FACTOR_TABLE

    def test_is_dict(self):
        self.assertIsInstance(self.table, dict)

    def test_has_required_keys(self):
        expected = {"revenge_game", "contract_year", "nationally_televised",
                    "rivalry", "playoff_implications", "pace_up",
                    "trap_game", "back_to_back", "altitude",
                    "blowout_risk", "pace_down"}
        self.assertEqual(set(self.table.keys()), expected)

    def test_values_are_floats(self):
        for key, val in self.table.items():
            self.assertIsInstance(val, (int, float), f"{key} is not numeric")

    def test_revenge_game_positive(self):
        self.assertGreater(self.table["revenge_game"], 0)

    def test_trap_game_negative(self):
        self.assertLess(self.table["trap_game"], 0)

    def test_specific_values(self):
        self.assertAlmostEqual(self.table["revenge_game"], 2.5)
        self.assertAlmostEqual(self.table["trap_game"], -3.0)
        self.assertAlmostEqual(self.table["back_to_back"], -1.5)


class TestVerdictEmojis(unittest.TestCase):
    def setUp(self):
        from engine.joseph_brain import VERDICT_EMOJIS
        self.emojis = VERDICT_EMOJIS

    def test_is_dict(self):
        self.assertIsInstance(self.emojis, dict)

    def test_has_all_verdicts(self):
        for v in ("SMASH", "LEAN", "FADE", "STAY_AWAY"):
            self.assertIn(v, self.emojis)

    def test_values_are_strings(self):
        for k, v in self.emojis.items():
            self.assertIsInstance(v, str)
            self.assertTrue(len(v) > 0)

    def test_specific_emojis(self):
        self.assertEqual(self.emojis["SMASH"], "\U0001f525")
        self.assertEqual(self.emojis["LEAN"], "\u2705")
        self.assertEqual(self.emojis["STAY_AWAY"], "\U0001f6ab")


class TestTicketNames(unittest.TestCase):
    def setUp(self):
        from engine.joseph_brain import TICKET_NAMES
        self.names = TICKET_NAMES

    def test_is_dict(self):
        self.assertIsInstance(self.names, dict)

    def test_has_required_keys(self):
        for k in (2, 3, 4, 5, 6):
            self.assertIn(k, self.names)

    def test_values_are_strings(self):
        for k, v in self.names.items():
            self.assertIsInstance(v, str)
            self.assertTrue(len(v) > 0)

    def test_specific_names(self):
        self.assertEqual(self.names[2], "POWER PLAY")
        self.assertEqual(self.names[3], "TRIPLE THREAT")
        self.assertEqual(self.names[6], "THE FULL SEND")


# ── Section F: New function stubs ───────────────────────────


class TestSelectFragmentStub(unittest.TestCase):
    def test_returns_dict(self):
        from engine.joseph_brain import _select_fragment
        pool = [{"id": "test_01", "text": "Hello"}]
        result = _select_fragment(pool, set())
        self.assertIsInstance(result, dict)
        self.assertIn("id", result)
        self.assertIn("text", result)

    def test_empty_pool_returns_fallback(self):
        from engine.joseph_brain import _select_fragment
        result = _select_fragment([], set())
        self.assertEqual(result["id"], "fallback")
        self.assertEqual(result["text"], "")


# ── Section G: Phase 1B Implementation Tests ────────────────


class TestSelectFragmentAntiRepeat(unittest.TestCase):
    def test_tracks_used_ids(self):
        from engine.joseph_brain import _select_fragment
        # Use a pool of 5 so the 60% threshold (3 items) won't be hit during 3 picks
        pool = [{"id": f"item_{i}", "text": f"T{i}"} for i in range(5)]
        used = set()
        r1 = _select_fragment(pool, used)
        r2 = _select_fragment(pool, used)
        r3 = _select_fragment(pool, used)
        ids = {r1["id"], r2["id"], r3["id"]}
        # All 3 picks should be distinct (no repeats within 3 of 5)
        self.assertEqual(len(ids), 3)

    def test_resets_at_60_percent(self):
        from engine.joseph_brain import _select_fragment
        pool = [{"id": f"x{i}", "text": f"T{i}"} for i in range(10)]
        used = {f"x{i}" for i in range(7)}  # 70% used
        result = _select_fragment(pool, used)
        # Should have cleared and picked from full pool
        self.assertIn("id", result)
        self.assertTrue(len(used) <= 2)  # cleared then added 1


class TestBuildJosephRantImplementation(unittest.TestCase):
    def test_contains_player_name(self):
        from engine.joseph_brain import build_joseph_rant, reset_fragment_state
        reset_fragment_state()
        result = build_joseph_rant("LeBron", {"stat": "points", "line": 25.5, "edge": 8.0, "prob": 62.0}, "SMASH", [])
        self.assertIn("LeBron", result)

    def test_multiple_sentences(self):
        from engine.joseph_brain import build_joseph_rant, reset_fragment_state
        reset_fragment_state()
        result = build_joseph_rant("Curry", {"stat": "threes", "line": 4.5, "edge": 5.0, "prob": 58.0}, "LEAN", [])
        # Should have at least 3 sentences (opener + body + closer)
        self.assertTrue(len(result.split(".")) >= 2 or len(result.split("!")) >= 2)

    def test_high_energy_includes_catchphrase(self):
        from engine.joseph_brain import build_joseph_rant, reset_fragment_state
        reset_fragment_state()
        result = build_joseph_rant("Giannis", {"stat": "rebounds"}, "SMASH", [], energy="high")
        self.assertIsInstance(result, str)
        self.assertTrue(len(result) > 50)

    def test_with_comp(self):
        from engine.joseph_brain import build_joseph_rant, reset_fragment_state
        reset_fragment_state()
        comp = {"name": "Test Comp", "template": "This reminds me of greatness... {reason}"}
        result = build_joseph_rant("Player", {"stat": "points"}, "LEAN", [], comp=comp)
        self.assertIn("reminds me", result)

    def test_with_mismatch(self):
        from engine.joseph_brain import build_joseph_rant, reset_fragment_state
        reset_fragment_state()
        mismatch = {"description": "a size advantage that is MASSIVE"}
        result = build_joseph_rant("Player", {"stat": "points"}, "LEAN", [], mismatch=mismatch)
        self.assertIn("MISMATCH", result)

    def test_with_pivot_on_mixed_tags(self):
        from engine.joseph_brain import build_joseph_rant, reset_fragment_state
        reset_fragment_state()
        result = build_joseph_rant("Player", {"stat": "points"}, "LEAN",
                                   ["revenge_game", "back_to_back"])
        self.assertIsInstance(result, str)
        self.assertTrue(len(result) > 30)


class TestGenerateCounterArgument(unittest.TestCase):
    def test_back_to_back(self):
        from engine.joseph_brain import _generate_counter_argument
        result = _generate_counter_argument({}, {}, ["back_to_back"])
        self.assertIn("fatigue", result.lower())

    def test_trap_game(self):
        from engine.joseph_brain import _generate_counter_argument
        result = _generate_counter_argument({}, {}, ["trap_game"])
        self.assertIn("trap", result.lower())

    def test_default(self):
        from engine.joseph_brain import _generate_counter_argument
        result = _generate_counter_argument({}, {}, [])
        self.assertIn("variance", result.lower())


class TestJosephFullAnalysisImplementation(unittest.TestCase):
    def test_returns_verdict_and_edge(self):
        from engine.joseph_brain import joseph_full_analysis, VERDICT_EMOJIS
        result = joseph_full_analysis(
            {"probability_over": 60.0, "edge_percentage": 10.0, "stat_type": "points", "line": 25},
            {"name": "TestPlayer", "games_played": 30},
            {},
            {}
        )
        self.assertIn(result["verdict"], {"SMASH", "LEAN", "FADE", "STAY_AWAY"})
        self.assertEqual(result["verdict_emoji"], VERDICT_EMOJIS.get(result["verdict"], ""))
        self.assertIsInstance(result["edge"], (int, float))

    def test_narrative_tags_populated(self):
        from engine.joseph_brain import joseph_full_analysis
        result = joseph_full_analysis(
            {"probability_over": 55.0, "edge_percentage": 3.0},
            {"name": "Player"},
            {"is_back_to_back": True},
            {}
        )
        self.assertIn("back_to_back", result["narrative_tags"])

    def test_has_rant(self):
        from engine.joseph_brain import joseph_full_analysis, reset_fragment_state
        reset_fragment_state()
        result = joseph_full_analysis(
            {"probability_over": 70.0, "edge_percentage": 12.0, "stat_type": "points", "line": 25},
            {"name": "LeBron"},
            {},
            {}
        )
        self.assertIsInstance(result["rant"], str)
        self.assertTrue(len(result["rant"]) > 10)

    def test_has_reasoning_chain(self):
        from engine.joseph_brain import joseph_full_analysis
        result = joseph_full_analysis({}, {}, {}, {})
        self.assertIn("reasoning_chain", result)
        self.assertIsInstance(result["reasoning_chain"], list)

    def test_handles_empty_input(self):
        from engine.joseph_brain import joseph_full_analysis
        result = joseph_full_analysis({}, {}, {}, {})
        self.assertIsInstance(result, dict)
        self.assertIn("verdict", result)


class TestJosephAnalyzeGameImplementation(unittest.TestCase):
    def test_returns_narrative(self):
        from engine.joseph_brain import joseph_analyze_game
        result = joseph_analyze_game(
            {"home_team": "Lakers", "away_team": "Celtics"},
            {},
            []
        )
        self.assertIsInstance(result["game_narrative"], str)
        self.assertTrue(len(result["game_narrative"]) > 0)

    def test_has_pace_take(self):
        from engine.joseph_brain import joseph_analyze_game
        result = joseph_analyze_game(
            {"home_team": "Lakers", "away_team": "Celtics", "pace_delta": 5.0},
            {}, []
        )
        self.assertIn("pace", result["pace_take"].lower())


class TestJosephAnalyzePlayerImplementation(unittest.TestCase):
    def test_returns_scouting_report(self):
        from engine.joseph_brain import joseph_analyze_player
        result = joseph_analyze_player(
            {"name": "LeBron James"},
            [{"points": 28}],
            {},
            []
        )
        self.assertIsInstance(result["scouting_report"], str)
        self.assertTrue(len(result["scouting_report"]) > 0)

    def test_returns_archetype(self):
        from engine.joseph_brain import joseph_analyze_player
        result = joseph_analyze_player({"name": "Player"}, [], {}, [])
        self.assertIsInstance(result["archetype"], str)


class TestJosephGenerateBestBetsImplementation(unittest.TestCase):
    def test_empty_results_returns_explanatory(self):
        from engine.joseph_brain import joseph_generate_best_bets
        result = joseph_generate_best_bets(3, [], {})
        self.assertEqual(result["ticket_name"], "TRIPLE THREAT")
        self.assertEqual(result["legs"], [])
        self.assertTrue(len(result["rant"]) > 0)

    def test_with_results(self):
        from engine.joseph_brain import joseph_generate_best_bets
        results = [
            {"player_name": "P1", "verdict": "SMASH", "joseph_edge": 10.0,
             "joseph_probability": 65.0, "edge": 10.0, "confidence": 80.0,
             "stat_type": "points", "line": 25, "direction": "OVER", "game_id": "g1"},
            {"player_name": "P2", "verdict": "SMASH", "joseph_edge": 9.0,
             "joseph_probability": 63.0, "edge": 9.0, "confidence": 75.0,
             "stat_type": "rebounds", "line": 8, "direction": "OVER", "game_id": "g2"},
            {"player_name": "P3", "verdict": "LEAN", "joseph_edge": 6.0,
             "joseph_probability": 58.0, "edge": 6.0, "confidence": 65.0,
             "stat_type": "assists", "line": 7, "direction": "OVER", "game_id": "g3"},
        ]
        result = joseph_generate_best_bets(2, results, {})
        self.assertEqual(result["ticket_name"], "POWER PLAY")
        self.assertEqual(len(result["legs"]), 2)


class TestJosephQuickTakeImplementation(unittest.TestCase):
    def test_returns_multi_sentence(self):
        from engine.joseph_brain import joseph_quick_take, reset_fragment_state
        reset_fragment_state()
        result = joseph_quick_take([], {}, [])
        self.assertIsInstance(result, str)
        self.assertTrue(len(result) > 30)

    def test_unique_per_call(self):
        from engine.joseph_brain import joseph_quick_take, reset_fragment_state
        reset_fragment_state()
        r1 = joseph_quick_take([], {}, [])
        r2 = joseph_quick_take([], {}, [])
        # At least one should be different (anti-repetition)
        self.assertIsInstance(r1, str)
        self.assertIsInstance(r2, str)


class TestJosephGetAmbientContextImplementation(unittest.TestCase):
    def test_idle_default(self):
        from engine.joseph_brain import joseph_get_ambient_context
        ctx, kwargs = joseph_get_ambient_context({})
        self.assertEqual(ctx, "idle")
        self.assertIsInstance(kwargs, dict)

    def test_games_loaded(self):
        from engine.joseph_brain import joseph_get_ambient_context
        state = {"todays_games": [{"home_team": "LAL", "away_team": "BOS"}]}
        ctx, kwargs = joseph_get_ambient_context(state)
        self.assertEqual(ctx, "games_loaded")
        self.assertEqual(kwargs["n"], 1)

    def test_analysis_complete(self):
        from engine.joseph_brain import joseph_get_ambient_context
        state = {"analysis_results": [{"verdict": "SMASH", "tier": "Platinum", "is_override": False}]}
        ctx, kwargs = joseph_get_ambient_context(state)
        self.assertEqual(ctx, "analysis_complete")
        self.assertEqual(kwargs["smash_count"], 1)
        self.assertEqual(kwargs["total"], 1)

    def test_entry_built(self):
        from engine.joseph_brain import joseph_get_ambient_context
        state = {"joseph_entry_just_built": 3}
        ctx, kwargs = joseph_get_ambient_context(state)
        self.assertEqual(ctx, "entry_built")
        self.assertEqual(kwargs["n"], 3)

    def test_page_context_overrides_generic(self):
        """Page context should override generic analysis_complete/games_loaded."""
        from engine.joseph_brain import joseph_get_ambient_context
        state = {
            "joseph_page_context": "page_live_sweat",
            "analysis_results": [{"verdict": "SMASH", "tier": "Gold", "is_override": False}],
        }
        ctx, kwargs = joseph_get_ambient_context(state)
        self.assertEqual(ctx, "page_live_sweat")
        self.assertEqual(kwargs, {})

    def test_page_context_all_pages(self):
        """All 18 page context keys should be recognized."""
        from engine.joseph_brain import joseph_get_ambient_context
        page_keys = [
            "page_home", "page_live_scores", "page_live_games",
            "page_prop_scanner", "page_analysis", "page_game_report",
            "page_live_sweat", "page_simulator", "page_entry_builder",
            "page_studio", "page_risk_shield", "page_data_feed",
            "page_correlation", "page_bet_tracker", "page_backtester",
            "page_settings", "page_premium", "page_vegas_vault",
        ]
        for key in page_keys:
            ctx, kwargs = joseph_get_ambient_context({"joseph_page_context": key})
            self.assertEqual(ctx, key, f"Expected {key}, got {ctx}")

    def test_invalid_page_context_falls_through(self):
        """An unknown page context should fall through to generic checks."""
        from engine.joseph_brain import joseph_get_ambient_context
        state = {"joseph_page_context": "page_nonexistent"}
        ctx, kwargs = joseph_get_ambient_context(state)
        self.assertEqual(ctx, "idle")


class TestJosephAmbientLineImplementation(unittest.TestCase):
    def test_anti_repetition(self):
        from engine.joseph_brain import joseph_ambient_line, reset_fragment_state
        reset_fragment_state()
        lines = set()
        for _ in range(15):
            lines.add(joseph_ambient_line("idle"))
        # Should have cycled through many unique lines
        self.assertGreater(len(lines), 5)

    def test_format_placeholders(self):
        from engine.joseph_brain import joseph_ambient_line, reset_fragment_state
        reset_fragment_state()
        # Games loaded lines use {n}
        for _ in range(20):
            result = joseph_ambient_line("games_loaded", n=5)
            if "5" in result:
                break
        self.assertIsInstance(result, str)

    def test_unknown_falls_back(self):
        from engine.joseph_brain import joseph_ambient_line, reset_fragment_state
        reset_fragment_state()
        result = joseph_ambient_line("totally_unknown_context")
        self.assertIsInstance(result, str)
        self.assertTrue(len(result) > 0)


class TestJosephCommentaryImplementation(unittest.TestCase):
    def test_with_results(self):
        from engine.joseph_brain import joseph_commentary, reset_fragment_state
        reset_fragment_state()
        results = [{"player_name": "LeBron", "joseph_edge": 8.0}]
        result = joseph_commentary(results, "analysis_results")
        self.assertIn("LeBron", result)

    def test_empty_results(self):
        from engine.joseph_brain import joseph_commentary, reset_fragment_state
        reset_fragment_state()
        result = joseph_commentary([], "analysis_results")
        self.assertIsInstance(result, str)
        self.assertTrue(len(result) > 10)

    def test_multiple_calls_vary(self):
        from engine.joseph_brain import joseph_commentary, reset_fragment_state
        reset_fragment_state()
        r1 = joseph_commentary([], "analysis_results")
        r2 = joseph_commentary([], "analysis_results")
        self.assertIsInstance(r1, str)
        self.assertIsInstance(r2, str)


class TestJosephAutoLogBetsImplementation(unittest.TestCase):
    def test_returns_tuple_with_message(self):
        from engine.joseph_brain import joseph_auto_log_bets
        count, msg = joseph_auto_log_bets([], {})
        self.assertIsInstance(count, int)
        self.assertIsInstance(msg, str)


class TestBuildJosephRantStub(unittest.TestCase):
    def test_returns_string(self):
        from engine.joseph_brain import build_joseph_rant
        result = build_joseph_rant("LeBron", {"stat_type": "points"}, "SMASH", [])
        self.assertIsInstance(result, str)
        self.assertIn("LeBron", result)


class TestJosephFullAnalysisStub(unittest.TestCase):
    def test_returns_dict_with_required_keys(self):
        from engine.joseph_brain import joseph_full_analysis
        result = joseph_full_analysis({}, {}, {}, {})
        self.assertIsInstance(result, dict)
        for key in ("verdict", "verdict_emoji", "is_override", "edge",
                     "confidence", "rant", "dawg_factor", "narrative_tags",
                     "comp", "grade"):
            self.assertIn(key, result)

    def test_verdict_emoji_matches_verdict(self):
        from engine.joseph_brain import joseph_full_analysis, VERDICT_EMOJIS
        result = joseph_full_analysis({}, {}, {}, {})
        self.assertEqual(result["verdict_emoji"],
                         VERDICT_EMOJIS.get(result["verdict"], ""))


class TestJosephAnalyzeGameStub(unittest.TestCase):
    def test_returns_dict_with_required_keys(self):
        from engine.joseph_brain import joseph_analyze_game
        result = joseph_analyze_game({}, {}, [])
        self.assertIsInstance(result, dict)
        for key in ("game_narrative", "pace_take", "scheme_analysis",
                     "blowout_risk", "best_props"):
            self.assertIn(key, result)


class TestJosephAnalyzePlayerStub(unittest.TestCase):
    def test_returns_dict_with_required_keys(self):
        from engine.joseph_brain import joseph_analyze_player
        result = joseph_analyze_player({}, [], {}, [])
        self.assertIsInstance(result, dict)
        for key in ("scouting_report", "archetype", "grade",
                     "gravity", "trend", "narrative_tags"):
            self.assertIn(key, result)


class TestJosephGenerateBestBetsStub(unittest.TestCase):
    def test_returns_dict_with_required_keys(self):
        from engine.joseph_brain import joseph_generate_best_bets
        result = joseph_generate_best_bets(3, [], {})
        self.assertIsInstance(result, dict)
        for key in ("ticket_name", "legs", "total_ev",
                     "correlation_score", "rant"):
            self.assertIn(key, result)

    def test_ticket_name_matches(self):
        from engine.joseph_brain import joseph_generate_best_bets, TICKET_NAMES
        for n in (2, 3, 4, 5, 6):
            result = joseph_generate_best_bets(n, [], {})
            self.assertEqual(result["ticket_name"], TICKET_NAMES[n])


class TestJosephQuickTakeStub(unittest.TestCase):
    def test_returns_string(self):
        from engine.joseph_brain import joseph_quick_take
        result = joseph_quick_take([], {}, [])
        self.assertIsInstance(result, str)
        self.assertTrue(len(result) > 0)


class TestJosephGetAmbientContextStub(unittest.TestCase):
    def test_returns_tuple(self):
        from engine.joseph_brain import joseph_get_ambient_context
        result = joseph_get_ambient_context({})
        self.assertIsInstance(result, tuple)
        self.assertEqual(len(result), 2)
        self.assertIsInstance(result[0], str)
        self.assertIsInstance(result[1], dict)


class TestJosephAmbientLineStub(unittest.TestCase):
    def test_returns_string(self):
        from engine.joseph_brain import joseph_ambient_line
        result = joseph_ambient_line("idle")
        self.assertIsInstance(result, str)
        self.assertTrue(len(result) > 0)

    def test_fallback_to_idle(self):
        from engine.joseph_brain import joseph_ambient_line
        result = joseph_ambient_line("nonexistent")
        self.assertIsInstance(result, str)
        self.assertTrue(len(result) > 0)


class TestJosephCommentaryStub(unittest.TestCase):
    def test_returns_string(self):
        from engine.joseph_brain import joseph_commentary
        result = joseph_commentary([], "analysis_results")
        self.assertIsInstance(result, str)
        self.assertTrue(len(result) > 0)


class TestJosephAutoLogBetsStub(unittest.TestCase):
    def test_returns_tuple(self):
        from engine.joseph_brain import joseph_auto_log_bets
        result = joseph_auto_log_bets([], {})
        self.assertIsInstance(result, tuple)
        self.assertEqual(len(result), 2)
        self.assertIsInstance(result[0], int)
        self.assertIsInstance(result[1], str)


# ── Import fallbacks for new exports ────────────────────────


class TestNewExportsImportable(unittest.TestCase):
    def test_all_new_constants_importable(self):
        from engine.joseph_brain import (
            DAWG_FACTOR_TABLE, VERDICT_EMOJIS, TICKET_NAMES,
        )
        self.assertIsInstance(DAWG_FACTOR_TABLE, dict)
        self.assertIsInstance(VERDICT_EMOJIS, dict)
        self.assertIsInstance(TICKET_NAMES, dict)

    def test_all_new_functions_importable(self):
        from engine.joseph_brain import (
            _select_fragment, build_joseph_rant,
            joseph_full_analysis, joseph_analyze_game,
            joseph_analyze_player, joseph_generate_best_bets,
            joseph_quick_take, joseph_get_ambient_context,
            joseph_ambient_line, joseph_commentary,
            joseph_auto_log_bets,
        )
        self.assertTrue(callable(_select_fragment))
        self.assertTrue(callable(build_joseph_rant))
        self.assertTrue(callable(joseph_full_analysis))
        self.assertTrue(callable(joseph_analyze_game))
        self.assertTrue(callable(joseph_analyze_player))
        self.assertTrue(callable(joseph_generate_best_bets))
        self.assertTrue(callable(joseph_quick_take))
        self.assertTrue(callable(joseph_get_ambient_context))
        self.assertTrue(callable(joseph_ambient_line))
        self.assertTrue(callable(joseph_commentary))
        self.assertTrue(callable(joseph_auto_log_bets))


# ── Independent Pick Generation Tests ──────────────────────


class TestJosephGenerateIndependentPicks(unittest.TestCase):
    """Tests for joseph_generate_independent_picks."""

    def _import(self):
        from engine.joseph_brain import joseph_generate_independent_picks
        return joseph_generate_independent_picks

    def test_importable(self):
        fn = self._import()
        self.assertTrue(callable(fn))

    def test_empty_props_returns_empty(self):
        fn = self._import()
        self.assertEqual(fn([], {}, [], {}), [])

    def test_none_props_returns_empty(self):
        fn = self._import()
        self.assertEqual(fn(None, {}, [], {}), [])

    def test_basic_pick_generation(self):
        fn = self._import()
        props = [
            {"player_name": "TestPlayer", "stat_type": "points",
             "line": 20.0, "team": "LAL", "platform": "PrizePicks"},
        ]
        players = {"testplayer": {"name": "TestPlayer", "points_avg": 25.0,
                                  "team": "LAL", "games_played": 40}}
        results = fn(props, players, [], {})
        self.assertIsInstance(results, list)
        self.assertEqual(len(results), 1)

    def test_result_has_dawg_factor(self):
        fn = self._import()
        props = [
            {"player_name": "TestPlayer", "stat_type": "points",
             "line": 20.0, "team": "LAL", "platform": "PrizePicks"},
        ]
        players = {"testplayer": {"name": "TestPlayer", "points_avg": 25.0,
                                  "team": "LAL", "games_played": 40}}
        results = fn(props, players, [], {})
        self.assertIn("dawg_factor", results[0])
        self.assertIsInstance(results[0]["dawg_factor"], (int, float))

    def test_result_has_narrative_tags(self):
        fn = self._import()
        props = [
            {"player_name": "TestPlayer", "stat_type": "points",
             "line": 20.0, "team": "LAL", "platform": "PrizePicks"},
        ]
        players = {"testplayer": {"name": "TestPlayer", "points_avg": 25.0,
                                  "team": "LAL", "games_played": 40}}
        results = fn(props, players, [], {})
        self.assertIn("narrative_tags", results[0])
        self.assertIsInstance(results[0]["narrative_tags"], list)

    def test_result_has_verdict(self):
        fn = self._import()
        props = [
            {"player_name": "TestPlayer", "stat_type": "points",
             "line": 20.0, "team": "LAL", "platform": "PrizePicks"},
        ]
        players = {"testplayer": {"name": "TestPlayer", "points_avg": 25.0,
                                  "team": "LAL", "games_played": 40}}
        results = fn(props, players, [], {})
        self.assertIn("verdict", results[0])
        self.assertIn(results[0]["verdict"], ("SMASH", "LEAN", "FADE", "STAY_AWAY"))

    def test_result_has_player_and_prop(self):
        fn = self._import()
        props = [
            {"player_name": "TestPlayer", "stat_type": "points",
             "line": 20.0, "team": "LAL", "platform": "PrizePicks"},
        ]
        players = {"testplayer": {"name": "TestPlayer", "points_avg": 25.0,
                                  "team": "LAL", "games_played": 40}}
        results = fn(props, players, [], {})
        self.assertEqual(results[0]["player"], "TestPlayer")
        self.assertEqual(results[0]["prop"], "points")
        self.assertEqual(results[0]["line"], 20.0)
        self.assertEqual(results[0]["team"], "LAL")

    def test_skips_zero_line(self):
        fn = self._import()
        props = [
            {"player_name": "TestPlayer", "stat_type": "points",
             "line": 0, "team": "LAL"},
        ]
        players = {"testplayer": {"name": "TestPlayer", "points_avg": 25.0}}
        results = fn(props, players, [], {})
        self.assertEqual(len(results), 0)

    def test_skips_zero_avg(self):
        fn = self._import()
        props = [
            {"player_name": "TestPlayer", "stat_type": "points",
             "line": 20.0, "team": "LAL"},
        ]
        players = {"testplayer": {"name": "TestPlayer", "points_avg": 0.0}}
        results = fn(props, players, [], {})
        self.assertEqual(len(results), 0)

    def test_max_picks_cap(self):
        fn = self._import()
        props = [
            {"player_name": f"Player{i}", "stat_type": "points",
             "line": 20.0, "team": "LAL"}
            for i in range(50)
        ]
        players = {
            f"player{i}": {"name": f"Player{i}", "points_avg": 25.0,
                           "team": "LAL", "games_played": 40}
            for i in range(50)
        }
        results = fn(props, players, [], {}, max_picks=5)
        self.assertLessEqual(len(results), 5)

    def test_combo_stat_support(self):
        fn = self._import()
        props = [
            {"player_name": "TestPlayer", "stat_type": "pts+rebs",
             "line": 30.0, "team": "LAL"},
        ]
        players = {"testplayer": {"name": "TestPlayer", "points_avg": 25.0,
                                  "rebounds_avg": 8.0, "team": "LAL",
                                  "games_played": 40}}
        results = fn(props, players, [], {})
        self.assertEqual(len(results), 1)

    def test_game_data_matched(self):
        fn = self._import()
        props = [
            {"player_name": "TestPlayer", "stat_type": "points",
             "line": 20.0, "team": "LAL"},
        ]
        players = {"testplayer": {"name": "TestPlayer", "points_avg": 25.0,
                                  "team": "LAL", "games_played": 40}}
        games = [{"home_team": "LAL", "away_team": "GSW", "total": 224.5}]
        results = fn(props, players, games, {})
        self.assertEqual(len(results), 1)


class TestJosephRankPicksImplementation(unittest.TestCase):
    """Tests for the implemented joseph_rank_picks."""

    def _import(self):
        from engine.joseph_brain import joseph_rank_picks
        return joseph_rank_picks

    def test_empty_returns_empty(self):
        fn = self._import()
        self.assertEqual(fn([]), [])

    def test_ranks_by_edge(self):
        fn = self._import()
        picks = [
            {"edge": 5.0, "verdict": "LEAN", "dawg_factor": 0, "confidence": 60},
            {"edge": 10.0, "verdict": "SMASH", "dawg_factor": 0, "confidence": 70},
        ]
        ranked = fn(picks)
        self.assertEqual(len(ranked), 2)
        self.assertEqual(ranked[0]["rank"], 1)
        self.assertEqual(ranked[1]["rank"], 2)
        self.assertGreater(ranked[0]["_rank_score"], ranked[1]["_rank_score"])

    def test_dawg_factor_boosts_rank(self):
        fn = self._import()
        picks = [
            {"edge": 5.0, "verdict": "LEAN", "dawg_factor": 5.0, "confidence": 60},
            {"edge": 5.0, "verdict": "LEAN", "dawg_factor": 0.0, "confidence": 60},
        ]
        ranked = fn(picks)
        self.assertEqual(ranked[0]["rank"], 1)
        self.assertGreater(ranked[0]["_rank_score"], ranked[1]["_rank_score"])

    def test_smash_verdict_boosts(self):
        fn = self._import()
        picks = [
            {"edge": 5.0, "verdict": "SMASH", "dawg_factor": 0, "confidence": 60},
            {"edge": 5.0, "verdict": "FADE", "dawg_factor": 0, "confidence": 60},
        ]
        ranked = fn(picks)
        self.assertEqual(ranked[0]["verdict"], "SMASH")

    def test_preserves_original_keys(self):
        fn = self._import()
        picks = [{"edge": 5.0, "verdict": "LEAN", "player": "TestPlayer",
                  "dawg_factor": 0, "confidence": 50}]
        ranked = fn(picks)
        self.assertEqual(ranked[0]["player"], "TestPlayer")


class TestJosephFullSlateImplementation(unittest.TestCase):
    """Tests for the implemented joseph_generate_full_slate_analysis."""

    def _import(self):
        from engine.joseph_brain import joseph_generate_full_slate_analysis
        return joseph_generate_full_slate_analysis

    def test_empty_returns_structure(self):
        fn = self._import()
        result = fn([], [], {})
        self.assertIsInstance(result, dict)
        for key in ("picks", "parlays", "top_plays", "summary_rant"):
            self.assertIn(key, result)
        self.assertEqual(len(result["picks"]), 0)

    def test_with_data_returns_picks(self):
        fn = self._import()
        players = [{"name": "TestPlayer", "points_avg": 25.0,
                     "team": "LAL", "games_played": 40}]
        props = [{"player_name": "TestPlayer", "stat_type": "points",
                  "line": 20.0, "team": "LAL"}]
        result = fn(players, props, {})
        self.assertIsInstance(result["picks"], list)
        self.assertGreater(len(result["picks"]), 0)
        self.assertIn("rank", result["picks"][0])

    def test_summary_rant_nonempty(self):
        fn = self._import()
        players = [{"name": "TestPlayer", "points_avg": 25.0,
                     "team": "LAL", "games_played": 40}]
        props = [{"player_name": "TestPlayer", "stat_type": "points",
                  "line": 20.0, "team": "LAL"}]
        result = fn(players, props, {})
        self.assertIsInstance(result["summary_rant"], str)
        self.assertGreater(len(result["summary_rant"]), 0)

    def test_top_plays_are_smash_or_lean(self):
        fn = self._import()
        players = [{"name": "TestPlayer", "points_avg": 25.0,
                     "team": "LAL", "games_played": 40}]
        props = [{"player_name": "TestPlayer", "stat_type": "points",
                  "line": 20.0, "team": "LAL"}]
        result = fn(players, props, {})
        for tp in result["top_plays"]:
            self.assertIn(tp.get("verdict", "").upper(), ("SMASH", "LEAN"))


if __name__ == "__main__":
    unittest.main()
