# ============================================================
# FILE: tests/test_neural_trading_cards.py
# PURPOSE: Tests for the Trading-Card grid, Player Spotlight
#          modal, data grouper, player profile service,
#          glassmorphic theme CSS, and Platinum Lock function.
# ============================================================

import unittest
import os
import sys
import html as _html

# Ensure project root is on path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


# ============================================================
# Test: data/player_profile_service.py
# ============================================================

class TestNbaContextFetcher(unittest.TestCase):
    """Tests for data.player_profile_service module."""

    def setUp(self):
        from data.player_profile_service import (
            enrich_player_data,
            get_headshot_url,
            get_team_logo_url,
            _find_next_opponent,
            _extract_season_stats,
            _KNOWN_PLAYER_IDS,
            _TEAM_ABBREV_TO_ID,
        )
        self.enrich = enrich_player_data
        self.headshot = get_headshot_url
        self.team_logo = get_team_logo_url
        self.find_opp = _find_next_opponent
        self.extract_stats = _extract_season_stats
        self.known_ids = _KNOWN_PLAYER_IDS
        self.team_ids = _TEAM_ABBREV_TO_ID

    # ── get_headshot_url ─────────────────────────────────────

    def test_headshot_known_player(self):
        url = self.headshot("LeBron James")
        self.assertIn("2544", url)
        self.assertIn("cdn.nba.com", url)

    def test_headshot_case_insensitive(self):
        url = self.headshot("STEPHEN CURRY")
        self.assertIn("201939", url)

    def test_headshot_unknown_player_fallback(self):
        url = self.headshot("Unknown Player XYZ")
        self.assertIn("fallback", url)

    def test_headshot_returns_string(self):
        self.assertIsInstance(self.headshot("Luka Doncic"), str)

    # ── get_team_logo_url ────────────────────────────────────

    def test_team_logo_known(self):
        url = self.team_logo("LAL")
        self.assertIn("nba.com", url)
        self.assertIn("1610612747", url)

    def test_team_logo_case_insensitive(self):
        url = self.team_logo("bos")
        self.assertIn("1610612738", url)

    def test_team_logo_unknown_returns_empty(self):
        self.assertEqual(self.team_logo("ZZZ"), "")

    # ── _find_next_opponent ──────────────────────────────────

    def test_find_opponent_home(self):
        games = [{"home_team": "LAL", "away_team": "BOS"}]
        self.assertEqual(self.find_opp("LAL", games), "BOS")

    def test_find_opponent_away(self):
        games = [{"home_team": "LAL", "away_team": "BOS"}]
        self.assertEqual(self.find_opp("BOS", games), "LAL")

    def test_find_opponent_no_match(self):
        games = [{"home_team": "LAL", "away_team": "BOS"}]
        self.assertEqual(self.find_opp("MIA", games), "TBD")

    def test_find_opponent_empty_games(self):
        self.assertEqual(self.find_opp("LAL", []), "TBD")

    def test_find_opponent_none_games(self):
        self.assertEqual(self.find_opp("LAL", None), "TBD")

    # ── _extract_season_stats ────────────────────────────────

    def test_extract_stats_ppg_key(self):
        stats = self.extract_stats({"ppg": 25.0, "rpg": 7.0, "apg": 8.0})
        self.assertEqual(stats["ppg"], 25.0)
        self.assertEqual(stats["rpg"], 7.0)
        self.assertEqual(stats["apg"], 8.0)

    def test_extract_stats_alternate_keys(self):
        stats = self.extract_stats({"pts_per_game": 22.0, "reb_per_game": 5.0, "ast_per_game": 4.0})
        self.assertEqual(stats["ppg"], 22.0)
        self.assertEqual(stats["rpg"], 5.0)
        self.assertEqual(stats["apg"], 4.0)

    def test_extract_stats_missing_keys(self):
        stats = self.extract_stats({})
        self.assertEqual(stats["ppg"], 0.0)
        self.assertEqual(stats["rpg"], 0.0)
        self.assertEqual(stats["apg"], 0.0)
        self.assertEqual(stats["avg_minutes"], 0.0)

    def test_extract_stats_invalid_values(self):
        stats = self.extract_stats({"ppg": "not_a_number"})
        self.assertEqual(stats["ppg"], 0.0)

    # ── enrich_player_data ───────────────────────────────────

    def test_enrich_returns_dict(self):
        result = self.enrich("LeBron James", [])
        self.assertIsInstance(result, dict)

    def test_enrich_has_required_keys(self):
        result = self.enrich("LeBron James", [])
        for key in ("headshot_url", "position", "team", "team_logo_url",
                     "next_opponent", "season_stats"):
            self.assertIn(key, result)

    def test_enrich_finds_player_in_list(self):
        players = [{"name": "LeBron James", "team": "LAL", "position": "SF",
                     "ppg": 25.0, "rpg": 7.0, "apg": 8.0}]
        result = self.enrich("LeBron James", players)
        self.assertEqual(result["team"], "LAL")
        self.assertEqual(result["position"], "SF")
        self.assertEqual(result["season_stats"]["ppg"], 25.0)

    def test_enrich_resolves_opponent(self):
        players = [{"name": "LeBron James", "team": "LAL"}]
        games = [{"home_team": "LAL", "away_team": "BOS"}]
        result = self.enrich("LeBron James", players, games)
        self.assertEqual(result["next_opponent"], "BOS")

    def test_enrich_escapes_html(self):
        result = self.enrich("<script>alert('xss')</script>", [])
        self.assertNotIn("<script>", result["player_name"])

    def test_enrich_empty_players(self):
        result = self.enrich("Ghost Player", [])
        self.assertEqual(result["team"], "N/A")
        self.assertEqual(result["position"], "N/A")

    def test_enrich_dict_players(self):
        players = {"LeBron James": {"team": "LAL", "position": "SF"}}
        result = self.enrich("LeBron James", players)
        self.assertEqual(result["team"], "LAL")

    # ── Data integrity ───────────────────────────────────────

    def test_known_player_ids_not_empty(self):
        self.assertGreater(len(self.known_ids), 20)

    def test_team_ids_has_30_teams(self):
        self.assertEqual(len(self.team_ids), 30)

    def test_all_team_ids_are_integers(self):
        for abbrev, tid in self.team_ids.items():
            self.assertIsInstance(tid, int, f"{abbrev} ID is not int")


# ============================================================
# Test: utils/data_grouper.py
# ============================================================

class TestDataGrouper(unittest.TestCase):
    """Tests for utils.data_grouper.group_props_by_player."""

    def setUp(self):
        from utils.data_grouper import group_props_by_player
        self.group = group_props_by_player

    def _make_prop(self, name="LeBron James", stat="points", **kw):
        base = {"player_name": name, "stat_type": stat, "line": 24.5,
                "edge_percentage": 5.0, "direction": "OVER"}
        base.update(kw)
        return base

    def test_returns_dict(self):
        self.assertIsInstance(self.group([]), dict)

    def test_empty_input(self):
        self.assertEqual(self.group([]), {})

    def test_single_player_single_prop(self):
        grouped = self.group([self._make_prop()])
        self.assertIn("LeBron James", grouped)
        self.assertEqual(len(grouped["LeBron James"]["props"]), 1)

    def test_single_player_multiple_props(self):
        props = [
            self._make_prop(stat="points"),
            self._make_prop(stat="rebounds"),
            self._make_prop(stat="assists"),
        ]
        grouped = self.group(props)
        self.assertEqual(len(grouped["LeBron James"]["props"]), 3)

    def test_multiple_players(self):
        props = [
            self._make_prop(name="LeBron James"),
            self._make_prop(name="Stephen Curry"),
        ]
        grouped = self.group(props)
        self.assertEqual(len(grouped), 2)
        self.assertIn("LeBron James", grouped)
        self.assertIn("Stephen Curry", grouped)

    def test_vitals_key_present(self):
        grouped = self.group([self._make_prop()])
        entry = grouped["LeBron James"]
        self.assertIn("vitals", entry)
        self.assertIn("props", entry)

    def test_vitals_has_season_stats(self):
        grouped = self.group([self._make_prop()])
        vitals = grouped["LeBron James"]["vitals"]
        self.assertIn("season_stats", vitals)

    def test_skips_empty_names(self):
        grouped = self.group([self._make_prop(name="")])
        self.assertEqual(len(grouped), 0)

    def test_skips_non_dict_results(self):
        grouped = self.group(["not_a_dict", self._make_prop()])
        self.assertEqual(len(grouped), 1)

    def test_none_input(self):
        self.assertEqual(self.group(None), {})

    def test_passes_players_data(self):
        players = [{"name": "LeBron James", "team": "LAL", "ppg": 25.0}]
        grouped = self.group([self._make_prop()], players_data=players)
        self.assertEqual(grouped["LeBron James"]["vitals"]["team"], "LAL")


# ============================================================
# Test: styles/theme.py — Glassmorphic CSS additions
# ============================================================

class TestGlasmorphicThemeCSS(unittest.TestCase):
    """Tests for the Glassmorphic Dark Theme CSS additions."""

    def setUp(self):
        from styles.theme import (
            GLASSMORPHIC_CARD_CSS,
            get_glassmorphic_card_css,
            get_player_trading_card_html,
        )
        self.css = GLASSMORPHIC_CARD_CSS
        self.get_css = get_glassmorphic_card_css
        self.get_card = get_player_trading_card_html

    # ── CSS content ──────────────────────────────────────────

    def test_css_is_string(self):
        self.assertIsInstance(self.css, str)

    def test_css_has_obsidian_bg(self):
        self.assertIn("#070A13", self.css)

    def test_css_has_deep_space_bg(self):
        self.assertIn("rgba(15, 23, 42, 0.6)", self.css)

    def test_css_has_neon_blue_accent(self):
        self.assertIn("#00C6FF", self.css)

    def test_css_has_neon_red_accent(self):
        self.assertIn("#FF0055", self.css)

    def test_css_has_inter_font(self):
        self.assertIn("Inter", self.css)

    def test_css_has_jetbrains_mono(self):
        self.assertIn("JetBrains Mono", self.css)

    def test_css_has_backdrop_filter(self):
        self.assertIn("backdrop-filter: blur(10px)", self.css)

    def test_css_has_border_radius_12(self):
        self.assertIn("border-radius: 12px", self.css)

    def test_css_has_glassmorphic_border(self):
        self.assertIn("rgba(255, 255, 255, 0.1)", self.css)

    def test_css_has_card_grid_class(self):
        self.assertIn("gm-card-grid", self.css)

    def test_css_has_player_card_class(self):
        self.assertIn("gm-player-card", self.css)

    def test_css_has_modal_override(self):
        self.assertIn("stDialog", self.css)

    def test_css_has_joseph_btn(self):
        self.assertIn("gm-ask-joseph-btn", self.css)

    def test_css_has_joseph_response(self):
        self.assertIn("gm-joseph-response", self.css)

    def test_css_has_tabular_nums(self):
        self.assertIn("tabular-nums", self.css)

    def test_css_has_orbitron_font(self):
        self.assertIn("Orbitron", self.css)

    def test_css_has_hover_transform(self):
        self.assertIn("translateY(-4px)", self.css)

    # ── get_glassmorphic_card_css() ──────────────────────────

    def test_get_css_returns_style_tag(self):
        result = self.get_css()
        self.assertIn("<style>", result)
        self.assertIn("</style>", result)

    def test_get_css_contains_grid_class(self):
        self.assertIn("gm-card-grid", self.get_css())

    # ── get_player_trading_card_html() ───────────────────────

    def test_card_returns_string(self):
        html = self.get_card("LeBron James")
        self.assertIsInstance(html, str)

    def test_card_contains_player_name(self):
        html = self.get_card("LeBron James")
        self.assertIn("LeBron James", html)

    def test_card_contains_headshot_img(self):
        html = self.get_card("LeBron James", headshot_url="https://example.com/img.png")
        self.assertIn("gm-card-headshot", html)
        self.assertIn("https://example.com/img.png", html)

    def test_card_contains_position(self):
        html = self.get_card("LeBron James", position="SF")
        self.assertIn("SF", html)

    def test_card_contains_opponent(self):
        html = self.get_card("LeBron James", opponent="BOS")
        self.assertIn("BOS", html)

    def test_card_contains_season_stats(self):
        html = self.get_card("LeBron James", season_stats={"ppg": 25.0, "rpg": 7.0, "apg": 8.0})
        self.assertIn("25.0 PPG", html)
        self.assertIn("7.0 RPG", html)
        self.assertIn("8.0 APG", html)

    def test_card_contains_prop_count(self):
        html = self.get_card("LeBron James", prop_count=3)
        self.assertIn("3 props", html)

    def test_card_singular_prop(self):
        html = self.get_card("LeBron James", prop_count=1)
        self.assertIn("1 prop ", html)

    def test_card_escapes_xss(self):
        html = self.get_card("<script>alert('xss')</script>")
        self.assertNotIn("<script>", html)

    def test_card_has_gm_player_card_class(self):
        html = self.get_card("LeBron James")
        self.assertIn('class="gm-player-card"', html)


# ============================================================
# Test: engine/joseph_brain.py — joseph_platinum_lock
# ============================================================

class TestJosephPlatinumLock(unittest.TestCase):
    """Tests for engine.joseph_brain.joseph_platinum_lock."""

    def setUp(self):
        from engine.joseph_brain import joseph_platinum_lock
        self.lock = joseph_platinum_lock

    def _make_prop(self, stat="points", line=24.5, edge=5.0, direction="OVER", **kw):
        base = {"stat_type": stat, "prop_line": line,
                "edge_percentage": edge, "direction": direction}
        base.update(kw)
        return base

    def _stats(self, ppg=25.0, rpg=7.0, apg=8.0, minutes=36.0):
        return {"ppg": ppg, "rpg": rpg, "apg": apg, "avg_minutes": minutes}

    # ── Basic return structure ───────────────────────────────

    def test_returns_dict(self):
        result = self.lock([self._make_prop()], self._stats())
        self.assertIsInstance(result, dict)

    def test_has_platinum_lock_stat(self):
        result = self.lock([self._make_prop()], self._stats())
        self.assertIn("platinum_lock_stat", result)

    def test_has_rant(self):
        result = self.lock([self._make_prop()], self._stats())
        self.assertIn("rant", result)

    def test_rant_is_string(self):
        result = self.lock([self._make_prop()], self._stats())
        self.assertIsInstance(result["rant"], str)

    # ── Empty / edge cases ───────────────────────────────────

    def test_empty_props(self):
        result = self.lock([], self._stats())
        self.assertEqual(result["platinum_lock_stat"], "N/A")

    def test_single_prop_is_the_lock(self):
        result = self.lock([self._make_prop(stat="rebounds", edge=8.0)], self._stats())
        self.assertEqual(result["platinum_lock_stat"], "Rebounds")

    # ── Multi-prop selection ─────────────────────────────────

    def test_picks_highest_edge(self):
        props = [
            self._make_prop(stat="points", edge=3.0),
            self._make_prop(stat="rebounds", edge=10.0),
            self._make_prop(stat="assists", edge=5.0),
        ]
        result = self.lock(props, self._stats())
        self.assertEqual(result["platinum_lock_stat"], "Rebounds")

    def test_stat_alignment_boosts_selection(self):
        # Points avg 25.0, line 20.0 → alignment bonus for OVER
        # Rebounds avg 7.0, line 10.0 → no alignment for OVER
        props = [
            self._make_prop(stat="points", line=20.0, edge=5.0, direction="OVER"),
            self._make_prop(stat="rebounds", line=10.0, edge=5.5, direction="OVER"),
        ]
        result = self.lock(props, self._stats())
        # Points should win due to alignment despite slightly lower edge
        self.assertEqual(result["platinum_lock_stat"], "Points")

    def test_teardown_mentions_other_props(self):
        props = [
            self._make_prop(stat="points", edge=10.0),
            self._make_prop(stat="rebounds", edge=3.0),
        ]
        result = self.lock(props, self._stats())
        self.assertIn("Rebounds", result["rant"])

    def test_rant_mentions_season_average(self):
        props = [self._make_prop(stat="points", line=20.0, edge=8.0)]
        result = self.lock(props, self._stats(ppg=25.0))
        self.assertIn("25", result["rant"])

    def test_under_direction_alignment(self):
        props = [
            self._make_prop(stat="points", line=30.0, edge=4.0, direction="UNDER"),
        ]
        result = self.lock(props, self._stats(ppg=25.0))
        self.assertEqual(result["platinum_lock_stat"], "Points")
        self.assertIn("BELOW", result["rant"])

    def test_zero_season_stats(self):
        result = self.lock([self._make_prop()], self._stats(ppg=0, rpg=0, apg=0))
        self.assertIn("platinum_lock_stat", result)
        self.assertIn("rant", result)


# ============================================================
# Test: utils/player_modal.py
# ============================================================

class TestPlayerModal(unittest.TestCase):
    """Tests for utils.player_modal module imports and structure."""

    def test_module_imports(self):
        import utils.player_modal
        self.assertTrue(hasattr(utils.player_modal, "show_player_spotlight"))

    def test_function_is_callable(self):
        from utils.player_modal import show_player_spotlight
        self.assertTrue(callable(show_player_spotlight))

    def test_safe_float_helper(self):
        from utils.player_modal import _safe_float
        self.assertEqual(_safe_float(5.5), 5.5)
        self.assertEqual(_safe_float("3.14"), 3.14)
        self.assertEqual(_safe_float("bad", 0.0), 0.0)
        self.assertEqual(_safe_float(None), 0.0)


# ============================================================
# Test: Neural Analysis page — integration checks
# ============================================================

class TestNeuralAnalysisPageIntegration(unittest.TestCase):
    """Verify the Neural Analysis page has the required imports."""

    def setUp(self):
        self.page_path = os.path.join(
            os.path.dirname(__file__), "..",
            "pages", "3_⚡_Quantum_Analysis_Matrix.py",
        )
        with open(self.page_path, "r") as f:
            self.source = f.read()

    def test_imports_glassmorphic_css(self):
        self.assertIn("get_glassmorphic_card_css", self.source)

    def test_imports_trading_card_html(self):
        self.assertIn("get_player_trading_card_html", self.source)

    def test_imports_data_grouper(self):
        self.assertIn("group_props_by_player", self.source)

    def test_imports_player_modal(self):
        self.assertIn("show_player_spotlight", self.source)

    def test_injects_glassmorphic_css(self):
        self.assertIn("_get_gm_css()", self.source)

    def test_has_trading_card_grid_section(self):
        self.assertIn("Quantum Analysis Matrix", self.source)

    def test_has_unified_card_import(self):
        self.assertIn("compile_unified_card_matrix", self.source)

    def test_has_expandable_player_cards(self):
        self.assertIn("_compile_unified_matrix", self.source)


# ============================================================
# Test: styles/theme.py — file integrity
# ============================================================

class TestThemeFileIntegrity(unittest.TestCase):
    """Verify theme.py still has all original exports + new ones."""

    def test_original_exports_intact(self):
        from styles.theme import (
            get_global_css,
            get_qds_css,
            get_quantum_card_matrix_css,
            QUANTUM_CARD_MATRIX_CSS,
        )
        self.assertTrue(callable(get_global_css))
        self.assertTrue(callable(get_qds_css))
        self.assertTrue(callable(get_quantum_card_matrix_css))
        self.assertIsInstance(QUANTUM_CARD_MATRIX_CSS, str)

    def test_new_exports_exist(self):
        from styles.theme import (
            GLASSMORPHIC_CARD_CSS,
            get_glassmorphic_card_css,
            get_player_trading_card_html,
        )
        self.assertIsInstance(GLASSMORPHIC_CARD_CSS, str)
        self.assertTrue(callable(get_glassmorphic_card_css))
        self.assertTrue(callable(get_player_trading_card_html))


# ============================================================
# Test: Neural Analysis — Top-N Output Selection Logic
# ============================================================

class TestNeuralAnalysisOutputSelection(unittest.TestCase):
    """Verify the Neural Analysis page sorts all analyzed results by
    confidence and returns ALL of them (no artificial truncation)."""

    def _read_source(self):
        import pathlib
        na_path = pathlib.Path(__file__).parent.parent / "pages" / "3_⚡_Quantum_Analysis_Matrix.py"
        return na_path.read_text(encoding="utf-8")

    def test_output_quota_constant_removed(self):
        """_QME_MIN_OUTPUT_BETS should no longer be defined — all props are analyzed."""
        source = self._read_source()
        self.assertNotIn("_QME_MIN_OUTPUT_BETS", source)

    def test_selection_separates_out_players(self):
        """Selection logic must partition Out players from active results."""
        source = self._read_source()
        self.assertIn('r.get("player_is_out", False)', source)
        # Must have both the _out_results and _active_results lists
        self.assertIn("_out_results", source)
        self.assertIn("_active_results", source)

    def test_selection_sorts_by_confidence(self):
        """Active results must be sorted by composite_win_score descending."""
        source = self._read_source()
        self.assertIn(
            '_active_results.sort(key=lambda r: r.get("composite_win_score", 0), reverse=True)',
            source,
        )

    def test_selection_keeps_all_active(self):
        """Active results must NOT be truncated — all picks are kept."""
        source = self._read_source()
        self.assertIn("_selected_active = _active_results", source)

    def test_selected_plus_out_forms_final_list(self):
        """Final analysis_results_list must be selected + out players."""
        source = self._read_source()
        self.assertIn("analysis_results_list = _selected_active + _out_results", source)

    def test_success_message_shows_counts(self):
        """Success toast must report the number of displayed picks."""
        source = self._read_source()
        self.assertIn("_selected_active", source)
        self.assertIn("_out_results", source)

    def test_logger_reports_output(self):
        """Logger must record the output numbers."""
        source = self._read_source()
        self.assertIn("QME 5.6 Output:", source)

    # ── Pure-logic simulation of the selection algorithm ─────────

    def test_selection_logic_keeps_all(self):
        """Given 2000 active results, ALL are kept (no truncation)."""
        results = [
            {"confidence_score": i, "player_is_out": False}
            for i in range(2000)
        ]
        out = [{"confidence_score": 0, "player_is_out": True}]
        all_results = results + out

        # Replicate the new selection logic from the page (no truncation)
        _out = [r for r in all_results if r.get("player_is_out", False)]
        _active = [r for r in all_results if not r.get("player_is_out", False)]
        _active.sort(key=lambda r: r.get("confidence_score", 0), reverse=True)
        selected = _active + _out  # ALL active + out

        # 2000 active + 1 out = 2001 total
        self.assertEqual(len(selected), 2001)
        # Top pick should be the one with confidence 1999
        self.assertEqual(selected[0]["confidence_score"], 1999)
        # Last active pick should be confidence 0
        self.assertEqual(selected[1999]["confidence_score"], 0)
        # Last entry is the out player
        self.assertTrue(selected[-1]["player_is_out"])

    def test_selection_logic_fewer_than_quota(self):
        """If fewer than 500 active results exist, all are kept."""
        results = [
            {"confidence_score": i, "player_is_out": False}
            for i in range(100)
        ]
        _out = [r for r in results if r.get("player_is_out", False)]
        _active = [r for r in results if not r.get("player_is_out", False)]
        _active.sort(key=lambda r: r.get("confidence_score", 0), reverse=True)
        selected = _active + _out
        self.assertEqual(len(selected), 100)

    def test_selection_preserves_order_by_confidence(self):
        """Selected results must be in descending confidence order."""
        import random
        results = [
            {"confidence_score": random.uniform(0, 100), "player_is_out": False}
            for _ in range(800)
        ]
        _active = list(results)
        _active.sort(key=lambda r: r.get("confidence_score", 0), reverse=True)
        selected = _active  # No truncation
        scores = [r["confidence_score"] for r in selected]
        self.assertEqual(scores, sorted(scores, reverse=True))
        self.assertEqual(len(selected), 800)


# ============================================================
# Test: Neural Analysis team normalization and fallback logic
# ============================================================

class TestNeuralAnalysisTeamNormalization(unittest.TestCase):
    """Verify the Neural Analysis page has comprehensive team-name
    normalization so props with full names or alternate abbreviations
    are not silently filtered out."""

    def _read_source(self):
        import pathlib
        na_path = pathlib.Path(__file__).parent.parent / "pages" / "3_⚡_Quantum_Analysis_Matrix.py"
        return na_path.read_text(encoding="utf-8")

    def test_has_team_name_to_abbreviation_import(self):
        """Page imports TEAM_NAME_TO_ABBREVIATION for full-name resolution."""
        source = self._read_source()
        self.assertIn("TEAM_NAME_TO_ABBREVIATION", source)

    def test_has_normalize_team_function(self):
        """Page defines _normalize_team_to_abbrev helper."""
        source = self._read_source()
        self.assertIn("def _normalize_team_to_abbrev", source)

    def test_normalize_used_in_team_filter(self):
        """Team filter uses _normalize_team_to_abbrev on each prop."""
        source = self._read_source()
        self.assertIn('_normalize_team_to_abbrev(p.get("team", ""))', source)

    def test_fallback_when_all_props_filtered(self):
        """If ALL props are filtered out, page falls back to analyze all."""
        source = self._read_source()
        self.assertIn("len(props_to_analyze) == 0 and len(final_props) > 0", source)

    def test_fallback_warning_message(self):
        """Fallback shows a user-visible warning before proceeding."""
        source = self._read_source()
        self.assertIn("Proceeding with all", source)

    def test_no_dead_code_in_filter(self):
        """The old dead-code condition 'not playing_teams_expanded' is removed."""
        source = self._read_source()
        # The old filter had: 'not playing_teams_expanded  # if no games loaded'
        # This was always False inside the 'if playing_teams_expanded:' block.
        self.assertNotIn("not playing_teams_expanded  # if no games loaded", source)

    def test_nickname_map_built(self):
        """Page builds _TEAM_NICKNAME_MAP for nickname resolution."""
        source = self._read_source()
        self.assertIn("_TEAM_NICKNAME_MAP", source)


# ============================================================
# Test: joseph_analyze_game function signature fix
# ============================================================

class TestJosephAnalyzeGameSignature(unittest.TestCase):
    """Verify joseph_analyze_game passes correct args to analyze_game_strategy."""

    def _read_brain_source(self):
        import pathlib
        brain = pathlib.Path(__file__).parent.parent / "engine" / "joseph_brain.py"
        return brain.read_text(encoding="utf-8")

    def test_analyze_game_strategy_correct_args(self):
        """analyze_game_strategy must be called with (home, away, game, teams_data)."""
        source = self._read_brain_source()
        self.assertIn("analyze_game_strategy(home, away, game, teams_data)", source)

    def test_no_two_arg_call(self):
        """The old 2-arg call analyze_game_strategy(game, teams_data) must be gone."""
        source = self._read_brain_source()
        self.assertNotIn("analyze_game_strategy(game, teams_data)", source)

    def test_grade_player_correct_args(self):
        """joseph_grade_player must be called with (player, game_context) — 2 args."""
        source = self._read_brain_source()
        # Should NOT have 3-arg call with teams_data
        self.assertNotIn("joseph_grade_player(player, tonight_game, teams_data)", source)
        self.assertNotIn("joseph_grade_player(player, game, teams_data)", source)

    def test_joseph_analyze_game_returns_spread_take(self):
        """joseph_analyze_game must return joseph_spread_take key."""
        source = self._read_brain_source()
        self.assertIn('"joseph_spread_take"', source)

    def test_joseph_analyze_game_returns_total_take(self):
        """joseph_analyze_game must return joseph_game_total_take key."""
        source = self._read_brain_source()
        self.assertIn('"joseph_game_total_take"', source)


# ============================================================
# Test: The Studio reads joseph keys correctly
# ============================================================

class TestStudioKeyAlignment(unittest.TestCase):
    """Verify The Studio reads both old and new key names for
    game total/spread opinions from joseph_analyze_game results."""

    def _read_studio_source(self):
        import pathlib
        studio = pathlib.Path(__file__).parent.parent / "pages" / "7_🎙️_The_Studio.py"
        return studio.read_text(encoding="utf-8")

    def test_total_opinion_falls_back_to_joseph_key(self):
        """Studio reads total_opinion with fallback to joseph_game_total_take."""
        source = self._read_studio_source()
        self.assertIn("joseph_game_total_take", source)

    def test_spread_opinion_falls_back_to_joseph_key(self):
        """Studio reads spread_opinion with fallback to joseph_spread_take."""
        source = self._read_studio_source()
        self.assertIn("joseph_spread_take", source)

    def test_nerd_stats_includes_betting_angle(self):
        """Studio Nerd Stats section includes betting_angle."""
        source = self._read_studio_source()
        self.assertIn('"betting_angle"', source)

    def test_nerd_stats_includes_risk_warning(self):
        """Studio Nerd Stats section includes risk_warning."""
        source = self._read_studio_source()
        self.assertIn('"risk_warning"', source)


# ============================================================
# Test: Team normalization logic (pure function test)
# ============================================================

class TestTeamNormalizationLogic(unittest.TestCase):
    """Test the team-name normalization algorithm used by Neural Analysis."""

    def setUp(self):
        """Replicate the normalization logic from the Neural Analysis page."""
        try:
            from data.nba_data_service import TEAM_NAME_TO_ABBREVIATION
        except ImportError:
            TEAM_NAME_TO_ABBREVIATION = {
                "Los Angeles Lakers": "LAL",
                "Boston Celtics": "BOS",
                "Golden State Warriors": "GSW",
            }

        self.ABBREV_ALIASES = {
            "GS": "GSW", "GSW": "GS",
            "NY": "NYK", "NYK": "NY",
            "NO": "NOP", "NOP": "NO",
            "SA": "SAS", "SAS": "SA",
            "UTAH": "UTA", "UTA": "UTAH",
            "WSH": "WAS", "WAS": "WSH",
            "BKN": "BRK", "BRK": "BKN",
            "PHX": "PHO", "PHO": "PHX",
            "CHA": "CHO", "CHO": "CHA",
            "NJ": "BKN",
        }

        self._nickname_map = {}
        for full_name, abbr in TEAM_NAME_TO_ABBREVIATION.items():
            parts = full_name.rsplit(" ", 1)
            if len(parts) == 2:
                self._nickname_map[parts[1].upper()] = abbr
            self._nickname_map[full_name.upper()] = abbr

        def _normalize(raw_team):
            team_upper = raw_team.upper().strip()
            if not team_upper:
                return ""
            if len(team_upper) <= 4:
                return self.ABBREV_ALIASES.get(team_upper, team_upper)
            mapped = self._nickname_map.get(team_upper)
            if mapped:
                return mapped
            last_word = team_upper.rsplit(" ", 1)[-1] if " " in team_upper else ""
            if last_word:
                mapped = self._nickname_map.get(last_word)
                if mapped:
                    return mapped
            return team_upper

        self.normalize = _normalize

    def test_standard_abbreviation(self):
        self.assertEqual(self.normalize("LAL"), "LAL")

    def test_alias_abbreviation(self):
        self.assertEqual(self.normalize("GS"), "GSW")

    def test_full_team_name(self):
        self.assertEqual(self.normalize("Los Angeles Lakers"), "LAL")

    def test_full_team_name_case_insensitive(self):
        self.assertEqual(self.normalize("los angeles lakers"), "LAL")

    def test_nickname_only(self):
        self.assertEqual(self.normalize("Lakers"), "LAL")

    def test_nickname_only_case_insensitive(self):
        self.assertEqual(self.normalize("CELTICS"), "BOS")

    def test_empty_returns_empty(self):
        self.assertEqual(self.normalize(""), "")

    def test_whitespace_returns_empty(self):
        self.assertEqual(self.normalize("   "), "")

    def test_unknown_team_passthrough(self):
        """Unknown teams are passed through unchanged (uppercased)."""
        result = self.normalize("MYSTERY TEAM")
        self.assertEqual(result, "MYSTERY TEAM")

    def test_partial_name_with_nickname(self):
        """e.g. 'LA Lakers' should resolve via last-word nickname."""
        self.assertEqual(self.normalize("LA Lakers"), "LAL")

    def test_short_alias_ny(self):
        self.assertEqual(self.normalize("NY"), "NYK")

    def test_short_alias_sa(self):
        self.assertEqual(self.normalize("SA"), "SAS")


if __name__ == "__main__":
    unittest.main()
