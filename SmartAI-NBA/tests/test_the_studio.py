# ============================================================
# FILE: tests/test_the_studio.py
# PURPOSE: Tests for pages/7_🎙️_The_Studio.py
#          (Joseph's dedicated interactive page — Layer 7)
# ============================================================
import sys, os, unittest, ast
from unittest.mock import MagicMock

# Ensure repo root is on path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


class TestStudioFileSyntax(unittest.TestCase):
    """Verify the Studio page file parses without syntax errors."""

    def setUp(self):
        self.filepath = os.path.join(
            os.path.dirname(__file__), "..",
            "pages", "7_🎙️_The_Studio.py",
        )

    def test_file_exists(self):
        self.assertTrue(os.path.isfile(self.filepath), "Studio page file should exist")

    def test_valid_python_syntax(self):
        with open(self.filepath, "r") as fh:
            source = fh.read()
        # Should not raise SyntaxError
        tree = ast.parse(source)
        self.assertIsInstance(tree, ast.Module)

    def test_file_not_empty(self):
        with open(self.filepath, "r") as fh:
            source = fh.read()
        self.assertGreater(len(source), 1000, "Studio page should be substantial")


class TestStudioFileStructure(unittest.TestCase):
    """Verify key structural elements exist in the Studio page source."""

    def setUp(self):
        self.filepath = os.path.join(
            os.path.dirname(__file__), "..",
            "pages", "7_🎙️_The_Studio.py",
        )
        with open(self.filepath, "r") as fh:
            self.source = fh.read()

    def test_page_config_present(self):
        self.assertIn("set_page_config", self.source)

    def test_page_title(self):
        self.assertIn("The Studio", self.source)

    def test_page_icon(self):
        self.assertIn("🎙️", self.source)

    def test_layout_wide(self):
        self.assertIn('layout="wide"', self.source)

    def test_global_css_import(self):
        self.assertIn("get_global_css", self.source)

    def test_qds_css_import(self):
        self.assertIn("get_qds_css", self.source)

    def test_premium_gate(self):
        self.assertIn("premium_gate", self.source)

    def test_render_global_settings(self):
        self.assertIn("render_global_settings", self.source)

    def test_hero_banner(self):
        self.assertIn("studio-hero", self.source)
        self.assertIn("God-Mode Analyst", self.source)

    def test_three_modes(self):
        self.assertIn("GAMES TONIGHT", self.source)
        self.assertIn("SCOUT A PLAYER", self.source)
        self.assertIn("BUILD MY BETS", self.source)

    def test_games_mode_imports(self):
        self.assertIn("joseph_analyze_game", self.source)

    def test_player_mode_imports(self):
        self.assertIn("joseph_analyze_player", self.source)

    def test_bets_mode_imports(self):
        self.assertIn("joseph_generate_best_bets", self.source)

    def test_leg_buttons(self):
        self.assertIn("POWER PLAY", self.source)
        self.assertIn("TRIPLE THREAT", self.source)
        self.assertIn("THE QUAD", self.source)
        self.assertIn("HIGH FIVE", self.source)
        self.assertIn("THE FULL SEND", self.source)

    def test_platform_selector(self):
        self.assertIn("PrizePicks", self.source)
        self.assertIn("Underdog Fantasy", self.source)
        self.assertIn("DraftKings Pick6", self.source)

    def test_dawg_board_section(self):
        self.assertIn("DAWG BOARD", self.source)
        self.assertIn("render_dawg_board", self.source)

    def test_override_report_section(self):
        self.assertIn("OVERRIDE REPORT", self.source)
        self.assertIn("render_override_report", self.source)

    def test_track_record_section(self):
        self.assertIn("TRACK RECORD", self.source)
        self.assertIn("joseph_get_track_record", self.source)

    def test_accuracy_by_verdict(self):
        self.assertIn("joseph_get_accuracy_by_verdict", self.source)

    def test_bet_history_section(self):
        self.assertIn("BET HISTORY", self.source)

    def test_signoff_section(self):
        self.assertIn("SIGN-OFF", self.source.upper())

    def test_payout_table(self):
        self.assertIn("PLATFORM_FLEX_TABLES", self.source)

    def test_regenerate_button(self):
        self.assertIn("Regenerate", self.source)

    def test_alternatives_expander(self):
        self.assertIn("get_alternative_tickets", self.source)

    def test_team_colors(self):
        self.assertIn("get_team_colors", self.source)

    def test_bet_card_html(self):
        self.assertIn("get_bet_card_html", self.source)

    def test_override_accuracy(self):
        self.assertIn("joseph_get_override_accuracy", self.source)

    def test_joseph_commentary(self):
        self.assertIn("joseph_commentary", self.source)

    def test_nerd_stats_expanders(self):
        self.assertIn("Nerd Stats", self.source)


class TestStudioCSS(unittest.TestCase):
    """Verify the Studio page contains all required CSS classes."""

    def setUp(self):
        self.filepath = os.path.join(
            os.path.dirname(__file__), "..",
            "pages", "7_🎙️_The_Studio.py",
        )
        with open(self.filepath, "r") as fh:
            self.source = fh.read()

    def test_studio_hero_css(self):
        self.assertIn("studio-hero", self.source)

    def test_studio_avatar_css(self):
        self.assertIn("studio-avatar-lg", self.source)
        self.assertIn("120px", self.source)

    def test_studio_game_card_css(self):
        self.assertIn("studio-game-card", self.source)

    def test_studio_ticket_card_css(self):
        self.assertIn("studio-ticket-card", self.source)

    def test_studio_metric_card_css(self):
        self.assertIn("studio-metric-card", self.source)

    def test_studio_payout_table_css(self):
        self.assertIn("studio-payout-table", self.source)

    def test_glassmorphic_styling(self):
        self.assertIn("backdrop-filter", self.source)

    def test_orange_accent(self):
        self.assertIn("#ff5e00", self.source)

    def test_orbitron_font(self):
        self.assertIn("Orbitron", self.source)

    # ── Elite NBA ESPN AI theme enhancements ─────────────────

    def test_hero_bottom_broadcast_bar(self):
        """Hero should have a bottom shimmer bar like ESPN broadcast."""
        self.assertIn("studio-hero::after", self.source)

    def test_hero_outer_glow(self):
        """Hero should have an outer glow box-shadow."""
        self.assertIn("box-shadow", self.source)

    def test_hero_title_text_shadow(self):
        """Hero title should have a neon text-shadow."""
        self.assertIn("text-shadow", self.source)

    def test_avatar_animated_pulse(self):
        """120px avatar should have an animated pulse glow ring."""
        self.assertIn("studioAvatarPulse", self.source)

    def test_game_card_left_accent(self):
        """Game cards should have ESPN-style left accent border."""
        self.assertIn("border-left", self.source)

    def test_game_card_hover_lift(self):
        """Game cards should lift on hover."""
        self.assertIn("translateY", self.source)

    def test_ticket_card_top_accent(self):
        """Ticket cards should have a top accent gradient line."""
        self.assertIn("studio-ticket-card::before", self.source)

    def test_metric_value_tabular_nums(self):
        """Metric values should use tabular-nums for scoreboard alignment."""
        self.assertIn("tabular-nums", self.source)

    def test_metric_label_uppercase(self):
        """Metric labels should be uppercase."""
        self.assertIn("text-transform:uppercase", self.source)

    def test_metric_card_hover(self):
        """Metric cards should have hover effects."""
        self.assertIn("studio-metric-card:hover", self.source)

    def test_section_title_left_border(self):
        """Section titles should have ESPN-style left accent border."""
        self.assertIn("border-left:3px solid #ff5e00", self.source)

    def test_montserrat_font(self):
        self.assertIn("Montserrat", self.source)

    def test_jetbrains_mono_font(self):
        self.assertIn("JetBrains Mono", self.source)

    def test_payout_table_tabular_nums(self):
        """Payout table should use tabular-nums for scoreboard numbers."""
        self.assertIn("tabular-nums", self.source)


class TestStudioImports(unittest.TestCase):
    """Verify the Studio page imports all required modules."""

    def setUp(self):
        self.filepath = os.path.join(
            os.path.dirname(__file__), "..",
            "pages", "7_🎙️_The_Studio.py",
        )
        with open(self.filepath, "r") as fh:
            self.source = fh.read()

    def test_imports_joseph_brain(self):
        self.assertIn("from engine.joseph_brain import", self.source)

    def test_imports_joseph_tickets(self):
        self.assertIn("from engine.joseph_tickets import", self.source)

    def test_imports_joseph_bets(self):
        self.assertIn("from engine.joseph_bets import", self.source)

    def test_imports_joseph_live_desk(self):
        self.assertIn("from pages.helpers.joseph_live_desk import", self.source)

    def test_imports_theme(self):
        self.assertIn("from styles.theme import", self.source)

    def test_imports_premium_gate(self):
        self.assertIn("from utils.premium_gate import", self.source)

    def test_imports_components(self):
        self.assertIn("from utils.components import", self.source)

    def test_imports_data_manager(self):
        self.assertIn("from data.data_manager import", self.source)

    def test_imports_entry_optimizer(self):
        self.assertIn("from engine.entry_optimizer import", self.source)

    def test_safe_import_pattern(self):
        """All major imports should use try/except for resilience."""
        # Count try blocks (should be >= 8 for all the import sections)
        try_count = self.source.count("except ImportError")
        self.assertGreaterEqual(try_count, 8, "Should have many safe import blocks")


class TestLiveDeskFileExists(unittest.TestCase):
    """Verify the live desk helper file exists."""

    def test_helper_file_exists(self):
        filepath = os.path.join(
            os.path.dirname(__file__), "..",
            "pages", "helpers", "joseph_live_desk.py",
        )
        self.assertTrue(os.path.isfile(filepath))

    def test_helper_valid_syntax(self):
        filepath = os.path.join(
            os.path.dirname(__file__), "..",
            "pages", "helpers", "joseph_live_desk.py",
        )
        with open(filepath, "r") as fh:
            source = fh.read()
        tree = ast.parse(source)
        self.assertIsInstance(tree, ast.Module)


class TestStudioPlatformPreference(unittest.TestCase):
    """Tests for Joseph's platform preference selector in The Studio."""

    def setUp(self):
        self.filepath = os.path.join(
            os.path.dirname(__file__), "..",
            "pages", "7_🎙️_The_Studio.py",
        )
        with open(self.filepath, "r") as fh:
            self.source = fh.read()

    def test_platform_preference_state_key(self):
        """Platform preference should use session state."""
        self.assertIn("joseph_preferred_platform", self.source)

    def test_platform_question_prompt(self):
        """Joseph should ask what betting app the user uses."""
        self.assertIn("What betting app are you using tonight", self.source)

    def test_platform_options_all_three(self):
        """Primary sportsbook platforms should be available."""
        self.assertIn("PrizePicks", self.source)
        self.assertIn("Underdog Fantasy", self.source)
        self.assertIn("DraftKings Pick6", self.source)

    def test_platform_radio_key(self):
        """Platform radio should have a unique key."""
        self.assertIn("joseph_platform_radio", self.source)

    def test_build_bets_uses_platform_preference(self):
        """Build My Bets should use the Joseph platform preference."""
        self.assertIn("joseph_platform", self.source)

    def test_build_bets_no_hard_block(self):
        """Build My Bets should not hard-block without analysis_results."""
        # The old line that hard-blocked: 'if not analysis_results: st.info("Run Neural")'
        # Should now have a fallback to platform_props
        self.assertIn("platform_props", self.source)


class TestFindTeamDataDictSupport(unittest.TestCase):
    """Tests for _find_team_data accepting dict-keyed teams data."""

    def setUp(self):
        from engine.joseph_strategy import _find_team_data
        self.find = _find_team_data
        self.teams_dict = {
            "LAL": {"abbreviation": "LAL", "pace": 100.5, "off_rating": 114.0},
            "BOS": {"abbreviation": "BOS", "pace": 98.5, "off_rating": 116.0},
        }
        self.teams_list = list(self.teams_dict.values())

    def test_dict_exact_key(self):
        result = self.find("LAL", self.teams_dict)
        self.assertEqual(result["pace"], 100.5)

    def test_dict_case_insensitive(self):
        result = self.find("lal", self.teams_dict)
        self.assertEqual(result["pace"], 100.5)

    def test_dict_not_found(self):
        result = self.find("XXX", self.teams_dict)
        self.assertEqual(result, {})

    def test_list_still_works(self):
        result = self.find("BOS", self.teams_list)
        self.assertEqual(result["pace"], 98.5)

    def test_empty_data(self):
        self.assertEqual(self.find("LAL", {}), {})
        self.assertEqual(self.find("LAL", []), {})
        self.assertEqual(self.find("LAL", None), {})


class TestJosephAnalyzePick(unittest.TestCase):
    """Tests for the implemented joseph_analyze_pick function."""

    def setUp(self):
        from engine.joseph_brain import joseph_analyze_pick
        self.analyze = joseph_analyze_pick
        self.player = {
            "name": "Test Player",
            "points_avg": 20.0,
            "rebounds_avg": 8.0,
            "assists_avg": 5.0,
            "games_played": 50,
        }

    def test_returns_dict(self):
        result = self.analyze(self.player, 18.5, "points", {})
        self.assertIsInstance(result, dict)

    def test_has_verdict(self):
        result = self.analyze(self.player, 18.5, "points", {})
        self.assertIn(result["verdict"], ("SMASH", "LEAN", "FADE", "STAY_AWAY"))

    def test_has_edge(self):
        result = self.analyze(self.player, 18.5, "points", {})
        self.assertIsInstance(result["edge"], float)

    def test_has_confidence(self):
        result = self.analyze(self.player, 18.5, "points", {})
        self.assertIsInstance(result["confidence"], float)

    def test_has_rant(self):
        result = self.analyze(self.player, 18.5, "points", {})
        self.assertIsInstance(result["rant"], str)
        # Rant should not be empty — it was a stub before
        self.assertGreater(len(result["rant"]), 0)

    def test_has_platform(self):
        result = self.analyze(self.player, 18.5, "points", {}, platform="Underdog")
        self.assertEqual(result["platform"], "Underdog")

    def test_has_player_name(self):
        result = self.analyze(self.player, 18.5, "points", {})
        self.assertEqual(result["player_name"], "Test Player")

    def test_has_verdict_emoji(self):
        result = self.analyze(self.player, 18.5, "points", {})
        self.assertIn("verdict_emoji", result)

    def test_has_projected_avg(self):
        result = self.analyze(self.player, 18.5, "points", {})
        self.assertIsInstance(result["projected_avg"], float)
        self.assertGreater(result["projected_avg"], 0)

    def test_graceful_empty_player(self):
        result = self.analyze({}, 10.0, "points", {})
        self.assertIn(result["verdict"], ("SMASH", "LEAN", "FADE", "STAY_AWAY"))

    def test_different_stat_types(self):
        for stat in ("points", "rebounds", "assists", "steals", "threes"):
            result = self.analyze(self.player, 5.0, stat, {})
            self.assertIn("verdict", result)


class TestBuildRant(unittest.TestCase):
    """Tests for the build_rant function (was an empty stub)."""

    def setUp(self):
        from engine.joseph_brain import build_rant
        self.build_rant = build_rant

    def test_returns_string(self):
        result = self.build_rant("SMASH", player="Test", stat="points", line="24.5")
        self.assertIsInstance(result, str)

    def test_not_empty(self):
        result = self.build_rant("SMASH", player="Test", stat="points", line="24.5")
        self.assertGreater(len(result), 0)

    def test_all_verdicts_produce_rant(self):
        for v in ("SMASH", "LEAN", "FADE", "STAY_AWAY"):
            result = self.build_rant(v, player="Player", stat="assists")
            self.assertGreater(len(result), 0, f"build_rant({v}) should produce text")


class TestGameAnalysisWithDictTeamsData(unittest.TestCase):
    """Test joseph_analyze_game works with dict-format teams_data."""

    def setUp(self):
        from engine.joseph_brain import joseph_analyze_game
        self.analyze_game = joseph_analyze_game

    def test_game_analysis_with_dict(self):
        teams_data = {
            "LAL": {"abbreviation": "LAL", "team": "LAL",
                     "pace": 100.5, "off_rating": 114.0, "def_rating": 112.0},
            "BOS": {"abbreviation": "BOS", "team": "BOS",
                     "pace": 98.5, "off_rating": 116.0, "def_rating": 107.0},
        }
        game = {"home_team": "LAL", "away_team": "BOS",
                "spread": -3.5, "total": 220.0}
        result = self.analyze_game(game, teams_data, [])
        self.assertTrue(result.get("game_narrative"), "Should produce narrative")
        self.assertTrue(result.get("joseph_game_total_take"), "Should produce total take")

    def test_game_analysis_uses_strategy_narrative(self):
        teams_data = {
            "LAL": {"abbreviation": "LAL", "team": "LAL",
                     "pace": 100.5, "off_rating": 114.0, "def_rating": 112.0},
            "BOS": {"abbreviation": "BOS", "team": "BOS",
                     "pace": 98.5, "off_rating": 116.0, "def_rating": 107.0},
        }
        game = {"home_team": "LAL", "away_team": "BOS",
                "spread": -3.5, "total": 220.0}
        result = self.analyze_game(game, teams_data, [])
        # Strategy narrative includes "visits" when teams are real
        narrative = result.get("game_narrative", "")
        self.assertIn("BOS", narrative)
        self.assertIn("LAL", narrative)

    def test_total_opinion_uses_model_projection(self):
        teams_data = {
            "LAL": {"abbreviation": "LAL", "team": "LAL",
                     "pace": 100.5, "off_rating": 114.0, "def_rating": 112.0},
            "BOS": {"abbreviation": "BOS", "team": "BOS",
                     "pace": 98.5, "off_rating": 116.0, "def_rating": 107.0},
        }
        game = {"home_team": "LAL", "away_team": "BOS",
                "spread": -3.5, "total": 220.0}
        result = self.analyze_game(game, teams_data, [])
        total_take = result.get("joseph_game_total_take", "")
        # Should mention model projection when it differs from line
        self.assertIn("model projects", total_take)

    def test_scheme_not_raw_dict(self):
        teams_data = {
            "LAL": {"abbreviation": "LAL", "team": "LAL",
                     "pace": 100.5, "off_rating": 114.0, "def_rating": 112.0},
            "BOS": {"abbreviation": "BOS", "team": "BOS",
                     "pace": 98.5, "off_rating": 116.0, "def_rating": 107.0},
        }
        game = {"home_team": "LAL", "away_team": "BOS",
                "spread": -3.5, "total": 220.0}
        result = self.analyze_game(game, teams_data, [])
        scheme = result.get("scheme_analysis", "")
        # Should not contain raw dict representation
        self.assertNotIn("primary_scheme", scheme)
        self.assertNotIn("rim_protection", scheme)
        # Should contain the extracted scheme name and both team names
        self.assertIn("LAL", scheme)
        self.assertIn("defense", scheme.lower())

    def test_game_analysis_empty_results_still_works(self):
        teams_data = {
            "LAL": {"abbreviation": "LAL", "team": "LAL",
                     "pace": 100.5, "off_rating": 114.0, "def_rating": 112.0},
        }
        game = {"home_team": "LAL", "away_team": "UNK",
                "spread": 0, "total": 220.0}
        result = self.analyze_game(game, teams_data, [])
        self.assertIsInstance(result, dict)


if __name__ == "__main__":
    unittest.main()
