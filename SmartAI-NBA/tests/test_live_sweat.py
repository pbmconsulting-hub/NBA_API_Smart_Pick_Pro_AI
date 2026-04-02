# ============================================================
# FILE: tests/test_live_sweat.py
# PURPOSE: Unit tests for the Live Sweat dashboard modules:
#          data/live_game_tracker.py, engine/live_math.py,
#          styles/live_theme.py, agent/live_persona.py
# ============================================================

import unittest
import sys
import os

# Ensure the project root is on the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


# ============================================================
# SECTION 1: engine/live_math tests
# ============================================================

class TestLiveMathPacing(unittest.TestCase):
    """Tests for calculate_live_pace()."""

    def setUp(self):
        from engine.live_math import calculate_live_pace
        self.calc = calculate_live_pace

    def test_basic_pace_projection(self):
        """Player with 10 pts in 20 min should project forward using remaining mins."""
        result = self.calc(10, 20, 24.5)
        # Projection uses remaining minutes, not naive *48
        self.assertGreater(result["projected_final"], 10)
        self.assertIn("minutes_remaining", result)

    def test_cashed_flag(self):
        """If current_stat >= target, cashed must be True for OVER."""
        result = self.calc(25, 30, 24.5)
        self.assertTrue(result["cashed"])

    def test_not_cashed(self):
        result = self.calc(10, 20, 24.5)
        self.assertFalse(result["cashed"])

    def test_zero_minutes_safe(self):
        """Zero minutes should not cause division-by-zero."""
        result = self.calc(0, 0, 20)
        self.assertEqual(result["projected_final"], 0.0)

    def test_blowout_risk_third_quarter(self):
        """Score diff > 20 in Q3 triggers blowout_risk."""
        result = self.calc(10, 20, 30, live_score_diff=25, period="3")
        self.assertTrue(result["blowout_risk"])

    def test_no_blowout_first_quarter(self):
        """Blowout risk should not trigger in Q1 even with big diff."""
        result = self.calc(10, 20, 30, live_score_diff=30, period="1")
        self.assertFalse(result["blowout_risk"])

    def test_blowout_risk_fourth_quarter(self):
        result = self.calc(10, 20, 30, live_score_diff=25, period="Q4")
        self.assertTrue(result["blowout_risk"])

    def test_foul_trouble_first_half(self):
        """3+ fouls with < 24 min triggers foul_trouble."""
        result = self.calc(8, 18, 25, current_fouls=3)
        self.assertTrue(result["foul_trouble"])

    def test_no_foul_trouble_second_half(self):
        """3 fouls after 24 min should NOT be foul trouble."""
        result = self.calc(15, 30, 25, current_fouls=3)
        self.assertFalse(result["foul_trouble"])

    def test_distance_calculation(self):
        result = self.calc(10, 20, 25)
        self.assertAlmostEqual(result["distance"], 15.0, places=1)

    def test_distance_when_cashed(self):
        result = self.calc(30, 20, 25)
        self.assertAlmostEqual(result["distance"], 0.0, places=1)

    def test_pct_of_target(self):
        result = self.calc(12, 24, 24)
        self.assertGreater(result["pct_of_target"], 0)

    def test_pace_per_minute(self):
        result = self.calc(10, 20, 25)
        self.assertAlmostEqual(result["pace_per_minute"], 0.5, places=2)

    def test_negative_inputs_clamped(self):
        result = self.calc(-5, -10, 20)
        self.assertEqual(result["current_stat"], 0.0)
        self.assertEqual(result["minutes_played"], 0.0)

    def test_return_keys(self):
        result = self.calc(10, 20, 25)
        expected_keys = {
            "current_stat", "target_stat", "distance", "minutes_played",
            "minutes_remaining", "pace_per_minute", "projected_final",
            "pct_of_target", "blowout_risk", "foul_trouble", "on_pace",
            "cashed", "direction", "is_overtime",
        }
        self.assertEqual(set(result.keys()), expected_keys)

    # ── New: Direction-aware tests ───────────────────────────

    def test_direction_over_default(self):
        """Default direction should be OVER."""
        result = self.calc(10, 20, 25)
        self.assertEqual(result["direction"], "OVER")

    def test_direction_under_on_pace(self):
        """UNDER bet: projected below target means on_pace=True."""
        result = self.calc(3, 20, 25, direction="UNDER")
        self.assertEqual(result["direction"], "UNDER")
        # Low stat → projected below target → on_pace for UNDER
        self.assertTrue(result["on_pace"])

    def test_direction_under_not_on_pace(self):
        """UNDER bet: projected above target means on_pace=False."""
        result = self.calc(20, 20, 25, direction="UNDER")
        self.assertFalse(result["on_pace"])

    def test_direction_under_never_cashed_early(self):
        """UNDER bets shouldn't cash during the game (only at final)."""
        result = self.calc(5, 30, 25, direction="UNDER")
        self.assertFalse(result["cashed"])

    def test_invalid_direction_defaults_over(self):
        result = self.calc(10, 20, 25, direction="INVALID")
        self.assertEqual(result["direction"], "OVER")

    # ── New: Overtime handling tests ─────────────────────────

    def test_overtime_detection(self):
        """OT period should set is_overtime=True."""
        result = self.calc(20, 40, 30, period="OT")
        self.assertTrue(result["is_overtime"])

    def test_overtime_ot1(self):
        result = self.calc(20, 40, 30, period="OT1")
        self.assertTrue(result["is_overtime"])

    def test_overtime_ot2(self):
        result = self.calc(20, 44, 30, period="OT2")
        self.assertTrue(result["is_overtime"])

    def test_not_overtime_regulation(self):
        result = self.calc(20, 30, 30, period="3")
        self.assertFalse(result["is_overtime"])

    # ── New: Minutes remaining tests ─────────────────────────

    def test_minutes_remaining_present(self):
        result = self.calc(10, 20, 25, period="2")
        self.assertIn("minutes_remaining", result)
        self.assertGreaterEqual(result["minutes_remaining"], 0)

    def test_minutes_remaining_decreases_with_time(self):
        """Later in the game → fewer minutes remaining."""
        early = self.calc(5, 10, 25, period="1")
        late = self.calc(20, 36, 25, period="4")
        self.assertGreater(early["minutes_remaining"], late["minutes_remaining"])

    # ── New: Remaining-time projection accuracy ──────────────

    def test_projection_uses_remaining_time(self):
        """With 40 min played, projection should NOT just be pace*48."""
        result = self.calc(20, 40, 30, period="4")
        naive_48 = (20 / 40) * 48  # = 24
        # Remaining-time projection: 20 + (0.5 * remaining) which should
        # be close to 20 + small amount, not the naive 24
        self.assertNotAlmostEqual(result["projected_final"], naive_48, places=0)


class TestPaceColorTier(unittest.TestCase):
    """Tests for pace_color_tier()."""

    def setUp(self):
        from engine.live_math import pace_color_tier
        self.tier = pace_color_tier

    def test_blue(self):
        self.assertEqual(self.tier(30), "blue")
        self.assertEqual(self.tier(0), "blue")
        self.assertEqual(self.tier(50), "blue")

    def test_orange(self):
        self.assertEqual(self.tier(51), "orange")
        self.assertEqual(self.tier(75), "orange")
        self.assertEqual(self.tier(85), "orange")

    def test_red(self):
        self.assertEqual(self.tier(86), "red")
        self.assertEqual(self.tier(95), "red")
        self.assertEqual(self.tier(99), "red")

    def test_green(self):
        self.assertEqual(self.tier(100), "green")
        self.assertEqual(self.tier(150), "green")

    # ── New: UNDER direction tests ───────────────────────────

    def test_under_low_pct_is_green(self):
        """For UNDER, low projection = green (good)."""
        self.assertEqual(self.tier(50, "UNDER"), "green")

    def test_under_high_pct_is_orange(self):
        self.assertEqual(self.tier(90, "UNDER"), "orange")

    def test_under_over_100_is_red(self):
        """UNDER: projected > target = red (bad)."""
        self.assertEqual(self.tier(105, "UNDER"), "red")


class TestParsePeriod(unittest.TestCase):
    """Tests for _parse_period helper."""

    def setUp(self):
        from engine.live_math import _parse_period
        self.parse = _parse_period

    def test_numeric(self):
        self.assertEqual(self.parse("1"), (1, False))
        self.assertEqual(self.parse("4"), (4, False))

    def test_q_prefix(self):
        self.assertEqual(self.parse("Q3"), (3, False))

    def test_ot(self):
        self.assertEqual(self.parse("OT"), (5, True))

    def test_ot1(self):
        self.assertEqual(self.parse("OT1"), (5, True))

    def test_ot2(self):
        self.assertEqual(self.parse("OT2"), (6, True))

    def test_empty(self):
        self.assertEqual(self.parse(""), (0, False))

    def test_invalid(self):
        self.assertEqual(self.parse("halftime"), (0, False))


# ============================================================
# SECTION 2: data/live_game_tracker entity matcher tests
# ============================================================

class TestMatchLivePlayer(unittest.TestCase):
    """Tests for match_live_player()."""

    def setUp(self):
        from data.live_game_tracker import match_live_player
        self.match = match_live_player
        self.players = [
            {"name": "Shai Gilgeous-Alexander", "pts": 30},
            {"name": "LeBron James", "pts": 22},
            {"name": "Anthony Edwards", "pts": 18},
            {"name": "Nikola Jokic", "pts": 25},
            {"name": "Stephen Curry", "pts": 28},
            {"name": "Bam Adebayo", "pts": 14},
            {"name": "Victor Wembanyama", "pts": 20},
            {"name": "Jalen Brunson", "pts": 24},
        ]

    def test_exact_match(self):
        result = self.match("LeBron James", self.players)
        self.assertIsNotNone(result)
        self.assertEqual(result["name"], "LeBron James")

    def test_case_insensitive(self):
        result = self.match("lebron james", self.players)
        self.assertIsNotNone(result)
        self.assertEqual(result["name"], "LeBron James")

    def test_fuzzy_match(self):
        result = self.match("Lebron Jame", self.players)
        self.assertIsNotNone(result)
        self.assertEqual(result["name"], "LeBron James")

    def test_nickname_sga(self):
        result = self.match("SGA", self.players)
        self.assertIsNotNone(result)
        self.assertEqual(result["name"], "Shai Gilgeous-Alexander")

    def test_nickname_lbj(self):
        result = self.match("LBJ", self.players)
        self.assertIsNotNone(result)
        self.assertEqual(result["name"], "LeBron James")

    def test_nickname_ant(self):
        result = self.match("ant", self.players)
        self.assertIsNotNone(result)
        self.assertEqual(result["name"], "Anthony Edwards")

    def test_nickname_steph(self):
        result = self.match("steph", self.players)
        self.assertIsNotNone(result)
        self.assertEqual(result["name"], "Stephen Curry")

    def test_nickname_jokic(self):
        result = self.match("jokic", self.players)
        self.assertIsNotNone(result)
        self.assertEqual(result["name"], "Nikola Jokic")

    def test_substring_match(self):
        """Partial name should still match."""
        result = self.match("Gilgeous", self.players)
        self.assertIsNotNone(result)
        self.assertEqual(result["name"], "Shai Gilgeous-Alexander")

    def test_no_match(self):
        result = self.match("zzz_nonexistent", self.players)
        self.assertIsNone(result)

    def test_empty_target(self):
        self.assertIsNone(self.match("", self.players))

    def test_empty_list(self):
        self.assertIsNone(self.match("LeBron James", []))

    def test_none_inputs(self):
        self.assertIsNone(self.match(None, self.players))
        self.assertIsNone(self.match("LeBron", None))

    # ── New: Expanded nickname tests ─────────────────────────

    def test_nickname_bam(self):
        result = self.match("bam", self.players)
        self.assertIsNotNone(result)
        self.assertEqual(result["name"], "Bam Adebayo")

    def test_nickname_wemby(self):
        result = self.match("wemby", self.players)
        self.assertIsNotNone(result)
        self.assertEqual(result["name"], "Victor Wembanyama")

    def test_nickname_brunson(self):
        result = self.match("brunson", self.players)
        self.assertIsNotNone(result)
        self.assertEqual(result["name"], "Jalen Brunson")

    def test_nickname_lebron(self):
        """'lebron' (without James) should still resolve."""
        result = self.match("lebron", self.players)
        self.assertIsNotNone(result)
        self.assertEqual(result["name"], "LeBron James")


class TestGetAllLivePlayers(unittest.TestCase):

    def test_flattens_players(self):
        from data.live_game_tracker import get_all_live_players
        games = [
            {
                "home_players": [{"name": "A", "pts": 1}],
                "away_players": [{"name": "B", "pts": 2}],
            },
            {
                "home_players": [{"name": "C", "pts": 3}],
                "away_players": [],
            },
        ]
        result = get_all_live_players(games)
        self.assertEqual(len(result), 3)
        names = {p["name"] for p in result}
        self.assertEqual(names, {"A", "B", "C"})


# ============================================================
# SECTION 3: styles/live_theme tests
# ============================================================

class TestLiveThemeCSS(unittest.TestCase):
    """Tests for the Live Sweat CSS generator."""

    def test_returns_style_tag(self):
        from styles.live_theme import get_live_sweat_css
        css = get_live_sweat_css()
        self.assertIn("<style>", css)
        self.assertIn("</style>", css)

    def test_contains_sweat_card(self):
        from styles.live_theme import get_live_sweat_css
        css = get_live_sweat_css()
        self.assertIn(".sweat-card", css)
        self.assertIn("backdrop-filter", css)
        self.assertIn("blur(12px)", css)

    def test_progress_fill_classes(self):
        from styles.live_theme import get_live_sweat_css
        css = get_live_sweat_css()
        for cls in ("progress-fill-blue", "progress-fill-orange",
                     "progress-fill-red", "progress-fill-green"):
            self.assertIn(cls, css, f"Missing {cls}")

    def test_progress_base(self):
        from styles.live_theme import get_live_sweat_css
        css = get_live_sweat_css()
        self.assertIn(".progress-base", css)

    def test_pulse_animation(self):
        from styles.live_theme import get_live_sweat_css
        css = get_live_sweat_css()
        self.assertIn("pulse-red", css)

    def test_green_glow(self):
        from styles.live_theme import get_live_sweat_css
        css = get_live_sweat_css()
        self.assertIn("box-shadow", css)

    # ── New: Direction badge CSS ─────────────────────────────

    def test_direction_badge_over(self):
        from styles.live_theme import get_live_sweat_css
        css = get_live_sweat_css()
        self.assertIn(".sweat-badge-over", css)

    def test_direction_badge_under(self):
        from styles.live_theme import get_live_sweat_css
        css = get_live_sweat_css()
        self.assertIn(".sweat-badge-under", css)

    def test_waiting_card_style(self):
        from styles.live_theme import get_live_sweat_css
        css = get_live_sweat_css()
        self.assertIn(".sweat-card-waiting", css)

    def test_ot_badge_style(self):
        from styles.live_theme import get_live_sweat_css
        css = get_live_sweat_css()
        self.assertIn(".sweat-badge-ot", css)

    def test_pct_label_style(self):
        from styles.live_theme import get_live_sweat_css
        css = get_live_sweat_css()
        self.assertIn(".progress-pct-label", css)


class TestRenderProgressBar(unittest.TestCase):

    def test_returns_html(self):
        from styles.live_theme import render_progress_bar
        html = render_progress_bar(50.0, "blue")
        self.assertIn("progress-fill-blue", html)
        self.assertIn("progress-base", html)

    def test_clamps_above_100(self):
        from styles.live_theme import render_progress_bar
        html = render_progress_bar(150, "green")
        self.assertIn("width:100.0%", html)

    def test_clamps_below_0(self):
        from styles.live_theme import render_progress_bar
        html = render_progress_bar(-10, "blue")
        self.assertIn("width:0", html)

    def test_pct_label_present(self):
        from styles.live_theme import render_progress_bar
        html = render_progress_bar(75, "orange")
        self.assertIn("75%", html)
        self.assertIn("progress-pct-label", html)


class TestRenderSweatCard(unittest.TestCase):

    def test_basic_card(self):
        from styles.live_theme import render_sweat_card
        html = render_sweat_card(
            player_name="LeBron James",
            stat_type="points",
            current_stat=20,
            target_stat=25.5,
            projected_final=28.0,
            pct_of_target=110,
            color_tier="green",
        )
        self.assertIn("LeBron James", html)
        self.assertIn("sweat-card", html)

    def test_cashed_card(self):
        from styles.live_theme import render_sweat_card
        html = render_sweat_card(
            player_name="Test", stat_type="points",
            current_stat=30, target_stat=25,
            projected_final=35, pct_of_target=140,
            color_tier="green", cashed=True,
        )
        self.assertIn("sweat-card-cashed", html)
        self.assertIn("CASHED", html)

    def test_blowout_badge(self):
        from styles.live_theme import render_sweat_card
        html = render_sweat_card(
            player_name="Test", stat_type="points",
            current_stat=10, target_stat=25,
            projected_final=15, pct_of_target=60,
            color_tier="orange", blowout_risk=True,
        )
        self.assertIn("Blowout Risk", html)

    def test_foul_badge(self):
        from styles.live_theme import render_sweat_card
        html = render_sweat_card(
            player_name="Test", stat_type="points",
            current_stat=10, target_stat=25,
            projected_final=15, pct_of_target=60,
            color_tier="orange", foul_trouble=True,
        )
        self.assertIn("Foul Trouble", html)

    def test_html_escape(self):
        """Player name with HTML special chars must be escaped."""
        from styles.live_theme import render_sweat_card
        html = render_sweat_card(
            player_name='<script>alert("xss")</script>',
            stat_type="points", current_stat=10, target_stat=25,
            projected_final=15, pct_of_target=60, color_tier="blue",
        )
        self.assertNotIn("<script>", html)
        self.assertIn("&lt;script&gt;", html)

    # ── New: Direction display tests ─────────────────────────

    def test_over_direction_badge(self):
        from styles.live_theme import render_sweat_card
        html = render_sweat_card(
            player_name="Test", stat_type="points",
            current_stat=10, target_stat=25,
            projected_final=20, pct_of_target=80,
            color_tier="orange", direction="OVER",
        )
        self.assertIn("OVER", html)
        self.assertIn("sweat-badge-over", html)

    def test_under_direction_badge(self):
        from styles.live_theme import render_sweat_card
        html = render_sweat_card(
            player_name="Test", stat_type="points",
            current_stat=10, target_stat=25,
            projected_final=18, pct_of_target=72,
            color_tier="green", direction="UNDER",
        )
        self.assertIn("UNDER", html)
        self.assertIn("sweat-badge-under", html)

    def test_minutes_remaining_display(self):
        from styles.live_theme import render_sweat_card
        html = render_sweat_card(
            player_name="Test", stat_type="points",
            current_stat=10, target_stat=25,
            projected_final=20, pct_of_target=80,
            color_tier="orange", minutes_remaining=12.0,
        )
        self.assertIn("12 MIN left", html)

    def test_ot_badge(self):
        from styles.live_theme import render_sweat_card
        html = render_sweat_card(
            player_name="Test", stat_type="points",
            current_stat=25, target_stat=30,
            projected_final=28, pct_of_target=93,
            color_tier="red", is_overtime=True,
        )
        self.assertIn("sweat-badge-ot", html)


class TestRenderWaitingCard(unittest.TestCase):
    """Tests for the awaiting tip-off card."""

    def test_basic_waiting_card(self):
        from styles.live_theme import render_waiting_card
        html = render_waiting_card("LeBron James", "points", 25.5)
        self.assertIn("LeBron James", html)
        self.assertIn("sweat-card-waiting", html)
        self.assertIn("Awaiting Tip-Off", html)

    def test_direction_shown(self):
        from styles.live_theme import render_waiting_card
        html = render_waiting_card("Test", "rebounds", 10.5, "UNDER")
        self.assertIn("UNDER", html)

    def test_html_escape(self):
        from styles.live_theme import render_waiting_card
        html = render_waiting_card('<script>x</script>', "pts", 20)
        self.assertNotIn("<script>", html)


# ============================================================
# SECTION 4: agent/live_persona tests
# ============================================================

class TestJosephLiveReaction(unittest.TestCase):

    def test_cashed_reaction(self):
        from agent.live_persona import get_joseph_live_reaction
        result = get_joseph_live_reaction({"cashed": True})
        self.assertIsInstance(result, str)
        self.assertGreater(len(result), 0)

    def test_blowout_reaction(self):
        from agent.live_persona import get_joseph_live_reaction
        result = get_joseph_live_reaction({
            "cashed": False, "blowout_risk": True, "foul_trouble": False,
        })
        self.assertIsInstance(result, str)

    def test_foul_reaction(self):
        from agent.live_persona import get_joseph_live_reaction
        result = get_joseph_live_reaction({
            "cashed": False, "blowout_risk": False, "foul_trouble": True,
        })
        self.assertIsInstance(result, str)

    def test_on_pace_reaction(self):
        from agent.live_persona import get_joseph_live_reaction
        result = get_joseph_live_reaction({
            "cashed": False, "blowout_risk": False,
            "foul_trouble": False, "on_pace": True,
        })
        self.assertIsInstance(result, str)

    def test_behind_pace_reaction(self):
        from agent.live_persona import get_joseph_live_reaction
        result = get_joseph_live_reaction({
            "cashed": False, "blowout_risk": False,
            "foul_trouble": False, "on_pace": False,
        })
        self.assertIsInstance(result, str)

    def test_invalid_input(self):
        from agent.live_persona import get_joseph_live_reaction
        result = get_joseph_live_reaction(None)
        self.assertIsInstance(result, str)

    def test_empty_dict(self):
        from agent.live_persona import get_joseph_live_reaction
        result = get_joseph_live_reaction({})
        self.assertIsInstance(result, str)

    def test_priority_cashed_over_blowout(self):
        """Cashed takes priority even if blowout_risk is set."""
        from agent.live_persona import (
            get_joseph_live_reaction, _CASHED_BRAGS,
        )
        result = get_joseph_live_reaction({
            "cashed": True, "blowout_risk": True,
        })
        self.assertIn(result, _CASHED_BRAGS)

    # ── New: UNDER direction reaction tests ──────────────────

    def test_under_on_pace_reaction(self):
        """UNDER bet on pace should get a positive UNDER-specific reaction."""
        from agent.live_persona import (
            get_joseph_live_reaction, _UNDER_ON_PACE_VIBES,
        )
        result = get_joseph_live_reaction({
            "cashed": False, "blowout_risk": False,
            "foul_trouble": False, "on_pace": True,
            "direction": "UNDER",
        })
        self.assertIn(result, _UNDER_ON_PACE_VIBES)

    def test_under_losing_reaction(self):
        """UNDER bet off pace should get a worried UNDER-specific reaction."""
        from agent.live_persona import (
            get_joseph_live_reaction, _UNDER_LOSING_WORRY,
        )
        result = get_joseph_live_reaction({
            "cashed": False, "blowout_risk": False,
            "foul_trouble": False, "on_pace": False,
            "direction": "UNDER",
        })
        self.assertIn(result, _UNDER_LOSING_WORRY)

    def test_under_blowout_is_positive(self):
        """For UNDER, blowout is actually good (starters pulled)."""
        from agent.live_persona import (
            get_joseph_live_reaction, _UNDER_ON_PACE_VIBES,
        )
        result = get_joseph_live_reaction({
            "cashed": False, "blowout_risk": True,
            "foul_trouble": False, "on_pace": True,
            "direction": "UNDER",
        })
        self.assertIn(result, _UNDER_ON_PACE_VIBES)

    def test_overtime_prefix(self):
        """OT flag should add an overtime prefix."""
        from agent.live_persona import get_joseph_live_reaction
        result = get_joseph_live_reaction({
            "cashed": False, "blowout_risk": False,
            "foul_trouble": False, "on_pace": True,
            "is_overtime": True,
        })
        # Both OT messages contain either "OVERTIME" or "OT!"
        has_ot_indicator = "OVERTIME" in result or "OT!" in result
        self.assertTrue(has_ot_indicator, f"Expected OT indicator in: {result}")


class TestStreamJosephText(unittest.TestCase):

    def test_yields_characters(self):
        from agent.live_persona import stream_joseph_text
        text = "Hello"
        chars = list(stream_joseph_text(text, delay=0))
        self.assertEqual(chars, ["H", "e", "l", "l", "o"])

    def test_empty_string(self):
        from agent.live_persona import stream_joseph_text
        chars = list(stream_joseph_text("", delay=0))
        self.assertEqual(chars, [])

    def test_default_delay_positive(self):
        """Default delay should be > 0 for typing effect."""
        from agent.live_persona import _TYPING_DELAY
        self.assertGreater(_TYPING_DELAY, 0)


# ============================================================
# SECTION 5: Live Sweat page file structure tests
# ============================================================

class TestLiveSweatPageFile(unittest.TestCase):
    """Verify the Live Sweat page file exists and has expected structure."""

    @classmethod
    def setUpClass(cls):
        page_path = os.path.join(
            os.path.dirname(__file__), "..",
            "pages", "5_💦_Live_Sweat.py",
        )
        with open(page_path, "r", encoding="utf-8") as f:
            cls.source = f.read()

    def test_valid_python_syntax(self):
        compile(self.source, "5_💦_Live_Sweat.py", "exec")

    def test_page_config_present(self):
        self.assertIn("set_page_config", self.source)

    def test_imports_live_game_tracker(self):
        self.assertIn("from data.live_game_tracker import", self.source)

    def test_imports_live_math(self):
        self.assertIn("from engine.live_math import", self.source)

    def test_imports_live_theme(self):
        self.assertIn("from styles.live_theme import", self.source)

    def test_imports_live_persona(self):
        self.assertIn("from agent.live_persona import", self.source)

    def test_autorefresh_imported(self):
        self.assertIn("streamlit_autorefresh", self.source)

    def test_autorefresh_interval(self):
        self.assertIn("120_000", self.source)

    def test_global_css(self):
        self.assertIn("get_global_css", self.source)

    def test_live_css(self):
        self.assertIn("get_live_sweat_css", self.source)

    def test_balloons_trigger(self):
        self.assertIn("st.balloons()", self.source)

    def test_vibe_check_section(self):
        self.assertIn("Vibe Check", self.source)

    def test_write_stream(self):
        self.assertIn("st.write_stream", self.source)

    def test_cashed_label(self):
        self.assertIn("Cashed", self.source)

    # ── New: Enhancement presence tests ──────────────────────

    def test_direction_passed_to_pace(self):
        self.assertIn("direction=direction", self.source)

    def test_render_waiting_card_import(self):
        self.assertIn("render_waiting_card", self.source)

    def test_manual_refresh_button(self):
        self.assertIn("Refresh Now", self.source)

    def test_last_refresh_timestamp(self):
        self.assertIn("Last refreshed", self.source)

    def test_awaiting_metric(self):
        self.assertIn("Awaiting", self.source)

    def test_blocks_steals_combo(self):
        self.assertIn("blocks_steals", self.source)


# ============================================================
# SECTION 6: data/live_game_tracker retriever tests
# ============================================================

class TestFetchLiveBoxscoresImpl(unittest.TestCase):
    """Test _get_live_boxscores_impl."""

    def test_returns_list(self):
        from data.live_game_tracker import _get_live_boxscores_impl
        # Should return list even when API is unreachable
        result = _get_live_boxscores_impl()
        self.assertIsInstance(result, list)


# ============================================================
# SECTION 7: _parse_iso_minutes tests
# ============================================================

class TestParseIsoMinutes(unittest.TestCase):
    """Tests for _parse_iso_minutes helper."""

    def setUp(self):
        from data.live_game_tracker import _parse_iso_minutes
        self.parse = _parse_iso_minutes

    def test_iso_full(self):
        """PT25M30.00S → 25.5 minutes."""
        self.assertAlmostEqual(self.parse("PT25M30.00S"), 25.5, places=1)

    def test_iso_minutes_only(self):
        """PT12M → 12.0."""
        self.assertAlmostEqual(self.parse("PT12M"), 12.0, places=1)

    def test_iso_seconds_only(self):
        """PT90S → 1.5."""
        self.assertAlmostEqual(self.parse("PT90S"), 1.5, places=1)

    def test_iso_zero(self):
        """PT00M00.00S → 0.0."""
        self.assertAlmostEqual(self.parse("PT00M00.00S"), 0.0, places=1)

    def test_plain_float_string(self):
        self.assertAlmostEqual(self.parse("25.5"), 25.5, places=1)

    def test_plain_int_string(self):
        self.assertAlmostEqual(self.parse("30"), 30.0, places=1)

    def test_numeric_int(self):
        self.assertAlmostEqual(self.parse(25), 25.0, places=1)

    def test_numeric_float(self):
        self.assertAlmostEqual(self.parse(12.3), 12.3, places=1)

    def test_none(self):
        self.assertEqual(self.parse(None), 0.0)

    def test_empty_string(self):
        self.assertEqual(self.parse(""), 0.0)

    def test_garbage(self):
        self.assertEqual(self.parse("not a number"), 0.0)

    def test_negative_clamped(self):
        self.assertEqual(self.parse(-5), 0.0)

    def test_case_insensitive(self):
        """Lowercase 'pt25m30.00s' should still parse."""
        self.assertAlmostEqual(self.parse("pt25m30.00s"), 25.5, places=1)


# ============================================================
# SECTION 8: _build_player_list tests
# ============================================================

class TestBuildPlayerList(unittest.TestCase):
    """Tests for _build_player_list helper."""

    def setUp(self):
        from data.live_game_tracker import _build_player_list
        self.build = _build_player_list

    def test_empty_team(self):
        self.assertEqual(self.build({}), [])

    def test_no_players_key(self):
        self.assertEqual(self.build({"teamTricode": "LAL"}), [])

    def test_basic_player(self):
        team = {
            "players": [
                {
                    "firstName": "LeBron",
                    "familyName": "James",
                    "statistics": {
                        "points": 28,
                        "reboundsTotal": 7,
                        "assists": 9,
                        "steals": 2,
                        "blocks": 1,
                        "turnovers": 3,
                        "threePointersMade": 4,
                        "minutes": "PT32M15.00S",
                        "foulsPersonal": 2,
                    },
                }
            ],
        }
        result = self.build(team)
        self.assertEqual(len(result), 1)
        p = result[0]
        self.assertEqual(p["name"], "LeBron James")
        self.assertEqual(p["pts"], 28)
        self.assertEqual(p["reb"], 7)
        self.assertEqual(p["ast"], 9)
        self.assertEqual(p["stl"], 2)
        self.assertEqual(p["blk"], 1)
        self.assertEqual(p["tov"], 3)
        self.assertEqual(p["fg3m"], 4)
        self.assertAlmostEqual(p["minutes"], 32.2, places=1)
        self.assertEqual(p["fouls"], 2)

    def test_name_fallback_to_name_field(self):
        team = {
            "players": [
                {
                    "name": "Stephen Curry",
                    "statistics": {"points": 30},
                }
            ],
        }
        result = self.build(team)
        self.assertEqual(result[0]["name"], "Stephen Curry")

    def test_skips_unnamed_players(self):
        team = {
            "players": [
                {"statistics": {"points": 10}},
            ],
        }
        self.assertEqual(self.build(team), [])

    def test_missing_stats(self):
        """Player with empty statistics should get zero defaults."""
        team = {
            "players": [
                {
                    "firstName": "Test",
                    "familyName": "Player",
                    "statistics": {},
                }
            ],
        }
        result = self.build(team)
        self.assertEqual(len(result), 1)
        p = result[0]
        self.assertEqual(p["pts"], 0)
        self.assertEqual(p["reb"], 0)
        self.assertEqual(p["minutes"], 0.0)

    def test_multiple_players(self):
        team = {
            "players": [
                {"firstName": "A", "familyName": "B", "statistics": {"points": 10}},
                {"firstName": "C", "familyName": "D", "statistics": {"points": 20}},
            ],
        }
        result = self.build(team)
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]["name"], "A B")
        self.assertEqual(result[1]["name"], "C D")


if __name__ == "__main__":
    unittest.main()
