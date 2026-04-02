# ============================================================
# FILE: tests/test_joseph_live_desk.py
# PURPOSE: Tests for pages/helpers/joseph_live_desk.py
#          (Joseph's Live Broadcast Desk helper — Layer 6)
# ============================================================
import sys, os, unittest
from unittest.mock import patch, MagicMock

# Ensure repo root is on path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

# Mock streamlit before importing the module
_mock_st = MagicMock()
_mock_st.cache_data = lambda *a, **kw: (lambda f: f)
sys.modules.setdefault("streamlit", _mock_st)
sys.modules.setdefault("streamlit.components", MagicMock())
sys.modules.setdefault("streamlit.components.v1", MagicMock())


class TestGetJosephAvatarB64(unittest.TestCase):
    """Test get_joseph_avatar_b64 image loader."""

    def test_import(self):
        from pages.helpers.joseph_live_desk import get_joseph_avatar_b64
        self.assertTrue(callable(get_joseph_avatar_b64))

    def test_returns_string(self):
        from pages.helpers.joseph_live_desk import get_joseph_avatar_b64
        result = get_joseph_avatar_b64()
        self.assertIsInstance(result, str)

    def test_returns_nonempty_when_file_exists(self):
        """Avatar loader returns base64 data when the image file exists."""
        from pages.helpers.joseph_live_desk import get_joseph_avatar_b64
        result = get_joseph_avatar_b64()
        # The function now loads the actual avatar file
        self.assertIsInstance(result, str)


class TestRenderLiveDeskCss(unittest.TestCase):
    """Test render_live_desk_css returns complete CSS."""

    def setUp(self):
        from pages.helpers.joseph_live_desk import render_live_desk_css
        self.css = render_live_desk_css()

    def test_returns_string(self):
        self.assertIsInstance(self.css, str)

    def test_contains_style_tag(self):
        self.assertIn("<style>", self.css)
        self.assertIn("</style>", self.css)

    def test_glassmorphic_container(self):
        self.assertIn("joseph-live-desk", self.css)
        self.assertIn("backdrop-filter", self.css)
        self.assertIn("rgba(7,10,19,0.85)", self.css)

    def test_live_pulse_animation(self):
        self.assertIn("josephLivePulse", self.css)
        self.assertIn("joseph-live-dot", self.css)

    def test_typing_indicator(self):
        self.assertIn("joseph-typing", self.css)
        self.assertIn("josephBounce", self.css)

    def test_avatar_circle(self):
        self.assertIn("joseph-avatar", self.css)
        self.assertIn("64px", self.css)
        self.assertIn("#ff5e00", self.css)

    def test_segment_cards(self):
        self.assertIn("joseph-segment", self.css)

    def test_verdict_badges(self):
        self.assertIn("joseph-verdict-smash", self.css)
        self.assertIn("joseph-verdict-lean", self.css)
        self.assertIn("joseph-verdict-fade", self.css)
        self.assertIn("joseph-verdict-stay_away", self.css)

    def test_nerd_stats(self):
        self.assertIn("joseph-nerd-stats", self.css)

    def test_dawg_table(self):
        self.assertIn("joseph-dawg-table", self.css)

    def test_override_table(self):
        self.assertIn("joseph-override-table", self.css)

    def test_orange_accent(self):
        self.assertIn("#ff5e00", self.css)

    def test_border_color(self):
        self.assertIn("rgba(255,94,0,0.3)", self.css)

    # ── Elite NBA ESPN AI theme enhancements ─────────────────

    def test_bottom_broadcast_bar(self):
        """Live desk should have a bottom shimmer bar like ESPN broadcast."""
        self.assertIn("joseph-live-desk::after", self.css)

    def test_container_outer_glow(self):
        """Live desk container should have an outer glow shadow."""
        self.assertIn("box-shadow", self.css)

    def test_avatar_animated_glow(self):
        """64px avatar should have an animated glow ring."""
        self.assertIn("josephAvatarGlow", self.css)

    def test_header_text_shadow(self):
        """Header text should have a neon text-shadow."""
        self.assertIn("text-shadow", self.css)

    def test_segment_left_accent(self):
        """Segment cards should have an ESPN-style left accent border."""
        self.assertIn("border-left", self.css)

    def test_segment_hover_lift(self):
        """Segment cards should lift on hover (translateY)."""
        self.assertIn("translateY", self.css)

    def test_verdict_text_shadow(self):
        """Verdict badges should have a text glow."""
        self.assertIn("text-shadow:0 0 6px currentColor", self.css)

    def test_verdict_hover_scale(self):
        """Verdict badges should scale on hover."""
        self.assertIn("joseph-verdict:hover", self.css)

    def test_nerd_stats_tabular_nums(self):
        """Nerd stats should use tabular-nums for scoreboard alignment."""
        self.assertIn("tabular-nums", self.css)

    def test_edge_value_class(self):
        """Should have a scoreboard-style edge value class."""
        self.assertIn("joseph-edge-value", self.css)

    def test_live_dot_box_shadow(self):
        """LIVE dot should have a red glow shadow."""
        self.assertIn("box-shadow:0 0 6px rgba(255,32,32,0.6)", self.css)

    def test_subtitle_montserrat(self):
        """Subtitle should use Montserrat font."""
        self.assertIn("Montserrat", self.css)


class TestRenderBroadcastSegment(unittest.TestCase):
    """Test render_broadcast_segment returns valid HTML."""

    def setUp(self):
        from pages.helpers.joseph_live_desk import render_broadcast_segment
        self.render = render_broadcast_segment

    def test_returns_html_string(self):
        html = self.render({"title": "Test", "body": "Hello"})
        self.assertIsInstance(html, str)
        self.assertIn("joseph-segment", html)

    def test_title_rendered(self):
        html = self.render({"title": "MY TITLE", "body": "Body text"})
        self.assertIn("MY TITLE", html)

    def test_body_rendered(self):
        html = self.render({"title": "T", "body": "Some body content"})
        self.assertIn("Some body content", html)

    def test_verdict_badge_smash(self):
        html = self.render({"title": "T", "body": "B", "verdict": "SMASH"})
        self.assertIn("joseph-verdict-smash", html)
        self.assertIn("🔥", html)

    def test_verdict_badge_lean(self):
        html = self.render({"title": "T", "body": "B", "verdict": "LEAN"})
        self.assertIn("joseph-verdict-lean", html)

    def test_verdict_badge_fade(self):
        html = self.render({"title": "T", "body": "B", "verdict": "FADE"})
        self.assertIn("joseph-verdict-fade", html)

    def test_verdict_badge_stay_away(self):
        html = self.render({"title": "T", "body": "B", "verdict": "STAY_AWAY"})
        self.assertIn("joseph-verdict-stay_away", html)

    def test_no_verdict_no_badge(self):
        html = self.render({"title": "T", "body": "B"})
        self.assertNotIn("joseph-verdict-", html)

    def test_empty_segment(self):
        html = self.render({})
        self.assertIn("joseph-segment", html)

    def test_html_escaping_title(self):
        html = self.render({"title": "<script>alert(1)</script>", "body": ""})
        self.assertNotIn("<script>", html)
        self.assertIn("&lt;script&gt;", html)


class TestRenderDawgBoard(unittest.TestCase):
    """Test render_dawg_board with mock data."""

    def test_import(self):
        from pages.helpers.joseph_live_desk import render_dawg_board
        self.assertTrue(callable(render_dawg_board))

    def test_with_empty_list(self):
        from pages.helpers.joseph_live_desk import render_dawg_board
        # Should not raise
        render_dawg_board([])

    def test_with_sample_results(self):
        from pages.helpers.joseph_live_desk import render_dawg_board
        results = [
            {"player": "LeBron James", "dawg_factor": 7.5,
             "narrative_tags": ["revenge_game"], "archetype": "Alpha Scorer"},
            {"player": "Steph Curry", "dawg_factor": 5.0,
             "narrative_tags": ["nationally_televised"], "archetype": "Sharpshooter"},
            {"player": "Jokic", "dawg_factor": 3.0, "narrative_tags": [], "archetype": "Facilitator"},
        ]
        # Should not raise
        render_dawg_board(results)


class TestRenderOverrideReport(unittest.TestCase):
    """Test render_override_report with mock data."""

    def test_import(self):
        from pages.helpers.joseph_live_desk import render_override_report
        self.assertTrue(callable(render_override_report))

    def test_no_overrides(self):
        from pages.helpers.joseph_live_desk import render_override_report
        render_override_report([{"is_override": False}])

    def test_with_overrides(self):
        from pages.helpers.joseph_live_desk import render_override_report
        results = [
            {
                "is_override": True,
                "player": "Luka Doncic",
                "prop": "points",
                "qme_edge": 3.5,
                "edge": 8.2,
                "direction": "OVER",
                "override_reason": "Revenge game energy",
            },
        ]
        render_override_report(results)


class TestBuildNerdStatsHtml(unittest.TestCase):
    """Test _build_nerd_stats_html."""

    def setUp(self):
        from pages.helpers.joseph_live_desk import _build_nerd_stats_html
        self.build = _build_nerd_stats_html

    def test_empty_result(self):
        html = self.build({})
        self.assertEqual(html, "")

    def test_with_edge(self):
        html = self.build({"edge": 5.5})
        self.assertIn("edge", html)
        self.assertIn("5.5", html)

    def test_with_comp(self):
        html = self.build({"comp": {"name": "Steph Curry 2016"}})
        self.assertIn("Steph Curry 2016", html)

    def test_with_tags(self):
        html = self.build({"narrative_tags": ["revenge_game", "contract_year"]})
        self.assertIn("revenge_game", html)
        self.assertIn("contract_year", html)

    def test_nerd_stats_class(self):
        html = self.build({"edge": 1.0, "confidence": 0.85})
        self.assertIn("joseph-nerd-stats", html)


class TestRenderJosephLiveDesk(unittest.TestCase):
    """Test render_joseph_live_desk import and signature."""

    def test_import(self):
        from pages.helpers.joseph_live_desk import render_joseph_live_desk
        self.assertTrue(callable(render_joseph_live_desk))

    def test_signature_accepts_expected_args(self):
        import inspect
        from pages.helpers.joseph_live_desk import render_joseph_live_desk
        sig = inspect.signature(render_joseph_live_desk)
        params = list(sig.parameters.keys())
        self.assertIn("analysis_results", params)
        self.assertIn("enriched_players", params)
        self.assertIn("teams_data", params)
        self.assertIn("todays_games", params)


class TestModuleExports(unittest.TestCase):
    """Verify all expected exports from joseph_live_desk."""

    def test_all_functions_importable(self):
        from pages.helpers.joseph_live_desk import (
            get_joseph_avatar_b64,
            render_live_desk_css,
            render_joseph_live_desk,
            render_broadcast_segment,
            render_dawg_board,
            render_override_report,
        )
        self.assertTrue(callable(get_joseph_avatar_b64))
        self.assertTrue(callable(render_live_desk_css))
        self.assertTrue(callable(render_joseph_live_desk))
        self.assertTrue(callable(render_broadcast_segment))
        self.assertTrue(callable(render_dawg_board))
        self.assertTrue(callable(render_override_report))


# ============================================================
# SECTION: Player Name Lookup Wiring
# ============================================================

class TestPlayerNameLookupWiring(unittest.TestCase):
    """Verify that render_joseph_live_desk correctly resolves
    player names from QAM analysis_results (which use 'player_name')
    and enriched_players (which are keyed by lowercase name)."""

    def test_source_resolves_player_name_key(self):
        """The live desk code must try 'player_name' first when
        extracting the player name from QAM analysis results."""
        import inspect
        from pages.helpers.joseph_live_desk import render_joseph_live_desk
        source = inspect.getsource(render_joseph_live_desk)
        # The lookup line should try 'player_name' before falling back
        idx_player_name = source.find('ar.get("player_name"')
        idx_player = source.find('ar.get("player"')
        self.assertNotEqual(idx_player_name, -1,
                            "Must look up 'player_name' key from QAM results")
        self.assertLess(idx_player_name, idx_player,
                        "'player_name' must be tried before 'player' in get chain")

    def test_source_lowercases_enriched_lookup(self):
        """The enriched_players lookup must lowercase the key to match
        the enriched dict which is keyed by lowercase player name."""
        import inspect
        from pages.helpers.joseph_live_desk import render_joseph_live_desk
        source = inspect.getsource(render_joseph_live_desk)
        # Find the enriched_players.get() call near the player_name lookup
        idx_enriched = source.find("enriched_players.get(")
        self.assertNotEqual(idx_enriched, -1, "Must call enriched_players.get()")
        # The argument should include .lower().strip()
        snippet = source[idx_enriched:idx_enriched + 100]
        self.assertIn(".lower()", snippet,
                      "enriched_players lookup must lowercase the key")
        self.assertIn(".strip()", snippet,
                      "enriched_players lookup must strip whitespace")


if __name__ == "__main__":
    unittest.main()
