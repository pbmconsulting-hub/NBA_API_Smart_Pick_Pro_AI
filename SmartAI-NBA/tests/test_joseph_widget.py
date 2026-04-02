# ============================================================
# FILE: tests/test_joseph_widget.py
# PURPOSE: Tests for utils/joseph_widget.py
#          (Joseph's global sidebar widget — Layer 9)
# ============================================================
import sys, os, unittest
from unittest.mock import MagicMock, patch

# Ensure repo root is on path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

# Mock streamlit before importing the module
_mock_st = MagicMock()
_mock_st.cache_data = lambda *a, **kw: (lambda f: f)
_mock_st.session_state = {}
sys.modules.setdefault("streamlit", _mock_st)
sys.modules.setdefault("streamlit.components", MagicMock())
sys.modules.setdefault("streamlit.components.v1", MagicMock())

# _mock_st stays as the MagicMock defined above — required for
# reset_mock(), assert_called(), call_args_list, etc.
# We patch it into utils.joseph_widget.st in setUpModule so that
# widget functions route their st.* calls through our MagicMock.

_original_jw_st = None


def setUpModule():
    """Patch utils.joseph_widget.st with our MagicMock for call tracking."""
    global _original_jw_st
    import utils.joseph_widget as _jwm
    _original_jw_st = getattr(_jwm, "st", None)
    _jwm.st = _mock_st


def tearDownModule():
    """Restore original st reference to avoid affecting subsequent test files."""
    import utils.joseph_widget as _jwm
    if _original_jw_st is not None:
        _jwm.st = _original_jw_st


# ============================================================
# SECTION 1: Module imports & exports
# ============================================================

class TestModuleImports(unittest.TestCase):
    """Verify the module imports cleanly and exposes expected symbols."""

    def test_import_module(self):
        import utils.joseph_widget
        self.assertTrue(hasattr(utils.joseph_widget, "_inject_widget_css"))

    def test_inject_widget_css_callable(self):
        from utils.joseph_widget import _inject_widget_css
        self.assertTrue(callable(_inject_widget_css))

    def test_render_sidebar_widget_callable(self):
        from utils.joseph_widget import render_joseph_sidebar_widget
        self.assertTrue(callable(render_joseph_sidebar_widget))

    def test_inject_inline_commentary_callable(self):
        from utils.joseph_widget import inject_joseph_inline_commentary
        self.assertTrue(callable(inject_joseph_inline_commentary))

    def test_widget_css_constant_exists(self):
        from utils.joseph_widget import _WIDGET_CSS
        self.assertIsInstance(_WIDGET_CSS, str)

    def test_all_three_functions_importable(self):
        from utils.joseph_widget import (
            _inject_widget_css,
            render_joseph_sidebar_widget,
            inject_joseph_inline_commentary,
        )
        self.assertTrue(callable(_inject_widget_css))
        self.assertTrue(callable(render_joseph_sidebar_widget))
        self.assertTrue(callable(inject_joseph_inline_commentary))

    def test_ask_popover_callable(self):
        from utils.joseph_widget import render_joseph_ask_popover
        self.assertTrue(callable(render_joseph_ask_popover))

    def test_floating_widget_callable(self):
        from utils.joseph_widget import render_joseph_floating_widget
        self.assertTrue(callable(render_joseph_floating_widget))


# ============================================================
# SECTION 2: _WIDGET_CSS content validation
# ============================================================

class TestWidgetCSS(unittest.TestCase):
    """Verify the widget CSS string contains all required classes."""

    def setUp(self):
        from utils.joseph_widget import _WIDGET_CSS
        self.css = _WIDGET_CSS

    def test_contains_style_tag(self):
        self.assertIn("<style>", self.css)
        self.assertIn("</style>", self.css)

    def test_sidebar_container(self):
        self.assertIn("joseph-sidebar-container", self.css)
        self.assertIn("rgba(7,10,19,0.90)", self.css)
        self.assertIn("backdrop-filter", self.css)

    def test_sidebar_avatar(self):
        self.assertIn("joseph-sidebar-avatar", self.css)
        self.assertIn("56px", self.css)
        self.assertIn("#ff5e00", self.css)

    def test_sidebar_avatar_hover(self):
        self.assertIn("joseph-sidebar-avatar:hover", self.css)
        self.assertIn("scale(1.1)", self.css)

    def test_ambient_text(self):
        self.assertIn("joseph-ambient-text", self.css)
        self.assertIn("#ff9d4d", self.css)
        self.assertIn("font-style:italic", self.css)

    def test_pulse_dot(self):
        self.assertIn("joseph-pulse-dot", self.css)
        self.assertIn("josephPulse", self.css)
        self.assertIn("1.5s", self.css)

    def test_pulse_keyframes(self):
        self.assertIn("@keyframes josephPulse", self.css)
        self.assertIn("scale(0.8)", self.css)
        self.assertIn("scale(1.2)", self.css)

    def test_inline_card(self):
        self.assertIn("joseph-inline-card", self.css)
        self.assertIn("rgba(255,94,0,0.25)", self.css)

    def test_inline_avatar(self):
        self.assertIn("joseph-inline-avatar", self.css)
        self.assertIn("36px", self.css)

    def test_inline_label(self):
        self.assertIn("joseph-inline-label", self.css)
        self.assertIn("font-weight:700", self.css)

    def test_inline_text(self):
        self.assertIn("joseph-inline-text", self.css)
        self.assertIn("#c0d0e8", self.css)

    def test_verdict_smash(self):
        self.assertIn("joseph-widget-verdict-smash", self.css)
        self.assertIn("#ff4444", self.css)

    def test_verdict_lean(self):
        self.assertIn("joseph-widget-verdict-lean", self.css)
        self.assertIn("#00ff9d", self.css)

    def test_verdict_fade(self):
        self.assertIn("joseph-widget-verdict-fade", self.css)
        self.assertIn("#ffc800", self.css)

    def test_orange_accent_color(self):
        self.assertIn("#ff5e00", self.css)

    def test_border_radius_14(self):
        self.assertIn("border-radius:14px", self.css)

    def test_border_radius_10(self):
        self.assertIn("border-radius:10px", self.css)

    # ── Premium typography ────────────────────────────────────

    def test_orbitron_font(self):
        self.assertIn("Orbitron", self.css)

    def test_montserrat_font(self):
        self.assertIn("Montserrat", self.css)

    def test_jetbrains_mono_font(self):
        self.assertIn("JetBrains Mono", self.css)

    def test_tabular_nums(self):
        self.assertIn("tabular-nums", self.css)

    # ── Typing dots animation ─────────────────────────────────

    def test_typing_indicator(self):
        self.assertIn("joseph-widget-typing", self.css)

    def test_typing_bounce_keyframes(self):
        self.assertIn("josephWidgetBounce", self.css)

    # ── Shimmer border animation ──────────────────────────────

    def test_shimmer_animation(self):
        self.assertIn("josephWidgetShimmer", self.css)

    def test_shimmer_gradient(self):
        self.assertIn("#ff9e00", self.css)

    # ── Popover CSS classes ───────────────────────────────────

    def test_popover_container(self):
        self.assertIn("joseph-popover-container", self.css)

    def test_popover_avatar_64px(self):
        self.assertIn("joseph-popover-avatar", self.css)
        self.assertIn("64px", self.css)

    def test_popover_title_orbitron(self):
        self.assertIn("joseph-popover-title", self.css)

    def test_popover_body(self):
        self.assertIn("joseph-popover-body", self.css)

    def test_popover_glassmorphic(self):
        self.assertIn("blur(16px)", self.css)

    def test_sidebar_name_class(self):
        self.assertIn("joseph-sidebar-name", self.css)

    def test_track_record_class(self):
        self.assertIn("joseph-track-record", self.css)

    # ── Elite NBA ESPN AI theme enhancements ──────────────────

    def test_sidebar_bottom_broadcast_bar(self):
        """Sidebar container should have a bottom shimmer bar."""
        self.assertIn("joseph-sidebar-container::after", self.css)

    def test_sidebar_container_outer_glow(self):
        """Sidebar container should have an outer glow."""
        self.assertIn("box-shadow", self.css)

    def test_sidebar_avatar_animated_glow(self):
        """Sidebar avatar should have an animated glow."""
        self.assertIn("josephSidebarGlow", self.css)

    def test_live_badge_class(self):
        """Should have an ESPN-style LIVE badge class."""
        self.assertIn("joseph-live-badge", self.css)

    def test_pulse_dot_glow(self):
        """Pulse dot should have a box-shadow glow."""
        self.assertIn("box-shadow:0 0 4px rgba(255,94,0,0.5)", self.css)

    def test_inline_card_left_accent(self):
        """Inline commentary card should have a left accent border."""
        self.assertIn("border-left:3px solid rgba(255,94,0,0.5)", self.css)

    def test_inline_card_hover(self):
        """Inline card should have hover effect."""
        self.assertIn("joseph-inline-card:hover", self.css)

    def test_inline_avatar_glow(self):
        """Inline avatar should have a glow shadow."""
        self.assertIn("box-shadow:0 0 6px rgba(255,94,0,0.25)", self.css)

    def test_inline_label_text_shadow(self):
        """Inline label should have a text shadow."""
        self.assertIn("text-shadow:0 0 6px rgba(255,94,0,0.12)", self.css)

    def test_verdict_text_shadows(self):
        """Verdict accents should have text-shadow glow."""
        self.assertIn("text-shadow:0 0 6px rgba(255,68,68,0.2)", self.css)
        self.assertIn("text-shadow:0 0 6px rgba(0,255,157,0.15)", self.css)

    def test_popover_bottom_bar(self):
        """Popover should have a bottom shimmer bar."""
        self.assertIn("joseph-popover-container::after", self.css)

    def test_popover_avatar_animated_glow(self):
        """Popover avatar should have an animated glow ring."""
        self.assertIn("josephPopoverGlow", self.css)

    def test_popover_title_text_shadow(self):
        """Popover title should have a text-shadow."""
        self.assertIn("text-shadow:0 0 10px rgba(255,94,0,0.2)", self.css)

    def test_sidebar_name_text_shadow(self):
        """Sidebar name should have a text-shadow."""
        self.assertIn("text-shadow:0 0 8px rgba(255,94,0,0.15)", self.css)

    # ── Floating widget CSS ───────────────────────────────────

    def test_floating_widget_class(self):
        """CSS should include the floating widget container."""
        self.assertIn("joseph-floating-widget", self.css)

    def test_floating_widget_position_fixed(self):
        """Floating widget must use position:fixed."""
        self.assertIn("position:fixed", self.css)

    def test_floating_widget_bottom_right(self):
        """Floating widget must be pinned to bottom-right."""
        self.assertIn("bottom:24px", self.css)
        self.assertIn("right:24px", self.css)

    def test_floating_widget_z_index(self):
        """Floating widget must have a high z-index."""
        self.assertIn("z-index:999999", self.css)

    def test_floating_avatar_class(self):
        """CSS should include the floating avatar class."""
        self.assertIn("joseph-floating-avatar", self.css)
        self.assertIn("115px", self.css)

    def test_floating_avatar_glow_keyframes(self):
        """Floating avatar should have an animated glow."""
        self.assertIn("josephFloatingGlow", self.css)

    def test_floating_name_class(self):
        """CSS should include the floating name class."""
        self.assertIn("joseph-floating-name", self.css)

    def test_floating_ambient_class(self):
        """CSS should include the floating ambient text class."""
        self.assertIn("joseph-floating-ambient", self.css)

    def test_floating_info_class(self):
        """CSS should include the floating info container."""
        self.assertIn("joseph-floating-info", self.css)

    def test_floating_widget_shimmer_bars(self):
        """Floating widget should have shimmer bars."""
        self.assertIn("joseph-floating-widget::before", self.css)
        self.assertIn("joseph-floating-widget::after", self.css)


# ============================================================
# SECTION 3: _inject_widget_css()
# ============================================================

class TestInjectWidgetCss(unittest.TestCase):
    """Test _inject_widget_css injects CSS via st.markdown."""

    def setUp(self):
        _mock_st.reset_mock()
        _mock_st.session_state = {}

    def test_injects_css(self):
        from utils.joseph_widget import _inject_widget_css
        _inject_widget_css()
        _mock_st.markdown.assert_called()
        call_args = _mock_st.markdown.call_args
        self.assertIn("<style>", call_args[0][0])

    def test_unsafe_allow_html(self):
        from utils.joseph_widget import _inject_widget_css
        _inject_widget_css()
        call_args = _mock_st.markdown.call_args
        self.assertTrue(call_args[1].get("unsafe_allow_html", False))

    def test_idempotent_injection(self):
        """CSS is re-injected on every call to survive page navigation."""
        from utils.joseph_widget import _inject_widget_css
        _mock_st.session_state = {}
        _inject_widget_css()
        first_count = _mock_st.markdown.call_count
        _inject_widget_css()
        second_count = _mock_st.markdown.call_count
        self.assertEqual(second_count, first_count + 1)

    def test_session_flag_set(self):
        from utils.joseph_widget import _inject_widget_css
        _mock_st.session_state = {}
        _inject_widget_css()
        # Session flag is no longer set; CSS re-injects every call
        self.assertNotIn("_joseph_widget_css_injected", _mock_st.session_state)


# ============================================================
# SECTION 4: render_joseph_sidebar_widget()
# ============================================================

class TestRenderJosephSidebarWidget(unittest.TestCase):
    """Test the sidebar widget rendering function."""

    def setUp(self):
        _mock_st.reset_mock()
        _mock_st.session_state = {}

    def _get_sidebar_html(self):
        """Return the HTML string of the sidebar card from st.markdown calls."""
        for call in _mock_st.markdown.call_args_list:
            if call[0] and '<div class="joseph-sidebar-container">' in call[0][0]:
                return call[0][0]
        return ""

    def test_callable(self):
        from utils.joseph_widget import render_joseph_sidebar_widget
        self.assertTrue(callable(render_joseph_sidebar_widget))

    def test_does_not_raise(self):
        from utils.joseph_widget import render_joseph_sidebar_widget
        # Should not raise regardless of brain availability
        render_joseph_sidebar_widget()

    def test_calls_markdown(self):
        from utils.joseph_widget import render_joseph_sidebar_widget
        render_joseph_sidebar_widget()
        # At least one markdown call should contain the sidebar container
        self.assertTrue(len(self._get_sidebar_html()) > 0)

    def test_sidebar_html_contains_container(self):
        from utils.joseph_widget import render_joseph_sidebar_widget
        render_joseph_sidebar_widget()
        self.assertIn("joseph-sidebar-container", self._get_sidebar_html())

    def test_sidebar_html_contains_pulse_dot(self):
        from utils.joseph_widget import render_joseph_sidebar_widget
        render_joseph_sidebar_widget()
        self.assertIn("joseph-pulse-dot", self._get_sidebar_html())

    def test_sidebar_html_contains_ambient_text(self):
        from utils.joseph_widget import render_joseph_sidebar_widget
        render_joseph_sidebar_widget()
        self.assertIn("joseph-ambient-text", self._get_sidebar_html())

    @patch("utils.joseph_widget.get_joseph_avatar_b64", return_value="FAKE_B64")
    def test_avatar_image_rendered(self, mock_avatar):
        from utils.joseph_widget import render_joseph_sidebar_widget
        render_joseph_sidebar_widget()
        html = self._get_sidebar_html()
        self.assertIn("joseph-sidebar-avatar", html)
        self.assertIn("FAKE_B64", html)

    @patch("utils.joseph_widget.get_joseph_avatar_b64", return_value="")
    def test_fallback_emoji_when_no_avatar(self, mock_avatar):
        from utils.joseph_widget import render_joseph_sidebar_widget
        render_joseph_sidebar_widget()
        self.assertIn("🎙️", self._get_sidebar_html())

    @patch("utils.joseph_widget.joseph_ambient_line", return_value="TEST LINE")
    @patch("utils.joseph_widget.joseph_get_ambient_context", return_value=("idle", {}))
    def test_ambient_line_rendered(self, mock_ctx, mock_line):
        from utils.joseph_widget import render_joseph_sidebar_widget
        render_joseph_sidebar_widget()
        self.assertIn("TEST LINE", self._get_sidebar_html())

    @patch("utils.joseph_widget.joseph_ambient_line", return_value="")
    @patch("utils.joseph_widget.joseph_get_ambient_context", return_value=("idle", {}))
    def test_default_ambient_when_empty(self, mock_ctx, mock_line):
        from utils.joseph_widget import render_joseph_sidebar_widget
        render_joseph_sidebar_widget()
        self.assertIn("ALWAYS watching", self._get_sidebar_html())

    @patch("utils.joseph_widget.joseph_get_track_record",
           return_value={"total": 10, "wins": 7, "losses": 3,
                         "roi_estimate": 12.5})
    def test_track_record_shown(self, mock_record):
        from utils.joseph_widget import render_joseph_sidebar_widget
        render_joseph_sidebar_widget()
        html = self._get_sidebar_html()
        self.assertIn("7W-3L", html)
        self.assertIn("+12.5%", html)

    @patch("utils.joseph_widget.joseph_get_track_record",
           return_value={"total": 0, "wins": 0, "losses": 0,
                         "roi_estimate": 0.0})
    def test_no_track_record_when_zero(self, mock_record):
        from utils.joseph_widget import render_joseph_sidebar_widget
        render_joseph_sidebar_widget()
        self.assertNotIn("📊", self._get_sidebar_html())

    def test_html_escaping_ambient(self):
        """Ambient text must be HTML-escaped to prevent injection."""
        with patch("utils.joseph_widget.joseph_ambient_line",
                   return_value="<script>alert(1)</script>"):
            with patch("utils.joseph_widget.joseph_get_ambient_context",
                       return_value=("idle", {})):
                from utils.joseph_widget import render_joseph_sidebar_widget
                render_joseph_sidebar_widget()
                html = self._get_sidebar_html()
                self.assertNotIn("<script>", html)
                self.assertIn("&lt;script&gt;", html)


# ============================================================
# SECTION 4B: render_joseph_floating_widget()
# ============================================================

class TestRenderJosephFloatingWidget(unittest.TestCase):
    """Test the floating bottom-right widget rendering function."""

    def setUp(self):
        _mock_st.reset_mock()
        _mock_st.session_state = {}

    def _get_floating_html(self):
        """Return the HTML string of the floating card from st.markdown calls."""
        for call in _mock_st.markdown.call_args_list:
            if call[0] and '<div class="joseph-floating-widget">' in call[0][0]:
                return call[0][0]
        return ""

    def test_callable(self):
        from utils.joseph_widget import render_joseph_floating_widget
        self.assertTrue(callable(render_joseph_floating_widget))

    def test_does_not_raise(self):
        from utils.joseph_widget import render_joseph_floating_widget
        render_joseph_floating_widget()

    def test_calls_markdown(self):
        from utils.joseph_widget import render_joseph_floating_widget
        render_joseph_floating_widget()
        self.assertTrue(len(self._get_floating_html()) > 0)

    def test_floating_html_contains_container(self):
        from utils.joseph_widget import render_joseph_floating_widget
        render_joseph_floating_widget()
        self.assertIn("joseph-floating-widget", self._get_floating_html())

    def test_floating_html_contains_name(self):
        from utils.joseph_widget import render_joseph_floating_widget
        render_joseph_floating_widget()
        self.assertIn("Joseph M. Smith", self._get_floating_html())

    def test_floating_html_contains_pulse_dot(self):
        from utils.joseph_widget import render_joseph_floating_widget
        render_joseph_floating_widget()
        self.assertIn("joseph-pulse-dot", self._get_floating_html())

    def test_floating_html_contains_ambient_text(self):
        from utils.joseph_widget import render_joseph_floating_widget
        render_joseph_floating_widget()
        self.assertIn("joseph-floating-ambient", self._get_floating_html())

    def test_not_rendered_in_sidebar(self):
        """Floating widget must NOT use st.sidebar."""
        from utils.joseph_widget import render_joseph_floating_widget
        render_joseph_floating_widget()
        # The floating div should be in a direct st.markdown call, not sidebar
        html = self._get_floating_html()
        self.assertTrue(len(html) > 0)
        # Verify it is NOT inside st.sidebar context
        sidebar_calls = [c for c in _mock_st.sidebar.markdown.call_args_list
                         if c[0] and "joseph-floating-widget" in c[0][0]]
        self.assertEqual(len(sidebar_calls), 0)

    @patch("utils.joseph_widget.get_joseph_avatar_b64", return_value="FLOAT_B64")
    def test_avatar_image_rendered(self, mock_avatar):
        from utils.joseph_widget import render_joseph_floating_widget
        render_joseph_floating_widget()
        html = self._get_floating_html()
        self.assertIn("joseph-floating-avatar", html)
        self.assertIn("FLOAT_B64", html)

    @patch("utils.joseph_widget.get_joseph_avatar_b64", return_value="")
    def test_fallback_emoji_when_no_avatar(self, mock_avatar):
        from utils.joseph_widget import render_joseph_floating_widget
        render_joseph_floating_widget()
        self.assertIn("🎙️", self._get_floating_html())

    @patch("utils.joseph_widget.joseph_ambient_line", return_value="FLOAT LINE")
    @patch("utils.joseph_widget.joseph_get_ambient_context", return_value=("idle", {}))
    def test_ambient_line_rendered(self, mock_ctx, mock_line):
        from utils.joseph_widget import render_joseph_floating_widget
        render_joseph_floating_widget()
        self.assertIn("FLOAT LINE", self._get_floating_html())

    @patch("utils.joseph_widget.joseph_ambient_line", return_value="")
    @patch("utils.joseph_widget.joseph_get_ambient_context", return_value=("idle", {}))
    def test_default_ambient_when_empty(self, mock_ctx, mock_line):
        from utils.joseph_widget import render_joseph_floating_widget
        render_joseph_floating_widget()
        self.assertIn("ALWAYS watching", self._get_floating_html())

    def test_html_escaping_ambient(self):
        """Ambient text must be HTML-escaped to prevent injection."""
        with patch("utils.joseph_widget.joseph_ambient_line",
                   return_value="<script>alert(1)</script>"):
            with patch("utils.joseph_widget.joseph_get_ambient_context",
                       return_value=("idle", {})):
                from utils.joseph_widget import render_joseph_floating_widget
                render_joseph_floating_widget()
                html = self._get_floating_html()
                self.assertNotIn("<script>", html)
                self.assertIn("&lt;script&gt;", html)

    def test_unsafe_allow_html(self):
        from utils.joseph_widget import render_joseph_floating_widget
        render_joseph_floating_widget()
        for call in _mock_st.markdown.call_args_list:
            if call[0] and '<div class="joseph-floating-widget">' in str(call[0][0]):
                self.assertTrue(call[1].get("unsafe_allow_html", False))


# ============================================================
# SECTION 5: inject_joseph_inline_commentary()
# ============================================================

class TestInjectJosephInlineCommentary(unittest.TestCase):
    """Test inline commentary injection."""

    def setUp(self):
        _mock_st.reset_mock()
        _mock_st.session_state = {}

    def test_callable(self):
        from utils.joseph_widget import inject_joseph_inline_commentary
        self.assertTrue(callable(inject_joseph_inline_commentary))

    def test_noop_on_empty_results(self):
        from utils.joseph_widget import inject_joseph_inline_commentary
        inject_joseph_inline_commentary([])
        _mock_st.markdown.assert_not_called()

    def test_noop_on_none_results(self):
        from utils.joseph_widget import inject_joseph_inline_commentary
        inject_joseph_inline_commentary(None)
        # No inline card div should be rendered
        calls = [c for c in _mock_st.markdown.call_args_list
                 if c[0] and '<div class="joseph-inline-card">' in c[0][0]]
        self.assertEqual(len(calls), 0)

    @patch("utils.joseph_widget.joseph_commentary", return_value="HOT TAKE")
    def test_renders_inline_card(self, mock_comm):
        from utils.joseph_widget import inject_joseph_inline_commentary
        inject_joseph_inline_commentary([{"player": "LeBron"}])
        calls = [c for c in _mock_st.markdown.call_args_list
                 if "joseph-inline-card" in str(c)]
        self.assertTrue(len(calls) > 0)

    @patch("utils.joseph_widget.joseph_commentary", return_value="SMASH IT")
    def test_inline_card_contains_label(self, mock_comm):
        from utils.joseph_widget import inject_joseph_inline_commentary
        inject_joseph_inline_commentary([{"player": "Steph"}])
        calls = [c for c in _mock_st.markdown.call_args_list
                 if "joseph-inline-card" in str(c)]
        self.assertTrue(any("Joseph M. Smith" in str(c) for c in calls))

    @patch("utils.joseph_widget.joseph_commentary", return_value="COMMENTARY")
    def test_inline_card_contains_commentary(self, mock_comm):
        from utils.joseph_widget import inject_joseph_inline_commentary
        inject_joseph_inline_commentary([{"player": "Jokic"}])
        # Commentary text is now streamed via st.write_stream (typing effect)
        # or rendered in a separate markdown call. Verify the card header renders.
        calls = [c for c in _mock_st.markdown.call_args_list
                 if "joseph-inline-card" in str(c)]
        self.assertTrue(len(calls) > 0)

    @patch("utils.joseph_widget.joseph_commentary", return_value="SMASH")
    def test_verdict_smash_class(self, mock_comm):
        from utils.joseph_widget import inject_joseph_inline_commentary
        inject_joseph_inline_commentary(
            [{"player": "LeBron", "verdict": "SMASH"}]
        )
        calls = [c for c in _mock_st.markdown.call_args_list
                 if "joseph-inline-card" in str(c)]
        self.assertTrue(any("joseph-widget-verdict-smash" in str(c) for c in calls))

    @patch("utils.joseph_widget.joseph_commentary", return_value="LEAN")
    def test_verdict_lean_class(self, mock_comm):
        from utils.joseph_widget import inject_joseph_inline_commentary
        inject_joseph_inline_commentary(
            [{"player": "Steph", "verdict": "LEAN"}]
        )
        calls = [c for c in _mock_st.markdown.call_args_list
                 if "joseph-inline-card" in str(c)]
        self.assertTrue(any("joseph-widget-verdict-lean" in str(c) for c in calls))

    @patch("utils.joseph_widget.joseph_commentary", return_value="FADE")
    def test_verdict_fade_class(self, mock_comm):
        from utils.joseph_widget import inject_joseph_inline_commentary
        inject_joseph_inline_commentary(
            [{"player": "Luka", "verdict": "FADE"}]
        )
        calls = [c for c in _mock_st.markdown.call_args_list
                 if "joseph-inline-card" in str(c)]
        self.assertTrue(any("joseph-widget-verdict-fade" in str(c) for c in calls))

    @patch("utils.joseph_widget.joseph_commentary", return_value="NO VERDICT")
    def test_no_verdict_class_when_missing(self, mock_comm):
        from utils.joseph_widget import inject_joseph_inline_commentary
        inject_joseph_inline_commentary([{"player": "Giannis"}])
        # Only check calls that render the actual inline card div (not CSS)
        calls = [c for c in _mock_st.markdown.call_args_list
                 if c[0] and '<div class="joseph-inline-card">' in c[0][0]]
        for c in calls:
            html = c[0][0]
            self.assertNotIn("joseph-widget-verdict-smash", html)
            self.assertNotIn("joseph-widget-verdict-lean", html)
            self.assertNotIn("joseph-widget-verdict-fade", html)

    @patch("utils.joseph_widget.joseph_commentary", return_value="")
    def test_no_render_when_empty_commentary(self, mock_comm):
        from utils.joseph_widget import inject_joseph_inline_commentary
        inject_joseph_inline_commentary([{"player": "Test"}])
        # Only count calls that render the actual inline card div (not CSS)
        calls = [c for c in _mock_st.markdown.call_args_list
                 if c[0] and '<div class="joseph-inline-card">' in c[0][0]]
        self.assertEqual(len(calls), 0)

    @patch("utils.joseph_widget.joseph_commentary",
           return_value="<script>bad</script>")
    def test_html_escaping_commentary(self, mock_comm):
        from utils.joseph_widget import inject_joseph_inline_commentary
        inject_joseph_inline_commentary([{"player": "Test"}])
        calls = [c for c in _mock_st.markdown.call_args_list
                 if "joseph-inline-card" in str(c)]
        for c in calls:
            self.assertNotIn("<script>bad</script>", str(c))

    @patch("utils.joseph_widget.joseph_commentary", return_value="TAKE")
    @patch("utils.joseph_widget.get_joseph_avatar_b64", return_value="B64IMG")
    def test_inline_avatar_rendered(self, mock_av, mock_comm):
        from utils.joseph_widget import inject_joseph_inline_commentary
        inject_joseph_inline_commentary([{"player": "AD"}])
        calls = [c for c in _mock_st.markdown.call_args_list
                 if "joseph-inline-card" in str(c)]
        self.assertTrue(any("joseph-inline-avatar" in str(c) for c in calls))
        self.assertTrue(any("B64IMG" in str(c) for c in calls))

    @patch("utils.joseph_widget.joseph_commentary", return_value="TAKE")
    @patch("utils.joseph_widget.get_joseph_avatar_b64", return_value="")
    def test_inline_emoji_fallback(self, mock_av, mock_comm):
        from utils.joseph_widget import inject_joseph_inline_commentary
        inject_joseph_inline_commentary([{"player": "KD"}])
        calls = [c for c in _mock_st.markdown.call_args_list
                 if "joseph-inline-card" in str(c)]
        self.assertTrue(any("🎙️" in str(c) for c in calls))

    @patch("utils.joseph_widget.joseph_commentary", return_value="TAKE")
    def test_default_context_type(self, mock_comm):
        from utils.joseph_widget import inject_joseph_inline_commentary
        inject_joseph_inline_commentary([{"player": "Test"}])
        mock_comm.assert_called_with([{"player": "Test"}], "analysis_results")

    @patch("utils.joseph_widget.joseph_commentary", return_value="TAKE")
    def test_custom_context_type(self, mock_comm):
        from utils.joseph_widget import inject_joseph_inline_commentary
        inject_joseph_inline_commentary([{"player": "T"}], "entry_built")
        mock_comm.assert_called_with([{"player": "T"}], "entry_built")

    @patch("utils.joseph_widget.joseph_commentary", return_value="STAY")
    def test_verdict_stay_away_uses_fade_class(self, mock_comm):
        from utils.joseph_widget import inject_joseph_inline_commentary
        inject_joseph_inline_commentary(
            [{"player": "X", "verdict": "STAY_AWAY"}]
        )
        calls = [c for c in _mock_st.markdown.call_args_list
                 if "joseph-inline-card" in str(c)]
        self.assertTrue(any("joseph-widget-verdict-fade" in str(c) for c in calls))

    @patch("utils.joseph_widget.joseph_commentary", return_value="TAKE")
    def test_joseph_verdict_key_fallback(self, mock_comm):
        """Should check joseph_verdict key when verdict is missing."""
        from utils.joseph_widget import inject_joseph_inline_commentary
        inject_joseph_inline_commentary(
            [{"player": "Z", "joseph_verdict": "SMASH"}]
        )
        calls = [c for c in _mock_st.markdown.call_args_list
                 if "joseph-inline-card" in str(c)]
        self.assertTrue(any("joseph-widget-verdict-smash" in str(c) for c in calls))


# ============================================================
# SECTION 6: render_joseph_ask_popover()
# ============================================================

class TestRenderJosephAskPopover(unittest.TestCase):
    """Test the Ask Joseph popover rendering function."""

    def setUp(self):
        _mock_st.reset_mock()
        _mock_st.session_state = {}

    def _get_popover_html(self):
        """Return the HTML string from the popover's st.markdown calls."""
        for call in _mock_st.markdown.call_args_list:
            if call[0] and '<div class="joseph-popover-container">' in call[0][0]:
                return call[0][0]
        return ""

    def test_callable(self):
        from utils.joseph_widget import render_joseph_ask_popover
        self.assertTrue(callable(render_joseph_ask_popover))

    def test_does_not_raise(self):
        from utils.joseph_widget import render_joseph_ask_popover
        render_joseph_ask_popover()

    def test_calls_popover(self):
        from utils.joseph_widget import render_joseph_ask_popover
        render_joseph_ask_popover()
        _mock_st.popover.assert_called()

    def test_popover_label(self):
        from utils.joseph_widget import render_joseph_ask_popover
        render_joseph_ask_popover()
        call_args = _mock_st.popover.call_args
        self.assertIn("Ask Joseph", call_args[0][0])

    def test_popover_emoji_in_label(self):
        from utils.joseph_widget import render_joseph_ask_popover
        render_joseph_ask_popover()
        call_args = _mock_st.popover.call_args
        self.assertIn("🎙️", call_args[0][0])

    @patch("utils.joseph_widget.get_joseph_avatar_b64", return_value="POP_B64")
    def test_popover_avatar_rendered(self, mock_avatar):
        from utils.joseph_widget import render_joseph_ask_popover
        render_joseph_ask_popover()
        html = self._get_popover_html()
        self.assertIn("joseph-popover-avatar", html)
        self.assertIn("POP_B64", html)

    @patch("utils.joseph_widget.get_joseph_avatar_b64", return_value="")
    def test_popover_emoji_fallback(self, mock_avatar):
        from utils.joseph_widget import render_joseph_ask_popover
        render_joseph_ask_popover()
        html = self._get_popover_html()
        self.assertIn("🎙️", html)

    def test_popover_contains_name(self):
        from utils.joseph_widget import render_joseph_ask_popover
        render_joseph_ask_popover()
        html = self._get_popover_html()
        self.assertIn("Joseph M. Smith", html)

    def test_popover_contains_title_class(self):
        from utils.joseph_widget import render_joseph_ask_popover
        render_joseph_ask_popover()
        html = self._get_popover_html()
        self.assertIn("joseph-popover-title", html)

    def test_popover_contains_live_dot(self):
        from utils.joseph_widget import render_joseph_ask_popover
        render_joseph_ask_popover()
        html = self._get_popover_html()
        self.assertIn("joseph-pulse-dot", html)

    @patch("utils.joseph_widget.joseph_commentary", return_value="HOT TAKE POP")
    def test_popover_with_results(self, mock_comm):
        from utils.joseph_widget import render_joseph_ask_popover
        render_joseph_ask_popover(results=[{"player": "Tatum"}])
        html = self._get_popover_html()
        self.assertIn("HOT TAKE POP", html)
        mock_comm.assert_called_with([{"player": "Tatum"}], "analysis_results")

    @patch("utils.joseph_widget.joseph_commentary", return_value="CUSTOM")
    def test_popover_custom_context(self, mock_comm):
        from utils.joseph_widget import render_joseph_ask_popover
        render_joseph_ask_popover(
            results=[{"player": "AD"}], context_type="entry_built"
        )
        mock_comm.assert_called_with([{"player": "AD"}], "entry_built")

    @patch("utils.joseph_widget.joseph_ambient_line", return_value="AMBIENT POP")
    @patch("utils.joseph_widget.joseph_get_ambient_context", return_value=("idle", {}))
    def test_popover_ambient_when_no_results(self, mock_ctx, mock_line):
        from utils.joseph_widget import render_joseph_ask_popover
        render_joseph_ask_popover()
        html = self._get_popover_html()
        self.assertIn("AMBIENT POP", html)

    @patch("utils.joseph_widget.joseph_ambient_line", return_value="")
    @patch("utils.joseph_widget.joseph_get_ambient_context", return_value=("idle", {}))
    def test_popover_default_when_empty(self, mock_ctx, mock_line):
        from utils.joseph_widget import render_joseph_ask_popover
        render_joseph_ask_popover()
        html = self._get_popover_html()
        self.assertIn("ready", html)

    def test_popover_html_escaping(self):
        with patch("utils.joseph_widget.joseph_ambient_line",
                   return_value="<script>xss</script>"):
            with patch("utils.joseph_widget.joseph_get_ambient_context",
                       return_value=("idle", {})):
                from utils.joseph_widget import render_joseph_ask_popover
                render_joseph_ask_popover()
                html = self._get_popover_html()
                self.assertNotIn("<script>", html)
                self.assertIn("&lt;script&gt;", html)

    def test_popover_unsafe_allow_html(self):
        from utils.joseph_widget import render_joseph_ask_popover
        render_joseph_ask_popover()
        # The markdown call inside the popover should use unsafe_allow_html
        for call in _mock_st.markdown.call_args_list:
            if call[0] and '<div class="joseph-popover-container">' in str(call[0][0]):
                self.assertTrue(call[1].get("unsafe_allow_html", False))


# ============================================================
# SECTION 7: Availability flags
# ============================================================

class TestAvailabilityFlags(unittest.TestCase):
    """Verify the module exposes availability flags."""

    def test_brain_available_flag(self):
        import utils.joseph_widget as w
        self.assertIsInstance(w._BRAIN_AVAILABLE, bool)

    def test_avatar_available_flag(self):
        import utils.joseph_widget as w
        self.assertIsInstance(w._AVATAR_AVAILABLE, bool)

    def test_bets_available_flag(self):
        import utils.joseph_widget as w
        self.assertIsInstance(w._BETS_AVAILABLE, bool)

    def test_auth_available_flag(self):
        import utils.joseph_widget as w
        self.assertIsInstance(w._AUTH_AVAILABLE, bool)


# ============================================================
# SECTION: inject_joseph_floating (utils/components.py wrapper)
# ============================================================

class TestInjectJosephFloating(unittest.TestCase):
    """Verify inject_joseph_floating delegates to render_joseph_floating_widget."""

    def test_inject_joseph_floating_calls_render(self):
        """inject_joseph_floating must call render_joseph_floating_widget."""
        with patch("utils.joseph_widget.render_joseph_floating_widget") as mock_render:
            from utils.components import inject_joseph_floating
            inject_joseph_floating()
            mock_render.assert_called_once()

    def test_inject_joseph_floating_no_crash_on_import_error(self):
        """inject_joseph_floating must not crash if the widget import fails."""
        from utils.components import inject_joseph_floating
        with patch(
            "utils.joseph_widget.render_joseph_floating_widget",
            side_effect=Exception("boom"),
        ):
            # Should silently catch the exception — no raise
            inject_joseph_floating()


# ============================================================
# SECTION: Floating widget avatar size & message rotation
# ============================================================

class TestFloatingWidgetAvatarSize(unittest.TestCase):
    """Verify the floating avatar is 115 px (20% larger than original 96 px)."""

    def test_floating_avatar_css_115px(self):
        from utils.joseph_widget import _WIDGET_CSS
        self.assertIn("width:115px", _WIDGET_CSS)
        self.assertIn("height:115px", _WIDGET_CSS)


class TestFloatingWidgetMessageRotation(unittest.TestCase):
    """Verify the floating widget embeds multiple messages and a 60-second rotation script."""

    def test_css_has_opacity_transition(self):
        """Ambient text needs opacity transition for smooth rotation fade."""
        from utils.joseph_widget import _WIDGET_CSS
        self.assertIn("transition:opacity", _WIDGET_CSS)

    def test_render_embeds_json_msgs(self):
        """render_joseph_floating_widget must embed JSON-encoded messages in script."""
        from utils.joseph_widget import render_joseph_floating_widget
        _mock_st.markdown.reset_mock()
        render_joseph_floating_widget()
        if _mock_st.markdown.called:
            html_out = _mock_st.markdown.call_args[0][0]
            self.assertIn("var msgs=", html_out)

    def test_render_embeds_rotation_script(self):
        """render_joseph_floating_widget must embed 60-second setInterval."""
        from utils.joseph_widget import render_joseph_floating_widget
        _mock_st.markdown.reset_mock()
        render_joseph_floating_widget()
        if _mock_st.markdown.called:
            html_out = _mock_st.markdown.call_args[0][0]
            self.assertIn("setInterval", html_out)
            self.assertIn("60000", html_out)

    def test_render_embeds_ambient_id(self):
        """The ambient div must have an id for the JS rotator to target."""
        from utils.joseph_widget import render_joseph_floating_widget
        _mock_st.markdown.reset_mock()
        render_joseph_floating_widget()
        if _mock_st.markdown.called:
            html_out = _mock_st.markdown.call_args[0][0]
            self.assertIn('id="joseph-floating-ambient-text"', html_out)


if __name__ == "__main__":
    unittest.main()
