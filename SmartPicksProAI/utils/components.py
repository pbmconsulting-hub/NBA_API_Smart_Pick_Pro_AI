# ============================================================
# FILE: utils/components.py
# PURPOSE: Shared UI components — render_joseph_hero_banner()
#          and inject_joseph_floating() for use on every page.
# CONNECTS TO: styles/theme.py, engine/joseph_brain.py,
#              utils/joseph_widget.py
# ============================================================

"""Shared Joseph M. Smith UI components.

Provides ``render_joseph_hero_banner()`` for page headers and
``inject_joseph_floating()`` for the floating Joseph avatar
that appears in the bottom-right corner.
"""

import logging

_logger = logging.getLogger(__name__)

try:
    import streamlit as st
    _ST_AVAILABLE = True
except ImportError:
    _ST_AVAILABLE = False

try:
    from styles.theme import get_hero_banner_html, _load_image_b64
except ImportError:
    def get_hero_banner_html() -> str:  # type: ignore[misc]
        return ""

    def _load_image_b64(filename: str) -> str:  # type: ignore[misc]
        return ""

try:
    from engine.joseph_brain import get_ambient_line
except ImportError:
    def get_ambient_line() -> str:  # type: ignore[misc]
        return ""


# ============================================================
# SECTION: Hero Banner
# ============================================================

def render_joseph_hero_banner() -> None:
    """Render the Joseph M. Smith hero banner at the top of a page.

    Displays the full-width hero banner image with Joseph's
    branding. Falls back to a styled text header if the banner
    image is unavailable.
    """
    if not _ST_AVAILABLE:
        return

    try:
        banner_html = get_hero_banner_html()
        if banner_html:
            st.markdown(banner_html, unsafe_allow_html=True)
        else:
            # Fallback styled header
            st.markdown(
                '<div style="background:linear-gradient(135deg,#0a1628,#162d50);'
                'padding:20px 30px;border-radius:12px;margin-bottom:20px;'
                'border:1px solid rgba(0,240,255,0.2);">'
                '<h2 style="font-family:Orbitron,sans-serif;color:#00f0ff;'
                'margin:0;font-size:1.4rem;">🎙️ Joseph M. Smith</h2>'
                '<p style="color:#aaa;margin:4px 0 0;font-size:0.85rem;">'
                'AI Analyst • Smart Pick Pro</p></div>',
                unsafe_allow_html=True,
            )
    except Exception as exc:
        _logger.debug("[Components] Hero banner render error: %s", exc)


# ============================================================
# SECTION: Floating Avatar
# ============================================================

_FLOATING_CSS = """
<style>
.joseph-floating {
    position: fixed;
    bottom: 24px;
    right: 24px;
    z-index: 9999;
    cursor: pointer;
    transition: transform 0.3s ease, box-shadow 0.3s ease;
}
.joseph-floating:hover {
    transform: scale(1.1);
}
.joseph-floating img {
    width: 56px;
    height: 56px;
    border-radius: 50%;
    border: 2px solid #00f0ff;
    box-shadow: 0 0 16px rgba(0,240,255,0.35);
    animation: pulse-float 3s ease-in-out infinite;
}
.joseph-floating .tooltip {
    display: none;
    position: absolute;
    bottom: 64px;
    right: 0;
    background: rgba(10,22,40,0.95);
    border: 1px solid rgba(0,240,255,0.3);
    border-radius: 8px;
    padding: 10px 14px;
    width: 220px;
    font-size: 0.75rem;
    color: #ccc;
    font-style: italic;
    box-shadow: 0 4px 20px rgba(0,0,0,0.4);
}
.joseph-floating:hover .tooltip {
    display: block;
}
@keyframes pulse-float {
    0%, 100% { box-shadow: 0 0 12px rgba(0,240,255,0.25); }
    50%       { box-shadow: 0 0 24px rgba(0,240,255,0.55); }
}
</style>
"""


def inject_joseph_floating() -> None:
    """Inject a floating Joseph M. Smith avatar into the page.

    The avatar appears in the bottom-right corner with a hover
    tooltip showing an ambient commentary line.
    """
    if not _ST_AVAILABLE:
        return

    try:
        avatar_b64 = _load_image_b64("Joseph_M_Smith_Avatar.png")
        line = get_ambient_line()

        if avatar_b64:
            img_tag = (
                f'<img src="data:image/png;base64,{avatar_b64}" '
                f'alt="Joseph M. Smith">'
            )
        else:
            img_tag = (
                '<div style="width:56px;height:56px;border-radius:50%;'
                'background:rgba(0,240,255,0.15);display:flex;align-items:center;'
                'justify-content:center;font-size:1.5rem;border:2px solid #00f0ff;">🧠</div>'
            )

        tooltip = f'<div class="tooltip">"{line}"</div>' if line else ""

        html = f"""
        {_FLOATING_CSS}
        <div class="joseph-floating">
            {img_tag}
            {tooltip}
        </div>
        """
        st.markdown(html, unsafe_allow_html=True)
    except Exception as exc:
        _logger.debug("[Components] Floating avatar error: %s", exc)
