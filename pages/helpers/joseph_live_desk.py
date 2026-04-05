# ============================================================
# FILE: pages/helpers/joseph_live_desk.py
# PURPOSE: Joseph M. Smith broadcast desk helpers — avatar
#          loader, live desk CSS, and full analysis rendering.
# CONNECTS TO: styles/theme.py, engine/joseph_brain.py,
#              engine/joseph_eval.py
# ============================================================

"""Joseph M. Smith live broadcast desk helpers.

Provides utility functions for rendering Joseph's broadcast desk
UI — avatar loading, desk-specific CSS injection, and full
analysis display panels.
"""

import os
import base64
import logging

_logger = logging.getLogger(__name__)

try:
    import streamlit as st
    _ST_AVAILABLE = True
except ImportError:
    _ST_AVAILABLE = False


# ============================================================
# SECTION: Avatar Loader
# ============================================================

_ASSETS_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
    "SmartPicksProAI", "assets",
)


def get_joseph_avatar_b64() -> str:
    """Load Joseph M. Smith avatar as a base64-encoded string.

    Returns:
        Base64 string of the avatar PNG, or empty string if
        the file is not found.
    """
    path = os.path.join(_ASSETS_DIR, "Joseph_M_Smith_Avatar.png")
    if not os.path.isfile(path):
        return ""
    try:
        with open(path, "rb") as f:
            return base64.b64encode(f.read()).decode()
    except Exception:
        return ""


def get_joseph_banner_b64() -> str:
    """Load the Joseph M. Smith hero banner as base64."""
    path = os.path.join(_ASSETS_DIR, "Joseph_M_Smith_Hero_Banner.png")
    if not os.path.isfile(path):
        return ""
    try:
        with open(path, "rb") as f:
            return base64.b64encode(f.read()).decode()
    except Exception:
        return ""


# ============================================================
# SECTION: Live Desk CSS
# ============================================================

_LIVE_DESK_CSS = """
<style>
/* ── Joseph Live Desk ─────────────────────────────────────── */
.joseph-desk {
    background: linear-gradient(135deg, #0a1628 0%, #162d50 50%, #0a1628 100%);
    border: 1px solid rgba(0,240,255,0.25);
    border-radius: 16px;
    padding: 24px;
    margin-bottom: 20px;
    position: relative;
    overflow: hidden;
}
.joseph-desk::before {
    content: '';
    position: absolute;
    top: 0; left: 0; right: 0;
    height: 3px;
    background: linear-gradient(90deg, transparent, #00f0ff, transparent);
    animation: deskShimmer 4s ease-in-out infinite;
}
@keyframes deskShimmer {
    0%, 100% { opacity: 0.3; }
    50% { opacity: 1; }
}
.joseph-desk-header {
    display: flex;
    align-items: center;
    gap: 16px;
    margin-bottom: 16px;
}
.joseph-desk-avatar {
    width: 72px;
    height: 72px;
    border-radius: 50%;
    border: 2px solid #00f0ff;
    box-shadow: 0 0 20px rgba(0,240,255,0.3);
}
.joseph-desk-title {
    font-family: 'Orbitron', sans-serif;
    color: #00f0ff;
    font-size: 1.2rem;
    font-weight: 700;
    margin: 0;
}
.joseph-desk-subtitle {
    color: #888;
    font-size: 0.8rem;
    margin: 2px 0 0;
}
.joseph-desk-content {
    color: #ddd;
    font-size: 0.9rem;
    line-height: 1.6;
}
.joseph-desk-section {
    background: rgba(0,240,255,0.04);
    border-left: 3px solid #00f0ff;
    border-radius: 0 8px 8px 0;
    padding: 12px 16px;
    margin: 12px 0;
}
.joseph-desk-section h4 {
    font-family: 'Orbitron', sans-serif;
    color: #00f0ff;
    font-size: 0.85rem;
    margin: 0 0 6px;
}
.joseph-live-badge {
    display: inline-block;
    background: #ff3333;
    color: white;
    font-size: 0.65rem;
    font-weight: 700;
    padding: 2px 8px;
    border-radius: 10px;
    text-transform: uppercase;
    letter-spacing: 1px;
    animation: livePulse 2s ease-in-out infinite;
}
@keyframes livePulse {
    0%, 100% { opacity: 1; }
    50% { opacity: 0.6; }
}
</style>
"""


def inject_live_desk_css() -> None:
    """Inject the live desk CSS into the Streamlit page."""
    if _ST_AVAILABLE:
        st.markdown(_LIVE_DESK_CSS, unsafe_allow_html=True)


# ============================================================
# SECTION: Full Analysis Rendering
# ============================================================

def render_joseph_desk_header() -> None:
    """Render the broadcast desk header with avatar and LIVE badge."""
    if not _ST_AVAILABLE:
        return

    avatar_b64 = get_joseph_avatar_b64()
    if avatar_b64:
        avatar_html = (
            f'<img src="data:image/png;base64,{avatar_b64}" '
            f'class="joseph-desk-avatar" alt="Joseph M. Smith">'
        )
    else:
        avatar_html = (
            '<div class="joseph-desk-avatar" style="background:rgba(0,240,255,0.15);'
            'display:flex;align-items:center;justify-content:center;font-size:2rem;">🧠</div>'
        )

    html = f"""
    <div class="joseph-desk-header">
        {avatar_html}
        <div>
            <p class="joseph-desk-title">Joseph M. Smith
                <span class="joseph-live-badge">LIVE</span>
            </p>
            <p class="joseph-desk-subtitle">AI Analyst • Smart Pick Pro</p>
        </div>
    </div>
    """
    st.markdown(html, unsafe_allow_html=True)


def render_full_analysis(analysis: dict) -> None:
    """Render a complete Joseph analysis result on the broadcast desk.

    Args:
        analysis: Dictionary returned by
            :func:`engine.joseph_brain.joseph_full_analysis`.
    """
    if not _ST_AVAILABLE:
        return

    try:
        # ── Verdict Section ────────────────────────────────
        verdict = analysis.get("verdict", analysis.get("joseph_commentary", ""))
        if verdict:
            st.markdown(
                f'<div class="joseph-desk-section">'
                f'<h4>📋 Verdict</h4>'
                f'<div class="joseph-desk-content">{verdict}</div>'
                f'</div>',
                unsafe_allow_html=True,
            )

        # ── Platinum Lock Badge ────────────────────────────
        if analysis.get("platinum_lock"):
            st.markdown(
                '<div style="text-align:center;margin:12px 0;">'
                '<span style="background:linear-gradient(135deg,#00f0ff,#7b2ff7);'
                'color:white;padding:8px 20px;border-radius:20px;font-family:Orbitron,'
                'sans-serif;font-size:0.85rem;font-weight:700;">'
                '💎 PLATINUM LOCK</span></div>',
                unsafe_allow_html=True,
            )

        # ── Player Grade ───────────────────────────────────
        pg = analysis.get("player_grade", {})
        if pg and pg.get("grade") not in (None, "N/A"):
            grade = pg.get("grade", "N/A")
            score = pg.get("score", 0)
            archetype = pg.get("archetype", "")
            st.markdown(
                f'<div class="joseph-desk-section">'
                f'<h4>📊 Player Grade</h4>'
                f'<div class="joseph-desk-content">'
                f'Grade: <strong>{grade}</strong> ({score:.0f}/100) '
                f'• Archetype: {archetype}</div></div>',
                unsafe_allow_html=True,
            )
            assessment = pg.get("joseph_assessment", "")
            if assessment:
                st.markdown(
                    f'<div class="joseph-desk-content" style="padding:0 16px;">'
                    f'{assessment}</div>',
                    unsafe_allow_html=True,
                )

        # ── Narrative Tags ─────────────────────────────────
        tags = analysis.get("narrative_tags", [])
        if tags:
            tag_html = " ".join(
                f'<span style="background:rgba(0,240,255,0.1);border:1px solid '
                f'rgba(0,240,255,0.25);padding:3px 10px;border-radius:12px;'
                f'font-size:0.75rem;color:#00f0ff;margin-right:6px;">'
                f'{t.get("label", t.get("tag", ""))}</span>'
                for t in tags
            )
            st.markdown(
                f'<div class="joseph-desk-section">'
                f'<h4>🏷️ Narrative Tags</h4>'
                f'<div style="margin-top:8px;">{tag_html}</div></div>',
                unsafe_allow_html=True,
            )

        # ── Strategy ───────────────────────────────────────
        strategy = analysis.get("strategy", {})
        strat_take = strategy.get("joseph_strategy_take", "")
        if strat_take:
            st.markdown(
                f'<div class="joseph-desk-section">'
                f'<h4>🎯 Strategy</h4>'
                f'<div class="joseph-desk-content">{strat_take}</div></div>',
                unsafe_allow_html=True,
            )

        # ── Ambient Line ───────────────────────────────────
        ambient = analysis.get("ambient_line", "")
        if ambient:
            st.markdown(
                f'<div style="text-align:center;font-style:italic;color:#888;'
                f'font-size:0.8rem;margin-top:12px;">"{ambient}"</div>',
                unsafe_allow_html=True,
            )
    except Exception as exc:
        _logger.debug("[LiveDesk] render_full_analysis error: %s", exc)
        if _ST_AVAILABLE:
            st.error("Unable to render analysis.")
