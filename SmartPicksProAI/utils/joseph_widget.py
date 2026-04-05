# ============================================================
# FILE: utils/joseph_widget.py
# PURPOSE: Joseph M. Smith global sidebar widget — clickable
#          avatar, ambient commentary, and track record display
#          rendered on every page.
# CONNECTS TO: styles/theme.py, engine/joseph_brain.py,
#              engine/joseph_bets.py
# ============================================================

"""Joseph M. Smith sidebar widget.

Provides a reusable Streamlit sidebar component that displays
Joseph's avatar, a rotating ambient commentary line, and a
summary of his betting track record.  Call :func:`render_joseph_sidebar`
from any page to inject the widget.
"""

import logging

_logger = logging.getLogger(__name__)

try:
    import streamlit as st
    _ST_AVAILABLE = True
except ImportError:
    _ST_AVAILABLE = False

try:
    from styles.theme import get_sidebar_avatar_html
except ImportError:
    def get_sidebar_avatar_html() -> str:  # type: ignore[misc]
        return ""

try:
    from engine.joseph_brain import get_ambient_line
except ImportError:
    def get_ambient_line() -> str:  # type: ignore[misc]
        return "Trust the data, not the hype."

try:
    from engine.joseph_bets import joseph_get_track_record
except ImportError:
    def joseph_get_track_record(**kw) -> dict:  # type: ignore[misc]
        return {"summary": {}, "by_tier": {}, "recent_bets": [],
                "joseph_headline": "Track record unavailable."}


# ============================================================
# SECTION: Sidebar Widget
# ============================================================

def render_joseph_sidebar() -> None:
    """Render the Joseph M. Smith sidebar widget.

    Displays:
    1. Clickable avatar with glow animation
    2. Ambient commentary line (rotates each page load)
    3. Track record summary (win/loss, ROI)

    Safe to call even when Streamlit or backend modules
    are unavailable — degrades gracefully.
    """
    if not _ST_AVAILABLE:
        return

    try:
        with st.sidebar:
            # ── Avatar ─────────────────────────────────────
            avatar_html = get_sidebar_avatar_html()
            if avatar_html:
                st.markdown(avatar_html, unsafe_allow_html=True)
            else:
                st.markdown(
                    '<div style="text-align:center;font-size:3rem;'
                    'margin-bottom:0.5rem;">🧠</div>',
                    unsafe_allow_html=True,
                )

            # ── Name & Title ───────────────────────────────
            st.markdown(
                '<p style="text-align:center;font-family:Orbitron,sans-serif;'
                'color:#00f0ff;font-size:0.85rem;margin:0;">'
                'Joseph M. Smith</p>'
                '<p style="text-align:center;color:#aaa;font-size:0.7rem;'
                'margin-top:2px;">AI Analyst • Smart Pick Pro</p>',
                unsafe_allow_html=True,
            )

            # ── Ambient Commentary ─────────────────────────
            line = get_ambient_line()
            st.markdown(
                f'<div style="background:rgba(0,240,255,0.06);'
                f'border-left:3px solid #00f0ff;padding:8px 10px;'
                f'margin:8px 0;border-radius:4px;font-size:0.75rem;'
                f'color:#ccc;font-style:italic;">'
                f'"{line}"</div>',
                unsafe_allow_html=True,
            )

            # ── Track Record ───────────────────────────────
            record = joseph_get_track_record(limit=10)
            headline = record.get("joseph_headline", "")
            summary = record.get("summary", {})

            if headline:
                st.caption(headline)

            total = summary.get("total_bets", 0)
            if total > 0:
                wins = summary.get("wins", 0)
                losses = summary.get("losses", 0)
                pushes = summary.get("pushes", 0)
                st.markdown(
                    f"<div style='font-size:0.75rem;color:#888;'>"
                    f"📈 {wins}W – {losses}L – {pushes}P "
                    f"({total} total)</div>",
                    unsafe_allow_html=True,
                )

            st.markdown("---")
    except Exception as exc:
        _logger.debug("[JosephWidget] Sidebar render error: %s", exc)
