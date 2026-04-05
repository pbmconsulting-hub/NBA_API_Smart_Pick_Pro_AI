# ============================================================
# FILE: utils/player_modal.py
# PURPOSE: Player modal dialog with "Ask Joseph M. Smith"
#          integration — displays player details and allows
#          invoking joseph_platinum_lock() from the UI.
# CONNECTS TO: engine/joseph_brain.py, styles/theme.py
# ============================================================

"""Player modal with Joseph M. Smith integration.

Provides a Streamlit dialog (modal) that displays player stats,
archetype evaluation, and an "Ask Joseph" button that runs the
platinum lock analysis on the selected player/prop combination.
"""

import logging

_logger = logging.getLogger(__name__)

try:
    import streamlit as st
    _ST_AVAILABLE = True
except ImportError:
    _ST_AVAILABLE = False

try:
    from engine.joseph_brain import joseph_platinum_lock, joseph_full_analysis
except ImportError:
    _logger.warning("[PlayerModal] Could not import joseph_brain")

    def joseph_platinum_lock(player, prop_line, stat_type, **kw):  # type: ignore[misc]
        return {"is_lock": False, "lock_score": 0, "reasons": [],
                "joseph_pitch": "Analysis module unavailable."}

    def joseph_full_analysis(player, prop_line, stat_type, **kw):  # type: ignore[misc]
        return {"verdict": "Analysis module unavailable.",
                "platinum_lock": False, "confidence_tier": "Bronze"}

try:
    from engine.joseph_eval import joseph_grade_player
except ImportError:
    def joseph_grade_player(player, archetype=None):  # type: ignore[misc]
        return {"grade": "N/A", "score": 0, "archetype": "Unknown",
                "strengths": [], "weaknesses": [],
                "joseph_assessment": "Evaluation module unavailable."}

try:
    from styles.theme import get_verdict_banner_html
except ImportError:
    def get_verdict_banner_html(text: str) -> str:  # type: ignore[misc]
        return f"<div>{text}</div>"


# ============================================================
# SECTION: Player Modal
# ============================================================

def render_player_modal(
    player: dict,
    *,
    prop_line: float = 0.0,
    stat_type: str = "points",
    show_ask_joseph: bool = True,
) -> None:
    """Render a player detail modal with Joseph M. Smith integration.

    Displays player stats, archetype grade, and optionally an
    "Ask Joseph M. Smith" button that triggers platinum lock analysis.

    Args:
        player: Player stats dictionary.
        prop_line: Current prop line value (for Joseph analysis).
        stat_type: Stat type being considered.
        show_ask_joseph: Whether to show the Ask Joseph button.
    """
    if not _ST_AVAILABLE:
        return

    try:
        name = player.get("player_name", player.get("name", "Unknown"))
        team = player.get("team", player.get("team_abbreviation", ""))
        position = player.get("position", "")

        # ── Header ─────────────────────────────────────────
        st.markdown(f"### 🏀 {name}")
        if team or position:
            st.caption(f"{position} | {team}" if position else team)

        # ── Key Stats ──────────────────────────────────────
        cols = st.columns(4)
        stat_map = [
            ("PPG", "points_avg", "ppg"),
            ("RPG", "rebounds_avg", "rpg"),
            ("APG", "assists_avg", "apg"),
            ("FG%", "fg_pct", "fg_pct"),
        ]
        for i, (label, *keys) in enumerate(stat_map):
            val = None
            for k in keys:
                val = player.get(k)
                if val is not None:
                    break
            display = f"{float(val):.1f}" if val is not None else "N/A"
            cols[i].metric(label, display)

        # ── Player Grade ───────────────────────────────────
        grade_result = joseph_grade_player(player)
        grade = grade_result.get("grade", "N/A")
        score = grade_result.get("score", 0)
        archetype = grade_result.get("archetype", "Unknown")

        st.markdown(f"**Archetype:** {archetype} | **Grade:** {grade} ({score:.0f}/100)")

        assessment = grade_result.get("joseph_assessment", "")
        if assessment:
            st.info(f"🧠 {assessment}")

        # ── Ask Joseph Button ──────────────────────────────
        if show_ask_joseph and prop_line > 0:
            st.divider()
            if st.button(f"🎙️ Ask Joseph M. Smith about {name} {stat_type} @ {prop_line}",
                         key=f"ask_joseph_{name}_{stat_type}"):
                with st.spinner("Joseph is analyzing..."):
                    lock_result = joseph_platinum_lock(
                        player, prop_line, stat_type,
                    )

                is_lock = lock_result.get("is_lock", False)
                pitch = lock_result.get("joseph_pitch", "")

                if is_lock:
                    st.markdown(
                        get_verdict_banner_html(f"💎 PLATINUM LOCK: {pitch}"),
                        unsafe_allow_html=True,
                    )
                else:
                    st.markdown(
                        get_verdict_banner_html(pitch),
                        unsafe_allow_html=True,
                    )

                reasons = lock_result.get("reasons", [])
                if reasons:
                    for reason in reasons:
                        st.markdown(f"- {reason}")
    except Exception as exc:
        _logger.debug("[PlayerModal] render error: %s", exc)
        if _ST_AVAILABLE:
            st.error("Unable to render player modal.")
