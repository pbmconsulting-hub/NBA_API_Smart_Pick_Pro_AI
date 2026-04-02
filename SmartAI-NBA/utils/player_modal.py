# ============================================================
# FILE: utils/player_modal.py
# PURPOSE: Render the "Player Spotlight" Jumbo Card modal via
#          Streamlit's @st.dialog decorator.  Contains:
#          - Zone 1: Vitals & Season Stats
#          - Zone 2: Market (all available bets)
#          - Zone 3: "Ask Joseph M. Smith" trigger button
#          - Zone 4: Joseph's Broadcast Desk (on-demand)
#
# USAGE:
#   from utils.player_modal import show_player_spotlight
#   show_player_spotlight(player_name, grouped_data)
# ============================================================

import html as _html
import logging as _logging

_logger = _logging.getLogger(__name__)


def _safe_float(val, default=0.0):
    try:
        return float(val)
    except (TypeError, ValueError):
        return default


def show_player_spotlight(player_name: str, grouped_entry: dict) -> None:
    """Render the Player Spotlight modal content.

    This function is called inside an ``@st.dialog`` wrapper by the
    Neural Analysis page.  It renders four zones:

    1. **Vitals & Stats** — headshot, name, position, matchup, season averages.
    2. **The Market** — every available bet for this player.
    3. **Ask Joseph Trigger** — explicit button to invoke the AI agent.
    4. **Broadcast Desk** — Joseph's response rendered in a neon container.

    Parameters
    ----------
    player_name : str
        The player's full display name.
    grouped_entry : dict
        ``{"vitals": dict, "props": list[dict]}`` from
        :func:`utils.data_grouper.group_props_by_player`.
    """
    try:
        import streamlit as st
    except ImportError:
        return

    vitals = grouped_entry.get("vitals", {})
    props = grouped_entry.get("props", [])
    stats = vitals.get("season_stats", {})

    safe_name = _html.escape(str(player_name))
    headshot = _html.escape(str(vitals.get("headshot_url", "")))
    position = _html.escape(str(vitals.get("position", "N/A")))
    team = _html.escape(str(vitals.get("team", "N/A")))
    opponent = _html.escape(str(vitals.get("next_opponent", "TBD")))

    # ── Zone 1: Vitals & Season Stats ────────────────────────
    st.markdown(
        f'<div class="gm-modal-vitals">'
        f'<img class="gm-modal-headshot" src="{headshot}" alt="{safe_name}" '
        f'onerror="this.src=\'https://cdn.nba.com/headshots/nba/latest/1040x760/fallback.png\'">'
        f'<div class="gm-modal-info">'
        f'<h2>{safe_name}</h2>'
        f'<p>{position} · {team} vs {opponent}</p>'
        f'</div></div>',
        unsafe_allow_html=True,
    )

    ppg = stats.get("ppg", 0.0)
    rpg = stats.get("rpg", 0.0)
    apg = stats.get("apg", 0.0)
    avg_min = stats.get("avg_minutes", 0.0)

    st.markdown(
        f'<div class="gm-season-bar">'
        f'<div class="gm-season-metric"><div class="val">{ppg}</div><div class="lbl">PPG</div></div>'
        f'<div class="gm-season-metric"><div class="val">{rpg}</div><div class="lbl">RPG</div></div>'
        f'<div class="gm-season-metric"><div class="val">{apg}</div><div class="lbl">APG</div></div>'
        f'<div class="gm-season-metric"><div class="val">{avg_min}</div><div class="lbl">MIN</div></div>'
        f'</div>',
        unsafe_allow_html=True,
    )

    # ── Zone 2: The Market ───────────────────────────────────
    if props:
        st.markdown("#### 📊 Available Props")
        cells_html = ""
        for p in props:
            stat = _html.escape(str(p.get("stat_type", "")).title())
            line = p.get("prop_line", p.get("line", 0))
            try:
                line_display = f"{float(line):g}"
            except (ValueError, TypeError):
                line_display = str(line)
            direction = str(p.get("direction", "OVER")).upper()
            edge = _safe_float(p.get("edge_percentage", 0))
            edge_class = "edge-pos" if edge >= 0 else "edge-neg"
            edge_display = f"+{edge:.1f}%" if edge >= 0 else f"{edge:.1f}%"

            cells_html += (
                f'<div class="gm-market-cell">'
                f'<div class="stat-label">{stat}</div>'
                f'True Line: {_html.escape(line_display)}<br>'
                f'Direction: {_html.escape(direction)}<br>'
                f'Edge: <span class="{edge_class}">{_html.escape(edge_display)}</span>'
                f'</div>'
            )
        st.markdown(
            f'<div class="gm-market-grid">{cells_html}</div>',
            unsafe_allow_html=True,
        )
    else:
        st.info("No props available for this player.")

    # ── Zone 3: Ask Joseph M. Smith trigger ──────────────────
    st.markdown('<div class="gm-ask-joseph-btn">', unsafe_allow_html=True)
    ask_clicked = st.button(
        "🎙️ Ask Joseph M. Smith",
        key=f"ask_joseph_{player_name}",
        use_container_width=True,
    )
    st.markdown('</div>', unsafe_allow_html=True)

    # ── Zone 4: Broadcast Desk (on-demand only) ─────────────
    if ask_clicked:
        with st.spinner("Joseph M. Smith is reviewing the tape…"):
            try:
                from engine.joseph_brain import joseph_platinum_lock
                result = joseph_platinum_lock(props, stats)
            except ImportError:
                result = {
                    "platinum_lock_stat": "N/A",
                    "rant": "Joseph M. Smith brain module is loading. Try again shortly.",
                }
            except Exception as exc:
                _logger.warning("joseph_platinum_lock error: %s", exc)
                result = {
                    "platinum_lock_stat": "Error",
                    "rant": f"Joseph encountered an issue: {_html.escape(str(exc))}",
                }

        # ── Load Joseph avatar ────────────────────────────────
        avatar_html = ""
        try:
            from pages.helpers.joseph_live_desk import get_joseph_avatar_b64
            avatar_b64 = get_joseph_avatar_b64()
            if avatar_b64:
                avatar_html = (
                    f'<img src="data:image/png;base64,{avatar_b64}" '
                    f'class="gm-joseph-avatar" alt="Joseph M. Smith" />'
                )
        except Exception:
            _logger.debug("Failed to load Joseph avatar for player modal")

        lock_stat = _html.escape(str(result.get("platinum_lock_stat", "N/A")))
        rant = _html.escape(str(result.get("rant", "")))

        st.markdown(
            f'<div class="gm-joseph-response">'
            f'{avatar_html}'
            f'<div class="gm-joseph-lock">💎 PLATINUM LOCK: {lock_stat}</div>'
            f'<div class="gm-joseph-rant">{rant}</div>'
            f'</div>',
            unsafe_allow_html=True,
        )
