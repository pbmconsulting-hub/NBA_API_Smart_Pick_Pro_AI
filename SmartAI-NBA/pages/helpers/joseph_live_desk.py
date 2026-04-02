# ============================================================
# FILE: pages/helpers/joseph_live_desk.py
# PURPOSE: Joseph M. Smith's Live Broadcast Desk — renders above
#          prop cards on the Neural Analysis page (page 3).
# CONNECTS TO: engine/joseph_brain.py, styles/theme.py
# ============================================================

import os
import base64
import html as _html
import logging
import random

import streamlit as st

try:
    from utils.logger import get_logger
    _logger = get_logger(__name__)
except ImportError:
    _logger = logging.getLogger(__name__)

# ── Engine imports (safe) ────────────────────────────────────
try:
    from engine.joseph_brain import (
        joseph_full_analysis,
        joseph_quick_take,
        joseph_commentary,
        joseph_generate_best_bets,
        _extract_edge,
        _select_fragment,
        CLOSER_POOL,
        CATCHPHRASE_POOL,
        VERDICT_EMOJIS,
        TICKET_NAMES,
        DAWG_FACTOR_TABLE,
    )
    _BRAIN_AVAILABLE = True
except ImportError:
    _BRAIN_AVAILABLE = False
    VERDICT_EMOJIS = {"SMASH": "🔥", "LEAN": "✅", "FADE": "⚠️", "STAY_AWAY": "🚫"}
    TICKET_NAMES = {2: "POWER PLAY", 3: "TRIPLE THREAT", 4: "THE QUAD",
                    5: "HIGH FIVE", 6: "THE FULL SEND"}

try:
    from engine.joseph_tickets import build_joseph_ticket, generate_ticket_pitch
    _TICKETS_AVAILABLE = True
except ImportError:
    _TICKETS_AVAILABLE = False


# ═════════════════════════════════════════════════════════════
# get_joseph_avatar_b64 — cached base64 loader for avatar
# ═════════════════════════════════════════════════════════════

@st.cache_data(show_spinner=False)
def get_joseph_avatar_b64() -> str:
    """Load Joseph M Smith Avatar.png and return base64-encoded string."""
    _this = os.path.dirname(os.path.abspath(__file__))
    candidates = [
        os.path.join(_this, "..", "..", "Joseph M Smith Avatar.png"),
        os.path.join(os.getcwd(), "Joseph M Smith Avatar.png"),
        os.path.join(_this, "..", "..", "assets", "Joseph M Smith Avatar.png"),
    ]
    for path in candidates:
        norm = os.path.normpath(path)
        if os.path.isfile(norm):
            try:
                with open(norm, "rb") as fh:
                    _logger.debug("Avatar loaded from %s", norm)
                    return base64.b64encode(fh.read()).decode("utf-8")
            except Exception:
                _logger.warning("Failed reading avatar at %s", norm)
    _logger.warning("Joseph M Smith Avatar.png not found in any candidate path")
    return ""


# ═════════════════════════════════════════════════════════════
# render_live_desk_css — complete CSS for the broadcast desk
# ═════════════════════════════════════════════════════════════

def render_live_desk_css() -> str:
    """Return complete CSS string for Joseph's Live Broadcast Desk."""
    return """<style>
/* ── Joseph Live Desk — Glassmorphic Container ─────────────── */
.joseph-live-desk{
    background:rgba(7,10,19,0.85);
    backdrop-filter:blur(16px);
    -webkit-backdrop-filter:blur(16px);
    border:1px solid rgba(255,94,0,0.3);
    border-radius:16px;
    padding:24px 28px;
    margin:24px 0;
    position:relative;
    overflow:hidden;
    box-shadow:0 0 40px rgba(255,94,0,0.08),inset 0 1px 0 rgba(255,158,0,0.1);
}
/* Top broadcast bar */
.joseph-live-desk::before{
    content:'';position:absolute;top:0;left:0;right:0;height:3px;
    background:linear-gradient(90deg,#ff5e00,#ff9e00,#ff5e00);
    background-size:200% 100%;
    animation:josephShimmer 3s linear infinite;
}
/* Bottom broadcast bar */
.joseph-live-desk::after{
    content:'';position:absolute;bottom:0;left:0;right:0;height:3px;
    background:linear-gradient(90deg,#ff9e00,#ff5e00,#ff9e00);
    background-size:200% 100%;
    animation:josephShimmer 3s linear infinite reverse;
}
@keyframes josephShimmer{
    0%{background-position:-200% 0}
    100%{background-position:200% 0}
}

/* ── LIVE Pulsing Dot ─────────────────────────────────────── */
.joseph-live-dot{
    display:inline-block;width:10px;height:10px;
    background:#ff2020;border-radius:50%;
    margin-right:8px;vertical-align:middle;
    animation:josephLivePulse 1.4s ease-in-out infinite;
    box-shadow:0 0 6px rgba(255,32,32,0.6);
}
@keyframes josephLivePulse{
    0%,100%{opacity:1;transform:scale(1);box-shadow:0 0 6px rgba(255,32,32,0.6)}
    50%{opacity:0.4;transform:scale(0.85);box-shadow:0 0 2px rgba(255,32,32,0.3)}
}

/* ── Typing Indicator — 3 bouncing dots ───────────────────── */
.joseph-typing{display:inline-flex;gap:4px;align-items:center;padding:4px 0}
.joseph-typing span{
    display:inline-block;width:6px;height:6px;
    background:#ff5e00;border-radius:50%;
    animation:josephBounce 1.2s ease-in-out infinite;
}
.joseph-typing span:nth-child(2){animation-delay:0.15s}
.joseph-typing span:nth-child(3){animation-delay:0.3s}
@keyframes josephBounce{
    0%,80%,100%{transform:translateY(0)}
    40%{transform:translateY(-8px)}
}

/* ── Joseph Avatar Circle ─────────────────────────────────── */
.joseph-avatar{
    width:64px;height:64px;border-radius:50%;
    border:2px solid #ff5e00;object-fit:cover;
    box-shadow:0 0 12px rgba(255,94,0,0.35),0 0 24px rgba(255,94,0,0.15);
    flex-shrink:0;
    animation:josephAvatarGlow 3s ease-in-out infinite;
}
@keyframes josephAvatarGlow{
    0%,100%{box-shadow:0 0 12px rgba(255,94,0,0.35),0 0 24px rgba(255,94,0,0.15)}
    50%{box-shadow:0 0 18px rgba(255,94,0,0.55),0 0 36px rgba(255,94,0,0.2)}
}
.joseph-avatar-sm{width:48px;height:48px;border-radius:50%;
    border:2px solid #ff5e00;object-fit:cover;
    box-shadow:0 0 8px rgba(255,94,0,0.25);flex-shrink:0}

/* ── Broadcast Header Row ─────────────────────────────────── */
.joseph-header{display:flex;align-items:center;gap:16px;margin-bottom:16px}
.joseph-header-text{
    font-family:'Orbitron',sans-serif;font-size:1.25rem;
    color:#ff5e00;font-weight:700;letter-spacing:0.5px;
    text-shadow:0 0 12px rgba(255,94,0,0.3);
}
.joseph-subtitle{
    color:#94a3b8;font-size:0.85rem;margin-top:2px;
    font-family:'Montserrat',sans-serif;letter-spacing:0.3px;
}

/* ── Broadcast Segment Cards ──────────────────────────────── */
.joseph-segment{
    background:rgba(15,23,42,0.7);
    backdrop-filter:blur(8px);
    -webkit-backdrop-filter:blur(8px);
    border:1px solid rgba(255,94,0,0.15);
    border-left:3px solid rgba(255,94,0,0.5);
    border-radius:12px;padding:18px 22px;
    margin-bottom:14px;
    transition:all 0.25s ease;
}
.joseph-segment:hover{
    border-color:rgba(255,94,0,0.45);
    transform:translateY(-1px);
    box-shadow:0 4px 20px rgba(255,94,0,0.1);
}

.joseph-segment-title{
    font-family:'Orbitron',sans-serif;
    color:#ff5e00;font-size:1rem;font-weight:600;
    margin-bottom:10px;letter-spacing:0.5px;
    text-shadow:0 0 8px rgba(255,94,0,0.15);
}
.joseph-segment-body{
    color:#e2e8f0;font-size:0.92rem;
    line-height:1.65;font-family:'Montserrat',sans-serif;
}
.joseph-segment-body strong{color:#ff9e00}

/* ── Verdict Badges ───────────────────────────────────────── */
.joseph-verdict{
    display:inline-block;padding:3px 12px;border-radius:6px;
    font-family:'Orbitron',sans-serif;font-size:0.75rem;
    font-weight:700;letter-spacing:0.8px;margin-right:8px;
    vertical-align:middle;
    text-shadow:0 0 6px currentColor;
    transition:transform 0.15s ease;
}
.joseph-verdict:hover{transform:scale(1.05)}
.joseph-verdict-smash{background:rgba(239,68,68,0.25);color:#ff4444;border:1px solid rgba(239,68,68,0.4);box-shadow:0 0 8px rgba(239,68,68,0.15)}
.joseph-verdict-lean{background:rgba(34,197,94,0.2);color:#22c55e;border:1px solid rgba(34,197,94,0.35);box-shadow:0 0 8px rgba(34,197,94,0.12)}
.joseph-verdict-fade{background:rgba(234,179,8,0.2);color:#eab308;border:1px solid rgba(234,179,8,0.35);box-shadow:0 0 8px rgba(234,179,8,0.12)}
.joseph-verdict-stay_away{background:rgba(107,114,128,0.25);color:#9ca3af;border:1px solid rgba(107,114,128,0.35)}

/* ── Dawg Board Table ─────────────────────────────────────── */
.joseph-dawg-table{
    width:100%;border-collapse:separate;border-spacing:0;
    font-family:'Montserrat',sans-serif;font-size:0.85rem;
}
.joseph-dawg-table th{
    background:rgba(255,94,0,0.12);color:#ff5e00;
    padding:10px 14px;text-align:left;
    font-family:'Orbitron',sans-serif;font-size:0.75rem;
    letter-spacing:0.5px;border-bottom:1px solid rgba(255,94,0,0.25);
}
.joseph-dawg-table td{
    padding:8px 14px;color:#e2e8f0;
    border-bottom:1px solid rgba(148,163,184,0.1);
    transition:background 0.15s ease;
}
.joseph-dawg-table tr:hover td{background:rgba(255,94,0,0.06)}

/* ── Override Report Table ────────────────────────────────── */
.joseph-override-table{
    width:100%;border-collapse:separate;border-spacing:0;
    font-family:'Montserrat',sans-serif;font-size:0.85rem;
}
.joseph-override-table th{
    background:rgba(0,240,255,0.08);color:#00f0ff;
    padding:10px 14px;text-align:left;
    font-family:'Orbitron',sans-serif;font-size:0.75rem;
    letter-spacing:0.5px;border-bottom:1px solid rgba(0,240,255,0.2);
}
.joseph-override-table td{
    padding:8px 14px;color:#e2e8f0;
    border-bottom:1px solid rgba(148,163,184,0.1);
    transition:background 0.15s ease;
}
.joseph-override-table tr:hover td{background:rgba(0,240,255,0.04)}

/* ── Nerd Stats Toggle ────────────────────────────────────── */
.joseph-nerd-stats{
    background:rgba(7,10,19,0.6);border:1px solid rgba(148,163,184,0.15);
    border-radius:8px;padding:14px 18px;margin-top:10px;
    font-family:'JetBrains Mono',monospace;font-size:0.78rem;
    color:#94a3b8;line-height:1.6;
    font-variant-numeric:tabular-nums;
}
.joseph-nerd-stats strong{color:#00f0ff}

/* ── Edge Scoreboard Numbers ──────────────────────────────── */
.joseph-edge-value{
    font-family:'JetBrains Mono',monospace;
    font-variant-numeric:tabular-nums;
    font-weight:700;color:#ff5e00;
}
</style>"""


# ═════════════════════════════════════════════════════════════
# render_broadcast_segment — HTML for one broadcast card
# ═════════════════════════════════════════════════════════════

def render_broadcast_segment(segment_data: dict) -> str:
    """Return complete HTML for one broadcast card.

    Parameters
    ----------
    segment_data : dict
        Keys: title (str), body (str — may contain pre-built HTML;
        callers are responsible for escaping user-facing strings via
        html.escape before embedding them), verdict (str|None),
        player (str|None), prop_line (str|None).
    """
    title = _html.escape(str(segment_data.get("title", "")))
    body = str(segment_data.get("body", ""))
    verdict = segment_data.get("verdict")

    badge_html = ""
    if verdict:
        v_lower = verdict.lower().replace(" ", "_")
        emoji = VERDICT_EMOJIS.get(verdict.upper().replace(" ", "_"), "")
        badge_html = (
            f'<span class="joseph-verdict joseph-verdict-{_html.escape(v_lower)}">'
            f'{_html.escape(emoji)} {_html.escape(verdict)}</span>'
        )

    return (
        f'<div class="joseph-segment">'
        f'<div class="joseph-segment-title">{badge_html}{title}</div>'
        f'<div class="joseph-segment-body">{body}</div>'
        f'</div>'
    )


# ═════════════════════════════════════════════════════════════
# render_dawg_board — top 10 by Dawg Factor
# ═════════════════════════════════════════════════════════════

def render_dawg_board(joseph_results: list) -> None:
    """Render top-10 players by dawg_factor as an HTML leaderboard."""
    scored = []
    for r in joseph_results:
        df = r.get("dawg_factor", 0)
        if isinstance(df, (int, float)):
            scored.append(r)
    scored.sort(key=lambda x: x.get("dawg_factor", 0), reverse=True)
    top10 = scored[:10]

    if not top10:
        st.info("No Dawg Factor data available yet.")
        return

    rows_html = ""
    for idx, r in enumerate(top10, 1):
        name = _html.escape(str(r.get("player", r.get("name", "Unknown"))))
        df_val = r.get("dawg_factor", 0)
        tags_raw = r.get("narrative_tags", r.get("tags", []))
        if isinstance(tags_raw, list):
            tags = ", ".join(_html.escape(str(t)) for t in tags_raw[:4])
        else:
            tags = _html.escape(str(tags_raw))
        archetype = _html.escape(str(r.get("archetype", r.get("comp", {}).get("archetype", "—"))))

        # Color-code dawg factor
        if df_val >= 5:
            df_color = "#ff4444"
        elif df_val >= 3:
            df_color = "#ff5e00"
        elif df_val >= 1:
            df_color = "#eab308"
        else:
            df_color = "#94a3b8"

        rows_html += (
            f'<tr>'
            f'<td style="color:#ff5e00;font-weight:700">#{idx}</td>'
            f'<td><strong>{name}</strong></td>'
            f'<td style="color:{df_color};font-weight:700">{df_val:+.1f}</td>'
            f'<td style="font-size:0.8rem">{tags}</td>'
            f'<td style="color:#00f0ff">{archetype}</td>'
            f'</tr>'
        )

    html = (
        '<table class="joseph-dawg-table">'
        '<thead><tr>'
        '<th>Rank</th><th>Player</th><th>Dawg Factor</th><th>Tags</th><th>Archetype</th>'
        '</tr></thead>'
        f'<tbody>{rows_html}</tbody>'
        '</table>'
    )
    st.markdown(html, unsafe_allow_html=True)


# ═════════════════════════════════════════════════════════════
# render_override_report — if Joseph disagreed with QME
# ═════════════════════════════════════════════════════════════

def render_override_report(joseph_results: list) -> None:
    """Render override table when Joseph disagreed with the QME."""
    overrides = [r for r in joseph_results if r.get("is_override")]
    if not overrides:
        st.markdown(
            '<p style="color:#94a3b8;font-style:italic;">No overrides tonight — '
            'Joseph and the Quantum Matrix Engine agree across the board.</p>',
            unsafe_allow_html=True,
        )
        return

    rows_html = ""
    for r in overrides:
        name = _html.escape(str(r.get("player", r.get("name", "Unknown"))))
        prop = _html.escape(str(r.get("prop", r.get("stat_type", "—"))))
        qme_edge = r.get("qme_edge", r.get("original_edge", 0))
        j_edge = r.get("edge", 0)
        direction = _html.escape(str(r.get("direction", r.get("verdict", "—"))))
        reasoning = _html.escape(str(r.get("override_reason", r.get("rant", "")[:120])))

        rows_html += (
            f'<tr>'
            f'<td><strong>{name}</strong></td>'
            f'<td>{prop}</td>'
            f'<td style="color:#00f0ff">{qme_edge:+.1f}%</td>'
            f'<td style="color:#ff5e00;font-weight:700">{j_edge:+.1f}%</td>'
            f'<td>{direction}</td>'
            f'<td style="font-size:0.8rem;max-width:300px">{reasoning}</td>'
            f'</tr>'
        )

    html = (
        '<table class="joseph-override-table">'
        '<thead><tr>'
        '<th>Player</th><th>Prop</th><th>QME Edge</th><th>Joseph Edge</th>'
        '<th>Direction</th><th>Reasoning</th>'
        '</tr></thead>'
        f'<tbody>{rows_html}</tbody>'
        '</table>'
    )
    st.markdown(html, unsafe_allow_html=True)


# ═════════════════════════════════════════════════════════════
# _build_nerd_stats_html — collapsible stats block
# ═════════════════════════════════════════════════════════════

def _build_nerd_stats_html(result: dict) -> str:
    """Build HTML for the nerd-stats collapsible section."""
    parts = []
    for key in ("edge", "confidence", "dawg_factor", "grade",
                "archetype", "gravity_score", "switchability",
                "qme_edge", "original_edge", "ev_estimate",
                "correlation_score"):
        val = result.get(key)
        if val is not None:
            parts.append(f"<strong>{_html.escape(key)}:</strong> {_html.escape(str(val))}")
    comp = result.get("comp")
    if isinstance(comp, dict) and comp.get("name"):
        parts.append(f"<strong>comp:</strong> {_html.escape(comp['name'])}")
    tags = result.get("narrative_tags", [])
    if tags:
        tag_str = ", ".join(str(t) for t in tags)
        parts.append(f"<strong>tags:</strong> {_html.escape(tag_str)}")
    return '<div class="joseph-nerd-stats">' + "<br>".join(parts) + "</div>" if parts else ""


# ═════════════════════════════════════════════════════════════
# render_joseph_live_desk — the main broadcast desk
# ═════════════════════════════════════════════════════════════

def render_joseph_live_desk(
    analysis_results: list,
    enriched_players: dict,
    teams_data: dict,
    todays_games: list,
) -> None:
    """Render Joseph's full Live Broadcast Desk.

    NON-BLOCKING: uses st.empty() + st.container().
    Processes top 20 results by edge, stores in session state,
    then renders 10+ broadcast segments.
    """
    if not _BRAIN_AVAILABLE:
        st.warning("Joseph's brain module is unavailable.")
        return

    # ── Inject CSS ────────────────────────────────────────────
    st.markdown(render_live_desk_css(), unsafe_allow_html=True)

    avatar_b64 = get_joseph_avatar_b64()
    avatar_img = (
        f'<img src="data:image/png;base64,{avatar_b64}" class="joseph-avatar" '
        f'alt="Joseph M. Smith">'
        if avatar_b64
        else '<div class="joseph-avatar" style="background:#1e293b;display:flex;'
             'align-items:center;justify-content:center;font-size:1.5rem">🎙️</div>'
    )

    desk = st.empty()
    with desk.container():
        st.markdown('<div class="joseph-live-desk">', unsafe_allow_html=True)

        # ─────────────────────────────────────────────────────
        # SEGMENT 0 — Opening Monologue
        # ─────────────────────────────────────────────────────
        opening_text = ""
        try:
            opening_text = joseph_quick_take(analysis_results, teams_data, todays_games)
        except Exception as exc:
            _logger.warning("joseph_quick_take failed: %s", exc)
            opening_text = "Good evening, everybody. The board is loaded tonight and I've got some STRONG takes for you."

        st.markdown(
            f'<div class="joseph-header">'
            f'{avatar_img}'
            f'<div>'
            f'<div class="joseph-header-text">'
            f'<span class="joseph-live-dot"></span>'
            f'🔴 LIVE — Joseph M. Smith\'s Broadcast Desk'
            f'</div>'
            f'<div class="joseph-subtitle">God-Mode Analyst • Live Commentary</div>'
            f'</div></div>',
            unsafe_allow_html=True,
        )
        st.markdown(
            render_broadcast_segment({
                "title": "📢 OPENING MONOLOGUE",
                "body": _html.escape(opening_text),
            }),
            unsafe_allow_html=True,
        )

        # ─────────────────────────────────────────────────────
        # Run Joseph full analysis on top 20 by edge
        # ─────────────────────────────────────────────────────
        joseph_results = st.session_state.get("joseph_results", [])
        if not joseph_results and analysis_results:
            sorted_results = sorted(
                analysis_results,
                key=lambda r: abs(_extract_edge(r)),
                reverse=True,
            )[:20]

            progress_placeholder = st.empty()
            progress_placeholder.markdown(
                '<div class="joseph-typing">'
                '<span></span><span></span><span></span>'
                '</div> <span style="color:#94a3b8;font-size:0.85rem">'
                'Joseph is studying the tape...</span>',
                unsafe_allow_html=True,
            )

            computed = []
            for ar in sorted_results:
                player_name = ar.get("player_name", ar.get("player", ar.get("name", "")))
                player_data = enriched_players.get(str(player_name).lower().strip(), {})
                game_data = {}
                player_team = ar.get("team", player_data.get("team", ""))
                for g in todays_games:
                    if player_team in (g.get("home_team", ""), g.get("away_team", "")):
                        game_data = g
                        break
                try:
                    result = joseph_full_analysis(ar, player_data, game_data, teams_data)
                    result["player"] = player_name
                    result["prop"] = ar.get("stat_type", ar.get("prop", ""))
                    result["line"] = ar.get("line", ar.get("prop_line", ""))
                    result["direction"] = ar.get("direction", "")
                    result["team"] = player_team
                    computed.append(result)
                except Exception as exc:
                    _logger.warning("joseph_full_analysis failed for %s: %s", player_name, exc)

            joseph_results = computed
            st.session_state["joseph_results"] = joseph_results
            progress_placeholder.empty()

        # ─────────────────────────────────────────────────────
        # SEGMENTS 1-5 — Top 5 Picks
        # ─────────────────────────────────────────────────────
        top5 = sorted(
            joseph_results,
            key=lambda x: abs(x.get("edge", 0)),
            reverse=True,
        )[:5]

        if top5:
            st.markdown(
                render_broadcast_segment({
                    "title": "🏆 JOSEPH'S TOP 5 PICKS TONIGHT",
                    "body": "",
                }),
                unsafe_allow_html=True,
            )

        for idx, pick in enumerate(top5, 1):
            verdict = pick.get("verdict", "LEAN")
            emoji = VERDICT_EMOJIS.get(verdict.upper().replace(" ", "_"), "✅")
            player = _html.escape(str(pick.get("player", "Unknown")))
            prop = _html.escape(str(pick.get("prop", "")))
            line = pick.get("line", "")
            direction = _html.escape(str(pick.get("direction", "")))
            rant = pick.get("rant", "")
            edge = pick.get("edge", 0)

            # Mismatch alert
            mismatch_html = ""
            mismatch = pick.get("mismatch")
            if isinstance(mismatch, dict) and mismatch.get("alert"):
                mismatch_html = (
                    f'<div style="color:#ff9e00;font-size:0.85rem;margin-top:6px">'
                    f'⚡ <strong>MISMATCH:</strong> {_html.escape(str(mismatch.get("alert", "")))}'
                    f'</div>'
                )

            # Historical comp
            comp_html = ""
            comp = pick.get("comp")
            if isinstance(comp, dict) and comp.get("name"):
                comp_html = (
                    f'<div style="color:#00f0ff;font-size:0.85rem;margin-top:6px">'
                    f'📜 <strong>Comp:</strong> {_html.escape(str(comp["name"]))}'
                    f'</div>'
                )

            body = (
                f'<strong style="font-size:1.05rem">{player}</strong> — '
                f'{prop} {direction} {_html.escape(str(line))} '
                f'<span style="color:#ff5e00;font-weight:600">(Edge: {edge:+.1f}%)</span>'
                f'<div style="margin-top:8px">{_html.escape(rant)}</div>'
                f'{mismatch_html}{comp_html}'
            )

            st.markdown(
                render_broadcast_segment({
                    "title": f"#{idx} PICK",
                    "body": body,
                    "verdict": verdict,
                }),
                unsafe_allow_html=True,
            )

            # Nerd Stats expander
            nerd_html = _build_nerd_stats_html(pick)
            if nerd_html:
                with st.expander(f"📊 Nerd Stats — {pick.get('player', 'Unknown')}"):
                    st.markdown(nerd_html, unsafe_allow_html=True)

        # ─────────────────────────────────────────────────────
        # SEGMENT 6 — Quick Picks (best 2-leg and 3-leg)
        # ─────────────────────────────────────────────────────
        ticket_html_parts = []
        for legs in (2, 3):
            try:
                ticket = joseph_generate_best_bets(legs, analysis_results, teams_data)
                if ticket and ticket.get("legs"):
                    tname = _html.escape(str(ticket.get("ticket_name", TICKET_NAMES.get(legs, ""))))
                    pitch = _html.escape(str(ticket.get("rant", ticket.get("pitch", ""))))
                    leg_lines = []
                    for leg in ticket.get("legs", []):
                        lp = _html.escape(str(leg.get("player", "")))
                        ls = _html.escape(str(leg.get("stat_type", leg.get("prop", ""))))
                        ld = _html.escape(str(leg.get("direction", "")))
                        leg_lines.append(f"{lp} {ls} {ld}")
                    legs_str = " | ".join(leg_lines)
                    ticket_html_parts.append(
                        f'<div style="margin-bottom:8px">'
                        f'<strong style="color:#ff5e00">{tname}:</strong> {legs_str}'
                        f'<div style="color:#94a3b8;font-size:0.85rem;margin-top:4px">{pitch}</div>'
                        f'</div>'
                    )
            except Exception as exc:
                _logger.warning("joseph_generate_best_bets(%d) failed: %s", legs, exc)

        if ticket_html_parts:
            st.markdown(
                render_broadcast_segment({
                    "title": "🎰 QUICK PICKS — Best Tickets",
                    "body": "".join(ticket_html_parts),
                }),
                unsafe_allow_html=True,
            )

        # ─────────────────────────────────────────────────────
        # SEGMENT 7 — Dawg Board
        # ─────────────────────────────────────────────────────
        if joseph_results:
            st.markdown(
                '<div class="joseph-segment">'
                '<div class="joseph-segment-title">🐕 THE DAWG BOARD</div>'
                '</div>',
                unsafe_allow_html=True,
            )
            render_dawg_board(joseph_results)

        # ─────────────────────────────────────────────────────
        # SEGMENT 8 — Override Report
        # ─────────────────────────────────────────────────────
        overrides = [r for r in joseph_results if r.get("is_override")]
        st.markdown(
            '<div class="joseph-segment">'
            '<div class="joseph-segment-title">⚡ OVERRIDE REPORT</div>'
            '</div>',
            unsafe_allow_html=True,
        )
        render_override_report(joseph_results)

        # ─────────────────────────────────────────────────────
        # SEGMENT 9 — Bet Log
        # ─────────────────────────────────────────────────────
        n_bets = len([r for r in joseph_results
                      if r.get("verdict", "").upper() in ("SMASH", "LEAN")])
        st.markdown(
            render_broadcast_segment({
                "title": "📝 BET LOG",
                "body": (
                    f"I've logged <strong>{n_bets}</strong> bets tonight. "
                    f"Track my record on <strong>The Studio</strong> page!"
                ),
            }),
            unsafe_allow_html=True,
        )

        # ─────────────────────────────────────────────────────
        # SEGMENT 10 — Sign-Off
        # ─────────────────────────────────────────────────────
        closer = ""
        catchphrase = ""
        try:
            _used = set()
            c = _select_fragment(CLOSER_POOL, _used)
            closer = c.get("text", "And that's a WRAP!")
            cp = _select_fragment(CATCHPHRASE_POOL, _used)
            catchphrase = cp.get("text", "")
        except Exception:
            closer = "And that's a WRAP, ladies and gentlemen!"
            catchphrase = "Stay dangerous."

        signoff = f"{_html.escape(closer)}"
        if catchphrase:
            signoff += f' <em style="color:#ff9e00">{_html.escape(catchphrase)}</em>'

        st.markdown(
            render_broadcast_segment({
                "title": "🎙️ SIGN-OFF",
                "body": signoff,
            }),
            unsafe_allow_html=True,
        )

        st.markdown('</div>', unsafe_allow_html=True)
