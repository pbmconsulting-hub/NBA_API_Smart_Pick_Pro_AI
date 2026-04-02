# ============================================================
# FILE: pages/7_🎙️_The_Studio.py
# PURPOSE: Joseph M. Smith's dedicated interactive page — the
#          deep-dive destination for game analysis, player
#          scouting, and bet building.
# CONNECTS TO: engine/joseph_brain.py, engine/joseph_tickets.py,
#              engine/joseph_bets.py, pages/helpers/joseph_live_desk.py
# ============================================================

import streamlit as st
import os
import html as _html
import logging
import random

# ── Styles ───────────────────────────────────────────────────
try:
    from styles.theme import (
        get_global_css,
        get_qds_css,
        get_team_colors,
        get_bet_card_html,
        get_summary_cards_html,
    )
except ImportError:
    def get_global_css():
        return ""
    def get_qds_css():
        return ""
    def get_team_colors(_t):
        return ("#ff5e00", "#1e293b")
    def get_bet_card_html(_b, show_live_status=False):
        return ""
    def get_summary_cards_html(*a, **kw):
        return ""

st.set_page_config(
    page_title="The Studio — Joseph M. Smith",
    page_icon="🎙️",
    layout="wide",
)

st.markdown(get_global_css(), unsafe_allow_html=True)
st.markdown(get_qds_css(), unsafe_allow_html=True)

# ── Sidebar global settings ──────────────────────────────────
try:
    from utils.components import render_global_settings, inject_joseph_floating, render_joseph_hero_banner
    with st.sidebar:
        render_global_settings()
    st.session_state["joseph_page_context"] = "page_studio"
    inject_joseph_floating()
    render_joseph_hero_banner()
except ImportError:
    pass

# ── Premium gate ─────────────────────────────────────────────
try:
    from utils.premium_gate import premium_gate
    if not premium_gate("The Studio"):
        st.stop()
except ImportError:
    pass

# ── Logger ───────────────────────────────────────────────────
try:
    from utils.logger import get_logger
    _logger = get_logger(__name__)
except ImportError:
    _logger = logging.getLogger(__name__)

# ── Engine imports (all safe) ────────────────────────────────
try:
    from engine.joseph_brain import (
        joseph_full_analysis,
        joseph_analyze_game,
        joseph_analyze_player,
        joseph_generate_best_bets,
        joseph_generate_independent_picks,
        joseph_quick_take,
        joseph_commentary,
        _extract_edge,
        _select_fragment,
        CLOSER_POOL,
        CATCHPHRASE_POOL,
        VERDICT_EMOJIS,
        TICKET_NAMES,
    )
    _BRAIN_AVAILABLE = True
except ImportError:
    _BRAIN_AVAILABLE = False
    VERDICT_EMOJIS = {"SMASH": "🔥", "LEAN": "✅", "FADE": "⚠️", "STAY_AWAY": "🚫"}
    TICKET_NAMES = {2: "POWER PLAY", 3: "TRIPLE THREAT", 4: "THE QUAD",
                    5: "HIGH FIVE", 6: "THE FULL SEND"}

try:
    from engine.joseph_tickets import (
        build_joseph_ticket,
        generate_ticket_pitch,
        get_alternative_tickets,
    )
    _TICKETS_AVAILABLE = True
except ImportError:
    _TICKETS_AVAILABLE = False

try:
    from engine.joseph_bets import (
        joseph_get_track_record,
        joseph_get_accuracy_by_verdict,
        joseph_get_override_accuracy,
    )
    _BETS_AVAILABLE = True
except ImportError:
    _BETS_AVAILABLE = False

try:
    from engine.entry_optimizer import PLATFORM_FLEX_TABLES
    _FLEX_TABLES_AVAILABLE = True
except ImportError:
    PLATFORM_FLEX_TABLES = {}
    _FLEX_TABLES_AVAILABLE = False

try:
    from data.data_manager import load_players_data, load_teams_data
except ImportError:
    def load_players_data():
        return []
    def load_teams_data():
        return []

try:
    from pages.helpers.joseph_live_desk import (
        get_joseph_avatar_b64,
        render_live_desk_css,
        render_dawg_board,
        render_override_report,
        render_broadcast_segment,
    )
    _DESK_AVAILABLE = True
except ImportError:
    _DESK_AVAILABLE = False

    def get_joseph_avatar_b64():
        return ""

    def render_live_desk_css():
        return ""

    def render_dawg_board(_r):
        st.info("Dawg Board unavailable.")

    def render_override_report(_r):
        st.info("Override report unavailable.")

    def render_broadcast_segment(seg_dict):
        """Fallback when joseph_live_desk is not available."""
        title = seg_dict.get("title", "")
        body = seg_dict.get("body", "")
        return (
            f'<div style="border-left:3px solid #ff5e00;padding:10px 14px;'
            f'margin:8px 0;background:rgba(255,94,0,0.04);border-radius:4px">'
            f'<div style="color:#ff5e00;font-weight:600;font-size:0.92rem">{title}</div>'
            f'<div style="color:#e2e8f0;font-size:0.88rem;margin-top:4px">{body}</div>'
            f'</div>'
        )


# ── Inject desk CSS ──────────────────────────────────────────
st.markdown(render_live_desk_css(), unsafe_allow_html=True)

# ── Studio-specific supplemental CSS ─────────────────────────
_STUDIO_CSS = """<style>
.studio-hero{
    background:rgba(7,10,19,0.88);
    backdrop-filter:blur(20px);-webkit-backdrop-filter:blur(20px);
    border:1px solid rgba(255,94,0,0.35);
    border-radius:18px;padding:32px 24px;
    text-align:center;margin-bottom:28px;
    position:relative;overflow:hidden;
    box-shadow:0 0 60px rgba(255,94,0,0.08),inset 0 1px 0 rgba(255,158,0,0.1);
}
/* Top broadcast bar */
.studio-hero::before{
    content:'';position:absolute;top:0;left:0;right:0;height:3px;
    background:linear-gradient(90deg,#ff5e00,#ff9e00,#ff5e00);
    background-size:200% 100%;
    animation:studioShimmer 3s linear infinite;
}
/* Bottom broadcast bar */
.studio-hero::after{
    content:'';position:absolute;bottom:0;left:0;right:0;height:3px;
    background:linear-gradient(90deg,#ff9e00,#ff5e00,#ff9e00);
    background-size:200% 100%;
    animation:studioShimmer 3s linear infinite reverse;
}
@keyframes studioShimmer{
    0%{background-position:-200% 0}100%{background-position:200% 0}
}
.studio-hero-title{
    font-family:'Orbitron',sans-serif;font-size:1.8rem;
    color:#ff5e00;font-weight:700;margin:14px 0 6px;
    letter-spacing:1px;
    text-shadow:0 0 20px rgba(255,94,0,0.3),0 0 40px rgba(255,94,0,0.1);
}
.studio-hero-subtitle{
    color:#94a3b8;font-size:0.95rem;
    font-family:'Montserrat',sans-serif;
    letter-spacing:0.3px;
}
.studio-avatar-lg{
    width:120px;height:120px;border-radius:50%;
    border:3px solid #ff5e00;object-fit:cover;
    box-shadow:0 0 24px rgba(255,94,0,0.3),0 0 48px rgba(255,94,0,0.12);
    margin:0 auto;display:block;
    animation:studioAvatarPulse 4s ease-in-out infinite;
}
@keyframes studioAvatarPulse{
    0%,100%{box-shadow:0 0 24px rgba(255,94,0,0.3),0 0 48px rgba(255,94,0,0.12)}
    50%{box-shadow:0 0 32px rgba(255,94,0,0.5),0 0 64px rgba(255,94,0,0.2)}
}
.studio-game-card{
    background:rgba(15,23,42,0.75);
    backdrop-filter:blur(8px);
    -webkit-backdrop-filter:blur(8px);
    border:1px solid rgba(148,163,184,0.18);
    border-left:3px solid rgba(255,94,0,0.4);
    border-radius:12px;padding:16px 20px;
    cursor:pointer;transition:all 0.25s ease;
    margin-bottom:10px;
}
.studio-game-card:hover{
    border-color:rgba(255,94,0,0.5);
    box-shadow:0 4px 24px rgba(255,94,0,0.12);
    transform:translateY(-1px);
}
.studio-ticket-card{
    background:rgba(7,10,19,0.8);
    backdrop-filter:blur(12px);
    -webkit-backdrop-filter:blur(12px);
    border:1px solid rgba(255,94,0,0.3);
    border-radius:14px;padding:22px 26px;
    margin:16px 0;position:relative;overflow:hidden;
    box-shadow:0 0 30px rgba(255,94,0,0.06);
}
.studio-ticket-card::before{
    content:'';position:absolute;top:0;left:0;right:0;height:2px;
    background:linear-gradient(90deg,transparent,#ff5e00,transparent);
    opacity:0.6;
}
.studio-ticket-header{
    font-family:'Orbitron',sans-serif;
    color:#ff5e00;font-size:1.15rem;
    font-weight:700;margin-bottom:12px;
    text-shadow:0 0 8px rgba(255,94,0,0.2);
}
.studio-ticket-leg{
    padding:8px 0;border-bottom:1px solid rgba(148,163,184,0.08);
    color:#e2e8f0;font-size:0.9rem;
    font-family:'Montserrat',sans-serif;
    transition:background 0.15s ease;
}
.studio-ticket-leg:last-child{border-bottom:none}
.studio-ticket-leg:hover{background:rgba(255,94,0,0.03);border-radius:4px}
.studio-section-title{
    font-family:'Orbitron',sans-serif;
    color:#ff5e00;font-size:1.2rem;font-weight:700;
    margin:28px 0 14px;letter-spacing:0.5px;
    text-shadow:0 0 10px rgba(255,94,0,0.15);
    padding-left:12px;
    border-left:3px solid #ff5e00;
}
.studio-metric-row{
    display:flex;gap:14px;flex-wrap:wrap;margin:14px 0;
}
.studio-metric-card{
    flex:1;min-width:140px;
    background:rgba(15,23,42,0.7);
    border:1px solid rgba(255,94,0,0.2);
    border-radius:10px;padding:14px 18px;
    text-align:center;
    transition:all 0.2s ease;
    position:relative;overflow:hidden;
}
.studio-metric-card::before{
    content:'';position:absolute;top:0;left:0;right:0;height:2px;
    background:linear-gradient(90deg,transparent,#ff5e00,transparent);
    opacity:0.4;
}
.studio-metric-card:hover{
    border-color:rgba(255,94,0,0.4);
    box-shadow:0 0 16px rgba(255,94,0,0.1);
    transform:translateY(-1px);
}
.studio-metric-value{
    font-family:'JetBrains Mono',monospace;
    font-variant-numeric:tabular-nums;
    font-size:1.4rem;color:#ff5e00;font-weight:700;
    text-shadow:0 0 8px rgba(255,94,0,0.2);
}
.studio-metric-label{
    color:#94a3b8;font-size:0.78rem;
    margin-top:4px;font-family:'Montserrat',sans-serif;
    text-transform:uppercase;letter-spacing:0.5px;
}
.studio-payout-table{
    width:100%;border-collapse:separate;border-spacing:0;
    font-family:'Montserrat',sans-serif;font-size:0.83rem;
    margin:12px 0;
}
.studio-payout-table th{
    background:rgba(255,94,0,0.1);color:#ff5e00;
    padding:8px 12px;text-align:center;
    font-family:'Orbitron',sans-serif;font-size:0.72rem;
    letter-spacing:0.4px;
    border-bottom:1px solid rgba(255,94,0,0.2);
}
.studio-payout-table td{
    padding:6px 12px;text-align:center;color:#e2e8f0;
    border-bottom:1px solid rgba(148,163,184,0.08);
    font-family:'JetBrains Mono',monospace;
    font-variant-numeric:tabular-nums;
}
.studio-payout-table td.highlight{color:#22c55e;font-weight:700}
</style>"""
st.markdown(_STUDIO_CSS, unsafe_allow_html=True)


# ═════════════════════════════════════════════════════════════
# HERO BANNER
# ═════════════════════════════════════════════════════════════

st.markdown(
    f'<div class="studio-hero">'
    f'<div class="studio-hero-title">🎙️ THE STUDIO — Joseph M. Smith</div>'
    f'<div class="studio-hero-subtitle">'
    f'God-Mode Analyst • Live Commentator • Your Betting Edge'
    f'</div></div>',
    unsafe_allow_html=True,
)

with st.expander("📖 How to Use The Studio", expanded=False):
    st.markdown("""
    ### The Studio — Joseph M. Smith's Analysis Desk
    
    The Studio is your **interactive AI analyst experience** with Joseph M. Smith. Choose from three modes:
    
    **🏀 GAMES TONIGHT**
    - Joseph breaks down every game on tonight's slate
    - Get his takes, overrides, and situational reads
    - Hear the "broadcast segments" like a real sports show
    
    **👤 SCOUT A PLAYER**
    - Deep dive into any specific player's outlook
    - Get archetype analysis, matchup grades, and ceiling/floor projections
    - Joseph shares his honest evaluation and betting take
    
    **🎰 BUILD MY BETS**
    - Let Joseph construct optimal tickets (2-6 legs)
    - He ranks picks by conviction and builds parlays using real analysis
    - See the Dawg Board — his highest-confidence plays
    
    💡 **Pro Tips:**
    - Select your betting platform (PrizePicks, Underdog Fantasy, DraftKings Pick6) for tailored advice
    - Use the Regenerate button to get fresh takes with different narrative angles
    - The Dawg Board at the bottom shows Joseph's strongest picks across all games
    """)


# Helper for small inline avatar (defined early so all modes can use it)
def _avatar_inline(size=48):
    return f'<span style="font-size:{size // 2}px">🎙️</span>'


# ═════════════════════════════════════════════════════════════
# THREE INTERACTIVE MODES
# ═════════════════════════════════════════════════════════════

mode = st.radio(
    "Choose your mode",
    ["🏀 GAMES TONIGHT", "👤 SCOUT A PLAYER", "🎰 BUILD MY BETS"],
    horizontal=True,
    label_visibility="collapsed",
)

# ── Joseph's Platform Preference ──────────────────────────────
# Joseph asks what betting platform the user is using.
# Persists in session state so it's remembered across interactions.
_PLATFORM_OPTIONS = ["PrizePicks", "Underdog Fantasy", "DraftKings Pick6"]
if "joseph_preferred_platform" not in st.session_state:
    st.session_state["joseph_preferred_platform"] = "PrizePicks"

_platform_fallback_icon = '<span style="font-size:16px">🎙️</span>'
avatar_b64 = get_joseph_avatar_b64()
_platform_avatar = _avatar_inline(32) if avatar_b64 else _platform_fallback_icon
st.markdown(
    f'<div style="display:flex;align-items:center;gap:10px;'
    f'margin:8px 0 16px 0;padding:10px 16px;'
    f'background:rgba(255,94,0,0.06);border-left:3px solid #ff5e00;'
    f'border-radius:6px">'
    f'{_platform_avatar}'
    f'<span style="color:#e2e8f0;font-size:0.88rem;font-family:Montserrat,sans-serif">'
    f'What betting app are you using tonight?</span></div>',
    unsafe_allow_html=True,
)
joseph_platform = st.radio(
    "Your betting platform",
    _PLATFORM_OPTIONS,
    index=_PLATFORM_OPTIONS.index(st.session_state["joseph_preferred_platform"]),
    horizontal=True,
    label_visibility="collapsed",
    key="joseph_platform_radio",
)
st.session_state["joseph_preferred_platform"] = joseph_platform

# Shared data
analysis_results = st.session_state.get("analysis_results", [])
teams_data_list = st.session_state.get("teams_data", None)
if teams_data_list is None:
    try:
        teams_data_list = load_teams_data()
    except Exception:
        teams_data_list = []

# Convert list to dict keyed by team abbreviation if needed
if isinstance(teams_data_list, list):
    _teams_dict = {}
    for t in teams_data_list:
        key = t.get("team", t.get("abbreviation", t.get("name", "")))
        if key:
            _teams_dict[key] = t
    teams_data = _teams_dict
elif isinstance(teams_data_list, dict):
    teams_data = teams_data_list
else:
    teams_data = {}


# ─────────────────────────────────────────────────────────────
# MODE 1: GAMES TONIGHT
# ─────────────────────────────────────────────────────────────
if mode == "🏀 GAMES TONIGHT":
    todays_games = st.session_state.get("todays_games", [])

    if not todays_games:
        st.info("📡 Load games on **📡 Live Games** first!")
    else:
        st.markdown(
            '<div class="studio-section-title">Tonight\'s Games</div>',
            unsafe_allow_html=True,
        )

        for g_idx, game in enumerate(todays_games):
            away = game.get("away_team", "AWAY")
            home = game.get("home_team", "HOME")
            spread = game.get("spread", "—")
            total = game.get("total", "—")
            game_time = game.get("time", game.get("commence_time", ""))

            # Team colors
            try:
                h_pri, h_sec = get_team_colors(home)
            except Exception:
                h_pri, h_sec = "#ff5e00", "#1e293b"

            label = f"{away} @ {home} | Spread: {spread} | Total: {total}"
            if game_time:
                label += f" | {game_time}"

            if st.button(label, key=f"studio_game_{g_idx}", use_container_width=True):
                if not _BRAIN_AVAILABLE:
                    st.warning("Joseph's brain module is not available.")
                else:
                    with st.spinner("Joseph is analyzing this game..."):
                        try:
                            result = joseph_analyze_game(game, teams_data, analysis_results)
                        except Exception as exc:
                            _logger.warning("joseph_analyze_game failed: %s", exc)
                            result = {}

                    if result:
                        # Avatar + commentary
                        try:
                            commentary = joseph_commentary(
                                [result], "analysis_results"
                            )
                        except Exception:
                            commentary = ""

                        st.markdown(
                            f'<div style="display:flex;align-items:flex-start;gap:14px;'
                            f'margin:16px 0">'
                            f'{_avatar_inline(48)}'
                            f'<div style="color:#e2e8f0;font-size:0.92rem;'
                            f'line-height:1.6">{_html.escape(commentary)}</div>'
                            f'</div>',
                            unsafe_allow_html=True,
                        )

                        # Game narrative
                        narrative = result.get("game_narrative", "")
                        if narrative:
                            st.markdown(
                                render_broadcast_segment({
                                    "title": "📖 GAME NARRATIVE",
                                    "body": _html.escape(narrative),
                                }),
                                unsafe_allow_html=True,
                            )

                        # Pace take
                        pace = result.get("pace_take", "")
                        if pace:
                            st.markdown(
                                render_broadcast_segment({
                                    "title": "⚡ PACE TAKE",
                                    "body": _html.escape(pace),
                                }),
                                unsafe_allow_html=True,
                            )

                        # Scheme analysis
                        scheme = result.get("scheme_analysis", "")
                        if scheme:
                            st.markdown(
                                render_broadcast_segment({
                                    "title": "🛡️ SCHEME ANALYSIS",
                                    "body": _html.escape(scheme),
                                }),
                                unsafe_allow_html=True,
                            )

                        # Key matchup
                        matchup = result.get("key_matchup", result.get("matchup", ""))
                        if matchup:
                            st.markdown(
                                render_broadcast_segment({
                                    "title": "🔑 KEY MATCHUP",
                                    "body": _html.escape(str(matchup)),
                                }),
                                unsafe_allow_html=True,
                            )

                        # Joseph's top 3 bets for this game
                        best_props = result.get("best_props", [])[:3]
                        if best_props:
                            st.markdown(
                                '<div class="joseph-segment-title">'
                                '🎯 Joseph\'s Top 3 Bets for this Game'
                                '</div>',
                                unsafe_allow_html=True,
                            )
                            for bp in best_props:
                                v = bp.get("verdict", "LEAN")
                                emoji = VERDICT_EMOJIS.get(
                                    v.upper().replace(" ", "_"), "✅"
                                )
                                bp_name = _html.escape(str(bp.get("player_name", bp.get("player", ""))))
                                bp_rant = _html.escape(str(bp.get("rant", "")))
                                st.markdown(
                                    render_broadcast_segment({
                                        "title": f"{bp_name}",
                                        "body": bp_rant,
                                        "verdict": v,
                                    }),
                                    unsafe_allow_html=True,
                                )

                        # Game total and spread opinions
                        total_opinion = result.get("total_opinion", result.get("joseph_game_total_take", ""))
                        if total_opinion:
                            st.markdown(
                                render_broadcast_segment({
                                    "title": "📊 TOTAL OPINION",
                                    "body": _html.escape(total_opinion),
                                }),
                                unsafe_allow_html=True,
                            )

                        spread_opinion = result.get("spread_opinion", result.get("joseph_spread_take", ""))
                        if spread_opinion:
                            st.markdown(
                                render_broadcast_segment({
                                    "title": "📏 SPREAD OPINION",
                                    "body": _html.escape(spread_opinion),
                                }),
                                unsafe_allow_html=True,
                            )

                        # Risk warning
                        risk = result.get("blowout_risk", result.get("risk_warning", ""))
                        if risk:
                            st.markdown(
                                f'<div style="color:#eab308;font-size:0.88rem;'
                                f'margin:10px 0;padding:10px 14px;'
                                f'border-left:3px solid #eab308;'
                                f'background:rgba(234,179,8,0.06);'
                                f'border-radius:4px">'
                                f'⚠️ <strong>Risk Warning:</strong> '
                                f'{_html.escape(str(risk))}'
                                f'</div>',
                                unsafe_allow_html=True,
                            )

                        # Nerd stats
                        with st.expander("📊 Nerd Stats"):
                            nerd_keys = [
                                "pace_take", "scheme_analysis", "blowout_risk",
                                "game_narrative", "total_opinion", "joseph_game_total_take",
                                "spread_opinion", "joseph_spread_take",
                                "betting_angle", "risk_warning",
                            ]
                            for nk in nerd_keys:
                                nv = result.get(nk)
                                if nv:
                                    st.markdown(
                                        f"**{nk}:** {_html.escape(str(nv)[:500])}"
                                    )
                    else:
                        st.info("Joseph couldn't analyze this game — data may be limited.")


# ─────────────────────────────────────────────────────────────
# MODE 2: SCOUT A PLAYER
# ─────────────────────────────────────────────────────────────
elif mode == "👤 SCOUT A PLAYER":
    # Build player list from analysis results or loaded data
    player_options = []
    _seen = set()
    for ar in analysis_results:
        name = ar.get("player_name", ar.get("player", ar.get("name", "")))
        team = ar.get("team", "")
        label = f"{name} ({team})" if team else name
        if label and label not in _seen:
            _seen.add(label)
            player_options.append((label, ar))

    if not player_options:
        try:
            players_data = load_players_data()
            for pd in players_data:
                name = pd.get("name", pd.get("player", ""))
                team = pd.get("team", "")
                label = f"{name} ({team})" if team else name
                if label and label not in _seen:
                    _seen.add(label)
                    player_options.append((label, pd))
        except Exception:
            pass

    if not player_options:
        st.info("No players available. Run **⚡ Neural Analysis** or check data.")
    else:
        # Sort by team, then name
        player_options.sort(key=lambda x: x[0])

        selected_label = st.selectbox(
            "Select a player to scout",
            [p[0] for p in player_options],
        )

        if selected_label:
            player_data = next(
                (p[1] for p in player_options if p[0] == selected_label), {}
            )

            if not _BRAIN_AVAILABLE:
                st.warning("Joseph's brain module is not available.")
            else:
                todays_games = st.session_state.get("todays_games", [])
                with st.spinner("Joseph is scouting this player..."):
                    try:
                        result = joseph_analyze_player(
                            player_data, todays_games, teams_data, analysis_results
                        )
                    except Exception as exc:
                        _logger.warning("joseph_analyze_player failed: %s", exc)
                        result = {}

                if result:
                    # Avatar + scouting report
                    report = result.get("scouting_report", "")
                    st.markdown(
                        f'<div style="display:flex;align-items:flex-start;gap:14px;'
                        f'margin:16px 0">'
                        f'{_avatar_inline(48)}'
                        f'<div style="color:#e2e8f0;font-size:0.92rem;'
                        f'line-height:1.6">{_html.escape(str(report))}</div>'
                        f'</div>',
                        unsafe_allow_html=True,
                    )

                    # Archetype badge + letter grade
                    archetype = result.get("archetype", "")
                    grade = result.get("grade", "")
                    if archetype or grade:
                        badge_parts = []
                        if archetype:
                            badge_parts.append(
                                f'<span style="background:rgba(255,94,0,0.15);'
                                f'color:#ff5e00;padding:4px 12px;border-radius:6px;'
                                f'font-family:\'Orbitron\',sans-serif;font-size:0.8rem;'
                                f'font-weight:600">{_html.escape(str(archetype))}</span>'
                            )
                        if grade:
                            grade_color = "#22c55e" if grade in ("A+", "A", "A-") else (
                                "#eab308" if grade.startswith("B") else "#94a3b8"
                            )
                            badge_parts.append(
                                f'<span style="background:rgba(15,23,42,0.8);'
                                f'color:{grade_color};padding:4px 14px;'
                                f'border-radius:6px;font-family:\'Orbitron\',sans-serif;'
                                f'font-size:1rem;font-weight:700;'
                                f'border:1px solid {grade_color}">'
                                f'{_html.escape(str(grade))}</span>'
                            )
                        st.markdown(
                            f'<div style="display:flex;gap:10px;margin:12px 0">'
                            + "".join(badge_parts)
                            + "</div>",
                            unsafe_allow_html=True,
                        )

                    # Tonight's matchup take
                    matchup_take = result.get("matchup_take", result.get("tonight_matchup", ""))
                    if matchup_take:
                        st.markdown(
                            render_broadcast_segment({
                                "title": "🎯 TONIGHT'S MATCHUP",
                                "body": _html.escape(str(matchup_take)),
                            }),
                            unsafe_allow_html=True,
                        )

                    # Best prop with full rant
                    best_prop = result.get("best_prop", {})
                    if isinstance(best_prop, dict) and best_prop:
                        v = best_prop.get("verdict", "LEAN")
                        rant = best_prop.get("rant", "")
                        st.markdown(
                            render_broadcast_segment({
                                "title": "💰 BEST PROP",
                                "body": _html.escape(str(rant)),
                                "verdict": v,
                            }),
                            unsafe_allow_html=True,
                        )

                    # Alternative props
                    alt_props = result.get("alternative_props", result.get("alt_props", []))
                    if alt_props:
                        st.markdown(
                            '<div class="joseph-segment-title">'
                            '📋 Alternative Props</div>',
                            unsafe_allow_html=True,
                        )
                        for ap in alt_props[:3]:
                            if isinstance(ap, dict):
                                ap_text = ap.get("summary", ap.get("rant", ""))
                                if not ap_text:
                                    # Format key fields instead of dumping the raw dict
                                    _ap_player = ap.get("player_name", ap.get("player", ""))
                                    _ap_stat = ap.get("stat_type", ap.get("stat", ""))
                                    _ap_dir = ap.get("direction", "")
                                    _ap_line = ap.get("prop_line", ap.get("line", ""))
                                    _ap_verdict = ap.get("verdict", "")
                                    ap_text = " ".join(
                                        p for p in [_ap_player, _ap_stat, _ap_dir, str(_ap_line), _ap_verdict] if p
                                    ) or "Alternative prop"
                            else:
                                ap_text = str(ap)
                            st.markdown(
                                f'<div style="color:#e2e8f0;font-size:0.88rem;'
                                f'padding:6px 0;border-bottom:1px solid '
                                f'rgba(148,163,184,0.08)">'
                                f'{_html.escape(str(ap_text))}</div>',
                                unsafe_allow_html=True,
                            )

                    # Historical comp
                    comp = result.get("comp", result.get("historical_comp", {}))
                    if isinstance(comp, dict) and comp.get("name"):
                        st.markdown(
                            render_broadcast_segment({
                                "title": "📜 HISTORICAL COMP",
                                "body": _html.escape(str(comp.get("name", "")))
                                + (" — " + _html.escape(str(comp.get("narrative", "")))
                                   if comp.get("narrative") else ""),
                            }),
                            unsafe_allow_html=True,
                        )

                    # Fun fact
                    fun_fact = result.get("fun_fact", "")
                    if fun_fact:
                        st.markdown(
                            f'<div style="color:#ff9e00;font-size:0.88rem;'
                            f'margin:10px 0;padding:10px 14px;'
                            f'border-left:3px solid #ff9e00;'
                            f'background:rgba(255,158,0,0.06);'
                            f'border-radius:4px">🎲 '
                            f'{_html.escape(str(fun_fact))}</div>',
                            unsafe_allow_html=True,
                        )

                    # Risk factors
                    risks = result.get("risk_factors", result.get("risks", []))
                    if risks:
                        st.markdown(
                            '<div class="joseph-segment-title">⚠️ Risk Factors</div>',
                            unsafe_allow_html=True,
                        )
                        for rf in risks:
                            st.markdown(
                                f'<div style="color:#eab308;font-size:0.85rem;'
                                f'padding:4px 0">• {_html.escape(str(rf))}</div>',
                                unsafe_allow_html=True,
                            )

                    # Nerd Stats expander
                    with st.expander("📊 Nerd Stats"):
                        display_keys = [
                            "gravity", "trend", "grade", "archetype",
                            "scouting_report", "matchup_take",
                        ]
                        tags = result.get("narrative_tags", [])
                        if tags:
                            st.markdown(
                                f"**narrative_tags:** "
                                f"{_html.escape(', '.join(str(t) for t in tags))}"
                            )
                        for dk in display_keys:
                            dv = result.get(dk)
                            if dv is not None:
                                st.markdown(
                                    f"**{dk}:** {_html.escape(str(dv)[:500])}"
                                )
                else:
                    st.info(
                        "Joseph couldn't scout this player — data may be limited."
                    )


# ─────────────────────────────────────────────────────────────
# MODE 3: BUILD MY BETS
# ─────────────────────────────────────────────────────────────
elif mode == "🎰 BUILD MY BETS":
    if not _BRAIN_AVAILABLE:
        st.warning("Joseph's brain module is not available.")
    else:
        # Use analysis_results if available, otherwise try platform props
        _bets_data = analysis_results
        if not _bets_data:
            # Fallback: use platform props from session state
            _platform_props = st.session_state.get("platform_props", [])
            if _platform_props:
                _bets_data = _platform_props
                st.info(
                    f"📡 Using **{len(_platform_props)}** live props from "
                    f"betting platforms. For full analysis, run **⚡ Neural Analysis** first."
                )
            else:
                st.info(
                    "📡 Load games on **📡 Live Games** and get live props, "
                    "or run **⚡ Neural Analysis** to populate data for bet building!"
                )

        if _bets_data:
            st.markdown(
                '<div class="studio-section-title">Choose Your Entry Size</div>',
                unsafe_allow_html=True,
            )

            # 5 large columns with leg-count buttons
            col1, col2, col3, col4, col5 = st.columns(5)
            with col1:
                btn2 = st.button("2️⃣ POWER PLAY", use_container_width=True)
            with col2:
                btn3 = st.button("3️⃣ TRIPLE THREAT", use_container_width=True)
            with col3:
                btn4 = st.button("4️⃣ THE QUAD", use_container_width=True)
            with col4:
                btn5 = st.button("5️⃣ HIGH FIVE", use_container_width=True)
            with col5:
                btn6 = st.button("6️⃣ THE FULL SEND", use_container_width=True)

            # Use Joseph's platform preference (from top selector)
            platform = joseph_platform

            st.markdown(
                f'<div style="color:#94a3b8;font-size:0.82rem;margin:4px 0 8px 0">'
                f'Building bets for <strong style="color:#ff5e00">{platform}</strong></div>',
                unsafe_allow_html=True,
            )

            # Determine which button was pressed
            selected_legs = None
            if btn2:
                selected_legs = 2
            elif btn3:
                selected_legs = 3
            elif btn4:
                selected_legs = 4
            elif btn5:
                selected_legs = 5
            elif btn6:
                selected_legs = 6

            # Regenerate key
            if "studio_regen_seed" not in st.session_state:
                st.session_state["studio_regen_seed"] = 0

            if selected_legs:
                # Set random seed for regeneration variety
                random.seed(st.session_state["studio_regen_seed"] + (selected_legs * 1000))

                with st.spinner(
                    f"Joseph is building your {TICKET_NAMES.get(selected_legs, '')}..."
                ):
                    try:
                        ticket_result = joseph_generate_best_bets(
                            selected_legs, _bets_data, teams_data
                        )
                    except Exception as exc:
                        _logger.warning("joseph_generate_best_bets failed: %s", exc)
                        ticket_result = {}

                if ticket_result and ticket_result.get("legs"):
                    # Inline reaction
                    try:
                        reaction = joseph_commentary([ticket_result], "ticket_generated")
                    except Exception:
                        reaction = ""

                    if reaction:
                        st.markdown(
                            f'<div style="display:flex;align-items:flex-start;gap:12px;'
                            f'margin:14px 0">'
                            f'{_avatar_inline(40)}'
                            f'<div style="color:#e2e8f0;font-size:0.9rem;'
                            f'line-height:1.5;font-style:italic">'
                            f'{_html.escape(reaction)}</div></div>',
                            unsafe_allow_html=True,
                        )

                    # Ticket card
                    ticket_name = ticket_result.get(
                        "ticket_name", TICKET_NAMES.get(selected_legs, "TICKET")
                    )
                    pitch = ticket_result.get("rant", ticket_result.get("pitch", ""))

                    legs_html = ""
                    for leg in ticket_result.get("legs", []):
                        l_player = _html.escape(str(leg.get("player_name", leg.get("player", ""))))
                        l_dir = _html.escape(str(leg.get("direction", "")))
                        l_line = leg.get("prop_line", leg.get("line", ""))
                        l_stat = _html.escape(
                            str(leg.get("stat_type", leg.get("prop", "")))
                        )
                        l_verdict = leg.get("verdict", "LEAN")
                        l_emoji = VERDICT_EMOJIS.get(
                            l_verdict.upper().replace(" ", "_"), "✅"
                        )
                        l_oneliner = _html.escape(
                            str(leg.get("one_liner", leg.get("rant", "")[:80]))
                        )

                        legs_html += (
                            f'<div class="studio-ticket-leg">'
                            f'{_html.escape(l_emoji)} '
                            f'<strong>{l_player}</strong> '
                            f'{l_stat} {l_dir} {_html.escape(str(l_line))} '
                            f'<span style="color:#94a3b8;font-size:0.82rem">'
                            f'— {l_oneliner}</span>'
                            f'</div>'
                        )

                    # Combined stats
                    combined_prob = ticket_result.get("combined_probability",
                                                      ticket_result.get("total_ev", 0))
                    ev = ticket_result.get("expected_value",
                                           ticket_result.get("total_ev", 0))
                    synergy = ticket_result.get("synergy_score",
                                                ticket_result.get("correlation_score", 0))

                    st.markdown(
                        f'<div class="studio-ticket-card">'
                        f'<div class="studio-ticket-header">'
                        f'🎫 {_html.escape(str(ticket_name))}</div>'
                        f'<div style="color:#94a3b8;font-size:0.88rem;margin-bottom:14px">'
                        f'{_html.escape(str(pitch))}</div>'
                        f'{legs_html}'
                        f'<div style="display:flex;gap:20px;margin-top:14px;'
                        f'padding-top:12px;border-top:1px solid rgba(148,163,184,0.12)">'
                        f'<div><span style="color:#94a3b8;font-size:0.78rem">'
                        f'Combined Prob</span><br>'
                        f'<span style="color:#22c55e;font-family:\'JetBrains Mono\','
                        f'monospace;font-weight:600">'
                        f'{combined_prob:.1%}</span></div>'
                        f'<div><span style="color:#94a3b8;font-size:0.78rem">EV</span><br>'
                        f'<span style="color:#ff5e00;font-family:\'JetBrains Mono\','
                        f'monospace;font-weight:600">'
                        f'{ev:+.2f}</span></div>'
                        f'<div><span style="color:#94a3b8;font-size:0.78rem">'
                        f'Synergy</span><br>'
                        f'<span style="color:#00f0ff;font-family:\'JetBrains Mono\','
                        f'monospace;font-weight:600">'
                        f'{synergy:.2f}</span></div>'
                        f'</div></div>',
                        unsafe_allow_html=True,
                    )

                    # "Why These Connect:" narrative
                    why = ticket_result.get("why_these_legs",
                                            ticket_result.get("why_these_connect", ""))
                    if why:
                        st.markdown(
                            render_broadcast_segment({
                                "title": "🔗 Why These Connect",
                                "body": _html.escape(str(why)),
                            }),
                            unsafe_allow_html=True,
                        )

                    # Risk disclaimer
                    disclaimer = ticket_result.get(
                        "risk_disclaimer",
                        "All picks carry risk. Bet responsibly. Past performance does not guarantee future results.",
                    )
                    st.markdown(
                        f'<div style="color:#94a3b8;font-size:0.78rem;'
                        f'margin:10px 0;font-style:italic">'
                        f'⚠️ {_html.escape(str(disclaimer))}</div>',
                        unsafe_allow_html=True,
                    )

                    # Nerd Stats expander
                    nerd = ticket_result.get("nerd_stats", {})
                    with st.expander("📊 Nerd Stats"):
                        if isinstance(nerd, dict) and nerd:
                            for nk, nv in nerd.items():
                                st.markdown(
                                    f"**{_html.escape(str(nk))}:** "
                                    f"{_html.escape(str(nv)[:500])}"
                                )
                        else:
                            for key in ("combined_probability", "expected_value",
                                        "synergy_score", "correlation_score",
                                        "total_ev", "joseph_confidence"):
                                val = ticket_result.get(key)
                                if val is not None:
                                    st.markdown(
                                        f"**{key}:** {_html.escape(str(val))}"
                                    )

                    # Regenerate button
                    if st.button("🔄 Regenerate", key="studio_regen"):
                        st.session_state["studio_regen_seed"] += 1
                        st.rerun()

                    # View All Options expander
                    if _TICKETS_AVAILABLE:
                        with st.expander("📋 View All Options — Top 3 Alternatives"):
                            joseph_results = st.session_state.get("joseph_results", [])
                            try:
                                alts = get_alternative_tickets(
                                    selected_legs, joseph_results, top_n=3
                                )
                            except Exception as exc:
                                _logger.warning("get_alternative_tickets failed: %s", exc)
                                alts = []

                            if alts:
                                for a_idx, alt in enumerate(alts, 1):
                                    alt_name = alt.get("ticket_name", f"Alt #{a_idx}")
                                    alt_pitch = alt.get("pitch", "")
                                    alt_edge = alt.get("total_edge", 0)
                                    st.markdown(
                                        f"**{a_idx}. {_html.escape(str(alt_name))}** "
                                        f"(Edge: {alt_edge:+.1f}%)"
                                    )
                                    if alt_pitch:
                                        st.markdown(
                                            f'<div style="color:#94a3b8;font-size:0.85rem;'
                                            f'margin-bottom:8px">'
                                            f'{_html.escape(str(alt_pitch))}</div>',
                                            unsafe_allow_html=True,
                                        )
                                    alt_legs = alt.get("legs", [])
                                    for al in alt_legs:
                                        if isinstance(al, dict):
                                            al_name = _html.escape(
                                                str(al.get("player_name", al.get("player", "")))
                                            )
                                            al_stat = _html.escape(
                                                str(al.get("stat_type", al.get("prop", "")))
                                            )
                                            al_dir = _html.escape(
                                                str(al.get("direction", ""))
                                            )
                                            st.markdown(
                                                f"  • {al_name} {al_stat} {al_dir}"
                                            )
                                        else:
                                            # Alternative legs may be plain strings
                                            st.markdown(
                                                f"  • {_html.escape(str(al))}"
                                            )
                            else:
                                st.info("No alternative tickets available.")

                    # Payout table
                    if _FLEX_TABLES_AVAILABLE and platform in PLATFORM_FLEX_TABLES:
                        payout_table = PLATFORM_FLEX_TABLES[platform]
                        leg_payouts = payout_table.get(selected_legs, {})
                        if leg_payouts:
                            header_cells = "".join(
                                f"<th>{k} Correct</th>"
                                for k in sorted(leg_payouts.keys(), reverse=True)
                            )
                            data_cells = "".join(
                                f'<td class="{"highlight" if v > 0 else ""}">'
                                f'{v:.1f}x</td>'
                                for k, v in sorted(
                                    leg_payouts.items(), reverse=True
                                )
                            )
                            st.markdown(
                                f'<div style="margin-top:16px">'
                                f'<div style="color:#ff5e00;font-size:0.85rem;'
                                f'font-family:\'Orbitron\',sans-serif;margin-bottom:6px">'
                                f'{_html.escape(platform)} Payouts — '
                                f'{selected_legs}-Leg Entry</div>'
                                f'<table class="studio-payout-table">'
                                f'<thead><tr>{header_cells}</tr></thead>'
                                f'<tbody><tr>{data_cells}</tr></tbody>'
                                f'</table></div>',
                                unsafe_allow_html=True,
                            )
                else:
                    st.warning(
                        "Joseph couldn't build a ticket — not enough qualifying picks."
                    )


# ═════════════════════════════════════════════════════════════
# BELOW INTERACTIVE AREA
# ═════════════════════════════════════════════════════════════
st.divider()

# ── Dawg Board ───────────────────────────────────────────────
st.markdown(
    '<div class="studio-section-title">🐕 THE DAWG BOARD</div>',
    unsafe_allow_html=True,
)
joseph_results = st.session_state.get("joseph_results", [])

# Auto-generate Joseph's independent picks when none exist yet
if not joseph_results and _BRAIN_AVAILABLE:
    _gen_source = analysis_results
    _from_props = False
    if not _gen_source:
        _gen_source = st.session_state.get("platform_props", [])
        _from_props = True

    if _gen_source:
        with st.spinner("🎙️ Joseph is scouting the board..."):
            try:
                _players_raw = load_players_data()
                _p_lookup = {
                    str(p.get("name", p.get("player_name", ""))).lower().strip(): p
                    for p in _players_raw if p
                }
                _games = st.session_state.get("todays_games", [])

                if _from_props:
                    joseph_results = joseph_generate_independent_picks(
                        _gen_source, _p_lookup, _games, teams_data,
                    )
                else:
                    _sorted_ar = sorted(
                        _gen_source,
                        key=lambda r: abs(_extract_edge(r)),
                        reverse=True,
                    )[:20]
                    joseph_results = []
                    for _ar in _sorted_ar:
                        _pn = _ar.get(
                            "player_name",
                            _ar.get("player", _ar.get("name", "")),
                        )
                        _pd = _p_lookup.get(str(_pn).lower().strip(), {})
                        _gd = {}
                        _pt = _ar.get(
                            "team", _ar.get("player_team", _pd.get("team", "")),
                        )
                        for _g in _games:
                            if _pt in (
                                _g.get("home_team", ""),
                                _g.get("away_team", ""),
                            ):
                                _gd = _g
                                break
                        try:
                            _res = joseph_full_analysis(
                                _ar, _pd, _gd, teams_data,
                            )
                            _res["player"] = _pn
                            _res["prop"] = _ar.get("stat_type", "")
                            _res["line"] = _ar.get(
                                "line", _ar.get("prop_line", ""),
                            )
                            _res["direction"] = _ar.get("direction", "")
                            _res["team"] = _pt
                            joseph_results.append(_res)
                        except Exception:
                            pass

                if joseph_results:
                    st.session_state["joseph_results"] = joseph_results
            except Exception as _dawg_err:
                _logger.warning(
                    "Auto-generation of Joseph's picks failed: %s", _dawg_err,
                )

if joseph_results:
    render_dawg_board(joseph_results)
else:
    st.info(
        "📡 Load games on **📡 Live Games** and get live props, "
        "or run **⚡ Neural Analysis** to populate Joseph's Dawg Board!"
    )

# ── Override Report ──────────────────────────────────────────
st.markdown(
    '<div class="studio-section-title">⚡ OVERRIDE REPORT</div>',
    unsafe_allow_html=True,
)
if joseph_results:
    render_override_report(joseph_results)
else:
    st.markdown(
        '<p style="color:#94a3b8;font-style:italic">No overrides tonight.</p>',
        unsafe_allow_html=True,
    )

# ── Joseph's Track Record ───────────────────────────────────
st.markdown(
    '<div class="studio-section-title">📊 JOSEPH\'S TRACK RECORD</div>',
    unsafe_allow_html=True,
)
if _BETS_AVAILABLE:
    try:
        track = joseph_get_track_record()
    except Exception as exc:
        _logger.warning("joseph_get_track_record failed: %s", exc)
        track = {}

    total = track.get("total", 0)
    wins = track.get("wins", 0)
    losses = track.get("losses", 0)
    pending = track.get("pending", 0)
    win_rate = track.get("win_rate", 0)
    streak = track.get("streak", 0)

    # Accuracy by verdict
    try:
        by_verdict = joseph_get_accuracy_by_verdict()
    except Exception:
        by_verdict = {}

    smash_pct = by_verdict.get("SMASH", {}).get("pct", 0)
    lean_pct = by_verdict.get("LEAN", {}).get("pct", 0)

    st.markdown(
        f'<div class="studio-metric-row">'
        f'<div class="studio-metric-card">'
        f'<div class="studio-metric-value">{total}</div>'
        f'<div class="studio-metric-label">Total Bets</div></div>'
        f'<div class="studio-metric-card">'
        f'<div class="studio-metric-value">{win_rate:.1%}</div>'
        f'<div class="studio-metric-label">Win Rate</div></div>'
        f'<div class="studio-metric-card">'
        f'<div class="studio-metric-value">{smash_pct:.1%}</div>'
        f'<div class="studio-metric-label">SMASH Accuracy</div></div>'
        f'<div class="studio-metric-card">'
        f'<div class="studio-metric-value">{lean_pct:.1%}</div>'
        f'<div class="studio-metric-label">LEAN Accuracy</div></div>'
        f'</div>',
        unsafe_allow_html=True,
    )

    # Override accuracy
    try:
        override_acc = joseph_get_override_accuracy()
        if isinstance(override_acc, dict) and override_acc.get("overrides_total", 0) > 0:
            st.markdown(
                f'<div style="color:#00f0ff;font-size:0.88rem;margin:8px 0">'
                f'🔄 <strong>Override Accuracy:</strong> '
                f'{override_acc.get("override_accuracy", 0):.1%} '
                f'({override_acc.get("overrides_correct", 0)}/'
                f'{override_acc.get("overrides_total", 0)} correct)</div>',
                unsafe_allow_html=True,
            )
    except Exception:
        pass
else:
    st.info("Bet tracking module not available.")

# ── Joseph's Bet History ─────────────────────────────────────
st.markdown(
    '<div class="studio-section-title">📜 JOSEPH\'S BET HISTORY</div>',
    unsafe_allow_html=True,
)

_bets_loaded = False
if _BETS_AVAILABLE:
    try:
        from tracking.bet_tracker import load_all_bets as _load_all_bets
        all_bets = _load_all_bets()
        # Filter for Joseph's bets
        joseph_bets = [
            b for b in all_bets
            if b.get("source", "").lower() == "joseph"
            or "joseph" in b.get("notes", "").lower()
            or b.get("analyst", "").lower() == "joseph"
        ]
        _bets_loaded = True
    except ImportError:
        try:
            from tracking.database import load_all_bets as _load_all_bets
            all_bets = _load_all_bets()
            joseph_bets = [
                b for b in all_bets
                if b.get("source", "").lower() == "joseph"
                or "joseph" in b.get("notes", "").lower()
                or b.get("analyst", "").lower() == "joseph"
            ]
            _bets_loaded = True
        except ImportError:
            joseph_bets = []
    except Exception as exc:
        _logger.warning("Failed to load bet history: %s", exc)
        joseph_bets = []

if _bets_loaded and joseph_bets:
    for bet in joseph_bets[:20]:
        card_html = get_bet_card_html(bet)
        if card_html:
            st.markdown(card_html, unsafe_allow_html=True)
        else:
            player_name = _html.escape(str(bet.get("player", "Unknown")))
            stat = _html.escape(str(bet.get("stat_type", "")))
            direction = _html.escape(str(bet.get("direction", "")))
            result_val = _html.escape(str(bet.get("result", "pending")))
            st.markdown(
                f'<div class="joseph-segment">'
                f'<strong>{player_name}</strong> — {stat} {direction} '
                f'| Result: {result_val}'
                f'</div>',
                unsafe_allow_html=True,
            )
elif _bets_loaded:
    st.info("No bets logged by Joseph yet. Run analysis to start tracking!")
else:
    st.info("Bet history module not available.")


# ═════════════════════════════════════════════════════════════
# SIGN-OFF
# ═════════════════════════════════════════════════════════════
st.divider()
closer_text = "That's a WRAP, everybody."
catchphrase_text = ""
if _BRAIN_AVAILABLE:
    try:
        _used = set()
        c = _select_fragment(CLOSER_POOL, _used)
        closer_text = c.get("text", closer_text)
        cp = _select_fragment(CATCHPHRASE_POOL, _used)
        catchphrase_text = cp.get("text", "")
    except Exception:
        pass

signoff = _html.escape(closer_text)
if catchphrase_text:
    signoff += f' <em style="color:#ff9e00">{_html.escape(catchphrase_text)}</em>'

st.markdown(
    f'<div style="text-align:center;margin:24px 0">'
    f'{_avatar_inline(64)}'
    f'<div style="color:#ff5e00;font-family:\'Orbitron\',sans-serif;'
    f'font-size:1rem;margin-top:12px">{signoff}</div>'
    f'<div style="color:#94a3b8;font-size:0.8rem;margin-top:4px">'
    f'— Joseph M. Smith, The Studio</div>'
    f'</div>',
    unsafe_allow_html=True,
)
