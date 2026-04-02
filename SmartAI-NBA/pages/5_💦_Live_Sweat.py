# ============================================================
# FILE: pages/5_💦_Live_Sweat.py
# PURPOSE: Live Sweat in-game dashboard.  Tracks locked pre-game
#          bets against real-time NBA box scores with auto-refresh,
#          glassmorphic cards, neon progress bars, and Joseph M.
#          Smith's live vibe-check reactions.
# DESIGN:  API Firewall (120 s cache) + streamlit-autorefresh
# ============================================================

import streamlit as st
import datetime
import logging

try:
    from utils.logger import get_logger
    _logger = get_logger(__name__)
except ImportError:
    _logger = logging.getLogger(__name__)

# ── Page Configuration (MUST be first Streamlit call) ─────────

st.set_page_config(
    page_title="Live Sweat — SmartBetPro NBA",
    page_icon="💦",
    layout="wide",
)

# ── Global CSS ────────────────────────────────────────────────

from styles.theme import get_global_css
st.markdown(get_global_css(), unsafe_allow_html=True)

# ── Live Sweat CSS (glassmorphic cards + neon progress) ───────

from styles.live_theme import get_live_sweat_css
st.markdown(get_live_sweat_css(), unsafe_allow_html=True)

# ── Joseph M. Smith Floating Widget ──────────────────────────

try:
    from utils.components import inject_joseph_floating, render_joseph_hero_banner
    render_joseph_hero_banner()
    st.session_state["joseph_page_context"] = "page_live_sweat"
    inject_joseph_floating()
except ImportError:
    pass

# ── Auto-Refresh (silent 120 s reload) ───────────────────────

try:
    from streamlit_autorefresh import st_autorefresh
    _tick = st_autorefresh(interval=120_000, key="live_sweat_refresh")
except ImportError:
    _tick = 0

# ============================================================
# SECTION: Imports — Pacing Engine, Tracker, Persona
# ============================================================

from data.live_game_tracker import (
    get_live_boxscores,
    get_all_live_players,
    get_game_for_player,
    match_live_player,
)
from engine.live_math import calculate_live_pace, pace_color_tier
from styles.live_theme import render_sweat_card, render_waiting_card
from agent.live_persona import get_joseph_live_reaction, stream_joseph_text

# ── Pillar 4 Panic Room imports ──────────────────────────────

from agent.payload_builder import build_live_vibe_payload, get_grudge_buffer
from agent.response_parser import parse_vibe_response, generate_vibe_css_class, get_vibe_emoji
from styles.live_theme import get_panic_room_css, render_panic_room_card

# Inject Panic Room CSS
st.markdown(get_panic_room_css(), unsafe_allow_html=True)

# ============================================================
# SECTION: Header
# ============================================================

st.title("💦 Live Sweat Dashboard")
st.markdown(
    f"**{datetime.date.today().strftime('%A, %B %d, %Y')}** — "
    "Tracking your locked bets against real-time box scores.  "
    "Auto-refreshes every **2 minutes**."
)

with st.expander("📖 How to Use the Live Sweat Dashboard", expanded=False):
    st.markdown("""
### What This Page Does
The Live Sweat Dashboard monitors your **locked player prop bets** against
real-time NBA box-score data.  It calculates whether each player is
**on pace** to hit their line (OVER or UNDER) and surfaces Joseph M. Smith's
live vibe-check reactions so you know the emotional temperature of every bet.

### How Bets Get Here
Your active bets are pulled automatically from **three sources** (in priority order):
1. **Manual Locks** — props you locked on the Neural Analysis or Prop Scanner pages.
2. **Analysis Results** — the last Neural Analysis run stored in your session.
3. **Bet Tracker Database** — bets recorded through the 📈 Bet Tracker page.

If no bets appear, head to **📡 Live Games → ⚡ Quantum Analysis Matrix** and
lock some props first, or record a bet in the **📈 Bet Tracker**.

### Reading the Sweat Cards
| Element | Meaning |
|---------|---------|
| **Pace Bar** | Green = on pace to cash, Red = falling behind |
| **Projected Total** | Current stat total + pace × remaining minutes |
| **OVER / UNDER Badge** | Which direction you bet |
| **🔥 / 😰 Emoji** | Joseph's real-time vibe on this bet |
| **Awaiting Tip-Off** | Game hasn't started yet — card is greyed out |
| **OT Badge** | Game is in overtime — pace recalculated accordingly |

### Tips
- 💡 The dashboard **auto-refreshes every 2 minutes** — no need to manually reload.
- 💡 Pace projections are most reliable after the **first quarter** of play.
- 💡 UNDER bets flip the pace bar — green means the player is staying low.
- 💡 Use this page alongside the **📈 Bet Tracker** to log final results once games finish.
""")

st.divider()

# ============================================================
# SECTION: Resolve Active Bets
# We check three sources in priority order:
#   1. st.session_state["active_bets"] (manual locks)
#   2. st.session_state["analysis_results"] (Neural Analysis)
#   3. Database via load_all_bets (Bet Tracker)
# ============================================================

_STAT_MAP = {
    "points":               "pts",
    "rebounds":             "reb",
    "assists":              "ast",
    "steals":               "stl",
    "blocks":               "blk",
    "turnovers":            "tov",
    "threes":               "fg3m",
    "three_pointers":       "fg3m",
    "pts":                  "pts",
    "reb":                  "reb",
    "ast":                  "ast",
    "stl":                  "stl",
    "blk":                  "blk",
    "tov":                  "tov",
    "fg3m":                 "fg3m",
}

_COMBO_STATS = {
    "points_rebounds":          ("pts", "reb"),
    "points_assists":           ("pts", "ast"),
    "rebounds_assists":         ("reb", "ast"),
    "points_rebounds_assists":  ("pts", "reb", "ast"),
    "blocks_steals":            ("blk", "stl"),
}


def _resolve_current_stat(player_stats: dict, stat_type: str) -> float | None:
    """Extract the correct stat value from a player's live stats dict."""
    st_lower = str(stat_type).lower().strip()
    if st_lower in _COMBO_STATS:
        return sum(player_stats.get(k, 0) for k in _COMBO_STATS[st_lower])
    box_key = _STAT_MAP.get(st_lower)
    if box_key:
        return float(player_stats.get(box_key, 0))
    return None


def _get_active_bets() -> list[dict]:
    """
    Collect the user's active bets from available sources.

    Each returned dict has at least:
        player_name, stat_type, line (float), direction
    """
    # Source 1: explicit active_bets in session state
    active = st.session_state.get("active_bets", [])
    if active:
        return list(active)

    # Source 2: analysis_results from Neural Analysis
    analysis = st.session_state.get("analysis_results", [])
    if analysis:
        bets = []
        for r in analysis:
            if r.get("should_avoid"):
                continue
            bets.append({
                "player_name": r.get("player_name", ""),
                "stat_type":   r.get("stat_type", ""),
                "line":        float(r.get("line", 0) or 0),
                "direction":   r.get("direction", "OVER"),
                "tier":        r.get("tier", ""),
            })
        return bets

    # Source 3: database (today's pending bets)
    try:
        from tracking.bet_tracker import load_all_bets
        today_str = datetime.date.today().strftime("%Y-%m-%d")
        all_bets = load_all_bets(limit=200)
        today_bets = [
            b for b in all_bets
            if str(b.get("date", "")).startswith(today_str)
            and str(b.get("result", "")).upper() not in ("WIN", "LOSS", "PUSH")
        ]
        bets = []
        for b in today_bets:
            bets.append({
                "player_name": b.get("player_name", ""),
                "stat_type":   b.get("stat_type", ""),
                "line":        float(b.get("prop_line", 0) or 0),
                "direction":   b.get("direction", "OVER"),
                "tier":        b.get("tier", ""),
            })
        return bets
    except Exception:
        pass

    return []


# ============================================================
# SECTION: Load Live Data & Render Cards
# ============================================================

active_bets = _get_active_bets()

if not active_bets:
    st.info(
        "📌 **No active bets to track yet.**\n\n"
        "Run **⚡ Neural Analysis** to generate picks, or lock bets "
        "in the **📈 Bet Tracker**, then come back here to monitor them live."
    )
    st.stop()

# Load live box scores (API-firewalled: cached 120 s)
live_games = get_live_boxscores()
all_live_players = get_all_live_players(live_games)

# ── Last Refresh Timestamp ────────────────────────────────────

_now = datetime.datetime.now()
_refresh_col, _btn_col, _ = st.columns([2, 1, 3])
with _refresh_col:
    st.caption(f"🕐 Last refreshed: **{_now.strftime('%I:%M:%S %p')}**")
with _btn_col:
    if st.button("🔄 Refresh Now", key="manual_refresh"):
        st.rerun()

if not live_games:
    st.warning(
        "📡 No live games in progress right now.  "
        "Cards will populate once tonight's games tip off."
    )

# ── Metrics Row ───────────────────────────────────────────────

cashed_count = 0
tracking_count = 0
risk_count = 0
waiting_count = 0

cards_html = ""
waiting_html = ""
vibe_checks: list[tuple[str, dict]] = []  # (player_name, pace_result)

for bet in active_bets:
    player_name = bet.get("player_name", "")
    stat_type = bet.get("stat_type", "")
    target = float(bet.get("line", 0) or 0)
    direction = str(bet.get("direction", "OVER")).upper()

    if not player_name or target <= 0:
        continue

    # Fuzzy-match the player in the live box score
    matched = match_live_player(player_name, all_live_players)
    if matched is None:
        # Player not found in live data — show awaiting card
        waiting_count += 1
        waiting_html += render_waiting_card(
            player_name=player_name,
            stat_type=stat_type,
            target_stat=target,
            direction=direction,
        )
        continue

    tracking_count += 1

    # Get the game context for blowout detection
    game = get_game_for_player(player_name, live_games)
    score_diff = 0.0
    period = ""
    if game:
        score_diff = abs(game.get("home_score", 0) - game.get("away_score", 0))
        period = game.get("period", "")

    current_stat_val = _resolve_current_stat(matched, stat_type)
    if current_stat_val is None:
        continue

    # Run the pacing engine
    pace = calculate_live_pace(
        current_stat=current_stat_val,
        minutes_played=matched.get("minutes", 0),
        target_stat=target,
        live_score_diff=score_diff,
        current_fouls=matched.get("fouls", 0),
        period=period,
        direction=direction,
    )

    color = pace_color_tier(pace["pct_of_target"], direction)

    if pace["cashed"]:
        cashed_count += 1
    if pace["blowout_risk"] or pace["foul_trouble"]:
        risk_count += 1

    # Render card
    cards_html += render_sweat_card(
        player_name=player_name,
        stat_type=stat_type,
        current_stat=pace["current_stat"],
        target_stat=pace["target_stat"],
        projected_final=pace["projected_final"],
        pct_of_target=pace["pct_of_target"],
        color_tier=color,
        blowout_risk=pace["blowout_risk"],
        foul_trouble=pace["foul_trouble"],
        cashed=pace["cashed"],
        minutes_played=pace["minutes_played"],
        direction=direction,
        minutes_remaining=pace["minutes_remaining"],
        is_overtime=pace["is_overtime"],
    )

    vibe_checks.append((player_name, pace))

# ── Top-Level Metrics ─────────────────────────────────────────

c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("🎯 Active Bets", len(active_bets))
c2.metric("📡 Tracking Live", tracking_count)
c3.metric("✅ Cashed", cashed_count)
c4.metric("🚨 At Risk", risk_count)
c5.metric("🕐 Awaiting", waiting_count)

st.divider()

# ── Balloons for any cashed bet ───────────────────────────────

if cashed_count > 0:
    st.balloons()

# ── Render all sweat cards ────────────────────────────────────

if cards_html:
    st.markdown(cards_html, unsafe_allow_html=True)
elif live_games:
    st.info(
        "📊 No matching box-score data found for your active bets yet. "
        "Stats appear once games tip off and players check in."
    )

# ── Render awaiting tip-off cards ─────────────────────────────

if waiting_html:
    st.markdown(
        '<div style="margin-top:8px;">'
        '<div class="sweat-stat-label" style="margin-bottom:6px;">🕐 Awaiting Tip-Off</div>'
        f'{waiting_html}</div>',
        unsafe_allow_html=True,
    )

# ============================================================
# SECTION: Joseph's Live Vibe Checks (Pillar 4 Panic Room)
# ============================================================

if vibe_checks:
    st.divider()
    st.subheader("🎙️ Joseph's Live Panic Room")

    grudge = get_grudge_buffer()

    panic_cards_html = ""
    for player_name, pace in vibe_checks:
        # Fast-path fragment reaction (always available offline)
        reaction = get_joseph_live_reaction(pace)

        # Build the Pillar 4 structured payload for this bet
        bet_for_payload = next(
            (b for b in active_bets
             if b.get("player_name", "") == player_name),
            {"player_name": player_name, "stat_type": "",
             "line": pace.get("target_stat", 0),
             "direction": pace.get("direction", "OVER")},
        )
        game = get_game_for_player(player_name, live_games)
        matched = match_live_player(player_name, all_live_players)

        payload = build_live_vibe_payload(
            ticket=bet_for_payload,
            live_stats=matched or {},
            game_context=game or {},
            grudge_buffer=grudge,
            pace_result=pace,
        )

        game_state = payload.get("game_state", "")

        # Map game state to a vibe status for the card glow
        from agent.response_parser import _STATE_TO_DEFAULT_VIBE
        vibe_status = _STATE_TO_DEFAULT_VIBE.get(game_state, "Sweating")

        # Generate ticker-tape headline from the game state
        _STATE_HEADLINES = {
            "THE_HOOK":             "DYING ON THE HOOK!",
            "FREE_THROW_MERCHANT":  "FREE THROW MERCHANT!",
            "BENCH_SWEAT":          "BENCH SWEAT ALERT!",
            "USAGE_FREEZE_OUT":     "GIVE HIM THE BALL!",
            "GARBAGE_TIME_MIRACLE": "GARBAGE TIME MIRACLE!",
            "LOCKER_ROOM_TRAGEDY":  "INJURY SCARE!",
            "THE_REF_SHOW":         "BLAME THE REFS!",
            "THE_CLEAN_CASH":       "CASHED IT!",
        }
        headline = _STATE_HEADLINES.get(game_state, "JOSEPH IS SWEATING!")

        # Render the panic room card
        panic_cards_html += render_panic_room_card(
            vibe_status=vibe_status,
            ticker_headline=headline,
            joseph_rant=reaction,
            player_name=player_name,
            game_state=game_state,
        )

        # Push reaction into grudge buffer for anti-repetition
        grudge.add(reaction)

    # Render all panic room cards
    if panic_cards_html:
        st.markdown(panic_cards_html, unsafe_allow_html=True)

    # Still offer the streaming text in expanders for detail
    st.markdown(
        '<div class="sweat-stat-label" style="margin-top:12px;margin-bottom:4px;">'
        '📜 Detailed Reactions</div>',
        unsafe_allow_html=True,
    )
    for player_name, pace in vibe_checks:
        reaction = get_joseph_live_reaction(pace)
        with st.expander(f"🎙️ {player_name}", expanded=pace.get("cashed", False)):
            st.write_stream(stream_joseph_text(reaction))
