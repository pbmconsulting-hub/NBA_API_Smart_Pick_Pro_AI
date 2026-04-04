"""
app.py — Slim router for SmartPicksProAI Streamlit dashboard.

All page logic lives in ``pages/<module>.py``; this file handles:

* ``st.set_page_config``
* Session-state initialisation
* Global CSS injection
* Sidebar (navigation, player search, bankroll, admin sync)
* Page dispatch via ``_PAGE_DISPATCH``

Start the dashboard::

    cd SmartPicksProAI/frontend
    streamlit run app.py
"""

import sys
import streamlit as st

from typing import Any
from collections.abc import Callable
from pathlib import Path

# ── Ensure SmartPicksProAI package root is on sys.path ──────
_FRONTEND_DIR = Path(__file__).resolve().parent
_PACKAGE_ROOT = _FRONTEND_DIR.parent
if str(_PACKAGE_ROOT) not in sys.path:
    sys.path.insert(0, str(_PACKAGE_ROOT))

from styles.theme import (
    get_global_css,
    get_sidebar_avatar_html,
    get_sidebar_brand_html,
)
from tracking.database import initialize_database as _init_tracker_db
from tracking.auto_resolver import auto_resolve_pending_picks as _auto_resolve
from api_service import (
    get_defense_vs_position,
    get_league_leaders,
    get_player_last5,
    get_recent_games,
    get_standings,
    get_team_roster,
    get_team_stats,
    get_teams,
    get_todays_games,
    search_players,
    trigger_refresh,
)
from pages._shared import (
    DEFAULT_BANKROLL,
    load_persisted_bankroll,
    nav,
    save_persisted_bankroll,
)

# ── Page modules (lazy-ish: imported once at startup) ────────
from pages import (
    bet_tracker as _pg_bet_tracker,
    defense as _pg_defense,
    game_detail as _pg_game_detail,
    home as _pg_home,
    leaders as _pg_leaders,
    model_health as _pg_model_health,
    pick_history as _pg_pick_history,
    player_profile as _pg_player_profile,
    prop_analyzer as _pg_prop_analyzer,
    schedule as _pg_schedule,
    standings as _pg_standings,
    team_detail as _pg_team_detail,
    teams_browse as _pg_teams_browse,
    trade_impact as _pg_trade_impact,
)

# ═══════════════════════════════════════════════════════════════════════════
# Page configuration
# ═══════════════════════════════════════════════════════════════════════════

st.set_page_config(
    page_title="Smart Pick Pro — NBA Edition",
    page_icon="🏀",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Initialize bet tracker database
_init_tracker_db()

# Auto-resolve pending picks from yesterday's box scores
if not st.session_state.get("_auto_resolve_done"):
    try:
        _auto_resolve()
    except Exception:
        pass  # Never block app startup for auto-resolve
    st.session_state["_auto_resolve_done"] = True

# ═══════════════════════════════════════════════════════════════════════════
# Session-state navigation
# ═══════════════════════════════════════════════════════════════════════════

# F14: load persisted bankroll if first visit
_persisted = load_persisted_bankroll()
_initial_bankroll = _persisted if _persisted is not None else DEFAULT_BANKROLL

_DEFAULT_STATE: dict[str, Any] = {
    "page": "home",
    "selected_game_id": None,
    "selected_player_id": None,
    "selected_team_id": None,
    "game_context": {},
    "user_bankroll": _initial_bankroll,
    "last_analysis": None,
}
for _key, _default in _DEFAULT_STATE.items():
    if _key not in st.session_state:
        st.session_state[_key] = _default

# ═══════════════════════════════════════════════════════════════════════════
# Global theme CSS
# ═══════════════════════════════════════════════════════════════════════════

st.markdown(get_global_css(), unsafe_allow_html=True)

# ═══════════════════════════════════════════════════════════════════════════
# Sidebar — Smart Pick Pro branded nav + Joseph M Smith avatar
# ═══════════════════════════════════════════════════════════════════════════

with st.sidebar:
    # Logo / Brand
    st.markdown(get_sidebar_brand_html(), unsafe_allow_html=True)

    # Joseph M Smith avatar
    st.markdown(get_sidebar_avatar_html(), unsafe_allow_html=True)
    st.divider()

    # ── Navigation (F1: active page highlighted) ──────────────
    st.markdown(
        '<div class="section-hdr">Navigation</div>',
        unsafe_allow_html=True,
    )

    nav_items = [
        ("🏠  Home", "home"),
        ("🎯  Prop Analyzer", "prop_analyzer"),
        ("📈  Bet Tracker", "bet_tracker"),
        ("📋  Pick History", "pick_history"),
        ("🩺  Model Health", "model_health"),
        ("🔀  Trade Impact", "trade_impact"),
        ("🏆  Standings", "standings"),
        ("🏟️  Teams", "teams_browse"),
        ("📊  Leaders & Stats", "leaders"),
        ("🛡️  Defense vs Position", "defense"),
        ("🗓️  Schedule", "more"),
    ]
    _current_page = st.session_state.page
    for label, page_key in nav_items:
        # F1: highlight the currently active page with primary button type
        _btn_type = "primary" if _current_page == page_key else "secondary"
        if st.button(label, key=f"nav_{page_key}",
                     use_container_width=True, type=_btn_type):
            nav(page_key)
            st.rerun()

    st.divider()

    # ── Player Quick Search ───────────────────────────────────
    st.markdown(
        '<div class="section-hdr">Quick Player Search</div>',
        unsafe_allow_html=True,
    )
    sidebar_search = st.text_input(
        "Search player",
        placeholder="e.g. LeBron, Curry …",
        key="sidebar_search",
        label_visibility="collapsed",
    )
    if sidebar_search.strip():
        results = search_players(sidebar_search.strip())
        if results:
            for r in results[:8]:
                pid = r["player_id"]
                nm = r.get("full_name", "")
                pos = r.get("position", "")
                tm = r.get("team_abbreviation", "")
                btn_label = f"{nm}"
                if pos:
                    btn_label += f" ({pos})"
                if tm:
                    btn_label += f" · {tm}"
                if st.button(btn_label, key=f"sb_p_{pid}",
                             use_container_width=True):
                    nav("player_profile", selected_player_id=pid)
                    st.rerun()
        else:
            st.caption("No players found.")

    st.divider()

    # ── Bankroll Setting (F14: persistence) ───────────────────
    st.markdown(
        '<div class="section-hdr">Bankroll</div>',
        unsafe_allow_html=True,
    )
    _new_bankroll = st.number_input(
        "Your bankroll ($)",
        min_value=10.0,
        value=float(st.session_state.user_bankroll),
        step=50.0,
        key="sidebar_bankroll",
        help="Used for Kelly bet sizing. Not financial advice.",
    )
    if _new_bankroll != st.session_state.user_bankroll:
        st.session_state.user_bankroll = _new_bankroll
        save_persisted_bankroll(_new_bankroll)  # F14

    st.divider()

    # ── Admin ─────────────────────────────────────────────────
    st.markdown(
        '<div class="section-hdr">Admin</div>',
        unsafe_allow_html=True,
    )
    if st.button("🔄 Sync Latest Data", use_container_width=True,
                 key="admin_sync"):
        with st.spinner("Syncing with NBA API…"):
            result = trigger_refresh()
        if result.get("status") == "success":
            st.success(result.get("message", "Refresh complete."))
            for fn in [get_todays_games, get_player_last5, search_players,
                       get_teams, get_team_roster, get_team_stats,
                       get_defense_vs_position, get_standings,
                       get_league_leaders, get_recent_games]:
                fn.clear()
            # F13: invalidate DVP cache on sync
            st.session_state.pop("dvp_all_teams_cache", None)
        else:
            st.error(f"Failed: {result.get('message', 'Unknown error')}")

    st.divider()
    st.markdown(
        '<div class="sidebar-engine-label">'
        '⚡ Powered by Quantum Matrix Engine 5.6</div>',
        unsafe_allow_html=True,
    )


# ═══════════════════════════════════════════════════════════════════════════
# Page router — dispatches to pages/*.render()
# ═══════════════════════════════════════════════════════════════════════════

_PAGE_DISPATCH: dict[str, Callable[[], None]] = {
    "home": _pg_home.render,
    "game_detail": _pg_game_detail.render,
    "player_profile": _pg_player_profile.render,
    "standings": _pg_standings.render,
    "teams_browse": _pg_teams_browse.render,
    "team_detail": _pg_team_detail.render,
    "leaders": _pg_leaders.render,
    "defense": _pg_defense.render,
    "more": _pg_schedule.render,
    "prop_analyzer": _pg_prop_analyzer.render,
    "pick_history": _pg_pick_history.render,
    "bet_tracker": _pg_bet_tracker.render,
    "model_health": _pg_model_health.render,
    "trade_impact": _pg_trade_impact.render,
}

_page_fn = _PAGE_DISPATCH.get(st.session_state.page)
if _page_fn is not None:
    _page_fn()
else:
    st.warning(f"Unknown page: {st.session_state.page}")
    _pg_home.render()
