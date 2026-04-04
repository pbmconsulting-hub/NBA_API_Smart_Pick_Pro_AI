"""
app.py — Root-level Streamlit entry point for SmartPicksProAI.

This is the primary entry point that can be run from the project root::

    streamlit run app.py

It sets up sys.path and delegates to the internal SmartPicksProAI frontend.
When Streamlit detects a ``pages/`` folder at this level it will use native
multi-page navigation automatically.
"""

import sys
from pathlib import Path

# ── Ensure SmartPicksProAI package is importable ────────────────────────
_ROOT_DIR = Path(__file__).resolve().parent
_PKG_DIR = _ROOT_DIR / "SmartPicksProAI"
for _p in (_ROOT_DIR, _PKG_DIR, str(_PKG_DIR / "frontend")):
    _p_str = str(_p)
    if _p_str not in sys.path:
        sys.path.insert(0, _p_str)

import streamlit as st
from typing import Any

# ── Page configuration (must be first Streamlit command) ────────────────
st.set_page_config(
    page_title="Smart Pick Pro AI — NBA Edition",
    page_icon="🏀",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Safe imports (graceful fallback if backend not running) ─────────────
try:
    from SmartPicksProAI.styles.theme import (
        get_global_css,
        get_sidebar_avatar_html,
        get_sidebar_brand_html,
        get_hero_banner_html,
    )
except ImportError:
    def get_global_css() -> str:
        return ""
    def get_sidebar_avatar_html() -> str:
        return ""
    def get_sidebar_brand_html() -> str:
        return ""
    def get_hero_banner_html() -> str:
        return ""

try:
    from SmartPicksProAI.tracking.database import initialize_database as _init_db
    from SmartPicksProAI.tracking.auto_resolver import (
        auto_resolve_pending_picks as _auto_resolve,
    )
except ImportError:
    def _init_db() -> None:
        pass
    def _auto_resolve() -> None:
        pass

try:
    from SmartPicksProAI.frontend.api_service import (
        get_todays_games,
        get_recent_games,
        get_todays_slate,
        trigger_refresh,
    )
    _API_AVAILABLE = True
except ImportError:
    _API_AVAILABLE = False

# ── Initialisation ──────────────────────────────────────────────────────
_init_db()

if not st.session_state.get("_auto_resolve_done"):
    try:
        _auto_resolve()
    except Exception:
        pass
    st.session_state["_auto_resolve_done"] = True

# ── Session state defaults ──────────────────────────────────────────────
_DEFAULTS: dict[str, Any] = {
    "user_bankroll": 500.0,
    "last_analysis": None,
    "avoid_list": [],
    "settings": {
        "default_platform": "PrizePicks",
        "simulation_iterations": 5000,
        "kelly_fraction": 0.25,
        "auto_log_bets": True,
    },
}
for _k, _v in _DEFAULTS.items():
    if _k not in st.session_state:
        st.session_state[_k] = _v

# ── Global CSS ──────────────────────────────────────────────────────────
st.markdown(get_global_css(), unsafe_allow_html=True)

# ── Sidebar ─────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown(get_sidebar_brand_html(), unsafe_allow_html=True)
    st.markdown(get_sidebar_avatar_html(), unsafe_allow_html=True)
    st.divider()

    st.markdown("### 💰 Bankroll")
    new_br = st.number_input(
        "Your bankroll ($)",
        min_value=10.0,
        value=float(st.session_state.user_bankroll),
        step=50.0,
        key="sidebar_bankroll_root",
        help="Used for Kelly bet sizing.",
    )
    if new_br != st.session_state.user_bankroll:
        st.session_state.user_bankroll = new_br

    st.divider()

    st.markdown("### 🔧 Admin")
    if _API_AVAILABLE and st.button(
        "🔄 Sync Latest Data", use_container_width=True, key="root_sync"
    ):
        with st.spinner("Syncing with NBA API…"):
            result = trigger_refresh()
        if result.get("status") == "success":
            st.success(result.get("message", "Refresh complete."))
        else:
            st.error(f"Failed: {result.get('message', 'Unknown error')}")

    st.divider()
    st.markdown(
        '<div style="text-align:center;font-size:0.75rem;opacity:0.6">'
        "⚡ Powered by Quantum Matrix Engine 5.6</div>",
        unsafe_allow_html=True,
    )

# ── Home page content ───────────────────────────────────────────────────

st.markdown(get_hero_banner_html(), unsafe_allow_html=True)
st.title("🏀 Smart Pick Pro AI")
st.caption(
    "Quantum AI Prop Intelligence — use the sidebar pages to navigate"
)

# ── Today's Games ───────────────────────────────────────────────────────
st.markdown("## 📅 Today's Matchups")

if _API_AVAILABLE:
    games = get_todays_games()
    if games:
        cols = st.columns(min(len(games), 4))
        for idx, game in enumerate(games):
            with cols[idx % len(cols)]:
                matchup = game.get("matchup", "TBD")
                home_score = game.get("home_score")
                away_score = game.get("away_score")
                if home_score is not None and away_score is not None:
                    st.metric(matchup, f"{home_score} – {away_score}")
                else:
                    st.info(matchup)
    else:
        st.info("No games scheduled for today.")

    st.divider()

    # ── Today's AI Picks ────────────────────────────────────────────────
    st.markdown("## 🤖 Today's Best AI Picks")
    slate = get_todays_slate(top_n=5)
    top_picks = slate.get("picks", [])
    if top_picks:
        for pick in top_picks[:5]:
            p_name = pick.get("player_name", "Unknown")
            p_stat = pick.get("stat_type", "?")
            p_line = pick.get("prop_line", 0)
            p_dir = pick.get("direction", "OVER")
            p_tier = pick.get("tier", "Bronze")
            p_conf = pick.get("confidence_score", 0)
            p_edge = pick.get("edge_pct", 0.0)

            tier_icons = {
                "Platinum": "💎", "Gold": "🥇",
                "Silver": "🥈", "Bronze": "🥉",
            }
            icon = tier_icons.get(p_tier, "🥉")
            dir_icon = "🟢" if p_dir == "OVER" else "🔴"

            card_cols = st.columns([3, 1, 1])
            card_cols[0].markdown(
                f"**{icon} {p_name}** — "
                f"{p_stat.upper()} {dir_icon} {p_dir} {p_line}"
            )
            card_cols[1].metric("Confidence", f"{p_conf:.0f}")
            card_cols[2].metric("Edge", f"{p_edge:+.1f}%")
    else:
        st.info("No AI picks available yet — wait for today's games.")

    st.divider()

    # ── Recent Games ────────────────────────────────────────────────────
    st.markdown("## 🕐 Recent Games")
    recent = get_recent_games()
    if recent:
        for game in recent[:10]:
            matchup = game.get("matchup", "TBD")
            gdate = game.get("game_date", "")
            st.markdown(f"🏀 **{matchup}** — {gdate}")
    else:
        st.info("No recent games available.")
else:
    st.warning(
        "Backend API is not available. Start the backend server first:\n\n"
        "```bash\n"
        "cd SmartPicksProAI/backend && python -m uvicorn api:app "
        "--host 127.0.0.1 --port 8098\n"
        "```"
    )

st.divider()
st.caption(
    "Navigate using the sidebar pages: Today's Games, Import Props, "
    "Analysis, Entry Builder, Avoid List, Model Health, Settings, Update Data."
)
