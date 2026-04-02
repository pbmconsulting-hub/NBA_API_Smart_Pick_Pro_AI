# ============================================================
# FILE: app.py
# PURPOSE: Main entry point for the SmartBetPro NBA Streamlit app.
#          Professional dark-themed dashboard with today's slate,
#          status cards, and quick-start guide.
# HOW TO RUN: streamlit run app.py
# ============================================================

import streamlit as st
import datetime
import os
import base64
import logging

from data.data_manager import load_players_data, load_props_data, load_teams_data
from data.nba_data_service import load_last_updated
from tracking.database import initialize_database
from styles.theme import get_global_css

# ============================================================
# SECTION: Page Configuration
# ============================================================

st.set_page_config(
    page_title="Smart Pick Pro — NBA Edition",
    page_icon="🏀",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ─── Inject Global CSS Theme ──────────────────────────────────
st.markdown(get_global_css(), unsafe_allow_html=True)

# ─── App Logo — Smart Pick Pro ───────────────────────────────
# Logo is rendered per-page only on key pages (Neural Analysis, Studio,
# Live Games, Live Scores & Props, Bet Tracker) instead of globally.
_ROOT_LOGO_PATH   = os.path.join(os.path.dirname(__file__), "Smart_Pick_Pro_Logo.png")
_ASSETS_LOGO_PATH = os.path.join(os.path.dirname(__file__), "assets", "Smart_Pick_Pro_Logo.png")
_LOGO_PATH = _ROOT_LOGO_PATH if os.path.exists(_ROOT_LOGO_PATH) else _ASSETS_LOGO_PATH


@st.cache_data(show_spinner=False)
def _load_logo_b64(path: str) -> str:
    """Read the logo PNG and return a base64-encoded string (cached per session)."""
    try:
        with open(path, "rb") as _f:
            return base64.b64encode(_f.read()).decode()
    except Exception:
        return ""

# ─── Quantum Edge Theme CSS — page-level overrides ───────────
st.markdown("""
<style>
/* ═══════════════════════════════════════════════════════════
   PILLAR 3 — Hero HUD: Glassmorphic, Responsive Flexbox
   ═══════════════════════════════════════════════════════════ */
.hero-hud {
    background: rgba(15,23,42,0.50);
    backdrop-filter: blur(24px);
    -webkit-backdrop-filter: blur(24px);
    border: 1px solid rgba(255,255,255,0.06);
    border-radius: 16px;
    padding: 32px 40px;
    margin-bottom: 24px;
    box-shadow: 0 0 40px rgba(0,240,255,0.06), 0 8px 32px rgba(0,0,0,0.45);
    position: relative;
    overflow: hidden;
    display: flex;
    align-items: center;
    gap: 28px;
}
/* Rainbow accent bar at top */
.hero-hud::before {
    content: '';
    position: absolute;
    top: 0; left: 0; right: 0; height: 2px;
    background: linear-gradient(90deg, #00f0ff, #00ff9d, #FFD700, #c800ff, #00f0ff);
    background-size: 200% 100%;
    animation: headerShimmer 4s ease infinite;
}
.hero-hud-text { flex: 1; min-width: 0; }
.hero-tagline {
    font-size: clamp(1.2rem, 2.5vw, 1.8rem);
    font-weight: 800;
    font-family: 'Orbitron', sans-serif;
    background: linear-gradient(135deg, #00f0ff, #00ff9d);
    -webkit-background-clip: text;
    -webkit-text-fill-color: transparent;
    background-clip: text;
    letter-spacing: 0.04em;
    margin: 0;
    line-height: 1.3;
}
.hero-subtext {
    font-size: clamp(0.72rem, 1.1vw, 0.85rem);
    color: #94A3B8;
    font-family: 'JetBrains Mono', 'Courier New', monospace;
    font-variant-numeric: tabular-nums;
    letter-spacing: 0.05em;
    margin-top: 8px;
}
.hero-date {
    font-size: 0.82rem;
    color: rgba(148,163,184,0.80);
    margin-top: 10px;
    font-family: 'JetBrains Mono', 'Courier New', monospace;
    font-variant-numeric: tabular-nums;
    letter-spacing: 0.03em;
}
.spp-hero-logo {
    width: 88px !important;
    max-width: 100%;
    height: auto;
    object-fit: contain;
    border-radius: 50%;
    box-shadow: 0 0 18px rgba(0,240,255,0.30);
    flex-shrink: 0;
}
/* Responsive: stack on mobile */
@media (max-width: 640px) {
    .hero-hud { flex-direction: column; text-align: center; padding: 24px 20px; }
    .spp-hero-logo { width: 88px !important; max-width: 100%; }
}
/* Status card — dark glass panel */
.status-card {
    background: rgba(15,23,42,0.50);
    border: 1px solid rgba(255,255,255,0.05);
    border-radius: 12px;
    padding: 16px 20px;
    text-align: center;
    box-shadow: 0 0 16px rgba(0,240,255,0.04), 0 4px 16px rgba(0,0,0,0.30);
    backdrop-filter: blur(12px);
    -webkit-backdrop-filter: blur(12px);
    transition: border-color 0.2s ease, transform 0.2s ease, box-shadow 0.2s ease;
}
.status-card:hover {
    border-color: rgba(0,240,255,0.20);
    transform: translateY(-3px);
    box-shadow: 0 0 24px rgba(0,240,255,0.10), 0 6px 20px rgba(0,0,0,0.40);
}
.status-card-value {
    font-size: 2rem;
    font-weight: 800;
    color: rgba(255,255,255,0.95);
    font-family: 'JetBrains Mono', 'Courier New', monospace;
    font-variant-numeric: tabular-nums;
}
.status-card-label {
    font-size: 0.72rem;
    color: #94A3B8;
    text-transform: uppercase;
    letter-spacing: 1.2px;
    margin-top: 4px;
    font-family: 'Inter', sans-serif;
}
/* Tonight's slate team chips */
.team-chip {
    display: inline-block;
    background: rgba(0,240,255,0.05);
    color: rgba(255,255,255,0.90);
    border: 1px solid rgba(255,255,255,0.08);
    padding: 3px 10px;
    border-radius: 6px;
    font-size: 0.85rem;
    font-weight: 600;
    margin: 3px;
    transition: border-color 0.2s ease;
}
.team-chip:hover { border-color: rgba(0,240,255,0.25); }
/* LIVE badge with "The Pulse" dot built in */
.live-badge {
    display: inline-flex;
    align-items: center;
    gap: 6px;
    background: rgba(0,255,157,0.08);
    color: #00ff9d;
    border: 1px solid rgba(0,255,157,0.30);
    padding: 3px 10px;
    border-radius: 12px;
    font-size: 0.8rem;
    font-weight: 700;
}
.live-badge::before {
    content: '';
    display: inline-block;
    width: 7px;
    height: 7px;
    border-radius: 50%;
    background: #00ff9d;
    animation: thePulse 1.8s ease-in-out infinite;
    flex-shrink: 0;
}
.sample-badge {
    display: inline-block;
    background: rgba(255,94,0,0.08);
    color: #ff9d4d;
    border: 1px solid rgba(255,94,0,0.30);
    padding: 3px 10px;
    border-radius: 12px;
    font-size: 0.8rem;
    font-weight: 700;
}
</style>
""", unsafe_allow_html=True)

# ============================================================
# END SECTION: Page Configuration
# ============================================================

# ============================================================
# SECTION: Initialize App on Startup
# ============================================================

initialize_database()

# ── SQLite production warning ─────────────────────────────────
# Warn if running in production mode with the default SQLite path.
# SQLite is not ideal for multi-user cloud deployments.
if os.environ.get("SMARTAI_PRODUCTION", "").lower() in ("true", "1", "yes"):
    _db_path = os.environ.get("DB_PATH", "db/smartai_nba.db")
    if os.path.basename(_db_path) == "smartai_nba.db":
        logging.getLogger(__name__).warning(
            "Running in production mode with SQLite (%s). "
            "SQLite is not recommended for multi-user cloud deployments. "
            "Consider PostgreSQL or persistent storage. See docs/database_migration.md",
            _db_path,
        )
        if "sqlite_warning_shown" not in st.session_state:
            st.session_state["sqlite_warning_shown"] = True
            st.warning(
                "⚠️ **SQLite in Production Mode** — This app is running with SQLite, "
                "which may not persist data on ephemeral cloud platforms (e.g., Streamlit Cloud). "
                "See `docs/database_migration.md` for migration options."
            )

# ── Premium Status — Check and display in sidebar ─────────────
# This runs silently on app load.  is_premium_user() is cached in
# session state so it won't make Stripe API calls on every rerun.
try:
    from utils.auth import is_premium_user as _is_premium, handle_checkout_redirect as _handle_checkout
    from utils.stripe_manager import _PREMIUM_PAGE_PATH as _PREM_PATH
    # Handle checkout redirects even on the home page
    _handle_checkout()
    _user_is_premium = _is_premium()
except Exception:
    _user_is_premium = True  # Fail open — don't block the home page
    _PREM_PATH = "/14_%F0%9F%92%8E_Subscription_Level"

with st.sidebar:
    if _user_is_premium:
        st.markdown(
            '<div style="background:rgba(0,255,157,0.10);border:1px solid rgba(0,255,157,0.3);'
            'border-radius:10px;padding:10px 14px;text-align:center;margin-bottom:8px;">'
            '<span style="color:#00ff9d;font-weight:700;font-size:0.9rem;">💎 Premium Active</span>'
            '</div>',
            unsafe_allow_html=True,
        )
    else:
        st.markdown(
            f'<div style="background:rgba(255,94,0,0.08);border:1px solid rgba(255,94,0,0.25);'
            f'border-radius:10px;padding:10px 14px;text-align:center;margin-bottom:8px;">'
            f'<span style="color:#a0b4d0;font-size:0.85rem;">⭐ Free Plan</span><br>'
            f'<a href="{_PREM_PATH}" style="color:#ff5e00;font-size:0.78rem;'
            f'font-weight:600;text-decoration:none;">Upgrade to Premium →</a>'
            f'</div>',
            unsafe_allow_html=True,
        )

if "simulation_depth" not in st.session_state:
    st.session_state["simulation_depth"] = 1000
if "minimum_edge_threshold" not in st.session_state:
    st.session_state["minimum_edge_threshold"] = 5.0
if "entry_fee" not in st.session_state:
    st.session_state["entry_fee"] = 10.0
if "total_bankroll" not in st.session_state:
    st.session_state["total_bankroll"] = 1000.0
if "kelly_multiplier" not in st.session_state:
    st.session_state["kelly_multiplier"] = 0.25
if "selected_platforms" not in st.session_state:
    st.session_state["selected_platforms"] = [
        "PrizePicks", "Underdog Fantasy", "DraftKings Pick6",
    ]
if "todays_games" not in st.session_state:
    st.session_state["todays_games"] = []
if "analysis_results" not in st.session_state:
    st.session_state["analysis_results"] = []
if "selected_picks" not in st.session_state:
    st.session_state["selected_picks"] = []
if "session_props" not in st.session_state:
    st.session_state["session_props"] = []
if "loaded_live_picks" not in st.session_state:
    st.session_state["loaded_live_picks"] = []

# ═══ Joseph M. Smith Session State ═══
st.session_state.setdefault("joseph_enabled", True)
st.session_state.setdefault("joseph_used_fragments", set())
st.session_state.setdefault("joseph_bets_logged", False)
st.session_state.setdefault("joseph_results", [])
st.session_state.setdefault("joseph_widget_mode", None)
st.session_state.setdefault("joseph_widget_selection", None)
st.session_state.setdefault("joseph_widget_response", None)
st.session_state.setdefault("joseph_ambient_line", "")
st.session_state.setdefault("joseph_ambient_context", "idle")
st.session_state.setdefault("joseph_last_commentary", "")
st.session_state.setdefault("joseph_entry_just_built", False)

# ── Global Settings Popover (accessible from sidebar) ─────────
from utils.components import render_global_settings, inject_joseph_floating, render_joseph_hero_banner
with st.sidebar:
    render_global_settings()
st.session_state["joseph_page_context"] = "page_home"
inject_joseph_floating()
render_joseph_hero_banner()

# ============================================================
# END SECTION: Initialize App on Startup
# ============================================================

# ============================================================
# SECTION: Hero Banner
# ============================================================

today_str = datetime.date.today().strftime("%A, %B %d, %Y")
todays_games = st.session_state.get("todays_games", [])
game_count = len(todays_games)
game_count_text = f"{game_count} game{'s' if game_count != 1 else ''} loaded" if game_count else "No games loaded yet"

# Embed logo using cached base64 load — root-level high-res version
_logo_b64 = _load_logo_b64(_LOGO_PATH) if os.path.exists(_LOGO_PATH) else ""

_logo_img_tag = (
    f'<img src="data:image/png;base64,{_logo_b64}" class="spp-hero-logo" alt="Smart Pick Pro logo" />'
    if _logo_b64 else ""
)

st.markdown(f"""
<div class="hero-hud ss-fade-in-up">
  {_logo_img_tag}
  <div class="hero-hud-text">
    <div class="hero-tagline">Quantum-Powered Prop Analytics</div>
    <div class="hero-subtext">1,000 Quantum Matrix Engine Simulations &bull; Institutional Edge Detection</div>
    <div class="hero-date">📅 {today_str} &nbsp;&bull;&nbsp; 🏟️ {game_count_text}</div>
  </div>
</div>
""", unsafe_allow_html=True)

# ============================================================
# END SECTION: Hero Banner
# ============================================================

with st.expander("📖 How to Use SmartBetPro NBA", expanded=False):
    st.markdown("""
    ### Welcome to SmartBetPro NBA — Your Quantum-Powered Betting Edge
    
    This app uses **Monte Carlo simulations**, **machine learning confidence scoring**, 
    and **real-time NBA data** to analyze player prop bets.
    
    **Recommended Workflow**
    1. **📡 Data Feed** — Update player stats and team metrics (do this first each day)
    2. **📡 Live Games** — Load tonight's games and load live prop lines
    3. **⚡ Quantum Analysis** — Run the Neural Analysis engine on your props
    4. **📋 Game Report** — Review detailed breakdowns for each game
    5. **🧬 Entry Builder** — Build optimal parlays from the best picks
    6. **📈 Bet Tracker** — Log your bets and track performance over time
    
    **Key Pages**
    - 🔬 **Prop Scanner**: Enter props manually or upload CSV files
    - 🎙️ **The Studio**: Get AI analysis from Joseph M. Smith
    - 🛡️ **Risk Shield**: See which props to avoid and why
    - 🗺️ **Correlation Matrix**: Understand how props relate to each other
    - 🔮 **Player Simulator**: Run what-if scenarios for any player
    - 📊 **Backtester**: Validate the model against historical results
    
    💡 **Pro Tip:** Start with the Data Feed → Live Games → Quantum Analysis workflow each session.
    """)

# ============================================================
# SECTION: Tonight's Slate
# ============================================================

if todays_games:
    st.subheader("🏟️ Tonight's Slate")

    # Build team chips
    chips_html = ""
    for game in todays_games:
        away = game.get("away_team", "")
        home = game.get("home_team", "")
        aw = game.get("away_wins", 0)
        al = game.get("away_losses", 0)
        hw = game.get("home_wins", 0)
        hl = game.get("home_losses", 0)
        rec_a = f" ({aw}-{al})" if aw or al else ""
        rec_h = f" ({hw}-{hl})" if hw or hl else ""
        chips_html += f'<span class="team-chip">🚌 {away}{rec_a}</span> '
        chips_html += f'<span style="color:#c800ff; font-weight:700; font-size:0.9rem;">vs</span> '
        chips_html += f'<span class="team-chip">🏠 {home}{rec_h}</span> &nbsp;&nbsp;&nbsp;'

    st.markdown(f'<div style="margin:8px 0 4px 0;">{chips_html}</div>', unsafe_allow_html=True)
    st.caption("Go to 📡 Live Games to update spreads/totals")
    st.divider()

# ============================================================
# END SECTION: Tonight's Slate
# ============================================================

# ============================================================
# SECTION: Status Dashboard
# ============================================================

players_data = load_players_data()
props_data = load_props_data()
teams_data = load_teams_data()

# ── Teams Data Staleness Check ─────────────────────────────────
try:
    from data.nba_data_service import load_last_updated as _load_lu
    from data.data_manager import load_teams_data as _load_teams
    _last_updated = _load_lu() or {}
    _teams_ts = _last_updated.get("teams_stats")
    if _teams_ts:
        import datetime as _dt
        _teams_date = _dt.datetime.fromisoformat(_teams_ts)
        _teams_age_days = (_dt.datetime.now() - _teams_date).days
        if _teams_age_days >= 7:
            st.warning(
                f"⚠️ Team stats are **{_teams_age_days} days old**. "
                f"Go to **📡 Data Feed → Smart Update** to refresh."
            )
        elif _teams_age_days >= 3:
            st.info(
                f"ℹ️ Team stats last updated {_teams_age_days} days ago. "
                f"Consider refreshing on the Data Feed page."
            )
    else:
        _teams_data = _load_teams()
        if not _teams_data:
            st.warning(
                "⚠️ No team stats found. Go to **📡 Data Feed → Smart Update** "
                "to load team data for accurate analysis."
            )
except Exception as _exc:
    logging.getLogger(__name__).warning(f"[App] Setup step failed: {_exc}")

# ── Data Validation on Startup ─────────────────────────────────
try:
    from data.validators import validate_players_csv, validate_teams_csv
    from data.data_manager import load_players_data as _lp_val, load_teams_data as _lt_val
    _val_players = _lp_val()
    _val_teams = _lt_val()
    _p_errors = validate_players_csv(_val_players)
    _t_errors = validate_teams_csv(_val_teams)
    if _p_errors or _t_errors:
        with st.expander("⚠️ Data Validation Issues (click to expand)", expanded=False):
            for e in _p_errors:
                st.warning(f"players.csv: {e}")
            for e in _t_errors:
                st.warning(f"teams.csv: {e}")
except Exception as _exc:
    logging.getLogger(__name__).warning(f"[App] Setup step failed: {_exc}")

# ── Teams/Defensive Ratings Staleness Warning (Feature 11) ───────
try:
    from data.nba_data_service import get_teams_staleness_warning
    _staleness_warn = get_teams_staleness_warning()
    if _staleness_warn:
        st.sidebar.warning(_staleness_warn)
except Exception as _exc:
    logging.getLogger(__name__).warning(f"[App] Setup step failed: {_exc}")

current_props = st.session_state.get("current_props", props_data)
number_of_current_props = len(current_props)
number_of_analysis_results = len(st.session_state.get("analysis_results", []))

# Live data status
live_data_timestamps = load_last_updated()
is_using_live_data = live_data_timestamps.get("is_live", False)
data_badge = '<span class="live-badge">LIVE</span>' if is_using_live_data else '<span class="sample-badge">📊 SAMPLE</span>'

st.subheader("📊 Status")

col1, col2, col3, col4, col5 = st.columns(5)

with col1:
    st.markdown(f"""
    <div class="status-card">
      <div class="status-card-value">{len(players_data)}</div>
      <div class="status-card-label">👤 Players</div>
    </div>
    """, unsafe_allow_html=True)

with col2:
    st.markdown(f"""
    <div class="status-card">
      <div class="status-card-value">{number_of_current_props}</div>
      <div class="status-card-label">🎯 Props</div>
    </div>
    """, unsafe_allow_html=True)

with col3:
    val3 = str(game_count) if game_count else "—"
    st.markdown(f"""
    <div class="status-card">
      <div class="status-card-value">{val3}</div>
      <div class="status-card-label">🏟️ Games Tonight</div>
    </div>
    """, unsafe_allow_html=True)

with col4:
    val4 = str(number_of_analysis_results) if number_of_analysis_results else "—"
    st.markdown(f"""
    <div class="status-card">
      <div class="status-card-value">{val4}</div>
      <div class="status-card-label">📈 Analyzed</div>
    </div>
    """, unsafe_allow_html=True)

with col5:
    st.markdown(f"""
    <div class="status-card">
      <div class="status-card-value" style="font-size:1.1rem; padding-top:8px;">{data_badge}</div>
      <div class="status-card-label">Data Source</div>
    </div>
    """, unsafe_allow_html=True)

st.divider()

# ============================================================
# END SECTION: Status Dashboard
# ============================================================

# ============================================================
# SECTION: Live Data Status Banner
# ============================================================

if is_using_live_data:
    player_ts = live_data_timestamps.get("players")
    team_ts = live_data_timestamps.get("teams")

    def format_timestamp(ts_string):
        if not ts_string:
            return "never"
        try:
            dt = datetime.datetime.fromisoformat(ts_string)
            return dt.strftime("%b %d at %I:%M %p")
        except Exception:
            return "unknown"

    st.success(
        f"✅ **Using Live NBA Data** — "
        f"Players: {format_timestamp(player_ts)} | "
        f"Teams: {format_timestamp(team_ts)}"
    )
else:
    st.info(
        "📊 **No live data loaded yet** — Go to the **📡 Data Feed** page to pull "
        "real, up-to-date NBA stats for accurate predictions!"
    )

# ============================================================
# END SECTION: Live Data Status Banner
# ============================================================

st.divider()

# ============================================================
# SECTION: Quick Start Guide
# ============================================================

left_column, right_column = st.columns([2, 1])

with left_column:
    st.subheader("🚀 Quick Start Guide")

    # ── ⚡ One-Click Setup button ─────────────────────────────────────
    _home_one_click = st.button(
        "⚡ One-Click Setup — Load Games + Get Live Props",
        key="home_one_click_btn",
        type="primary",
        help="Runs Auto-Load Tonight's Games AND Get Live Props from all platforms in one click. Best starting point!",
    )
    if _home_one_click:
        with st.spinner("⚡ Running One-Click Setup…"):
            try:
                from data.nba_data_service import get_todays_games as _hoc_fg, get_todays_players as _hoc_fp
                from data.data_manager import (
                    clear_all_caches as _hoc_cc,
                    load_injury_status as _hoc_li,
                )
                _hoc_games = _hoc_fg()
                if _hoc_games:
                    st.session_state["todays_games"] = _hoc_games
                else:
                    _hoc_games = st.session_state.get("todays_games", [])
                _hoc_fp(_hoc_games)
                _hoc_cc()
                try:
                    st.session_state["injury_status_map"] = _hoc_li()
                except Exception:
                    pass
                try:
                    from data.sportsbook_service import get_all_sportsbook_props as _hoc_fap
                    from data.data_manager import (
                        save_platform_props_to_session as _hoc_sps,
                        save_props_to_session as _hoc_sp,
                    )
                    _hoc_live = _hoc_fap(
                        include_prizepicks=True,
                        include_underdog=True,
                        include_draftkings=True,
                    )
                    if _hoc_live:
                        _hoc_sps(_hoc_live, st.session_state)
                        _hoc_sp(_hoc_live, st.session_state)
                        _hoc_total = len(st.session_state.get("current_props", []))
                        st.success(f"✅ Setup complete! {len(_hoc_games)} games · {_hoc_total} props loaded. Go to ⚡ Neural Analysis.")
                    else:
                        st.success(f"✅ Setup complete! {len(_hoc_games)} games loaded. Go to ⚡ Neural Analysis.")
                except Exception as _hoc_plat_err:
                    _hoc_err_str = str(_hoc_plat_err)
                    if "WebSocketClosedError" in _hoc_err_str or "StreamClosedError" in _hoc_err_str:
                        pass
                    else:
                        st.success(f"✅ Setup complete! {len(_hoc_games)} games loaded. Go to ⚡ Neural Analysis.")
            except Exception as _hoc_err:
                _hoc_err_str = str(_hoc_err)
                if "WebSocketClosedError" in _hoc_err_str or "StreamClosedError" in _hoc_err_str:
                    pass
                else:
                    st.error(f"❌ One-Click Setup failed: {_hoc_err}")

    st.markdown("""
    **Follow these steps to find tonight's best bets:**

    **Step 0** → 📡 **Live Games** — Click
    "**Auto-Load Tonight's Games**" for a ONE-CLICK setup:
    retrieves tonight's matchups + current rosters + player stats + team stats.
    Everything you need in a single button press!

    **Step 1** → 🔬 **Prop Scanner** — Enter prop lines manually, upload a CSV,
    or load live lines from all major sportsbooks via The Odds API.

    **Step 2** → ⚡ **Neural Analysis** — Click "Run Analysis" to run Quantum Matrix Engine 5.6
    simulation. See probability gauges, tier badges, and force breakdowns.

    **Step 3** → 🧬 **Entry Builder** — Build optimal parlays with exact EV
    calculations across FanDuel, DraftKings, BetMGM, Caesars, and more.

    **Step 4** → 📈 **Bet Tracker & Model Health** — After games, log results to track
    how accurate the model is over time.

    ---
    💎 **Premium features** (Entry Builder, Risk Shield, Game Report, Player Simulator,
    Bet Tracker, and unlimited analysis) require a subscription.
    Visit the **💎 Premium** page in the sidebar to unlock everything!
    """)

with right_column:
    st.subheader("⚙️ Current Settings")
    st.info(f"""
    **Simulations:** {st.session_state['simulation_depth']:,}

    **Min Edge:** {st.session_state['minimum_edge_threshold']}%

    **Entry Fee:** ${st.session_state['entry_fee']:.2f}

    **Platforms:** {', '.join(st.session_state['selected_platforms'])}
    """)
    st.caption("Change these on the ⚙️ Settings page")

# ============================================================
# END SECTION: Quick Start Guide
# ============================================================

# ============================================================
# SECTION: Legal Disclaimer
# ============================================================

with st.expander("⚠️ Important Legal Disclaimer — Please Read", expanded=False):
    st.markdown("""
    ## ⚠️ IMPORTANT DISCLAIMER
    
    **SmartBetPro NBA ("Smart Pick Pro")** is an analytical tool for **entertainment and educational purposes only**. This application does NOT guarantee profits or winning outcomes.
    
    - 📊 Past performance does not guarantee future results
    - 🔢 All predictions are based on statistical models that have inherent limitations  
    - 💰 Sports betting involves significant financial risk — **never bet more than you can afford to lose**
    - 🔞 You must be **21+** (or legal age in your jurisdiction) to participate in sports betting
    - ⚠️ This tool is **not affiliated** with the NBA or any sportsbook
    - 🆘 Always gamble responsibly. If you or someone you know has a gambling problem, call **1-800-GAMBLER (1-800-426-2537)**
    
    **By using this application, you acknowledge that all betting decisions are your own responsibility.**
    
    ---
    
    **Responsible Gaming Resources:**
    - 📞 **National Problem Gambling Helpline: 1-800-GAMBLER (1-800-426-2537)** — 24/7 confidential support
    - 📞 National Council on Problem Gambling: **1-800-522-4700** — crisis counseling & referrals
    - 🌐 [www.ncpgambling.org](https://www.ncpgambling.org)
    - 🌐 [www.begambleaware.org](https://www.begambleaware.org)
    """)

# ============================================================
# END SECTION: Legal Disclaimer
# ============================================================

st.divider()

# ============================================================
# SECTION: How It Works
# ============================================================

with st.expander("📖 How Does Smart Pick Pro Work?", expanded=False):
    st.markdown("""
    ### The Engine Under the Hood

    Smart Pick Pro — NBA Edition uses **Quantum Matrix Engine 5.6 simulation** to predict player stat outcomes.
    Here's what happens when you click "Run Analysis":

    ---

    #### 1. 📐 Projection Building
    For each player's stat, we take their season average and adjust it for:
    - **Opponent defense** — tough defenders reduce expected output
    - **Game pace** — faster games = more stat opportunities
    - **Home/away** — home court advantage is real
    - **Rest** — back-to-back games cause fatigue

    #### 2. 🎲 Quantum Matrix Engine 5.6 Simulation
    We simulate **1,000+ games** for each player. In each simulated game:
    - Minutes are randomized (blowout risk, foul trouble)
    - Stats are randomly drawn from a normal distribution
    - The result is recorded (over or under the line?)

    After 1,000 games, the % of games that went over the line = our probability.

    #### 3. 🔍 Force Analysis
    We identify all factors pushing the stat **MORE** or **LESS**:
    - Weak defense? → OVER force
    - Back-to-back game? → UNDER force
    - The count and strength of forces determines confidence.

    #### 4. 🏆 Confidence Scoring
    A weighted 0-100 score combines probability strength, edge size,
    directional agreement, matchup quality, and player consistency.
    This gives you **Platinum/Gold/Silver/Bronze** tiers.

    #### 5. 💰 EV Calculation
    For parlays, we calculate **Expected Value** = exactly how much
    you'd win (or lose) on average per dollar wagered.

    ---

    #### 🎯 Smart Data Loading (New!)
    The "Update Data" page now has a **Smart Update** option that only loads
    players on today's teams using `CommonTeamRoster` — which reflects all
    trades and signings. This is 10x faster than the full update!
    """)

st.divider()

# ── System Health Dashboard ───────────────────────────────────────────────────
try:
    from data.data_manager import get_data_health_report
    _health = get_data_health_report()

    with st.expander("🩺 System Health", expanded=False):
        hc1, hc2, hc3, hc4 = st.columns(4)
        hc1.metric("👥 Players", _health["players_count"])
        hc2.metric("🏀 Teams", _health["teams_count"])
        hc3.metric("📋 Props", _health["props_count"])
        _freshness_label = f"{_health['days_old']}d old" if _health.get("last_updated") else "Never"
        hc4.metric("🕐 Data Age", _freshness_label,
                   delta="⚠️ Stale" if _health["is_stale"] else "✅ Fresh",
                   delta_color="inverse" if _health["is_stale"] else "normal")

        if _health["warnings"]:
            for w in _health["warnings"]:
                st.warning(w)
        else:
            st.success("✅ All data files present and fresh.")

        st.caption("Go to **📡 Data Feed** to refresh data.")
except Exception as _exc:
    logging.getLogger(__name__).warning(f"[App] Setup step failed: {_exc}")

st.caption(
    f"© Smart Pick Pro | {datetime.datetime.now().strftime('%Y-%m-%d %H:%M')} | "
    f"{len(players_data)} players, {len(teams_data)} teams | "
    "For entertainment & educational purposes only. Not financial advice. Bet responsibly. 21+"
)
