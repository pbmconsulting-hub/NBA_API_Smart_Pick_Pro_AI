# ============================================================
# FILE: pages/9_📡_Data_Feed.py
# PURPOSE: Streamlit page that lets the user retrieve live NBA data
#          from the API-NBA API and The Odds API. Updates player
#          stats, team stats, standings, and today's games with real,
#          current data.
# CONNECTS TO: data/nba_data_service.py, data/data_manager.py
# CONCEPTS COVERED: Progress bars, API calls, session state, error handling
#
# BEGINNER NOTE: This page is your "data refresh" control panel.
# Click a button to pull live stats from API-NBA and The Odds API.
# After updating, all the other pages in the app will use the fresh data!
# ============================================================

# Import streamlit — our UI framework
import streamlit as st

# Standard library imports
import datetime  # For formatting timestamps
import json      # For reading the last_updated.json file

# Import our data loading function (to preview data after loading)
from data.data_manager import (
    load_players_data,     # Load player stats from CSV
    load_teams_data,       # Load team stats from CSV
)

# Import our live data service functions
# These functions call API-NBA API for roster/stats/injury data
# and The Odds API for sportsbook lines.
from data.nba_data_service import (
    get_todays_games,          # Retrieve tonight's NBA games
    get_player_stats,          # Retrieve all player season averages
    get_team_stats,            # Retrieve all team stats + defensive ratings
    get_all_data,              # Retrieve everything at once
    get_todays_players,   # Targeted: only today's team rosters
    get_all_todays_data,       # One-click: games + players + teams
    load_last_updated,           # Load timestamps from last_updated.json
)

# ETL refresh functions — use pre-populated SQLite DB instead of live API calls
try:
    from data.nba_data_service import refresh_from_etl, full_refresh_from_etl
    _ETL_AVAILABLE = True
except ImportError:
    _ETL_AVAILABLE = False

# ETL database status
try:
    from data.etl_data_service import is_db_available, get_db_counts
    _ETL_DB_AVAILABLE = is_db_available()
    _ETL_DB_COUNTS = get_db_counts() if _ETL_DB_AVAILABLE else {}
except Exception:
    _ETL_DB_AVAILABLE = False
    _ETL_DB_COUNTS = {}

# ============================================================
# SECTION: Page Setup
# ============================================================

# Configure the page (MUST be the first streamlit call)
st.set_page_config(
    page_title="Data Feed — SmartBetPro NBA",
    page_icon="📡",
    layout="wide",  # Use full-width layout
)

# ─── Inject Global CSS Theme ──────────────────────────────────
from styles.theme import get_global_css, get_education_box_html
st.markdown(get_global_css(), unsafe_allow_html=True)

# ── Joseph M. Smith Floating Widget ────────────────────────────
from utils.components import inject_joseph_floating
st.session_state["joseph_page_context"] = "page_data_feed"
inject_joseph_floating()

# Page title and description
st.title("📡 Data Feed")
st.markdown(
    "Pull real, up-to-date NBA stats and player prop lines "
    "from PrizePicks, Underdog Fantasy, and DraftKings Pick6. "
    "Update before each betting session for the most accurate predictions!"
)

with st.expander("📖 How to Use This Page", expanded=False):
    st.markdown("""
    ### Data Feed — Keep Your Data Fresh
    
    The Data Feed connects to **live NBA data sources** to keep your analysis accurate and current.
    
    **Recommended Daily Workflow**
    1. **Smart Update**: Click this before each betting session — retrieves stats for tonight's teams only
    2. **Full Update**: Run once a day or once a week to refresh all 450+ NBA player stats
    3. **Get Props**: After updating stats, load live prop lines from sportsbooks
    
    **Data Sources**
    - Real-time NBA player stats, team metrics, and game logs
    - Live odds from PrizePicks, Underdog Fantasy, and DraftKings Pick6
    
    💡 **Pro Tips:**
    - Always update data BEFORE running analysis — stale data leads to bad predictions
    - The status cards at the top show when each data source was last refreshed
    - Use "Refresh Game Logs" for backtesting — it pulls 10-20 game historical logs per player
    """)

st.divider()

st.markdown(get_education_box_html(
    "📖 How Data Updates Work",
    """
    <strong>Smart Update (Recommended)</strong>: Only retrieves players on tonight's teams. 
    Fast and efficient — use this before each session.<br><br>
    <strong>Full Update</strong>: Retrieves all NBA player season stats. 
    Use this once a day or week to keep averages current.<br><br>
    <strong>Live data</strong>: Real current season stats,
    saved to players.csv and teams.csv. Load before each session for accurate predictions.<br><br>
    <strong>Prop lines</strong>: Covers PrizePicks, 
    Underdog Fantasy, and DraftKings Pick6.
    """
), unsafe_allow_html=True)

# ============================================================
# END SECTION: Page Setup
# ============================================================


# ============================================================
# SECTION: Data Source Status
# API-NBA API and The Odds API are now the primary data sources.
# ============================================================

# RosterEngine uses API-NBA API.
_roster_engine_available = True


# ============================================================
# SECTION: Data Status Display
# Show when each data type was last updated (so user knows
# if data is fresh or stale).
# ============================================================

st.subheader("📅 Data Status")

# Load the timestamps from the JSON file
timestamps = load_last_updated()

# ── Staleness helpers ──────────────────────────────────────────
def _staleness_badge(timestamp_str: str | None, warn_hours: float = 4.0, error_hours: float = 24.0):
    """Return (badge_html, age_hours) for a data timestamp."""
    if not timestamp_str:
        return '<span style="background:#553c9a;color:#e9d8fd;padding:2px 8px;border-radius:4px;font-size:0.75rem;font-weight:700;">NEVER</span>', None
    try:
        dt = datetime.datetime.fromisoformat(timestamp_str)
        age_h = (datetime.datetime.now() - dt).total_seconds() / 3600
        if age_h < warn_hours:
            color, label = "#276749", f"FRESH ({age_h:.0f}h ago)"
            text_color = "#9ae6b4"
        elif age_h < error_hours:
            color, label = "#744210", f"AGING ({age_h:.0f}h ago)"
            text_color = "#fbd38d"
        else:
            color, label = "#742a2a", f"STALE ({age_h:.1f}h ago)"
            text_color = "#feb2b2"
        badge = f'<span style="background:{color};color:{text_color};padding:2px 8px;border-radius:4px;font-size:0.75rem;font-weight:700;">{label}</span>'
        return badge, age_h
    except Exception:
        return '<span style="background:#553c9a;color:#e9d8fd;padding:2px 8px;border-radius:4px;font-size:0.75rem;font-weight:700;">UNKNOWN</span>', None


def _health_bar(age_h: float | None, max_age: float = 24.0) -> str:
    """Return a colored health bar HTML string."""
    if age_h is None:
        pct, color = 0, "#742a2a"
    else:
        freshness = max(0.0, 1.0 - age_h / max_age)
        pct = round(freshness * 100)
        color = "#00ff9d" if pct > 70 else ("#ffcc00" if pct > 30 else "#ff4444")
    return (
        f'<div style="height:6px;background:#1a2035;border-radius:3px;margin:6px 0;">'
        f'<div style="height:6px;width:{pct}%;background:{color};border-radius:3px;'
        f'transition:width 0.4s ease;"></div>'
        f'</div>'
        f'<div style="font-size:0.72rem;color:#b0bec5;">Health: {pct}%</div>'
    )

# ── Data health cards ──────────────────────────────────────────
todays_games = st.session_state.get("todays_games", [])

_data_sources = [
    ("👤 Players",       timestamps.get("players"),  6.0,  "player stats / game logs"),
    ("🏟️ Teams",        timestamps.get("teams"),     12.0, "team stats / defensive ratings"),
    ("🏀 Tonight's Games", None if not todays_games else datetime.datetime.now().isoformat(), 4.0, f"{len(todays_games)} game(s) in session"),
    ("🏥 Injuries",      timestamps.get("injuries"),  4.0,  "injury report / roster status"),
]

_health_cols = st.columns(4)
for _ci, (_label, _ts, _warn_h, _desc) in enumerate(_data_sources):
    with _health_cols[_ci]:
        badge_html, age_h = _staleness_badge(_ts, warn_hours=_warn_h)
        health_html = _health_bar(age_h, max_age=24.0)
        st.markdown(
            f'<div style="background:#14192b;border-radius:8px;padding:14px 16px;'
            f'border:1px solid rgba(0,240,255,0.15);">'
            f'<div style="font-size:0.95rem;font-weight:700;color:#c0d0e8;margin-bottom:6px;">{_label}</div>'
            f'{badge_html}'
            f'{health_html}'
            f'<div style="color:#8b949e;font-size:0.75rem;margin-top:4px;">{_desc}</div>'
            f'</div>',
            unsafe_allow_html=True,
        )

# ── Staleness warning ──────────────────────────────────────────
try:
    from data.nba_data_service import get_teams_staleness_warning
    _stale_warning = get_teams_staleness_warning()
    if _stale_warning:
        st.warning(f"⏰ {_stale_warning}")
except Exception:
    pass

st.divider()

# ============================================================
# END SECTION: Data Status Display
# ============================================================


# ============================================================
# SECTION: ETL Database Status
# Shows the state of the local SQLite database populated by
# the ETL pipeline (scripts/initial_pull.py / data_updater.py).
# ============================================================

st.subheader("🗄️ ETL Database (Local SQLite)")

_etl_status_cols = st.columns([1, 3])
with _etl_status_cols[0]:
    if _ETL_DB_AVAILABLE:
        st.success("✅ Database ready")
        _p = _ETL_DB_COUNTS.get("players", 0)
        _g = _ETL_DB_COUNTS.get("games",   0)
        _l = _ETL_DB_COUNTS.get("logs",    0)
        st.caption(f"👤 **{_p:,}** players  |  🏀 **{_g:,}** games  |  📋 **{_l:,}** logs")
    else:
        st.warning("⚠️ Database empty")
        st.caption("Run `python scripts/initial_pull.py` to populate.")

with _etl_status_cols[1]:
    st.markdown("""
**ETL Database** is the fastest, most reliable data source.
- **Smart ETL Update** — pulls only game logs added since the last stored date (~30 seconds).
- **Full ETL Pull** — re-fetches the entire season and repopulates the database (~60 seconds).

Both options avoid live per-player API calls.  Use these **before** each session for fresh stats.
""")

_etl_btn_cols = st.columns([1, 1, 2])
with _etl_btn_cols[0]:
    if st.button(
        "⚡ Smart ETL Update",
        type="primary",
        help="Incremental update — fetches only new games since the last stored date",
        key="etl_smart_btn",
    ):
        st.session_state["update_action"] = "etl_smart"

with _etl_btn_cols[1]:
    if st.button(
        "🔄 Full ETL Pull",
        help="Re-pull entire season from nba_api and repopulate db/etl_data.db",
        key="etl_full_btn",
    ):
        st.session_state["update_action"] = "etl_full"

st.divider()

# ============================================================
# END SECTION: ETL Database Status
# ============================================================


# ============================================================
# SECTION: Update Action Buttons
# Four buttons: players, teams, games, or everything at once.
# ============================================================

st.subheader("🔧 Update Data")

# ─── One-Click Full Setup (recommended) ─────────────────────────────
st.markdown("""
<div style="background:linear-gradient(135deg,#0f3460,#533483); border:2px solid #e94560; border-radius:10px; padding:16px 20px; margin-bottom:16px;">
  <div style="font-size:1.1rem; font-weight:700; color:#ffffff;">🏀 One-Click Full Setup (Best Choice)</div>
  <div style="color:rgba(255,255,255,0.8); font-size:0.9rem; margin-top:4px;">
    Retrieves tonight's games → current rosters for those teams → player stats → team stats.
    <strong>Everything in one click.</strong> Same as clicking Auto-Load on the Today's Games page.
  </div>
</div>
""", unsafe_allow_html=True)

one_click_col1, one_click_col2 = st.columns([1, 3])
with one_click_col1:
    if st.button(
        "🏀 One-Click Full Setup",
        width="stretch",
        type="primary",
        help="Games + rosters + player stats + team stats — all in one click",
    ):
        st.session_state["update_action"] = "one_click"

with one_click_col2:
    st.caption(
        "Best for first-time setup each day. "
        "Retrieves games first, then only the players on tonight's teams (~1-3 min total)."
    )

st.markdown("---")

# ─── Smart Update (recommended) ─────────────────────────────
st.markdown("""
<div style="background:linear-gradient(135deg,#1a1a2e,#16213e); border:1px solid #0f3460; border-radius:10px; padding:16px 20px; margin-bottom:16px;">
  <div style="font-size:1.05rem; font-weight:700; color:#e2e8f0;">⚡ Smart Update — Today's Teams Only</div>
  <div style="color:#a0aec0; font-size:0.9rem; margin-top:4px;">
    Retrieves team rosters using <code>CommonTeamRoster</code> (current, post-trade) 
    then game logs for only those players. Requires games to already be loaded.
    Takes <strong>1–2 minutes</strong> instead of 10–15.
  </div>
</div>
""", unsafe_allow_html=True)

smart_col1, smart_col2 = st.columns([1, 3])
with smart_col1:
    if st.button(
        "⚡ Smart Update (Today's Teams Only)",
        width="stretch",
        help="Fastest: retrieves only players on teams playing tonight using current rosters",
    ):
        st.session_state["update_action"] = "smart"

with smart_col2:
    todays_games_for_hint = st.session_state.get("todays_games", [])
    if todays_games_for_hint:
        teams_playing = set()
        for g in todays_games_for_hint:
            teams_playing.add(g.get("home_team", ""))
            teams_playing.add(g.get("away_team", ""))
        teams_playing.discard("")
        st.caption(f"Tonight's teams: {', '.join(sorted(teams_playing))}")
    else:
        st.caption("⚠️ Load tonight's games first (🏀 Today's Games page) for Smart Update to work.")

st.markdown("---")
st.markdown("**Full Updates (all 30 teams, takes 10–15 minutes):**")

btn_col1, btn_col2, btn_col3, btn_col4 = st.columns(4)

if "update_action" not in st.session_state:
    st.session_state["update_action"] = None

with btn_col1:
    if st.button(
        "🏟️ Get Tonight's Games",
        width="stretch",
        help="Pull tonight's real NBA matchups automatically",
    ):
        st.session_state["update_action"] = "games"

with btn_col2:
    if st.button(
        "👤 Update Player Stats (All — Slow)",
        width="stretch",
        help="Pull current season averages for ALL NBA players (~500, 5-15 min)",
    ):
        st.session_state["update_action"] = "players"

with btn_col3:
    if st.button(
        "🏆 Update Team Stats",
        width="stretch",
        help="Pull team pace, offensive rating, and defensive rating",
    ):
        st.session_state["update_action"] = "teams"

with btn_col4:
    if st.button(
        "🔄 Update Everything (Full)",
        width="stretch",
        help="Update all data: games, all players, and teams (slow)",
    ):
        st.session_state["update_action"] = "all"

st.markdown("---")

# ─── Injury Report Section ──────────────────────────────────
st.markdown("""
<div style="background:linear-gradient(135deg,#1a0a2e,#0f1a2e); border:1px solid #c800ff; border-radius:10px; padding:16px 20px; margin-bottom:16px;">
  <div style="font-size:1.05rem; font-weight:700; color:#e2e8f0;">🏥 Real-Time Injury Report</div>
  <div style="color:#a0aec0; font-size:0.9rem; margin-top:4px;">
    Retrieves live injury designations 
    with NBA CDN feed as fallback —
    real-time GTD/Out/Doubtful status, specific injury details, and expected return dates.
  </div>
</div>
""", unsafe_allow_html=True)

injury_btn_col1, injury_btn_col2 = st.columns([1, 3])
with injury_btn_col1:
    _injury_btn_disabled = not _roster_engine_available
    if st.button(
        "🔄 Refresh Injury Report",
        width="stretch",
        help="Load live GTD/Out/injury data with NBA CDN as fallback",
        disabled=_injury_btn_disabled,
    ):
        st.session_state["update_action"] = "injury_report"

with injury_btn_col2:
    if _roster_engine_available:
        # Show last-retrieved timestamp if available
        _last_scraped = st.session_state.get("injury_report_last_scraped")
        if _last_scraped:
            st.caption(f"Last retrieved: {_last_scraped}")
        else:
            st.caption("Click to pull real-time injury designations.")
    else:
        st.caption(
            "⚠️ RosterEngine unavailable. "
            "Check that data/roster_engine.py is present."
        )

st.markdown("---")

# ─── Standings & News Section ────────────────────────────────
st.markdown("""
<div style="background:linear-gradient(135deg,#0a1628,#0f2040); border:1px solid #00c9ff; border-radius:10px; padding:16px 20px; margin-bottom:16px;">
  <div style="font-size:1.05rem; font-weight:700; color:#e2e8f0;">📊 NBA Standings & Player News</div>
  <div style="color:#a0aec0; font-size:0.9rem; margin-top:4px;">
    Retrieves current NBA standings and recent player/team news —
    conference ranks, W-L records,
    streaks, injury news, and trade updates.
  </div>
</div>
""", unsafe_allow_html=True)

sn_col1, sn_col2 = st.columns([1, 3])
with sn_col1:
    if st.button(
        "📊 Refresh Standings & News",
        use_container_width=True,
        help="Load NBA standings and recent news",
    ):
        st.session_state["update_action"] = "standings_news"

with sn_col2:
    _last_sn = st.session_state.get("standings_news_last_loaded")
    if _last_sn:
        st.caption(f"Last retrieved: {_last_sn}")
    else:
        st.caption(
            "Retrieves conference standings (rank, W-L, home/away splits, last-10, streak) "
            "and recent player/team news."
        )

# ============================================================
# END SECTION: Update Action Buttons
# ============================================================


# ============================================================
# SECTION: Execute the Selected Action
# Based on which button was clicked, run the appropriate service.
# ============================================================

# Get the current action (set by button clicks above)
current_action = st.session_state.get("update_action")

# Only run if an action was selected
if current_action:
    st.divider()

    # --------------------------------------------------------
    # Action: Smart ETL Update (incremental)
    # --------------------------------------------------------
    if current_action == "etl_smart":
        st.subheader("⚡ Smart ETL Update")
        progress_bar = st.progress(0, text="Starting incremental ETL update…")
        status_text = st.empty()

        def _etl_smart_progress(current, total, message):
            frac = current / max(total, 1)
            progress_bar.progress(frac, text=message)
            status_text.caption(message)

        try:
            with st.spinner("Fetching new game logs from nba_api…"):
                result = refresh_from_etl(progress_callback=_etl_smart_progress)

            st.session_state["update_action"] = None
            ng = result.get("new_games", 0)
            nl = result.get("new_logs",  0)
            err = result.get("error")

            if err:
                st.error(f"❌ Smart ETL Update failed: {err}")
            else:
                st.success(
                    f"✅ Smart ETL Update complete! "
                    f"**{ng}** new game(s) · **{nl}** new log row(s) added to db/etl_data.db."
                )
                if ng == 0 and nl == 0:
                    st.info("ℹ️ Database is already up to date — no new games since last update.")
                # Show updated counts
                try:
                    from data.etl_data_service import get_db_counts
                    counts = get_db_counts()
                    st.caption(
                        f"DB now has **{counts['players']:,}** players · "
                        f"**{counts['games']:,}** games · **{counts['logs']:,}** logs"
                    )
                except Exception:
                    pass
        except Exception as _etl_err:
            st.session_state["update_action"] = None
            st.error(f"❌ Smart ETL Update failed: {_etl_err}")
        finally:
            try:
                progress_bar.empty()
                status_text.empty()
            except Exception:
                pass

    # --------------------------------------------------------
    # Action: Full ETL Pull (re-seed entire season)
    # --------------------------------------------------------
    elif current_action == "etl_full":
        st.subheader("🔄 Full ETL Pull")
        st.info(
            "⏳ This re-fetches the entire 2025-26 season from nba_api. "
            "Takes approximately 30–60 seconds."
        )
        progress_bar = st.progress(0, text="Starting full ETL pull…")
        status_text = st.empty()

        def _etl_full_progress(current, total, message):
            frac = current / max(total, 1)
            progress_bar.progress(frac, text=message)
            status_text.caption(message)

        try:
            with st.spinner("Pulling full season game logs…"):
                result = full_refresh_from_etl(progress_callback=_etl_full_progress)

            st.session_state["update_action"] = None
            pi = result.get("players_inserted", 0)
            gi = result.get("games_inserted",   0)
            li = result.get("logs_inserted",    0)
            err = result.get("error")

            if err:
                st.error(f"❌ Full ETL Pull failed: {err}")
            else:
                st.success(
                    f"✅ Full ETL Pull complete! "
                    f"db/etl_data.db now has **{pi:,}** players · "
                    f"**{gi:,}** games · **{li:,}** logs."
                )
        except Exception as _etl_err:
            st.session_state["update_action"] = None
            st.error(f"❌ Full ETL Pull failed: {_etl_err}")
        finally:
            try:
                progress_bar.empty()
                status_text.empty()
            except Exception:
                pass

    # --------------------------------------------------------
    # Action: One-Click Full Setup
    # --------------------------------------------------------
    elif current_action == "one_click":
        st.subheader("🏀 One-Click Full Setup")

        progress_bar = st.progress(0, text="Starting one-click setup...")
        status_text = st.empty()

        def one_click_progress(current, total, message):
            frac = current / max(total, 1)
            progress_bar.progress(frac, text=message)
            status_text.caption(message)

        try:
            with st.spinner("🏀 Loading games + rosters + player stats + team stats..."):
                result = get_all_todays_data(progress_callback=one_click_progress)

            st.session_state["update_action"] = None

            games = result.get("games", [])
            players_ok = result.get("players_updated", False)
            teams_ok = result.get("teams_updated", False)

            if games:
                st.session_state["todays_games"] = games
                from data.data_manager import load_players_data
                updated_players = load_players_data()
                st.success(
                    f"✅ One-Click Setup complete! "
                    f"**{len(games)} game(s)** loaded | "
                    f"**{len(updated_players)} players** retrieved | "
                    f"Teams: {'✅' if teams_ok else '⚠️ failed'}"
                )

                # Show bonus data enrichment status (standings, news, historical data)
                _bonus_parts = []
                if result.get("standings"):
                    _bonus_parts.append(f"📊 Standings loaded ({len(result['standings'])} teams)")
                if result.get("news"):
                    _bonus_parts.append(f"📰 News loaded ({len(result['news'])} items)")
                if _bonus_parts:
                    st.caption("**Bonus data auto-enriched:** " + " · ".join(_bonus_parts))
            else:
                st.warning(
                    "⚠️ Could not retrieve tonight's games (no games tonight, or data unavailable). "
                    "Try again or load games manually on the 🏀 Today's Games page."
                )
        except Exception as _oc_err:
            st.session_state["update_action"] = None
            _oc_err_str = str(_oc_err)
            if "WebSocketClosedError" not in _oc_err_str and "StreamClosedError" not in _oc_err_str:
                st.error(f"❌ One-Click Setup failed: {_oc_err}")
        finally:
            try:
                progress_bar.empty()
                status_text.empty()
            except Exception:
                pass

    # --------------------------------------------------------
    # Action: Smart Update (today's teams only)
    # --------------------------------------------------------
    elif current_action == "smart":
        st.subheader("⚡ Smart Update — Today's Teams Only")

        todays_games_for_smart = st.session_state.get("todays_games", [])

        if not todays_games_for_smart:
            st.warning(
                "⚠️ No games loaded yet. Please go to 🏀 **Today's Games** first and "
                "click 'Auto-Load Tonight's Games', then come back here."
            )
            st.session_state["update_action"] = None
        else:
            # Show which teams we'll retrieve
            teams_set = set()
            for g in todays_games_for_smart:
                teams_set.add(g.get("home_team", ""))
                teams_set.add(g.get("away_team", ""))
            teams_set.discard("")
            st.info(f"Loading current rosters for: **{', '.join(sorted(teams_set))}**")

            progress_bar = st.progress(0, text="Starting smart update...")
            status_text = st.empty()

            def smart_progress(current, total, message):
                frac = current / max(total, 1)
                progress_bar.progress(frac, text=message)
                status_text.caption(message)

            try:
                with st.spinner("Loading today's team rosters and player stats..."):
                    success = get_todays_players(
                        todays_games_for_smart,
                        progress_callback=smart_progress
                    )

                st.session_state["update_action"] = None

                if success:
                    from data.data_manager import load_players_data
                    updated_players = load_players_data()
                    st.success(
                        f"✅ Smart Update complete! Loaded **{len(updated_players)} players** "
                        f"from today's {len(todays_games_for_smart)} game(s). "
                        f"Only current roster players — no traded players!"
                    )
                    st.caption(f"Teams retrieved: {', '.join(sorted(teams_set))}")
                else:
                    st.error(
                        "❌ Smart Update failed. Check your internet connection or try again.\n"
                        "You can still use the full 'Update Player Stats' button as a fallback."
                    )
            except Exception as _smart_err:
                st.session_state["update_action"] = None
                _smart_err_str = str(_smart_err)
                if "WebSocketClosedError" not in _smart_err_str and "StreamClosedError" not in _smart_err_str:
                    st.error(f"❌ Smart Update failed: {_smart_err}")
            finally:
                try:
                    progress_bar.empty()
                    status_text.empty()
                except Exception:
                    pass

    # --------------------------------------------------------
    # Action: Get Tonight's Games
    # --------------------------------------------------------
    elif current_action == "games":
        st.subheader("🏟️ Loading Tonight's Games...")

        try:
            # Show a spinner while we load
            with st.spinner("Loading game data…"):
                todays_games = get_todays_games()

            # Check if we got any games
            if todays_games:
                st.session_state["todays_games"] = todays_games
                st.session_state["update_action"] = None

                st.success(f"✅ Found **{len(todays_games)} game(s)** for tonight!")
                st.info(
                    "💡 Vegas lines and totals are retrieved from consensus data. "
                    "You can also edit them on the **🏀 Today's Games** page."
                )

                st.markdown("**Tonight's Matchups:**")

                games_display = []
                for game in todays_games:
                    games_display.append({
                        "Away Team": game.get("away_team", ""),
                        "Home Team": game.get("home_team", ""),
                        "Game Date": game.get("game_date", ""),
                        "Total (O/U)": game.get("consensus_total") or game.get("game_total", ""),
                        "Spread": game.get("consensus_spread") or game.get("vegas_spread", ""),
                    })

                st.dataframe(games_display, width="stretch", hide_index=True)

            else:
                st.session_state["update_action"] = None

                st.warning(
                    "⚠️ No games found for tonight, or there was a data error. "
                    "\n\nPossible reasons:\n"
                    "- No NBA games are scheduled today\n"
                    "- Check your internet connection\n\n"
                    "You can still enter games manually on the **🏀 Today's Games** page."
                )
        except Exception as _games_err:
            st.session_state["update_action"] = None
            _games_err_str = str(_games_err)
            if "WebSocketClosedError" not in _games_err_str and "StreamClosedError" not in _games_err_str:
                st.error(f"❌ Failed to load games: {_games_err}")

    # --------------------------------------------------------
    # Action: Update Player Stats
    # --------------------------------------------------------
    elif current_action == "players":
        st.subheader("👤 Updating Player Stats...")

        st.info(
            "⏳ **This takes a few minutes.** We retrieve stats for every player "
            "and then download game logs to calculate standard deviations. "
            "Please be patient!"
        )

        progress_bar = st.progress(0)
        status_text = st.empty()

        def update_player_progress(current, total, message):
            """Update the progress bar and status text."""
            fraction = min(current / max(total, 1), 1.0)
            progress_bar.progress(fraction)
            status_text.text(f"⏳ {message}")

        try:
            success = get_player_stats(progress_callback=update_player_progress)

            st.session_state["update_action"] = None

            if success:
                progress_bar.progress(1.0)
                status_text.text("✅ Done!")

                st.success("✅ **Player stats updated successfully!**")

                st.markdown("**Updated Player Data (first 20 rows):**")
                updated_players = load_players_data()

                if updated_players:
                    players_display = []
                    for player in updated_players[:20]:
                        players_display.append({
                            "Name": player.get("name", ""),
                            "Team": player.get("team", ""),
                            "Pos": player.get("position", ""),
                            "MIN": player.get("minutes_avg", ""),
                            "PTS": player.get("points_avg", ""),
                            "REB": player.get("rebounds_avg", ""),
                            "AST": player.get("assists_avg", ""),
                            "3PM": player.get("threes_avg", ""),
                        })

                    st.dataframe(players_display, width="stretch", hide_index=True)
                    st.caption(f"Showing 20 of {len(updated_players)} players. Full data saved to players.csv")
            else:
                st.error(
                    "❌ **Failed to update player stats.**\n\n"
                    "Possible reasons:\n"
                    "- No internet connection\n"
                    "- Try again in a few minutes\n\n"
                    "The app will continue to use the existing data until a successful update."
                )
        except Exception as _player_err:
            st.session_state["update_action"] = None
            _player_err_str = str(_player_err)
            if "WebSocketClosedError" not in _player_err_str and "StreamClosedError" not in _player_err_str:
                st.error(f"❌ Player stats update failed: {_player_err}")
        finally:
            try:
                progress_bar.empty()
                status_text.empty()
            except Exception:
                pass

    # --------------------------------------------------------
    # Action: Update Team Stats
    # --------------------------------------------------------
    elif current_action == "teams":
        st.subheader("🏆 Updating Team Stats...")

        progress_bar = st.progress(0)
        status_text = st.empty()

        def update_team_progress(current, total, message):
            """Update the progress bar for team stats."""
            fraction = min(current / max(total, 1), 1.0)
            progress_bar.progress(fraction)
            status_text.text(f"⏳ {message}")

        try:
            with st.spinner("Loading team data..."):
                success = get_team_stats(progress_callback=update_team_progress)

            st.session_state["update_action"] = None

            if success:
                progress_bar.progress(1.0)
                status_text.text("✅ Done!")

                st.success("✅ **Team stats updated successfully!**")

                st.markdown("**Updated Team Data:**")
                updated_teams = load_teams_data()

                if updated_teams:
                    teams_display = []
                    for team in updated_teams:
                        teams_display.append({
                            "Team": team.get("team_name", ""),
                            "Abbrev": team.get("abbreviation", ""),
                            "Conf": team.get("conference", ""),
                            "Pace": team.get("pace", ""),
                            "ORTG": team.get("ortg", ""),
                            "DRTG": team.get("drtg", ""),
                        })

                    st.dataframe(teams_display, width="stretch", hide_index=True)
                    st.caption(f"All {len(updated_teams)} teams saved to teams.csv and defensive_ratings.csv")
            else:
                st.error(
                    "❌ **Failed to update team stats.**\n\n"
                    "Check your internet connection and try again."
                )
        except Exception as _team_err:
            st.session_state["update_action"] = None
            _team_err_str = str(_team_err)
            if "WebSocketClosedError" not in _team_err_str and "StreamClosedError" not in _team_err_str:
                st.error(f"❌ Team stats update failed: {_team_err}")
        finally:
            try:
                progress_bar.empty()
                status_text.empty()
            except Exception:
                pass

    # --------------------------------------------------------
    # Action: Update Everything
    # --------------------------------------------------------
    elif current_action == "all":
        st.subheader("🔄 Updating All Data...")

        st.info(
            "⏳ **This may take several minutes.** We're retrieving player stats, "
            "team stats, and game logs for standard deviation calculations. "
            "Please wait — don't close the tab!"
        )

        progress_bar = st.progress(0)
        status_text = st.empty()

        def update_all_progress(current, total, message):
            """Update progress bar for full update."""
            fraction = min(current / max(total, 1), 1.0)
            progress_bar.progress(fraction)
            status_text.text(f"⏳ {message}")

        try:
            results = get_all_data(progress_callback=update_all_progress)

            st.session_state["update_action"] = None

            progress_bar.progress(1.0)
            status_text.text("✅ Update complete!")

            players_ok = results.get("players", False)
            teams_ok = results.get("teams", False)

            if players_ok and teams_ok:
                st.success("✅ **All data updated successfully!**")
            elif players_ok or teams_ok:
                st.warning(
                    "⚠️ **Partial update completed.**\n"
                    f"Players: {'✅ Success' if players_ok else '❌ Failed'}\n"
                    f"Teams: {'✅ Success' if teams_ok else '❌ Failed'}"
                )
            else:
                st.error(
                    "❌ **Update failed for all data types.**\n\n"
                    "Check your internet connection and try again."
                )

            if players_ok:
                updated_players = load_players_data()
                st.metric(
                    label="👤 Players Updated",
                    value=len(updated_players),
                    help="Players now in players.csv"
                )

            if teams_ok:
                updated_teams = load_teams_data()
                st.metric(
                    label="🏆 Teams Updated",
                    value=len(updated_teams),
                    help="Teams now in teams.csv"
                )

            st.markdown("---")
            st.markdown("**Loading tonight's games...**")

            with st.spinner("Loading tonight's games..."):
                todays_games = get_todays_games()

            if todays_games:
                st.session_state["todays_games"] = todays_games
                st.success(f"🏟️ Found **{len(todays_games)} game(s)** for tonight!")
            else:
                st.info("No games found for tonight (or no games scheduled). Enter games manually on the 🏀 Today's Games page.")
        except Exception as _all_err:
            st.session_state["update_action"] = None
            _all_err_str = str(_all_err)
            if "WebSocketClosedError" not in _all_err_str and "StreamClosedError" not in _all_err_str:
                st.error(f"❌ Full update failed: {_all_err}")
        finally:
            try:
                progress_bar.empty()
                status_text.empty()
            except Exception:
                pass

    # --------------------------------------------------------
    # Action: Refresh Injury Report (API-NBA + NBA CDN fallback)
    # --------------------------------------------------------
    elif current_action == "injury_report":
        st.subheader("🏥 Refreshing Injury Report…")

        st.info(
            "Loading real-time injury data "
            "with NBA CDN feed as fallback. "
            "This typically takes 5–15 seconds."
        )

        # Clear the action flag immediately so a page reload doesn't re-run it
        st.session_state["update_action"] = None

        with st.spinner("Loading injury data from API-NBA…"):
            try:
                from data.roster_engine import RosterEngine as _RE
                _re = _RE()
                _re.refresh()
                scraped_data = _re.get_injury_report()
            except Exception as scrape_exc:
                scraped_data = {}
                st.error(f"❌ **Retrieval failed:** {scrape_exc}")

        if scraped_data:
            # Record the timestamp
            _now_str = datetime.datetime.now().strftime("%b %d, %Y at %I:%M %p")
            st.session_state["injury_report_last_scraped"] = _now_str
            st.session_state["injury_report_data"] = scraped_data

            # Summary metrics
            out_count = sum(1 for v in scraped_data.values() if v.get("status") == "Out")
            gtd_count = sum(
                1 for v in scraped_data.values()
                if v.get("status") in ("GTD", "Questionable", "Doubtful", "Day-to-Day")
            )
            total_count = len(scraped_data)

            if total_count == 0:
                st.warning(
                    "⚠️ **0 injuries found** — all data sources returned empty results. "
                    "The data feed may be temporarily unavailable. Please try again."
                )
            else:
                st.success(
                    f"✅ **Injury report refreshed** ({_now_str})  \n"
                    f"Found **{total_count}** players — "
                    f"**{out_count}** Out, **{gtd_count}** GTD/Questionable/Doubtful"
                )

            # Show a summary table
            st.markdown("### 📋 Injury Data (API-NBA + NBA CDN)")
            st.caption(
                "Showing players with a non-Active designation from API-NBA / NBA CDN. "
                "Run a Smart Update or Full Setup to apply these to the "
                "full injury_status.json."
            )

            # Build display rows — only non-Active players
            _STATUS_ORDER = {
                "Out": 0, "Injured Reserve": 0,
                "Doubtful": 1, "GTD": 2, "Questionable": 2,
                "Day-to-Day": 3, "Probable": 4,
            }
            display_rows = [
                {
                    "Player":       key.title(),
                    "Team":         v.get("team", ""),
                    "Status":       v.get("status", ""),
                    "Injury":       v.get("injury", "") or v.get("comment", ""),
                    "Return Date":  v.get("return_date", ""),
                    "Source":       v.get("source", ""),
                    "_order":       _STATUS_ORDER.get(v.get("status", ""), 99),
                }
                for key, v in scraped_data.items()
                if v.get("status", "Active") not in ("Active", "Unknown")
            ]
            display_rows.sort(key=lambda r: (r["_order"], r["Player"]))

            if display_rows:
                # Render as an HTML table with status badge colours
                _STATUS_COLOURS = {
                    "Out":            ("#ff3366", "#fff"),
                    "Injured Reserve":("#cc0033", "#fff"),
                    "Doubtful":       ("#ff6600", "#fff"),
                    "GTD":            ("#ffd700", "#000"),
                    "Questionable":   ("#ffd700", "#000"),
                    "Day-to-Day":     ("#ffa500", "#000"),
                    "Probable":       ("#00cc66", "#000"),
                }

                def _badge(status):
                    bg, fg = _STATUS_COLOURS.get(status, ("#8b949e", "#fff"))
                    return (
                        f'<span style="background:{bg};color:{fg};padding:2px 7px;'
                        f'border-radius:6px;font-size:0.75rem;font-weight:700;">'
                        f"{status}</span>"
                    )

                table_html = (
                    '<table style="width:100%;border-collapse:collapse;font-size:0.88rem;">'
                    "<thead><tr>"
                    + "".join(
                        f'<th style="text-align:left;padding:6px 10px;border-bottom:1px solid #334;">{h}</th>'
                        for h in ("Player", "Team", "Status", "Injury / Note", "Return", "Source")
                    )
                    + "</tr></thead><tbody>"
                )
                for row in display_rows:
                    table_html += (
                        "<tr>"
                        f'<td style="padding:5px 10px;">{row["Player"]}</td>'
                        f'<td style="padding:5px 10px;">{row["Team"]}</td>'
                        f'<td style="padding:5px 10px;">{_badge(row["Status"])}</td>'
                        f'<td style="padding:5px 10px;">{row["Injury"]}</td>'
                        f'<td style="padding:5px 10px;">{row["Return Date"]}</td>'
                        f'<td style="padding:5px 10px;font-size:0.75rem;color:#8b949e;">{row["Source"]}</td>'
                        "</tr>"
                    )
                table_html += "</tbody></table>"
                st.markdown(table_html, unsafe_allow_html=True)
            else:
                st.info("No injured / non-Active players found in the scraped data.")

        else:
            if scraped_data is not None and not scraped_data:
                st.warning(
                    "⚠️ **No injury data was returned.** "
                    "The data feed may be temporarily unavailable. Please try again."
                )

    # --------------------------------------------------------
    # Action: Refresh Standings & News (API-NBA)
    # --------------------------------------------------------
    elif current_action == "standings_news":
        st.subheader("📊 Refreshing Standings & News…")
        st.session_state["update_action"] = None

        import datetime as _dt_sn

        with st.spinner("Loading NBA standings from API-NBA…"):
            try:
                from data.nba_data_service import get_standings as _get_standings_svc
                _standings_data = _get_standings_svc()
                st.session_state["league_standings"] = _standings_data
            except Exception as _sn_err:
                _standings_data = []
                st.warning(f"Standings retrieval failed: {_sn_err}")

        with st.spinner("Loading recent NBA news from API-NBA…"):
            try:
                from data.nba_data_service import get_player_news as _get_news_svc
                _news_data = _get_news_svc(limit=30)
                st.session_state["player_news"] = _news_data
            except Exception as _news_err:
                _news_data = []
                st.warning(f"News retrieval failed: {_news_err}")

        _now_sn = _dt_sn.datetime.now().strftime("%Y-%m-%d %H:%M")
        st.session_state["standings_news_last_loaded"] = _now_sn

        if _standings_data:
            st.success(
                f"✅ Standings loaded: **{len(_standings_data)} teams** · "
                f"News: **{len(_news_data)} items**"
            )
        elif not _standings_data:
            st.warning(
                "No standings returned. The data feed may be temporarily unavailable. Please try again."
            )

# ============================================================
# END SECTION: Execute the Selected Action
# ============================================================


# ============================================================
# SECTION: Get Platform Props
# Pull live prop lines from all major sportsbooks
# (via The Odds API) without needing
# the nba_api at all. Platforms only list players who are
# active and playing tonight, which naturally handles
# the injury/availability problem.
# ============================================================

st.divider()
st.subheader("📊 Get Platform Props")

st.markdown(
    "Pull **live prop lines** directly from the betting platforms. "
    "Platforms only list active players playing **tonight** — so this also "
    "acts as a real-time active roster check!"
)

st.markdown(get_education_box_html(
    "📖 How Platform Prop Loading Works",
    """
    <strong>Sportsbook Lines</strong>: Retrieves tonight's NBA prop lines from 
    PrizePicks, Underdog Fantasy, and DraftKings Pick6.<br><br>
    <strong>Cross-platform comparison</strong>: After loading, the app shows all lines 
    side-by-side so you can see which platform has the best line for each pick.
    """
), unsafe_allow_html=True)

# ── Import platform service ────────────────────────────────────
try:
    from data.sportsbook_service import (
        get_all_sportsbook_props,
        summarize_props_by_platform,
        find_new_players_from_props,
        build_cross_platform_comparison,
    )
    from data.data_manager import (
        save_platform_props_to_session,
        load_platform_props_from_session,
        save_platform_props_to_csv,
    )
    _SPORTSBOOK_SERVICE_AVAILABLE = True
except ImportError as _pf_err:
    _SPORTSBOOK_SERVICE_AVAILABLE = False
    st.warning(f"⚠️ Platform service not available: {_pf_err}")

if _SPORTSBOOK_SERVICE_AVAILABLE:

    # ── Read current settings ──────────────────────────────────
    _dk_on = st.session_state.get("load_draftkings_enabled", True)
    _dk_key = st.session_state.get("odds_api_key", "").strip()

    # Platform checkboxes
    _df_pp_col, _df_ud_col, _df_dk_col = st.columns(3)
    with _df_pp_col:
        _pp_on = st.checkbox("🟢 PrizePicks", value=True, key="datafeed_pp_checkbox")
    with _df_ud_col:
        _ud_on = st.checkbox("🟡 Underdog Fantasy", value=True, key="datafeed_ud_checkbox")
    with _df_dk_col:
        _dk_cb_on = st.checkbox("🔵 DraftKings Pick6", value=_dk_on and bool(_dk_key), key="datafeed_dk_checkbox",
                                disabled=not (_dk_on and bool(_dk_key)),
                                help="Requires Odds API key — configure on ⚙️ Settings page." if not (_dk_on and bool(_dk_key)) else "")

    # Show platform status badges
    _badge_style = (
        "padding:3px 10px;border-radius:6px;font-size:0.82rem;font-weight:700;"
        "margin-right:8px;display:inline-block;"
    )
    _pp_badge = (
        f'<span style="{_badge_style}background:#1a4d2f;color:#b8f8c8;">'
        f'{"✅" if _pp_on else "⏸️"} PrizePicks</span>'
    )
    _ud_badge = (
        f'<span style="{_badge_style}background:#4d3a1a;color:#f8e4b8;">'
        f'{"✅" if _ud_on else "⏸️"} Underdog</span>'
    )
    _dk_badge = (
        f'<span style="{_badge_style}background:#1a2f4d;color:#bee3f8;">'
        f'{"✅" if _dk_cb_on else "⏸️"} DraftKings</span>'
    )
    st.markdown(
        f'<div style="margin-bottom:12px;">{_pp_badge}{_ud_badge}{_dk_badge}</div>',
        unsafe_allow_html=True,
    )
    st.caption("Toggle platforms above. DraftKings requires an Odds API key (⚙️ Settings page).")

    # ── Check for already-loaded props in session ─────────────
    _cached_platform_props = load_platform_props_from_session(st.session_state)
    if _cached_platform_props:
        _cached_summary = summarize_props_by_platform(_cached_platform_props)
        _total_cached = sum(_cached_summary.values())
        st.info(
            f"📦 **{_total_cached} props cached** from last load: "
            + " | ".join(f"{plat}: {cnt}" for plat, cnt in _cached_summary.items())
        )

    # ── Load buttons ─────────────────────────────────────────
    _any_platform_on = _pp_on or _ud_on or _dk_cb_on
    _load_col1, _load_col2 = st.columns(2)

    with _load_col1:
        _load_dk = st.button(
            "🔵 Get DraftKings Only",
            disabled=not _dk_cb_on,
            width="stretch",
            help="Load lines from DraftKings Pick6 only.",
        )
    with _load_col2:
        _load_all = st.button(
            "🔄 Refresh All Props",
            type="primary",
            width="stretch",
            help="Load from all enabled platforms at once.",
            disabled=not _any_platform_on,
        )

    # ── Execute loads ────────────────────────────────────────
    _load_triggered = False
    _load_dk_only = False

    if _load_all:
        _load_triggered = True
    elif _load_dk:
        _load_triggered = True
        _load_dk_only = True

    if _load_triggered:
        _progress_bar = st.progress(0, text="Starting load...")

        def _progress_cb(current, total, message):
            pct = int((current / max(total, 1)) * 100)
            _progress_bar.progress(pct, text=message)

        try:
            with st.spinner("Loading live props from betting platforms..."):
                _new_props = get_all_sportsbook_props(
                    include_prizepicks=_pp_on and not _load_dk_only,
                    include_underdog=_ud_on and not _load_dk_only,
                    include_draftkings=_dk_cb_on and (_load_all or _load_dk_only),
                    odds_api_key=_dk_key or None,
                    progress_callback=_progress_cb,
                )

            _progress_bar.progress(100, text="Done!")

            if _new_props:
                # Save to session state so Prop Scanner and Analysis pages can use it
                save_platform_props_to_session(_new_props, st.session_state)

                # Also save props to session as current_props so they're immediately
                # available on the Prop Scanner page
                from data.data_manager import save_props_to_session
                save_props_to_session(_new_props, st.session_state)

                # Auto-save to disk so data persists across page navigations
                _saved_ok = save_platform_props_to_csv(_new_props)

                # Show per-platform summary
                _new_summary = summarize_props_by_platform(_new_props)
                st.success(
                    f"✅ Retrieved **{len(_new_props)} props** from "
                    + ", ".join(f"**{plat}** ({cnt})" for plat, cnt in _new_summary.items())
                    + (". Saved to `data/live_props.csv`." if _saved_ok else ".")
                )

                # Warn about new players not in our database
                _players_data_for_check = load_players_data()
                _new_players = find_new_players_from_props(_new_props, _players_data_for_check)
                if _new_players:
                    with st.expander(
                        f"⚠️ {len(_new_players)} players from platforms not in local database",
                        expanded=False,
                    ):
                        st.markdown(
                            "These players appear on betting platforms but are not in your "
                            "local player database. Consider running a **Smart Update** above "
                            "to retrieve their season stats."
                        )
                        for _np in _new_players[:20]:
                            st.markdown(f"- {_np}")
                        if len(_new_players) > 20:
                            st.caption(f"... and {len(_new_players) - 20} more")

            else:
                st.warning(
                    "⚠️ No props were returned. "
                    "Check your internet connection and try again."
                )
        except Exception as _platform_err:
            _plat_err_str = str(_platform_err)
            if "WebSocketClosedError" not in _plat_err_str and "StreamClosedError" not in _plat_err_str:
                st.error(f"❌ Platform load failed: {_platform_err}")
        finally:
            try:
                _progress_bar.empty()
            except Exception:
                pass

    # ── Show cached props preview ──────────────────────────────
    _display_props = load_platform_props_from_session(st.session_state)
    if _display_props:
        with st.expander(
            f"📋 Preview Retrieved Props ({len(_display_props)} total)",
            expanded=False,
        ):
            _preview_rows = []
            for _p in _display_props:
                _preview_rows.append({
                    "Player": _p.get("player_name", ""),
                    "Team": _p.get("team", ""),
                    "Stat": _p.get("stat_type", ""),
                    "Line": _p.get("line", ""),
                    "Platform": _p.get("platform", ""),
                    "Date": _p.get("game_date", ""),
                })
            st.dataframe(_preview_rows, width="stretch", hide_index=True)

# ============================================================
# END SECTION: Get Platform Props
# ============================================================


# ============================================================
# SECTION: Platform Roster Insights
# Cross-reference platform-confirmed active players against
# our player database to find gaps and potential injuries.
# ============================================================

if _SPORTSBOOK_SERVICE_AVAILABLE:
    _roster_props = load_platform_props_from_session(st.session_state)
    if _roster_props:
        st.divider()
        st.subheader("🏥 Platform Roster Insights")
        st.markdown(
            "Cross-reference tonight's platform props against your player database "
            "to spot **missing players** and **potential injuries**."
        )

        try:
            from data.sportsbook_service import (
                extract_active_players_from_props,
                cross_reference_with_player_data,
                get_platform_confirmed_injuries,
            )

            _ri_players_data = load_players_data()
            _ri_active = extract_active_players_from_props(_roster_props)
            _ri_xref = cross_reference_with_player_data(_ri_active, _ri_players_data)

            # ── Summary metrics ──────────────────────────────────────
            _ri_col1, _ri_col2, _ri_col3 = st.columns(3)
            with _ri_col1:
                st.metric(
                    "✅ Platform-Confirmed Active",
                    len(_ri_active),
                    help="Unique players listed on at least one platform tonight.",
                )
            with _ri_col2:
                st.metric(
                    "⚠️ Missing from Your Database",
                    len(_ri_xref["missing_from_csv"]),
                    help="Players on platforms but not in your local players.csv. Run a Smart Update to add them.",
                )
            with _ri_col3:
                st.metric(
                    "🔴 Potentially Out Tonight",
                    len(_ri_xref["in_csv_but_not_on_platforms"]),
                    help="Players in your database on tonight's teams who are NOT listed on any platform (may be injured/sitting).",
                )

            # ── Missing players warning ──────────────────────────────
            if _ri_xref["missing_from_csv"]:
                with st.expander(
                    f"⚠️ {len(_ri_xref['missing_from_csv'])} players on platforms but NOT in your database",
                    expanded=False,
                ):
                    st.markdown(
                        "These players have active props on betting platforms but their stats "
                        "are **not in your local database**. Run a **Smart Update** above to "
                        "retrieve their season stats before analyzing their props."
                    )
                    for _mp in _ri_xref["missing_from_csv"][:25]:
                        st.markdown(f"- {_mp}")
                    if len(_ri_xref["missing_from_csv"]) > 25:
                        st.caption(f"... and {len(_ri_xref['missing_from_csv']) - 25} more")

            # ── Platform-inferred injury report ──────────────────────
            _todays_games = st.session_state.get("todays_games", [])
            _ri_injuries = get_platform_confirmed_injuries(
                _ri_active, _ri_players_data, _todays_games
            )
            if _ri_injuries:
                with st.expander(
                    f"🔴 {len(_ri_injuries)} players potentially out (not on any platform)",
                    expanded=False,
                ):
                    st.markdown(
                        "These players are in your database and on a team playing tonight, "
                        "but **no platform has props for them**. They may be injured, resting, "
                        "or sitting out — even if not yet on the official injury report."
                    )
                    _inj_rows = [
                        {"Player": p["name"], "Team": p["team"], "Status": p["reason"]}
                        for p in _ri_injuries[:30]
                    ]
                    st.dataframe(_inj_rows, width="stretch", hide_index=True)
                    if len(_ri_injuries) > 30:
                        st.caption(f"... and {len(_ri_injuries) - 30} more")
            elif _todays_games:
                st.success("✅ All players in your database playing tonight appear on at least one platform.")

        except Exception as _ri_err:
            st.warning(f"⚠️ Could not load roster insights: {_ri_err}")

# ============================================================
# END SECTION: Platform Roster Insights
# ============================================================


# ============================================================
# SECTION: Standings & News Display
# Shows current NBA standings and recent news from API-NBA.
# ============================================================

_standings_display = st.session_state.get("league_standings", [])
_news_display      = st.session_state.get("player_news", [])

if _standings_display or _news_display:
    st.divider()
    st.subheader("📊 NBA Standings & News")

    _sn_tab1, _sn_tab2 = st.tabs(["🏆 Standings", "📰 Player & Team News"])

    with _sn_tab1:
        if _standings_display:
            # Split into East/West
            _east = [t for t in _standings_display if "east" in str(t.get("conference", "")).lower()]
            _west = [t for t in _standings_display if "west" in str(t.get("conference", "")).lower()]
            _other = [t for t in _standings_display if t not in _east and t not in _west]
            if _other:
                _east += _other  # fallback: dump into East if conference missing

            def _standings_table(teams, title):
                if not teams:
                    return
                st.markdown(f"**{title}**")
                rows = []
                for t in sorted(teams, key=lambda x: x.get("conference_rank", 99)):
                    w = t.get("wins", 0)
                    l = t.get("losses", 0)
                    hw = t.get("home_wins", 0)
                    hl = t.get("home_losses", 0)
                    aw = t.get("away_wins", 0)
                    al = t.get("away_losses", 0)
                    l10w = t.get("last_10_wins", 0)
                    l10l = t.get("last_10_losses", 0)
                    streak = t.get("streak", "")
                    gb = t.get("games_back", 0.0)
                    rows.append({
                        "Rank": t.get("conference_rank", "—"),
                        "Team": t.get("team_abbreviation", ""),
                        "W": w,
                        "L": l,
                        "W%": f"{t.get('win_pct', 0):.3f}",
                        "GB": f"{gb:.1f}" if gb else "—",
                        "Home": f"{hw}-{hl}",
                        "Away": f"{aw}-{al}",
                        "L10": f"{l10w}-{l10l}",
                        "Streak": streak,
                    })
                st.dataframe(rows, hide_index=True, use_container_width=True)

            _c_east, _c_west = st.columns(2)
            with _c_east:
                _standings_table(_east, "🏀 Eastern Conference")
            with _c_west:
                _standings_table(_west, "🏀 Western Conference")
        else:
            st.info(
                "No standings loaded yet. Click **📊 Refresh Standings & News** above."
            )

    with _sn_tab2:
        if _news_display:
            _imp_colors = {"high": "#ff4444", "medium": "#ffd700", "low": "#00ff9d"}
            _cat_emoji  = {
                "injury": "🏥", "trade": "🔄", "performance": "📈",
                "suspension": "🚫", "contract": "💰", "roster": "📋",
            }
            for _item in _news_display[:25]:
                _title = _item.get("title", "")
                _body  = _item.get("body", "")
                _player = _item.get("player_name", "")
                _team  = _item.get("team_abbreviation", "")
                _cat   = _item.get("category", "")
                _imp   = _item.get("impact", "").lower()
                _pub   = _item.get("published_at", "")[:10]
                if not _title:
                    continue
                _imp_badge = (
                    f'<span style="background:{_imp_colors.get(_imp,"#555")};'
                    f'color:#000;border-radius:4px;padding:1px 6px;font-size:0.72rem;'
                    f'font-weight:700;">{_imp.upper()}</span>'
                    if _imp else ""
                )
                _cat_label = _cat_emoji.get(_cat, "📰") + " " + _cat.title() if _cat else "📰 News"
                _who = f"**{_player}**" if _player else f"*{_team}*" if _team else ""
                with st.expander(
                    f"{_cat_emoji.get(_cat, '📰')} {_title[:80]}"
                    + (f"  ·  {_pub}" if _pub else ""),
                    expanded=False,
                ):
                    if _who:
                        st.markdown(f"{_who} · {_cat_label} {_imp_badge}", unsafe_allow_html=True)
                    if _body:
                        st.markdown(_body)
        else:
            st.info(
                "No news loaded yet. Click **📊 Refresh Standings & News** above."
            )

# ============================================================
# END SECTION: Standings & News Display
# ============================================================


# ============================================================
# SECTION: Deep Fetch — Advanced Stats Enrichment
# ============================================================

st.divider()

st.subheader("🔬 Advanced Stats Enrichment")
st.markdown(
    "Pre-fetch full advanced NBA data for tonight's games: team dashboards, "
    "5-man lineups, player estimated metrics, rotation data, and standings context. "
    "This enriches every engine module — projections, simulation, confidence scoring, "
    "and matchup analysis — with richer real data from the NBA API."
)

with st.expander("📖 What Does Deep Fetch Do?", expanded=False):
    st.markdown("""
    **🔬 Deep Fetch: Advanced Stats** pre-loads the following data for each of tonight's games:

    | Data Type | Source | Benefit |
    |---|---|---|
    | Team game logs (last 10) | NBA Stats API | Recent blowout frequency, pace trends |
    | 5-man lineup stats | NBA Stats API | Rotation patterns, +/- data |
    | Player estimated metrics | NBA Stats API | Real pace, ortg, drtg, net rating |
    | Team dashboards | NBA Stats API | Home/away splits, rest-day performance |
    | League standings | NBA Stats API | Opponent record context |
    | Today's scoreboard | NBA Stats API | Live game status |

    **Note:** Deep Fetch respects the NBA API rate limit (1.5s delay per call). For a 10-game
    slate it may take 2–5 minutes. Results are cached for 1 hour.
    """)

if st.button(
    "🔬 Deep Fetch: Advanced Stats",
    help="Pre-fetch advanced enrichment data for tonight's games (team logs, lineups, metrics, standings)",
    type="secondary",
):
    _deep_fetch_games = st.session_state.get("todays_games", [])

    if not _deep_fetch_games:
        st.warning(
            "⚠️ No games loaded yet. Click **🏀 Auto-Load Tonight's Games** "
            "or **One-Click Full Setup** first, then run Deep Fetch."
        )
    else:
        _deep_progress = st.progress(0, text="Starting advanced stats enrichment…")
        _deep_status = st.empty()

        def _deep_progress_callback(current: int, total: int, msg: str) -> None:
            try:
                pct = int((current / max(total, 1)) * 100)
                _deep_progress.progress(pct, text=msg)
                _deep_status.caption(f"Step {current}/{total}: {msg}")
            except Exception as _cb_err:
                import logging as _logging
                _logging.getLogger(__name__).debug(
                    "Deep Fetch progress callback error (non-fatal): %s", _cb_err
                )

        try:
            from data.advanced_fetcher import enrich_tonights_slate, build_enrichment_summary

            with st.spinner("Fetching advanced stats from NBA API…"):
                _enriched = enrich_tonights_slate(
                    _deep_fetch_games,
                    progress_callback=_deep_progress_callback,
                )

            # Store in session state for engine modules to access
            st.session_state["advanced_enrichment"] = _enriched

            _deep_progress.progress(100, text="✅ Advanced stats enrichment complete!")
            _deep_status.empty()

            # Build and display summary
            _summary = build_enrichment_summary(_enriched)

            st.success("✅ Advanced Stats Loaded!")

            _col1, _col2, _col3 = st.columns(3)
            with _col1:
                st.metric("Games Enriched", _summary.get("games_enriched", 0))
                st.metric("Team Game Logs", _summary.get("game_logs_fetched", 0))
            with _col2:
                st.metric("5-Man Lineups", _summary.get("lineups_fetched", 0))
                st.metric("Team Dashboards", _summary.get("dashboards_fetched", 0))
            with _col3:
                st.metric("Standing Rows", _summary.get("standings_rows", 0))
                st.metric("Player Metrics", _summary.get("player_metrics_rows", 0))

            if _summary.get("scoreboard_available"):
                st.caption("✅ Live scoreboard data loaded")

            # Detail breakdown per game
            if _enriched:
                with st.expander("📋 Per-Game Enrichment Details", expanded=False):
                    for _gid, _gdata in _enriched.items():
                        _home_logs = len(_gdata.get("home_game_logs", []))
                        _away_logs = len(_gdata.get("away_game_logs", []))
                        _home_lin = len(_gdata.get("home_lineups", []))
                        _away_lin = len(_gdata.get("away_lineups", []))
                        st.markdown(
                            f"**Game {_gid}** — "
                            f"Home logs: {_home_logs} | Away logs: {_away_logs} | "
                            f"Home lineups: {_home_lin} | Away lineups: {_away_lin} | "
                            f"Dashboard: {'✅' if _gdata.get('home_dashboard') else '—'}"
                        )

        except Exception as _deep_err:
            _deep_progress.empty()
            _deep_status.empty()
            st.error(
                f"❌ Deep Fetch failed: {_deep_err}\n\n"
                "The NBA API may be temporarily unavailable. "
                "All other app features continue to work with existing data."
            )


# ============================================================
# END SECTION: Deep Fetch — Advanced Stats Enrichment
# ============================================================


# ============================================================
# SECTION: Help and Tips
# ============================================================

st.divider()

with st.expander("💡 Tips & FAQ", expanded=False):
    st.markdown("""
    ### Frequently Asked Questions

    **Q: How often should I update?**
    A: Update **before each betting session**. Player stats change slowly,
    but team and player situations change week-to-week. Updating once per day
    before you bet is ideal.

    ---

    **Q: Why does the update take so long?**
    A: We add a 1.5-second delay between each data request to avoid being blocked
    by the servers. With 500+ players, retrieving game logs takes time.
    This is normal and necessary!

    BEGINNER NOTE: "Rate limiting" means a website limits how many requests
    you can make per minute. If you ask too fast, they block you temporarily.
    The delay prevents this.

    ---

    **Q: What happens if the update fails?**
    A: Nothing breaks! The app just keeps using the existing CSV data.
    Try again in a few minutes — the data feed may be temporarily slow or down.

    ---

    **Q: Where does the data come from?**
    A: Player stats, team stats, rosters, injuries, standings, and live scores
    are retrieved from professional sports data providers.
    Prop lines come from all major sportsbooks.

    ---

    **Q: Does this work during the offseason?**
    A: Player and team stats from the most recent completed season will still
    be available. "Tonight's games" will return nothing during the offseason.

    ---

    **Q: I see 'no data yet' even after updating. Why?**
    A: The players.csv file gets written when you click Update. If you still see "no data"
    in the status, try refreshing the page or running the update again.

    ---

    **Q: How do I get DraftKings props?**
    A: Enable DraftKings on the ⚙️ Settings page, then click "Get DraftKings" or "Refresh All Props".

    ---

    **Q: How do I get NBA Standings and News?**
    A: Click **📊 Refresh Standings & News** (above). This retrieves current conference
    standings and recent player/team news. Standings are also
    auto-loaded whenever you run **One-Click Full Setup** or **🏀 Auto-Load Tonight's Games**.
    """)

# ============================================================
# END SECTION: Help and Tips
# ============================================================
