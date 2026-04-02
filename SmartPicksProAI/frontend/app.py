"""
app.py
------
Streamlit dashboard for SmartPicksProAI.

A premium "luxury AI portal" interface for viewing NBA matchups, analysing
player performance, browsing team rosters, and exploring all data.

Every game card is clickable — opening a rich detail view with box scores,
team comparisons, and player lists.  Every player name is clickable —
opening a deep-dive profile with bio, career, advanced stats, and more.

Start the dashboard::

    cd SmartPicksProAI/frontend
    streamlit run app.py
"""

import pandas as pd
import streamlit as st

from typing import Optional

from api_service import (
    get_defense_vs_position,
    get_draft_history,
    get_game_box_score,
    get_game_rotation,
    get_league_dash_players,
    get_league_dash_teams,
    get_league_leaders,
    get_lineups,
    get_play_by_play,
    get_player_advanced,
    get_player_awards,
    get_player_bio,
    get_player_career,
    get_player_clutch,
    get_player_hustle,
    get_player_last5,
    get_player_matchups,
    get_player_scoring,
    get_player_shot_chart,
    get_player_tracking,
    get_player_usage,
    get_recent_games,
    get_schedule,
    get_standings,
    get_team_clutch,
    get_team_details,
    get_team_estimated_metrics,
    get_team_hustle,
    get_team_roster,
    get_team_stats,
    get_team_synergy,
    get_teams,
    get_todays_games,
    get_win_probability,
    search_players,
    trigger_refresh,
)

# ═══════════════════════════════════════════════════════════════════════════
# Page configuration
# ═══════════════════════════════════════════════════════════════════════════

st.set_page_config(
    page_title="SmartPicksProAI",
    page_icon="🏀",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ═══════════════════════════════════════════════════════════════════════════
# Session-state navigation
# ═══════════════════════════════════════════════════════════════════════════

if "page" not in st.session_state:
    st.session_state.page = "home"
if "selected_game_id" not in st.session_state:
    st.session_state.selected_game_id = None
if "selected_player_id" not in st.session_state:
    st.session_state.selected_player_id = None
if "selected_team_id" not in st.session_state:
    st.session_state.selected_team_id = None
if "game_context" not in st.session_state:
    st.session_state.game_context = {}


def _nav(page: str, **kwargs) -> None:
    """Navigate to a page, setting any additional session state keys."""
    st.session_state.page = page
    for k, v in kwargs.items():
        st.session_state[k] = v


# ═══════════════════════════════════════════════════════════════════════════
# Premium luxury + AI portal CSS
# ═══════════════════════════════════════════════════════════════════════════

st.markdown(
    """
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800&display=swap');

    /* ── Global ───────────────────────────────────────────────── */
    .stApp {
        background: linear-gradient(135deg, #0a0e1a 0%, #0d1225 40%, #111830 100%);
        color: #e0e6f0;
        font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif;
    }
    section[data-testid="stSidebar"] {
        background: linear-gradient(180deg, #0c1024 0%, #111830 100%);
        border-right: 1px solid rgba(212, 175, 55, 0.15);
    }

    /* ── Typography ───────────────────────────────────────────── */
    h1 {
        background: linear-gradient(135deg, #d4af37 0%, #f0d060 50%, #d4af37 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        font-weight: 800;
        letter-spacing: -0.02em;
    }
    h2, h3 {
        color: #d4af37;
        font-weight: 700;
        letter-spacing: -0.01em;
    }
    h4 {
        color: #7eb8da;
        font-weight: 600;
    }

    /* ── Glass cards ──────────────────────────────────────────── */
    .glass-card {
        background: rgba(18, 25, 50, 0.65);
        backdrop-filter: blur(12px);
        -webkit-backdrop-filter: blur(12px);
        border: 1px solid rgba(212, 175, 55, 0.12);
        border-radius: 16px;
        padding: 1.2rem 1.4rem;
        margin-bottom: 0.8rem;
        transition: all 0.25s ease;
    }
    .glass-card:hover {
        border-color: rgba(212, 175, 55, 0.35);
        box-shadow: 0 8px 32px rgba(212, 175, 55, 0.08);
        transform: translateY(-2px);
    }
    .glass-card-sm {
        background: rgba(18, 25, 50, 0.5);
        backdrop-filter: blur(8px);
        border: 1px solid rgba(255, 255, 255, 0.06);
        border-radius: 12px;
        padding: 0.8rem 1rem;
        margin-bottom: 0.5rem;
    }

    /* ── Game cards (clickable) ───────────────────────────────── */
    .game-tile {
        background: linear-gradient(135deg, rgba(18, 25, 55, 0.8) 0%, rgba(15, 20, 45, 0.9) 100%);
        border: 1px solid rgba(212, 175, 55, 0.15);
        border-radius: 16px;
        padding: 1.4rem;
        text-align: center;
        transition: all 0.3s ease;
        position: relative;
        overflow: hidden;
    }
    .game-tile::before {
        content: '';
        position: absolute;
        top: 0; left: 0; right: 0;
        height: 3px;
        background: linear-gradient(90deg, transparent, #d4af37, transparent);
        opacity: 0;
        transition: opacity 0.3s ease;
    }
    .game-tile:hover::before { opacity: 1; }
    .game-tile:hover {
        border-color: rgba(212, 175, 55, 0.4);
        box-shadow: 0 12px 40px rgba(212, 175, 55, 0.1);
        transform: translateY(-3px);
    }
    .game-tile .teams {
        font-size: 1.15rem;
        font-weight: 700;
        color: #ffffff;
        letter-spacing: 0.02em;
    }
    .game-tile .vs { color: #d4af37; margin: 0 0.4rem; }
    .game-tile .score {
        font-size: 1.5rem;
        font-weight: 800;
        color: #d4af37;
        margin: 0.4rem 0;
    }
    .game-tile .meta {
        font-size: 0.7rem;
        color: rgba(255, 255, 255, 0.35);
        margin-top: 0.5rem;
    }

    /* ── Metric cards ─────────────────────────────────────────── */
    [data-testid="stMetric"] {
        background: rgba(18, 25, 50, 0.6);
        backdrop-filter: blur(8px);
        border: 1px solid rgba(212, 175, 55, 0.1);
        border-radius: 12px;
        padding: 0.8rem 1rem;
    }
    [data-testid="stMetricValue"] {
        color: #d4af37 !important;
        font-weight: 700;
    }
    [data-testid="stMetricLabel"] {
        color: rgba(255, 255, 255, 0.5) !important;
        font-size: 0.75rem;
        text-transform: uppercase;
        letter-spacing: 0.08em;
    }

    /* ── Buttons ──────────────────────────────────────────────── */
    .stButton > button {
        background: linear-gradient(135deg, #d4af37 0%, #b8941e 100%);
        color: #0a0e1a;
        border: none;
        border-radius: 10px;
        font-weight: 600;
        letter-spacing: 0.02em;
        transition: all 0.25s ease;
    }
    .stButton > button:hover {
        background: linear-gradient(135deg, #e6c453 0%, #d4af37 100%);
        box-shadow: 0 6px 24px rgba(212, 175, 55, 0.25);
        transform: translateY(-1px);
    }
    .stButton > button:active {
        transform: translateY(0);
    }

    /* ── Tabs ─────────────────────────────────────────────────── */
    .stTabs [data-baseweb="tab-list"] {
        gap: 4px;
        background: rgba(18, 25, 50, 0.3);
        border-radius: 12px;
        padding: 4px;
    }
    .stTabs [data-baseweb="tab"] {
        background: transparent;
        border-radius: 8px;
        color: rgba(255, 255, 255, 0.45);
        font-weight: 500;
        padding: 0.5rem 1.2rem;
        transition: all 0.2s ease;
    }
    .stTabs [data-baseweb="tab"]:hover {
        color: rgba(255, 255, 255, 0.7);
        background: rgba(212, 175, 55, 0.06);
    }
    .stTabs [aria-selected="true"] {
        background: rgba(212, 175, 55, 0.12) !important;
        color: #d4af37 !important;
        font-weight: 600;
        border-bottom: 2px solid #d4af37;
    }

    /* ── Data tables ──────────────────────────────────────────── */
    .stDataFrame { font-size: 0.82rem; }
    .stDataFrame [data-testid="stDataFrameResizable"] {
        border: 1px solid rgba(212, 175, 55, 0.08);
        border-radius: 12px;
        overflow: hidden;
    }

    /* ── Layout ───────────────────────────────────────────────── */
    .block-container {
        padding-top: 1.5rem;
        padding-bottom: 1rem;
        max-width: 1400px;
    }

    /* ── Section headers ──────────────────────────────────────── */
    .section-hdr {
        font-size: 0.8rem;
        font-weight: 600;
        color: rgba(212, 175, 55, 0.6);
        text-transform: uppercase;
        letter-spacing: 0.12em;
        margin: 1.5rem 0 0.6rem 0;
        padding-bottom: 0.4rem;
        border-bottom: 1px solid rgba(212, 175, 55, 0.1);
    }

    /* ── AI glow accent ───────────────────────────────────────── */
    .ai-glow {
        position: relative;
    }
    .ai-glow::after {
        content: '';
        position: absolute;
        top: 50%; left: 50%;
        width: 200%; height: 200%;
        transform: translate(-50%, -50%);
        background: radial-gradient(circle, rgba(0, 212, 255, 0.03) 0%, transparent 70%);
        pointer-events: none;
    }

    /* ── Player chip (clickable pill) ─────────────────────────── */
    .player-chip {
        display: inline-block;
        background: rgba(212, 175, 55, 0.08);
        border: 1px solid rgba(212, 175, 55, 0.15);
        border-radius: 20px;
        padding: 0.25rem 0.75rem;
        font-size: 0.82rem;
        color: #e0e6f0;
        margin: 0.15rem 0.1rem;
        transition: all 0.2s ease;
    }
    .player-chip:hover {
        background: rgba(212, 175, 55, 0.18);
        border-color: rgba(212, 175, 55, 0.4);
    }
    .player-chip .pos {
        color: rgba(212, 175, 55, 0.6);
        font-size: 0.7rem;
        margin-left: 0.3rem;
    }

    /* ── Empty state ──────────────────────────────────────────── */
    .empty-state {
        text-align: center;
        color: rgba(255, 255, 255, 0.3);
        padding: 3rem;
        font-style: italic;
        font-size: 0.9rem;
    }

    /* ── Back nav ─────────────────────────────────────────────── */
    .back-btn {
        font-size: 0.82rem;
        color: rgba(212, 175, 55, 0.7);
    }

    /* ── Divider style ────────────────────────────────────────── */
    hr {
        border-color: rgba(212, 175, 55, 0.08) !important;
    }

    /* ── Scrollbar ────────────────────────────────────────────── */
    ::-webkit-scrollbar { width: 6px; }
    ::-webkit-scrollbar-track { background: transparent; }
    ::-webkit-scrollbar-thumb {
        background: rgba(212, 175, 55, 0.2);
        border-radius: 3px;
    }
    ::-webkit-scrollbar-thumb:hover {
        background: rgba(212, 175, 55, 0.35);
    }
    </style>
    """,
    unsafe_allow_html=True,
)


# ═══════════════════════════════════════════════════════════════════════════
# Helpers
# ═══════════════════════════════════════════════════════════════════════════

def _show_df(data, columns=None, height=None):
    """Display data as a styled dataframe."""
    if not data:
        st.markdown('<div class="empty-state">No data available.</div>',
                    unsafe_allow_html=True)
        return
    df = pd.DataFrame(data) if isinstance(data, list) else data
    if columns:
        columns = [c for c in columns if c in df.columns]
        if columns:
            df = df[columns]
    kwargs = {"use_container_width": True, "hide_index": True}
    if height:
        kwargs["height"] = height
    st.dataframe(df, **kwargs)


def _player_button(player_id, name, position=None, team=None, key_prefix=""):
    """Render a clickable button for a player that navigates to their profile."""
    label_parts = [name]
    if position:
        label_parts.append(f"({position})")
    if team:
        label_parts.append(f"· {team}")
    label = " ".join(label_parts)
    if st.button(f"👤 {label}", key=f"{key_prefix}_p_{player_id}",
                 use_container_width=True):
        _nav("player_profile", selected_player_id=player_id)
        st.rerun()


def _game_button(game, key_prefix=""):
    """Render a clickable game card button."""
    matchup = game.get("matchup", "TBD")
    home_score = game.get("home_score")
    away_score = game.get("away_score")
    game_date = game.get("game_date", "")
    gid = game.get("game_id", "")

    if home_score is not None and away_score is not None:
        label = f"🏀 {matchup}  |  {home_score} – {away_score}  |  {game_date}"
    else:
        label = f"🏀 {matchup}  |  {game_date}"

    if st.button(label, key=f"{key_prefix}_g_{gid}", use_container_width=True):
        _nav("game_detail", selected_game_id=gid, game_context=game)
        st.rerun()


# ═══════════════════════════════════════════════════════════════════════════
# Sidebar — Premium navigation + Admin + Team browser
# ═══════════════════════════════════════════════════════════════════════════

with st.sidebar:
    # Logo / Brand
    st.markdown(
        """
        <div style="text-align:center; padding: 0.5rem 0 1rem 0;">
            <div style="font-size:2.5rem;">🏀</div>
            <div style="
                font-size:1.1rem; font-weight:800; letter-spacing:0.05em;
                background: linear-gradient(135deg, #d4af37, #f0d060);
                -webkit-background-clip: text;
                -webkit-text-fill-color: transparent;
            ">SMARTPICKS PRO AI</div>
            <div style="font-size:0.65rem; color:rgba(255,255,255,0.3);
                        letter-spacing:0.15em; text-transform:uppercase;
                        margin-top:0.2rem;">
                NBA Intelligence Platform
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )
    st.divider()

    # ── Navigation ────────────────────────────────────────────
    st.markdown(
        '<div class="section-hdr">Navigation</div>',
        unsafe_allow_html=True,
    )

    nav_items = [
        ("🏠  Home", "home"),
        ("🏆  Standings", "standings"),
        ("🏟️  Teams", "teams_browse"),
        ("📊  Leaders & Stats", "leaders"),
        ("🛡️  Defense vs Position", "defense"),
        ("📈  More Data", "more"),
    ]
    for label, page_key in nav_items:
        if st.button(label, key=f"nav_{page_key}", use_container_width=True):
            _nav(page_key)
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
                    _nav("player_profile", selected_player_id=pid)
                    st.rerun()
        else:
            st.caption("No players found.")

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
        else:
            st.error(f"Failed: {result.get('message', 'Unknown error')}")

    st.divider()
    st.markdown(
        "<div style='text-align:center; color:rgba(255,255,255,0.2); "
        "font-size:0.65rem;'>SmartPicksProAI v3.0<br>"
        "Luxury AI Portal</div>",
        unsafe_allow_html=True,
    )

# ═══════════════════════════════════════════════════════════════════════════
# Page router
# ═══════════════════════════════════════════════════════════════════════════

current_page = st.session_state.page


# ─────────────────────────────────────────────────────────────────────────
# PAGE: HOME
# ─────────────────────────────────────────────────────────────────────────

if current_page == "home":
    st.title("🏀 SmartPicks Pro AI")
    st.caption("Your premium NBA intelligence platform — click any game or player to explore")

    # ── Today's Games ─────────────────────────────────────────
    st.markdown('<div class="section-hdr">Today\'s Matchups</div>',
                unsafe_allow_html=True)

    games = get_todays_games()
    if games:
        cols = st.columns(min(len(games), 4))
        for idx, game in enumerate(games):
            with cols[idx % len(cols)]:
                _game_button(game, key_prefix="today")
    else:
        st.info("No games scheduled for today.")

    st.divider()

    # ── Recent Games ──────────────────────────────────────────
    st.markdown('<div class="section-hdr">Recent Games</div>',
                unsafe_allow_html=True)

    recent = get_recent_games()
    if recent:
        # Show as clickable list
        for idx, game in enumerate(recent[:20]):
            _game_button(game, key_prefix="recent")
    else:
        st.info("No recent game data available.")

    st.divider()

    # ── Quick Player Search ───────────────────────────────────
    st.markdown('<div class="section-hdr">Player Lookup</div>',
                unsafe_allow_html=True)
    st.caption("Search for any player to view their complete profile.")

    search_col, id_col = st.columns([3, 1])
    with search_col:
        player_query = st.text_input(
            "Search by name",
            placeholder="e.g. LeBron, Curry, Jokic …",
            key="home_search",
        )
    with id_col:
        player_id_direct = st.number_input(
            "Player ID",
            min_value=0, value=0, step=1,
            key="home_pid",
        )

    if player_query.strip():
        results = search_players(player_query.strip())
        if results:
            for r in results[:10]:
                _player_button(
                    r["player_id"],
                    r.get("full_name", ""),
                    r.get("position"),
                    r.get("team_abbreviation"),
                    key_prefix="hs",
                )
        else:
            st.warning("No players found.")
    elif player_id_direct > 0:
        if st.button("Open Player Profile", key="home_open_pid"):
            _nav("player_profile", selected_player_id=player_id_direct)
            st.rerun()


# ─────────────────────────────────────────────────────────────────────────
# PAGE: GAME DETAIL
# ─────────────────────────────────────────────────────────────────────────

elif current_page == "game_detail":
    gid = st.session_state.selected_game_id
    ctx = st.session_state.game_context or {}

    if st.button("← Back to Home", key="back_home_gd"):
        _nav("home")
        st.rerun()

    matchup = ctx.get("matchup", gid or "Game Detail")
    home_score = ctx.get("home_score")
    away_score = ctx.get("away_score")
    game_date = ctx.get("game_date", "")

    st.title(f"🏀 {matchup}")

    # Score + date header
    header_parts = []
    if game_date:
        header_parts.append(game_date)
    if home_score is not None and away_score is not None:
        header_parts.append(f"**{home_score} – {away_score}**")
    if header_parts:
        st.caption(" · ".join(header_parts))

    if not gid:
        st.warning("No game selected.")
    else:
        # ── Team info from context ────────────────────────────
        home_tid = ctx.get("home_team_id")
        away_tid = ctx.get("away_team_id")
        home_abbrev = ctx.get("home_abbrev", "")
        away_abbrev = ctx.get("away_abbrev", "")

        # Team stats comparison
        teams_data = get_teams()
        team_lookup = {t["team_id"]: t for t in teams_data} if teams_data else {}

        if home_tid and away_tid:
            home_team = team_lookup.get(home_tid, {})
            away_team = team_lookup.get(away_tid, {})

            if home_team or away_team:
                st.markdown('<div class="section-hdr">Team Comparison</div>',
                            unsafe_allow_html=True)
                c1, c2 = st.columns(2)
                with c1:
                    ht_name = home_team.get("team_name", home_abbrev)
                    st.markdown(f"#### 🏠 {home_abbrev} — {ht_name}")
                    m1 = st.columns(3)
                    m1[0].metric("Pace", home_team.get("pace", "N/A"))
                    m1[1].metric("ORtg", home_team.get("ortg", "N/A"))
                    m1[2].metric("DRtg", home_team.get("drtg", "N/A"))
                with c2:
                    at_name = away_team.get("team_name", away_abbrev)
                    st.markdown(f"#### ✈️ {away_abbrev} — {at_name}")
                    m2 = st.columns(3)
                    m2[0].metric("Pace", away_team.get("pace", "N/A"))
                    m2[1].metric("ORtg", away_team.get("ortg", "N/A"))
                    m2[2].metric("DRtg", away_team.get("drtg", "N/A"))

        st.divider()

        # ── Tabs for game data ────────────────────────────────
        tab_box, tab_rosters, tab_pbp, tab_wp, tab_rot = st.tabs([
            "📊 Box Score",
            "👥 Rosters (click players)",
            "📝 Play-by-Play",
            "📈 Win Probability",
            "🔄 Rotation",
        ])

        with tab_box:
            box = get_game_box_score(gid)
            if box:
                # Split by team
                teams_in_box = sorted(
                    set(p.get("team_abbreviation", "") for p in box)
                )
                for team_abbr in teams_in_box:
                    st.markdown(f"#### {team_abbr}")
                    team_players = [p for p in box
                                    if p.get("team_abbreviation") == team_abbr]
                    _show_df(team_players, [
                        "full_name", "position", "pts", "reb", "ast",
                        "stl", "blk", "tov", "fgm", "fga", "fg_pct",
                        "fg3m", "fg3a", "fg3_pct", "ftm", "fta", "ft_pct",
                        "oreb", "dreb", "pf", "plus_minus", "min", "wl",
                    ])
                    # Clickable player names
                    for p in team_players:
                        _player_button(
                            p["player_id"],
                            p.get("full_name", ""),
                            p.get("position"),
                            key_prefix=f"box_{team_abbr}",
                        )
                    st.divider()
            else:
                st.info("No box score data for this game.")

        with tab_rosters:
            if home_tid:
                st.markdown(f"#### 🏠 {home_abbrev} Roster")
                h_roster = get_team_roster(home_tid)
                if h_roster:
                    for p in h_roster:
                        _player_button(
                            p["player_id"],
                            p.get("full_name", ""),
                            p.get("position"),
                            key_prefix="hr",
                        )
                else:
                    st.info("No roster data.")
                st.divider()

            if away_tid:
                st.markdown(f"#### ✈️ {away_abbrev} Roster")
                a_roster = get_team_roster(away_tid)
                if a_roster:
                    for p in a_roster:
                        _player_button(
                            p["player_id"],
                            p.get("full_name", ""),
                            p.get("position"),
                            key_prefix="ar",
                        )
                else:
                    st.info("No roster data.")

        with tab_pbp:
            pbp = get_play_by_play(gid)
            if pbp:
                _show_df(pbp, [
                    "period", "clock", "description", "action_type",
                    "sub_type", "player_name", "team_tricode",
                    "score_home", "score_away", "shot_result",
                    "shot_distance",
                ], height=500)
            else:
                st.info("No play-by-play data.")

        with tab_wp:
            wp = get_win_probability(gid)
            if wp:
                df_wp = pd.DataFrame(wp)
                if "home_pct" in df_wp.columns:
                    st.line_chart(
                        df_wp.set_index("event_num")[
                            ["home_pct", "visitor_pct"]
                        ],
                        use_container_width=True,
                    )
                st.divider()
                _show_df(wp, [
                    "event_num", "home_pct", "visitor_pct",
                    "home_pts", "visitor_pts",
                    "home_score_margin", "period", "description",
                ], height=400)
            else:
                st.info("No win probability data.")

        with tab_rot:
            rot = get_game_rotation(gid)
            if rot:
                _show_df(rot, [
                    "full_name", "team_abbrev", "in_time_real",
                    "out_time_real", "player_pts", "pt_diff", "usg_pct",
                ], height=500)
            else:
                st.info("No rotation data.")


# ─────────────────────────────────────────────────────────────────────────
# PAGE: PLAYER PROFILE
# ─────────────────────────────────────────────────────────────────────────

elif current_page == "player_profile":
    pid = st.session_state.selected_player_id

    if st.button("← Back", key="back_from_player"):
        _nav("home")
        st.rerun()

    if not pid:
        st.warning("No player selected.")
    else:
        pid = int(pid)

        # ── Header: Name + Bio summary ────────────────────────
        bio = get_player_bio(pid)
        last5 = get_player_last5(pid)

        player_name = ""
        if last5:
            player_name = (
                f"{last5.get('first_name', '')} {last5.get('last_name', '')}"
            ).strip()
        if not player_name and bio:
            player_name = bio.get("player_name", f"Player #{pid}")
        if not player_name:
            player_name = f"Player #{pid}"

        st.title(f"👤 {player_name}")

        # Quick bio metrics
        if bio:
            bio_cols = st.columns(8)
            bio_cols[0].metric("Height", bio.get("player_height", "N/A"))
            bio_cols[1].metric(
                "Weight",
                f"{bio.get('player_weight', 'N/A')} lbs"
                if bio.get("player_weight") else "N/A",
            )
            bio_cols[2].metric("Age", bio.get("age", "N/A"))
            bio_cols[3].metric("College", bio.get("college", "N/A"))
            bio_cols[4].metric("Country", bio.get("country", "N/A"))
            bio_cols[5].metric("Draft Yr", bio.get("draft_year", "N/A"))
            bio_cols[6].metric("GP", bio.get("gp", "N/A"))
            bio_cols[7].metric(
                "USG%",
                f"{(bio.get('usg_pct') or 0):.1%}",
            )

        # Last 5 averages hero row
        if last5:
            avgs = last5.get("averages", {})
            st.markdown('<div class="section-hdr">Last 5 Games Average</div>',
                        unsafe_allow_html=True)
            a_cols = st.columns(8)
            stat_map = {
                "pts": "PTS", "reb": "REB", "ast": "AST", "blk": "BLK",
                "stl": "STL", "tov": "TOV", "fg_pct": "FG%",
                "plus_minus": "+/-",
            }
            for i, (k, lbl) in enumerate(stat_map.items()):
                val = avgs.get(k, 0.0)
                if k in ("fg_pct",):
                    a_cols[i].metric(lbl, f"{(val or 0):.1%}")
                else:
                    a_cols[i].metric(lbl, val)

        st.divider()

        # ── Detailed tabs ─────────────────────────────────────
        (p_t_last5, p_t_career, p_t_adv, p_t_scoring, p_t_usage,
         p_t_shots, p_t_tracking, p_t_clutch, p_t_hustle,
         p_t_matchups, p_t_awards) = st.tabs([
            "📊 Last 5 Games",
            "📈 Career Stats",
            "🧠 Advanced",
            "🎯 Scoring",
            "⚡ Usage",
            "🏀 Shot Chart",
            "🏃 Tracking",
            "🔥 Clutch",
            "💪 Hustle",
            "⚔️ Matchups",
            "🏅 Awards",
        ])

        with p_t_last5:
            if last5 and last5.get("games"):
                _show_df(last5["games"], [
                    "game_date", "matchup", "wl", "pts", "reb", "ast",
                    "blk", "stl", "tov", "fgm", "fga", "fg_pct",
                    "fg3m", "fg3a", "fg3_pct", "ftm", "fta", "ft_pct",
                    "oreb", "dreb", "pf", "plus_minus", "min",
                ])
            else:
                st.info("No recent game data.")

        with p_t_career:
            career = get_player_career(pid)
            if career:
                _show_df(career, [
                    "season_id", "team_abbreviation", "player_age", "gp",
                    "gs", "min", "pts", "reb", "ast", "stl", "blk",
                    "tov", "fgm", "fga", "fg_pct", "fg3m", "fg3a",
                    "fg3_pct", "ftm", "fta", "ft_pct", "oreb", "dreb",
                    "pf",
                ])
            else:
                st.info("No career data.")

        with p_t_adv:
            adv = get_player_advanced(pid)
            if adv:
                _show_df(adv, [
                    "game_date", "matchup", "minutes", "off_rating",
                    "def_rating", "net_rating", "ts_pct", "efg_pct",
                    "usg_pct", "ast_pct", "oreb_pct", "dreb_pct",
                    "reb_pct", "tov_ratio", "pace", "pie",
                ])
            else:
                st.info("No advanced data.")

        with p_t_scoring:
            scoring = get_player_scoring(pid)
            if scoring:
                _show_df(scoring, [
                    "game_date", "matchup", "minutes",
                    "pct_fga_2pt", "pct_fga_3pt", "pct_pts_2pt",
                    "pct_pts_3pt", "pct_pts_fast_break", "pct_pts_ft",
                    "pct_pts_paint", "pct_pts_off_tov",
                    "pct_assisted_fgm", "pct_unassisted_fgm",
                ])
            else:
                st.info("No scoring data.")

        with p_t_usage:
            usage = get_player_usage(pid)
            if usage:
                _show_df(usage, [
                    "game_date", "matchup", "minutes", "usg_pct",
                    "pct_fgm", "pct_fga", "pct_fg3m", "pct_fg3a",
                    "pct_ftm", "pct_fta", "pct_oreb", "pct_dreb",
                    "pct_reb", "pct_ast", "pct_tov", "pct_stl",
                    "pct_blk", "pct_pts",
                ])
            else:
                st.info("No usage data.")

        with p_t_shots:
            shots = get_player_shot_chart(pid)
            if shots:
                df_shots = pd.DataFrame(shots)
                if "shot_zone_basic" in df_shots.columns:
                    st.markdown(
                        '<div class="section-hdr">Shot Zone Summary</div>',
                        unsafe_allow_html=True,
                    )
                    zone_summary = (
                        df_shots.groupby("shot_zone_basic")
                        .agg(
                            attempts=("shot_attempted_flag", "sum"),
                            makes=("shot_made_flag", "sum"),
                        )
                        .reset_index()
                    )
                    zone_summary["fg_pct"] = (
                        zone_summary["makes"] / zone_summary["attempts"]
                    ).round(3)
                    st.dataframe(
                        zone_summary.sort_values("attempts", ascending=False),
                        use_container_width=True,
                        hide_index=True,
                    )
                st.caption(f"Showing {len(shots)} shot attempts")
                _show_df(shots, [
                    "game_date", "period", "event_type", "action_type",
                    "shot_type", "shot_zone_basic", "shot_zone_area",
                    "shot_distance", "shot_made_flag", "loc_x", "loc_y",
                ], height=400)
            else:
                st.info("No shot chart data.")

        with p_t_tracking:
            tracking = get_player_tracking(pid)
            if tracking:
                _show_df(tracking, [
                    "game_date", "matchup", "minutes", "speed",
                    "distance", "touches", "passes", "assists",
                    "contested_fg_made", "contested_fg_attempted",
                    "contested_fg_pct", "uncontested_fg_made",
                    "uncontested_fg_attempted", "uncontested_fg_pct",
                    "defended_at_rim_fg_made",
                    "defended_at_rim_fg_attempted",
                    "defended_at_rim_fg_pct",
                ])
            else:
                st.info("No tracking data.")

        with p_t_clutch:
            clutch = get_player_clutch(pid)
            if clutch:
                _show_df(clutch, [
                    "season", "team_abbreviation", "gp", "min", "pts",
                    "reb", "ast", "stl", "blk", "tov", "fg_pct",
                    "fg3_pct", "ft_pct", "plus_minus",
                ])
            else:
                st.info("No clutch data.")

        with p_t_hustle:
            hustle = get_player_hustle(pid)
            if hustle:
                _show_df(hustle, [
                    "season", "team_abbreviation", "gp", "min",
                    "contested_shots", "contested_shots_2pt",
                    "contested_shots_3pt", "deflections", "charges_drawn",
                    "screen_assists", "screen_ast_pts", "loose_balls",
                    "off_boxouts", "def_boxouts", "boxouts",
                ])
            else:
                st.info("No hustle data.")

        with p_t_matchups:
            matchups = get_player_matchups(pid)
            if matchups:
                _show_df(matchups, [
                    "game_date", "game_matchup", "defender_name",
                    "matchup_min", "partial_poss", "player_pts",
                    "matchup_fgm", "matchup_fga", "matchup_fg_pct",
                    "matchup_fg3m", "matchup_fg3a", "matchup_fg3_pct",
                    "matchup_ast", "matchup_tov", "matchup_blk",
                    "switches_on",
                ])
            else:
                st.info("No matchup data.")

        with p_t_awards:
            awards = get_player_awards(pid)
            if awards:
                _show_df(awards, [
                    "season", "description", "type", "subtype1",
                    "all_nba_team_number", "conference",
                ])
            else:
                st.info("No awards data.")


# ─────────────────────────────────────────────────────────────────────────
# PAGE: STANDINGS
# ─────────────────────────────────────────────────────────────────────────

elif current_page == "standings":
    st.title("🏆 League Standings")

    standings_data = get_standings()
    if standings_data:
        east = [s for s in standings_data if s.get("conference") == "East"]
        west = [s for s in standings_data if s.get("conference") == "West"]

        col_e, col_w = st.columns(2)
        standing_cols = [
            "playoff_rank", "abbreviation", "team_name", "wins", "losses",
            "win_pct", "home", "road", "l10", "str_current_streak",
            "conference_games_back", "points_pg", "opp_points_pg",
            "diff_points_pg",
        ]

        with col_e:
            st.markdown("### 🔵 Eastern Conference")
            if east:
                _show_df(east, standing_cols, height=550)

        with col_w:
            st.markdown("### 🟠 Western Conference")
            if west:
                _show_df(west, standing_cols, height=550)

        st.divider()
        st.markdown("### 📋 Full Standings Detail")
        _show_df(standings_data, height=500)
    else:
        st.info("No standings data available. Run a data sync to populate.")


# ─────────────────────────────────────────────────────────────────────────
# PAGE: TEAMS BROWSE
# ─────────────────────────────────────────────────────────────────────────

elif current_page == "teams_browse":
    st.title("🏟️ Teams")

    all_teams = get_teams()
    if all_teams:
        st.caption("Select a team to view details, roster, stats, and more.")
        # Group by conference
        east_teams = [t for t in all_teams if t.get("conference") == "East"]
        west_teams = [t for t in all_teams if t.get("conference") == "West"]

        ce, cw = st.columns(2)
        with ce:
            st.markdown("### 🔵 Eastern Conference")
            for t in east_teams:
                if st.button(
                    f"🏟️ {t['abbreviation']} — {t['team_name']}",
                    key=f"tb_e_{t['team_id']}",
                    use_container_width=True,
                ):
                    _nav("team_detail", selected_team_id=t["team_id"])
                    st.rerun()

        with cw:
            st.markdown("### 🟠 Western Conference")
            for t in west_teams:
                if st.button(
                    f"🏟️ {t['abbreviation']} — {t['team_name']}",
                    key=f"tb_w_{t['team_id']}",
                    use_container_width=True,
                ):
                    _nav("team_detail", selected_team_id=t["team_id"])
                    st.rerun()
    else:
        st.info("No teams loaded yet.")


# ─────────────────────────────────────────────────────────────────────────
# PAGE: TEAM DETAIL
# ─────────────────────────────────────────────────────────────────────────

elif current_page == "team_detail":
    tid = st.session_state.selected_team_id

    if st.button("← Back to Teams", key="back_teams"):
        _nav("teams_browse")
        st.rerun()

    if not tid:
        st.warning("No team selected.")
    else:
        all_teams = get_teams()
        team_data = next((t for t in all_teams if t["team_id"] == tid), {})
        abbrev = team_data.get("abbreviation", "")

        st.title(f"🏟️ {abbrev} — {team_data.get('team_name', '')}")

        # Overview metrics
        ov = st.columns(5)
        ov[0].metric("Conference", team_data.get("conference", "N/A"))
        ov[1].metric("Division", team_data.get("division", "N/A"))
        ov[2].metric("Pace", team_data.get("pace", "N/A"))
        ov[3].metric("ORtg", team_data.get("ortg", "N/A"))
        ov[4].metric("DRtg", team_data.get("drtg", "N/A"))

        st.divider()

        (t_tab_roster, t_tab_games, t_tab_details, t_tab_synergy,
         t_tab_clutch, t_tab_hustle, t_tab_metrics, t_tab_dvp) = st.tabs([
            "👥 Roster (click players)",
            "📊 Recent Games",
            "🏢 Details",
            "🎭 Synergy",
            "🔥 Clutch",
            "💪 Hustle",
            "📈 Metrics",
            "🛡️ Def vs Pos",
        ])

        with t_tab_roster:
            roster = get_team_roster(tid)
            if roster:
                for p in roster:
                    _player_button(
                        p["player_id"],
                        p.get("full_name", ""),
                        p.get("position"),
                        key_prefix=f"tr_{tid}",
                    )
            else:
                st.info("No roster data.")

        with t_tab_games:
            team_games = get_team_stats(tid, last_n=20)
            if team_games:
                _show_df(team_games, [
                    "game_date", "matchup", "points_scored",
                    "points_allowed", "pace_est", "ortg_est", "drtg_est",
                ])
            else:
                st.info("No game stats.")

        with t_tab_details:
            details = get_team_details(tid)
            if details:
                dc = st.columns(3)
                dc[0].metric("Arena", details.get("arena", "N/A"))
                dc[1].metric(
                    "Capacity",
                    f"{details['arena_capacity']:,}"
                    if details.get("arena_capacity") is not None else "N/A",
                )
                dc[2].metric("Founded", details.get("year_founded", "N/A"))
                dc2 = st.columns(3)
                dc2[0].metric("Coach", details.get("head_coach", "N/A"))
                dc2[1].metric("GM", details.get("general_manager", "N/A"))
                dc2[2].metric("Owner", details.get("owner", "N/A"))
            else:
                st.info("No team details.")

        with t_tab_synergy:
            synergy = get_team_synergy(tid)
            if synergy:
                _show_df(synergy, [
                    "season_id", "play_type", "type_grouping",
                    "percentile", "poss_pct", "ppp", "fg_pct",
                    "efg_pct", "tov_poss_pct", "score_poss_pct",
                    "poss", "pts",
                ])
            else:
                st.info("No synergy data.")

        with t_tab_clutch:
            t_clutch = get_team_clutch(tid)
            if t_clutch:
                _show_df(t_clutch, [
                    "season", "gp", "w", "l", "w_pct", "pts", "reb",
                    "ast", "stl", "blk", "tov", "fg_pct", "fg3_pct",
                    "ft_pct", "plus_minus",
                ])
            else:
                st.info("No clutch data.")

        with t_tab_hustle:
            t_hustle = get_team_hustle(tid)
            if t_hustle:
                _show_df(t_hustle, [
                    "season", "contested_shots", "deflections",
                    "charges_drawn", "screen_assists", "loose_balls",
                    "off_boxouts", "def_boxouts", "boxouts",
                ])
            else:
                st.info("No hustle data.")

        with t_tab_metrics:
            t_metrics = get_team_estimated_metrics(tid)
            if t_metrics:
                _show_df(t_metrics, [
                    "season", "gp", "w", "l", "w_pct",
                    "e_off_rating", "e_def_rating", "e_net_rating",
                    "e_pace", "e_reb_pct", "e_tm_tov_pct",
                ])
            else:
                st.info("No estimated metrics.")

        with t_tab_dvp:
            if abbrev:
                dvp = get_defense_vs_position(abbrev)
                if dvp:
                    st.caption(
                        "Multiplier > 1.0 = weaker defense (allows more). "
                        "< 1.0 = tougher defense."
                    )
                    _show_df(dvp, [
                        "pos", "vs_pts_mult", "vs_reb_mult",
                        "vs_ast_mult", "vs_stl_mult", "vs_blk_mult",
                        "vs_3pm_mult",
                    ])
                else:
                    st.info("No defense-vs-position data.")


# ─────────────────────────────────────────────────────────────────────────
# PAGE: LEADERS & STATS
# ─────────────────────────────────────────────────────────────────────────

elif current_page == "leaders":
    st.title("📊 League Leaders & Season Stats")

    l_tab_leaders, l_tab_players, l_tab_teams = st.tabs([
        "🏅 League Leaders",
        "👤 Season Player Stats",
        "🏟️ Season Team Stats",
    ])

    with l_tab_leaders:
        leaders = get_league_leaders()
        if leaders:
            _show_df(leaders, [
                "rank", "full_name", "position", "team_abbreviation",
                "gp", "min", "pts", "reb", "ast", "stl", "blk",
                "tov", "fg_pct", "fg3_pct", "ft_pct", "eff",
            ], height=600)
            # Clickable player list
            st.markdown('<div class="section-hdr">Click a player</div>',
                        unsafe_allow_html=True)
            for ldr in leaders[:25]:
                _player_button(
                    ldr.get("player_id"),
                    ldr.get("full_name", ""),
                    ldr.get("position"),
                    ldr.get("team_abbreviation"),
                    key_prefix="ldr",
                )
        else:
            st.info("No league leaders data.")

    with l_tab_players:
        dash_players = get_league_dash_players()
        if dash_players:
            _show_df(dash_players, [
                "full_name", "position", "team_abbreviation", "season",
                "gp", "w", "l", "min", "pts", "reb", "ast", "stl",
                "blk", "tov", "fg_pct", "fg3_pct", "ft_pct",
                "plus_minus", "nba_fantasy_pts", "dd2", "td3",
            ], height=600)
        else:
            st.info("No season player stats.")

    with l_tab_teams:
        dash_teams = get_league_dash_teams()
        if dash_teams:
            _show_df(dash_teams, [
                "abbreviation", "team_name", "season", "gp", "w", "l",
                "w_pct", "pts", "reb", "ast", "stl", "blk", "tov",
                "fg_pct", "fg3_pct", "ft_pct", "plus_minus",
            ], height=600)
        else:
            st.info("No season team stats.")


# ─────────────────────────────────────────────────────────────────────────
# PAGE: DEFENSE VS POSITION
# ─────────────────────────────────────────────────────────────────────────

elif current_page == "defense":
    st.title("🛡️ Defense vs Position")
    st.caption(
        "How each team defends different positions. "
        "Multiplier > 1.0 = allows more than average. < 1.0 = tougher."
    )

    dvp_teams = get_teams()
    if dvp_teams:
        selected_dvp = st.selectbox(
            "Select a team (or All Teams)",
            options=["All Teams"] + [
                t["abbreviation"] for t in dvp_teams
            ],
            key="dvp_select",
        )

        if selected_dvp == "All Teams":
            all_dvp = []
            for t in dvp_teams:
                positions = get_defense_vs_position(t["abbreviation"])
                for p in positions:
                    p["team"] = t["abbreviation"]
                    all_dvp.append(p)
            if all_dvp:
                _show_df(all_dvp, [
                    "team", "pos", "vs_pts_mult", "vs_reb_mult",
                    "vs_ast_mult", "vs_stl_mult", "vs_blk_mult",
                    "vs_3pm_mult",
                ], height=600)
            else:
                st.info("No defense-vs-position data available.")
        else:
            positions = get_defense_vs_position(selected_dvp)
            if positions:
                _show_df(positions, [
                    "pos", "vs_pts_mult", "vs_reb_mult",
                    "vs_ast_mult", "vs_stl_mult", "vs_blk_mult",
                    "vs_3pm_mult",
                ])
            else:
                st.info("No data for this team.")
    else:
        st.info("No teams loaded.")


# ─────────────────────────────────────────────────────────────────────────
# PAGE: MORE DATA
# ─────────────────────────────────────────────────────────────────────────

elif current_page == "more":
    st.title("📈 Additional Data")

    m_tab_schedule, m_tab_lineups, m_tab_draft = st.tabs([
        "🗓️ Schedule",
        "👥 Lineups",
        "📋 Draft History",
    ])

    with m_tab_schedule:
        schedule = get_schedule()
        if schedule:
            _show_df(schedule, [
                "game_date", "game_status_text", "home_team_tricode",
                "away_team_tricode", "home_team_score", "away_team_score",
                "arena_name", "arena_city", "game_id",
            ], height=500)
        else:
            st.info("No schedule data.")

    with m_tab_lineups:
        lineups = get_lineups()
        if lineups:
            _show_df(lineups, [
                "season", "group_name", "team_abbreviation", "gp",
                "w", "l", "w_pct", "min", "pts", "reb", "ast", "stl",
                "blk", "tov", "fg_pct", "fg3_pct", "ft_pct",
                "plus_minus",
            ], height=600)
        else:
            st.info("No lineup data.")

    with m_tab_draft:
        drafts = get_draft_history()
        if drafts:
            _show_df(drafts, [
                "season", "overall_pick", "round_number", "round_pick",
                "full_name", "team_abbreviation", "organization",
                "organization_type",
            ], height=600)
        else:
            st.info("No draft history data.")
