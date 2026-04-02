"""
app.py
------
Streamlit dashboard for SmartPicksProAI.

A dark, sleek, high-density "FinTech terminal" interface for viewing NBA
matchups, analysing player performance, browsing team rosters, exploring
advanced box scores, standings, league leaders, and much more.

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

# ---------------------------------------------------------------------------
# Page configuration — must be the very first Streamlit command
# ---------------------------------------------------------------------------

st.set_page_config(
    page_title="SmartPicksProAI",
    page_icon="🏀",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ---------------------------------------------------------------------------
# Dark FinTech-terminal theme (injected CSS)
# ---------------------------------------------------------------------------

st.markdown(
    """
    <style>
    /* --- dark, data-heavy terminal aesthetic --- */
    .stApp {
        background-color: #0e1117;
        color: #c9d1d9;
    }
    section[data-testid="stSidebar"] {
        background-color: #161b22;
    }
    h1, h2, h3, h4 {
        color: #58a6ff;
        letter-spacing: 0.03em;
    }
    /* tighter padding for high-density feel */
    .block-container {
        padding-top: 1.5rem;
        padding-bottom: 1rem;
    }
    /* style dataframes / tables */
    .stDataFrame, .stTable {
        font-size: 0.85rem;
    }
    /* accent buttons */
    .stButton > button {
        background-color: #238636;
        color: #ffffff;
        border: none;
        border-radius: 4px;
    }
    .stButton > button:hover {
        background-color: #2ea043;
    }
    /* metric cards */
    [data-testid="stMetric"] {
        background-color: #161b22;
        border: 1px solid #30363d;
        border-radius: 6px;
        padding: 0.6rem 0.8rem;
    }
    /* tabs styling */
    .stTabs [data-baseweb="tab-list"] {
        gap: 2px;
    }
    .stTabs [data-baseweb="tab"] {
        background-color: #161b22;
        border-radius: 4px 4px 0 0;
        color: #8b949e;
        padding: 0.5rem 1rem;
    }
    .stTabs [aria-selected="true"] {
        background-color: #0e1117;
        color: #58a6ff;
        border-bottom: 2px solid #58a6ff;
    }
    /* stat card styling */
    .stat-card {
        background: #161b22;
        border: 1px solid #30363d;
        border-radius: 8px;
        padding: 1rem;
        margin-bottom: 0.5rem;
        text-align: center;
    }
    .stat-card .value {
        font-size: 1.8rem;
        font-weight: 700;
        color: #58a6ff;
    }
    .stat-card .label {
        font-size: 0.75rem;
        color: #8b949e;
        text-transform: uppercase;
        letter-spacing: 0.05em;
    }
    /* matchup game card */
    .game-card {
        background: #161b22;
        border: 1px solid #30363d;
        border-radius: 6px;
        padding: 0.8rem;
        margin-bottom: 0.5rem;
        text-align: center;
    }
    .game-card .teams {
        color: #58a6ff;
        font-weight: 600;
        font-size: 1rem;
    }
    .game-card .score {
        color: #c9d1d9;
        font-size: 0.85rem;
    }
    .game-card .meta {
        color: #8b949e;
        font-size: 0.75rem;
    }
    /* section headers */
    .section-header {
        color: #58a6ff;
        font-size: 1.1rem;
        font-weight: 600;
        margin: 1rem 0 0.5rem 0;
        padding-bottom: 0.3rem;
        border-bottom: 1px solid #30363d;
    }
    /* empty state */
    .empty-state {
        text-align: center;
        color: #8b949e;
        padding: 2rem;
        font-style: italic;
    }
    </style>
    """,
    unsafe_allow_html=True,
)


# ---------------------------------------------------------------------------
# Helper: render a styled dataframe
# ---------------------------------------------------------------------------

def _show_df(data: list[dict], columns: list[str] | None = None,
             height: int | None = None) -> None:
    """Display a list of dicts as a Streamlit dataframe with optional column
    filter and height."""
    if not data:
        st.markdown('<div class="empty-state">No data available.</div>',
                    unsafe_allow_html=True)
        return
    df = pd.DataFrame(data)
    if columns:
        columns = [c for c in columns if c in df.columns]
        if columns:
            df = df[columns]
    kwargs: dict = {"use_container_width": True, "hide_index": True}
    if height:
        kwargs["height"] = height
    st.dataframe(df, **kwargs)


# ---------------------------------------------------------------------------
# Sidebar — Admin Controls + Navigation Help
# ---------------------------------------------------------------------------

with st.sidebar:
    st.header("⚙️ Admin Controls")
    st.caption("Manually sync the latest box scores from the NBA API.")

    if st.button("🔄 Sync Latest NBA Data", use_container_width=True):
        with st.spinner("Syncing with NBA API…"):
            result = trigger_refresh()
        if result.get("status") == "success":
            st.success(result.get("message", "Refresh complete."))
            # Bust cached GET results so the UI reloads fresh data.
            get_todays_games.clear()
            get_player_last5.clear()
            search_players.clear()
            get_teams.clear()
            get_team_roster.clear()
            get_team_stats.clear()
            get_defense_vs_position.clear()
            get_standings.clear()
            get_league_leaders.clear()
            get_recent_games.clear()
        else:
            st.error(f"Refresh failed: {result.get('message', 'Unknown error')}")

    st.divider()

    # --- Team Browser ---
    st.header("🏟️ Team Browser")
    teams = get_teams()
    if teams:
        team_labels = {
            t["team_id"]: f"{t['abbreviation']} — {t['team_name']}"
            for t in teams
        }
        selected_team_id = st.selectbox(
            "Select a team",
            options=list(team_labels.keys()),
            format_func=lambda tid: team_labels[tid],
            key="sidebar_team_select",
        )
        if selected_team_id:
            roster = get_team_roster(selected_team_id)
            if roster:
                st.caption(f"**Roster** ({len(roster)} players)")
                for p in roster:
                    pos = p.get("position") or ""
                    label = f"{p.get('full_name', '')}  {f'({pos})' if pos else ''}"
                    st.markdown(
                        f"<span style='color:#c9d1d9;font-size:0.85rem;'>"
                        f"• {label}</span>",
                        unsafe_allow_html=True,
                    )
            else:
                st.info("No roster data yet — run initial_pull.py to seed.")
    else:
        st.info("No teams loaded yet.")

    st.divider()
    st.caption("SmartPicksProAI v2.0 — Full Data Dashboard")


# ---------------------------------------------------------------------------
# Header
# ---------------------------------------------------------------------------

st.title("🏀 SmartPicksProAI")
st.caption("NBA Data Intelligence Terminal — All Tables, All Data")

# ---------------------------------------------------------------------------
# Main Tab Layout
# ---------------------------------------------------------------------------

tab_home, tab_standings, tab_players, tab_teams, tab_leaders, \
    tab_defense, tab_games, tab_more = st.tabs([
        "🏠 Home",
        "🏆 Standings",
        "👤 Player Deep Dive",
        "🏟️ Team Central",
        "📊 Leaders & Stats",
        "🛡️ Defense vs Position",
        "🎮 Game Explorer",
        "📈 More Data",
    ])

# ═══════════════════════════════════════════════════════════════════════════
# TAB 1: HOME — Today's Matchups + Player Search
# ═══════════════════════════════════════════════════════════════════════════

with tab_home:
    st.subheader("📅 Today's Matchups")

    games = get_todays_games()

    if games:
        cols = st.columns(min(len(games), 4))
        for idx, game in enumerate(games):
            with cols[idx % len(cols)]:
                score_line = ""
                if (game.get("home_score") is not None
                        and game.get("away_score") is not None):
                    score_line = (
                        f'<span class="score">'
                        f'{game.get("home_score", "")} – '
                        f'{game.get("away_score", "")}'
                        f'</span><br>'
                    )
                st.markdown(
                    f"""
                    <div class="game-card">
                        <span class="teams">
                            {game.get("matchup", "TBD")}
                        </span><br>
                        {score_line}
                        <span class="meta">
                            {game.get("game_id", "")}
                        </span>
                    </div>
                    """,
                    unsafe_allow_html=True,
                )
    else:
        st.info("No games found for today. The schedule may not be loaded yet.")

    st.divider()

    # --- Player Performance Card ---
    st.subheader("🔍 Player Performance Card")
    st.caption("Search for an NBA player by name, or enter a player ID directly.")

    search_col, id_col = st.columns([3, 1])

    with search_col:
        player_query = st.text_input(
            "Search by name",
            placeholder="e.g. LeBron, Curry, Jokic …",
            key="home_player_search",
        )
    with id_col:
        player_id_direct = st.number_input(
            "Or enter ID",
            min_value=0,
            value=0,
            step=1,
            help="e.g. 2544 = LeBron James, 201939 = Stephen Curry",
            key="home_player_id",
        )

    selected_player_id: Optional[int] = None

    if player_query.strip():
        results = search_players(player_query.strip())
        if results:
            options = {
                r["player_id"]: (
                    f"{r.get('full_name', '')}  "
                    f"({r.get('team_abbreviation', '')}"
                    f"{', ' + r['position'] if r.get('position') else ''})"
                )
                for r in results
            }
            selected_player_id = st.selectbox(
                "Select a player",
                options=list(options.keys()),
                format_func=lambda pid: options[pid],
                key="home_player_select",
            )
        else:
            st.warning("No players found matching your search.")
    elif player_id_direct > 0:
        selected_player_id = player_id_direct

    if selected_player_id and st.button("Load Player Card", key="home_load_player"):
        data = get_player_last5(int(selected_player_id))

        if not data:
            st.warning("Player not found or backend is unavailable.")
        else:
            st.markdown(
                f"### {data.get('first_name', '')} {data.get('last_name', '')}"
            )

            avgs = data.get("averages", {})
            avg_cols = st.columns(6)
            stat_labels = {
                "pts": "PPG", "reb": "RPG", "ast": "APG",
                "blk": "BPG", "stl": "SPG", "tov": "TOV",
            }
            for i, (key, label) in enumerate(stat_labels.items()):
                avg_cols[i].metric(label, avgs.get(key, 0.0))

            game_logs = data.get("games", [])
            if game_logs:
                df = pd.DataFrame(game_logs)
                display_cols = [
                    c
                    for c in [
                        "game_date", "matchup", "wl", "pts", "reb", "ast",
                        "blk", "stl", "tov", "fgm", "fga", "fg_pct", "fg3m",
                        "fg3a", "fg3_pct", "ftm", "fta", "ft_pct", "oreb",
                        "dreb", "pf", "plus_minus", "min",
                    ]
                    if c in df.columns
                ]
                st.dataframe(
                    df[display_cols],
                    use_container_width=True,
                    hide_index=True,
                )
            else:
                st.info("No game logs available for this player.")


# ═══════════════════════════════════════════════════════════════════════════
# TAB 2: STANDINGS
# ═══════════════════════════════════════════════════════════════════════════

with tab_standings:
    st.subheader("🏆 League Standings")

    standings_data = get_standings()

    if standings_data:
        # Split by conference
        east = [s for s in standings_data if s.get("conference") == "East"]
        west = [s for s in standings_data if s.get("conference") == "West"]

        col_e, col_w = st.columns(2)

        with col_e:
            st.markdown(
                '<div class="section-header">🔵 Eastern Conference</div>',
                unsafe_allow_html=True,
            )
            if east:
                _show_df(east, [
                    "playoff_rank", "abbreviation", "team_name", "wins",
                    "losses", "win_pct", "home", "road", "l10",
                    "str_current_streak", "conference_games_back",
                    "points_pg", "opp_points_pg", "diff_points_pg",
                ])

        with col_w:
            st.markdown(
                '<div class="section-header">🟠 Western Conference</div>',
                unsafe_allow_html=True,
            )
            if west:
                _show_df(west, [
                    "playoff_rank", "abbreviation", "team_name", "wins",
                    "losses", "win_pct", "home", "road", "l10",
                    "str_current_streak", "conference_games_back",
                    "points_pg", "opp_points_pg", "diff_points_pg",
                ])

        st.divider()
        st.markdown(
            '<div class="section-header">📋 Full Standings Detail</div>',
            unsafe_allow_html=True,
        )
        _show_df(standings_data, height=500)
    else:
        st.info("No standings data available. Run a data sync to populate.")


# ═══════════════════════════════════════════════════════════════════════════
# TAB 3: PLAYER DEEP DIVE
# ═══════════════════════════════════════════════════════════════════════════

with tab_players:
    st.subheader("👤 Player Deep Dive")
    st.caption("Search for a player to explore all available data.")

    p_search = st.text_input(
        "Search player by name",
        placeholder="e.g. LeBron, Giannis, Luka …",
        key="deep_player_search",
    )

    deep_player_id: Optional[int] = None

    if p_search.strip():
        p_results = search_players(p_search.strip())
        if p_results:
            p_options = {
                r["player_id"]: (
                    f"{r.get('full_name', '')}  "
                    f"({r.get('team_abbreviation', '')}"
                    f"{', ' + r['position'] if r.get('position') else ''})"
                )
                for r in p_results
            }
            deep_player_id = st.selectbox(
                "Select a player",
                options=list(p_options.keys()),
                format_func=lambda pid: p_options[pid],
                key="deep_player_select",
            )
        else:
            st.warning("No players found.")

    if deep_player_id:
        # Sub-tabs for player data
        p_tab_bio, p_tab_last5, p_tab_career, p_tab_advanced, \
            p_tab_scoring, p_tab_usage, p_tab_shots, p_tab_tracking, \
            p_tab_clutch, p_tab_hustle, p_tab_matchups, p_tab_awards = st.tabs([
                "📋 Bio",
                "📊 Last 5",
                "📈 Career",
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

        pid = int(deep_player_id)

        with p_tab_bio:
            bio = get_player_bio(pid)
            if bio:
                bio_cols = st.columns(4)
                bio_cols[0].metric("Height", bio.get("player_height", "N/A"))
                bio_cols[1].metric(
                    "Weight", f"{bio.get('player_weight', 'N/A')} lbs"
                )
                bio_cols[2].metric("Age", bio.get("age", "N/A"))
                bio_cols[3].metric("College", bio.get("college", "N/A"))

                bio_cols2 = st.columns(4)
                bio_cols2[0].metric("Country", bio.get("country", "N/A"))
                bio_cols2[1].metric("Draft Year", bio.get("draft_year", "N/A"))
                bio_cols2[2].metric("Draft Round", bio.get("draft_round", "N/A"))
                bio_cols2[3].metric("Draft Pick", bio.get("draft_number", "N/A"))

                if bio.get("gp"):
                    st.divider()
                    st.markdown(
                        '<div class="section-header">Season Averages</div>',
                        unsafe_allow_html=True,
                    )
                    s_cols = st.columns(6)
                    s_cols[0].metric("GP", bio.get("gp", 0))
                    s_cols[1].metric("PPG", bio.get("pts", 0))
                    s_cols[2].metric("RPG", bio.get("reb", 0))
                    s_cols[3].metric("APG", bio.get("ast", 0))
                    s_cols[4].metric("TS%", f"{(bio.get('ts_pct') or 0):.1%}")
                    s_cols[5].metric("USG%", f"{(bio.get('usg_pct') or 0):.1%}")
            else:
                st.info("No bio data available for this player.")

        with p_tab_last5:
            last5 = get_player_last5(pid)
            if last5:
                avgs = last5.get("averages", {})
                mcols = st.columns(6)
                stat_map = {
                    "pts": "PPG", "reb": "RPG", "ast": "APG",
                    "blk": "BPG", "stl": "SPG", "tov": "TOV",
                }
                for i, (k, lbl) in enumerate(stat_map.items()):
                    mcols[i].metric(lbl, avgs.get(k, 0.0))

                _show_df(
                    last5.get("games", []),
                    [
                        "game_date", "matchup", "wl", "pts", "reb", "ast",
                        "blk", "stl", "tov", "fgm", "fga", "fg_pct",
                        "fg3m", "fg3a", "fg3_pct", "ftm", "fta", "ft_pct",
                        "oreb", "dreb", "pf", "plus_minus", "min",
                    ],
                )
            else:
                st.info("No recent game data.")

        with p_tab_career:
            career = get_player_career(pid)
            if career:
                _show_df(career, [
                    "season_id", "team_abbreviation", "player_age", "gp",
                    "gs", "min", "pts", "reb", "ast", "stl", "blk", "tov",
                    "fgm", "fga", "fg_pct", "fg3m", "fg3a", "fg3_pct",
                    "ftm", "fta", "ft_pct", "oreb", "dreb", "pf",
                ])
            else:
                st.info("No career data available.")

        with p_tab_advanced:
            adv = get_player_advanced(pid)
            if adv:
                _show_df(adv, [
                    "game_date", "matchup", "minutes", "off_rating",
                    "def_rating", "net_rating", "ts_pct", "efg_pct",
                    "usg_pct", "ast_pct", "oreb_pct", "dreb_pct",
                    "reb_pct", "tov_ratio", "pace", "pie",
                ])
            else:
                st.info("No advanced box score data.")

        with p_tab_scoring:
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
                st.info("No scoring breakdown data.")

        with p_tab_usage:
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

        with p_tab_shots:
            shots = get_player_shot_chart(pid)
            if shots:
                st.markdown(
                    '<div class="section-header">Shot Distribution</div>',
                    unsafe_allow_html=True,
                )
                df_shots = pd.DataFrame(shots)
                # Summary by zone
                if "shot_zone_basic" in df_shots.columns:
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

                st.divider()
                st.caption(f"Showing {len(shots)} shot attempts")
                _show_df(shots, [
                    "game_date", "period", "event_type", "action_type",
                    "shot_type", "shot_zone_basic", "shot_zone_area",
                    "shot_distance", "shot_made_flag", "loc_x", "loc_y",
                ], height=400)
            else:
                st.info("No shot chart data.")

        with p_tab_tracking:
            tracking = get_player_tracking(pid)
            if tracking:
                _show_df(tracking, [
                    "game_date", "matchup", "minutes", "speed",
                    "distance", "touches", "passes", "assists",
                    "contested_fg_made", "contested_fg_attempted",
                    "contested_fg_pct", "uncontested_fg_made",
                    "uncontested_fg_attempted", "uncontested_fg_pct",
                    "defended_at_rim_fg_made", "defended_at_rim_fg_attempted",
                    "defended_at_rim_fg_pct",
                ])
            else:
                st.info("No tracking data.")

        with p_tab_clutch:
            clutch = get_player_clutch(pid)
            if clutch:
                _show_df(clutch, [
                    "season", "team_abbreviation", "gp", "min", "pts",
                    "reb", "ast", "stl", "blk", "tov", "fg_pct",
                    "fg3_pct", "ft_pct", "plus_minus",
                ])
            else:
                st.info("No clutch data.")

        with p_tab_hustle:
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

        with p_tab_matchups:
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

        with p_tab_awards:
            awards = get_player_awards(pid)
            if awards:
                _show_df(awards, [
                    "season", "description", "type", "subtype1",
                    "all_nba_team_number", "conference",
                ])
            else:
                st.info("No awards data.")


# ═══════════════════════════════════════════════════════════════════════════
# TAB 4: TEAM CENTRAL
# ═══════════════════════════════════════════════════════════════════════════

with tab_teams:
    st.subheader("🏟️ Team Central")

    all_teams = get_teams()
    if all_teams:
        team_map = {
            t["team_id"]: f"{t['abbreviation']} — {t['team_name']}"
            for t in all_teams
        }
        team_abbrev_map = {t["team_id"]: t["abbreviation"] for t in all_teams}

        chosen_team = st.selectbox(
            "Select a team to explore",
            options=list(team_map.keys()),
            format_func=lambda tid: team_map[tid],
            key="team_central_select",
        )

        if chosen_team:
            # Team overview metrics
            team_data = next(
                (t for t in all_teams if t["team_id"] == chosen_team), {}
            )
            tcols = st.columns(5)
            tcols[0].metric("Team", team_data.get("abbreviation", ""))
            tcols[1].metric("Conference", team_data.get("conference", "N/A"))
            tcols[2].metric("Pace", team_data.get("pace", "N/A"))
            tcols[3].metric("ORtg", team_data.get("ortg", "N/A"))
            tcols[4].metric("DRtg", team_data.get("drtg", "N/A"))

            t_tab_roster, t_tab_games, t_tab_details, t_tab_synergy, \
                t_tab_clutch, t_tab_hustle, t_tab_metrics, \
                t_tab_dvp = st.tabs([
                    "👥 Roster",
                    "📊 Recent Games",
                    "🏢 Details",
                    "🎭 Synergy",
                    "🔥 Clutch",
                    "💪 Hustle",
                    "📈 Metrics",
                    "🛡️ Def vs Pos",
                ])

            with t_tab_roster:
                roster = get_team_roster(chosen_team)
                if roster:
                    _show_df(roster, [
                        "full_name", "position", "team_abbreviation",
                    ])
                else:
                    st.info("No roster data.")

            with t_tab_games:
                team_games = get_team_stats(chosen_team, last_n=20)
                if team_games:
                    _show_df(team_games, [
                        "game_date", "matchup", "points_scored",
                        "points_allowed", "pace_est", "ortg_est", "drtg_est",
                    ])
                else:
                    st.info("No game stats.")

            with t_tab_details:
                details = get_team_details(chosen_team)
                if details:
                    d_cols = st.columns(3)
                    d_cols[0].metric("Arena", details.get("arena", "N/A"))
                    d_cols[1].metric(
                        "Capacity",
                        f"{details.get('arena_capacity', 'N/A'):,}"
                        if details.get("arena_capacity")
                        else "N/A",
                    )
                    d_cols[2].metric(
                        "Founded",
                        details.get("year_founded", "N/A"),
                    )
                    d_cols2 = st.columns(3)
                    d_cols2[0].metric(
                        "Head Coach",
                        details.get("head_coach", "N/A"),
                    )
                    d_cols2[1].metric("GM", details.get("general_manager", "N/A"))
                    d_cols2[2].metric("Owner", details.get("owner", "N/A"))
                else:
                    st.info("No team details.")

            with t_tab_synergy:
                synergy = get_team_synergy(chosen_team)
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
                t_clutch = get_team_clutch(chosen_team)
                if t_clutch:
                    _show_df(t_clutch, [
                        "season", "gp", "w", "l", "w_pct", "pts",
                        "reb", "ast", "stl", "blk", "tov", "fg_pct",
                        "fg3_pct", "ft_pct", "plus_minus",
                    ])
                else:
                    st.info("No clutch data.")

            with t_tab_hustle:
                t_hustle = get_team_hustle(chosen_team)
                if t_hustle:
                    _show_df(t_hustle, [
                        "season", "contested_shots", "deflections",
                        "charges_drawn", "screen_assists", "loose_balls",
                        "off_boxouts", "def_boxouts", "boxouts",
                    ])
                else:
                    st.info("No hustle data.")

            with t_tab_metrics:
                t_metrics = get_team_estimated_metrics(chosen_team)
                if t_metrics:
                    _show_df(t_metrics, [
                        "season", "gp", "w", "l", "w_pct",
                        "e_off_rating", "e_def_rating", "e_net_rating",
                        "e_pace", "e_reb_pct", "e_tm_tov_pct",
                    ])
                else:
                    st.info("No estimated metrics.")

            with t_tab_dvp:
                abbrev = team_abbrev_map.get(chosen_team, "")
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
    else:
        st.info("No teams loaded yet.")


# ═══════════════════════════════════════════════════════════════════════════
# TAB 5: LEADERS & SEASON STATS
# ═══════════════════════════════════════════════════════════════════════════

with tab_leaders:
    st.subheader("📊 League Leaders & Season Stats")

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
                "gp", "min", "pts", "reb", "ast", "stl", "blk", "tov",
                "fg_pct", "fg3_pct", "ft_pct", "eff",
            ], height=600)
        else:
            st.info("No league leaders data.")

    with l_tab_players:
        dash_players = get_league_dash_players()
        if dash_players:
            _show_df(dash_players, [
                "full_name", "position", "team_abbreviation", "season",
                "gp", "w", "l", "min", "pts", "reb", "ast", "stl", "blk",
                "tov", "fg_pct", "fg3_pct", "ft_pct", "plus_minus",
                "nba_fantasy_pts", "dd2", "td3",
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


# ═══════════════════════════════════════════════════════════════════════════
# TAB 6: DEFENSE VS POSITION
# ═══════════════════════════════════════════════════════════════════════════

with tab_defense:
    st.subheader("🛡️ Defense vs Position — All Teams")
    st.caption(
        "How each team defends different positions. "
        "Multiplier > 1.0 = allows more than avg. < 1.0 = tougher."
    )

    dvp_teams = get_teams()
    if dvp_teams:
        # Build a combined table of all teams
        all_dvp: list[dict] = []
        selected_dvp_team = st.selectbox(
            "Select a team (or view all below)",
            options=["All Teams"] + [
                t["abbreviation"] for t in dvp_teams
            ],
            key="dvp_team_select",
        )

        if selected_dvp_team == "All Teams":
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
            positions = get_defense_vs_position(selected_dvp_team)
            if positions:
                _show_df(positions, [
                    "pos", "vs_pts_mult", "vs_reb_mult", "vs_ast_mult",
                    "vs_stl_mult", "vs_blk_mult", "vs_3pm_mult",
                ])
            else:
                st.info("No data for this team.")
    else:
        st.info("No teams loaded.")


# ═══════════════════════════════════════════════════════════════════════════
# TAB 7: GAME EXPLORER
# ═══════════════════════════════════════════════════════════════════════════

with tab_games:
    st.subheader("🎮 Game Explorer")

    g_tab_recent, g_tab_schedule, g_tab_detail = st.tabs([
        "📅 Recent Games",
        "🗓️ Schedule",
        "🔎 Game Detail",
    ])

    with g_tab_recent:
        recent = get_recent_games()
        if recent:
            _show_df(recent, [
                "game_date", "matchup", "home_abbrev", "away_abbrev",
                "home_score", "away_score", "season", "game_id",
            ], height=500)
        else:
            st.info("No recent games.")

    with g_tab_schedule:
        schedule = get_schedule()
        if schedule:
            _show_df(schedule, [
                "game_date", "game_status_text", "home_team_tricode",
                "away_team_tricode", "home_team_score", "away_team_score",
                "arena_name", "arena_city", "game_id",
            ], height=500)
        else:
            st.info("No schedule data.")

    with g_tab_detail:
        st.caption("Enter a game ID to explore box score, play-by-play, "
                    "win probability, and rotations.")
        game_id_input = st.text_input(
            "Game ID",
            placeholder="e.g. 0022501050",
            key="game_detail_id",
        )

        if game_id_input.strip():
            gid = game_id_input.strip()

            gd_tab_box, gd_tab_pbp, gd_tab_wp, gd_tab_rot = st.tabs([
                "📊 Box Score",
                "📝 Play-by-Play",
                "📈 Win Probability",
                "🔄 Rotation",
            ])

            with gd_tab_box:
                box = get_game_box_score(gid)
                if box:
                    _show_df(box, [
                        "full_name", "position", "team_abbreviation",
                        "pts", "reb", "ast", "stl", "blk", "tov",
                        "fgm", "fga", "fg_pct", "fg3m", "fg3a",
                        "fg3_pct", "ftm", "fta", "ft_pct", "oreb",
                        "dreb", "pf", "plus_minus", "min", "wl",
                    ], height=500)
                else:
                    st.info("No box score data for this game.")

            with gd_tab_pbp:
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

            with gd_tab_wp:
                wp = get_win_probability(gid)
                if wp:
                    df_wp = pd.DataFrame(wp)
                    if "home_pct" in df_wp.columns:
                        st.line_chart(
                            df_wp.set_index("event_num")[["home_pct", "visitor_pct"]],
                            use_container_width=True,
                        )
                    st.divider()
                    _show_df(wp, [
                        "event_num", "home_pct", "visitor_pct",
                        "home_pts", "visitor_pts", "home_score_margin",
                        "period", "description",
                    ], height=400)
                else:
                    st.info("No win probability data.")

            with gd_tab_rot:
                rot = get_game_rotation(gid)
                if rot:
                    _show_df(rot, [
                        "full_name", "team_abbrev", "in_time_real",
                        "out_time_real", "player_pts", "pt_diff",
                        "usg_pct",
                    ], height=500)
                else:
                    st.info("No rotation data.")


# ═══════════════════════════════════════════════════════════════════════════
# TAB 8: MORE DATA (Lineups, Draft, etc.)
# ═══════════════════════════════════════════════════════════════════════════

with tab_more:
    st.subheader("📈 Additional Data")

    m_tab_lineups, m_tab_draft = st.tabs([
        "👥 Lineups",
        "📋 Draft History",
    ])

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
