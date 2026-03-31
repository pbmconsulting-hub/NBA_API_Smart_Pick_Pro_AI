"""
app.py
------
Streamlit dashboard for SmartPicksProAI.

A dark, sleek, high-density "FinTech terminal" interface for viewing NBA
matchups, analysing player performance, browsing team rosters, and
triggering on-demand data refreshes.

Start the dashboard::

    cd SmartPicksProAI/frontend
    streamlit run app.py
"""

import pandas as pd
import streamlit as st

from typing import Optional

from api_service import (
    get_defense_vs_position,
    get_player_last5,
    get_team_roster,
    get_team_stats,
    get_teams,
    get_todays_games,
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
    </style>
    """,
    unsafe_allow_html=True,
)

# ---------------------------------------------------------------------------
# Sidebar — Admin Controls + Team Browser
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
        else:
            st.error(f"Refresh failed: {result.get('message', 'Unknown error')}")

    st.divider()

    # --- Team Browser ---
    st.header("🏟️ Team Browser")
    teams = get_teams()
    if teams:
        team_labels = {t["team_id"]: f"{t['abbreviation']} — {t['team_name']}" for t in teams}
        selected_team_id = st.selectbox(
            "Select a team",
            options=list(team_labels.keys()),
            format_func=lambda tid: team_labels[tid],
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
    st.caption("SmartPicksProAI v1.0 — local MVP")

# ---------------------------------------------------------------------------
# Header
# ---------------------------------------------------------------------------

st.title("🏀 SmartPicksProAI")
st.caption("NBA Player Props Projection Terminal")

# ---------------------------------------------------------------------------
# Section 1 — Today's Matchup Grid
# ---------------------------------------------------------------------------

st.subheader("📅 Today's Matchups")

games = get_todays_games()

if games:
    cols = st.columns(min(len(games), 4))
    for idx, game in enumerate(games):
        with cols[idx % len(cols)]:
            score_line = ""
            if game.get("home_score") is not None and game.get("away_score") is not None:
                score_line = (
                    f'<span style="color:#c9d1d9;font-size:0.85rem;">'
                    f'{game.get("home_score", "")} – {game.get("away_score", "")}'
                    f'</span><br>'
                )
            st.markdown(
                f"""
                <div style="
                    background:#161b22;
                    border:1px solid #30363d;
                    border-radius:6px;
                    padding:0.8rem;
                    margin-bottom:0.5rem;
                    text-align:center;
                ">
                    <span style="color:#58a6ff;font-weight:600;">
                        {game.get("matchup", "TBD")}
                    </span><br>
                    {score_line}
                    <span style="color:#8b949e;font-size:0.75rem;">
                        {game.get("game_id", "")}
                    </span>
                </div>
                """,
                unsafe_allow_html=True,
            )
else:
    st.info("No games found for today. The schedule may not be loaded yet.")

st.divider()

# ---------------------------------------------------------------------------
# Section 2 — Player Performance Card (search by name or ID)
# ---------------------------------------------------------------------------

st.subheader("🔍 Player Performance Card")
st.caption("Search for an NBA player by name, or enter a player ID directly.")

search_col, id_col = st.columns([3, 1])

with search_col:
    player_query = st.text_input(
        "Search by name",
        placeholder="e.g. LeBron, Curry, Jokic …",
    )
with id_col:
    player_id_direct = st.number_input(
        "Or enter ID",
        min_value=0,
        value=0,
        step=1,
        help="e.g. 2544 = LeBron James, 201939 = Stephen Curry",
    )

# Resolve which player_id to display.
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
        )
    else:
        st.warning("No players found matching your search.")
elif player_id_direct > 0:
    selected_player_id = player_id_direct

if selected_player_id and st.button("Load Player Card"):
    data = get_player_last5(int(selected_player_id))

    if not data:
        st.warning("Player not found or backend is unavailable.")
    else:
        # Player header
        st.markdown(
            f"### {data.get('first_name', '')} {data.get('last_name', '')}"
        )

        # Averages (metric row)
        avgs = data.get("averages", {})
        avg_cols = st.columns(6)
        stat_labels = {
            "pts": "PPG",
            "reb": "RPG",
            "ast": "APG",
            "blk": "BPG",
            "stl": "SPG",
            "tov": "TOV",
        }
        for i, (key, label) in enumerate(stat_labels.items()):
            avg_cols[i].metric(label, avgs.get(key, 0.0))

        # Game log table
        game_logs = data.get("games", [])
        if game_logs:
            df = pd.DataFrame(game_logs)
            display_cols = [
                c
                for c in [
                    "game_date", "matchup", "wl", "pts", "reb", "ast", "blk",
                    "stl", "tov", "fgm", "fga", "fg_pct", "fg3m", "fg3a",
                    "fg3_pct", "ftm", "fta", "ft_pct", "oreb", "dreb", "pf",
                    "plus_minus", "min",
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
