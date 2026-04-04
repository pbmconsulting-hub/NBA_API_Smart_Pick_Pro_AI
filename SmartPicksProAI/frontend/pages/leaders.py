"""Leaders & Stats page."""
import streamlit as st
from pages._shared import show_df, player_button
from api_service import get_league_leaders, get_league_dash_players, get_league_dash_teams


def render() -> None:
    st.title("📊 League Leaders & Season Stats")

    with st.expander("ℹ️ Understanding Leaders & Stats", expanded=False):
        st.markdown("""
This section gives you three views into NBA performance:

**🏅 League Leaders** — The top players ranked by overall efficiency.
The "EFF" (Efficiency) rating is a simple formula:
`(PTS + REB + AST + STL + BLK) − (Missed FG + Missed FT + TOV)`.
Higher = better overall production.

**👤 Season Player Stats** — Per-game averages for every player this season.
Use this to compare any two players head-to-head across all box-score stats.

**🏟️ Season Team Stats** — Per-game averages for every team this season.
Great for spotting the best offensive teams (high PTS), defensive teams
(low Opp PTS), or efficient shooting teams (high FG%).

| Key Stat | Meaning |
|----------|---------|
| **GP** | Games played |
| **MIN** | Minutes per game |
| **FG%** | Field goal percentage — overall shooting accuracy |
| **FG3%** | Three-point percentage |
| **FT%** | Free throw percentage |
| **+/−** | Plus-minus — team's net score while this player is on court |
| **Fantasy PTS** | NBA Fantasy points (standard scoring) |
| **DD2 / TD3** | Double-doubles / Triple-doubles this season |
        """)

    l_tab_leaders, l_tab_players, l_tab_teams = st.tabs([
        "🏅 League Leaders",
        "👤 Season Player Stats",
        "🏟️ Season Team Stats",
    ])

    with l_tab_leaders:
        st.caption("Top players ranked by efficiency. Click any name to open their full profile.")
        leaders = get_league_leaders()
        if leaders:
            show_df(leaders, [
                "rank", "full_name", "position", "team_abbreviation",
                "gp", "min", "pts", "reb", "ast", "stl", "blk",
                "tov", "fg_pct", "fg3_pct", "ft_pct", "eff",
            ], height=600)
            st.markdown('<div class="section-hdr">Click a player</div>',
                        unsafe_allow_html=True)
            for ldr in leaders[:25]:
                player_button(
                    ldr.get("player_id"),
                    ldr.get("full_name", ""),
                    ldr.get("position"),
                    ldr.get("team_abbreviation"),
                    key_prefix="ldr",
                )
        else:
            st.info("No league leaders data.")

    with l_tab_players:
        st.caption(
            "Season per-game averages for every player. "
            "Sort by any column header to find leaders in a specific stat."
        )
        dash_players = get_league_dash_players()
        if dash_players:
            show_df(dash_players, [
                "full_name", "position", "team_abbreviation", "season",
                "gp", "w", "l", "min", "pts", "reb", "ast", "stl",
                "blk", "tov", "fg_pct", "fg3_pct", "ft_pct",
                "plus_minus", "nba_fantasy_pts", "dd2", "td3",
            ], height=600)
        else:
            st.info("No season player stats.")

    with l_tab_teams:
        st.caption(
            "Season per-game averages for every team. "
            "Compare offensive and defensive performance across the league."
        )
        dash_teams = get_league_dash_teams()
        if dash_teams:
            show_df(dash_teams, [
                "abbreviation", "team_name", "season", "gp", "w", "l",
                "w_pct", "pts", "reb", "ast", "stl", "blk", "tov",
                "fg_pct", "fg3_pct", "ft_pct", "plus_minus",
            ], height=600)
        else:
            st.info("No season team stats.")
