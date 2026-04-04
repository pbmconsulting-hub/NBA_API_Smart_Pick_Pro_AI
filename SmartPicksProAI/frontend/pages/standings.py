"""Standings page."""
import streamlit as st
from pages._shared import show_df
from api_service import get_standings


def render() -> None:
    st.title("🏆 League Standings")

    with st.expander("ℹ️ Understanding League Standings", expanded=False):
        st.markdown("""
**League Standings** show how every NBA team ranks in their conference.
The top 6 teams in each conference automatically qualify for the playoffs,
while seeds 7–10 compete in the **Play-In Tournament**.

| Column | Meaning |
|--------|---------|
| **Playoff Rank** | Team's seed in their conference (1 = best) |
| **W / L** | Wins and losses |
| **Win%** | Win percentage — wins ÷ total games played |
| **Home / Road** | Record at home vs. on the road (e.g. "25-5") |
| **L10** | Record over the last 10 games — shows recent form |
| **Streak** | Current winning or losing streak (e.g. "W3" = 3 straight wins) |
| **GB** | Games Back — how many games behind the conference leader |
| **PPG** | Points Per Game scored |
| **Opp PPG** | Opponent Points Per Game — points allowed |
| **Diff** | Point differential (PPG − Opp PPG); positive = outscoring opponents |
        """)

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
                show_df(east, standing_cols, height=550)

        with col_w:
            st.markdown("### 🟠 Western Conference")
            if west:
                show_df(west, standing_cols, height=550)

        st.divider()
        st.markdown("### 📋 Full Standings Detail")
        st.caption("Scroll right to see all columns including division records and vs-conference breakdowns.")
        show_df(standings_data, height=500)
    else:
        st.info("No standings data available. Run a data sync to populate.")
