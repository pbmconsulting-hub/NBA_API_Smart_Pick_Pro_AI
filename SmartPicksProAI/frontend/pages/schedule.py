"""Schedule page (F10: date filter + clickable rows)."""
import datetime
import streamlit as st
from pages._shared import nav, show_df, game_button
from api_service import get_schedule


def render() -> None:
    st.title("🗓️ NBA Schedule")

    schedule = get_schedule()
    if not schedule:
        st.info("No schedule data.")
        return

    # F10: Date range filter
    st.subheader("📅 Filter by Date")
    today = datetime.date.today()
    date_range = st.date_input(
        "Date range",
        value=(today - datetime.timedelta(days=7), today + datetime.timedelta(days=7)),
        key="schedule_date_range",
    )

    filtered = schedule
    if date_range and len(date_range) == 2:
        start_str = str(date_range[0])
        end_str = str(date_range[1])
        filtered = [
            g for g in schedule
            if g.get("game_date", "")[:10] >= start_str
            and g.get("game_date", "")[:10] <= end_str
        ]

    if not filtered:
        st.info("No games in the selected date range.")
        return

    st.caption(f"Showing {len(filtered)} games")

    # F10: Clickable game rows grouped by date
    current_date = ""
    for game in filtered:
        gd = game.get("game_date", "")[:10]
        if gd != current_date:
            current_date = gd
            st.markdown(f"### {current_date}")
        home = game.get("home_team_tricode", "?")
        away = game.get("away_team_tricode", "?")
        status = game.get("game_status_text", "")
        h_score = game.get("home_team_score")
        a_score = game.get("away_team_score")
        gid = game.get("game_id", "")

        if h_score is not None and a_score is not None and h_score > 0:
            label = f"🏀 {away} @ {home}  |  {a_score} – {h_score}  |  {status}"
        else:
            label = f"🏀 {away} @ {home}  |  {status}"

        if st.button(label, key=f"sched_g_{gid}", use_container_width=True):
            nav(
                "game_detail",
                selected_game_id=gid,
                game_context={
                    "matchup": f"{away} @ {home}",
                    "game_date": gd,
                    "game_id": gid,
                    "home_score": h_score,
                    "away_score": a_score,
                },
            )
            st.rerun()

    # Full dataframe below
    st.divider()
    st.subheader("📋 Full Schedule")
    show_df(filtered, [
        "game_date", "game_status_text", "home_team_tricode",
        "away_team_tricode", "home_team_score", "away_team_score",
        "arena_name", "arena_city", "game_id",
    ], height=600)
