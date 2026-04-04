"""Teams browse page."""
import streamlit as st
from pages._shared import nav
from api_service import get_teams


def render() -> None:
    st.title("🏟️ Teams")

    all_teams = get_teams()
    if all_teams:
        st.caption("Select a team to view their roster, game stats, clutch/hustle metrics, synergy play types, and defense vs position.")
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
                    nav("team_detail", selected_team_id=t["team_id"])
                    st.rerun()

        with cw:
            st.markdown("### 🟠 Western Conference")
            for t in west_teams:
                if st.button(
                    f"🏟️ {t['abbreviation']} — {t['team_name']}",
                    key=f"tb_w_{t['team_id']}",
                    use_container_width=True,
                ):
                    nav("team_detail", selected_team_id=t["team_id"])
                    st.rerun()
    else:
        st.info("No teams loaded yet.")
