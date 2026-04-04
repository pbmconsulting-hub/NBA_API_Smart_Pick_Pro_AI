"""Team detail page."""
import streamlit as st
from pages._shared import nav, show_df, player_button
from api_service import (
    get_teams, get_team_roster, get_team_stats, get_team_details,
    get_team_clutch, get_team_hustle, get_team_estimated_metrics,
    get_defense_vs_position,
)


def render() -> None:
    tid = st.session_state.selected_team_id

    if st.button("← Back to Teams", key="back_teams"):
        nav("teams_browse")
        st.rerun()

    if not tid:
        st.warning("No team selected.")
    else:
        all_teams = get_teams()
        team_data = next((t for t in all_teams if t["team_id"] == tid), {})
        abbrev = team_data.get("abbreviation", "")

        st.title(f"🏟️ {abbrev} — {team_data.get('team_name', '')}")

        ov = st.columns(5)
        ov[0].metric("Conference", team_data.get("conference", "N/A"))
        ov[1].metric("Division", team_data.get("division", "N/A"))
        ov[2].metric("Pace", team_data.get("pace", "N/A"))
        ov[3].metric("ORtg", team_data.get("ortg", "N/A"))
        ov[4].metric("DRtg", team_data.get("drtg", "N/A"))

        st.divider()

        (t_tab_roster, t_tab_games, t_tab_details,
         t_tab_clutch, t_tab_hustle, t_tab_metrics, t_tab_dvp) = st.tabs([
            "👥 Roster (click players)",
            "📊 Recent Games",
            "🏢 Details",
            "🔥 Clutch",
            "💪 Hustle",
            "📈 Metrics",
            "🛡️ Def vs Pos",
        ])

        with t_tab_roster:
            roster = get_team_roster(tid)
            if roster:
                for p in roster:
                    player_button(
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
                show_df(team_games, [
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

        with t_tab_clutch:
            st.caption("Team performance in clutch time — last 5 min when score is within 5 points.")
            t_clutch = get_team_clutch(tid)
            if t_clutch:
                show_df(t_clutch, [
                    "season", "gp", "w", "l", "w_pct", "pts", "reb",
                    "ast", "stl", "blk", "tov", "fg_pct", "fg3_pct",
                    "ft_pct", "plus_minus",
                ])
            else:
                st.info("No clutch data.")

        with t_tab_hustle:
            st.caption("Effort metrics — deflections, contested shots, loose balls recovered, box-outs.")
            t_hustle = get_team_hustle(tid)
            if t_hustle:
                show_df(t_hustle, [
                    "season", "contested_shots", "deflections",
                    "charges_drawn", "screen_assists", "loose_balls",
                    "off_boxouts", "def_boxouts", "boxouts",
                ])
            else:
                st.info("No hustle data.")

        with t_tab_metrics:
            st.caption("NBA's estimated advanced metrics — ORtg, DRtg, Net Rtg, Pace derived from league-wide tracking data.")
            t_metrics = get_team_estimated_metrics(tid)
            if t_metrics:
                show_df(t_metrics, [
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
                        f"How **{abbrev}** defends each position. "
                        "Multiplier > 1.0 = weaker defense (allows more than avg). "
                        "< 1.0 = tougher defense (allows less than avg)."
                    )
                    show_df(dvp, [
                        "pos", "vs_pts_mult", "vs_reb_mult",
                        "vs_ast_mult", "vs_stl_mult", "vs_blk_mult",
                        "vs_3pm_mult",
                    ])
                else:
                    st.info("No defense-vs-position data.")
