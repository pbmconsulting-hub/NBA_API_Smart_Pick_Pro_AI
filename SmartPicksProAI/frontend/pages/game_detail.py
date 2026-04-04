"""Game detail page."""
import pandas as pd
import streamlit as st
from pages._shared import nav, show_df, player_button
from api_service import (
    get_teams, get_game_box_score, get_team_roster,
    get_play_by_play, get_win_probability, get_game_rotation,
)


def render() -> None:
    gid = st.session_state.selected_game_id
    ctx = st.session_state.game_context or {}

    if st.button("← Back to Home", key="back_home_gd"):
        nav("home")
        st.rerun()

    matchup = ctx.get("matchup", gid or "Game Detail")
    home_score = ctx.get("home_score")
    away_score = ctx.get("away_score")
    game_date = ctx.get("game_date", "")

    st.title(f"🏀 {matchup}")

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
        home_tid = ctx.get("home_team_id")
        away_tid = ctx.get("away_team_id")
        home_abbrev = ctx.get("home_abbrev", "")
        away_abbrev = ctx.get("away_abbrev", "")

        teams_data = get_teams()
        team_lookup = {t["team_id"]: t for t in teams_data} if teams_data else {}

        if home_tid and away_tid:
            home_team = team_lookup.get(home_tid, {})
            away_team = team_lookup.get(away_tid, {})

            if home_team or away_team:
                st.markdown('<div class="section-hdr">Team Comparison</div>',
                            unsafe_allow_html=True)
                st.caption(
                    "**Pace** = possessions per game (higher = faster).  "
                    "**ORtg** = points scored per 100 possessions (higher = better offense).  "
                    "**DRtg** = points allowed per 100 possessions (lower = better defense)."
                )
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
                teams_in_box = sorted(
                    set(p.get("team_abbreviation", "") for p in box)
                )
                for team_abbr in teams_in_box:
                    st.markdown(f"#### {team_abbr}")
                    team_players = [p for p in box
                                    if p.get("team_abbreviation") == team_abbr]
                    show_df(team_players, [
                        "full_name", "position", "pts", "reb", "ast",
                        "stl", "blk", "tov", "fgm", "fga", "fg_pct",
                        "fg3m", "fg3a", "fg3_pct", "ftm", "fta", "ft_pct",
                        "oreb", "dreb", "pf", "plus_minus", "min", "wl",
                    ])
                    for p in team_players:
                        player_button(
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
                        player_button(
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
                        player_button(
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
                show_df(pbp, [
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
                show_df(wp, [
                    "event_num", "home_pct", "visitor_pct",
                    "home_pts", "visitor_pts",
                    "home_score_margin", "period", "description",
                ], height=400)
            else:
                st.info("No win probability data.")

        with tab_rot:
            rot = get_game_rotation(gid)
            if rot:
                show_df(rot, [
                    "full_name", "team_abbrev", "in_time_real",
                    "out_time_real", "player_pts", "pt_diff", "usg_pct",
                ], height=500)
            else:
                st.info("No rotation data.")
