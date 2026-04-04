"""Player profile page."""
import numpy as np
import pandas as pd
import streamlit as st

from pages._shared import nav, show_df
from api_service import (
    get_player_bio, get_player_last5, get_player_career,
    get_player_advanced, get_player_scoring, get_player_usage,
    get_player_shot_chart, get_player_tracking, get_player_clutch,
    get_player_hustle, get_player_matchups,
)


def render() -> None:
    pid = st.session_state.selected_player_id

    if st.button("← Back", key="back_from_player"):
        nav("home")
        st.rerun()

    if not pid:
        st.warning("No player selected.")
        return

    pid = int(pid)

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
        bio_cols[5].metric(
            "Experience",
            f"{bio.get('seasons')} yrs"
            if bio.get("seasons") is not None else "N/A",
        )
        bio_cols[6].metric("GP", bio.get("gp", "N/A"))
        bio_cols[7].metric("USG%", f"{(bio.get('usg_pct') or 0):.1%}")

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

    # F12: Analyze Prop shortcut
    if st.button(
        f"🎯 Analyze a Prop for {player_name}",
        key="profile_analyze_prop",
        use_container_width=True,
        type="primary",
    ):
        st.session_state["auto_prop_player_id"] = pid
        nav("prop_analyzer")
        st.rerun()

    st.divider()

    # ── Detailed tabs ─────────────────────────────────────
    (p_t_last5, p_t_career, p_t_adv, p_t_scoring, p_t_usage,
     p_t_shots, p_t_tracking, p_t_clutch, p_t_hustle,
     p_t_matchups, p_t_lineup) = st.tabs([
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
        "👥 Lineup Data",
    ])

    with p_t_last5:
        if last5 and last5.get("games"):
            show_df(last5["games"], [
                "game_date", "matchup", "wl", "pts", "reb", "ast",
                "blk", "stl", "tov", "fgm", "fga", "fg_pct",
                "fg3m", "fg3a", "fg3_pct", "ftm", "fta", "ft_pct",
                "oreb", "dreb", "pf", "plus_minus", "min",
            ])
        else:
            st.info("No recent game data.")

    with p_t_career:
        st.caption("Season-by-season totals across the player's entire NBA career.")
        career = get_player_career(pid)
        if career:
            show_df(career, [
                "season_id", "team_abbreviation", "player_age", "gp",
                "gs", "min", "pts", "reb", "ast", "stl", "blk",
                "tov", "fgm", "fga", "fg_pct", "fg3m", "fg3a",
                "fg3_pct", "ftm", "fta", "ft_pct", "oreb", "dreb",
                "pf",
            ])
        else:
            st.info("No career data.")

    with p_t_adv:
        with st.expander("ℹ️ What do advanced stats mean?", expanded=False):
            st.markdown("""
| Stat | Meaning |
|------|---------|
| **ORtg** | Offensive Rating — points produced per 100 possessions |
| **DRtg** | Defensive Rating — points allowed per 100 possessions (lower = better) |
| **Net Rtg** | ORtg − DRtg; positive = outscoring opponents while on court |
| **TS%** | True Shooting % — shooting efficiency including FT and 3PT |
| **eFG%** | Effective FG% — adjusts for 3PT being worth more |
| **USG%** | Usage Rate — % of team possessions used while on court |
| **AST%** | Assist % — % of teammate FGs assisted while on court |
| **TOV Ratio** | Turnovers per 100 possessions |
| **OREB% / DREB%** | Offensive / Defensive rebound % |
| **Pace** | Possessions per 48 minutes |
| **PIE** | Player Impact Estimate — overall contribution (higher = better) |
            """)
        adv = get_player_advanced(pid)
        if adv:
            show_df(adv, [
                "game_date", "matchup", "minutes", "off_rating",
                "def_rating", "net_rating", "ts_pct", "efg_pct",
                "usg_pct", "ast_pct", "oreb_pct", "dreb_pct",
                "reb_pct", "tov_ratio", "pace", "pie",
            ])
        else:
            st.info("No advanced data.")

    with p_t_scoring:
        st.caption("How this player's points are distributed — 2PT vs 3PT, paint vs midrange, assisted vs unassisted.")
        scoring = get_player_scoring(pid)
        if scoring:
            show_df(scoring, [
                "game_date", "matchup", "minutes",
                "pct_fga_2pt", "pct_fga_3pt", "pct_pts_2pt",
                "pct_pts_3pt", "pct_pts_fast_break", "pct_pts_ft",
                "pct_pts_paint", "pct_pts_off_tov",
                "pct_assisted_fgm", "pct_unassisted_fgm",
            ])
        else:
            st.info("No scoring data.")

    with p_t_usage:
        st.caption("Usage shows what % of the team's actions this player is responsible for while on the court.")
        usage = get_player_usage(pid)
        if usage:
            show_df(usage, [
                "game_date", "matchup", "minutes", "usg_pct",
                "pct_fgm", "pct_fga", "pct_fg3m", "pct_fg3a",
                "pct_ftm", "pct_fta", "pct_oreb", "pct_dreb",
                "pct_reb", "pct_ast", "pct_tov", "pct_stl",
                "pct_blk", "pct_pts",
            ])
        else:
            st.info("No usage data.")

    with p_t_shots:
        st.caption("Every field goal attempt plotted by court location, zone, and distance.")
        shots = get_player_shot_chart(pid)
        if shots:
            df_shots = pd.DataFrame(shots)

            # F11: Shot chart scatter plot
            if "loc_x" in df_shots.columns and "loc_y" in df_shots.columns:
                try:
                    import plotly.graph_objects as go

                    fig = go.Figure()
                    made = df_shots[df_shots.get("shot_made_flag", pd.Series(dtype=int)) == 1] if "shot_made_flag" in df_shots.columns else pd.DataFrame()
                    missed = df_shots[df_shots.get("shot_made_flag", pd.Series(dtype=int)) == 0] if "shot_made_flag" in df_shots.columns else pd.DataFrame()

                    if not made.empty:
                        fig.add_trace(go.Scatter(
                            x=made["loc_x"], y=made["loc_y"],
                            mode="markers", name="Made",
                            marker=dict(color="#00ff88", size=6, opacity=0.7),
                        ))
                    if not missed.empty:
                        fig.add_trace(go.Scatter(
                            x=missed["loc_x"], y=missed["loc_y"],
                            mode="markers", name="Missed",
                            marker=dict(color="#ff4444", size=6, opacity=0.5),
                        ))

                    # Half-court outline
                    fig.add_shape(type="line", x0=-250, y0=-47.5, x1=250, y1=-47.5, line=dict(color="white", width=1))
                    fig.add_shape(type="line", x0=-250, y0=-47.5, x1=-250, y1=422.5, line=dict(color="white", width=1))
                    fig.add_shape(type="line", x0=250, y0=-47.5, x1=250, y1=422.5, line=dict(color="white", width=1))
                    # 3-point arc
                    theta = np.linspace(0, np.pi, 100)
                    three_x = 237.5 * np.cos(theta)
                    three_y = 237.5 * np.sin(theta) + 5.25
                    fig.add_trace(go.Scatter(
                        x=three_x, y=three_y, mode="lines",
                        line=dict(color="white", width=1), showlegend=False,
                    ))
                    # Hoop
                    fig.add_shape(
                        type="circle", x0=-7.5, y0=-7.5, x1=7.5, y1=7.5,
                        line=dict(color="orange", width=2),
                    )

                    fig.update_layout(
                        width=600, height=600,
                        plot_bgcolor="#0e1117", paper_bgcolor="#0e1117",
                        font_color="white",
                        xaxis=dict(range=[-260, 260], showgrid=False, zeroline=False, visible=False),
                        yaxis=dict(range=[-60, 440], showgrid=False, zeroline=False, visible=False),
                        showlegend=True, legend=dict(x=0.8, y=1),
                        margin=dict(l=0, r=0, t=30, b=0),
                    )
                    st.plotly_chart(fig, use_container_width=True)
                except ImportError:
                    st.caption("Install plotly for the interactive shot chart: pip install plotly")

            # Zone summary
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
                    zone_summary["makes"] / zone_summary["attempts"].replace(0, 1)
                ).round(3)
                st.dataframe(
                    zone_summary.sort_values("attempts", ascending=False),
                    use_container_width=True,
                    hide_index=True,
                )
            st.caption(f"Showing {len(shots)} shot attempts")
            show_df(shots, [
                "game_date", "period", "event_type", "action_type",
                "shot_type", "shot_zone_basic", "shot_zone_area",
                "shot_distance", "shot_made_flag", "loc_x", "loc_y",
            ], height=400)
        else:
            st.info("No shot chart data.")

    with p_t_tracking:
        st.caption("Player-tracking data from NBA cameras — speed, distance, touches, and shot contest rates.")
        tracking = get_player_tracking(pid)
        if tracking:
            show_df(tracking, [
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
        st.caption("Performance in clutch time — the last 5 minutes when the score is within 5 points.")
        clutch = get_player_clutch(pid)
        if clutch:
            show_df(clutch, [
                "season", "team_abbreviation", "gp", "min", "pts",
                "reb", "ast", "stl", "blk", "tov", "fg_pct",
                "fg3_pct", "ft_pct", "plus_minus",
            ])
        else:
            st.info("No clutch data.")

    with p_t_hustle:
        st.caption("Effort plays that don't show up in traditional stats — deflections, loose balls, contested shots, screens.")
        hustle = get_player_hustle(pid)
        if hustle:
            show_df(hustle, [
                "season", "team_abbreviation", "gp", "min",
                "contested_shots", "contested_shots_2pt",
                "contested_shots_3pt", "deflections", "charges_drawn",
                "screen_assists", "screen_ast_pts", "loose_balls",
                "off_boxouts", "def_boxouts", "boxouts",
            ])
        else:
            st.info("No hustle data.")

    with p_t_matchups:
        st.caption("Head-to-head defensive matchup data — who guarded this player and how they performed.")
        matchups = get_player_matchups(pid)
        if matchups:
            show_df(matchups, [
                "game_date", "game_matchup", "defender_name",
                "matchup_min", "partial_poss", "player_pts",
                "matchup_fgm", "matchup_fga", "matchup_fg_pct",
                "matchup_fg3m", "matchup_fg3a", "matchup_fg3_pct",
                "matchup_ast", "matchup_tov", "matchup_blk",
                "switches_on",
            ])
        else:
            st.info("No matchup data.")

    with p_t_lineup:
        st.caption("Lineup analysis — how this player performs in different 5-man unit combinations.")
        try:
            from engine.lineup_analysis import (
                analyze_lineup_combination,
                detect_lineup_weaknesses,
            )
            from api_service import get_team_roster

            # Get player's team from bio
            _team_id = bio.get("team_id") if bio else None
            if _team_id:
                roster_data = get_team_roster(int(_team_id))
                if roster_data:
                    # Build a mini player dict from bio/career data (more
                    # reliable than volatile last-5 averages).
                    _bio_stats = bio if bio else {}
                    _last5_avgs = last5.get("averages", {}) if last5 else {}

                    def _stat(key):
                        """Return season/bio avg if available, else last-5."""
                        return float(_bio_stats.get(key, 0) or 0) or float(_last5_avgs.get(key, 0) or 0)

                    _player_dict = {
                        "player_id": pid,
                        "name": player_name,
                        "position": _bio_stats.get("position", "G"),
                        "pts": _stat("pts"),
                        "reb": _stat("reb"),
                        "ast": _stat("ast"),
                        "stl": _stat("stl"),
                        "blk": _stat("blk"),
                        "min": _stat("min"),
                    }

                    # Build teammate dicts from roster
                    teammates = []
                    for r in roster_data[:12]:  # Top 12 roster players
                        if r.get("player_id") != pid:
                            teammates.append({
                                "player_id": r.get("player_id", 0),
                                "name": r.get("player", ""),
                                "position": r.get("position", "G"),
                                "pts": float(r.get("pts", 0) or 0),
                                "reb": float(r.get("reb", 0) or 0),
                                "ast": float(r.get("ast", 0) or 0),
                                "stl": float(r.get("stl", 0) or 0),
                                "blk": float(r.get("blk", 0) or 0),
                                "min": float(r.get("min", 0) or 0),
                            })

                    if teammates:
                        lineup = [_player_dict] + teammates[:4]
                        analysis = analyze_lineup_combination(lineup)
                        if analysis:
                            lu_cols = st.columns([1, 1, 1])
                            lu_cols[0].metric(
                                "Est. Net Rating",
                                f"{analysis.get('estimated_net_rating', 0):+.1f}",
                            )
                            lu_cols[1].metric(
                                "Synergy Score",
                                f"{analysis.get('synergy_score', 0):.1f}",
                            )
                            lu_cols[2].metric(
                                "Lineup Players",
                                len(lineup),
                            )

                            # Show lineup members
                            st.markdown("**Lineup Combination**")
                            for lp in lineup:
                                st.text(
                                    f"  • {lp.get('name', '?')} ({lp.get('position', '?')}) "
                                    f"— {lp.get('pts', 0):.1f} PPG"
                                )

                        weaknesses = detect_lineup_weaknesses(lineup)
                        if weaknesses:
                            st.markdown("**Detected Weaknesses**")
                            for w in weaknesses:
                                st.warning(w)
                    else:
                        st.info("No teammate data available.")
                else:
                    st.info("Could not load team roster.")
            else:
                st.info("Team information not available for lineup analysis.")
        except ImportError:
            st.info("Lineup analysis module not available.")
        except Exception as _exc:
            st.info(f"Could not generate lineup analysis: {_exc}")
