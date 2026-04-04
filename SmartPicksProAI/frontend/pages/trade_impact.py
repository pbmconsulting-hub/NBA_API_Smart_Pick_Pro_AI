"""Trade Impact page — evaluate how trades affect player projections."""
import streamlit as st
from pages._shared import nav, show_df
from api_service import search_players, get_teams


def render() -> None:
    st.title("🔀 Trade Impact")
    st.caption(
        "Evaluate how a player trade affects usage, stat projections, and role.  "
        "Powered by engine/trade_evaluator.py."
    )

    try:
        from engine.trade_evaluator import evaluate_trade, score_roster_fit
        _trade_eval_available = True
    except ImportError:
        _trade_eval_available = False

    if not _trade_eval_available:
        st.error("Trade evaluator module not available. Ensure engine/trade_evaluator.py is on the path.")
        return

    # ── Player search ────────────────────────────────────────────
    col_search, col_team = st.columns(2)

    with col_search:
        player_query = st.text_input(
            "🔍 Search traded player", key="trade_player_search",
            placeholder="e.g. Bradley Beal",
        )
        selected_player = None
        if player_query and len(player_query) >= 2:
            results = search_players(player_query)
            if results:
                options = {
                    f"{p.get('full_name', '')} ({p.get('team_abbreviation', '???')})": p
                    for p in results[:8]
                }
                choice = st.selectbox("Select player", list(options.keys()), key="trade_select")
                selected_player = options.get(choice)
            else:
                st.info("No players found.")

    with col_team:
        teams = get_teams()
        team_options = {}
        if teams:
            team_options = {
                f"{t.get('full_name', t.get('abbreviation', '?'))} ({t.get('abbreviation', '?')})": t
                for t in teams
            }
        new_team_choice = st.selectbox(
            "🏀 New team", list(team_options.keys()) if team_options else ["No teams loaded"],
            key="trade_new_team",
        )
        new_team = team_options.get(new_team_choice) if team_options else None

    if st.button("📊 Evaluate Trade Impact", type="primary", use_container_width=True):
        if not selected_player:
            st.warning("Please select a traded player.")
            return
        if not new_team:
            st.warning("Please select a new team.")
            return

        player_name = selected_player.get(
            "full_name",
            f"{selected_player.get('first_name', '')} {selected_player.get('last_name', '')}".strip(),
        )
        old_team = selected_player.get("team_abbreviation", "???")
        new_team_abbrev = new_team.get("abbreviation", "???")

        if old_team == new_team_abbrev:
            st.info("Player is already on this team — no trade to evaluate.")
            return

        with st.spinner(f"Evaluating {player_name} → {new_team_abbrev}…"):
            # Build a minimal player_data dict for the evaluator
            player_data = {
                "player_id": selected_player.get("player_id"),
                "name": player_name,
                "team": old_team,
                "position": selected_player.get("position", "G"),
                "pts": float(selected_player.get("pts", 0) or 0),
                "reb": float(selected_player.get("reb", 0) or 0),
                "ast": float(selected_player.get("ast", 0) or 0),
                "stl": float(selected_player.get("stl", 0) or 0),
                "blk": float(selected_player.get("blk", 0) or 0),
                "tov": float(selected_player.get("tov", 0) or 0),
                "min": float(selected_player.get("min", 0) or 0),
                "gp": int(selected_player.get("gp", 0) or 0),
                "fga": float(selected_player.get("fga", 0) or 0),
                "fgm": float(selected_player.get("fgm", 0) or 0),
                "fta": float(selected_player.get("fta", 0) or 0),
                "ftm": float(selected_player.get("ftm", 0) or 0),
                "fg3m": float(selected_player.get("fg3m", 0) or 0),
                "oreb": float(selected_player.get("oreb", 0) or 0),
                "dreb": float(selected_player.get("dreb", 0) or 0),
            }

            try:
                fit_result = score_roster_fit(player_data, [])  # Roster not available
            except Exception:
                fit_result = {}

        st.divider()
        st.subheader(f"📋 {player_name}: {old_team} → {new_team_abbrev}")

        # Display trade impact metrics
        tc = st.columns([1, 1, 1, 1])
        tc[0].metric("Old Team", old_team)
        tc[1].metric("New Team", new_team_abbrev)
        tc[2].metric("Position", player_data.get("position", "?"))
        tc[3].metric("PPG (Old Role)", f"{player_data.get('pts', 0):.1f}")

        if fit_result and "error" not in fit_result:
            st.divider()
            st.markdown("**Roster Fit Analysis**")
            fit_cols = st.columns([1, 1, 1])
            fit_cols[0].metric(
                "Fit Score",
                f"{fit_result.get('fit_score', 50):.0f}/100",
            )
            fit_cols[1].metric(
                "Role Grade",
                fit_result.get("role_grade", "N/A"),
            )
            fit_cols[2].metric(
                "Usage Projection",
                f"{fit_result.get('projected_usage', 0):.1f}%",
            )

            notes = fit_result.get("notes", [])
            if notes:
                st.markdown("**Notes**")
                for n in notes:
                    st.info(n)
        else:
            st.info(
                "Trade impact evaluation uses real roster data when available.  "
                "With limited data, showing basic role comparison."
            )

            # Basic stat projection display
            st.markdown("**Current Stats (Old Role)**")
            stat_c = st.columns(6)
            for i, (lbl, key) in enumerate([
                ("PTS", "pts"), ("REB", "reb"), ("AST", "ast"),
                ("STL", "stl"), ("BLK", "blk"), ("TOV", "tov"),
            ]):
                stat_c[i].metric(lbl, f"{player_data.get(key, 0):.1f}")
