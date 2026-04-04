"""Prop Analyzer page – AI-powered prop analysis with projection, simulation,
edge detection, and confidence scoring."""

from __future__ import annotations

import numpy as np
import pandas as pd
import streamlit as st

from pages._shared import (
    nav,
    show_df,
    TIER_COLORS,
    ANALYSIS_STAT_TYPES,
    MAX_SEARCH_RESULTS,
)
from styles.theme import get_tier_badge_html, get_verdict_banner_html
from api_service import analyze_prop, get_dfs_lines, save_pick, search_players
from tracking.bet_tracker import auto_log_analysis_bets


def render() -> None:
    """Interactive prop-analysis page powered by the engine modules."""

    st.title("🎯 Prop Analyzer")
    st.caption(
        "Enter a player prop to get an AI-powered analysis with projection, "
        "simulation, edge detection, and confidence scoring."
    )

    # ── Pre-fill from auto_prop_player_id (set by player_profile / home) ──
    auto_pid = st.session_state.pop("auto_prop_player_id", None)
    auto_stat = st.session_state.pop("auto_prop_stat", None)
    auto_line = st.session_state.pop("auto_prop_line", None)
    if auto_pid:
        try:
            from api_service import get_player_bio
            bio = get_player_bio(int(auto_pid))
            if bio:
                st.session_state["prop_player_search"] = bio.get("player_name", "")
        except Exception:
            pass

    # ── Sidebar inputs ──────────────────────────────────────────────
    with st.sidebar:
        st.subheader("🔍 Prop Setup")

        # Player search
        query = st.text_input("Search player", key="prop_player_search")
        selected_player: dict | None = None
        if query and len(query) >= 2:
            results = search_players(query)
            if results:
                options = {
                    f"{p.get('full_name', p.get('first_name', '') + ' ' + p.get('last_name', ''))} "
                    f"({p.get('team_abbreviation', '???')})": p
                    for p in results[:MAX_SEARCH_RESULTS]
                }
                choice = st.selectbox("Select player", list(options.keys()), key="prop_player_select")
                selected_player = options.get(choice)
            else:
                st.info("No players found.")

        stat_type = st.selectbox("Stat type", ANALYSIS_STAT_TYPES, key="prop_stat_type")

        # Auto-populate prop line from DFS platforms when player/stat selection changes
        _default_line = 20.5
        if selected_player:
            _pid = selected_player.get("player_id")
            _auto_key = f"{_pid}_{stat_type}"
            if _pid and st.session_state.get("_prop_auto_key") != _auto_key:
                _dfs = get_dfs_lines(int(_pid), stat_type)
                if _dfs.get("consensus"):
                    _default_line = round(float(_dfs["consensus"]) * 2) / 2  # nearest 0.5
                    st.session_state["_prop_auto_key"] = _auto_key
                    st.caption(f"Auto-filled from DFS platforms: {_default_line}")
            elif st.session_state.get("_prop_auto_key") == _auto_key:
                pass  # Keep the user's manual edit

        prop_line = st.number_input("Prop line", min_value=0.5, value=_default_line, step=0.5, key="prop_line_input")

        st.divider()
        st.subheader("⚙️ Game Context")
        opponent = st.text_input(
            "Opponent (abbrev, e.g. BOS)",
            key="prop_opponent",
            help="Leave blank to auto-detect from today's schedule.",
        ).strip().upper() or None
        vegas_spread = st.number_input("Vegas spread", value=0.0, step=0.5, key="prop_spread")
        game_total = st.number_input("Game total (O/U)", value=220.0, step=0.5, key="prop_total")
        platform = st.selectbox(
            "Platform",
            ["prizepicks", "underdog", "draftkings", "fanduel"],
            key="prop_platform",
        )

        run_analysis = st.button("🚀 Analyze Prop", type="primary", use_container_width=True)

    # ── Main content area ───────────────────────────────────────────
    # Show cached result when no new analysis was triggered
    if not run_analysis:
        cached = st.session_state.get("last_analysis")
        if cached and cached.get("confidence"):
            result = cached
            # Recover inputs from cached result for display
            player_id = result.get("player_id")
            player_name = result.get("player_name", "")
            stat_type = result.get("stat_type", stat_type)
            prop_line = result.get("prop_line", prop_line)
            platform = result.get("platform", platform)
        else:
            st.info("👈 Configure a prop in the sidebar and click **Analyze Prop** to begin.")
            return
    else:
        # New analysis requested
        if not selected_player:
            st.warning("Please search for and select a player first.")
            return

        player_id = selected_player.get("player_id")
        if not player_id:
            st.error("Selected player has no ID.")
            return

        player_name = (
            selected_player.get("full_name")
            or f"{selected_player.get('first_name', '')} {selected_player.get('last_name', '')}".strip()
        )

        with st.spinner(f"Analyzing {player_name} — {stat_type} {prop_line}…"):
            result = analyze_prop(
                player_id=int(player_id),
                stat_type=stat_type,
                prop_line=float(prop_line),
                opponent=opponent,
                vegas_spread=float(vegas_spread),
                game_total=float(game_total),
                platform=platform,
            )

        if result.get("status") == "error":
            st.error(f"Analysis failed: {result.get('message', 'Unknown error')}")
            return

        if "confidence" not in result:
            st.error("Unexpected response from the analysis engine.")
            return

        # Store successful result for session persistence
        result["player_id"] = int(player_id)
        result["player_name"] = player_name
        result["stat_type"] = stat_type
        result["prop_line"] = float(prop_line)
        result["platform"] = platform
        st.session_state["last_analysis"] = result

        # ── Auto-log to bet tracker ──────────────────────────────
        try:
            auto_log_analysis_bets(result, platform=platform)
        except Exception:
            pass  # Never block UI for tracking errors

    # ── Extract core data ───────────────────────────────────────────
    conf = result.get("confidence", {})
    tier = conf.get("tier", "Bronze")
    tier_emoji = conf.get("tier_emoji", "🥉")
    score = conf.get("confidence_score", 0)
    direction = result.get("direction", "OVER")
    model_prob = result.get("model_probability", 0.5)
    edge = result.get("edge_pct", 0.0)
    explanation = result.get("explanation", {})
    proj = result.get("projection", {})
    sim = result.get("simulation", {})
    forces = result.get("forces", {})
    bankroll = result.get("bankroll", {})
    regime = result.get("regime", {})
    kelly_frac = bankroll.get("kelly_fraction", 0.0)
    regime_changed = regime.get("regime_changed", False)
    regime_dir = regime.get("direction", "stable")
    team_abbrev = result.get("team", "???")
    opp_abbrev = result.get("opponent", "???")

    # ── Player Result Card ──────────────────────────────────────────
    st.markdown('<div class="player-result-card">', unsafe_allow_html=True)

    # Card header: player name, matchup info, tier badge
    dir_icon = "🟢" if direction == "OVER" else "🔴"
    st.markdown(
        f"""<div class="player-card-header">
            <div>
                <div class="player-card-name">{tier_emoji} {player_name}</div>
                <div class="player-card-meta">
                    {team_abbrev} vs {opp_abbrev}  ·  {stat_type.upper()} {direction} {prop_line}  ·  {platform.title()}
                </div>
            </div>
            <div class="player-card-tier">
                {get_tier_badge_html(tier)}
            </div>
        </div>""",
        unsafe_allow_html=True,
    )

    st.markdown('<div class="player-card-body">', unsafe_allow_html=True)

    # ── Joseph M Smith's Verdict ────────────────────────────────────
    verdict_text = explanation.get("verdict", "")
    tldr = explanation.get("tldr", "")
    if verdict_text:
        st.markdown(get_verdict_banner_html(verdict_text), unsafe_allow_html=True)
    elif tldr:
        st.markdown(get_verdict_banner_html(tldr), unsafe_allow_html=True)

    # ── Quick Metrics Row ───────────────────────────────────────────
    qm = st.columns([1, 1, 1, 1, 1])
    qm[0].metric("Confidence", f"{score:.0f}/100", delta=tier)
    qm[1].metric("Win Prob", f"{model_prob:.1%}")
    qm[2].metric("Edge", f"{edge:+.1f}%")
    qm[3].metric("Direction", f"{dir_icon} {direction}")
    kelly_pct = bankroll.get("recommended_pct", "0.00%")
    qm[4].metric("Kelly Size", kelly_pct)

    # ── Tabbed Sections ─────────────────────────────────────────────
    tab_info, tab_pred, tab_sim, tab_bet = st.tabs([
        "📋 Player Info & Stats",
        "🔮 Predictions & Analysis",
        "📊 Simulation Chart",
        "💰 Bet Sizing & Verdict",
    ])

    # ══════════════════════════════════════════════════════════════
    # TAB 1: Player Info & Stats
    # ══════════════════════════════════════════════════════════════
    with tab_info:
        # Matchup summary
        st.subheader(f"🏀 {player_name} — {team_abbrev} vs {opp_abbrev}")

        info_c1, info_c2 = st.columns(2)

        # F3: st.metric() grids for projection factors
        with info_c1:
            st.markdown("**Projection Factors**")
            proj_items = [
                (lbl, v) for lbl, v in [
                    ("Projected Points", proj.get("projected_points")),
                    ("Projected Rebounds", proj.get("projected_rebounds")),
                    ("Projected Assists", proj.get("projected_assists")),
                    ("Projected Threes", proj.get("projected_threes")),
                    ("Projected Steals", proj.get("projected_steals")),
                    ("Projected Blocks", proj.get("projected_blocks")),
                ] if v is not None
            ]
            for row_start in range(0, len(proj_items), 3):
                row = proj_items[row_start:row_start + 3]
                mc = st.columns(3)
                for j, (lbl, v) in enumerate(row):
                    mc[j].metric(lbl, f"{v:.1f}" if isinstance(v, float) else v)

        # F3: st.metric() grids for context adjustments
        with info_c2:
            st.markdown("**Context Adjustments**")
            ctx_items = [
                (lbl, v) for lbl, v in [
                    ("Pace Factor", proj.get("pace_factor")),
                    ("Defense Factor", proj.get("defense_factor")),
                    ("Home/Away Factor", proj.get("home_away_factor")),
                    ("Rest Factor", proj.get("rest_factor")),
                    ("Blowout Risk", proj.get("blowout_risk")),
                ] if v is not None
            ]
            for row_start in range(0, len(ctx_items), 3):
                row = ctx_items[row_start:row_start + 3]
                mc = st.columns(3)
                for j, (lbl, v) in enumerate(row):
                    mc[j].metric(lbl, f"{v:.1f}" if isinstance(v, float) else v)

        # Matchup History
        matchup = result.get("matchup_history", {})
        if matchup and "error" not in matchup:
            st.divider()
            st.markdown("**Matchup History**")
            if matchup.get("cold_start"):
                st.info(
                    f"Fewer than 5 games vs {opp_abbrev} — matchup adjustment is neutral."
                )
            else:
                mh_cols = st.columns([1, 1, 1, 1])
                avg_vs = matchup.get("avg_vs_team")
                mh_cols[0].metric(
                    "Avg vs Team",
                    f"{avg_vs:.1f}" if avg_vs is not None else "N/A",
                )
                mh_cols[1].metric("Games Found", matchup.get("games_found", 0))
                fav = matchup.get("matchup_favorability_score", 50)
                mh_cols[2].metric(
                    "Favorability",
                    f"{fav:.0f}/100",
                    delta="Favorable" if fav > 55 else ("Unfavorable" if fav < 45 else "Neutral"),
                    delta_color="normal" if fav > 55 else ("inverse" if fav < 45 else "off"),
                )
                adj = matchup.get("adjustment_factor", 1.0)
                mh_cols[3].metric(
                    "Projection Adj",
                    f"{adj:.2f}x",
                    delta=f"{(adj - 1) * 100:+.1f}%" if adj != 1.0 else "None",
                    delta_color="normal" if adj > 1.0 else ("inverse" if adj < 1.0 else "off"),
                )

        # Rotation / Minutes
        rotation_data = result.get("rotation", {})
        if rotation_data and "error" not in rotation_data:
            st.divider()
            st.markdown("**Minutes & Rotation**")
            rt_cols = st.columns([1, 1, 1])
            min_adj = rotation_data.get("minutes_adjustment", 1.0)
            rt_cols[0].metric(
                "Minutes Adj",
                f"{min_adj:.2f}x",
                delta=f"{(min_adj - 1) * 100:+.1f}%" if min_adj != 1.0 else "Stable",
                delta_color="normal" if min_adj > 1.0 else ("inverse" if min_adj < 1.0 else "off"),
            )
            role_changed = rotation_data.get("role_change_detected", False)
            if role_changed:
                change_type = rotation_data.get("change_type", "none").replace("_", " → ").title()
                rt_cols[1].metric("Role Change", f"⚠️ {change_type}")
                rt_cols[2].metric(
                    "Minutes Shift",
                    f"{rotation_data.get('change_magnitude', 0):+.1f} min",
                    delta=f"{rotation_data.get('minutes_before', 0):.0f} → "
                          f"{rotation_data.get('minutes_after', 0):.0f}",
                )
            else:
                rt_cols[1].metric("Role Change", "✅ None")
                rt_cols[2].metric("Status", "Stable rotation")

        # Player Efficiency
        eff = result.get("efficiency", {})
        if eff and "error" not in eff:
            st.divider()
            st.markdown("**Player Efficiency Profile**")
            eff_cols = st.columns([1, 1, 1, 1])
            eff_cols[0].metric("True Shooting", f"{eff.get('ts_pct', 0):.1%}")
            eff_cols[1].metric("eFG%", f"{eff.get('efg_pct', 0):.1%}")
            eff_cols[2].metric("Usage Rate", f"{eff.get('usage_rate', 0):.1f}%")
            eff_cols[3].metric("Efficiency Tier", eff.get("efficiency_tier", "N/A"))

            epm = eff.get("estimated_epm", {})
            raptor = eff.get("estimated_raptor", {})
            if epm or raptor:
                adv_cols = st.columns([1, 1, 1, 1])
                if epm:
                    adv_cols[0].metric("EPM Total", f"{epm.get('total', 0):+.1f}")
                    adv_cols[1].metric("EPM Percentile", f"{epm.get('percentile', 50):.0f}th")
                if raptor:
                    adv_cols[2].metric("RAPTOR Total", f"{raptor.get('raptor_total', 0):+.1f}")
                    adv_cols[3].metric("Est. WAR", f"{raptor.get('war', 0):.1f}")

    # ══════════════════════════════════════════════════════════════
    # TAB 2: Predictions & Analysis
    # ══════════════════════════════════════════════════════════════
    with tab_pred:
        # TL;DR
        if tldr:
            st.info(f"**TL;DR:** {tldr}")

        # Simulation
        st.subheader("🎲 Simulation Results")
        sim_cols = st.columns([1, 1, 1, 1])
        sim_cols[0].metric("Simulated Mean", f"{sim.get('simulated_mean', 0):.1f}")
        sim_cols[1].metric("P(Over)", f"{sim.get('probability_over', 0):.1%}")
        sim_cols[2].metric(
            "90% CI",
            f"{sim.get('ci_90_low', 0):.1f} – {sim.get('ci_90_high', 0):.1f}",
        )
        sim_cols[3].metric("Sims Run", sim.get("simulations_run", 0))

        sim_detail_cols = st.columns([1, 1, 1])
        sim_detail_cols[0].metric("10th %ile", f"{sim.get('percentile_10', 0):.1f}")
        sim_detail_cols[1].metric("50th %ile", f"{sim.get('percentile_50', 0):.1f}")
        sim_detail_cols[2].metric("90th %ile", f"{sim.get('percentile_90', 0):.1f}")

        st.divider()

        # Directional Forces
        st.subheader("⚡ Directional Forces")
        over_forces = forces.get("over_forces", [])
        under_forces = forces.get("under_forces", [])

        force_c1, force_c2 = st.columns(2)
        with force_c1:
            st.markdown("**🟢 OVER Forces**")
            if over_forces:
                for f in over_forces:
                    name = f.get("name", f.get("force_name", "Unknown"))
                    strength = f.get("strength", f.get("magnitude", 0))
                    st.text(f"  ↑ {name}: {strength:.2f}")
            else:
                st.caption("No OVER forces detected.")

        with force_c2:
            st.markdown("**🔴 UNDER Forces**")
            if under_forces:
                for f in under_forces:
                    name = f.get("name", f.get("force_name", "Unknown"))
                    strength = f.get("strength", f.get("magnitude", 0))
                    st.text(f"  ↓ {name}: {strength:.2f}")
            else:
                st.caption("No UNDER forces detected.")

        # Game Script
        game_script = result.get("game_script", {})
        if game_script and "error" not in game_script:
            st.divider()
            st.subheader("🎬 Game Script Simulation")
            gs_cols = st.columns([1, 1, 1, 1])
            gs_cols[0].metric(
                "Blended Mean",
                f"{game_script.get('blended_mean', 0):.1f}",
                delta=f"Script: {game_script.get('game_script_mean', 0):.1f}",
            )
            gs_cols[1].metric("Flat Mean", f"{game_script.get('flat_mean', 0):.1f}")
            gs_cols[2].metric("Blowout Rate", f"{game_script.get('blowout_game_rate', 0):.1%}")
            gs_cols[3].metric("Player Tier", game_script.get("player_tier", "rotation").title())
            st.caption(
                f"Blend: {game_script.get('blend_weight', 0.3):.0%} game-script "
                f"+ {1 - game_script.get('blend_weight', 0.3):.0%} flat simulation."
            )

        # Distribution Cross-Check
        dist_check = result.get("distribution_check", {})
        if dist_check and "error" not in dist_check:
            st.divider()
            st.subheader("📐 Distribution Cross-Check")
            dc_cols = st.columns([1, 1, 1])
            dc_cols[0].metric(
                "Analytical P(Over)",
                f"{dist_check.get('analytical_probability', 0):.1%}",
            )
            dc_cols[1].metric(
                "Monte Carlo P(Over)",
                f"{dist_check.get('monte_carlo_probability', 0):.1%}",
            )
            delta_val = dist_check.get("delta", 0)
            dc_cols[2].metric(
                "Delta",
                f"{delta_val:.1%}",
                delta="Agreement" if delta_val < 0.05 else "Divergence",
                delta_color="off" if delta_val < 0.05 else "inverse",
            )
            if delta_val >= 0.05:
                st.caption(
                    "⚠️ Analytical and Monte Carlo probabilities differ by ≥5%.  "
                    "This may indicate unusual stat distribution or high variance."
                )

        # Full Explanation
        with st.expander("📝 Full Explanation", expanded=False):
            for key in [
                "average_vs_line", "matchup_explanation", "pace_explanation",
                "home_away_explanation", "rest_explanation", "vegas_explanation",
                "projection_explanation", "simulation_narrative", "forces_summary",
                "recent_form_explanation", "verdict",
            ]:
                text = explanation.get(key)
                if text:
                    st.markdown(f"**{key.replace('_', ' ').title()}:** {text}")

        # ── Correlated Props (correlation.py) ────────────────────────
        st.divider()
        st.subheader("🔗 Correlated Props")
        st.caption("Props that tend to move together with this pick.")
        try:
            from engine.correlation import (
                get_teammate_correlation,
                get_within_player_cross_stat_correlation,
                calculate_game_environment_correlation,
            )
            _cross_stats = ["points", "rebounds", "assists", "threes", "steals", "blocks"]
            corr_rows = []
            for cs in _cross_stats:
                if cs == stat_type:
                    continue
                try:
                    within_corr = get_within_player_cross_stat_correlation(stat_type, cs)
                    env_corr = calculate_game_environment_correlation(float(game_total), cs)
                    if abs(within_corr) > 0.05 or abs(env_corr) > 0.05:
                        corr_rows.append({
                            "Linked Stat": cs.title(),
                            "Cross-Stat Corr": f"{within_corr:+.2f}",
                            "Game Env Corr": f"{env_corr:+.2f}",
                            "Signal": "🟢 Positive" if within_corr > 0 else "🔴 Negative",
                        })
                except Exception:
                    pass
            if corr_rows:
                import pandas as _pd
                st.dataframe(_pd.DataFrame(corr_rows), use_container_width=True)
            else:
                st.caption("No significant correlations detected.")
        except ImportError:
            st.caption("Correlation module not available.")
        except Exception:
            st.caption("Could not compute correlations.")

    # ══════════════════════════════════════════════════════════════
    # TAB 3: Simulation Chart
    # ══════════════════════════════════════════════════════════════
    with tab_sim:
        st.subheader("📊 Simulation Distribution")
        # Build a histogram from simulation percentiles / distribution data
        sim_distribution = sim.get("distribution", [])
        sim_mean = sim.get("simulated_mean", 0)
        if sim_distribution:
            sim_arr = np.array(sim_distribution, dtype=float)
            # Create histogram data
            hist_counts, bin_edges = np.histogram(sim_arr, bins=40)
            bin_centers = [(bin_edges[i] + bin_edges[i + 1]) / 2 for i in range(len(hist_counts))]
            hist_df = pd.DataFrame({"Stat Value": bin_centers, "Frequency": hist_counts})
            st.bar_chart(hist_df, x="Stat Value", y="Frequency", use_container_width=True)
            st.markdown(
                f"**Prop Line:** {prop_line} &nbsp;|&nbsp; "
                f"**Simulated Mean:** {sim_mean:.1f} &nbsp;|&nbsp; "
                f"**P(Over):** {sim.get('probability_over', 0):.1%}"
            )

            # F5: Prop line marker
            over_count = int(np.sum(sim_arr >= prop_line))
            under_count = len(sim_arr) - over_count
            total = len(sim_arr)
            if total > 0:
                st.progress(
                    over_count / total,
                    text=f"OVER: {over_count} ({over_count/total:.1%})  |  UNDER: {under_count} ({under_count/total:.1%})",
                )

        elif sim_mean > 0:
            # Fallback: generate approximate distribution from percentiles
            p10 = sim.get("percentile_10", sim_mean * 0.7)
            p50 = sim.get("percentile_50", sim_mean)
            p90 = sim.get("percentile_90", sim_mean * 1.3)
            # p10 to p90 spans ±1.28σ from mean in a normal distribution (total 2.56σ)
            std_est = max((p90 - p10) / 2.56, 1.0)
            # F4: Deterministic seeding for reproducible fallback chart
            sim_arr = np.random.default_rng(seed=int(sim_mean * 100)).normal(
                loc=sim_mean, scale=std_est, size=5000,
            )
            sim_arr = np.clip(sim_arr, 0, None)
            hist_counts, bin_edges = np.histogram(sim_arr, bins=40)
            bin_centers = [(bin_edges[i] + bin_edges[i + 1]) / 2 for i in range(len(hist_counts))]
            hist_df = pd.DataFrame({"Stat Value": bin_centers, "Frequency": hist_counts})
            st.bar_chart(hist_df, x="Stat Value", y="Frequency", use_container_width=True)
            st.caption("Distribution approximated from simulation percentiles.")
            st.markdown(
                f"**Prop Line:** {prop_line} &nbsp;|&nbsp; "
                f"**Simulated Mean:** {sim_mean:.1f} &nbsp;|&nbsp; "
                f"**P(Over):** {sim.get('probability_over', 0):.1%}"
            )

            # F5: Prop line marker
            over_count = int(np.sum(sim_arr >= prop_line))
            under_count = len(sim_arr) - over_count
            total = len(sim_arr)
            if total > 0:
                st.progress(
                    over_count / total,
                    text=f"OVER: {over_count} ({over_count/total:.1%})  |  UNDER: {under_count} ({under_count/total:.1%})",
                )

        else:
            st.info("No simulation data available for charting.")

        # Key metrics below chart
        st.divider()
        sim_cols = st.columns([1, 1, 1, 1])
        sim_cols[0].metric("Simulated Mean", f"{sim.get('simulated_mean', 0):.1f}")
        sim_cols[1].metric("P(Over)", f"{sim.get('probability_over', 0):.1%}")
        sim_cols[2].metric(
            "90% CI",
            f"{sim.get('ci_90_low', 0):.1f} – {sim.get('ci_90_high', 0):.1f}",
        )
        sim_cols[3].metric("Sims Run", sim.get("simulations_run", 0))

    # ══════════════════════════════════════════════════════════════
    # TAB 4: Bet Sizing & Verdict
    # ══════════════════════════════════════════════════════════════
    with tab_bet:
        # Joseph M Smith's verdict (prominent in this tab)
        if verdict_text:
            st.markdown(get_verdict_banner_html(verdict_text), unsafe_allow_html=True)

        st.subheader("💰 Bankroll & Sizing")
        bet_cols = st.columns([1, 1, 1, 1])
        bet_cols[0].metric(
            "Kelly Sizing",
            bankroll.get("recommended_pct", "0.00%"),
            delta=bankroll.get("kelly_mode", "quarter").title(),
        )
        regime_label = f"{'⚠️ ' if regime_changed else '✅ '}{regime_dir.title()}"
        bet_cols[1].metric(
            "Regime",
            regime_label,
            delta=f"Magnitude: {regime.get('magnitude', 0.0):.1f}" if regime_changed else "Stable",
            delta_color="off" if not regime_changed else ("normal" if regime_dir == "up" else "inverse"),
        )
        bet_cols[2].metric(
            "Payout",
            f"{bankroll.get('payout_multiplier', 1.909):.3f}x",
        )
        _bankroll = st.session_state.user_bankroll
        if kelly_frac > 0:
            example_bet = round(kelly_frac * _bankroll, 2)
            bet_cols[3].metric(f"${_bankroll:.0f} Bankroll →", f"${example_bet:.2f}")
        else:
            bet_cols[3].metric(f"${_bankroll:.0f} Bankroll →", "No bet")

        # Risk factors
        risk_factors = explanation.get("risk_factors", [])
        if risk_factors:
            st.divider()
            st.subheader("⚠️ Risk Factors")
            for rf in risk_factors:
                st.warning(rf)

        # ── Line Movement (market_movement.py) ─────────────────────
        st.divider()
        st.subheader("📉 Line Movement")
        try:
            from engine.market_movement import get_movement_summary
            _move = get_movement_summary(player_name, stat_type, platform=platform)
            if _move and _move.get("has_movement"):
                lm_cols = st.columns([1, 1, 1])
                _open = _move.get("opening_line", prop_line)
                _curr = _move.get("current_line", prop_line)
                _dir_lm = "⬆️ Up" if _curr > _open else ("⬇️ Down" if _curr < _open else "➡️ No Change")
                lm_cols[0].metric("Opening Line", f"{_open}")
                lm_cols[1].metric("Current Line", f"{_curr}", delta=f"{_curr - _open:+.1f}")
                lm_cols[2].metric("Direction", _dir_lm)
                if _move.get("sharp_money_flag"):
                    st.warning("🔥 **Sharp money detected** — significant line movement indicates professional action.")
                elif abs(_curr - _open) >= 0.5:
                    st.info(f"Line moved {abs(_curr - _open):.1f} points from open.")
            else:
                st.caption("No line movement data available for this player/stat.")
        except ImportError:
            st.caption("Line movement module not available.")
        except Exception:
            st.caption("No line movement data available.")

        avoid_reasons = conf.get("avoid_reasons", [])
        if avoid_reasons:
            st.error("**Avoid Reasons:** " + " • ".join(avoid_reasons))

        # Save Pick
        st.divider()

        # T2: Notes field in Save Pick flow
        save_notes = st.text_input(
            "Notes (optional)",
            key="save_pick_notes",
            placeholder="e.g. player questionable, back-to-back",
        )

        if st.button("💾 Save Pick", key="save_pick_btn", use_container_width=True):
            save_data = {
                "player_id": int(player_id),
                "player_name": player_name,
                "team": result.get("team", ""),
                "opponent": result.get("opponent", ""),
                "stat_type": stat_type,
                "prop_line": float(prop_line),
                "direction": direction,
                "model_probability": model_prob,
                "edge_pct": edge,
                "confidence_score": score,
                "tier": tier,
                "kelly_fraction": kelly_frac,
                "recommended_bet": round(kelly_frac * st.session_state.user_bankroll, 2),
                "regime_flag": regime_dir,
                "platform": platform,
                "vegas_spread": float(vegas_spread),
                "game_total": float(game_total),
                "notes": save_notes,
            }
            save_result = save_pick(save_data)
            if save_result.get("status") == "saved":
                st.success(f"✅ Pick saved (ID: {save_result.get('pick_id')})")
            else:
                st.error(f"Failed to save: {save_result.get('message', 'Unknown error')}")

    # Close the card divs
    st.markdown('</div></div>', unsafe_allow_html=True)
