"""Prop Analyzer page – AI-powered prop analysis with projection, simulation,
edge detection, and confidence scoring.

Consolidated hub: Single Prop, Bulk Analysis, and Auto Slate all in one page.
"""

from __future__ import annotations

import io

import numpy as np
import pandas as pd
import streamlit as st

from pages._shared import (
    ANALYSIS_STAT_TYPES,
    DEFAULT_BANKROLL,
    MAX_SEARCH_RESULTS,
    TIER_EMOJI,
)
from styles.theme import get_tier_badge_html, get_verdict_banner_html
from api_service import (
    analyze_prop,
    get_dfs_lines,
    get_todays_slate,
    save_pick,
    search_players,
)
from tracking.bet_tracker import auto_log_analysis_bets


# ── Tier filter helpers ────────────────────────────────────────────────────
_TIER_RANK = {"Platinum": 4, "Gold": 3, "Silver": 2, "Bronze": 1, "Avoid": 0}

_MIN_TIER_MAP: dict[str, int] = {
    "All": 0,
    "Silver+": 2,
    "Gold+": 3,
    "Platinum only": 4,
}


def _passes_tier_filter(tier: str, min_tier_label: str) -> bool:
    """Return True if *tier* meets the minimum tier threshold."""
    return _TIER_RANK.get(tier, 0) >= _MIN_TIER_MAP.get(min_tier_label, 0)


# ── Reusable analysis-tab renderer ────────────────────────────────────────


def _render_analysis_tabs(
    result: dict,
    player_name: str,
    stat_type: str,
    prop_line: float,
    platform: str,
    vegas_spread: float = 0.0,
    game_total: float = 220.0,
    key_suffix: str = "",
) -> None:
    """Render the four analysis tabs for a single prop result.

    Used by Single Prop, Bulk Analysis, and Auto Slate modes so the output
    format is identical everywhere.

    Parameters
    ----------
    result : dict
        Full analysis result from ``analyze_prop()`` (or compatible dict).
    player_name, stat_type, prop_line, platform : str / float
        Prop metadata (for display & save-pick flow).
    vegas_spread, game_total : float
        Vegas context values (for save-pick flow).
    key_suffix : str
        Unique suffix appended to every Streamlit widget key so this
        function can be called multiple times on the same page.
    """

    # ── Extract core data ──────────────────────────────────────────
    conf = result.get("confidence", {})
    tier = conf.get("tier", result.get("tier", "Bronze"))
    tier_emoji = conf.get("tier_emoji", TIER_EMOJI.get(tier, "🥉"))
    score = conf.get("confidence_score", result.get("confidence_score", 0))
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
    player_id = result.get("player_id")

    # ── Player Result Card ─────────────────────────────────────────
    st.markdown('<div class="player-result-card">', unsafe_allow_html=True)

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

    # ── Joseph M Smith's Verdict ───────────────────────────────────
    verdict_text = explanation.get("verdict", "")
    tldr = explanation.get("tldr", "")
    if verdict_text:
        st.markdown(get_verdict_banner_html(verdict_text), unsafe_allow_html=True)
    elif tldr:
        st.markdown(get_verdict_banner_html(tldr), unsafe_allow_html=True)

    # ── Quick Metrics Row ──────────────────────────────────────────
    qm = st.columns([1, 1, 1, 1, 1])
    qm[0].metric("Confidence", f"{score:.0f}/100", delta=tier)
    qm[1].metric("Win Prob", f"{model_prob:.1%}")
    qm[2].metric("Edge", f"{edge:+.1f}%")
    qm[3].metric("Direction", f"{dir_icon} {direction}")
    kelly_pct = bankroll.get("recommended_pct", "0.00%")
    qm[4].metric("Kelly Size", kelly_pct)

    # ── Tabbed Sections ────────────────────────────────────────────
    tab_info, tab_pred, tab_sim, tab_bet = st.tabs([
        "📋 Player Info & Stats",
        "🔮 Predictions & Analysis",
        "📊 Simulation Chart",
        "💰 Bet Sizing & Verdict",
    ])

    # ════════════════════════════════════════════════════════════
    # TAB 1: Player Info & Stats
    # ════════════════════════════════════════════════════════════
    with tab_info:
        st.subheader(f"🏀 {player_name} — {team_abbrev} vs {opp_abbrev}")

        info_c1, info_c2 = st.columns(2)

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
                    adv_cols[0].metric("EPM Total", f"{epm.get('total_epm', epm.get('total', 0)):+.1f}")
                    adv_cols[1].metric("EPM Percentile", f"{epm.get('percentile', 50):.0f}th")
                if raptor:
                    adv_cols[2].metric("RAPTOR Total", f"{raptor.get('raptor_total', 0):+.1f}")
                    adv_cols[3].metric("Est. WAR", f"{raptor.get('war', 0):.1f}")

    # ════════════════════════════════════════════════════════════
    # TAB 2: Predictions & Analysis
    # ════════════════════════════════════════════════════════════
    with tab_pred:
        if tldr:
            st.info(f"**TL;DR:** {tldr}")

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

        # ── Correlated Props (correlation.py) ──────────────────────
        st.divider()
        st.subheader("🔗 Correlated Props")
        st.caption("Props that tend to move together with this pick.")
        try:
            from engine.correlation import (
                get_within_player_cross_stat_correlation,
                calculate_game_environment_correlation,
            )
            _cross_stats = ["points", "rebounds", "assists", "threes", "steals", "blocks"]
            corr_rows: list[dict] = []
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
                st.dataframe(pd.DataFrame(corr_rows), use_container_width=True)
            else:
                st.caption("No significant correlations detected.")
        except ImportError:
            st.caption("Correlation module not available.")
        except Exception:
            st.caption("Could not compute correlations.")

    # ════════════════════════════════════════════════════════════
    # TAB 3: Simulation Chart
    # ════════════════════════════════════════════════════════════
    with tab_sim:
        st.subheader("📊 Simulation Distribution")
        sim_distribution = sim.get("distribution", [])
        sim_mean = sim.get("simulated_mean", 0)
        if sim_distribution:
            sim_arr = np.array(sim_distribution, dtype=float)
            hist_counts, bin_edges = np.histogram(sim_arr, bins=40)
            bin_centers = [(bin_edges[i] + bin_edges[i + 1]) / 2 for i in range(len(hist_counts))]
            hist_df = pd.DataFrame({"Stat Value": bin_centers, "Frequency": hist_counts})
            st.bar_chart(hist_df, x="Stat Value", y="Frequency", use_container_width=True)
            st.markdown(
                f"**Prop Line:** {prop_line} &nbsp;|&nbsp; "
                f"**Simulated Mean:** {sim_mean:.1f} &nbsp;|&nbsp; "
                f"**P(Over):** {sim.get('probability_over', 0):.1%}"
            )

            over_count = int(np.sum(sim_arr >= prop_line))
            under_count = len(sim_arr) - over_count
            total = len(sim_arr)
            if total > 0:
                st.progress(
                    over_count / total,
                    text=f"OVER: {over_count} ({over_count/total:.1%})  |  UNDER: {under_count} ({under_count/total:.1%})",
                )

        elif sim_mean > 0:
            p10 = sim.get("percentile_10", sim_mean * 0.7)
            p90 = sim.get("percentile_90", sim_mean * 1.3)
            std_est = max((p90 - p10) / 2.56, 1.0)
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

        st.divider()
        sim_cols2 = st.columns([1, 1, 1, 1])
        sim_cols2[0].metric("Simulated Mean", f"{sim.get('simulated_mean', 0):.1f}")
        sim_cols2[1].metric("P(Over)", f"{sim.get('probability_over', 0):.1%}")
        sim_cols2[2].metric(
            "90% CI",
            f"{sim.get('ci_90_low', 0):.1f} – {sim.get('ci_90_high', 0):.1f}",
        )
        sim_cols2[3].metric("Sims Run", sim.get("simulations_run", 0))

    # ════════════════════════════════════════════════════════════
    # TAB 4: Bet Sizing & Verdict
    # ════════════════════════════════════════════════════════════
    with tab_bet:
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
        _bankroll = st.session_state.get("user_bankroll", DEFAULT_BANKROLL)
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
        save_notes = st.text_input(
            "Notes (optional)",
            key=f"save_pick_notes{key_suffix}",
            placeholder="e.g. player questionable, back-to-back",
        )

        if st.button("💾 Save Pick", key=f"save_pick_btn{key_suffix}", use_container_width=True):
            save_data = {
                "player_id": int(player_id) if player_id else 0,
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
                "recommended_bet": round(
                    kelly_frac * st.session_state.get("user_bankroll", DEFAULT_BANKROLL), 2
                ),
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


# ── Leaderboard builder (shared by Bulk & Slate) ──────────────────────────


def _build_leaderboard_df(results: list[dict]) -> pd.DataFrame:
    """Build a summary leaderboard ``DataFrame`` sorted by confidence."""
    rows = []
    for r in results:
        conf = r.get("confidence", {})
        tier = conf.get("tier", r.get("tier", "Bronze"))
        rows.append({
            "Player": r.get("player_name", ""),
            "Stat": r.get("stat_type", ""),
            "Line": r.get("prop_line", 0),
            "Direction": r.get("direction", ""),
            "Tier": f"{TIER_EMOJI.get(tier, '🥉')} {tier}",
            "Confidence": round(
                conf.get("confidence_score", r.get("confidence_score", 0)), 1
            ),
            "Edge %": round(r.get("edge_pct", 0), 1),
            "Projection": round(
                r.get("projection", {}).get(
                    f"projected_{r.get('stat_type', 'points')}",
                    r.get("projected_value", 0),
                ) or 0,
                1,
            ),
            "Win Prob": round(r.get("model_probability", 0.5) * 100, 1),
            "Kelly Size": r.get("bankroll", {}).get("recommended_pct", "—"),
        })
    df = pd.DataFrame(rows)
    if not df.empty:
        df = df.sort_values("Confidence", ascending=False).reset_index(drop=True)
    return df


def _render_result_cards(
    results: list[dict],
    mode_prefix: str,
    min_tier_label: str = "Silver+",
) -> None:
    """Render expandable full-result cards for qualifying picks."""
    for idx, r in enumerate(results):
        conf = r.get("confidence", {})
        tier = conf.get("tier", r.get("tier", "Bronze"))
        if not _passes_tier_filter(tier, min_tier_label):
            continue
        tier_emoji = TIER_EMOJI.get(tier, "🥉")
        pname = r.get("player_name", "Unknown")
        stype = r.get("stat_type", "")
        direction = r.get("direction", "")
        pline = r.get("prop_line", 0)
        label = f"{tier_emoji} {pname} — {stype.upper()} {direction} {pline}"
        with st.expander(label, expanded=False):
            _render_analysis_tabs(
                result=r,
                player_name=pname,
                stat_type=stype,
                prop_line=float(pline),
                platform=r.get("platform", "prizepicks"),
                vegas_spread=float(r.get("vegas_spread", 0.0)),
                game_total=float(r.get("game_total", 220.0)),
                key_suffix=f"_{mode_prefix}_{idx}",
            )


# ═══════════════════════════════════════════════════════════════════════════
# Main render
# ═══════════════════════════════════════════════════════════════════════════


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
    if auto_stat:
        st.session_state["prop_stat_type"] = auto_stat
    if auto_line:
        st.session_state["prop_line_input"] = float(auto_line)

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

    # ── Sidebar: Auto Slate options ──────────────────────────────────
    with st.sidebar:
        st.divider()
        st.subheader("🤖 Auto Slate Options")
        slate_top_n = st.slider(
            "Number of picks", min_value=5, max_value=30, value=10,
            key="slate_top_n",
        )
        slate_platforms = st.multiselect(
            "Platforms",
            ["prizepicks", "underdog", "draftkings", "fanduel"],
            default=["prizepicks", "underdog", "draftkings", "fanduel"],
            key="slate_platforms",
        )
        slate_stat_types = st.multiselect(
            "Stat types",
            ANALYSIS_STAT_TYPES,
            default=ANALYSIS_STAT_TYPES,
            key="slate_stat_types",
        )
        slate_min_tier = st.selectbox(
            "Min tier filter",
            ["All", "Silver+", "Gold+", "Platinum only"],
            index=1,
            key="slate_min_tier",
        )
        generate_slate = st.button(
            "🤖 Generate Slate", type="primary", use_container_width=True,
        )

    # ══════════════════════════════════════════════════════════════
    # Top-level mode tabs
    # ══════════════════════════════════════════════════════════════
    mode_single, mode_bulk, mode_slate = st.tabs([
        "🎯 Single Prop",
        "📊 Bulk Analysis",
        "🤖 Auto Slate",
    ])

    # ══════════════════════════════════════════════════════════════
    # MODE 1: 🎯 Single Prop  (original logic — untouched)
    # ══════════════════════════════════════════════════════════════
    with mode_single:
        if not run_analysis:
            cached = st.session_state.get("last_analysis")
            if cached and cached.get("confidence"):
                result = cached
                player_id = result.get("player_id")
                player_name = result.get("player_name", "")
                stat_type = result.get("stat_type", stat_type)
                prop_line = result.get("prop_line", prop_line)
                platform = result.get("platform", platform)
            else:
                st.info("👈 Configure a prop in the sidebar and click **Analyze Prop** to begin.")
                result = None
        else:
            if not selected_player:
                st.warning("Please search for and select a player first.")
                result = None
            else:
                player_id = selected_player.get("player_id")
                if not player_id:
                    st.error("Selected player has no ID.")
                    result = None
                else:
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
                        result = None
                    elif "confidence" not in result:
                        st.error("Unexpected response from the analysis engine.")
                        result = None
                    else:
                        result["player_id"] = int(player_id)
                        result["player_name"] = player_name
                        result["stat_type"] = stat_type
                        result["prop_line"] = float(prop_line)
                        result["platform"] = platform
                        st.session_state["last_analysis"] = result

                        try:
                            auto_log_analysis_bets(result, platform=platform)
                        except Exception:
                            pass

        if result is not None:
            _render_analysis_tabs(
                result=result,
                player_name=result.get("player_name", ""),
                stat_type=result.get("stat_type", stat_type),
                prop_line=float(result.get("prop_line", prop_line)),
                platform=result.get("platform", platform),
                vegas_spread=float(vegas_spread),
                game_total=float(game_total),
                key_suffix="_single",
            )

    # ══════════════════════════════════════════════════════════════
    # MODE 2: 📊 Bulk Analysis
    # ══════════════════════════════════════════════════════════════
    with mode_bulk:
        st.subheader("📊 Bulk Analysis")
        st.caption(
            "Paste props below (one per line) in the format: "
            "**Player Name, Stat Type, Line, Over/Under, Platform**"
        )

        # Pre-populate from imported props if available
        default_text = ""
        imported = st.session_state.get("imported_props", [])
        if imported:
            lines = []
            for ip in imported:
                lines.append(
                    f"{ip.get('player_name', '')}, "
                    f"{ip.get('stat_type', 'points')}, "
                    f"{ip.get('prop_line', 20.5)}, "
                    f"{ip.get('direction', 'Over')}, "
                    f"{ip.get('platform', 'prizepicks')}"
                )
            default_text = "\n".join(lines)

        bulk_text = st.text_area(
            "Props (one per line)",
            value=default_text,
            height=180,
            key="bulk_props_text",
            placeholder=(
                "LeBron James, points, 25.5, Over, prizepicks\n"
                "Stephen Curry, threes, 4.5, Over, draftkings\n"
                "Nikola Jokic, rebounds, 12.5, Under, prizepicks"
            ),
        )

        run_bulk = st.button(
            "🚀 Run Bulk Analysis", type="primary", key="run_bulk_btn",
        )

        if run_bulk and bulk_text.strip():
            parsed_props: list[dict] = []
            for raw_line in bulk_text.strip().splitlines():
                raw_line = raw_line.strip()
                if not raw_line:
                    continue
                parts = [p.strip() for p in raw_line.split(",")]
                if len(parts) < 3:
                    st.warning(f"Skipping malformed line: {raw_line}")
                    continue
                parsed_props.append({
                    "player_name": parts[0],
                    "stat_type": parts[1].lower() if len(parts) > 1 else "points",
                    "prop_line": float(parts[2]) if len(parts) > 2 else 20.5,
                    "direction": parts[3].upper() if len(parts) > 3 else "OVER",
                    "platform": parts[4].lower() if len(parts) > 4 else "prizepicks",
                })

            if not parsed_props:
                st.warning("No valid props found. Check the format.")
            else:
                bulk_results: list[dict] = []
                progress = st.progress(0, text="Starting bulk analysis…")
                total = len(parsed_props)

                for i, prop in enumerate(parsed_props):
                    pname = prop["player_name"]
                    stype = prop["stat_type"]
                    pline = prop["prop_line"]
                    plat = prop["platform"]
                    progress.progress(
                        (i) / total,
                        text=f"Analyzing {pname} — {stype} {pline}… ({i + 1}/{total})",
                    )
                    try:
                        players = search_players(pname)
                        if not players:
                            st.warning(f"⚠️ Player not found: {pname}")
                            continue
                        pid = players[0].get("player_id")
                        if not pid:
                            st.warning(f"⚠️ No player ID for: {pname}")
                            continue
                        resolved_name = (
                            players[0].get("full_name")
                            or f"{players[0].get('first_name', '')} {players[0].get('last_name', '')}".strip()
                        )
                        res = analyze_prop(
                            player_id=int(pid),
                            stat_type=stype,
                            prop_line=float(pline),
                            platform=plat,
                        )
                        if res.get("status") == "error" or "confidence" not in res:
                            st.warning(f"⚠️ Analysis failed for {pname}: {res.get('message', 'unknown')}")
                            continue
                        res["player_id"] = int(pid)
                        res["player_name"] = resolved_name
                        res["stat_type"] = stype
                        res["prop_line"] = float(pline)
                        res["platform"] = plat
                        res["direction"] = res.get("direction", prop.get("direction", "OVER"))
                        bulk_results.append(res)
                    except Exception as exc:
                        st.warning(f"⚠️ Error analyzing {pname}: {exc}")

                progress.progress(1.0, text="Bulk analysis complete!")
                st.session_state["bulk_results"] = bulk_results

        # ── Display stored bulk results ────────────────────────────
        bulk_results = st.session_state.get("bulk_results", [])
        if bulk_results:
            st.divider()

            # View 1: Summary Leaderboard
            st.subheader("🏆 Leaderboard")
            lb_df = _build_leaderboard_df(bulk_results)
            st.dataframe(lb_df, use_container_width=True, hide_index=True)

            # CSV export
            csv_buf = io.StringIO()
            lb_df.to_csv(csv_buf, index=False)
            st.download_button(
                "📥 Download Results CSV",
                data=csv_buf.getvalue(),
                file_name="bulk_analysis_results.csv",
                mime="text/csv",
                key="bulk_csv_dl",
            )

            # View 2: Full Result Cards
            st.divider()
            st.subheader("📋 Full Result Cards")
            _render_result_cards(bulk_results, mode_prefix="bulk", min_tier_label="Silver+")

    # ══════════════════════════════════════════════════════════════
    # MODE 3: 🤖 Auto Slate
    # ══════════════════════════════════════════════════════════════
    with mode_slate:
        st.subheader("🤖 Auto Slate")
        st.caption(
            "Automatically scan today's games and generate the best picks. "
            "Configure options in the sidebar."
        )

        if generate_slate:
            with st.spinner("Fetching today's slate from the engine…"):
                raw_slate = get_todays_slate(top_n=slate_top_n)

            raw_picks = raw_slate.get("picks", [])
            games_scanned = raw_slate.get("games_scanned", 0)
            players_scanned = raw_slate.get("players_scanned", 0)

            if not raw_picks:
                st.info(
                    raw_slate.get("message", "No picks generated — there may be no games today.")
                )
            else:
                # Filter by platform, stat type, min tier
                filtered: list[dict] = []
                for pk in raw_picks:
                    pk_platforms = pk.get("platforms_available", [])
                    # Accept if no platform data or if any selected platform matches
                    platform_ok = (
                        not slate_platforms
                        or not pk_platforms
                        or any(p in slate_platforms for p in pk_platforms)
                    )
                    stat_ok = (
                        not slate_stat_types
                        or pk.get("stat_type", "") in slate_stat_types
                    )
                    tier_ok = _passes_tier_filter(
                        pk.get("tier", "Bronze"), slate_min_tier,
                    )
                    if platform_ok and stat_ok and tier_ok:
                        filtered.append(pk)

                # Enforce tier distribution to prevent inflation
                try:
                    from engine.confidence import enforce_tier_distribution
                    filtered, _any_downgrades = enforce_tier_distribution(filtered)
                    if _any_downgrades:
                        st.info("ℹ️ Tier distribution adjusted to prevent overconfidence inflation.")
                except ImportError:
                    pass

                # Re-analyze each filtered pick for full rich data
                full_results: list[dict] = []
                if filtered:
                    progress_slate = st.progress(0, text="Running deep analysis on slate picks…")
                    total_f = len(filtered)
                    for si, pk in enumerate(filtered):
                        pname = pk.get("player_name", "Unknown")
                        stype = pk.get("stat_type", "points")
                        pline = float(pk.get("prop_line", 20.5))
                        pid = pk.get("player_id")
                        progress_slate.progress(
                            si / total_f,
                            text=f"Analyzing {pname} — {stype} {pline}… ({si + 1}/{total_f})",
                        )
                        if not pid:
                            # Resolve via search
                            try:
                                sr = search_players(pname)
                                if sr:
                                    pid = sr[0].get("player_id")
                            except Exception:
                                pass
                        if not pid:
                            # Fall back to slate-level data only
                            pk.setdefault("confidence", {
                                "tier": pk.get("tier", "Bronze"),
                                "tier_emoji": TIER_EMOJI.get(pk.get("tier", "Bronze"), "🥉"),
                                "confidence_score": pk.get("confidence_score", 50),
                            })
                            pk.setdefault("player_name", pname)
                            pk.setdefault("stat_type", stype)
                            pk.setdefault("prop_line", pline)
                            pk.setdefault("platform", "prizepicks")
                            full_results.append(pk)
                            continue
                        try:
                            res = analyze_prop(
                                player_id=int(pid),
                                stat_type=stype,
                                prop_line=pline,
                                platform="prizepicks",
                            )
                            if res.get("status") == "error" or "confidence" not in res:
                                # Fall back to slate data
                                pk.setdefault("confidence", {
                                    "tier": pk.get("tier", "Bronze"),
                                    "tier_emoji": TIER_EMOJI.get(pk.get("tier", "Bronze"), "🥉"),
                                    "confidence_score": pk.get("confidence_score", 50),
                                })
                                pk.setdefault("player_name", pname)
                                pk.setdefault("stat_type", stype)
                                pk.setdefault("prop_line", pline)
                                pk.setdefault("platform", "prizepicks")
                                full_results.append(pk)
                                continue
                            res["player_id"] = int(pid)
                            res["player_name"] = pname
                            res["stat_type"] = stype
                            res["prop_line"] = pline
                            res["platform"] = "prizepicks"
                            full_results.append(res)
                        except Exception:
                            pk.setdefault("confidence", {
                                "tier": pk.get("tier", "Bronze"),
                                "confidence_score": pk.get("confidence_score", 50),
                            })
                            pk.setdefault("player_name", pname)
                            full_results.append(pk)

                    progress_slate.progress(1.0, text="Slate analysis complete!")

                st.session_state["slate_results"] = full_results
                st.session_state["slate_meta"] = {
                    "games_scanned": games_scanned,
                    "players_scanned": players_scanned,
                }

        # ── Display stored slate results ───────────────────────────
        slate_results = st.session_state.get("slate_results", [])
        slate_meta = st.session_state.get("slate_meta", {})
        if slate_results:
            # Header metrics
            hm = st.columns([1, 1, 1, 1])
            hm[0].metric("Games Scanned", slate_meta.get("games_scanned", "—"))
            hm[1].metric("Players Scanned", slate_meta.get("players_scanned", "—"))
            hm[2].metric("Picks Generated", len(slate_results))
            # Top tier
            tiers_found = [
                r.get("confidence", {}).get("tier", r.get("tier", "Bronze"))
                for r in slate_results
            ]
            top_tier = "Bronze"
            for t in ("Platinum", "Gold", "Silver"):
                if t in tiers_found:
                    top_tier = t
                    break
            hm[3].metric("Top Tier", f"{TIER_EMOJI.get(top_tier, '🥉')} {top_tier}")

            st.divider()

            # Summary Leaderboard
            st.subheader("🏆 Slate Leaderboard")
            sl_df = _build_leaderboard_df(slate_results)
            st.dataframe(sl_df, use_container_width=True, hide_index=True)

            csv_buf2 = io.StringIO()
            sl_df.to_csv(csv_buf2, index=False)
            st.download_button(
                "📥 Download Slate CSV",
                data=csv_buf2.getvalue(),
                file_name="auto_slate_results.csv",
                mime="text/csv",
                key="slate_csv_dl",
            )

            # Auto-save toggle
            auto_save = st.toggle(
                "Auto-save all Gold+ picks to Pick History",
                key="slate_auto_save",
            )
            if auto_save:
                saved_count = 0
                for sr in slate_results:
                    sr_tier = sr.get("confidence", {}).get("tier", sr.get("tier", "Bronze"))
                    if sr_tier in ("Gold", "Platinum"):
                        try:
                            save_data = {
                                "player_id": int(sr.get("player_id", 0)),
                                "player_name": sr.get("player_name", ""),
                                "team": sr.get("team", ""),
                                "opponent": sr.get("opponent", ""),
                                "stat_type": sr.get("stat_type", ""),
                                "prop_line": float(sr.get("prop_line", 0)),
                                "direction": sr.get("direction", "OVER"),
                                "model_probability": sr.get("model_probability", 0.5),
                                "edge_pct": sr.get("edge_pct", 0),
                                "confidence_score": sr.get("confidence", {}).get(
                                    "confidence_score", sr.get("confidence_score", 0)
                                ),
                                "tier": sr_tier,
                                "kelly_fraction": sr.get("bankroll", {}).get("kelly_fraction", 0),
                                "platform": sr.get("platform", "prizepicks"),
                                "notes": "Auto-saved from slate",
                            }
                            save_pick(save_data)
                            saved_count += 1
                        except Exception:
                            pass
                if saved_count:
                    st.success(f"✅ Auto-saved {saved_count} Gold+ pick(s) to Pick History.")

            # Full Result Cards
            st.divider()
            st.subheader("📋 Full Result Cards")
            _render_result_cards(
                slate_results,
                mode_prefix="slate",
                min_tier_label=st.session_state.get("slate_min_tier", "Silver+"),
            )
