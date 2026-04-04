"""Model Health page (M1: per-stat accuracy, M2: drift alert, M3: break-even)."""
from typing import Any

import pandas as pd
import streamlit as st
from pages._shared import CALIBRATION_GAP_THRESHOLD, BREAK_EVEN_WIN_RATE
from api_service import get_pick_history


def render() -> None:
    st.title("🩺 Model Health")
    st.caption(
        "Calibration curves, per-stat accuracy, and confidence-tier analysis — "
        "powered by your historical pick data."
    )

    try:
        from engine.calibration import get_calibration_summary, get_isotonic_calibration_curve
    except ImportError:
        st.error("Calibration module not available.  Ensure engine/calibration.py is on the path.")
        return

    # ── Time range selector ───────────────────────────────────────
    days_options = {"Last 30 days": 30, "Last 90 days": 90, "Last 180 days": 180, "All time": 365}
    selected_range = st.selectbox("Analysis period", list(days_options.keys()), index=1, key="mh_range")
    days = days_options[selected_range]

    summary = get_calibration_summary(days=days)
    curve_data = get_isotonic_calibration_curve(days=days)

    # ── Overview metrics ──────────────────────────────────────────
    ov_cols = st.columns([1, 1, 1, 1])
    ov_cols[0].metric("Total Bets Analyzed", summary.get("total_bets", 0))
    overall_acc = summary.get("overall_accuracy")
    ov_cols[1].metric(
        "Overall Accuracy",
        f"{overall_acc:.1%}" if overall_acc is not None else "N/A",
    )
    overconf_count = len(summary.get("overconfidence_buckets", []))
    ov_cols[2].metric("Overconfident Buckets", overconf_count)
    ov_cols[3].metric("Has Data", "✅" if summary.get("has_data") else "❌")

    # M3: Break-even reference
    if overall_acc is not None:
        be_status = "above" if overall_acc * 100 > BREAK_EVEN_WIN_RATE else "below"
        st.caption(f"📊 Break-even at -110 juice: {BREAK_EVEN_WIN_RATE}% — currently {be_status} break-even")

    # M2: Model drift alert
    if summary.get("has_data"):
        recent_summary = get_calibration_summary(days=14)
        prior_summary = get_calibration_summary(days=44)
        recent_acc = recent_summary.get("overall_accuracy")
        prior_acc = prior_summary.get("overall_accuracy")
        if recent_acc is not None and prior_acc is not None and prior_acc > 0:
            drift = (prior_acc - recent_acc) * 100
            if drift > 5:
                st.error(
                    f"⚠️ **Model accuracy has declined** — "
                    f"Last 14 days: {recent_acc:.1%} vs prior period: {prior_acc:.1%} "
                    f"(−{drift:.1f}pp). Consider retraining."
                )
            elif drift < -5:
                st.success(
                    f"✅ **Model accuracy is improving** — "
                    f"Last 14 days: {recent_acc:.1%} vs prior period: {prior_acc:.1%} "
                    f"(+{abs(drift):.1f}pp)."
                )

    if not summary.get("has_data"):
        st.info(
            "Not enough historical data for calibration analysis.  "
            "Save more picks and record outcomes to unlock this page."
        )
        return

    st.divider()

    # ── Per-Stat Calibration Curves ──────────────────────────────
    st.subheader("📈 Calibration Curves (Per Stat)")
    st.caption("Predicted probability vs actual hit rate. Perfect calibration = diagonal line.")

    # Collect stat types from picks for tabs
    _stat_types_for_curves = sorted({p.get("stat_type", "") for p in (get_pick_history(limit=500) or []) if p.get("stat_type")})
    _cal_tab_labels = ["All Stats"] + [s.title() for s in _stat_types_for_curves]
    _cal_tab_keys = [None] + _stat_types_for_curves

    if _cal_tab_labels:
        cal_tabs = st.tabs(_cal_tab_labels)
        for cal_tab, cal_stat in zip(cal_tabs, _cal_tab_keys):
            with cal_tab:
                _curve = get_isotonic_calibration_curve(days=days, stat_type=cal_stat)
                _pts = _curve.get("curve", [])
                if _pts:
                    _cdf = pd.DataFrame(_pts)
                    _cdf["Perfect"] = _cdf["predicted"]
                    _chart = _cdf[["predicted", "actual", "Perfect"]].rename(
                        columns={"predicted": "Predicted", "actual": "Actual Hit Rate"}
                    )
                    st.line_chart(_chart.set_index("Predicted"), use_container_width=True)
                    if _curve.get("is_isotonic"):
                        st.caption("✅ Isotonic (PAVA-smoothed) calibration applied.")
                    else:
                        st.caption("Coarse bucket calibration (not enough data for isotonic).")
                    with st.expander("📋 Calibration Data", expanded=False):
                        st.dataframe(
                            pd.DataFrame(_pts)[["predicted", "actual", "count", "gap"]],
                            use_container_width=True,
                        )
                else:
                    st.info(f"No calibration data for {cal_stat.title() if cal_stat else 'all stats'}.")

    st.divider()

    # ── Per-Stat Accuracy (M1) ────────────────────────────────────
    st.subheader("📊 Accuracy by Stat Type")

    picks = get_pick_history(limit=500)
    if picks:
        stat_perf: dict[str, dict[str, int]] = {}
        for p in picks:
            s = p.get("stat_type", "unknown")
            if s not in stat_perf:
                stat_perf[s] = {"hits": 0, "misses": 0, "total": 0}
            stat_perf[s]["total"] += 1
            if p.get("result") == "hit":
                stat_perf[s]["hits"] += 1
            elif p.get("result") == "miss":
                stat_perf[s]["misses"] += 1

        stat_rows = []
        for stat_name, counts in sorted(stat_perf.items()):
            dec = counts["hits"] + counts["misses"]
            wr = (counts["hits"] / dec * 100) if dec > 0 else 0.0
            stat_rows.append({
                "Stat Type": stat_name.title(),
                "Total": counts["total"],
                "Hits": counts["hits"],
                "Misses": counts["misses"],
                "Win Rate %": round(wr, 1),
                "vs Break-Even": f"{'✅' if wr > BREAK_EVEN_WIN_RATE else '❌'} {wr - BREAK_EVEN_WIN_RATE:+.1f}pp" if dec > 0 else "N/A",
            })
        if stat_rows:
            st.dataframe(pd.DataFrame(stat_rows), use_container_width=True)

        # M1: Per-stat tabs with mini metrics
        st.divider()
        st.subheader("📈 Per-Stat Breakdown")
        stat_names = sorted(stat_perf.keys())
        if stat_names:
            stat_tabs = st.tabs([s.title() for s in stat_names])
            for stab, sname in zip(stat_tabs, stat_names):
                with stab:
                    counts = stat_perf[sname]
                    dec = counts["hits"] + counts["misses"]
                    wr = (counts["hits"] / dec * 100) if dec > 0 else 0.0
                    mc = st.columns(4)
                    mc[0].metric("Total Picks", counts["total"])
                    mc[1].metric("Hits", counts["hits"])
                    mc[2].metric("Misses", counts["misses"])
                    mc[3].metric(
                        "Win Rate",
                        f"{wr:.1f}%" if dec > 0 else "N/A",
                        delta=f"{'✅' if wr > BREAK_EVEN_WIN_RATE else '❌'} vs {BREAK_EVEN_WIN_RATE}%" if dec > 0 else None,
                        delta_color="normal" if wr > BREAK_EVEN_WIN_RATE else "inverse",
                    )
    else:
        st.info("No pick data available for per-stat breakdown.")

    st.divider()

    # ── Tier Confidence Analysis ──────────────────────────────────
    st.subheader("🎯 Tier Confidence Analysis")
    st.caption("Are higher tiers actually more accurate?")

    if picks:
        tier_perf: dict[str, dict[str, Any]] = {}
        for p in picks:
            t = p.get("tier", "Bronze")
            if t not in tier_perf:
                tier_perf[t] = {"hits": 0, "misses": 0, "total": 0, "conf_sum": 0.0}
            tier_perf[t]["total"] += 1
            tier_perf[t]["conf_sum"] += p.get("confidence_score", 0)
            if p.get("result") == "hit":
                tier_perf[t]["hits"] += 1
            elif p.get("result") == "miss":
                tier_perf[t]["misses"] += 1

        tier_order = ["Platinum", "Gold", "Silver", "Bronze", "Avoid"]
        tier_rows = []
        for tier_name in tier_order:
            if tier_name not in tier_perf:
                continue
            counts = tier_perf[tier_name]
            dec = counts["hits"] + counts["misses"]
            wr = (counts["hits"] / dec * 100) if dec > 0 else 0.0
            avg_conf = counts["conf_sum"] / counts["total"] if counts["total"] > 0 else 0
            tier_rows.append({
                "Tier": tier_name,
                "Total Picks": counts["total"],
                "Decided": dec,
                "Win Rate %": round(wr, 1),
                "Avg Confidence": round(avg_conf, 1),
                "Calibration Gap": round(wr - avg_conf, 1) if dec > 0 else None,
            })
        if tier_rows:
            st.dataframe(pd.DataFrame(tier_rows), use_container_width=True)
            for row in tier_rows:
                gap = row.get("Calibration Gap")
                if gap is not None and gap < -CALIBRATION_GAP_THRESHOLD:
                    st.warning(f"⚠️ **{row['Tier']}** tier is overconfident by {abs(gap):.0f}pp")
                elif gap is not None and gap > CALIBRATION_GAP_THRESHOLD:
                    st.success(f"✅ **{row['Tier']}** tier is underconfident by {gap:.0f}pp — model is conservative")

    # ── Overconfidence Buckets ────────────────────────────────────
    overconf = summary.get("overconfidence_buckets", [])
    if overconf:
        st.divider()
        st.subheader("⚠️ Overconfident Probability Buckets")
        st.caption("Buckets where predicted probability exceeds actual hit rate by >5%.")
        for mid in overconf:
            st.text(f"  • {mid:.0%} bucket")

    # ── Backtester ────────────────────────────────────────────────
    st.divider()
    st.subheader("🧪 Historical Backtester")
    st.caption("Run the engine's backtester on archived slates to validate model accuracy on historical data.")

    try:
        from engine.backtester import run_backtest
        _backtester_available = True
    except ImportError:
        _backtester_available = False

    if _backtester_available:
        bt_cols = st.columns([1, 1, 1])
        bt_season = bt_cols[0].text_input("Season", value="2024-25", key="bt_season")
        bt_stat_types = bt_cols[1].multiselect(
            "Stat Types",
            ["points", "rebounds", "assists", "threes", "steals", "blocks", "turnovers"],
            default=["points", "rebounds", "assists"],
            key="bt_stat_types",
        )
        bt_min_edge = bt_cols[2].slider("Min Edge %", 0.0, 0.20, 0.05, 0.01, key="bt_min_edge")
        bt_tier = st.selectbox("Tier Filter", ["All", "Platinum", "Gold", "Silver", "Bronze"], key="bt_tier")

        if st.button("🚀 Run Backtest", key="bt_run", use_container_width=True):
            with st.spinner("Running historical backtest…"):
                tier_arg = None if bt_tier == "All" else bt_tier
                bt_result = run_backtest(
                    season=bt_season,
                    stat_types=bt_stat_types,
                    min_edge=bt_min_edge,
                    tier_filter=tier_arg,
                )

            if bt_result.get("message"):
                st.info(bt_result["message"])
            else:
                bt_m = st.columns([1, 1, 1, 1])
                bt_m[0].metric("Total Picks", bt_result.get("total_picks", 0))
                bt_acc = bt_result.get("win_rate", 0)
                bt_m[1].metric("Accuracy", f"{bt_acc:.1%}" if bt_acc else "N/A")
                bt_roi = bt_result.get("roi", 0)
                bt_m[2].metric("ROI", f"{bt_roi:+.1f}%" if bt_roi else "N/A")
                bt_m[3].metric("Sharpe Ratio", f"{bt_result.get('sharpe_ratio', 0):.2f}")

                # Tier breakdown
                tier_breakdown = bt_result.get("tier_breakdown", {})
                if tier_breakdown:
                    st.markdown("**Tier Breakdown**")
                    bt_rows = []
                    for t_name, t_data in tier_breakdown.items():
                        t_dec = t_data.get("hits", 0) + t_data.get("misses", 0)
                        t_wr = (t_data["hits"] / t_dec * 100) if t_dec > 0 else 0
                        bt_rows.append({
                            "Tier": t_name,
                            "Plays": t_data.get("total", 0),
                            "Hits": t_data.get("hits", 0),
                            "Misses": t_data.get("misses", 0),
                            "Win Rate %": round(t_wr, 1),
                        })
                    if bt_rows:
                        st.dataframe(pd.DataFrame(bt_rows), use_container_width=True)

                # P&L chart
                pick_log = bt_result.get("pick_log", [])
                if pick_log:
                    st.markdown("**Cumulative P&L**")
                    cumulative = []
                    running = 0.0
                    for pl in pick_log:
                        running += pl.get("pnl", 0)
                        cumulative.append({"Play #": len(cumulative) + 1, "Cumulative P&L": round(running, 2)})
                    if cumulative:
                        st.line_chart(pd.DataFrame(cumulative).set_index("Play #"), use_container_width=True)
    else:
        st.info("Backtester module not available. Ensure engine/backtester.py is on the path.")
