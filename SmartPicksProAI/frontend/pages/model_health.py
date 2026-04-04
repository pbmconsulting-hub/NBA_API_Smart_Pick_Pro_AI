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

    # ── Calibration Curve ─────────────────────────────────────────
    st.subheader("📈 Calibration Curve")
    st.caption("Predicted probability vs actual hit rate. Perfect calibration = diagonal line.")

    curve_pts = curve_data.get("curve", [])
    if curve_pts:
        cal_df = pd.DataFrame(curve_pts)
        cal_df["Perfect"] = cal_df["predicted"]
        chart_df = cal_df[["predicted", "actual", "Perfect"]].rename(
            columns={"predicted": "Predicted", "actual": "Actual Hit Rate"}
        )
        st.line_chart(chart_df.set_index("Predicted"), use_container_width=True)

        if curve_data.get("is_isotonic"):
            st.caption("✅ Isotonic (PAVA-smoothed) calibration applied.")
        else:
            st.caption("Coarse bucket calibration (not enough data for isotonic).")

        with st.expander("📋 Calibration Data", expanded=False):
            st.dataframe(
                pd.DataFrame(curve_pts)[["predicted", "actual", "count", "gap"]],
                use_container_width=True,
            )
    else:
        st.info("No calibration curve data available.")

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
