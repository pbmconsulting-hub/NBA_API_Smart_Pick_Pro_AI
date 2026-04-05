"""
6_📊_Model_Health — Model health dashboard with calibration, accuracy, and drift detection.

Delegates to the SmartPicksProAI calibration engine for detailed model
performance metrics.
"""

import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
for _p in (_ROOT, _ROOT / "SmartPicksProAI", _ROOT / "SmartPicksProAI" / "frontend"):
    if str(_p) not in sys.path:
        sys.path.insert(0, str(_p))

import streamlit as st  # noqa: E402
import pandas as pd  # noqa: E402

st.set_page_config(page_title="Model Health", page_icon="📊", layout="wide")

BREAK_EVEN_WIN_RATE = 52.4

st.title("📊 Model Health")
st.caption(
    "Calibration curves, per-stat accuracy, confidence-tier analysis, "
    "and model drift detection."
)

# ── Try loading calibration engine ──────────────────────────────────────
try:
    from SmartPicksProAI.engine.calibration import (
        get_calibration_summary,
        get_isotonic_calibration_curve,
    )
    _CALIBRATION = True
except ImportError:
    _CALIBRATION = False

try:
    from SmartPicksProAI.tracking.bet_tracker import get_model_performance_stats
    _TRACKER = True
except ImportError:
    _TRACKER = False

# ── Time range ──────────────────────────────────────────────────────────
days_options = {"Last 30 days": 30, "Last 90 days": 90, "Last 180 days": 180, "All time": 365}
selected_range = st.selectbox("Analysis Period", list(days_options.keys()), index=1, key="mh_range")
days = days_options[selected_range]

tab_overview, tab_calibration, tab_stats, tab_drift = st.tabs([
    "📈 Overview", "📉 Calibration", "📊 Per-Stat", "🔔 Drift Detection"
])

# ── Tab 1: Overview ────────────────────────────────────────────────────
with tab_overview:
    st.markdown("### Model Performance Overview")

    if _CALIBRATION:
        summary = get_calibration_summary(days=days)
        m_cols = st.columns(4)
        m_cols[0].metric("Total Bets Analyzed", summary.get("total_bets", 0))
        overall_acc = summary.get("overall_accuracy")
        m_cols[1].metric(
            "Overall Accuracy",
            f"{overall_acc:.1%}" if overall_acc is not None else "N/A",
        )
        overconf = len(summary.get("overconfidence_buckets", []))
        m_cols[2].metric("Overconfident Buckets", overconf)
        m_cols[3].metric("Has Data", "✅" if summary.get("has_data") else "❌")

        if overall_acc is not None:
            be_status = "above" if overall_acc * 100 > BREAK_EVEN_WIN_RATE else "below"
            st.caption(
                f"📊 Break-even at -110 juice: {BREAK_EVEN_WIN_RATE}% — "
                f"currently {be_status} break-even"
            )
    else:
        st.warning("Calibration module not available.")

    if _TRACKER:
        stats = get_model_performance_stats()
        summary_data = stats.get("summary", {})
        if summary_data:
            st.markdown("### Bet Tracking Summary")
            sc = st.columns(4)
            sc[0].metric("Total Bets", summary_data.get("total_bets", 0))
            sc[1].metric("Win Rate", f"{summary_data.get('win_rate', 0):.1%}")
            sc[2].metric("Wins", summary_data.get("wins", 0))
            sc[3].metric("Losses", summary_data.get("losses", 0))

# ── Tab 2: Calibration ─────────────────────────────────────────────────
with tab_calibration:
    st.markdown("### Calibration Curve")

    if _CALIBRATION:
        curve = get_isotonic_calibration_curve(days=days)
        if curve and curve.get("bins"):
            df = pd.DataFrame({
                "Predicted": curve["bins"],
                "Actual": curve["actual"],
                "Count": curve.get("counts", [0] * len(curve["bins"])),
            })
            st.line_chart(df.set_index("Predicted")["Actual"])
            st.caption("Ideal calibration = diagonal line. Above = underconfident. Below = overconfident.")
            st.dataframe(df, use_container_width=True, hide_index=True)
        else:
            st.info("Not enough data for calibration curve.")
    else:
        st.warning("Calibration module not available.")

# ── Tab 3: Per-Stat Accuracy ──────────────────────────────────────────
with tab_stats:
    st.markdown("### Accuracy by Stat Type")

    if _TRACKER:
        stats = get_model_performance_stats()
        by_stat = stats.get("by_stat", {})
        if by_stat:
            rows = []
            for stat_name, data in by_stat.items():
                rows.append({
                    "Stat": stat_name.upper(),
                    "Bets": data.get("total", 0),
                    "Wins": data.get("wins", 0),
                    "Win Rate": f"{data.get('win_rate', 0):.1%}",
                })
            df = pd.DataFrame(rows)
            st.dataframe(df, use_container_width=True, hide_index=True)
        else:
            st.info("No per-stat data available yet.")

        by_tier = stats.get("by_tier", {})
        if by_tier:
            st.markdown("### Accuracy by Confidence Tier")
            rows = []
            for tier_name, data in by_tier.items():
                rows.append({
                    "Tier": tier_name,
                    "Bets": data.get("total", 0),
                    "Wins": data.get("wins", 0),
                    "Win Rate": f"{data.get('win_rate', 0):.1%}",
                })
            df = pd.DataFrame(rows)
            st.dataframe(df, use_container_width=True, hide_index=True)
    else:
        st.warning("Bet tracker module not available.")

# ── Tab 4: Drift Detection ─────────────────────────────────────────────
with tab_drift:
    st.markdown("### Model Drift Detection")
    st.caption(
        "Compares recent 14-day performance against the longer analysis period "
        "to detect performance degradation."
    )

    if _CALIBRATION:
        recent = get_calibration_summary(days=14)
        longer = get_calibration_summary(days=days)

        recent_acc = recent.get("overall_accuracy")
        longer_acc = longer.get("overall_accuracy")

        if recent_acc is not None and longer_acc is not None:
            drift = (recent_acc - longer_acc) * 100
            d_cols = st.columns(3)
            d_cols[0].metric("Last 14 Days", f"{recent_acc:.1%}")
            d_cols[1].metric(f"Last {days} Days", f"{longer_acc:.1%}")
            d_cols[2].metric("Drift", f"{drift:+.1f}pp")

            if drift < -5:
                st.error(
                    f"⚠️ Model drift detected: accuracy dropped {abs(drift):.1f}pp. "
                    "Consider retraining."
                )
            elif drift < -2:
                st.warning(
                    f"🔔 Slight drift: accuracy dropped {abs(drift):.1f}pp in recent period."
                )
            else:
                st.success("✅ No significant drift detected.")
        else:
            st.info("Not enough data for drift detection.")
    else:
        st.warning("Calibration module not available.")
