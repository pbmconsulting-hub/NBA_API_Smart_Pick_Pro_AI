"""Pick History page (F6: date filter, F7: actual value entry, M3: break-even)."""
import datetime
import streamlit as st
from pages._shared import (
    TIER_EMOJI, TIER_COLORS, RESULT_EMOJI, BREAK_EVEN_WIN_RATE,
)
from api_service import get_pick_history, update_pick_result


def _parse_date(s: str) -> datetime.date | None:
    try:
        return datetime.date.fromisoformat(s[:10])
    except (ValueError, TypeError):
        return None


def render() -> None:
    st.title("📋 Pick History")
    st.caption("Track your saved prop analyses and record outcomes.")

    picks = get_pick_history(limit=100)

    if not picks:
        st.info("No saved picks yet.  Use the **Prop Analyzer** to analyze and save picks.")
        return

    # ── Summary stats ───────────────────────────────────────────────
    total = len(picks)
    hits = sum(1 for p in picks if p.get("result") == "hit")
    misses = sum(1 for p in picks if p.get("result") == "miss")
    pushes = sum(1 for p in picks if p.get("result") == "push")
    pending = total - hits - misses - pushes
    decided = hits + misses
    win_rate = (hits / decided * 100) if decided > 0 else 0.0

    # ROI mode toggle
    roi_mode = st.radio("ROI Mode", ["Flat (-110)", "Kelly-Weighted"], horizontal=True, key="roi_mode")

    if roi_mode == "Flat (-110)":
        roi = ((hits * 0.909 - misses) / decided * 100) if decided > 0 else 0.0
    else:
        # Kelly-weighted ROI: sum kelly_fraction * payout for hits, subtract kelly_fraction for misses
        kelly_profit = 0.0
        kelly_risked = 0.0
        for p in picks:
            kf = float(p.get("kelly_fraction", 0) or 0)
            if kf <= 0:
                kf = 0.01  # minimum fraction for decided picks
            if p.get("result") == "hit":
                kelly_profit += kf * 0.909  # payout at -110
                kelly_risked += kf
            elif p.get("result") == "miss":
                kelly_profit -= kf
                kelly_risked += kf
        roi = (kelly_profit / kelly_risked * 100) if kelly_risked > 0 else 0.0

    sum_cols = st.columns(7)
    sum_cols[0].metric("Total Picks", total)
    sum_cols[1].metric("Hits", f"✅ {hits}")
    sum_cols[2].metric("Misses", f"❌ {misses}")
    sum_cols[3].metric("Pushes", f"➖ {pushes}")
    sum_cols[4].metric("Pending", f"⏳ {pending}")
    sum_cols[5].metric("Win Rate", f"{win_rate:.1f}%" if decided > 0 else "N/A")
    sum_cols[6].metric(
        "Est. ROI (-110)",
        f"{roi:+.1f}%" if decided > 0 else "N/A",
        delta="Profit" if roi > 0 else ("Loss" if roi < 0 else "Break Even"),
        delta_color="normal" if roi > 0 else ("inverse" if roi < 0 else "off"),
    )

    # M3: Break-even reference
    if decided > 0:
        be_status = "✅ Above" if win_rate > BREAK_EVEN_WIN_RATE else "❌ Below"
        st.caption(f"📊 Break-even at -110 juice: {BREAK_EVEN_WIN_RATE}% — {be_status} break-even")

    # ── Tier breakdown ──────────────────────────────────────────────
    if decided > 0:
        st.divider()
        st.subheader("📊 Performance by Tier")
        tiers_seen: dict[str, dict[str, int]] = {}
        for p in picks:
            t = p.get("tier", "Bronze")
            if t not in tiers_seen:
                tiers_seen[t] = {"hits": 0, "misses": 0, "total": 0}
            tiers_seen[t]["total"] += 1
            if p.get("result") == "hit":
                tiers_seen[t]["hits"] += 1
            elif p.get("result") == "miss":
                tiers_seen[t]["misses"] += 1

        tier_cols = st.columns(min(len(tiers_seen), 5))
        sorted_tiers = sorted(
            tiers_seen.items(),
            key=lambda x: -x[1].get("total", 0),
        )
        for i, (tier_name, counts) in enumerate(sorted_tiers):
            tier_decided = counts["hits"] + counts["misses"]
            tier_wr = (counts["hits"] / tier_decided * 100) if tier_decided > 0 else 0.0
            emoji = TIER_EMOJI.get(tier_name, "🥉")
            tier_cols[i % len(tier_cols)].metric(
                f"{emoji} {tier_name}",
                f"{tier_wr:.0f}% WR" if tier_decided > 0 else "N/A",
                delta=f"{counts['hits']}/{tier_decided} decided" if tier_decided > 0 else f"{counts['total']} pending",
            )

    st.divider()

    # ── Filter controls (F6: date range added) ──────────────────────
    st.subheader("🗂️ All Picks")

    all_tiers = sorted({p.get("tier", "Bronze") for p in picks})
    all_stats = sorted({p.get("stat_type", "?") for p in picks})
    result_options = ["All", "Pending", "Hit", "Miss", "Push"]

    col_tier, col_stat, col_result, col_date = st.columns(4)
    filter_tier = col_tier.selectbox(
        "Filter by Tier",
        ["All"] + all_tiers,
        key="ph_filter_tier",
    )
    filter_stat = col_stat.selectbox(
        "Filter by Stat",
        ["All"] + all_stats,
        key="ph_filter_stat",
    )
    filter_result = col_result.selectbox(
        "Filter by Result",
        result_options,
        key="ph_filter_result",
    )
    with col_date:
        date_range = st.date_input(
            "Date Range",
            value=(),
            key="ph_filter_date",
        )

    # Apply filters
    _result_map = {"Hit": "hit", "Miss": "miss", "Push": "push", "Pending": None}
    filtered = picks
    if filter_tier != "All":
        filtered = [p for p in filtered if p.get("tier", "Bronze") == filter_tier]
    if filter_stat != "All":
        filtered = [p for p in filtered if p.get("stat_type", "?") == filter_stat]
    if filter_result != "All":
        if filter_result == "Pending":
            filtered = [p for p in filtered if not p.get("result")]
        else:
            filtered = [p for p in filtered if p.get("result") == _result_map.get(filter_result)]
    # F6: date range filter
    if date_range and len(date_range) == 2:
        start_d, end_d = date_range
        filtered = [
            p for p in filtered
            if (d := _parse_date(p.get("pick_date", ""))) is not None
            and start_d <= d <= end_d
        ]

    if not filtered:
        st.info("No picks match the current filters.")

    for pick in filtered:
        pick_id = pick.get("pick_id", "?")
        p_name = pick.get("player_name", "Unknown")
        p_tier = pick.get("tier", "Bronze")
        p_dir = pick.get("direction", "?")
        p_stat = pick.get("stat_type", "?")
        p_line = pick.get("prop_line", 0)
        p_conf = pick.get("confidence_score", 0)
        p_edge = pick.get("edge_pct", 0)
        p_kelly = pick.get("kelly_fraction", 0)
        p_result = pick.get("result")
        p_date = pick.get("pick_date", "?")
        p_opp = pick.get("opponent", "?")
        p_regime = pick.get("regime_flag", "stable")

        tier_emoji = TIER_EMOJI.get(p_tier, "🥉")
        tier_color = TIER_COLORS.get(p_tier, "#C0C0C0")
        result_emoji = RESULT_EMOJI.get(p_result, "⏳") if p_result else "⏳"
        dir_icon = "🟢" if p_dir == "OVER" else "🔴"

        with st.expander(
            f"{result_emoji} {tier_emoji} **{p_name}** — {p_stat.upper()} "
            f"{dir_icon} {p_dir} {p_line}  •  vs {p_opp}  •  {p_date}",
            expanded=False,
        ):
            st.markdown(
                f'<div style="height:4px;background:{tier_color};border-radius:2px;margin-bottom:8px"></div>',
                unsafe_allow_html=True,
            )
            det_cols = st.columns([1, 1, 1, 1, 1])
            det_cols[0].metric("Confidence", f"{p_conf:.0f}/100")
            det_cols[1].metric("Edge", f"{p_edge:+.1f}%")
            det_cols[2].metric("Kelly", f"{p_kelly * 100:.2f}%")
            det_cols[3].metric("Regime", p_regime.title())
            det_cols[4].metric("Platform", pick.get("platform", "?").title())

            if not p_result:
                st.caption("Record outcome:")
                # F7: Actual value entry
                actual_val = st.number_input(
                    "Actual stat value",
                    min_value=0.0,
                    step=0.5,
                    value=0.0,
                    key=f"actual_{pick_id}",
                )
                res_cols = st.columns(4)
                _av = actual_val if actual_val > 0 else None
                if res_cols[0].button("✅ Hit", key=f"hit_{pick_id}"):
                    update_pick_result(int(pick_id), "hit", actual_value=_av)
                    st.rerun()
                if res_cols[1].button("❌ Miss", key=f"miss_{pick_id}"):
                    update_pick_result(int(pick_id), "miss", actual_value=_av)
                    st.rerun()
                if res_cols[2].button("➖ Push", key=f"push_{pick_id}"):
                    update_pick_result(int(pick_id), "push", actual_value=_av)
                    st.rerun()
            else:
                actual = pick.get("actual_value")
                if actual is not None:
                    st.metric("Actual Value", actual)
