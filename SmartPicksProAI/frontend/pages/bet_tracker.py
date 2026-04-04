"""Bet Tracker page (F8: My Bets filters, F9: ROI chart)."""
import pandas as pd
import streamlit as st
from pages._shared import (
    show_df, TIER_EMOJI, TIER_COLORS, BET_RESULT_EMOJI, TRACKER_STAT_TYPES,
)
from styles.theme import get_summary_cards_html
from tracking.bet_tracker import (
    get_model_performance_stats, log_new_bet, record_bet_result,
    VALID_PLATFORMS,
)
from tracking.database import load_all_bets


def render() -> None:
    st.title("📈 Bet Tracker")
    st.caption("Track model performance • Log bets • Record results")

    # ── Summary Cards ─────────────────────────────────────────
    stats = get_model_performance_stats()
    summary = stats.get("summary", {})
    st.markdown(
        get_summary_cards_html(
            total_bets=summary.get("total_bets", 0),
            wins=summary.get("wins", 0),
            losses=summary.get("losses", 0),
            pushes=summary.get("pushes", 0),
            pending=summary.get("pending", 0),
            win_rate=summary.get("win_rate", 0.0),
        ),
        unsafe_allow_html=True,
    )

    # ── Tabs ──────────────────────────────────────────────────
    tab_bets, tab_log, tab_perf = st.tabs([
        "📋 My Bets", "➕ Log Bet", "📊 Performance",
    ])

    # ── Tab: My Bets (F8: filters) ───────────────────────────
    with tab_bets:
        bets = load_all_bets(limit=100)
        if not bets:
            st.info("No bets logged yet. Use the **Log Bet** tab or run the **Prop Analyzer**.")
        else:
            # F8: filter controls
            all_tiers = sorted({b.get("confidence_tier", "") for b in bets if b.get("confidence_tier")})
            all_stats = sorted({b.get("stat_type", "?") for b in bets})
            result_options = ["All", "Pending", "Win", "Loss", "Push"]

            fc1, fc2, fc3 = st.columns(3)
            bt_filter_tier = fc1.selectbox("Filter by Tier", ["All"] + all_tiers, key="bt_filter_tier")
            bt_filter_stat = fc2.selectbox("Filter by Stat", ["All"] + all_stats, key="bt_filter_stat")
            bt_filter_result = fc3.selectbox("Filter by Result", result_options, key="bt_filter_result")

            filtered_bets = bets
            if bt_filter_tier != "All":
                filtered_bets = [b for b in filtered_bets if b.get("confidence_tier") == bt_filter_tier]
            if bt_filter_stat != "All":
                filtered_bets = [b for b in filtered_bets if b.get("stat_type") == bt_filter_stat]
            if bt_filter_result != "All":
                _rmap = {"Win": "win", "Loss": "loss", "Push": "push"}
                if bt_filter_result == "Pending":
                    filtered_bets = [b for b in filtered_bets if not b.get("result")]
                else:
                    filtered_bets = [b for b in filtered_bets if b.get("result") == _rmap.get(bt_filter_result)]

            if not filtered_bets:
                st.info("No bets match the current filters.")

            for bet in filtered_bets:
                bid = bet.get("bet_id", "?")
                name = bet.get("player_name", "Unknown")
                stat = bet.get("stat_type", "?")
                line = bet.get("prop_line", 0)
                direction = bet.get("direction", "?")
                tier = bet.get("confidence_tier", "")
                score = bet.get("confidence_score", 0)
                edge = bet.get("edge_pct", 0)
                result_val = bet.get("result")
                bet_date = bet.get("bet_date", "?")
                opp = bet.get("opponent", "?")
                plat = bet.get("platform", "?")
                src = bet.get("source", "manual")

                dir_icon = "🟢" if direction == "OVER" else "🔴"
                tier_emoji = TIER_EMOJI.get(tier, "")
                tier_color = TIER_COLORS.get(tier, "#C0C0C0")
                result_emoji = BET_RESULT_EMOJI.get(result_val, "⏳") if result_val else "⏳"

                with st.expander(
                    f"{result_emoji} {tier_emoji} **{name}** — {stat.upper()} "
                    f"{dir_icon} {direction} {line}  •  vs {opp}  •  {bet_date}",
                    expanded=False,
                ):
                    st.markdown(
                        f'<div style="height:4px;background:{tier_color};border-radius:2px;margin-bottom:8px"></div>',
                        unsafe_allow_html=True,
                    )
                    det = st.columns([1, 1, 1, 1, 1])
                    det[0].metric("Confidence", f"{score:.0f}/100")
                    det[1].metric("Edge", f"{edge:+.1f}%")
                    det[2].metric("Platform", plat)
                    det[3].metric("Tier", tier or "—")
                    det[4].metric("Source", src.title())

                    if not result_val:
                        st.caption("Record outcome:")
                        res = st.columns(4)
                        if res[0].button("✅ Win", key=f"bt_win_{bid}"):
                            record_bet_result(int(bid), "win")
                            st.rerun()
                        if res[1].button("❌ Loss", key=f"bt_loss_{bid}"):
                            record_bet_result(int(bid), "loss")
                            st.rerun()
                        if res[2].button("➖ Push", key=f"bt_push_{bid}"):
                            record_bet_result(int(bid), "push")
                            st.rerun()
                    else:
                        actual = bet.get("actual_value")
                        if actual is not None:
                            st.metric("Actual Value", actual)

    # ── Tab: Log Bet ──────────────────────────────────────────
    with tab_log:
        st.subheader("➕ Log a New Bet")
        with st.form("log_bet_form", clear_on_submit=True):
            fc1, fc2 = st.columns(2)
            with fc1:
                lb_player = st.text_input("Player Name", placeholder="e.g. LeBron James")
                lb_stat = st.selectbox("Stat Type", TRACKER_STAT_TYPES)
                lb_line = st.number_input("Prop Line", min_value=0.5, step=0.5, value=20.0)
            with fc2:
                lb_direction = st.selectbox("Direction", ["OVER", "UNDER"])
                lb_platform = st.selectbox("Platform", sorted(VALID_PLATFORMS))
                lb_opponent = st.text_input("Opponent (optional)", placeholder="e.g. BOS")

            lb_notes = st.text_input("Notes (optional)")
            submitted = st.form_submit_button("📝 Log Bet", use_container_width=True)

        if submitted:
            if not lb_player or not lb_player.strip():
                st.error("Player name is required.")
            else:
                res = log_new_bet(
                    player_name=lb_player,
                    stat_type=lb_stat,
                    prop_line=float(lb_line),
                    direction=lb_direction,
                    platform=lb_platform,
                    opponent=lb_opponent,
                    notes=lb_notes,
                    source="manual",
                )
                if res.get("success"):
                    st.success(f"Bet logged! (ID: {res['bet_id']})")
                else:
                    st.error(res.get("error", "Failed to log bet."))

    # ── Tab: Performance (F9: ROI chart) ──────────────────────
    with tab_perf:
        st.subheader("📊 Performance by Tier")
        tier_data = stats.get("by_tier", [])
        if tier_data:
            tc = st.columns(min(len(tier_data), 4))
            for i, t in enumerate(tier_data):
                tier_name = t.get("tier", "?")
                wr = t.get("win_rate", 0)
                decided = t.get("wins", 0) + t.get("losses", 0)
                emoji = TIER_EMOJI.get(tier_name, "🥉")
                tc[i % len(tc)].metric(
                    f"{emoji} {tier_name}",
                    f"{wr:.0f}% WR" if decided > 0 else "N/A",
                    delta=f"{t.get('wins', 0)}/{decided} decided" if decided > 0 else f"{t.get('total', 0)} pending",
                )
        else:
            st.info("No tier data yet.")

        st.divider()
        st.subheader("📊 Performance by Stat Type")
        stat_data = stats.get("by_stat", [])
        if stat_data:
            show_df(stat_data, columns=["stat_type", "total", "wins", "losses", "pending", "win_rate"])
        else:
            st.info("No stat data yet.")

        st.divider()
        st.subheader("📊 Performance by Platform")
        plat_data = stats.get("by_platform", [])
        if plat_data:
            show_df(plat_data, columns=["platform", "total", "wins", "losses", "pending", "win_rate"])
        else:
            st.info("No platform data yet.")

        # F9: Cumulative P&L chart over time
        st.divider()
        st.subheader("📈 Cumulative P&L Over Time")
        all_bets = load_all_bets(limit=500)
        dated_bets = sorted(
            [b for b in all_bets if b.get("bet_date") and b.get("result") in ("win", "loss")],
            key=lambda b: b["bet_date"],
        )
        if dated_bets:
            cumulative = []
            running_pl = 0.0
            for b in dated_bets:
                if b["result"] == "win":
                    running_pl += 0.909
                elif b["result"] == "loss":
                    running_pl -= 1.0
                cumulative.append({"Date": b["bet_date"], "Cumulative P&L (units)": round(running_pl, 2)})
            pl_df = pd.DataFrame(cumulative)
            st.line_chart(pl_df, x="Date", y="Cumulative P&L (units)", use_container_width=True)
            st.caption("P&L assumes $1 unit bets at -110 juice (payout = $0.909 per win).")
        else:
            st.info("No decided bets with dates available for charting.")
