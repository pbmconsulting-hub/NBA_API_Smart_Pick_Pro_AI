# pages/12_📊_Backtester.py
# Historical backtesting UI for SmartAI-NBA.
# Runs the prediction model against historical game log data to validate accuracy.

import streamlit as st

st.set_page_config(
    page_title="Backtester — SmartAI-NBA",
    page_icon="📊",
    layout="wide",
)

from styles.theme import get_global_css
st.markdown(get_global_css(), unsafe_allow_html=True)

# ── Joseph M. Smith Floating Widget ────────────────────────────
from utils.components import inject_joseph_floating
st.session_state["joseph_page_context"] = "page_backtester"
inject_joseph_floating()

from utils.premium_gate import premium_gate
if not premium_gate("Backtester"):
    st.stop()

st.title("📊 Historical Backtester")
st.markdown(
    "Validate the model against real game logs. "
    "See win rates, ROI, and tier-by-tier performance metrics."
)

with st.expander("📖 How to Use This Page", expanded=False):
    st.markdown("""
    ### Historical Backtester — Validate Before You Bet
    
    The Backtester runs the prediction model against **real historical game logs** to measure accuracy.
    
    **How to Run a Backtest**
    1. Select players (or use all cached players)
    2. Choose the stat types to backtest (points, rebounds, assists, etc.)
    3. Set the number of historical games per player (more games = more reliable results)
    4. Click "Run Backtest" and wait for the analysis to complete
    
    **Understanding Results**
    - **Win Rate**: Percentage of picks that would have been correct
    - **ROI**: Return on investment assuming flat $10 bets
    - **Tier Breakdown**: How each confidence tier (Platinum/Gold/Silver/Bronze) performed
    - **By Stat Type**: Which stat categories the model predicts best
    
    **What Good Results Look Like**
    - Overall win rate above 55% is strong
    - Platinum/Gold tiers should have the highest win rates
    - If Bronze tier beats Platinum, the confidence model needs recalibration
    
    💡 **Pro Tips:**
    - Run "Refresh Game Logs" on the Data Feed page first to load historical data
    - Backtest with at least 10 games per player for statistically meaningful results
    - Focus on stat types where the model shows consistent edge (>55% win rate)
    """)

st.divider()

# ── Imports ──────────────────────────────────────────────────
try:
    from engine.backtester import run_backtest
    _BACKTESTER_AVAILABLE = True
except ImportError:
    _BACKTESTER_AVAILABLE = False

try:
    from data.game_log_cache import (
        get_all_cached_players,
        load_game_logs_from_cache,
        save_game_logs_to_cache,
    )
    _CACHE_AVAILABLE = True
except ImportError:
    _CACHE_AVAILABLE = False

try:
    from data.nba_data_service import refresh_historical_data_for_tonight as _refresh_hist
    _HIST_REFRESH_AVAILABLE = True
except ImportError:
    _HIST_REFRESH_AVAILABLE = False

try:
    from engine.clv_tracker import get_clv_summary, validate_model_edge
    _CLV_AVAILABLE = True
except ImportError:
    _CLV_AVAILABLE = False

# ── Sidebar Controls ─────────────────────────────────────────
with st.sidebar:
    st.subheader("⚙️ Backtest Settings")

    season = st.selectbox("Season", ["2025-26", "2024-25", "2023-24", "2022-23"], index=0)

    stat_options = [
        "points", "rebounds", "assists", "steals", "blocks", "threes", "turnovers",
        "ftm", "fta", "fgm", "fga", "minutes", "personal_fouls",
        "offensive_rebounds", "defensive_rebounds",
    ]
    selected_stats = st.multiselect(
        "Stat Types",
        stat_options,
        default=["points", "rebounds", "assists"],
    )

    min_edge = st.slider("Min Edge (%)", min_value=1, max_value=20, value=5, step=1) / 100.0

    tier_filter = st.selectbox(
        "Tier Filter (optional)",
        ["All Tiers", "ELITE", "STRONG", "VALUE", "LEAN"],
        index=0,
    )
    tier_filter_val = None if tier_filter == "All Tiers" else tier_filter

    use_real_lines = st.checkbox(
        "📦 Use Real PrizePicks Lines (archive)",
        value=True,
        help="When available, use actual PrizePicks prop lines from the "
             "mirror archive instead of synthetic season-average lines. "
             "Only available for dates the archive has captured.",
    )

    run_btn = st.button("▶ Run Backtest", type="primary", use_container_width=True)

    st.divider()

    # ── Historical data refresh ───────────────────────────────
    st.subheader("📡 Historical Data")
    st.caption(
        "Auto-load game logs for tonight's players "
        "and update CLV closing lines."
    )
    refresh_hist_btn = st.button(
        "🔄 Refresh Historical Data",
        use_container_width=True,
        disabled=not _HIST_REFRESH_AVAILABLE,
        help="Retrieves the last 30 games per player for all teams playing tonight.",
    )

# ── Info if backtester not available ─────────────────────────
if not _BACKTESTER_AVAILABLE:
    st.error("⚠️ Backtester engine not available. Check engine/backtester.py.")
    st.stop()

# ── Historical Data Refresh Handler ──────────────────────────
if refresh_hist_btn and _HIST_REFRESH_AVAILABLE:
    _prog = st.progress(0, text="Loading historical game logs…")
    def _prog_cb(current, total, msg):
        _prog.progress(min(current / max(total, 1), 1.0), text=msg)

    with st.spinner("Loading historical game logs from API-NBA…"):
        todays_games = st.session_state.get("todays_games", [])
        hist_result = _refresh_hist(games=todays_games, last_n_games=30, progress_callback=_prog_cb)

    _prog.empty()
    refreshed  = hist_result.get("players_refreshed", 0)
    clv_closed = hist_result.get("clv_updated", 0)
    errs       = hist_result.get("errors", 0)

    if refreshed > 0:
        st.success(
            f"✅ Historical data refreshed: **{refreshed} player(s)** cached"
            + (f", **{clv_closed} CLV record(s)** updated" if clv_closed else "")
            + (f", {errs} error(s)" if errs else "")
        )
    elif not todays_games:
        st.warning(
            "⚠️ No tonight's games loaded. Go to **📡 Live Games** and click "
            "**Auto-Load Tonight's Games** first, then refresh historical data."
        )
    else:
        st.info(
            "ℹ️ No game logs retrieved. This typically means the data feed is "
            "temporarily unavailable, or players don't have IDs in the loaded data."
        )

# ── Load cached game logs ─────────────────────────────────────
game_logs_by_player = {}
stale_count = 0
if _CACHE_AVAILABLE:
    cached_players = get_all_cached_players()
    if cached_players:
        for pname in cached_players:
            logs, is_stale = load_game_logs_from_cache(pname)
            if logs:
                game_logs_by_player[pname] = logs
                if is_stale:
                    stale_count += 1

# Also check session state (set by Player Simulator page)
session_logs = st.session_state.get("game_logs_by_player", {})
for pname, logs in session_logs.items():
    if logs and pname not in game_logs_by_player:
        game_logs_by_player[pname] = logs

# ── Status ────────────────────────────────────────────────────
_fresh = len(game_logs_by_player) - stale_count
if game_logs_by_player:
    st.info(
        f"📁 **{len(game_logs_by_player)} player(s)** with cached game logs available "
        f"({_fresh} fresh · {stale_count} stale). "
        f"Use **🔄 Refresh Historical Data** in the sidebar to update all logs at once."
    )
else:
    st.warning(
        "No game log data found. Click **🔄 Refresh Historical Data** in the sidebar "
        "(requires tonight's games to be loaded on **📡 Live Games** first). "
        "Or go to **🔮 Player Simulator**, search for players, "
        "and their logs will be cached for backtesting."
    )
    if not _HIST_REFRESH_AVAILABLE:
        st.stop()

# ── CLV Model Validation Panel ────────────────────────────────
if _CLV_AVAILABLE:
    clv_summary = get_clv_summary(days=90)
    clv_records_count = clv_summary.get("total_records", 0)
    if clv_records_count > 0:
        with st.expander(
            f"🎯 CLV Model Validation — {clv_records_count} records (last 90 days)",
            expanded=False,
        ):
            avg_clv = clv_summary.get("avg_clv", 0.0)
            pos_rate = clv_summary.get("positive_clv_rate", 0.0)
            clv_c1, clv_c2, clv_c3 = st.columns(3)
            clv_c1.metric(
                "Avg CLV",
                f"{avg_clv:+.2f}",
                help="Positive = model consistently beat the closing line (real edge). "
                     "Negative = market moved away from model (no edge).",
            )
            clv_c2.metric(
                "Positive CLV Rate",
                f"{pos_rate*100:.1f}%",
                delta="✅ Sharp" if pos_rate > 0.55 else "⚠️ Below sharp threshold",
                delta_color="off",
                help="% of picks where the market moved in our direction (beat close).",
            )
            clv_c3.metric(
                "Total Records",
                clv_records_count,
                help="Number of picks with both opening and closing lines recorded.",
            )

            # Per-stat breakdown
            edge_data = validate_model_edge(days=90)
            clv_by_stat = edge_data.get("clv_by_stat", {})
            if clv_by_stat:
                st.markdown("**CLV by Stat Type:**")
                _clv_rows = []
                for stat, info in sorted(clv_by_stat.items()):
                    cnt = info.get("count", 0)
                    if cnt >= 3:
                        _clv_rows.append({
                            "Stat": stat.capitalize(),
                            "Picks": cnt,
                            "Avg CLV": f"{info.get('avg_clv', 0):+.2f}",
                            "Positive Rate": f"{info.get('positive_clv_rate', 0)*100:.1f}%",
                            "Signal": "✅ Edge" if info.get("avg_clv", 0) > 0 else "❌ No Edge",
                        })
                if _clv_rows:
                    st.dataframe(_clv_rows, hide_index=True, use_container_width=True)

        st.divider()

if not game_logs_by_player:
    st.stop()

# ── Run Backtest ──────────────────────────────────────────────
if run_btn:
    if not selected_stats:
        st.error("Please select at least one stat type.")
        st.stop()

    try:
        with st.spinner("Running backtest simulation…"):
            result = run_backtest(
                season=season,
                stat_types=selected_stats,
                min_edge=min_edge,
                tier_filter=tier_filter_val,
                game_logs_by_player=game_logs_by_player,
            )

        st.session_state["backtest_result"] = result
    except Exception as _bt_err:
        st.error(f"❌ Backtest failed: {_bt_err}")

# ── Display Results ───────────────────────────────────────────
result = st.session_state.get("backtest_result")

if not result:
    st.markdown("### Configure settings and click **▶ Run Backtest** to see results.")
    st.stop()

if result.get("status") == "no_data":
    st.warning(result.get("message", "No data available."))
    st.stop()

st.success(result.get("message", "Backtest complete."))

# ── Top-level Metrics ─────────────────────────────────────────
c1, c2, c3, c4, c5, c6, c7 = st.columns(7)
c1.metric("Total Picks", result["total_picks"])
c2.metric("Wins ✅", result["wins"])
c3.metric("Losses ❌", result["losses"])
c4.metric("Win Rate", f"{result['win_rate']*100:.1f}%",
          delta=f"{(result['win_rate'] - 0.5238)*100:+.1f}% vs breakeven")
c5.metric("ROI", f"{result['roi']*100:.2f}%",
          delta=f"${result['total_pnl']:.2f} P&L")
_sharpe = result.get("sharpe_ratio", 0.0)
c6.metric("Sharpe Ratio", f"{_sharpe:.3f}",
          delta="✅ Good" if _sharpe > 1.0 else ("⚠️ Fair" if _sharpe > 0 else "❌ Bad"),
          delta_color="off")
_dd = result.get("max_drawdown", 0.0)
c7.metric("Max Drawdown", f"{_dd:.2f}u",
          delta="Peak-to-trough units", delta_color="off")

st.divider()

# ── Sharpe / Drawdown / OOS Explainer ────────────────────────
with st.expander("📖 Understanding Sharpe Ratio, Drawdown & Out-of-Sample", expanded=False):
    st.markdown("""
    **Sharpe Ratio** measures return-per-unit-of-risk (consistency).
    - > 2.0 = Excellent (consistent profitable edge)
    - 1.0–2.0 = Good
    - 0–1.0 = Fair (profitable but with variance)
    - < 0 = Strategy is losing money

    **Max Drawdown** is the worst peak-to-trough decline in cumulative units.
    - -5 means the strategy fell 5 units from its best point before recovering.
    - Smaller (less negative) is better.

    **Out-of-Sample (OOS) Split**: The pick log is split 70% in-sample / 30% OOS.
    - If OOS win rate ≈ in-sample win rate, the model generalizes well.
    - If OOS is significantly lower, the model may be overfit to historical data.
    """)

# ── In-Sample vs Out-of-Sample ────────────────────────────────
oos = result.get("oos_metrics", {})
if oos and oos.get("oos_picks", 0) > 0:
    st.subheader("🔬 In-Sample vs Out-of-Sample Validation")
    oos_col1, oos_col2, oos_col3, oos_col4 = st.columns(4)
    oos_col1.metric("In-Sample Picks", oos.get("is_picks", 0))
    oos_col2.metric("In-Sample Win Rate", f"{oos.get('is_win_rate', 0)*100:.1f}%")
    oos_col3.metric("OOS Picks", oos.get("oos_picks", 0))
    _oos_wr  = oos.get("oos_win_rate", 0)
    _is_wr   = oos.get("is_win_rate", 0)
    _wr_gap  = (_oos_wr - _is_wr) * 100
    oos_col4.metric(
        "OOS Win Rate",
        f"{_oos_wr*100:.1f}%",
        delta=f"{_wr_gap:+.1f}% vs in-sample",
        delta_color="normal",
    )
    if abs(_wr_gap) < 3:
        st.success("✅ Model generalizes well — OOS win rate is within 3% of in-sample rate.")
    elif _wr_gap < -5:
        st.warning(
            f"⚠️ OOS win rate is {abs(_wr_gap):.1f}% below in-sample. "
            "The model may be overfit — check if thresholds need adjustment."
        )
    else:
        st.info(f"ℹ️ OOS win rate gap: {_wr_gap:+.1f}%")

st.divider()

# ── Cumulative P&L Chart ──────────────────────────────────────
pick_log = result.get("pick_log", [])
if pick_log:
    st.subheader("📈 Cumulative P&L Curve")
    # Build cumulative P&L series
    _PAYOUT = 0.909  # -110 win payout per unit
    _cumulative = 0.0
    _pnl_series = []
    _dates_seen  = []
    for _p in pick_log:
        _cumulative += _PAYOUT if _p["correct"] else -1.0
        _pnl_series.append(round(_cumulative, 2))
        _dates_seen.append(_p.get("date", ""))

    # Simple ASCII-style chart using Streamlit's built-in line_chart
    # We use st.line_chart with a dict of {pick_index: pnl}
    _chart_data = {
        "Cumulative P&L (units)": _pnl_series,
    }
    st.line_chart(_chart_data, height=260)
    st.caption(
        f"📊 {len(pick_log)} picks · "
        f"Final: {_cumulative:+.2f} units · "
        f"ROI: {result['roi']*100:+.2f}% per pick"
    )

    st.divider()

# ── By Tier ───────────────────────────────────────────────────
with st.expander("📊 Win Rate & ROI by Model Tier", expanded=True):
    tier_data = result.get("tier_win_rates", {})
    if tier_data:
        rows = []
        for tier, d in tier_data.items():
            if d["picks"] > 0:
                _roi_pct = d.get("roi", 0.0) * 100
                _pnl = d.get("pnl", 0.0)
                rows.append({
                    "Tier": tier,
                    "Picks": d["picks"],
                    "Wins": d["wins"],
                    "Losses": d["picks"] - d["wins"],
                    "Win Rate": f"{d['win_rate']*100:.1f}%",
                    "ROI/pick": f"{_roi_pct:+.2f}%",
                    "P&L (units)": f"{_pnl:+.2f}",
                    "Profitable": "✅" if _roi_pct > 0 else "❌",
                })
        if rows:
            st.dataframe(rows, hide_index=True, use_container_width=True)
        else:
            st.caption("No picks met the criteria.")
    else:
        st.caption("No tier data available.")

# ── By Stat Type ─────────────────────────────────────────────
with st.expander("📈 Win Rate by Stat Type", expanded=True):
    stat_data = result.get("stat_win_rates", {})
    if stat_data:
        rows = []
        for stat, d in sorted(stat_data.items(), key=lambda x: -x[1]["wins"]):
            if d["picks"] > 0:
                rows.append({
                    "Stat": stat.capitalize(),
                    "Picks": d["picks"],
                    "Wins": d["wins"],
                    "Losses": d["picks"] - d["wins"],
                    "Win Rate": f"{d['win_rate']*100:.1f}%",
                    "Above 52%": "✅" if d["win_rate"] > 0.52 else "❌",
                })
        if rows:
            st.dataframe(rows, hide_index=True, use_container_width=True)
        else:
            st.caption("No picks met the criteria.")
    else:
        st.caption("No stat data available.")

# ── By Edge Bucket ────────────────────────────────────────────
with st.expander("🎯 Win Rate by Edge Bucket", expanded=True):
    edge_data = result.get("edge_win_rates", {})
    if edge_data:
        rows = []
        for label, d in edge_data.items():
            if d["picks"] > 0:
                rows.append({
                    "Edge Range": label,
                    "Picks": d["picks"],
                    "Wins": d["wins"],
                    "Win Rate": f"{d['win_rate']*100:.1f}%",
                    "Insight": (
                        "✅ Higher edge = higher win rate (healthy)" if d["win_rate"] > 0.55
                        else "⚠️ Expected >55% win rate at this edge level"
                    ),
                })
        if rows:
            st.dataframe(rows, hide_index=True, use_container_width=True)
        else:
            st.caption("No picks in any edge bucket.")

st.divider()

# ── Pick Log ──────────────────────────────────────────────────
with st.expander("📋 Full Pick Log (last 200)", expanded=False):
    if pick_log:
        display = []
        for p in reversed(pick_log):
            display.append({
                "Date": p["date"],
                "Player": p["player"],
                "Stat": p["stat"].capitalize(),
                "Line": p["line"],
                "Actual": p["actual"],
                "Direction": p["direction"],
                "Result": "✅ WIN" if p["correct"] else "❌ LOSS",
                "Prob": f"{p['model_prob']*100:.1f}%",
                "Tier": p["tier"],
                "Edge": f"{p['edge']*100:.1f}%",
            })
        st.dataframe(display, hide_index=True, use_container_width=True)

        # Download button for the pick log
        import json as _json
        _log_json = _json.dumps(pick_log, indent=2)
        st.download_button(
            "⬇️ Download Pick Log (JSON)",
            data=_log_json,
            file_name=f"backtest_pick_log_{result.get('season','')}.json",
            mime="application/json",
        )
    else:
        st.caption("No picks in the log.")
