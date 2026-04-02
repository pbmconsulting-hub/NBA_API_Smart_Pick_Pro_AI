# ============================================================
# FILE: pages/11_📈_Bet_Tracker.py
# PURPOSE: Unified Bet Tracker & Model Health hub combining
#          bet logging, auto-resolve, AI picks, performance
#          predictor, daily history, and per-platform views.
# TABS:
#   1. 📊 Model Health  — Overall metrics + win rate by tier/platform/stat
#   2. 📊 AI Picks      — Auto-logged picks from Neural Analysis
#   3. 📋 All Picks     — Every pick the app outputs (complete performance record)
#   4. 🤖 Auto-Resolve  — Get actual stats and mark results
#   5. 📋 My Bets       — Full bets as styled cards
#   6. ➕ Log a Bet     — Manual / analysis-prefilled bet entry
#   7. 🔮 Predictor     — Forward-looking bankroll / ROI forecasts
#   8. 📅 History       — 2-week rolling day-by-day timeline
# CONNECTS TO: tracking/bet_tracker.py, tracking/database.py
# ============================================================

import datetime
import json
import logging
import os

import streamlit as st

from tracking.bet_tracker import (
    auto_log_analysis_bets,
    auto_resolve_bet_results,
    resolve_all_pending_bets,
    resolve_all_analysis_picks,
    get_model_performance_stats,
    log_new_bet,
    log_props_to_tracker,
    record_bet_result,
)
from tracking.database import (
    initialize_database,
    load_all_bets,
    load_daily_snapshots,
    get_rolling_stats,
    save_daily_snapshot,
    load_all_analysis_picks,
    load_analysis_picks_for_date,
    get_analysis_pick_dates,
)
from styles.theme import (
    get_global_css,
    get_qds_css,
    get_education_box_html,
    get_bet_card_css,
    get_bet_card_html,
    get_summary_cards_html,
    get_styled_stats_table_html,
)

try:
    from engine.clv_tracker import get_clv_summary, get_tier_accuracy_report, validate_model_edge  # F1 enhanced
except ImportError:
    get_clv_summary = None
    get_tier_accuracy_report = None
    validate_model_edge = None

try:
    from engine.calibration import get_calibration_summary, get_isotonic_calibration_curve  # F4 isotonic
except ImportError:
    get_calibration_summary = None
    get_isotonic_calibration_curve = None

# ============================================================
# SECTION: Page Setup
# ============================================================

st.set_page_config(
    page_title="Bet Tracker & Model Health — SmartBetPro NBA",
    page_icon="📈",
    layout="wide",
)

st.markdown(get_global_css(), unsafe_allow_html=True)
st.markdown(get_qds_css(), unsafe_allow_html=True)
st.markdown(get_bet_card_css(), unsafe_allow_html=True)

# ── Joseph M. Smith Floating Widget ───────────────────────────
from utils.components import render_joseph_hero_banner, inject_joseph_floating
render_joseph_hero_banner()
st.session_state["joseph_page_context"] = "page_bet_tracker"
inject_joseph_floating()

# ── Premium Gate ───────────────────────────────────────────────
from utils.premium_gate import premium_gate
if not premium_gate("Bet Tracker"):
    st.stop()
# ── End Premium Gate ───────────────────────────────────────────

# Ensure DB is initialised
initialize_database()

# Auto-resolve past bets AND today's completed bets on page load (best-effort, silent).
# Guarded by a session-state flag so this runs at most once per browser session,
# preventing repeated blocking API calls on every Streamlit rerun.
if not st.session_state.get("_bet_tracker_auto_resolved", False):
    st.session_state["_bet_tracker_auto_resolved"] = True
    with st.spinner("🤖 Auto-resolving pending bets..."):
        try:
            _today_str = datetime.date.today().isoformat()
            _all_bets_check = load_all_bets()

            # Resolve past pending bets (yesterday and older)
            _pending_old = [
                b for b in _all_bets_check
                if not b.get("result") and b.get("bet_date", "") < _today_str
            ]
            if _pending_old:
                _dates_to_resolve = sorted({b.get("bet_date", "") for b in _pending_old if b.get("bet_date")})
                _total_resolved = 0
                for _d in _dates_to_resolve:
                    try:
                        _cnt, _ = auto_resolve_bet_results(date_str=_d)
                        _total_resolved += _cnt
                    except Exception:
                        pass  # One date failed — continue with others
                if _total_resolved > 0:
                    st.toast(f"🤖 Auto-resolved {_total_resolved} past bet(s) on page load.")

            # Silently attempt to resolve today's bets where games are Final
            try:
                from tracking.bet_tracker import resolve_todays_bets
                _today_result = resolve_todays_bets()
                if _today_result.get("resolved", 0) > 0:
                    st.toast(
                        f"⚡ Auto-resolved {_today_result['resolved']} of today's bet(s) "
                        f"({_today_result['wins']}W / {_today_result['losses']}L)."
                    )
            except Exception:
                pass  # Not available or API error — silently skip

            # Auto-update CLV closing lines using Odds API prop lines
            try:
                from engine.clv_tracker import auto_update_closing_lines as _auto_clv
                _clv_result = _auto_clv(days_back=1)
                if _clv_result.get("updated", 0) > 0:
                    st.toast(
                        f"📈 CLV updated: {_clv_result['updated']} record(s) closed "
                        f"with live closing lines."
                    )
            except Exception:
                pass  # Optional — never block page load
        except Exception:
            pass  # Best-effort — never block page load

st.title("📈 Bet Tracker & Model Health")
st.markdown(
    "Your unified hub for tracking model performance, logging bets, "
    "auto-resolving results, and forecasting ROI."
)

with st.expander("📖 How to Use This Page", expanded=False):
    st.markdown("""
    ### Bet Tracker — Logging and Tracking Your Bets
    
    **Logging Bets:**
    - Bets are **auto-logged** when you run Neural Analysis (top picks are saved)
    - You can also manually log bets using the "Add Bet" form
    - Each bet records: player, stat, line, direction (over/under), confidence tier
    
    **Auto-Resolve:**
    - Click "Check Results Now" to automatically resolve today's bets
    - The system retrieves actual game stats and marks bets as Won/Lost
    - Player name matching uses fuzzy logic to handle name variations
    
    **Reading Stats:**
    - **Win Rate**: % of resolved bets that won
    - **ROI**: Return on investment (positive = profitable)
    - **CLV**: Closing Line Value — did you beat the closing line?
    
    💡 **Pro Tips:**
    - Run "Check Results Now" after games finish (usually 11 PM ET)
    - Check the "Model Health" section to see which tiers perform best
    - Use filters to analyze performance by platform or stat type
    """)

# ── Prominent "Check Results Now" button — always visible at top ──────────
_check_col, _check_info_col = st.columns([1, 3])
with _check_col:
    _check_now_btn = st.button(
        "🔄 Check Results Now",
        type="primary",
        width="stretch",
        help="Immediately check live NBA scoreboard for Final games and resolve today's pending bets.",
        key="top_check_results_btn",
    )
with _check_info_col:
    st.caption(
        "Checks the live NBA scoreboard for completed games and instantly resolves today's pending bets. "
        "Click any time — no need to wait until tomorrow."
    )

if _check_now_btn:
    with st.spinner("Checking live NBA games and resolving today's bets…"):
        try:
            from tracking.bet_tracker import resolve_todays_bets as _rtr_top
            _top_result = _rtr_top()
            if _top_result.get("resolved", 0) > 0:
                st.success(
                    f"✅ Resolved **{_top_result['resolved']}** bet(s): "
                    f"**{_top_result['wins']}** WIN · **{_top_result['losses']}** LOSS · **{_top_result['pushes']}** PUSH"
                )
                st.rerun()
            else:
                st.info(
                    f"ℹ️ No bets resolved. Games may still be in progress or not started. "
                    f"Pending: {_top_result.get('pending', 0)}"
                )
            if _top_result.get("errors"):
                st.warning("⚠️ " + " | ".join(_top_result["errors"][:3]))
                if len(_top_result["errors"]) > 3:
                    with st.expander(f"See all {len(_top_result['errors'])} detail(s)"):
                        for _e in _top_result["errors"]:
                            st.markdown(f"- {_e}")
        except Exception as _top_err:
            st.error(f"❌ Could not check results: {_top_err}")

# ============================================================
# SECTION: Top-Level Platform Selector
# ============================================================

st.divider()

platform_filter = st.radio(
    "Filter by Platform",
    ["🏠 All Platforms", "🟢 PrizePicks", "🟣 Underdog Fantasy", "🔵 DraftKings Pick6"],
    horizontal=True,
    label_visibility="collapsed",
)

# Map radio selection to DB platform string(s)
def _platform_filter_fn(bet):
    """Return True if this bet matches the selected platform filter."""
    plat = str(bet.get("platform") or "").lower()
    if platform_filter == "🏠 All Platforms":
        return True
    elif platform_filter == "🟢 PrizePicks":
        return "prizepicks" in plat
    elif platform_filter == "🟣 Underdog Fantasy":
        return "underdog" in plat
    elif platform_filter == "🔵 DraftKings Pick6":
        return "draftkings" in plat or "pick6" in plat or plat == "dk"
    return True

st.divider()

# ============================================================
# END SECTION: Top-Level Platform Selector
# ============================================================

# ============================================================
# SECTION: Tabs
# ============================================================

(
    tab_model_health,
    tab_ai_picks,
    tab_all_picks,
    tab_auto_resolve,
    tab_bets,
    tab_log,
    tab_predict,
    tab_history,
    tab_achievements,
) = st.tabs([
    "📊 Model Health",
    "📊 AI Picks",
    "📋 All Picks",
    "🤖 Auto-Resolve",
    "📋 My Bets",
    "➕ Log a Bet",
    "🔮 Predictor",
    "📅 History",
    "🏆 Achievements",
])

# ============================================================
# END SECTION: Tabs
# ============================================================

# Shared result emoji map
_RESULT_EMOJI = {"WIN": "✅", "LOSS": "❌", "PUSH": "🔄", None: "⏳", "": "⏳"}

# ============================================================
# SECTION: Model Health Tab
# ============================================================

with tab_model_health:
    st.markdown(get_education_box_html(
        "📖 What is Model Health?",
        """
        <strong>Model Calibration</strong>: Are the model's probabilities accurate?
        If we say 70% probability, does the pick hit ~70% of the time?<br><br>
        <strong>Win Rate by Tier</strong>: Platinum picks should win more than Bronze picks.
        If Bronze is winning more than Platinum, the model needs recalibration.<br><br>
        <strong>Why track results?</strong>: The more bets you log, the better we can see
        if the model is making money over time. Track at least 50+ bets to see meaningful
        patterns.<br><br>
        <strong>ROI</strong>: Return on Investment = (profit / total wagered) × 100.
        Positive ROI means you're profitable long-term.
        """
    ), unsafe_allow_html=True)

    all_bets_for_health = load_all_bets()
    filtered_health = [b for b in all_bets_for_health if _platform_filter_fn(b)]
    resolved_health = [b for b in filtered_health if b.get("result") in ("WIN", "LOSS", "PUSH")]
    wins_h   = sum(1 for b in resolved_health if b.get("result") == "WIN")
    losses_h = sum(1 for b in resolved_health if b.get("result") == "LOSS")
    pushes_h = sum(1 for b in resolved_health if b.get("result") == "PUSH")
    total_h  = len(resolved_health)
    win_rate_h = round(wins_h / max(wins_h + losses_h, 1) * 100, 1)
    pending_h = sum(1 for b in filtered_health if not b.get("result"))

    # Rolling stats for streak
    rolling = get_rolling_stats(days=14)
    streak_val = rolling.get("streak", 0)

    # Best platform
    _plat_wins: dict = {}
    for b in resolved_health:
        p = str(b.get("platform") or "Unknown")
        w = 1 if b.get("result") == "WIN" else 0
        l = 1 if b.get("result") == "LOSS" else 0
        if p not in _plat_wins:
            _plat_wins[p] = {"wins": 0, "total": 0}
        _plat_wins[p]["wins"] += w
        _plat_wins[p]["total"] += (w + l)
    best_platform = ""
    if _plat_wins:
        _sorted_plats = sorted(
            [(p, d["wins"] / max(d["total"], 1)) for p, d in _plat_wins.items() if d["total"] >= 3],
            key=lambda x: x[1], reverse=True,
        )
        if _sorted_plats:
            best_platform = _sorted_plats[0][0]

    # Summary cards
    st.markdown(
        get_summary_cards_html(
            total=len(filtered_health),
            wins=wins_h,
            losses=losses_h,
            pushes=pushes_h,
            pending=pending_h,
            win_rate=win_rate_h,
            streak=streak_val,
            best_platform=best_platform,
        ),
        unsafe_allow_html=True,
    )

    if total_h == 0:
        st.info(
            "📝 No resolved bets found. Use **➕ Log a Bet** to start tracking your picks! "
            "After logging bets and recording results you'll see performance stats here."
        )

    if total_h > 0:
        st.divider()

        performance_stats = get_model_performance_stats()

        # Win rate by tier
        with st.expander("🏆 Win Rate by Tier", expanded=True):
            tier_perf = performance_stats.get("by_tier", {})
            if tier_perf:
                tier_order = ["Platinum", "Gold", "Silver", "Bronze"]
                tier_rows = [
                    {
                        "Tier": t,
                        "Total": tier_perf[t].get("total", 0),
                        "Wins":  tier_perf[t].get("wins", 0),
                        "Losses": tier_perf[t].get("losses", 0),
                        "Win Rate": f"{tier_perf[t].get('win_rate', 0):.1f}%",
                    }
                    for t in tier_order if t in tier_perf
                ]
                st.markdown(
                    get_styled_stats_table_html(
                        tier_rows,
                        ["Tier", "Total", "Wins", "Losses", "Win Rate"],
                    ),
                    unsafe_allow_html=True,
                )
                # Tier spotlight metric cards
                _tier_spot_cols = st.columns(4)
                _tier_icons = {"Platinum": "💎", "Gold": "🥇", "Silver": "🥈", "Bronze": "🥉"}
                for _ti, _tn in enumerate(["Platinum", "Gold", "Silver", "Bronze"]):
                    if _tn in tier_perf:
                        _td = tier_perf[_tn]
                        _tier_spot_cols[_ti].metric(
                            f"{_tier_icons[_tn]} {_tn}",
                            f"{_td.get('win_rate', 0):.1f}%",
                            help=f"{_tn}: {_td.get('wins', 0)}W / {_td.get('losses', 0)}L "
                                 f"({_td.get('total', 0)} total bets)",
                        )
            else:
                st.caption("No tier data yet.")

        # Win rate by platform
        with st.expander("🎰 Win Rate by Platform", expanded=True):
            plat_perf = performance_stats.get("by_platform", {})
            if plat_perf:
                plat_rows = [
                    {
                        "Platform": p,
                        "Total": d.get("total", 0),
                        "Wins":  d.get("wins", 0),
                        "Win Rate": f"{d.get('win_rate', 0):.1f}%",
                    }
                    for p, d in plat_perf.items()
                ]
                st.markdown(
                    get_styled_stats_table_html(
                        plat_rows,
                        ["Platform", "Total", "Wins", "Win Rate"],
                    ),
                    unsafe_allow_html=True,
                )
            else:
                st.caption("No platform data yet.")

        # Win rate by stat type
        with st.expander("📐 Win Rate by Stat Type", expanded=False):
            stat_perf = performance_stats.get("by_stat_type", {})
            if stat_perf:
                stat_rows = [
                    {
                        "Stat Type": s.capitalize(),
                        "Total": d.get("total", 0),
                        "Wins":  d.get("wins", 0),
                        "Win Rate": f"{d.get('win_rate', 0):.1f}%",
                    }
                    for s, d in sorted(stat_perf.items())
                ]
                st.markdown(
                    get_styled_stats_table_html(
                        stat_rows,
                        ["Stat Type", "Total", "Wins", "Win Rate"],
                    ),
                    unsafe_allow_html=True,
                )
            else:
                st.caption("No stat type data yet.")

        # Win rate by bet classification
        bet_type_perf = performance_stats.get("by_bet_type", {})
        if bet_type_perf:
            with st.expander("Win Rate by Bet Classification", expanded=True):
                bt_rows = [
                    {
                        "Bet Type":  bt.title(),
                        "Total":     d.get("total", 0),
                        "Wins":      d.get("wins", 0),
                        "Losses":    d.get("losses", 0),
                        "Win Rate":  f"{d.get('win_rate', 0):.1f}%",
                    }
                    for bt, d in sorted(bet_type_perf.items())
                ]
                st.markdown(
                    get_styled_stats_table_html(
                        bt_rows,
                        ["Bet Type", "Total", "Wins", "Losses", "Win Rate"],
                    ),
                    unsafe_allow_html=True,
                )

        # Feature 1: Enhanced tier accuracy report
        try:
            if get_tier_accuracy_report is not None:
                st.subheader("📊 Model Tier Accuracy")
                _tier_report = get_tier_accuracy_report(days=90)
                if _tier_report.get("has_data"):
                    for _tier, _stats in _tier_report.get("by_tier", {}).items():
                        _col_a, _col_b, _col_c = st.columns(3)
                        _col_a.metric(f"{_tier} — Avg CLV", f"{_stats.get('avg_clv', 0):.3f}")
                        _col_b.metric(f"Positive CLV Rate", f"{_stats.get('positive_clv_rate', 0)*100:.1f}%")
                        if _stats.get('win_rate') is not None:
                            _col_c.metric(f"Win Rate", f"{_stats['win_rate']*100:.1f}%")
                        else:
                            _col_c.caption("Win rate: no bet results recorded yet")
                else:
                    st.info("📊 Tier accuracy report will appear after recording bets and closing lines.")
        except Exception as _exc:
            logging.getLogger(__name__).warning(f"[BetTrackerPage] Unexpected error: {_exc}")

        # Feature 4: Isotonic calibration curve
        try:
            if get_isotonic_calibration_curve is not None:
                st.subheader("📈 Isotonic Calibration Curve")
                _iso_curve = get_isotonic_calibration_curve(days=90)
                if _iso_curve.get("has_data"):
                    st.caption(f"Based on {_iso_curve['total_records']} historical predictions | "
                               f"{'Isotonic smoothing applied' if _iso_curve['is_isotonic'] else 'Coarse buckets (need 200+ records for isotonic)'}")
                    for _pt in _iso_curve.get("curve", []):
                        _gap = _pt["actual"] - _pt["predicted"]
                        _indicator = "✅" if abs(_gap) < 0.05 else ("📈" if _gap > 0 else "📉")
                        st.markdown(
                            f"{_indicator} **{_pt['predicted']*100:.0f}%** predicted → "
                            f"**{_pt['actual']*100:.1f}%** actual "
                            f"({'n=' + str(_pt['count'])})"
                        )
                else:
                    st.info("📈 Isotonic calibration curve will appear after recording enough bets (200+ for fine-grained view).")
        except Exception as _exc:
            logging.getLogger(__name__).warning(f"[BetTrackerPage] Unexpected error: {_exc}")

        # CLV Summary — aggregate closing-line-value performance
        try:
            if get_clv_summary is not None:
                st.subheader("📈 Closing Line Value (CLV) Summary")
                _clv_data = get_clv_summary()
                if _clv_data and _clv_data.get("has_data"):
                    _clv_c1, _clv_c2, _clv_c3 = st.columns(3)
                    _clv_c1.metric("Total CLV Records", _clv_data.get("total_records", 0))
                    _avg_clv = _clv_data.get("avg_clv", 0)
                    _clv_c2.metric(
                        "Average CLV",
                        f"{_avg_clv:+.3f}",
                        delta="Beating the market" if _avg_clv > 0 else "Behind the market",
                        delta_color="normal" if _avg_clv > 0 else "inverse",
                    )
                    _pos_rate = _clv_data.get("positive_clv_rate", 0)
                    _clv_c3.metric("Positive CLV Rate", f"{_pos_rate * 100:.1f}%")

                    _clv_by_tier = _clv_data.get("clv_by_tier", {})
                    if _clv_by_tier:
                        st.markdown("**CLV by Tier:**")
                        for _tier_name, _tier_clv in _clv_by_tier.items():
                            _t_avg = _tier_clv.get("avg_clv", 0)
                            _t_pos = _tier_clv.get("positive_clv_rate", 0)
                            _t_n = _tier_clv.get("count", 0)
                            _t_icon = "🟢" if _t_avg > 0 else "🔴"
                            st.markdown(
                                f"{_t_icon} **{_tier_name}**: avg CLV {_t_avg:+.3f} · "
                                f"positive rate {_t_pos * 100:.1f}% · {_t_n} record(s)"
                            )

                    _interpretation = _clv_data.get("interpretation", "")
                    if _interpretation:
                        st.info(f"💡 {_interpretation}")
                else:
                    st.info(
                        "📈 CLV summary will appear after recording bets and tracking closing lines. "
                        "Run **⚡ Neural Analysis** and allow auto-CLV updates to build your CLV history."
                    )
        except Exception as _exc:
            logging.getLogger(__name__).warning(f"[BetTrackerPage] CLV summary error: {_exc}")

# ============================================================
# END SECTION: Model Health Tab
# ============================================================


# ============================================================
# SECTION: AI Picks Tab
# ============================================================

with tab_ai_picks:
    st.subheader("📊 AI Picks — Auto-Logged by SmartAI")
    st.markdown(
        "These bets were automatically logged by the Neural Analysis engine "
        "or the Platform Props & Analyze pipeline."
    )

    all_bets_for_ai = load_all_bets()
    ai_bets_raw = [
        b for b in all_bets_for_ai
        if b.get("platform", "") in ("SmartAI-Auto", "PrizePicks", "Underdog Fantasy", "DraftKings Pick6")
        or str(b.get("notes", "")).startswith("Auto-logged")
        or int(b.get("auto_logged", 0) or 0) == 1
    ]
    ai_bets = [b for b in ai_bets_raw if _platform_filter_fn(b)]

    # ── Tier Filter & Bet Classification Filter ───────────────────────────
    _ai_filter_col1, _ai_filter_col2 = st.columns(2)
    with _ai_filter_col1:
        _ai_tier_filter = st.multiselect(
            "Filter by Tier",
            ["Platinum 💎", "Gold 🥇", "Silver 🥈", "Bronze 🥉"],
            default=[],
            key="ai_tier_filter",
            help="Show only picks matching the selected tiers. Leave empty to show all tiers.",
        )
    with _ai_filter_col2:
        _ai_bet_type_filter = st.multiselect(
            "Bet Classification",
            ["Standard"],
            default=[],
            key="ai_bet_type_filter",
            help="Filter by bet classification. Leave empty to show all.",
        )
    if _ai_tier_filter:
        _ai_tier_names = [t.split(" ")[0] for t in _ai_tier_filter]
        ai_bets = [b for b in ai_bets if b.get("tier") in _ai_tier_names]
    if _ai_bet_type_filter:
        ai_bets = [b for b in ai_bets if b.get("bet_type", "standard") == "standard"]

    if not ai_bets:
        st.info(
            "📭 No AI-auto-logged picks yet. "
            "Run **⚡ Neural Analysis** or click **📊 Get Platform Props & Analyze** on the Live Games page."
        )
    else:
        ai_resolved = [b for b in ai_bets if b.get("result") in ("WIN", "LOSS", "PUSH")]
        ai_wins   = sum(1 for b in ai_resolved if b.get("result") == "WIN")
        ai_losses = sum(1 for b in ai_resolved if b.get("result") == "LOSS")
        ai_pushes = sum(1 for b in ai_resolved if b.get("result") == "PUSH")
        ai_total  = len(ai_resolved)
        ai_rate   = round(ai_wins / max(ai_wins + ai_losses, 1) * 100, 1)

        st.markdown(
            get_summary_cards_html(
                total=len(ai_bets),
                wins=ai_wins,
                losses=ai_losses,
                pushes=ai_pushes,
                pending=sum(1 for b in ai_bets if not b.get("result")),
                win_rate=ai_rate,
            ),
            unsafe_allow_html=True,
        )

        if ai_total > 0:
            st.success(
                f"🎯 **Model accuracy:** Predicted **{ai_wins}/{ai_total}** correctly "
                f"(**{ai_rate:.1f}%**) — {ai_pushes} push(es)"
            )

        st.divider()

        # Daily breakdown with bet cards
        _by_date: dict = {}
        for b in ai_bets:
            _by_date.setdefault(b.get("bet_date", "Unknown"), []).append(b)

        for _date in sorted(_by_date.keys(), reverse=True):
            _day_bets  = _by_date[_date]
            _day_res   = [b for b in _day_bets if b.get("result") in ("WIN", "LOSS", "PUSH")]
            _day_wins  = sum(1 for b in _day_res if b.get("result") == "WIN")
            _day_total = len(_day_res)
            _day_rate  = round(_day_wins / max(_day_wins + (_day_total - _day_wins), 1) * 100, 1) if _day_total > 0 else None

            _day_label = (
                f"📅 {_date} — {len(_day_bets)} picks"
                + (f" · {_day_wins}/{_day_total} correct ({_day_rate:.0f}%)"
                   if _day_rate is not None else " · pending")
            )
            with st.expander(_day_label, expanded=(_date == max(_by_date.keys()))):
                _col_a, _col_b = st.columns(2)
                for _idx, _b in enumerate(_day_bets):
                    _col = _col_a if _idx % 2 == 0 else _col_b
                    with _col:
                        st.markdown(get_bet_card_html(_b), unsafe_allow_html=True)

# ============================================================
# END SECTION: AI Picks Tab
# ============================================================


# ============================================================
# SECTION: All Picks Tab
# ============================================================

with tab_all_picks:
    st.subheader("📋 All Picks — Complete App Output Performance Record")
    st.markdown(
        "Every pick the Neural Analysis engine outputs — not just the AI-auto-logged ones. "
        "Track the **complete** performance record of every prediction the app makes."
    )

    # ── 🔄 Resolve All Picks button ───────────────────────────────────
    _rap_col1, _rap_col2 = st.columns([1, 3])
    with _rap_col1:
        _resolve_all_picks_btn = st.button(
            "🔄 Resolve All Picks",
            key="resolve_all_picks_btn",
            type="primary",
            help="Retrieve actual NBA stats and auto-resolve ALL unresolved picks (manual and AI-logged).",
        )
    with _rap_col2:
        st.caption("Auto-resolves every pending pick using live NBA stats — includes both manual and AI-logged picks.")

    if _resolve_all_picks_btn:
        with st.spinner("🔄 Resolving all pending picks across all dates…"):
            try:
                # ── Step 1: Resolve all_analysis_picks table (what this tab displays) ──
                _picks_result  = resolve_all_analysis_picks(include_today=True)
                # ── Step 2: Also resolve the bets table (manual + AI bets) ──────────
                _bets_result   = resolve_all_pending_bets()

                # Combine both results for reporting
                _rap_resolved  = _picks_result.get("resolved", 0) + _bets_result.get("resolved", 0)
                _rap_wins      = _picks_result.get("wins", 0)    + _bets_result.get("wins", 0)
                _rap_losses    = _picks_result.get("losses", 0)  + _bets_result.get("losses", 0)
                _rap_pushes    = _picks_result.get("pushes", 0)  + _bets_result.get("pushes", 0)
                _rap_pending   = _picks_result.get("pending", 0) + _bets_result.get("pending", 0)
                _rap_errors    = _picks_result.get("errors", []) + _bets_result.get("errors", [])

                # Deduplicate errors (same player may appear in both tables)
                _rap_errors = list(dict.fromkeys(_rap_errors))

                if _rap_resolved > 0:
                    st.success(
                        f"✅ Resolved **{_rap_resolved}** pick(s): "
                        f"✅ {_rap_wins} WIN · ❌ {_rap_losses} LOSS · 🔄 {_rap_pushes} PUSH"
                        + (f" | ⏳ {_rap_pending} still pending (game may not be final)" if _rap_pending > 0 else "")
                    )
                    # Show per-date breakdown
                    _combined_by_date = {}
                    for _d, _n in {**_picks_result.get("by_date", {}), **_bets_result.get("by_date", {})}.items():
                        _combined_by_date[_d] = _combined_by_date.get(_d, 0) + _n
                    if _combined_by_date:
                        for _d in sorted(_combined_by_date):
                            st.caption(f"  📅 {_d}: {_combined_by_date[_d]} resolved")
                elif _rap_errors and not _rap_pending:
                    st.info("ℹ️ No picks were resolved. See errors below.")
                else:
                    st.info(
                        "ℹ️ No pending picks found to resolve. "
                        "Either all picks are already resolved, today's games aren't final yet, "
                        "or no picks have been logged."
                    )
                if _rap_errors:
                    with st.expander(f"⚠️ {len(_rap_errors)} resolution error(s) (click to expand)"):
                        for _e in _rap_errors:
                            st.caption(_e)
            except Exception as _rap_exc:
                st.error(f"❌ Resolution failed: {_rap_exc}")

    st.divider()

    # ── 🗓️ Resolve / Re-check by Date ────────────────────────────────
    st.markdown("### 🗓️ Resolve / Re-check Picks by Date")
    st.caption(
        "Select a past night to view its picks and re-verify results against "
        "actual NBA stats — useful when bets weren't resolved automatically."
    )

    _rbd_available_dates = get_analysis_pick_dates(days=30)

    if not _rbd_available_dates:
        st.info("ℹ️ No pick history found in the last 30 days. Run Neural Analysis to log picks.")
    else:
        _rbd_date_options = _rbd_available_dates  # already sorted newest-first
        _rbd_selected = st.selectbox(
            "📅 Select date",
            options=_rbd_date_options,
            index=0,
            key="rbd_date_selectbox",
            help="Choose a past night to inspect and resolve.",
        )

        # Load all picks for the selected date (pending AND already resolved)
        _rbd_picks = load_analysis_picks_for_date(_rbd_selected)

        if not _rbd_picks:
            st.info(f"No picks logged for {_rbd_selected}.")
        else:
            # Summary line
            _rbd_w = sum(1 for p in _rbd_picks if p.get("result") == "WIN")
            _rbd_l = sum(1 for p in _rbd_picks if p.get("result") == "LOSS")
            _rbd_push = sum(1 for p in _rbd_picks if p.get("result") == "PUSH")
            _rbd_pend = sum(1 for p in _rbd_picks if not p.get("result"))
            _rbd_res = _rbd_w + _rbd_l
            _rbd_wr_str = f" · **{_rbd_w / max(_rbd_res, 1) * 100:.0f}% win rate**" if _rbd_res > 0 else ""
            st.markdown(
                f"**{len(_rbd_picks)} picks** for {_rbd_selected}{_rbd_wr_str}  \n"
                f"✅ {_rbd_w} WIN · ❌ {_rbd_l} LOSS · 🔄 {_rbd_push} PUSH · ⏳ {_rbd_pend} Pending"
            )

            # Quick picks table
            _rbd_rows = []
            for _p in _rbd_picks:
                _res = _p.get("result") or "⏳ Pending"
                _res_icon = {"WIN": "✅ WIN", "LOSS": "❌ LOSS", "PUSH": "🔄 PUSH"}.get(_res, "⏳ Pending")
                _actual = _p.get("actual_value")
                _actual_str = f"{_actual:.1f}" if _actual is not None else "—"
                _rbd_rows.append({
                    "Player": _p.get("player_name", "—"),
                    "Stat": _p.get("stat_type", "—"),
                    "Line": _p.get("prop_line", "—"),
                    "Dir": _p.get("direction", "—"),
                    "Actual": _actual_str,
                    "Result": _res_icon,
                    "Tier": _p.get("tier", "—"),
                })
            st.dataframe(
                _rbd_rows,
                use_container_width=True,
                hide_index=True,
                column_config={
                    "Player": st.column_config.TextColumn("Player", width="medium"),
                    "Stat":   st.column_config.TextColumn("Stat",   width="small"),
                    "Line":   st.column_config.NumberColumn("Line",  format="%.1f"),
                    "Dir":    st.column_config.TextColumn("Dir",    width="small"),
                    "Actual": st.column_config.TextColumn("Actual", width="small"),
                    "Result": st.column_config.TextColumn("Result", width="small"),
                    "Tier":   st.column_config.TextColumn("Tier",   width="small"),
                },
            )

            # Resolve / Re-check button for this date
            _rbd_resolve_btn = st.button(
                f"🔄 Resolve / Re-check {_rbd_selected}",
                key="rbd_resolve_btn",
                type="primary",
                help=f"Retrieve actual NBA stats for {_rbd_selected} and update WIN/LOSS/PUSH for all picks on that night.",
            )

            if _rbd_resolve_btn:
                with st.spinner(f"Retrieving NBA stats and resolving picks for {_rbd_selected}…"):
                    try:
                        # Resolve all_analysis_picks for this date (re-checks even resolved rows)
                        _rbd_picks_res  = resolve_all_analysis_picks(date_str=_rbd_selected)
                        # Also resolve the bets table for this date
                        _rbd_bets_cnt, _rbd_bets_errs = auto_resolve_bet_results(date_str=_rbd_selected)

                        _rbd_total = _rbd_picks_res.get("resolved", 0) + _rbd_bets_cnt
                        _rbd_wins  = _rbd_picks_res.get("wins", 0)
                        _rbd_loss  = _rbd_picks_res.get("losses", 0)
                        _rbd_psh   = _rbd_picks_res.get("pushes", 0)
                        _rbd_errs  = _rbd_picks_res.get("errors", []) + _rbd_bets_errs
                        _rbd_errs  = list(dict.fromkeys(_rbd_errs))

                        if _rbd_total > 0:
                            st.success(
                                f"✅ Resolved **{_rbd_total}** pick(s) for {_rbd_selected}: "
                                f"✅ {_rbd_wins} WIN · ❌ {_rbd_loss} LOSS · 🔄 {_rbd_psh} PUSH"
                            )
                            st.rerun()
                        else:
                            st.warning(
                                f"⚠️ No picks were resolved for {_rbd_selected}. "
                                "The game log may not yet be available, or no matching players were found."
                            )
                        if _rbd_errs:
                            with st.expander(f"⚠️ {len(_rbd_errs)} error(s)"):
                                for _e in _rbd_errs:
                                    st.caption(_e)
                    except Exception as _rbd_exc:
                        st.error(f"❌ Resolution failed: {_rbd_exc}")

    st.divider()

    # ── Session-state picks (most recent run) ─────────────────────────
    session_picks = st.session_state.get("analysis_results", [])
    # ── Historical picks from DB (persistent) ────────────────────────
    db_all_picks = load_all_analysis_picks(days=30)

    # ── Build combined dataset for aggregate metrics ───────────────────
    # Deduplicate by (player_name, stat_type, prop_line, direction, pick_date)
    _seen_keys: set = set()
    _combined_picks: list = []
    for _cp in db_all_picks:
        _key = (
            _cp.get("player_name", ""),
            _cp.get("stat_type", ""),
            str(_cp.get("prop_line") or _cp.get("line", "")),
            _cp.get("direction", ""),
            _cp.get("pick_date", ""),
        )
        if _key not in _seen_keys:
            _seen_keys.add(_key)
            _combined_picks.append(_cp)
    for _cp in session_picks:
        _key = (
            _cp.get("player_name", ""),
            _cp.get("stat_type", ""),
            str(_cp.get("prop_line") or _cp.get("line", "")),
            _cp.get("direction", ""),
            _cp.get("pick_date", ""),
        )
        if _key not in _seen_keys:
            _seen_keys.add(_key)
            _combined_picks.append(_cp)

    # ── Tier Filter & Bet Classification Filter ───────────────────────────
    _ap_filter_col1, _ap_filter_col2 = st.columns(2)
    with _ap_filter_col1:
        _ap_tier_filter = st.multiselect(
            "Filter by Tier",
            ["Platinum 💎", "Gold 🥇", "Silver 🥈", "Bronze 🥉"],
            default=[],
            key="ap_tier_filter",
            help="Show only picks matching the selected tiers. Leave empty to show all tiers.",
        )
    with _ap_filter_col2:
        _ap_bet_type_filter = st.multiselect(
            "Bet Classification",
            ["Standard"],
            default=[],
            key="ap_bet_type_filter",
            help="Filter by bet classification. Leave empty to show all.",
        )

    # Apply filters to the combined dataset used for aggregate metrics
    all_picks_data = list(_combined_picks)
    if _ap_tier_filter:
        _ap_tier_names = [t.split(" ")[0] for t in _ap_tier_filter]
        all_picks_data = [p for p in all_picks_data if p.get("tier") in _ap_tier_names]
    if _ap_bet_type_filter:
        all_picks_data = [p for p in all_picks_data if p.get("bet_type", "standard") == "standard"]

    if not all_picks_data:
        st.info(
            "📭 No picks to display. Run **Neural Analysis** to generate picks — "
            "all outputs are automatically stored here for tracking."
        )
    else:
        # ── Compute summary stats ──────────────────────────────────────
        _ap_total   = len(all_picks_data)
        _ap_wins    = sum(1 for p in all_picks_data if p.get("result") == "WIN")
        _ap_losses  = sum(1 for p in all_picks_data if p.get("result") == "LOSS")
        _ap_pushes  = sum(1 for p in all_picks_data if p.get("result") == "PUSH")
        _ap_pending = sum(1 for p in all_picks_data if not p.get("result"))
        _ap_resolved = _ap_wins + _ap_losses
        _ap_win_rate = round(_ap_wins / max(_ap_resolved, 1) * 100, 1) if _ap_resolved > 0 else 0.0
        _ap_avg_edge = (
            sum(abs(float(p.get("edge_percentage", 0) or 0)) for p in all_picks_data) / _ap_total
            if _ap_total > 0 else 0.0
        )
        _ap_avg_conf = (
            sum(float(p.get("confidence_score", 0) or 0) for p in all_picks_data) / _ap_total
            if _ap_total > 0 else 0.0
        )

        # Streak: walk most-recent resolved picks to find consecutive W/L run
        _ap_streak = 0
        _ap_resolved_sorted = sorted(
            [p for p in all_picks_data if p.get("result") in ("WIN", "LOSS")],
            key=lambda p: p.get("pick_date", ""),
            reverse=True,
        )
        if _ap_resolved_sorted:
            _ap_streak_result = _ap_resolved_sorted[0].get("result")
            _ap_streak = 1 if _ap_streak_result == "WIN" else -1
            for _ap_sp in _ap_resolved_sorted[1:]:
                if _ap_sp.get("result") == _ap_streak_result:
                    _ap_streak += 1 if _ap_streak_result == "WIN" else -1
                else:
                    break

        # Best platform by win rate (min 3 resolved picks)
        _ap_plat_perf: dict = {}
        for _p in all_picks_data:
            _plat = str(_p.get("platform") or "Unknown")
            _res  = _p.get("result")
            if _plat not in _ap_plat_perf:
                _ap_plat_perf[_plat] = {"wins": 0, "total": 0}
            if _res in ("WIN", "LOSS"):
                _ap_plat_perf[_plat]["total"] += 1
                if _res == "WIN":
                    _ap_plat_perf[_plat]["wins"] += 1
        _ap_best_platform = ""
        _ap_sorted_plats = sorted(
            [(p, d["wins"] / max(d["total"], 1)) for p, d in _ap_plat_perf.items() if d["total"] >= 3],
            key=lambda x: x[1], reverse=True,
        )
        if _ap_sorted_plats:
            _ap_best_platform = _ap_sorted_plats[0][0]

        # ── Summary Cards (same style as AI Picks / Model Health) ─────
        st.markdown(
            get_summary_cards_html(
                total=_ap_total,
                wins=_ap_wins,
                losses=_ap_losses,
                pushes=_ap_pushes,
                pending=_ap_pending,
                win_rate=_ap_win_rate,
                streak=_ap_streak,
                best_platform=_ap_best_platform,
            ),
            unsafe_allow_html=True,
        )

        # Model accuracy banner
        if _ap_resolved > 0:
            st.success(
                f"🎯 **Model accuracy:** Predicted **{_ap_wins}/{_ap_resolved}** correctly "
                f"(**{_ap_win_rate:.1f}%**) — {_ap_pushes} push(es)"
            )

        # Secondary pick-quality metrics (Avg Edge + Avg Confidence + direction counts)
        _apc1, _apc2, _apc3, _apc4 = st.columns(4)
        _apc1.metric("⬆️ OVER", sum(1 for p in all_picks_data if p.get("direction") == "OVER"))
        _apc2.metric("⬇️ UNDER", sum(1 for p in all_picks_data if p.get("direction") == "UNDER"))
        _apc3.metric("Avg Edge", f"{_ap_avg_edge:.1f}%")
        _apc4.metric("Avg Confidence", f"{_ap_avg_conf:.0f}/100")

        st.divider()

        # ── Win Rate by Tier ──────────────────────────────────────────
        with st.expander("🏆 Win Rate by Tier", expanded=True):
            _tier_rows = []
            for _tn in ["Platinum", "Gold", "Silver", "Bronze"]:
                _t_picks = [p for p in all_picks_data if p.get("tier") == _tn]
                if not _t_picks:
                    continue
                _t_w = sum(1 for p in _t_picks if p.get("result") == "WIN")
                _t_l = sum(1 for p in _t_picks if p.get("result") == "LOSS")
                _t_res = _t_w + _t_l
                _tier_rows.append({
                    "Tier": _tn,
                    "Total": len(_t_picks),
                    "Wins": _t_w,
                    "Losses": _t_l,
                    "Win Rate": f"{_t_w / max(_t_res, 1) * 100:.1f}%" if _t_res > 0 else "—",
                })
            if _tier_rows:
                st.markdown(
                    get_styled_stats_table_html(
                        _tier_rows,
                        ["Tier", "Total", "Wins", "Losses", "Win Rate"],
                    ),
                    unsafe_allow_html=True,
                )
                _tier_spot_cols = st.columns(4)
                _tier_icons = {"Platinum": "💎", "Gold": "🥇", "Silver": "🥈", "Bronze": "🥉"}
                for _ti, _tn in enumerate(["Platinum", "Gold", "Silver", "Bronze"]):
                    _tr = next((r for r in _tier_rows if r["Tier"] == _tn), None)
                    if _tr:
                        _tier_spot_cols[_ti].metric(
                            f"{_tier_icons.get(_tn, '')} {_tn}",
                            _tr["Win Rate"],
                            help=f"{_tn}: {_tr['Wins']}W / {_tr['Losses']}L ({_tr['Total']} total picks)",
                        )
            else:
                st.caption("No tier data yet.")

        # ── Win Rate by Platform ──────────────────────────────────────
        _ap_plat_data: dict = {}
        for _p in all_picks_data:
            _plat = str(_p.get("platform") or "Unknown")
            _res  = _p.get("result")
            if _plat not in _ap_plat_data:
                _ap_plat_data[_plat] = {"wins": 0, "losses": 0, "total": 0}
            _ap_plat_data[_plat]["total"] += 1
            if _res == "WIN":
                _ap_plat_data[_plat]["wins"] += 1
            elif _res == "LOSS":
                _ap_plat_data[_plat]["losses"] += 1
        if _ap_plat_data:
            with st.expander("🎰 Win Rate by Platform", expanded=True):
                _plat_rows = [
                    {
                        "Platform": _plat,
                        "Total":    d["total"],
                        "Wins":     d["wins"],
                        "Win Rate": (
                            f"{d['wins'] / max(d['wins'] + d['losses'], 1) * 100:.1f}%"
                            if d["wins"] + d["losses"] > 0 else "—"
                        ),
                    }
                    for _plat, d in sorted(_ap_plat_data.items())
                ]
                st.markdown(
                    get_styled_stats_table_html(
                        _plat_rows,
                        ["Platform", "Total", "Wins", "Win Rate"],
                    ),
                    unsafe_allow_html=True,
                )
        else:
            with st.expander("🎰 Win Rate by Platform", expanded=True):
                st.caption("No platform data yet.")

        # ── Win Rate by Stat Type ──────────────────────────────────────
        with st.expander("📐 Win Rate by Stat Type", expanded=False):
            _stat_rows = []
            for _stype in sorted({p.get("stat_type", "unknown") for p in all_picks_data}):
                _s_picks = [p for p in all_picks_data if p.get("stat_type") == _stype]
                _s_w = sum(1 for p in _s_picks if p.get("result") == "WIN")
                _s_res = sum(1 for p in _s_picks if p.get("result") in ("WIN", "LOSS"))
                _stat_rows.append({
                    "Stat Type": _stype.replace("_", " ").title(),
                    "Total": len(_s_picks),
                    "Wins": _s_w,
                    "Win Rate": f"{_s_w / max(_s_res, 1) * 100:.1f}%" if _s_res > 0 else "—",
                })
            if _stat_rows:
                st.markdown(
                    get_styled_stats_table_html(
                        _stat_rows,
                        ["Stat Type", "Total", "Wins", "Win Rate"],
                    ),
                    unsafe_allow_html=True,
                )
            else:
                st.caption("No stat type data yet.")

        # ── Win Rate by Bet Classification ────────────────────────────
        _ap_bt_data: dict = {}
        for _p in all_picks_data:
            _bt = str(_p.get("bet_type") or "standard")
            _res = _p.get("result")
            if _bt not in _ap_bt_data:
                _ap_bt_data[_bt] = {"wins": 0, "losses": 0, "total": 0}
            _ap_bt_data[_bt]["total"] += 1
            if _res == "WIN":
                _ap_bt_data[_bt]["wins"] += 1
            elif _res == "LOSS":
                _ap_bt_data[_bt]["losses"] += 1
        if _ap_bt_data:
            with st.expander("Win Rate by Bet Classification", expanded=True):
                _bt_rows = [
                    {
                        "Bet Type": _bt.title(),
                        "Total": d["total"],
                        "Wins": d["wins"],
                        "Losses": d["losses"],
                        "Win Rate": (
                            f"{d['wins'] / max(d['wins'] + d['losses'], 1) * 100:.1f}%"
                            if d["wins"] + d["losses"] > 0 else "—"
                        ),
                    }
                    for _bt, d in sorted(_ap_bt_data.items())
                ]
                st.markdown(
                    get_styled_stats_table_html(
                        _bt_rows,
                        ["Bet Type", "Total", "Wins", "Losses", "Win Rate"],
                    ),
                    unsafe_allow_html=True,
                )

        # ── Model Tier Accuracy placeholder ──────────────────────────
        with st.expander("📊 Model Tier Accuracy", expanded=False):
            st.info(
                "Coming soon — this section will compare predicted tier accuracy "
                "vs actual results over time."
            )

        st.divider()

        # ── Source toggle (controls per-day detail section only) ──────
        ap_source_radio = st.radio(
            "Show detailed picks:",
            ["30-Day History (database)", "Today's Analysis (live session)"],
            horizontal=True,
            key="ap_source_radio",
        )

        # ── Per-day sections ──────────────────────────────────────────
        if ap_source_radio == "30-Day History (database)":
            # Group DB picks by date
            _detail_picks = db_all_picks
            if _ap_tier_filter:
                _detail_picks = [p for p in _detail_picks if p.get("tier") in _ap_tier_names]
            if _ap_bet_type_filter:
                _detail_picks = [p for p in _detail_picks if p.get("bet_type", "normal") in _ap_bt_values]
            if _detail_picks:
                _by_date_ap: dict = {}
                for _p in _detail_picks:
                    _d = _p.get("pick_date", "Unknown")
                    _by_date_ap.setdefault(_d, []).append(_p)
                for _ap_date in sorted(_by_date_ap.keys(), reverse=True):
                    _day_data = _by_date_ap[_ap_date]
                    _d_w = sum(1 for p in _day_data if p.get("result") == "WIN")
                    _d_l = sum(1 for p in _day_data if p.get("result") == "LOSS")
                    _d_res = _d_w + _d_l
                    _d_wr = f" — {_d_w / max(_d_res,1)*100:.0f}% win rate" if _d_res > 0 else ""
                    with st.expander(
                        f"📅 {_ap_date} · {len(_day_data)} picks · ✅{_d_w} ❌{_d_l}{_d_wr}"
                    ):
                        _ca, _cb = st.columns(2)
                        for _idx, _pick in enumerate(_day_data):
                            # Remap pick_date → bet_date so get_bet_card_html renders date correctly
                            _pick_card = dict(_pick)
                            if "bet_date" not in _pick_card:
                                _pick_card["bet_date"] = _pick_card.get("pick_date", "")
                            _col = _ca if _idx % 2 == 0 else _cb
                            with _col:
                                st.markdown(
                                    get_bet_card_html(_pick_card),
                                    unsafe_allow_html=True,
                                )
            else:
                st.info("📭 No database picks found for this time range.")
        else:
            # Session picks: show all in two-column grid
            _session_detail = list(session_picks)
            if _ap_tier_filter:
                _session_detail = [p for p in _session_detail if p.get("tier") in _ap_tier_names]
            if _ap_bet_type_filter:
                _session_detail = [p for p in _session_detail if p.get("bet_type", "normal") in _ap_bt_values]
            if _session_detail:
                _col_a, _col_b = st.columns(2)
                for _idx, _pick in enumerate(_session_detail):
                    _col = _col_a if _idx % 2 == 0 else _col_b
                    with _col:
                        st.markdown(get_bet_card_html(_pick), unsafe_allow_html=True)
            else:
                st.info(
                    "📭 No live session picks to display. Run **Neural Analysis** to generate picks."
                )

# ============================================================
# END SECTION: All Picks Tab
# ============================================================


# ============================================================
# SECTION: Auto-Resolve Tab
# ============================================================

with tab_auto_resolve:
    st.subheader("🤖 Auto-Resolve — Get Actual Stats & Mark Results")
    st.markdown(
        "Automatically retrieve actual player stats and mark pending bets "
        "as WIN / LOSS / PUSH. Use **⚡ Resolve Now** to resolve today's completed games instantly."
    )

    # ── Resolve Now (today's bets) ────────────────────────────────────
    st.markdown("### ⚡ Resolve Today's Bets")
    st.caption("Checks live scores for Final games and resolves today's pending bets immediately.")

    resolve_today_btn = st.button(
        "⚡ Resolve Now",
        type="primary",
        help="Get live game status and resolve today's bets where games are Final",
    )

    if resolve_today_btn:
        with st.spinner("Checking today's games and resolving completed bets…"):
            try:
                from tracking.bet_tracker import resolve_todays_bets
                _result = resolve_todays_bets()
                if _result["resolved"] > 0:
                    st.success(
                        f"✅ Resolved **{_result['resolved']}** bet(s) today: "
                        f"**{_result['wins']}** WIN · **{_result['losses']}** LOSS · **{_result['pushes']}** PUSH"
                    )
                    st.rerun()
                else:
                    st.info(
                        f"ℹ️ No bets resolved. "
                        f"Games may still be in progress or not started. "
                        f"Still pending: {_result.get('pending', 0)}"
                    )
                if _result.get("errors"):
                    st.warning("⚠️ " + " | ".join(_result["errors"][:3]))
                    if len(_result["errors"]) > 3:
                        with st.expander(f"See all {len(_result['errors'])} issue(s)"):
                            for err in _result["errors"]:
                                st.markdown(f"- {err}")
            except Exception as _err:
                st.error(f"❌ Resolve failed: {_err}")

    st.divider()

    # ── Live Status Section ────────────────────────────────────────────
    st.markdown("### 🔄 Live Bet Status — Today's Picks")
    _today_bets_all = load_all_bets()
    _today_str_ar = datetime.date.today().isoformat()
    _today_bets = [
        b for b in _today_bets_all
        if b.get("bet_date") == _today_str_ar and _platform_filter_fn(b)
    ]
    _today_pending = [b for b in _today_bets if not b.get("result")]

    auto_refresh = st.checkbox("🔁 Auto-refresh live status (every 60s)", value=False)

    if _today_bets:
        if _today_pending:
            try:
                from tracking.bet_tracker import get_live_bet_status
                _live_bets = get_live_bet_status(_today_pending)
            except Exception:
                _live_bets = _today_pending

            _col_a, _col_b = st.columns(2)
            for _idx, _lb in enumerate(_live_bets):
                _col = _col_a if _idx % 2 == 0 else _col_b
                with _col:
                    st.markdown(get_bet_card_html(_lb, show_live_status=True), unsafe_allow_html=True)

            # Already resolved today
            _today_resolved = [b for b in _today_bets if b.get("result")]
            if _today_resolved:
                st.markdown("**✅ Already Resolved Today:**")
                _cr_a, _cr_b = st.columns(2)
                for _idx, _rb in enumerate(_today_resolved):
                    _col = _cr_a if _idx % 2 == 0 else _cr_b
                    with _col:
                        st.markdown(get_bet_card_html(_rb), unsafe_allow_html=True)
        else:
            st.markdown("**Today's Bets (All Resolved):**")
            _cr_a, _cr_b = st.columns(2)
            for _idx, _rb in enumerate(_today_bets):
                _col = _cr_a if _idx % 2 == 0 else _cr_b
                with _col:
                    st.markdown(get_bet_card_html(_rb), unsafe_allow_html=True)
    else:
        st.info("No bets logged for today yet.")

    if auto_refresh:
        import time as _time
        _time.sleep(60)
        # Attempt to resolve any new Final games before refreshing the display
        try:
            from tracking.bet_tracker import resolve_todays_bets as _rtr
            _rtr_result = _rtr()
            if _rtr_result.get("resolved", 0) > 0:
                st.toast(
                    f"🔄 Auto-resolved {_rtr_result['resolved']} bet(s) "
                    f"({_rtr_result['wins']}W / {_rtr_result['losses']}L)"
                )
            if _rtr_result.get("errors"):
                for _rtr_err in _rtr_result["errors"]:
                    logging.getLogger(__name__).warning(f"[BetTrackerPage] resolve: {_rtr_err}")
        except Exception as _exc:
            logging.getLogger(__name__).warning(f"[BetTrackerPage] Unexpected error: {_exc}")
        st.rerun()

    st.divider()

    # ── Resolve Past Bets ─────────────────────────────────────────────
    st.markdown("### 🗓️ Resolve Past Bets")
    all_bets_for_resolve = load_all_bets()
    pending_all = [
        b for b in all_bets_for_resolve
        if not b.get("result") and b.get("bet_date", "") < _today_str_ar and _platform_filter_fn(b)
    ]

    if not pending_all:
        st.info("✅ No past pending bets found.")
    else:
        st.markdown(f"**{len(pending_all)} past pending bet(s):**")
        _pa, _pb = st.columns(2)
        for _idx, _pb_bet in enumerate(pending_all):
            _col = _pa if _idx % 2 == 0 else _pb
            with _col:
                st.markdown(get_bet_card_html(_pb_bet), unsafe_allow_html=True)

    col_date, col_btn = st.columns([2, 1])
    with col_date:
        yesterday_dt = datetime.date.today() - datetime.timedelta(days=1)
        resolve_date = st.date_input(
            "Date to resolve",
            value=yesterday_dt,
            help="Bets with this date that still have no result will be auto-resolved.",
        )
    with col_btn:
        st.markdown("<br>", unsafe_allow_html=True)
        resolve_btn = st.button(
            "🔄 Get Actual Stats & Auto-Resolve",
            type="primary",
            width="stretch",
        )

    if resolve_btn:
        with st.spinner("Retrieving actual stats…"):
            resolved, errors = auto_resolve_bet_results(date_str=resolve_date.isoformat())

        if resolved > 0:
            try:
                save_daily_snapshot(resolve_date.isoformat())
            except Exception as _exc:
                logging.getLogger(__name__).warning(f"[BetTrackerPage] Unexpected error: {_exc}")
            st.success(f"✅ Auto-resolved **{resolved}** bet(s) for {resolve_date.isoformat()}.")
            st.rerun()
        else:
            st.warning(f"⚠️ No bets resolved for {resolve_date.isoformat()}.")

        if errors:
            with st.expander(f"⚠️ {len(errors)} error(s) during auto-resolve"):
                for err in errors:
                    st.markdown(f"- {err}")

    st.divider()
    st.markdown("### 🔄 Resolve All Pending Bets")
    st.markdown(
        "Resolves **every** unresolved bet in your tracker — manual bets, AI picks, "
        "and bets from any platform or date."
    )
    resolve_all_btn = st.button(
        "🔄 Resolve All Pending Bets",
        type="primary",
        help="Retrieve actual NBA stats for every pending bet regardless of date.",
    )

    if resolve_all_btn:
        try:
            with st.spinner("Resolving all pending bets — this may take a moment…"):
                _all_result = resolve_all_pending_bets()

            _all_resolved = _all_result.get("resolved", 0)
            _all_errors   = _all_result.get("errors", [])
            _by_date      = _all_result.get("by_date", {})
            if _all_resolved > 0:
                st.success(
                    f"✅ Resolved **{_all_resolved}** bet(s) — "
                    f"{_all_result.get('wins', 0)} W / "
                    f"{_all_result.get('losses', 0)} L / "
                    f"{_all_result.get('pushes', 0)} Push"
                )
                if _by_date:
                    for _d, _cnt in sorted(_by_date.items()):
                        st.markdown(f"  • **{_d}**: {_cnt} resolved")
                st.rerun()
            else:
                st.info("No pending bets found to resolve, or all bets are already resolved.")

            if _all_errors:
                st.warning("⚠️ " + " | ".join(_all_errors[:3]))
                if len(_all_errors) > 3:
                    with st.expander(f"See all {len(_all_errors)} error(s) during resolve-all"):
                        for err in _all_errors:
                            st.markdown(f"- {err}")
        except Exception as _resolve_all_err:
            _resolve_err_str = str(_resolve_all_err)
            if "WebSocketClosedError" not in _resolve_err_str and "StreamClosedError" not in _resolve_err_str:
                st.error(f"❌ Resolve all failed: {_resolve_all_err}")

# ============================================================
# END SECTION: Auto-Resolve Tab
# ============================================================


# ============================================================
# SECTION: My Bets Tab (Styled Cards)
# ============================================================

with tab_bets:
    st.subheader("📋 My Bets")

    all_bets_raw = load_all_bets()
    all_bets = [b for b in all_bets_raw if _platform_filter_fn(b)]

    if not all_bets:
        st.info("No bets logged yet. Use **➕ Log a Bet** to add your first bet.")
    else:
        # ── Filter Pills ──────────────────────────────────────────────
        filter_choice = st.radio(
            "Filter bets",
            ["All", "Wins Only", "Losses Only", "Pending", "Platinum", "Gold", "Silver", "Bronze"],
            horizontal=True,
            label_visibility="collapsed",
        )

        # ── Bet Classification Filter ─────────────────────────────────
        _bets_bet_type_filter = st.multiselect(
            "Bet Classification",
            ["Standard"],
            default=[],
            key="bets_bet_type_filter",
            help="Filter by bet classification. Leave empty to show all.",
        )

        def _apply_filter(bets, choice):
            if choice == "Wins Only":
                return [b for b in bets if b.get("result") == "WIN"]
            elif choice == "Losses Only":
                return [b for b in bets if b.get("result") == "LOSS"]
            elif choice == "Pending":
                return [b for b in bets if not b.get("result")]
            elif choice in ("Platinum", "Gold", "Silver", "Bronze"):
                return [b for b in bets if b.get("tier", "") == choice]
            return bets

        filtered_bets = _apply_filter(all_bets, filter_choice)
        if _bets_bet_type_filter:
            filtered_bets = [b for b in filtered_bets if b.get("bet_type", "standard") == "standard"]

        # ── Summary Cards ─────────────────────────────────────────────
        _res_bets = [b for b in all_bets if b.get("result") in ("WIN", "LOSS", "PUSH")]
        _w = sum(1 for b in _res_bets if b.get("result") == "WIN")
        _l = sum(1 for b in _res_bets if b.get("result") == "LOSS")
        _p = sum(1 for b in _res_bets if b.get("result") == "PUSH")
        _wr = round(_w / max(_w + _l, 1) * 100, 1)
        _pend = sum(1 for b in all_bets if not b.get("result"))
        st.markdown(
            get_summary_cards_html(
                total=len(all_bets), wins=_w, losses=_l, pushes=_p,
                pending=_pend, win_rate=_wr,
            ),
            unsafe_allow_html=True,
        )

        st.markdown(f"**Showing {len(filtered_bets)} bet(s)** (filter: {filter_choice})")
        st.divider()

        # ── Mark a Result ─────────────────────────────────────────────
        pending_bets = [b for b in filtered_bets if not b.get("result")]
        if pending_bets:
            with st.expander("✅ Mark a Result", expanded=False):
                pending_labels = {
                    b.get("id", b.get("bet_id", idx)): (
                        f"#{b.get('id', b.get('bet_id', idx))} — {b.get('player_name', '?')} "
                        f"{b.get('direction', '')} {b.get('prop_line', '')} {str(b.get('stat_type', '')).title()} "
                        f"({b.get('platform', '?')}, {b.get('bet_date', '')})"
                    )
                    for idx, b in enumerate(pending_bets)
                }
                with st.form("mark_result_form_bt"):
                    selected_bet_id = st.selectbox(
                        "Select Pending Bet",
                        options=list(pending_labels.keys()),
                        format_func=lambda x: pending_labels[x],
                    )
                    result_choice = st.radio("Result", ["WIN", "LOSS", "PUSH"], horizontal=True)
                    actual_value  = st.number_input(
                        "Actual Stat Value",
                        min_value=0.0, max_value=200.0, value=0.0, step=0.5,
                    )
                    submit_result = st.form_submit_button("💾 Save Result", type="primary")

                if submit_result:
                    ok, msg = record_bet_result(selected_bet_id, result_choice, actual_value)
                    if ok:
                        st.success(f"✅ {msg}")
                        st.rerun()
                    else:
                        st.error(f"❌ {msg}")

        # ── Bet Cards Grid — grouped by date ─────────────────────────
        if not filtered_bets:
            st.info(f"No bets match the '{filter_choice}' filter.")
        else:
            _today_str = datetime.date.today().isoformat()
            _today_bets = [b for b in filtered_bets if b.get("bet_date", "") == _today_str]
            _past_bets_by_date: dict = {}
            for _b in filtered_bets:
                _bd = _b.get("bet_date", "Unknown")
                if _bd != _today_str:
                    _past_bets_by_date.setdefault(_bd, []).append(_b)

            # ── Today's Bets ─────────────────────────────────────────
            if _today_bets:
                st.subheader("📅 Today's Bets")
                col_a, col_b = st.columns(2)
                for idx, bet in enumerate(_today_bets):
                    col = col_a if idx % 2 == 0 else col_b
                    with col:
                        st.markdown(get_bet_card_html(bet), unsafe_allow_html=True)

            # ── Previous Days — collapsible expanders ────────────────
            for _past_date in sorted(_past_bets_by_date.keys(), reverse=True):
                _pd_bets = _past_bets_by_date[_past_date]
                _pd_w    = sum(1 for b in _pd_bets if b.get("result") == "WIN")
                _pd_l    = sum(1 for b in _pd_bets if b.get("result") == "LOSS")
                _pd_pend = sum(1 for b in _pd_bets if not b.get("result"))
                _pd_label = (
                    f"📅 {_past_date} — {len(_pd_bets)} bet(s) · ✅{_pd_w} ❌{_pd_l}"
                    + (f" ⏳{_pd_pend} pending" if _pd_pend else "")
                )
                with st.expander(_pd_label, expanded=False):
                    col_a, col_b = st.columns(2)
                    for idx, bet in enumerate(_pd_bets):
                        col = col_a if idx % 2 == 0 else col_b
                        with col:
                            st.markdown(get_bet_card_html(bet), unsafe_allow_html=True)

            if not _today_bets and not _past_bets_by_date:
                st.info(f"No bets match the '{filter_choice}' filter.")

# ============================================================
# END SECTION: My Bets Tab
# ============================================================


# ============================================================
# SECTION: Log a Bet Tab
# ============================================================

with tab_log:
    st.subheader("➕ Log a New Bet")

    # ── Bulk-add platform props ────────────────────────────────────────
    _platform_props = st.session_state.get("platform_props", [])

    # Fall back to CSV on disk if session state is empty
    if not _platform_props:
        try:
            from data.data_manager import load_platform_props_from_csv
            _platform_props = load_platform_props_from_csv()
        except Exception:
            _platform_props = []

    if _platform_props:
        with st.expander(
            f"📋 Add Platform Props to Bet Tracker ({len(_platform_props)} props available)",
            expanded=False,
        ):
            st.caption(
                "These are today's live platform props loaded from all major sportsbooks. "
                "Click **Add All** to log them as "
                "PENDING bets so you can track their results."
            )

            # Summary by platform
            _by_plat: dict = {}
            for _p in _platform_props:
                _pl = str(_p.get("platform", "Unknown"))
                _by_plat[_pl] = _by_plat.get(_pl, 0) + 1
            _plat_cols = st.columns(max(len(_by_plat), 1))
            for _i, (_plname, _cnt) in enumerate(_by_plat.items()):
                _plat_cols[_i % len(_plat_cols)].metric(_plname, _cnt, help="Props from this platform")

            # Preview table
            import pandas as _pd_props
            _preview_rows = [
                {
                    "Player": _p.get("player_name", ""),
                    "Team": _p.get("team", ""),
                    "Stat": _p.get("stat_type", ""),
                    "Line": _p.get("line", ""),
                    "Platform": _p.get("platform", ""),
                    "Game Date": _p.get("game_date", ""),
                }
                for _p in _platform_props
            ]
            st.dataframe(
                _pd_props.DataFrame(_preview_rows),
                use_container_width=True,
                hide_index=True,
                height=min(250, 40 + len(_preview_rows) * 35),
            )

            _prop_dir = st.radio(
                "Default Direction",
                ["OVER", "UNDER"],
                horizontal=True,
                key="props_bulk_direction",
                help="Direction applied to props that don't have one set by the platform.",
            )

            if st.button(
                f"➕ Add All {len(_platform_props)} Props to Bet Tracker",
                type="primary",
                key="btn_add_all_props",
            ):
                with st.spinner("Adding props to bet tracker…"):
                    _saved, _skipped, _errs = log_props_to_tracker(
                        _platform_props, direction=_prop_dir
                    )
                if _saved:
                    st.success(
                        f"✅ Added **{_saved}** prop(s) to the Bet Tracker "
                        f"({_skipped} duplicate(s) skipped)."
                    )
                elif _skipped == len(_platform_props):
                    st.info("ℹ️ All props are already in the Bet Tracker for today.")
                else:
                    st.warning("⚠️ No new props were added.")
                if _errs:
                    with st.expander(f"⚠️ {len(_errs)} warning(s)"):
                        for _e in _errs[:20]:
                            st.caption(_e)
    else:
        st.info(
            "💡 No platform props loaded yet. Load live props from the "
            "**📡 Live Games** page and then return here to bulk-add them."
        )

    st.divider()
    analysis_results = st.session_state.get("analysis_results", [])
    player_options = ["— type manually —"]
    if analysis_results:
        player_options += sorted({
            r.get("player_name", "") for r in analysis_results
            if r.get("player_name")
        })

    with st.form("log_bet_form_bt", clear_on_submit=True):
        col1, col2 = st.columns(2)

        with col1:
            selected_player = st.selectbox(
                "Player (from tonight's analysis)", player_options
            )
            manual_player = st.text_input(
                "Or enter player name manually", placeholder="e.g., LeBron James"
            )
            stat_type  = st.selectbox(
                "Stat Type",
                ["points", "rebounds", "assists", "threes", "steals", "blocks", "turnovers"],
            )
            prop_line  = st.number_input("Line", min_value=0.0, max_value=200.0, value=24.5, step=0.5)
            direction  = st.radio("Direction", ["OVER", "UNDER"], horizontal=True)

        with col2:
            platform   = st.selectbox("Platform", ["PrizePicks", "Underdog Fantasy", "DraftKings Pick6"])
            tier       = st.selectbox("Tier", ["Platinum", "Gold", "Silver", "Bronze"])
            entry_fee  = st.number_input("Entry Fee ($)", min_value=0.0, max_value=10000.0, value=10.0, step=5.0)
            team       = st.text_input("Team (optional)", placeholder="e.g., LAL")
            notes      = st.text_input("Notes (optional)", placeholder="e.g., revenge game")

        # Pre-fill confidence from analysis results
        confidence_score = 0.0
        probability_over = 0.5
        edge_percentage  = 0.0
        final_player = (
            manual_player.strip()
            if manual_player.strip()
            else (selected_player if selected_player != "— type manually —" else "")
        )
        if final_player and analysis_results:
            for r in analysis_results:
                if r.get("player_name", "").lower() == final_player.lower():
                    confidence_score = r.get("confidence_score", 0.0)
                    probability_over = r.get("probability_over", 0.5)
                    edge_percentage  = r.get("edge_percentage", 0.0)
                    if not tier or tier == "Bronze":
                        tier = r.get("tier", "Bronze")
                    break

        st.caption(
            f"Auto-filled from analysis: confidence={confidence_score:.1f}, "
            f"P(over)={probability_over:.2f}, edge={edge_percentage:+.1f}%"
            if confidence_score
            else "Enter player name to auto-fill from tonight's analysis."
        )

        submit_bet = st.form_submit_button(
            "📌 Log Bet", width="stretch", type="primary"
        )

    if submit_bet:
        if not final_player:
            st.error("Please enter or select a player name.")
        elif prop_line <= 0:
            st.error("Prop line must be greater than 0.")
        else:
            ok, msg = log_new_bet(
                player_name=final_player,
                stat_type=stat_type,
                prop_line=prop_line,
                direction=direction,
                platform=platform,
                confidence_score=confidence_score,
                probability_over=probability_over,
                edge_percentage=edge_percentage,
                tier=tier,
                entry_fee=entry_fee,
                team=team.strip().upper() if team else "",
                notes=notes.strip(),
            )
            if ok:
                st.success(f"✅ {msg}")
            else:
                st.error(f"❌ {msg}")

# ============================================================
# END SECTION: Log a Bet Tab
# ============================================================


# ============================================================
# SECTION: Performance Predictor Tab
# ============================================================

with tab_predict:
    st.subheader("🔮 Performance Predictor")
    st.markdown(
        "Based on tonight's analysis results, here is a forward-looking prediction "
        "for your slate's expected performance."
    )

    analysis_results = st.session_state.get("analysis_results", [])

    if not analysis_results:
        st.info(
            "💡 No analysis results found. "
            "Go to **⚡ Neural Analysis** and run analysis first, "
            "then return here for your performance forecast."
        )
    else:
        confidences  = [r.get("confidence_score", 0) for r in analysis_results]
        avg_conf     = sum(confidences) / len(confidences) if confidences else 0

        # Tier-weighted win-rate estimation
        _TIER_BASE_WIN_RATE = {
            "Platinum": 72.0,
            "Gold":     62.0,
            "Silver":   54.0,
            "Bronze":   46.0,
        }
        _tier_counts_wr: dict = {}
        for r in analysis_results:
            t = r.get("tier", "Bronze")
            _tier_counts_wr[t] = _tier_counts_wr.get(t, 0) + 1

        total_picks = len(analysis_results)
        tier_weighted_win_rate = 0.0
        if total_picks > 0:
            for tier_name, base_wr in _TIER_BASE_WIN_RATE.items():
                cnt = _tier_counts_wr.get(tier_name, 0)
                tier_weighted_win_rate += (cnt / total_picks) * base_wr

        conf_based = min(95.0, max(40.0, avg_conf * 0.9 + 5.0))
        est_win_rate = round(0.70 * tier_weighted_win_rate + 0.30 * conf_based, 1)
        est_win_rate = min(95.0, max(40.0, est_win_rate))

        pc1, pc2, pc3 = st.columns(3)
        pc1.metric("Props in Slate",  len(analysis_results))
        pc2.metric("Avg Confidence",  f"{avg_conf:.1f}")
        pc3.metric("Est. Win Rate",   f"{est_win_rate:.1f}%",
                   help="Tier-weighted estimate blended with model confidence")

        if _tier_counts_wr:
            st.markdown("**Per-Tier Expected Win Rates**")
            _TIER_EMOJI_WR = {"Platinum": "💎", "Gold": "🥇", "Silver": "🥈", "Bronze": "🥉"}
            tier_wr_rows = [
                {
                    "Tier":            f"{_TIER_EMOJI_WR.get(t, '')} {t}",
                    "Picks":           _tier_counts_wr[t],
                    "Est. Win Rate":   f"{_TIER_BASE_WIN_RATE.get(t, 50):.0f}%",
                }
                for t in ["Platinum", "Gold", "Silver", "Bronze"]
                if t in _tier_counts_wr
            ]
            st.markdown(
                get_styled_stats_table_html(
                    tier_wr_rows,
                    ["Tier", "Picks", "Est. Win Rate"],
                ),
                unsafe_allow_html=True,
            )

        st.divider()

        st.subheader("💰 Recommended Bankroll Allocation")
        st.markdown("Based on the SAFE Score™ tier distribution in tonight's slate.")

        _tier_counts: dict = {}
        for r in analysis_results:
            t = r.get("tier", "Bronze")
            _tier_counts[t] = _tier_counts.get(t, 0) + 1

        _TIER_PCT   = {"Platinum": 0.30, "Gold": 0.25, "Silver": 0.20, "Bronze": 0.10}
        _TIER_EMOJI = {"Platinum": "💎", "Gold": "🥇", "Silver": "🥈", "Bronze": "🥉"}
        bankroll    = st.number_input(
            "Total Bankroll ($)", min_value=10.0, max_value=100000.0, value=100.0, step=10.0
        )
        alloc_rows = []
        for tier_name in ["Platinum", "Gold", "Silver", "Bronze"]:
            cnt = _tier_counts.get(tier_name, 0)
            if cnt > 0:
                alloc_pct = _TIER_PCT.get(tier_name, 0.10)
                alloc_amt = bankroll * alloc_pct
                alloc_rows.append({
                    "Tier":          f"{_TIER_EMOJI.get(tier_name, '')} {tier_name}",
                    "Picks":         cnt,
                    "Allocation %":  f"{alloc_pct * 100:.0f}%",
                    "Amount ($)":    f"${alloc_amt:.2f}",
                    "Per Pick ($)":  f"${alloc_amt / max(cnt, 1):.2f}",
                })
        if alloc_rows:
            st.markdown(
                get_styled_stats_table_html(
                    alloc_rows,
                    ["Tier", "Picks", "Allocation %", "Amount ($)", "Per Pick ($)"],
                ),
                unsafe_allow_html=True,
            )
        else:
            st.info("No tier data found in tonight's analysis results.")

        st.divider()

        platinum_picks = [
            r for r in analysis_results
            if r.get("tier", "") == "Platinum"
            and not r.get("should_avoid", False)
        ]
        if platinum_picks:
            st.subheader("🏆 Projected ROI — All Platinum Picks Hit")
            plat_alloc       = bankroll * 0.30
            projected_payout = plat_alloc * 2.0
            projected_roi    = (
                (projected_payout - plat_alloc) / max(plat_alloc, 1)
            ) * 100

            rp1, rp2, rp3 = st.columns(3)
            rp1.metric("Platinum Picks", len(platinum_picks))
            rp2.metric("Allocated",      f"${plat_alloc:.2f}")
            rp3.metric(
                "Projected Profit",
                f"${projected_payout - plat_alloc:.2f}",
                delta=f"{projected_roi:.1f}% ROI",
            )

            st.caption(
                "⚠️ Projections are illustrative estimates based on typical platform "
                "multipliers. Actual payouts vary by platform and entry type."
            )
        else:
            st.info(
                "No Platinum tier picks found in tonight's analysis. "
                "Run **⚡ Neural Analysis** for updated results."
            )

# ============================================================
# END SECTION: Performance Predictor Tab
# ============================================================


# ============================================================
# SECTION: History Tab — 2-Week Rolling Timeline
# ============================================================

with tab_history:
    st.subheader("📅 2-Week Rolling History")
    st.markdown(
        "Day-by-day breakdown of the last 14 days. "
        "Expand any day to see individual picks as styled cards."
    )

    rolling_stats = get_rolling_stats(days=14)
    snapshots = rolling_stats.get("snapshots", [])

    # ── Rolling Summary Bar ────────────────────────────────────────────
    if rolling_stats.get("total_bets", 0) > 0:
        streak_val = rolling_stats.get("streak", 0)
        streak_label = (
            f"🔥 W{streak_val}" if streak_val > 0
            else f"❄️ L{abs(streak_val)}" if streak_val < 0
            else "—"
        )
        best_d = rolling_stats.get("best_day", {})
        worst_d = rolling_stats.get("worst_day", {})
        best_day_str = f"{best_d.get('snapshot_date','—')} ({best_d.get('win_rate',0):.0f}%)" if best_d else "—"
        worst_day_str = f"{worst_d.get('snapshot_date','—')} ({worst_d.get('win_rate',0):.0f}%)" if worst_d else "—"

        rs1, rs2, rs3, rs4, rs5 = st.columns(5)
        rs1.metric("14-Day Bets",  rolling_stats.get("total_bets", 0))
        rs2.metric("Win Rate",     f"{rolling_stats.get('win_rate', 0):.1f}%")
        rs3.metric("Streak",       streak_label)
        rs4.metric("Best Day",     best_day_str)
        rs5.metric("Worst Day",    worst_day_str)
        st.divider()

    # ── Cumulative ROI Time-Series Chart ───────────────────────────────
    # Build a cumulative P&L series from all resolved bets over the
    # last 14 days to visualize the strategy's trend.
    _all_hist_bets = load_all_bets(limit=1000)
    _resolved_bets = [
        b for b in _all_hist_bets
        if b.get("result") in ("WIN", "LOSS", "PUSH")
        and _platform_filter_fn(b)
    ]
    _resolved_bets.sort(key=lambda b: (b.get("bet_date", ""), b.get("id", 0)))

    if len(_resolved_bets) >= 3:
        with st.expander("📈 Cumulative P&L Curve (resolved bets)", expanded=True):
            _HIST_PAYOUT = 0.909
            _cum_pnl = 0.0
            _pnl_vals = []
            _pnl_labels = []
            _daily_pnl: dict = {}  # date → cumulative at end of day
            for _rb in _resolved_bets:
                _res = _rb.get("result", "")
                if _res == "WIN":
                    _cum_pnl += _HIST_PAYOUT
                elif _res == "LOSS":
                    _cum_pnl -= 1.0
                # PUSH: no change
                _pnl_vals.append(round(_cum_pnl, 2))
                _pnl_labels.append(_rb.get("bet_date", ""))
                _daily_pnl[_rb.get("bet_date", "")] = round(_cum_pnl, 2)

            _wins_total = sum(1 for b in _resolved_bets if b.get("result") == "WIN")
            _losses_total = sum(1 for b in _resolved_bets if b.get("result") == "LOSS")
            _wr_total = _wins_total / max(_wins_total + _losses_total, 1) * 100

            _roi_cols = st.columns(4)
            _roi_cols[0].metric("Resolved Bets", len(_resolved_bets))
            _roi_cols[1].metric("Win Rate", f"{_wr_total:.1f}%",
                                delta=f"{_wr_total - 52.38:+.1f}% vs breakeven")
            _roi_cols[2].metric("Total P&L", f"{_cum_pnl:+.2f} units")
            _roi_cols[3].metric(
                "ROI/bet",
                f"{_cum_pnl / max(len(_resolved_bets), 1):+.3f}u",
                delta="positive = profitable" if _cum_pnl > 0 else "negative = losing"
            )

            # Line chart of cumulative P&L
            st.line_chart({"Cumulative P&L (units)": _pnl_vals}, height=220)
            st.caption(
                f"📊 {len(_resolved_bets)} resolved bets · "
                f"{_wins_total}W / {_losses_total}L · "
                f"Final: {_cum_pnl:+.2f}u"
            )

            # Daily P&L mini-table
            if _daily_pnl:
                _dpnl_rows = [
                    {"Date": d, "End-of-Day P&L": f"{v:+.2f}u"}
                    for d, v in sorted(_daily_pnl.items(), reverse=True)[:14]
                ]
                st.dataframe(_dpnl_rows, hide_index=True, use_container_width=True)
        st.divider()

    # ── History Filter ─────────────────────────────────────────────────
    hist_filter = st.radio(
        "History filter",
        ["All", "Wins Only", "Losses Only", "Pending"],
        horizontal=True,
        label_visibility="collapsed",
    )

    # ── Day-by-Day Timeline ────────────────────────────────────────────
    if not snapshots:
        # No snapshot data yet — fall back to grouping raw bets by date
        all_bets_hist = load_all_bets()
        all_bets_hist = [b for b in all_bets_hist if _platform_filter_fn(b)]
        _by_date_hist: dict = {}
        for _b in all_bets_hist:
            _by_date_hist.setdefault(_b.get("bet_date", "Unknown"), []).append(_b)
        
        if not _by_date_hist:
            st.info("📭 No bets logged yet. Start logging bets to build your history.")
        else:
            for _date in sorted(_by_date_hist.keys(), reverse=True):
                _day_bets = _by_date_hist[_date]
                _day_w = sum(1 for b in _day_bets if b.get("result") == "WIN")
                _day_l = sum(1 for b in _day_bets if b.get("result") == "LOSS")
                _day_p = sum(1 for b in _day_bets if b.get("result") == "PUSH")
                _day_pend = sum(1 for b in _day_bets if not b.get("result"))
                _day_total_res = _day_w + _day_l
                _day_wr = round(_day_w / max(_day_total_res, 1) * 100, 1) if _day_total_res > 0 else None

                if _day_wr is not None and _day_wr >= 55:
                    _day_color = "🟢"
                    _day_card_class = "day-card-green"
                elif _day_wr is not None and _day_wr < 45:
                    _day_color = "🔴"
                    _day_card_class = "day-card-red"
                else:
                    _day_color = "🟡"
                    _day_card_class = "day-card-yellow"

                _wr_str = f" — {_day_wr:.0f}% win rate" if _day_wr is not None else " — ⏳ pending"
                _label = (
                    f"{_day_color} {_date} · {len(_day_bets)} picks · "
                    f"✅{_day_w} ❌{_day_l}" + _wr_str
                )

                # Apply history filter
                if hist_filter == "Wins Only":
                    _show_bets = [b for b in _day_bets if b.get("result") == "WIN"]
                elif hist_filter == "Losses Only":
                    _show_bets = [b for b in _day_bets if b.get("result") == "LOSS"]
                elif hist_filter == "Pending":
                    _show_bets = [b for b in _day_bets if not b.get("result")]
                else:
                    _show_bets = _day_bets

                if not _show_bets and hist_filter != "All":
                    continue

                with st.expander(_label, expanded=(_date == max(_by_date_hist.keys()))):
                    if not _show_bets:
                        st.info(f"No bets match '{hist_filter}' for this day.")
                    else:
                        _ha, _hb = st.columns(2)
                        for _idx, _hb_bet in enumerate(_show_bets):
                            _hcol = _ha if _idx % 2 == 0 else _hb
                            with _hcol:
                                st.markdown(get_bet_card_html(_hb_bet), unsafe_allow_html=True)
                        if len(_show_bets) > 6:
                            st.caption(f"*{len(_show_bets)} bets shown for {_date}.*")
    else:
        # Use daily_snapshots for summaries + load individual bets for expansion
        all_bets_hist = load_all_bets(limit=1000)
        all_bets_hist = [b for b in all_bets_hist if _platform_filter_fn(b)]
        _bets_by_date: dict = {}
        for _b in all_bets_hist:
            _bets_by_date.setdefault(_b.get("bet_date", "Unknown"), []).append(_b)

        for snap in snapshots:
            _date = snap.get("snapshot_date", "")
            _w = snap.get("wins", 0)
            _l = snap.get("losses", 0)
            _pend = snap.get("pending", 0)
            _total = snap.get("total_picks", 0)
            _wr = snap.get("win_rate", 0.0)

            if _wr >= 55:
                _day_color = "🟢"
            elif _wr < 45 and (_w + _l) > 0:
                _day_color = "🔴"
            else:
                _day_color = "🟡"

            _wr_str = f" — {_wr:.0f}% win rate" if (_w + _l) > 0 else " — ⏳ all pending"
            _label = (
                f"{_day_color} {_date} · {_total} picks · "
                f"✅{_w} ❌{_l}" + _wr_str
            )

            _day_bets = _bets_by_date.get(_date, [])

            if hist_filter == "Wins Only":
                _show_bets = [b for b in _day_bets if b.get("result") == "WIN"]
            elif hist_filter == "Losses Only":
                _show_bets = [b for b in _day_bets if b.get("result") == "LOSS"]
            elif hist_filter == "Pending":
                _show_bets = [b for b in _day_bets if not b.get("result")]
            else:
                _show_bets = _day_bets

            if not _show_bets and hist_filter != "All" and _total == 0:
                continue

            with st.expander(_label):
                if not _show_bets:
                    if _total > 0:
                        st.info(f"No individual bets match '{hist_filter}' for this day.")
                    else:
                        st.info("No bets logged for this day.")
                else:
                    _ha, _hb = st.columns(2)
                    for _idx, _hb_bet in enumerate(_show_bets):
                        _hcol = _ha if _idx % 2 == 0 else _hb
                        with _hcol:
                            st.markdown(get_bet_card_html(_hb_bet), unsafe_allow_html=True)

# ============================================================
# END SECTION: History Tab
# ============================================================

# ============================================================
# SECTION: Achievements Tab
# Gamification panel: streaks, win-rate badges, milestones,
# personal bests, and a betting journal summary.
# ============================================================

with tab_achievements:
    st.subheader("🏆 Achievements & Streak Tracker")
    st.markdown(
        "Track your betting streaks, unlock milestones, and review your personal bests."
    )

    # ── Load all resolved bets ─────────────────────────────────────
    _ach_all_bets = load_all_analysis_picks() + load_all_bets()
    _ach_resolved = [
        b for b in _ach_all_bets
        if b.get("result") in ("WIN", "LOSS", "PUSH")
    ]
    _ach_resolved.sort(key=lambda b: b.get("bet_date", ""), reverse=True)

    if not _ach_resolved:
        st.info(
            "No resolved bets yet. Log and resolve some bets to start earning achievements!"
        )
    else:
        total_bets   = len(_ach_resolved)
        total_wins   = sum(1 for b in _ach_resolved if b.get("result") == "WIN")
        total_losses = sum(1 for b in _ach_resolved if b.get("result") == "LOSS")
        overall_wr   = round(total_wins / max(total_bets, 1) * 100, 1)

        # ── Current hot/cold streak ────────────────────────────────
        _cur_streak = 0
        _cur_streak_type = ""
        if _ach_resolved:
            _first_result = _ach_resolved[0].get("result", "")
            _cur_streak_type = _first_result
            for _sb in _ach_resolved:
                if _sb.get("result") == _first_result:
                    _cur_streak += 1
                else:
                    break

        # ── Longest win/loss streak ────────────────────────────────
        _longest_win = 0
        _longest_loss = 0
        _run_w = 0
        _run_l = 0
        for _sb in reversed(_ach_resolved):
            if _sb.get("result") == "WIN":
                _run_w += 1
                _run_l = 0
            elif _sb.get("result") == "LOSS":
                _run_l += 1
                _run_w = 0
            else:
                _run_w = _run_l = 0
            _longest_win  = max(_longest_win,  _run_w)
            _longest_loss = max(_longest_loss, _run_l)

        # ── Best single-day win rate (min 3 bets) ──────────────────
        _day_groups: dict = {}
        for _sb in _ach_resolved:
            _d = _sb.get("bet_date", "Unknown")
            _day_groups.setdefault(_d, []).append(_sb)
        _best_day_wr = 0.0
        _best_day    = ""
        for _d, _dbets in _day_groups.items():
            if len(_dbets) >= 3:
                _dw = sum(1 for b in _dbets if b.get("result") == "WIN")
                _dwr = _dw / len(_dbets) * 100
                if _dwr > _best_day_wr:
                    _best_day_wr = _dwr
                    _best_day    = _d

        # ── Top stat type by win rate ──────────────────────────────
        _stat_groups: dict = {}
        for _sb in _ach_resolved:
            _st = _sb.get("stat_type", "unknown")
            _stat_groups.setdefault(_st, {"wins": 0, "total": 0})
            _stat_groups[_st]["total"] += 1
            if _sb.get("result") == "WIN":
                _stat_groups[_st]["wins"] += 1
        _stat_wr = {
            s: round(v["wins"] / max(v["total"], 1) * 100, 1)
            for s, v in _stat_groups.items()
            if v["total"] >= 3
        }
        _best_stat = max(_stat_wr, key=_stat_wr.get) if _stat_wr else ""
        _best_stat_wr = _stat_wr.get(_best_stat, 0.0)

        # ── Summary metrics row ────────────────────────────────────
        _ac1, _ac2, _ac3, _ac4 = st.columns(4)
        with _ac1:
            st.metric("Total Resolved", total_bets)
        with _ac2:
            st.metric("Overall Win Rate", f"{overall_wr:.1f}%",
                      delta=f"+{overall_wr - 50:.1f}% vs 50%" if overall_wr != 50 else None)
        with _ac3:
            _streak_label = (
                f"🔥 {_cur_streak} W" if _cur_streak_type == "WIN" and _cur_streak >= 2
                else f"❄️ {_cur_streak} L" if _cur_streak_type == "LOSS" and _cur_streak >= 2
                else f"{_cur_streak} {_cur_streak_type}"
            )
            st.metric("Current Streak", _streak_label)
        with _ac4:
            st.metric("Best Day W%", f"{_best_day_wr:.0f}%" if _best_day else "—",
                      help=f"Date: {_best_day}" if _best_day else "Need 3+ bets in one day")

        st.divider()

        # ── Achievement badges ────────────────────────────────────
        st.markdown("### 🎖️ Earned Badges")

        _badges: list[dict] = []

        # Volume badges
        if total_bets >= 100:
            _badges.append({"icon": "💯", "name": "Century Club", "desc": "100+ resolved bets"})
        elif total_bets >= 50:
            _badges.append({"icon": "🥈", "name": "Fifty Strong", "desc": "50+ resolved bets"})
        elif total_bets >= 10:
            _badges.append({"icon": "🎯", "name": "Getting Serious", "desc": "10+ resolved bets"})

        # Win rate badges
        if overall_wr >= 70 and total_bets >= 20:
            _badges.append({"icon": "👑", "name": "Elite Picker", "desc": f"{overall_wr:.0f}% win rate (20+ bets)"})
        elif overall_wr >= 60 and total_bets >= 10:
            _badges.append({"icon": "⭐", "name": "Sharp", "desc": f"{overall_wr:.0f}% win rate (10+ bets)"})
        elif overall_wr >= 55 and total_bets >= 10:
            _badges.append({"icon": "✅", "name": "Consistent", "desc": f"{overall_wr:.0f}% win rate"})

        # Streak badges
        if _longest_win >= 10:
            _badges.append({"icon": "🔥🔥", "name": "On Fire", "desc": f"{_longest_win}-game win streak"})
        elif _longest_win >= 5:
            _badges.append({"icon": "🔥", "name": "Hot Streak", "desc": f"{_longest_win}-game win streak"})
        elif _longest_win >= 3:
            _badges.append({"icon": "⚡", "name": "Warming Up", "desc": f"{_longest_win}-game win streak"})

        # Stat mastery badges
        if _best_stat and _best_stat_wr >= 65:
            _badges.append({
                "icon": "🏆",
                "name": f"{_best_stat.replace('_',' ').title()} Master",
                "desc": f"{_best_stat_wr:.0f}% win rate on {_best_stat}",
            })

        # Resilience badge
        if _longest_loss >= 5 and overall_wr >= 55:
            _badges.append({"icon": "💪", "name": "Resilient", "desc": "Bounced back from 5+ loss streak"})

        # Current streak badge
        if _cur_streak_type == "WIN" and _cur_streak >= 5:
            _badges.append({"icon": "🌶️", "name": "White Hot", "desc": f"Current {_cur_streak}-game win streak!"})

        if not _badges:
            st.info("No badges earned yet. Keep logging bets to unlock achievements!")
        else:
            _badge_cols = st.columns(min(len(_badges), 4))
            for _bi, _badge in enumerate(_badges):
                with _badge_cols[_bi % 4]:
                    st.markdown(
                        f'<div style="background:rgba(0,240,255,0.06);border:1px solid rgba(0,240,255,0.18);'
                        f'border-radius:10px;padding:12px 10px;text-align:center;min-height:90px;">'
                        f'<div style="font-size:1.8rem;">{_badge["icon"]}</div>'
                        f'<div style="color:#e8f4ff;font-weight:700;font-size:0.82rem;margin:4px 0 2px;">'
                        f'{_badge["name"]}</div>'
                        f'<div style="color:#8a9bb8;font-size:0.70rem;">{_badge["desc"]}</div>'
                        f'</div>',
                        unsafe_allow_html=True,
                    )

        st.divider()

        # ── Streak history mini-chart (last 30 bets) ──────────────
        st.markdown("### 📈 Last 30 Results — Win/Loss Timeline")
        _last30 = _ach_resolved[:30]
        if _last30:
            _timeline_dots = []
            for _tb in reversed(_last30):
                _tr = _tb.get("result", "")
                if _tr == "WIN":
                    _timeline_dots.append('<span style="display:inline-block;width:16px;height:16px;border-radius:50%;background:#00d084;margin:2px;" title="WIN"></span>')
                elif _tr == "LOSS":
                    _timeline_dots.append('<span style="display:inline-block;width:16px;height:16px;border-radius:50%;background:#ff4d4d;margin:2px;" title="LOSS"></span>')
                else:
                    _timeline_dots.append('<span style="display:inline-block;width:16px;height:16px;border-radius:50%;background:#444;margin:2px;" title="PUSH"></span>')
            st.markdown(
                '<div style="display:flex;flex-wrap:wrap;gap:3px;padding:8px;background:rgba(13,20,45,0.55);'
                'border:1px solid rgba(0,240,255,0.1);border-radius:8px;">'
                + "".join(_timeline_dots)
                + "<span style='color:#8a9bb8;font-size:0.72rem;margin-left:8px;align-self:center;'>"
                  "← oldest &nbsp; newest →</span>"
                + "</div>",
                unsafe_allow_html=True,
            )
        else:
            st.caption("No timeline data available.")

        st.divider()

        # ── Personal bests table ───────────────────────────────────
        st.markdown("### 🥇 Personal Bests")
        _pb_col1, _pb_col2 = st.columns(2)
        with _pb_col1:
            st.metric("🔥 Longest Win Streak", f"{_longest_win} games")
            st.metric("📅 Best Single-Day Win %", f"{_best_day_wr:.0f}%" if _best_day else "—",
                      help=f"Date: {_best_day}" if _best_day else "")
        with _pb_col2:
            st.metric("🏆 Best Stat Type", f"{_best_stat.replace('_',' ').title()}" if _best_stat else "—",
                      delta=f"{_best_stat_wr:.0f}% WR" if _best_stat else None)
            st.metric("📊 Total Wins", total_wins)

        st.divider()

        # ── Win rate by stat type summary ─────────────────────────
        if _stat_wr:
            st.markdown("### 📊 Win Rate by Stat Type")
            _wr_rows = [
                {
                    "Stat Type": s.replace("_", " ").title(),
                    "Bets": _stat_groups[s]["total"],
                    "Wins": _stat_groups[s]["wins"],
                    "Win Rate": f"{wr:.1f}%",
                    "Grade": "A ✅" if wr >= 65 else ("B ✅" if wr >= 55 else ("C ➡️" if wr >= 50 else "D ❌")),
                }
                for s, wr in sorted(_stat_wr.items(), key=lambda x: x[1], reverse=True)
            ]
            st.dataframe(_wr_rows, hide_index=True, use_container_width=True)

# ============================================================
# END SECTION: Achievements Tab
# ============================================================

