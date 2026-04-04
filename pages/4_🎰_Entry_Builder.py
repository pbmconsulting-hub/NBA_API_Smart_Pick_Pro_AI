"""
4_🎰_Entry_Builder — Build optimized parlay/slate entries.

Uses the engine's entry optimizer and correlation analysis to construct
multi-leg entries with correlated-pick awareness and bankroll sizing.
"""

import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
for _p in (_ROOT, _ROOT / "SmartPicksProAI", _ROOT / "SmartPicksProAI" / "frontend"):
    if str(_p) not in sys.path:
        sys.path.insert(0, str(_p))

import streamlit as st
import pandas as pd

st.set_page_config(page_title="Entry Builder", page_icon="🎰", layout="wide")

try:
    from SmartPicksProAI.engine.entry_optimizer import (
        optimize_entry,
        validate_entry,
        PLATFORM_CONFIGS,
    )
    _OPTIMIZER = True
except ImportError:
    _OPTIMIZER = False

TIER_EMOJI = {
    "Platinum": "💎", "Gold": "🥇", "Silver": "🥈",
    "Bronze": "🥉", "Avoid": "⛔",
}

st.title("🎰 Entry Builder")
st.caption(
    "Build optimized parlay and slate entries using AI-powered correlation "
    "analysis and bankroll sizing."
)

# ── Entry legs from session state ───────────────────────────────────────
if "entry_legs" not in st.session_state:
    st.session_state.entry_legs = []

tab_build, tab_optimize, tab_history = st.tabs([
    "🔧 Build Entry", "🎯 Optimize", "📜 History"
])

# ── Tab 1: Build Entry ─────────────────────────────────────────────────
with tab_build:
    st.markdown("### Add Legs to Your Entry")

    cols = st.columns(2)
    with cols[0]:
        player = st.text_input("Player Name", key="eb_player")
        stat = st.selectbox(
            "Stat Type",
            ["points", "rebounds", "assists", "threes", "steals", "blocks",
             "turnovers", "pts+reb", "pts+ast", "reb+ast", "pts+reb+ast"],
            key="eb_stat",
        )
    with cols[1]:
        line = st.number_input("Prop Line", min_value=0.5, value=20.5, step=0.5, key="eb_line")
        direction = st.selectbox("Direction", ["OVER", "UNDER"], key="eb_dir")

    col_a, col_b = st.columns(2)
    with col_a:
        confidence = st.slider("Confidence (0-100)", 0, 100, 70, key="eb_conf")
    with col_b:
        edge = st.number_input("Edge %", value=5.0, step=0.5, key="eb_edge")

    if st.button("➕ Add Leg", key="btn_add_leg", type="primary"):
        if player.strip():
            leg = {
                "player_name": player.strip(),
                "stat_type": stat,
                "prop_line": line,
                "direction": direction,
                "confidence": confidence,
                "edge_pct": edge,
            }
            st.session_state.entry_legs.append(leg)
            st.success(f"Added: {player.strip()} {stat} {direction} {line}")
        else:
            st.warning("Enter a player name.")

    # Import from analysis
    st.divider()
    last = st.session_state.get("last_analysis")
    if last and not last.get("error"):
        st.markdown("#### 📊 Import from Last Analysis")
        pname = last.get("player_name", "Unknown")
        pstat = last.get("stat_type", "")
        pline = last.get("prop_line", 0)
        pdir = last.get("direction", "OVER")
        conf = last.get("confidence", {}).get("confidence_score", 0)
        pedge = last.get("edge_pct", 0)
        st.markdown(f"**{pname}** — {pstat} {pdir} {pline} (Conf: {conf:.0f})")
        if st.button("📥 Add to Entry", key="btn_import_analysis"):
            st.session_state.entry_legs.append({
                "player_name": pname,
                "stat_type": pstat,
                "prop_line": pline,
                "direction": pdir,
                "confidence": conf,
                "edge_pct": pedge,
            })
            st.success(f"Added from analysis: {pname}")

    # Display current legs
    st.divider()
    st.markdown("### 📋 Current Entry Legs")
    legs = st.session_state.entry_legs
    if legs:
        df = pd.DataFrame(legs)
        st.dataframe(df, use_container_width=True, hide_index=True)

        avg_conf = sum(l["confidence"] for l in legs) / len(legs)
        avg_edge = sum(l["edge_pct"] for l in legs) / len(legs)
        m_cols = st.columns(3)
        m_cols[0].metric("Legs", len(legs))
        m_cols[1].metric("Avg Confidence", f"{avg_conf:.0f}")
        m_cols[2].metric("Avg Edge", f"{avg_edge:+.1f}%")

        if st.button("🗑️ Clear All Legs", key="btn_clear_legs"):
            st.session_state.entry_legs = []
            st.rerun()
    else:
        st.info("No legs added yet. Use the form above to build your entry.")

# ── Tab 2: Optimize ────────────────────────────────────────────────────
with tab_optimize:
    st.markdown("### Optimize Entry")

    legs = st.session_state.entry_legs
    if not legs:
        st.info("Add legs in the Build tab first.")
    else:
        platform = st.selectbox(
            "Platform",
            ["PrizePicks", "Underdog Fantasy", "DraftKings Pick6", "FanDuel"],
            key="opt_platform",
        )
        bankroll = st.session_state.get("user_bankroll", 500.0)
        st.markdown(f"**Bankroll:** ${bankroll:,.2f}")

        max_legs = st.slider("Max Legs", 2, 10, min(6, len(legs)), key="opt_max")
        min_conf = st.slider("Min Confidence per Leg", 0, 100, 55, key="opt_min_conf")

        if st.button("🎯 Optimize Entry", key="btn_optimize", type="primary"):
            if _OPTIMIZER:
                with st.spinner("Running optimizer…"):
                    result = optimize_entry(
                        legs=legs,
                        platform=platform,
                        bankroll=bankroll,
                        max_legs=max_legs,
                        min_confidence=min_conf,
                    )
                if result.get("success"):
                    st.success("✅ Optimized entry found!")
                    opt_legs = result.get("selected_legs", [])
                    if opt_legs:
                        df = pd.DataFrame(opt_legs)
                        st.dataframe(df, use_container_width=True, hide_index=True)

                    m_cols = st.columns(4)
                    m_cols[0].metric("Selected Legs", result.get("num_legs", 0))
                    m_cols[1].metric("Combined Edge", f"{result.get('combined_edge', 0):+.1f}%")
                    m_cols[2].metric("Win Probability", f"{result.get('win_prob', 0):.1%}")
                    m_cols[3].metric("Suggested Stake", f"${result.get('suggested_stake', 0):,.2f}")

                    if result.get("correlation_warnings"):
                        st.markdown("#### ⚠️ Correlation Warnings")
                        for w in result["correlation_warnings"]:
                            st.warning(w)
                else:
                    st.error(result.get("error", "Optimization failed."))
            else:
                # Fallback: simple filtering without optimizer
                filtered = [l for l in legs if l["confidence"] >= min_conf]
                filtered.sort(key=lambda x: x["confidence"], reverse=True)
                selected = filtered[:max_legs]
                if selected:
                    st.success(f"Selected top {len(selected)} legs by confidence.")
                    df = pd.DataFrame(selected)
                    st.dataframe(df, use_container_width=True, hide_index=True)
                else:
                    st.warning("No legs meet the minimum confidence threshold.")

# ── Tab 3: Entry History ────────────────────────────────────────────────
with tab_history:
    st.markdown("### Entry Submission History")
    if "entry_history" not in st.session_state:
        st.session_state.entry_history = []
    history = st.session_state.entry_history
    if history:
        for idx, entry in enumerate(reversed(history)):
            with st.expander(f"Entry #{len(history) - idx} — {entry.get('date', '')}"):
                df = pd.DataFrame(entry.get("legs", []))
                st.dataframe(df, use_container_width=True, hide_index=True)
    else:
        st.info("No entries submitted yet.")
