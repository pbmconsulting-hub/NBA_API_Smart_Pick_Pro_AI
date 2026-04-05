"""
3_🏆_Analysis — Analysis hub for running prop analysis on imported or searched props.

Provides bulk and single-prop analysis with confidence scoring, edge detection,
and simulation results.
"""

import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
for _p in (_ROOT, _ROOT / "SmartPicksProAI", _ROOT / "SmartPicksProAI" / "frontend"):
    if str(_p) not in sys.path:
        sys.path.insert(0, str(_p))

import streamlit as st  # noqa: E402
import pandas as pd  # noqa: E402

st.set_page_config(page_title="Analysis", page_icon="🏆", layout="wide")

try:
    from SmartPicksProAI.utils.components import inject_joseph_floating  # noqa: E402
except ImportError:
    def inject_joseph_floating() -> None:  # type: ignore[misc]
        pass

try:
    from SmartPicksProAI.frontend.api_service import (
        analyze_prop,
        search_players,
        get_todays_slate,
    )
    _API = True
except ImportError:
    _API = False

TIER_EMOJI = {
    "Platinum": "💎", "Gold": "🥇", "Silver": "🥈",
    "Bronze": "🥉", "Avoid": "⛔",
}
TIER_COLORS = {
    "Platinum": "#E5E4E2", "Gold": "#FFD700", "Silver": "#C0C0C0",
    "Bronze": "#CD7F32", "Avoid": "#FF4444",
}

st.title("🏆 Analysis Hub")
st.caption(
    "Run AI-powered prop analysis with confidence scoring, edge detection, "
    "and Monte Carlo simulation."
)

if not _API:
    st.error("Backend API unavailable — start the backend first.")
    st.stop()

tab_single, tab_bulk, tab_slate = st.tabs([
    "🎯 Single Prop", "📊 Bulk Analysis", "🤖 Auto Slate"
])

# ── Tab 1: Single Prop Analysis ─────────────────────────────────────────
with tab_single:
    st.markdown("### Analyze a Single Prop")

    cols = st.columns(2)
    with cols[0]:
        search_q = st.text_input("Search Player", placeholder="e.g. LeBron James", key="analysis_search")
        player_id = None
        if search_q.strip():
            results = search_players(search_q.strip())
            if results:
                options = {
                    f"{r.get('full_name', '')} ({r.get('team_abbreviation', '')})": r["player_id"]
                    for r in results[:10]
                }
                selected = st.selectbox("Select Player", list(options.keys()), key="analysis_player_select")
                player_id = options.get(selected)
            else:
                st.warning("No players found.")

    with cols[1]:
        stat_type = st.selectbox(
            "Stat Type",
            ["points", "rebounds", "assists", "threes", "steals", "blocks", "turnovers"],
            key="analysis_stat",
        )
        prop_line = st.number_input("Prop Line", min_value=0.5, value=20.5, step=0.5, key="analysis_line")
        platform = st.selectbox(
            "Platform",
            ["prizepicks", "underdog", "draftkings", "fanduel"],
            key="analysis_platform",
        )

    if st.button("🔍 Analyze Prop", key="btn_analyze_single", type="primary"):
        if player_id:
            with st.spinner("Running analysis…"):
                result = analyze_prop(
                    player_id=player_id,
                    stat_type=stat_type,
                    prop_line=prop_line,
                    platform=platform,
                )
            if result and not result.get("error"):
                st.session_state.last_analysis = result

                # Display results
                tier = result.get("confidence", {}).get("tier", "Bronze")
                conf = result.get("confidence", {}).get("confidence_score", 0)
                edge = result.get("edge_pct", 0)
                direction = result.get("direction", "OVER")
                projection = result.get("projection", 0)
                prob = result.get("model_probability", 0)

                emoji = TIER_EMOJI.get(tier, "🥉")
                color = TIER_COLORS.get(tier, "#C0C0C0")
                dir_icon = "🟢" if direction == "OVER" else "🔴"

                st.markdown(
                    f'<div style="height:4px;background:{color};'
                    f'border-radius:2px;margin:8px 0"></div>',
                    unsafe_allow_html=True,
                )

                m_cols = st.columns(5)
                m_cols[0].metric("Verdict", f"{emoji} {tier}")
                m_cols[1].metric("Direction", f"{dir_icon} {direction}")
                m_cols[2].metric("Confidence", f"{conf:.0f}")
                m_cols[3].metric("Edge", f"{edge:+.1f}%")
                m_cols[4].metric("Projection", f"{projection:.1f}")

                st.markdown("#### 📊 Detailed Breakdown")
                det_cols = st.columns(3)
                det_cols[0].metric("Model Probability", f"{prob:.1%}")
                det_cols[1].metric("Prop Line", f"{prop_line}")
                det_cols[2].metric("Stat Type", stat_type.upper())

                # Explanation
                explanation = result.get("explanation", "")
                if explanation:
                    st.markdown("#### 🧠 AI Explanation")
                    st.info(explanation)

                # Risk flags
                flags = result.get("risk_flags", [])
                if flags:
                    st.markdown("#### ⚠️ Risk Flags")
                    for flag in flags:
                        st.warning(flag)
            else:
                st.error(result.get("error", "Analysis failed."))
        else:
            st.warning("Please search and select a player first.")

# ── Tab 2: Bulk Analysis ────────────────────────────────────────────────
with tab_bulk:
    st.markdown("### Bulk Prop Analysis")
    st.caption("Analyze all imported props at once.")

    props = st.session_state.get("imported_props", [])
    if props:
        st.info(f"{len(props)} props ready for analysis.")
        if st.button("🚀 Run Bulk Analysis", key="btn_bulk", type="primary"):
            results = []
            progress = st.progress(0)
            for idx, prop in enumerate(props):
                # Try to find the player
                search_res = search_players(prop.get("player_name", ""))
                if search_res:
                    pid = search_res[0]["player_id"]
                    result = analyze_prop(
                        player_id=pid,
                        stat_type=prop.get("stat_type", "points"),
                        prop_line=prop.get("prop_line", 20.0),
                        platform=prop.get("platform", "prizepicks").lower(),
                    )
                    if result and not result.get("error"):
                        result["player_name"] = prop.get("player_name", "")
                        results.append(result)
                progress.progress((idx + 1) / len(props))

            if results:
                rows = []
                for r in results:
                    tier = r.get("confidence", {}).get("tier", "Bronze")
                    rows.append({
                        "Player": r.get("player_name", ""),
                        "Stat": r.get("stat_type", "").upper() if r.get("stat_type") else "",
                        "Line": r.get("prop_line", 0),
                        "Direction": r.get("direction", ""),
                        "Tier": f"{TIER_EMOJI.get(tier, '')} {tier}",
                        "Confidence": r.get("confidence", {}).get("confidence_score", 0),
                        "Edge %": r.get("edge_pct", 0),
                        "Projection": r.get("projection", 0),
                    })
                df = pd.DataFrame(rows)
                df = df.sort_values("Confidence", ascending=False)
                st.dataframe(df, use_container_width=True, hide_index=True)
            else:
                st.warning("No results returned from analysis.")
    else:
        st.info("No imported props. Go to **Import Props** page first.")

# ── Tab 3: Auto Slate ──────────────────────────────────────────────────
with tab_slate:
    st.markdown("### Auto-Generated Slate")
    st.caption("AI-generated best picks from today's games.")

    top_n = st.slider("Number of picks", 5, 25, 10, key="slate_n")
    if st.button("🤖 Generate Slate", key="btn_slate", type="primary"):
        with st.spinner("Generating AI slate…"):
            slate = get_todays_slate(top_n=top_n)
        picks = slate.get("picks", [])
        if picks:
            rows = []
            for p in picks:
                tier = p.get("tier", "Bronze")
                rows.append({
                    "Player": p.get("player_name", ""),
                    "Team": p.get("team", ""),
                    "Opponent": p.get("opponent", ""),
                    "Stat": p.get("stat_type", "").upper() if p.get("stat_type") else "",
                    "Line": p.get("prop_line", 0),
                    "Direction": p.get("direction", ""),
                    "Tier": f"{TIER_EMOJI.get(tier, '')} {tier}",
                    "Confidence": p.get("confidence_score", 0),
                    "Edge %": p.get("edge_pct", 0),
                })
            df = pd.DataFrame(rows)
            st.dataframe(df, use_container_width=True, hide_index=True)
            st.caption(
                f"Scanned {slate.get('games_scanned', 0)} games, "
                f"{slate.get('players_scanned', 0)} players"
            )
        else:
            st.info("No slate picks available — check back when games are scheduled.")

# ── Floating Joseph widget ──────────────────────────────────────────────
inject_joseph_floating()
