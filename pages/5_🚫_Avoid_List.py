"""
5_🚫_Avoid_List — Manage players and situations to avoid betting on.

Tracks injured players, cold streaks, unfavorable matchups, and
user-defined exclusions.
"""

import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
for _p in (_ROOT, _ROOT / "SmartPicksProAI", _ROOT / "SmartPicksProAI" / "frontend"):
    if str(_p) not in sys.path:
        sys.path.insert(0, str(_p))

import streamlit as st  # noqa: E402
import pandas as pd  # noqa: E402
import datetime  # noqa: E402

st.set_page_config(page_title="Avoid List", page_icon="🚫", layout="wide")

# ── Session state ───────────────────────────────────────────────────────
if "avoid_list" not in st.session_state:
    st.session_state.avoid_list = []

st.title("🚫 Avoid List")
st.caption(
    "Track players, teams, and situations to avoid. The analysis engine "
    "will flag picks that appear on your avoid list."
)

tab_manage, tab_auto, tab_rules = st.tabs([
    "📋 Manage List", "🤖 Auto-Detect", "📏 Rules"
])

# ── Tab 1: Manage ──────────────────────────────────────────────────────
with tab_manage:
    st.markdown("### Add to Avoid List")

    cols = st.columns(3)
    with cols[0]:
        avoid_name = st.text_input("Player / Team Name", key="avoid_name")
    with cols[1]:
        avoid_type = st.selectbox(
            "Type",
            ["Player", "Team", "Matchup", "Stat Type"],
            key="avoid_type",
        )
    with cols[2]:
        avoid_reason = st.selectbox(
            "Reason",
            ["Injury", "Cold Streak", "Tough Matchup", "Minute Restriction",
             "Back-to-Back", "Blowout Risk", "Model Underperforming", "Other"],
            key="avoid_reason",
        )

    avoid_notes = st.text_input("Notes (optional)", key="avoid_notes")
    avoid_expiry = st.date_input(
        "Expiry Date (optional)",
        value=None,
        key="avoid_expiry",
        help="Auto-remove from avoid list after this date.",
    )

    if st.button("🚫 Add to Avoid List", key="btn_add_avoid", type="primary"):
        if avoid_name.strip():
            entry = {
                "name": avoid_name.strip(),
                "type": avoid_type,
                "reason": avoid_reason,
                "notes": avoid_notes,
                "added_date": datetime.date.today().isoformat(),
                "expiry_date": avoid_expiry.isoformat() if avoid_expiry else "",
            }
            st.session_state.avoid_list.append(entry)
            st.success(f"✅ Added '{avoid_name.strip()}' to avoid list.")
        else:
            st.warning("Please enter a name.")

    st.divider()

    # ── Current Avoid List ──────────────────────────────────────────────
    st.markdown("### Current Avoid List")

    avoid = st.session_state.avoid_list

    # Auto-expire entries
    today = datetime.date.today().isoformat()
    active = [
        e for e in avoid
        if not e.get("expiry_date") or e["expiry_date"] >= today
    ]
    expired = len(avoid) - len(active)
    if expired > 0:
        st.session_state.avoid_list = active
        avoid = active
        st.info(f"🗑️ {expired} expired entries auto-removed.")

    if avoid:
        df = pd.DataFrame(avoid)
        st.dataframe(df, use_container_width=True, hide_index=True)
        st.caption(f"Total: {len(avoid)} entries")

        # Remove individual entries
        names = [f"{e['name']} ({e['reason']})" for e in avoid]
        to_remove = st.selectbox("Select entry to remove", names, key="avoid_remove_select")
        if st.button("🗑️ Remove Selected", key="btn_remove_avoid"):
            idx = names.index(to_remove)
            st.session_state.avoid_list.pop(idx)
            st.rerun()

        if st.button("🗑️ Clear Entire List", key="btn_clear_avoid"):
            st.session_state.avoid_list = []
            st.rerun()
    else:
        st.info("Avoid list is empty. Add players or teams to track.")

# ── Tab 2: Auto-Detect ─────────────────────────────────────────────────
with tab_auto:
    st.markdown("### Auto-Detect Avoidable Situations")
    st.caption(
        "Automatically detect players who should be avoided based on "
        "recent performance, injuries, and matchups."
    )

    auto_criteria = st.multiselect(
        "Detection Criteria",
        [
            "Players on back-to-back",
            "Players with minutes restrictions",
            "Cold streak (last 5 games below average)",
            "Model accuracy below 40% for player",
            "Injury report — questionable/doubtful",
            "Blowout-risk games (spread > 10)",
        ],
        default=[
            "Players on back-to-back",
            "Injury report — questionable/doubtful",
        ],
        key="auto_criteria",
    )

    if st.button("🔍 Run Auto-Detection", key="btn_auto_detect"):
        st.info(
            "Auto-detection requires the backend API to be running. "
            "This feature scans today's slate and flags avoidable situations."
        )
        # Placeholder results — would be populated from backend
        auto_results = [
            {
                "name": "Example Player",
                "type": "Player",
                "reason": "Back-to-Back",
                "notes": "Playing 2nd game in 2 days",
                "confidence": "High",
            },
        ]
        if auto_results:
            df = pd.DataFrame(auto_results)
            st.dataframe(df, use_container_width=True, hide_index=True)
            if st.button("➕ Add All to Avoid List", key="btn_add_auto"):
                for r in auto_results:
                    st.session_state.avoid_list.append({
                        "name": r["name"],
                        "type": r["type"],
                        "reason": r["reason"],
                        "notes": r.get("notes", ""),
                        "added_date": datetime.date.today().isoformat(),
                        "expiry_date": "",
                    })
                st.success(f"Added {len(auto_results)} entries to avoid list.")

# ── Tab 3: Rules ───────────────────────────────────────────────────────
with tab_rules:
    st.markdown("### Avoid List Rules")
    st.caption("Configure automatic rules for the avoid list.")

    st.markdown("""
    The avoid list helps you track situations where betting may be risky:

    | Rule | Description |
    |------|------------|
    | 🤕 **Injury** | Players listed as questionable, doubtful, or out |
    | 📉 **Cold Streak** | Players performing below average over last 5 games |
    | 🏟️ **Tough Matchup** | Players facing elite defensive teams at their position |
    | ⏱️ **Minutes Restriction** | Players on limited minutes (return from injury) |
    | 🔄 **Back-to-Back** | Players in second game of a back-to-back |
    | 💨 **Blowout Risk** | Games with large point spreads (>10 points) |
    | 🤖 **Model Flag** | Players where model accuracy is historically low |
    """)

    st.markdown("#### Custom Rules")
    st.info(
        "Custom avoid rules will be available in a future update. "
        "For now, use the manual entry and auto-detect features."
    )
