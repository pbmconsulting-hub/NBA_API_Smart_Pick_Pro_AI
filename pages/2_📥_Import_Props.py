"""
2_📥_Import_Props — Import prop lines from DFS platforms or CSV files.

Allows users to:
* Paste prop lines from PrizePicks / Underdog / DraftKings / FanDuel
* Upload a CSV of prop lines
* View and manage imported props in session state
"""

import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
for _p in (_ROOT, _ROOT / "SmartPicksProAI", _ROOT / "SmartPicksProAI" / "frontend"):
    if str(_p) not in sys.path:
        sys.path.insert(0, str(_p))

import streamlit as st
import pandas as pd
import csv
import io

st.set_page_config(page_title="Import Props", page_icon="📥", layout="wide")

# ── Session state for imported props ────────────────────────────────────
if "imported_props" not in st.session_state:
    st.session_state.imported_props = []

st.title("📥 Import Props")
st.caption(
    "Import player prop lines from DFS platforms or upload a CSV. "
    "Imported props will be available for analysis across all pages."
)

# ── Platform selection ──────────────────────────────────────────────────
PLATFORMS = ["PrizePicks", "Underdog Fantasy", "DraftKings Pick6", "FanDuel"]

tab_paste, tab_csv, tab_manual, tab_view = st.tabs([
    "📋 Paste Props", "📁 Upload CSV", "✏️ Manual Entry", "👁️ View Imported"
])

# ── Tab 1: Paste Props ─────────────────────────────────────────────────
with tab_paste:
    st.markdown("### Paste Prop Lines")
    st.markdown(
        "Paste prop lines in the format: "
        "`Player Name, Stat Type, Line, Over/Under`  \n"
        "One prop per line."
    )
    platform = st.selectbox("Platform", PLATFORMS, key="paste_platform")
    paste_input = st.text_area(
        "Paste props here",
        height=200,
        placeholder=(
            "LeBron James, Points, 25.5, Over\n"
            "Stephen Curry, Threes, 4.5, Over\n"
            "Nikola Jokic, Rebounds, 12.5, Under"
        ),
        key="paste_props_input",
    )

    if st.button("📥 Import Pasted Props", key="btn_paste"):
        if paste_input.strip():
            lines = paste_input.strip().split("\n")
            imported = 0
            errors = 0
            for line in lines:
                parts = [p.strip() for p in line.split(",")]
                if len(parts) >= 4:
                    try:
                        prop = {
                            "player_name": parts[0],
                            "stat_type": parts[1].lower(),
                            "prop_line": float(parts[2]),
                            "direction": parts[3].upper(),
                            "platform": platform,
                            "source": "paste",
                        }
                        if prop["direction"] in ("OVER", "UNDER"):
                            st.session_state.imported_props.append(prop)
                            imported += 1
                        else:
                            errors += 1
                    except (ValueError, IndexError):
                        errors += 1
                else:
                    errors += 1
            if imported > 0:
                st.success(f"✅ Imported {imported} props from {platform}.")
            if errors > 0:
                st.warning(f"⚠️ {errors} lines could not be parsed.")
        else:
            st.warning("Please paste prop lines first.")

# ── Tab 2: Upload CSV ──────────────────────────────────────────────────
with tab_csv:
    st.markdown("### Upload CSV File")
    st.markdown(
        "CSV should have columns: `player_name`, `stat_type`, `prop_line`, `direction`  \n"
        "Optional columns: `platform`, `team`, `opponent`"
    )

    platform_csv = st.selectbox("Platform", PLATFORMS, key="csv_platform")
    uploaded_file = st.file_uploader(
        "Choose a CSV file",
        type=["csv"],
        key="csv_upload",
    )

    if uploaded_file is not None:
        try:
            df = pd.read_csv(uploaded_file)
            st.dataframe(df, use_container_width=True, hide_index=True)

            required = {"player_name", "stat_type", "prop_line", "direction"}
            if required.issubset(set(df.columns)):
                if st.button("📥 Import CSV Props", key="btn_csv"):
                    imported = 0
                    for _, row in df.iterrows():
                        try:
                            prop = {
                                "player_name": str(row["player_name"]),
                                "stat_type": str(row["stat_type"]).lower(),
                                "prop_line": float(row["prop_line"]),
                                "direction": str(row["direction"]).upper(),
                                "platform": str(row.get("platform", platform_csv)),
                                "team": str(row.get("team", "")),
                                "opponent": str(row.get("opponent", "")),
                                "source": "csv",
                            }
                            if prop["direction"] in ("OVER", "UNDER"):
                                st.session_state.imported_props.append(prop)
                                imported += 1
                        except (ValueError, KeyError):
                            pass
                    st.success(f"✅ Imported {imported} props from CSV.")
            else:
                missing = required - set(df.columns)
                st.error(f"Missing required columns: {', '.join(missing)}")
        except Exception as e:
            st.error(f"Error reading CSV: {e}")

# ── Tab 3: Manual Entry ────────────────────────────────────────────────
with tab_manual:
    st.markdown("### Manual Prop Entry")

    mcols = st.columns(2)
    with mcols[0]:
        player = st.text_input("Player Name", key="manual_player")
        stat = st.selectbox(
            "Stat Type",
            ["points", "rebounds", "assists", "threes", "steals", "blocks",
             "turnovers", "pts+reb", "pts+ast", "reb+ast", "pts+reb+ast"],
            key="manual_stat",
        )
    with mcols[1]:
        line = st.number_input("Prop Line", min_value=0.5, value=20.5, step=0.5, key="manual_line")
        direction = st.selectbox("Direction", ["OVER", "UNDER"], key="manual_dir")

    manual_platform = st.selectbox("Platform", PLATFORMS, key="manual_platform")

    if st.button("➕ Add Prop", key="btn_manual"):
        if player.strip():
            st.session_state.imported_props.append({
                "player_name": player.strip(),
                "stat_type": stat,
                "prop_line": line,
                "direction": direction,
                "platform": manual_platform,
                "source": "manual",
            })
            st.success(f"✅ Added: {player.strip()} {stat} {direction} {line}")
        else:
            st.warning("Please enter a player name.")

# ── Tab 4: View Imported ───────────────────────────────────────────────
with tab_view:
    st.markdown("### Imported Props")

    props = st.session_state.imported_props
    if props:
        df = pd.DataFrame(props)
        st.dataframe(df, use_container_width=True, hide_index=True)
        st.caption(f"Total: {len(props)} props imported")

        col1, col2 = st.columns(2)
        with col1:
            if st.button("🗑️ Clear All Props", key="btn_clear"):
                st.session_state.imported_props = []
                st.rerun()
        with col2:
            csv_data = df.to_csv(index=False)
            st.download_button(
                "📥 Export as CSV",
                data=csv_data,
                file_name="imported_props.csv",
                mime="text/csv",
                key="btn_export",
            )
    else:
        st.info("No props imported yet. Use the tabs above to import props.")
