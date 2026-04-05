"""
8_🔄_Update_Data — Data refresh and update management.

Allows users to:
* Trigger NBA API data refresh
* Run the ML pipeline
* Retrain models
* View data freshness
* Manage cached data
"""

import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
for _p in (_ROOT, _ROOT / "SmartPicksProAI", _ROOT / "SmartPicksProAI" / "frontend"):
    if str(_p) not in sys.path:
        sys.path.insert(0, str(_p))

import streamlit as st  # noqa: E402
import datetime  # noqa: E402

st.set_page_config(page_title="Update Data", page_icon="🔄", layout="wide")

try:
    from SmartPicksProAI.utils.components import inject_joseph_floating  # noqa: E402
except ImportError:
    def inject_joseph_floating() -> None:  # type: ignore[misc]
        pass

try:
    from SmartPicksProAI.frontend.api_service import trigger_refresh
    _API = True
except ImportError:
    _API = False

st.title("🔄 Update Data")
st.caption(
    "Refresh data from the NBA API, run the ML pipeline, and manage "
    "data freshness."
)

tab_refresh, tab_pipeline, tab_status = st.tabs([
    "🔄 Data Refresh", "🔧 ML Pipeline", "📊 Data Status"
])

# ── Tab 1: Data Refresh ────────────────────────────────────────────────
with tab_refresh:
    st.markdown("### NBA API Data Refresh")
    st.markdown(
        "Pull the latest data from the NBA API including games, stats, "
        "rosters, and standings."
    )

    refresh_options = st.multiselect(
        "Select data to refresh",
        [
            "Today's Games",
            "Player Stats (Last 5)",
            "Team Rosters",
            "Team Stats",
            "Standings",
            "League Leaders",
            "Defense vs Position",
            "Schedule",
        ],
        default=["Today's Games", "Player Stats (Last 5)"],
        key="refresh_options",
    )

    if st.button("🔄 Refresh Selected Data", key="btn_refresh", type="primary"):
        if _API:
            with st.spinner("Refreshing data from NBA API…"):
                result = trigger_refresh()
            if result.get("status") == "success":
                st.success(f"✅ {result.get('message', 'Data refreshed!')}")
                st.session_state["last_refresh"] = datetime.datetime.now().isoformat()
            else:
                st.error(f"❌ {result.get('message', 'Refresh failed.')}")
        else:
            st.error("Backend API not available. Start the backend first.")

    st.divider()

    st.markdown("### Full Data Sync")
    st.caption("Pull all data from NBA API. This may take several minutes.")
    if st.button("🔄 Full Sync", key="btn_full_sync"):
        if _API:
            with st.spinner("Running full data sync — this may take several minutes…"):
                result = trigger_refresh()
            if result.get("status") == "success":
                st.success("✅ Full sync complete!")
            else:
                st.error(f"Sync failed: {result.get('message', 'Unknown error')}")
        else:
            st.error("Backend API not available.")

# ── Tab 2: ML Pipeline ─────────────────────────────────────────────────
with tab_pipeline:
    st.markdown("### ML Pipeline Management")
    st.caption("Run the 6-step ML pipeline to process data and retrain models.")

    st.markdown("""
    | Step | Module | Description |
    |------|--------|-------------|
    | 1 | `step_1_ingest` | Ingest raw data from database |
    | 2 | `step_2_clean` | Clean and validate data |
    | 3 | `step_3_features` | Feature engineering |
    | 4 | `step_4_split` | Train/test split |
    | 5 | `step_5_train` | Model training |
    | 6 | `step_6_evaluate` | Model evaluation |
    """)

    pipeline_action = st.selectbox(
        "Action",
        ["Run Full Pipeline", "Run Single Step"],
        key="pipeline_action",
    )

    if pipeline_action == "Run Single Step":
        step = st.selectbox(
            "Select Step",
            ["Step 1: Ingest", "Step 2: Clean", "Step 3: Features",
             "Step 4: Split", "Step 5: Train", "Step 6: Evaluate"],
            key="pipeline_step",
        )

    if st.button("🚀 Run Pipeline", key="btn_pipeline", type="primary"):
        st.info(
            "Pipeline execution requires running from the command line:\n\n"
            "```bash\n"
            "cd SmartPicksProAI && python -m engine.pipeline.run_pipeline\n"
            "```\n\n"
            "Or use the Makefile: `make pipeline`"
        )

    st.divider()

    st.markdown("### Model Training")
    st.caption("Retrain ML models with latest data.")
    if st.button("🎓 Retrain Models", key="btn_retrain"):
        st.info(
            "Model training requires running from the command line:\n\n"
            "```bash\n"
            "cd SmartPicksProAI && python -m engine.models.train\n"
            "```\n\n"
            "Or use the Makefile: `make train`"
        )

# ── Tab 3: Data Status ─────────────────────────────────────────────────
with tab_status:
    st.markdown("### Data Freshness")

    last_refresh = st.session_state.get("last_refresh")
    if last_refresh:
        st.metric("Last Refresh", last_refresh)
    else:
        st.metric("Last Refresh", "Unknown")

    st.divider()

    st.markdown("### Data Directory Status")

    data_dirs = {
        "Raw Data": _ROOT / "SmartPicksProAI" / "data" / "raw",
        "Processed Data": _ROOT / "SmartPicksProAI" / "data" / "processed",
        "ML Ready": _ROOT / "SmartPicksProAI" / "data" / "ml_ready",
        "Seed Data": _ROOT / "data",
        "Database": _ROOT / "SmartPicksProAI" / "db",
        "Saved Models": _ROOT / "SmartPicksProAI" / "engine" / "models" / "saved",
    }

    for name, path in data_dirs.items():
        exists = path.exists()
        icon = "✅" if exists else "❌"
        if exists and path.is_dir():
            files = list(path.iterdir())
            st.markdown(f"{icon} **{name}**: `{path}` — {len(files)} files")
        elif exists:
            st.markdown(f"{icon} **{name}**: `{path}` — file exists")
        else:
            st.markdown(f"{icon} **{name}**: `{path}` — not found")

    st.divider()

    st.markdown("### Cache Management")
    if st.button("🗑️ Clear Streamlit Cache", key="btn_clear_cache"):
        st.cache_data.clear()
        st.success("Streamlit cache cleared.")

# ── Floating Joseph widget ──────────────────────────────────────────────
inject_joseph_floating()
