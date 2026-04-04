"""
7_⚙️_Settings — App-wide settings and configuration management.

Allows users to configure:
* Default platform
* Simulation iterations
* Kelly fraction / bankroll
* Display preferences
* API keys (masked)
"""

import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
for _p in (_ROOT, _ROOT / "SmartPicksProAI", _ROOT / "SmartPicksProAI" / "frontend"):
    if str(_p) not in sys.path:
        sys.path.insert(0, str(_p))

import streamlit as st
import json

st.set_page_config(page_title="Settings", page_icon="⚙️", layout="wide")

# ── Settings persistence ────────────────────────────────────────────────
_SETTINGS_DIR = Path.home() / ".smartpickpro"
_SETTINGS_FILE = _SETTINGS_DIR / "settings.json"


def _load_settings() -> dict:
    """Load settings from disk."""
    defaults = {
        "default_platform": "PrizePicks",
        "simulation_iterations": 5000,
        "kelly_fraction": 0.25,
        "auto_log_bets": True,
        "bankroll": 500.0,
        "display_theme": "Dark",
        "show_risk_flags": True,
        "show_explanation": True,
        "api_backend_url": "http://localhost:8098",
        "odds_api_key": "",
    }
    try:
        if _SETTINGS_FILE.exists():
            data = json.loads(_SETTINGS_FILE.read_text())
            defaults.update(data)
    except Exception:
        pass
    return defaults


def _save_settings(data: dict) -> bool:
    """Save settings to disk."""
    try:
        _SETTINGS_DIR.mkdir(parents=True, exist_ok=True)
        _SETTINGS_FILE.write_text(json.dumps(data, indent=2))
        return True
    except Exception:
        return False


settings = _load_settings()

st.title("⚙️ Settings")
st.caption("Configure SmartPicksProAI preferences and parameters.")

tab_general, tab_analysis, tab_display, tab_advanced = st.tabs([
    "🔧 General", "🎯 Analysis", "🎨 Display", "🔬 Advanced"
])

# ── Tab 1: General Settings ────────────────────────────────────────────
with tab_general:
    st.markdown("### General Settings")

    platform = st.selectbox(
        "Default Platform",
        ["PrizePicks", "Underdog Fantasy", "DraftKings Pick6", "FanDuel"],
        index=["PrizePicks", "Underdog Fantasy", "DraftKings Pick6", "FanDuel"].index(
            settings.get("default_platform", "PrizePicks")
        ),
        key="set_platform",
        help="Default platform for prop analysis and bet logging.",
    )

    bankroll = st.number_input(
        "Default Bankroll ($)",
        min_value=10.0,
        value=float(settings.get("bankroll", 500.0)),
        step=50.0,
        key="set_bankroll",
    )

    auto_log = st.checkbox(
        "Auto-log bets from analysis",
        value=settings.get("auto_log_bets", True),
        key="set_auto_log",
        help="Automatically log bets when you run prop analysis.",
    )

    backend_url = st.text_input(
        "Backend API URL",
        value=settings.get("api_backend_url", "http://localhost:8098"),
        key="set_backend_url",
    )

# ── Tab 2: Analysis Settings ──────────────────────────────────────────
with tab_analysis:
    st.markdown("### Analysis Configuration")

    sim_iters = st.number_input(
        "Simulation Iterations",
        min_value=1000,
        max_value=50000,
        value=int(settings.get("simulation_iterations", 5000)),
        step=1000,
        key="set_sim_iters",
        help="More iterations = more accurate but slower.",
    )

    kelly = st.slider(
        "Kelly Fraction",
        min_value=0.05,
        max_value=1.0,
        value=float(settings.get("kelly_fraction", 0.25)),
        step=0.05,
        key="set_kelly",
        help="0.25 = quarter-Kelly (conservative). 1.0 = full Kelly (aggressive).",
    )

    st.markdown("#### Confidence Tier Thresholds")
    st.caption("Adjust the confidence score thresholds for each tier.")

    tier_cols = st.columns(4)
    platinum_t = tier_cols[0].number_input("💎 Platinum", value=84, key="set_t_plat")
    gold_t = tier_cols[1].number_input("🥇 Gold", value=65, key="set_t_gold")
    silver_t = tier_cols[2].number_input("🥈 Silver", value=57, key="set_t_silver")
    bronze_t = tier_cols[3].number_input("🥉 Bronze", value=45, key="set_t_bronze")

    st.markdown("#### Minimum Edge per Tier (%)")
    edge_cols = st.columns(4)
    edge_plat = edge_cols[0].number_input("Platinum", value=10.0, step=0.5, key="set_e_plat")
    edge_gold = edge_cols[1].number_input("Gold", value=7.0, step=0.5, key="set_e_gold")
    edge_silver = edge_cols[2].number_input("Silver", value=3.0, step=0.5, key="set_e_silver")
    edge_bronze = edge_cols[3].number_input("Bronze", value=1.0, step=0.5, key="set_e_bronze")

# ── Tab 3: Display Settings ───────────────────────────────────────────
with tab_display:
    st.markdown("### Display Preferences")

    show_flags = st.checkbox(
        "Show risk flags",
        value=settings.get("show_risk_flags", True),
        key="set_show_flags",
    )

    show_explanation = st.checkbox(
        "Show AI explanation",
        value=settings.get("show_explanation", True),
        key="set_explanation",
    )

    theme = st.selectbox(
        "Theme",
        ["Dark", "Light"],
        index=["Dark", "Light"].index(settings.get("display_theme", "Dark")),
        key="set_theme",
    )

# ── Tab 4: Advanced ───────────────────────────────────────────────────
with tab_advanced:
    st.markdown("### Advanced Settings")

    st.markdown("#### API Keys")
    st.caption("API keys are stored locally and never transmitted.")
    odds_key = st.text_input(
        "Odds API Key",
        value=settings.get("odds_api_key", ""),
        type="password",
        key="set_odds_key",
        help="Get a key from https://the-odds-api.com",
    )

    st.divider()
    st.markdown("#### Database")
    st.caption("Database management options.")
    if st.button("🗑️ Reset Bet Tracker Database", key="btn_reset_db"):
        st.warning("This will delete all bet history. Are you sure?")
        if st.button("Yes, reset database", key="btn_confirm_reset"):
            try:
                from SmartPicksProAI.tracking.database import initialize_database
                initialize_database()
                st.success("Database reset successfully.")
            except Exception as e:
                st.error(f"Error: {e}")

    st.divider()
    st.markdown("#### Current Configuration")
    st.json(settings)

# ── Save Button ─────────────────────────────────────────────────────────
st.divider()
col1, col2 = st.columns([1, 3])
with col1:
    if st.button("💾 Save Settings", key="btn_save_settings", type="primary"):
        new_settings = {
            "default_platform": platform,
            "bankroll": bankroll,
            "auto_log_bets": auto_log,
            "api_backend_url": backend_url,
            "simulation_iterations": sim_iters,
            "kelly_fraction": kelly,
            "show_risk_flags": show_flags,
            "show_explanation": show_explanation,
            "display_theme": theme,
            "odds_api_key": odds_key,
            "confidence_tiers": {
                "platinum": platinum_t,
                "gold": gold_t,
                "silver": silver_t,
                "bronze": bronze_t,
            },
            "min_edge": {
                "platinum": edge_plat,
                "gold": edge_gold,
                "silver": edge_silver,
                "bronze": edge_bronze,
            },
        }
        if _save_settings(new_settings):
            st.session_state.user_bankroll = bankroll
            st.session_state.settings = new_settings
            st.success("✅ Settings saved!")
        else:
            st.error("Failed to save settings.")
with col2:
    if st.button("↩️ Reset to Defaults", key="btn_reset_settings"):
        if _SETTINGS_FILE.exists():
            _SETTINGS_FILE.unlink()
        st.success("Settings reset to defaults.")
        st.rerun()
