# ============================================================
# FILE: pages/3_⚡_Quantum_Analysis_Matrix.py
# PURPOSE: The main analysis page. Runs Quantum Matrix Engine 5.6 simulation
#          for each prop and shows probability, edge, tier, and
#          directional forces in the Quantum Design System (QDS) UI.
# CONNECTS TO: engine/ (all modules), data_manager.py, session state
# ============================================================

import streamlit as st  # Main UI framework
import streamlit.components.v1 as _components  # For Full Breakdown iframe rendering
import math             # For rounding in display
import html as _html   # For safe HTML escaping in inline cards
import datetime         # For analysis result freshness timestamps
import time             # For elapsed-time measurement
import os               # For logo path resolution

try:
    from utils.logger import get_logger
    _logger = get_logger(__name__)
except ImportError:
    import logging
    _logger = logging.getLogger(__name__)

# Import our engine modules
from engine.simulation import (
    run_quantum_matrix_simulation,
    build_histogram_from_results,
    simulate_combo_stat,
    simulate_fantasy_score,
    simulate_double_double,
    simulate_triple_double,
    generate_alt_line_probabilities,
)
from engine import COMBO_STAT_TYPES, FANTASY_STAT_TYPES, YESNO_STAT_TYPES
from engine.projections import build_player_projection, get_stat_standard_deviation, calculate_teammate_out_boost, POSITION_PRIORS
from engine.edge_detection import analyze_directional_forces, should_avoid_prop, detect_correlated_props, detect_trap_line, detect_line_sharpness, classify_bet_type, calculate_composite_win_score

try:
    from engine.rotation_tracker import track_minutes_trend
    _rotation_tracker_available = True
except ImportError:
    _rotation_tracker_available = False
from engine.confidence import calculate_confidence_score, get_tier_color
from engine.math_helpers import calculate_edge_percentage, clamp_probability
from engine.explainer import generate_pick_explanation
from engine.odds_engine import american_odds_to_implied_probability as _odds_to_implied_prob
from engine.calibration import get_calibration_adjustment   # C10: historical calibration
from engine.clv_tracker import store_opening_line, get_stat_type_clv_penalties  # C12: CLV + penalties

try:
    from engine.market_movement import detect_line_movement  # F9: Sharp money
except ImportError:
    detect_line_movement = None

try:
    from engine.matchup_history import calculate_matchup_adjustment, get_matchup_force_signal  # F2
except ImportError:
    calculate_matchup_adjustment = None
    get_matchup_force_signal = None

try:
    from engine.ensemble import get_ensemble_projection  # Ensemble 3-model blend
    _ensemble_available = True
except ImportError:
    _ensemble_available = False
    get_ensemble_projection = None

try:
    from engine.game_script import simulate_game_script, blend_with_flat_simulation  # Game script blend
    _game_script_available = True
except ImportError:
    _game_script_available = False
    simulate_game_script = None
    blend_with_flat_simulation = None

try:
    from engine.minutes_model import project_player_minutes  # Precise minutes projection
    _minutes_model_available = True
except ImportError:
    _minutes_model_available = False
    project_player_minutes = None

# Import data loading functions
from data.data_manager import (
    load_players_data,
    load_defensive_ratings_data,
    load_teams_data,
    find_player_by_name,
    load_props_from_session,
    get_roster_health_report,
    validate_props_against_roster,
    get_player_status,
    get_status_badge_html,
    load_injury_status,
    filter_props_to_platform_players,
)

# Import the theme helpers — including new QDS generators
from styles.theme import (
    get_global_css,
    get_logo_img_tag,
    get_roster_health_html,
    get_best_bets_section_html,
    get_qds_css,
    get_qds_confidence_bar_html,
    get_qds_metrics_grid_html,
    get_qds_prop_card_html,
    get_qds_matchup_header_html,
    get_qds_team_card_html,
    get_qds_strategy_table_html,
    get_qds_framework_logic_html,
    get_qds_final_verdict_html,
    get_education_box_html,
    GLOSSARY,
)

from data.platform_mappings import COMBO_STATS, FANTASY_SCORING
from data.sportsbook_service import smart_filter_props as _smart_filter_props
from utils.renderers import compile_card_matrix as _compile_card_matrix
from utils.renderers import compile_unified_card_matrix as _compile_unified_matrix
from utils.renderers import build_horizontal_card_html as _build_h_card
from styles.theme import get_quantum_card_matrix_css as _get_qcm_css

# ── Glassmorphic Trading-Card imports ────────────────────────────────────────
from styles.theme import get_glassmorphic_card_css as _get_gm_css
from styles.theme import get_player_trading_card_html as _get_trading_card_html
from utils.data_grouper import group_props_by_player as _group_props
from utils.player_modal import show_player_spotlight as _show_spotlight

# ── Section logo paths ────────────────────────────────────────────────────────
# Logos are stored in assets/ and loaded via st.image() for efficient serving.
_ASSETS_DIR      = os.path.join(os.path.dirname(os.path.dirname(__file__)), "assets")
# Legacy logo paths disabled – branding removed from UI
_GOLD_LOGO_PATH   = os.path.join(_ASSETS_DIR, "NewGold_Logo.png")


# ── Iframe card renderer ─────────────────────────────────────────────────────
# Renders the unified card matrix inside a self-resizing <iframe> via
# streamlit.components.v1.html() instead of st.markdown(unsafe_allow_html=True).
#
# Why this is more resilient than inline st.markdown:
#   1. Atomic delivery — the iframe either loads fully or not at all; a
#      mid-render WebSocket closure does not crash the page.
#   2. CSS isolation — card styles cannot leak into (or be affected by) the
#      main Streamlit page.
#   3. Self-resizing — a ResizeObserver + toggle listener adjusts the iframe
#      height when <details> cards are expanded / collapsed, so no fixed
#      height or scroll-bar is needed.
# ---------------------------------------------------------------------------

_MIN_IFRAME_HEIGHT = 400       # px — minimum even for a single player
_HEIGHT_PER_PLAYER = 200       # px — collapsed card ≈ 180 px + padding
_MAX_IFRAME_HEIGHT = 3000      # px — cap before ResizeObserver takes over
_RESIZE_DEBOUNCE_MS = 50       # ms — debounce rapid ResizeObserver events

# Auto-resize JavaScript injected into every card-matrix iframe.
# Sends ``streamlit:setFrameHeight`` postMessages so Streamlit adjusts
# the iframe height whenever the content changes (e.g. <details> toggle).
_IFRAME_RESIZE_JS = (
    "<script>"
    "(function(){"
    "var timer;"
    "function sendHeight(){clearTimeout(timer);timer=setTimeout(function(){"
    "window.parent.postMessage({type:'streamlit:setFrameHeight',"
    f"height:document.body.scrollHeight}},'*')}},{_RESIZE_DEBOUNCE_MS})}}"
    "sendHeight();new ResizeObserver(sendHeight).observe(document.body);"
    "document.addEventListener('toggle',sendHeight,true);"
    "window.addEventListener('load',sendHeight)"
    "})()"
    "</script>"
)


def _render_card_iframe(card_html, player_count):
    """Render *card_html* inside a self-resizing iframe.

    Parameters
    ----------
    card_html : str
        Complete HTML (including ``<style>`` blocks) returned by
        :func:`utils.renderers.compile_unified_card_matrix`.
    player_count : int
        Number of player groups — used to estimate the initial iframe
        height before the ``ResizeObserver`` adjusts it.
    """
    _est_h = max(_MIN_IFRAME_HEIGHT, min(player_count * _HEIGHT_PER_PLAYER, _MAX_IFRAME_HEIGHT))
    _doc = (
        "<!DOCTYPE html><html><head>"
        '<meta charset="utf-8">'
        '<meta name="viewport" content="width=device-width,initial-scale=1">'
        "<style>html{overflow:hidden}"
        "body{margin:0;padding:0;background:transparent;color:#e0e0e0}</style>"
        "</head><body>"
        f"{card_html}"
        f"{_IFRAME_RESIZE_JS}"
        "</body></html>"
    )
    _components.html(_doc, height=_est_h, scrolling=False)


st.set_page_config(
    page_title="Neural Analysis — SmartBetPro NBA",
    page_icon="⚡",
    layout="wide",
)

# Inject global CSS + QDS CSS
st.markdown(get_global_css(), unsafe_allow_html=True)
st.markdown(get_qds_css(), unsafe_allow_html=True)
st.markdown(_get_gm_css(), unsafe_allow_html=True)

# ── Global Settings Popover (accessible from sidebar) ─────────
from utils.components import render_global_settings, inject_joseph_floating, render_joseph_hero_banner
with st.sidebar:
    render_global_settings()
st.session_state["joseph_page_context"] = "page_analysis"
inject_joseph_floating()
render_joseph_hero_banner()

# ── Premium Status (partial gate — free users capped at 3 props) ──
from utils.auth import is_premium_user as _is_premium_user
try:
    from utils.stripe_manager import _PREMIUM_PAGE_PATH as _PREM_PATH
except Exception:
    _PREM_PATH = "/14_%F0%9F%92%8E_Subscription_Level"
_FREE_ANALYSIS_LIMIT = 3   # Free users can analyze up to 3 props
_user_is_premium = _is_premium_user()
if "selected_picks" not in st.session_state:
    st.session_state["selected_picks"] = []
if "injury_status_map" not in st.session_state:
    st.session_state["injury_status_map"] = load_injury_status()

st.session_state.setdefault("joseph_enabled", True)
st.session_state.setdefault("joseph_used_fragments", set())
st.session_state.setdefault("joseph_bets_logged", False)

# ── Analysis Session Persistence — Rehydrate from DB if session empty ──────
# If the user's session state has no analysis results (e.g. after inactivity
# or a page refresh), reload the most recently saved session from SQLite so
# they never have to re-run analysis just because time passed.
if not st.session_state.get("analysis_results"):
    try:
        from tracking.database import load_latest_analysis_session as _load_session
        _saved_session = _load_session()
        if _saved_session and _saved_session.get("analysis_results"):
            st.session_state["analysis_results"] = _saved_session["analysis_results"]
            if _saved_session.get("todays_games") and not st.session_state.get("todays_games"):
                st.session_state["todays_games"] = _saved_session["todays_games"]
            if _saved_session.get("selected_picks") and not st.session_state.get("selected_picks"):
                st.session_state["selected_picks"] = _saved_session["selected_picks"]
            # Record the timestamp so the UI can show when the session was saved
            st.session_state["_analysis_session_reloaded_at"] = _saved_session.get("analysis_timestamp", "")
    except Exception:
        pass  # Non-fatal — just show empty state

# ─── Auto-refresh injury data if empty or stale (>4 hours) ──
# Use a 30-minute in-session cooldown to avoid re-loading on every
# page navigation, while still updating when data is genuinely stale.
_INJURY_STALE_HOURS = 4
_INJURY_REFRESH_COOLDOWN_SECS = 1800  # 30 minutes

_should_auto_refresh_injuries = not st.session_state["injury_status_map"]
if not _should_auto_refresh_injuries:
    # Check if we already refreshed recently in this session
    _last_refresh_ts = st.session_state.get("_injury_last_refreshed_at")
    if _last_refresh_ts is not None:
        import time as _time_mod
        _mins_since = (_time_mod.time() - _last_refresh_ts) / 60
        if _mins_since < 30:
            _should_auto_refresh_injuries = False
        else:
            # Been 30+ minutes since last refresh — re-check file age
            try:
                import datetime as _dt
                from pathlib import Path as _Path
                _inj_json_path = _Path(__file__).parent.parent / "data" / "injury_status.json"
                if _inj_json_path.exists():
                    _inj_age_hours = (
                        _dt.datetime.now().timestamp() - _inj_json_path.stat().st_mtime
                    ) / 3600.0
                    _should_auto_refresh_injuries = _inj_age_hours > _INJURY_STALE_HOURS
            except Exception:
                pass
    else:
        # No record of a refresh this session — check file age
        try:
            import datetime as _dt
            from pathlib import Path as _Path
            _inj_json_path = _Path(__file__).parent.parent / "data" / "injury_status.json"
            if _inj_json_path.exists():
                _inj_age_hours = (
                    _dt.datetime.now().timestamp() - _inj_json_path.stat().st_mtime
                ) / 3600.0
                _should_auto_refresh_injuries = _inj_age_hours > _INJURY_STALE_HOURS
        except Exception:
            pass  # Staleness check is best-effort

if _should_auto_refresh_injuries:
    try:
        import time as _time_mod
        from data.roster_engine import RosterEngine as _RosterEngine
        _re = _RosterEngine()
        _re.refresh()
        _scraped_inj = _re.get_injury_report()
        if _scraped_inj:
            _auto_status_map = {
                _k: {
                    "status":        _v.get("status", "Active"),
                    "injury_note":   _v.get("injury", ""),
                    "games_missed":  0,
                    "return_date":   _v.get("return_date", ""),
                    "last_game_date": "",
                    "gp_ratio":      1.0,
                    "injury":        _v.get("injury", ""),
                    "source":        _v.get("source", ""),
                    "comment":       _v.get("comment", ""),
                }
                for _k, _v in _scraped_inj.items()
            }
            st.session_state["injury_status_map"] = _auto_status_map
        # Record this refresh so subsequent page navigations skip it
        st.session_state["_injury_last_refreshed_at"] = _time_mod.time()
    except Exception:
        pass  # Non-fatal — analysis page works without auto-refresh

# ============================================================
# END SECTION: Page Setup
# ============================================================


# ============================================================
# SECTION: Helper Functions (extracted to pages/helpers/neural_analysis_helpers.py)
# ============================================================
from pages.helpers.neural_analysis_helpers import (
    find_game_context_for_player,
    _build_result_metrics,
    _build_bonus_factors,
    _build_entry_strategy,
    _render_qds_full_breakdown_html,
    render_inline_breakdown_html as _render_inline_breakdown,
    display_prop_analysis_card_qds,
)
# ============================================================
# END SECTION: Helper Functions
# ============================================================

# ============================================================
# SECTION: Load All Required Data
# ============================================================

players_data           = load_players_data()
teams_data             = load_teams_data()
defensive_ratings_data = load_defensive_ratings_data()

current_props  = load_props_from_session(st.session_state)
todays_games   = st.session_state.get("todays_games", [])

# ── Safety net: enrich with alt-line categories if missing ──────
# Props saved before the enrichment pipeline was wired may lack
# line_category.  Re-enrich to stamp all props as "standard".
if current_props and not any(p.get("line_category") for p in current_props):
    try:
        from data.sportsbook_service import parse_alt_lines_from_platform_props
        current_props = parse_alt_lines_from_platform_props(current_props)
    except ImportError:
        _logger.warning("parse_alt_lines_from_platform_props unavailable — line categories may be missing")
simulation_depth = st.session_state.get("simulation_depth", 2000)
minimum_edge     = st.session_state.get("minimum_edge_threshold", 5.0)

# ============================================================
# END SECTION: Load All Required Data
# ============================================================

# ============================================================
# SECTION: QDS Page Header
# ============================================================

st.markdown(
    '<h2 style="font-family:\'Orbitron\',sans-serif;color:#00ffd5;'
    'margin-bottom:4px;">⚡ Neural Analysis</h2>'
    '<p style="color:#a0b4d0;margin-top:0;">SmartBetPro Quantum Matrix Engine 5.6 — Powered by N.A.N. (Neural Analysis Network) — '
    'Quantum Matrix Engine 5.6 Prop Analysis with Quantum Design System</p>',
    unsafe_allow_html=True,
)

with st.expander("📖 How to Use This Page", expanded=False):
    st.markdown("""
    ### Neural Analysis — Understanding Your Results
    
    **Running Analysis:**
    1. Ensure props are loaded (from Prop Scanner or Live Games page)
    2. Click "▶️ Run Analysis" to start Quantum Matrix Engine 5.6 simulation
    3. Each prop is simulated 1,000+ times to calculate probability
    
    **Reading Results:**
    - **Probability**: % chance the stat goes MORE than the line (>55% = meaningful edge)
    - **Edge**: How much better than 50/50 your edge is (higher = better value)
    - **Confidence Score**: 0-100 composite score (70+ = high confidence)
    - **Tier**: Platinum (85+) > Gold (70+) > Silver (55+) > Bronze
    
    **Directional Forces:**
    - Green arrows = factors pushing MORE (weak defense, fast pace, etc.)
    - Red arrows = factors pushing LESS (tough defense, injury risk, etc.)
    
    **Ensemble Model 🧬:**
    - When game logs are available, the 3-model ensemble is used (season avg + recent form + matchup history)
    - `🧬 Ensemble` badge on result cards means blended projection was used
    - Model disagreement reduces confidence automatically
    
    💡 **Pro Tips:**
    - Focus on Platinum and Gold tier picks for best results
    - Avoid picks with ⚠️ "should avoid" flags
    - Select picks to send to Entry Builder for parlay optimization
    - Run **Smart Update** on the Data Feed page before each session for freshest data
    """)

# ── Data Freshness Banner ─────────────────────────────────────
# Show a warning if players.csv is stale (>24 hours old).
# Stale data is the #1 source of inaccurate projections.
try:
    import os as _os_check
    _players_csv = _os_check.path.join(
        _os_check.path.dirname(_os_check.path.dirname(__file__)), "data", "players.csv"
    )
    if _os_check.path.exists(_players_csv):
        _players_age_h = (
            datetime.datetime.now().timestamp()
            - _os_check.path.getmtime(_players_csv)
        ) / 3600.0
        if _players_age_h > 48:
            st.error(
                f"🚨 **Player data is {_players_age_h:.0f}h old!** "
                "Go to **📡 Data Feed** → Smart Update to refresh. "
                "Stale data severely reduces projection accuracy."
            )
        elif _players_age_h > 24:
            st.warning(
                f"⚠️ **Player data is {_players_age_h:.0f}h old.** "
                "Consider running a **Smart Update** on the Data Feed page for the most accurate projections."
            )
        elif _players_age_h < 2:
            st.success(
                f"✅ Player data is fresh ({_players_age_h*60:.0f} minutes old). "
                "Projections are using current stats."
            )
except Exception:
    pass  # Non-critical check

# ── Matchup header (shown when games are configured) ──────────
if len(todays_games) == 1:
    g = todays_games[0]
    st.markdown(
        get_qds_matchup_header_html(
            away_team=g.get("away_team", "AWAY"),
            home_team=g.get("home_team", "HOME"),
            game_info=(
                f"Tonight · Tip-off: {g.get('game_time', '')}"
                if g.get("game_time") else "Tonight"
            ),
        ),
        unsafe_allow_html=True,
    )
elif len(todays_games) > 1:
    # Multiple games — show count instead of a single matchup header
    st.markdown(
        f'<div style="background:rgba(0,255,213,0.06);border-radius:8px;'
        f'padding:10px 16px;border:1px solid rgba(0,255,213,0.12);'
        f'color:#a0b4d0;font-size:0.85rem;margin-bottom:12px;">'
        f'🏀 {len(todays_games)} games configured for tonight.'
        f'</div>',
        unsafe_allow_html=True,
    )

# ============================================================
# END SECTION: QDS Page Header
# ============================================================

# ============================================================
# SECTION: Pre-Run Status Check
# ============================================================

status_col, settings_col = st.columns([2, 1])

with status_col:
    if current_props:
        # Show per-platform breakdown of loaded props
        _prerun_plat_counts: dict = {}
        for _pp in current_props:
            _plat = _pp.get("platform", "Unknown")
            _prerun_plat_counts[_plat] = _prerun_plat_counts.get(_plat, 0) + 1
        _plat_detail = " · ".join(
            f"**{_pl}**: {_n}" for _pl, _n in sorted(_prerun_plat_counts.items())
        )
        st.info(
            f"📋 **{len(current_props)} props** loaded and ready for analysis — {_plat_detail}."
        )
    else:
        st.warning(
            "⚠️ No props loaded. Go to **🔬 Prop Scanner** → "
            "**🤖 Auto-Generate Props for Tonight's Games** or import props manually."
        )

    if todays_games:
        st.success(f"🏟️ **{len(todays_games)} game(s)** configured for tonight.")
    else:
        st.warning(
            "⚠️ No games loaded for today. Click **🔄 Auto-Load Tonight's Games** "
            "on the Live Games page first to ensure only tonight's players are analyzed."
        )

    if current_props and players_data:
        validation     = validate_props_against_roster(current_props, players_data)
        total          = validation["total"]
        matched_count  = validation["matched_count"]

        if validation["unmatched"] or validation["fuzzy_matched"]:
            with st.expander(
                f"⚠️ Roster Health: {matched_count}/{total} players matched "
                f"({int(matched_count / max(total, 1) * 100)}%) — click to see details"
            ):
                st.markdown(
                    get_roster_health_html(
                        validation["matched"],
                        validation["fuzzy_matched"],
                        validation["unmatched"],
                    ),
                    unsafe_allow_html=True,
                )
        else:
            st.success(f"✅ All {total} players matched in database.")

with settings_col:
    st.caption(f"⚙️ Simulations: **{simulation_depth:,}**")
    st.caption(f"⚙️ Min Edge: **{minimum_edge}%**")
    _shown_platforms = st.session_state.get("selected_platforms", ["PrizePicks", "Underdog Fantasy", "DraftKings Pick6"])
    st.caption(f"⚙️ Platforms: **{', '.join(_shown_platforms)}**")
    st.caption("Change on the ⚙️ Settings page")

# ============================================================
# END SECTION: Pre-Run Status Check
# ============================================================

st.divider()

# ── Framework Logic (collapsible) ──────────────────────────────
with st.expander("📖 How Neural Analysis Works — Framework Logic"):
    st.markdown(get_qds_framework_logic_html(), unsafe_allow_html=True)

# ============================================================
# SECTION: Prop Pool (all available props passed to engine)
# ============================================================

# All available props are sent to the engine — no stat-type filtering or
# intake cap.  The analysis loop will process every prop until all are
# exhausted, outputting as many high-confidence bets as possible.
final_props = list(current_props)

st.metric(
    label="⚡ PROPS IN POOL",
    value=f"{len(final_props):,}",
)

# ============================================================
# END SECTION: Prop Pool
# ============================================================

# ============================================================
# SECTION: Analysis Runner
# ============================================================

run_col, show_col = st.columns([1, 2])

with run_col:
    run_analysis = st.button(
        "🚀 Run Analysis",
        type="primary",
        width="stretch",
        disabled=(len(final_props) == 0),
        help="Analyze all loaded props with Quantum Matrix Engine 5.6",
    )

with show_col:
    show_all_or_top = st.radio(
        "Show:",
        ["All picks", "Top picks only (edge ≥ threshold)"],
        horizontal=True,
        index=0,
    )

if run_analysis:
    _analysis_start_time = time.time()
    progress_bar         = st.progress(0, text="Starting analysis...")
    analysis_results_list = []

    # Clear stale Joseph results so fresh ones are generated after this run.
    st.session_state.pop("joseph_results", None)
    st.session_state["joseph_bets_logged"] = False

    try:
        # ── Filter props to only tonight's teams (with abbreviation aliases) ──
        # Build expanded playing-teams set that covers all known alias variants
        # (e.g. "GS" ↔ "GSW", "NY" ↔ "NYK") so team-abbreviation mismatches
        # don't silently drop valid props.
        ABBREV_ALIASES = {
            "GS": "GSW", "GSW": "GS",
            "NY": "NYK", "NYK": "NY",
            "NO": "NOP", "NOP": "NO",
            "SA": "SAS", "SAS": "SA",
            "UTAH": "UTA", "UTA": "UTAH",
            "WSH": "WAS", "WAS": "WSH",
            "BKN": "BRK", "BRK": "BKN",
            "PHX": "PHO", "PHO": "PHX",
            "CHA": "CHO", "CHO": "CHA",
            "NJ": "BKN",
        }

        # Full team name → abbreviation mapping for platform props that use
        # full names or nicknames instead of standard 3-letter codes.
        try:
            from data.nba_data_service import TEAM_NAME_TO_ABBREVIATION as _TEAM_FULL_MAP
        except ImportError:
            _TEAM_FULL_MAP = {}

        # Build reverse lookups: nickname → abbrev
        _TEAM_NICKNAME_MAP: dict = {}   # e.g. "LAKERS" → "LAL"
        for _full_name, _abbr in _TEAM_FULL_MAP.items():
            parts = _full_name.rsplit(" ", 1)
            if len(parts) == 2:
                _TEAM_NICKNAME_MAP[parts[1].upper()] = _abbr
            _TEAM_NICKNAME_MAP[_full_name.upper()] = _abbr

        def _normalize_team_to_abbrev(raw_team: str) -> str:
            """Convert any team representation to a standard abbreviation.

            Handles: 3-letter codes, full names, nicknames, common aliases.
            Returns the uppercased team string unchanged if no mapping found.
            """
            team_upper = raw_team.upper().strip()
            if not team_upper:
                return ""
            # Already a known abbreviation or alias?
            if len(team_upper) <= 4:
                return ABBREV_ALIASES.get(team_upper, team_upper)
            # Full name or nickname match? (e.g. "Los Angeles Lakers", "Lakers")
            mapped = _TEAM_NICKNAME_MAP.get(team_upper)
            if mapped:
                return mapped
            # Last word might be nickname (e.g. "LA Lakers" → "Lakers")
            last_word = team_upper.rsplit(" ", 1)[-1] if " " in team_upper else ""
            if last_word:
                mapped = _TEAM_NICKNAME_MAP.get(last_word)
                if mapped:
                    return mapped
            return team_upper

        playing_teams_expanded: set = set()
        for _g in todays_games:
            for _abbrev in (
                _g.get("home_team", "").upper().strip(),
                _g.get("away_team", "").upper().strip(),
            ):
                if not _abbrev:
                    continue
                playing_teams_expanded.add(_abbrev)
                # Add known alias for this abbreviation (if any)
                _alias = ABBREV_ALIASES.get(_abbrev)
                if _alias:
                    playing_teams_expanded.add(_alias)
        playing_teams_expanded.discard("")

        if playing_teams_expanded:
            props_to_analyze = [
                p for p in final_props
                if (
                    _normalize_team_to_abbrev(p.get("team", "")) in playing_teams_expanded
                    or not p.get("team", "").strip()  # include props with no team set
                )
            ]
            skipped_count = len(final_props) - len(props_to_analyze)
            if skipped_count > 0:
                st.info(
                    f"ℹ️ Skipping **{skipped_count}** prop(s) for teams not playing tonight. "
                    f"Analyzing **{len(props_to_analyze)}** prop(s) for tonight's {len(todays_games)} game(s)."
                )

            # ── Fallback: if ALL props were filtered out, analyze them all ──
            # This prevents a dead-end where a team-name format mismatch
            # between platforms and the games list silently drops every prop.
            if len(props_to_analyze) == 0 and len(final_props) > 0:
                st.warning(
                    f"⚠️ All **{len(final_props)}** props were filtered out by tonight's team list. "
                    f"This usually means the team names in your props don't match the loaded games. "
                    f"**Proceeding with all {len(final_props)} props** so your analysis isn't blocked."
                )
                props_to_analyze = list(final_props)
        else:
            props_to_analyze = list(final_props)  # Fallback: no games loaded

        # ── Also skip confirmed Out/IR/Doubtful players via injury map ─────
        # If injury_map_pre is empty (failed to load), do NOT filter — just proceed.
        injury_map_pre = st.session_state.get("injury_status_map", {})
        _INACTIVE_STATUSES = frozenset({
            "Out", "Doubtful", "Injured Reserve", "Out (No Recent Games)",
            "Suspended", "Not With Team",
            "G League - Two-Way", "G League - On Assignment", "G League",
        })
        if injury_map_pre:
            before_inj = len(props_to_analyze)
            props_to_analyze = [
                p for p in props_to_analyze
                if injury_map_pre.get(
                    p.get("player_name", "").lower().strip(), {}
                ).get("status", "Active") not in _INACTIVE_STATUSES
            ]
            inj_skipped = before_inj - len(props_to_analyze)
            if inj_skipped > 0:
                st.info(f"ℹ️ Skipping **{inj_skipped}** prop(s) for confirmed Out/IR/Doubtful players.")

        # ── Filter to only players on real betting platforms ───────────────
        # If live platform props have been loaded, drop synthetic-only props
        # for players who don't appear on any real betting app today.
        _live_platform_props = st.session_state.get("platform_props", [])
        if _live_platform_props:
            before_plat_filter = len(props_to_analyze)
            props_to_analyze = filter_props_to_platform_players(props_to_analyze, _live_platform_props)
            plat_players_skipped = before_plat_filter - len(props_to_analyze)
            if plat_players_skipped > 0:
                st.info(
                    f"ℹ️ Skipping **{plat_players_skipped}** prop(s) for players not found on "
                    f"all major sportsbooks today. "
                    f"Only platform-verified players are shown. ({len(props_to_analyze)} remaining)"
                )

        # ── Filter to selected platforms (from ⚙️ Settings) ──────────────
        _selected_platforms = st.session_state.get(
            "selected_platforms", ["PrizePicks", "Underdog Fantasy", "DraftKings Pick6"]
        )
        if _selected_platforms:
            before_plat = len(props_to_analyze)
            props_to_analyze = [
                p for p in props_to_analyze
                if not p.get("platform", "").strip()          # include props with no platform
                or p.get("platform", "") in _selected_platforms
            ]
            plat_skipped = before_plat - len(props_to_analyze)
            if plat_skipped > 0:
                st.info(
                    f"ℹ️ Skipping **{plat_skipped}** prop(s) for platforms not in your "
                    f"selection ({', '.join(_selected_platforms)}). "
                    "Change platforms on the ⚙️ Settings page."
                )

        # ── Show per-platform prop count summary ─────────────────────────
        if props_to_analyze:
            _plat_counts = {}
            for _p in props_to_analyze:
                _plat = _p.get("platform", "Unknown")
                _plat_counts[_plat] = _plat_counts.get(_plat, 0) + 1
            _plat_summary = " · ".join(
                f"**{_plat}**: {_n}" for _plat, _n in sorted(_plat_counts.items())
            )
            st.caption(f"📊 Analyzing: {_plat_summary}")

        # ── Pre-Analysis Filter: team + injury + dedup only (no stat-type cap) ─
        # All stat types are passed through — no funnel or stat-type filtering.
        _before_filter = len(props_to_analyze)
        props_to_analyze, _filter_summary = _smart_filter_props(
            all_props=props_to_analyze,
            players_data=players_data,
            todays_games=todays_games,
            injury_map=st.session_state.get("injury_status_map", {}),
            max_props_per_player=None,  # No per-player cap
            stat_types=None,            # Accept ALL stat types
            deduplicate_cross_platform=True,
        )
        _after_filter = len(props_to_analyze)
        if _before_filter > _after_filter:
            st.info(
                f"🎯 **Pre-Analysis Filter**: Reduced **{_before_filter}** → **{_after_filter}** props "
                f"(team/injury/dedup only — all stat types included)."
            )

        total_props_count    = len(props_to_analyze)
        if total_props_count == 0:
            st.warning("⚠️ No props remain after filtering to tonight's teams / injury status. Check your games and props.")
            progress_bar.empty()
            st.stop()

        # ── Analysis proceeds with all available props (no cap) ────

        for prop_index, prop in enumerate(props_to_analyze):
            progress_fraction = (prop_index + 1) / total_props_count
            progress_bar.progress(
                progress_fraction,
                text=f"Analyzing {prop.get('player_name', 'Player')}… ({prop_index + 1}/{total_props_count})"
            )

            try:
                player_name = prop.get("player_name", "")
                stat_type   = prop.get("stat_type", "points").lower()
                prop_line   = float(prop.get("line", 0))
                platform    = prop.get("platform", "DraftKings")

                # Phase 2: Use quarantined main line when available
                _raw_target = prop.get("prop_target_line")
                prop_target_line = None
                if _raw_target is not None:
                    try:
                        _ptl = float(_raw_target)
                        if _ptl > 0:
                            prop_target_line = _ptl
                    except (ValueError, TypeError):
                        pass

                # ── Injury gate ───────────────────────────────────────────
                injury_map        = st.session_state.get("injury_status_map", {})
                player_status_info = get_player_status(player_name, injury_map)
                player_status      = player_status_info.get("status", "Active")

                if player_status in (
                    "Out", "Injured Reserve", "Out (No Recent Games)",
                    "Suspended", "Not With Team",
                    "G League - Two-Way", "G League - On Assignment", "G League",
                ):
                    injury_note = player_status_info.get("injury_note", "Player is not active")
                    analysis_results_list.append({
                        "player_name":   player_name,
                        "team":          prop.get("team", ""),
                        "player_team":   prop.get("team", ""),
                        "player_position": "",
                        "stat_type":     stat_type,
                        "line":          prop_line,
                        "platform":      platform,
                        "season_pts_avg": 0, "season_reb_avg": 0, "season_ast_avg": 0,
                        "points_avg": 0, "rebounds_avg": 0, "assists_avg": 0,
                        "opponent":      "",
                        "probability_over": 0.0, "probability_under": 1.0,
                        "simulated_mean": 0.0, "simulated_std": 0.0,
                        "percentile_10": 0.0, "percentile_50": 0.0, "percentile_90": 0.0,
                        "adjusted_projection": 0.0, "overall_adjustment": 1.0,
                        "recent_form_ratio": None, "games_played": None,
                        "edge_percentage": -50.0, "confidence_score": 0,
                        "tier": "Bronze", "tier_emoji": "🥉",
                        "direction": "UNDER",
                        "recommendation": f"SKIP — {player_name} is {player_status}",
                        "forces": {"over_forces": [], "under_forces": []},
                        "should_avoid": True,
                        "avoid_reasons": [f"Player is {player_status}: {injury_note}"],
                        "histogram": [], "score_breakdown": {},
                        "line_vs_avg_pct": 0, "recent_form_results": [],
                        "player_matched": False, "explanation": None,
                        "line_sharpness_force": None, "line_sharpness_penalty": 0.0,
                        "trap_line_result": {}, "trap_line_penalty": 0.0,
                        "teammate_out_notes": [], "minutes_adjustment_factor": 1.0,
                        "player_is_out": True,
                        "player_status": player_status,
                        "player_status_note": injury_note,
                        "player_id": "",
                    })
                    continue

                # ── Find player in database ───────────────────────────────
                player_data    = find_player_by_name(players_data, player_name)
                player_matched = player_data is not None

                if player_data is None:
                    # Build a complete fallback using positional priors.
                    # Using only one stat's avg (the old approach) caused zero-projections for
                    # every other stat — which breaks combo stats, double_double, fantasy scores,
                    # and the directional forces analysis entirely.
                    #
                    # Strategy:
                    #  1. Resolve the player's position from the NBA bio API (or default to SF).
                    #  2. Fill all 7 stat avgs from that position's prior.
                    #  3. For the specific stat being analyzed, anchor to prop_line (the most
                    #     reliable single data point we have) by scaling the prior components
                    #     proportionally so the expected total matches the prop line.
                    #  4. Set games_played=30 (above the Bayesian threshold of 25) so shrinkage
                    #     is NOT applied — the prop_line is already our best anchor and further
                    #     shrinkage toward league priors would move estimates away from it.
                    _pos = "SF"  # default when position is unknown
                    try:
                        from data.player_profile_service import get_player_bio
                        _bio = get_player_bio(player_name)
                        if _bio.get("position"):
                            _bio_pos = _bio["position"].split("-")[0].strip()
                            _BIO_POS_ALIAS = {"Guard": "PG", "Forward": "SF", "Center": "C"}
                            _pos = _BIO_POS_ALIAS.get(_bio_pos, _bio_pos)
                    except Exception:
                        pass
                    _prior = POSITION_PRIORS.get(_pos, POSITION_PRIORS["SF"])
                    player_data = {
                        "name":          player_name,
                        "team":          prop.get("team", ""),
                        "position":      _pos,
                        "games_played":  30,   # above Bayesian threshold — trust prop_line anchor
                        "minutes_avg":   28.0,
                        # All seven stats seeded from position priors
                        "points_avg":    str(_prior["points"]),
                        "rebounds_avg":  str(_prior["rebounds"]),
                        "assists_avg":   str(_prior["assists"]),
                        "threes_avg":    str(_prior["threes"]),
                        "steals_avg":    str(_prior["steals"]),
                        "blocks_avg":    str(_prior["blocks"]),
                        "turnovers_avg": str(_prior["turnovers"]),
                    }

                    if stat_type in COMBO_STAT_TYPES:
                        # Scale prior components so they sum to the prop_line.
                        # e.g. PRA line=50.5, SF prior P+R+A=25.0 → scale=2.02
                        # → pts=32.3, reb=11.1, ast=7.1  (realistic split that totals 50.5)
                        _components = COMBO_STATS.get(stat_type, [])
                        _prior_sum = sum(_prior.get(s, 0.0) for s in _components)
                        if _prior_sum > 0 and prop_line > 0:
                            _scale = prop_line / _prior_sum
                            for _c in _components:
                                _est = round(_prior.get(_c, 0.0) * _scale, 1)
                                player_data[f"{_c}_avg"] = str(_est)
                                player_data[f"{_c}_std"] = str(round(max(0.5, _est * 0.35), 1))
                        else:
                            # Equal split fallback
                            _split = round(prop_line / max(len(_components), 1), 1)
                            for _c in _components:
                                player_data[f"{_c}_avg"] = str(_split)
                                player_data[f"{_c}_std"] = str(round(max(0.5, _split * 0.35), 1))

                    elif stat_type in FANTASY_STAT_TYPES:
                        # Scale all stats so the weighted fantasy total matches prop_line.
                        _formula = FANTASY_SCORING.get(stat_type, {})
                        _prior_fantasy = sum(_prior.get(s, 0.0) * w for s, w in _formula.items())
                        if _prior_fantasy > 0 and prop_line > 0:
                            _scale = prop_line / _prior_fantasy
                            for _fs in _formula:
                                _est = round(_prior.get(_fs, 0.0) * _scale, 1)
                                player_data[f"{_fs}_avg"] = str(_est)
                                player_data[f"{_fs}_std"] = str(round(max(0.5, _est * 0.35), 1))

                    elif stat_type not in {"double_double", "triple_double"}:
                        # Simple stat: prop_line is the best single-point estimate.
                        player_data[f"{stat_type}_avg"] = str(prop_line)
                        player_data[f"{stat_type}_std"] = str(round(prop_line * 0.35, 1))
                    # For double_double / triple_double, the position priors already seed
                    # all five required components — no further override needed.

                player_team  = player_data.get("team", prop.get("team", ""))
                game_context = find_game_context_for_player(player_team, todays_games)

                recent_form_games  = prop.get("recent_form_results", [])

                # ── Feature 6: Minutes Trend — compute using rotation_tracker ──
                # If game logs contain minutes (MIN field), detect trend vs season avg.
                _minutes_trend = None
                _minutes_trend_indicator = "➡️"  # default: stable
                if _rotation_tracker_available and recent_form_games:
                    try:
                        _minutes_trend = track_minutes_trend(recent_form_games, window=5)
                        _td = _minutes_trend.get("trend_direction", "stable")
                        _minutes_trend_indicator = "🔺" if _td == "up" else ("🔻" if _td == "down" else "➡️")
                    except Exception:
                        _minutes_trend = None

                # ── C4: Teammate-Out Usage Adjustment ────────────────────
                # Check if a high-usage teammate is OUT and boost this player's
                # projection accordingly (+8% primary option, +5% secondary, cap +15%).
                teammate_boost, teammate_boost_notes = calculate_teammate_out_boost(
                    player_data=player_data,
                    injury_status_map=injury_map,
                    teammates_data=players_data,
                )

                # ── Precise Minutes Projection (minutes_model.py) ─────────
                # Use the dedicated minutes model to get a more accurate minutes estimate
                # before running the full stat projection. The minutes projection
                # accounts for blowout spread, back-to-back, teammate injuries, and pace.
                _precise_minutes = None
                if _minutes_model_available and project_player_minutes is not None:
                    try:
                        _teammate_status = {
                            k: v.get("status", "Active")
                            for k, v in injury_map.items()
                        } if injury_map else None
                        _min_result = project_player_minutes(
                            player_data=player_data,
                            game_context={
                                "opponent": game_context.get("opponent", ""),
                                "is_home": game_context.get("is_home", True),
                                "vegas_spread": game_context.get("vegas_spread", 0.0),
                                "game_total": game_context.get("game_total", 220.0),
                                "rest_days": game_context.get("rest_days", 2),
                                "back_to_back": game_context.get("back_to_back", False),
                                "game_id": game_context.get("game_id", ""),
                            },
                            teammate_status=_teammate_status,
                            game_logs=recent_form_games if recent_form_games else None,
                        )
                        _precise_minutes = _min_result.get("projected_minutes")
                    except Exception:
                        _precise_minutes = None

                # ── Pull per-player advanced context from Deep Fetch enrichment ──
                # After the user clicks "Deep Fetch", st.session_state["advanced_enrichment"]
                # is populated with player_metrics (estimated metrics) for all rostered players.
                # Look up the matching player by ID or name and pass usage_pct to the projection.
                _advanced_context: dict | None = None
                try:
                    _game_enr = st.session_state.get("advanced_enrichment", {}).get(
                        game_context.get("game_id", ""), {}
                    )
                    _all_metrics = _game_enr.get("player_metrics", [])
                    _player_id = player_data.get("player_id") or player_data.get("id")
                    _player_name_lower = str(player_data.get("name", "")).lower()
                    for _m in _all_metrics:
                        _mid = _m.get("PLAYER_ID") or _m.get("playerId")
                        _mname = str(_m.get("PLAYER_NAME") or _m.get("playerName") or "").lower()
                        if (_player_id and _mid and int(_player_id) == int(_mid)) or (
                            _player_name_lower and _mname and _player_name_lower in _mname
                        ):
                            # Normalise usage: API may return 0–100 or 0–1 scale
                            _usg = _m.get("E_USG_PCT") or _m.get("USG_PCT") or _m.get("usage_pct")
                            if _usg is not None:
                                try:
                                    _usg_f = float(_usg)
                                    _advanced_context = {
                                        "usage_pct": _usg_f / 100.0 if _usg_f > 1.0 else _usg_f
                                    }
                                except (TypeError, ValueError):
                                    pass
                            break
                except Exception:
                    pass

                projection_result  = build_player_projection(
                    player_data=player_data,
                    opponent_team_abbreviation=game_context.get("opponent", ""),
                    is_home_game=game_context.get("is_home", True),
                    rest_days=game_context.get("rest_days", 2),
                    game_total=game_context.get("game_total", 220.0),
                    defensive_ratings_data=defensive_ratings_data,
                    teams_data=teams_data,
                    recent_form_games=recent_form_games if recent_form_games else None,
                    vegas_spread=game_context.get("vegas_spread", 0.0),
                    minutes_adjustment_factor=teammate_boost,
                    teammate_out_notes=teammate_boost_notes,
                    advanced_context=_advanced_context,
                )

                # ── Ensemble Model Override (3-model blend) ────────────────
                # When game logs are available, run the full ensemble model (Model A:
                # season avg + context, Model B: recent form, Model C: matchup history)
                # and use the blended projection as the primary projected_stat.
                # This is more accurate than a single model approach.
                _ensemble_result = None
                _ensemble_penalty = 0.0
                if _ensemble_available and get_ensemble_projection is not None and stat_type not in (
                    "double_double", "triple_double"
                ):
                    try:
                        _ens_ctx = {
                            "stat_type": stat_type,
                            "opponent": game_context.get("opponent", ""),
                            "is_home": game_context.get("is_home", True),
                            "rest_factor": projection_result.get("rest_factor", 1.0),
                            "pace_factor": projection_result.get("pace_factor", 1.0),
                            "defense_factor": projection_result.get("defense_factor", 1.0),
                        }
                        _ensemble_result = get_ensemble_projection(
                            player_data=player_data,
                            game_context=_ens_ctx,
                            game_logs=recent_form_games if len(recent_form_games or []) >= 3 else None,
                        )
                        _ensemble_penalty = _ensemble_result.get("confidence_adjustment", 0.0)
                    except Exception:
                        _ensemble_result = None
                        _ensemble_penalty = 0.0

                stat_std      = get_stat_standard_deviation(player_data, stat_type)
                projected_stat = projection_result.get(
                    f"projected_{stat_type}",
                    float(player_data.get(f"{stat_type}_avg", prop_line))
                )

                # ── Apply Ensemble Projection Override ───────────────────
                # When the ensemble produced a blended projection, use it to
                # override the single-model projected_stat. The ensemble blends
                # season-avg, recent-form, and matchup-history models with
                # inverse-variance weighting — consistently more accurate.
                _ensemble_used = False
                if (_ensemble_result is not None
                        and stat_type not in ("double_double", "triple_double")
                        and stat_type not in list(COMBO_STAT_TYPES) + list(FANTASY_STAT_TYPES)):
                    _ens_proj = _ensemble_result.get("ensemble_projection", 0)
                    _ens_std  = _ensemble_result.get("ensemble_std", 0)
                    if _ens_proj and _ens_proj > 0:
                        projected_stat = _ens_proj
                        _ensemble_used = True
                        # Blend ensemble std with base std for richer variance estimate
                        if _ens_std > 0:
                            stat_std = (_ens_std + stat_std) / 2.0

                # ── C8: Minutes-Stat Correlation — pass projected_minutes to sim ─
                # ── C11: KDE from Game Logs — pass recent_game_logs to sim ──────
                # Build stat-specific game log list from recent_form_games for KDE.
                # Maps stat_type keys to the game-log column names.
                _stat_log_key_map = {
                    "points": "pts", "rebounds": "reb", "assists": "ast",
                    "threes": "fg3m", "steals": "stl", "blocks": "blk",
                    "turnovers": "tov",
                }
                _log_key = _stat_log_key_map.get(stat_type, stat_type)
                recent_game_log_values = []
                for _g in (recent_form_games or []):
                    _v = _g.get(_log_key, _g.get(stat_type))
                    if _v is not None:
                        try:
                            recent_game_log_values.append(float(_v))
                        except (TypeError, ValueError):
                            pass

                # ── Simulation dispatch: use specialist functions for combo/fantasy/yesno ──
                # Combo stats (PRA, Pts+Rebs, etc.) use correlated Cholesky simulation (C7).
                # Fantasy score stats use the platform-specific weighted-sum formula.
                # Double/triple-double props use threshold-counting simulation.
                # Simple stats fall back to the standard Quantum Matrix Engine 5.6 path.
                _sim_kwargs = dict(
                    blowout_risk_factor=projection_result.get("blowout_risk", 0.15),
                    pace_adjustment_factor=projection_result.get("pace_factor", 1.0),
                    matchup_adjustment_factor=projection_result.get("defense_factor", 1.0),
                    home_away_adjustment=projection_result.get("home_away_factor", 0.0),
                    rest_adjustment_factor=projection_result.get("rest_factor", 1.0),
                    game_context=game_context if game_context.get("game_id") else None,
                )

                if stat_type in COMBO_STAT_TYPES:
                    # Build component projections from the per-stat projection outputs
                    _combo_stat_components = COMBO_STATS.get(stat_type, [])
                    _comp_proj = {
                        s: projection_result.get(
                            f"projected_{s}",
                            float(player_data.get(f"{s}_avg", 0) or 0),
                        )
                        for s in _combo_stat_components
                    }
                    _comp_std = {
                        s: get_stat_standard_deviation(player_data, s)
                        for s in _combo_stat_components
                    }
                    simulation_output = simulate_combo_stat(
                        component_projections=_comp_proj,
                        component_std_devs=_comp_std,
                        prop_line=prop_line,
                        number_of_simulations=simulation_depth,
                        **_sim_kwargs,
                    )
                    # Update projected_stat to the adjusted combo sum for edge calc
                    projected_stat = simulation_output.get("adjusted_projection", sum(_comp_proj.values()))

                elif stat_type in FANTASY_STAT_TYPES:
                    # Use the platform's weighted-sum fantasy formula
                    _formula = FANTASY_SCORING.get(stat_type, {})
                    _stat_proj = {
                        s: projection_result.get(
                            f"projected_{s}",
                            float(player_data.get(f"{s}_avg", 0) or 0),
                        )
                        for s in _formula
                    }
                    _stat_std = {
                        s: get_stat_standard_deviation(player_data, s)
                        for s in _formula
                    }
                    simulation_output = simulate_fantasy_score(
                        stat_projections=_stat_proj,
                        stat_std_devs=_stat_std,
                        fantasy_formula=_formula,
                        prop_line=prop_line,
                        number_of_simulations=simulation_depth,
                        **_sim_kwargs,
                    )
                    projected_stat = simulation_output.get("adjusted_projection", projected_stat)

                elif stat_type == "double_double":
                    _dd_stats = ["points", "rebounds", "assists", "blocks", "steals"]
                    _dd_proj = {
                        s: projection_result.get(
                            f"projected_{s}",
                            float(player_data.get(f"{s}_avg", 0) or 0),
                        )
                        for s in _dd_stats
                    }
                    _dd_std = {s: get_stat_standard_deviation(player_data, s) for s in _dd_stats}
                    simulation_output = simulate_double_double(
                        stat_projections=_dd_proj,
                        stat_std_devs=_dd_std,
                        number_of_simulations=simulation_depth,
                        **_sim_kwargs,
                    )

                elif stat_type == "triple_double":
                    _td_stats = ["points", "rebounds", "assists"]
                    _td_proj = {
                        s: projection_result.get(
                            f"projected_{s}",
                            float(player_data.get(f"{s}_avg", 0) or 0),
                        )
                        for s in _td_stats
                    }
                    _td_std = {s: get_stat_standard_deviation(player_data, s) for s in _td_stats}
                    simulation_output = simulate_triple_double(
                        stat_projections=_td_proj,
                        stat_std_devs=_td_std,
                        number_of_simulations=simulation_depth,
                        **_sim_kwargs,
                    )

                else:
                    # Simple stat: standard Quantum Matrix Engine 5.6 simulation (C5 skew-normal, C8 minutes, C11 KDE)
                    _flat_sim_minutes = _precise_minutes or projection_result.get("projected_minutes")
                    simulation_output = run_quantum_matrix_simulation(
                        projected_stat_average=projected_stat,
                        stat_standard_deviation=stat_std,
                        prop_line=prop_line,
                        number_of_simulations=simulation_depth,
                        stat_type=stat_type,
                        projected_minutes=_flat_sim_minutes,
                        minutes_std=4.0,
                        recent_game_logs=recent_game_log_values if len(recent_game_log_values) >= 15 else None,
                        prop_target_line=prop_target_line,
                        platform=platform,
                        **_sim_kwargs,
                    )

                    # ── Game Script Blend (30% game-script + 70% flat) ────
                    # For simple stats, blend in the game-script simulation to
                    # capture within-game dynamics (blowout minutes cuts, close
                    # game OT boosts) that the flat model misses.
                    # IMPORTANT: We update the simulated_mean/std but keep all
                    # other keys (probability_over, percentiles, etc.) from the
                    # flat simulation since blend_with_flat_simulation only
                    # provides mean/std — no probability recalculation.
                    if _game_script_available and simulate_game_script is not None:
                        try:
                            _gs_proj_dict = {
                                "projected_stat":    projected_stat,
                                "projected_minutes": _flat_sim_minutes or 32.0,
                                "stat_std":          stat_std,
                            }
                            _gs_ctx = {
                                "vegas_spread": game_context.get("vegas_spread", 0.0),
                                "game_total":   game_context.get("game_total", 220.0),
                                "is_home":      game_context.get("is_home", True),
                                "stat_type":    stat_type,
                            }
                            _gs_result = simulate_game_script(
                                player_projection=_gs_proj_dict,
                                game_context=_gs_ctx,
                                num_simulations=min(500, simulation_depth),
                            )
                            # game_script returns 'simulated_values' not 'simulated_results'
                            if _gs_result and _gs_result.get("simulated_values"):
                                # blend_with_flat_simulation expects 'mean'/'std' keys,
                                # but simulation.py returns 'simulated_mean'/'simulated_std'.
                                # Build a normalized flat dict for the blend function.
                                _flat_for_blend = {
                                    "mean": simulation_output.get(
                                        "simulated_mean",
                                        simulation_output.get("mean", 0.0)
                                    ),
                                    "std": simulation_output.get(
                                        "simulated_std",
                                        simulation_output.get("std", 0.0)
                                    ),
                                }
                                _blended = blend_with_flat_simulation(
                                    game_script_results=_gs_result,
                                    flat_simulation_results=_flat_for_blend,
                                )
                                if _blended and _blended.get("blended_mean", 0) > 0:
                                    # Merge blended mean/std back into simulation_output
                                    # without overwriting probability keys
                                    simulation_output = dict(simulation_output)
                                    simulation_output["simulated_mean"] = _blended["blended_mean"]
                                    simulation_output["simulated_std"]  = _blended["blended_std"]
                                    simulation_output["game_script_applied"] = True
                        except Exception:
                            pass  # Game script is additive — never block main flow

                forces_result = analyze_directional_forces(
                    player_data=player_data,
                    prop_line=prop_line,
                    stat_type=stat_type,
                    projection_result=projection_result,
                    game_context=game_context,
                )

                season_avg_for_stat  = float(player_data.get(f"{stat_type}_avg", 0) or 0)
                line_sharpness_force = detect_line_sharpness(
                    prop_line=prop_line,
                    season_average=season_avg_for_stat if season_avg_for_stat > 0 else None,
                    stat_type=stat_type,
                )
                line_sharpness_penalty = 0.0
                if line_sharpness_force is not None:
                    line_sharpness_penalty = min(8.0, line_sharpness_force.get("strength", 0) * 2.5)

                trap_line_result = detect_trap_line(
                    prop_line=prop_line,
                    season_average=season_avg_for_stat if season_avg_for_stat > 0 else None,
                    defense_factor=projection_result.get("defense_factor", 1.0),
                    rest_factor=projection_result.get("rest_factor", 1.0),
                    game_total=game_context.get("game_total", 220.0),
                    blowout_risk=projection_result.get("blowout_risk", 0.15),
                    stat_type=stat_type,
                )
                trap_line_penalty = trap_line_result.get("confidence_penalty", 0.0)

                probability_over  = simulation_output.get("probability_over", 0.5)

                # Use actual odds from the prop when available so the edge reflects the
                # true implied probability for this platform/line, not a fixed -110 default.
                # No-vig platform list cleared — all platforms now use actual odds.
                _prop_over_odds  = prop.get("over_odds", -110)
                _prop_under_odds = prop.get("under_odds", -110)
                _platform_for_odds = prop.get("platform", "")
                # Platforms without per-leg vig: treat as standard -110 breakeven
                _NO_VIG_PLATFORMS = set()  # Legacy no-vig platforms removed
                if _platform_for_odds in _NO_VIG_PLATFORMS:
                    # No vig — use the standard -110 breakeven (0.5238)
                    _implied_prob_for_edge = None  # let calculate_edge_percentage use default
                else:
                    # DraftKings and other sportsbooks: use actual over odds for the implied prob
                    _implied_prob_for_edge = _odds_to_implied_prob(_prop_over_odds)

                edge_pct = calculate_edge_percentage(probability_over, _implied_prob_for_edge)

                # C10: Historical calibration — adjust confidence score based on
                # how well-calibrated the model has been historically at this
                # probability level.  Returns 0.0 on cold start (no history yet).
                calibration_adj = get_calibration_adjustment(probability_over)

                # ── Fetch real matchup data for SAFE Score enrichment ─────────
                # on_off_data: player's team On/Off net-rating differentials
                # matchup_data: per-matchup defensive assignments from box score
                # Both are optional — confidence scoring degrades gracefully.
                _on_off_data: dict | None = None
                _matchup_data: dict | None = None
                try:
                    from data.nba_data_service import get_player_on_off, get_box_score_matchups
                    _player_team_id = game_context.get("home_team_id") if game_context.get("is_home") else game_context.get("away_team_id")
                    if _player_team_id:
                        _on_off_data = get_player_on_off(_player_team_id) or None
                    _game_id_ctx = game_context.get("game_id", "")
                    if _game_id_ctx:
                        _matchup_data = get_box_score_matchups(_game_id_ctx) or None
                except Exception:
                    pass

                confidence_output = calculate_confidence_score(
                    probability_over=probability_over,
                    edge_percentage=edge_pct,
                    directional_forces=forces_result,
                    defense_factor=projection_result.get("defense_factor", 1.0),
                    stat_standard_deviation=stat_std,
                    stat_average=season_avg_for_stat,
                    simulation_results=simulation_output,
                    games_played=int(player_data.get("games_played", 0) or 0) or None,
                    recent_form_ratio=projection_result.get("recent_form_ratio"),
                    line_sharpness_penalty=line_sharpness_penalty,
                    trap_line_penalty=trap_line_penalty,
                    calibration_adjustment=calibration_adj,  # C10
                    on_off_data=_on_off_data,
                    matchup_data=_matchup_data,
                )

                # ── Apply ensemble model-disagreement penalty to confidence ─
                if _ensemble_penalty > 0:
                    _cur_conf = confidence_output.get("confidence_score", 50)
                    confidence_output["confidence_score"] = max(0.0, _cur_conf - _ensemble_penalty)

                # C12: Closing Line Value — record the model's opening projection and
                # recommendation at analysis time.  Callers can later call
                # engine.clv_tracker.update_closing_line() with the final closing line
                # to compute CLV and validate the model's edge.
                try:
                    store_opening_line(
                        player_name=player_name,
                        stat_type=stat_type,
                        opening_line=prop_line,
                        model_projection=projected_stat,
                        model_direction=confidence_output.get("direction", "OVER"),
                        confidence_score=confidence_output.get("confidence_score", 0.0),
                        tier=confidence_output.get("tier", "Bronze"),
                        edge_percentage=edge_pct,
                    )
                except Exception:
                    pass  # CLV recording is non-critical; never block analysis

                # F9: Store initial line snapshot for market movement tracking
                try:
                    _snap_key = f"{player_name}_{stat_type}"
                    if "line_snapshots" not in st.session_state:
                        st.session_state["line_snapshots"] = {}
                    if _snap_key not in st.session_state["line_snapshots"]:
                        st.session_state["line_snapshots"][_snap_key] = {
                            "initial_line": prop_line,
                            "timestamp": datetime.datetime.now().isoformat(),
                        }
                except Exception:
                    pass

                should_avoid_flag, avoid_reasons = should_avoid_prop(
                    probability_over=probability_over,
                    directional_forces_result=forces_result,
                    edge_percentage=edge_pct,
                    stat_standard_deviation=stat_std,
                    stat_average=float(player_data.get(f"{stat_type}_avg", prop_line)),
                    stat_type=stat_type,
                    platform=prop.get("platform", ""),
                    over_odds=prop.get("over_odds", -110),
                )

                # Merge kill-switch flags from confidence engine (C2/C3) with
                # should_avoid_prop() results so all sources are surfaced in the UI.
                if confidence_output.get("should_avoid"):
                    should_avoid_flag = True
                for extra_reason in confidence_output.get("avoid_reasons", []):
                    if extra_reason and extra_reason not in avoid_reasons:
                        avoid_reasons.append(extra_reason)

                histogram_data = build_histogram_from_results(
                    simulation_output.get("simulated_results", []),
                    prop_line,
                    number_of_buckets=15,
                )

                explanation = generate_pick_explanation(
                    player_data=player_data,
                    prop_line=prop_line,
                    stat_type=stat_type,
                    direction=confidence_output.get("direction", "OVER"),
                    projection_result=projection_result,
                    simulation_results=simulation_output,
                    forces=forces_result,
                    confidence_result=confidence_output,
                    game_context=game_context,
                    platform=platform,
                    recent_form_games=prop.get("recent_form_results", []),
                    should_avoid=should_avoid_flag,
                    avoid_reasons=avoid_reasons,
                    trap_line_result=trap_line_result,
                    line_sharpness_info=line_sharpness_force,
                    teammate_out_notes=projection_result.get("teammate_out_notes", []),
                )

                full_result = {
                    "player_name":      player_name,
                    "team":             player_team,
                    "player_team":      player_team,
                    "player_position":  player_data.get("position", ""),
                    "stat_type":        stat_type,
                    "line":             prop_line,
                    "platform":         platform,
                    "player_id":        player_data.get("player_id", ""),
                    "season_pts_avg":   float(player_data.get("points_avg",   0) or 0),
                    "season_reb_avg":   float(player_data.get("rebounds_avg", 0) or 0),
                    "season_ast_avg":   float(player_data.get("assists_avg",  0) or 0),
                    "points_avg":       float(player_data.get("points_avg",   0) or 0),
                    "rebounds_avg":     float(player_data.get("rebounds_avg", 0) or 0),
                    "assists_avg":      float(player_data.get("assists_avg",  0) or 0),
                    "opponent":         game_context.get("opponent", ""),
                    "probability_over": round(probability_over, 4),
                    "probability_under":round(1.0 - probability_over, 4),
                    "simulated_mean":   round(simulation_output.get("simulated_mean", 0), 1),
                    "simulated_std":    round(simulation_output.get("simulated_std",  0), 1),
                    "percentile_10":    round(simulation_output.get("percentile_10",  0), 1),
                    "percentile_50":    round(simulation_output.get("percentile_50",  0), 1),
                    "percentile_90":    round(simulation_output.get("percentile_90",  0), 1),
                    "adjusted_projection": round(projected_stat, 1),
                    "overall_adjustment":  round(projection_result.get("overall_adjustment", 1.0), 3),
                    "recent_form_ratio":   projection_result.get("recent_form_ratio"),
                    "games_played":        int(player_data.get("games_played", 0) or 0) or None,
                    "edge_percentage":     round(edge_pct, 1),
                    "confidence_score":    confidence_output.get("confidence_score", 50),
                    "tier":                confidence_output.get("tier", "Bronze"),
                    "tier_emoji":          confidence_output.get("tier_emoji", "🥉"),
                    "direction":           confidence_output.get("direction", "OVER"),
                    "recommendation":      confidence_output.get("recommendation", ""),
                    "forces":              forces_result,
                    "should_avoid":        should_avoid_flag,
                    "avoid_reasons":       avoid_reasons,
                    "histogram":           histogram_data,
                    "score_breakdown":     confidence_output.get("score_breakdown", {}),
                    "line_vs_avg_pct":     prop.get("line_vs_avg_pct", 0),
                    "recent_form_results": prop.get("recent_form_results", []),
                    "player_matched":      player_matched,
                    "explanation":         explanation,
                    "line_sharpness_force":   line_sharpness_force,
                    "line_sharpness_penalty": round(line_sharpness_penalty, 1),
                    "trap_line_result":       trap_line_result,
                    "trap_line_penalty":      round(trap_line_penalty, 1),
                    "teammate_out_notes":     projection_result.get("teammate_out_notes", []),
                    "minutes_adjustment_factor": round(projection_result.get("minutes_adjustment_factor", 1.0), 4),
                    "minutes_trend":           _minutes_trend,
                    "minutes_trend_indicator": _minutes_trend_indicator,
                    "projected_minutes":       round(_precise_minutes, 1) if _precise_minutes else None,
                    "player_is_out":    False,
                    "player_status":    player_status,
                    "player_status_note": player_status_info.get("injury_note", ""),
                    # Ensemble model metadata
                    "ensemble_used":       _ensemble_used,
                    "ensemble_models":     _ensemble_result.get("effective_models", 1) if _ensemble_result else 1,
                    "ensemble_disagreement": (
                        _ensemble_result.get("disagreement", {}).get("description", "")
                        if _ensemble_result else ""
                    ),
                    "ensemble_model_weights": (
                        _ensemble_result.get("model_weights", {}) if _ensemble_result else {}
                    ),
                    # Simulation array for fair-value odds explorer / slider
                    "simulated_results": simulation_output.get("simulated_results", []),
                }

                # ── Phase 2: DFS Fixed-Payout Metrics ───────────────────────
                # Stamp quarantined target line + DFS parlay EV metrics so the
                # downstream UI can display breakeven thresholds per flex tier.
                if simulation_output.get("prop_target_line"):
                    full_result["prop_target_line"] = simulation_output["prop_target_line"]
                    full_result["probability_over_target"] = simulation_output.get(
                        "probability_over_target", probability_over
                    )
                if simulation_output.get("dfs_breakevens"):
                    full_result["dfs_breakevens"] = simulation_output["dfs_breakevens"]
                if simulation_output.get("dfs_parlay_ev"):
                    full_result["dfs_parlay_ev"] = simulation_output["dfs_parlay_ev"]
                if simulation_output.get("dfs_platform"):
                    full_result["dfs_platform"] = simulation_output["dfs_platform"]

                # ── Bet Classification ──────────────────────────────────────
                # Primary classification is driven by line_category (from the
                # ingestion layer).  Statistical overlay applies to
                # standard-line picks.  Risk flags (conflicting forces, variance,
                # fatigue, regression) are separate from bet_type and feed into
                # the avoid-list system via is_uncertain.
                try:
                    _season_avg_for_classify = float(player_data.get(f"{stat_type}_avg", 0) or 0) or None
                    # Determine the source of the prop line so the classifier can
                    # validate whether it is a real platform line or a synthetic one.
                    _line_source = prop.get("platform") or prop.get("line_source") or "synthetic"
                    _standard_line = prop.get("standard_line", None)
                    _bet_classification = classify_bet_type(
                        probability_over=probability_over,
                        edge_percentage=edge_pct,
                        stat_standard_deviation=stat_std,
                        projected_stat=projected_stat,
                        prop_line=prop_line,
                        stat_type=stat_type,
                        directional_forces_result=forces_result,
                        rest_days=game_context.get("rest_days", 1),
                        vegas_spread=game_context.get("vegas_spread", 0.0),
                        recent_form_ratio=projection_result.get("recent_form_ratio"),
                        season_average=_season_avg_for_classify,
                        line_source=_line_source,
                    )
                    full_result["bet_type"]        = _bet_classification.get("bet_type", "standard")
                    full_result["bet_type_emoji"]  = _bet_classification.get("bet_type_emoji", "")
                    full_result["bet_type_label"]  = _bet_classification.get("bet_type_label", "Standard Bet")
                    full_result["bet_type_reasons"]= _bet_classification.get("reasons", [])
                    full_result["std_devs_from_line"] = _bet_classification.get("std_devs_from_line", 0.0)
                    full_result["line_verified"]   = _bet_classification.get("line_verified", True)
                    full_result["line_reliability_warning"] = _bet_classification.get("line_reliability_warning")
                    full_result["standard_line"]   = _standard_line
                    full_result["risk_flags"]      = _bet_classification.get("risk_flags", [])
                    full_result["is_uncertain"]    = _bet_classification.get("is_uncertain", False)
                    # Risk flags (conflicting forces, high-variance stat, etc.) are
                    # informational — they appear on the card UI as warnings but do NOT
                    # block the pick from being displayed or auto-logged.  The genuine
                    # should_avoid decision comes from should_avoid_prop() only.
                except Exception:
                    full_result["bet_type"]         = "standard"
                    full_result["bet_type_emoji"]   = ""
                    full_result["bet_type_label"]   = "Standard Bet"
                    full_result["bet_type_reasons"] = []
                    full_result["std_devs_from_line"] = 0.0
                    full_result["line_verified"]    = True
                    full_result["line_reliability_warning"] = None
                    full_result["standard_line"]    = None
                    full_result["risk_flags"]       = []
                    full_result["is_uncertain"]     = False

                # ── Capture odds from the original prop (for display) ────────
                full_result["over_odds"]  = prop.get("over_odds",  -110)
                full_result["under_odds"] = prop.get("under_odds", -110)

                # ── Alt-Line Probability Generation ──────────────────────────
                # Generate alt lines for analysis.
                # Probabilities are computed from the raw simulation distribution.
                try:
                    _alt_lines = generate_alt_line_probabilities(simulation_output, prop_line)
                    full_result["alt_lines"] = _alt_lines
                    _best_alt = _alt_lines.get("best_alt", {})
                    full_result["prediction"] = _best_alt.get("prediction", "")
                    # Override: when the bet classification says this prop IS an
                    # alt-line bet type, use the strict prediction string from the
                    # edge_detection formatter (anchored to the actual prop line)
                    # rather than the simulation-best line which may differ.
                except Exception:
                    full_result["alt_lines"] = {}
                    full_result["prediction"] = ""


                try:
                    _clv_penalties = get_stat_type_clv_penalties(days=90)
                    _clv_stat_penalty = _clv_penalties.get(stat_type, 0.0)
                    if _clv_stat_penalty > 0:
                        full_result["confidence_score"] = max(0.0, full_result["confidence_score"] - _clv_stat_penalty)
                        full_result["clv_stat_penalty"] = _clv_stat_penalty
                except Exception:
                    pass

                # ── Feature 9: Market movement adjustment ────────────────────
                try:
                    if detect_line_movement is not None:
                        _opening_snap = st.session_state.get("line_snapshots", {}).get(
                            f"{player_name}_{stat_type}", {}
                        )
                        if _opening_snap:
                            _mv = detect_line_movement(
                                player_name, stat_type,
                                _opening_snap.get("initial_line", prop_line),
                                prop_line,
                                full_result.get("direction", "OVER"),
                            )
                            _mv_adj = _mv.get("confidence_adjustment", 0.0)
                            if _mv_adj != 0.0:
                                full_result["confidence_score"] = max(0.0, min(100.0, full_result["confidence_score"] + _mv_adj))
                                full_result["market_movement"] = _mv
                except Exception:
                    pass

                # ── Composite Win Score ───────────────────────────────────────
                # Single 0-100 score combining all signals for quick sorting.
                try:
                    _dir = full_result.get("direction", "OVER")
                    _prob_in_dir = (
                        full_result.get("probability_over", 0.5)
                        if _dir == "OVER"
                        else full_result.get("probability_under", 0.5)
                    )
                    _streak_mult = projection_result.get("streak_multiplier", 1.0)
                    _cws_result = calculate_composite_win_score(
                        probability_in_direction=_prob_in_dir,
                        confidence_score=full_result.get("confidence_score", 50),
                        edge_percentage=full_result.get("edge_percentage", 0),
                        directional_forces_result=full_result.get("forces"),
                        streak_multiplier=_streak_mult,
                        risk_score=5.0,
                        is_coin_flip=full_result.get("is_coin_flip", False),
                        should_avoid=full_result.get("should_avoid", False),
                    )
                    full_result["composite_win_score"] = _cws_result["composite_win_score"]
                    full_result["win_score_grade"] = _cws_result["grade"]
                    full_result["win_score_label"] = _cws_result["grade_label"]
                except Exception:
                    full_result["composite_win_score"] = 0.0
                    full_result["win_score_grade"] = "F"
                    full_result["win_score_label"] = "Error"

                analysis_results_list.append(full_result)
            except Exception as _prop_loop_err:
                _logger.warning(
                    "Prop #%d (%s/%s) analysis failed — skipped: %s",
                    prop_index,
                    prop.get('player_name', '?'),
                    prop.get('stat_type', '?'),
                    _prop_loop_err,
                )
                # Build a minimal error result so this prop is still visible
                _err_name = prop.get('player_name', '')
                _err_stat = prop.get('stat_type', 'points').lower()
                _err_line = float(prop.get('line', 0))
                analysis_results_list.append({
                    "player_name": _err_name,
                    "team": prop.get("team", ""),
                    "player_team": prop.get("team", ""),
                    "player_position": "",
                    "stat_type": _err_stat,
                    "line": _err_line,
                    "platform": prop.get("platform", "DraftKings"),
                    "season_pts_avg": 0, "season_reb_avg": 0, "season_ast_avg": 0,
                    "points_avg": 0, "rebounds_avg": 0, "assists_avg": 0,
                    "opponent": "",
                    "probability_over": 0.5, "probability_under": 0.5,
                    "simulated_mean": _err_line, "simulated_std": 0.0,
                    "percentile_10": 0.0, "percentile_50": 0.0, "percentile_90": 0.0,
                    "adjusted_projection": _err_line, "overall_adjustment": 1.0,
                    "recent_form_ratio": None, "games_played": None,
                    "edge_percentage": 0.0, "confidence_score": 0,
                    "tier": "Bronze", "tier_emoji": "🥉",
                    "direction": "OVER",
                    "recommendation": f"⚠️ Analysis error for {_err_name} ({_err_stat})",
                    "forces": {"over_forces": [], "under_forces": []},
                    "should_avoid": True,
                    "avoid_reasons": [f"Analysis error: {_prop_loop_err}"],
                    "histogram": [], "score_breakdown": {},
                    "line_vs_avg_pct": 0, "recent_form_results": [],
                    "player_matched": False, "explanation": None,
                    "line_sharpness_force": None, "line_sharpness_penalty": 0.0,
                    "trap_line_result": {}, "trap_line_penalty": 0.0,
                    "teammate_out_notes": [], "minutes_adjustment_factor": 1.0,
                    "player_is_out": False,
                    "player_status": "Analysis Error",
                    "player_status_note": str(_prop_loop_err),
                    "player_id": "",
                    "composite_win_score": 0.0,
                    "win_score_grade": "F",
                    "win_score_label": "Error",
                })

        # Detect correlated props
        correlation_warnings = detect_correlated_props(analysis_results_list)
        for idx, warning in correlation_warnings.items():
            if idx < len(analysis_results_list):
                analysis_results_list[idx]["_correlation_warning"] = warning

        # ── Auto-trigger Smart Update if >20% of players are unmatched ─
        # Unmatched players use skeleton stats which reduces accuracy.
        # Loading fresh rosters resolves most mismatches without user action.
        _unmatched_players = list(dict.fromkeys(
            r.get("player_name", "")
            for r in analysis_results_list
            if not r.get("player_matched", True) and not r.get("player_is_out", False)
        ))
        _total_non_out = sum(
            1 for r in analysis_results_list if not r.get("player_is_out", False)
        )
        _unmatched_ratio = len(_unmatched_players) / max(_total_non_out, 1)
        if _unmatched_ratio > 0.20 and todays_games:
            st.info(
                f"🔄 **{len(_unmatched_players)} player(s) not found** in local database "
                f"({_unmatched_ratio*100:.0f}% of props). Triggering Smart Roster Update…"
            )
            try:
                from data.nba_data_service import get_todays_players as _get_today
                _roster_result = _get_today(todays_games, progress_callback=None)
                if _roster_result:
                    # Re-run the full analysis now that players.csv is populated.
                    # Simply clearing the analysis cache and re-running the page gives
                    # every player a complete projection from real season averages
                    # rather than the position-prior estimates used above.
                    try:
                        load_players_data.clear()  # bust Streamlit's CSV cache
                    except Exception:
                        pass
                    st.success(
                        f"✅ Smart Roster Update complete — re-running analysis with "
                        f"fresh player data for {len(_unmatched_players)} player(s)."
                    )
                    st.rerun()
            except Exception as _su_err:
                # Non-fatal — proceed with existing results
                _logger.warning(f"Smart Update error (non-fatal): {_su_err}")

        # ── Sort all analyzed bets by composite win score (keep every single pick) ──
        # The engine analyzes ALL available props and returns ALL of them
        # sorted by composite win score (multi-factor: probability + confidence +
        # edge + forces + streak + risk).  Out players are kept at the end for
        # transparency.  No artificial truncation — every prop is shown.
        _total_analyzed = len(analysis_results_list)
        _out_results = [r for r in analysis_results_list if r.get("player_is_out", False)]
        _active_results = [r for r in analysis_results_list if not r.get("player_is_out", False)]
        _active_results.sort(key=lambda r: r.get("composite_win_score", 0), reverse=True)
        _selected_active = _active_results  # Keep ALL active results — no truncation
        analysis_results_list = _selected_active + _out_results
        _logger.info(
            "QME 5.6 Output: %d analyzed → %d active + %d Out = %d total",
            _total_analyzed, len(_selected_active), len(_out_results), len(analysis_results_list),
        )

        st.session_state["analysis_results"] = analysis_results_list
        st.session_state["analysis_timestamp"] = datetime.datetime.now()

        # ── Persist analysis session to SQLite (survives page refresh/inactivity) ──
        try:
            from tracking.database import save_analysis_session as _save_session
            _save_session(
                analysis_results=analysis_results_list,
                todays_games=st.session_state.get("todays_games", []),
                selected_picks=st.session_state.get("selected_picks", []),
            )
        except Exception as _persist_err:
            pass  # Non-fatal — session state still has results
        progress_bar.empty()
        _analysis_elapsed = time.time() - _analysis_start_time
        st.success(
            f"✅ Analysis complete! Analyzed and displaying **{len(_selected_active)}** picks "
            f"(+ {len(_out_results)} out) in **{_analysis_elapsed:.1f}s**."
        )

        # ── Store ALL picks to all_analysis_picks table ──────────────
        try:
            from tracking.database import insert_analysis_picks as _insert_picks
            _stored = _insert_picks(analysis_results_list)
            if _stored > 0:
                _logger.info(f"Stored {_stored} analysis picks to all_analysis_picks table.")
        except Exception as _store_err:
            _logger.warning(f"Store analysis picks error (non-fatal): {_store_err}")

        # ── Auto-log all qualifying picks to the Bet Tracker ────────
        try:
            from tracking.bet_tracker import auto_log_analysis_bets as _auto_log
            _auto_logged = _auto_log(analysis_results_list, minimum_edge=minimum_edge)
            if _auto_logged > 0:
                st.info(
                    f"📊 Auto-logged **{_auto_logged}** qualifying pick(s) to the Bet Tracker."
                )
        except Exception as _auto_log_err:
            # Auto-logging is best-effort — never block the main analysis flow
            _logger.warning(f"Auto-log error (non-fatal): {_auto_log_err}")

        st.rerun()
    except Exception as _analysis_err:
        _err_str = str(_analysis_err)
        if "WebSocketClosedError" not in _err_str and "StreamClosedError" not in _err_str:
            st.error(f"❌ Analysis failed: {_analysis_err}")
    finally:
        try:
            progress_bar.empty()
        except Exception:
            pass

# ============================================================
# END SECTION: Analysis Runner
# ============================================================

# ============================================================
# SECTION: Display Analysis Results
# ============================================================

analysis_results = st.session_state.get("analysis_results", [])

# Build a player → news lookup from API-NBA news in session state.
# This is used to show injury/trade/performance alerts on prop cards.
_player_news_lookup: dict = {}  # {player_name_lower: [news_item, ...]}
for _ni in st.session_state.get("player_news", []):
    _ni_player = _ni.get("player_name", "").strip().lower()
    if _ni_player:
        _player_news_lookup.setdefault(_ni_player, []).append(_ni)

# Show a notice if results were reloaded from the saved session
if analysis_results and st.session_state.get("_analysis_session_reloaded_at"):
    _reloaded_ts = st.session_state["_analysis_session_reloaded_at"]
    st.info(
        f"💾 **Analysis restored from saved session** (last run: {_reloaded_ts}). "
        "Results are preserved from your last analysis run — click **🚀 Run Analysis** above to refresh."
    )

# ════ JOSEPH M. SMITH LIVE BROADCAST DESK ════
# Reduce Joseph's container size by 60% on this page per design requirements.
_JOSEPH_DESK_SIZE_CSS = """<style>
.joseph-live-desk{
    padding:10px 12px !important;
    margin:10px 0 !important;
    font-size:0.85rem !important;
    max-height:40vh;
    overflow-y:auto;
}
.joseph-live-desk .joseph-desk-avatar{
    width:40px !important;height:40px !important;
}
.joseph-live-desk h3,.joseph-live-desk h4{
    font-size:0.85rem !important;margin:4px 0 !important;
}
.joseph-live-desk .joseph-desk-title{
    font-size:0.9rem !important;
}
</style>"""
if analysis_results and st.session_state.get("joseph_enabled", True):
    st.markdown(_JOSEPH_DESK_SIZE_CSS, unsafe_allow_html=True)
    try:
        from pages.helpers.joseph_live_desk import render_joseph_live_desk
        from data.advanced_metrics import enrich_player_god_mode
        from data.data_manager import load_players_data, load_teams_data
        from engine.joseph_bets import joseph_auto_log_bets
        from utils.joseph_widget import inject_joseph_inline_commentary

        _players = load_players_data()
        _teams = {t.get("abbreviation", "").upper(): t for t in load_teams_data()}
        _games = st.session_state.get("todays_games", [])

        _enriched = []
        for _p in _players:
            try:
                _enriched.append(enrich_player_god_mode(_p, _games, _teams))
            except Exception:
                _enriched.append(_p)
        _enriched_lookup = {str(p.get("name", "")).lower().strip(): p for p in _enriched}

        with st.container():
            render_joseph_live_desk(
                analysis_results=analysis_results,
                enriched_players=_enriched_lookup,
                teams_data=_teams,
                todays_games=_games,
            )

        # Use joseph_results (enriched with verdicts) for inline commentary
        # when available; fall back to raw analysis_results.
        _joseph_results = st.session_state.get("joseph_results", [])
        inject_joseph_inline_commentary(
            _joseph_results if _joseph_results else analysis_results,
            "analysis_results",
        )

        if not st.session_state.get("joseph_bets_logged", False):
            if _joseph_results:
                _logged_count, _logged_msg = joseph_auto_log_bets(_joseph_results)
                if _logged_count > 0:
                    st.toast(f"🎙️ {_logged_msg}")
                st.session_state["joseph_bets_logged"] = True

        st.divider()
    except Exception as _joseph_err:
        import logging
        logging.getLogger(__name__).warning(f"Joseph Live Desk error: {_joseph_err}")
# ════ END JOSEPH LIVE DESK ════

if analysis_results:
    st.divider()

    # Filter results
    if show_all_or_top == "Top picks only (edge ≥ threshold)":
        displayed_results = [
            r for r in analysis_results
            if abs(r.get("edge_percentage", 0)) >= minimum_edge
        ]
    else:
        displayed_results = analysis_results

    # ── Tier Filter & Bet Classification Filter ──────────────────────────
    _na_filter_col1, _na_filter_col2 = st.columns(2)
    with _na_filter_col1:
        _na_tier_filter = st.multiselect(
            "Filter by Tier",
            ["Platinum 💎", "Gold 🥇", "Silver 🥈", "Bronze 🥉"],
            default=[],
            key="na_tier_filter",
            help="Show only picks matching the selected tiers. Leave empty to show all tiers.",
        )
    with _na_filter_col2:
        pass  # Bet Classification filter removed (tier system removed)
    if _na_tier_filter:
        _na_tier_names = [t.split(" ")[0] for t in _na_tier_filter]
        displayed_results = [r for r in displayed_results if r.get("tier") in _na_tier_names]

    # Sort by confidence score descending
    displayed_results.sort(
        key=lambda r: r.get("confidence_score", 0),
        reverse=True,
    )

    # ── Deduplicate by (player_name, stat_type, line, direction) ──
    # Prevents duplicate player cards and duplicate Streamlit element keys
    # when the same prop appears multiple times (e.g. from multiple platforms).
    _seen_result_keys: set = set()
    _deduped: list = []
    for _r in displayed_results:
        _rkey = (
            _r.get("player_name", ""),
            _r.get("stat_type", ""),
            _r.get("line", 0),
            _r.get("direction", "OVER"),
        )
        if _rkey not in _seen_result_keys:
            _seen_result_keys.add(_rkey)
            _deduped.append(_r)
    displayed_results = _deduped

    # ── Summary metrics ────────────────────────────────────────
    total_analyzed   = len(analysis_results)
    total_over_picks = sum(1 for r in displayed_results if r.get("direction") == "OVER")
    total_under_picks= sum(1 for r in displayed_results if r.get("direction") == "UNDER")
    platinum_count   = sum(1 for r in displayed_results if r.get("tier") == "Platinum")
    gold_count       = sum(1 for r in displayed_results if r.get("tier") == "Gold")
    avg_edge         = (
        sum(abs(r.get("edge_percentage", 0)) for r in displayed_results) / len(displayed_results)
        if displayed_results else 0
    )
    unmatched_count  = sum(1 for r in analysis_results if not r.get("player_matched", True))

    # Phase 3: DFS aggregate metrics
    _dfs_results = [r for r in displayed_results if r.get("dfs_parlay_ev")]
    _beats_be_count = sum(
        1 for r in _dfs_results
        if (r.get("dfs_parlay_ev") or {}).get("best_tier") is not None
    )

    st.subheader(f"📊 Results: {len(displayed_results)} picks (of {total_analyzed} analyzed)")

    sum_col1, sum_col2, sum_col3, sum_col4, sum_col5 = st.columns(5)
    sum_col1.metric("Showing",     len(displayed_results))
    sum_col2.metric("⬆️ MORE",    total_over_picks)
    sum_col3.metric("⬇️ LESS",   total_under_picks)
    sum_col4.metric("💎 Platinum", platinum_count)
    sum_col5.metric("Gold 🥇",     gold_count)

    # Phase 3: DFS Edge row (only shown when DFS metrics exist)
    if _dfs_results:
        _avg_dfs_edge = sum(
            (r.get("dfs_parlay_ev") or {}).get("tiers", {}).get(
                (r.get("dfs_parlay_ev") or {}).get("best_tier", 3), {}
            ).get("edge_vs_breakeven", 0) * 100
            for r in _dfs_results
            if (r.get("dfs_parlay_ev") or {}).get("best_tier") is not None
        ) / max(_beats_be_count, 1)
        _dfs_edge_c = "#00ff9d" if _avg_dfs_edge > 0 else "#ff5e00"
        st.markdown(
            f'<div style="background:linear-gradient(135deg,#0f1424,#14192b);'
            f'border:1px solid rgba(0,255,157,0.2);border-radius:8px;padding:10px 16px;margin:6px 0;">'
            f'<span style="color:#64748b;font-size:0.72rem;text-transform:uppercase;letter-spacing:0.08em;">'
            f'📈 DFS FLEX EDGE</span>'
            f'<span style="color:#475569;font-size:0.68rem;margin-left:8px;">'
            f'{_beats_be_count}/{len(_dfs_results)} legs beat breakeven</span>'
            f'<span style="color:{_dfs_edge_c};font-size:0.82rem;font-weight:800;margin-left:12px;'
            f"font-family:'JetBrains Mono',monospace;font-variant-numeric:tabular-nums;\">"
            f'Avg Edge: {_avg_dfs_edge:+.1f}%</span>'
            f'</div>',
            unsafe_allow_html=True,
        )

    # ── Slate Summary Dashboard ────────────────────────────────
    silver_count  = sum(1 for r in displayed_results if r.get("tier") == "Silver")
    bronze_count  = sum(1 for r in displayed_results if r.get("tier") == "Bronze")
    best_pick     = max(
        (r for r in displayed_results if not r.get("player_is_out", False)),
        key=lambda r: r.get("confidence_score", 0),
        default=None,
    )
    _tier_bar_html = (
        f'<span style="color:#c800ff;font-weight:700;">💎 {platinum_count} Platinum</span>'
        f' &nbsp;·&nbsp; <span style="color:#ffd700;font-weight:600;">🥇 {gold_count} Gold</span>'
        f' &nbsp;·&nbsp; <span style="color:#b0bec5;">🥈 {silver_count} Silver</span>'
        f' &nbsp;·&nbsp; <span style="color:#b0bec5;">🥉 {bronze_count} Bronze</span>'
    )
    _best_html = ""
    if best_pick:
        _bp_name  = _html.escape(str(best_pick.get("player_name", "")))
        _bp_stat  = _html.escape(str(best_pick.get("stat_type", "")).title())
        _bp_line  = best_pick.get("line", 0)
        _bp_dir   = "More" if best_pick.get("direction") == "OVER" else "Less"
        _bp_conf  = best_pick.get("confidence_score", 0)
        _bp_tier  = best_pick.get("tier", "")
        _bp_emoji = {"Platinum": "💎", "Gold": "🥇", "Silver": "🥈", "Bronze": "🥉"}.get(_bp_tier, "🏀")
        _best_html = (
            f'<div style="margin-top:10px;padding:10px 14px;background:rgba(255,94,0,0.08);'
            f'border-radius:6px;border-left:3px solid #ff5e00;">'
            f'<span style="color:#ff5e00;font-weight:700;font-size:0.85rem;">🏆 Best Pick: </span>'
            f'<span style="color:#e0e7ef;font-weight:600;">{_bp_emoji} {_bp_name} — {_bp_dir} {_bp_line} {_bp_stat}</span>'
            f'<span style="color:#00f0ff;font-weight:700;margin-left:10px;">{_bp_conf:.0f}/100</span>'
            f'</div>'
        )
    st.markdown(
        f'<div style="background:linear-gradient(135deg,#0f1424,#14192b);border:1px solid rgba(255,94,0,0.25);'
        f'border-radius:8px;padding:14px 18px;margin:8px 0 14px;">'
        f'<div style="font-size:0.9rem;font-weight:600;color:#e0e7ef;margin-bottom:6px;">'
        f'🗂️ Tier Distribution &nbsp;·&nbsp; '
        f'<span style="color:#00f0ff;">Avg Edge: {avg_edge:.1f}%</span>'
        f'</div>'
        f'<div style="font-size:0.85rem;">{_tier_bar_html}</div>'
        + _best_html +
        f'</div>',
        unsafe_allow_html=True,
    )

    # ── Quick-select buttons ───────────────────────────────────
    _qb_col1, _qb_col2, _qb_col3 = st.columns([1, 1, 2])
    with _qb_col1:
        if st.button("💎 Select All Platinum", help="Add all Platinum tier picks to Entry Builder"):
            _plat_picks = [
                r for r in displayed_results
                if r.get("tier") == "Platinum"
                and not r.get("player_is_out", False)
                and not r.get("should_avoid", False)
            ]
            _existing_keys = {p.get("key") for p in st.session_state.get("selected_picks", [])}
            _added = 0
            for r in _plat_picks:
                _stat     = r.get("stat_type", "").lower()
                _line     = r.get("line", 0)
                _dir      = r.get("direction", "OVER")
                _pick_key = f"{r.get('player_name', '')}_{_stat}_{_line}_{_dir}"
                if _pick_key not in _existing_keys:
                    st.session_state.setdefault("selected_picks", []).append({
                        "key":             _pick_key,
                        "player_name":     r.get("player_name", ""),
                        "stat_type":       _stat,
                        "line":            _line,
                        "direction":       _dir,
                        "confidence_score": r.get("confidence_score", 0),
                        "tier":            r.get("tier", "Platinum"),
                        "tier_emoji":      "💎",
                        "platform":        r.get("platform", ""),
                        "edge_percentage": r.get("edge_percentage", 0),
                    })
                    _added += 1
            if _added:
                st.success(f"✅ Added {_added} Platinum pick(s).")
                st.rerun()
            else:
                st.info("All Platinum picks already added.")
    with _qb_col2:
        if st.button("🥇 Select All Gold+", help="Add all Gold and Platinum tier picks to Entry Builder"):
            _gold_picks = [
                r for r in displayed_results
                if r.get("tier") in ("Platinum", "Gold")
                and not r.get("player_is_out", False)
                and not r.get("should_avoid", False)
            ]
            _existing_keys = {p.get("key") for p in st.session_state.get("selected_picks", [])}
            _added = 0
            for r in _gold_picks:
                _stat     = r.get("stat_type", "").lower()
                _line     = r.get("line", 0)
                _dir      = r.get("direction", "OVER")
                _pick_key = f"{r.get('player_name', '')}_{_stat}_{_line}_{_dir}"
                if _pick_key not in _existing_keys:
                    _t_emoji = "💎" if r.get("tier") == "Platinum" else "🥇"
                    st.session_state.setdefault("selected_picks", []).append({
                        "key":             _pick_key,
                        "player_name":     r.get("player_name", ""),
                        "stat_type":       _stat,
                        "line":            _line,
                        "direction":       _dir,
                        "confidence_score": r.get("confidence_score", 0),
                        "tier":            r.get("tier", "Gold"),
                        "tier_emoji":      _t_emoji,
                        "platform":        r.get("platform", ""),
                        "edge_percentage": r.get("edge_percentage", 0),
                    })
                    _added += 1
            if _added:
                st.success(f"✅ Added {_added} Gold+ pick(s).")
                st.rerun()
            else:
                st.info("All Gold+ picks already added.")

    if unmatched_count > 0:
        # Deduplicate: same player may have multiple stat types, each flagged separately.
        # Only count and list each unique player name once.
        unmatched_names_deduped = list(dict.fromkeys(
            r.get("player_name", "") for r in analysis_results
            if not r.get("player_matched", True)
            and not r.get("player_is_out", False)  # exclude confirmed-out players
        ))
        unmatched_unique_count = len(unmatched_names_deduped)
        if unmatched_unique_count > 0:
            _display_names = unmatched_names_deduped[:10]
            _overflow = unmatched_unique_count - len(_display_names)
            _inline = ", ".join(_display_names) + (f" and {_overflow} more" if _overflow > 0 else "")
            st.warning(
                f"⚠️ **{unmatched_unique_count} player(s) not found** in database — "
                + _inline
                + " — results may be less accurate. Run a **Smart Update** on the Data Feed page to refresh roster data."
            )
            if _overflow > 0:
                with st.expander(f"See all {unmatched_unique_count} unmatched players"):
                    st.write(", ".join(unmatched_names_deduped))

    st.divider()

    if not displayed_results:
        st.warning(
            "📭 **No picks match the current filters.** All analyzed props were filtered out. "
            "Try switching to **All picks** above, or loosen the Tier / Bet Classification filters."
        )

    # ============================================================
    # SECTION: Player News Alerts (API-NBA)
    # Show injury/trade/performance news for players in today's slate.
    # ============================================================
    _slate_players = {
        str(r.get("player_name", "")).strip().lower()
        for r in displayed_results
        if r.get("player_name")
    }
    _slate_news: list = []
    for _pname_lower in _slate_players:
        for _news_item in _player_news_lookup.get(_pname_lower, []):
            _slate_news.append(_news_item)
    # Sort by impact (high > medium > low) then by published date
    _imp_order = {"high": 0, "medium": 1, "low": 2}
    _slate_news.sort(key=lambda x: (_imp_order.get(x.get("impact", "low"), 3), x.get("published_at", "")))

    if _slate_news:
        _imp_colors = {"high": "#ff4444", "medium": "#ffd700", "low": "#8b949e"}
        _cat_emoji  = {
            "injury": "🏥", "trade": "🔄", "performance": "📈",
            "suspension": "🚫", "contract": "💰", "roster": "📋",
        }
        with st.expander(
            f"📰 Player News Alerts — {len(_slate_news)} item(s) for tonight's slate",
            expanded=any(n.get("impact") == "high" for n in _slate_news),
        ):
            for _na in _slate_news[:15]:
                _na_title  = _na.get("title", "")
                _na_player = _na.get("player_name", "")
                _na_body   = _na.get("body", "")
                _na_cat    = _na.get("category", "")
                _na_imp    = _na.get("impact", "").lower()
                _na_pub    = _na.get("published_at", "")[:10]
                if not _na_title:
                    continue
                _na_c = _imp_colors.get(_na_imp, "#555")
                _na_em = _cat_emoji.get(_na_cat, "📰")
                st.markdown(
                    f'<div style="background:#0d1117;border-left:4px solid {_na_c};'
                    f'border-radius:6px;padding:10px 14px;margin-bottom:8px;">'
                    f'<div style="display:flex;justify-content:space-between;align-items:center;">'
                    f'<span style="color:#e0e7ef;font-weight:700;">{_na_em} {_html.escape(_na_title[:80])}</span>'
                    f'<span style="background:{_na_c};color:#000;border-radius:4px;'
                    f'padding:1px 6px;font-size:0.72rem;font-weight:700;">'
                    f'{_na_imp.upper() if _na_imp else "NEWS"}</span>'
                    f'</div>'
                    f'<div style="color:#8b949e;font-size:0.78rem;margin-top:4px;">'
                    f'<strong style="color:#c0d0e8;">{_html.escape(_na_player)}</strong>'
                    + (f' · {_html.escape(_na_pub)}' if _na_pub else "")
                    + f'</div>'
                    + (f'<div style="color:#a0b4d0;font-size:0.82rem;margin-top:6px;">'
                       f'{_html.escape(_na_body[:200])}'
                       + ("…" if len(_na_body) > 200 else "")
                       + f'</div>' if _na_body else "")
                    + f'</div>',
                    unsafe_allow_html=True,
                )
        st.divider()

    # ============================================================
    # SECTION: Market Movement Alerts (Odds API line snapshots)
    # Shows sharp-money / line-movement signals detected during analysis.
    # ============================================================
    _mm_results = [
        r for r in displayed_results
        if r.get("market_movement") and not r.get("player_is_out", False)
    ]
    if _mm_results:
        with st.expander(
            f"📉 Market Movement Alerts — {len(_mm_results)} line shift(s) detected",
            expanded=False,
        ):
            for _mm_r in _mm_results:
                _mm = _mm_r["market_movement"]
                _mm_player = _mm_r.get("player_name", "")
                _mm_stat = _mm_r.get("stat_type", "")
                _mm_dir = _mm.get("direction", "")
                _mm_shift = _mm.get("line_shift", 0)
                _mm_signal = _mm.get("signal", "neutral")
                _mm_adj = _mm.get("confidence_adjustment", 0)
                _sig_colors = {"sharp_buy": "#00ff9d", "sharp_fade": "#ff6b6b", "neutral": "#8b949e"}
                _sig_c = _sig_colors.get(_mm_signal, "#8b949e")
                _sig_labels = {"sharp_buy": "🟢 SHARP BUY", "sharp_fade": "🔴 SHARP FADE", "neutral": "⚪ NEUTRAL"}
                _sig_lbl = _sig_labels.get(_mm_signal, "⚪ NEUTRAL")
                st.markdown(
                    f'<div style="background:#0d1117;border-left:4px solid {_sig_c};'
                    f'border-radius:6px;padding:10px 14px;margin-bottom:8px;">'
                    f'<div style="display:flex;justify-content:space-between;align-items:center;">'
                    f'<span style="color:#e0e7ef;font-weight:700;">'
                    f'{_html.escape(_mm_player)} — {_html.escape(_mm_stat.title())} {_html.escape(_mm_dir)}</span>'
                    f'<span style="color:{_sig_c};font-weight:700;font-size:0.85rem;">{_sig_lbl}</span>'
                    f'</div>'
                    f'<div style="color:#8b949e;font-size:0.78rem;margin-top:4px;">'
                    f'Line shift: <strong style="color:#c0d0e8;">{_mm_shift:+.1f}</strong>'
                    + (f' · Confidence adj: <strong style="color:{_sig_c};">{_mm_adj:+.1f}</strong>' if _mm_adj else '')
                    + f'</div></div>',
                    unsafe_allow_html=True,
                )
        st.divider()

    # ============================================================
    # SECTION B: Uncertain Picks (Risk Warnings — conflicting forces)
    # ============================================================
    _uncertain_picks = [
        r for r in analysis_results
        if r.get("is_uncertain", False)
        and not r.get("player_is_out", False)
    ]
    if _uncertain_picks:
        with st.expander(
            f"⚠️ Uncertain Picks — Risk Flags ({len(_uncertain_picks)}) — Conflicting Signals, Use Caution",
            expanded=False,
        ):
            st.markdown(
                '<div style="background:rgba(255,193,7,0.10);border:2px solid #ffc107;'
                'border-radius:10px;padding:14px 18px;margin-bottom:14px;">'
                '<strong style="color:#ffc107;font-size:1.0rem;">UNCERTAIN PICKS — Conflicting Signals</strong><br>'
                '<span style="color:#ffe082;font-size:0.85rem;">These picks have hidden structural risks: '
                'conflicting forces, high variance with low edge, fatigue combos, or hot-streak regression. '
                'They are automatically added to your Avoid List.</span>'
                '</div>',
                unsafe_allow_html=True,
            )
            st.markdown(
                get_education_box_html(
                    "What are Uncertain Picks (Risk Flags)?",
                    "Uncertain picks have one or more hidden risk signals that make them dangerous despite "
                    "appearing to have edge.<br><br>"
                    "<strong>There are 4 risk patterns:</strong><br>"
                    "1. <strong>Conflicting Forces:</strong> The model's forces are fighting each other — "
                    "nearly 50/50 MORE vs LESS. It's a coin flip disguised as an edge.<br>"
                    "2. <strong>High Variance:</strong> High-variance stat (3-pointers, steals, blocks) "
                    "with a tiny edge (&lt;8%). These stats are too random game-to-game.<br>"
                    "3. <strong>Fatigue:</strong> Back-to-back game + big spread (blowout expected). "
                    "Player will likely rest in the 4th quarter.<br>"
                    "4. <strong>Regression:</strong> The line is set at a hot streak value (125%+ of "
                    "season average). The player is due to come back to earth.<br><br>"
                    "Uncertain picks are <em>automatically added to your Avoid List</em>.",
                ),
                unsafe_allow_html=True,
            )
            for _up in _uncertain_picks:
                _up_name  = _html.escape(str(_up.get("player_name", "")))
                _up_team  = _html.escape(str(_up.get("player_team", _up.get("team", ""))))
                _up_stat  = _html.escape(str(_up.get("stat_type", "")).title())
                _up_dir   = _html.escape(str(_up.get("direction", "OVER")))
                _up_line  = _up.get("line", 0)
                _up_proj  = _up.get("adjusted_projection", 0)
                _up_edge  = _up.get("edge_percentage", 0)
                _up_flags = _up.get("risk_flags", _up.get("bet_type_reasons", []))
                _up_team_badge = (
                    f'<span style="background:rgba(255,193,7,0.15);color:#ffe082;padding:1px 7px;'
                    f'border-radius:4px;font-size:0.78rem;font-weight:600;margin-left:7px;'
                    f'border:1px solid rgba(255,193,7,0.3);">{_up_team}</span>'
                    if _up_team else ""
                )
                _up_flags_html = "".join(
                    f'<li style="color:#ffe082;font-size:0.82rem;">{_html.escape(str(r))}</li>'
                    for r in _up_flags
                )
                # Classify risk type from flag text
                _up_flag_type = "Uncertain"
                for _ft in _up_flags:
                    _ftl = str(_ft).lower()
                    if "conflict" in _ftl:
                        _up_flag_type = "Conflicting Forces"
                        break
                    elif "variance" in _ftl or "high-variance" in _ftl:
                        _up_flag_type = "High Variance"
                        break
                    elif "fatigue" in _ftl or "back-to-back" in _ftl:
                        _up_flag_type = "Fatigue Risk"
                        break
                    elif "regression" in _ftl or "hot streak" in _ftl or "inflated" in _ftl:
                        _up_flag_type = "Regression Risk"
                        break
                st.markdown(
                    f'<div style="background:rgba(255,193,7,0.06);border:1px solid rgba(255,193,7,0.35);'
                    f'border-radius:8px;padding:12px 16px;margin-bottom:10px;">'
                    f'<div style="display:flex;justify-content:space-between;align-items:center;">'
                    f'<div>'
                    f'<span style="color:#ffc107;font-weight:700;">⚠️ {_up_name}</span>'
                    f'{_up_team_badge}'
                    f'<span style="background:#ffc107;color:#333;padding:2px 8px;border-radius:4px;'
                    f'font-size:0.72rem;font-weight:700;margin-left:8px;">{_up_flag_type}</span>'
                    f'</div>'
                    f'<div style="text-align:right;">'
                    f'<span style="color:#ffe082;font-size:0.85rem;">{_up_dir} {_up_line} {_up_stat} '
                    f'(Proj: {_up_proj:.1f})</span>'
                    f'<br><span style="color:#ffc107;font-size:0.8rem;font-weight:600;">'
                    f'Edge: {_up_edge:+.1f}%</span>'
                    f'</div>'
                    f'</div>'
                    f'<div style="margin-top:8px;">'
                    f'<span style="color:#ffc107;font-size:0.75rem;font-weight:600;">RISK FLAGS (AVOID):</span>'
                    f'<ul style="margin:4px 0 0 16px;padding:0;">{_up_flags_html}</ul>'
                    f'</div>'
                    + _render_inline_breakdown(_up, accent_color="#ffc107")
                    + f'</div>',
                    unsafe_allow_html=True,
                )
        st.divider()

    # ── 🏆 Best Single Bets (shown before parlays for maximum visibility) ─
    _single_bet_pool = [
        r for r in displayed_results
        if not r.get("should_avoid", False)
        and not r.get("player_is_out", False)
        and r.get("tier", "Bronze") in {"Platinum", "Gold", "Silver"}
    ]
    _single_bet_pool = sorted(
        _single_bet_pool,
        key=lambda r: (r.get("confidence_score", 0), abs(r.get("edge_percentage", 0))),
        reverse=True,
    )[:8]  # Show top 8

    if _single_bet_pool:
        # ── Gold tier banner (with Gold_Logo.png) ──────────────────────
        _gold_pool = [r for r in _single_bet_pool if r.get("tier") in ("Gold", "Platinum")]
        if _gold_pool:
            _goldcol_logo, _goldcol_title = st.columns([1, 6])
            with _goldcol_logo:
                if os.path.exists(_GOLD_LOGO_PATH):
                    st.image(_GOLD_LOGO_PATH, width=110)
            with _goldcol_title:
                st.markdown(
                    '<div style="background:linear-gradient(135deg,#1a1200,#231800);'
                    'border:2px solid #ffd700;border-radius:10px;padding:14px 18px;margin-bottom:4px;">'
                    '<h3 style="color:#ffd700;font-family:Orbitron,sans-serif;margin:0 0 4px;">🥇 Gold Tier Picks</h3>'
                    '<p style="color:#ffe082;font-size:0.85rem;margin:0;">'
                    'High-confidence picks with strong model projections and favorable matchups. '
                    'Gold picks are ideal for your core entry legs.'
                    '</p>'
                    '</div>',
                    unsafe_allow_html=True,
                )

        st.markdown(
            '<div style="background:linear-gradient(135deg,#0f1a2e,#14192b);'
            'border:2px solid #00f0ff;border-radius:10px;padding:16px 20px;margin-bottom:20px;">'
            '<h3 style="color:#00f0ff;font-family:Orbitron,sans-serif;margin:0 0 6px;">🏆 Best Single Bets</h3>'
            '<p style="color:#a0b4d0;font-size:0.85rem;margin:0;">Top individual picks ranked by SAFE Score™ — Silver tier and above</p>'
            '</div>',
            unsafe_allow_html=True,
        )
        _TIER_COLORS = {"Platinum": "#c800ff", "Gold": "#ff5e00", "Silver": "#b0c0d8"}
        # Inject the shared QCM CSS once for horizontal cards
        st.markdown(_get_qcm_css(), unsafe_allow_html=True)
        for _sb in _single_bet_pool:
            _sb_tier = _sb.get("tier", "Bronze")
            _sb_color = _TIER_COLORS.get(_sb_tier, "#b0c0d8")
            _h_card_html = _build_h_card(_sb, accent_color=_sb_color)
            st.markdown(_h_card_html, unsafe_allow_html=True)

    st.divider()

    # ── 🎯 Strongly Suggested Parlays (at TOP for maximum visibility) ─
    strategy_entries = _build_entry_strategy(displayed_results)
    if strategy_entries:
        st.markdown(
            '<div style="background:linear-gradient(135deg,#0f1a2e,#14192b);'
            'border:2px solid #ff5e00;border-radius:10px;padding:16px 20px;margin-bottom:20px;">'
            '<h3 style="color:#ff5e00;font-family:Orbitron,sans-serif;margin:0 0 6px;">🎯 Strongly Suggested Parlays</h3>'
            '<p style="color:#a0b4d0;font-size:0.85rem;margin:0;">Optimized multi-leg combos ranked by combined EDGE Score™</p>'
            '</div>',
            unsafe_allow_html=True,
        )
        _PARLAY_STARS = {2: "⭐", 3: "⭐⭐", 4: "⭐⭐⭐", 5: "⭐⭐⭐", 6: "⭐⭐⭐"}
        _PARLAY_LABEL = {
            2: "Best 2-Leg Parlay",
            3: "Best 3-Leg Parlay",
            4: "Best 4-Leg Parlay",
            5: "Best 5-Leg Parlay",
            6: "Max Entry (6-Leg)",
        }
        for _i, entry in enumerate(strategy_entries):
            _num     = entry.get("num_legs", 0)
            _label   = _PARLAY_LABEL.get(_num, entry.get("combo_type", ""))
            _star    = _PARLAY_STARS.get(_num, "")
            # Top 2 entries get a glow border
            _glow = "box-shadow:0 0 14px rgba(255,94,0,0.45);" if _i < 2 else ""
            picks_html = ""
            for pick_str in entry.get("picks", []):
                parts = pick_str.split(" ", 1)
                pname = _html.escape(parts[0]) if parts else ""
                rest  = _html.escape(parts[1]) if len(parts) > 1 else ""
                picks_html += (
                    f'<div style="display:flex;align-items:center;gap:8px;margin-bottom:4px;">'
                    f'<span style="color:#ff5e00;font-weight:600;">{pname}</span>'
                    f'<span style="color:#c0d0e8;">{rest}</span>'
                    f'</div>'
                )
            reasons = entry.get("reasons", [])
            reason_text = _html.escape(" | ".join(reasons)) if reasons else _html.escape(entry.get("strategy", ""))
            combined = entry.get("combined_prob", 0)
            avg_edge = entry.get("avg_edge", 0)
            avg_conf = entry.get("safe_avg", "—")
            st.markdown(
                f'<div style="background:#14192b;border-radius:8px;padding:15px 18px;'
                f'margin-bottom:14px;border-left:4px solid #ff5e00;{_glow}">'
                f'<div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:10px;">'
                f'<h4 style="color:#ff5e00;margin:0;font-family:Orbitron,sans-serif;">'
                f'{_star} {_label}</h4>'
                f'<span style="background:#ff5e00;color:#0a0f1a;padding:3px 10px;border-radius:4px;'
                f'font-size:0.8rem;font-weight:700;">SAFE: {avg_conf}/100</span>'
                f'</div>'
                f'{picks_html}'
                f'<div style="margin-top:10px;padding:7px 10px;background:rgba(20,25,43,0.7);border-radius:4px;">'
                f'<span style="color:#00c8ff;font-size:0.82rem;">💡 {reason_text}</span>'
                f'</div>'
                f'<div style="display:flex;gap:18px;margin-top:8px;font-size:0.8rem;color:#c0d0e8;">'
                f'<span>Combined prob: {combined:.1f}%</span>'
                f'<span>Avg edge: {avg_edge:+.1f}%</span>'
                f'</div>'
                f'</div>',
                unsafe_allow_html=True,
            )
    else:
        st.info("Not enough high-edge picks to build parlay combinations. Lower the edge threshold or add more props.")

    st.divider()

    # ── Confidence Bars: sorted ranking ─────────────────────────
    non_out_results = [r for r in displayed_results if not r.get("player_is_out", False)]
    if non_out_results:
        with st.expander("📈 Confidence Rankings (all picks)", expanded=True):
            _tier_icon_map = {
                "Platinum": "💎", "Gold": "🔒", "Silver": "✓", "Bronze": "⭐"
            }
            _STAT_EMOJI = {
                "points": "🏀", "rebounds": "📊", "assists": "🎯",
                "threes": "🎯", "steals": "⚡", "blocks": "🛡️", "turnovers": "❌",
            }
            for r in non_out_results:
                _stat     = r.get("stat_type", "")
                _emoji    = _STAT_EMOJI.get(_stat, "🏀")
                _dir      = "More" if r.get("direction") == "OVER" else "Less"
                _label    = (
                    f"{r.get('player_name', '')} — "
                    f"{_emoji} {_dir} {r.get('line', '')} {_stat.title()}"
                )
                st.markdown(
                    get_qds_confidence_bar_html(
                        label=_label,
                        percentage=r.get("confidence_score", 50),
                        tier_icon=_tier_icon_map.get(r.get("tier", "Bronze"), "⭐"),
                    ),
                    unsafe_allow_html=True,
                )

    # ── Team Breakdown (when single game) ────────────────────────
    if len(todays_games) == 1:
        g = todays_games[0]
        home_t = g.get("home_team", "")
        away_t = g.get("away_team", "")
        if home_t and away_t:
            with st.expander("🏀 Team Matchup Breakdown"):
                tc1, tc2 = st.columns(2)
                from styles.theme import get_team_colors
                home_color, _ = get_team_colors(home_t)
                away_color, _ = get_team_colors(away_t)
                hw = g.get("home_wins"); hl = g.get("home_losses")
                aw = g.get("away_wins"); al = g.get("away_losses")
                home_record = f"{hw}-{hl}" if hw is not None and hl is not None and (hw > 0 or hl > 0) else "N/A"
                away_record = f"{aw}-{al}" if aw is not None and al is not None and (aw > 0 or al > 0) else "N/A"

                home_players = [
                    r.get("player_name", "") for r in analysis_results
                    if r.get("player_team") == home_t and not r.get("player_is_out", False)
                ][:5]
                away_players = [
                    r.get("player_name", "") for r in analysis_results
                    if r.get("player_team") == away_t and not r.get("player_is_out", False)
                ][:5]

                with tc1:
                    st.markdown(
                        get_qds_team_card_html(
                            team_name=home_t,
                            team_abbrev=home_t,
                            record=home_record,
                            stats=[
                                {"label": "Game Total", "value": str(g.get("game_total", "N/A"))},
                                {"label": "Spread",     "value": str(g.get("vegas_spread", "N/A"))},
                            ],
                            key_players=home_players,
                            team_color=home_color,
                        ),
                        unsafe_allow_html=True,
                    )
                with tc2:
                    st.markdown(
                        get_qds_team_card_html(
                            team_name=away_t,
                            team_abbrev=away_t,
                            record=away_record,
                            stats=[
                                {"label": "Game Total", "value": str(g.get("game_total", "N/A"))},
                                {"label": "Spread",     "value": str(g.get("vegas_spread", "N/A"))},
                            ],
                            key_players=away_players,
                            team_color=away_color,
                        ),
                        unsafe_allow_html=True,
                    )

    st.divider()

    # ── Unified Expandable Player Cards ──────────────────────────
    # Each player gets one expandable card combining their identity
    # (headshot, name, team, season stats) with all their prop
    # analysis cards.  Click a card to expand and see full analysis.
    #
    # Rendered inside a self-resizing <iframe> via _render_card_iframe()
    # rather than inline st.markdown(). This eliminates the WebSocket-
    # ClosedError crash that occurred when large HTML payloads were sent
    # over the Tornado WebSocket mid-rerun.
    _active_results = [r for r in displayed_results if not r.get("player_is_out", False)]
    _grouped = _group_props(_active_results, players_data, todays_games)

    if _grouped:
        st.markdown(
            '<h3 style="font-family:\'Orbitron\',sans-serif;color:#00C6FF;'
            'margin-bottom:8px;">🃏 Quantum Analysis Matrix</h3>'
            '<p style="color:#94A3B8;font-size:0.82rem;margin-bottom:12px;">'
            'Click any player card to expand and view their full prop analysis.</p>',
            unsafe_allow_html=True,
        )

        # Pre-compute Joseph's Platinum Lock opinion for each player
        _joseph_opinions: dict = {}
        try:
            from engine.joseph_brain import joseph_platinum_lock as _joseph_lock
            for _pname, _pdata in _grouped.items():
                _props = _pdata.get("props", [])
                _stats = (_pdata.get("vitals") or {}).get("season_stats", {})
                if _props:
                    try:
                        _joseph_opinions[_pname] = _joseph_lock(_props, _stats)
                    except Exception:
                        _logger.debug("joseph_platinum_lock failed for %s", _pname, exc_info=True)
        except ImportError:
            _logger.debug("joseph_brain not available for card opinions")

        _unified_html = _compile_unified_matrix(_grouped, _joseph_opinions)
        _render_card_iframe(_unified_html, len(_grouped))

    # Show OUT players in a separate collapsed section
    _out_display = [r for r in displayed_results if r.get("player_is_out", False)]
    if _out_display:
        _out_grouped = _group_props(_out_display, players_data, todays_games)
        if _out_grouped:
            st.markdown(
                '<div style="font-size:0.78rem;color:#64748b;margin:12px 0 4px;">'
                '⚠️ OUT / Inactive Players</div>',
                unsafe_allow_html=True,
            )
            _out_unified_html = _compile_unified_matrix(_out_grouped)
            _render_card_iframe(_out_unified_html, len(_out_grouped))

    # ── Final Verdict ─────────────────────────────────────────────
    st.divider()
    with st.expander("🏁 Final Verdict", expanded=True):
        top_picks_for_verdict = [
            r for r in displayed_results
            if not r.get("player_is_out", False)
            and not r.get("should_avoid", False)
        ][:3]

        if top_picks_for_verdict:
            top_names  = ", ".join(r.get("player_name", "") for r in top_picks_for_verdict)
            avg_conf   = round(
                sum(r.get("confidence_score", 0) for r in top_picks_for_verdict)
                / len(top_picks_for_verdict), 1
            )
            summary    = (
                f"The Quantum Matrix Engine 5.6 identified {len(top_picks_for_verdict)} high-confidence "
                f"props led by {top_names}, with a composite confidence score of {avg_conf}/100. "
                f"Layer 5 injury validation and Quantum Matrix Engine 5.6 simulation align on these selections."
            )
        else:
            summary = (
                "No high-confidence picks were identified in the current analysis. "
                "Review injury status updates and consider adjusting your prop list."
            )

        recs = [
            "Focus on Platinum and Gold tier picks for maximum confidence.",
            "Avoid props flagged on the avoid list or with active GTD designations.",
            "Use the Entry Strategy Matrix to build 2-, 3-, or 5-leg combos.",
            "Confirm injury status via the 🔄 Data Feed before placing bets.",
        ]
        st.markdown(
            get_qds_final_verdict_html(summary, recs),
            unsafe_allow_html=True,
        )

    # ── Floating selected-picks counter ──────────────────────────
    selected_count = len(st.session_state.get("selected_picks", []))
    if selected_count > 0:
        st.success(
            f"✅ {selected_count} pick(s) selected for Entry Builder → "
            "Go to 🧬 Entry Builder to build your entry!"
        )

    if st.session_state.get("selected_picks"):
        if st.button("🗑️ Clear Selected Picks"):
            st.session_state["selected_picks"] = []
            st.rerun()

elif not run_analysis:
    if current_props:
        st.info("👆 Click **Run Analysis** to analyze all loaded props.")
    else:
        _has_games = bool(st.session_state.get("todays_games"))
        if _has_games:
            st.warning(
                "⚠️ No props loaded yet. "
                "Go to **🔬 Prop Scanner** and click **🤖 Auto-Generate Props for Tonight** "
                "to instantly create props for all active players on tonight's teams — "
                "or click **🔄 Auto-Load Tonight's Games** on the **📡 Live Games** page "
                "to reload games and auto-generate props in one step."
            )
        else:
            st.warning(
                "⚠️ No props loaded and no games found. "
                "Start on the **📡 Live Games** page — click **🔄 Auto-Load Tonight's Games** "
                "to load tonight's schedule and auto-generate props for all active players."
            )

# ============================================================
# END SECTION: Display Analysis Results
# ============================================================
