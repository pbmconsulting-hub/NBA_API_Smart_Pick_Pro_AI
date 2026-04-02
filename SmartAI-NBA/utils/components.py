# ============================================================
# FILE: utils/components.py
# PURPOSE: Shared UI components for the SmartBetPro NBA app.
#          Contains the global settings popover that can be
#          injected into any page's sidebar or header.
# ============================================================

import os
import base64
import logging
import streamlit as st

_components_logger = logging.getLogger(__name__)


# ── Cached Hero Banner Loader ─────────────────────────────────────────────
@st.cache_data(show_spinner=False)
def _get_hero_banner_b64() -> str:
    """Load the Joseph M Smith Hero Banner and return base64-encoded string."""
    _this = os.path.dirname(os.path.abspath(__file__))
    candidates = [
        os.path.join(_this, "..", "Joseph M Smith Hero Banner.png"),
        os.path.join(os.getcwd(), "Joseph M Smith Hero Banner.png"),
        os.path.join(_this, "..", "assets", "Joseph M Smith Hero Banner.png"),
    ]
    for path in candidates:
        norm = os.path.normpath(path)
        if os.path.isfile(norm):
            try:
                with open(norm, "rb") as fh:
                    _components_logger.debug("Hero banner loaded from %s", norm)
                    return base64.b64encode(fh.read()).decode("utf-8")
            except Exception:
                _components_logger.warning("Failed reading hero banner at %s", norm)
    _components_logger.warning("Joseph hero banner not found in any candidate path")
    return ""


def render_joseph_hero_banner() -> None:
    """Render the Joseph M Smith Hero Banner at the top of the page."""
    b64 = _get_hero_banner_b64()
    if not b64:
        return
    st.markdown(
        f'<div style="width:100%;margin-bottom:12px;">'
        f'<img src="data:image/png;base64,{b64}" '
        f'style="width:100%;border-radius:10px;box-shadow:0 4px 20px rgba(0,0,0,0.4);" '
        f'alt="Joseph M Smith Hero Banner" />'
        f'</div>',
        unsafe_allow_html=True,
    )


def render_global_settings():
    """Render an inline settings popover for edge threshold and simulation depth.

    Uses ``st.popover`` so users can adjust core engine parameters without
    leaving the current page.  Widget values are bound directly to
    ``st.session_state`` keys that the analysis engine already reads
    (``minimum_edge_threshold``, ``simulation_depth``), so changes
    propagate instantly on the next rerun.
    """
    with st.popover("⚙️ Settings"):
        st.markdown(
            "**Quantum Matrix Engine 5.6 — Quick Settings**"
        )

        # ── Simulation Depth ──────────────────────────────────────
        st.number_input(
            "Simulation Depth",
            min_value=100,
            max_value=10000,
            step=100,
            value=st.session_state.get("simulation_depth", 1000),
            key="sim_depth_widget",
            help="Number of Monte Carlo simulations per prop. Higher = more accurate but slower.",
            on_change=_sync_sim_depth,
        )

        # ── Minimum Edge Threshold ────────────────────────────────
        st.number_input(
            "Min Edge Threshold (%)",
            min_value=0.0,
            max_value=50.0,
            step=0.5,
            value=float(st.session_state.get("minimum_edge_threshold", 5.0)),
            key="edge_threshold_widget",
            help="Only display props with an edge at or above this percentage.",
            on_change=_sync_edge_threshold,
        )

        # ── Entry Fee ─────────────────────────────────────────────
        st.number_input(
            "Entry Fee ($)",
            min_value=1.0,
            max_value=1000.0,
            step=1.0,
            value=float(st.session_state.get("entry_fee", 10.0)),
            key="entry_fee_widget",
            help="Default dollar amount per entry for EV calculations.",
            on_change=_sync_entry_fee,
        )

        st.divider()

        # ── Total Bankroll ────────────────────────────────────────
        st.number_input(
            "Total Bankroll ($)",
            min_value=10.0,
            max_value=1_000_000.0,
            step=50.0,
            value=float(st.session_state.get("total_bankroll", 1000.0)),
            key="total_bankroll_widget",
            help="Your total bankroll in dollars. Used for Kelly Criterion bet sizing.",
            on_change=_sync_total_bankroll,
        )

        # ── Kelly Multiplier ──────────────────────────────────────
        st.slider(
            "Kelly Multiplier",
            min_value=0.1,
            max_value=1.0,
            step=0.05,
            value=float(st.session_state.get("kelly_multiplier", 0.25)),
            key="kelly_multiplier_widget",
            help=(
                "Fraction of the full Kelly bet to use. "
                "0.25 = Quarter Kelly (conservative, recommended). "
                "1.0 = Full Kelly (aggressive, higher variance)."
            ),
            on_change=_sync_kelly_multiplier,
        )

        st.caption("Changes apply on next analysis run.")

    # ── Responsible Gambling Disclaimer ───────────────────────────
    render_sidebar_disclaimer()


def inject_joseph_floating():
    """Render the Joseph M. Smith floating widget in the main content area.

    Delegates to :func:`utils.joseph_widget.render_joseph_floating_widget`
    so the widget appears on every page that calls this helper.
    Also renders the responsible gambling disclaimer in the sidebar.
    """
    try:
        from utils.joseph_widget import render_joseph_floating_widget
        render_joseph_floating_widget()
    except Exception as exc:
        _components_logger.debug("inject_joseph_floating failed: %s", exc)
    # Show the disclaimer on every page that calls this helper
    render_sidebar_disclaimer()


def render_sidebar_disclaimer():
    """Render a collapsed responsible gambling disclaimer in the sidebar.

    Uses a session-state flag to avoid rendering the same disclaimer
    twice on pages that call both ``render_global_settings()`` and
    ``inject_joseph_floating()``.
    """
    if st.session_state.get("_disclaimer_rendered"):
        return
    st.session_state["_disclaimer_rendered"] = True
    with st.sidebar:
        with st.expander("⚠️ Responsible Gambling", expanded=False):
            st.caption(
                "This app is for **personal entertainment and analysis** only. "
                "Always gamble responsibly. Past model performance does not guarantee "
                "future results. Prop betting involves risk. Never bet more than you "
                "can afford to lose."
            )


# ── on_change callbacks ──────────────────────────────────────────
# These propagate widget values into the canonical session-state keys
# that the rest of the app reads (simulation_depth, minimum_edge_threshold,
# entry_fee).  Each uses .get() with a safe default in case the widget
# key hasn't been registered yet (avoids KeyError on first render).

def _sync_sim_depth():
    st.session_state["simulation_depth"] = st.session_state.get(
        "sim_depth_widget", st.session_state.get("simulation_depth", 1000)
    )


def _sync_edge_threshold():
    st.session_state["minimum_edge_threshold"] = st.session_state.get(
        "edge_threshold_widget", st.session_state.get("minimum_edge_threshold", 5.0)
    )


def _sync_entry_fee():
    st.session_state["entry_fee"] = st.session_state.get(
        "entry_fee_widget", st.session_state.get("entry_fee", 10.0)
    )


def _sync_total_bankroll():
    st.session_state["total_bankroll"] = st.session_state.get(
        "total_bankroll_widget", st.session_state.get("total_bankroll", 1000.0)
    )


def _sync_kelly_multiplier():
    st.session_state["kelly_multiplier"] = st.session_state.get(
        "kelly_multiplier_widget", st.session_state.get("kelly_multiplier", 0.25)
    )
