"""Shared constants and helpers for all page modules.

Single source of truth for tier/emoji/color dicts (fixes Q1 duplication)
and reusable column-list constants (Q2).
"""

from __future__ import annotations

import datetime
import json
from pathlib import Path
from typing import Any

import pandas as pd
import streamlit as st

# ── Tier / emoji / color constants (Q1: single definition) ──────────────

TIER_EMOJI: dict[str, str] = {
    "Platinum": "💎",
    "Gold": "🥇",
    "Silver": "🥈",
    "Bronze": "🥉",
    "Avoid": "⛔",
}

TIER_COLORS: dict[str, str] = {
    "Platinum": "#E5E4E2",
    "Gold": "#FFD700",
    "Silver": "#C0C0C0",
    "Bronze": "#CD7F32",
    "Avoid": "#FF4444",
}

RESULT_EMOJI: dict[str, str] = {
    "hit": "✅",
    "miss": "❌",
    "push": "➖",
}

BET_RESULT_EMOJI: dict[str, str] = {
    "win": "✅",
    "loss": "❌",
    "push": "➖",
}

# ── Stat types ──────────────────────────────────────────────────────────

ANALYSIS_STAT_TYPES: list[str] = [
    "points", "rebounds", "assists", "threes",
    "steals", "blocks", "turnovers",
]

TRACKER_STAT_TYPES: list[str] = [
    "points", "rebounds", "assists", "threes",
    "steals", "blocks", "turnovers",
    "pts+reb", "pts+ast", "reb+ast", "pts+reb+ast",
]

# ── Layout constants ────────────────────────────────────────────────────

MAX_GAME_COLUMNS = 4
MAX_RECENT_GAMES = 20
MAX_SEARCH_RESULTS = 10

# Break-even win rate at standard -110 juice (M3).
BREAK_EVEN_WIN_RATE = 52.4

# Calibration gap (in pp) before flagging over/underconfidence.
CALIBRATION_GAP_THRESHOLD = 10

# Default bankroll used when the user has not set one.
DEFAULT_BANKROLL = 500.0

# Bankroll persistence path (F14).
_SETTINGS_DIR = Path.home() / ".smartpickpro"
_SETTINGS_FILE = _SETTINGS_DIR / "settings.json"


# ── Bankroll persistence helpers (F14) ──────────────────────────────────

def load_persisted_bankroll() -> float | None:
    """Return the bankroll from the settings file, or *None*."""
    try:
        if _SETTINGS_FILE.exists():
            data = json.loads(_SETTINGS_FILE.read_text())
            return float(data.get("bankroll", DEFAULT_BANKROLL))
    except Exception:
        pass
    return None


def save_persisted_bankroll(value: float) -> None:
    """Persist *value* as the user bankroll."""
    try:
        _SETTINGS_DIR.mkdir(parents=True, exist_ok=True)
        data: dict[str, Any] = {}
        if _SETTINGS_FILE.exists():
            data = json.loads(_SETTINGS_FILE.read_text())
        data["bankroll"] = value
        _SETTINGS_FILE.write_text(json.dumps(data))
    except Exception:
        pass  # Never crash the UI for a settings write failure.


# ── Navigation helper ───────────────────────────────────────────────────

def nav(page: str, **kwargs: Any) -> None:
    """Navigate to *page*, setting extra session-state keys."""
    st.session_state.page = page
    for k, v in kwargs.items():
        st.session_state[k] = v


# ── Dataframe helper ────────────────────────────────────────────────────

def show_df(
    data: list[dict[str, Any]] | pd.DataFrame,
    columns: list[str] | None = None,
    height: int | None = None,
) -> None:
    """Display *data* as a styled dataframe."""
    if not data:
        st.markdown(
            '<div class="empty-state">No data available.</div>',
            unsafe_allow_html=True,
        )
        return
    df = pd.DataFrame(data) if isinstance(data, list) else data
    if columns:
        columns = [c for c in columns if c in df.columns]
        if columns:
            df = df[columns]
    kw: dict[str, Any] = {"use_container_width": True, "hide_index": True}
    if height:
        kw["height"] = height
    st.dataframe(df, **kw)


# ── Button helpers ──────────────────────────────────────────────────────

def player_button(
    player_id: int,
    name: str,
    position: str | None = None,
    team: str | None = None,
    key_prefix: str = "",
) -> None:
    """Render a clickable button that navigates to a player profile."""
    parts = [name]
    if position:
        parts.append(f"({position})")
    if team:
        parts.append(f"· {team}")
    label = " ".join(parts)
    if st.button(
        f"👤 {label}",
        key=f"{key_prefix}_p_{player_id}",
        use_container_width=True,
    ):
        nav("player_profile", selected_player_id=player_id)
        st.rerun()


def game_button(game: dict[str, Any], key_prefix: str = "") -> None:
    """Render a clickable game-card button."""
    matchup = game.get("matchup", "TBD")
    home_score = game.get("home_score")
    away_score = game.get("away_score")
    game_date = game.get("game_date", "")
    gid = game.get("game_id", "")

    if home_score is not None and away_score is not None:
        label = f"🏀 {matchup}  |  {home_score} – {away_score}  |  {game_date}"
    else:
        label = f"🏀 {matchup}  |  {game_date}"

    if st.button(label, key=f"{key_prefix}_g_{gid}", use_container_width=True):
        nav("game_detail", selected_game_id=gid, game_context=game)
        st.rerun()
