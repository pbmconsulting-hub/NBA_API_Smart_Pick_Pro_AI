"""
api_service.py
--------------
HTTP client layer for the SmartPicksProAI Streamlit frontend.

Each public function wraps one FastAPI endpoint using the ``requests`` library.
GET requests are decorated with Streamlit's ``@st.cache_data(ttl=CACHE_TTL_SECONDS)`` so the
frontend does not call the API on every UI re-render.

All functions return safe defaults (empty lists / dicts) when the backend is
unreachable, keeping the dashboard functional even if the API server is down.

Usage::

    from api_service import (
        get_todays_games,
        get_player_last5,
        search_players,
        get_teams,
        get_team_roster,
        get_team_stats,
        get_defense_vs_position,
        get_standings,
        get_league_leaders,
        get_player_bio,
        get_player_career,
        get_player_advanced,
        get_player_shot_chart,
        get_player_tracking,
        get_player_clutch,
        get_player_hustle,
        get_player_scoring,
        get_player_usage,
        get_player_matchups,
        get_team_details,
        get_team_clutch,
        get_team_hustle,
        get_team_estimated_metrics,
        get_play_by_play,
        get_win_probability,
        get_game_rotation,
        get_game_box_score,
        get_league_dash_players,
        get_league_dash_teams,
        get_recent_games,
        get_schedule,
        trigger_refresh,
    )
"""

from __future__ import annotations

import logging
from typing import Any

import requests
import streamlit as st

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

BASE_URL = "http://localhost:8098"
CACHE_TTL_SECONDS = 3600          # 1 hour — standings, rosters, player bios
CACHE_TTL_LIVE = 300              # 5 min  — today's games, recent games
DEFAULT_REQUEST_TIMEOUT = 15
REFRESH_REQUEST_TIMEOUT = 120


# ---------------------------------------------------------------------------
# Internal helper
# ---------------------------------------------------------------------------


def _get(
    path: str,
    params: dict | None = None,
    *,
    key: str | None,
    default: Any = None,
    timeout: int = DEFAULT_REQUEST_TIMEOUT,
) -> Any:
    """Generic GET request to the backend API.

    Args:
        path: URL path relative to BASE_URL (e.g. "/api/teams").
        params: Optional query parameters dict.
        key: JSON response key to extract (e.g. "teams", "games").
            When *None*, the full JSON response is returned.
        default: Default value to return on error.  Defaults to ``[]``.
        timeout: Request timeout in seconds.

    Returns:
        The extracted value from the JSON response, or *default* on error.
    """
    if default is None:
        default = []
    try:
        resp = requests.get(f"{BASE_URL}{path}", params=params, timeout=timeout)
        resp.raise_for_status()
        data = resp.json()
        return data if key is None else data.get(key, default)
    except (requests.RequestException, ValueError) as exc:
        logger.error("GET %s failed: %s", path, exc)
        return default


# ---------------------------------------------------------------------------
# GET endpoints (cached for 1 hour)
# ---------------------------------------------------------------------------


@st.cache_data(ttl=CACHE_TTL_LIVE)
def get_todays_games() -> list[dict]:
    """Fetch today's NBA matchups from the backend."""
    return _get("/api/games/today", key="games")


@st.cache_data(ttl=CACHE_TTL_LIVE)
def get_player_last5(player_id: int) -> dict:
    """Fetch a player's last 5 game logs from the backend."""
    return _get(f"/api/players/{player_id}/last5", key=None, default={})


@st.cache_data(ttl=CACHE_TTL_SECONDS)
def search_players(query: str) -> list[dict]:
    """Search for players by name."""
    return _get("/api/players/search", params={"q": query}, key="results")


@st.cache_data(ttl=CACHE_TTL_SECONDS)
def get_teams() -> list[dict]:
    """Fetch all NBA teams from the backend."""
    return _get("/api/teams", key="teams")


@st.cache_data(ttl=CACHE_TTL_SECONDS)
def get_team_roster(team_id: int) -> list[dict]:
    """Fetch a team's roster from the backend."""
    return _get(f"/api/teams/{team_id}/roster", key="players")


@st.cache_data(ttl=CACHE_TTL_SECONDS)
def get_team_stats(team_id: int, last_n: int = 10) -> list[dict]:
    """Fetch a team's recent game-level stats from the backend."""
    return _get(
        f"/api/teams/{team_id}/stats", params={"last_n": last_n}, key="games"
    )


@st.cache_data(ttl=CACHE_TTL_LIVE)
def get_defense_vs_position(team_abbreviation: str) -> list[dict]:
    """Fetch defense-vs-position multipliers for a team."""
    return _get(
        f"/api/defense-vs-position/{team_abbreviation}", key="positions"
    )


@st.cache_data(ttl=CACHE_TTL_SECONDS)
def get_standings() -> list[dict]:
    """Fetch current NBA standings from the backend."""
    return _get("/api/standings", key="standings")


@st.cache_data(ttl=CACHE_TTL_SECONDS)
def get_league_leaders() -> list[dict]:
    """Fetch league leaders from the backend."""
    return _get("/api/league-leaders", key="leaders")


@st.cache_data(ttl=CACHE_TTL_SECONDS)
def get_player_bio(player_id: int) -> dict:
    """Fetch a player's biographical info from the backend."""
    return _get(f"/api/players/{player_id}/bio", key="bio", default={})


@st.cache_data(ttl=CACHE_TTL_SECONDS)
def get_player_career(player_id: int) -> list[dict]:
    """Fetch a player's career stats from the backend."""
    return _get(f"/api/players/{player_id}/career", key="career")


@st.cache_data(ttl=CACHE_TTL_SECONDS)
def get_player_advanced(player_id: int) -> list[dict]:
    """Fetch a player's advanced stats from the backend."""
    return _get(f"/api/players/{player_id}/advanced", key="advanced")


@st.cache_data(ttl=CACHE_TTL_SECONDS)
def get_player_shot_chart(player_id: int) -> list[dict]:
    """Fetch a player's shot chart data from the backend."""
    return _get(f"/api/players/{player_id}/shot-chart", key="shots")


@st.cache_data(ttl=CACHE_TTL_SECONDS)
def get_player_tracking(player_id: int) -> list[dict]:
    """Fetch a player's tracking stats from the backend."""
    return _get(f"/api/players/{player_id}/tracking", key="tracking")


@st.cache_data(ttl=CACHE_TTL_SECONDS)
def get_player_clutch(player_id: int) -> list[dict]:
    """Fetch a player's clutch stats from the backend."""
    return _get(f"/api/players/{player_id}/clutch", key="clutch")


@st.cache_data(ttl=CACHE_TTL_SECONDS)
def get_player_hustle(player_id: int) -> list[dict]:
    """Fetch a player's hustle stats from the backend."""
    return _get(f"/api/players/{player_id}/hustle", key="hustle")


@st.cache_data(ttl=CACHE_TTL_SECONDS)
def get_player_scoring(player_id: int) -> list[dict]:
    """Fetch a player's scoring stats from the backend."""
    return _get(f"/api/players/{player_id}/scoring", key="scoring")


@st.cache_data(ttl=CACHE_TTL_SECONDS)
def get_player_usage(player_id: int) -> list[dict]:
    """Fetch a player's usage stats from the backend."""
    return _get(f"/api/players/{player_id}/usage", key="usage")


@st.cache_data(ttl=CACHE_TTL_SECONDS)
def get_player_matchups(player_id: int) -> list[dict]:
    """Fetch a player's matchup data from the backend."""
    return _get(f"/api/players/{player_id}/matchups", key="matchups")


@st.cache_data(ttl=CACHE_TTL_SECONDS)
def get_team_details(team_id: int) -> dict:
    """Fetch detailed info for a team from the backend."""
    return _get(f"/api/teams/{team_id}/details", key="details", default={})


@st.cache_data(ttl=CACHE_TTL_SECONDS)
def get_team_clutch(team_id: int) -> list[dict]:
    """Fetch a team's clutch stats from the backend."""
    return _get(f"/api/teams/{team_id}/clutch", key="clutch")


@st.cache_data(ttl=CACHE_TTL_SECONDS)
def get_team_hustle(team_id: int) -> list[dict]:
    """Fetch a team's hustle stats from the backend."""
    return _get(f"/api/teams/{team_id}/hustle", key="hustle")


@st.cache_data(ttl=CACHE_TTL_SECONDS)
def get_team_estimated_metrics(team_id: int) -> list[dict]:
    """Fetch a team's estimated metrics from the backend."""
    return _get(f"/api/teams/{team_id}/estimated-metrics", key="metrics")


@st.cache_data(ttl=CACHE_TTL_SECONDS)
def get_play_by_play(game_id: str) -> list[dict]:
    """Fetch play-by-play data for a game from the backend."""
    return _get(f"/api/games/{game_id}/play-by-play", key="plays")


@st.cache_data(ttl=CACHE_TTL_SECONDS)
def get_win_probability(game_id: str) -> list[dict]:
    """Fetch win probability data for a game from the backend."""
    return _get(f"/api/games/{game_id}/win-probability", key="probabilities")


@st.cache_data(ttl=CACHE_TTL_SECONDS)
def get_game_rotation(game_id: str) -> list[dict]:
    """Fetch rotation data for a game from the backend."""
    return _get(f"/api/games/{game_id}/rotation", key="rotations")


@st.cache_data(ttl=CACHE_TTL_SECONDS)
def get_game_box_score(game_id: str) -> list[dict]:
    """Fetch box score data for a game from the backend."""
    return _get(f"/api/games/{game_id}/box-score", key="players")


@st.cache_data(ttl=CACHE_TTL_SECONDS)
def get_league_dash_players() -> list[dict]:
    """Fetch league dashboard player stats from the backend."""
    return _get("/api/league-dash/players", key="players")


@st.cache_data(ttl=CACHE_TTL_SECONDS)
def get_league_dash_teams() -> list[dict]:
    """Fetch league dashboard team stats from the backend."""
    return _get("/api/league-dash/teams", key="teams")


@st.cache_data(ttl=CACHE_TTL_LIVE)
def get_recent_games() -> list[dict]:
    """Fetch recent NBA games from the backend."""
    return _get("/api/games/recent", key="games")


@st.cache_data(ttl=CACHE_TTL_LIVE)
def get_schedule() -> list[dict]:
    """Fetch the NBA schedule from the backend."""
    return _get("/api/schedule", key="schedule")


# ---------------------------------------------------------------------------
# Engine-powered analysis endpoints
# ---------------------------------------------------------------------------


@st.cache_data(ttl=CACHE_TTL_SECONDS)
def get_player_projection(
    player_id: int,
    opponent: str | None = None,
    vegas_spread: float = 0.0,
    game_total: float = 220.0,
) -> dict:
    """Fetch an engine-powered stat projection for a player.

    Calls ``GET /api/players/{player_id}/projection``.

    Args:
        player_id: NBA player ID.
        opponent: Opponent team abbreviation (auto-detected if omitted).
        vegas_spread: Vegas point spread.
        game_total: Vegas over/under game total.

    Returns:
        Projection dict, or empty dict on error.
    """
    params: dict = {"vegas_spread": vegas_spread, "game_total": game_total}
    if opponent:
        params["opponent"] = opponent
    return _get(
        f"/api/players/{player_id}/projection",
        params=params,
        key=None,
        default={},
    )


def analyze_prop(
    player_id: int,
    stat_type: str,
    prop_line: float,
    opponent: str | None = None,
    vegas_spread: float = 0.0,
    game_total: float = 220.0,
    platform: str = "prizepicks",
) -> dict:
    """Run a full prop analysis via the engine.

    Calls ``POST /api/picks/analyze``.  Not cached because the user may
    re-analyse with different lines.

    Args:
        player_id: NBA player ID.
        stat_type: Engine stat type (e.g. ``'points'``).
        prop_line: The prop line value.
        opponent: Opponent abbreviation (auto-detected if omitted).
        vegas_spread: Vegas spread.
        game_total: Over/under total.
        platform: Betting platform name.

    Returns:
        Full analysis dict, or ``{"status": "error"}`` on failure.
    """
    payload = {
        "player_id": player_id,
        "stat_type": stat_type,
        "prop_line": prop_line,
        "vegas_spread": vegas_spread,
        "game_total": game_total,
        "platform": platform,
    }
    if opponent:
        payload["opponent"] = opponent
    try:
        resp = requests.post(
            f"{BASE_URL}/api/picks/analyze",
            json=payload,
            timeout=DEFAULT_REQUEST_TIMEOUT * 2,
        )
        resp.raise_for_status()
        return resp.json()
    except (requests.RequestException, ValueError) as exc:
        logger.error("POST /api/picks/analyze failed: %s", exc)
        return {"status": "error", "message": str(exc)}


# ---------------------------------------------------------------------------
# POST endpoints (never cached — always live)
# ---------------------------------------------------------------------------


def trigger_refresh() -> dict:
    """Trigger an on-demand data refresh via the backend.

    Calls ``POST /api/admin/refresh-data``.  This is intentionally **not**
    cached — every button press should issue a real HTTP request.

    Returns:
        The JSON response dict (``status``, ``new_records``, ``message``),
        or a dict with ``status: "error"`` if the call fails.
    """
    try:
        resp = requests.post(f"{BASE_URL}/api/admin/refresh-data", timeout=REFRESH_REQUEST_TIMEOUT)
        resp.raise_for_status()
        return resp.json()
    except (requests.RequestException, ValueError) as exc:
        logger.error("Failed to trigger refresh: %s", exc)
        return {"status": "error", "message": str(exc)}


# ---------------------------------------------------------------------------
# Pick-tracking endpoints (Phase 3)
# ---------------------------------------------------------------------------


def save_pick(pick_data: dict) -> dict:
    """Persist a pick analysis result for future tracking.

    Calls ``POST /api/picks/save``.

    Args:
        pick_data: Dict matching the SavePickRequest schema.

    Returns:
        ``{"status": "saved", "pick_id": <int>}`` on success,
        or ``{"status": "error", ...}`` on failure.
    """
    try:
        resp = requests.post(
            f"{BASE_URL}/api/picks/save",
            json=pick_data,
            timeout=DEFAULT_REQUEST_TIMEOUT,
        )
        resp.raise_for_status()
        return resp.json()
    except (requests.RequestException, ValueError) as exc:
        logger.error("POST /api/picks/save failed: %s", exc)
        return {"status": "error", "message": str(exc)}


@st.cache_data(ttl=CACHE_TTL_SECONDS)
def get_pick_history(limit: int = 50) -> list[dict]:
    """Retrieve saved pick history from the backend.

    Calls ``GET /api/picks/history``.

    Args:
        limit: Maximum number of picks to return.

    Returns:
        List of pick dicts, newest first.
    """
    return _get("/api/picks/history", params={"limit": limit}, key="picks")


def update_pick_result(pick_id: int, result: str, actual_value: float | None = None) -> dict:
    """Record the outcome of a previously saved pick.

    Calls ``POST /api/picks/result``.

    Args:
        pick_id: The pick's database ID.
        result: ``'hit'``, ``'miss'``, or ``'push'``.
        actual_value: The player's actual stat value, if known.

    Returns:
        ``{"status": "updated", "pick_id": <int>}`` or error dict.
    """
    payload: dict = {"pick_id": pick_id, "result": result}
    if actual_value is not None:
        payload["actual_value"] = actual_value
    try:
        resp = requests.post(
            f"{BASE_URL}/api/picks/result",
            json=payload,
            timeout=DEFAULT_REQUEST_TIMEOUT,
        )
        resp.raise_for_status()
        return resp.json()
    except (requests.RequestException, ValueError) as exc:
        logger.error("POST /api/picks/result failed: %s", exc)
        return {"status": "error", "message": str(exc)}


# ---------------------------------------------------------------------------
# DFS prop-line lookup
# ---------------------------------------------------------------------------


@st.cache_data(ttl=CACHE_TTL_LIVE)
def get_dfs_lines(player_id: int, stat_type: str) -> dict:
    """Fetch DFS platform lines for a player and stat type.

    Calls ``GET /api/dfs/lines``.

    Returns:
        ``{"lines": {"PrizePicks": 24.5, ...}, "consensus": 24.5}``
        or empty dict on error.
    """
    return _get(
        "/api/dfs/lines",
        params={"player_id": player_id, "stat_type": stat_type},
        key=None,
        default={},
    )


@st.cache_data(ttl=CACHE_TTL_LIVE)
def get_todays_slate(top_n: int = 5) -> dict:
    """Fetch today's top AI-generated picks from the autonomous slate builder.

    Calls ``GET /api/slate/today``.

    Returns:
        Slate dict with ``picks``, ``games_scanned``, etc., or empty dict on error.
    """
    return _get(
        "/api/slate/today",
        params={"top_n": top_n, "min_edge": 2.0},
        key=None,
        default={},
    )
