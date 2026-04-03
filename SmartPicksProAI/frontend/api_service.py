"""
api_service.py
--------------
HTTP client layer for the SmartPicksProAI Streamlit frontend.

Each public function wraps one FastAPI endpoint using the ``requests`` library.
GET requests are decorated with Streamlit's ``@st.cache_data(ttl=3600)`` so the
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
        get_team_synergy,
        get_play_by_play,
        get_win_probability,
        get_game_rotation,
        get_game_box_score,
        get_draft_history,
        get_lineups,
        get_league_dash_players,
        get_league_dash_teams,
        get_recent_games,
        get_schedule,
        trigger_refresh,
    )
"""

from __future__ import annotations

import logging

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

BASE_URL = "http://localhost:8000"


# ---------------------------------------------------------------------------
# Internal helper
# ---------------------------------------------------------------------------


def _get(
    path: str,
    params: dict | None = None,
    *,
    key: str | None,
    default=None,
    timeout: int = 15,
):
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
    except Exception as exc:
        logger.error("GET %s failed: %s", path, exc)
        return default


# ---------------------------------------------------------------------------
# GET endpoints (cached for 1 hour)
# ---------------------------------------------------------------------------


@st.cache_data(ttl=3600)
def get_todays_games() -> list[dict]:
    """Fetch today's NBA matchups from the backend."""
    return _get("/api/games/today", key="games")


@st.cache_data(ttl=3600)
def get_player_last5(player_id: int) -> dict:
    """Fetch a player's last 5 game logs from the backend."""
    return _get(f"/api/players/{player_id}/last5", key=None, default={})


@st.cache_data(ttl=3600)
def search_players(query: str) -> list[dict]:
    """Search for players by name."""
    return _get("/api/players/search", params={"q": query}, key="results")


@st.cache_data(ttl=3600)
def get_teams() -> list[dict]:
    """Fetch all NBA teams from the backend."""
    return _get("/api/teams", key="teams")


@st.cache_data(ttl=3600)
def get_team_roster(team_id: int) -> list[dict]:
    """Fetch a team's roster from the backend."""
    return _get(f"/api/teams/{team_id}/roster", key="players")


@st.cache_data(ttl=3600)
def get_team_stats(team_id: int, last_n: int = 10) -> list[dict]:
    """Fetch a team's recent game-level stats from the backend."""
    return _get(
        f"/api/teams/{team_id}/stats", params={"last_n": last_n}, key="games"
    )


@st.cache_data(ttl=3600)
def get_defense_vs_position(team_abbreviation: str) -> list[dict]:
    """Fetch defense-vs-position multipliers for a team."""
    return _get(
        f"/api/defense-vs-position/{team_abbreviation}", key="positions"
    )


@st.cache_data(ttl=3600)
def get_standings() -> list[dict]:
    """Fetch current NBA standings from the backend."""
    return _get("/api/standings", key="standings")


@st.cache_data(ttl=3600)
def get_league_leaders() -> list[dict]:
    """Fetch league leaders from the backend."""
    return _get("/api/league-leaders", key="leaders")


@st.cache_data(ttl=3600)
def get_player_bio(player_id: int) -> dict:
    """Fetch a player's biographical info from the backend."""
    return _get(f"/api/players/{player_id}/bio", key="bio", default={})


@st.cache_data(ttl=3600)
def get_player_career(player_id: int) -> list[dict]:
    """Fetch a player's career stats from the backend."""
    return _get(f"/api/players/{player_id}/career", key="career")


@st.cache_data(ttl=3600)
def get_player_advanced(player_id: int) -> list[dict]:
    """Fetch a player's advanced stats from the backend."""
    return _get(f"/api/players/{player_id}/advanced", key="advanced")


@st.cache_data(ttl=3600)
def get_player_shot_chart(player_id: int) -> list[dict]:
    """Fetch a player's shot chart data from the backend."""
    return _get(f"/api/players/{player_id}/shot-chart", key="shots")


@st.cache_data(ttl=3600)
def get_player_tracking(player_id: int) -> list[dict]:
    """Fetch a player's tracking stats from the backend."""
    return _get(f"/api/players/{player_id}/tracking", key="tracking")


@st.cache_data(ttl=3600)
def get_player_clutch(player_id: int) -> list[dict]:
    """Fetch a player's clutch stats from the backend."""
    return _get(f"/api/players/{player_id}/clutch", key="clutch")


@st.cache_data(ttl=3600)
def get_player_hustle(player_id: int) -> list[dict]:
    """Fetch a player's hustle stats from the backend."""
    return _get(f"/api/players/{player_id}/hustle", key="hustle")


@st.cache_data(ttl=3600)
def get_player_scoring(player_id: int) -> list[dict]:
    """Fetch a player's scoring stats from the backend."""
    return _get(f"/api/players/{player_id}/scoring", key="scoring")


@st.cache_data(ttl=3600)
def get_player_usage(player_id: int) -> list[dict]:
    """Fetch a player's usage stats from the backend."""
    return _get(f"/api/players/{player_id}/usage", key="usage")


@st.cache_data(ttl=3600)
def get_player_matchups(player_id: int) -> list[dict]:
    """Fetch a player's matchup data from the backend."""
    return _get(f"/api/players/{player_id}/matchups", key="matchups")


@st.cache_data(ttl=3600)
def get_team_details(team_id: int) -> dict:
    """Fetch detailed info for a team from the backend."""
    return _get(f"/api/teams/{team_id}/details", key="details", default={})


@st.cache_data(ttl=3600)
def get_team_clutch(team_id: int) -> list[dict]:
    """Fetch a team's clutch stats from the backend."""
    return _get(f"/api/teams/{team_id}/clutch", key="clutch")


@st.cache_data(ttl=3600)
def get_team_hustle(team_id: int) -> list[dict]:
    """Fetch a team's hustle stats from the backend."""
    return _get(f"/api/teams/{team_id}/hustle", key="hustle")


@st.cache_data(ttl=3600)
def get_team_estimated_metrics(team_id: int) -> list[dict]:
    """Fetch a team's estimated metrics from the backend."""
    return _get(f"/api/teams/{team_id}/estimated-metrics", key="metrics")


@st.cache_data(ttl=3600)
def get_team_synergy(team_id: int) -> list[dict]:
    """Fetch a team's synergy data from the backend."""
    return _get(f"/api/teams/{team_id}/synergy", key="synergy")


@st.cache_data(ttl=3600)
def get_play_by_play(game_id: str) -> list[dict]:
    """Fetch play-by-play data for a game from the backend."""
    return _get(f"/api/games/{game_id}/play-by-play", key="plays")


@st.cache_data(ttl=3600)
def get_win_probability(game_id: str) -> list[dict]:
    """Fetch win probability data for a game from the backend."""
    return _get(f"/api/games/{game_id}/win-probability", key="probabilities")


@st.cache_data(ttl=3600)
def get_game_rotation(game_id: str) -> list[dict]:
    """Fetch rotation data for a game from the backend."""
    return _get(f"/api/games/{game_id}/rotation", key="rotations")


@st.cache_data(ttl=3600)
def get_game_box_score(game_id: str) -> list[dict]:
    """Fetch box score data for a game from the backend."""
    return _get(f"/api/games/{game_id}/box-score", key="players")


@st.cache_data(ttl=3600)
def get_draft_history() -> list[dict]:
    """Fetch NBA draft history from the backend."""
    return _get("/api/draft-history", key="drafts")


@st.cache_data(ttl=3600)
def get_lineups() -> list[dict]:
    """Fetch lineup data from the backend."""
    return _get("/api/lineups", key="lineups")


@st.cache_data(ttl=3600)
def get_league_dash_players() -> list[dict]:
    """Fetch league dashboard player stats from the backend."""
    return _get("/api/league-dash/players", key="players")


@st.cache_data(ttl=3600)
def get_league_dash_teams() -> list[dict]:
    """Fetch league dashboard team stats from the backend."""
    return _get("/api/league-dash/teams", key="teams")


@st.cache_data(ttl=3600)
def get_recent_games() -> list[dict]:
    """Fetch recent NBA games from the backend."""
    return _get("/api/games/recent", key="games")


@st.cache_data(ttl=3600)
def get_schedule() -> list[dict]:
    """Fetch the NBA schedule from the backend."""
    return _get("/api/schedule", key="schedule")


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
        resp = requests.post(f"{BASE_URL}/api/admin/refresh-data", timeout=120)
        resp.raise_for_status()
        return resp.json()
    except Exception as exc:
        logger.error("Failed to trigger refresh: %s", exc)
        return {"status": "error", "message": str(exc)}
