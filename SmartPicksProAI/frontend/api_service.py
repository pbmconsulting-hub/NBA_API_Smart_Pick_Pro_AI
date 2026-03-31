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
        trigger_refresh,
    )
"""

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
# GET endpoints (cached for 1 hour)
# ---------------------------------------------------------------------------


@st.cache_data(ttl=3600)
def get_todays_games() -> list[dict]:
    """Fetch today's NBA matchups from the backend.

    Calls ``GET /api/games/today`` and returns the ``games`` list from the
    JSON response.  Results are cached by Streamlit for 3 600 seconds
    (1 hour) so repeated re-renders do not issue additional HTTP requests.

    Returns:
        A list of game dicts (each containing at least ``game_id`` and
        ``matchup``), or an empty list if the backend is unreachable.
    """
    try:
        resp = requests.get(f"{BASE_URL}/api/games/today", timeout=15)
        resp.raise_for_status()
        data = resp.json()
        return data.get("games", [])
    except Exception as exc:
        logger.error("Failed to fetch today's games: %s", exc)
        return []


@st.cache_data(ttl=3600)
def get_player_last5(player_id: int) -> dict:
    """Fetch a player's last 5 game logs from the backend.

    Calls ``GET /api/players/{player_id}/last5`` and returns the full JSON
    payload which includes ``player_id``, ``first_name``, ``last_name``,
    ``games`` (list of box-score dicts), and ``averages``.

    Results are cached by Streamlit for 3 600 seconds (1 hour).

    Args:
        player_id: The NBA player ID.

    Returns:
        The JSON response dict, or an empty dict if the backend is
        unreachable or the player is not found.
    """
    try:
        resp = requests.get(
            f"{BASE_URL}/api/players/{player_id}/last5", timeout=15
        )
        resp.raise_for_status()
        return resp.json()
    except Exception as exc:
        logger.error("Failed to fetch last-5 for player %d: %s", player_id, exc)
        return {}


@st.cache_data(ttl=3600)
def search_players(query: str) -> list[dict]:
    """Search for players by name.

    Calls ``GET /api/players/search?q=<query>`` and returns the ``results``
    list.  Cached for 1 hour.

    Args:
        query: Free-text search string (e.g. ``'LeBron'``).

    Returns:
        A list of matching player dicts, or an empty list on error.
    """
    try:
        resp = requests.get(
            f"{BASE_URL}/api/players/search",
            params={"q": query},
            timeout=15,
        )
        resp.raise_for_status()
        return resp.json().get("results", [])
    except Exception as exc:
        logger.error("Failed to search players for q=%s: %s", query, exc)
        return []


@st.cache_data(ttl=3600)
def get_teams() -> list[dict]:
    """Fetch the list of all NBA teams from the backend.

    Calls ``GET /api/teams`` and returns the ``teams`` list.
    Cached for 1 hour.

    Returns:
        A list of team dicts, or an empty list on error.
    """
    try:
        resp = requests.get(f"{BASE_URL}/api/teams", timeout=15)
        resp.raise_for_status()
        return resp.json().get("teams", [])
    except Exception as exc:
        logger.error("Failed to fetch teams: %s", exc)
        return []


@st.cache_data(ttl=3600)
def get_team_roster(team_id: int) -> list[dict]:
    """Fetch a team's roster from the backend.

    Calls ``GET /api/teams/{team_id}/roster`` and returns the ``players``
    list.  Cached for 1 hour.

    Args:
        team_id: The NBA team ID.

    Returns:
        A list of player dicts, or an empty list on error.
    """
    try:
        resp = requests.get(
            f"{BASE_URL}/api/teams/{team_id}/roster", timeout=15
        )
        resp.raise_for_status()
        return resp.json().get("players", [])
    except Exception as exc:
        logger.error("Failed to fetch roster for team %d: %s", team_id, exc)
        return []


@st.cache_data(ttl=3600)
def get_team_stats(team_id: int, last_n: int = 10) -> list[dict]:
    """Fetch a team's recent game-level stats from the backend.

    Calls ``GET /api/teams/{team_id}/stats?last_n=<last_n>`` and returns
    the ``games`` list.  Cached for 1 hour.

    Args:
        team_id: The NBA team ID.
        last_n:  Number of recent games to return (default 10).

    Returns:
        A list of game stat dicts, or an empty list on error.
    """
    try:
        resp = requests.get(
            f"{BASE_URL}/api/teams/{team_id}/stats",
            params={"last_n": last_n},
            timeout=15,
        )
        resp.raise_for_status()
        return resp.json().get("games", [])
    except Exception as exc:
        logger.error("Failed to fetch stats for team %d: %s", team_id, exc)
        return []


@st.cache_data(ttl=3600)
def get_defense_vs_position(team_abbreviation: str) -> list[dict]:
    """Fetch defense-vs-position multipliers for a specific team.

    Calls ``GET /api/defense-vs-position/{team_abbreviation}`` and returns
    the ``positions`` list.  Cached for 1 hour.

    Args:
        team_abbreviation: Three-letter team code (e.g. ``'BOS'``).

    Returns:
        A list of position multiplier dicts, or an empty list on error.
    """
    try:
        resp = requests.get(
            f"{BASE_URL}/api/defense-vs-position/{team_abbreviation}",
            timeout=15,
        )
        resp.raise_for_status()
        return resp.json().get("positions", [])
    except Exception as exc:
        logger.error(
            "Failed to fetch defense-vs-position for %s: %s",
            team_abbreviation,
            exc,
        )
        return []


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
