"""engine/scrapers/balldontlie_client.py – BallDontLie API REST client (primary data source).

Covers all NBA endpoints from the BallDontLie v1 API with:
- API key authentication via Authorization header
- TTL-based in-memory caching (LIVE_TTL=300s, HIST_TTL=3600s)
- Configurable rate-limiting delay (default 1.0 s, free tier)
- Graceful degradation: returns [] or {} on any failure
"""
from __future__ import annotations

import logging
import os
import time
from typing import Any

try:
    from utils.logger import get_logger
    _logger = get_logger(__name__)
except Exception:
    _logger = logging.getLogger(__name__)

_BASE_URL = "https://api.balldontlie.io"
_DELAY = 1.0  # free tier: 1 request/second

# ── TTL constants (seconds) ──────────────────────────────────────────────────
LIVE_TTL: int = 300    # 5 minutes  — live/box-score data
HIST_TTL: int = 3600   # 1 hour     — historical / season data

# ── In-memory cache ──────────────────────────────────────────────────────────
_CACHE: dict[str, tuple[Any, float]] = {}


def _cache_get(key: str, ttl: int) -> Any | None:
    entry = _CACHE.get(key)
    if entry is None:
        return None
    payload, ts = entry
    if time.time() - ts > ttl:
        _CACHE.pop(key, None)
        return None
    return payload


def _cache_set(key: str, payload: Any) -> None:
    _CACHE[key] = (payload, time.time())


# ── requests import (graceful) ───────────────────────────────────────────────
try:
    import requests
    from requests.exceptions import ConnectionError as _ConnError, Timeout as _Timeout
    _REQUESTS_AVAILABLE = True
except ImportError:
    _REQUESTS_AVAILABLE = False
    # Fallback exception types used only to satisfy the except clause syntax;
    # when requests is unavailable, _get() early-returns before any network call.
    _ConnError = ConnectionError  # type: ignore[misc,assignment]
    _Timeout = TimeoutError       # type: ignore[misc,assignment]
    _logger.debug("requests not installed; balldontlie_client unavailable")


# ── API key loading ──────────────────────────────────────────────────────────
def _load_api_key() -> str:
    """Return the BallDontLie API key from env or Streamlit secrets."""
    key = os.environ.get("BALLDONTLIE_API_KEY", "")
    if not key:
        try:
            import streamlit as st
            key = st.secrets.get("BALLDONTLIE_API_KEY", "")
        except Exception:
            pass
    return key or ""


def has_api_key() -> bool:
    """Return True if a BallDontLie API key is configured."""
    return bool(_load_api_key())


# ── Core request helper ──────────────────────────────────────────────────────
def _get(path: str, params: dict | None = None) -> dict:
    """Make an authenticated GET request to the BallDontLie API.

    Args:
        path: Full path including version prefix, e.g. ``/nba/v1/players``.
        params: Optional query parameters.

    Returns:
        Parsed JSON response dict, or empty dict on failure.
    """
    if not _REQUESTS_AVAILABLE:
        return {}

    time.sleep(_DELAY)
    url = f"{_BASE_URL}{path}"
    headers: dict[str, str] = {}
    api_key = _load_api_key()
    if api_key:
        headers["Authorization"] = api_key

    try:
        resp = requests.get(url, params=params or {}, headers=headers, timeout=10)
        if resp.status_code == 401:
            _logger.error("BallDontLie API: invalid or missing API key (401) for %s — "
                          "set BALLDONTLIE_API_KEY in env or Streamlit secrets", path)
            return {}
        if resp.status_code == 403:
            _logger.error("BallDontLie API: forbidden (403) for %s — "
                          "your plan may not include this endpoint", path)
            return {}
        if resp.status_code == 404:
            _logger.error("BallDontLie API: endpoint not found (404) for %s", path)
            return {}
        if resp.status_code == 429:
            _logger.warning("BallDontLie API: rate limit hit (429) for %s", path)
            return {}
        resp.raise_for_status()
        return resp.json()
    except _ConnError as exc:
        _logger.error("BallDontLie API: connection failed for %s — "
                      "check network connectivity: %s", path, exc)
        return {}
    except _Timeout as exc:
        _logger.error("BallDontLie API: request timed out for %s: %s", path, exc)
        return {}
    except Exception as exc:
        _logger.error("BallDontLie API error [%s]: %s", path, exc)
        return {}


# ═══════════════════════════════════════════════════════════════════════════════
# Teams
# ═══════════════════════════════════════════════════════════════════════════════

def get_teams(conference: str | None = None, division: str | None = None) -> list:
    """Return all NBA teams, optionally filtered by conference/division.

    GET /nba/v1/teams
    """
    cache_key = f"bdl:teams:{conference}:{division}"
    cached = _cache_get(cache_key, HIST_TTL)
    if cached is not None:
        return cached

    params: dict = {}
    if conference:
        params["conference"] = conference
    if division:
        params["division"] = division

    data = _get("/nba/v1/teams", params)
    result = data.get("data", [])
    _cache_set(cache_key, result)
    return result


def get_team(team_id: int) -> dict:
    """Return a single NBA team by ID.

    GET /nba/v1/teams/{id}
    """
    cache_key = f"bdl:team:{team_id}"
    cached = _cache_get(cache_key, HIST_TTL)
    if cached is not None:
        return cached

    data = _get(f"/nba/v1/teams/{team_id}")
    result = data.get("data", {})
    if result:
        _cache_set(cache_key, result)
    return result


# ═══════════════════════════════════════════════════════════════════════════════
# Players
# ═══════════════════════════════════════════════════════════════════════════════

def get_players(
    search: str | None = None,
    team_id: int | None = None,
    per_page: int = 25,
) -> list:
    """Return players, optionally filtered by name search or team.

    GET /nba/v1/players
    """
    cache_key = f"bdl:players:{search}:{team_id}:{per_page}"
    cached = _cache_get(cache_key, HIST_TTL)
    if cached is not None:
        return cached

    params: dict = {"per_page": per_page}
    if search:
        params["search"] = search
    if team_id is not None:
        params["team_ids[]"] = team_id

    data = _get("/nba/v1/players", params)
    result = data.get("data", [])
    _cache_set(cache_key, result)
    return result


def get_active_players(
    search: str | None = None,
    team_id: int | None = None,
    per_page: int = 25,
) -> list:
    """Return currently active players.

    GET /nba/v1/players/active
    """
    cache_key = f"bdl:active_players:{search}:{team_id}:{per_page}"
    cached = _cache_get(cache_key, HIST_TTL)
    if cached is not None:
        return cached

    params: dict = {"per_page": per_page}
    if search:
        params["search"] = search
    if team_id is not None:
        params["team_ids[]"] = team_id

    data = _get("/nba/v1/players/active", params)
    result = data.get("data", [])
    _cache_set(cache_key, result)
    return result


def get_player(player_id: int) -> dict:
    """Return a single player by ID.

    GET /nba/v1/players/{id}
    """
    cache_key = f"bdl:player:{player_id}"
    cached = _cache_get(cache_key, HIST_TTL)
    if cached is not None:
        return cached

    data = _get(f"/nba/v1/players/{player_id}")
    result = data.get("data", {})
    if result:
        _cache_set(cache_key, result)
    return result


# ── Legacy alias ──────────────────────────────────────────────────────────────
def search_players(name: str) -> list:
    """Search for players by name (legacy alias for get_players)."""
    return get_players(search=name)


# ═══════════════════════════════════════════════════════════════════════════════
# Games
# ═══════════════════════════════════════════════════════════════════════════════

def get_games(
    date: str | None = None,
    season: int | None = None,
    team_id: int | None = None,
    per_page: int = 25,
) -> list:
    """Return games filtered by date, season, and/or team.

    GET /nba/v1/games
    """
    cache_key = f"bdl:games:{date}:{season}:{team_id}:{per_page}"
    cached = _cache_get(cache_key, HIST_TTL)
    if cached is not None:
        return cached

    params: dict = {"per_page": per_page}
    if date:
        params["dates[]"] = date
    if season is not None:
        params["seasons[]"] = season
    if team_id is not None:
        params["team_ids[]"] = team_id

    data = _get("/nba/v1/games", params)
    result = data.get("data", [])
    _cache_set(cache_key, result)
    return result


def get_game(game_id: int) -> dict:
    """Return a single game by ID.

    GET /nba/v1/games/{id}
    """
    cache_key = f"bdl:game:{game_id}"
    cached = _cache_get(cache_key, HIST_TTL)
    if cached is not None:
        return cached

    data = _get(f"/nba/v1/games/{game_id}")
    result = data.get("data", {})
    if result:
        _cache_set(cache_key, result)
    return result


# ═══════════════════════════════════════════════════════════════════════════════
# Stats
# ═══════════════════════════════════════════════════════════════════════════════

def get_stats(
    player_id: int | None = None,
    game_id: int | None = None,
    season: int | None = None,
    per_page: int = 25,
) -> list:
    """Return per-game box-score stats.

    GET /nba/v1/stats
    """
    cache_key = f"bdl:stats:{player_id}:{game_id}:{season}:{per_page}"
    cached = _cache_get(cache_key, HIST_TTL)
    if cached is not None:
        return cached

    params: dict = {"per_page": per_page}
    if player_id is not None:
        params["player_ids[]"] = player_id
    if game_id is not None:
        params["game_ids[]"] = game_id
    if season is not None:
        params["seasons[]"] = season

    data = _get("/nba/v1/stats", params)
    result = data.get("data", [])
    _cache_set(cache_key, result)
    return result


def get_advanced_stats(
    player_id: int | None = None,
    game_id: int | None = None,
    season: int | None = None,
    per_page: int = 25,
) -> list:
    """Return advanced box-score stats.

    GET /nba/v1/stats/advanced
    """
    cache_key = f"bdl:adv_stats:{player_id}:{game_id}:{season}:{per_page}"
    cached = _cache_get(cache_key, HIST_TTL)
    if cached is not None:
        return cached

    params: dict = {"per_page": per_page}
    if player_id is not None:
        params["player_ids[]"] = player_id
    if game_id is not None:
        params["game_ids[]"] = game_id
    if season is not None:
        params["seasons[]"] = season

    data = _get("/nba/v1/stats/advanced", params)
    result = data.get("data", [])
    _cache_set(cache_key, result)
    return result


def get_season_averages(player_id: int, season: int) -> list:
    """Return season averages for a player.

    GET /nba/v1/season_averages
    """
    cache_key = f"bdl:season_avg:{player_id}:{season}"
    cached = _cache_get(cache_key, HIST_TTL)
    if cached is not None:
        return cached

    data = _get(
        "/nba/v1/season_averages",
        {"player_ids[]": player_id, "season": season},
    )
    result = data.get("data", [])
    _cache_set(cache_key, result)
    return result


# ── Legacy alias ──────────────────────────────────────────────────────────────
def get_player_stats(player_id: int, season: int) -> list:
    """Fetch season averages for a player (legacy alias for get_season_averages)."""
    return get_season_averages(player_id, season)


# ═══════════════════════════════════════════════════════════════════════════════
# Standings & Leaders
# ═══════════════════════════════════════════════════════════════════════════════

def get_standings(season: int | None = None) -> list:
    """Return league standings for a season.

    GET /nba/v1/standings
    """
    cache_key = f"bdl:standings:{season}"
    cached = _cache_get(cache_key, HIST_TTL)
    if cached is not None:
        return cached

    params: dict = {}
    if season is not None:
        params["season"] = season

    data = _get("/nba/v1/standings", params)
    result = data.get("data", [])
    _cache_set(cache_key, result)
    return result


def get_leaders(
    stat_type: str,
    season: int | None = None,
    season_type: str | None = None,
) -> list:
    """Return statistical leaders.

    GET /nba/v1/leaders
    """
    cache_key = f"bdl:leaders:{stat_type}:{season}:{season_type}"
    cached = _cache_get(cache_key, HIST_TTL)
    if cached is not None:
        return cached

    params: dict = {"stat_type": stat_type}
    if season is not None:
        params["season"] = season
    if season_type:
        params["season_type"] = season_type

    data = _get("/nba/v1/leaders", params)
    result = data.get("data", [])
    _cache_set(cache_key, result)
    return result


# ═══════════════════════════════════════════════════════════════════════════════
# Injuries
# ═══════════════════════════════════════════════════════════════════════════════

def get_injuries(team_id: int | None = None, per_page: int = 25) -> list:
    """Return current player injury report.

    GET /nba/v1/player_injuries
    """
    cache_key = f"bdl:injuries:{team_id}:{per_page}"
    cached = _cache_get(cache_key, LIVE_TTL)
    if cached is not None:
        return cached

    params: dict = {"per_page": per_page}
    if team_id is not None:
        params["team_ids[]"] = team_id

    data = _get("/nba/v1/player_injuries", params)
    result = data.get("data", [])
    _cache_set(cache_key, result)
    return result


# ═══════════════════════════════════════════════════════════════════════════════
# Box Scores
# ═══════════════════════════════════════════════════════════════════════════════

def get_box_scores_live() -> list:
    """Return live in-progress box scores.

    GET /nba/v1/box_scores/live
    """
    cache_key = "bdl:box_scores_live"
    cached = _cache_get(cache_key, LIVE_TTL)
    if cached is not None:
        return cached

    data = _get("/nba/v1/box_scores/live")
    result = data.get("data", [])
    _cache_set(cache_key, result)
    return result


def get_box_scores(date: str) -> list:
    """Return box scores for a given date.

    GET /nba/v1/box_scores
    """
    cache_key = f"bdl:box_scores:{date}"
    cached = _cache_get(cache_key, HIST_TTL)
    if cached is not None:
        return cached

    data = _get("/nba/v1/box_scores", {"date": date})
    result = data.get("data", [])
    _cache_set(cache_key, result)
    return result


# ═══════════════════════════════════════════════════════════════════════════════
# Betting / Odds  (v2 endpoints)
# ═══════════════════════════════════════════════════════════════════════════════

def get_odds(
    date: str | None = None,
    game_id: int | None = None,
    per_page: int = 25,
) -> list:
    """Return betting odds.

    GET /nba/v1/odds
    """
    cache_key = f"bdl:odds:{date}:{game_id}:{per_page}"
    cached = _cache_get(cache_key, LIVE_TTL)
    if cached is not None:
        return cached

    params: dict = {"per_page": per_page}
    if date:
        params["date"] = date
    if game_id is not None:
        params["game_ids[]"] = game_id

    data = _get("/nba/v1/odds", params)
    result = data.get("data", [])
    _cache_set(cache_key, result)
    return result


def get_player_props(
    game_id: int,
    player_id: int | None = None,
    prop_type: str | None = None,
) -> list:
    """Return player prop lines for a game.

    GET /nba/v1/player_props
    """
    cache_key = f"bdl:player_props:{game_id}:{player_id}:{prop_type}"
    cached = _cache_get(cache_key, LIVE_TTL)
    if cached is not None:
        return cached

    params: dict = {"game_ids[]": game_id}
    if player_id is not None:
        params["player_ids[]"] = player_id
    if prop_type:
        params["prop_type"] = prop_type

    data = _get("/nba/v1/player_props", params)
    result = data.get("data", [])
    _cache_set(cache_key, result)
    return result


# ═══════════════════════════════════════════════════════════════════════════════
# Lineups & Play-by-Play
# ═══════════════════════════════════════════════════════════════════════════════

def get_lineups(game_id: int) -> list:
    """Return lineup data for a game.

    GET /nba/v1/lineups
    """
    cache_key = f"bdl:lineups:{game_id}"
    cached = _cache_get(cache_key, HIST_TTL)
    if cached is not None:
        return cached

    data = _get("/nba/v1/lineups", {"game_ids[]": game_id})
    result = data.get("data", [])
    _cache_set(cache_key, result)
    return result


def get_plays(game_id: int | None = None, per_page: int = 100) -> list:
    """Return play-by-play data.

    GET /nba/v1/plays
    """
    cache_key = f"bdl:plays:{game_id}:{per_page}"
    cached = _cache_get(cache_key, HIST_TTL)
    if cached is not None:
        return cached

    params: dict = {"per_page": per_page}
    if game_id is not None:
        params["game_ids[]"] = game_id

    data = _get("/nba/v1/plays", params)
    result = data.get("data", [])
    _cache_set(cache_key, result)
    return result


# ═══════════════════════════════════════════════════════════════════════════════
# Health / Diagnostics
# ═══════════════════════════════════════════════════════════════════════════════

def check_api_health() -> dict:
    """Run a lightweight health check against the BallDontLie API.

    Returns a dict with diagnostic info::

        {"ok": True/False,
         "has_api_key": True/False,
         "requests_available": True/False,
         "status_code": 200 | None,
         "error": "..." | None}
    """
    result: dict = {
        "ok": False,
        "has_api_key": has_api_key(),
        "requests_available": _REQUESTS_AVAILABLE,
        "status_code": None,
        "error": None,
    }

    if not _REQUESTS_AVAILABLE:
        result["error"] = "requests library not installed"
        return result

    if not result["has_api_key"]:
        result["error"] = ("BALLDONTLIE_API_KEY not configured — "
                           "set it in environment variables or Streamlit secrets")
        return result

    try:
        url = f"{_BASE_URL}/nba/v1/teams"
        headers = {"Authorization": _load_api_key()}
        resp = requests.get(url, headers=headers, timeout=10,
                            params={"per_page": 1})
        result["status_code"] = resp.status_code
        if resp.status_code == 200:
            result["ok"] = True
        elif resp.status_code == 401:
            result["error"] = "API key is invalid or expired (401)"
        elif resp.status_code == 403:
            result["error"] = "API key lacks permission for this endpoint (403)"
        elif resp.status_code == 429:
            result["error"] = "Rate limit exceeded (429)"
        else:
            result["error"] = f"Unexpected status code: {resp.status_code}"
    except _ConnError:
        result["error"] = "Cannot connect to api.balldontlie.io — check network"
    except _Timeout:
        result["error"] = "Request timed out"
    except Exception as exc:
        result["error"] = str(exc)

    return result
