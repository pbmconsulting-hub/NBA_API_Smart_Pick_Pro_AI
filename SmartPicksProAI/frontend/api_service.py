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
        get_player_awards,
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


@st.cache_data(ttl=3600)
def get_standings() -> list[dict]:
    """Fetch current NBA standings from the backend.

    Calls ``GET /api/standings`` and returns the ``standings`` list.
    Cached for 1 hour.

    Returns:
        A list of standings dicts, or an empty list on error.
    """
    try:
        resp = requests.get(f"{BASE_URL}/api/standings", timeout=15)
        resp.raise_for_status()
        return resp.json().get("standings", [])
    except Exception as exc:
        logger.error("Failed to fetch standings: %s", exc)
        return []


@st.cache_data(ttl=3600)
def get_league_leaders() -> list[dict]:
    """Fetch league leaders from the backend.

    Calls ``GET /api/league-leaders`` and returns the ``leaders`` list.
    Cached for 1 hour.

    Returns:
        A list of leader dicts, or an empty list on error.
    """
    try:
        resp = requests.get(f"{BASE_URL}/api/league-leaders", timeout=15)
        resp.raise_for_status()
        return resp.json().get("leaders", [])
    except Exception as exc:
        logger.error("Failed to fetch league leaders: %s", exc)
        return []


@st.cache_data(ttl=3600)
def get_player_bio(player_id: int) -> dict:
    """Fetch a player's biographical info from the backend.

    Calls ``GET /api/players/{player_id}/bio`` and returns the ``bio`` dict.
    Cached for 1 hour.

    Args:
        player_id: The NBA player ID.

    Returns:
        A bio dict, or an empty dict on error.
    """
    try:
        resp = requests.get(f"{BASE_URL}/api/players/{player_id}/bio", timeout=15)
        resp.raise_for_status()
        return resp.json().get("bio", {})
    except Exception as exc:
        logger.error("Failed to fetch bio for player %d: %s", player_id, exc)
        return {}


@st.cache_data(ttl=3600)
def get_player_career(player_id: int) -> list[dict]:
    """Fetch a player's career stats from the backend.

    Calls ``GET /api/players/{player_id}/career`` and returns the ``career``
    list.  Cached for 1 hour.

    Args:
        player_id: The NBA player ID.

    Returns:
        A list of career stat dicts, or an empty list on error.
    """
    try:
        resp = requests.get(
            f"{BASE_URL}/api/players/{player_id}/career", timeout=15
        )
        resp.raise_for_status()
        return resp.json().get("career", [])
    except Exception as exc:
        logger.error("Failed to fetch career for player %d: %s", player_id, exc)
        return []


@st.cache_data(ttl=3600)
def get_player_awards(player_id: int) -> list[dict]:
    """Fetch a player's awards from the backend.

    Calls ``GET /api/players/{player_id}/awards`` and returns the ``awards``
    list.  Cached for 1 hour.

    Args:
        player_id: The NBA player ID.

    Returns:
        A list of award dicts, or an empty list on error.
    """
    try:
        resp = requests.get(
            f"{BASE_URL}/api/players/{player_id}/awards", timeout=15
        )
        resp.raise_for_status()
        return resp.json().get("awards", [])
    except Exception as exc:
        logger.error("Failed to fetch awards for player %d: %s", player_id, exc)
        return []


@st.cache_data(ttl=3600)
def get_player_advanced(player_id: int) -> list[dict]:
    """Fetch a player's advanced stats from the backend.

    Calls ``GET /api/players/{player_id}/advanced`` and returns the
    ``advanced`` list.  Cached for 1 hour.

    Args:
        player_id: The NBA player ID.

    Returns:
        A list of advanced stat dicts, or an empty list on error.
    """
    try:
        resp = requests.get(
            f"{BASE_URL}/api/players/{player_id}/advanced", timeout=15
        )
        resp.raise_for_status()
        return resp.json().get("advanced", [])
    except Exception as exc:
        logger.error("Failed to fetch advanced stats for player %d: %s", player_id, exc)
        return []


@st.cache_data(ttl=3600)
def get_player_shot_chart(player_id: int) -> list[dict]:
    """Fetch a player's shot chart data from the backend.

    Calls ``GET /api/players/{player_id}/shot-chart`` and returns the
    ``shots`` list.  Cached for 1 hour.

    Args:
        player_id: The NBA player ID.

    Returns:
        A list of shot dicts, or an empty list on error.
    """
    try:
        resp = requests.get(
            f"{BASE_URL}/api/players/{player_id}/shot-chart", timeout=15
        )
        resp.raise_for_status()
        return resp.json().get("shots", [])
    except Exception as exc:
        logger.error("Failed to fetch shot chart for player %d: %s", player_id, exc)
        return []


@st.cache_data(ttl=3600)
def get_player_tracking(player_id: int) -> list[dict]:
    """Fetch a player's tracking stats from the backend.

    Calls ``GET /api/players/{player_id}/tracking`` and returns the
    ``tracking`` list.  Cached for 1 hour.

    Args:
        player_id: The NBA player ID.

    Returns:
        A list of tracking stat dicts, or an empty list on error.
    """
    try:
        resp = requests.get(
            f"{BASE_URL}/api/players/{player_id}/tracking", timeout=15
        )
        resp.raise_for_status()
        return resp.json().get("tracking", [])
    except Exception as exc:
        logger.error("Failed to fetch tracking stats for player %d: %s", player_id, exc)
        return []


@st.cache_data(ttl=3600)
def get_player_clutch(player_id: int) -> list[dict]:
    """Fetch a player's clutch stats from the backend.

    Calls ``GET /api/players/{player_id}/clutch`` and returns the ``clutch``
    list.  Cached for 1 hour.

    Args:
        player_id: The NBA player ID.

    Returns:
        A list of clutch stat dicts, or an empty list on error.
    """
    try:
        resp = requests.get(
            f"{BASE_URL}/api/players/{player_id}/clutch", timeout=15
        )
        resp.raise_for_status()
        return resp.json().get("clutch", [])
    except Exception as exc:
        logger.error("Failed to fetch clutch stats for player %d: %s", player_id, exc)
        return []


@st.cache_data(ttl=3600)
def get_player_hustle(player_id: int) -> list[dict]:
    """Fetch a player's hustle stats from the backend.

    Calls ``GET /api/players/{player_id}/hustle`` and returns the ``hustle``
    list.  Cached for 1 hour.

    Args:
        player_id: The NBA player ID.

    Returns:
        A list of hustle stat dicts, or an empty list on error.
    """
    try:
        resp = requests.get(
            f"{BASE_URL}/api/players/{player_id}/hustle", timeout=15
        )
        resp.raise_for_status()
        return resp.json().get("hustle", [])
    except Exception as exc:
        logger.error("Failed to fetch hustle stats for player %d: %s", player_id, exc)
        return []


@st.cache_data(ttl=3600)
def get_player_scoring(player_id: int) -> list[dict]:
    """Fetch a player's scoring stats from the backend.

    Calls ``GET /api/players/{player_id}/scoring`` and returns the
    ``scoring`` list.  Cached for 1 hour.

    Args:
        player_id: The NBA player ID.

    Returns:
        A list of scoring stat dicts, or an empty list on error.
    """
    try:
        resp = requests.get(
            f"{BASE_URL}/api/players/{player_id}/scoring", timeout=15
        )
        resp.raise_for_status()
        return resp.json().get("scoring", [])
    except Exception as exc:
        logger.error("Failed to fetch scoring stats for player %d: %s", player_id, exc)
        return []


@st.cache_data(ttl=3600)
def get_player_usage(player_id: int) -> list[dict]:
    """Fetch a player's usage stats from the backend.

    Calls ``GET /api/players/{player_id}/usage`` and returns the ``usage``
    list.  Cached for 1 hour.

    Args:
        player_id: The NBA player ID.

    Returns:
        A list of usage stat dicts, or an empty list on error.
    """
    try:
        resp = requests.get(
            f"{BASE_URL}/api/players/{player_id}/usage", timeout=15
        )
        resp.raise_for_status()
        return resp.json().get("usage", [])
    except Exception as exc:
        logger.error("Failed to fetch usage stats for player %d: %s", player_id, exc)
        return []


@st.cache_data(ttl=3600)
def get_player_matchups(player_id: int) -> list[dict]:
    """Fetch a player's matchup data from the backend.

    Calls ``GET /api/players/{player_id}/matchups`` and returns the
    ``matchups`` list.  Cached for 1 hour.

    Args:
        player_id: The NBA player ID.

    Returns:
        A list of matchup dicts, or an empty list on error.
    """
    try:
        resp = requests.get(
            f"{BASE_URL}/api/players/{player_id}/matchups", timeout=15
        )
        resp.raise_for_status()
        return resp.json().get("matchups", [])
    except Exception as exc:
        logger.error("Failed to fetch matchups for player %d: %s", player_id, exc)
        return []


@st.cache_data(ttl=3600)
def get_team_details(team_id: int) -> dict:
    """Fetch detailed info for a team from the backend.

    Calls ``GET /api/teams/{team_id}/details`` and returns the ``details``
    dict.  Cached for 1 hour.

    Args:
        team_id: The NBA team ID.

    Returns:
        A details dict, or an empty dict on error.
    """
    try:
        resp = requests.get(
            f"{BASE_URL}/api/teams/{team_id}/details", timeout=15
        )
        resp.raise_for_status()
        return resp.json().get("details", {})
    except Exception as exc:
        logger.error("Failed to fetch details for team %d: %s", team_id, exc)
        return {}


@st.cache_data(ttl=3600)
def get_team_clutch(team_id: int) -> list[dict]:
    """Fetch a team's clutch stats from the backend.

    Calls ``GET /api/teams/{team_id}/clutch`` and returns the ``clutch``
    list.  Cached for 1 hour.

    Args:
        team_id: The NBA team ID.

    Returns:
        A list of clutch stat dicts, or an empty list on error.
    """
    try:
        resp = requests.get(
            f"{BASE_URL}/api/teams/{team_id}/clutch", timeout=15
        )
        resp.raise_for_status()
        return resp.json().get("clutch", [])
    except Exception as exc:
        logger.error("Failed to fetch clutch stats for team %d: %s", team_id, exc)
        return []


@st.cache_data(ttl=3600)
def get_team_hustle(team_id: int) -> list[dict]:
    """Fetch a team's hustle stats from the backend.

    Calls ``GET /api/teams/{team_id}/hustle`` and returns the ``hustle``
    list.  Cached for 1 hour.

    Args:
        team_id: The NBA team ID.

    Returns:
        A list of hustle stat dicts, or an empty list on error.
    """
    try:
        resp = requests.get(
            f"{BASE_URL}/api/teams/{team_id}/hustle", timeout=15
        )
        resp.raise_for_status()
        return resp.json().get("hustle", [])
    except Exception as exc:
        logger.error("Failed to fetch hustle stats for team %d: %s", team_id, exc)
        return []


@st.cache_data(ttl=3600)
def get_team_estimated_metrics(team_id: int) -> list[dict]:
    """Fetch a team's estimated metrics from the backend.

    Calls ``GET /api/teams/{team_id}/estimated-metrics`` and returns the
    ``metrics`` list.  Cached for 1 hour.

    Args:
        team_id: The NBA team ID.

    Returns:
        A list of metric dicts, or an empty list on error.
    """
    try:
        resp = requests.get(
            f"{BASE_URL}/api/teams/{team_id}/estimated-metrics", timeout=15
        )
        resp.raise_for_status()
        return resp.json().get("metrics", [])
    except Exception as exc:
        logger.error("Failed to fetch estimated metrics for team %d: %s", team_id, exc)
        return []


@st.cache_data(ttl=3600)
def get_team_synergy(team_id: int) -> list[dict]:
    """Fetch a team's synergy data from the backend.

    Calls ``GET /api/teams/{team_id}/synergy`` and returns the ``synergy``
    list.  Cached for 1 hour.

    Args:
        team_id: The NBA team ID.

    Returns:
        A list of synergy dicts, or an empty list on error.
    """
    try:
        resp = requests.get(
            f"{BASE_URL}/api/teams/{team_id}/synergy", timeout=15
        )
        resp.raise_for_status()
        return resp.json().get("synergy", [])
    except Exception as exc:
        logger.error("Failed to fetch synergy data for team %d: %s", team_id, exc)
        return []


@st.cache_data(ttl=3600)
def get_play_by_play(game_id: str) -> list[dict]:
    """Fetch play-by-play data for a game from the backend.

    Calls ``GET /api/games/{game_id}/play-by-play`` and returns the
    ``plays`` list.  Cached for 1 hour.

    Args:
        game_id: The NBA game ID.

    Returns:
        A list of play dicts, or an empty list on error.
    """
    try:
        resp = requests.get(
            f"{BASE_URL}/api/games/{game_id}/play-by-play", timeout=15
        )
        resp.raise_for_status()
        return resp.json().get("plays", [])
    except Exception as exc:
        logger.error("Failed to fetch play-by-play for game %s: %s", game_id, exc)
        return []


@st.cache_data(ttl=3600)
def get_win_probability(game_id: str) -> list[dict]:
    """Fetch win probability data for a game from the backend.

    Calls ``GET /api/games/{game_id}/win-probability`` and returns the
    ``probabilities`` list.  Cached for 1 hour.

    Args:
        game_id: The NBA game ID.

    Returns:
        A list of probability dicts, or an empty list on error.
    """
    try:
        resp = requests.get(
            f"{BASE_URL}/api/games/{game_id}/win-probability", timeout=15
        )
        resp.raise_for_status()
        return resp.json().get("probabilities", [])
    except Exception as exc:
        logger.error("Failed to fetch win probability for game %s: %s", game_id, exc)
        return []


@st.cache_data(ttl=3600)
def get_game_rotation(game_id: str) -> list[dict]:
    """Fetch rotation data for a game from the backend.

    Calls ``GET /api/games/{game_id}/rotation`` and returns the
    ``rotations`` list.  Cached for 1 hour.

    Args:
        game_id: The NBA game ID.

    Returns:
        A list of rotation dicts, or an empty list on error.
    """
    try:
        resp = requests.get(
            f"{BASE_URL}/api/games/{game_id}/rotation", timeout=15
        )
        resp.raise_for_status()
        return resp.json().get("rotations", [])
    except Exception as exc:
        logger.error("Failed to fetch rotation for game %s: %s", game_id, exc)
        return []


@st.cache_data(ttl=3600)
def get_game_box_score(game_id: str) -> list[dict]:
    """Fetch box score data for a game from the backend.

    Calls ``GET /api/games/{game_id}/box-score`` and returns the
    ``players`` list.  Cached for 1 hour.

    Args:
        game_id: The NBA game ID.

    Returns:
        A list of player box-score dicts, or an empty list on error.
    """
    try:
        resp = requests.get(
            f"{BASE_URL}/api/games/{game_id}/box-score", timeout=15
        )
        resp.raise_for_status()
        return resp.json().get("players", [])
    except Exception as exc:
        logger.error("Failed to fetch box score for game %s: %s", game_id, exc)
        return []


@st.cache_data(ttl=3600)
def get_draft_history() -> list[dict]:
    """Fetch NBA draft history from the backend.

    Calls ``GET /api/draft-history`` and returns the ``drafts`` list.
    Cached for 1 hour.

    Returns:
        A list of draft dicts, or an empty list on error.
    """
    try:
        resp = requests.get(f"{BASE_URL}/api/draft-history", timeout=15)
        resp.raise_for_status()
        return resp.json().get("drafts", [])
    except Exception as exc:
        logger.error("Failed to fetch draft history: %s", exc)
        return []


@st.cache_data(ttl=3600)
def get_lineups() -> list[dict]:
    """Fetch lineup data from the backend.

    Calls ``GET /api/lineups`` and returns the ``lineups`` list.
    Cached for 1 hour.

    Returns:
        A list of lineup dicts, or an empty list on error.
    """
    try:
        resp = requests.get(f"{BASE_URL}/api/lineups", timeout=15)
        resp.raise_for_status()
        return resp.json().get("lineups", [])
    except Exception as exc:
        logger.error("Failed to fetch lineups: %s", exc)
        return []


@st.cache_data(ttl=3600)
def get_league_dash_players() -> list[dict]:
    """Fetch league dashboard player stats from the backend.

    Calls ``GET /api/league-dash/players`` and returns the ``players`` list.
    Cached for 1 hour.

    Returns:
        A list of player stat dicts, or an empty list on error.
    """
    try:
        resp = requests.get(f"{BASE_URL}/api/league-dash/players", timeout=15)
        resp.raise_for_status()
        return resp.json().get("players", [])
    except Exception as exc:
        logger.error("Failed to fetch league dash player stats: %s", exc)
        return []


@st.cache_data(ttl=3600)
def get_league_dash_teams() -> list[dict]:
    """Fetch league dashboard team stats from the backend.

    Calls ``GET /api/league-dash/teams`` and returns the ``teams`` list.
    Cached for 1 hour.

    Returns:
        A list of team stat dicts, or an empty list on error.
    """
    try:
        resp = requests.get(f"{BASE_URL}/api/league-dash/teams", timeout=15)
        resp.raise_for_status()
        return resp.json().get("teams", [])
    except Exception as exc:
        logger.error("Failed to fetch league dash team stats: %s", exc)
        return []


@st.cache_data(ttl=3600)
def get_recent_games() -> list[dict]:
    """Fetch recent NBA games from the backend.

    Calls ``GET /api/games/recent`` and returns the ``games`` list.
    Cached for 1 hour.

    Returns:
        A list of recent game dicts, or an empty list on error.
    """
    try:
        resp = requests.get(f"{BASE_URL}/api/games/recent", timeout=15)
        resp.raise_for_status()
        return resp.json().get("games", [])
    except Exception as exc:
        logger.error("Failed to fetch recent games: %s", exc)
        return []


@st.cache_data(ttl=3600)
def get_schedule() -> list[dict]:
    """Fetch the NBA schedule from the backend.

    Calls ``GET /api/schedule`` and returns the ``schedule`` list.
    Cached for 1 hour.

    Returns:
        A list of schedule dicts, or an empty list on error.
    """
    try:
        resp = requests.get(f"{BASE_URL}/api/schedule", timeout=15)
        resp.raise_for_status()
        return resp.json().get("schedule", [])
    except Exception as exc:
        logger.error("Failed to fetch schedule: %s", exc)
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
