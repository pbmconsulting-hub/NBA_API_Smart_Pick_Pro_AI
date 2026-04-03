"""
api.py
------
FastAPI backend for SmartPicksProAI.

Serves player and game data from the local SQLite database and exposes an
admin endpoint to trigger incremental data refreshes on-demand.

Start the server::

    python api.py
    # or
    uvicorn api:app --reload

Endpoints
---------
GET  /api/health                       – Health check.
GET  /api/players/{player_id}/last5    – Last 5 game logs with computed averages.
GET  /api/games/today                  – Today's NBA matchups (database only).
POST /api/admin/refresh-data           – Trigger an incremental data update.
"""

import logging
import sqlite3
import sys
from contextlib import contextmanager
from datetime import date
from pathlib import Path
from typing import Generator

import uvicorn
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

import data_updater
import setup_db

# Ensure the SmartPicksProAI package root is importable so that the
# ``engine`` package can be loaded regardless of working directory.
# The ``backend/utils.py`` helper may already be cached in sys.modules
# as ``utils`` (loaded by ``data_updater``).  We stash that reference
# and re-register it under ``backend.utils`` so the ``utils/`` *package*
# at the SmartPicksProAI root can be imported normally by engine code.
_PACKAGE_ROOT = str(Path(__file__).resolve().parent.parent)
if _PACKAGE_ROOT not in sys.path:
    sys.path.insert(0, _PACKAGE_ROOT)
_backend_utils = sys.modules.pop("utils", None)
if _backend_utils is not None and not hasattr(_backend_utils, "__path__"):
    # Re-register the single-file backend helper under a distinct name
    # so existing references (data_updater.utils) keep working.
    sys.modules["backend_utils"] = _backend_utils

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

DB_PATH = setup_db.DB_PATH

# ---------------------------------------------------------------------------
# Endpoint defaults — centralised to avoid scattered magic numbers
# ---------------------------------------------------------------------------

MAX_SEARCH_RESULTS = 25
LAST_N_GAMES_DEFAULT = 5
MAX_SEASON_GAMES = 82
TEAM_STATS_DEFAULT_LIMIT = 10

app = FastAPI(
    title="SmartPicksProAI API",
    description="NBA player stats and game data for ML-powered prop predictions.",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)


# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------


@contextmanager
def _db() -> Generator[sqlite3.Connection, None, None]:
    """Context manager that opens a read-only SQLite connection and closes it.

    Usage::

        with _db() as conn:
            rows = conn.execute("SELECT ...").fetchall()

    Yields:
        An open :class:`sqlite3.Connection` with ``row_factory`` configured.
    """
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
    finally:
        conn.close()


def _query_rows(sql: str, params: tuple = (), *, label: str = "query") -> list[dict]:
    """Execute *sql* and return all rows as dicts.

    A thin helper that wraps the common
    ``open → execute → fetchall → dictify → close`` pattern used by
    almost every GET endpoint, centralising error handling and logging.

    Args:
        sql:    SQL query string (use ``?`` placeholders).
        params: Tuple of bind-parameter values.
        label:  Human-readable label for log/error messages.

    Returns:
        A list of dicts (one per row).

    Raises:
        HTTPException 500: On any database error.
    """
    try:
        with _db() as conn:
            rows = conn.execute(sql, params).fetchall()
        return [dict(r) for r in rows]
    except sqlite3.Error as exc:
        logger.exception("Error in %s.", label)
        raise HTTPException(status_code=500, detail=str(exc)) from exc


def _query_one(sql: str, params: tuple = (), *, label: str = "query") -> dict | None:
    """Execute *sql* and return the first row as a dict, or ``None``.

    Args:
        sql:    SQL query string.
        params: Tuple of bind-parameter values.
        label:  Human-readable label for log/error messages.

    Returns:
        A single dict, or ``None`` if no rows matched.

    Raises:
        HTTPException 500: On any database error.
    """
    try:
        with _db() as conn:
            row = conn.execute(sql, params).fetchone()
        return dict(row) if row else None
    except sqlite3.Error as exc:
        logger.exception("Error in %s.", label)
        raise HTTPException(status_code=500, detail=str(exc)) from exc


def _get_conn() -> sqlite3.Connection:
    """Open and return a SQLite connection with row_factory set.

    .. deprecated::
        Use the :func:`_db` context manager, :func:`_query_rows`, or
        :func:`_query_one` instead.  This function is retained only for
        the ``data_updater`` write-path (which manages its own connection
        lifecycle) and will be removed in a future release.

    Returns:
        An open :class:`sqlite3.Connection` with ``row_factory`` configured
        so that rows are returned as :class:`sqlite3.Row` objects (accessible
        by column name).
    """
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


# ---------------------------------------------------------------------------
# Health check
# ---------------------------------------------------------------------------


@app.get("/api/health")
def health_check() -> dict:
    """Return a simple health-check response.

    Returns:
        JSON ``{"status": "ok"}`` if the API and database are reachable.
    """
    try:
        with _db() as conn:
            conn.execute("SELECT 1").fetchone()
        return {"status": "ok"}
    except sqlite3.Error as exc:
        logger.exception("Health check failed.")
        raise HTTPException(status_code=500, detail=str(exc)) from exc


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

# Stat columns used for computing per-player averages.
_PLAYER_STAT_KEYS: list[str] = [
    "pts", "reb", "ast", "blk", "stl", "tov",
    "fgm", "fga", "fg_pct",
    "fg3m", "fg3a", "fg3_pct",
    "ftm", "fta", "ft_pct",
    "oreb", "dreb", "pf", "plus_minus",
]


def _compute_stat_averages(
    games: list[dict], stat_keys: list[str] = _PLAYER_STAT_KEYS,
) -> dict[str, float]:
    """Return the mean of each *stat_key* across *games*.

    Missing/None values are treated as 0.  Returns all-zeros when the
    input list is empty.

    Args:
        games: List of game-log dicts (each containing the stat keys).
        stat_keys: Stat column names to average.

    Returns:
        Dict mapping each stat key to its rounded average.
    """
    if not games:
        return {k: 0.0 for k in stat_keys}
    return {
        k: round(sum(g.get(k) or 0 for g in games) / len(games), 1)
        for k in stat_keys
    }


@app.get("/api/players/{player_id}/last5")
def get_player_last5(player_id: int) -> dict:
    """Return a player's last 5 game logs with computed 5-game averages.

    The response is structured for easy parsing by an AI model calculating
    moving averages and player trends::

        {
          "player_id": 2544,
          "first_name": "LeBron",
          "last_name": "James",
          "games": [
            {
              "game_date": "2026-03-20",
              "game_id": "0022501050",
              "pts": 28, "reb": 8, "ast": 9,
              "blk": 1, "stl": 2, "tov": 3, "min": "35:42"
            },
            ...
          ],
          "averages": {
            "pts": 27.4, "reb": 7.2, "ast": 8.6,
            "blk": 0.8, "stl": 1.4, "tov": 2.8
          }
        }

    Args:
        player_id: The NBA player ID.

    Returns:
        JSON response with player info, last 5 game logs, and stat averages.

    Raises:
        HTTPException 404: If the player is not found in the database.
        HTTPException 500: On unexpected database errors.
    """
    logger.info("GET /api/players/%d/last5", player_id)

    player_row = _query_one(
        "SELECT player_id, first_name, last_name FROM Players WHERE player_id = ?",
        (player_id,),
        label="get_player_last5/player",
    )
    if player_row is None:
        raise HTTPException(status_code=404, detail=f"Player {player_id} not found.")

    games = _query_rows(
        """
        SELECT
            g.game_date,
            g.season,
            g.home_abbrev,
            g.away_abbrev,
            g.matchup,
            g.home_score,
            g.away_score,
            l.game_id,
            l.wl,
            l.min,
            l.pts, l.reb, l.ast, l.blk, l.stl, l.tov,
            l.fgm, l.fga, l.fg_pct,
            l.fg3m, l.fg3a, l.fg3_pct,
            l.ftm, l.fta, l.ft_pct,
            l.oreb, l.dreb, l.pf, l.plus_minus
        FROM Player_Game_Logs l
        JOIN Games g ON g.game_id = l.game_id
        WHERE l.player_id = ?
        ORDER BY g.game_date DESC
        LIMIT 5
        """,
        (player_id,),
        label="get_player_last5/logs",
    )

    return {
        "player_id": player_row["player_id"],
        "first_name": player_row["first_name"],
        "last_name": player_row["last_name"],
        "games": games,
        "averages": _compute_stat_averages(games),
    }


@app.get("/api/games/today")
def get_games_today() -> dict:
    """Return today's NBA matchups from the database.

    Queries the Games table for today's date.  Today's schedule is populated
    by :func:`data_updater.sync_todays_games` (called during each data
    refresh) so this endpoint never needs to reach the live NBA API.

    Returns:
        JSON with a list of today's games::

            {
              "date": "2026-03-30",
              "source": "database",
              "games": [
                {"game_id": "...", "matchup": "LAL vs. BOS"},
                ...
              ]
            }

    Raises:
        HTTPException 500: On unexpected errors.
    """
    today = date.today().isoformat()
    logger.info("GET /api/games/today — checking for date %s", today)

    rows = _query_rows(
        "SELECT game_id, game_date, season, home_team_id, away_team_id, "
        "home_abbrev, away_abbrev, matchup, home_score, away_score "
        "FROM Games WHERE game_date = ?",
        (today,),
        label="get_games_today",
    )

    logger.info("Found %d games in DB for %s.", len(rows), today)
    return {
        "date": today,
        "source": "database",
        "games": rows,
    }


@app.post("/api/admin/refresh-data")
def refresh_data() -> dict:
    """Trigger an incremental data refresh from the NBA API.

    Calls :func:`data_updater.run_update` which fetches all game logs
    between the last stored date and yesterday, then appends any new rows to
    the database.

    Returns:
        JSON with a status message and the count of new records added::

            {
              "status": "success",
              "new_records": 342,
              "message": "Added 342 new game log records."
            }

    Raises:
        HTTPException 500: If the update fails for any reason.
    """
    logger.info("POST /api/admin/refresh-data — starting update …")
    try:
        new_records = data_updater.run_update(DB_PATH)
        message = (
            f"Added {new_records} new game log records."
            if new_records > 0
            else "Database is already up to date — no new records added."
        )
        logger.info("Refresh complete: %s", message)
        return {"status": "success", "new_records": new_records, "message": message}
    except Exception as exc:  # Broad catch: wraps entire external update pipeline.
        logger.exception("Error during data refresh.")
        raise HTTPException(status_code=500, detail=str(exc)) from exc


# ---------------------------------------------------------------------------
# Player / Team lookup endpoints
# ---------------------------------------------------------------------------


@app.get("/api/players/search")
def search_players(q: str = "") -> dict:
    """Search for players by name.

    Performs a case-insensitive ``LIKE`` search against ``full_name``,
    ``first_name``, and ``last_name`` in the Players table.  Returns up to
    :data:`MAX_SEARCH_RESULTS` matching players with basic info.

    Args:
        q: Search query string (e.g. ``'LeBron'``).

    Returns:
        JSON with a ``results`` list of matching player dicts.

    Raises:
        HTTPException 500: On unexpected database errors.
    """
    logger.info("GET /api/players/search?q=%s", q)
    if not q.strip():
        return {"results": []}

    pattern = f"%{q.strip()}%"
    rows = _query_rows(
        """
        SELECT player_id, first_name, last_name, full_name,
               team_id, team_abbreviation, position
        FROM Players
        WHERE full_name LIKE ?
           OR first_name LIKE ?
           OR last_name LIKE ?
        ORDER BY full_name
        LIMIT ?
        """,
        (*([pattern] * 3), MAX_SEARCH_RESULTS),
        label="search_players",
    )
    return {"results": rows}


@app.get("/api/teams")
def get_teams() -> dict:
    """List all NBA teams stored in the database.

    Returns:
        JSON with a ``teams`` list sorted by abbreviation.

    Raises:
        HTTPException 500: On unexpected database errors.
    """
    logger.info("GET /api/teams")
    result = _query_rows(
        "SELECT team_id, abbreviation, team_name, conference, division, "
        "pace, ortg, drtg "
        "FROM Teams ORDER BY abbreviation",
        label="get_teams",
    )
    return {"teams": result}


@app.get("/api/teams/{team_id}/roster")
def get_team_roster(team_id: int) -> dict:
    """Return the current roster for a specific team.

    Joins ``Team_Roster`` with ``Players`` to return player info for every
    player currently assigned to the team.  Falls back to a direct lookup
    in the ``Players`` table (by ``team_id``) if the ``Team_Roster`` table
    is not yet populated.

    Args:
        team_id: The NBA team ID.

    Returns:
        JSON with ``team_id`` and a ``players`` list.

    Raises:
        HTTPException 500: On unexpected database errors.
    """
    logger.info("GET /api/teams/%d/roster", team_id)

    players = _query_rows(
        """
        SELECT p.player_id, p.first_name, p.last_name, p.full_name,
               p.position, p.team_abbreviation
        FROM Team_Roster r
        JOIN Players p ON p.player_id = r.player_id
        WHERE r.team_id = ?
        ORDER BY p.last_name
        """,
        (team_id,),
        label="get_team_roster",
    )

    # Fallback: if Team_Roster has no rows for this team, use Players.team_id.
    if not players:
        players = _query_rows(
            """
            SELECT player_id, first_name, last_name, full_name,
                   position, team_abbreviation
            FROM Players
            WHERE team_id = ?
            ORDER BY last_name
            """,
            (team_id,),
            label="get_team_roster/fallback",
        )

    return {"team_id": team_id, "players": players}


@app.get("/api/teams/{team_id}/stats")
def get_team_stats(team_id: int, last_n: int = 10) -> dict:
    """Return recent game-level stats for a specific team.

    Queries ``Team_Game_Stats`` for the most recent *last_n* games played
    by the team, ordered by ``game_date DESC``.

    Args:
        team_id: The NBA team ID.
        last_n:  Number of recent games to return (default 10, max 82).

    Returns:
        JSON with ``team_id`` and a ``games`` list of per-game stat dicts::

            {
              "team_id": 1610612747,
              "games": [
                {
                  "game_id": "0022501100",
                  "game_date": "2026-03-28",
                  "opponent_team_id": 1610612738,
                  "is_home": 1,
                  "points_scored": 112,
                  "points_allowed": 105,
                  "pace_est": 99.2,
                  "ortg_est": 113.5,
                  "drtg_est": 106.4
                }
              ]
            }

    Raises:
        HTTPException 500: On unexpected database errors.
    """
    logger.info("GET /api/teams/%d/stats?last_n=%d", team_id, last_n)
    last_n = max(1, min(last_n, MAX_SEASON_GAMES))

    games = _query_rows(
        """
        SELECT tgs.game_id, g.game_date, g.matchup,
               tgs.opponent_team_id, tgs.is_home,
               tgs.points_scored, tgs.points_allowed,
               tgs.pace_est, tgs.ortg_est, tgs.drtg_est
        FROM Team_Game_Stats tgs
        JOIN Games g ON g.game_id = tgs.game_id
        WHERE tgs.team_id = ?
        ORDER BY g.game_date DESC
        LIMIT ?
        """,
        (team_id, last_n),
        label="get_team_stats",
    )
    return {"team_id": team_id, "games": games}


@app.get("/api/defense-vs-position/{team_abbreviation}")
def get_defense_vs_position(team_abbreviation: str) -> dict:
    """Return Defense_Vs_Position multipliers for a specific team.

    Each multiplier indicates how players at a given position perform against
    this team relative to the league average.  A multiplier **> 1.0** means
    the team allows more than average (weaker defense); **< 1.0** means
    tougher defense.

    Positions use a **5-position** model: ``PG``, ``SG``, ``SF``, ``PF``,
    ``C``.

    Args:
        team_abbreviation: Three-letter team code (e.g. ``'BOS'``).

    Returns:
        JSON with ``team_abbreviation`` and a ``positions`` list::

            {
              "team_abbreviation": "BOS",
              "positions": [
                {
                  "pos": "PG",
                  "vs_pts_mult": 0.95,
                  "vs_reb_mult": 1.02,
                  "vs_ast_mult": 0.98,
                  "vs_stl_mult": 1.01,
                  "vs_blk_mult": 0.90,
                  "vs_3pm_mult": 0.93
                }
              ]
            }

    Raises:
        HTTPException 500: On unexpected database errors.
    """
    abbrev = team_abbreviation.upper()
    logger.info("GET /api/defense-vs-position/%s", abbrev)
    result = _query_rows(
        """
        SELECT pos, vs_pts_mult, vs_reb_mult, vs_ast_mult,
               vs_stl_mult, vs_blk_mult, vs_3pm_mult
        FROM Defense_Vs_Position
        WHERE team_abbreviation = ?
        ORDER BY pos
        """,
        (abbrev,),
        label="get_defense_vs_position",
    )
    return {
        "team_abbreviation": abbrev,
        "positions": result,
    }


# ---------------------------------------------------------------------------
# Additional data endpoints
# ---------------------------------------------------------------------------


@app.get("/api/standings")
def get_standings() -> dict:
    """Return all standings rows."""
    logger.info("GET /api/standings")
    result = _query_rows(
        "SELECT s.*, t.abbreviation, t.team_name "
        "FROM Standings s "
        "LEFT JOIN Teams t ON s.team_id = t.team_id "
        "ORDER BY s.conference, s.playoff_rank",
        label="get_standings",
    )
    return {"standings": result}


@app.get("/api/league-leaders")
def get_league_leaders() -> dict:
    """Return league leaders."""
    logger.info("GET /api/league-leaders")
    result = _query_rows(
        "SELECT ll.*, p.full_name, p.position, p.team_abbreviation "
        "FROM League_Leaders ll "
        "LEFT JOIN Players p ON ll.player_id = p.player_id "
        "ORDER BY ll.rank "
        "LIMIT 100",
        label="get_league_leaders",
    )
    return {"leaders": result}


@app.get("/api/players/{player_id}/bio")
def get_player_bio(player_id: int) -> dict:
    """Return player bio information."""
    logger.info("GET /api/players/%d/bio", player_id)
    row = _query_one(
        "SELECT * FROM Player_Bio WHERE player_id = ?",
        (player_id,),
        label="get_player_bio",
    )
    if row is None:
        # Fallback to Common_Player_Info
        row = _query_one(
            "SELECT * FROM Common_Player_Info WHERE person_id = ?",
            (player_id,),
            label="get_player_bio_fallback",
        )
    return {"bio": row or {}}


@app.get("/api/players/{player_id}/career")
def get_player_career(player_id: int) -> dict:
    """Return player career stats."""
    logger.info("GET /api/players/%d/career", player_id)
    result = _query_rows(
        "SELECT * FROM Player_Career_Stats WHERE player_id = ? "
        "ORDER BY season_id DESC",
        (player_id,),
        label="get_player_career",
    )
    return {"career": result}



@app.get("/api/players/{player_id}/advanced")
def get_player_advanced(player_id: int) -> dict:
    """Return advanced box score stats for a player."""
    logger.info("GET /api/players/%d/advanced", player_id)
    result = _query_rows(
        "SELECT bsa.*, g.game_date, g.matchup "
        "FROM Box_Score_Advanced bsa "
        "JOIN Games g ON bsa.game_id = g.game_id "
        "WHERE bsa.person_id = ? "
        "ORDER BY g.game_date DESC "
        "LIMIT 20",
        (player_id,),
        label="get_player_advanced",
    )
    return {"advanced": result}


@app.get("/api/players/{player_id}/shot-chart")
def get_player_shot_chart(player_id: int) -> dict:
    """Return shot chart data for a player."""
    logger.info("GET /api/players/%d/shot-chart", player_id)
    result = _query_rows(
        "SELECT * FROM Shot_Chart WHERE player_id = ? "
        "ORDER BY game_date DESC "
        "LIMIT 500",
        (player_id,),
        label="get_player_shot_chart",
    )
    return {"shots": result}


@app.get("/api/players/{player_id}/tracking")
def get_player_tracking(player_id: int) -> dict:
    """Return player tracking stats."""
    logger.info("GET /api/players/%d/tracking", player_id)
    result = _query_rows(
        "SELECT pts.*, g.game_date, g.matchup "
        "FROM Player_Tracking_Stats pts "
        "JOIN Games g ON pts.game_id = g.game_id "
        "WHERE pts.person_id = ? "
        "ORDER BY g.game_date DESC "
        "LIMIT 20",
        (player_id,),
        label="get_player_tracking",
    )
    return {"tracking": result}


@app.get("/api/players/{player_id}/clutch")
def get_player_clutch(player_id: int) -> dict:
    """Return player clutch stats."""
    logger.info("GET /api/players/%d/clutch", player_id)
    result = _query_rows(
        "SELECT * FROM Player_Clutch_Stats WHERE player_id = ? "
        "ORDER BY season DESC",
        (player_id,),
        label="get_player_clutch",
    )
    return {"clutch": result}


@app.get("/api/players/{player_id}/hustle")
def get_player_hustle(player_id: int) -> dict:
    """Return player hustle stats."""
    logger.info("GET /api/players/%d/hustle", player_id)
    result = _query_rows(
        "SELECT * FROM Player_Hustle_Stats WHERE player_id = ? "
        "ORDER BY season DESC",
        (player_id,),
        label="get_player_hustle",
    )
    return {"hustle": result}


@app.get("/api/players/{player_id}/scoring")
def get_player_scoring(player_id: int) -> dict:
    """Return scoring box score stats for a player."""
    logger.info("GET /api/players/%d/scoring", player_id)
    result = _query_rows(
        "SELECT bss.*, g.game_date, g.matchup "
        "FROM Box_Score_Scoring bss "
        "JOIN Games g ON bss.game_id = g.game_id "
        "WHERE bss.person_id = ? "
        "ORDER BY g.game_date DESC "
        "LIMIT 20",
        (player_id,),
        label="get_player_scoring",
    )
    return {"scoring": result}


@app.get("/api/players/{player_id}/usage")
def get_player_usage(player_id: int) -> dict:
    """Return usage box score stats for a player."""
    logger.info("GET /api/players/%d/usage", player_id)
    result = _query_rows(
        "SELECT bsu.*, g.game_date, g.matchup "
        "FROM Box_Score_Usage bsu "
        "JOIN Games g ON bsu.game_id = g.game_id "
        "WHERE bsu.person_id = ? "
        "ORDER BY g.game_date DESC "
        "LIMIT 20",
        (player_id,),
        label="get_player_usage",
    )
    return {"usage": result}


@app.get("/api/teams/{team_id}/details")
def get_team_details(team_id: int) -> dict:
    """Return detailed team information."""
    logger.info("GET /api/teams/%d/details", team_id)
    result = _query_one(
        "SELECT * FROM Team_Details WHERE team_id = ?",
        (team_id,),
        label="get_team_details",
    )
    return {"details": result or {}}


@app.get("/api/teams/{team_id}/clutch")
def get_team_clutch(team_id: int) -> dict:
    """Return team clutch stats."""
    logger.info("GET /api/teams/%d/clutch", team_id)
    result = _query_rows(
        "SELECT * FROM Team_Clutch_Stats WHERE team_id = ? "
        "ORDER BY season DESC",
        (team_id,),
        label="get_team_clutch",
    )
    return {"clutch": result}


@app.get("/api/teams/{team_id}/hustle")
def get_team_hustle(team_id: int) -> dict:
    """Return team hustle stats."""
    logger.info("GET /api/teams/%d/hustle", team_id)
    result = _query_rows(
        "SELECT * FROM Team_Hustle_Stats WHERE team_id = ? "
        "ORDER BY season DESC",
        (team_id,),
        label="get_team_hustle",
    )
    return {"hustle": result}


@app.get("/api/teams/{team_id}/estimated-metrics")
def get_team_estimated_metrics(team_id: int) -> dict:
    """Return team estimated metrics."""
    logger.info("GET /api/teams/%d/estimated-metrics", team_id)
    result = _query_rows(
        "SELECT * FROM Team_Estimated_Metrics WHERE team_id = ? "
        "ORDER BY season DESC",
        (team_id,),
        label="get_team_estimated_metrics",
    )
    return {"metrics": result}


@app.get("/api/games/{game_id}/play-by-play")
def get_play_by_play(game_id: str) -> dict:
    """Return play-by-play data for a game."""
    logger.info("GET /api/games/%s/play-by-play", game_id)
    result = _query_rows(
        "SELECT * FROM Play_By_Play WHERE game_id = ? "
        "ORDER BY period, action_number",
        (game_id,),
        label="get_play_by_play",
    )
    return {"game_id": game_id, "plays": result}


@app.get("/api/games/{game_id}/win-probability")
def get_win_probability(game_id: str) -> dict:
    """Return win probability data for a game."""
    logger.info("GET /api/games/%s/win-probability", game_id)
    result = _query_rows(
        "SELECT * FROM Win_Probability_PBP WHERE game_id = ? "
        "ORDER BY event_num",
        (game_id,),
        label="get_win_probability",
    )
    return {"game_id": game_id, "probabilities": result}


@app.get("/api/games/{game_id}/rotation")
def get_game_rotation(game_id: str) -> dict:
    """Return rotation data for a game."""
    logger.info("GET /api/games/%s/rotation", game_id)
    result = _query_rows(
        "SELECT gr.*, p.full_name, t.abbreviation AS team_abbrev "
        "FROM Game_Rotation gr "
        "LEFT JOIN Players p ON gr.person_id = p.player_id "
        "LEFT JOIN Teams t ON gr.team_id = t.team_id "
        "WHERE gr.game_id = ? "
        "ORDER BY gr.team_id, gr.in_time_real",
        (game_id,),
        label="get_game_rotation",
    )
    return {"game_id": game_id, "rotations": result}


@app.get("/api/games/{game_id}/box-score")
def get_game_box_score(game_id: str) -> dict:
    """Return combined box score data for a game."""
    logger.info("GET /api/games/%s/box-score", game_id)
    result = _query_rows(
        "SELECT pgl.*, p.full_name, p.position, p.team_abbreviation "
        "FROM Player_Game_Logs pgl "
        "JOIN Players p ON pgl.player_id = p.player_id "
        "WHERE pgl.game_id = ? "
        "ORDER BY p.team_abbreviation, pgl.pts DESC",
        (game_id,),
        label="get_game_box_score",
    )
    return {"game_id": game_id, "players": result}


@app.get("/api/league-dash/players")
def get_league_dash_players() -> dict:
    """Return league dashboard player stats."""
    logger.info("GET /api/league-dash/players")
    result = _query_rows(
        "SELECT ldps.*, p.full_name, p.position "
        "FROM League_Dash_Player_Stats ldps "
        "JOIN Players p ON ldps.player_id = p.player_id "
        "ORDER BY ldps.pts DESC "
        "LIMIT 200",
        label="get_league_dash_players",
    )
    return {"players": result}


@app.get("/api/league-dash/teams")
def get_league_dash_teams() -> dict:
    """Return league dashboard team stats."""
    logger.info("GET /api/league-dash/teams")
    result = _query_rows(
        "SELECT ldts.*, t.abbreviation, t.team_name "
        "FROM League_Dash_Team_Stats ldts "
        "JOIN Teams t ON ldts.team_id = t.team_id "
        "ORDER BY ldts.w_pct DESC",
        label="get_league_dash_teams",
    )
    return {"teams": result}


@app.get("/api/games/recent")
def get_recent_games() -> dict:
    """Return the most recent games."""
    logger.info("GET /api/games/recent")
    result = _query_rows(
        "SELECT * FROM Games "
        "WHERE home_score IS NOT NULL "
        "ORDER BY game_date DESC "
        "LIMIT 50",
        label="get_recent_games",
    )
    return {"games": result}


@app.get("/api/players/{player_id}/matchups")
def get_player_matchups(player_id: int) -> dict:
    """Return matchup data for a player (offensive)."""
    logger.info("GET /api/players/%d/matchups", player_id)
    result = _query_rows(
        "SELECT bsm.*, g.game_date, g.matchup AS game_matchup, "
        "p.full_name AS defender_name "
        "FROM Box_Score_Matchups bsm "
        "JOIN Games g ON bsm.game_id = g.game_id "
        "LEFT JOIN Players p ON bsm.person_id_def = p.player_id "
        "WHERE bsm.person_id_off = ? "
        "ORDER BY g.game_date DESC, bsm.matchup_min_sort DESC "
        "LIMIT 50",
        (player_id,),
        label="get_player_matchups",
    )
    return {"matchups": result}


@app.get("/api/schedule")
def get_schedule() -> dict:
    """Return schedule data."""
    logger.info("GET /api/schedule")
    result = _query_rows(
        "SELECT * FROM Schedule ORDER BY game_date DESC LIMIT 100",
        label="get_schedule",
    )
    return {"schedule": result}


# ---------------------------------------------------------------------------
# Engine-powered analysis endpoints
# ---------------------------------------------------------------------------


class PropAnalysisRequest(BaseModel):
    """Request body for the prop-analysis endpoint."""

    player_id: int
    stat_type: str = Field(..., description="Engine stat type, e.g. 'points', 'rebounds'.")
    prop_line: float = Field(..., gt=0, description="The sportsbook prop line.")
    opponent: str | None = Field(
        None,
        description="Opponent team abbreviation (e.g. 'BOS').  Auto-detected from today's schedule when omitted.",
    )
    vegas_spread: float = Field(0.0, description="Vegas spread (positive = player's team favored).")
    game_total: float = Field(220.0, description="Vegas over/under game total.")
    platform: str = Field("prizepicks", description="Betting platform name.")


@app.get("/api/players/{player_id}/projection")
def get_player_projection(
    player_id: int,
    opponent: str | None = None,
    vegas_spread: float = 0.0,
    game_total: float = 220.0,
) -> dict:
    """Return a full engine-powered stat projection for a player.

    Queries the local database for the player's season game logs, team data,
    and defensive ratings, then runs the projection engine to produce
    matchup-adjusted stat projections for tonight's game.

    Args:
        player_id: NBA player ID.
        opponent: Opponent team abbreviation.  Auto-detected from today's
            schedule when omitted.
        vegas_spread: Vegas point spread (positive = player team favored).
        game_total: Vegas over/under game total.

    Returns:
        JSON with ``projection`` dict and ``player_data`` metadata.

    Raises:
        HTTPException 404: Player not found.
        HTTPException 500: Engine or database error.
    """
    logger.info("GET /api/players/%d/projection", player_id)

    from engine.data_adapter import (
        build_engine_defense_data,
        build_engine_game_context,
        build_engine_game_logs,
        build_engine_player_data,
        build_engine_teams_data,
    )
    from engine.projections import build_player_projection

    # --- Player metadata ---
    player_row = _query_one(
        "SELECT * FROM Players WHERE player_id = ?",
        (player_id,),
        label="projection/player",
    )
    if player_row is None:
        raise HTTPException(status_code=404, detail=f"Player {player_id} not found.")

    team_abbrev = player_row.get("team_abbreviation", "")

    # --- Season game logs (up to 82 games) ---
    season_logs = _query_rows(
        """
        SELECT l.*, g.game_date, g.matchup, g.home_abbrev, g.away_abbrev
        FROM Player_Game_Logs l
        JOIN Games g ON g.game_id = l.game_id
        WHERE l.player_id = ?
        ORDER BY g.game_date DESC
        LIMIT ?
        """,
        (player_id, MAX_SEASON_GAMES),
        label="projection/season_logs",
    )

    # --- Auto-detect opponent from today's schedule if not provided ---
    game_row: dict | None = None
    if not opponent:
        today = date.today().isoformat()
        game_row = _query_one(
            """
            SELECT * FROM Games
            WHERE game_date = ?
              AND (home_abbrev = ? OR away_abbrev = ?)
            LIMIT 1
            """,
            (today, team_abbrev, team_abbrev),
            label="projection/today_game",
        )
        if game_row:
            home = game_row.get("home_abbrev", "")
            away = game_row.get("away_abbrev", "")
            opponent = away if team_abbrev.upper() == home.upper() else home

    if not opponent:
        raise HTTPException(
            status_code=400,
            detail=(
                "Could not auto-detect opponent.  "
                "Provide ?opponent=BOS (or the relevant team abbreviation)."
            ),
        )

    # --- Teams and defense data ---
    teams_raw = _query_rows("SELECT * FROM Teams", label="projection/teams")
    defense_raw = _query_rows(
        "SELECT * FROM Defense_Vs_Position WHERE team_abbreviation = ?",
        (opponent.upper(),),
        label="projection/defense",
    )

    # --- Transform to engine format ---
    engine_logs = build_engine_game_logs(season_logs)
    player_data = build_engine_player_data(player_row, engine_logs)
    teams_data = build_engine_teams_data(teams_raw)
    defense_data = build_engine_defense_data(defense_raw)
    is_home = (
        game_row.get("home_abbrev", "").upper() == team_abbrev.upper()
        if game_row
        else True
    )
    recent_5 = engine_logs[:5]

    try:
        projection = build_player_projection(
            player_data=player_data,
            opponent_team_abbreviation=opponent.upper(),
            is_home_game=is_home,
            rest_days=1,
            game_total=game_total,
            defensive_ratings_data=defense_data,
            teams_data=teams_data,
            recent_form_games=recent_5,
            vegas_spread=vegas_spread,
        )
    except Exception as exc:
        logger.exception("Projection engine failed for player %d.", player_id)
        raise HTTPException(status_code=500, detail=f"Projection error: {exc}") from exc

    return {
        "player_id": player_id,
        "player_name": player_data.get("name", ""),
        "team": team_abbrev,
        "opponent": opponent.upper(),
        "is_home": is_home,
        "projection": projection,
    }


@app.post("/api/picks/analyze")
def analyze_prop(body: PropAnalysisRequest) -> dict:
    """Run a complete pick analysis: projection → simulation → edge → confidence → explanation.

    This is the main engine endpoint.  It chains every engine module into a
    single response that the frontend can render as a rich prop-analysis card.

    Request body:
        See :class:`PropAnalysisRequest`.

    Returns:
        JSON with ``projection``, ``simulation``, ``edge``, ``confidence``,
        ``explanation``, and metadata.
    """
    logger.info(
        "POST /api/picks/analyze player=%d stat=%s line=%.1f",
        body.player_id,
        body.stat_type,
        body.prop_line,
    )

    from engine.confidence import calculate_confidence_score
    from engine.data_adapter import (
        STAT_TYPE_TO_DB_COL,
        STAT_TYPE_TO_PROJECTION_KEY,
        build_engine_defense_data,
        build_engine_game_logs,
        build_engine_player_data,
        build_engine_teams_data,
        compute_season_averages,
        get_stat_std_from_logs,
    )
    from engine.edge_detection import analyze_directional_forces
    from engine.explainer import generate_pick_explanation
    from engine.projections import build_player_projection
    from engine.simulation import run_enhanced_simulation

    # Phase 3 engine modules
    from engine.bankroll import calculate_kelly_fraction, odds_to_payout_multiplier
    from engine.regime_detection import detect_regime_change

    # Phase 4 engine modules
    from engine.game_script import blend_with_flat_simulation, simulate_game_script
    from engine.impact_metrics import calculate_player_efficiency_profile
    from engine.matchup_history import calculate_matchup_adjustment, get_player_vs_team_history
    from engine.rotation_tracker import detect_role_change, get_minutes_adjustment
    from engine.stat_distributions import get_over_probability

    # --- Validate stat type ---
    stat_type = body.stat_type.lower()
    if stat_type not in STAT_TYPE_TO_DB_COL:
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported stat_type '{body.stat_type}'.  "
            f"Valid types: {', '.join(sorted(STAT_TYPE_TO_DB_COL))}",
        )

    # --- Player ---
    player_row = _query_one(
        "SELECT * FROM Players WHERE player_id = ?",
        (body.player_id,),
        label="analyze/player",
    )
    if player_row is None:
        raise HTTPException(status_code=404, detail=f"Player {body.player_id} not found.")

    team_abbrev = player_row.get("team_abbreviation", "")

    # --- Season logs ---
    season_logs = _query_rows(
        """
        SELECT l.*, g.game_date, g.matchup, g.home_abbrev, g.away_abbrev
        FROM Player_Game_Logs l
        JOIN Games g ON g.game_id = l.game_id
        WHERE l.player_id = ?
        ORDER BY g.game_date DESC
        LIMIT ?
        """,
        (body.player_id, MAX_SEASON_GAMES),
        label="analyze/season_logs",
    )

    # --- Opponent auto-detect ---
    opponent = body.opponent
    game_row: dict | None = None
    if not opponent:
        today = date.today().isoformat()
        game_row = _query_one(
            """
            SELECT * FROM Games
            WHERE game_date = ?
              AND (home_abbrev = ? OR away_abbrev = ?)
            LIMIT 1
            """,
            (today, team_abbrev, team_abbrev),
            label="analyze/today_game",
        )
        if game_row:
            home = game_row.get("home_abbrev", "")
            away = game_row.get("away_abbrev", "")
            opponent = away if team_abbrev.upper() == home.upper() else home

    if not opponent:
        raise HTTPException(
            status_code=400,
            detail="Could not auto-detect opponent.  Provide 'opponent' in the request body.",
        )
    opponent = opponent.upper()

    # --- Teams & defense ---
    teams_raw = _query_rows("SELECT * FROM Teams", label="analyze/teams")
    defense_raw = _query_rows(
        "SELECT * FROM Defense_Vs_Position WHERE team_abbreviation = ?",
        (opponent,),
        label="analyze/defense",
    )

    # --- Transform ---
    engine_logs = build_engine_game_logs(season_logs)
    player_data = build_engine_player_data(player_row, engine_logs)
    teams_data = build_engine_teams_data(teams_raw)
    defense_data = build_engine_defense_data(defense_raw)
    is_home = (
        game_row.get("home_abbrev", "").upper() == team_abbrev.upper()
        if game_row
        else True
    )
    recent_5 = engine_logs[:5]

    # ── Step 1: Projection ──────────────────────────────────────────
    try:
        projection = build_player_projection(
            player_data=player_data,
            opponent_team_abbreviation=opponent,
            is_home_game=is_home,
            rest_days=1,
            game_total=body.game_total,
            defensive_ratings_data=defense_data,
            teams_data=teams_data,
            recent_form_games=recent_5,
            vegas_spread=body.vegas_spread,
        )
    except Exception as exc:
        logger.exception("Projection failed for player %d.", body.player_id)
        raise HTTPException(status_code=500, detail=f"Projection error: {exc}") from exc

    # ── Step 2: Simulation ──────────────────────────────────────────
    proj_key = STAT_TYPE_TO_PROJECTION_KEY.get(stat_type, f"projected_{stat_type}")
    projected_avg = float(projection.get(proj_key, 0) or 0)
    stat_std = get_stat_std_from_logs(season_logs, stat_type)
    recent_values = [float(g.get(STAT_TYPE_TO_DB_COL[stat_type]) or 0) for g in engine_logs[:20]]

    try:
        sim_result = run_enhanced_simulation(
            projected_stat_average=projected_avg,
            stat_standard_deviation=stat_std,
            prop_line=body.prop_line,
            blowout_risk_factor=float(projection.get("blowout_risk", 0.15)),
            pace_adjustment_factor=float(projection.get("pace_factor", 1.0)),
            matchup_adjustment_factor=float(projection.get("defense_factor", 1.0)),
            home_away_adjustment=float(projection.get("home_away_factor", 0.0)),
            rest_adjustment_factor=float(projection.get("rest_factor", 1.0)),
            stat_type=stat_type,
            recent_game_logs=recent_values if len(recent_values) >= 10 else None,
            vegas_spread=body.vegas_spread,
            game_total=body.game_total,
        )
    except Exception as exc:
        logger.exception("Simulation failed for player %d.", body.player_id)
        raise HTTPException(status_code=500, detail=f"Simulation error: {exc}") from exc

    prob_over = float(sim_result.get("probability_over", 0.5))

    # ── Step 3: Edge detection ──────────────────────────────────────
    game_ctx = {
        "opponent": opponent,
        "is_home": is_home,
        "rest_days": 1,
        "game_total": body.game_total,
        "vegas_spread": body.vegas_spread,
    }

    try:
        forces = analyze_directional_forces(
            player_data=player_data,
            prop_line=body.prop_line,
            stat_type=stat_type,
            projection_result=projection,
            game_context=game_ctx,
            recent_form_ratio=projection.get("recent_form_ratio"),
        )
    except Exception as exc:
        logger.warning("Edge detection failed: %s", exc)
        forces = {
            "over_forces": [], "under_forces": [],
            "over_count": 0, "under_count": 0,
            "over_strength": 0.0, "under_strength": 0.0,
            "net_direction": "OVER" if prob_over > 0.5 else "UNDER",
            "net_strength": 0.0, "conflict_severity": 0.0,
        }

    # Direction
    direction = "OVER" if prob_over >= 0.5 else "UNDER"
    model_prob = prob_over if direction == "OVER" else (1.0 - prob_over)

    # Edge (vs -110 breakeven of 52.38%)
    implied_prob_minus_110 = 110.0 / (110.0 + 100.0)
    edge_pct = round((model_prob - implied_prob_minus_110) * 100, 2)

    # ── Step 4: Confidence scoring ──────────────────────────────────
    season_avgs = compute_season_averages(engine_logs)
    db_col = STAT_TYPE_TO_DB_COL[stat_type]
    stat_avg = season_avgs.get(db_col, projected_avg)

    try:
        confidence = calculate_confidence_score(
            probability_over=prob_over,
            edge_percentage=edge_pct,
            directional_forces=forces,
            defense_factor=float(projection.get("defense_factor", 1.0)),
            stat_standard_deviation=stat_std,
            stat_average=stat_avg,
            simulation_results=sim_result.get("simulated_results", []),
            games_played=player_data.get("games_played"),
            recent_form_ratio=projection.get("recent_form_ratio"),
            stat_type=stat_type,
            platform=body.platform,
        )
    except Exception as exc:
        logger.warning("Confidence scoring failed: %s", exc)
        confidence = {
            "confidence_score": 50.0,
            "tier": "Bronze",
            "tier_emoji": "🥉",
            "direction": direction,
            "should_avoid": False,
            "avoid_reasons": [],
        }

    # ── Step 5: Regime detection ────────────────────────────────────
    try:
        db_col = STAT_TYPE_TO_DB_COL[stat_type]
        regime = detect_regime_change(
            game_logs=engine_logs,
            stat_key=db_col,
            window=10,
        )
    except Exception as exc:
        logger.warning("Regime detection failed: %s", exc)
        regime = {
            "regime_changed": False,
            "direction": "stable",
            "magnitude": 0.0,
            "confidence": 0.0,
            "detection_method": "error_fallback",
        }

    # ── Step 6: Kelly Criterion bet sizing ──────────────────────────
    # Default -110 payout for standard props (1.909x gross payout).
    payout_multiplier = odds_to_payout_multiplier(-110)
    try:
        kelly_fraction = calculate_kelly_fraction(
            win_probability=model_prob,
            payout_multiplier=payout_multiplier,
            kelly_fraction_mode="quarter",
        )
    except Exception as exc:
        logger.warning("Kelly sizing failed: %s", exc)
        kelly_fraction = 0.0

    bankroll_sizing = {
        "kelly_fraction": round(kelly_fraction, 6),
        "kelly_mode": "quarter",
        "payout_multiplier": round(payout_multiplier, 4),
        "recommended_pct": f"{kelly_fraction * 100:.2f}%",
    }

    # ── Step 7: Game-script simulation (Phase 4) ───────────────────
    try:
        gs_projection = {
            "projected_stat": projected_avg,
            "projected_minutes": float(player_data.get("minutes_avg", 32.0)),
            "stat_std": stat_std,
        }
        gs_context = {
            "vegas_spread": body.vegas_spread,
            "game_total": body.game_total,
            "is_home": is_home,
        }
        gs_result = simulate_game_script(gs_projection, gs_context)
        flat_for_blend = {
            "mean": float(sim_result.get("simulated_mean", projected_avg)),
            "std": float(sim_result.get("simulated_std", stat_std)),
        }
        gs_blend = blend_with_flat_simulation(gs_result, flat_for_blend)
        game_script = {
            "blended_mean": gs_blend.get("blended_mean"),
            "blended_std": gs_blend.get("blended_std"),
            "game_script_mean": gs_blend.get("game_script_mean"),
            "flat_mean": gs_blend.get("flat_mean"),
            "blend_weight": gs_blend.get("blend_weight"),
            "blowout_game_rate": gs_result.get("blowout_game_rate", 0.0),
            "player_tier": gs_result.get("player_tier", "rotation"),
        }
    except Exception as exc:
        logger.warning("Game-script simulation failed: %s", exc)
        game_script = {"error": "game_script_unavailable"}

    # ── Step 8: Matchup history (Phase 4) ──────────────────────────
    try:
        player_name = player_data.get("name", "")
        matchup_hist = get_player_vs_team_history(
            player_name=player_name,
            opponent_team=opponent,
            stat_type=stat_type,
            game_logs=engine_logs,
            season_average=stat_avg,
        )
        matchup_adj = calculate_matchup_adjustment(
            player_name=player_name,
            opponent_team=opponent,
            stat_type=stat_type,
            game_logs=engine_logs,
            season_average=stat_avg,
        )
        matchup_history = {
            "games_found": matchup_hist.get("games_found", 0),
            "avg_vs_team": matchup_hist.get("avg_vs_team"),
            "std_vs_team": matchup_hist.get("std_vs_team", 0.0),
            "matchup_favorability_score": matchup_hist.get("matchup_favorability_score", 50.0),
            "cold_start": matchup_hist.get("cold_start", True),
            "adjustment_factor": matchup_adj,
        }
    except Exception as exc:
        logger.warning("Matchup history failed: %s", exc)
        matchup_history = {"cold_start": True, "error": "matchup_history_unavailable"}

    # ── Step 9: Rotation / minutes trend (Phase 4) ─────────────────
    try:
        role_change = detect_role_change(engine_logs)
        minutes_adj = get_minutes_adjustment(engine_logs)
        rotation = {
            "role_change_detected": role_change.get("role_change_detected", False),
            "change_type": role_change.get("change_type", "none"),
            "minutes_before": role_change.get("minutes_before", 0.0),
            "minutes_after": role_change.get("minutes_after", 0.0),
            "change_magnitude": role_change.get("change_magnitude", 0.0),
            "minutes_adjustment": round(minutes_adj, 4),
        }
    except Exception as exc:
        logger.warning("Rotation tracker failed: %s", exc)
        rotation = {"role_change_detected": False, "minutes_adjustment": 1.0, "error": "rotation_unavailable"}

    # ── Step 10: Distribution cross-check (Phase 4) ────────────────
    try:
        analytical_prob = get_over_probability(
            mean=projected_avg,
            std=stat_std,
            line=body.prop_line,
            stat_type=stat_type,
        )
        distribution_check = {
            "analytical_probability": round(analytical_prob, 4),
            "monte_carlo_probability": round(prob_over, 4),
            "delta": round(abs(analytical_prob - prob_over), 4),
        }
    except Exception as exc:
        logger.warning("Distribution cross-check failed: %s", exc)
        distribution_check = {"error": "distribution_check_unavailable"}

    # ── Step 11: Player efficiency profile (Phase 4) ───────────────
    try:
        efficiency = calculate_player_efficiency_profile(player_data)
    except Exception as exc:
        logger.warning("Efficiency profile failed: %s", exc)
        efficiency = {"error": "efficiency_unavailable"}

    # ── Step 12: Explanation ────────────────────────────────────────
    try:
        explanation = generate_pick_explanation(
            player_data=player_data,
            prop_line=body.prop_line,
            stat_type=stat_type,
            direction=direction,
            projection_result=projection,
            simulation_results=sim_result,
            forces=forces,
            confidence_result=confidence,
            game_context=game_ctx,
            platform=body.platform,
            recent_form_games=recent_5,
        )
    except Exception as exc:
        logger.warning("Explainer failed: %s", exc)
        explanation = {"tldr": "Analysis complete.", "verdict": direction}

    # ── Assemble response ───────────────────────────────────────────
    return {
        "player_id": body.player_id,
        "player_name": player_data.get("name", ""),
        "team": team_abbrev,
        "opponent": opponent,
        "stat_type": stat_type,
        "prop_line": body.prop_line,
        "direction": direction,
        "model_probability": round(model_prob, 4),
        "edge_pct": edge_pct,
        "projection": projection,
        "simulation": {
            k: v
            for k, v in sim_result.items()
            if k != "simulated_results"  # exclude the raw 2000-element array
        },
        "forces": forces,
        "confidence": confidence,
        "regime": regime,
        "bankroll": bankroll_sizing,
        # Phase 4 — advanced engine modules
        "game_script": game_script,
        "matchup_history": matchup_history,
        "rotation": rotation,
        "distribution_check": distribution_check,
        "efficiency": efficiency,
        "explanation": explanation,
    }


# ---------------------------------------------------------------------------
# Pick-tracking endpoints (Phase 3)
# ---------------------------------------------------------------------------


class SavePickRequest(BaseModel):
    """Request body for persisting an analysis result."""

    player_id: int
    player_name: str = ""
    team: str = ""
    opponent: str = ""
    stat_type: str
    prop_line: float
    direction: str
    model_probability: float = 0.5
    edge_pct: float = 0.0
    confidence_score: float = 0.0
    tier: str = "Bronze"
    kelly_fraction: float = 0.0
    recommended_bet: float = 0.0
    regime_flag: str = "stable"
    platform: str = "prizepicks"
    vegas_spread: float = 0.0
    game_total: float = 220.0
    game_date: str | None = None


@app.post("/api/picks/save")
def save_pick(body: SavePickRequest) -> dict:
    """Persist a pick analysis result for future tracking.

    Stores the key fields from an ``/api/picks/analyze`` response into the
    ``Saved_Picks`` table so the user can review history and eventual
    outcomes.

    Returns:
        ``{"status": "saved", "pick_id": <int>}`` on success.
    """
    logger.info(
        "POST /api/picks/save player=%d stat=%s line=%.1f",
        body.player_id,
        body.stat_type,
        body.prop_line,
    )

    pick_date = date.today().isoformat()

    try:
        with _db() as conn:
            cursor = conn.execute(
                """
                INSERT INTO Saved_Picks (
                    player_id, player_name, team, opponent, stat_type,
                    prop_line, direction, model_probability, edge_pct,
                    confidence_score, tier, kelly_fraction, recommended_bet,
                    regime_flag, platform, vegas_spread, game_total,
                    pick_date, game_date
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    body.player_id,
                    body.player_name,
                    body.team,
                    body.opponent,
                    body.stat_type,
                    body.prop_line,
                    body.direction,
                    body.model_probability,
                    body.edge_pct,
                    body.confidence_score,
                    body.tier,
                    body.kelly_fraction,
                    body.recommended_bet,
                    body.regime_flag,
                    body.platform,
                    body.vegas_spread,
                    body.game_total,
                    pick_date,
                    body.game_date or pick_date,
                ),
            )
            conn.commit()
            pick_id = cursor.lastrowid
        return {"status": "saved", "pick_id": pick_id}
    except sqlite3.Error as exc:
        logger.exception("Failed to save pick.")
        raise HTTPException(status_code=500, detail=str(exc)) from exc


PICK_HISTORY_DEFAULT_LIMIT = 50


@app.get("/api/picks/history")
def get_pick_history(limit: int = PICK_HISTORY_DEFAULT_LIMIT) -> dict:
    """Return saved picks, newest first.

    Args:
        limit: Maximum number of picks to return (default 50).

    Returns:
        ``{"picks": [...]}``.
    """
    logger.info("GET /api/picks/history limit=%d", limit)
    picks = _query_rows(
        "SELECT * FROM Saved_Picks ORDER BY created_at DESC LIMIT ?",
        (min(limit, 500),),
        label="get_pick_history",
    )
    return {"picks": picks}


class UpdatePickResultRequest(BaseModel):
    """Request body for recording the outcome of a saved pick."""

    pick_id: int
    result: str = Field(..., description="'hit', 'miss', or 'push'.")
    actual_value: float | None = None


@app.post("/api/picks/result")
def update_pick_result(body: UpdatePickResultRequest) -> dict:
    """Record the outcome of a previously saved pick.

    Updates the ``result`` and ``actual_value`` columns of the matching
    ``Saved_Picks`` row.

    Returns:
        ``{"status": "updated", "pick_id": <int>}``.
    """
    logger.info("POST /api/picks/result pick_id=%d result=%s", body.pick_id, body.result)
    allowed = {"hit", "miss", "push"}
    if body.result.lower() not in allowed:
        raise HTTPException(status_code=400, detail=f"result must be one of {allowed}")

    try:
        with _db() as conn:
            conn.execute(
                "UPDATE Saved_Picks SET result = ?, actual_value = ? WHERE pick_id = ?",
                (body.result.lower(), body.actual_value, body.pick_id),
            )
            conn.commit()
        return {"status": "updated", "pick_id": body.pick_id}
    except sqlite3.Error as exc:
        logger.exception("Failed to update pick result.")
        raise HTTPException(status_code=500, detail=str(exc)) from exc


# ---------------------------------------------------------------------------
# ML Pipeline endpoints
# ---------------------------------------------------------------------------


@app.get("/api/picks/today")
def get_todays_picks():
    """Run the ML pipeline and return today's player prop predictions."""
    try:
        from engine.pipeline.run_pipeline import run_full_pipeline
        context = run_full_pipeline()
        return {
            "date": context.get("date_str"),
            "predictions": context.get("predictions", []),
            "evaluation": context.get("evaluation", {}),
            "errors": context.get("errors", []),
        }
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.post("/api/admin/train-models")
def train_models_endpoint():
    """Trigger ML model training for pts, reb, ast."""
    try:
        from engine.models.train import train_models
        results = {}
        for stat in ["pts", "reb", "ast"]:
            results[stat] = train_models(stat)
        return {"status": "success", "results": results}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    uvicorn.run("api:app", host="0.0.0.0", port=8000, reload=True)
