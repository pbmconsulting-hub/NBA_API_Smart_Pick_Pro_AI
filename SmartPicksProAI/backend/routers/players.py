"""
routers/players.py
------------------
Player-related API endpoints extracted from the monolithic ``api.py``.

All database access goes through the shared helpers in :mod:`db`.
"""

import logging
from datetime import date

from fastapi import APIRouter, HTTPException, Query

from db import (
    MAX_SEARCH_RESULTS,
    MAX_SEASON_GAMES,
    _compute_rest_days,
    _query_one,
    _query_rows,
)
from engine.data_adapter import (
    build_engine_defense_data,
    build_engine_game_context,
    build_engine_game_logs,
    build_engine_player_data,
    build_engine_teams_data,
)
from engine.projections import build_player_projection

logger = logging.getLogger(__name__)

router = APIRouter()

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


@router.get("/api/players/{player_id}/last5")
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


@router.get("/api/players/search")
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


@router.get("/api/players/{player_id}/bio")
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


@router.get("/api/players/{player_id}/career")
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


@router.get("/api/players/{player_id}/advanced")
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


@router.get("/api/players/{player_id}/shot-chart")
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


@router.get("/api/players/{player_id}/tracking")
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


@router.get("/api/players/{player_id}/clutch")
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


@router.get("/api/players/{player_id}/hustle")
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


@router.get("/api/players/{player_id}/scoring")
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


@router.get("/api/players/{player_id}/usage")
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


@router.get("/api/players/{player_id}/matchups")
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


@router.get("/api/players/{player_id}/projection")
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
            rest_days=_compute_rest_days(team_abbrev),
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
