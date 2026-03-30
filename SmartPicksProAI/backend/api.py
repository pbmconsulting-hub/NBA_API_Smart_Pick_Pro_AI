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
GET  /api/players/{player_id}/last5   – Last 5 game logs with computed averages.
GET  /api/games/today                 – Today's NBA matchups (DB first, then live).
POST /api/admin/refresh-data          – Trigger an incremental data update.
"""

import logging
import sqlite3
from datetime import date

import uvicorn
from fastapi import FastAPI, HTTPException
from nba_api.stats.endpoints import ScoreboardV3

import data_updater
import setup_db

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

DB_PATH = setup_db.DB_PATH

app = FastAPI(
    title="SmartPicksProAI API",
    description="NBA player stats and game data for ML-powered prop predictions.",
    version="1.0.0",
)


# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------


def _get_conn() -> sqlite3.Connection:
    """Open and return a SQLite connection with row_factory set.

    Returns:
        An open :class:`sqlite3.Connection` with ``row_factory`` configured
        so that rows are returned as :class:`sqlite3.Row` objects (accessible
        by column name).
    """
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


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
    conn = _get_conn()
    try:
        player_row = conn.execute(
            "SELECT player_id, first_name, last_name FROM Players WHERE player_id = ?",
            (player_id,),
        ).fetchone()

        if player_row is None:
            raise HTTPException(status_code=404, detail=f"Player {player_id} not found.")

        rows = conn.execute(
            """
            SELECT
                g.game_date,
                g.season,
                g.home_abbrev,
                g.away_abbrev,
                l.game_id,
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
        ).fetchall()

        games = [dict(row) for row in rows]

        stat_keys = [
            "pts", "reb", "ast", "blk", "stl", "tov",
            "fgm", "fga", "fg_pct",
            "fg3m", "fg3a", "fg3_pct",
            "ftm", "fta", "ft_pct",
            "oreb", "dreb", "pf", "plus_minus",
        ]
        if games:
            averages = {
                k: round(sum(g[k] or 0 for g in games) / len(games), 1)
                for k in stat_keys
            }
        else:
            averages = {k: 0.0 for k in stat_keys}

        return {
            "player_id": player_row["player_id"],
            "first_name": player_row["first_name"],
            "last_name": player_row["last_name"],
            "games": games,
            "averages": averages,
        }
    except HTTPException:
        raise
    except Exception as exc:
        logger.exception("Error fetching last-5 for player %d.", player_id)
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    finally:
        conn.close()


@app.get("/api/games/today")
def get_games_today() -> dict:
    """Return today's NBA matchups.

    Checks the Games table first.  If no games are found for today's date,
    falls back to a live query via the ``ScoreboardV3`` endpoint.

    Returns:
        JSON with a list of today's games::

            {
              "date": "2026-03-30",
              "source": "database",   // or "live"
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

    conn = _get_conn()
    try:
        rows = conn.execute(
            "SELECT game_id, game_date, season, home_abbrev, away_abbrev, matchup FROM Games WHERE game_date = ?",
            (today,),
        ).fetchall()
    except Exception as exc:
        logger.exception("Database error querying today's games.")
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    finally:
        conn.close()

    if rows:
        logger.info("Found %d games in DB for %s.", len(rows), today)
        return {
            "date": today,
            "source": "database",
            "games": [dict(row) for row in rows],
        }

    # Fall back to the live ScoreboardV3 endpoint.
    logger.info("No games in DB for %s — fetching live data via ScoreboardV3.", today)
    try:
        scoreboard = ScoreboardV3(game_date=today)
        game_header = scoreboard.game_header.get_data_frame()
        line_score = scoreboard.line_score.get_data_frame()

        live_games = []
        for _, game_row in game_header.iterrows():
            game_id = str(game_row.get("gameId", ""))
            # LineScore has 2 rows per game: away team first, home team second.
            teams = line_score[line_score["gameId"].astype(str) == game_id]
            if len(teams) >= 2:
                away_tri = teams.iloc[0].get("teamTricode", "")
                home_tri = teams.iloc[1].get("teamTricode", "")
                matchup = f"{away_tri} @ {home_tri}"
            else:
                matchup = game_row.get("gameCode", "TBD")
            live_games.append({"game_id": game_id, "matchup": matchup})
        logger.info("ScoreboardV3 returned %d games.", len(live_games))
        return {"date": today, "source": "live", "games": live_games}
    except Exception as exc:
        logger.exception("Error fetching live scoreboard for %s.", today)
        raise HTTPException(status_code=500, detail=str(exc)) from exc


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
    except Exception as exc:
        logger.exception("Error during data refresh.")
        raise HTTPException(status_code=500, detail=str(exc)) from exc


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    uvicorn.run("api:app", host="0.0.0.0", port=8000, reload=True)
