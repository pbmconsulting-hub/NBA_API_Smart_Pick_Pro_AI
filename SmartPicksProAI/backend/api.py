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
            "SELECT game_id, game_date, season, home_team_id, away_team_id, "
            "home_abbrev, away_abbrev, matchup, home_score, away_score "
            "FROM Games WHERE game_date = ?",
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
                raw_away = teams.iloc[0].get("teamId")
                raw_home = teams.iloc[1].get("teamId")
                away_team_id = int(raw_away) if raw_away is not None else None
                home_team_id = int(raw_home) if raw_home is not None else None
                matchup = f"{home_tri} vs. {away_tri}"
            else:
                away_tri = ""
                home_tri = ""
                away_team_id = None
                home_team_id = None
                matchup = game_row.get("gameCode", "TBD")
            live_games.append({
                "game_id": game_id,
                "game_date": today,
                "home_team_id": home_team_id,
                "away_team_id": away_team_id,
                "home_abbrev": home_tri,
                "away_abbrev": away_tri,
                "matchup": matchup,
            })
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
# Player / Team lookup endpoints
# ---------------------------------------------------------------------------


@app.get("/api/players/search")
def search_players(q: str = "") -> dict:
    """Search for players by name.

    Performs a case-insensitive ``LIKE`` search against ``full_name``,
    ``first_name``, and ``last_name`` in the Players table.  Returns up to
    25 matching players with basic info (id, name, team, position).

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

    conn = _get_conn()
    try:
        pattern = f"%{q.strip()}%"
        rows = conn.execute(
            """
            SELECT player_id, first_name, last_name, full_name,
                   team_id, team_abbreviation, position
            FROM Players
            WHERE full_name LIKE ?
               OR first_name LIKE ?
               OR last_name LIKE ?
            ORDER BY full_name
            LIMIT 25
            """,
            (pattern, pattern, pattern),
        ).fetchall()
        return {"results": [dict(r) for r in rows]}
    except Exception as exc:
        logger.exception("Error searching players for q=%s.", q)
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    finally:
        conn.close()


@app.get("/api/teams")
def get_teams() -> dict:
    """List all NBA teams stored in the database.

    Returns:
        JSON with a ``teams`` list sorted by abbreviation.

    Raises:
        HTTPException 500: On unexpected database errors.
    """
    logger.info("GET /api/teams")
    conn = _get_conn()
    try:
        rows = conn.execute(
            "SELECT team_id, abbreviation, team_name, conference, division, "
            "pace, ortg, drtg "
            "FROM Teams ORDER BY abbreviation"
        ).fetchall()
        return {"teams": [dict(r) for r in rows]}
    except Exception as exc:
        logger.exception("Error fetching teams list.")
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    finally:
        conn.close()


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
    conn = _get_conn()
    try:
        rows = conn.execute(
            """
            SELECT p.player_id, p.first_name, p.last_name, p.full_name,
                   p.position, p.team_abbreviation
            FROM Team_Roster r
            JOIN Players p ON p.player_id = r.player_id
            WHERE r.team_id = ?
            ORDER BY p.last_name
            """,
            (team_id,),
        ).fetchall()

        # Fallback: if Team_Roster has no rows for this team, use Players.team_id.
        if not rows:
            rows = conn.execute(
                """
                SELECT player_id, first_name, last_name, full_name,
                       position, team_abbreviation
                FROM Players
                WHERE team_id = ?
                ORDER BY last_name
                """,
                (team_id,),
            ).fetchall()

        return {"team_id": team_id, "players": [dict(r) for r in rows]}
    except Exception as exc:
        logger.exception("Error fetching roster for team %d.", team_id)
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    finally:
        conn.close()


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
    last_n = max(1, min(last_n, 82))
    conn = _get_conn()
    try:
        rows = conn.execute(
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
        ).fetchall()
        return {"team_id": team_id, "games": [dict(r) for r in rows]}
    except Exception as exc:
        logger.exception("Error fetching stats for team %d.", team_id)
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    finally:
        conn.close()


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
    conn = _get_conn()
    try:
        rows = conn.execute(
            """
            SELECT pos, vs_pts_mult, vs_reb_mult, vs_ast_mult,
                   vs_stl_mult, vs_blk_mult, vs_3pm_mult
            FROM Defense_Vs_Position
            WHERE team_abbreviation = ?
            ORDER BY pos
            """,
            (abbrev,),
        ).fetchall()
        return {
            "team_abbreviation": abbrev,
            "positions": [dict(r) for r in rows],
        }
    except Exception as exc:
        logger.exception(
            "Error fetching defense-vs-position for %s.", abbrev
        )
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    uvicorn.run("api:app", host="0.0.0.0", port=8000, reload=True)
