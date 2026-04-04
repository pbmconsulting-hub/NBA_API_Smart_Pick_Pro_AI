"""engine/pipeline/step_1_ingest.py – Phase 1: Ingest raw NBA data from smartpicks.db.

Pulls Player_Game_Logs (with player/team metadata), Games, Team_Game_Stats,
Defense_Vs_Position, Box_Score_Advanced, Box_Score_Usage, and Standings so that
downstream feature engineering has the rich context needed by the ML models.
"""
import datetime
import os
import sqlite3
from utils.logger import get_logger

_logger = get_logger(__name__)
_RAW_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "data", "raw"
)
_DB_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "db", "smartpicks.db"
)


def _get_conn(db_path: str) -> sqlite3.Connection:
    """Open a WAL-mode connection with Row factory."""
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.row_factory = sqlite3.Row
    return conn


def _safe_query(db_path: str, sql: str, params: tuple = ()) -> list:
    """Execute *sql* and return a list of dicts, or [] on any error."""
    try:
        conn = _get_conn(db_path)
        cur = conn.cursor()
        cur.execute(sql, params)
        rows = [dict(r) for r in cur.fetchall()]
        conn.close()
        return rows
    except Exception as exc:
        _logger.debug("_safe_query failed: %s | sql=%s", exc, sql[:120])
        return []


# ── Individual table readers ──────────────────────────────────────────────

def _read_todays_games(db_path: str) -> list:
    """Read today's games from smartpicks.db."""
    today = datetime.date.today().isoformat()
    return _safe_query(db_path, "SELECT * FROM Games WHERE game_date = ?", (today,))


def _read_all_games(db_path: str) -> list:
    """Read *all* historical games for join/rest-day calculations."""
    return _safe_query(db_path, """
        SELECT game_id, game_date, season, home_team_id, away_team_id,
               home_abbrev, away_abbrev, matchup, home_score, away_score
        FROM Games
        ORDER BY game_date
    """)


def _read_player_stats(db_path: str) -> list:
    """Read recent player game logs enriched with player/team metadata."""
    return _safe_query(db_path, """
        SELECT p.full_name   AS player_name,
               p.position    AS player_position,
               p.team_abbreviation AS player_team_abbrev,
               t.pace        AS team_pace,
               t.ortg        AS team_ortg,
               t.drtg        AS team_drtg,
               g.game_date,
               g.home_team_id,
               g.away_team_id,
               g.home_abbrev,
               g.away_abbrev,
               pgl.*
        FROM Player_Game_Logs pgl
        JOIN Players p  ON pgl.player_id = p.player_id
        LEFT JOIN Teams t ON p.team_id = t.team_id
        LEFT JOIN Games g ON pgl.game_id = g.game_id
        ORDER BY g.game_date DESC
        LIMIT 10000
    """)


def _read_team_game_stats(db_path: str) -> list:
    """Read per-game team stats (pace, ORTG, DRTG)."""
    return _safe_query(db_path, """
        SELECT tgs.*, t.abbreviation AS team_abbrev
        FROM Team_Game_Stats tgs
        LEFT JOIN Teams t ON tgs.team_id = t.team_id
    """)


def _read_defense_vs_position(db_path: str) -> list:
    """Read positional defensive multipliers."""
    return _safe_query(db_path, "SELECT * FROM Defense_Vs_Position")


def _read_box_score_advanced(db_path: str) -> list:
    """Read per-game advanced box score stats."""
    return _safe_query(db_path, """
        SELECT game_id, person_id, usg_pct, ts_pct, ast_pct, reb_pct,
               net_rating, pace, off_rating, def_rating, efg_pct,
               tov_ratio, possessions, pie
        FROM Box_Score_Advanced
    """)


def _read_box_score_usage(db_path: str) -> list:
    """Read per-game usage/share box score stats."""
    return _safe_query(db_path, """
        SELECT game_id, person_id, usg_pct, pct_fgm, pct_fga,
               pct_pts, pct_ast, pct_reb, pct_tov, pct_stl, pct_blk
        FROM Box_Score_Usage
    """)


def _read_standings(db_path: str) -> list:
    """Read current standings for team-strength context."""
    return _safe_query(db_path, """
        SELECT team_id, wins, losses, win_pct, current_streak,
               diff_points_pg, points_pg, opp_points_pg, l10
        FROM Standings
    """)


def _read_teams(db_path: str) -> list:
    """Read team metadata (pace, ortg, drtg)."""
    return _safe_query(db_path, "SELECT * FROM Teams")


# ── Pipeline entry point ──────────────────────────────────────────────────

def run(context: dict) -> dict:
    """Fetch raw data from smartpicks.db and save to data/raw/.

    Args:
        context: Pipeline context dict with at least ``date_str``.

    Returns:
        Updated context with ``raw_data`` key.
    """
    os.makedirs(_RAW_DIR, exist_ok=True)
    date_str = context.get("date_str", datetime.date.today().isoformat())
    raw_data = {}

    # Core tables
    raw_data["todays_games"] = _read_todays_games(_DB_PATH)
    raw_data["all_games"] = _read_all_games(_DB_PATH)
    raw_data["player_stats"] = _read_player_stats(_DB_PATH)

    # Enrichment tables (used by step_3_features)
    raw_data["team_game_stats"] = _read_team_game_stats(_DB_PATH)
    raw_data["defense_vs_position"] = _read_defense_vs_position(_DB_PATH)
    raw_data["box_score_advanced"] = _read_box_score_advanced(_DB_PATH)
    raw_data["box_score_usage"] = _read_box_score_usage(_DB_PATH)
    raw_data["standings"] = _read_standings(_DB_PATH)
    raw_data["teams"] = _read_teams(_DB_PATH)

    for key, rows in raw_data.items():
        _logger.info("Ingested %d rows for %s", len(rows), key)

    # Persist raw data
    try:
        from utils.parquet_helpers import save_parquet
        import pandas as pd
        for key, rows in raw_data.items():
            if rows:
                timestamp = datetime.datetime.now(datetime.timezone.utc).strftime(
                    "%Y%m%dT%H%M%S"
                )
                path = os.path.join(
                    _RAW_DIR, f"{key}_{date_str}_{timestamp}.parquet"
                )
                df = pd.DataFrame(rows) if isinstance(rows, list) else rows
                save_parquet(df, path)
                _logger.debug("Saved raw %s → %s", key, path)
    except Exception as exc:
        _logger.debug("Could not persist raw data: %s", exc)

    context["raw_data"] = raw_data
    return context
