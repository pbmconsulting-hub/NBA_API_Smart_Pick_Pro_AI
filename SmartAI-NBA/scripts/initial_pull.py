"""
scripts/initial_pull.py
========================
One-time bulk fetch of all 2025-26 season game logs from nba_api.
Populates the Players, Games, and Player_Game_Logs tables in
db/etl_data.db.

Run this once (or whenever you need a full reset):
    python scripts/initial_pull.py

After this completes, use scripts/data_updater.py for daily incremental
updates.
"""

from __future__ import annotations

import logging
import sqlite3
import time
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

# ── Paths ─────────────────────────────────────────────────────────────────────

_REPO_ROOT = Path(__file__).resolve().parent.parent
DB_PATH = _REPO_ROOT / "db" / "etl_data.db"

# Season to pull — adjust as needed
SEASON = "2025-26"

# ── Helpers ───────────────────────────────────────────────────────────────────


def _ensure_schema(conn: sqlite3.Connection) -> None:
    """Create tables if they don't already exist (idempotent)."""
    from scripts.setup_db import _DDL_PLAYERS, _DDL_GAMES, _DDL_PLAYER_GAME_LOGS, _DDL_INDEXES

    cur = conn.cursor()
    for stmt, name in [
        (_DDL_PLAYERS,          "Players"),
        (_DDL_GAMES,            "Games"),
        (_DDL_PLAYER_GAME_LOGS, "Player_Game_Logs"),
    ]:
        logger.info("Creating table: %s", name)
        cur.executescript(stmt)
    for ddl in _DDL_INDEXES:
        cur.executescript(ddl)
    conn.commit()


def _parse_minutes(min_str: str) -> str:
    """Normalise minute strings — keep as-is, fall back to '0:00'."""
    if not min_str:
        return "0:00"
    s = str(min_str).strip()
    # nba_api sometimes returns float strings like '32.0' — convert to 'MM:SS'
    try:
        fval = float(s)
        mins = int(fval)
        secs = round((fval - mins) * 60)
        return f"{mins}:{secs:02d}"
    except (ValueError, TypeError):
        pass
    return s if s else "0:00"


def _safe_int(val, default: int = 0) -> int:
    try:
        return int(val) if val is not None else default
    except (ValueError, TypeError):
        return default


# ── Main pull ─────────────────────────────────────────────────────────────────


def run_initial_pull(season: str = SEASON, db_path: Path = DB_PATH) -> dict:
    """
    Fetch all game logs for *season* and populate the database.

    Returns
    -------
    dict with keys: players_inserted, games_inserted, logs_inserted
    """
    try:
        import pandas as pd
        from nba_api.stats.endpoints import LeagueGameLog
    except ImportError as exc:
        logger.error("Missing required package: %s", exc)
        raise

    # ── Ensure db directory / schema exists ──────────────────────────────────
    db_path.parent.mkdir(parents=True, exist_ok=True)
    logger.info("Connecting to database: %s", db_path)
    conn = sqlite3.connect(str(db_path))
    _ensure_schema(conn)
    conn.close()

    # ── Fetch from nba_api ────────────────────────────────────────────────────
    logger.info("Fetching player game logs for season %s …", season)
    time.sleep(1.0)  # polite pause before hitting stats.nba.com
    raw = LeagueGameLog(
        season=season,
        player_or_team_abbreviation="P",
        timeout=60,
    ).get_data_frames()[0]

    total_rows = len(raw)
    logger.info("Retrieved %d rows from the API.", total_rows)

    if total_rows == 0:
        logger.warning("No rows returned — database not populated.")
        return {"players_inserted": 0, "games_inserted": 0, "logs_inserted": 0}

    # ── Build DataFrames ──────────────────────────────────────────────────────
    # Players
    players_df = (
        raw[["PLAYER_ID", "PLAYER_NAME", "TEAM_ID", "TEAM_ABBREVIATION"]]
        .drop_duplicates(subset=["PLAYER_ID"])
        .copy()
    )
    players_df["first_name"] = players_df["PLAYER_NAME"].str.split(" ", n=1).str[0]
    players_df["last_name"] = players_df["PLAYER_NAME"].str.split(" ", n=1).str.get(1).fillna("")
    players_df = players_df.rename(columns={
        "PLAYER_ID": "player_id",
        "TEAM_ID": "team_id",
        "TEAM_ABBREVIATION": "team_abbreviation",
    })[["player_id", "first_name", "last_name", "team_id", "team_abbreviation"]]
    # position is not available from LeagueGameLog — left NULL for now
    players_df["position"] = None
    logger.info("Built Players DataFrame: %d unique players.", len(players_df))

    # Games
    games_df = (
        raw[["GAME_ID", "GAME_DATE", "MATCHUP"]]
        .drop_duplicates(subset=["GAME_ID"])
        .rename(columns={"GAME_ID": "game_id", "GAME_DATE": "game_date", "MATCHUP": "matchup"})
        .copy()
    )
    logger.info("Built Games DataFrame: %d unique games.", len(games_df))

    # Player_Game_Logs
    logs_df = raw.rename(columns={
        "PLAYER_ID":  "player_id",
        "GAME_ID":    "game_id",
        "PTS":        "pts",
        "REB":        "reb",
        "AST":        "ast",
        "BLK":        "blk",
        "STL":        "stl",
        "TOV":        "tov",
        "MIN":        "min",
        "FGM":        "fgm",
        "FGA":        "fga",
        "FG_PCT":     "fg_pct",
        "FG3M":       "fg3m",
        "FG3A":       "fg3a",
        "FG3_PCT":    "fg3_pct",
        "FTM":        "ftm",
        "FTA":        "fta",
        "FT_PCT":     "ft_pct",
        "OREB":       "oreb",
        "DREB":       "dreb",
        "PF":         "pf",
        "PLUS_MINUS": "plus_minus",
        "WL":         "wl",
    })[["player_id", "game_id", "pts", "reb", "ast", "blk", "stl", "tov", "min",
        "fgm", "fga", "fg_pct", "fg3m", "fg3a", "fg3_pct",
        "ftm", "fta", "ft_pct", "oreb", "dreb", "pf", "plus_minus", "wl"]].copy()

    # Normalise minute strings
    logs_df["min"] = logs_df["min"].apply(_parse_minutes)

    # Fill NaN integers with 0
    for col in ["pts", "reb", "ast", "blk", "stl", "tov",
                "fgm", "fga", "fg3m", "fg3a", "ftm", "fta", "oreb", "dreb", "pf", "plus_minus"]:
        logs_df[col] = logs_df[col].fillna(0).astype(int)

    # Fill NaN floats with 0.0
    for col in ["fg_pct", "fg3_pct", "ft_pct"]:
        logs_df[col] = logs_df[col].fillna(0.0).astype(float)

    logger.info("Built Player_Game_Logs DataFrame: %d rows.", len(logs_df))

    # ── Write to database ─────────────────────────────────────────────────────
    conn = sqlite3.connect(str(db_path))
    try:
        cur = conn.cursor()

        # Players — insert or replace so team can be updated
        rows_before = cur.execute("SELECT COUNT(*) FROM Players").fetchone()[0]
        players_df.to_sql("Players", conn, if_exists="replace" if rows_before == 0 else "append",
                          index=False, method="multi")
        # Use INSERT OR IGNORE for append mode to avoid duplicates
        if rows_before > 0:
            # Re-do via explicit upsert
            conn.execute("DELETE FROM Players")
            players_df.to_sql("Players", conn, if_exists="append", index=False, method="multi")
        rows_after = cur.execute("SELECT COUNT(*) FROM Players").fetchone()[0]
        players_inserted = rows_after
        logger.info("Players table: inserted %d new rows.", players_inserted)

        # Games
        rows_before = cur.execute("SELECT COUNT(*) FROM Games").fetchone()[0]
        if rows_before > 0:
            conn.execute("DELETE FROM Games")
        games_df.to_sql("Games", conn, if_exists="append", index=False, method="multi")
        rows_after = cur.execute("SELECT COUNT(*) FROM Games").fetchone()[0]
        games_inserted = rows_after
        logger.info("Games table: inserted %d new rows.", games_inserted)

        # Player_Game_Logs
        rows_before = cur.execute("SELECT COUNT(*) FROM Player_Game_Logs").fetchone()[0]
        if rows_before > 0:
            conn.execute("DELETE FROM Player_Game_Logs")
        logs_df.to_sql("Player_Game_Logs", conn, if_exists="append", index=False, method="multi")
        rows_after = cur.execute("SELECT COUNT(*) FROM Player_Game_Logs").fetchone()[0]
        logs_inserted = rows_after
        logger.info("Player_Game_Logs table: inserted %d new rows.", logs_inserted)

        conn.commit()
        logger.info("=== Initial pull complete. Database is ready. ===")
    finally:
        conn.close()
        logger.info("Database connection closed.")

    return {
        "players_inserted": players_inserted,
        "games_inserted": games_inserted,
        "logs_inserted": logs_inserted,
    }


if __name__ == "__main__":
    run_initial_pull()
