"""
scripts/data_updater.py
========================
Incremental daily updater — fetches only game logs that have been added
since the last stored game date in the database.

Run daily (e.g. via cron / GitHub Actions):
    python scripts/data_updater.py

Or call programmatically:
    from scripts.data_updater import run_update
    result = run_update()
"""

from __future__ import annotations

import datetime
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

SEASON = "2025-26"


# ── Helpers ───────────────────────────────────────────────────────────────────


def _parse_minutes(min_str: str) -> str:
    """Normalise minute strings from nba_api."""
    if not min_str:
        return "0:00"
    s = str(min_str).strip()
    try:
        fval = float(s)
        mins = int(fval)
        secs = round((fval - mins) * 60)
        return f"{mins}:{secs:02d}"
    except (ValueError, TypeError):
        pass
    return s if s else "0:00"


def _get_last_game_date(conn: sqlite3.Connection) -> str | None:
    """Return the most recent game_date stored in the Games table, or None."""
    row = conn.execute("SELECT MAX(game_date) FROM Games").fetchone()
    if row and row[0]:
        return str(row[0])
    return None


def run_update(season: str = SEASON, db_path: Path = DB_PATH) -> dict:
    """
    Fetch only game logs newer than the last stored date and insert them.

    Returns
    -------
    dict with keys: new_games, new_logs, new_players
    """
    try:
        from nba_api.stats.endpoints import LeagueGameLog
        import pandas as pd
    except ImportError as exc:
        logger.error("Missing required package: %s", exc)
        raise

    if not db_path.exists():
        logger.error("Database not found at %s — run initial_pull.py first.", db_path)
        return {"new_games": 0, "new_logs": 0, "new_players": 0}

    conn = sqlite3.connect(str(db_path))
    try:
        last_date = _get_last_game_date(conn)
    finally:
        conn.close()

    if last_date is None:
        logger.info("No existing data found — running initial pull instead.")
        from scripts.initial_pull import run_initial_pull
        result = run_initial_pull(season=season, db_path=db_path)
        return {
            "new_games": result["games_inserted"],
            "new_logs": result["logs_inserted"],
            "new_players": result["players_inserted"],
        }

    # We want games from *after* the last stored date up to yesterday
    # (today's games may be in progress and return incomplete stats).
    yesterday = (datetime.date.today() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")

    if last_date >= yesterday:
        logger.info("Database is already up to date (last date: %s).", last_date)
        return {"new_games": 0, "new_logs": 0, "new_players": 0}

    logger.info("Fetching game logs between %s and %s …", last_date, yesterday)
    time.sleep(1.0)

    try:
        raw = LeagueGameLog(
            season=season,
            player_or_team_abbreviation="P",
            date_from_nullable=last_date,
            date_to_nullable=yesterday,
            timeout=60,
        ).get_data_frames()[0]
    except Exception as exc:
        logger.error("LeagueGameLog API call failed: %s", exc)
        return {"new_games": 0, "new_logs": 0, "new_players": 0}

    if raw.empty:
        logger.info("No new game logs found between %s and %s.", last_date, yesterday)
        return {"new_games": 0, "new_logs": 0, "new_players": 0}

    # Filter to only truly new games (strictly after last_date)
    raw = raw[raw["GAME_DATE"] > last_date].copy()
    if raw.empty:
        logger.info("No new games strictly after %s.", last_date)
        return {"new_games": 0, "new_logs": 0, "new_players": 0}

    logger.info("Retrieved %d new rows from the API.", len(raw))

    # ── Prepare DataFrames ────────────────────────────────────────────────────
    # New players (may have debuted since initial pull)
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
    # position is not available from LeagueGameLog — leave NULL on new rows
    players_df["position"] = None

    # New games
    games_df = (
        raw[["GAME_ID", "GAME_DATE", "MATCHUP"]]
        .drop_duplicates(subset=["GAME_ID"])
        .rename(columns={"GAME_ID": "game_id", "GAME_DATE": "game_date", "MATCHUP": "matchup"})
        .copy()
    )

    # New logs — full column set from the API response
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
    logs_df["min"] = logs_df["min"].apply(_parse_minutes)
    for col in ["pts", "reb", "ast", "blk", "stl", "tov",
                "fgm", "fga", "fg3m", "fg3a", "ftm", "fta", "oreb", "dreb", "pf", "plus_minus"]:
        logs_df[col] = logs_df[col].fillna(0).astype(int)
    for col in ["fg_pct", "fg3_pct", "ft_pct"]:
        logs_df[col] = logs_df[col].fillna(0.0).astype(float)

    # ── Write to database ─────────────────────────────────────────────────────
    conn = sqlite3.connect(str(db_path))
    try:
        cur = conn.cursor()

        # Insert or replace players (handle trades — team changes)
        new_players = 0
        for _, row in players_df.iterrows():
            cur.execute(
                """INSERT INTO Players (player_id, first_name, last_name, team_id, team_abbreviation, position)
                   VALUES (?, ?, ?, ?, ?, ?)
                   ON CONFLICT(player_id) DO UPDATE SET
                       team_id = excluded.team_id,
                       team_abbreviation = excluded.team_abbreviation
                """,
                (int(row.player_id), row.first_name, row.last_name,
                 int(row.team_id) if row.team_id else None,
                 row.team_abbreviation, None),
            )
            if cur.rowcount > 0 and cur.lastrowid:
                new_players += 1

        # Insert new games (ignore duplicates)
        new_games = 0
        for _, row in games_df.iterrows():
            cur.execute(
                "INSERT OR IGNORE INTO Games (game_id, game_date, matchup) VALUES (?, ?, ?)",
                (row.game_id, row.game_date, row.matchup),
            )
            if cur.rowcount:
                new_games += 1

        # Insert new logs (ignore duplicates) — full column set
        new_logs = 0
        for _, row in logs_df.iterrows():
            cur.execute(
                """INSERT OR IGNORE INTO Player_Game_Logs
                   (player_id, game_id, pts, reb, ast, blk, stl, tov, min,
                    fgm, fga, fg_pct, fg3m, fg3a, fg3_pct,
                    ftm, fta, ft_pct, oreb, dreb, pf, plus_minus, wl)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?,
                           ?, ?, ?, ?, ?, ?,
                           ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (int(row.player_id), row.game_id,
                 int(row.pts), int(row.reb), int(row.ast),
                 int(row.blk), int(row.stl), int(row.tov), row["min"],
                 int(row.fgm), int(row.fga), float(row.fg_pct),
                 int(row.fg3m), int(row.fg3a), float(row.fg3_pct),
                 int(row.ftm), int(row.fta), float(row.ft_pct),
                 int(row.oreb), int(row.dreb), int(row.pf),
                 int(row.plus_minus), row.wl),
            )
            if cur.rowcount:
                new_logs += 1

        conn.commit()
        logger.info(
            "Update complete — %d new games, %d new logs, %d new/updated players.",
            new_games, new_logs, new_players,
        )
        return {"new_games": new_games, "new_logs": new_logs, "new_players": new_players}
    finally:
        conn.close()


if __name__ == "__main__":
    run_update()
