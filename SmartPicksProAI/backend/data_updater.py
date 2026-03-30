"""
data_updater.py
---------------
On-demand incremental update module for the SmartPicksProAI database.

Exposes a single public function, :func:`run_update`, which fetches only the
game logs that have occurred since the last date already stored in the
``Games`` table.  There are **no scheduling loops, no cron jobs, and no
``while True`` blocks** — the caller decides when to trigger an update (e.g.
via the FastAPI endpoint in api.py).

Usage::

    from data_updater import run_update
    new_records = run_update()
"""

import logging
import sqlite3
import time
from datetime import date, datetime, timedelta

import pandas as pd
from nba_api.stats.endpoints import LeagueGameLog

import setup_db

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

DB_PATH = setup_db.DB_PATH
SEASON = "2025-26"

# NBA API date format for the date_from_nullable / date_to_nullable params.
_NBA_DATE_FMT = "%m/%d/%Y"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _get_last_game_date(conn: sqlite3.Connection) -> date | None:
    """Return the most recent ``game_date`` stored in the Games table.

    Args:
        conn: Open SQLite connection.

    Returns:
        A :class:`datetime.date` object, or ``None`` if the table is empty.
    """
    row = conn.execute("SELECT MAX(game_date) FROM Games").fetchone()
    if row and row[0]:
        return datetime.strptime(row[0], "%Y-%m-%d").date()
    return None


def _fetch_logs_for_range(date_from: date, date_to: date) -> pd.DataFrame:
    """Fetch player game logs between *date_from* and *date_to* (inclusive).

    Converts dates to the NBA API's expected ``MM/DD/YYYY`` format before
    calling the endpoint.

    Args:
        date_from: Start date (inclusive).
        date_to:   End date (inclusive).

    Returns:
        Raw DataFrame from the LeagueGameLog endpoint, or an empty DataFrame
        if the API returns no data.
    """
    str_from = date_from.strftime(_NBA_DATE_FMT)
    str_to = date_to.strftime(_NBA_DATE_FMT)
    logger.info(
        "Fetching game logs from %s to %s …", str_from, str_to
    )

    endpoint = LeagueGameLog(
        player_or_team_abbreviation="P",
        season=SEASON,
        season_type_all_star="Regular Season",
        date_from_nullable=str_from,
        date_to_nullable=str_to,
    )
    time.sleep(2)  # Respect NBA API rate limits.
    df = endpoint.get_data_frames()[0]
    logger.info("API returned %d rows.", len(df))
    return df


def _parse_game_date(series: pd.Series) -> pd.Series:
    """Convert a Series of NBA-formatted date strings to ``YYYY-MM-DD``.

    Args:
        series: Series containing date strings (e.g. ``'OCT 22, 2025'``).

    Returns:
        Series of ISO-format date strings.
    """
    return (
        pd.to_datetime(series, format="mixed", dayfirst=False)
        .dt.strftime("%Y-%m-%d")
    )


# ---------------------------------------------------------------------------
# Upsert helpers (shared logic with initial_pull.py)
# ---------------------------------------------------------------------------


def _upsert_players(raw: pd.DataFrame, conn: sqlite3.Connection) -> None:
    """Insert any players from *raw* that are not already in the Players table.

    Args:
        raw: Raw game-log DataFrame.
        conn: Open SQLite connection.
    """
    players = raw[["PLAYER_ID", "PLAYER_NAME", "TEAM_ID"]].drop_duplicates("PLAYER_ID").copy()
    name_parts = players["PLAYER_NAME"].str.split(" ", n=1, expand=True)
    players["first_name"] = name_parts[0].str.strip()
    players["last_name"] = name_parts[1].str.strip() if 1 in name_parts.columns else ""
    players = players.rename(columns={"PLAYER_ID": "player_id", "TEAM_ID": "team_id"})
    players = players[["player_id", "first_name", "last_name", "team_id"]]

    existing = pd.read_sql("SELECT player_id FROM Players", conn)
    new_rows = players[~players["player_id"].isin(existing["player_id"])]
    if not new_rows.empty:
        new_rows.to_sql("Players", conn, if_exists="append", index=False)
        logger.info("Players: inserted %d new rows.", len(new_rows))


def _upsert_games(raw: pd.DataFrame, conn: sqlite3.Connection) -> None:
    """Insert any games from *raw* that are not already in the Games table.

    Args:
        raw: Raw game-log DataFrame.
        conn: Open SQLite connection.
    """
    games = raw[["GAME_ID", "GAME_DATE", "MATCHUP"]].drop_duplicates("GAME_ID").copy()
    games["GAME_DATE"] = _parse_game_date(games["GAME_DATE"])
    games = games.rename(
        columns={"GAME_ID": "game_id", "GAME_DATE": "game_date", "MATCHUP": "matchup"}
    )

    existing = pd.read_sql("SELECT game_id FROM Games", conn)
    new_rows = games[~games["game_id"].isin(existing["game_id"])]
    if not new_rows.empty:
        new_rows.to_sql("Games", conn, if_exists="append", index=False)
        logger.info("Games: inserted %d new rows.", len(new_rows))


def _upsert_logs(raw: pd.DataFrame, conn: sqlite3.Connection) -> int:
    """Insert new player-game log rows that are not already in the database.

    **DNP handling:** Players who did not play (0 minutes, null/None stats)
    have their numeric stat columns set to ``0`` and ``min`` set to
    ``'0:00'`` so downstream ML math never encounters NaN values.

    Args:
        raw: Raw game-log DataFrame.
        conn: Open SQLite connection.

    Returns:
        Number of new rows inserted into ``Player_Game_Logs``.
    """
    logs = raw[["PLAYER_ID", "GAME_ID", "PTS", "REB", "AST", "BLK", "STL", "TOV", "MIN"]].copy()
    logs = logs.rename(
        columns={
            "PLAYER_ID": "player_id",
            "GAME_ID": "game_id",
            "PTS": "pts",
            "REB": "reb",
            "AST": "ast",
            "BLK": "blk",
            "STL": "stl",
            "TOV": "tov",
            "MIN": "min",
        }
    )

    # --- DNP / inactive edge-case handling ---
    stat_cols = ["pts", "reb", "ast", "blk", "stl", "tov"]
    for col in stat_cols:
        logs[col] = pd.to_numeric(logs[col], errors="coerce").fillna(0).astype(int)
    logs["min"] = logs["min"].fillna("0:00").replace("", "0:00")

    existing = pd.read_sql("SELECT player_id, game_id FROM Player_Game_Logs", conn)
    if existing.empty:
        new_rows = logs
    else:
        merged = logs.merge(existing, on=["player_id", "game_id"], how="left", indicator=True)
        new_rows = logs[merged["_merge"] == "left_only"].copy()

    if new_rows.empty:
        logger.info("Player_Game_Logs: no new rows to insert.")
        return 0

    new_rows.to_sql("Player_Game_Logs", conn, if_exists="append", index=False)
    logger.info("Player_Game_Logs: inserted %d new rows.", len(new_rows))
    return len(new_rows)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def run_update(db_path: str = DB_PATH) -> int:
    """Fetch and append game logs that have occurred since the last DB update.

    Steps:

    1. Opens the database and queries for the most recent ``game_date`` in
       the ``Games`` table.
    2. If no date is found the database is empty — logs a warning and returns
       0 (run ``initial_pull.py`` first).
    3. Fetches all player game logs between ``last_date + 1 day`` and
       yesterday using the NBA API.
    4. De-duplicates and appends new rows to all three tables.
    5. Returns the total count of new ``Player_Game_Logs`` rows inserted.

    There are **no loops, no scheduling, and no ``while True`` blocks**.
    A single ``time.sleep(2)`` is called inside :func:`_fetch_logs_for_range`
    after each API request.

    Args:
        db_path: Path to the SQLite database file.

    Returns:
        Number of new records added to ``Player_Game_Logs``.
    """
    logger.info("=== SmartPicksProAI — Incremental Update ===")
    setup_db.create_tables(db_path)

    conn = sqlite3.connect(db_path)
    try:
        last_date = _get_last_game_date(conn)
        if last_date is None:
            logger.warning(
                "Games table is empty. Run initial_pull.py first to seed the database."
            )
            return 0

        yesterday = date.today() - timedelta(days=1)
        date_from = last_date + timedelta(days=1)

        if date_from > yesterday:
            logger.info(
                "Database is already up to date (last game date: %s).", last_date
            )
            return 0

        logger.info(
            "Updating from %s to %s.", date_from.isoformat(), yesterday.isoformat()
        )

        raw = _fetch_logs_for_range(date_from, yesterday)

        if raw.empty:
            logger.info("No new game data found for the requested date range.")
            return 0

        _upsert_players(raw, conn)
        _upsert_games(raw, conn)
        new_log_count = _upsert_logs(raw, conn)
        conn.commit()
        logger.info(
            "=== Update complete. %d new log records added. ===", new_log_count
        )
        return new_log_count
    finally:
        conn.close()
        logger.info("Database connection closed.")


if __name__ == "__main__":
    run_update()
