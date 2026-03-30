"""
initial_pull.py
---------------
One-time seed script for the SmartPicksProAI database.

Fetches every player game log for the entire 2025-26 NBA regular season via
the nba_api LeagueGameLog endpoint, cleans and transforms the data with
Pandas, and loads it into the three SQLite tables defined in setup_db.py.

Run this script exactly once to establish the historical baseline:
    python initial_pull.py
"""

import logging
import sqlite3

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


# ---------------------------------------------------------------------------
# Data Extraction
# ---------------------------------------------------------------------------


def fetch_season_logs(season: str = SEASON) -> pd.DataFrame:
    """Fetch all player game logs for *season* from the NBA API.

    Uses LeagueGameLog with player_or_team_abbreviation='P' to retrieve
    per-player box scores.  The returned DataFrame contains columns::

        PLAYER_ID, PLAYER_NAME, SEASON_ID, TEAM_ID, TEAM_ABBREVIATION,
        TEAM_NAME, GAME_ID, GAME_DATE, MATCHUP, WL, MIN, FGM, FGA, FG_PCT,
        FG3M, FG3A, FG3_PCT, FTM, FTA, FT_PCT, OREB, DREB, REB, AST, STL,
        BLK, TOV, PF, PTS, PLUS_MINUS, VIDEO_AVAILABLE

    Args:
        season: NBA season string, e.g. ``'2025-26'``.

    Returns:
        DataFrame of raw player game logs.
    """
    logger.info("Fetching player game logs for season %s …", season)
    endpoint = LeagueGameLog(
        player_or_team_abbreviation="P",
        season=season,
        season_type_all_star="Regular Season",
    )
    df = endpoint.get_data_frames()[0]
    logger.info("Retrieved %d rows from the API.", len(df))
    return df


# ---------------------------------------------------------------------------
# Data Transformation
# ---------------------------------------------------------------------------


def build_players_df(raw: pd.DataFrame) -> pd.DataFrame:
    """Extract a de-duplicated Players table from the raw game-log DataFrame.

    Splits PLAYER_NAME (e.g. ``'LeBron James'``) into ``first_name`` and
    ``last_name`` columns.  When a name contains more than two tokens (e.g.
    ``'Trae Young Jr.'``) the first token becomes ``first_name`` and the
    remainder becomes ``last_name``.

    Args:
        raw: Raw DataFrame returned by :func:`fetch_season_logs`.

    Returns:
        DataFrame with columns ``player_id``, ``first_name``, ``last_name``,
        ``team_id``.
    """
    players = raw[["PLAYER_ID", "PLAYER_NAME", "TEAM_ID"]].copy()
    players = players.drop_duplicates(subset="PLAYER_ID")

    name_parts = players["PLAYER_NAME"].str.split(" ", n=1, expand=True)
    players["first_name"] = name_parts[0].str.strip()
    players["last_name"] = name_parts[1].str.strip() if 1 in name_parts.columns else ""

    players = players.rename(columns={"PLAYER_ID": "player_id", "TEAM_ID": "team_id"})
    players = players[["player_id", "first_name", "last_name", "team_id"]]
    logger.info("Built Players DataFrame: %d unique players.", len(players))
    return players


def build_games_df(raw: pd.DataFrame) -> pd.DataFrame:
    """Extract a de-duplicated Games table from the raw game-log DataFrame.

    Converts GAME_DATE from NBA format (e.g. ``'OCT 22, 2025'``) to ISO
    ``YYYY-MM-DD`` strings.

    Args:
        raw: Raw DataFrame returned by :func:`fetch_season_logs`.

    Returns:
        DataFrame with columns ``game_id``, ``game_date``, ``matchup``.
    """
    games = raw[["GAME_ID", "GAME_DATE", "MATCHUP"]].copy()
    games = games.drop_duplicates(subset="GAME_ID")

    games["GAME_DATE"] = (
        pd.to_datetime(games["GAME_DATE"], format="mixed", dayfirst=False)
        .dt.strftime("%Y-%m-%d")
    )

    games = games.rename(
        columns={"GAME_ID": "game_id", "GAME_DATE": "game_date", "MATCHUP": "matchup"}
    )
    logger.info("Built Games DataFrame: %d unique games.", len(games))
    return games


def build_logs_df(raw: pd.DataFrame) -> pd.DataFrame:
    """Extract the Player_Game_Logs table from the raw game-log DataFrame.

    Selects and renames the columns required by the DB schema:
    ``player_id``, ``game_id``, ``pts``, ``reb``, ``ast``, ``blk``,
    ``stl``, ``tov``, ``min``.

    **DNP handling:** Players who did not play (0 minutes, null/None stats)
    have their numeric stat columns set to ``0`` and ``min`` set to
    ``'0:00'`` so downstream ML math never encounters NaN values.

    Args:
        raw: Raw DataFrame returned by :func:`fetch_season_logs`.

    Returns:
        DataFrame ready for insertion into ``Player_Game_Logs``.
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

    logger.info("Built Player_Game_Logs DataFrame: %d rows.", len(logs))
    return logs


# ---------------------------------------------------------------------------
# Data Loading
# ---------------------------------------------------------------------------


def load_players(players: pd.DataFrame, conn: sqlite3.Connection) -> None:
    """Upsert *players* into the Players table, skipping existing rows.

    Args:
        players: DataFrame produced by :func:`build_players_df`.
        conn: Open SQLite connection.
    """
    existing = pd.read_sql("SELECT player_id FROM Players", conn)
    new_rows = players[~players["player_id"].isin(existing["player_id"])]
    if new_rows.empty:
        logger.info("Players table: no new rows to insert.")
        return
    new_rows.to_sql("Players", conn, if_exists="append", index=False)
    logger.info("Players table: inserted %d new rows.", len(new_rows))


def load_games(games: pd.DataFrame, conn: sqlite3.Connection) -> None:
    """Upsert *games* into the Games table, skipping existing rows.

    Args:
        games: DataFrame produced by :func:`build_games_df`.
        conn: Open SQLite connection.
    """
    existing = pd.read_sql("SELECT game_id FROM Games", conn)
    new_rows = games[~games["game_id"].isin(existing["game_id"])]
    if new_rows.empty:
        logger.info("Games table: no new rows to insert.")
        return
    new_rows.to_sql("Games", conn, if_exists="append", index=False)
    logger.info("Games table: inserted %d new rows.", len(new_rows))


def load_logs(logs: pd.DataFrame, conn: sqlite3.Connection) -> None:
    """Append *logs* into Player_Game_Logs, skipping duplicate (player, game) pairs.

    Args:
        logs: DataFrame produced by :func:`build_logs_df`.
        conn: Open SQLite connection.
    """
    existing = pd.read_sql(
        "SELECT player_id, game_id FROM Player_Game_Logs", conn
    )
    if existing.empty:
        new_rows = logs
    else:
        merged = logs.merge(
            existing, on=["player_id", "game_id"], how="left", indicator=True
        )
        new_rows = logs[merged["_merge"] == "left_only"].copy()

    if new_rows.empty:
        logger.info("Player_Game_Logs table: no new rows to insert.")
        return
    new_rows.to_sql("Player_Game_Logs", conn, if_exists="append", index=False)
    logger.info("Player_Game_Logs table: inserted %d new rows.", len(new_rows))


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------


def run_initial_pull(db_path: str = DB_PATH, season: str = SEASON) -> None:
    """Orchestrate the full initial data pull and database seed.

    1. Ensures the database schema exists (calls :func:`setup_db.create_tables`).
    2. Fetches all player game logs for *season*.
    3. Builds and loads the Players, Games, and Player_Game_Logs tables.

    Args:
        db_path: Path to the SQLite database file.
        season: NBA season string, e.g. ``'2025-26'``.
    """
    logger.info("=== SmartPicksProAI — Initial Data Pull ===")
    setup_db.create_tables(db_path)

    raw = fetch_season_logs(season)

    players_df = build_players_df(raw)
    games_df = build_games_df(raw)
    logs_df = build_logs_df(raw)

    conn = sqlite3.connect(db_path)
    try:
        load_players(players_df, conn)
        load_games(games_df, conn)
        load_logs(logs_df, conn)
        conn.commit()
        logger.info("=== Initial pull complete. Database is ready. ===")
    finally:
        conn.close()
        logger.info("Database connection closed.")


if __name__ == "__main__":
    run_initial_pull()
