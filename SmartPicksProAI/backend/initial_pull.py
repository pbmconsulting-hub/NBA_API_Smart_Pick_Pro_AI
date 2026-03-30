"""
initial_pull.py
---------------
One-time seed script for the SmartPicksProAI database.

Fetches every player game log for the entire 2025-26 NBA regular season via
the nba_api LeagueGameLog endpoint, cleans and transforms the data with
Pandas, and loads it into the SQLite tables defined in setup_db.py.

Run this script exactly once to establish the historical baseline:
    python initial_pull.py
"""

import logging
import os
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

# Mapping from NBA API column names to Player_Game_Logs DB column names.
STAT_COLS_MAP = {
    "PLAYER_ID": "player_id",
    "GAME_ID":   "game_id",
    "MIN":       "min",
    "PTS":       "pts",
    "REB":       "reb",
    "AST":       "ast",
    "STL":       "stl",
    "BLK":       "blk",
    "TOV":       "tov",
    "FGM":       "fgm",
    "FGA":       "fga",
    "FG_PCT":    "fg_pct",
    "FG3M":      "fg3m",
    "FG3A":      "fg3a",
    "FG3_PCT":   "fg3_pct",
    "FTM":       "ftm",
    "FTA":       "fta",
    "FT_PCT":    "ft_pct",
    "OREB":      "oreb",
    "DREB":      "dreb",
    "PF":        "pf",
    "PLUS_MINUS": "plus_minus",
}

# Integer stat columns (filled with 0 for DNP players).
_INT_STAT_COLS = [
    "pts", "reb", "ast", "stl", "blk", "tov",
    "fgm", "fga", "fg3m", "fg3a", "ftm", "fta",
    "oreb", "dreb", "pf",
]

# Float/percentage stat columns (filled with 0.0 for DNP players).
_FLOAT_STAT_COLS = ["fg_pct", "fg3_pct", "ft_pct", "plus_minus"]


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

    Also captures ``full_name`` and ``team_abbreviation`` from the raw data.
    ``position`` is set to ``None`` and ``is_active`` to ``1`` as placeholders
    (populate separately via CommonPlayerInfo or a roster endpoint).

    Args:
        raw: Raw DataFrame returned by :func:`fetch_season_logs`.

    Returns:
        DataFrame with columns ``player_id``, ``first_name``, ``last_name``,
        ``full_name``, ``team_id``, ``team_abbreviation``, ``position``,
        ``is_active``.
    """
    raw_subset = raw[["PLAYER_ID", "PLAYER_NAME", "TEAM_ID", "TEAM_ABBREVIATION"]].copy()
    players = raw_subset.drop_duplicates(subset="PLAYER_ID")

    name_parts = players["PLAYER_NAME"].str.split(" ", n=1, expand=True)
    players["first_name"] = name_parts[0].str.strip()
    players["last_name"] = name_parts[1].str.strip() if 1 in name_parts.columns else ""

    players = players.rename(columns={"PLAYER_ID": "player_id", "TEAM_ID": "team_id"})
    players["full_name"] = players["PLAYER_NAME"]
    players["team_abbreviation"] = players["TEAM_ABBREVIATION"]
    players["position"] = None
    players["is_active"] = 1

    players = players[
        ["player_id", "first_name", "last_name", "full_name",
         "team_id", "team_abbreviation", "position", "is_active"]
    ]
    logger.info("Built Players DataFrame: %d unique players.", len(players))
    return players


def build_games_df(raw: pd.DataFrame) -> pd.DataFrame:
    """Extract a de-duplicated Games table from the raw game-log DataFrame.

    Converts GAME_DATE from NBA format (e.g. ``'OCT 22, 2025'``) to ISO
    ``YYYY-MM-DD`` strings.  Parses ``home_abbrev`` and ``away_abbrev`` from
    the MATCHUP string:

    - ``'LAL vs. BOS'`` → home team is the left abbreviation (``LAL``).
    - ``'LAL @ BOS'``   → home team is the right abbreviation (``BOS``).

    ``home_team_id`` and ``away_team_id`` are left as ``None`` for now (team
    IDs require a separate lookup); populate via :func:`seed_teams_from_csv`.

    Args:
        raw: Raw DataFrame returned by :func:`fetch_season_logs`.

    Returns:
        DataFrame with columns ``game_id``, ``game_date``, ``season``,
        ``home_team_id``, ``away_team_id``, ``home_abbrev``, ``away_abbrev``,
        ``matchup``.
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

    games["season"] = SEASON
    games["home_team_id"] = None
    games["away_team_id"] = None

    def _parse_abbrevs(matchup: str):
        if " vs. " in matchup:
            parts = matchup.split(" vs. ", 1)
            return parts[0].strip(), parts[1].strip()
        if " @ " in matchup:
            parts = matchup.split(" @ ", 1)
            # left team is away, right team is home
            return parts[1].strip(), parts[0].strip()
        return None, None

    home_abbrevs, away_abbrevs = zip(
        *games["matchup"].map(_parse_abbrevs)
    ) if not games.empty else ([], [])
    games["home_abbrev"] = list(home_abbrevs)
    games["away_abbrev"] = list(away_abbrevs)

    games = games[
        ["game_id", "game_date", "season", "home_team_id", "away_team_id",
         "home_abbrev", "away_abbrev", "matchup"]
    ]
    logger.info("Built Games DataFrame: %d unique games.", len(games))
    return games


def build_logs_df(raw: pd.DataFrame) -> pd.DataFrame:
    """Extract the Player_Game_Logs table from the raw game-log DataFrame.

    Selects and renames the full set of stat columns defined in
    :data:`STAT_COLS_MAP`.

    **DNP handling:** Players who did not play (0 minutes, null/None stats)
    have their integer stat columns set to ``0``, float/percentage columns set
    to ``0.0``, and ``min`` set to ``'0:00'`` so downstream ML math never
    encounters NaN values.

    Args:
        raw: Raw DataFrame returned by :func:`fetch_season_logs`.

    Returns:
        DataFrame ready for insertion into ``Player_Game_Logs``.
    """
    available_cols = [c for c in STAT_COLS_MAP if c in raw.columns]
    logs = raw[available_cols].copy()
    logs = logs.rename(columns={k: v for k, v in STAT_COLS_MAP.items() if k in available_cols})

    # --- DNP / inactive edge-case handling ---
    for col in _INT_STAT_COLS:
        if col in logs.columns:
            logs[col] = pd.to_numeric(logs[col], errors="coerce").fillna(0).astype(int)
    for col in _FLOAT_STAT_COLS:
        if col in logs.columns:
            logs[col] = pd.to_numeric(logs[col], errors="coerce").fillna(0.0)
    if "min" in logs.columns:
        logs["min"] = logs["min"].fillna("0:00").replace("", "0:00")

    logger.info("Built Player_Game_Logs DataFrame: %d rows.", len(logs))
    return logs


# ---------------------------------------------------------------------------
# Data Loading
# ---------------------------------------------------------------------------


def load_players(players: pd.DataFrame, conn: sqlite3.Connection) -> None:
    """Upsert *players* into the Players table.

    Uses INSERT OR REPLACE so that rows whose ``team_id`` or
    ``team_abbreviation`` has changed (e.g. due to a trade) are updated in
    place rather than silently skipped.

    Args:
        players: DataFrame produced by :func:`build_players_df`.
        conn: Open SQLite connection.
    """
    if players.empty:
        logger.info("Players table: no rows to upsert.")
        return
    cursor = conn.cursor()
    cols = list(players.columns)
    placeholders = ", ".join("?" for _ in cols)
    col_names = ", ".join(cols)
    sql = f"INSERT OR REPLACE INTO Players ({col_names}) VALUES ({placeholders})"
    cursor.executemany(sql, players.itertuples(index=False, name=None))
    logger.info("Players table: upserted %d rows.", len(players))


def seed_teams_from_csv(
    conn: sqlite3.Connection,
    csv_path: str = os.path.join(
        os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
        "data", "teams.csv",
    ),
) -> None:
    """Seed the Teams table from a CSV file.

    Reads *csv_path* and inserts any rows whose ``team_id`` is not already
    present in the Teams table.  Expected CSV columns (at minimum):
    ``team_id``, ``abbreviation``, ``team_name``.  Optional columns:
    ``conference``, ``division``, ``pace``, ``ortg``, ``drtg``.

    Args:
        conn:     Open SQLite connection.
        csv_path: Path to the teams CSV file.  Defaults to
                  ``<repo_root>/data/teams.csv``.
    """
    if not os.path.isfile(csv_path):
        logger.warning(
            "Teams CSV not found at %s — skipping Teams seed.", csv_path
        )
        return

    teams = pd.read_csv(csv_path)
    # Normalise column names to lower-case to be resilient to CSV variations.
    teams.columns = [c.lower() for c in teams.columns]

    required = {"team_id", "abbreviation", "team_name"}
    missing = required - set(teams.columns)
    if missing:
        logger.warning(
            "Teams CSV is missing required columns %s — skipping Teams seed.",
            missing,
        )
        return

    # Fill optional columns with None if absent.
    for col in ("conference", "division", "pace", "ortg", "drtg"):
        if col not in teams.columns:
            teams[col] = None

    teams = teams[["team_id", "abbreviation", "team_name",
                   "conference", "division", "pace", "ortg", "drtg"]]

    existing = pd.read_sql("SELECT team_id FROM Teams", conn)
    new_rows = teams[~teams["team_id"].isin(existing["team_id"])]
    if new_rows.empty:
        logger.info("Teams table: no new rows to insert.")
        return
    new_rows.to_sql("Teams", conn, if_exists="append", index=False)
    logger.info("Teams table: inserted %d rows from CSV.", len(new_rows))


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
    2. Seeds the Teams table from ``data/teams.csv`` (if present).
    3. Fetches all player game logs for *season*.
    4. Builds and loads the Players, Games, and Player_Game_Logs tables.

    Args:
        db_path: Path to the SQLite database file.
        season: NBA season string, e.g. ``'2025-26'``.
    """
    logger.info("=== SmartPicksProAI — Initial Data Pull ===")
    setup_db.create_tables(db_path)

    conn = sqlite3.connect(db_path)
    try:
        seed_teams_from_csv(conn)
        conn.commit()
    finally:
        conn.close()

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
