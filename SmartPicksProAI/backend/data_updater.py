"""
data_updater.py
---------------
On-demand incremental update module for the SmartPicksProAI database.

Exposes a single public function, :func:`run_update`, which fetches only the
game logs that have occurred since the last date already stored in the
``Games`` table.  Also refreshes season-level dashboards and advanced box
scores for newly added games.

There are **no scheduling loops, no cron jobs, and no ``while True`` blocks**
— the caller decides when to trigger an update (e.g. via the FastAPI
endpoint in api.py).

Usage::

    from data_updater import run_update
    new_records = run_update()
"""

import logging
import sqlite3
import time
from datetime import date, datetime, timedelta
from typing import Optional

import pandas as pd
from nba_api.stats.endpoints import LeagueGameLog, ScoreboardV3

import initial_pull
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

# Mapping from NBA API column names to Player_Game_Logs DB column names.
STAT_COLS_MAP = {
    "PLAYER_ID": "player_id",
    "GAME_ID":   "game_id",
    "WL":        "wl",
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
# Helpers
# ---------------------------------------------------------------------------


def _get_last_game_date(conn: sqlite3.Connection) -> Optional[date]:
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


def _fetch_team_logs_for_range(date_from: date, date_to: date) -> pd.DataFrame:
    """Fetch **team-level** game logs between *date_from* and *date_to*.

    Mirrors :func:`_fetch_logs_for_range` but with
    ``player_or_team_abbreviation='T'``.

    Args:
        date_from: Start date (inclusive).
        date_to:   End date (inclusive).

    Returns:
        Raw DataFrame of team-level game logs.
    """
    str_from = date_from.strftime(_NBA_DATE_FMT)
    str_to = date_to.strftime(_NBA_DATE_FMT)
    logger.info(
        "Fetching team logs from %s to %s …", str_from, str_to
    )

    endpoint = LeagueGameLog(
        player_or_team_abbreviation="T",
        season=SEASON,
        season_type_all_star="Regular Season",
        date_from_nullable=str_from,
        date_to_nullable=str_to,
    )
    time.sleep(2)  # Respect NBA API rate limits.
    df = endpoint.get_data_frames()[0]
    logger.info("Team-level API returned %d rows.", len(df))
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
    """Insert or update players from *raw* in the Players table.

    Uses INSERT OR REPLACE so that rows whose ``team_id`` or
    ``team_abbreviation`` has changed (e.g. due to a trade) are updated in
    place.

    Args:
        raw: Raw game-log DataFrame.
        conn: Open SQLite connection.
    """
    raw_subset = raw[["PLAYER_ID", "PLAYER_NAME", "TEAM_ID", "TEAM_ABBREVIATION"]].drop_duplicates("PLAYER_ID").copy()
    name_parts = raw_subset["PLAYER_NAME"].str.split(" ", n=1, expand=True)
    raw_subset["first_name"] = name_parts[0].str.strip()
    raw_subset["last_name"] = name_parts[1].str.strip() if 1 in name_parts.columns else ""
    raw_subset = raw_subset.rename(columns={"PLAYER_ID": "player_id", "TEAM_ID": "team_id"})
    raw_subset["full_name"] = raw_subset["PLAYER_NAME"]
    raw_subset["team_abbreviation"] = raw_subset["TEAM_ABBREVIATION"]
    raw_subset["position"] = None
    raw_subset["is_active"] = 1
    players = raw_subset[
        ["player_id", "first_name", "last_name", "full_name",
         "team_id", "team_abbreviation", "position", "is_active"]
    ]

    cursor = conn.cursor()
    cols = list(players.columns)
    placeholders = ", ".join("?" for _ in cols)
    col_names = ", ".join(cols)
    sql = f"INSERT OR REPLACE INTO Players ({col_names}) VALUES ({placeholders})"
    cursor.executemany(sql, players.itertuples(index=False, name=None))
    if len(players):
        logger.info("Players: upserted %d rows.", len(players))


def _upsert_games(raw: pd.DataFrame, conn: sqlite3.Connection) -> None:
    """Insert any games from *raw* that are not already in the Games table.

    Parses ``home_abbrev`` and ``away_abbrev`` from the MATCHUP string:

    - ``'LAL vs. BOS'`` → home is left abbreviation (``LAL``).
    - ``'LAL @ BOS'``   → home is right abbreviation (``BOS``).

    ``home_team_id`` and ``away_team_id`` are derived from the MATCHUP and
    TEAM_ID columns in the raw data.

    Args:
        raw: Raw game-log DataFrame.
        conn: Open SQLite connection.
    """
    games = raw[["GAME_ID", "GAME_DATE", "MATCHUP"]].drop_duplicates("GAME_ID").copy()
    games["GAME_DATE"] = _parse_game_date(games["GAME_DATE"])
    games = games.rename(
        columns={"GAME_ID": "game_id", "GAME_DATE": "game_date", "MATCHUP": "matchup"}
    )

    games["season"] = SEASON

    def _parse_abbrevs(matchup: str):
        if " vs. " in matchup:
            parts = matchup.split(" vs. ", 1)
            return parts[0].strip(), parts[1].strip()
        if " @ " in matchup:
            parts = matchup.split(" @ ", 1)
            return parts[1].strip(), parts[0].strip()
        return None, None

    if not games.empty:
        home_abbrevs, away_abbrevs = zip(*games["matchup"].map(_parse_abbrevs))
        games["home_abbrev"] = list(home_abbrevs)
        games["away_abbrev"] = list(away_abbrevs)
        # Normalise matchup to always use "{HOME} vs. {AWAY}" format.
        games["matchup"] = games["home_abbrev"] + " vs. " + games["away_abbrev"]
    else:
        games["home_abbrev"] = []
        games["away_abbrev"] = []

    # Derive home_team_id / away_team_id from the raw per-player rows.
    home_ids = (
        raw.loc[raw["MATCHUP"].str.contains(" vs. ", na=False), ["GAME_ID", "TEAM_ID"]]
        .drop_duplicates("GAME_ID")
        .rename(columns={"GAME_ID": "game_id", "TEAM_ID": "home_team_id"})
    )
    away_ids = (
        raw.loc[raw["MATCHUP"].str.contains(" @ ", na=False), ["GAME_ID", "TEAM_ID"]]
        .drop_duplicates("GAME_ID")
        .rename(columns={"GAME_ID": "game_id", "TEAM_ID": "away_team_id"})
    )
    games = games.merge(home_ids, on="game_id", how="left")
    games = games.merge(away_ids, on="game_id", how="left")

    games = games[
        ["game_id", "game_date", "season", "home_team_id", "away_team_id",
         "home_abbrev", "away_abbrev", "matchup"]
    ]

    existing = pd.read_sql("SELECT game_id FROM Games", conn)
    new_rows = games[~games["game_id"].isin(existing["game_id"])]
    if not new_rows.empty:
        new_rows.to_sql("Games", conn, if_exists="append", index=False)
        logger.info("Games: inserted %d new rows.", len(new_rows))


def _upsert_logs(raw: pd.DataFrame, conn: sqlite3.Connection) -> int:
    """Insert new player-game log rows that are not already in the database.

    Captures the full set of stat columns defined in :data:`STAT_COLS_MAP`.

    **DNP handling:** Players who did not play (0 minutes, null/None stats)
    have their integer stat columns set to ``0``, float/percentage columns set
    to ``0.0``, and ``min`` set to ``'0:00'`` so downstream ML math never
    encounters NaN values.

    Args:
        raw: Raw game-log DataFrame.
        conn: Open SQLite connection.

    Returns:
        Number of new rows inserted into ``Player_Game_Logs``.
    """
    available_cols = [c for c in STAT_COLS_MAP if c in raw.columns]
    logs = raw[available_cols].copy()
    logs = logs.rename(columns={k: v for k, v in STAT_COLS_MAP.items() if k in available_cols})

    # Deduplicate on the composite PK as a safety net — the NBA API should
    # return exactly one row per player-game, but duplicates have been
    # observed when raw data is merged from multiple partial fetches.
    logs = logs.drop_duplicates(subset=["player_id", "game_id"])

    # --- DNP / inactive edge-case handling ---
    for col in _INT_STAT_COLS:
        if col in logs.columns:
            logs[col] = pd.to_numeric(logs[col], errors="coerce").fillna(0).astype(int)
    for col in _FLOAT_STAT_COLS:
        if col in logs.columns:
            logs[col] = pd.to_numeric(logs[col], errors="coerce").fillna(0.0)
    if "min" in logs.columns:
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


def _upsert_team_game_stats(
    raw_team: pd.DataFrame, conn: sqlite3.Connection
) -> None:
    """Insert new Team_Game_Stats rows from team-level game log data.

    Delegates to :func:`initial_pull.build_team_game_stats_df` and
    :func:`initial_pull.load_team_game_stats` to reuse the shared transform
    and load logic.

    Args:
        raw_team: Raw team-level game-log DataFrame.
        conn: Open SQLite connection.
    """
    if raw_team.empty:
        logger.info("Team_Game_Stats: no team data to process.")
        return
    stats = initial_pull.build_team_game_stats_df(raw_team)
    initial_pull.load_team_game_stats(stats, conn)


# ---------------------------------------------------------------------------
# Today's schedule helper
# ---------------------------------------------------------------------------


def sync_todays_games(conn: sqlite3.Connection) -> int:
    """Fetch today's scheduled games via ``ScoreboardV3`` and insert them.

    Only games whose ``game_id`` is not already in the ``Games`` table are
    inserted.  This ensures that ``GET /api/games/today`` can be answered
    entirely from the database without a live API fallback.

    Args:
        conn: Open SQLite connection (caller is responsible for committing).

    Returns:
        Number of new game rows inserted into the ``Games`` table.
    """
    today_str = date.today().isoformat()
    logger.info("Syncing today's schedule (%s) via ScoreboardV3 …", today_str)

    try:
        scoreboard = ScoreboardV3(game_date=today_str)
        time.sleep(2)  # Respect NBA API rate limits.
        game_header = scoreboard.game_header.get_data_frame()
        line_score = scoreboard.line_score.get_data_frame()
    except Exception:
        logger.exception("Failed to fetch ScoreboardV3 for %s.", today_str)
        return 0

    if game_header.empty:
        logger.info("ScoreboardV3 returned no games for %s.", today_str)
        return 0

    # Pre-convert gameId column to string once to avoid repeated conversions.
    line_score_game_ids = line_score["gameId"].astype(str)

    inserted = 0
    cursor = conn.cursor()
    for _, game_row in game_header.iterrows():
        game_id = str(game_row.get("gameId", ""))
        if not game_id:
            continue

        # Skip if already stored.
        existing = cursor.execute(
            "SELECT 1 FROM Games WHERE game_id = ?", (game_id,)
        ).fetchone()
        if existing:
            continue

        # LineScore has 2 rows per game: away team first, home team second.
        teams = line_score[line_score_game_ids == game_id]
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

        cursor.execute(
            "INSERT INTO Games (game_id, game_date, season, home_team_id, "
            "away_team_id, home_abbrev, away_abbrev, matchup) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (game_id, today_str, SEASON, home_team_id, away_team_id,
             home_tri, away_tri, matchup),
        )
        inserted += 1

    logger.info("Today's schedule: inserted %d new games for %s.", inserted, today_str)
    return inserted


def _refresh_season_dashboards(
    conn: sqlite3.Connection, season: str,
) -> None:
    """Refresh all season-level dashboard tables.

    This is a lightweight operation — each call is a single NBA API request
    that returns data for all players/teams at once.

    Args:
        conn: Open SQLite connection.
        season: NBA season string.
    """
    logger.info("Refreshing season-level dashboard tables …")
    initial_pull.populate_player_clutch_stats(conn, season)
    initial_pull.populate_team_clutch_stats(conn, season)
    initial_pull.populate_player_hustle_stats(conn, season)
    initial_pull.populate_team_hustle_stats(conn, season)
    initial_pull.populate_player_bio(conn, season)
    initial_pull.populate_player_estimated_metrics(conn, season)
    initial_pull.populate_team_estimated_metrics(conn, season)
    initial_pull.populate_league_dash_player_stats(conn, season)
    initial_pull.populate_league_dash_team_stats(conn, season)
    initial_pull.populate_league_leaders(conn, season)
    initial_pull.populate_standings(conn, season)
    logger.info("Season-level dashboard tables refreshed.")


def _get_new_game_ids(
    conn: sqlite3.Connection, date_from: "date", date_to: "date",
) -> list[str]:
    """Return game_ids for completed games in the given date range.

    Args:
        conn: Open SQLite connection.
        date_from: Start date (inclusive).
        date_to: End date (inclusive).

    Returns:
        List of game_id strings.
    """
    rows = conn.execute(
        "SELECT game_id FROM Games "
        "WHERE game_date >= ? AND game_date <= ? AND home_score IS NOT NULL",
        (date_from.isoformat(), date_to.isoformat()),
    ).fetchall()
    return [r[0] for r in rows]


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
    4. De-duplicates and appends new rows to Players, Games,
       Player_Game_Logs, and Team_Game_Stats.
    5. Refreshes season-level dashboards (clutch, hustle, bio, estimated
       metrics, league dash stats, league leaders, standings).
    6. Fetches advanced box scores for any new games.
    7. Returns the total count of new ``Player_Game_Logs`` rows inserted.

    There are **no loops, no scheduling, and no ``while True`` blocks**.
    A single ``time.sleep(2)`` is called inside each fetch helper after
    every API request.

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
            # Still sync today's schedule so the games/today endpoint works.
            sync_todays_games(conn)
            conn.commit()
            # Refresh season dashboards even when no new games (standings etc change).
            _refresh_season_dashboards(conn, SEASON)
            conn.commit()
            return 0

        logger.info(
            "Updating from %s to %s.", date_from.isoformat(), yesterday.isoformat()
        )

        raw = _fetch_logs_for_range(date_from, yesterday)

        if raw.empty:
            logger.info("No new game data found for the requested date range.")
            # Still sync today's schedule so the games/today endpoint works.
            sync_todays_games(conn)
            conn.commit()
            _refresh_season_dashboards(conn, SEASON)
            conn.commit()
            return 0

        _upsert_players(raw, conn)
        _upsert_games(raw, conn)
        new_log_count = _upsert_logs(raw, conn)

        # Also update Team_Game_Stats from team-level logs.
        raw_team = _fetch_team_logs_for_range(date_from, yesterday)
        _upsert_team_game_stats(raw_team, conn)

        # Back-fill home/away scores from Team_Game_Stats into Games.
        initial_pull.populate_game_scores(conn)

        # Refresh season-level pace/ortg/drtg on the Teams table.
        initial_pull.update_team_season_stats(conn)

        # Refresh Defense_Vs_Position multipliers.
        initial_pull.populate_defense_vs_position(conn, SEASON)

        # Pre-populate today's scheduled games so GET /api/games/today
        # can be served entirely from the database.
        sync_todays_games(conn)

        conn.commit()

        # --- Refresh season-level dashboards ---
        _refresh_season_dashboards(conn, SEASON)
        conn.commit()

        # --- Fetch advanced box scores for new games ---
        new_game_ids = _get_new_game_ids(conn, date_from, yesterday)
        if new_game_ids:
            logger.info("Fetching advanced box scores for %d new games.", len(new_game_ids))
            initial_pull.populate_game_advanced_box_scores(conn, SEASON, new_game_ids)
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
