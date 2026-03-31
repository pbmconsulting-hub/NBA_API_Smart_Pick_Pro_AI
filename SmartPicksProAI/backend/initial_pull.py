"""
initial_pull.py
---------------
One-time seed script for the SmartPicksProAI database.

Fetches every player game log for the entire 2025-26 NBA regular season via
the nba_api LeagueGameLog endpoint, cleans and transforms the data with
Pandas, and loads it into the SQLite tables defined in setup_db.py.

Also seeds the Teams table from static data, populates Team_Game_Stats from
team-level game logs, and loads Team_Roster / player positions via the
CommonTeamRoster endpoint.

Run this script exactly once to establish the historical baseline:
    python initial_pull.py
"""

import logging
import sqlite3
import time

import pandas as pd
from nba_api.stats.endpoints import CommonTeamRoster, LeagueGameLog
from nba_api.stats.static import teams as static_teams

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

# Conference and division lookup keyed by team abbreviation.
_TEAM_CONFERENCE_DIVISION: dict[str, tuple[str, str]] = {
    "ATL": ("East", "Southeast"), "BOS": ("East", "Atlantic"),
    "BKN": ("East", "Atlantic"),  "CHA": ("East", "Southeast"),
    "CHI": ("East", "Central"),   "CLE": ("East", "Central"),
    "DAL": ("West", "Southwest"), "DEN": ("West", "Northwest"),
    "DET": ("East", "Central"),   "GSW": ("West", "Pacific"),
    "HOU": ("West", "Southwest"), "IND": ("East", "Central"),
    "LAC": ("West", "Pacific"),   "LAL": ("West", "Pacific"),
    "MEM": ("West", "Southwest"), "MIA": ("East", "Southeast"),
    "MIL": ("East", "Central"),   "MIN": ("West", "Northwest"),
    "NOP": ("West", "Southwest"), "NYK": ("East", "Atlantic"),
    "OKC": ("West", "Northwest"), "ORL": ("East", "Southeast"),
    "PHI": ("East", "Atlantic"),  "PHX": ("West", "Pacific"),
    "POR": ("West", "Northwest"), "SAC": ("West", "Pacific"),
    "SAS": ("West", "Southwest"), "TOR": ("East", "Atlantic"),
    "UTA": ("West", "Northwest"), "WAS": ("East", "Southeast"),
}


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


def fetch_team_season_logs(season: str = SEASON) -> pd.DataFrame:
    """Fetch all **team-level** game logs for *season* from the NBA API.

    Uses LeagueGameLog with ``player_or_team_abbreviation='T'`` to retrieve
    per-team box scores.  Returns two rows per game (one for each team).

    The returned DataFrame contains columns::

        SEASON_ID, TEAM_ID, TEAM_ABBREVIATION, TEAM_NAME, GAME_ID,
        GAME_DATE, MATCHUP, WL, MIN, FGM, FGA, FG_PCT, … PTS, PLUS_MINUS

    Args:
        season: NBA season string, e.g. ``'2025-26'``.

    Returns:
        DataFrame of raw team-level game logs.
    """
    logger.info("Fetching team game logs for season %s …", season)
    time.sleep(2)  # Respect NBA API rate limits.
    endpoint = LeagueGameLog(
        player_or_team_abbreviation="T",
        season=season,
        season_type_all_star="Regular Season",
    )
    df = endpoint.get_data_frames()[0]
    logger.info("Retrieved %d team-level rows from the API.", len(df))
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
    players = raw_subset.drop_duplicates(subset="PLAYER_ID").copy()

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

    ``home_team_id`` and ``away_team_id`` are derived from the MATCHUP and
    TEAM_ID columns in the raw data: a ``vs.`` matchup means the player's
    team is the home team, and an ``@`` matchup means the player's team is
    the away team.

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

    # Normalise the matchup column to always use the home team's perspective
    # ("{HOME} vs. {AWAY}") so the value is deterministic regardless of which
    # raw row was kept by drop_duplicates above.
    if not games.empty:
        games["matchup"] = games["home_abbrev"] + " vs. " + games["away_abbrev"]

    # Derive home_team_id / away_team_id from the raw per-player rows.
    # A "vs." matchup means the row's TEAM_ID is the home team.
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

    logger.info("Built Player_Game_Logs DataFrame: %d rows.", len(logs))
    return logs


def build_team_game_stats_df(raw_team: pd.DataFrame) -> pd.DataFrame:
    """Build the Team_Game_Stats table from team-level LeagueGameLog data.

    Each game produces **two rows** (one per participating team).  For each
    row the function determines:

    - ``is_home`` — ``1`` if the team's MATCHUP contains ``' vs. '``.
    - ``opponent_team_id`` — the other team's TEAM_ID in the same game.
    - ``points_allowed`` — the opponent's PTS in the same game.

    Also computes per-game estimates for ``pace_est``, ``ortg_est``, and
    ``drtg_est`` from the available box-score data::

        possessions ≈ FGA + 0.44 × FTA − OREB + TOV
        pace        = 48 × avg(poss, opp_poss) / (team_minutes / 5)
        ortg        = 100 × PTS / poss
        drtg        = 100 × opp_PTS / opp_poss

    Args:
        raw_team: Raw DataFrame returned by :func:`fetch_team_season_logs`.

    Returns:
        DataFrame with columns ``game_id``, ``team_id``, ``opponent_team_id``,
        ``is_home``, ``points_scored``, ``points_allowed``, ``pace_est``,
        ``ortg_est``, ``drtg_est``.
    """
    if raw_team.empty:
        return pd.DataFrame(
            columns=["game_id", "team_id", "opponent_team_id", "is_home",
                     "points_scored", "points_allowed", "pace_est",
                     "ortg_est", "drtg_est"]
        )

    needed = ["GAME_ID", "TEAM_ID", "MATCHUP", "PTS", "FGA", "FTA", "OREB", "TOV", "MIN"]
    df = raw_team[needed].copy()
    df = df.rename(columns={
        "GAME_ID": "game_id",
        "TEAM_ID": "team_id",
        "PTS": "points_scored",
        "FGA": "fga",
        "FTA": "fta",
        "OREB": "oreb",
        "TOV": "tov",
        "MIN": "min_played",
    })
    df["is_home"] = df["MATCHUP"].str.contains(" vs. ", na=False).astype(int)
    df = df.drop(columns=["MATCHUP"])

    # Coerce box-score columns to numeric for the pace calculations.
    for col in ("fga", "fta", "oreb", "tov"):
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    # MIN from team logs can be int (240) or string ("240:00").
    min_raw = df["min_played"].astype(str)
    if min_raw.str.contains(":").any():
        min_raw = min_raw.str.split(":").str[0]
    df["min_played"] = pd.to_numeric(min_raw, errors="coerce").fillna(240)

    # Self-join to find opponent's team_id, PTS, and box-score data.
    opp = df[["game_id", "team_id", "points_scored",
              "fga", "fta", "oreb", "tov", "min_played"]].rename(columns={
        "team_id": "opponent_team_id",
        "points_scored": "points_allowed",
        "fga": "opp_fga",
        "fta": "opp_fta",
        "oreb": "opp_oreb",
        "tov": "opp_tov",
        "min_played": "opp_min",
    })
    merged = df.merge(opp, on="game_id")
    merged = merged[merged["team_id"] != merged["opponent_team_id"]]

    # Possession estimates.
    merged["poss"] = (
        merged["fga"] + 0.44 * merged["fta"] - merged["oreb"] + merged["tov"]
    )
    merged["opp_poss"] = (
        merged["opp_fga"] + 0.44 * merged["opp_fta"]
        - merged["opp_oreb"] + merged["opp_tov"]
    )

    # Guard against division by zero.
    safe_poss = merged["poss"].replace(0, float("nan"))
    safe_opp_poss = merged["opp_poss"].replace(0, float("nan"))
    game_min = (merged["min_played"] / 5).replace(0, float("nan"))

    merged["pace_est"] = (
        48 * (merged["poss"] + merged["opp_poss"]) / 2 / game_min
    ).round(1)
    merged["ortg_est"] = (100 * merged["points_scored"] / safe_poss).round(1)
    merged["drtg_est"] = (100 * merged["points_allowed"] / safe_opp_poss).round(1)

    result = merged[
        ["game_id", "team_id", "opponent_team_id", "is_home",
         "points_scored", "points_allowed", "pace_est", "ortg_est", "drtg_est"]
    ]
    logger.info("Built Team_Game_Stats DataFrame: %d rows.", len(result))
    return result


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


def seed_teams_from_api(conn: sqlite3.Connection) -> None:
    """Seed the Teams table from the ``nba_api`` static teams list.

    Uses :func:`nba_api.stats.static.teams.get_teams` to load all 30 NBA
    teams.  Rows whose ``team_id`` already exists in the table are skipped.

    Args:
        conn: Open SQLite connection.
    """
    all_teams = static_teams.get_teams()
    teams = pd.DataFrame(all_teams)
    teams = teams.rename(columns={"id": "team_id", "full_name": "team_name"})

    # Keep only columns that exist in the Teams schema.
    teams = teams[["team_id", "abbreviation", "team_name"]]
    teams["conference"] = teams["abbreviation"].map(
        lambda a: _TEAM_CONFERENCE_DIVISION.get(a, (None, None))[0]
    )
    teams["division"] = teams["abbreviation"].map(
        lambda a: _TEAM_CONFERENCE_DIVISION.get(a, (None, None))[1]
    )
    teams["pace"] = None
    teams["ortg"] = None
    teams["drtg"] = None

    existing = pd.read_sql("SELECT team_id FROM Teams", conn)
    new_rows = teams[~teams["team_id"].isin(existing["team_id"])]
    if new_rows.empty:
        logger.info("Teams table: no new rows to insert.")
        return
    new_rows.to_sql("Teams", conn, if_exists="append", index=False)
    logger.info("Teams table: inserted %d rows from nba_api.", len(new_rows))


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


def load_team_game_stats(
    stats: pd.DataFrame, conn: sqlite3.Connection
) -> None:
    """Append *stats* into Team_Game_Stats, skipping existing (game, team) pairs.

    Args:
        stats: DataFrame produced by :func:`build_team_game_stats_df`.
        conn: Open SQLite connection.
    """
    if stats.empty:
        logger.info("Team_Game_Stats table: no rows to insert.")
        return
    existing = pd.read_sql(
        "SELECT game_id, team_id FROM Team_Game_Stats", conn
    )
    if existing.empty:
        new_rows = stats
    else:
        merged = stats.merge(
            existing, on=["game_id", "team_id"], how="left", indicator=True
        )
        new_rows = stats[merged["_merge"] == "left_only"].copy()

    if new_rows.empty:
        logger.info("Team_Game_Stats table: no new rows to insert.")
        return
    new_rows.to_sql("Team_Game_Stats", conn, if_exists="append", index=False)
    logger.info("Team_Game_Stats table: inserted %d new rows.", len(new_rows))


def populate_game_scores(conn: sqlite3.Connection) -> None:
    """Back-fill ``home_score`` and ``away_score`` in the Games table.

    Uses data already loaded in ``Team_Game_Stats`` to set the scores for
    every game whose ``home_team_id`` and ``away_team_id`` are known and
    whose scores have not yet been populated.

    Args:
        conn: Open SQLite connection.
    """
    updated = conn.execute(
        """
        UPDATE Games SET
            home_score = (
                SELECT tgs.points_scored
                FROM Team_Game_Stats tgs
                WHERE tgs.game_id = Games.game_id
                  AND tgs.team_id = Games.home_team_id
            ),
            away_score = (
                SELECT tgs.points_scored
                FROM Team_Game_Stats tgs
                WHERE tgs.game_id = Games.game_id
                  AND tgs.team_id = Games.away_team_id
            )
        WHERE home_team_id IS NOT NULL
          AND away_team_id IS NOT NULL
          AND home_score IS NULL
        """
    ).rowcount
    if updated:
        logger.info("Games: back-filled scores for %d rows.", updated)
    else:
        logger.info("Games: no scores to back-fill.")


def fetch_and_load_rosters(
    conn: sqlite3.Connection, season: str = SEASON
) -> None:
    """Fetch current rosters for all teams and load into Team_Roster.

    Iterates over every team in the Teams table, calls the
    :class:`~nba_api.stats.endpoints.CommonTeamRoster` endpoint for each,
    and inserts new rows into ``Team_Roster``.

    As a side-effect, updates ``Players.position`` for every player found on
    a roster (the position field is otherwise left as ``None`` by the
    game-log-only pipeline).

    Args:
        conn: Open SQLite connection.
        season: NBA season string, e.g. ``'2025-26'``.
    """
    team_ids = pd.read_sql("SELECT team_id FROM Teams", conn)["team_id"].tolist()
    if not team_ids:
        logger.warning("No teams in DB — cannot fetch rosters.")
        return

    all_roster_rows: list[dict] = []
    position_updates: list[tuple] = []

    for tid in team_ids:
        try:
            time.sleep(2)  # Respect NBA API rate limits.
            roster_ep = CommonTeamRoster(team_id=tid, season=season)
            df = roster_ep.get_data_frames()[0]  # CommonTeamRoster result set
        except Exception as exc:
            logger.warning("Failed to fetch roster for team %d: %s", tid, exc)
            continue

        if df.empty:
            continue

        for _, row in df.iterrows():
            pid = row.get("PLAYER_ID")
            pos = row.get("POSITION", None)
            if pid is None:
                continue
            all_roster_rows.append({
                "team_id": tid,
                "player_id": int(pid),
                "effective_start_date": season,
                "effective_end_date": None,
                "is_two_way": 0,
                "is_g_league": 0,
            })
            if pos:
                position_updates.append((pos, int(pid)))

    # Load Team_Roster rows.
    if all_roster_rows:
        roster_df = pd.DataFrame(all_roster_rows)
        existing = pd.read_sql(
            "SELECT team_id, player_id, effective_start_date FROM Team_Roster",
            conn,
        )
        if existing.empty:
            new_rows = roster_df
        else:
            merged = roster_df.merge(
                existing,
                on=["team_id", "player_id", "effective_start_date"],
                how="left",
                indicator=True,
            )
            new_rows = roster_df[merged["_merge"] == "left_only"].copy()

        if not new_rows.empty:
            new_rows.to_sql("Team_Roster", conn, if_exists="append", index=False)
            logger.info("Team_Roster: inserted %d rows.", len(new_rows))
        else:
            logger.info("Team_Roster: no new rows to insert.")
    else:
        logger.info("Team_Roster: no roster data retrieved.")

    # Update Players.position where known.
    if position_updates:
        cursor = conn.cursor()
        cursor.executemany(
            "UPDATE Players SET position = ? WHERE player_id = ?",
            position_updates,
        )
        logger.info("Players.position: updated %d rows.", len(position_updates))


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------


def run_initial_pull(db_path: str = DB_PATH, season: str = SEASON) -> None:
    """Orchestrate the full initial data pull and database seed.

    1. Ensures the database schema exists (calls :func:`setup_db.create_tables`).
    2. Seeds the Teams table from ``nba_api`` static team data.
    3. Fetches all player game logs for *season*.
    4. Builds and loads the Players, Games, and Player_Game_Logs tables.
    5. Fetches team-level game logs and populates Team_Game_Stats.
    6. Fetches rosters for every team and populates Team_Roster and
       Players.position.

    Args:
        db_path: Path to the SQLite database file.
        season: NBA season string, e.g. ``'2025-26'``.
    """
    logger.info("=== SmartPicksProAI — Initial Data Pull ===")
    setup_db.create_tables(db_path)

    conn = sqlite3.connect(db_path)
    try:
        seed_teams_from_api(conn)
        conn.commit()
    finally:
        conn.close()

    # --- Player game logs ---
    raw = fetch_season_logs(season)

    players_df = build_players_df(raw)
    games_df = build_games_df(raw)
    logs_df = build_logs_df(raw)

    # --- Team game logs ---
    raw_team = fetch_team_season_logs(season)
    team_stats_df = build_team_game_stats_df(raw_team)

    conn = sqlite3.connect(db_path)
    try:
        load_players(players_df, conn)
        load_games(games_df, conn)
        load_logs(logs_df, conn)
        load_team_game_stats(team_stats_df, conn)

        # Back-fill home/away scores from Team_Game_Stats into Games.
        populate_game_scores(conn)
        conn.commit()

        # --- Rosters (rate-limited: 1 req / team) ---
        fetch_and_load_rosters(conn, season)
        conn.commit()

        logger.info("=== Initial pull complete. Database is ready. ===")
    finally:
        conn.close()
        logger.info("Database connection closed.")


if __name__ == "__main__":
    run_initial_pull()
