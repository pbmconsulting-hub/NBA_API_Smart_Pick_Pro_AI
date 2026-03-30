"""
setup_db.py
-----------
Creates the smartpicks.db SQLite database and initialises all tables used by
the SmartPicksProAI data pipeline.

Run this script once before any other pipeline script:
    python setup_db.py

NOTE: The Player_Game_Logs table uses a composite PRIMARY KEY (player_id,
game_id) which replaces the old log_id autoincrement PK.  Because SQLite
does not support ALTER TABLE … ADD PRIMARY KEY, this composite PK only takes
effect on a fresh database.  Existing databases with the old log_id schema
must be re-initialised (delete smartpicks.db and re-run this script followed
by initial_pull.py).
"""

import logging
import sqlite3

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

DB_PATH = "smartpicks.db"

CREATE_PLAYERS = """
CREATE TABLE IF NOT EXISTS Players (
    player_id          INTEGER PRIMARY KEY,
    first_name         TEXT    NOT NULL,
    last_name          TEXT    NOT NULL,
    full_name          TEXT,
    team_id            INTEGER,
    team_abbreviation  TEXT,
    position           TEXT,
    is_active          INTEGER DEFAULT 1
);
"""

CREATE_TEAMS = """
CREATE TABLE IF NOT EXISTS Teams (
    team_id        INTEGER PRIMARY KEY,
    abbreviation   TEXT    NOT NULL,
    team_name      TEXT    NOT NULL,
    conference     TEXT,
    division       TEXT,
    pace           REAL,
    ortg           REAL,
    drtg           REAL
);
"""

CREATE_GAMES = """
CREATE TABLE IF NOT EXISTS Games (
    game_id        TEXT    PRIMARY KEY,
    game_date      TEXT    NOT NULL,
    season         TEXT,
    home_team_id   INTEGER,
    away_team_id   INTEGER,
    home_abbrev    TEXT,
    away_abbrev    TEXT,
    matchup        TEXT
);
"""

# NOTE: The composite PRIMARY KEY (player_id, game_id) only applies to fresh
# databases.  See module docstring for migration instructions.
CREATE_PLAYER_GAME_LOGS = """
CREATE TABLE IF NOT EXISTS Player_Game_Logs (
    player_id   INTEGER NOT NULL REFERENCES Players(player_id),
    game_id     TEXT    NOT NULL REFERENCES Games(game_id),
    min         TEXT,
    pts         INTEGER,
    reb         INTEGER,
    ast         INTEGER,
    stl         INTEGER,
    blk         INTEGER,
    tov         INTEGER,
    fgm         INTEGER,
    fga         INTEGER,
    fg_pct      REAL,
    fg3m        INTEGER,
    fg3a        INTEGER,
    fg3_pct     REAL,
    ftm         INTEGER,
    fta         INTEGER,
    ft_pct      REAL,
    oreb        INTEGER,
    dreb        INTEGER,
    pf          INTEGER,
    plus_minus  REAL,
    PRIMARY KEY (player_id, game_id)
);
"""

CREATE_TEAM_GAME_STATS = """
CREATE TABLE IF NOT EXISTS Team_Game_Stats (
    game_id          TEXT    NOT NULL REFERENCES Games(game_id),
    team_id          INTEGER NOT NULL REFERENCES Teams(team_id),
    opponent_team_id INTEGER,
    is_home          INTEGER,
    points_scored    INTEGER,
    points_allowed   INTEGER,
    pace_est         REAL,
    ortg_est         REAL,
    drtg_est         REAL,
    PRIMARY KEY (game_id, team_id)
);
"""

CREATE_DEFENSE_VS_POSITION = """
CREATE TABLE IF NOT EXISTS Defense_Vs_Position (
    team_abbreviation  TEXT    NOT NULL,
    season             TEXT    NOT NULL,
    pos                TEXT    NOT NULL,
    vs_pts_mult        REAL    DEFAULT 1.0,
    vs_reb_mult        REAL    DEFAULT 1.0,
    vs_ast_mult        REAL    DEFAULT 1.0,
    vs_stl_mult        REAL    DEFAULT 1.0,
    vs_blk_mult        REAL    DEFAULT 1.0,
    vs_3pm_mult        REAL    DEFAULT 1.0,
    PRIMARY KEY (team_abbreviation, season, pos)
);
"""

CREATE_TEAM_ROSTER = """
CREATE TABLE IF NOT EXISTS Team_Roster (
    team_id              INTEGER NOT NULL REFERENCES Teams(team_id),
    player_id            INTEGER NOT NULL REFERENCES Players(player_id),
    effective_start_date TEXT,
    effective_end_date   TEXT,
    is_two_way           INTEGER DEFAULT 0,
    is_g_league          INTEGER DEFAULT 0,
    PRIMARY KEY (team_id, player_id, effective_start_date)
);
"""

CREATE_INJURY_STATUS = """
CREATE TABLE IF NOT EXISTS Injury_Status (
    player_id       INTEGER NOT NULL REFERENCES Players(player_id),
    team_id         INTEGER,
    report_date     TEXT    NOT NULL,
    status          TEXT    NOT NULL,
    reason          TEXT,
    source          TEXT,
    last_updated_ts TEXT,
    PRIMARY KEY (player_id, report_date)
);
"""

# New columns added to Players for existing databases.
_PLAYERS_ALTER = [
    "ALTER TABLE Players ADD COLUMN full_name TEXT",
    "ALTER TABLE Players ADD COLUMN team_abbreviation TEXT",
    "ALTER TABLE Players ADD COLUMN position TEXT",
    "ALTER TABLE Players ADD COLUMN is_active INTEGER DEFAULT 1",
]

# New columns added to Games for existing databases.
_GAMES_ALTER = [
    "ALTER TABLE Games ADD COLUMN season TEXT",
    "ALTER TABLE Games ADD COLUMN home_team_id INTEGER",
    "ALTER TABLE Games ADD COLUMN away_team_id INTEGER",
    "ALTER TABLE Games ADD COLUMN home_abbrev TEXT",
    "ALTER TABLE Games ADD COLUMN away_abbrev TEXT",
]

# New stat columns added to Player_Game_Logs for existing databases.
_LOGS_ALTER = [
    "ALTER TABLE Player_Game_Logs ADD COLUMN fgm INTEGER",
    "ALTER TABLE Player_Game_Logs ADD COLUMN fga INTEGER",
    "ALTER TABLE Player_Game_Logs ADD COLUMN fg_pct REAL",
    "ALTER TABLE Player_Game_Logs ADD COLUMN fg3m INTEGER",
    "ALTER TABLE Player_Game_Logs ADD COLUMN fg3a INTEGER",
    "ALTER TABLE Player_Game_Logs ADD COLUMN fg3_pct REAL",
    "ALTER TABLE Player_Game_Logs ADD COLUMN ftm INTEGER",
    "ALTER TABLE Player_Game_Logs ADD COLUMN fta INTEGER",
    "ALTER TABLE Player_Game_Logs ADD COLUMN ft_pct REAL",
    "ALTER TABLE Player_Game_Logs ADD COLUMN oreb INTEGER",
    "ALTER TABLE Player_Game_Logs ADD COLUMN dreb INTEGER",
    "ALTER TABLE Player_Game_Logs ADD COLUMN pf INTEGER",
    "ALTER TABLE Player_Game_Logs ADD COLUMN plus_minus REAL",
]


def create_tables(db_path: str = DB_PATH) -> None:
    """Create all SmartPicksProAI tables in *db_path*.

    Uses IF NOT EXISTS clauses so it is safe to call multiple times without
    overwriting existing data.  For existing databases that pre-date this
    schema version, ALTER TABLE … ADD COLUMN IF NOT EXISTS statements add the
    new columns to Players, Games, and Player_Game_Logs automatically.

    Args:
        db_path: Path to the SQLite database file.  Created automatically if
                 it does not already exist.
    """
    logger.info("Connecting to database: %s", db_path)
    conn = sqlite3.connect(db_path)
    try:
        cursor = conn.cursor()

        logger.info("Creating table: Players")
        cursor.execute(CREATE_PLAYERS)
        logger.info("Creating table: Teams")
        cursor.execute(CREATE_TEAMS)
        logger.info("Creating table: Games")
        cursor.execute(CREATE_GAMES)
        logger.info("Creating table: Player_Game_Logs")
        cursor.execute(CREATE_PLAYER_GAME_LOGS)
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_pgl_player_date "
            "ON Player_Game_Logs (player_id, game_id)"
        )
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_pgl_game "
            "ON Player_Game_Logs (game_id)"
        )
        logger.info("Creating table: Team_Game_Stats")
        cursor.execute(CREATE_TEAM_GAME_STATS)
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_tgs_team "
            "ON Team_Game_Stats (team_id)"
        )
        logger.info("Creating table: Defense_Vs_Position")
        cursor.execute(CREATE_DEFENSE_VS_POSITION)
        logger.info("Creating table: Team_Roster")
        cursor.execute(CREATE_TEAM_ROSTER)
        logger.info("Creating table: Injury_Status")
        cursor.execute(CREATE_INJURY_STATUS)

        # Migrate existing databases by adding new columns where absent.
        for stmt in _PLAYERS_ALTER + _GAMES_ALTER + _LOGS_ALTER:
            try:
                cursor.execute(stmt)
            except sqlite3.OperationalError:
                # Column already exists — safe to ignore.
                pass

        conn.commit()
        logger.info("All tables created (or already exist) successfully.")
    finally:
        conn.close()
        logger.info("Database connection closed.")


if __name__ == "__main__":
    create_tables()
