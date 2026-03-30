"""
setup_db.py
-----------
Creates the smartpicks.db SQLite database and initialises the three core
tables used by the SmartPicksProAI data pipeline.

Run this script once before any other pipeline script:
    python setup_db.py
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
    player_id   INTEGER PRIMARY KEY,
    first_name  TEXT    NOT NULL,
    last_name   TEXT    NOT NULL,
    team_id     INTEGER
);
"""

CREATE_GAMES = """
CREATE TABLE IF NOT EXISTS Games (
    game_id    TEXT PRIMARY KEY,
    game_date  TEXT NOT NULL,
    matchup    TEXT
);
"""

CREATE_PLAYER_GAME_LOGS = """
CREATE TABLE IF NOT EXISTS Player_Game_Logs (
    log_id    INTEGER PRIMARY KEY AUTOINCREMENT,
    player_id INTEGER NOT NULL REFERENCES Players(player_id),
    game_id   TEXT    NOT NULL REFERENCES Games(game_id),
    pts       INTEGER,
    reb       INTEGER,
    ast       INTEGER,
    blk       INTEGER,
    stl       INTEGER,
    tov       INTEGER,
    min       TEXT
);
"""


def create_tables(db_path: str = DB_PATH) -> None:
    """Create the Players, Games, and Player_Game_Logs tables in *db_path*.

    Uses IF NOT EXISTS clauses so it is safe to call multiple times without
    overwriting existing data.

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
        logger.info("Creating table: Games")
        cursor.execute(CREATE_GAMES)
        logger.info("Creating table: Player_Game_Logs")
        cursor.execute(CREATE_PLAYER_GAME_LOGS)
        conn.commit()
        logger.info("All tables created (or already exist) successfully.")
    finally:
        conn.close()
        logger.info("Database connection closed.")


if __name__ == "__main__":
    create_tables()
