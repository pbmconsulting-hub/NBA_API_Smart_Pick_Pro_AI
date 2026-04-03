# ============================================================
# FILE: tracking/database.py
# PURPOSE: SQLite database wrapper for storing bet history
#          and tracking model performance over time.
# ============================================================

import sqlite3
import json
import os
import time
import datetime
import logging
from pathlib import Path

_logger = logging.getLogger(__name__)

# Database location
DB_DIRECTORY = Path(__file__).resolve().parent.parent / "db"
DB_FILE_PATH = DB_DIRECTORY / "smartpicks_tracker.db"

# Retry config for concurrent writes
_WRITE_RETRY_ATTEMPTS = 3
_WRITE_RETRY_DELAY = 0.25  # seconds


def _get_connection() -> sqlite3.Connection:
    """Get a SQLite connection with WAL mode enabled."""
    DB_DIRECTORY.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(DB_FILE_PATH), check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.row_factory = sqlite3.Row
    return conn


def _execute_write(sql: str, params: tuple = (), *, caller: str = "write"):
    """Execute a single INSERT/UPDATE with locked-database retry."""
    for attempt in range(_WRITE_RETRY_ATTEMPTS):
        try:
            conn = _get_connection()
            cursor = conn.cursor()
            cursor.execute(sql, params)
            conn.commit()
            conn.close()
            return cursor
        except sqlite3.OperationalError as err:
            if "locked" in str(err).lower() and attempt < _WRITE_RETRY_ATTEMPTS - 1:
                _logger.warning(
                    "%s: database locked, retry %d/%d",
                    caller, attempt + 1, _WRITE_RETRY_ATTEMPTS,
                )
                time.sleep(_WRITE_RETRY_DELAY * (2 ** attempt))
                continue
            _logger.error("%s error: %s", caller, err)
            return None
        except sqlite3.Error as err:
            _logger.error("%s error: %s", caller, err)
            return None
    return None


# ── Schema ──────────────────────────────────────────────────

CREATE_BETS_TABLE = """
CREATE TABLE IF NOT EXISTS bets (
    bet_id          INTEGER PRIMARY KEY AUTOINCREMENT,
    bet_date        TEXT NOT NULL,
    player_name     TEXT NOT NULL,
    team            TEXT,
    stat_type       TEXT NOT NULL,
    prop_line       REAL NOT NULL,
    direction       TEXT NOT NULL,
    platform        TEXT DEFAULT 'PrizePicks',
    confidence_score REAL,
    confidence_tier TEXT,
    model_probability REAL,
    edge_pct        REAL,
    kelly_fraction  REAL,
    recommended_bet REAL,
    opponent        TEXT,
    player_id       INTEGER,
    result          TEXT,
    actual_value    REAL,
    notes           TEXT,
    source          TEXT DEFAULT 'manual',
    created_at      TEXT DEFAULT (datetime('now'))  -- UTC
)"""

CREATE_DAILY_SNAPSHOTS_TABLE = """
CREATE TABLE IF NOT EXISTS daily_snapshots (
    snapshot_id INTEGER PRIMARY KEY AUTOINCREMENT,
    snapshot_date TEXT NOT NULL UNIQUE,
    total_bets INTEGER DEFAULT 0,
    wins INTEGER DEFAULT 0,
    losses INTEGER DEFAULT 0,
    pushes INTEGER DEFAULT 0,
    pending INTEGER DEFAULT 0,
    win_rate REAL DEFAULT 0.0,
    roi REAL DEFAULT 0.0,
    created_at TEXT DEFAULT (datetime('now'))  -- UTC
)"""

CREATE_ANALYSIS_PICKS_TABLE = """
CREATE TABLE IF NOT EXISTS analysis_picks (
    pick_id INTEGER PRIMARY KEY AUTOINCREMENT,
    analysis_date TEXT NOT NULL,
    player_name TEXT NOT NULL,
    player_id INTEGER,
    team TEXT,
    opponent TEXT,
    stat_type TEXT NOT NULL,
    prop_line REAL NOT NULL,
    direction TEXT NOT NULL,
    model_probability REAL,
    edge_pct REAL,
    confidence_score REAL,
    confidence_tier TEXT,
    kelly_fraction REAL,
    recommended_bet REAL,
    explanation TEXT,
    result TEXT,
    actual_value REAL,
    created_at TEXT DEFAULT (datetime('now'))  -- UTC
)"""


def initialize_database():
    """Create all tables if they don't exist."""
    try:
        conn = _get_connection()
        conn.execute(CREATE_BETS_TABLE)
        conn.execute(CREATE_DAILY_SNAPSHOTS_TABLE)
        conn.execute(CREATE_ANALYSIS_PICKS_TABLE)
        conn.commit()
        conn.close()
        _logger.info("Tracking database initialized at %s", DB_FILE_PATH)
    except sqlite3.Error as err:
        _logger.error("Failed to initialize tracking DB: %s", err)


# ── Bet CRUD ────────────────────────────────────────────────

def insert_bet(
    bet_date: str,
    player_name: str,
    stat_type: str,
    prop_line: float,
    direction: str,
    *,
    team: str = "",
    platform: str = "PrizePicks",
    confidence_score: float = 0.0,
    confidence_tier: str = "",
    model_probability: float = 0.0,
    edge_pct: float = 0.0,
    kelly_fraction: float = 0.0,
    recommended_bet: float = 0.0,
    opponent: str = "",
    player_id: int = 0,
    notes: str = "",
    source: str = "manual",
) -> int | None:
    """Insert a new bet. Returns the bet_id."""
    sql = """
    INSERT INTO bets (
        bet_date, player_name, team, stat_type, prop_line, direction,
        platform, confidence_score, confidence_tier, model_probability,
        edge_pct, kelly_fraction, recommended_bet, opponent, player_id,
        notes, source
    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """
    params = (
        bet_date, player_name, team, stat_type, prop_line, direction,
        platform, confidence_score, confidence_tier, model_probability,
        edge_pct, kelly_fraction, recommended_bet, opponent, player_id,
        notes, source,
    )
    cursor = _execute_write(sql, params, caller="insert_bet")
    return cursor.lastrowid if cursor else None


def update_bet_result(bet_id: int, result: str, actual_value: float | None = None) -> bool:
    """Record the result for a bet (win/loss/push)."""
    sql = "UPDATE bets SET result = ?, actual_value = ? WHERE bet_id = ?"
    cursor = _execute_write(sql, (result.lower(), actual_value, bet_id), caller="update_bet_result")
    return cursor is not None


def load_all_bets(limit: int = 200) -> list[dict]:
    """Load bets ordered by date desc."""
    try:
        conn = _get_connection()
        rows = conn.execute(
            "SELECT * FROM bets ORDER BY bet_date DESC, created_at DESC LIMIT ?",
            (limit,),
        ).fetchall()
        conn.close()
        return [dict(r) for r in rows]
    except sqlite3.Error as err:
        _logger.error("load_all_bets error: %s", err)
        return []


def get_performance_summary() -> dict:
    """Get aggregate win/loss/push counts."""
    try:
        conn = _get_connection()
        row = conn.execute("""
            SELECT
                COUNT(*) as total_bets,
                SUM(CASE WHEN result = 'win' THEN 1 ELSE 0 END) as wins,
                SUM(CASE WHEN result = 'loss' THEN 1 ELSE 0 END) as losses,
                SUM(CASE WHEN result = 'push' THEN 1 ELSE 0 END) as pushes,
                SUM(CASE WHEN result IS NULL THEN 1 ELSE 0 END) as pending
            FROM bets
        """).fetchone()
        conn.close()
        d = dict(row)
        decided = d["wins"] + d["losses"]
        d["win_rate"] = (d["wins"] / decided * 100) if decided > 0 else 0.0
        return d
    except sqlite3.Error as err:
        _logger.error("get_performance_summary error: %s", err)
        return {"total_bets": 0, "wins": 0, "losses": 0, "pushes": 0, "pending": 0, "win_rate": 0.0}


def get_performance_by_tier() -> list[dict]:
    """Performance breakdown by confidence tier."""
    try:
        conn = _get_connection()
        rows = conn.execute("""
            SELECT
                confidence_tier as tier,
                COUNT(*) as total,
                SUM(CASE WHEN result = 'win' THEN 1 ELSE 0 END) as wins,
                SUM(CASE WHEN result = 'loss' THEN 1 ELSE 0 END) as losses,
                SUM(CASE WHEN result IS NULL THEN 1 ELSE 0 END) as pending
            FROM bets
            WHERE confidence_tier IS NOT NULL AND confidence_tier != ''
            GROUP BY confidence_tier
            ORDER BY wins DESC
        """).fetchall()
        conn.close()
        result = []
        for r in rows:
            d = dict(r)
            decided = d["wins"] + d["losses"]
            d["win_rate"] = (d["wins"] / decided * 100) if decided > 0 else 0.0
            result.append(d)
        return result
    except sqlite3.Error as err:
        _logger.error("get_performance_by_tier error: %s", err)
        return []


def get_performance_by_stat() -> list[dict]:
    """Performance breakdown by stat type."""
    try:
        conn = _get_connection()
        rows = conn.execute("""
            SELECT
                stat_type,
                COUNT(*) as total,
                SUM(CASE WHEN result = 'win' THEN 1 ELSE 0 END) as wins,
                SUM(CASE WHEN result = 'loss' THEN 1 ELSE 0 END) as losses,
                SUM(CASE WHEN result IS NULL THEN 1 ELSE 0 END) as pending
            FROM bets GROUP BY stat_type ORDER BY total DESC
        """).fetchall()
        conn.close()
        result = []
        for r in rows:
            d = dict(r)
            decided = d["wins"] + d["losses"]
            d["win_rate"] = (d["wins"] / decided * 100) if decided > 0 else 0.0
            result.append(d)
        return result
    except sqlite3.Error as err:
        _logger.error("get_performance_by_stat error: %s", err)
        return []


def get_performance_by_platform() -> list[dict]:
    """Performance breakdown by platform."""
    try:
        conn = _get_connection()
        rows = conn.execute("""
            SELECT
                COALESCE(platform, 'Unknown') as platform,
                COUNT(*) as total,
                SUM(CASE WHEN result = 'win' THEN 1 ELSE 0 END) as wins,
                SUM(CASE WHEN result = 'loss' THEN 1 ELSE 0 END) as losses,
                SUM(CASE WHEN result IS NULL THEN 1 ELSE 0 END) as pending
            FROM bets GROUP BY platform ORDER BY total DESC
        """).fetchall()
        conn.close()
        result = []
        for r in rows:
            d = dict(r)
            decided = d["wins"] + d["losses"]
            d["win_rate"] = (d["wins"] / decided * 100) if decided > 0 else 0.0
            result.append(d)
        return result
    except sqlite3.Error as err:
        _logger.error("get_performance_by_platform error: %s", err)
        return []


# ── Analysis Picks ──────────────────────────────────────────

def insert_analysis_pick(
    analysis_date: str,
    player_name: str,
    stat_type: str,
    prop_line: float,
    direction: str,
    **kwargs,
) -> int | None:
    """Insert a pick from the Prop Analyzer."""
    sql = """
    INSERT INTO analysis_picks (
        analysis_date, player_name, player_id, team, opponent,
        stat_type, prop_line, direction, model_probability, edge_pct,
        confidence_score, confidence_tier, kelly_fraction,
        recommended_bet, explanation
    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """
    params = (
        analysis_date,
        player_name,
        kwargs.get("player_id", 0),
        kwargs.get("team", ""),
        kwargs.get("opponent", ""),
        stat_type,
        prop_line,
        direction,
        kwargs.get("model_probability", 0.0),
        kwargs.get("edge_pct", 0.0),
        kwargs.get("confidence_score", 0.0),
        kwargs.get("confidence_tier", ""),
        kwargs.get("kelly_fraction", 0.0),
        kwargs.get("recommended_bet", 0.0),
        kwargs.get("explanation", ""),
    )
    cursor = _execute_write(sql, params, caller="insert_analysis_pick")
    return cursor.lastrowid if cursor else None


def load_analysis_picks(limit: int = 100) -> list[dict]:
    """Load analysis picks ordered by date desc."""
    try:
        conn = _get_connection()
        rows = conn.execute(
            "SELECT * FROM analysis_picks ORDER BY analysis_date DESC, created_at DESC LIMIT ?",
            (limit,),
        ).fetchall()
        conn.close()
        return [dict(r) for r in rows]
    except sqlite3.Error as err:
        _logger.error("load_analysis_picks error: %s", err)
        return []


def delete_bet(bet_id: int) -> bool:
    """Delete a bet by ID."""
    cursor = _execute_write("DELETE FROM bets WHERE bet_id = ?", (bet_id,), caller="delete_bet")
    return cursor is not None
