# ============================================================
# FILE: tracking/database.py
# PURPOSE: SQLite database wrapper for storing bet history
#          and tracking model performance over time.
# CONNECTS TO: bet_tracker.py (uses these functions)
# CONCEPTS COVERED: SQLite, database CRUD operations,
#                   context managers, SQL queries
#
# NOTE: check_same_thread=False is set on ALL connections for
# Streamlit Server compatibility where multiple user sessions may
# access the database concurrently. SQLite handles this safely in
# WAL mode for read-heavy workloads (WAL mode is enabled below).
# ============================================================

# Standard library imports only
import sqlite3    # Built-in SQLite database (no install needed!)
import json       # For serializing/deserializing analysis session data
import os         # For file path operations
import time       # For retry backoff delays
import datetime   # For timestamps in analysis session persistence
from pathlib import Path  # Modern file path handling

try:
    from utils.logger import get_logger
    _logger = get_logger(__name__)
except ImportError:
    import logging
    _logger = logging.getLogger(__name__)


# ============================================================
# SECTION: Database Configuration
# ============================================================

# Path to the SQLite database file
# It will be created automatically on first run
DB_DIRECTORY = Path(__file__).parent.parent / "db"
DB_FILE_PATH = DB_DIRECTORY / "smartai_nba.db"

# Retry configuration for concurrent write safety.
# SQLite can throw "database is locked" when multiple Streamlit sessions
# attempt concurrent writes. Retrying with back-off avoids data loss.
_WRITE_RETRY_ATTEMPTS = 3
_WRITE_RETRY_DELAY = 0.25  # seconds between retries (doubles each attempt)


def _execute_write(sql, params=(), *, caller="write"):
    """Execute a single INSERT / UPDATE with locked-database retry.

    Centralises the retry-with-backoff loop that was previously inlined
    in ``insert_bet`` and ``update_bet_result``, so every write path
    gets the same concurrency protection.

    Args:
        sql (str): The SQL statement to execute.
        params (tuple): Bind parameters.
        caller (str): Label for log messages.

    Returns:
        sqlite3.Cursor | None: The cursor on success, or *None* after
        all retries are exhausted.
    """
    for _attempt in range(_WRITE_RETRY_ATTEMPTS):
        try:
            conn = sqlite3.connect(str(DB_FILE_PATH), check_same_thread=False)
            conn.execute("PRAGMA journal_mode=WAL")
            cursor = conn.cursor()
            cursor.execute(sql, params)
            conn.commit()
            conn.close()
            return cursor
        except sqlite3.OperationalError as op_err:
            if "locked" in str(op_err).lower() and _attempt < _WRITE_RETRY_ATTEMPTS - 1:
                _logger.warning(
                    f"{caller}: database locked, retry "
                    f"{_attempt + 1}/{_WRITE_RETRY_ATTEMPTS}"
                )
                time.sleep(_WRITE_RETRY_DELAY * (2 ** _attempt))
                continue
            _logger.error(f"{caller} error: {op_err}")
            return None
        except sqlite3.Error as db_err:
            _logger.error(f"{caller} error: {db_err}")
            return None
    return None


# SQL to create the bets table (runs once when app starts)
# BEGINNER NOTE: SQL is a language for managing databases.
# CREATE TABLE IF NOT EXISTS = only create if it doesn't exist yet
CREATE_BETS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS bets (
    bet_id INTEGER PRIMARY KEY AUTOINCREMENT,
    bet_date TEXT NOT NULL,
    player_name TEXT NOT NULL,
    team TEXT,
    stat_type TEXT NOT NULL,
    prop_line REAL NOT NULL,
    direction TEXT NOT NULL,
    platform TEXT,
    confidence_score REAL,
    probability_over REAL,
    edge_percentage REAL,
    tier TEXT,
    entry_type TEXT,
    entry_fee REAL,
    result TEXT,
    actual_value REAL,
    notes TEXT,
    auto_logged INTEGER DEFAULT 0,
    created_at TEXT DEFAULT (datetime('now'))
);
"""

# SQL to create the entries table (for tracking parlay entries)
CREATE_ENTRIES_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS entries (
    entry_id INTEGER PRIMARY KEY AUTOINCREMENT,
    entry_date TEXT NOT NULL,
    platform TEXT NOT NULL,
    entry_type TEXT,
    entry_fee REAL,
    expected_value REAL,
    result TEXT,
    payout REAL,
    pick_count INTEGER,
    notes TEXT,
    created_at TEXT DEFAULT (datetime('now'))
);
"""

# SQL to create the prediction_history table for model calibration (W7)
# Tracks each prediction made and whether it was correct.
# Used to compute calibration adjustments that self-correct model overconfidence.
CREATE_PREDICTION_HISTORY_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS prediction_history (
    prediction_id INTEGER PRIMARY KEY AUTOINCREMENT,
    prediction_date TEXT NOT NULL,
    player_name TEXT NOT NULL,
    stat_type TEXT NOT NULL,
    prop_line REAL NOT NULL,
    direction TEXT NOT NULL,
    confidence_score REAL,
    probability_predicted REAL,
    was_correct INTEGER,
    actual_value REAL,
    notes TEXT,
    created_at TEXT DEFAULT (datetime('now'))
);
"""

# SQL to create the daily_snapshots table for per-day performance tracking.
# Stores aggregated bet outcomes, win rates, and breakdowns per day.
CREATE_DAILY_SNAPSHOTS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS daily_snapshots (
    snapshot_id INTEGER PRIMARY KEY AUTOINCREMENT,
    snapshot_date TEXT NOT NULL UNIQUE,
    total_picks INTEGER DEFAULT 0,
    wins INTEGER DEFAULT 0,
    losses INTEGER DEFAULT 0,
    pushes INTEGER DEFAULT 0,
    pending INTEGER DEFAULT 0,
    win_rate REAL DEFAULT 0.0,
    platform_breakdown TEXT,
    tier_breakdown TEXT,
    stat_type_breakdown TEXT,
    best_pick TEXT,
    worst_pick TEXT,
    created_at TEXT DEFAULT (datetime('now'))
);
"""

# SQL to create the all_analysis_picks table.
# Stores EVERY pick output by Neural Analysis (not just AI-auto-logged ones)
# so users can track the complete performance record of the app's predictions.
CREATE_ALL_ANALYSIS_PICKS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS all_analysis_picks (
    pick_id INTEGER PRIMARY KEY AUTOINCREMENT,
    pick_date TEXT NOT NULL,
    player_name TEXT NOT NULL,
    team TEXT,
    stat_type TEXT NOT NULL,
    prop_line REAL NOT NULL,
    direction TEXT NOT NULL,
    platform TEXT,
    confidence_score REAL,
    probability_over REAL,
    edge_percentage REAL,
    tier TEXT,
    result TEXT,
    actual_value REAL,
    notes TEXT,
    created_at TEXT DEFAULT (datetime('now'))
);
"""

# SQL to create the subscriptions table.
# Stores Stripe subscription records for premium access tracking.
# Each row represents one subscriber; status reflects the current
# Stripe subscription status (active, trialing, cancelled, etc.).
CREATE_SUBSCRIPTIONS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS subscriptions (
    subscription_id TEXT PRIMARY KEY,
    customer_id TEXT NOT NULL,
    customer_email TEXT,
    status TEXT NOT NULL DEFAULT 'active',
    plan_name TEXT DEFAULT 'Premium',
    current_period_start TEXT,
    current_period_end TEXT,
    created_at TEXT DEFAULT (datetime('now')),
    updated_at TEXT DEFAULT (datetime('now'))
);
"""

# SQL to create the analysis_sessions table.
# Persists Neural Analysis results so users never lose their analysis
# after inactivity. On page load, session state is rehydrated from here.
CREATE_ANALYSIS_SESSIONS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS analysis_sessions (
    session_id INTEGER PRIMARY KEY AUTOINCREMENT,
    analysis_timestamp TEXT NOT NULL,
    analysis_results_json TEXT NOT NULL,
    todays_games_json TEXT,
    selected_picks_json TEXT,
    prop_count INTEGER DEFAULT 0,
    created_at TEXT DEFAULT (datetime('now'))
);
"""

# SQL to create the backtest_results table.
# Stores historical backtesting runs so results persist across sessions.
CREATE_BACKTEST_RESULTS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS backtest_results (
    backtest_id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_timestamp TEXT NOT NULL,
    season TEXT NOT NULL,
    stat_types_json TEXT NOT NULL,
    min_edge REAL NOT NULL,
    tier_filter TEXT,
    total_picks INTEGER NOT NULL DEFAULT 0,
    wins INTEGER NOT NULL DEFAULT 0,
    losses INTEGER NOT NULL DEFAULT 0,
    win_rate REAL NOT NULL DEFAULT 0.0,
    roi REAL NOT NULL DEFAULT 0.0,
    total_pnl REAL NOT NULL DEFAULT 0.0,
    tier_win_rates_json TEXT,
    stat_win_rates_json TEXT,
    edge_win_rates_json TEXT,
    pick_log_json TEXT,
    created_at TEXT DEFAULT (datetime('now'))
);
"""

# SQL to create the player_game_logs cache table.
# Feature 12: Game Log Persistence Across Sessions.
# Stores per-player game log rows retrieved from nba_api so browser
# refreshes and session resets don't lose expensive API data.
# Cache invalidation: re-retrieve if most recent game is > 24 hours old.
CREATE_PLAYER_GAME_LOGS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS player_game_logs (
    log_id INTEGER PRIMARY KEY AUTOINCREMENT,
    player_id TEXT NOT NULL,
    player_name TEXT NOT NULL,
    game_date TEXT NOT NULL,
    opponent TEXT,
    minutes REAL,
    points INTEGER,
    rebounds INTEGER,
    assists INTEGER,
    threes INTEGER,
    steals INTEGER,
    blocks INTEGER,
    turnovers INTEGER,
    fg_pct REAL,
    ft_pct REAL,
    plus_minus INTEGER,
    retrieved_at TEXT DEFAULT (datetime('now')),
    UNIQUE(player_id, game_date)
);
"""

# ============================================================
# END SECTION: Database Configuration
# ============================================================


# ============================================================
# SECTION: Database Initialization
# ============================================================

def initialize_database():
    """
    Create the database and tables if they don't exist.

    Call this once when the app starts. It's safe to call
    multiple times — CREATE TABLE IF NOT EXISTS won't
    overwrite existing tables.

    Returns:
        bool: True if successful, False if error occurred
    """
    # Make sure the db directory exists
    # exist_ok=True means don't error if it already exists
    DB_DIRECTORY.mkdir(parents=True, exist_ok=True)

    try:
        # Connect to the SQLite database file
        # BEGINNER NOTE: sqlite3.connect() opens (or creates) the DB file
        # 'with' statement ensures the connection is properly closed
        # check_same_thread=False: required for Streamlit Server multi-session access
        with sqlite3.connect(str(DB_FILE_PATH), check_same_thread=False, timeout=30) as connection:
            # Enable WAL mode for safe concurrent read access
            connection.execute("PRAGMA journal_mode=WAL")
            # Run integrity check on startup
            try:
                integrity_result = connection.execute("PRAGMA integrity_check").fetchone()
                if integrity_result and integrity_result[0] != "ok":
                    _logger.warning(
                        "Database integrity check returned: %s. "
                        "Consider restoring from backup or reinitializing the database.",
                        integrity_result[0],
                    )
                else:
                    _logger.debug("Database integrity check passed")
            except Exception as _ic_err:
                _logger.warning("Database integrity check failed: %s", _ic_err)
            cursor = connection.cursor()  # A cursor lets us run SQL commands

            # Create the tables
            cursor.execute(CREATE_BETS_TABLE_SQL)
            cursor.execute(CREATE_ENTRIES_TABLE_SQL)
            cursor.execute(CREATE_PREDICTION_HISTORY_TABLE_SQL)  # W7: calibration
            cursor.execute(CREATE_DAILY_SNAPSHOTS_TABLE_SQL)      # daily performance tracking
            cursor.execute(CREATE_ALL_ANALYSIS_PICKS_TABLE_SQL)   # all Neural Analysis outputs
            cursor.execute(CREATE_SUBSCRIPTIONS_TABLE_SQL)        # Stripe subscription records
            cursor.execute(CREATE_ANALYSIS_SESSIONS_TABLE_SQL)    # analysis session persistence
            cursor.execute(CREATE_BACKTEST_RESULTS_TABLE_SQL)     # historical backtesting results
            cursor.execute(CREATE_PLAYER_GAME_LOGS_TABLE_SQL)     # Feature 12: game log persistence

            # ── Schema migrations for existing databases ──────────────
            # Add auto_logged column if it doesn't exist yet
            # (ALTER TABLE is idempotent-safe via the try/except)
            try:
                cursor.execute(
                    "ALTER TABLE bets ADD COLUMN auto_logged INTEGER DEFAULT 0"
                )
            except sqlite3.OperationalError:
                pass  # Column already exists — safe to ignore

            # Ensure actual_value column exists (older schema may not have it)
            try:
                cursor.execute(
                    "ALTER TABLE bets ADD COLUMN actual_value REAL"
                )
            except sqlite3.OperationalError:
                pass  # Column already exists — safe to ignore

            # ── Subscriptions table migration ─────────────────────────
            # If the subscriptions table was created without updated_at
            # (e.g., from an older version of the schema), add it now.
            try:
                cursor.execute(
                    "ALTER TABLE subscriptions ADD COLUMN updated_at TEXT DEFAULT (datetime('now'))"
                )
            except sqlite3.OperationalError:
                pass  # Column already exists — safe to ignore

            # ── Goblin/Demon bet_type column migrations ───────────────
            # Add bet_type and std_devs_from_line to bets table
            try:
                cursor.execute(
                    "ALTER TABLE bets ADD COLUMN bet_type TEXT DEFAULT 'normal'"
                )
            except sqlite3.OperationalError:
                pass  # Column already exists — safe to ignore

            try:
                cursor.execute(
                    "ALTER TABLE bets ADD COLUMN std_devs_from_line REAL DEFAULT 0.0"
                )
            except sqlite3.OperationalError:
                pass  # Column already exists — safe to ignore

            # Add bet_type to all_analysis_picks table
            try:
                cursor.execute(
                    "ALTER TABLE all_analysis_picks ADD COLUMN bet_type TEXT DEFAULT 'normal'"
                )
            except sqlite3.OperationalError:
                pass  # Column already exists — safe to ignore

            try:
                cursor.execute(
                    "ALTER TABLE all_analysis_picks ADD COLUMN std_devs_from_line REAL DEFAULT 0.0"
                )
            except sqlite3.OperationalError:
                pass  # Column already exists — safe to ignore

            # ── Line category column migration ────────────────────────
            # Add line_category and standard_line columns for the three-tier
            # Goblin / 50_50 / Demon classification system.
            try:
                cursor.execute(
                    "ALTER TABLE bets ADD COLUMN line_category TEXT DEFAULT '50_50'"
                )
            except sqlite3.OperationalError:
                pass  # Column already exists — safe to ignore

            try:
                cursor.execute(
                    "ALTER TABLE bets ADD COLUMN standard_line REAL"
                )
            except sqlite3.OperationalError:
                pass  # Column already exists — safe to ignore

            # Remap old "demon" bet_type records (conflicting-forces picks under
            # the old system) to "50_50" — they were standard-line uncertain picks,
            # not true Demon bets (line above standard O/U).
            try:
                cursor.execute(
                    "UPDATE bets SET bet_type = '50_50' WHERE bet_type = 'demon'"
                )
            except sqlite3.OperationalError:
                pass

            # ── Rename retrieved_at column in player_game_logs ──
            # Older databases have the column named with old terminology; new schema
            # uses retrieved_at.  SQLite ≥ 3.25 supports ALTER TABLE RENAME
            # COLUMN, but we guard with try/except for older builds.
            try:
                cursor.execute(
                    "ALTER TABLE player_game_logs RENAME COLUMN fetched_at TO retrieved_at"
                )
            except sqlite3.OperationalError:
                pass  # Column already renamed or doesn't exist

            # Save the changes
            connection.commit()

        return True

    except sqlite3.Error as database_error:
        _logger.error(f"Database initialization error: {database_error}")
        return False


def get_database_connection():
    """
    Get a connection to the SQLite database.

    Returns:
        sqlite3.Connection: Active database connection
        Call .close() when done, or use 'with' statement.
    """
    # Ensure database exists before connecting
    initialize_database()

    # Connect with row_factory so results come back as dictionaries
    # BEGINNER NOTE: row_factory makes results easier to work with —
    # instead of tuples (24, 'LeBron') you get {'points': 24, 'name': 'LeBron'}
    # check_same_thread=False: required for Streamlit Server multi-session access
    connection = sqlite3.connect(str(DB_FILE_PATH), check_same_thread=False, timeout=30)
    # Enable WAL mode for safe concurrent read access
    connection.execute("PRAGMA journal_mode=WAL")
    connection.row_factory = sqlite3.Row  # Rows behave like dicts

    return connection

# ============================================================
# END SECTION: Database Initialization
# ============================================================


# ============================================================
# SECTION: Database CRUD Operations
# CRUD = Create, Read, Update, Delete
# ============================================================

def insert_bet(bet_data):
    """
    Save a new bet to the database.

    Args:
        bet_data (dict): Bet information with keys:
            bet_date, player_name, team, stat_type, prop_line,
            direction, platform, confidence_score, probability_over,
            edge_percentage, tier, entry_type, entry_fee, notes,
            auto_logged (optional, default 0)

    Returns:
        int or None: The new bet's ID, or None if error
    """
    # SQL INSERT statement — ? placeholders for safety
    # BEGINNER NOTE: Never put values directly in SQL strings!
    # Use ? placeholders to prevent "SQL injection" attacks
    insert_sql = """
    INSERT INTO bets (
        bet_date, player_name, team, stat_type, prop_line,
        direction, platform, confidence_score, probability_over,
        edge_percentage, tier, entry_type, entry_fee, notes, auto_logged,
        bet_type, std_devs_from_line
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """

    values = (
        bet_data.get("bet_date", ""),
        bet_data.get("player_name", ""),
        bet_data.get("team", ""),
        bet_data.get("stat_type", ""),
        bet_data.get("prop_line", 0.0),
        bet_data.get("direction", "OVER"),
        bet_data.get("platform", ""),
        bet_data.get("confidence_score", 0.0),
        bet_data.get("probability_over", 0.5),
        bet_data.get("edge_percentage", 0.0),
        bet_data.get("tier", "Bronze"),
        bet_data.get("entry_type", ""),
        bet_data.get("entry_fee", 0.0),
        bet_data.get("notes", ""),
        int(bet_data.get("auto_logged", 0)),
        bet_data.get("bet_type", "normal"),
        float(bet_data.get("std_devs_from_line", 0.0)),
    )

    for _attempt in range(_WRITE_RETRY_ATTEMPTS):
        try:
            with sqlite3.connect(str(DB_FILE_PATH), check_same_thread=False) as connection:
                connection.execute("PRAGMA journal_mode=WAL")
                cursor = connection.cursor()
                cursor.execute(insert_sql, values)
                connection.commit()
                return cursor.lastrowid  # Return the new row's ID

        except sqlite3.OperationalError as op_err:
            if "locked" in str(op_err).lower() and _attempt < _WRITE_RETRY_ATTEMPTS - 1:
                _logger.warning(f"insert_bet: database locked, retry {_attempt + 1}/{_WRITE_RETRY_ATTEMPTS}")
                time.sleep(_WRITE_RETRY_DELAY * (2 ** _attempt))
                continue
            _logger.error(f"Error inserting bet: {op_err}")
            return None
        except sqlite3.Error as database_error:
            _logger.error(f"Error inserting bet: {database_error}")
            return None
    return None


def update_bet_result(bet_id, result, actual_value):
    """
    Update a bet with its result after the game.

    Args:
        bet_id (int): The bet's database ID
        result (str): 'WIN', 'LOSS', or 'PUSH'
        actual_value (float): What the player actually scored

    Returns:
        bool: True if updated successfully
    """
    update_sql = """
    UPDATE bets
    SET result = ?, actual_value = ?
    WHERE bet_id = ?
    """

    for _attempt in range(_WRITE_RETRY_ATTEMPTS):
        try:
            with sqlite3.connect(str(DB_FILE_PATH), check_same_thread=False) as connection:
                connection.execute("PRAGMA journal_mode=WAL")
                cursor = connection.cursor()
                cursor.execute(update_sql, (result, actual_value, bet_id))
                connection.commit()
                return cursor.rowcount > 0  # True if a row was updated

        except sqlite3.OperationalError as op_err:
            if "locked" in str(op_err).lower() and _attempt < _WRITE_RETRY_ATTEMPTS - 1:
                _logger.warning(f"update_bet_result: database locked, retry {_attempt + 1}/{_WRITE_RETRY_ATTEMPTS}")
                time.sleep(_WRITE_RETRY_DELAY * (2 ** _attempt))
                continue
            _logger.error(f"Error updating bet result: {op_err}")
            return False
        except sqlite3.Error as database_error:
            _logger.error(f"Error updating bet result: {database_error}")
            return False
    return False


def load_all_bets(limit=200):
    """
    Load recent bets from the database.

    Args:
        limit (int): Maximum number of bets to return

    Returns:
        list of dict: Bet rows as dictionaries
    """
    select_sql = """
    SELECT * FROM bets
    ORDER BY created_at DESC
    LIMIT ?
    """

    try:
        with sqlite3.connect(str(DB_FILE_PATH), check_same_thread=False) as connection:
            connection.row_factory = sqlite3.Row
            cursor = connection.cursor()
            cursor.execute(select_sql, (limit,))
            rows = cursor.fetchall()
            # Convert sqlite3.Row objects to regular dicts
            return [dict(row) for row in rows]

    except sqlite3.Error as database_error:
        _logger.error(f"Error loading bets: {database_error}")
        return []


def get_performance_summary():
    """
    Get win/loss statistics from the database.

    Returns:
        dict: Performance stats including:
            'total_bets', 'wins', 'losses', 'pushes',
            'win_rate', 'roi'
    """
    # SQL aggregation query to count outcomes
    summary_sql = """
    SELECT
        COUNT(*) as total_bets,
        SUM(CASE WHEN result = 'WIN' THEN 1 ELSE 0 END) as wins,
        SUM(CASE WHEN result = 'LOSS' THEN 1 ELSE 0 END) as losses,
        SUM(CASE WHEN result = 'PUSH' THEN 1 ELSE 0 END) as pushes,
        AVG(CASE WHEN result IS NOT NULL AND result != '' THEN
            CASE WHEN result = 'WIN' THEN 1.0 ELSE 0.0 END
        END) as win_rate
    FROM bets
    WHERE result IS NOT NULL AND result != ''
    """

    try:
        with sqlite3.connect(str(DB_FILE_PATH), check_same_thread=False) as connection:
            connection.row_factory = sqlite3.Row
            cursor = connection.cursor()
            cursor.execute(summary_sql)
            row = cursor.fetchone()

            if row:
                total = row["total_bets"] or 0
                wins = row["wins"] or 0
                losses = row["losses"] or 0
                pushes = row["pushes"] or 0
                win_rate = row["win_rate"] or 0.0

                return {
                    "total_bets": total,
                    "wins": wins,
                    "losses": losses,
                    "pushes": pushes,
                    "win_rate": round(win_rate * 100, 1),
                }

    except sqlite3.Error as database_error:
        _logger.error(f"Error getting performance summary: {database_error}")

    return {
        "total_bets": 0,
        "wins": 0,
        "losses": 0,
        "pushes": 0,
        "win_rate": 0.0,
    }


# ============================================================
# SECTION: Prediction History & Calibration (W7)
# Store prediction outcomes and compute calibration adjustments
# so the model can self-correct systematic over/underconfidence.
# ============================================================

def insert_prediction(prediction_data):
    """
    Save a model prediction to the history table. (W7)

    Call this when a bet is placed. Later, update with
    `update_prediction_outcome()` when the result is known.

    Args:
        prediction_data (dict): Prediction data with keys:
            prediction_date (str), player_name (str), stat_type (str),
            prop_line (float), direction (str), confidence_score (float),
            probability_predicted (float), notes (str, optional)

    Returns:
        int or None: The new prediction's ID, or None if error
    """
    initialize_database()  # Ensure prediction_history table exists
    insert_sql = """
    INSERT INTO prediction_history (
        prediction_date, player_name, stat_type, prop_line,
        direction, confidence_score, probability_predicted, notes
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """
    values = (
        prediction_data.get("prediction_date", ""),
        prediction_data.get("player_name", ""),
        prediction_data.get("stat_type", ""),
        prediction_data.get("prop_line", 0.0),
        prediction_data.get("direction", "OVER"),
        prediction_data.get("confidence_score", 0.0),
        prediction_data.get("probability_predicted", 0.5),
        prediction_data.get("notes", ""),
    )
    cursor = _execute_write(insert_sql, values, caller="insert_prediction")
    return cursor.lastrowid if cursor else None


def load_recent_predictions(days=90):
    """
    Load recent prediction records from the prediction_history table.

    Used by the calibration engine to build calibration curves and
    compute self-correction adjustments.

    Args:
        days (int): Number of past days to include. Default 90.

    Returns:
        list of dict: Prediction records with keys including
            ``probability_over`` (aliased from probability_predicted),
            ``result`` (aliased from was_correct), ``stat_type``,
            ``date`` (aliased from prediction_date), and ``created_at``.
        Empty list on cold start or database errors.
    """
    initialize_database()
    cutoff = (
        datetime.datetime.now() - datetime.timedelta(days=days)
    ).strftime("%Y-%m-%d")
    query_sql = """
    SELECT
        prediction_id,
        prediction_date  AS date,
        player_name,
        stat_type,
        prop_line,
        direction,
        confidence_score,
        probability_predicted AS probability_over,
        was_correct           AS result,
        actual_value,
        notes,
        created_at
    FROM prediction_history
    WHERE prediction_date >= ?
    ORDER BY prediction_date DESC
    """
    try:
        with sqlite3.connect(str(DB_FILE_PATH), check_same_thread=False) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute(query_sql, (cutoff,))
            rows = cursor.fetchall()
            return [dict(r) for r in rows]
    except sqlite3.Error as exc:
        _logger.error("[database] load_recent_predictions error: %s", exc)
        return []


def update_prediction_outcome(prediction_id, was_correct, actual_value):
    """
    Record the actual outcome for a prediction. (W7)

    Args:
        prediction_id (int): The prediction's database ID
        was_correct (bool): True if the prediction was correct
        actual_value (float): The actual stat value achieved

    Returns:
        bool: True if updated successfully
    """
    update_sql = """
    UPDATE prediction_history
    SET was_correct = ?, actual_value = ?
    WHERE prediction_id = ?
    """
    cursor = _execute_write(
        update_sql,
        (1 if was_correct else 0, actual_value, prediction_id),
        caller="update_prediction_outcome",
    )
    return cursor is not None


def get_calibration_adjustment(stat_type=None, min_samples=20):
    """
    Compute a calibration adjustment for the confidence model. (W7)

    Compares the model's predicted probability to the actual
    hit rate. If the model says 62% but only 58% of picks hit,
    the calibration adjustment is +4 (subtract 4 from displayed score).

    Args:
        stat_type (str or None): Compute calibration for a specific
            stat type, or overall if None.
        min_samples (int): Minimum number of graded predictions needed
            before returning a non-zero adjustment (default: 20).
            With fewer samples, the adjustment is unreliable.

    Returns:
        float: Calibration adjustment in confidence points.
            Positive = model overestimates (subtract from score).
            Negative = model underestimates (add to score).
            Returns 0.0 if insufficient data.

    Example:
        Model predicts avg 62% probability, actual hit rate is 57%
        → calibration_adjustment = +5.0 points (reduce all scores by 5)
    """
    initialize_database()  # Ensure prediction_history table exists
    if stat_type:
        query_sql = """
        SELECT
            AVG(probability_predicted) as avg_predicted_prob,
            AVG(CAST(was_correct AS REAL)) as actual_hit_rate,
            COUNT(*) as sample_count
        FROM prediction_history
        WHERE was_correct IS NOT NULL AND stat_type = ?
        """
        params = (stat_type,)
    else:
        query_sql = """
        SELECT
            AVG(probability_predicted) as avg_predicted_prob,
            AVG(CAST(was_correct AS REAL)) as actual_hit_rate,
            COUNT(*) as sample_count
        FROM prediction_history
        WHERE was_correct IS NOT NULL
        """
        params = ()

    try:
        with sqlite3.connect(str(DB_FILE_PATH), check_same_thread=False) as connection:
            connection.row_factory = sqlite3.Row
            cursor = connection.cursor()
            cursor.execute(query_sql, params)
            row = cursor.fetchone()

            if row and row["sample_count"] >= min_samples:
                avg_predicted = row["avg_predicted_prob"] or 0.5
                actual_rate = row["actual_hit_rate"] or 0.5
                # Overconfidence = model probability higher than actual hit rate
                # Convert probability gap to confidence score adjustment
                # (1% probability gap ≈ 2 confidence score points)
                prob_gap_pct = (avg_predicted - actual_rate) * 100.0
                adjustment = prob_gap_pct * 2.0  # Scale to confidence score points
                # Cap adjustment to ±15 points to avoid extreme corrections
                return round(max(-15.0, min(15.0, adjustment)), 1)

    except sqlite3.Error as database_error:
        _logger.error(f"Error computing calibration: {database_error}")

    return 0.0  # No adjustment if insufficient data


def get_calibration_report():
    """
    Build a human-readable calibration report for the Model Health page. (W7)

    Returns:
        dict: {
            'overall': dict with avg_predicted, actual_hit_rate, sample_count, adjustment
            'by_stat': dict {stat_type: same dict}
            'summary_text': str (human-readable summary)
        }
    """
    initialize_database()  # Ensure prediction_history table exists
    report = {"overall": {}, "by_stat": {}, "summary_text": ""}

    query_overall = """
    SELECT
        stat_type,
        AVG(probability_predicted) as avg_predicted_prob,
        AVG(CAST(was_correct AS REAL)) as actual_hit_rate,
        COUNT(*) as sample_count
    FROM prediction_history
    WHERE was_correct IS NOT NULL
    GROUP BY stat_type
    ORDER BY sample_count DESC
    """

    query_all = """
    SELECT
        AVG(probability_predicted) as avg_predicted_prob,
        AVG(CAST(was_correct AS REAL)) as actual_hit_rate,
        COUNT(*) as sample_count
    FROM prediction_history
    WHERE was_correct IS NOT NULL
    """

    try:
        with sqlite3.connect(str(DB_FILE_PATH), check_same_thread=False) as connection:
            connection.row_factory = sqlite3.Row
            cursor = connection.cursor()

            # Overall calibration
            cursor.execute(query_all)
            row = cursor.fetchone()
            if row and row["sample_count"]:
                avg_p = row["avg_predicted_prob"] or 0.5
                actual = row["actual_hit_rate"] or 0.5
                report["overall"] = {
                    "avg_predicted_prob": round(avg_p * 100, 1),
                    "actual_hit_rate": round(actual * 100, 1),
                    "sample_count": row["sample_count"],
                    "calibration_adjustment": round((avg_p - actual) * 100 * 2, 1),
                }
                cal_dir = "overconfident" if avg_p > actual else "underconfident"
                report["summary_text"] = (
                    f"Model is {cal_dir}: predicts {avg_p*100:.1f}% avg but "
                    f"hits {actual*100:.1f}% ({row['sample_count']} graded predictions)"
                )

            # By stat type
            cursor.execute(query_overall)
            rows = cursor.fetchall()
            for r in rows:
                if r["sample_count"] and r["sample_count"] >= 5:
                    avg_p = r["avg_predicted_prob"] or 0.5
                    actual = r["actual_hit_rate"] or 0.5
                    report["by_stat"][r["stat_type"]] = {
                        "avg_predicted_prob": round(avg_p * 100, 1),
                        "actual_hit_rate": round(actual * 100, 1),
                        "sample_count": r["sample_count"],
                        "calibration_adjustment": round((avg_p - actual) * 100 * 2, 1),
                    }

    except sqlite3.Error as database_error:
        _logger.error(f"Error building calibration report: {database_error}")

    return report

# ============================================================
# END SECTION: Prediction History & Calibration
# ============================================================

# ============================================================
# SECTION: Daily Snapshots
# ============================================================

def save_daily_snapshot(date_str=None):
    """
    Aggregate all bets for *date_str* and write/update the daily_snapshots row.

    Args:
        date_str (str | None): ISO date string (YYYY-MM-DD). Defaults to today.

    Returns:
        bool: True on success, False on error.
    """
    import json
    import datetime as _dt

    if date_str is None:
        date_str = _dt.date.today().isoformat()

    conn = None
    try:
        conn = get_database_connection()
        cursor = conn.cursor()

        # Retrieve all bets for that date
        cursor.execute(
            "SELECT * FROM bets WHERE bet_date = ?",
            (date_str,),
        )
        rows = cursor.fetchall()
        cols = [d[0] for d in cursor.description]
        bets = [dict(zip(cols, row)) for row in rows]
    except Exception as exc:
        _logger.error(f"[database] save_daily_snapshot read error: {exc}")
        return False
    finally:
        try:
            if conn is not None:
                conn.close()
        except Exception:
            pass

    try:
        total = len(bets)
        wins = sum(1 for b in bets if b.get("result") == "WIN")
        losses = sum(1 for b in bets if b.get("result") == "LOSS")
        pushes = sum(1 for b in bets if b.get("result") == "PUSH")
        pending = sum(1 for b in bets if not b.get("result"))
        # win_rate is 0.0 when there are no resolved bets (wins + losses == 0)
        win_rate = round(wins / (wins + losses) * 100, 2) if (wins + losses) > 0 else 0.0

        # Platform breakdown
        platform_breakdown: dict = {}
        for b in bets:
            p = b.get("platform") or "Unknown"
            if p not in platform_breakdown:
                platform_breakdown[p] = {"wins": 0, "losses": 0, "pushes": 0, "pending": 0}
            res = b.get("result")
            if res == "WIN":
                platform_breakdown[p]["wins"] += 1
            elif res == "LOSS":
                platform_breakdown[p]["losses"] += 1
            elif res == "PUSH":
                platform_breakdown[p]["pushes"] += 1
            else:
                platform_breakdown[p]["pending"] += 1

        # Tier breakdown
        tier_breakdown: dict = {}
        for b in bets:
            t = b.get("tier") or "Unknown"
            if t not in tier_breakdown:
                tier_breakdown[t] = {"wins": 0, "losses": 0, "pushes": 0, "pending": 0}
            res = b.get("result")
            if res == "WIN":
                tier_breakdown[t]["wins"] += 1
            elif res == "LOSS":
                tier_breakdown[t]["losses"] += 1
            elif res == "PUSH":
                tier_breakdown[t]["pushes"] += 1
            else:
                tier_breakdown[t]["pending"] += 1

        # Stat-type breakdown
        stat_type_breakdown: dict = {}
        for b in bets:
            s = b.get("stat_type") or "Unknown"
            if s not in stat_type_breakdown:
                stat_type_breakdown[s] = {"wins": 0, "losses": 0, "pushes": 0, "pending": 0}
            res = b.get("result")
            if res == "WIN":
                stat_type_breakdown[s]["wins"] += 1
            elif res == "LOSS":
                stat_type_breakdown[s]["losses"] += 1
            elif res == "PUSH":
                stat_type_breakdown[s]["pushes"] += 1
            else:
                stat_type_breakdown[s]["pending"] += 1

        # Best / worst pick (by edge_percentage, resolved only)
        resolved = [b for b in bets if b.get("result") in ("WIN", "LOSS")]
        best_pick = ""
        worst_pick = ""
        if resolved:
            best = max(resolved, key=lambda b: float(b.get("edge_percentage") or 0))
            worst = min(resolved, key=lambda b: float(b.get("edge_percentage") or 0))
            best_pick = json.dumps({
                "player": best.get("player_name", ""),
                "stat": best.get("stat_type", ""),
                "edge": best.get("edge_percentage", 0),
                "result": best.get("result", ""),
            })
            worst_pick = json.dumps({
                "player": worst.get("player_name", ""),
                "stat": worst.get("stat_type", ""),
                "edge": worst.get("edge_percentage", 0),
                "result": worst.get("result", ""),
            })

        _upsert_sql = """
            INSERT INTO daily_snapshots
                (snapshot_date, total_picks, wins, losses, pushes, pending,
                 win_rate, platform_breakdown, tier_breakdown, stat_type_breakdown,
                 best_pick, worst_pick)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(snapshot_date) DO UPDATE SET
                total_picks        = excluded.total_picks,
                wins               = excluded.wins,
                losses             = excluded.losses,
                pushes             = excluded.pushes,
                pending            = excluded.pending,
                win_rate           = excluded.win_rate,
                platform_breakdown = excluded.platform_breakdown,
                tier_breakdown     = excluded.tier_breakdown,
                stat_type_breakdown = excluded.stat_type_breakdown,
                best_pick          = excluded.best_pick,
                worst_pick         = excluded.worst_pick
            """
        _upsert_params = (
            date_str,
            total,
            wins,
            losses,
            pushes,
            pending,
            win_rate,
            json.dumps(platform_breakdown),
            json.dumps(tier_breakdown),
            json.dumps(stat_type_breakdown),
            best_pick,
            worst_pick,
        )
        result = _execute_write(_upsert_sql, _upsert_params, caller="save_daily_snapshot")
        return result is not None
    except Exception as exc:
        _logger.error(f"[database] save_daily_snapshot error: {exc}")
        return False


def load_daily_snapshots(days=14):
    """Return the last *days* rows from daily_snapshots, newest first.

    Returns:
        list[dict]
    """
    import json

    conn = None
    try:
        conn = get_database_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT * FROM daily_snapshots
            ORDER BY snapshot_date DESC
            LIMIT ?
            """,
            (days,),
        )
        rows = cursor.fetchall()
        cols = [d[0] for d in cursor.description]
        conn.close()
        conn = None
        snapshots = []
        for row in rows:
            s = dict(zip(cols, row))
            for field in ("platform_breakdown", "tier_breakdown", "stat_type_breakdown"):
                try:
                    s[field] = json.loads(s.get(field) or "{}")
                except Exception:
                    s[field] = {}
            for field in ("best_pick", "worst_pick"):
                try:
                    s[field] = json.loads(s.get(field) or "{}")
                except Exception:
                    s[field] = {}
            snapshots.append(s)
        return snapshots
    except Exception as exc:
        _logger.error(f"[database] load_daily_snapshots error: {exc}")
        return []
    finally:
        try:
            if conn is not None:
                conn.close()
        except Exception:
            pass


def purge_old_snapshots(days=30):
    """Delete snapshots older than *days* days.

    Returns:
        int: Number of rows deleted.
    """
    import datetime as _dt

    cutoff = (_dt.date.today() - _dt.timedelta(days=days)).isoformat()
    conn = None
    try:
        conn = get_database_connection()
        cursor = conn.cursor()
        cursor.execute(
            "DELETE FROM daily_snapshots WHERE snapshot_date < ?",
            (cutoff,),
        )
        deleted = cursor.rowcount
        conn.commit()
        return deleted
    except Exception as exc:
        _logger.error(f"[database] purge_old_snapshots error: {exc}")
        return 0
    finally:
        try:
            if conn is not None:
                conn.close()
        except Exception:
            pass


def get_rolling_stats(days=14):
    """Compute rolling win rate, current streak, best/worst day from snapshots.

    Returns:
        dict with keys: total_bets, total_wins, total_losses, total_pushes,
                        win_rate, streak (int, positive=win streak, negative=loss streak),
                        best_day (dict), worst_day (dict), snapshots (list)
    """
    snapshots = load_daily_snapshots(days)
    if not snapshots:
        return {
            "total_bets": 0,
            "total_wins": 0,
            "total_losses": 0,
            "total_pushes": 0,
            "win_rate": 0.0,
            "streak": 0,
            "best_day": {},
            "worst_day": {},
            "snapshots": [],
        }

    total_bets = sum(s.get("total_picks", 0) for s in snapshots)
    total_wins = sum(s.get("wins", 0) for s in snapshots)
    total_losses = sum(s.get("losses", 0) for s in snapshots)
    total_pushes = sum(s.get("pushes", 0) for s in snapshots)
    win_rate = round(total_wins / max(total_wins + total_losses, 1) * 100, 1)

    # Current streak: walk from most recent snapshot backward
    streak = 0
    for snap in snapshots:
        w = snap.get("wins", 0)
        l = snap.get("losses", 0)
        if w + l == 0:
            continue
        day_wr = w / (w + l)
        if streak == 0:
            streak = 1 if day_wr >= 0.5 else -1
        elif streak > 0 and day_wr >= 0.5:
            streak += 1
        elif streak < 0 and day_wr < 0.5:
            streak -= 1
        else:
            break

    resolved = [s for s in snapshots if (s.get("wins", 0) + s.get("losses", 0)) > 0]
    best_day = max(resolved, key=lambda s: s.get("win_rate", 0)) if resolved else {}
    worst_day = min(resolved, key=lambda s: s.get("win_rate", 0)) if resolved else {}

    return {
        "total_bets": total_bets,
        "total_wins": total_wins,
        "total_losses": total_losses,
        "total_pushes": total_pushes,
        "win_rate": win_rate,
        "streak": streak,
        "best_day": best_day,
        "worst_day": worst_day,
        "snapshots": snapshots,
    }

# ============================================================
# END SECTION: Daily Snapshots
# ============================================================

# ============================================================
# SECTION: All Analysis Picks — Store and Load
# ============================================================

def insert_analysis_picks(analysis_results):
    """
    Persist all Neural Analysis output picks to the all_analysis_picks table.

    Deduplicates by (pick_date, player_name, stat_type, prop_line, direction)
    so re-running analysis does not create duplicate rows.

    Args:
        analysis_results (list[dict]): Full list of analysis result dicts from
            Neural Analysis (as stored in st.session_state["analysis_results"]).

    Returns:
        int: Number of new rows inserted.
    """
    if not analysis_results:
        return 0

    import datetime as _dt
    today_str = _dt.date.today().isoformat()
    inserted = 0

    for _attempt in range(_WRITE_RETRY_ATTEMPTS):
        try:
            with sqlite3.connect(str(DB_FILE_PATH), check_same_thread=False) as conn:
                conn.execute("PRAGMA journal_mode=WAL")
                # Load today's existing keys to deduplicate — use case-insensitive SQL collation
                existing = set()
                for row in conn.execute(
                    "SELECT lower(player_name), stat_type, prop_line, direction "
                    "FROM all_analysis_picks WHERE pick_date = ?",
                    (today_str,),
                ).fetchall():
                    existing.add((row[0], row[1], float(row[2] or 0), row[3]))

                for r in analysis_results:
                    key = (
                        r.get("player_name", "").lower(),
                        r.get("stat_type", ""),
                        float(r.get("line", 0) or 0),
                        r.get("direction", "OVER"),
                    )
                    if key in existing:
                        continue
                    conn.execute(
                        """
                        INSERT INTO all_analysis_picks
                            (pick_date, player_name, team, stat_type, prop_line,
                             direction, platform, confidence_score, probability_over,
                             edge_percentage, tier, result, actual_value, notes,
                             bet_type, std_devs_from_line)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL, ?, ?, ?)
                        """,
                        (
                            today_str,
                            r.get("player_name", ""),
                            r.get("player_team", r.get("team", "")),
                            r.get("stat_type", ""),
                            float(r.get("line", 0) or 0),
                            r.get("direction", "OVER"),
                            r.get("platform", ""),
                            float(r.get("confidence_score", 0) or 0),
                            float(r.get("probability_over", 0.5) or 0.5),
                            float(r.get("edge_percentage", 0) or 0),
                            r.get("tier", "Bronze"),
                            f"Auto-stored by Smart Pick Pro. SAFE Score: {r.get('confidence_score', 0):.0f}",
                            r.get("bet_type", "normal"),
                            float(r.get("std_devs_from_line", 0.0)),
                        ),
                    )
                    existing.add(key)
                    inserted += 1
                conn.commit()
            break  # success — exit retry loop
        except sqlite3.OperationalError as op_err:
            if "locked" in str(op_err).lower() and _attempt < _WRITE_RETRY_ATTEMPTS - 1:
                _logger.warning(
                    f"insert_analysis_picks: database locked, retry "
                    f"{_attempt + 1}/{_WRITE_RETRY_ATTEMPTS}"
                )
                time.sleep(_WRITE_RETRY_DELAY * (2 ** _attempt))
                # Reset: the fresh retry re-reads existing keys so
                # deduplication is re-applied and no duplicates arise.
                inserted = 0
                continue
            _logger.warning(f"insert_analysis_picks error (non-fatal): {op_err}")
        except Exception as err:
            _logger.warning(f"insert_analysis_picks error (non-fatal): {err}")
            break  # non-retryable error

    return inserted


def load_all_analysis_picks(days=30):
    """
    Load all Neural Analysis output picks from the database.

    Args:
        days (int): Number of days of history to load. Defaults to 30.

    Returns:
        list[dict]: List of pick dicts with columns as keys.
    """
    import datetime as _dt
    cutoff = (_dt.date.today() - _dt.timedelta(days=days)).isoformat()
    rows = []
    try:
        with sqlite3.connect(str(DB_FILE_PATH), check_same_thread=False) as conn:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.row_factory = sqlite3.Row
            cursor = conn.execute(
                "SELECT * FROM all_analysis_picks WHERE pick_date >= ? ORDER BY pick_date DESC, confidence_score DESC",
                (cutoff,),
            )
            rows = [dict(row) for row in cursor.fetchall()]
    except Exception as err:
        _logger.warning(f"load_all_analysis_picks error (non-fatal): {err}")
    return rows

def update_analysis_pick_result(pick_id, result, actual_value):
    """
    Write a WIN / LOSS / PUSH result back to a row in all_analysis_picks.

    Args:
        pick_id (int): Primary key of the row in all_analysis_picks.
        result (str): 'WIN', 'LOSS', or 'PUSH'.
        actual_value (float): The player's actual stat value.

    Returns:
        bool: True if a row was updated, False otherwise.
    """
    try:
        with sqlite3.connect(str(DB_FILE_PATH), check_same_thread=False) as conn:
            conn.execute("PRAGMA journal_mode=WAL")
            cursor = conn.execute(
                "UPDATE all_analysis_picks SET result = ?, actual_value = ? WHERE pick_id = ?",
                (result, float(actual_value), int(pick_id)),
            )
            conn.commit()
            return cursor.rowcount > 0
    except Exception as err:
        _logger.warning(f"update_analysis_pick_result error (non-fatal): {err}")
        return False


def load_pending_analysis_picks(limit=2000):
    """
    Load all rows from all_analysis_picks that have not yet been resolved
    (result IS NULL or result = '').

    Args:
        limit (int): Maximum rows to return.

    Returns:
        list[dict]: Pending pick rows as dicts.
    """
    rows = []
    try:
        with sqlite3.connect(str(DB_FILE_PATH), check_same_thread=False) as conn:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.row_factory = sqlite3.Row
            cursor = conn.execute(
                """SELECT * FROM all_analysis_picks
                   WHERE (result IS NULL OR result = '')
                   ORDER BY pick_date ASC
                   LIMIT ?""",
                (limit,),
            )
            rows = [dict(row) for row in cursor.fetchall()]
    except Exception as err:
        _logger.warning(f"load_pending_analysis_picks error (non-fatal): {err}")
    return rows


def load_analysis_picks_for_date(date_str):
    """
    Load ALL picks (resolved and pending) from all_analysis_picks for a
    specific date so users can review and re-resolve a past night's picks.

    Args:
        date_str (str): ISO date string "YYYY-MM-DD".

    Returns:
        list[dict]: Pick rows for that date ordered by confidence_score DESC.
    """
    rows = []
    try:
        with sqlite3.connect(str(DB_FILE_PATH), check_same_thread=False) as conn:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.row_factory = sqlite3.Row
            cursor = conn.execute(
                "SELECT * FROM all_analysis_picks WHERE pick_date = ? ORDER BY confidence_score DESC",
                (date_str,),
            )
            rows = [dict(row) for row in cursor.fetchall()]
    except Exception as err:
        _logger.warning(f"load_analysis_picks_for_date error (non-fatal): {err}")
    return rows


def get_analysis_pick_dates(days=30):
    """
    Return a sorted list (newest first) of distinct pick_date values in the
    all_analysis_picks table within the last *days* days.

    Args:
        days (int): How many days of history to scan. Defaults to 30.

    Returns:
        list[str]: ISO date strings, e.g. ["2026-03-13", "2026-03-12", ...].
    """
    import datetime as _dt
    cutoff = (_dt.date.today() - _dt.timedelta(days=days)).isoformat()
    dates = []
    try:
        with sqlite3.connect(str(DB_FILE_PATH), check_same_thread=False) as conn:
            conn.execute("PRAGMA journal_mode=WAL")
            rows = conn.execute(
                "SELECT DISTINCT pick_date FROM all_analysis_picks WHERE pick_date >= ? ORDER BY pick_date DESC",
                (cutoff,),
            ).fetchall()
            dates = [r[0] for r in rows]
    except Exception as err:
        _logger.warning(f"get_analysis_pick_dates error (non-fatal): {err}")
    return dates

# ============================================================
# END SECTION: All Analysis Picks
# ============================================================

# ============================================================
# SECTION: Analysis Session Persistence
# Saves and restores full Neural Analysis results to SQLite so
# users never lose their analysis after page refresh or inactivity.
# ============================================================

def save_analysis_session(analysis_results, todays_games=None, selected_picks=None):
    """
    Persist a full Neural Analysis session to SQLite.

    Args:
        analysis_results (list[dict]): Full analysis results list from Neural Analysis.
        todays_games (list[dict]|None): Tonight's games list.
        selected_picks (list[dict]|None): Currently selected picks.

    Returns:
        int: The new session_id, or -1 on error.
    """
    try:
        initialize_database()
        _ts = datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds")
        _results_json = json.dumps(analysis_results, default=str)
        _games_json = json.dumps(todays_games or [], default=str)
        _picks_json = json.dumps(selected_picks or [], default=str)
        _prop_count = len(analysis_results)
        cursor = _execute_write(
            """INSERT INTO analysis_sessions
               (analysis_timestamp, analysis_results_json, todays_games_json,
                selected_picks_json, prop_count)
               VALUES (?, ?, ?, ?, ?)""",
            (_ts, _results_json, _games_json, _picks_json, _prop_count),
            caller="save_analysis_session",
        )
        return cursor.lastrowid if cursor else -1
    except Exception as _err:
        _logger.warning(f"save_analysis_session error (non-fatal): {_err}")
        return -1


def load_latest_analysis_session():
    """
    Load the most recently saved Neural Analysis session from SQLite.

    Returns:
        dict|None: Session dict with keys:
            'analysis_timestamp', 'analysis_results', 'todays_games', 'selected_picks',
            'prop_count', 'created_at'
        Returns None if no session found or on error.
    """
    try:
        with sqlite3.connect(str(DB_FILE_PATH), check_same_thread=False) as conn:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.row_factory = sqlite3.Row
            cursor = conn.execute(
                """SELECT * FROM analysis_sessions
                   ORDER BY session_id DESC LIMIT 1"""
            )
            row = cursor.fetchone()
            if not row:
                return None
            row_dict = dict(row)
            # Deserialize JSON blobs
            try:
                row_dict["analysis_results"] = json.loads(row_dict.get("analysis_results_json") or "[]")
            except Exception:
                row_dict["analysis_results"] = []
            try:
                row_dict["todays_games"] = json.loads(row_dict.get("todays_games_json") or "[]")
            except Exception:
                row_dict["todays_games"] = []
            try:
                row_dict["selected_picks"] = json.loads(row_dict.get("selected_picks_json") or "[]")
            except Exception:
                row_dict["selected_picks"] = []
            return row_dict
    except Exception as _err:
        _logger.warning(f"load_latest_analysis_session error (non-fatal): {_err}")
        return None

# ============================================================
# END SECTION: Analysis Session Persistence
# ============================================================

# ============================================================
# SECTION: Backtest Results Persistence
# Saves and retrieves historical backtesting runs so results
# survive page reloads and can be compared across time.
# ============================================================

def save_backtest_result(backtest_result):
    """
    Persist a backtest run to the database.

    Args:
        backtest_result (dict): The dict returned by engine/backtester.run_backtest().

    Returns:
        int or None: The new backtest_id on success, None on error.

    Example:
        result = run_backtest(season="2024-25", stat_types=["points"])
        save_backtest_result(result)
    """
    if not backtest_result or backtest_result.get("status") != "ok":
        return None
    try:
        run_ts = datetime.datetime.now(datetime.timezone.utc).isoformat()
        stat_types_json = json.dumps(backtest_result.get("stat_types", []))
        tier_win_rates_json = json.dumps(backtest_result.get("tier_win_rates", {}))
        stat_win_rates_json = json.dumps(backtest_result.get("stat_win_rates", {}))
        edge_win_rates_json = json.dumps(backtest_result.get("edge_win_rates", {}))
        pick_log_json = json.dumps(backtest_result.get("pick_log", []))

        cursor = _execute_write(
            """
            INSERT INTO backtest_results (
                run_timestamp, season, stat_types_json, min_edge, tier_filter,
                total_picks, wins, losses, win_rate, roi, total_pnl,
                tier_win_rates_json, stat_win_rates_json, edge_win_rates_json,
                pick_log_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                run_ts,
                backtest_result.get("season", ""),
                stat_types_json,
                backtest_result.get("min_edge", 0.05),
                backtest_result.get("tier_filter"),
                backtest_result.get("total_picks", 0),
                backtest_result.get("wins", 0),
                backtest_result.get("losses", 0),
                backtest_result.get("win_rate", 0.0),
                backtest_result.get("roi", 0.0),
                backtest_result.get("total_pnl", 0.0),
                tier_win_rates_json,
                stat_win_rates_json,
                edge_win_rates_json,
                pick_log_json,
            ),
            caller="save_backtest_result",
        )
        return cursor.lastrowid if cursor else None
    except Exception as _err:
        _logger.error(f"save_backtest_result error: {_err}")
        return None


def load_backtest_results(limit=20):
    """
    Load the most recent backtest runs from the database.

    Args:
        limit (int): Maximum number of runs to return (default 20).

    Returns:
        list of dict: Recent backtest result rows, newest first.

    Example:
        results = load_backtest_results(limit=5)
        for r in results:
            print(r["season"], r["win_rate"])
    """
    try:
        with sqlite3.connect(str(DB_FILE_PATH), check_same_thread=False) as conn:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT * FROM backtest_results
                ORDER BY created_at DESC
                LIMIT ?
                """,
                (limit,)
            )
            rows = cursor.fetchall()
            results = []
            for row in rows:
                row_dict = dict(row)
                # Deserialize JSON fields — handle None, empty string, and invalid JSON
                for json_field in ("stat_types_json", "tier_win_rates_json",
                                   "stat_win_rates_json", "edge_win_rates_json",
                                   "pick_log_json"):
                    raw_value = row_dict.get(json_field)
                    if raw_value:
                        try:
                            row_dict[json_field] = json.loads(raw_value)
                        except (json.JSONDecodeError, TypeError):
                            row_dict[json_field] = None
                    else:
                        row_dict[json_field] = None
                results.append(row_dict)
            return results
    except Exception as _err:
        _logger.warning(f"load_backtest_results error (non-fatal): {_err}")
        return []

# ============================================================
# END SECTION: Backtest Results Persistence
# ============================================================


# ============================================================
# SECTION: Player Game Logs Persistence (Feature 12)
# Store and retrieve player game logs from SQLite so the KDE
# simulation engine has reliable data across browser refreshes.
# ============================================================

def save_player_game_logs_to_db(player_id, player_name, game_logs):
    """
    Persist a list of player game log rows to the player_game_logs table.

    Uses INSERT OR REPLACE so re-running the retrieval doesn't create
    duplicate rows (the UNIQUE constraint on player_id + game_date
    handles deduplication).

    Args:
        player_id (str): NBA API player ID
        player_name (str): Player display name
        game_logs (list[dict]): List of game log dicts with keys:
            game_date, opponent, minutes, points, rebounds, assists,
            threes, steals, blocks, turnovers, fg_pct, ft_pct, plus_minus

    Returns:
        int: Number of rows inserted/replaced.
    """
    if not game_logs:
        return 0

    import datetime as _dt
    retrieved_at = _dt.datetime.now(_dt.timezone.utc).isoformat()
    inserted = 0

    for _attempt in range(_WRITE_RETRY_ATTEMPTS):
        try:
            with sqlite3.connect(str(DB_FILE_PATH), check_same_thread=False) as conn:
                conn.execute("PRAGMA journal_mode=WAL")
                for g in game_logs:
                    conn.execute(
                        """
                        INSERT OR REPLACE INTO player_game_logs
                            (player_id, player_name, game_date, opponent,
                             minutes, points, rebounds, assists, threes,
                             steals, blocks, turnovers, fg_pct, ft_pct,
                             plus_minus, retrieved_at)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            str(player_id),
                            str(player_name),
                            str(g.get("game_date", g.get("GAME_DATE", ""))),
                            str(g.get("opponent", g.get("MATCHUP", ""))),
                            _safe_float(g.get("minutes", g.get("MIN", g.get("min")))),
                            _safe_int(g.get("points",    g.get("PTS", g.get("pts")))),
                            _safe_int(g.get("rebounds",  g.get("REB", g.get("reb")))),
                            _safe_int(g.get("assists",   g.get("AST", g.get("ast")))),
                            _safe_int(g.get("threes",    g.get("FG3M", g.get("fg3m")))),
                            _safe_int(g.get("steals",    g.get("STL", g.get("stl")))),
                            _safe_int(g.get("blocks",    g.get("BLK", g.get("blk")))),
                            _safe_int(g.get("turnovers", g.get("TOV", g.get("tov")))),
                            _safe_float(g.get("fg_pct",  g.get("FG_PCT", g.get("fg_pct")))),
                            _safe_float(g.get("ft_pct",  g.get("FT_PCT", g.get("ft_pct")))),
                            _safe_int(g.get("plus_minus", g.get("PLUS_MINUS", g.get("plus_minus")))),
                            retrieved_at,
                        ),
                    )
                    inserted += 1
                conn.commit()
            break  # success — exit retry loop
        except sqlite3.OperationalError as op_err:
            if "locked" in str(op_err).lower() and _attempt < _WRITE_RETRY_ATTEMPTS - 1:
                _logger.warning(
                    f"save_player_game_logs_to_db: database locked, retry "
                    f"{_attempt + 1}/{_WRITE_RETRY_ATTEMPTS}"
                )
                time.sleep(_WRITE_RETRY_DELAY * (2 ** _attempt))
                # Reset: INSERT OR REPLACE handles deduplication, so
                # re-inserting the same rows is idempotent.
                inserted = 0
                continue
            _logger.warning(f"save_player_game_logs_to_db error (non-fatal): {op_err}")
        except Exception as err:
            _logger.warning(f"save_player_game_logs_to_db error (non-fatal): {err}")
            break  # non-retryable error

    return inserted


def _safe_float(value, default=None):
    """Safely convert a value to float, returning default on failure."""
    if value is None:
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _safe_int(value, default=None):
    """Safely convert a value to int, returning default on failure."""
    if value is None:
        return default
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return default


def load_player_game_logs_from_db(player_id, days=60):
    """
    Load cached game logs for a player from SQLite.

    Args:
        player_id (str): NBA API player ID.
        days (int): How many days of history to return. Defaults to 60.

    Returns:
        list[dict]: Game log rows ordered most-recent-first.
            Returns empty list if no data or on error.
    """
    import datetime as _dt
    cutoff = (_dt.date.today() - _dt.timedelta(days=days)).isoformat()
    rows = []
    try:
        with sqlite3.connect(str(DB_FILE_PATH), check_same_thread=False) as conn:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.row_factory = sqlite3.Row
            cursor = conn.execute(
                """
                SELECT * FROM player_game_logs
                WHERE player_id = ? AND game_date >= ?
                ORDER BY game_date DESC
                """,
                (str(player_id), cutoff),
            )
            rows = [dict(row) for row in cursor.fetchall()]
    except Exception as err:
        _logger.warning(f"load_player_game_logs_from_db error (non-fatal): {err}")
    return rows


def is_game_log_cache_stale(player_id, max_age_hours=24):
    """
    Check whether the cached game logs for a player are stale.

    Args:
        player_id (str): NBA API player ID.
        max_age_hours (int): Age threshold in hours. Defaults to 24.

    Returns:
        bool: True if cache is missing or older than max_age_hours.
    """
    import datetime as _dt
    try:
        with sqlite3.connect(str(DB_FILE_PATH), check_same_thread=False) as conn:
            conn.execute("PRAGMA journal_mode=WAL")
            row = conn.execute(
                "SELECT MAX(retrieved_at) FROM player_game_logs WHERE player_id = ?",
                (str(player_id),),
            ).fetchone()
            if not row or not row[0]:
                return True
            latest_ts = _dt.datetime.fromisoformat(str(row[0]))
            # Ensure both sides are tz-aware (UTC) to avoid
            # "can't subtract offset-naive and offset-aware datetimes"
            now_utc = _dt.datetime.now(_dt.timezone.utc)
            if latest_ts.tzinfo is None:
                latest_ts = latest_ts.replace(tzinfo=_dt.timezone.utc)
            age_hours = (now_utc - latest_ts).total_seconds() / 3600.0
            return age_hours > max_age_hours
    except Exception:
        return True  # If we can't check, assume stale and re-retrieve


# ============================================================
# END SECTION: Player Game Logs Persistence
# ============================================================

# ============================================================
# END SECTION: Database CRUD Operations
# ============================================================
