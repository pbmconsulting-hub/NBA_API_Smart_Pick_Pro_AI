# tracking/model_performance.py
# ML model performance tracking using the existing smartpicks_tracker.db SQLite database.
# Provides functions to log model weights, predictions, and query best models by MAE.

import sqlite3
import logging
from pathlib import Path

_logger = logging.getLogger(__name__)

# Use the same DB as database.py
_DB_DIRECTORY = Path(__file__).resolve().parent.parent / "db"
_DB_FILE_PATH = _DB_DIRECTORY / "smartpicks_tracker.db"

_CREATE_MODEL_WEIGHTS_TABLE = """
CREATE TABLE IF NOT EXISTS model_weights (
    weight_id   INTEGER PRIMARY KEY AUTOINCREMENT,
    model_name  TEXT NOT NULL,
    weight      REAL NOT NULL,
    recorded_at TEXT DEFAULT (datetime('now'))
)"""

_CREATE_MODEL_PREDICTIONS_TABLE = """
CREATE TABLE IF NOT EXISTS model_predictions (
    pred_id     INTEGER PRIMARY KEY AUTOINCREMENT,
    model_name  TEXT NOT NULL,
    stat_type   TEXT NOT NULL,
    predicted   REAL NOT NULL,
    actual      REAL NOT NULL,
    abs_error   REAL GENERATED ALWAYS AS (ABS(predicted - actual)) VIRTUAL,
    recorded_at TEXT DEFAULT (datetime('now'))
)"""


def _get_connection() -> sqlite3.Connection:
    """Get a SQLite connection to the tracker DB."""
    _DB_DIRECTORY.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(_DB_FILE_PATH), check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.row_factory = sqlite3.Row
    return conn


def _ensure_tables() -> None:
    """Create model performance tables if they don't exist."""
    try:
        conn = _get_connection()
        conn.execute(_CREATE_MODEL_WEIGHTS_TABLE)
        conn.execute(_CREATE_MODEL_PREDICTIONS_TABLE)
        conn.commit()
        conn.close()
    except sqlite3.Error as err:
        _logger.error("model_performance: failed to create tables: %s", err)


def log_model_weight(model_name: str, weight: float) -> None:
    """Record a model's ensemble weight.

    Args:
        model_name: Name of the model (e.g. "ridge", "xgboost").
        weight: Inverse-variance weight assigned to this model.
    """
    _ensure_tables()
    try:
        conn = _get_connection()
        conn.execute(
            "INSERT INTO model_weights (model_name, weight) VALUES (?, ?)",
            (model_name, weight),
        )
        conn.commit()
        conn.close()
    except sqlite3.Error as err:
        _logger.error("log_model_weight error: %s", err)


def log_prediction(model_name: str, stat_type: str, predicted: float, actual: float) -> None:
    """Record a single prediction vs. actual result.

    Args:
        model_name: Name of the model that made the prediction.
        stat_type: Stat category (e.g. "pts", "reb", "ast").
        predicted: The model's predicted value.
        actual: The real observed value.
    """
    _ensure_tables()
    try:
        conn = _get_connection()
        conn.execute(
            "INSERT INTO model_predictions (model_name, stat_type, predicted, actual) VALUES (?, ?, ?, ?)",
            (model_name, stat_type, predicted, actual),
        )
        conn.commit()
        conn.close()
    except sqlite3.Error as err:
        _logger.error("log_prediction error: %s", err)


def get_best_model(stat_type: str) -> str:
    """Return the model name with the lowest mean absolute error for a stat type.

    Queries stored predictions and computes MAE per model. Falls back to
    ``"ensemble"`` if no data is available.

    Args:
        stat_type: Stat category to query (e.g. "pts").

    Returns:
        Model name string.
    """
    _ensure_tables()
    try:
        conn = _get_connection()
        row = conn.execute(
            """
            SELECT model_name, AVG(ABS(predicted - actual)) AS mae
            FROM model_predictions
            WHERE stat_type = ?
            GROUP BY model_name
            ORDER BY mae ASC
            LIMIT 1
            """,
            (stat_type,),
        ).fetchone()
        conn.close()
        if row:
            return row["model_name"]
    except sqlite3.Error as err:
        _logger.error("get_best_model error: %s", err)
    return "ensemble"
