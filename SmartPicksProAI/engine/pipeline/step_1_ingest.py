"""engine/pipeline/step_1_ingest.py – Phase 1: Ingest raw NBA data from smartpicks.db."""
import datetime
import os
import sqlite3
from utils.logger import get_logger

_logger = get_logger(__name__)
_RAW_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "data", "raw"
)
_DB_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "db", "smartpicks.db"
)


def _read_todays_games(db_path: str) -> list:
    """Read today's games from smartpicks.db."""
    today = datetime.date.today().isoformat()
    try:
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        cur.execute("SELECT * FROM Games WHERE game_date = ?", (today,))
        rows = [dict(r) for r in cur.fetchall()]
        conn.close()
        return rows
    except Exception as exc:
        _logger.debug("_read_todays_games failed: %s", exc)
        return []


def _read_player_stats(db_path: str) -> list:
    """Read recent player game logs from smartpicks.db."""
    try:
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        cur.execute("""
            SELECT p.full_name as player_name, pgl.*
            FROM Player_Game_Logs pgl
            JOIN Players p ON pgl.player_id = p.player_id
            ORDER BY pgl.game_id DESC
            LIMIT 2000
        """)
        rows = [dict(r) for r in cur.fetchall()]
        conn.close()
        return rows
    except Exception as exc:
        _logger.debug("_read_player_stats failed: %s", exc)
        return []


def run(context: dict) -> dict:
    """Fetch raw data from smartpicks.db and save to data/raw/.

    Args:
        context: Pipeline context dict with at least ``date_str``.

    Returns:
        Updated context with ``raw_data`` key.
    """
    os.makedirs(_RAW_DIR, exist_ok=True)
    date_str = context.get("date_str", datetime.date.today().isoformat())
    raw_data = {}

    raw_data["todays_games"] = _read_todays_games(_DB_PATH)
    _logger.info("Ingested %d today's games", len(raw_data["todays_games"]))

    raw_data["player_stats"] = _read_player_stats(_DB_PATH)
    _logger.info("Ingested %d player stat rows", len(raw_data["player_stats"]))

    # Persist raw data
    try:
        from utils.parquet_helpers import save_parquet
        import pandas as pd
        for key, rows in raw_data.items():
            if rows:
                ts = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%S")
                path = os.path.join(_RAW_DIR, f"{key}_{date_str}_{ts}.parquet")
                df = pd.DataFrame(rows) if isinstance(rows, list) else rows
                save_parquet(df, path)
                _logger.debug("Saved raw %s → %s", key, path)
    except Exception as exc:
        _logger.debug("Could not persist raw data: %s", exc)

    context["raw_data"] = raw_data
    return context
