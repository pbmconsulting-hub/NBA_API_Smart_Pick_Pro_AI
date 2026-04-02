"""engine/pipeline/step_1_ingest.py – Phase 1: Ingest raw NBA data."""
import datetime
import os
from utils.logger import get_logger

_logger = get_logger(__name__)
_RAW_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "data", "raw")


def run(context: dict) -> dict:
    """Fetch raw data from nba_data_service and save to data/raw/.

    Args:
        context: Pipeline context dict with at least ``date_str``.

    Returns:
        Updated context with ``raw_data`` key.
    """
    os.makedirs(_RAW_DIR, exist_ok=True)
    date_str = context.get("date_str", datetime.date.today().isoformat())
    raw_data = {}

    try:
        from data.nba_data_service import get_todays_games
        raw_data["todays_games"] = get_todays_games() or []
        _logger.info("Ingested %d today's games", len(raw_data["todays_games"]))
    except Exception as exc:
        _logger.debug("get_todays_games unavailable: %s", exc)
        raw_data["todays_games"] = []

    try:
        from data.nba_data_service import get_player_stats
        raw_data["player_stats"] = get_player_stats() or []
        _logger.info("Ingested %d player stat rows", len(raw_data["player_stats"]))
    except Exception as exc:
        _logger.debug("get_player_stats unavailable: %s", exc)
        raw_data["player_stats"] = []

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
