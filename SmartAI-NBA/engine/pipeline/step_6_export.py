"""engine/pipeline/step_6_export.py – Phase 6: Export predictions for app/API."""
import os
import json
import datetime
from utils.logger import get_logger

_logger = get_logger(__name__)
_EXPORT_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "data", "processed"
)


def run(context: dict) -> dict:
    """Export predictions as Parquet + JSON.

    Args:
        context: Pipeline context with ``predictions`` key.

    Returns:
        Updated context with ``export_paths`` key.
    """
    os.makedirs(_EXPORT_DIR, exist_ok=True)
    predictions = context.get("predictions", [])
    date_str = context.get("date_str", datetime.date.today().isoformat())
    export_paths = {}

    # JSON export (always available)
    json_path = os.path.join(_EXPORT_DIR, f"predictions_{date_str}.json")
    try:
        with open(json_path, "w", encoding="utf-8") as fh:
            json.dump(predictions, fh, indent=2, default=str)
        export_paths["json"] = json_path
        _logger.info("Exported %d predictions → %s", len(predictions), json_path)
    except Exception as exc:
        _logger.error("JSON export failed: %s", exc)

    # Parquet export (requires pyarrow/pandas)
    try:
        import pandas as pd
        from utils.parquet_helpers import save_parquet

        df = pd.DataFrame(predictions) if predictions else pd.DataFrame()
        parquet_path = os.path.join(_EXPORT_DIR, f"predictions_{date_str}.parquet")
        save_parquet(df, parquet_path)
        export_paths["parquet"] = parquet_path
        _logger.info("Parquet export → %s", parquet_path)
    except Exception as exc:
        _logger.debug("Parquet export skipped: %s", exc)

    context["export_paths"] = export_paths
    return context
