"""engine/pipeline/step_2_clean.py – Phase 2: Clean and normalize data."""
import os
import re
from utils.logger import get_logger

_logger = get_logger(__name__)
_PROCESSED_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "data", "processed"
)


def _normalize_columns(df):
    """Lower-case column names and replace spaces with underscores."""
    df.columns = [re.sub(r"\s+", "_", c.strip().lower()) for c in df.columns]
    return df


def run(context: dict) -> dict:
    """Load raw data, clean, and save to data/processed/.

    Args:
        context: Pipeline context with ``raw_data`` key.

    Returns:
        Updated context with ``clean_data`` key.
    """
    os.makedirs(_PROCESSED_DIR, exist_ok=True)
    raw_data = context.get("raw_data", {})
    clean_data = {}
    date_str = context.get("date_str", "unknown")

    try:
        import pandas as pd
        from utils.parquet_helpers import save_parquet

        for key, rows in raw_data.items():
            try:
                df = pd.DataFrame(rows) if isinstance(rows, list) else rows
                if df.empty:
                    clean_data[key] = df
                    continue

                df = _normalize_columns(df)
                # Drop fully-null columns
                df = df.dropna(axis=1, how="all")
                # Fill numeric nulls with 0
                num_cols = df.select_dtypes(include="number").columns
                df[num_cols] = df[num_cols].fillna(0)

                clean_data[key] = df
                path = os.path.join(_PROCESSED_DIR, f"{key}_{date_str}.parquet")
                save_parquet(df, path)
                _logger.debug("Cleaned %s → %d rows", key, len(df))
            except Exception as exc:
                _logger.debug("Error cleaning %s: %s", key, exc)
                clean_data[key] = rows
    except ImportError as exc:
        _logger.debug("pandas not available, skipping clean: %s", exc)
        clean_data = raw_data

    context["clean_data"] = clean_data
    return context
