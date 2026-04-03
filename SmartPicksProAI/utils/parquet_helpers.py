"""utils/parquet_helpers.py – Parquet I/O helpers with CSV fallback."""
import os
from utils.logger import get_logger

_logger = get_logger(__name__)


def save_parquet(df, path: str) -> None:
    """Save a DataFrame to Parquet; falls back to CSV if pyarrow is unavailable.

    Args:
        df: pandas DataFrame (or list-of-dicts that will be converted).
        path: Destination file path (will be created including parent dirs).
    """
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    try:
        if isinstance(df, list):
            import pandas as pd
            df = pd.DataFrame(df)
        try:
            df.to_parquet(path, index=False, engine="pyarrow")
            _logger.debug("Saved parquet: %s", path)
        except Exception:
            csv_path = path.replace(".parquet", ".csv")
            df.to_csv(csv_path, index=False)
            _logger.debug("Parquet unavailable; saved CSV fallback: %s", csv_path)
    except ImportError as exc:
        _logger.debug("pandas not available, skipping save: %s", exc)


def load_parquet(path: str):
    """Load a Parquet file into a DataFrame; tries CSV fallback.

    Args:
        path: Path to the Parquet file.

    Returns:
        pandas.DataFrame or empty list if unavailable.
    """
    try:
        import pandas as pd
        if os.path.exists(path):
            try:
                return pd.read_parquet(path, engine="pyarrow")
            except Exception:
                csv_path = path.replace(".parquet", ".csv")
                if os.path.exists(csv_path):
                    return pd.read_csv(csv_path)
        _logger.debug("File not found: %s", path)
        return pd.DataFrame()
    except ImportError as exc:
        _logger.debug("pandas not available: %s", exc)
        return []


def csv_to_parquet(csv_path: str, parquet_path: str) -> None:
    """One-time migration: convert a CSV file to Parquet.

    Args:
        csv_path: Source CSV file path.
        parquet_path: Destination Parquet file path.
    """
    try:
        import pandas as pd
        df = pd.read_csv(csv_path)
        save_parquet(df, parquet_path)
        _logger.info("Migrated %s → %s", csv_path, parquet_path)
    except Exception as exc:
        _logger.error("csv_to_parquet failed: %s", exc)
