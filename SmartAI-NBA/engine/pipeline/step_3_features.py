"""engine/pipeline/step_3_features.py – Phase 3: Feature engineering."""
import os
from utils.logger import get_logger

_logger = get_logger(__name__)
_ML_READY_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "data", "ml_ready"
)


def run(context: dict) -> dict:
    """Compute derived features and save ML-ready data.

    Args:
        context: Pipeline context with ``clean_data`` key.

    Returns:
        Updated context with ``feature_data`` key.
    """
    os.makedirs(_ML_READY_DIR, exist_ok=True)
    clean_data = context.get("clean_data", {})
    date_str = context.get("date_str", "unknown")
    feature_data = {}

    try:
        import pandas as pd
        from utils.parquet_helpers import save_parquet

        player_df = clean_data.get("player_stats")
        if player_df is not None and not (hasattr(player_df, "empty") and player_df.empty):
            df = pd.DataFrame(player_df) if isinstance(player_df, list) else player_df.copy()
            try:
                from engine.features.player_metrics import calculate_true_shooting
                if all(c in df.columns for c in ["pts", "fga", "fta"]):
                    df["ts_pct"] = df.apply(
                        lambda r: calculate_true_shooting(r["pts"], r["fga"], r["fta"]), axis=1
                    )
            except Exception as exc:
                _logger.debug("TS% feature failed: %s", exc)

            feature_data["player_features"] = df
            path = os.path.join(_ML_READY_DIR, f"player_features_{date_str}.parquet")
            save_parquet(df, path)
            _logger.info("Saved player features → %d rows", len(df))
    except Exception as exc:
        _logger.debug("Feature engineering error: %s", exc)
        feature_data = clean_data

    context["feature_data"] = feature_data
    return context
