"""engine/pipeline/step_4_predict.py – Phase 4: Generate predictions."""
from utils.logger import get_logger

_logger = get_logger(__name__)

_MAX_PLAYERS = 50


def run(context: dict) -> dict:
    """Run predictions using saved ML models.

    Args:
        context: Pipeline context with ``feature_data``.

    Returns:
        Updated context with ``predictions`` key.
    """
    predictions = []
    feature_data = context.get("feature_data", {})

    try:
        from engine.predict.predictor import predict_player_stat

        player_df = feature_data.get("player_features")
        if player_df is not None:
            try:
                import pandas as pd
                df = pd.DataFrame(player_df) if isinstance(player_df, list) else player_df
                for _, row in df.head(context.get("max_players", _MAX_PLAYERS)).iterrows():
                    player_name = row.get("player_name") or row.get("name", "Unknown")
                    for stat in ["pts", "reb", "ast"]:
                        try:
                            result = predict_player_stat(str(player_name), stat, {})
                            predictions.append(result)
                        except Exception as exc:
                            _logger.debug("predict %s/%s failed: %s", player_name, stat, exc)
            except Exception as exc:
                _logger.debug("DataFrame iteration failed: %s", exc)
    except ImportError as exc:
        _logger.debug("predictor not available: %s", exc)

    _logger.info("Generated %d predictions", len(predictions))
    context["predictions"] = predictions
    return context
