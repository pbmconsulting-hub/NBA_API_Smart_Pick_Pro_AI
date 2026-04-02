"""engine/predict/predictor.py – Load saved models and generate predictions."""
import os
from utils.logger import get_logger

_logger = get_logger(__name__)

_SAVED_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "models", "saved")


def _load_best_model(stat_type: str):
    """Load the best available saved model for a stat type.

    Args:
        stat_type: Target stat (e.g. "pts").

    Returns:
        Loaded model instance, or None if not found.
    """
    try:
        import joblib
        from tracking.model_performance import get_best_model

        best_name = get_best_model(stat_type)
        path = os.path.join(_SAVED_DIR, f"{best_name}_{stat_type}.joblib")
        if os.path.exists(path):
            model = joblib.load(path)
            _logger.debug("Loaded model %s for %s", best_name, stat_type)
            return model

        # Fallback: try any available model
        for fname in os.listdir(_SAVED_DIR) if os.path.isdir(_SAVED_DIR) else []:
            if fname.endswith(f"_{stat_type}.joblib"):
                model = joblib.load(os.path.join(_SAVED_DIR, fname))
                _logger.debug("Loaded fallback model %s", fname)
                return model
    except Exception as exc:
        _logger.debug("_load_best_model failed: %s", exc)
    return None


def predict_player_stat(
    player_name: str,
    stat_type: str,
    game_context: dict,
) -> dict:
    """Generate a prediction for a player stat.

    Args:
        player_name: Player's full name.
        stat_type: Stat to predict (e.g. "pts", "reb", "ast").
        game_context: Dict with game context (rest_days, is_home, opponent_drtg, etc.).

    Returns:
        Dict with ``player_name``, ``stat_type``, ``prediction``, ``confidence_interval``.
    """
    result = {
        "player_name": player_name,
        "stat_type": stat_type,
        "prediction": None,
        "confidence_interval": None,
        "source": "unavailable",
    }

    try:
        from engine.features.feature_engineering import build_feature_matrix
        import numpy as np

        # Build a simple feature vector from context
        features = build_feature_matrix({}, {}, {}, game_context)
        X = np.array([list(features.values())], dtype=float)

        model = _load_best_model(stat_type)
        if model is not None:
            preds = model.predict(X)
            prediction = float(preds[0]) if hasattr(preds, "__len__") else float(preds)
            result["prediction"] = round(prediction, 2)
            result["confidence_interval"] = (
                round(prediction * 0.85, 2),
                round(prediction * 1.15, 2),
            )
            result["source"] = "ml_model"
        else:
            # Statistical fallback: return a baseline value
            _STAT_DEFAULTS = {"pts": 15.0, "reb": 5.0, "ast": 4.0, "stl": 1.0, "blk": 0.5}
            result["prediction"] = _STAT_DEFAULTS.get(stat_type, 5.0)
            result["confidence_interval"] = None
            result["source"] = "default_fallback"
    except Exception as exc:
        _logger.debug("predict_player_stat failed for %s/%s: %s", player_name, stat_type, exc)
        result["source"] = "error"

    return result
