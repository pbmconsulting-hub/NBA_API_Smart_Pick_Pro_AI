"""engine/models/train.py – Training script with season holdout validation."""
import os
from utils.logger import get_logger

_logger = get_logger(__name__)

_SAVED_DIR = os.path.join(os.path.dirname(__file__), "saved")
_ML_READY_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "data", "ml_ready"
)


def _load_ml_ready_data(stat_type: str = "pts"):
    """Load ML-ready Parquet files for a given stat type.

    Args:
        stat_type: Target stat column name.

    Returns:
        Tuple (X, y, feature_names) or (None, None, []) on failure.
    """
    try:
        import pandas as pd
        import glob as _glob

        files = sorted(_glob.glob(os.path.join(_ML_READY_DIR, "player_features_*.parquet")))
        if not files:
            _logger.warning("No ML-ready data found in %s", _ML_READY_DIR)
            return None, None, []

        dfs = []
        for f in files:
            try:
                dfs.append(pd.read_parquet(f))
            except Exception:
                try:
                    dfs.append(pd.read_csv(f.replace(".parquet", ".csv")))
                except Exception as exc:
                    _logger.debug("Could not load %s: %s", f, exc)

        if not dfs:
            return None, None, []

        df = pd.concat(dfs, ignore_index=True)

        if stat_type not in df.columns:
            _logger.warning("Stat column '%s' not in data", stat_type)
            return None, None, []

        feature_cols = [c for c in df.select_dtypes(include="number").columns if c != stat_type]
        df = df.dropna(subset=[stat_type] + feature_cols)

        X = df[feature_cols].values
        y = df[stat_type].values
        return X, y, feature_cols

    except Exception as exc:
        _logger.error("_load_ml_ready_data failed: %s", exc)
        return None, None, []


def train_models(stat_type: str = "pts") -> dict:
    """Train all available models with season holdout validation.

    Args:
        stat_type: The target stat column to train on.

    Returns:
        Dict of model name → performance metrics.
    """
    os.makedirs(_SAVED_DIR, exist_ok=True)

    X, y, feature_cols = _load_ml_ready_data(stat_type)
    if X is None or len(X) < 10:
        _logger.warning("Insufficient data for training (need ≥10 samples)")
        return {}

    # Season holdout: last 20% of data as validation
    split = int(len(X) * 0.8)
    X_train, X_val = X[:split], X[split:]
    y_train, y_val = y[:split], y[split:]

    from engine.models.ensemble import ModelEnsemble
    from engine.models.ridge_model import RidgeModel

    results = {}
    models_to_train = [RidgeModel(), ModelEnsemble()]

    for model in models_to_train:
        try:
            model.train(X_train, y_train)
            metrics = model.evaluate(X_val, y_val)
            results[model.name if hasattr(model, "name") else str(model)] = metrics

            save_path = os.path.join(_SAVED_DIR, f"{getattr(model, 'name', 'model')}_{stat_type}.joblib")
            model.save(save_path)

            try:
                from tracking.model_performance import log_prediction
                for pred, actual in zip(model.predict(X_val), y_val):
                    log_prediction(getattr(model, "name", "model"), stat_type, float(pred), float(actual))
            except Exception as exc:
                _logger.debug("Performance logging failed: %s", exc)

            _logger.info("Trained %s | MAE=%.3f RMSE=%.3f R²=%.3f",
                         getattr(model, "name", "model"), metrics["mae"], metrics["rmse"], metrics["r2"])
        except Exception as exc:
            _logger.error("Training failed for %s: %s", model, exc)

    return results


if __name__ == "__main__":
    for stat in ["pts", "reb", "ast", "stl", "blk", "tov", "fg3m", "ftm"]:
        train_models(stat)
