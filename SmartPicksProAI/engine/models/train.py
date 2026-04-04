"""engine/models/train.py – Training script with walk-forward cross-validation.

Improvements over the original:
  • Walk-forward (expanding window) cross-validation for robust estimates.
  • Enforces date-sorted time-series split (no future leakage).
  • Logs feature importance from tree-based models.
  • Drops non-numeric / identifier columns before training.
"""
import os
from utils.logger import get_logger

_logger = get_logger(__name__)

_SAVED_DIR = os.path.join(os.path.dirname(__file__), "saved")
_ML_READY_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "data", "ml_ready"
)

# Columns that are identifiers or text — must not be used as features.
_DROP_COLS = {
    "player_name", "player_id", "game_id", "game_date", "wl", "min",
    "player_position", "player_team_abbrev", "home_abbrev", "away_abbrev",
    "matchup", "season", "home_team_id", "away_team_id", "opp_team_id",
    "l10",
}

# Walk-forward CV: minimum fold size and number of folds
_MIN_FOLD_SIZE = 100
_NUM_FOLDS = 5


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

        # Sort by date for time-series integrity
        if "game_date" in df.columns:
            df = df.sort_values("game_date").reset_index(drop=True)

        # Select numeric feature columns, excluding target and identifiers
        feature_cols = [
            c for c in df.select_dtypes(include="number").columns
            if c != stat_type and c.lower() not in _DROP_COLS
        ]
        df = df.dropna(subset=[stat_type])
        # Fill any remaining NaN in features with 0
        df[feature_cols] = df[feature_cols].fillna(0)

        X = df[feature_cols].values
        y = df[stat_type].values
        return X, y, feature_cols

    except Exception as exc:
        _logger.error("_load_ml_ready_data failed: %s", exc)
        return None, None, []


def _log_feature_importance(model, feature_cols: list, model_name: str, stat_type: str):
    """Log top-10 feature importances from a tree-based model."""
    try:
        importances = None
        inner = getattr(model, "_model", None)
        if inner is not None and hasattr(inner, "feature_importances_"):
            importances = inner.feature_importances_
        if importances is not None and len(importances) == len(feature_cols):
            import numpy as np
            indices = np.argsort(importances)[::-1][:10]
            top = [(feature_cols[i], round(float(importances[i]), 4)) for i in indices]
            _logger.info(
                "Feature importance (%s/%s): %s",
                model_name, stat_type,
                ", ".join(f"{name}={imp}" for name, imp in top),
            )
    except Exception as exc:
        _logger.debug("Feature importance logging failed: %s", exc)


def _walk_forward_splits(n_samples: int, num_folds: int, min_fold_size: int):
    """Generate walk-forward (expanding window) train/val index pairs.

    Args:
        n_samples: Total number of (date-sorted) samples.
        num_folds: Desired number of folds.
        min_fold_size: Minimum validation fold size.

    Yields:
        (train_end, val_start, val_end) index tuples.
    """
    # Reserve the last portion for validation folds
    fold_size = max(min_fold_size, n_samples // (num_folds + 2))
    # Ensure we have enough data for at least one fold
    first_train_end = n_samples - fold_size * num_folds
    if first_train_end < min_fold_size:
        # Fall back to a single 80/20 split
        split = int(n_samples * 0.8)
        yield 0, split, split, n_samples
        return

    for i in range(num_folds):
        train_end = first_train_end + i * fold_size
        val_start = train_end
        val_end = val_start + fold_size
        if val_end > n_samples:
            val_end = n_samples
        if val_start >= val_end:
            break
        yield 0, train_end, val_start, val_end


def train_models(stat_type: str = "pts") -> dict:
    """Train all available models with walk-forward cross-validation.

    Uses expanding-window CV: train on games 0..N, validate on N..N+fold,
    then expand the training window. Final model is trained on all data
    except the last fold, with the last fold used for reporting and
    ensemble weight computation.

    Args:
        stat_type: The target stat column to train on.

    Returns:
        Dict of model name → performance metrics.
    """
    os.makedirs(_SAVED_DIR, exist_ok=True)

    X, y, feature_cols = _load_ml_ready_data(stat_type)
    if X is None or len(X) < 10:
        _logger.warning("Insufficient data for training (need ≥10 samples, got %d)",
                        0 if X is None else len(X))
        return {}

    import numpy as np

    # Walk-forward cross-validation to get robust performance estimates
    fold_metrics: dict = {}  # model_name → list of per-fold metric dicts
    splits = list(_walk_forward_splits(len(X), _NUM_FOLDS, _MIN_FOLD_SIZE))

    if len(splits) > 1:
        _logger.info(
            "Walk-forward CV for %s: %d folds, %d total samples, %d features",
            stat_type, len(splits), len(X), len(feature_cols),
        )
        from engine.models.ensemble import ModelEnsemble
        from engine.models.ridge_model import RidgeModel

        for fold_idx, (tr_start, tr_end, val_start, val_end) in enumerate(splits):
            X_tr, y_tr = X[tr_start:tr_end], y[tr_start:tr_end]
            X_vl, y_vl = X[val_start:val_end], y[val_start:val_end]
            for ModelClass in [RidgeModel, ModelEnsemble]:
                model = ModelClass()
                try:
                    if isinstance(model, ModelEnsemble):
                        model.train(X_tr, y_tr, X_val=X_vl, y_val=y_vl)
                    else:
                        model.train(X_tr, y_tr)
                    metrics = model.evaluate(X_vl, y_vl)
                    fold_metrics.setdefault(model.name, []).append(metrics)
                except Exception as exc:
                    _logger.debug("Fold %d %s failed: %s", fold_idx, model.name, exc)

        # Log averaged walk-forward metrics
        for name, mlist in fold_metrics.items():
            avg_mae = np.mean([m["mae"] for m in mlist])
            avg_rmse = np.mean([m["rmse"] for m in mlist])
            avg_r2 = np.mean([m["r2"] for m in mlist])
            _logger.info(
                "Walk-forward avg (%s/%s): MAE=%.3f RMSE=%.3f R²=%.3f (%d folds)",
                name, stat_type, avg_mae, avg_rmse, avg_r2, len(mlist),
            )

    # Final training: use all data except last fold for training,
    # last fold for final evaluation and ensemble weight computation
    if splits:
        last_split = splits[-1]
        final_train_end = last_split[1]
    else:
        final_train_end = int(len(X) * 0.8)

    X_train, X_val = X[:final_train_end], X[final_train_end:]
    y_train, y_val = y[:final_train_end], y[final_train_end:]

    if len(X_val) == 0:
        # Fallback: 80/20 split
        split = int(len(X) * 0.8)
        X_train, X_val = X[:split], X[split:]
        y_train, y_val = y[:split], y[split:]

    _logger.info(
        "Final training %s models — %d train / %d val samples, %d features",
        stat_type, len(X_train), len(X_val), len(feature_cols),
    )

    from engine.models.ensemble import ModelEnsemble
    from engine.models.ridge_model import RidgeModel

    results = {}
    models_to_train = [RidgeModel(), ModelEnsemble()]

    for model in models_to_train:
        try:
            if isinstance(model, ModelEnsemble):
                model.train(X_train, y_train, X_val=X_val, y_val=y_val)
            else:
                model.train(X_train, y_train)
            metrics = model.evaluate(X_val, y_val)
            model_name = model.name if hasattr(model, "name") else str(model)
            results[model_name] = metrics

            save_path = os.path.join(_SAVED_DIR, f"{model_name}_{stat_type}.joblib")
            model.save(save_path)

            # Log feature importances for tree-based sub-models
            _log_feature_importance(model, feature_cols, model_name, stat_type)
            if hasattr(model, "models"):
                for sub in model.models:
                    _log_feature_importance(sub, feature_cols, sub.name, stat_type)

            try:
                from tracking.model_performance import log_prediction
                for pred, actual in zip(model.predict(X_val), y_val):
                    log_prediction(model_name, stat_type, float(pred), float(actual))
            except Exception as exc:
                _logger.debug("Performance logging failed: %s", exc)

            _logger.info("Trained %s | MAE=%.3f RMSE=%.3f R²=%.3f",
                         model_name, metrics["mae"], metrics["rmse"], metrics["r2"])
        except Exception as exc:
            _logger.error("Training failed for %s: %s", model, exc)

    return results


if __name__ == "__main__":
    for stat in ["pts", "reb", "ast", "stl", "blk", "tov", "fg3m", "ftm"]:
        train_models(stat)
