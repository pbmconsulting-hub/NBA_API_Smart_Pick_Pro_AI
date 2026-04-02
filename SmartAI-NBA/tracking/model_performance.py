"""tracking/model_performance.py – Per-model performance tracking."""
import math
import threading
from collections import defaultdict
from utils.logger import get_logger

_logger = get_logger(__name__)

_lock = threading.Lock()

# Storage: {model_name: {stat_type: [{"predicted": ..., "actual": ...}, ...]}}
_predictions: dict = defaultdict(lambda: defaultdict(list))

# Weight history: {model_name: [{"weight": float, "ts": str}]}
_weight_history: dict = defaultdict(list)


def log_prediction(
    model_name: str,
    stat_type: str,
    predicted: float,
    actual: float,
) -> None:
    """Record a single prediction vs actual for a model.

    Args:
        model_name: Name of the model (e.g. "ridge", "xgboost", "ensemble").
        stat_type: Stat type (e.g. "pts", "reb").
        predicted: Predicted value.
        actual: Actual observed value.
    """
    with _lock:
        _predictions[model_name][stat_type].append(
            {"predicted": float(predicted), "actual": float(actual)}
        )


def _compute_metrics(records: list) -> dict:
    """Compute MAE, RMSE, R² from a list of prediction records.

    Args:
        records: List of dicts with ``predicted`` and ``actual`` keys.

    Returns:
        Dict with ``mae``, ``rmse``, ``r2``, ``n``.
    """
    if not records:
        return {"mae": None, "rmse": None, "r2": None, "n": 0}

    errors = [abs(r["predicted"] - r["actual"]) for r in records]
    sq_errors = [(r["predicted"] - r["actual"]) ** 2 for r in records]
    mae = sum(errors) / len(errors)
    rmse = math.sqrt(sum(sq_errors) / len(sq_errors))

    mean_actual = sum(r["actual"] for r in records) / len(records)
    ss_tot = sum((r["actual"] - mean_actual) ** 2 for r in records)
    ss_res = sum(sq_errors)
    r2 = 1.0 - ss_res / ss_tot if ss_tot > 0 else 0.0

    return {"mae": round(mae, 4), "rmse": round(rmse, 4), "r2": round(r2, 4), "n": len(records)}


def get_model_stats(model_name: str = None) -> dict:
    """Retrieve performance stats for one or all models.

    Args:
        model_name: If given, return stats for only that model. Otherwise all.

    Returns:
        Nested dict: {model: {stat_type: {mae, rmse, r2, n}}}.
    """
    with _lock:
        models = [model_name] if model_name else list(_predictions.keys())
        result = {}
        for m in models:
            result[m] = {}
            for stat, records in _predictions[m].items():
                result[m][stat] = _compute_metrics(records)
        return result


def get_best_model(stat_type: str) -> str:
    """Return the model with the lowest RMSE for a given stat type.

    Args:
        stat_type: Stat type to compare (e.g. "pts").

    Returns:
        Model name with best RMSE, or "ridge" as default.
    """
    with _lock:
        best_name = "ridge"
        best_rmse = float("inf")
        for model_name, stats in _predictions.items():
            if stat_type in stats:
                metrics = _compute_metrics(stats[stat_type])
                if metrics["rmse"] is not None and metrics["rmse"] < best_rmse:
                    best_rmse = metrics["rmse"]
                    best_name = model_name
        return best_name


def log_model_weight(model_name: str, weight: float) -> None:
    """Record a model weight at the current time.

    Args:
        model_name: Model identifier.
        weight: Weight assigned to the model.
    """
    import datetime
    with _lock:
        _weight_history[model_name].append(
            {"weight": float(weight), "ts": datetime.datetime.now(datetime.timezone.utc).isoformat()}
        )


def get_weight_history(model_name: str = None) -> dict:
    """Return model weight history.

    Args:
        model_name: If given, return history for that model. Otherwise all.

    Returns:
        Dict mapping model name to list of weight history entries.
    """
    with _lock:
        if model_name:
            return {model_name: list(_weight_history.get(model_name, []))}
        return {k: list(v) for k, v in _weight_history.items()}
