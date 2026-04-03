"""engine/pipeline/step_5_evaluate.py – Phase 5: Evaluate prediction accuracy."""
import math
from utils.logger import get_logger

_logger = get_logger(__name__)


def run(context: dict) -> dict:
    """Compare predictions to actuals and log performance metrics.

    Args:
        context: Pipeline context with ``predictions`` key.

    Returns:
        Updated context with ``evaluation`` key.
    """
    predictions = context.get("predictions", [])
    actuals = context.get("actuals", [])

    evaluation = {"mae": None, "rmse": None, "n_evaluated": 0}

    if predictions and actuals:
        matched = []
        actual_map = {(a.get("player_name"), a.get("stat_type")): a.get("actual") for a in actuals}
        for pred in predictions:
            key = (pred.get("player_name"), pred.get("stat_type"))
            if key in actual_map and actual_map[key] is not None:
                matched.append((pred.get("prediction", 0), actual_map[key]))

        if matched:
            errors = [abs(p - a) for p, a in matched]
            sq_errors = [(p - a) ** 2 for p, a in matched]
            evaluation["mae"] = sum(errors) / len(errors)
            evaluation["rmse"] = math.sqrt(sum(sq_errors) / len(sq_errors))
            evaluation["n_evaluated"] = len(matched)

            try:
                from tracking.model_performance import log_prediction
                for pred, actual in matched:
                    log_prediction("pipeline", "unknown", pred, actual)
            except Exception as exc:
                _logger.debug("model_performance logging failed: %s", exc)

            _logger.info(
                "Evaluation: MAE=%.3f RMSE=%.3f n=%d",
                evaluation["mae"], evaluation["rmse"], evaluation["n_evaluated"]
            )
    else:
        _logger.info("No actuals available for evaluation (expected on prediction-only runs)")

    context["evaluation"] = evaluation
    return context
