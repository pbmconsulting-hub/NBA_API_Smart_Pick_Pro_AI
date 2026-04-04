"""engine/models/ensemble.py – Inverse-variance weighted ML ensemble.

NOTE: This is the NEW ML ensemble in engine/models/.
      The existing engine/ensemble.py is a separate betting ensemble and is NOT touched.
"""
from utils.logger import get_logger
from engine.models.base_model import BaseModel
from engine.models.ridge_model import RidgeModel
from engine.models.xgboost_model import XGBoostModel
from engine.models.catboost_model import CatBoostModel

_logger = get_logger(__name__)

try:
    from engine.models.lightgbm_model import LightGBMModel
    _LGBM_AVAILABLE = True
except Exception:
    _LGBM_AVAILABLE = False


class ModelEnsemble(BaseModel):
    """Inverse-variance weighted blend of all available ML models."""

    name = "ensemble"

    def __init__(self):
        self.models = [RidgeModel(), XGBoostModel(), CatBoostModel()]
        if _LGBM_AVAILABLE:
            self.models.append(LightGBMModel())
        self._weights: dict = {}
        self._variances: dict = {}

    def train(self, X, y, X_val=None, y_val=None) -> None:
        """Train all sub-models and compute inverse-variance weights.

        Weights are computed on held-out validation data when provided,
        preventing optimistic RMSE estimates from evaluating on training data.

        Args:
            X: Training feature matrix.
            y: Training target vector.
            X_val: Optional validation feature matrix for weight computation.
            y_val: Optional validation target vector for weight computation.
        """
        for model in self.models:
            try:
                model.train(X, y)
                # Evaluate on validation data if available, else fall back to train
                eval_X = X_val if X_val is not None else X
                eval_y = y_val if y_val is not None else y
                metrics = model.evaluate(eval_X, eval_y)
                variance = metrics["rmse"] ** 2 if metrics["rmse"] > 0 else 1e-6
                self._variances[model.name] = variance
                _logger.info("Trained %s → RMSE=%.4f (eval on %s)",
                             model.name, metrics["rmse"],
                             "val" if X_val is not None else "train")
            except Exception as exc:
                _logger.error("Ensemble train failed for %s: %s", model.name, exc)
                self._variances[model.name] = 1.0

        # Compute inverse-variance weights
        total_inv = sum(1.0 / v for v in self._variances.values() if v > 0)
        if total_inv > 0:
            self._weights = {
                name: (1.0 / var) / total_inv
                for name, var in self._variances.items()
                if var > 0
            }
        else:
            n = len(self.models)
            self._weights = {m.name: 1.0 / n for m in self.models}

        _logger.info("Ensemble weights: %s", self._weights)

        # Log weights to model_performance tracker
        try:
            from tracking.model_performance import log_model_weight
            for model_name, weight in self._weights.items():
                log_model_weight(model_name, weight)
        except Exception as exc:
            _logger.debug("log_model_weight failed: %s", exc)

    def predict(self, X):
        """Weighted blend prediction.

        Args:
            X: Feature matrix.

        Returns:
            Array of blended predictions.
        """
        try:
            import numpy as np
            blend = None
            for model in self.models:
                w = self._weights.get(model.name, 1.0 / len(self.models))
                preds = np.array(model.predict(X), dtype=float)
                if blend is None:
                    blend = w * preds
                else:
                    blend += w * preds
            return blend if blend is not None else np.zeros(len(X))
        except ImportError:
            # numpy not available: simple average
            all_preds = []
            for model in self.models:
                try:
                    all_preds.append(list(model.predict(X)))
                except Exception:
                    pass
            if not all_preds:
                return [0.0] * len(X)
            return [sum(row) / len(row) for row in zip(*all_preds)]

    def get_weights(self) -> dict:
        """Return current model weights.

        Returns:
            Dict mapping model name to weight.
        """
        return dict(self._weights)
