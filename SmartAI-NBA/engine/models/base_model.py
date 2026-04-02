"""engine/models/base_model.py – Abstract base class for all ML models."""
import abc
from utils.logger import get_logger

_logger = get_logger(__name__)


class BaseModel(abc.ABC):
    """Abstract base class for all SmartAI-NBA prediction models."""

    name: str = "base"

    @abc.abstractmethod
    def train(self, X, y) -> None:
        """Train the model on features X and target y.

        Args:
            X: Feature matrix (array-like or DataFrame).
            y: Target vector.
        """

    @abc.abstractmethod
    def predict(self, X):
        """Generate predictions for feature matrix X.

        Args:
            X: Feature matrix.

        Returns:
            Array of predictions.
        """

    def evaluate(self, X, y) -> dict:
        """Compute MAE, RMSE, and R² on held-out data.

        Args:
            X: Feature matrix.
            y: True target values.

        Returns:
            Dict with keys ``mae``, ``rmse``, ``r2``.
        """
        import math
        preds = self.predict(X)
        try:
            import numpy as np
            y_arr = np.array(y, dtype=float)
            p_arr = np.array(preds, dtype=float)
            mae = float(np.mean(np.abs(y_arr - p_arr)))
            rmse = float(np.sqrt(np.mean((y_arr - p_arr) ** 2)))
            ss_res = float(np.sum((y_arr - p_arr) ** 2))
            ss_tot = float(np.sum((y_arr - np.mean(y_arr)) ** 2))
            r2 = 1.0 - ss_res / ss_tot if ss_tot > 0 else 0.0
        except ImportError:
            errors = [abs(float(p) - float(a)) for p, a in zip(preds, y)]
            mae = sum(errors) / len(errors) if errors else 0.0
            sq = [(float(p) - float(a)) ** 2 for p, a in zip(preds, y)]
            rmse = math.sqrt(sum(sq) / len(sq)) if sq else 0.0
            r2 = 0.0
        return {"mae": mae, "rmse": rmse, "r2": r2}

    def save(self, path: str) -> None:
        """Persist model to disk using joblib.

        Args:
            path: File path (e.g. ``engine/models/saved/ridge.joblib``).
        """
        try:
            import joblib
            import os
            os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
            joblib.dump(self, path)
            _logger.info("Saved model %s → %s", self.name, path)
        except Exception as exc:
            _logger.error("save failed for %s: %s", self.name, exc)

    def load(self, path: str) -> "BaseModel":
        """Load model from disk.

        Args:
            path: File path.

        Returns:
            Loaded model instance.
        """
        try:
            import joblib
            return joblib.load(path)
        except Exception as exc:
            _logger.error("load failed for %s: %s", path, exc)
            return self
