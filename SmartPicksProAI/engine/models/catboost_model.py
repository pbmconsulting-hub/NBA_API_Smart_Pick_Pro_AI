"""engine/models/catboost_model.py – CatBoost model wrapper with graceful fallback."""
from utils.logger import get_logger
from engine.models.base_model import BaseModel

_logger = get_logger(__name__)

try:
    from catboost import CatBoostRegressor
    _CATBOOST_AVAILABLE = True
except ImportError:
    _CATBOOST_AVAILABLE = False
    _logger.debug("catboost not installed; CatBoostModel will be a no-op")


class CatBoostModel(BaseModel):
    """CatBoost regressor wrapper."""

    name = "catboost"

    def __init__(self, iterations: int = 200, depth: int = 6, learning_rate: float = 0.05):
        self.iterations = iterations
        self.depth = depth
        self.learning_rate = learning_rate
        self._model = None

    def train(self, X, y) -> None:
        """Train CatBoost model.

        Args:
            X: Feature matrix.
            y: Target vector.
        """
        if not _CATBOOST_AVAILABLE:
            _logger.warning("catboost not available; CatBoostModel.train is a no-op")
            return
        try:
            self._model = CatBoostRegressor(
                iterations=self.iterations,
                depth=self.depth,
                learning_rate=self.learning_rate,
                random_seed=42,
                verbose=0,
            )
            self._model.fit(X, y)
            _logger.info("CatBoostModel trained on %d samples", len(y))
        except Exception as exc:
            _logger.error("CatBoostModel.train failed: %s", exc)

    def predict(self, X):
        """Predict with CatBoost model.

        Args:
            X: Feature matrix.

        Returns:
            Array of predictions, or zeros if model not trained.
        """
        if self._model is None:
            try:
                import numpy as np
                return np.zeros(len(X))
            except ImportError:
                return [0.0] * len(X)
        try:
            return self._model.predict(X)
        except Exception as exc:
            _logger.error("CatBoostModel.predict failed: %s", exc)
            try:
                import numpy as np
                return np.zeros(len(X))
            except ImportError:
                return [0.0] * len(X)
