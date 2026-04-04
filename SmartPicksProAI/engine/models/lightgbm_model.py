"""engine/models/lightgbm_model.py – LightGBM model wrapper with graceful fallback."""
from utils.logger import get_logger
from engine.models.base_model import BaseModel

_logger = get_logger(__name__)

try:
    import lightgbm as lgb
    _LGBM_AVAILABLE = True
except ImportError:
    _LGBM_AVAILABLE = False
    _logger.debug("lightgbm not installed; LightGBMModel will be a no-op")


class LightGBMModel(BaseModel):
    """LightGBM regressor wrapper."""

    name = "lightgbm"

    def __init__(self, n_estimators: int = 300, max_depth: int = 6,
                 learning_rate: float = 0.05, num_leaves: int = 31,
                 subsample: float = 0.8, colsample_bytree: float = 0.8):
        self.n_estimators = n_estimators
        self.max_depth = max_depth
        self.learning_rate = learning_rate
        self.num_leaves = num_leaves
        self.subsample = subsample
        self.colsample_bytree = colsample_bytree
        self._model = None

    def train(self, X, y) -> None:
        """Train LightGBM model.

        Args:
            X: Feature matrix.
            y: Target vector.
        """
        if not _LGBM_AVAILABLE:
            _logger.warning("lightgbm not available; LightGBMModel.train is a no-op")
            return
        try:
            self._model = lgb.LGBMRegressor(
                n_estimators=self.n_estimators,
                max_depth=self.max_depth,
                learning_rate=self.learning_rate,
                num_leaves=self.num_leaves,
                subsample=self.subsample,
                colsample_bytree=self.colsample_bytree,
                random_state=42,
                verbosity=-1,
            )
            self._model.fit(X, y)
            _logger.info("LightGBMModel trained on %d samples", len(y))
        except Exception as exc:
            _logger.error("LightGBMModel.train failed: %s", exc)

    def predict(self, X):
        """Predict with LightGBM model.

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
            _logger.error("LightGBMModel.predict failed: %s", exc)
            try:
                import numpy as np
                return np.zeros(len(X))
            except ImportError:
                return [0.0] * len(X)
