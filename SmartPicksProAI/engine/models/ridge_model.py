"""engine/models/ridge_model.py – Ridge regression wrapper."""
from utils.logger import get_logger
from engine.models.base_model import BaseModel

_logger = get_logger(__name__)

try:
    from sklearn.linear_model import Ridge
    from sklearn.preprocessing import StandardScaler
    _SKLEARN_AVAILABLE = True
except ImportError:
    _SKLEARN_AVAILABLE = False
    _logger.debug("scikit-learn not installed; RidgeModel will be a no-op")


class RidgeModel(BaseModel):
    """Ridge regression model with standard scaling."""

    name = "ridge"

    def __init__(self, alpha: float = 1.0):
        self.alpha = alpha
        self._model = None
        self._scaler = None

    def train(self, X, y) -> None:
        """Train Ridge regression.

        Args:
            X: Feature matrix.
            y: Target vector.
        """
        if not _SKLEARN_AVAILABLE:
            _logger.warning("scikit-learn not available; RidgeModel.train is a no-op")
            return
        try:
            import numpy as np
            self._scaler = StandardScaler()
            X_scaled = self._scaler.fit_transform(X)
            self._model = Ridge(alpha=self.alpha)
            self._model.fit(X_scaled, y)
            _logger.info("RidgeModel trained on %d samples", len(y))
        except Exception as exc:
            _logger.error("RidgeModel.train failed: %s", exc)

    def predict(self, X):
        """Predict with Ridge model.

        Args:
            X: Feature matrix.

        Returns:
            Array of predictions, or zeros if model not trained.
        """
        if self._model is None or self._scaler is None:
            try:
                import numpy as np
                return np.zeros(len(X))
            except ImportError:
                return [0.0] * len(X)
        try:
            X_scaled = self._scaler.transform(X)
            return self._model.predict(X_scaled)
        except Exception as exc:
            _logger.error("RidgeModel.predict failed: %s", exc)
            try:
                import numpy as np
                return np.zeros(len(X))
            except ImportError:
                return [0.0] * len(X)
