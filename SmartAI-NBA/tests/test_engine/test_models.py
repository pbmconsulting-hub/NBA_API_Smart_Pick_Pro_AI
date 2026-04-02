"""tests/test_engine/test_models.py – Unit tests for engine/models modules."""
import pytest


class TestBaseModel:
    def test_base_model_is_abstract(self):
        from engine.models.base_model import BaseModel
        with pytest.raises(TypeError):
            BaseModel()

    def test_base_model_evaluate(self):
        from engine.models.base_model import BaseModel

        class DummyModel(BaseModel):
            name = "dummy"
            def train(self, X, y): pass
            def predict(self, X): return [float(x[0]) for x in X]

        model = DummyModel()
        X = [[1.0], [2.0], [3.0]]
        y = [1.0, 2.0, 3.0]
        metrics = model.evaluate(X, y)
        assert metrics["mae"] == 0.0
        assert metrics["rmse"] == 0.0


class TestRidgeModel:
    def test_ridge_model_predict_untrained(self):
        from engine.models.ridge_model import RidgeModel
        model = RidgeModel()
        result = model.predict([[1, 2], [3, 4]])
        assert len(result) == 2

    def test_ridge_model_train_predict(self):
        pytest.importorskip("sklearn")
        from engine.models.ridge_model import RidgeModel
        import numpy as np
        model = RidgeModel()
        X = np.array([[i, i * 2] for i in range(20)])
        y = np.array([i * 3.0 for i in range(20)])
        model.train(X, y)
        preds = model.predict(X[:5])
        assert len(preds) == 5

    def test_ridge_model_has_name(self):
        from engine.models.ridge_model import RidgeModel
        assert RidgeModel.name == "ridge"


class TestXGBoostModel:
    def test_xgboost_model_predict_untrained(self):
        from engine.models.xgboost_model import XGBoostModel
        model = XGBoostModel()
        result = model.predict([[1, 2], [3, 4]])
        assert len(result) == 2

    def test_xgboost_model_has_name(self):
        from engine.models.xgboost_model import XGBoostModel
        assert XGBoostModel.name == "xgboost"


class TestCatBoostModel:
    def test_catboost_model_predict_untrained(self):
        from engine.models.catboost_model import CatBoostModel
        model = CatBoostModel()
        result = model.predict([[1, 2], [3, 4]])
        assert len(result) == 2

    def test_catboost_model_has_name(self):
        from engine.models.catboost_model import CatBoostModel
        assert CatBoostModel.name == "catboost"


class TestModelEnsemble:
    def test_ensemble_get_weights_default(self):
        from engine.models.ensemble import ModelEnsemble
        ens = ModelEnsemble()
        weights = ens.get_weights()
        assert isinstance(weights, dict)

    def test_ensemble_predict_untrained(self):
        from engine.models.ensemble import ModelEnsemble
        ens = ModelEnsemble()
        result = ens.predict([[1, 2], [3, 4]])
        assert len(result) == 2

    def test_ensemble_inherits_base_model(self):
        from engine.models.ensemble import ModelEnsemble
        from engine.models.base_model import BaseModel
        assert issubclass(ModelEnsemble, BaseModel)

    def test_ensemble_has_name(self):
        from engine.models.ensemble import ModelEnsemble
        assert ModelEnsemble.name == "ensemble"

    def test_ensemble_has_evaluate(self):
        from engine.models.ensemble import ModelEnsemble
        ens = ModelEnsemble()
        assert hasattr(ens, "evaluate") and callable(ens.evaluate)

    def test_ensemble_has_save(self):
        from engine.models.ensemble import ModelEnsemble
        ens = ModelEnsemble()
        assert hasattr(ens, "save") and callable(ens.save)

    def test_ensemble_train_logs_weights(self):
        """train() must call log_model_weight for each sub-model weight."""
        pytest.importorskip("sklearn")
        import numpy as np
        from engine.models.ensemble import ModelEnsemble
        from tracking import model_performance as mp
        # Record weight history size before
        before = {k: len(v) for k, v in mp._weight_history.items()}
        ens = ModelEnsemble()
        X = np.array([[float(i)] for i in range(10)])
        y = np.array([float(i) for i in range(10)])
        ens.train(X, y)
        # At least one new weight entry must have been logged
        after_total = sum(len(v) for v in mp._weight_history.values())
        before_total = sum(before.values())
        assert after_total > before_total, "train() did not call log_model_weight"


class TestModelPerformance:
    def test_log_and_retrieve_prediction(self):
        from tracking.model_performance import log_prediction, get_model_stats
        log_prediction("test_model", "pts", 25.0, 27.0)
        stats = get_model_stats("test_model")
        assert "test_model" in stats
        assert "pts" in stats["test_model"]
        assert stats["test_model"]["pts"]["n"] >= 1

    def test_get_best_model_default(self):
        from tracking.model_performance import get_best_model
        result = get_best_model("pts")
        assert isinstance(result, str)

    def test_log_weight_history(self):
        from tracking.model_performance import log_model_weight, get_weight_history
        log_model_weight("ridge", 0.5)
        hist = get_weight_history("ridge")
        assert "ridge" in hist
        assert len(hist["ridge"]) >= 1
