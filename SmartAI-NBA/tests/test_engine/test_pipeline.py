"""tests/test_engine/test_pipeline.py – Unit tests for engine/pipeline modules."""
import pytest


class TestPipelineSteps:
    def test_step_1_ingest_runs(self):
        from engine.pipeline.step_1_ingest import run
        ctx = run({"date_str": "2025-01-01"})
        assert "raw_data" in ctx
        assert isinstance(ctx["raw_data"], dict)

    def test_step_2_clean_runs(self):
        from engine.pipeline.step_2_clean import run
        ctx = run({"date_str": "2025-01-01", "raw_data": {"player_stats": []}})
        assert "clean_data" in ctx

    def test_step_3_features_runs(self):
        from engine.pipeline.step_3_features import run
        ctx = run({"date_str": "2025-01-01", "clean_data": {}})
        assert "feature_data" in ctx

    def test_step_4_predict_runs(self):
        from engine.pipeline.step_4_predict import run
        ctx = run({"date_str": "2025-01-01", "feature_data": {}})
        assert "predictions" in ctx
        assert isinstance(ctx["predictions"], list)

    def test_step_5_evaluate_no_actuals(self):
        from engine.pipeline.step_5_evaluate import run
        ctx = run({"predictions": [], "actuals": []})
        assert "evaluation" in ctx
        assert ctx["evaluation"]["n_evaluated"] == 0

    def test_step_5_evaluate_with_actuals(self):
        from engine.pipeline.step_5_evaluate import run
        preds = [{"player_name": "Test Player", "stat_type": "pts", "prediction": 20.0}]
        actuals = [{"player_name": "Test Player", "stat_type": "pts", "actual": 22.0}]
        ctx = run({"predictions": preds, "actuals": actuals})
        assert ctx["evaluation"]["mae"] == pytest.approx(2.0)

    def test_step_6_export_runs(self, tmp_path, monkeypatch):
        import engine.pipeline.step_6_export as step6
        monkeypatch.setattr(step6, "_EXPORT_DIR", str(tmp_path))
        ctx = step6.run({"date_str": "2025-01-01", "predictions": [{"foo": "bar"}]})
        assert "export_paths" in ctx
        assert "json" in ctx["export_paths"]


class TestRunPipeline:
    def test_run_full_pipeline_returns_context(self):
        from engine.pipeline.run_pipeline import run_full_pipeline
        ctx = run_full_pipeline(date_str="2025-01-01")
        assert isinstance(ctx, dict)
        assert "date_str" in ctx
        assert ctx["date_str"] == "2025-01-01"

    def test_run_full_pipeline_has_errors_key(self):
        from engine.pipeline.run_pipeline import run_full_pipeline
        ctx = run_full_pipeline(date_str="2025-01-01")
        assert "errors" in ctx
        assert isinstance(ctx["errors"], list)
