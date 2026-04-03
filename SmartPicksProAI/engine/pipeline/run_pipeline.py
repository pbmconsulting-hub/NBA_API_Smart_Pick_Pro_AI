"""engine/pipeline/run_pipeline.py – orchestrates the 6-phase numbered pipeline."""
import time
import datetime
from utils.logger import get_logger

_logger = get_logger(__name__)

STEPS = [
    ("step_1_ingest",   "engine.pipeline.step_1_ingest"),
    ("step_2_clean",    "engine.pipeline.step_2_clean"),
    ("step_3_features", "engine.pipeline.step_3_features"),
    ("step_4_predict",  "engine.pipeline.step_4_predict"),
    ("step_5_evaluate", "engine.pipeline.step_5_evaluate"),
    ("step_6_export",   "engine.pipeline.step_6_export"),
]


def run_full_pipeline(date_str=None):
    """Run the full 6-phase prediction pipeline.

    Args:
        date_str (str | None): Date in YYYY-MM-DD format; defaults to today.

    Returns:
        dict: Final pipeline context after all steps.
    """
    if date_str is None:
        date_str = datetime.date.today().isoformat()

    context = {"date_str": date_str, "errors": []}
    _logger.info("=== Pipeline START — date=%s ===", date_str)
    pipeline_start = time.perf_counter()

    for step_name, module_path in STEPS:
        step_start = time.perf_counter()
        try:
            import importlib
            mod = importlib.import_module(module_path)
            context = mod.run(context)
            elapsed = time.perf_counter() - step_start
            _logger.info("  [OK] %s completed in %.2fs", step_name, elapsed)
        except Exception as exc:
            elapsed = time.perf_counter() - step_start
            _logger.error("  [FAIL] %s failed after %.2fs: %s", step_name, elapsed, exc)
            context.setdefault("errors", []).append({"step": step_name, "error": str(exc)})

    total = time.perf_counter() - pipeline_start
    _logger.info("=== Pipeline END — total=%.2fs errors=%d ===", total, len(context.get("errors", [])))
    return context


if __name__ == "__main__":
    run_full_pipeline()
