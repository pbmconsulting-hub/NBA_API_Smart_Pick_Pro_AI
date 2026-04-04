# engine/models/saved/

This directory stores **trained model artifacts** (`.joblib` files).

## How to generate models

Run the training script from the SmartPicksProAI root:

```bash
cd SmartPicksProAI
python -m engine.models.train
```

This trains **Ridge** and **ModelEnsemble** (XGBoost + CatBoost + Ridge) for
each of the 8 stat types: `pts`, `reb`, `ast`, `stl`, `blk`, `tov`, `fg3m`, `ftm`.

### Prerequisites

1. **ML-ready data** must exist in `data/ml_ready/` (Parquet files produced by
   the pipeline's step 3).
2. Run the full pipeline first if you haven't:
   ```bash
   python -m engine.pipeline.run_pipeline
   ```

## Expected output files

After training completes, you should see files like:

```
Ridge_pts.joblib
Ridge_reb.joblib
ModelEnsemble_pts.joblib
ModelEnsemble_reb.joblib
...
```

## Notes

- Models use a **date-sorted 80/20 time-series split** (no future leakage).
- Tree-based models log top-10 feature importances during training.
- The `.gitignore` excludes `*.joblib` files — models are not committed to
  version control. Each contributor must train locally.
