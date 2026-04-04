# data/ml_ready/

This directory stores **feature-engineered** data produced by
`engine/pipeline/step_3_features.py`, ready for model training.

## Contents after running the pipeline

| File pattern | Description |
|---|---|
| `player_features_YYYY-MM-DD.parquet` | ~180-column feature matrix with rolling averages, form ratios, rest days, pace/defense adjustments, DvP multipliers |

## Key feature groups

- **Rolling averages / std**: 3, 5, 10, 20-game windows for all stats
- **Form ratios**: Recent performance relative to season average
- **Rest / schedule**: Days of rest, back-to-back flag
- **Pace adjustments**: Team pace relative to league average
- **Defense adjustments**: Opponent DvP multipliers
- **Position matchup**: DvP multipliers by opponent × position

## Sample fixture

The file `sample_player_features.csv` is a minimal fixture (5 rows, subset of
columns) so contributors can verify model training without the full pipeline.

To generate real data, run the full pipeline:

```bash
cd SmartPicksProAI
python -m engine.pipeline.run_pipeline
```

Or train models directly (requires ML-ready parquet files):

```bash
cd SmartPicksProAI
python -m engine.models.train
```
