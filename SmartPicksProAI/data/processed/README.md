# data/processed/

This directory stores **cleaned** data produced by `engine/pipeline/step_2_clean.py`.

## Contents after running the pipeline

| File pattern | Description |
|---|---|
| `clean_player_logs_YYYY-MM-DD.parquet` | Game logs with nulls handled, DNPs removed, types cast |
| `clean_games_YYYY-MM-DD.parquet` | Games with date parsing and deduplication |

## Sample fixture

The file `sample_clean_player_logs.csv` contains a small fixture (5 rows) with
the same structure as the cleaned output, for contributors to verify step 3.

To generate real data, run steps 1–2 of the pipeline:

```bash
cd SmartPicksProAI
python -m engine.pipeline.run_pipeline
```
