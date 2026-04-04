# data/raw/

This directory stores **raw** data ingested by `engine/pipeline/step_1_ingest.py`.

## Contents after running the pipeline

| File pattern | Description |
|---|---|
| `player_game_logs_YYYY-MM-DD.parquet` | Player-level game logs joined with player/team info |
| `games_YYYY-MM-DD.parquet` | Full game history for the season |
| `team_game_stats_YYYY-MM-DD.parquet` | Team-level per-game stats |
| `defense_vs_position_YYYY-MM-DD.parquet` | DvP multipliers by team/position |

## Sample fixture

The file `sample_player_game_logs.csv` contains a small fixture (5 rows) so
new contributors can verify the pipeline without running the full NBA API seed.

To generate real data, run:

```bash
cd SmartPicksProAI/backend
python setup_db.py          # create schema
python initial_pull.py      # seed from NBA API (~5 min)
```
