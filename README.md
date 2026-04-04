# NBA_API-ETL — SmartPicksProAI

A full-stack NBA player-prop prediction platform featuring a **Python/FastAPI
backend**, a **Streamlit frontend**, and a modular **ML engine** that projects
player stats, simulates distributions, detects betting edges, and manages
bankroll — all powered by NBA API data.

---

## Quick Start

```bash
# 1. Clone & enter the repo
git clone https://github.com/pbmconsulting-hub/NBA_API_Smart_Pick_Pro_AI.git
cd NBA_API_Smart_Pick_Pro_AI

# 2. Install dependencies + create the database
make setup

# 3. Seed historical data from the NBA API (~5 min)
make seed

# 4. Run the ML pipeline and train models
make pipeline
make train

# 5. Launch backend + frontend
make run
```

Or use the one-command launcher:

```bash
./start.sh          # setup + backend + frontend
./start.sh --full   # setup + seed + pipeline + train + run
```

The backend serves at **http://127.0.0.1:8098** and the frontend at
**http://localhost:8501**.

---

## Project Structure

```
NBA_API_Smart_Pick_Pro_AI/
├── config.yaml                 # Central YAML config (thresholds, hyperparams, endpoints)
├── .env.example                # Environment variable template
├── Makefile                    # Convenience build/run targets
├── start.sh                    # One-command full-app launcher
├── requirements.txt            # Python dependencies
├── README.md                   # ← you are here
│
└── SmartPicksProAI/
    ├── backend/                # FastAPI REST API + data layer
    │   ├── api.py              # FastAPI app — 49 endpoints (players, games, picks, admin)
    │   ├── setup_db.py         # SQLite schema creation & migrations
    │   ├── initial_pull.py     # One-time historical data seed from NBA API
    │   ├── data_updater.py     # Incremental daily data refresh
    │   ├── odds_client.py      # The Odds API integration (sportsbook prop lines)
    │   ├── prizepicks_client.py# PrizePicks DFS lines client
    │   ├── underdog_client.py  # Underdog Fantasy DFS lines client
    │   ├── injury_client.py    # NBA CDN injury feed integration
    │   └── utils.py            # Backend-specific helpers (upsert, matchup parsing)
    │
    ├── engine/                 # ML & analytics engine
    │   ├── pipeline/           # 6-step numbered pipeline (see below)
    │   │   ├── run_pipeline.py
    │   │   ├── step_1_ingest.py
    │   │   ├── step_2_clean.py
    │   │   ├── step_3_features.py
    │   │   ├── step_4_predict.py
    │   │   ├── step_5_evaluate.py
    │   │   └── step_6_export.py
    │   │
    │   ├── models/             # ML model wrappers & training
    │   │   ├── base_model.py   # Abstract base class
    │   │   ├── ridge_model.py  # Ridge regression
    │   │   ├── xgboost_model.py# XGBoost gradient boosting
    │   │   ├── catboost_model.py# CatBoost gradient boosting
    │   │   ├── ensemble.py     # Weighted ensemble (XGBoost + CatBoost + Ridge)
    │   │   ├── train.py        # Training script with time-series holdout
    │   │   └── saved/          # Trained model artifacts (*.joblib, not committed)
    │   │
    │   ├── features/           # Feature engineering
    │   │   ├── feature_engineering.py  # build_feature_matrix() — ~180 columns
    │   │   ├── player_metrics.py       # True shooting %, usage rate
    │   │   └── team_metrics.py         # Possessions, offensive/defensive rating
    │   │
    │   ├── predict/            # Prediction interface
    │   │   └── predictor.py    # predict_player_stat() — loads saved models
    │   │
    │   ├── simulation.py       # Monte Carlo simulation (10k iterations, NumPy vectorised)
    │   ├── projections.py      # Contextual player projections
    │   ├── edge_detection.py   # 9-force directional edge analysis
    │   ├── confidence.py       # SAFE Score confidence tiers (Platinum → Do Not Bet)
    │   ├── explainer.py        # Human-readable pick explanations
    │   ├── backtester.py       # Historical backtesting engine
    │   ├── bankroll.py         # Kelly Criterion bet sizing
    │   ├── odds_engine.py      # Odds math (implied prob, true edge, parlay EV)
    │   ├── calibration.py      # Model calibration utilities
    │   ├── correlation.py      # Stat correlation analysis
    │   ├── data_adapter.py     # DB ↔ engine column mapping
    │   ├── game_script.py      # Game script / blowout risk analysis
    │   ├── impact_metrics.py   # Player impact metrics
    │   ├── lineup_analysis.py  # Lineup / rotation analysis
    │   ├── market_movement.py  # Line movement detection
    │   ├── matchup_history.py  # Head-to-head matchup history
    │   ├── math_helpers.py     # Sampling & distribution helpers
    │   ├── regime_detection.py # Performance regime detection
    │   ├── rotation_tracker.py # Rotation / minutes tracking
    │   ├── stat_distributions.py# Stat distribution fitting
    │   └── trade_evaluator.py  # Trade impact evaluation
    │
    ├── frontend/               # Streamlit dashboard
    │   ├── app.py              # Main app — 12 pages (see below)
    │   └── api_service.py      # HTTP client with Streamlit caching
    │
    ├── config/                 # Configuration
    │   └── thresholds.py       # Named constants for tiers, edges, simulation
    │
    ├── tracking/               # Bet tracking & model performance
    │   ├── bet_tracker.py      # Log bets, record results, performance stats
    │   ├── database.py         # SQLite storage for tracking data
    │   └── model_performance.py# Log predictions, get best model, weight tracking
    │
    ├── utils/                  # Shared utilities
    │   ├── constants.py        # NBA teams, stat columns, league averages
    │   ├── logger.py           # Centralized logging (get_logger)
    │   ├── geo.py              # Geolocation helpers
    │   ├── parquet_helpers.py  # Parquet I/O utilities
    │   ├── rate_limiter.py     # API rate limiting
    │   └── retry.py            # Retry decorator
    │
    ├── styles/                 # Frontend theming
    │   └── theme.py            # CSS, banners, tier badges, verdict UI
    │
    ├── data/                   # Pipeline data (gitignored except samples)
    │   ├── raw/                # Step 1 output — raw ingested data
    │   ├── processed/          # Step 2 output — cleaned data
    │   └── ml_ready/           # Step 3 output — feature matrices for training
    │
    ├── db/                     # Database directory
    └── assets/                 # Static assets (images, logos)
```

---

## Engine Pipeline

The ML pipeline runs in 6 sequential steps. Each step reads the previous
step's output and writes its own.

| Step | Module | What it does |
|---|---|---|
| **1. Ingest** | `step_1_ingest.py` | Pulls 9 tables from SQLite: Player_Game_Logs (with player/team joins), Games, Team_Game_Stats, Defense_Vs_Position, Box_Score_Advanced, Box_Score_Usage, Standings, Teams |
| **2. Clean** | `step_2_clean.py` | Handles nulls, removes DNPs, casts types, deduplicates |
| **3. Features** | `step_3_features.py` | Produces ~180-column feature matrix: rolling averages/std (3/5/10/20-game), form ratios, rest days, pace/defense adjustments, DvP multipliers |
| **4. Predict** | `step_4_predict.py` | Passes real game context through trained models, predicts 8 stat types |
| **5. Evaluate** | `step_5_evaluate.py` | Backtests predictions against actuals, computes accuracy metrics |
| **6. Export** | `step_6_export.py` | Writes final outputs (predictions, confidence scores, pick recommendations) |

Run the full pipeline:

```bash
cd SmartPicksProAI
python -m engine.pipeline.run_pipeline
```

---

## ML Models

| Model | Module | Description |
|---|---|---|
| **Ridge** | `ridge_model.py` | Linear baseline (fast, interpretable) |
| **XGBoost** | `xgboost_model.py` | Gradient-boosted trees (high accuracy) |
| **CatBoost** | `catboost_model.py` | Gradient boosting with native categorical support |
| **ModelEnsemble** | `ensemble.py` | Weighted blend of all three models |

### Training

```bash
cd SmartPicksProAI
python -m engine.models.train
```

Trains **Ridge + ModelEnsemble** for 8 stat types (`pts`, `reb`, `ast`, `stl`,
`blk`, `tov`, `fg3m`, `ftm`) with a date-sorted 80/20 time-series split.
Trained artifacts are saved as `.joblib` files in `engine/models/saved/`.

### Simulation Engine

The simulation engine (`engine/simulation.py`) runs **10,000 Monte Carlo
iterations** per player per stat using NumPy vectorized operations. Three
distribution paths:

- **Skew-normal** (default for most stats)
- **Zero-inflated** (threes / fg3m)
- **Poisson** (steals, blocks, turnovers)
- **KDE** path when 15+ game logs are available

### Edge Detection

The edge detector (`engine/edge_detection.py`) evaluates **9 directional
forces** that push a player's performance above or below the posted line:
matchup quality, pace environment, rest advantage, injury impact, form
trend, home/away, game script, line movement, and rotation changes.

### Confidence Scoring

The SAFE Score (`engine/confidence.py`) maps to four tiers:

| Tier | Score | Min Edge |
|---|---|---|
| **Platinum** | ≥ 84 | 10%+ |
| **Gold** | ≥ 65 | 7%+ |
| **Silver** | ≥ 57 | 3%+ |
| **Bronze** | ≥ 35 | 1%+ |
| **Do Not Bet** | < 35 | — |

---

## Backtester & Bankroll Manager

- **Backtester** (`engine/backtester.py`) — Replays the full projection →
  simulation → edge pipeline against historical game logs to measure accuracy
  and profitability.

- **Bankroll Manager** (`engine/bankroll.py`) — Implements the Kelly Criterion
  for optimal bet sizing. Default is quarter-Kelly (conservative).

---

## Bet Tracker

The tracking system (`tracking/`) persists pick history, bet results, and
model performance in a local SQLite database.

- `bet_tracker.py` — High-level interface: log bets, record results, view
  performance by tier/stat/platform.
- `model_performance.py` — Logs predictions vs actuals, tracks best model per
  stat type.

---

## Odds & DFS Clients

| Client | Module | Data Source |
|---|---|---|
| **The Odds API** | `backend/odds_client.py` | Sportsbook prop lines (DraftKings, FanDuel, etc.). Requires `ODDS_API_KEY` env var. |
| **PrizePicks** | `backend/prizepicks_client.py` | DFS prop lines. Caches in `DFS_Prop_Lines` table. |
| **Underdog Fantasy** | `backend/underdog_client.py` | DFS prop lines. Caches in `DFS_Prop_Lines` table. |
| **Injury Feed** | `backend/injury_client.py` | NBA CDN live injury data → `Injury_Status` table. |

Cross-platform edge detection (`engine/edge_detection.py`) merges sportsbook
+ DFS lines and evaluates each platform independently.

---

## API Endpoints (Selected)

The backend exposes **49 REST endpoints**. Key groups:

### Player Data
| Method | Path | Description |
|---|---|---|
| GET | `/api/players/{id}/last5` | Last 5 game logs with averages |
| GET | `/api/players/search?q=name` | Search players by name |
| GET | `/api/players/{id}/bio` | Player biography |
| GET | `/api/players/{id}/advanced` | Advanced stats |
| GET | `/api/players/{id}/projection` | Full projection with simulation |
| GET | `/api/players/{id}/matchups` | Head-to-head matchup stats |

### Games & Teams
| Method | Path | Description |
|---|---|---|
| GET | `/api/games/today` | Today's NBA matchups |
| GET | `/api/games/{id}/box-score` | Game box score |
| GET | `/api/teams` | All 30 teams with pace/ortg/drtg |
| GET | `/api/teams/{id}/roster` | Team roster |
| GET | `/api/defense-vs-position/{abbrev}` | DvP multipliers |
| GET | `/api/standings` | Current standings |

### Picks & Analysis
| Method | Path | Description |
|---|---|---|
| POST | `/api/picks/analyze` | Full prop analysis (projection + sim + edge + confidence) |
| GET | `/api/picks/today` | Auto-generated daily picks |
| GET | `/api/slate/today` | Autonomous slate builder (scans all games × rosters × stats) |
| POST | `/api/picks/save` | Save a pick to tracking |
| GET | `/api/picks/history` | Pick history |

### Admin
| Method | Path | Description |
|---|---|---|
| POST | `/api/admin/refresh-data` | Incremental data update |
| POST | `/api/admin/refresh-odds` | Refresh sportsbook odds |
| POST | `/api/admin/refresh-prizepicks` | Refresh PrizePicks lines |
| POST | `/api/admin/refresh-underdog` | Refresh Underdog lines |
| POST | `/api/admin/refresh-injuries` | Refresh injury data |
| POST | `/api/admin/train-models` | Trigger model training |

### Health
| Method | Path | Description |
|---|---|---|
| GET | `/api/health` | Health check → `{"status": "ok"}` |

---

## Frontend Pages

The Streamlit dashboard includes 12 pages:

| Page | Description |
|---|---|
| **Home** | Today's matchup grid, hero banner |
| **Game Detail** | Box scores, play-by-play, rotation, win probability |
| **Player Profile** | Last 5 logs, career stats, advanced metrics |
| **Standings** | Current NBA standings |
| **Teams Browse** | All 30 teams with search |
| **Team Detail** | Roster, game stats, estimated metrics |
| **League Leaders** | Statistical leaders |
| **Defense** | Defense-vs-position multipliers |
| **Prop Analyzer** | Full prop analysis with tabbed player cards (info, predictions, bet sizing) |
| **Pick History** | Saved picks with results |
| **Bet Tracker** | Log bets, record outcomes, performance dashboards |
| **More** | Additional tools and settings |

---

## Configuration

### `config.yaml`

Central YAML config for all tunable parameters — model hyperparameters,
confidence thresholds, API endpoints, database path, simulation defaults, and
more. See [`config.yaml`](config.yaml) for the full reference.

### `config/thresholds.py`

Python constants for confidence tiers, edge percentages, and simulation
defaults. Imported directly by engine modules:

```python
from config.thresholds import PLATINUM_THRESHOLD, GOLD_THRESHOLD
```

### `.env.example`

Template for environment variables. Copy to `.env` and fill in your API keys:

```bash
cp .env.example .env
```

| Variable | Required | Description |
|---|---|---|
| `ODDS_API_KEY` | For live odds | The Odds API key |
| `PRIZEPICKS_API_URL` | Optional | Override PrizePicks base URL |
| `UNDERDOG_API_URL` | Optional | Override Underdog base URL |

---

## Database

SQLite with WAL journal mode for concurrent reads/writes. Schema managed by
`backend/setup_db.py` (idempotent — safe to run multiple times).

### Core Tables

| Table | Description |
|---|---|
| `Players` | Player metadata (name, team, position) |
| `Teams` | Team info + pace/ortg/drtg |
| `Games` | Game schedule and scores |
| `Player_Game_Logs` | Per-player per-game stat lines (22 columns) |
| `Team_Game_Stats` | Per-team per-game stats |
| `Defense_Vs_Position` | DvP multipliers by team × position |
| `Team_Roster` | Roster assignments with effective dates |
| `Injury_Status` | Injury reports from NBA CDN |
| `Prop_Lines` | Cached sportsbook prop lines (The Odds API) |
| `DFS_Prop_Lines` | Cached DFS prop lines (PrizePicks, Underdog) |

---

## Development

### Prerequisites

- Python 3.10+
- `pip install -r requirements.txt`

### Useful commands

```bash
make help           # Show all available targets
make setup          # Install deps + create DB
make seed           # Seed historical data
make pipeline       # Run ML pipeline
make train          # Train models
make run            # Backend + frontend
make clean          # Remove generated files
```

---

## License

See repository for license details.
