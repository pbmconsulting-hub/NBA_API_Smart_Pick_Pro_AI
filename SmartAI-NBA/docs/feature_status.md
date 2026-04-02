# Feature Status

This document tracks which features are complete, in-progress, or planned.

## ✅ Complete Features

| Module | Feature | Status |
|--------|---------|--------|
| `engine/simulation.py` | Quantum Matrix Engine 5.6 (run_quantum_matrix_simulation) | Complete |
| `engine/confidence.py` | 8-weight + 4-penalty SAFE Score | Complete |
| `engine/edge_detection.py` | Directional force analysis, Goblin / 50_50 / Demon three-tier classification (line-position-based) | Complete |
| `engine/correlation.py` | Correlation-adjusted Kelly + parlay correlation | Complete |
| `engine/projections.py` | Player stat projections with injury/rest adjustments | Complete |
| `data/sportsbook_service.py` | PrizePicks, Underdog, DraftKings live prop fetching | Complete |
| `data/data_manager.py` | Props generation + platform player filtering | Complete |
| `tracking/bet_tracker.py` | Bet logging, auto-resolve, Goblin / 50_50 / Demon / Normal tier tracking | Complete |
| `engine/backtester.py` | Historical backtest engine | Complete |
| `engine/entry_optimizer.py` | Parlay entry optimization | Complete |

## 🔄 In-Progress Features

| Module | Feature | Status | Notes |
|--------|---------|--------|-------|
| `engine/game_script.py` | Game-script blended simulation | Partially integrated | Used optionally in run_enhanced_simulation; falls back to QME |
| `engine/market_movement.py` | Live line movement tracking | Stub/partial | Not wired into Neural Analysis; future feature |
| `engine/minutes_model.py` | Rotation-aware minutes model | Partial | Tries to import rotation_tracker; falls back gracefully |

## 📋 Planned Features

| Feature | Target Module | Priority |
|---------|--------------|---------|
| Background API refresh | `data/sportsbook_service.py` | Medium |
| Real-time line movement alerts | `engine/market_movement.py` | Low |
| Full rotation tracker integration | `engine/rotation_tracker.py` | Medium |
