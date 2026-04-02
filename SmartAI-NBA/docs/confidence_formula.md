# SAFE Score — Confidence Formula Documentation

The **SAFE Score** (Statistical Analysis of Force & Edge) is a composite confidence metric
from 0–100 that combines 8 signal weights and 4 penalties.

## Weights (W1–W8)

| # | Signal | Weight | Description |
|---|--------|--------|-------------|
| W1 | Line Sharpness | ~12 pts | Is the line priced sharply? Sharp lines indicate efficient market |
| W2 | Model Agreement | ~15 pts | Do multiple model layers agree on direction? |
| W3 | Edge Magnitude | ~20 pts | How large is the edge (probability vs implied odds)? |
| W4 | Historical Hit Rate | ~10 pts | How often has this player hit this stat type? |
| W5 | Trap Line Detection | ~8 pts | Is this line a trap? Reduces score if trap detected |
| W6 | Rest / Fatigue Factor | ~10 pts | Is the player rested? Back-to-back reduces confidence |
| W7 | Matchup Quality | ~12 pts | How favorable is the defensive matchup? |
| W8 | Pace Factor | ~8 pts | Does game pace favor this stat? |

*Note: Weights are relative; the exact formulation is implemented in `engine/confidence.py`.*

## Penalties (P1–P4)

| # | Trigger | Effect |
|---|---------|--------|
| P1 | Edge < SILVER_MIN_EDGE_PCT | Caps score below Silver threshold |
| P2 | Probability < tier minimum | Downgrades tier (Platinum→Gold, Gold→Silver) |
| P3 | High coefficient of variation | Applies variance penalty proportional to CV |
| P4 | Line reliability warning (synthetic line) | Reduces score; synthetic lines cannot reach Goblin |

## Tier Assignment

| Tier | Minimum Score | Minimum Edge | Minimum Probability |
|------|--------------|-------------|-------------------|
| 💎 Platinum | 84 | 10% | 62% |
| [Gold] Gold | 65 | 7% | 57% |
| 🥈 Silver | 57 | 3% | — |
| 🥉 Bronze | 35 | 1% | — |
| ❌ Do Not Bet | < 35 | — | — |

## Example Calculation

For a player with:
- Edge = 18%, Probability = 72%, Line is sharp, good matchup, 2 rest days

Expected score: ~78 (Gold tier)

With edge = 28%, prob = 85%, sharp line, excellent matchup, 3 rest days:

Expected score: ~88 (Platinum tier)
