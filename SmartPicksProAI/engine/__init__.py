# ============================================================
# FILE: engine/__init__.py
# PURPOSE: Shared constants for the SmartPicksProAI engine.
#          Import these in any module that needs them.
# ============================================================

# Simple (single-stat) stat types.
SIMPLE_STAT_TYPES = frozenset({
    "points",
    "rebounds",
    "assists",
    "threes",
    "steals",
    "blocks",
    "turnovers",
    # Extended NBA stat types
    "ftm",
    "fga",
    "fgm",
    "fta",
    "minutes",
    "personal_fouls",
    "offensive_rebounds",
    "defensive_rebounds",
})

# Combo stat types (sum of 2+ simple stats).
COMBO_STAT_TYPES = frozenset({
    "points_rebounds",
    "points_assists",
    "rebounds_assists",
    "points_rebounds_assists",
    "blocks_steals",
})

# Fantasy score stat types (weighted sum using platform formula).
FANTASY_STAT_TYPES = frozenset({
    "fantasy_score_pp",   # Fantasy scoring (legacy)
    "fantasy_score_dk",   # DraftKings fantasy scoring
    "fantasy_score_ud",   # Fantasy scoring (legacy)
})

# Yes/No prop types.
YESNO_STAT_TYPES = frozenset({
    "double_double",
    "triple_double",
})

# All supported stat types across the app.
# This is the single source of truth — don't define this elsewhere.
VALID_STAT_TYPES = (
    SIMPLE_STAT_TYPES | COMBO_STAT_TYPES | FANTASY_STAT_TYPES | YESNO_STAT_TYPES
)

# ============================================================
# SECTION: High-Impact Feature Modules
# These modules are imported directly by pages; they are not
# re-exported from __init__.py to keep the namespace clean.
# Available modules:
#   engine.matchup_history       — Player-vs-team history
#   engine.bankroll              — Kelly Criterion sizing
#   engine.game_script           — Quarter-by-quarter simulation
#   engine.market_movement       — Sharp money line movement
# ============================================================

# ============================================================
# SECTION: Enhanced Engine Public API
# Convenience re-exports for commonly used functions.
# ============================================================

# simulation.py — Quantum Matrix Engine
from engine.simulation import (  # noqa: E402
    run_enhanced_simulation,          # noqa: F401 – QME + game-script blended simulation
)

# edge_detection.py — Advanced Edge Analysis
from engine.edge_detection import (  # noqa: E402
    estimate_closing_line_value,      # noqa: F401 – CLV estimation
    calculate_dynamic_vig,            # noqa: F401 – Dynamic vig by platform
)

# confidence.py — Precision Confidence Scoring
from engine.confidence import (  # noqa: E402
    calculate_risk_score,             # noqa: F401 – Composite 1-10 risk rating
    enforce_tier_distribution,        # noqa: F401 – Tier distribution guardrails
)

# correlation.py — Advanced Correlation Engine
from engine.correlation import (  # noqa: E402
    get_position_correlation_adjustment,  # noqa: F401 – Position-based correlation priors
    get_correlation_confidence,           # noqa: F401 – Parlay correlation confidence
    correlation_adjusted_kelly,           # noqa: F401 – Correlation-adjusted Kelly sizing
)
