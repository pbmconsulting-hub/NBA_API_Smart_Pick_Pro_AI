# ============================================================
# FILE: engine/__init__.py
# PURPOSE: Shared constants for the SmartBetPro NBA engine.
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
    # Extended NBA stat types from mirror
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
VALID_STAT_TYPES = SIMPLE_STAT_TYPES | COMBO_STAT_TYPES | FANTASY_STAT_TYPES | YESNO_STAT_TYPES

# ============================================================
# SECTION: New High-Impact Feature Modules (v8)
# These modules are imported directly by pages; they are not
# re-exported from __init__.py to keep the namespace clean.
# Available modules:
#   engine.matchup_history       — Feature 2: Player-vs-team history
#   engine.platform_line_compare — Feature 3: Cross-platform line comparison
#   engine.bankroll              — Feature 5: Kelly Criterion sizing
#   engine.minutes_model         — Feature 6: Dedicated minutes projection
#   engine.game_script           — Feature 7: Quarter-by-quarter simulation
#   engine.market_movement       — Feature 9: Sharp money line movement
# ============================================================

# ============================================================
# SECTION: v9 Enhanced Engine Public API
# New public functions added by the comprehensive engine enhancement.
# These can be imported from engine.* modules directly or via these
# convenience re-exports.
# ============================================================

# simulation.py — Quantum Matrix Engine 6.0
from engine.simulation import (
    run_enhanced_simulation,          # QME + game-script blended simulation (1F)
)

# edge_detection.py — Advanced Edge Analysis
from engine.edge_detection import (
    estimate_closing_line_value,      # CLV estimation (2B)
    calculate_dynamic_vig,            # Dynamic vig by platform (2C)
)

# confidence.py — Precision Confidence Scoring
from engine.confidence import (
    calculate_risk_score,             # Composite 1-10 risk rating (3E)
    enforce_tier_distribution,        # Tier distribution guardrails (3F)
)

# correlation.py — Advanced Correlation Engine
from engine.correlation import (
    get_position_correlation_adjustment,  # Position-based correlation priors (4B)
    get_correlation_confidence,           # Parlay correlation confidence (4E)
    correlation_adjusted_kelly,           # Correlation-adjusted Kelly sizing (4F)
)
