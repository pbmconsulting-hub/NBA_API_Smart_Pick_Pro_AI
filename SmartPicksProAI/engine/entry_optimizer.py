# ============================================================
# FILE: engine/entry_optimizer.py
# PURPOSE: Entry optimization for parlay/slate construction.
#          Selects the best combination of legs from a pool of
#          analysed props, respecting correlation constraints,
#          platform rules, and bankroll limits.
# ============================================================

from __future__ import annotations

import itertools
import logging
from typing import Any

_logger = logging.getLogger(__name__)

# ── Platform configuration ──────────────────────────────────────────────

PLATFORM_CONFIGS: dict[str, dict[str, Any]] = {
    "PrizePicks": {
        "min_legs": 2,
        "max_legs": 6,
        "allow_same_game": True,
        "allow_same_player": False,
        "payout_multipliers": {2: 3.0, 3: 5.0, 4: 10.0, 5: 20.0, 6: 25.0},
    },
    "Underdog Fantasy": {
        "min_legs": 2,
        "max_legs": 6,
        "allow_same_game": True,
        "allow_same_player": False,
        "payout_multipliers": {2: 3.0, 3: 6.0, 4: 10.0, 5: 20.0, 6: 25.0},
    },
    "DraftKings Pick6": {
        "min_legs": 2,
        "max_legs": 6,
        "allow_same_game": True,
        "allow_same_player": False,
        "payout_multipliers": {2: 3.0, 3: 5.0, 4: 10.0, 5: 20.0, 6: 25.0},
    },
    "FanDuel": {
        "min_legs": 2,
        "max_legs": 5,
        "allow_same_game": True,
        "allow_same_player": False,
        "payout_multipliers": {2: 3.0, 3: 5.0, 4: 10.0, 5: 20.0},
    },
}


# ── Correlation penalty helpers ─────────────────────────────────────────

# Pairs of stat types that are positively correlated — parlaying both on
# the same player (if allowed) or same game reduces effective edge.
_CORRELATED_STAT_PAIRS = {
    frozenset({"points", "threes"}),
    frozenset({"points", "assists"}),
    frozenset({"rebounds", "blocks"}),
    frozenset({"steals", "turnovers"}),
    frozenset({"points", "rebounds"}),
}

# Same-team correlation: two players on the same team in the same game
# are correlated.
_SAME_TEAM_CORRELATION = 0.15
# Same-game correlation: players in the same game are weakly correlated.
_SAME_GAME_CORRELATION = 0.08


def _correlation_penalty(legs: list[dict[str, Any]]) -> float:
    """Return a correlation penalty (0-1) for a set of legs.

    Higher penalty = more correlation = less effective edge.
    """
    penalty = 0.0
    n = len(legs)
    for i in range(n):
        for j in range(i + 1, n):
            a, b = legs[i], legs[j]

            # Same player (shouldn't happen but penalise heavily)
            if a.get("player_name", "").lower() == b.get("player_name", "").lower():
                penalty += 0.40

            # Same team
            a_team = a.get("team", a.get("player_team", ""))
            b_team = b.get("team", b.get("player_team", ""))
            if a_team and b_team and a_team == b_team:
                penalty += _SAME_TEAM_CORRELATION

            # Same game
            a_game = a.get("game_id", "")
            b_game = b.get("game_id", "")
            if a_game and b_game and a_game == b_game:
                penalty += _SAME_GAME_CORRELATION

            # Correlated stat types
            pair = frozenset({
                a.get("stat_type", "").lower(),
                b.get("stat_type", "").lower(),
            })
            if pair in _CORRELATED_STAT_PAIRS:
                penalty += 0.05

    # Normalise to [0, 1]
    max_pairs = n * (n - 1) / 2
    if max_pairs > 0:
        penalty = min(penalty / max_pairs, 1.0)
    return penalty


# ── Validation ──────────────────────────────────────────────────────────


def validate_entry(
    legs: list[dict[str, Any]],
    platform: str = "PrizePicks",
) -> dict[str, Any]:
    """Validate an entry against platform rules.

    Returns
    -------
    dict with ``valid`` (bool), ``errors`` (list[str]),
    ``warnings`` (list[str]).
    """
    config = PLATFORM_CONFIGS.get(platform, PLATFORM_CONFIGS["PrizePicks"])
    errors: list[str] = []
    warnings: list[str] = []

    # Check leg count
    if len(legs) < config["min_legs"]:
        errors.append(
            f"Need at least {config['min_legs']} legs for {platform}."
        )
    if len(legs) > config["max_legs"]:
        errors.append(
            f"Maximum {config['max_legs']} legs for {platform}."
        )

    # Check duplicate players
    if not config["allow_same_player"]:
        names = [leg.get("player_name", "").lower() for leg in legs]
        seen: set[str] = set()
        for name in names:
            if name in seen:
                errors.append(f"Duplicate player: {name}")
            seen.add(name)

    # Check correlation
    penalty = _correlation_penalty(legs)
    if penalty > 0.30:
        warnings.append(
            f"High correlation penalty ({penalty:.0%}) — consider diversifying."
        )
    elif penalty > 0.15:
        warnings.append(
            f"Moderate correlation ({penalty:.0%}) between legs."
        )

    # Check for Avoid-tier legs
    for leg in legs:
        tier = leg.get("confidence_tier", leg.get("tier", ""))
        if tier == "Avoid":
            warnings.append(
                f"Leg '{leg.get('player_name', '?')}' is in Avoid tier."
            )

    return {
        "valid": len(errors) == 0,
        "errors": errors,
        "warnings": warnings,
        "correlation_penalty": penalty,
    }


# ── Scoring function ────────────────────────────────────────────────────


def _score_combination(
    combo: tuple[dict[str, Any], ...],
    platform: str,
) -> float:
    """Score a combination of legs.

    Score combines average confidence, average edge, and correlation
    penalty.  Higher is better.
    """
    if not combo:
        return 0.0

    avg_conf = sum(leg.get("confidence", 0) for leg in combo) / len(combo)
    avg_edge = sum(leg.get("edge_pct", 0) for leg in combo) / len(combo)
    penalty = _correlation_penalty(list(combo))

    # Weighted score: confidence (40%) + edge (40%) - correlation (20%)
    score = (avg_conf / 100.0) * 0.4 + min(avg_edge / 20.0, 1.0) * 0.4 - penalty * 0.2
    return score


# ── Win probability estimation ──────────────────────────────────────────


def _estimate_parlay_prob(legs: list[dict[str, Any]]) -> float:
    """Estimate combined win probability with correlation adjustment."""
    if not legs:
        return 0.0

    # Naive independent probability
    prob = 1.0
    for leg in legs:
        leg_prob = leg.get("model_probability", leg.get("win_prob", 0.55))
        prob *= max(0.01, min(0.99, leg_prob))

    # Apply correlation adjustment (correlated legs reduce true probability)
    penalty = _correlation_penalty(legs)
    adjusted = prob * (1.0 - penalty * 0.3)

    return max(0.01, adjusted)


# ── Kelly sizing for entries ────────────────────────────────────────────


def _kelly_stake(
    win_prob: float,
    payout_multiplier: float,
    bankroll: float,
    kelly_fraction: float = 0.25,
) -> float:
    """Quarter-Kelly stake for a parlay entry."""
    if payout_multiplier <= 0 or win_prob <= 0:
        return 0.0

    # Kelly formula: f* = (bp - q) / b
    b = payout_multiplier - 1  # net odds
    p = win_prob
    q = 1 - p
    kelly_full = (b * p - q) / b if b > 0 else 0.0
    kelly_full = max(0.0, kelly_full)

    suggested = bankroll * kelly_full * kelly_fraction
    # Cap at 5% of bankroll
    return min(suggested, bankroll * 0.05)


# ═══════════════════════════════════════════════════════════════════════════
# Main optimizer
# ═══════════════════════════════════════════════════════════════════════════


def optimize_entry(
    legs: list[dict[str, Any]],
    *,
    platform: str = "PrizePicks",
    bankroll: float = 500.0,
    max_legs: int = 6,
    min_confidence: float = 50.0,
    kelly_fraction: float = 0.25,
) -> dict[str, Any]:
    """Find the optimal entry from a pool of legs.

    Parameters
    ----------
    legs:
        Pool of candidate legs.  Each leg must have at least
        ``player_name``, ``stat_type``, ``prop_line``, ``direction``,
        ``confidence``, ``edge_pct``.
    platform:
        Target DFS platform.
    bankroll:
        Current bankroll for sizing.
    max_legs:
        Maximum legs to select.
    min_confidence:
        Minimum confidence score per leg.
    kelly_fraction:
        Kelly fraction (0.25 = quarter-Kelly).

    Returns
    -------
    dict with ``success``, ``selected_legs``, ``num_legs``,
    ``combined_edge``, ``win_prob``, ``suggested_stake``,
    ``correlation_warnings``.
    """
    config = PLATFORM_CONFIGS.get(platform, PLATFORM_CONFIGS["PrizePicks"])
    min_legs = config["min_legs"]
    effective_max = min(max_legs, config["max_legs"])

    # Filter by minimum confidence
    eligible = [leg for leg in legs if leg.get("confidence", 0) >= min_confidence]

    # Remove duplicates by player name
    seen_players: set[str] = set()
    unique_legs: list[dict[str, Any]] = []
    for leg in eligible:
        name = leg.get("player_name", "").lower()
        if name not in seen_players:
            seen_players.add(name)
            unique_legs.append(leg)

    if len(unique_legs) < min_legs:
        return {
            "success": False,
            "error": (
                f"Not enough eligible legs ({len(unique_legs)}) for "
                f"{platform} (minimum {min_legs}). "
                f"Try lowering the confidence threshold."
            ),
        }

    # Find optimal combination
    best_combo: tuple[dict[str, Any], ...] | None = None
    best_score = -1.0

    # Try combinations from max_legs down to min_legs
    for n in range(effective_max, min_legs - 1, -1):
        if n > len(unique_legs):
            continue

        # If pool is large, limit combinations to avoid combinatorial explosion
        pool = unique_legs
        if len(pool) > 15:
            pool = sorted(pool, key=lambda x: x.get("confidence", 0), reverse=True)[:15]

        for combo in itertools.combinations(pool, n):
            score = _score_combination(combo, platform)
            if score > best_score:
                best_score = score
                best_combo = combo

    if best_combo is None:
        return {"success": False, "error": "No valid combination found."}

    selected = list(best_combo)

    # Validation
    validation = validate_entry(selected, platform)

    # Win probability and sizing
    win_prob = _estimate_parlay_prob(selected)
    payout = config["payout_multipliers"].get(len(selected), 3.0)
    suggested_stake = _kelly_stake(win_prob, payout, bankroll, kelly_fraction)

    combined_edge = sum(leg.get("edge_pct", 0) for leg in selected) / len(selected)

    return {
        "success": True,
        "selected_legs": selected,
        "num_legs": len(selected),
        "combined_edge": combined_edge,
        "win_prob": win_prob,
        "payout_multiplier": payout,
        "suggested_stake": round(suggested_stake, 2),
        "bankroll": bankroll,
        "platform": platform,
        "correlation_penalty": _correlation_penalty(selected),
        "correlation_warnings": validation.get("warnings", []),
        "validation_errors": validation.get("errors", []),
        "score": best_score,
    }
