# ============================================================
# FILE: engine/joseph_eval.py
# PURPOSE: Player grading by archetype — letter grading,
#          archetype profiling, and Joseph M. Smith evaluation.
# CONNECTS TO: engine/trade_evaluator.py, engine/impact_metrics.py
# ============================================================

"""Player evaluation engine for the Joseph M. Smith AI persona.

Provides archetype-aware player grading, letter grades, and
composite evaluation scores used across trade analysis and
lineup construction modules.
"""

import logging
import math
from typing import Any

_logger = logging.getLogger(__name__)


# ============================================================
# SECTION: Archetype Profiles
# ============================================================

ARCHETYPE_PROFILES: dict[str, dict[str, Any]] = {
    "Elite Scorer": {
        "primary_stats": ["points_avg", "fg_pct", "ft_pct"],
        "weights": {"points_avg": 0.40, "fg_pct": 0.25, "ft_pct": 0.15, "usage_rate": 0.20},
        "elite_threshold": 85,
        "description": "High-volume scoring threat with efficient shooting.",
    },
    "Playmaker": {
        "primary_stats": ["assists_avg", "ast_to_ratio"],
        "weights": {"assists_avg": 0.40, "ast_to_ratio": 0.25, "points_avg": 0.20, "turnovers_avg": -0.15},
        "elite_threshold": 82,
        "description": "Primary ball handler who creates for others.",
    },
    "Two-Way Wing": {
        "primary_stats": ["points_avg", "steals_avg", "blocks_avg"],
        "weights": {"points_avg": 0.25, "steals_avg": 0.20, "blocks_avg": 0.15, "rebounds_avg": 0.20, "fg3_pct": 0.20},
        "elite_threshold": 80,
        "description": "Versatile wing who impacts both ends of the floor.",
    },
    "Rim Protector": {
        "primary_stats": ["blocks_avg", "rebounds_avg"],
        "weights": {"blocks_avg": 0.35, "rebounds_avg": 0.30, "fg_pct": 0.20, "points_avg": 0.15},
        "elite_threshold": 78,
        "description": "Interior anchor who protects the paint and grabs boards.",
    },
    "Stretch Big": {
        "primary_stats": ["fg3_pct", "rebounds_avg"],
        "weights": {"fg3_pct": 0.30, "rebounds_avg": 0.25, "points_avg": 0.25, "blocks_avg": 0.20},
        "elite_threshold": 78,
        "description": "Modern big who spaces the floor with three-point shooting.",
    },
    "3-and-D Specialist": {
        "primary_stats": ["fg3_pct", "steals_avg"],
        "weights": {"fg3_pct": 0.35, "steals_avg": 0.25, "points_avg": 0.20, "rebounds_avg": 0.20},
        "elite_threshold": 75,
        "description": "Perimeter defender who knocks down threes.",
    },
    "Role Player": {
        "primary_stats": ["fg_pct", "minutes_avg"],
        "weights": {"fg_pct": 0.25, "points_avg": 0.25, "rebounds_avg": 0.25, "assists_avg": 0.25},
        "elite_threshold": 70,
        "description": "Solid contributor who fills gaps in the rotation.",
    },
}


# ============================================================
# SECTION: Letter Grade System
# ============================================================

def letter_grade(score: float) -> str:
    """Convert a 0-100 numeric score to a letter grade.

    Grade boundaries:
        A+ ≥ 95, A ≥ 90, A- ≥ 85, B+ ≥ 80, B ≥ 75, B- ≥ 70,
        C+ ≥ 65, C ≥ 60, C- ≥ 55, D+ ≥ 50, D ≥ 45, D- ≥ 40, F < 40

    Args:
        score: Numeric score between 0 and 100.

    Returns:
        A letter grade string (e.g. ``"A+"``, ``"B-"``, ``"F"``).
    """
    try:
        score = float(score)
    except (TypeError, ValueError):
        return "F"

    if not math.isfinite(score):
        return "F"
    if score >= 95:
        return "A+"
    if score >= 90:
        return "A"
    if score >= 85:
        return "A-"
    if score >= 80:
        return "B+"
    if score >= 75:
        return "B"
    if score >= 70:
        return "B-"
    if score >= 65:
        return "C+"
    if score >= 60:
        return "C"
    if score >= 55:
        return "C-"
    if score >= 50:
        return "D+"
    if score >= 45:
        return "D"
    if score >= 40:
        return "D-"
    return "F"


# ============================================================
# SECTION: Stat Helpers
# ============================================================

def _safe_stat(player: dict, *keys: str, fallback: float = 0.0) -> float:
    """Retrieve a stat from a player dict trying multiple key names."""
    for key in keys:
        val = player.get(key)
        if val is not None:
            try:
                v = float(val)
                return v if math.isfinite(v) else fallback
            except (TypeError, ValueError):
                continue
    return fallback


# ============================================================
# SECTION: Player Grading
# ============================================================

def joseph_grade_player(player: dict, archetype: str | None = None) -> dict:
    """Grade a player using Joseph's archetype-aware evaluation.

    Args:
        player: Dictionary of player stats (keys like ``points_avg``,
                ``rebounds_avg``, ``assists_avg``, ``fg_pct``, etc.).
        archetype: Optional archetype override. If ``None``, the
                   function attempts to classify the player.

    Returns:
        Dictionary with ``grade``, ``score``, ``archetype``,
        ``strengths``, ``weaknesses``, and ``joseph_assessment``.
    """
    try:
        if not player:
            return _default_grade_result()

        # Determine archetype
        if not archetype or archetype not in ARCHETYPE_PROFILES:
            archetype = _classify_archetype(player)

        profile = ARCHETYPE_PROFILES.get(archetype, ARCHETYPE_PROFILES["Role Player"])

        # Calculate composite score
        raw_scores: dict[str, float] = {}
        weighted_total = 0.0
        weight_sum = 0.0

        for stat_key, weight in profile["weights"].items():
            val = _safe_stat(player, stat_key)
            # Normalize common stats to a 0-100 scale
            normalized = _normalize_stat(stat_key, val)
            raw_scores[stat_key] = normalized
            if weight < 0:
                # Negative weight means lower is better (e.g., turnovers)
                weighted_total += abs(weight) * (100.0 - normalized)
            else:
                weighted_total += weight * normalized
            weight_sum += abs(weight)

        composite = (weighted_total / weight_sum) if weight_sum > 0 else 50.0
        composite = max(0.0, min(100.0, composite))
        grade = letter_grade(composite)

        # Identify strengths and weaknesses
        strengths = [k for k, v in raw_scores.items() if v >= 70]
        weaknesses = [k for k, v in raw_scores.items() if v < 40]

        assessment = _build_joseph_assessment(
            player, archetype, grade, composite, strengths, weaknesses,
        )

        return {
            "grade": grade,
            "score": round(composite, 1),
            "archetype": archetype,
            "strengths": strengths,
            "weaknesses": weaknesses,
            "raw_scores": raw_scores,
            "joseph_assessment": assessment,
        }
    except Exception as exc:
        _logger.error("[JosephEval] joseph_grade_player error: %s", exc)
        return _default_grade_result()


def _default_grade_result() -> dict:
    return {
        "grade": "N/A",
        "score": 0.0,
        "archetype": "Unknown",
        "strengths": [],
        "weaknesses": [],
        "raw_scores": {},
        "joseph_assessment": "Insufficient data to evaluate this player.",
    }


def _classify_archetype(player: dict) -> str:
    """Attempt to classify a player's archetype from their stats."""
    pts = _safe_stat(player, "points_avg", "ppg", "pts")
    ast = _safe_stat(player, "assists_avg", "apg", "ast")
    reb = _safe_stat(player, "rebounds_avg", "rpg", "reb")
    blk = _safe_stat(player, "blocks_avg", "bpg", "blk")
    stl = _safe_stat(player, "steals_avg", "spg", "stl")
    fg3 = _safe_stat(player, "fg3_pct", "three_pct")

    if pts >= 22:
        return "Elite Scorer"
    if ast >= 7:
        return "Playmaker"
    if blk >= 1.5 and reb >= 7:
        return "Rim Protector"
    if fg3 >= 0.37 and stl >= 1.0:
        return "3-and-D Specialist"
    if fg3 >= 0.35 and reb >= 6:
        return "Stretch Big"
    if stl >= 1.2 and pts >= 14:
        return "Two-Way Wing"
    return "Role Player"


def _normalize_stat(stat_key: str, value: float) -> float:
    """Normalize a raw stat value to a 0-100 scale."""
    ranges: dict[str, tuple[float, float]] = {
        "points_avg": (0, 35),
        "rebounds_avg": (0, 14),
        "assists_avg": (0, 12),
        "steals_avg": (0, 2.5),
        "blocks_avg": (0, 3.5),
        "fg_pct": (0.35, 0.60),
        "fg3_pct": (0.28, 0.44),
        "ft_pct": (0.60, 0.95),
        "turnovers_avg": (0, 5),
        "usage_rate": (15, 35),
        "minutes_avg": (10, 38),
        "ast_to_ratio": (0.5, 4.0),
    }
    lo, hi = ranges.get(stat_key, (0, 100))
    if hi == lo:
        return 50.0
    clamped = max(lo, min(hi, value))
    return (clamped - lo) / (hi - lo) * 100.0


def _build_joseph_assessment(
    player: dict,
    archetype: str,
    grade: str,
    score: float,
    strengths: list[str],
    weaknesses: list[str],
) -> str:
    """Build a Joseph M. Smith narrative assessment of a player."""
    name = player.get("player_name", player.get("name", "This player"))
    parts: list[str] = []

    if score >= 85:
        parts.append(
            f"{name} is a CERTIFIED {archetype} — grade {grade} "
            f"({score:.0f}/100). This is the kind of player you build around."
        )
    elif score >= 70:
        parts.append(
            f"{name} grades out as a solid {archetype} at {grade} ({score:.0f}/100). "
            f"Not elite, but a very reliable contributor."
        )
    elif score >= 55:
        parts.append(
            f"{name} profiles as a {archetype} with a {grade} grade ({score:.0f}/100). "
            f"Some tools are there but consistency is the question."
        )
    else:
        parts.append(
            f"{name} only grades out at {grade} ({score:.0f}/100) as a {archetype}. "
            f"There are real concerns here."
        )

    if strengths:
        readable = [s.replace("_avg", "").replace("_pct", "%").replace("_", " ") for s in strengths]
        parts.append(f"Strengths: {', '.join(readable)}.")

    if weaknesses:
        readable = [s.replace("_avg", "").replace("_pct", "%").replace("_", " ") for s in weaknesses]
        parts.append(f"Areas to improve: {', '.join(readable)}.")

    return " ".join(parts)
