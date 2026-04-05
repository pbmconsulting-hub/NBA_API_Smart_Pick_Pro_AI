# ============================================================
# FILE: engine/joseph_brain.py
# PURPOSE: Joseph M. Smith full AI persona engine — analysis,
#          platinum lock picks, ambient commentary, and best
#          bets generation.
# CONNECTS TO: engine/joseph_eval.py, engine/joseph_strategy.py,
#              engine/confidence.py, styles/theme.py
# ============================================================

"""Joseph M. Smith AI persona engine.

Central module that orchestrates Joseph's full analysis pipeline,
generates platinum lock picks, ambient commentary lines for the UI,
and best bet recommendations.
"""

import logging
import random
from typing import Any

_logger = logging.getLogger(__name__)

# Graceful imports of sibling modules
try:
    from engine.joseph_eval import joseph_grade_player, letter_grade
except ImportError:
    _logger.warning("[JosephBrain] Could not import joseph_eval")

    def joseph_grade_player(player: dict, archetype: str | None = None) -> dict:  # type: ignore[misc]
        return {"grade": "N/A", "score": 0.0, "archetype": "Unknown",
                "strengths": [], "weaknesses": [], "raw_scores": {},
                "joseph_assessment": "Evaluation module unavailable."}

    def letter_grade(score: float) -> str:  # type: ignore[misc]
        return "N/A"

try:
    from engine.joseph_strategy import analyze_game_strategy, detect_narrative_tags
except ImportError:
    _logger.warning("[JosephBrain] Could not import joseph_strategy")

    def analyze_game_strategy(home: dict, away: dict, **kw: Any) -> dict:  # type: ignore[misc]
        return {"joseph_strategy_take": "Strategy module unavailable."}

    def detect_narrative_tags(ctx: dict) -> list[dict]:  # type: ignore[misc]
        return []


# ============================================================
# SECTION: Ambient Commentary
# ============================================================

_AMBIENT_LINES: list[str] = [
    "Trust the data, not the hype. That's the Joseph M. Smith way.",
    "The numbers never lie — but they do whisper. You gotta listen.",
    "Every edge is a brick in the foundation of profit.",
    "Platinum locks don't grow on trees. When I say lock, I MEAN lock.",
    "The sharps are moving. Are you paying attention?",
    "Back-to-backs are a prop bettor's best friend — if you know where to look.",
    "Matchup history is the most underrated edge in sports betting.",
    "I've seen this movie before. Let me tell you how it ends.",
    "Volume is the enemy of ROI. Be selective, be surgical.",
    "The best bet is the one the market hasn't priced in yet.",
    "Remember: one bad beat doesn't break a system. Stick to the process.",
    "I'm not in the prediction business — I'm in the edge business.",
    "When the model and the eye test agree, that's when I pound the table.",
    "Rest days matter more than people think. Fresh legs, fresh stats.",
    "The algorithm sees what your eyes can't. That's why I trust it.",
]


def get_ambient_line() -> str:
    """Return a random Joseph M. Smith ambient commentary line."""
    return random.choice(_AMBIENT_LINES)


def get_ambient_lines(count: int = 3) -> list[str]:
    """Return *count* unique ambient commentary lines."""
    count = max(1, min(count, len(_AMBIENT_LINES)))
    return random.sample(_AMBIENT_LINES, count)


# ============================================================
# SECTION: Full Analysis
# ============================================================

def joseph_full_analysis(
    player: dict,
    prop_line: float,
    stat_type: str,
    *,
    game_context: dict | None = None,
    simulation_result: dict | None = None,
    confidence_data: dict | None = None,
) -> dict:
    """Run Joseph's complete analysis pipeline on a single prop.

    Combines player grading, game strategy, narrative detection,
    simulation data, and confidence scoring into one unified take.

    Args:
        player: Player stats dictionary.
        prop_line: The prop line value (e.g., 24.5 points).
        stat_type: The stat type being analysed (e.g., ``"points"``).
        game_context: Optional game-level context for narrative tags.
        simulation_result: Optional pre-computed simulation result.
        confidence_data: Optional confidence/tier data.

    Returns:
        Dictionary with ``player_grade``, ``strategy``,
        ``narrative_tags``, ``verdict``, ``confidence_tier``,
        ``platinum_lock``, ``joseph_commentary``, and ``ambient_line``.
    """
    try:
        # ── Player evaluation ──────────────────────────────
        player_grade = joseph_grade_player(player)

        # ── Game strategy ──────────────────────────────────
        strategy: dict = {}
        tags: list[dict] = []
        if game_context:
            home = game_context.get("home_team", {})
            away = game_context.get("away_team", {})
            if home and away:
                strategy = analyze_game_strategy(home, away, game_context=game_context)
                tags = strategy.get("narrative_tags", [])
            else:
                tags = detect_narrative_tags(game_context)

        # ── Confidence ─────────────────────────────────────
        tier = "Bronze"
        conf_score = 50.0
        if confidence_data:
            tier = confidence_data.get("tier", "Bronze")
            conf_score = confidence_data.get("score", 50.0)

        # ── Simulation-based verdict ───────────────────────
        sim_edge = 0.0
        sim_prob = 0.50
        if simulation_result:
            sim_edge = simulation_result.get("edge", 0.0)
            sim_prob = simulation_result.get("probability", 0.50)

        # ── Platinum lock check ────────────────────────────
        is_platinum = _check_platinum_lock(
            grade_score=player_grade.get("score", 0.0),
            conf_score=conf_score,
            sim_prob=sim_prob,
            tier=tier,
        )

        # ── Verdict ────────────────────────────────────────
        verdict = _build_verdict(
            player, stat_type, prop_line, player_grade,
            tier, conf_score, sim_prob, sim_edge, is_platinum, tags,
        )

        return {
            "player_grade": player_grade,
            "strategy": strategy,
            "narrative_tags": tags,
            "verdict": verdict,
            "confidence_tier": tier,
            "confidence_score": round(conf_score, 1),
            "platinum_lock": is_platinum,
            "joseph_commentary": verdict,
            "ambient_line": get_ambient_line(),
        }
    except Exception as exc:
        _logger.error("[JosephBrain] joseph_full_analysis error: %s", exc)
        return {
            "player_grade": {},
            "strategy": {},
            "narrative_tags": [],
            "verdict": "Unable to complete analysis.",
            "confidence_tier": "Bronze",
            "confidence_score": 0.0,
            "platinum_lock": False,
            "joseph_commentary": "Analysis failed — insufficient data.",
            "ambient_line": get_ambient_line(),
        }


# ============================================================
# SECTION: Platinum Lock
# ============================================================

def joseph_platinum_lock(
    player: dict,
    prop_line: float,
    stat_type: str,
    *,
    simulation_result: dict | None = None,
    confidence_data: dict | None = None,
) -> dict:
    """Determine if a prop qualifies as a Joseph M. Smith Platinum Lock.

    A Platinum Lock requires:
      - Player grade score ≥ 75
      - Confidence score ≥ 80 (Platinum or high Gold)
      - Simulation win probability ≥ 0.65

    Args:
        player: Player stats dictionary.
        prop_line: The prop line value.
        stat_type: The stat type being analysed.
        simulation_result: Optional simulation result dict.
        confidence_data: Optional confidence/tier data.

    Returns:
        Dictionary with ``is_lock``, ``lock_score``,
        ``reasons``, and ``joseph_pitch``.
    """
    try:
        grade_result = joseph_grade_player(player)
        grade_score = grade_result.get("score", 0.0)

        conf_score = 0.0
        tier = "Bronze"
        if confidence_data:
            conf_score = confidence_data.get("score", 0.0)
            tier = confidence_data.get("tier", "Bronze")

        sim_prob = 0.50
        if simulation_result:
            sim_prob = simulation_result.get("probability", 0.50)

        is_lock = _check_platinum_lock(grade_score, conf_score, sim_prob, tier)

        # Lock score: weighted composite
        lock_score = (
            0.35 * min(grade_score, 100)
            + 0.35 * min(conf_score, 100)
            + 0.30 * (sim_prob * 100)
        )
        lock_score = max(0.0, min(100.0, lock_score))

        reasons: list[str] = []
        if grade_score >= 75:
            reasons.append(f"Player grades at {grade_score:.0f}/100 ({grade_result.get('grade', 'N/A')})")
        if conf_score >= 80:
            reasons.append(f"Confidence score: {conf_score:.0f} ({tier})")
        if sim_prob >= 0.65:
            reasons.append(f"Simulation probability: {sim_prob:.0%}")

        name = player.get("player_name", player.get("name", "This player"))

        if is_lock:
            pitch = (
                f"💎 PLATINUM LOCK: {name} {stat_type} {'OVER' if sim_prob > 0.5 else 'UNDER'} "
                f"{prop_line}. Grade: {grade_result.get('grade')}, Confidence: {tier}, "
                f"Win Prob: {sim_prob:.0%}. This is as good as it gets."
            )
        else:
            pitch = (
                f"{name} {stat_type} at {prop_line} doesn't meet Platinum Lock criteria. "
                f"Grade: {grade_result.get('grade')}, Confidence: {tier}, "
                f"Win Prob: {sim_prob:.0%}."
            )

        return {
            "is_lock": is_lock,
            "lock_score": round(lock_score, 1),
            "reasons": reasons,
            "joseph_pitch": pitch,
        }
    except Exception as exc:
        _logger.error("[JosephBrain] joseph_platinum_lock error: %s", exc)
        return {
            "is_lock": False,
            "lock_score": 0.0,
            "reasons": [],
            "joseph_pitch": "Unable to evaluate platinum lock status.",
        }


def _check_platinum_lock(
    grade_score: float,
    conf_score: float,
    sim_prob: float,
    tier: str,
) -> bool:
    """Return True if criteria for a Platinum Lock are met."""
    return (
        grade_score >= 75
        and conf_score >= 80
        and sim_prob >= 0.65
        and tier in ("Platinum", "Gold")
    )


# ============================================================
# SECTION: Best Bets Generation
# ============================================================

def generate_best_bets(
    analysis_results: list[dict],
    *,
    max_bets: int = 5,
) -> list[dict]:
    """Select the top bets from a list of analysis results.

    Ranks by a composite of confidence score, simulation edge,
    and player grade, then returns the top *max_bets*.

    Args:
        analysis_results: List of analysis result dictionaries
            (each containing ``confidence_score``, ``edge``,
            ``player_grade``, ``player_name``, ``stat_type``,
            ``prop_line``, ``direction``).
        max_bets: Maximum number of best bets to return.

    Returns:
        List of best bet dictionaries sorted by composite score.
    """
    try:
        scored: list[tuple[float, dict]] = []
        for res in analysis_results:
            conf = res.get("confidence_score", 0.0)
            edge = abs(res.get("edge", 0.0))
            grade_score = 50.0
            pg = res.get("player_grade", {})
            if isinstance(pg, dict):
                grade_score = pg.get("score", 50.0)

            composite = 0.40 * conf + 0.35 * (edge * 100) + 0.25 * grade_score
            scored.append((composite, res))

        scored.sort(key=lambda x: x[0], reverse=True)

        best: list[dict] = []
        for score, res in scored[:max_bets]:
            best.append({
                "player_name": res.get("player_name", "Unknown"),
                "stat_type": res.get("stat_type", ""),
                "prop_line": res.get("prop_line", 0),
                "direction": res.get("direction", "OVER"),
                "confidence_tier": res.get("confidence_tier", "Bronze"),
                "confidence_score": res.get("confidence_score", 0.0),
                "edge": res.get("edge", 0.0),
                "composite_score": round(score, 1),
                "joseph_note": _best_bet_note(score, res),
            })

        return best
    except Exception as exc:
        _logger.error("[JosephBrain] generate_best_bets error: %s", exc)
        return []


def _best_bet_note(composite: float, result: dict) -> str:
    """Generate a short Joseph note for a best bet entry."""
    name = result.get("player_name", "This pick")
    if composite >= 80:
        return f"🔒 {name} — this is a LOCK. Top of the board."
    if composite >= 65:
        return f"✅ {name} — strong value play. High conviction."
    if composite >= 50:
        return f"📊 {name} — solid edge but not a slam dunk."
    return f"⚡ {name} — marginal edge, proceed with caution."


# ============================================================
# SECTION: Verdict Builder
# ============================================================

def _build_verdict(
    player: dict,
    stat_type: str,
    prop_line: float,
    player_grade: dict,
    tier: str,
    conf_score: float,
    sim_prob: float,
    sim_edge: float,
    is_platinum: bool,
    tags: list[dict],
) -> str:
    """Build Joseph's verdict text for display in the UI."""
    name = player.get("player_name", player.get("name", "This player"))
    grade = player_grade.get("grade", "N/A")
    direction = "OVER" if sim_prob > 0.5 else "UNDER"
    parts: list[str] = []

    if is_platinum:
        parts.append(f"💎 PLATINUM LOCK — {name} {stat_type} {direction} {prop_line}.")
    elif tier == "Gold":
        parts.append(f"🥇 Strong play — {name} {stat_type} {direction} {prop_line}.")
    elif tier == "Silver":
        parts.append(f"🥈 Decent value — {name} {stat_type} at {prop_line}.")
    else:
        parts.append(f"📊 {name} {stat_type} at {prop_line} — proceed cautiously.")

    parts.append(f"Grade: {grade} | Confidence: {conf_score:.0f} ({tier}) | Win Prob: {sim_prob:.0%}")

    if sim_edge > 0.05:
        parts.append(f"Edge: +{sim_edge:.1%} — this line is mispriced in our favor.")
    elif sim_edge < -0.03:
        parts.append(f"Negative edge ({sim_edge:.1%}) — the juice isn't worth the squeeze.")

    # Tag callouts
    for tag in tags[:2]:
        parts.append(f"🏷️ {tag.get('label', '')}")

    return " ".join(parts)
