# ============================================================
# FILE: engine/joseph_tickets.py
# PURPOSE: Ticket and parlay builder for the Joseph M. Smith
#          AI persona — multi-leg ticket construction and
#          narrative pitch generation.
# CONNECTS TO: engine/joseph_brain.py, engine/correlation.py
# ============================================================

"""Joseph M. Smith ticket and parlay builder.

Builds multi-leg parlay tickets from analysis results, checks
for leg correlation, and generates Joseph-style narrative pitches
for each constructed ticket.
"""

import logging
from typing import Any

_logger = logging.getLogger(__name__)


# ============================================================
# SECTION: Ticket Builder
# ============================================================

def build_joseph_ticket(
    picks: list[dict],
    *,
    max_legs: int = 6,
    min_confidence: float = 55.0,
) -> dict[str, Any]:
    """Build a Joseph M. Smith multi-leg parlay ticket.

    Filters and ranks picks by confidence, checks for correlated
    legs, and assembles a ticket with an implied parlay probability.

    Args:
        picks: List of pick dictionaries, each containing
            ``player_name``, ``stat_type``, ``prop_line``,
            ``direction``, ``confidence_score``, ``confidence_tier``,
            ``probability`` (win probability), ``team``, ``game_id``.
        max_legs: Maximum number of legs on the ticket.
        min_confidence: Minimum confidence score to include a leg.

    Returns:
        Dictionary with ``legs``, ``leg_count``, ``implied_probability``,
        ``ticket_grade``, ``correlation_warning``, and ``joseph_pitch``.
    """
    try:
        # Filter by minimum confidence
        eligible = [
            p for p in picks
            if p.get("confidence_score", 0) >= min_confidence
        ]

        if not eligible:
            return _empty_ticket("No picks met the minimum confidence threshold.")

        # Sort by confidence descending
        eligible.sort(key=lambda p: p.get("confidence_score", 0), reverse=True)

        # Select top legs up to max
        selected = eligible[:max_legs]

        # Calculate implied probability
        implied_prob = 1.0
        for leg in selected:
            prob = leg.get("probability", 0.50)
            prob = max(0.01, min(0.99, prob))
            implied_prob *= prob

        # Check for correlation issues
        correlation_warning = _check_ticket_correlation(selected)

        # Grade the ticket
        avg_confidence = (
            sum(p.get("confidence_score", 0) for p in selected) / len(selected)
            if selected else 0.0
        )
        ticket_grade = _grade_ticket(avg_confidence, len(selected), implied_prob)

        # Build leg summaries
        legs = []
        for leg in selected:
            legs.append({
                "player_name": leg.get("player_name", "Unknown"),
                "stat_type": leg.get("stat_type", ""),
                "prop_line": leg.get("prop_line", 0),
                "direction": leg.get("direction", "OVER"),
                "confidence_score": round(leg.get("confidence_score", 0), 1),
                "confidence_tier": leg.get("confidence_tier", "Bronze"),
                "probability": round(leg.get("probability", 0.50), 3),
                "team": leg.get("team", ""),
            })

        pitch = generate_ticket_pitch(legs, implied_prob, ticket_grade, correlation_warning)

        return {
            "legs": legs,
            "leg_count": len(legs),
            "implied_probability": round(implied_prob, 4),
            "avg_confidence": round(avg_confidence, 1),
            "ticket_grade": ticket_grade,
            "correlation_warning": correlation_warning,
            "joseph_pitch": pitch,
        }
    except Exception as exc:
        _logger.error("[JosephTickets] build_joseph_ticket error: %s", exc)
        return _empty_ticket(f"Error building ticket: {exc}")


def _empty_ticket(message: str) -> dict[str, Any]:
    return {
        "legs": [],
        "leg_count": 0,
        "implied_probability": 0.0,
        "avg_confidence": 0.0,
        "ticket_grade": "F",
        "correlation_warning": "",
        "joseph_pitch": message,
    }


def _grade_ticket(avg_confidence: float, leg_count: int, implied_prob: float) -> str:
    """Grade a ticket based on confidence, leg count, and implied probability."""
    # Penalize large parlays (diminishing returns)
    leg_penalty = max(0, (leg_count - 3) * 5)
    score = avg_confidence - leg_penalty + (implied_prob * 20)
    score = max(0, min(100, score))

    if score >= 80:
        return "A"
    if score >= 70:
        return "B+"
    if score >= 60:
        return "B"
    if score >= 50:
        return "C+"
    if score >= 40:
        return "C"
    return "D"


def _check_ticket_correlation(legs: list[dict]) -> str:
    """Check for correlated legs on the ticket."""
    # Check same-game correlation
    game_ids = [leg.get("game_id", "") for leg in legs if leg.get("game_id")]
    seen_games: dict[str, int] = {}
    for gid in game_ids:
        if gid:
            seen_games[gid] = seen_games.get(gid, 0) + 1

    same_game_count = sum(1 for v in seen_games.values() if v > 1)

    # Check same-team correlation
    teams = [leg.get("team", "") for leg in legs if leg.get("team")]
    seen_teams: dict[str, int] = {}
    for t in teams:
        if t:
            seen_teams[t] = seen_teams.get(t, 0) + 1

    same_team_count = sum(1 for v in seen_teams.values() if v > 1)

    warnings: list[str] = []
    if same_game_count > 0:
        warnings.append(
            f"⚠️ {same_game_count} game(s) with multiple legs — "
            f"same-game legs are correlated."
        )
    if same_team_count > 0:
        warnings.append(
            f"⚠️ {same_team_count} team(s) with multiple legs — "
            f"same-team props can move together."
        )

    return " ".join(warnings) if warnings else ""


# ============================================================
# SECTION: Ticket Pitch
# ============================================================

def generate_ticket_pitch(
    legs: list[dict],
    implied_prob: float,
    grade: str,
    correlation_warning: str,
) -> str:
    """Generate a Joseph M. Smith narrative pitch for a parlay ticket.

    Args:
        legs: List of leg dictionaries.
        implied_prob: Combined implied probability.
        grade: Ticket grade letter.
        correlation_warning: Correlation warning string (may be empty).

    Returns:
        A narrative string suitable for display.
    """
    try:
        count = len(legs)
        if count == 0:
            return "No legs on this ticket — nothing to pitch."

        parts: list[str] = []

        # Opener
        if count <= 2:
            parts.append(f"Here's a lean {count}-leg ticket.")
        elif count <= 4:
            parts.append(f"Building a {count}-leg parlay — let's break it down.")
        else:
            parts.append(f"Big swing! {count}-leg parlay on the board.")

        # Highlight top leg
        top_leg = max(legs, key=lambda leg: leg.get("confidence_score", 0))
        top_name = top_leg.get("player_name", "Unknown")
        top_stat = top_leg.get("stat_type", "")
        top_dir = top_leg.get("direction", "OVER")
        top_line = top_leg.get("prop_line", 0)
        parts.append(
            f"Anchor leg: {top_name} {top_stat} {top_dir} {top_line} "
            f"(Confidence: {top_leg.get('confidence_score', 0):.0f})."
        )

        # Implied probability
        pct = implied_prob * 100
        if pct >= 15:
            parts.append(f"Implied hit rate: {pct:.1f}% — realistic range.")
        elif pct >= 5:
            parts.append(f"Implied hit rate: {pct:.1f}% — long shot but the value is there.")
        else:
            parts.append(f"Implied hit rate: {pct:.1f}% — lottery ticket territory.")

        # Grade
        parts.append(f"Ticket grade: {grade}.")

        # Correlation warning
        if correlation_warning:
            parts.append(correlation_warning)

        return " ".join(parts)
    except Exception as exc:
        _logger.error("[JosephTickets] generate_ticket_pitch error: %s", exc)
        return "Unable to generate ticket pitch."
