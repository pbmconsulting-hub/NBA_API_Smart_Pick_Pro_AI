# ============================================================
# FILE: engine/joseph_strategy.py
# PURPOSE: Game strategy analysis and narrative detection for
#          the Joseph M. Smith AI persona.
# CONNECTS TO: engine/lineup_analysis.py, engine/joseph_brain.py
# ============================================================

"""Game strategy and narrative detection engine.

Provides functions for analysing game-level strategy (pace, style,
matchup advantages) and detecting narrative tags (revenge games,
back-to-backs, milestone chases, etc.) that Joseph M. Smith uses
in his commentary.
"""

import logging
import math
from typing import Any

_logger = logging.getLogger(__name__)


# ============================================================
# SECTION: Narrative Tags
# ============================================================

_NARRATIVE_DEFINITIONS: dict[str, dict[str, Any]] = {
    "revenge_game": {
        "label": "Revenge Game 🔥",
        "description": "Player facing their former team.",
        "impact": "boost",
        "magnitude": 1.10,
    },
    "back_to_back": {
        "label": "Back-to-Back ⚠️",
        "description": "Second game on consecutive days.",
        "impact": "diminish",
        "magnitude": 0.93,
    },
    "rest_advantage": {
        "label": "Rest Advantage 💤",
        "description": "Team has 2+ days rest vs opponent.",
        "impact": "boost",
        "magnitude": 1.05,
    },
    "milestone_chase": {
        "label": "Milestone Watch 🎯",
        "description": "Player approaching a statistical milestone.",
        "impact": "boost",
        "magnitude": 1.07,
    },
    "blowout_risk": {
        "label": "Blowout Risk 📉",
        "description": "Large point spread may limit minutes.",
        "impact": "diminish",
        "magnitude": 0.90,
    },
    "rivalry": {
        "label": "Rivalry Game 🏆",
        "description": "Historic rivalry or playoff rematch.",
        "impact": "boost",
        "magnitude": 1.04,
    },
    "national_tv": {
        "label": "National TV 📺",
        "description": "Nationally televised game — stars tend to show up.",
        "impact": "boost",
        "magnitude": 1.03,
    },
    "altitude": {
        "label": "Altitude Factor 🏔️",
        "description": "Playing in Denver — cardio disadvantage for visitors.",
        "impact": "diminish",
        "magnitude": 0.96,
    },
    "home_stand": {
        "label": "Home Stand 🏠",
        "description": "Team on extended home stretch (3+ games).",
        "impact": "boost",
        "magnitude": 1.03,
    },
    "road_fatigue": {
        "label": "Road Fatigue ✈️",
        "description": "Team on 4+ game road trip.",
        "impact": "diminish",
        "magnitude": 0.95,
    },
}


def detect_narrative_tags(game_context: dict) -> list[dict]:
    """Detect applicable narrative tags from a game context.

    Args:
        game_context: Dictionary containing keys such as
            ``is_b2b``, ``days_rest``, ``opp_days_rest``,
            ``former_team``, ``spread``, ``is_rivalry``,
            ``is_national_tv``, ``venue_city``,
            ``home_streak``, ``road_streak``,
            ``season_stat_total``, ``milestone_target``.

    Returns:
        List of narrative tag dictionaries with ``tag``, ``label``,
        ``description``, ``impact``, and ``magnitude``.
    """
    try:
        tags: list[dict] = []

        # Back-to-back
        if game_context.get("is_b2b"):
            tags.append(_tag_dict("back_to_back"))

        # Rest advantage
        days_rest = game_context.get("days_rest", 0)
        opp_rest = game_context.get("opp_days_rest", 0)
        if days_rest >= 2 and (days_rest - opp_rest) >= 2:
            tags.append(_tag_dict("rest_advantage"))

        # Revenge game
        if game_context.get("former_team"):
            tags.append(_tag_dict("revenge_game"))

        # Blowout risk
        spread = abs(game_context.get("spread", 0))
        if spread >= 10:
            tags.append(_tag_dict("blowout_risk"))

        # Rivalry
        if game_context.get("is_rivalry"):
            tags.append(_tag_dict("rivalry"))

        # National TV
        if game_context.get("is_national_tv"):
            tags.append(_tag_dict("national_tv"))

        # Altitude (Denver)
        venue = (game_context.get("venue_city") or "").strip().lower()
        is_home = game_context.get("is_home", True)
        if "denver" in venue and not is_home:
            tags.append(_tag_dict("altitude"))

        # Home stand
        home_streak = game_context.get("home_streak", 0)
        if home_streak >= 3:
            tags.append(_tag_dict("home_stand"))

        # Road fatigue
        road_streak = game_context.get("road_streak", 0)
        if road_streak >= 4:
            tags.append(_tag_dict("road_fatigue"))

        # Milestone chase
        total = game_context.get("season_stat_total", 0)
        target = game_context.get("milestone_target", 0)
        if target > 0 and 0 < (target - total) <= 50:
            tags.append(_tag_dict("milestone_chase"))

        return tags
    except Exception as exc:
        _logger.error("[JosephStrategy] detect_narrative_tags error: %s", exc)
        return []


def _tag_dict(tag_key: str) -> dict:
    """Build a narrative tag dictionary from the definitions."""
    defn = _NARRATIVE_DEFINITIONS.get(tag_key, {})
    return {
        "tag": tag_key,
        "label": defn.get("label", tag_key),
        "description": defn.get("description", ""),
        "impact": defn.get("impact", "neutral"),
        "magnitude": defn.get("magnitude", 1.0),
    }


# ============================================================
# SECTION: Game Strategy Analysis
# ============================================================

def analyze_game_strategy(
    home_team: dict,
    away_team: dict,
    *,
    game_context: dict | None = None,
) -> dict:
    """Analyse the strategic dynamics of a game matchup.

    Evaluates pace differential, offensive/defensive style matchups,
    key advantages, and generates a Joseph M. Smith strategy take.

    Args:
        home_team: Dictionary with team-level stats (``pace``,
            ``off_rating``, ``def_rating``, ``fg3_pct``,
            ``reb_rate``, ``tov_rate``, ``team_name``).
        away_team: Same structure for the away team.
        game_context: Optional context for narrative tag detection.

    Returns:
        Dictionary with ``pace_matchup``, ``style_matchup``,
        ``key_advantages``, ``narrative_tags``, ``projected_pace``,
        ``projected_total``, and ``joseph_strategy_take``.
    """
    try:
        home_pace = _safe(home_team.get("pace"), 100.0)
        away_pace = _safe(away_team.get("pace"), 100.0)
        projected_pace = (home_pace + away_pace) / 2.0

        home_ortg = _safe(home_team.get("off_rating"), 110.0)
        away_ortg = _safe(away_team.get("off_rating"), 110.0)
        home_drtg = _safe(home_team.get("def_rating"), 110.0)
        away_drtg = _safe(away_team.get("def_rating"), 110.0)

        # Projected total (simplified four-factors model)
        home_eff = (home_ortg + away_drtg) / 2.0
        away_eff = (away_ortg + home_drtg) / 2.0
        projected_total = (home_eff + away_eff) * projected_pace / 100.0

        # Pace classification
        if projected_pace >= 102:
            pace_label = "Up-Tempo"
        elif projected_pace >= 98:
            pace_label = "Average Pace"
        else:
            pace_label = "Grind-It-Out"

        # Style matchups
        home_fg3 = _safe(home_team.get("fg3_pct"), 0.35)
        away_fg3 = _safe(away_team.get("fg3_pct"), 0.35)

        style_notes: list[str] = []
        if home_fg3 > 0.37 and away_fg3 > 0.37:
            style_notes.append("Three-point shootout potential — both teams live beyond the arc.")
        if home_drtg < 108 and away_drtg < 108:
            style_notes.append("Defensive slugfest — both teams are elite defensively.")
        if projected_pace >= 103:
            style_notes.append("Expect a fast-paced, high-possession contest.")

        # Key advantages
        advantages: list[dict] = []
        if home_ortg - away_drtg > 3:
            advantages.append({
                "team": home_team.get("team_name", "Home"),
                "area": "Offensive Edge",
                "detail": f"Home offense ({home_ortg:.1f}) vs Away defense ({away_drtg:.1f})",
            })
        if away_ortg - home_drtg > 3:
            advantages.append({
                "team": away_team.get("team_name", "Away"),
                "area": "Offensive Edge",
                "detail": f"Away offense ({away_ortg:.1f}) vs Home defense ({home_drtg:.1f})",
            })

        # Narrative tags
        tags = detect_narrative_tags(game_context) if game_context else []

        # Joseph strategy take
        joseph_take = _build_strategy_take(
            home_team, away_team, pace_label, projected_total,
            style_notes, advantages, tags,
        )

        return {
            "pace_matchup": {
                "home_pace": home_pace,
                "away_pace": away_pace,
                "projected_pace": round(projected_pace, 1),
                "label": pace_label,
            },
            "style_matchup": style_notes,
            "key_advantages": advantages,
            "narrative_tags": tags,
            "projected_pace": round(projected_pace, 1),
            "projected_total": round(projected_total, 1),
            "joseph_strategy_take": joseph_take,
        }
    except Exception as exc:
        _logger.error("[JosephStrategy] analyze_game_strategy error: %s", exc)
        return {
            "pace_matchup": {},
            "style_matchup": [],
            "key_advantages": [],
            "narrative_tags": [],
            "projected_pace": 100.0,
            "projected_total": 220.0,
            "joseph_strategy_take": "Unable to analyze this matchup due to an error.",
        }


def _build_strategy_take(
    home: dict,
    away: dict,
    pace_label: str,
    proj_total: float,
    style_notes: list[str],
    advantages: list[dict],
    tags: list[dict],
) -> str:
    """Generate a Joseph M. Smith strategy narrative."""
    home_name = home.get("team_name", "Home")
    away_name = away.get("team_name", "Away")
    parts: list[str] = []

    parts.append(f"{home_name} vs {away_name} — projecting a {pace_label.lower()} game with ~{proj_total:.0f} total points.")

    if style_notes:
        parts.append(style_notes[0])

    if advantages:
        adv = advantages[0]
        parts.append(f"Edge: {adv['team']} has the {adv['area'].lower()} — {adv['detail']}.")

    for tag in tags[:2]:
        parts.append(f"🏷️ {tag['label']}: {tag['description']}")

    return " ".join(parts)


def _safe(value: Any, fallback: float) -> float:
    """Safely convert to float."""
    if value is None:
        return fallback
    try:
        v = float(value)
        return v if math.isfinite(v) else fallback
    except (TypeError, ValueError):
        return fallback
