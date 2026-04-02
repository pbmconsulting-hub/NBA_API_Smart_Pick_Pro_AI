# ============================================================
# FILE: engine/joseph_eval.py
# PURPOSE: Joseph's INDEPENDENT player evaluation — his OWN grades
# CONNECTS TO: data/advanced_metrics.py, engine/math_helpers.py
# ============================================================

import logging
import math

_logger = logging.getLogger(__name__)

try:
    from data.advanced_metrics import normalize, classify_player_archetype
except ImportError:
    _logger.warning("[JosephEval] Could not import from advanced_metrics")

    def normalize(value, min_val, max_val, out_min=0.0, out_max=100.0):
        if max_val == min_val:
            return out_min
        clamped = max(min_val, min(max_val, value))
        return out_min + (clamped - min_val) / (max_val - min_val) * (out_max - out_min)

    def classify_player_archetype(player):
        return "Role Player"

try:
    from engine.math_helpers import _safe_float
except ImportError:

    def _safe_float(value, fallback=0.0):
        try:
            v = float(value)
            if math.isfinite(v):
                return v
            return float(fallback)
        except (ValueError, TypeError):
            return float(fallback)


# ------------------------------------------------------------------
# Archetype profiles — exported so other modules (e.g. joseph_brain)
# can access archetype metadata without re-defining it.
# ------------------------------------------------------------------

ARCHETYPE_PROFILES: dict = {
    "Stretch Big":          {"role": "spacing big",       "key_stats": ["threes", "rebounds"]},
    "3-and-D Wing":         {"role": "two-way wing",      "key_stats": ["threes", "steals"]},
    "Shot Creator":         {"role": "primary scorer",    "key_stats": ["points", "assists"]},
    "Primary Ball Handler": {"role": "lead guard",        "key_stats": ["assists", "points"]},
    "Rim Runner":           {"role": "athletic big",      "key_stats": ["rebounds", "blocks"]},
    "Floor General":        {"role": "pass-first guard",  "key_stats": ["assists", "steals"]},
    "Two-Way Wing":         {"role": "versatile wing",    "key_stats": ["steals", "rebounds"]},
    "Defensive Anchor":     {"role": "rim protector",     "key_stats": ["blocks", "rebounds"]},
    "Sharpshooter":         {"role": "catch-and-shoot",   "key_stats": ["threes", "points"]},
    "Slasher":              {"role": "driving scorer",    "key_stats": ["points", "assists"]},
    "Post Scorer":          {"role": "interior scorer",   "key_stats": ["points", "rebounds"]},
    "Combo Guard":          {"role": "hybrid guard",      "key_stats": ["points", "assists"]},
    "Role Player":          {"role": "utility",           "key_stats": ["rebounds", "assists"]},
}


# ------------------------------------------------------------------
# Letter-grade mapping
# ------------------------------------------------------------------

def letter_grade(score: float) -> str:
    """Convert a numeric score (0-100) into a letter grade.

    Args:
        score: Numeric score between 0 and 100.

    Returns:
        A letter-grade string from ``"A+"`` down to ``"F"``.
    """
    try:
        score = float(score)
    except (TypeError, ValueError):
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


# ------------------------------------------------------------------
# Gravity score
# ------------------------------------------------------------------

def calculate_gravity_score(player: dict) -> float:
    """Estimate offensive gravity — how much a player warps the defense.

    Blends three-point threat, free-throw drawing, usage rate, and
    scoring efficiency into a single 0-100 number.

    Args:
        player: Player statistics dictionary with keys such as
            ``fg3a``, ``fg3_pct``, ``fta``, ``fga``,
            ``turnovers_avg``, ``points_avg``, ``minutes_avg``.

    Returns:
        Gravity score clamped to the range ``[0.0, 100.0]``.
    """
    fg3a = _safe_float(player.get("fg3a", 0))
    fg3_pct = _safe_float(player.get("fg3_pct", 0))
    fta = _safe_float(player.get("fta", 0))
    fga = _safe_float(player.get("fga", 0))
    tov = _safe_float(player.get("turnovers_avg", 0))
    pts = _safe_float(player.get("points_avg", 0))
    minutes = _safe_float(player.get("minutes_avg", 0))

    usage_est = 100 * (fga + 0.44 * fta + tov) / (max(minutes, 1) * 2.0)

    gravity = (
        0.40 * normalize(fg3a * fg3_pct, 0, 4, 0, 100)
        + 0.25 * normalize(fta / max(fga, 1), 0, 0.5, 0, 100)
        + 0.20 * normalize(usage_est, 15, 35, 0, 100)
        + 0.15 * normalize(pts / max(fga + 0.44 * fta + tov, 1), 0.8, 1.3, 0, 100)
    )
    return max(0.0, min(100.0, gravity))


# ------------------------------------------------------------------
# Switchability score
# ------------------------------------------------------------------

def calculate_switchability(player: dict) -> float:
    """Measure how effectively a player can switch across defensive assignments.

    Combines steal rate, block rate, rebounding, minutes, and
    stocks-per-minute into a 0-100 switchability index.

    Args:
        player: Player statistics dictionary with keys such as
            ``steals_avg``, ``blocks_avg``, ``rebounds_avg``,
            ``minutes_avg``.

    Returns:
        Switchability score clamped to the range ``[0.0, 100.0]``.
    """
    stl = _safe_float(player.get("steals_avg", 0))
    blk = _safe_float(player.get("blocks_avg", 0))
    reb = _safe_float(player.get("rebounds_avg", 0))
    minutes = _safe_float(player.get("minutes_avg", 0))

    switchability = (
        0.40 * (normalize(stl, 0, 2, 0, 50) + normalize(blk, 0, 2, 0, 50))
        + 0.20 * normalize(reb, 3, 12, 0, 100)
        + 0.20 * normalize(minutes, 20, 36, 0, 100)
        + 0.20 * normalize((stl + blk) / max(minutes, 1) * 36, 0, 5, 0, 100)
    )
    return max(0.0, min(100.0, switchability))


# ------------------------------------------------------------------
# Scheme-fit helpers
# ------------------------------------------------------------------

_FAVORABLE_MATCHUPS: dict = {
    "Stretch Big": ["zone", "drop_coverage"],
    "3-and-D Wing": ["switch_everything", "zone"],
    "Shot Creator": ["drop_coverage", "man_to_man"],
    "Primary Ball Handler": ["zone", "press"],
    "Rim Runner": ["switch_everything", "small_ball"],
    "Floor General": ["zone", "press"],
    "Two-Way Wing": ["switch_everything", "man_to_man"],
    "Defensive Anchor": ["man_to_man", "drop_coverage"],
    "Sharpshooter": ["zone", "drop_coverage"],
    "Slasher": ["zone", "drop_coverage"],
    "Post Scorer": ["switch_everything", "small_ball"],
    "Combo Guard": ["zone", "press"],
    "Role Player": ["man_to_man"],
}

_UNFAVORABLE_MATCHUPS: dict = {
    "Stretch Big": ["switch_everything", "press"],
    "3-and-D Wing": ["drop_coverage"],
    "Shot Creator": ["switch_everything", "press"],
    "Primary Ball Handler": ["switch_everything", "man_to_man"],
    "Rim Runner": ["zone", "press"],
    "Floor General": ["press", "man_to_man"],
    "Two-Way Wing": ["zone"],
    "Defensive Anchor": ["small_ball", "zone"],
    "Sharpshooter": ["switch_everything", "press"],
    "Slasher": ["switch_everything", "man_to_man"],
    "Post Scorer": ["zone", "press"],
    "Combo Guard": ["man_to_man"],
    "Role Player": ["press", "zone"],
}

_POSITIVE_NARRATIVE_TAGS = frozenset({
    "revenge_game",
    "contract_year",
    "nationally_televised",
    "rivalry",
})


def _determine_scheme_fit(archetype: str, scheme: dict) -> str:
    """Return ``'exploitable'``, ``'problematic'``, or ``'neutral'``.

    Args:
        archetype: Player archetype string.
        scheme: Opponent scheme dictionary (must contain ``primary_scheme``).

    Returns:
        One of ``'exploitable'``, ``'problematic'``, or ``'neutral'``.
    """
    try:
        primary = scheme.get("primary_scheme", "man_to_man") if scheme else "man_to_man"
        favorable = _FAVORABLE_MATCHUPS.get(archetype, [])
        unfavorable = _UNFAVORABLE_MATCHUPS.get(archetype, [])
        if primary in favorable:
            return "exploitable"
        if primary in unfavorable:
            return "problematic"
    except Exception:
        _logger.debug("[JosephEval] _determine_scheme_fit error, returning neutral")
    return "neutral"


# ------------------------------------------------------------------
# Core evaluation
# ------------------------------------------------------------------

def _default_grade_result() -> dict:
    """Return a safe fallback grading dictionary with zeroes and ``'F'``."""
    return {
        "overall_grade": "F",
        "offense_grade": 0.0,
        "defense_grade": 0.0,
        "impact_grade": 0.0,
        "matchup_grade": 0.0,
        "offensive_profile": {
            "scoring_volume": 0.0,
            "scoring_efficiency": 0.0,
            "creation_burden": 0.0,
            "gravity": 0.0,
            "free_throw_drawing": 0.0,
        },
        "defensive_profile": {
            "rim_protection": 0.0,
            "perimeter_disruption": 0.0,
            "rebounding_impact": 0.0,
            "switchability": 0.0,
            "hustle_index": 0.0,
        },
        "tonight_factors": {
            "matchup_advantage": 0.0,
            "scheme_fit": "neutral",
            "fatigue_risk": 0.05,
            "motivation_boost": 0.0,
            "ceiling_percentile": 0.0,
            "floor_percentile": 0.0,
        },
        "joseph_take": "",
    }


def joseph_grade_player(player: dict, game_context: dict) -> dict:
    """Grade a single player using Joseph's independent evaluation system.

    Produces offensive, defensive, impact, and matchup grades together
    with rich sub-profiles and tonight-specific factors.

    Args:
        player: Dictionary of player stats.  Expected keys include
            ``name``, ``position``, ``points_avg``, ``assists_avg``,
            ``rebounds_avg``, ``steals_avg``, ``blocks_avg``,
            ``turnovers_avg``, ``minutes_avg``, ``fg3a``, ``fg3_pct``,
            ``fga``, ``fta``, ``defensive_rebounds_avg``.
        game_context: Dictionary describing tonight's matchup.  Expected
            keys: ``opponent_team``, ``opponent_def_rating``,
            ``narrative_tags`` (list of str), ``scheme`` (dict with
            ``primary_scheme``), ``spread``, ``total``, ``rest_days``.

    Returns:
        A dictionary containing letter grades, numeric sub-grades,
        offensive/defensive profiles, tonight factors, and a placeholder
        ``joseph_take`` string.
    """
    try:
        # -------------------------------------------------------
        # Extract raw stats
        # -------------------------------------------------------
        pts = _safe_float(player.get("points_avg", 0))
        ast = _safe_float(player.get("assists_avg", 0))
        reb = _safe_float(player.get("rebounds_avg", 0))
        stl = _safe_float(player.get("steals_avg", 0))
        blk = _safe_float(player.get("blocks_avg", 0))
        tov = _safe_float(player.get("turnovers_avg", 0))
        minutes = _safe_float(player.get("minutes_avg", 0))
        fg3a = _safe_float(player.get("fg3a", 0))
        fg3_pct = _safe_float(player.get("fg3_pct", 0))
        fga = _safe_float(player.get("fga", 0))
        fta = _safe_float(player.get("fta", 0))
        dreb = _safe_float(player.get("defensive_rebounds_avg", 0))
        position = str(player.get("position", "")).upper().strip()

        # True-shooting percentage estimate
        ts_denom = 2 * (fga + 0.44 * fta)
        ts_pct = pts / max(ts_denom, 1)

        # Usage estimate
        usage_est = 100 * (fga + 0.44 * fta + tov) / (max(minutes, 1) * 2.0)

        # Assist rate proxy (ast * 3 as specified)
        ast_rate_proxy = ast * 3

        # FTA rate
        fta_rate = fta / max(fga, 1)

        # Stocks per minute
        stocks_per_min = (stl + blk) / max(minutes, 1)

        # Archetype
        archetype = classify_player_archetype(player)

        # -------------------------------------------------------
        # Offensive sub-components
        # -------------------------------------------------------
        scoring_eff = normalize(ts_pct, 0.45, 0.65, 0, 100)
        creation = normalize(usage_est + ast_rate_proxy, 0, 60, 0, 100)
        gravity = calculate_gravity_score(player)
        volume = normalize(pts, 0, 35, 0, 100)
        free_throw_drawing = normalize(fta_rate, 0, 0.5, 0, 100)

        offense_grade = (
            0.30 * scoring_eff
            + 0.25 * creation
            + 0.25 * gravity
            + 0.20 * volume
        )

        # -------------------------------------------------------
        # Defensive sub-components
        # -------------------------------------------------------
        if position in ("C", "PF"):
            rim_prot = normalize(blk, 0, 3, 0, 100)
        else:
            rim_prot = normalize(stl, 0, 2, 0, 100)

        perim = normalize(stl, 0, 2.5, 0, 100)
        reb_grade = normalize(dreb, 0, 10, 0, 100)
        switch_grade = calculate_switchability(player)
        hustle = normalize(stocks_per_min * 36, 0, 5, 0, 100)

        defense_grade = (
            0.30 * rim_prot
            + 0.25 * perim
            + 0.20 * reb_grade
            + 0.15 * switch_grade
            + 0.10 * hustle
        )

        # -------------------------------------------------------
        # Impact grade
        # -------------------------------------------------------
        usage_component = normalize(usage_est, 15, 35, 0, 100)
        ast_tov_component = normalize(ast / max(tov, 0.5), 0, 4, 0, 100)

        impact_grade = (
            0.40 * offense_grade
            + 0.30 * defense_grade
            + 0.15 * usage_component
            + 0.15 * ast_tov_component
        )

        # -------------------------------------------------------
        # Matchup grade
        # -------------------------------------------------------
        opp_def_rating = _safe_float(game_context.get("opponent_def_rating", 110))
        narrative_tags = game_context.get("narrative_tags", []) or []
        scheme = game_context.get("scheme", {}) or {}

        matchup_base = normalize(130 - opp_def_rating, 0, 30, 0, 100)

        positive_tag_count = sum(
            1 for tag in narrative_tags if tag in _POSITIVE_NARRATIVE_TAGS
        )
        narrative_boost = min(positive_tag_count * 5, 25)

        scheme_fit_label = _determine_scheme_fit(archetype, scheme)
        if scheme_fit_label == "exploitable":
            scheme_bonus = 10.0
        elif scheme_fit_label == "problematic":
            scheme_bonus = -10.0
        else:
            scheme_bonus = 0.0

        matchup_grade = max(0.0, min(100.0, matchup_base + narrative_boost + scheme_bonus))

        # -------------------------------------------------------
        # Composite / overall
        # -------------------------------------------------------
        composite = (
            impact_grade * 0.50
            + matchup_grade * 0.30
            + offense_grade * 0.10
            + defense_grade * 0.10
        )

        # -------------------------------------------------------
        # Tonight factors
        # -------------------------------------------------------
        rest_days = _safe_float(game_context.get("rest_days", 2))
        if rest_days == 0:
            fatigue_risk = 0.8
        elif rest_days == 1:
            fatigue_risk = 0.2
        else:
            fatigue_risk = 0.05

        motivation_boost = min(
            sum(1 for tag in narrative_tags if tag in _POSITIVE_NARRATIVE_TAGS) / 5.0,
            1.0,
        )

        ceiling_percentile = min(100.0, impact_grade + 15)
        floor_percentile = max(0.0, impact_grade - 20)

        # -------------------------------------------------------
        # Offensive profile (for defensive_profile rim_protection
        # we use the position-aware value computed above)
        # -------------------------------------------------------
        offensive_profile = {
            "scoring_volume": normalize(pts, 0, 35, 0, 100),
            "scoring_efficiency": normalize(ts_pct, 0.45, 0.65, 0, 100),
            "creation_burden": normalize(usage_est + ast_rate_proxy, 0, 60, 0, 100),
            "gravity": gravity,
            "free_throw_drawing": free_throw_drawing,
        }

        defensive_profile = {
            "rim_protection": normalize(blk, 0, 3, 0, 100) if position in ("C", "PF") else 0.0,
            "perimeter_disruption": normalize(stl, 0, 2.5, 0, 100),
            "rebounding_impact": normalize(dreb, 0, 10, 0, 100),
            "switchability": switch_grade,
            "hustle_index": normalize(stocks_per_min * 36, 0, 5, 0, 100),
        }

        tonight_factors = {
            "matchup_advantage": matchup_grade,
            "scheme_fit": scheme_fit_label,
            "fatigue_risk": fatigue_risk,
            "motivation_boost": motivation_boost,
            "ceiling_percentile": ceiling_percentile,
            "floor_percentile": floor_percentile,
        }

        return {
            "overall_grade": letter_grade(composite),
            "offense_grade": offense_grade,
            "defense_grade": defense_grade,
            "impact_grade": impact_grade,
            "matchup_grade": matchup_grade,
            "offensive_profile": offensive_profile,
            "defensive_profile": defensive_profile,
            "tonight_factors": tonight_factors,
            "joseph_take": "",
        }

    except Exception:
        _logger.exception("[JosephEval] Error grading player: %s", player.get("name", "unknown"))
        return _default_grade_result()


# ------------------------------------------------------------------
# Player comparison
# ------------------------------------------------------------------

def joseph_compare_players(player_a: dict, player_b: dict) -> dict:
    """Compare two players head-to-head using Joseph's grading system.

    Both players are graded via :func:`joseph_grade_player` with an
    empty game-context (all defaults), then compared across the four
    main grade categories.

    Args:
        player_a: First player statistics dictionary.
        player_b: Second player statistics dictionary.

    Returns:
        A dictionary with the winner's name, advantage margins for
        each category, and a one-sentence comparison take.
    """
    try:
        empty_context: dict = {
            "opponent_team": "",
            "opponent_def_rating": 110,
            "narrative_tags": [],
            "scheme": {},
            "spread": 0,
            "total": 0,
            "rest_days": 2,
        }

        grade_a = joseph_grade_player(player_a, empty_context)
        grade_b = joseph_grade_player(player_b, empty_context)

        name_a = str(player_a.get("name", "Player A"))
        name_b = str(player_b.get("name", "Player B"))

        impact_a = _safe_float(grade_a.get("impact_grade", 0))
        impact_b = _safe_float(grade_b.get("impact_grade", 0))
        offense_a = _safe_float(grade_a.get("offense_grade", 0))
        offense_b = _safe_float(grade_b.get("offense_grade", 0))
        defense_a = _safe_float(grade_a.get("defense_grade", 0))
        defense_b = _safe_float(grade_b.get("defense_grade", 0))
        matchup_a = _safe_float(grade_a.get("matchup_grade", 0))
        matchup_b = _safe_float(grade_b.get("matchup_grade", 0))

        winner = name_a if impact_a >= impact_b else name_b
        advantage_margin = abs(impact_a - impact_b)

        offense_edge = name_a if offense_a >= offense_b else name_b
        defense_edge = name_a if defense_a >= defense_b else name_b
        impact_edge = name_a if impact_a >= impact_b else name_b
        matchup_edge = name_a if matchup_a >= matchup_b else name_b

        if advantage_margin > 0:
            comparison_take = (
                f"{winner} holds the edge with a {advantage_margin:.1f}-point "
                f"impact advantage over the competition."
            )
        else:
            comparison_take = (
                f"{name_a} and {name_b} are dead even in impact grade."
            )

        return {
            "winner": winner,
            "advantage_margin": advantage_margin,
            "offense_edge": offense_edge,
            "defense_edge": defense_edge,
            "impact_edge": impact_edge,
            "matchup_edge": matchup_edge,
            "joseph_comparison_take": comparison_take,
        }

    except Exception:
        _logger.exception("[JosephEval] Error comparing players")
        name_a = str(player_a.get("name", "Player A"))
        name_b = str(player_b.get("name", "Player B"))
        return {
            "winner": name_a,
            "advantage_margin": 0.0,
            "offense_edge": name_a,
            "defense_edge": name_b,
            "impact_edge": name_a,
            "matchup_edge": name_a,
            "joseph_comparison_take": "Unable to compare players due to an evaluation error.",
        }
