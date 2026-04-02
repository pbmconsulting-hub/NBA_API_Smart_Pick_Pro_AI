# ============================================================
# FILE: engine/trade_evaluator.py
# PURPOSE: Trade evaluation, roster construction, and cap analysis
# CONNECTS TO: data/advanced_metrics.py, engine/math_helpers.py,
#              engine/joseph_eval.py
# ============================================================

import logging
import math
from typing import List, Optional

_logger = logging.getLogger(__name__)

# ------------------------------------------------------------------
# External imports (graceful fallbacks)
# ------------------------------------------------------------------

try:
    from data.advanced_metrics import normalize, classify_player_archetype
except ImportError:
    _logger.warning("[TradeEval] Could not import from advanced_metrics")

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

try:
    from engine.joseph_eval import letter_grade
except ImportError:
    _logger.warning("[TradeEval] Could not import letter_grade from joseph_eval")

    def letter_grade(score: float) -> str:
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
# Constants
# ------------------------------------------------------------------

SALARY_CAP_2025: float = 140.588
LUXURY_TAX_THRESHOLD: float = 171.292
FIRST_APRON: float = 178.132
SECOND_APRON: float = 188.931
WAR_TO_DOLLARS_MULTIPLIER: float = 5.5
REPLACEMENT_LEVEL_WAR: float = 0.0

_POSITION_GROUPS: dict = {
    "PG": "guard",
    "SG": "guard",
    "SF": "wing",
    "PF": "wing",
    "C": "big",
    "G": "guard",
    "F": "wing",
}

_SKILL_TAGS: dict = {
    "shooting": ["fg3_pct", "fg3a"],
    "playmaking": ["assists_avg"],
    "rim_protection": ["blocks_avg"],
    "rebounding": ["rebounds_avg"],
    "perimeter_defense": ["steals_avg"],
    "scoring": ["points_avg"],
}


# ------------------------------------------------------------------
# 1. WAR Calculation
# ------------------------------------------------------------------

def calculate_player_war(player_data: dict) -> dict:
    """Calculate Wins Above Replacement using box-score approximation.

    Offensive WAR components: scoring efficiency, playmaking, and
    spacing contribution.  Defensive WAR components: steal/block rate,
    rebounding, and switchability proxy.  Both are scaled to an
    82-game season based on minutes played.

    Args:
        player_data: Player statistics dictionary with keys such as
            ``points_avg``, ``assists_avg``, ``rebounds_avg``,
            ``steals_avg``, ``blocks_avg``, ``turnovers_avg``,
            ``minutes_avg``, ``fg3a``, ``fg3_pct``, ``fga``, ``fta``,
            ``games_played``.

    Returns:
        Dictionary with ``total_war``, ``offensive_war``,
        ``defensive_war``, ``war_percentile``, and ``tier``.
    """
    try:
        pts = _safe_float(player_data.get("points_avg", 0))
        ast = _safe_float(player_data.get("assists_avg", 0))
        reb = _safe_float(player_data.get("rebounds_avg", 0))
        stl = _safe_float(player_data.get("steals_avg", 0))
        blk = _safe_float(player_data.get("blocks_avg", 0))
        tov = _safe_float(player_data.get("turnovers_avg", 0))
        minutes = _safe_float(player_data.get("minutes_avg", 0))
        fg3a = _safe_float(player_data.get("fg3a", 0))
        fg3_pct = _safe_float(player_data.get("fg3_pct", 0))
        fga = _safe_float(player_data.get("fga", 0))
        fta = _safe_float(player_data.get("fta", 0))
        games = _safe_float(player_data.get("games_played", 0))

        # True-shooting percentage estimate
        ts_denom = 2.0 * (fga + 0.44 * fta)
        ts_pct = pts / max(ts_denom, 1.0)

        # Points above league-average efficiency (assume ~0.55 TS%)
        league_avg_ts = 0.55
        scoring_efficiency = (ts_pct - league_avg_ts) * pts * 0.5

        # Playmaking value: assists create ~1.0 pts of value, minus turnovers
        playmaking = ast * 1.0 - tov * 1.2

        # Spacing contribution from 3-point shooting
        spacing = fg3a * fg3_pct * 1.5

        offensive_war_per_game = scoring_efficiency + playmaking * 0.2 + spacing * 0.15

        # Defensive WAR components
        stocks_rate = (stl + blk) / max(minutes, 1.0) * 36.0
        defensive_stocks = stocks_rate * 0.35

        reb_contribution = reb * 0.1

        # Switchability proxy: guards who rebound or bigs who steal
        position = str(player_data.get("position", "")).upper().strip()
        pos_group = _POSITION_GROUPS.get(position, "wing")
        if pos_group == "guard":
            switchability_bonus = min(reb * 0.05, 0.3)
        elif pos_group == "big":
            switchability_bonus = min(stl * 0.15, 0.3)
        else:
            switchability_bonus = min((stl + reb * 0.02) * 0.1, 0.25)

        defensive_war_per_game = defensive_stocks + reb_contribution + switchability_bonus

        # Scale to per-82-game season using minutes played
        minutes_factor = min(minutes / 36.0, 1.0)
        games_factor = min(games / 82.0, 1.0) if games > 0 else 1.0

        offensive_war = offensive_war_per_game * minutes_factor * 82.0 / 10.0
        defensive_war = defensive_war_per_game * minutes_factor * 82.0 / 10.0

        # Adjust if player has limited games
        if 0 < games < 82:
            prorated = games_factor
            offensive_war = offensive_war * prorated + offensive_war * (1.0 - prorated) * 0.5
            defensive_war = defensive_war * prorated + defensive_war * (1.0 - prorated) * 0.5

        total_war = offensive_war + defensive_war

        # Percentile estimate (league median WAR ~ 1.0, stdev ~ 3.5)
        z_score = (total_war - 1.0) / max(3.5, 0.01)
        war_percentile = max(0.0, min(100.0, 50.0 + z_score * 20.0))

        # Tier assignment
        if total_war > 10.0:
            tier = "Superstar"
        elif total_war >= 7.0:
            tier = "All-Star"
        elif total_war >= 3.0:
            tier = "Starter"
        elif total_war >= 1.0:
            tier = "Rotation"
        elif total_war >= 0.0:
            tier = "Bench"
        else:
            tier = "Replacement"

        return {
            "total_war": round(total_war, 2),
            "offensive_war": round(offensive_war, 2),
            "defensive_war": round(defensive_war, 2),
            "war_percentile": round(war_percentile, 1),
            "tier": tier,
        }

    except Exception:
        _logger.exception(
            "[TradeEval] Error calculating WAR for %s",
            player_data.get("name", "unknown"),
        )
        return {
            "total_war": 0.0,
            "offensive_war": 0.0,
            "defensive_war": 0.0,
            "war_percentile": 0.0,
            "tier": "Replacement",
        }


# ------------------------------------------------------------------
# 2. Contract Value Evaluation
# ------------------------------------------------------------------

def evaluate_player_contract_value(
    player_data: dict,
    salary: float = 0.0,
    years_remaining: int = 1,
) -> dict:
    """Compare a player's WAR-based market value to their actual salary.

    Uses the WAR-to-dollars multiplier to estimate what a player
    *should* earn, then compares against the actual salary to produce
    a surplus value and letter-style contract grade.

    Args:
        player_data: Player statistics dictionary (passed to
            :func:`calculate_player_war`).
        salary: Current annual salary in millions of dollars.
        years_remaining: Years left on the contract (used for
            age-based depreciation).

    Returns:
        Dictionary with ``war``, ``estimated_value_millions``,
        ``salary_millions``, ``surplus_value``, and ``contract_grade``.
    """
    try:
        salary = _safe_float(salary, 0.0)
        years_remaining = max(1, int(years_remaining))

        war_result = calculate_player_war(player_data)
        total_war = _safe_float(war_result.get("total_war", 0.0))

        # Estimate market value from WAR
        estimated_value = total_war * WAR_TO_DOLLARS_MULTIPLIER

        # Age depreciation: players past 30 lose projected value per year
        age = _safe_float(player_data.get("age", 27), 27)
        if age > 30 and years_remaining > 1:
            depreciation = 0.05 * (age - 30) * (years_remaining - 1)
            estimated_value = estimated_value * max(1.0 - depreciation, 0.3)

        surplus_value = estimated_value - salary

        # Contract grade based on surplus value relative to salary
        if salary <= 0:
            contract_grade = "Steal" if total_war > 0.5 else "Fair"
        else:
            ratio = surplus_value / max(salary, 0.01)
            if ratio > 0.5:
                contract_grade = "Steal"
            elif ratio > 0.1:
                contract_grade = "Good Value"
            elif ratio > -0.2:
                contract_grade = "Fair"
            elif ratio > -0.5:
                contract_grade = "Overpaid"
            else:
                contract_grade = "Albatross"

        return {
            "war": round(total_war, 2),
            "estimated_value_millions": round(estimated_value, 2),
            "salary_millions": round(salary, 2),
            "surplus_value": round(surplus_value, 2),
            "contract_grade": contract_grade,
        }

    except Exception:
        _logger.exception(
            "[TradeEval] Error evaluating contract for %s",
            player_data.get("name", "unknown"),
        )
        return {
            "war": 0.0,
            "estimated_value_millions": 0.0,
            "salary_millions": round(_safe_float(salary), 2),
            "surplus_value": 0.0,
            "contract_grade": "Fair",
        }


# ------------------------------------------------------------------
# 3. Trade Evaluation
# ------------------------------------------------------------------

def _aggregate_side(players: List[dict]) -> dict:
    """Aggregate WAR, salary, and age for one side of a trade."""
    total_war = 0.0
    total_salary = 0.0
    total_age = 0.0
    count = 0

    for p in players:
        war_result = calculate_player_war(p)
        total_war += _safe_float(war_result.get("total_war", 0.0))
        total_salary += _safe_float(p.get("salary", 0.0))
        total_age += _safe_float(p.get("age", 27), 27)
        count += 1

    avg_age = total_age / max(count, 1)
    return {
        "total_war": total_war,
        "total_salary": total_salary,
        "avg_age": avg_age,
        "player_count": count,
    }


def _calculate_fit_improvement(
    incoming: List[dict], team_needs: Optional[List[str]]
) -> float:
    """Score how well incoming players address stated team needs (0-100)."""
    if not team_needs:
        return 50.0

    needs_met = 0
    for need in team_needs:
        need_lower = need.lower()
        for p in incoming:
            archetype = classify_player_archetype(p).lower()
            position = str(p.get("position", "")).lower()
            if need_lower in archetype or need_lower in position:
                needs_met += 1
                break
            # Check stat-based skill tags
            for skill, stat_keys in _SKILL_TAGS.items():
                if need_lower in skill:
                    for key in stat_keys:
                        if _safe_float(p.get(key, 0)) > 0:
                            needs_met += 0.5
                            break
                    break

    return min(100.0, (needs_met / max(len(team_needs), 1)) * 100.0)


def evaluate_trade(
    outgoing_players: List[dict],
    incoming_players: List[dict],
    team_needs: Optional[List[str]] = None,
) -> dict:
    """Evaluate a trade by comparing combined WAR, salary, age, and fit.

    Compares the aggregated value of outgoing and incoming players,
    factors in team-need fit improvement, and assigns an overall
    letter grade along with a winner determination.

    Args:
        outgoing_players: List of player-stat dicts being sent away.
        incoming_players: List of player-stat dicts being received.
        team_needs: Optional list of positional or skill needs
            (e.g. ``["shooting", "rim_protection", "PG"]``).

    Returns:
        Dictionary with ``net_war_change``, ``net_salary_change``,
        ``fit_improvement``, ``grade``, ``winner``, ``joseph_take``,
        and ``breakdown``.
    """
    try:
        outgoing = _aggregate_side(outgoing_players)
        incoming = _aggregate_side(incoming_players)

        net_war = incoming["total_war"] - outgoing["total_war"]
        net_salary = incoming["total_salary"] - outgoing["total_salary"]
        fit_improvement = _calculate_fit_improvement(incoming_players, team_needs)

        # Age advantage: younger is better (negative = getting younger)
        age_diff = incoming["avg_age"] - outgoing["avg_age"]
        age_bonus = max(-10.0, min(10.0, -age_diff * 2.0))

        # Composite score: WAR is king, salary savings help, fit matters
        war_score = normalize(net_war, -5, 5, 0, 100)
        salary_score = normalize(-net_salary, -20, 20, 0, 100)
        composite = (
            0.45 * war_score
            + 0.20 * salary_score
            + 0.20 * fit_improvement
            + 0.15 * normalize(age_bonus, -10, 10, 0, 100)
        )

        grade = letter_grade(composite)

        # Winner determination
        if net_war > 1.0:
            winner = "Receiving Team"
        elif net_war < -1.0:
            winner = "Sending Team"
        else:
            winner = "Even"

        # Joseph's narrative take
        if composite >= 80:
            joseph_take = (
                "This is a slam dunk. The receiving team upgrades significantly "
                "while the cost is manageable."
            )
        elif composite >= 60:
            joseph_take = (
                "Solid trade. There's clear value here, though it's not a home run. "
                "Both sides can justify this."
            )
        elif composite >= 45:
            joseph_take = (
                "This is a coin flip. Neither side gains a clear edge — "
                "execution and development will determine the winner."
            )
        elif composite >= 30:
            joseph_take = (
                "Questionable move. The receiving team is giving up too much "
                "relative to what they're getting back."
            )
        else:
            joseph_take = (
                "This is a heist. One side is clearly being fleeced — "
                "the value gap is too wide to ignore."
            )

        breakdown = {
            "outgoing_war": round(outgoing["total_war"], 2),
            "incoming_war": round(incoming["total_war"], 2),
            "outgoing_salary": round(outgoing["total_salary"], 2),
            "incoming_salary": round(incoming["total_salary"], 2),
            "outgoing_avg_age": round(outgoing["avg_age"], 1),
            "incoming_avg_age": round(incoming["avg_age"], 1),
            "war_score": round(war_score, 1),
            "salary_score": round(salary_score, 1),
            "age_bonus": round(age_bonus, 1),
            "composite_score": round(composite, 1),
        }

        return {
            "net_war_change": round(net_war, 2),
            "net_salary_change": round(net_salary, 2),
            "fit_improvement": round(fit_improvement, 1),
            "grade": grade,
            "winner": winner,
            "joseph_take": joseph_take,
            "breakdown": breakdown,
        }

    except Exception:
        _logger.exception("[TradeEval] Error evaluating trade")
        return {
            "net_war_change": 0.0,
            "net_salary_change": 0.0,
            "fit_improvement": 0.0,
            "grade": "F",
            "winner": "Even",
            "joseph_take": "Unable to evaluate trade due to an error.",
            "breakdown": {},
        }


# ------------------------------------------------------------------
# 4. Roster Fit Scoring
# ------------------------------------------------------------------

def score_roster_fit(player_data: dict, team_roster: List[dict]) -> dict:
    """Score how well a player fits with an existing roster.

    Checks positional need, skill-gap coverage, archetype
    complementarity, and age alignment to produce a 0-100 fit score.

    Args:
        player_data: Candidate player statistics dictionary.
        team_roster: List of player-stat dicts for current roster.

    Returns:
        Dictionary with ``fit_score``, ``fills_needs``,
        ``redundancies``, ``archetype_fit``, and ``age_fit``.
    """
    try:
        # Position distribution of current roster
        position_counts: dict = {"guard": 0, "wing": 0, "big": 0}
        roster_archetypes: list = []
        roster_ages: list = []
        roster_skills: dict = {skill: 0.0 for skill in _SKILL_TAGS}

        for rp in team_roster:
            pos = str(rp.get("position", "")).upper().strip()
            group = _POSITION_GROUPS.get(pos, "wing")
            position_counts[group] += 1
            roster_archetypes.append(classify_player_archetype(rp))
            roster_ages.append(_safe_float(rp.get("age", 27), 27))

            for skill, stat_keys in _SKILL_TAGS.items():
                for key in stat_keys:
                    roster_skills[skill] += _safe_float(rp.get(key, 0))

        roster_size = max(len(team_roster), 1)

        # Candidate info
        candidate_pos = str(player_data.get("position", "")).upper().strip()
        candidate_group = _POSITION_GROUPS.get(candidate_pos, "wing")
        candidate_archetype = classify_player_archetype(player_data)
        candidate_age = _safe_float(player_data.get("age", 27), 27)

        # --- Positional need score (0-30) ---
        ideal_distribution = {"guard": 4, "wing": 5, "big": 4}
        current = position_counts.get(candidate_group, 0)
        ideal = ideal_distribution.get(candidate_group, 4)
        if current < ideal:
            positional_score = 30.0
        elif current == ideal:
            positional_score = 15.0
        else:
            positional_score = max(0.0, 15.0 - (current - ideal) * 5.0)

        # --- Skill-gap score (0-30) ---
        fills_needs: list = []
        avg_skills = {s: v / roster_size for s, v in roster_skills.items()}
        # Identify weakest skills
        sorted_skills = sorted(avg_skills.items(), key=lambda x: x[1])
        weakest_skills = [s for s, _ in sorted_skills[:3]]

        skill_gap_score = 0.0
        for skill in weakest_skills:
            stat_keys = _SKILL_TAGS.get(skill, [])
            for key in stat_keys:
                val = _safe_float(player_data.get(key, 0))
                if val > 0:
                    skill_gap_score += 10.0
                    fills_needs.append(skill)
                    break
        skill_gap_score = min(30.0, skill_gap_score)

        # --- Archetype complementarity (0-20) ---
        redundancies: list = []
        archetype_count = roster_archetypes.count(candidate_archetype)
        if archetype_count == 0:
            archetype_score = 20.0
            archetype_fit = "Unique — fills a new role"
        elif archetype_count <= 2:
            archetype_score = 12.0
            archetype_fit = "Complementary — moderate overlap"
        else:
            archetype_score = 5.0
            archetype_fit = "Redundant — roster already has this archetype"
            redundancies.append(candidate_archetype)

        # --- Age alignment (0-20) ---
        if roster_ages:
            avg_roster_age = sum(roster_ages) / len(roster_ages)
        else:
            avg_roster_age = 27.0

        age_diff = abs(candidate_age - avg_roster_age)
        if age_diff <= 2:
            age_score = 20.0
            age_fit = "Core alignment — matches team timeline"
        elif age_diff <= 5:
            age_score = 12.0
            age_fit = "Moderate gap — still workable"
        else:
            age_score = 5.0
            age_fit = "Timeline mismatch — significant age gap"

        fit_score = positional_score + skill_gap_score + archetype_score + age_score
        fit_score = max(0.0, min(100.0, fit_score))

        return {
            "fit_score": round(fit_score, 1),
            "fills_needs": fills_needs,
            "redundancies": redundancies,
            "archetype_fit": archetype_fit,
            "age_fit": age_fit,
        }

    except Exception:
        _logger.exception(
            "[TradeEval] Error scoring roster fit for %s",
            player_data.get("name", "unknown"),
        )
        return {
            "fit_score": 0.0,
            "fills_needs": [],
            "redundancies": [],
            "archetype_fit": "Unknown",
            "age_fit": "Unknown",
        }


# ------------------------------------------------------------------
# 5. Cap Sheet Projection
# ------------------------------------------------------------------

def project_cap_sheet(
    roster: List[dict], cap_limit: float = 140.0
) -> dict:
    """Project a team's salary against the salary cap and luxury tax.

    Sums all player salaries, computes cap space (or overage), and
    calculates the luxury-tax bill using the NBA's incremental
    tax-rate schedule.

    Args:
        roster: List of player dicts, each with a ``salary`` key
            (in millions).
        cap_limit: Salary cap in millions (default ``140.0``).

    Returns:
        Dictionary with ``total_salary``, ``cap_space``,
        ``luxury_tax_bill``, ``is_over_cap``, ``is_in_tax``,
        and ``tax_level``.
    """
    try:
        cap_limit = _safe_float(cap_limit, SALARY_CAP_2025)

        total_salary = 0.0
        for p in roster:
            total_salary += _safe_float(p.get("salary", 0.0))

        cap_space = cap_limit - total_salary
        is_over_cap = total_salary > cap_limit
        is_in_tax = total_salary > LUXURY_TAX_THRESHOLD

        # Luxury tax bill calculation (NBA incremental brackets)
        luxury_tax_bill = 0.0
        if is_in_tax:
            overage = total_salary - LUXURY_TAX_THRESHOLD
            # Bracket structure: first $5M at $1.50, next $5M at $1.75,
            # next $5M at $2.50, next $5M at $3.25, each additional $5M +$0.50
            brackets = [
                (5.0, 1.50),
                (5.0, 1.75),
                (5.0, 2.50),
                (5.0, 3.25),
                (5.0, 3.75),
                (5.0, 4.25),
            ]
            remaining = overage
            for bracket_size, rate in brackets:
                if remaining <= 0:
                    break
                taxable = min(remaining, bracket_size)
                luxury_tax_bill += taxable * rate
                remaining -= taxable

            # Anything beyond the defined brackets at escalating rate
            if remaining > 0:
                extra_brackets = math.ceil(remaining / 5.0)
                base_rate = 4.75
                for i in range(int(extra_brackets)):
                    chunk = min(remaining, 5.0)
                    luxury_tax_bill += chunk * (base_rate + i * 0.50)
                    remaining -= chunk
                    if remaining <= 0:
                        break

        # Tax level classification
        if total_salary > SECOND_APRON:
            tax_level = "Second Apron"
        elif total_salary > FIRST_APRON:
            tax_level = "First Apron"
        elif is_in_tax:
            tax_level = "Tax Payer"
        elif is_over_cap:
            tax_level = "Over Cap"
        else:
            tax_level = "Under Cap"

        return {
            "total_salary": round(total_salary, 2),
            "cap_space": round(cap_space, 2),
            "luxury_tax_bill": round(luxury_tax_bill, 2),
            "is_over_cap": is_over_cap,
            "is_in_tax": is_in_tax,
            "tax_level": tax_level,
        }

    except Exception:
        _logger.exception("[TradeEval] Error projecting cap sheet")
        return {
            "total_salary": 0.0,
            "cap_space": round(_safe_float(cap_limit, SALARY_CAP_2025), 2),
            "luxury_tax_bill": 0.0,
            "is_over_cap": False,
            "is_in_tax": False,
            "tax_level": "Under Cap",
        }


# ------------------------------------------------------------------
# 6. Trade Package Builder
# ------------------------------------------------------------------

def _salary_match_valid(
    outgoing_salary: float, target_salary: float
) -> bool:
    """Check if outgoing salary satisfies the 125% + $250K trade rule."""
    if target_salary <= 0:
        return True
    threshold = target_salary * 1.25 + 0.25
    return outgoing_salary >= target_salary * 0.75 and outgoing_salary <= threshold


def build_trade_package(
    target_player: dict,
    team_roster: List[dict],
    salary_matching: bool = True,
) -> dict:
    """Suggest trade packages to acquire a target player.

    Iterates through roster combinations to find packages that
    match salary within the 125% rule (for over-cap teams) while
    minimizing the WAR lost.  Returns up to three ranked packages.

    Args:
        target_player: Player-stat dict for the acquisition target
            (must include ``salary`` key).
        team_roster: List of player-stat dicts for the team's
            current roster (each with ``salary``).
        salary_matching: If ``True``, enforce the 125% salary-
            matching rule.

    Returns:
        Dictionary with ``packages`` (list of package dicts),
        ``best_package`` (the top-ranked option), and
        ``joseph_take``.
    """
    try:
        target_salary = _safe_float(target_player.get("salary", 0.0))
        target_war = calculate_player_war(target_player)
        target_war_val = _safe_float(target_war.get("total_war", 0.0))
        target_name = str(target_player.get("name", "Target"))

        # Build roster info with WAR pre-computed
        roster_info: list = []
        for p in team_roster:
            p_name = str(p.get("name", "Unknown"))
            p_salary = _safe_float(p.get("salary", 0.0))
            p_war = calculate_player_war(p)
            p_war_val = _safe_float(p_war.get("total_war", 0.0))
            roster_info.append({
                "name": p_name,
                "salary": p_salary,
                "war": p_war_val,
                "data": p,
            })

        # Sort by WAR ascending — prefer trading lower-WAR players first
        roster_info.sort(key=lambda x: x["war"])

        packages: list = []
        seen_combos: set = set()

        # Try single-player packages first, then two-player combos
        for size in range(1, min(len(roster_info) + 1, 5)):
            if len(packages) >= 3:
                break
            _find_packages(
                roster_info, target_salary, target_war_val,
                salary_matching, size, 0, [], packages,
                seen_combos, max_packages=3,
            )

        # Sort packages by WAR lost (ascending = least WAR lost first)
        packages.sort(key=lambda pkg: pkg["war_lost"])

        best_package = packages[0] if packages else {
            "players": [],
            "total_salary": 0.0,
            "war_lost": 0.0,
            "war_net": 0.0,
            "salary_match": False,
        }

        # Joseph's take
        if not packages:
            joseph_take = (
                f"There's no realistic trade package on this roster to acquire "
                f"{target_name}. The salary math just doesn't work."
            )
        elif best_package["war_net"] > 2.0:
            joseph_take = (
                f"This is a clear upgrade. Acquiring {target_name} at a cost of "
                f"{best_package['war_lost']:.1f} WAR is a no-brainer."
            )
        elif best_package["war_net"] > 0.0:
            joseph_take = (
                f"Marginal upgrade for {target_name}. The cost is real, but the "
                f"return is worth the gamble if the fit is right."
            )
        else:
            joseph_take = (
                f"Acquiring {target_name} comes at a steep price — "
                f"you're giving up {best_package['war_lost']:.1f} WAR. "
                f"Make sure the intangibles justify this."
            )

        return {
            "packages": packages[:3],
            "best_package": best_package,
            "joseph_take": joseph_take,
        }

    except Exception:
        _logger.exception("[TradeEval] Error building trade package")
        return {
            "packages": [],
            "best_package": {
                "players": [],
                "total_salary": 0.0,
                "war_lost": 0.0,
                "war_net": 0.0,
                "salary_match": False,
            },
            "joseph_take": "Unable to build trade packages due to an error.",
        }


def _find_packages(
    roster_info: list,
    target_salary: float,
    target_war: float,
    salary_matching: bool,
    size: int,
    start_idx: int,
    current: list,
    results: list,
    seen: set,
    max_packages: int = 3,
) -> None:
    """Recursively find valid trade package combinations."""
    if len(results) >= max_packages:
        return

    if len(current) == size:
        combo_key = tuple(sorted(p["name"] for p in current))
        if combo_key in seen:
            return
        seen.add(combo_key)

        total_sal = sum(p["salary"] for p in current)
        total_war = sum(p["war"] for p in current)

        if salary_matching and not _salary_match_valid(total_sal, target_salary):
            return

        results.append({
            "players": [p["name"] for p in current],
            "total_salary": round(total_sal, 2),
            "war_lost": round(total_war, 2),
            "war_net": round(target_war - total_war, 2),
            "salary_match": _salary_match_valid(total_sal, target_salary),
        })
        return

    for i in range(start_idx, len(roster_info)):
        if len(results) >= max_packages:
            return
        current.append(roster_info[i])
        _find_packages(
            roster_info, target_salary, target_war, salary_matching,
            size, i + 1, current, results, seen, max_packages,
        )
        current.pop()
