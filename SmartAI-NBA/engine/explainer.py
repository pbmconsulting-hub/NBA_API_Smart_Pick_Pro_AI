# ============================================================
# FILE: engine/explainer.py
# PURPOSE: Generate plain-English explanations for every prop
#          pick so users understand WHY the model recommends
#          (or avoids) a specific pick.
# CONNECTS TO: pages/3_🏆_Analysis.py (display), simulation.py,
#              projections.py, edge_detection.py, confidence.py
# ============================================================

# Standard library only
import math

from engine.math_helpers import calculate_platform_edge_percentage


# ============================================================
# SECTION: Narrative Templates
# Human-readable description builders for each analysis factor.
# ============================================================

def _describe_line_vs_avg(season_avg, prop_line, stat_label):
    """
    Return a sentence comparing the season average to the prop line.

    Args:
        season_avg (float): Player's season average for the stat
        prop_line (float): Tonight's prop line
        stat_label (str): Human-readable stat name (e.g., "points")

    Returns:
        tuple: (description str, indicator status str)
            indicator: 'favorable' | 'unfavorable' | 'neutral'
    """
    if season_avg <= 0:
        return (
            f"Season average unavailable — using {prop_line:.1f} as baseline.",
            "neutral",
        )

    diff = prop_line - season_avg
    diff_pct = diff / season_avg * 100

    if abs(diff_pct) < 3:
        status = "neutral"
        desc = (
            f"Season average is {season_avg:.1f} {stat_label} vs. line of {prop_line:.1f} "
            f"({diff_pct:+.1f}%). The books have set a very tight line — essentially at "
            f"the season average."
        )
    elif diff_pct < 0:
        status = "favorable"
        desc = (
            f"Season average is {season_avg:.1f} {stat_label}, and the line is {prop_line:.1f} "
            f"({diff_pct:+.1f}% below the average). The OVER is accessible — "
            f"player only needs to hit his normal production."
        )
    else:
        status = "unfavorable"
        desc = (
            f"Season average is {season_avg:.1f} {stat_label}, but the line is {prop_line:.1f} "
            f"({diff_pct:+.1f}% above the average). Player needs an above-average night "
            f"to hit the OVER."
        )
    return desc, status


def _describe_matchup(defense_factor, opponent, position):
    """
    Return a sentence describing the opponent defensive matchup.

    Args:
        defense_factor (float): Defensive multiplier (1.0 = neutral,
            >1.0 = weak D = helps OVER, <1.0 = tough D = helps UNDER)
        opponent (str): Opponent team abbreviation
        position (str): Player's position

    Returns:
        tuple: (description str, indicator status str)
    """
    if not opponent:
        opponent = "tonight's opponent"

    pct_impact = (defense_factor - 1.0) * 100

    if abs(pct_impact) < 2:
        status = "neutral"
        desc = (
            f"{opponent} has an average defense vs. {position or 'this position'} "
            f"(multiplier: {defense_factor:.2f}x). Matchup is neutral."
        )
    elif pct_impact > 0:
        status = "favorable"
        desc = (
            f"{opponent} allows {abs(pct_impact):.0f}% more {'' if not position else position + ' '}stats "
            f"than average (defensive multiplier: {defense_factor:.2f}x). "
            f"This is a favorable matchup that bumps the projection up."
        )
    else:
        status = "unfavorable"
        desc = (
            f"{opponent} is a tough defensive team, suppressing "
            f"{'' if not position else position + ' '}stats by ~{abs(pct_impact):.0f}% "
            f"(defensive multiplier: {defense_factor:.2f}x). "
            f"Expect a tougher-than-average night."
        )
    return desc, status


def _describe_pace(pace_factor, opponent):
    """
    Return a sentence about game pace impact.

    Args:
        pace_factor (float): Pace multiplier (>1 = faster pace)
        opponent (str): Opponent team abbreviation

    Returns:
        tuple: (description str, indicator status str)
    """
    pct = (pace_factor - 1.0) * 100
    if abs(pct) < 1.5:
        status = "neutral"
        desc = (
            f"This game is projected at a league-average pace (factor: {pace_factor:.2f}x). "
            f"No significant pace boost or drag expected."
        )
    elif pct > 0:
        status = "favorable"
        desc = (
            f"Tonight's matchup projects to a faster-than-average pace "
            f"({pct:+.1f}%, factor: {pace_factor:.2f}x). "
            f"More possessions means more opportunities to accumulate stats."
        )
    else:
        status = "unfavorable"
        desc = (
            f"This game projects as slower than average "
            f"({pct:+.1f}%, factor: {pace_factor:.2f}x). "
            f"Fewer possessions could limit counting stat totals."
        )
    return desc, status


def _describe_home_away(home_away_factor, is_home):
    """
    Return a sentence about home/away impact.

    Args:
        home_away_factor (float): Additive home-court factor
        is_home (bool): True if playing at home

    Returns:
        tuple: (description str, indicator status str)
    """
    location = "home" if is_home else "away"
    pct = home_away_factor * 100
    if abs(pct) < 1:
        status = "neutral"
        desc = f"Home/away factor is minimal (playing {location}, factor: {pct:+.1f}%)."
    elif is_home:
        status = "favorable"
        desc = (
            f"Playing at home tonight ({pct:+.1f}% boost). Home-court advantage "
            f"typically gives a small but consistent edge in counting stats."
        )
    else:
        status = "unfavorable"
        desc = (
            f"Playing away tonight ({pct:+.1f}% adjustment). Road games "
            f"tend to see slightly lower stat totals, all else equal."
        )
    return desc, status


def _describe_rest(rest_factor, rest_days):
    """
    Return a sentence about rest days impact.

    Args:
        rest_factor (float): Rest adjustment multiplier
        rest_days (int): Number of rest days before this game

    Returns:
        tuple: (description str, indicator status str)
    """
    pct = (rest_factor - 1.0) * 100
    if rest_days == 0:
        status = "unfavorable"
        desc = (
            f"Back-to-back game tonight (0 rest days). Fatigue typically "
            f"reduces efficiency and counting stats (factor: {rest_factor:.2f}x)."
        )
    elif rest_days == 1:
        status = "neutral"
        desc = (
            f"One day of rest — this is normal scheduling. "
            f"Minimal fatigue impact expected (factor: {rest_factor:.2f}x)."
        )
    elif rest_days >= 4:
        status = "favorable"
        desc = (
            f"Well-rested with {rest_days} days off. Extended rest often leads "
            f"to better focus and performance (factor: {rest_factor:.2f}x)."
        )
    else:
        status = "neutral"
        desc = (
            f"{rest_days} days of rest — standard between games. "
            f"No significant rest advantage or fatigue (factor: {rest_factor:.2f}x)."
        )
    return desc, status


def _describe_vegas(game_total, vegas_spread, prop_line, stat_type):
    """
    Return a sentence about what Vegas lines imply.

    Args:
        game_total (float): Vegas over/under for the game
        vegas_spread (float): Spread (positive = favored)
        prop_line (float): Tonight's prop line
        stat_type (str): Internal stat key

    Returns:
        tuple: (description str, indicator status str)
    """
    league_avg_total = 220.0
    total_diff = game_total - league_avg_total

    if abs(total_diff) < 3:
        total_desc = f"The game total is {game_total:.1f} (near league average of {league_avg_total:.0f})."
        status = "neutral"
    elif total_diff > 0:
        total_desc = (
            f"The game total is {game_total:.1f} — {total_diff:.0f} points above average. "
            f"Vegas expects a high-scoring game, which broadly benefits scoring props."
        )
        status = "favorable" if stat_type in ("points", "fantasy_score_pp", "fantasy_score_dk") else "neutral"
    else:
        total_desc = (
            f"The game total is {game_total:.1f} — {abs(total_diff):.0f} points below average. "
            f"Vegas expects a slower-paced, defensive game."
        )
        status = "unfavorable" if stat_type in ("points", "fantasy_score_pp", "fantasy_score_dk") else "neutral"

    spread_desc = ""
    if abs(vegas_spread) >= 5:
        direction = "favorites" if vegas_spread > 0 else "underdogs"
        spread_desc = (
            f" Player's team is {abs(vegas_spread):.1f}-point {direction}, "
            f"{'raising blowout risk if lead grows large' if vegas_spread > 5 else 'likely to be competitive throughout'}."
        )

    return total_desc + spread_desc, status


def _describe_simulation(sim_mean, sim_std, prob_over, prop_line, n_sims, direction):
    """
    Return a simulation narrative sentence.

    Args:
        sim_mean (float): Simulated mean
        sim_std (float): Simulated standard deviation
        prob_over (float): P(over) from simulation
        prop_line (float): The prop line
        n_sims (int): Number of simulations run
        direction (str): 'OVER' or 'UNDER'

    Returns:
        str: Narrative sentence
    """
    pct = prob_over * 100 if direction == "OVER" else (1 - prob_over) * 100
    over_count = int(round(prob_over * n_sims))
    return (
        f"In {n_sims:,} simulated games, the player went OVER {prop_line:.1f} in "
        f"{over_count:,} of them ({pct:.1f}%). "
        f"The simulated average was {sim_mean:.1f} ± {sim_std:.1f}."
    )


def _describe_forces(over_forces, under_forces):
    """
    Return a summary of directional forces agreement or disagreement.

    Args:
        over_forces (list of dict): OVER forces with 'name', 'strength', 'description'
        under_forces (list of dict): UNDER forces

    Returns:
        tuple: (description str, indicator status str)
    """
    n_over = len(over_forces)
    n_under = len(under_forces)

    if n_over > n_under + 1:
        status = "favorable"
        names = ", ".join(f["name"] for f in over_forces[:3])
        desc = (
            f"{n_over} forces favor the OVER vs. {n_under} for the UNDER. "
            f"OVER factors: {names}."
        )
    elif n_under > n_over + 1:
        status = "unfavorable"
        names = ", ".join(f["name"] for f in under_forces[:3])
        desc = (
            f"{n_under} forces favor the UNDER vs. {n_over} for the OVER. "
            f"UNDER factors: {names}."
        )
    elif n_over == 0 and n_under == 0:
        status = "neutral"
        desc = "No strong directional forces detected — model is relying primarily on base rates."
    else:
        status = "neutral"
        desc = (
            f"Forces are split: {n_over} OVER vs. {n_under} UNDER. "
            f"No strong directional consensus — proceed with caution."
        )
    return desc, status


def _describe_recent_form(recent_form_ratio, recent_form_games, stat_type, prop_line):
    """
    Return a sentence about recent performance trend.

    Args:
        recent_form_ratio (float or None): Recent avg / season avg
        recent_form_games (list of dict): Last N game logs
        stat_type (str): Internal stat key
        prop_line (float): The prop line

    Returns:
        tuple: (description str, indicator status str)
    """
    if recent_form_ratio is None:
        return "Recent game log data not available.", "neutral"

    pct = (recent_form_ratio - 1.0) * 100
    if pct > 10:
        status = "favorable"
        desc = (
            f"Player is running HOT recently — recent form is {pct:+.1f}% above "
            f"season average (ratio: {recent_form_ratio:.2f}x). 🔥 This is a positive signal."
        )
    elif pct < -10:
        status = "unfavorable"
        desc = (
            f"Player is running COLD recently — recent form is {pct:+.1f}% below "
            f"season average (ratio: {recent_form_ratio:.2f}x). 🧊 Proceed with caution."
        )
    else:
        status = "neutral"
        desc = (
            f"Recent form is consistent with season average "
            f"(ratio: {recent_form_ratio:.2f}x, {pct:+.1f}%). No hot/cold streak detected."
        )
    return desc, status


def _build_verdict(direction, confidence_score, tier, prob_pct, edge):
    """
    Return a final verdict sentence.

    Args:
        direction (str): 'OVER' or 'UNDER'
        confidence_score (float): 0-100 confidence score
        tier (str): 'Platinum', 'Gold', 'Silver', 'Bronze'
        prob_pct (float): Display probability (0-100)
        edge (float): Edge percentage

    Returns:
        str: Verdict text
    """
    tier_descs = {
        "Platinum": "STRONG — this is a high-conviction pick",
        "Gold": "GOOD — solid edge, worth including",
        "Silver": "MODERATE — reasonable pick, but not high conviction",
        "Bronze": "WEAK — low confidence, approach with caution",
    }
    tier_desc = tier_descs.get(tier, "")
    sign = "+" if edge >= 0 else ""
    return (
        f"**{tier} ({tier_desc})** — {direction} at {prob_pct:.1f}% probability "
        f"with a {sign}{edge:.1f}% edge. Confidence: {confidence_score:.0f}/100."
    )


def _build_risk_factors(
    should_avoid, avoid_reasons, recent_form_ratio,
    stat_std, stat_avg, game_total, rest_days,
):
    """
    Build a list of risk factors that could hurt this pick.

    Args:
        should_avoid (bool): Whether the avoid-list flagged this prop
        avoid_reasons (list of str): Reasons for avoid flag
        recent_form_ratio (float or None): Recent form vs season avg
        stat_std (float): Stat standard deviation (variance indicator)
        stat_avg (float): Season average for the stat
        game_total (float): Vegas game total
        rest_days (int): Rest days before tonight's game

    Returns:
        list of str: Risk factor descriptions
    """
    risks = []

    if should_avoid:
        for reason in avoid_reasons:
            risks.append(f"⛔ Avoid list flag: {reason}")

    if rest_days == 0:
        risks.append("😴 Back-to-back game — player may be fatigued.")

    if recent_form_ratio is not None and recent_form_ratio < 0.9:
        risks.append("📉 Recent cold stretch — player has been underperforming season average.")

    if stat_avg > 0:
        cv = stat_std / stat_avg  # Coefficient of variation
        if cv > 0.5:
            risks.append(
                f"📊 High volatility stat (CV: {cv:.1f}) — this player's output varies significantly game-to-game."
            )

    if game_total < 210:
        risks.append(f"🛡️ Low game total ({game_total:.0f}) — Vegas expects a defensive grind.")

    if not risks:
        risks.append("✅ No major risk factors identified for this pick.")

    return risks


# ============================================================
# END SECTION: Narrative Templates
# ============================================================


# ============================================================
# SECTION: Main Explanation Generator
# ============================================================

def generate_pick_explanation(
    player_data,
    prop_line,
    stat_type,
    direction,
    projection_result,
    simulation_results,
    forces,
    confidence_result,
    game_context,
    platform,
    recent_form_games=None,
    should_avoid=False,
    avoid_reasons=None,
    trap_line_result=None,
    line_sharpness_info=None,
    teammate_out_notes=None,
):
    """
    Generate a complete plain-English explanation for a prop pick.

    Each section explains one analysis factor in accessible language,
    like a sports analyst writing up their pick.

    Args:
        player_data (dict): Player row from the database (name, team,
            position, points_avg, rebounds_avg, assists_avg, etc.)
        prop_line (float): Tonight's prop line
        stat_type (str): Internal stat key (e.g., 'points', 'rebounds')
        direction (str): 'OVER' or 'UNDER'
        projection_result (dict): Output from build_player_projection()
            (projected stats + adjustment factors)
        simulation_results (dict): Output from run_monte_carlo_simulation()
            (probability_over, simulated_mean, simulated_std, percentiles)
        forces (dict): Output from analyze_directional_forces()
            (over_forces, under_forces, net_direction)
        confidence_result (dict): Output from calculate_confidence_score()
            (confidence_score, tier, tier_emoji, direction, recommendation)
        game_context (dict): Tonight's game context
            (opponent, is_home, rest_days, game_total, vegas_spread)
        platform (str): Platform name (FanDuel, DraftKings, BetMGM, etc.)
        recent_form_games (list of dict, optional): Last N game logs
        should_avoid (bool): Whether the prop is on the avoid list
        avoid_reasons (list of str, optional): Reasons for avoid flag
        trap_line_result (dict, optional): Output from detect_trap_line() (W5).
            Includes 'is_trap', 'trap_type', 'warning_message' if applicable.
        line_sharpness_info (dict, optional): Line sharpness force details (W1).
            Includes 'name', 'description', 'strength' if sharpness detected.
        teammate_out_notes (list of str, optional): Notes about absent teammates
            and how they affect this player's projected usage (W8).

    Returns:
        dict: Explanation components:
            - 'tldr' (str): One-sentence summary
            - 'average_vs_line' (str): Season avg vs line analysis
            - 'matchup_explanation' (str): Defense matchup analysis
            - 'pace_explanation' (str): Game pace impact
            - 'home_away_explanation' (str): Home/away impact
            - 'rest_explanation' (str): Rest days impact
            - 'vegas_explanation' (str): Vegas lines interpretation
            - 'projection_explanation' (str): Final adjusted projection
            - 'simulation_narrative' (str): Simulation results narrative
            - 'forces_summary' (str): Forces agreement/disagreement
            - 'recent_form_explanation' (str): Recent form trend
            - 'line_sharpness_explanation' (str): W1 line sharpness warning
            - 'trap_line_explanation' (str): W5 trap line warning
            - 'teammate_impact_explanation' (str): W8 teammate impact note
            - 'verdict' (str): Final recommendation with reasoning
            - 'risk_factors' (list of str): Things that could go wrong
            - 'indicators' (list of dict): Per-factor status badges
    """
    if avoid_reasons is None:
        avoid_reasons = []

    # ── Extract player info ──────────────────────────────────
    player_name = player_data.get("name", "Player")
    position = player_data.get("position", "")
    stat_label = stat_type.replace("_", " ")

    # Season average for this specific stat
    stat_avg_map = {
        "points": float(player_data.get("points_avg", prop_line) or prop_line),
        "rebounds": float(player_data.get("rebounds_avg", prop_line) or prop_line),
        "assists": float(player_data.get("assists_avg", prop_line) or prop_line),
        "threes": float(player_data.get("threes_avg", prop_line) or prop_line),
        "steals": float(player_data.get("steals_avg", prop_line) or prop_line),
        "blocks": float(player_data.get("blocks_avg", prop_line) or prop_line),
        "turnovers": float(player_data.get("turnovers_avg", prop_line) or prop_line),
    }
    season_avg = stat_avg_map.get(stat_type, prop_line)

    # ── Extract game context ─────────────────────────────────
    opponent = game_context.get("opponent", "")
    is_home = game_context.get("is_home", True)
    rest_days = game_context.get("rest_days", 2)
    game_total = game_context.get("game_total", 220.0)
    vegas_spread = game_context.get("vegas_spread", 0.0)

    # ── Extract projection factors ───────────────────────────
    defense_factor = projection_result.get("defense_factor", 1.0)
    pace_factor = projection_result.get("pace_factor", 1.0)
    home_away_factor = projection_result.get("home_away_factor", 0.0)
    rest_factor = projection_result.get("rest_factor", 1.0)
    adjusted_proj = projection_result.get(f"projected_{stat_type}", prop_line)

    # ── Extract simulation results ───────────────────────────
    prob_over = simulation_results.get("probability_over", 0.5)
    sim_mean = simulation_results.get("simulated_mean", adjusted_proj)
    sim_std = simulation_results.get("simulated_std", 1.0)

    # ── Extract confidence result ────────────────────────────
    confidence_score = confidence_result.get("confidence_score", 50)
    tier = confidence_result.get("tier", "Bronze")
    prob_pct = prob_over * 100 if direction == "OVER" else (1 - prob_over) * 100

    # ── Extract forces ───────────────────────────────────────
    over_forces = forces.get("over_forces", [])
    under_forces = forces.get("under_forces", [])

    # ── Extract recent form ──────────────────────────────────
    recent_form_ratio = projection_result.get("recent_form_ratio")

    # ── Calculate edge ───────────────────────────────────────
    edge = calculate_platform_edge_percentage(prob_over, platform)

    # ── Calculate stat std dev ───────────────────────────────
    stat_std = float(player_data.get(f"{stat_type}_std", season_avg * 0.35) or season_avg * 0.35)

    # ============================================================
    # Build each explanation section
    # ============================================================

    indicators = []  # Will hold {factor, status, emoji} dicts

    # 1. Season average vs line
    avg_desc, avg_status = _describe_line_vs_avg(season_avg, prop_line, stat_label)
    indicators.append({
        "factor": "Season Avg vs Line",
        "status": avg_status,
        "emoji": "🟢" if avg_status == "favorable" else ("🔴" if avg_status == "unfavorable" else "⚪"),
    })

    # 2. Matchup
    matchup_desc, matchup_status = _describe_matchup(defense_factor, opponent, position)
    indicators.append({
        "factor": "Matchup",
        "status": matchup_status,
        "emoji": "🟢" if matchup_status == "favorable" else ("🔴" if matchup_status == "unfavorable" else "⚪"),
    })

    # 3. Game pace
    pace_desc, pace_status = _describe_pace(pace_factor, opponent)
    indicators.append({
        "factor": "Game Pace",
        "status": pace_status,
        "emoji": "🟢" if pace_status == "favorable" else ("🔴" if pace_status == "unfavorable" else "⚪"),
    })

    # 4. Home/Away
    home_away_desc, ha_status = _describe_home_away(home_away_factor, is_home)
    indicators.append({
        "factor": "Home/Away",
        "status": ha_status,
        "emoji": "🟢" if ha_status == "favorable" else ("🔴" if ha_status == "unfavorable" else "⚪"),
    })

    # 5. Rest
    rest_desc, rest_status = _describe_rest(rest_factor, rest_days)
    indicators.append({
        "factor": "Rest Days",
        "status": rest_status,
        "emoji": "🟢" if rest_status == "favorable" else ("🔴" if rest_status == "unfavorable" else "⚪"),
    })

    # 6. Vegas lines
    vegas_desc, vegas_status = _describe_vegas(game_total, vegas_spread, prop_line, stat_type)
    indicators.append({
        "factor": "Vegas Total",
        "status": vegas_status,
        "emoji": "🟢" if vegas_status == "favorable" else ("🔴" if vegas_status == "unfavorable" else "⚪"),
    })

    # 7. Projection explanation
    overall_adj = projection_result.get("overall_adjustment", 1.0)
    adj_pct = (overall_adj - 1.0) * 100
    projection_desc = (
        f"After applying all adjustments (matchup, pace, home/away, rest), "
        f"the model projects {player_name} at {adjusted_proj:.1f} {stat_label} tonight "
        f"({adj_pct:+.1f}% from season average of {season_avg:.1f})."
    )

    # 8. Simulation narrative
    n_sims = len(simulation_results.get("simulated_results", [])) or 1000
    sim_narrative = _describe_simulation(sim_mean, sim_std, prob_over, prop_line, n_sims, direction)

    # 9. Forces summary
    forces_desc, forces_status = _describe_forces(over_forces, under_forces)
    indicators.append({
        "factor": "Directional Forces",
        "status": forces_status,
        "emoji": "🟢" if forces_status == "favorable" else ("🔴" if forces_status == "unfavorable" else "⚪"),
    })

    # 10. Recent form
    form_desc, form_status = _describe_recent_form(
        recent_form_ratio, recent_form_games, stat_type, prop_line
    )
    indicators.append({
        "factor": "Recent Form",
        "status": form_status,
        "emoji": "🟢" if form_status == "favorable" else ("🔴" if form_status == "unfavorable" else "⚪"),
    })

    # 11. Risk factors (including new W1/W5/W8 factors)
    risk_factors = _build_risk_factors(
        should_avoid, avoid_reasons,
        recent_form_ratio, stat_std, season_avg,
        game_total, rest_days,
    )

    # ---- W1: Line Sharpness Explanation ----
    line_sharpness_text = ""
    if line_sharpness_info:
        line_sharpness_text = (
            f"📐 Line Sharpness: {line_sharpness_info.get('description', '')} "
            f"Sharp lines have minimal edge — confidence reduced by "
            f"{line_sharpness_info.get('strength', 0)*2:.0f} points."
        )
        risk_factors.append(f"📐 Sharp line detected — confidence penalized")

    # ---- W5: Trap Line Explanation ----
    trap_line_text = ""
    if trap_line_result and trap_line_result.get("is_trap"):
        trap_line_text = trap_line_result.get("warning_message", "")
        penalty = trap_line_result.get("confidence_penalty", 0)
        if penalty > 0:
            risk_factors.append(
                f"⚠️ Trap line penalty: −{penalty:.0f} confidence points applied"
            )
        neg = trap_line_result.get("negative_factors", [])
        pos = trap_line_result.get("positive_factors", [])
        if neg:
            risk_factors.append(f"⬇️ Negative factors present: {'; '.join(neg)}")
        if pos and trap_line_result.get("trap_type") == "over_trap":
            risk_factors.append(f"⬆️ Positive factors: {'; '.join(pos)}")

    # ---- W8: Teammate Impact Explanation ----
    teammate_text = ""
    if teammate_out_notes:
        teammate_text = "👥 Teammate impact: " + " | ".join(teammate_out_notes)
        mins_adj = projection_result.get("minutes_adjustment_factor", 1.0)
        if mins_adj > 1.01:
            teammate_text += f" → Projection boosted +{(mins_adj-1)*100:.0f}% for extra usage"
        elif mins_adj < 0.99:
            teammate_text += f" → Projection reduced {(1-mins_adj)*100:.0f}% for load concerns"

    # 12. Verdict
    verdict = _build_verdict(direction, confidence_score, tier, prob_pct, edge)

    # 13. TL;DR (one-sentence summary)
    opp_str = f" against {opponent}" if opponent else ""
    tldr = (
        f"{player_name} averages {season_avg:.1f} {stat_label} and we project "
        f"{adjusted_proj:.1f} tonight{opp_str} — "
        f"take the {direction} at {prop_line:.1f} with {prob_pct:.1f}% probability "
        f"({confidence_score:.0f}/100 confidence)."
    )

    # 14. Platform notes
    platform_notes_map = {
        "PrizePicks": "PrizePicks uses a 2-6 pick entry format. Flex Play and Power Play options available.",
        "Underdog Fantasy": "Underdog Fantasy uses a 2-6 pick entry. Higher entry counts yield higher multipliers.",
        "DraftKings Pick6": "DraftKings Pick6 uses a 6-pick entry format with tiered payout structure.",
        # Backward-compat aliases
        "Underdog": "Underdog Fantasy uses a 2-6 pick entry. Higher entry counts yield higher multipliers.",
        "DraftKings": "DraftKings Pick6 uses a 6-pick entry format with tiered payout structure.",
    }
    platform_notes = platform_notes_map.get(platform, f"{platform} — check platform rules for payout structure.")

    return {
        "tldr": tldr,
        "average_vs_line": avg_desc,
        "matchup_explanation": matchup_desc,
        "pace_explanation": pace_desc,
        "home_away_explanation": home_away_desc,
        "rest_explanation": rest_desc,
        "vegas_explanation": vegas_desc,
        "projection_explanation": projection_desc,
        "simulation_narrative": sim_narrative,
        "forces_summary": forces_desc,
        "recent_form_explanation": form_desc,
        "line_sharpness_explanation": line_sharpness_text,   # W1
        "trap_line_explanation": trap_line_text,             # W5
        "teammate_impact_explanation": teammate_text,         # W8
        "verdict": verdict,
        "risk_factors": risk_factors,
        "platform_notes": platform_notes,
        "indicators": indicators,
    }

# ============================================================
# END SECTION: Main Explanation Generator
# ============================================================
