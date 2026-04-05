# ============================================================
# FILE: engine/game_script.py
# PURPOSE: Game Script Simulation Engine
#          Simulates within-game quarter-by-quarter dynamics to
#          model how score differentials affect player minutes
#          and stat accumulation. Captures effects that flat
#          multipliers miss: star players sitting in blowouts,
#          faster/slower pace depending on lead size, etc.
#
#          Result is blended with flat Quantum Matrix results (70%
#          flat, 30% game-script) since game-script adds signal
#          but is noisier than the proven flat simulation.
#
# CONNECTS TO: engine/simulation.py (blend_with_flat_simulation),
#              engine/minutes_model.py (minutes simulation)
# CONCEPTS COVERED: Game script modeling, score differential,
#                   quarter-by-quarter simulation, star player
#                   usage patterns, blowout substitution
# ============================================================

import math
import random


def _safe_float(value, fallback=0.0):
    """Return *value* as a finite float, or *fallback* if NaN/inf/non-numeric."""
    try:
        v = float(value)
        if math.isfinite(v):
            return v
        return float(fallback)
    except (ValueError, TypeError):
        return float(fallback)


# ============================================================
# SECTION: Module-Level Constants
# ============================================================

# Default number of simulations for game script engine
DEFAULT_GAME_SCRIPT_SIMULATIONS = 500   # Fewer than full Quantum Matrix Engine 5.6 (noisier)

# Blend weight for game-script vs flat simulation results
GAME_SCRIPT_BLEND_WEIGHT = 0.30   # 30% game-script + 70% flat Quantum Matrix

# Score differential thresholds for player management (positive = winning)
BLOWOUT_DIFFERENTIAL_MILD = 12     # 12+ point lead: minor reduction
BLOWOUT_DIFFERENTIAL_HEAVY = 20    # 20+ point lead: starters sit

# Quarter-specific baseline minute distributions (out of ~12 per quarter)
# Starters typically play more in Q1/Q2, slightly less in Q3, less in Q4 (blowout risk)
STARTER_QUARTER_BASELINE_MINUTES = {
    1: 10.5,   # Q1: 10.5 of 12 available
    2: 10.0,   # Q2: 10.0 (second stint variations)
    3: 10.0,   # Q3: 10.0 (fatigue creep)
    4:  9.5,   # Q4: 9.5 (late-game management / blowout risk)
}

# Minutes reduction in blowout Q4 based on score differential
BLOWOUT_Q4_REDUCTION_MILD  = 4.0   # Mild blowout: -4 minutes in Q4
BLOWOUT_Q4_REDUCTION_HEAVY = 9.5   # Heavy blowout: effectively DNP in Q4

# Standard deviation for quarter score differential simulation
QUARTER_SCORE_STD = 8.0   # Each quarter's net score is ~N(0, 8)

# ============================================================
# SECTION: Player Importance Tier Constants
# BEGINNER NOTE: Stars get pulled earlier in blowouts (coach protects them),
# while bench players actually GAIN minutes in blowout garbage time.
# This asymmetry is a key difference from treating all players identically.
# ============================================================

# Player importance tiers determine blowout minute impact
PLAYER_TIER_STAR     = "star"      # Top-usage, franchise player: loses most in blowouts
PLAYER_TIER_ROTATION = "rotation"  # Regular starter/key rotation: moderate impact
PLAYER_TIER_BENCH    = "bench"     # Backup/fringe: GAINS minutes in blowouts (garbage time)

# Star thresholds: projected stat determines tier
STAR_POINTS_THRESHOLD  = 20.0  # 20+ PPG → star
ROTATION_POINTS_THRESHOLD = 10.0  # 10-20 PPG → rotation

# Blowout minute adjustments by tier (applied in addition to base reduction)
# Stars lose MORE minutes in blowouts (coaches protect them)
STAR_BLOWOUT_EXTRA_REDUCTION_MILD  = 2.0  # Extra -2 mins for stars in mild blowout
STAR_BLOWOUT_EXTRA_REDUCTION_HEAVY = 3.5  # Extra -3.5 mins for stars in heavy blowout
# Bench players GAIN minutes in blowouts (garbage time)
BENCH_BLOWOUT_GAIN_MILD  = 2.5   # +2.5 mins for bench in mild blowout
BENCH_BLOWOUT_GAIN_HEAVY = 5.0   # +5.0 mins for bench in heavy blowout

# Garbage time stat boost for bench players
BENCH_GARBAGE_TIME_STAT_BOOST = 0.15   # +15% stat boost for bench in blowouts

# Close-game extra effort for stars
CLOSE_GAME_SPREAD_THRESHOLD = 3.0    # Game within 3 points → "close game"
STAR_CLOSE_GAME_BOOST       = 0.03   # +3% stat boost for stars in close games

# ============================================================
# END SECTION: Player Importance Tier Constants
# ============================================================


def _determine_player_tier(projected_stat):
    """
    Determine a player's importance tier based on their projected stat.

    BEGINNER NOTE: NBA coaches treat stars, rotation players, and bench
    players very differently in blowout situations. Stars get pulled early
    to prevent injury; bench players flood the court for garbage time.

    Args:
        projected_stat (float): Player's projected stat value (usually points)

    Returns:
        str: One of PLAYER_TIER_STAR, PLAYER_TIER_ROTATION, PLAYER_TIER_BENCH
    """
    if projected_stat >= STAR_POINTS_THRESHOLD:
        return PLAYER_TIER_STAR
    elif projected_stat >= ROTATION_POINTS_THRESHOLD:
        return PLAYER_TIER_ROTATION
    else:
        return PLAYER_TIER_BENCH


# ============================================================
# END SECTION: Quarter Minutes Estimation
# ============================================================


# ============================================================
# SECTION: Quarter Minutes Estimation
# ============================================================

def estimate_quarter_minutes(avg_quarter_minutes, quarter, score_differential,
                             is_starter=True, player_tier=None):
    """
    Estimate minutes in a specific quarter given score differential and player tier.

    BEGINNER NOTE: Stars get pulled earlier in blowouts than rotation players.
    Bench players GAIN minutes in garbage time (large blowouts).

    Args:
        avg_quarter_minutes (float): Player's typical minutes per quarter.
        quarter (int): Quarter number (1-4).
        score_differential (float): Current score difference (positive = leading).
        is_starter (bool): Whether the player is a starter. Default True.
        player_tier (str or None): One of 'star', 'rotation', 'bench'.
            When None, defaults to 'rotation' behavior.

    Returns:
        float: Estimated minutes in this quarter (0.0 to 12.0).

    Example:
        # Star player in Q4 with big lead: sits early
        mins = estimate_quarter_minutes(8.5, quarter=4, score_differential=22,
                                        is_starter=True, player_tier='star')
        # → ~0.5 minutes (star sits most of Q4 in blowout)
        # Bench player in Q4 with big lead: plays MORE
        mins = estimate_quarter_minutes(3.0, quarter=4, score_differential=22,
                                        is_starter=False, player_tier='bench')
        # → ~7.0 minutes (bench player gets garbage time)
    """
    tier = player_tier or PLAYER_TIER_ROTATION

    # Start with the player's typical quarter minutes, capped at 12
    base = min(avg_quarter_minutes, 12.0)

    # Q4 blowout logic: apply tier-specific adjustments
    if quarter == 4 and abs(score_differential) > BLOWOUT_DIFFERENTIAL_HEAVY:
        if tier == PLAYER_TIER_STAR:
            if score_differential > 0:
                # Star winning big → sits most of Q4 to protect from injury
                base -= (BLOWOUT_Q4_REDUCTION_HEAVY + STAR_BLOWOUT_EXTRA_REDUCTION_HEAVY)
            else:
                # Star losing big → may stay in for comeback attempt (smaller cut)
                base -= BLOWOUT_Q4_REDUCTION_MILD
        elif tier == PLAYER_TIER_BENCH:
            # Bench player in blowout → garbage time bonus (both winning AND losing)
            base += BENCH_BLOWOUT_GAIN_HEAVY
        else:
            # Rotation player: standard blowout treatment
            if is_starter and score_differential > 0:
                base -= BLOWOUT_Q4_REDUCTION_HEAVY
            elif is_starter and score_differential < 0:
                base -= BLOWOUT_Q4_REDUCTION_MILD

    elif quarter == 4 and abs(score_differential) > BLOWOUT_DIFFERENTIAL_MILD:
        if tier == PLAYER_TIER_STAR:
            if score_differential > 0:
                # Star mild winning lead → some early rest
                base -= (BLOWOUT_Q4_REDUCTION_MILD + STAR_BLOWOUT_EXTRA_REDUCTION_MILD)
        elif tier == PLAYER_TIER_BENCH:
            # Bench player in mild blowout → some garbage time
            base += BENCH_BLOWOUT_GAIN_MILD
        else:
            if is_starter and score_differential > 0:
                base -= BLOWOUT_Q4_REDUCTION_MILD

    # Add natural game-to-game variation in quarter minutes
    base += random.gauss(0, 1.5)

    # Minutes can't exceed a full quarter or go negative
    return max(0.0, min(12.0, base))

# ============================================================
# END SECTION: Quarter Minutes Estimation
# ============================================================


# ============================================================
# SECTION: Game Script Simulation
# ============================================================

def simulate_game_script(player_projection, game_context, num_simulations=500,
                         clutch_context=None):
    """
    Simulate a player's stat distribution using quarter-by-quarter game script.

    For each simulation: simulates 4 quarters of score differential,
    determines player court time each quarter based on differential and
    player importance tier (star/rotation/bench), then derives stat
    accumulation from per-minute rate.

    New features:
    - Player importance tier (star/rotation/bench) affects blowout behavior
    - Garbage time stat inflation (+15%) for bench players in blowouts
    - Close-game extra effort (+3%) for stars in games projected within 3 pts
    - Asymmetric blowout treatment: both winning AND losing team loses starters

    Args:
        player_projection (dict): Output from projections.py with keys:
            'projected_stat' (float), 'projected_minutes' (float),
            'stat_std' (float, optional).
        game_context (dict): Game info with 'vegas_spread' (float),
            'game_total' (float), 'is_home' (bool),
            'player_tier' (str, optional): 'star'/'rotation'/'bench'.
        num_simulations (int): Number of game-script simulations. Default 500.

    Returns:
        dict: {
            'simulated_values': list of float,   # Raw simulation outputs
            'mean': float,
            'std': float,
            'p10': float,   # 10th percentile (floor)
            'p90': float,   # 90th percentile (ceiling)
            'blowout_game_rate': float,  # Fraction of simulations that became blowouts
            'player_tier': str,          # Tier used for this simulation
        }

    Example:
        results = simulate_game_script(
            player_projection={'projected_stat': 25.0, 'projected_minutes': 34.0},
            game_context={'vegas_spread': 8.0, 'game_total': 228.5, 'is_home': True,
                          'player_tier': 'star'},
        )
    """
    projected_stat = float(player_projection.get('projected_stat', 20.0))
    projected_minutes = float(player_projection.get('projected_minutes', 32.0))

    vegas_spread = float(game_context.get('vegas_spread', 0.0))
    is_home = bool(game_context.get('is_home', False))

    # Determine player importance tier from game_context or auto-detect from projection
    player_tier = game_context.get('player_tier', None)
    if player_tier not in (PLAYER_TIER_STAR, PLAYER_TIER_ROTATION, PLAYER_TIER_BENCH):
        player_tier = _determine_player_tier(projected_stat)

    # Close-game extra effort modifier for stars
    abs_spread = abs(vegas_spread)
    close_game_boost = 0.0
    if abs_spread <= CLOSE_GAME_SPREAD_THRESHOLD and player_tier == PLAYER_TIER_STAR:
        close_game_boost = STAR_CLOSE_GAME_BOOST  # +3% for stars in close games

    # Per-minute production rate (stat per minute on the court)
    stat_per_minute = projected_stat / max(1.0, projected_minutes)

    # Clutch stats adjustment: modify close-game behavior based on actual clutch performance
    # When clutch_context is available, use it to adjust how the player performs
    # in the last 5 minutes of close games (score within 5 points).
    clutch_q4_multiplier = 1.0
    if clutch_context:
        clutch_gp = clutch_context.get("gp", 0)
        clutch_pts = clutch_context.get("pts", 0)
        clutch_min_val = clutch_context.get("min", 0)
        clutch_plus_minus = clutch_context.get("plus_minus", 0)

        # Only apply if player has enough clutch games for a reliable signal
        if clutch_gp >= 5 and clutch_min_val > 0:
            # Clutch scoring rate: pts per minute in clutch situations
            clutch_rate = clutch_pts / max(1.0, clutch_min_val)
            # Compare to overall per-minute rate
            if stat_per_minute > 0:
                clutch_ratio = clutch_rate / stat_per_minute
                # Clutch ratio > 1.0 = player elevates in crunch time
                # Clutch ratio < 1.0 = player shrinks in crunch time
                # Scale: each 1.0 deviation in ratio → 20% Q4 adjustment, capped ±8%
                clutch_q4_multiplier = 1.0 + (clutch_ratio - 1.0) * 0.2
                clutch_q4_multiplier = max(0.92, min(1.08, clutch_q4_multiplier))

            # Plus/minus in clutch: positive = team wins close games with this player
            if clutch_plus_minus > 3.0:
                close_game_boost += 0.02  # Extra +2% for proven clutch closer
            elif clutch_plus_minus < -3.0:
                close_game_boost = max(0.0, close_game_boost - 0.01)  # Reduce close-game boost

    # Average minutes per quarter based on full game projection
    avg_quarter_minutes = projected_minutes / 4.0

    # Vegas spread shifts expected differential per quarter
    # Positive spread means player's team favored to win by that many points
    spread_per_quarter = vegas_spread / 4.0

    # Home court advantage contributes to Q1 differential
    home_bias = 1.5 if is_home else 0.0

    simulated_values = []
    blowout_count = 0

    for _ in range(num_simulations):
        running_differential = 0.0
        total_simulated_minutes = 0.0
        game_became_blowout = False
        is_blowout_bench_game = False

        for quarter in range(1, 5):
            # Simulate this quarter's net score swing
            # Home bias only applied in Q1 opener; spread distributes evenly
            q1_home_component = home_bias if quarter == 1 else 0.0
            quarter_swing = random.gauss(
                spread_per_quarter + q1_home_component,
                QUARTER_SCORE_STD
            )
            running_differential += quarter_swing

            # Detect blowout at any quarter
            if abs(running_differential) > BLOWOUT_DIFFERENTIAL_HEAVY:
                game_became_blowout = True
                if player_tier == PLAYER_TIER_BENCH:
                    is_blowout_bench_game = True

            # Estimate how many minutes the player plays this quarter
            # Pass player_tier for tier-specific blowout behavior
            quarter_minutes = estimate_quarter_minutes(
                avg_quarter_minutes,
                quarter,
                running_differential,
                is_starter=(player_tier != PLAYER_TIER_BENCH),
                player_tier=player_tier,
            )
            total_simulated_minutes += quarter_minutes

        if game_became_blowout:
            blowout_count += 1

        # Derive simulated stat from per-minute rate × actual minutes × game variance
        raw_stat = stat_per_minute * total_simulated_minutes * random.gauss(1.0, 0.15)

        # Apply garbage time stat inflation for bench players in blowouts
        # BEGINNER NOTE: Bench players get easier matchups and more offensive freedom
        # in garbage time — their per-minute production typically increases.
        if is_blowout_bench_game:
            raw_stat *= (1.0 + BENCH_GARBAGE_TIME_STAT_BOOST)

        # Apply close-game extra effort for stars
        if close_game_boost > 0 and not game_became_blowout:
            raw_stat *= (1.0 + close_game_boost)

        # Apply clutch Q4 multiplier for close-game simulations
        # When clutch data shows the player elevates/shrinks in crunch time,
        # adjust the stat output proportionally for close-game simulations.
        if not game_became_blowout and abs(clutch_q4_multiplier - 1.0) >= 0.005:
            raw_stat *= clutch_q4_multiplier

        simulated_stat = max(0.0, raw_stat)
        simulated_values.append(simulated_stat)

    # ---- Summarize distribution ----
    n = len(simulated_values)
    mean_val = sum(simulated_values) / n if n > 0 else 0.0

    variance = sum((v - mean_val) ** 2 for v in simulated_values) / max(1, n - 1)
    std_val = math.sqrt(variance)

    sorted_vals = sorted(simulated_values)
    p10_idx = max(0, int(0.10 * n) - 1)
    p90_idx = min(n - 1, int(0.90 * n))
    p10_val = sorted_vals[p10_idx] if sorted_vals else 0.0
    p90_val = sorted_vals[p90_idx] if sorted_vals else 0.0

    blowout_rate = blowout_count / n if n > 0 else 0.0

    return {
        'simulated_values': simulated_values,
        'mean': round(_safe_float(mean_val, 0.0), 3),
        'std': round(_safe_float(std_val, 1.0), 3),
        'p10': round(_safe_float(p10_val, 0.0), 3),
        'p90': round(_safe_float(p90_val, 0.0), 3),
        'blowout_game_rate': round(_safe_float(blowout_rate, 0.0), 4),
        'player_tier': player_tier,
    }

# ============================================================
# END SECTION: Game Script Simulation
# ============================================================


# ============================================================
# SECTION: Flat Simulation Blending
# ============================================================

def blend_with_flat_simulation(game_script_results, flat_simulation_results, blend_weight=0.30):
    """
    Blend game-script simulation results with flat Quantum Matrix results.

    Uses blend_weight for game-script and (1-blend_weight) for flat simulation.
    The mean and std are blended; the combined result is what gets used
    for probability calculation.

    Args:
        game_script_results (dict): Output from simulate_game_script.
        flat_simulation_results (dict): Standard Quantum Matrix Engine 5.6 output (from simulation.py).
            Expected keys: 'mean' (or 'average'), 'std' (or 'standard_deviation').
        blend_weight (float): Weight for game-script component. Default 0.30.

    Returns:
        dict: {
            'blended_mean': float,
            'blended_std': float,
            'game_script_mean': float,
            'flat_mean': float,
            'blend_weight': float,
        }

    Example:
        blended = blend_with_flat_simulation(gs_results, flat_results, blend_weight=0.30)
        # blended['blended_mean'] = 0.30 * gs_mean + 0.70 * flat_mean
    """
    gs_mean = float(game_script_results.get('mean', 0.0))
    gs_std  = float(game_script_results.get('std', 0.0))

    # Support both key naming conventions from simulation.py
    flat_mean = float(
        flat_simulation_results.get('mean',
        flat_simulation_results.get('average', 0.0))
    )
    flat_std = float(
        flat_simulation_results.get('std',
        flat_simulation_results.get('standard_deviation', 0.0))
    )

    flat_weight = 1.0 - blend_weight

    blended_mean = blend_weight * gs_mean + flat_weight * flat_mean
    blended_std  = blend_weight * gs_std  + flat_weight * flat_std

    return {
        'blended_mean':      round(_safe_float(blended_mean, 0.0), 3),
        'blended_std':       round(_safe_float(blended_std, 1.0), 3),
        'game_script_mean':  round(_safe_float(gs_mean, 0.0), 3),
        'flat_mean':         round(_safe_float(flat_mean, 0.0), 3),
        'blend_weight':      _safe_float(blend_weight, 0.0),
    }

# ============================================================
# END SECTION: Flat Simulation Blending
# ============================================================
