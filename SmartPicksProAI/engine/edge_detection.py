# ============================================================
# FILE: engine/edge_detection.py
# PURPOSE: Detect betting edges by analyzing directional forces
#          that push a player's actual performance MORE or LESS
#          than the posted prop line.
# CONNECTS TO: projections.py, confidence.py, simulation.py
# CONCEPTS COVERED: Asymmetric forces, edge detection,
#                   directional analysis
# ============================================================
# Standard library only
import math  # For rounding calculations
def _safe_float(value, fallback=0.0):
    """Return *value* if it is a finite float, otherwise *fallback*.
    Last-line-of-defence guard that prevents NaN or ±inf from leaking
    out of the edge detection engine into downstream scoring code.
    """
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
# Coefficient of variation (std / mean) above which a stat is
# considered "too unpredictable" to bet reliably.
# CV of 0.45 means the std is 45% of the average — very noisy.
HIGH_VARIANCE_CV_THRESHOLD = 0.45  # loosened from 0.40 so secondary stats (steals, blocks, threes) aren't auto-avoided
# Sportsbook vig/juice is typically ~4.5%. We subtract 2.0% from raw edge
# to account for this before declaring a qualifying edge.
VIG_ADJUSTMENT_PCT = 2.0
# Minimum edge required AFTER vig deduction for a pick to qualify
MIN_EDGE_AFTER_VIG = 2.0  # lowered from 4.0 (was 3.0 originally) to align with Silver 3% min edge
# Per-stat minimum edge thresholds (2D).
# Different stats have different variance levels — require proportionally more edge.
# Higher-variance stats need larger edges to overcome noise.
STAT_EDGE_THRESHOLDS = {
    "points": 2.5,
    "rebounds": 3.0,
    "assists": 3.0,
    "threes": 4.0,
    "steals": 5.0,
    "blocks": 5.0,
    "turnovers": 4.0,
    "ftm": 4.0,
    "points_rebounds_assists": 2.0,
    "points_rebounds": 2.0,
    "points_assists": 2.0,
    "rebounds_assists": 2.5,
    "blocks_steals": 4.0,
    "fantasy_score_pp": 2.0,
    "fantasy_score_dk": 2.0,
    "fantasy_score_ud": 2.0,
}
# Low-volume stat types with inherently higher variance.
# These require a larger raw edge to overcome uncertainty.
LOW_VOLUME_STATS = {"steals", "blocks", "turnovers", "threes", "ftm"}
# Uncertainty multiplier applied to low-volume stats' edge calculations.
# 1.3x means a steal prop needs effectively 1.3x more edge to qualify.
LOW_VOLUME_UNCERTAINTY_MULTIPLIER = 1.3  # softened from 1.5 to reduce over-penalizing secondary stats
# Conflict severity threshold for "high conflict" force detection (2E).
# When min(over, under) / max(over, under) exceeds this, forces are considered
# in high conflict (both sides nearly equal strength with significant magnitude).
CONFLICT_SEVERITY_HIGH_THRESHOLD = 0.7  # was inline magic number 0.7
# ============================================================
# SECTION: Force Definitions
# "Forces" are contextual factors that push a player's stat
# outcome OVER or UNDER the prop line.
# Each force has a name, direction, and strength (0-3 scale).
# ============================================================
def analyze_directional_forces(
    player_data,
    prop_line,
    stat_type,
    projection_result,
    game_context,
    platform_lines=None,
    recent_form_ratio=None,
) -> dict:
    """
    Identify all forces pushing the stat OVER or UNDER the line.
    Checks multiple factors: projection vs line, matchup,
    pace, blowout risk, rest, home/away, etc.
    Returns a list of active forces and a summary.
    Args:
        player_data (dict): Player season stats from CSV
        prop_line (float): The betting line (e.g., 24.5)
        stat_type (str): 'points', 'rebounds', 'assists', etc.
        projection_result (dict): Output from projections.py
            includes projected values and factors
        game_context (dict): Tonight's game info:
            'opponent', 'is_home', 'rest_days', 'game_total',
            'vegas_spread' (positive = player's team favored)
        platform_lines (dict or None): Optional mapping of platform name
            to posted line (e.g. {'DraftKings': 24.5, 'FanDuel': 25.0}).
            Used for Market Consensus force (2A).
        recent_form_ratio (float or None): Ratio of recent-game average to
            season average (e.g. 1.25 = running 25% above average).
            Used for Regression-to-Mean force (2F).
    Returns:
        dict: {
            'over_forces': list of force dicts (pushing OVER)
            'under_forces': list of force dicts (pushing UNDER)
            'over_count': int
            'under_count': int
            'over_strength': float (total strength of over forces)
            'under_strength': float (total strength of under forces)
            'net_direction': str 'OVER' or 'UNDER'
            'net_strength': float
            'conflict_severity': float (0-1, 1=perfectly balanced forces)
        }
    Example:
        If projection is 26.8 and line is 24.5,
        "Projection Exceeds Line" → OVER force, strength 2.1
    """
    # Lists to collect forces pushing each direction
    all_over_forces = []   # Forces that suggest going OVER
    all_under_forces = []  # Forces that suggest going UNDER
    # Get the projected value for the relevant stat
    projected_value = projection_result.get(f"projected_{stat_type}", 0)
    # Get contextual values
    defense_factor = projection_result.get("defense_factor", 1.0)
    pace_factor = projection_result.get("pace_factor", 1.0)
    blowout_risk = projection_result.get("blowout_risk", 0.15)
    rest_factor = projection_result.get("rest_factor", 1.0)
    is_home = game_context.get("is_home", True)
    vegas_spread = game_context.get("vegas_spread", 0.0)  # + = player's team favored
    game_total = game_context.get("game_total", 220.0)
    # ============================================================
    # SECTION: Check Each Force
    # ============================================================
    # --- Force 1: Projection vs Line ---
    # Most important force: does our model project OVER or UNDER?
    if projected_value > prop_line:
        projection_gap = projected_value - prop_line
        strength = min(3.0, projection_gap / 3.0)  # 1 point gap = 0.33 strength
        all_over_forces.append({
            "name": "Model Projection Exceeds Line",
            "description": f"Projects {projected_value:.1f} vs line of {prop_line}",
            "strength": round(strength, 2),
            "direction": "OVER",
        })
    elif projected_value < prop_line:
        projection_gap = prop_line - projected_value
        strength = min(3.0, projection_gap / 3.0)
        all_under_forces.append({
            "name": "Model Projection Below Line",
            "description": f"Projects {projected_value:.1f} vs line of {prop_line}",
            "strength": round(strength, 2),
            "direction": "UNDER",
        })
    # --- Force 2: Matchup / Defensive Rating ---
    if defense_factor > 1.05:
        strength = min(2.0, (defense_factor - 1.0) * 10.0)
        all_over_forces.append({
            "name": "Favorable Matchup",
            "description": f"Opponent allows {(defense_factor-1)*100:.0f}% more than avg to this position",
            "strength": round(strength, 2),
            "direction": "OVER",
        })
    elif defense_factor < 0.95:
        strength = min(2.0, (1.0 - defense_factor) * 10.0)
        all_under_forces.append({
            "name": "Tough Matchup",
            "description": f"Opponent allows {(1-defense_factor)*100:.0f}% less than avg to this position",
            "strength": round(strength, 2),
            "direction": "UNDER",
        })
    # --- Force 3: Game Pace ---
    if pace_factor > 1.02:
        strength = min(1.5, (pace_factor - 1.0) * 15.0)
        all_over_forces.append({
            "name": "Fast Pace Game",
            "description": f"Expected game pace {pace_factor*100-100:.1f}% above league average",
            "strength": round(strength, 2),
            "direction": "OVER",
        })
    elif pace_factor < 0.98:
        strength = min(1.5, (1.0 - pace_factor) * 15.0)
        all_under_forces.append({
            "name": "Slow Pace Game",
            "description": f"Expected game pace {(1-pace_factor)*100:.1f}% below league average",
            "strength": round(strength, 2),
            "direction": "UNDER",
        })
    # --- Force 4: Blowout Risk ---
    # High blowout risk = stars may sit in garbage time
    if blowout_risk > 0.25:
        strength = min(2.0, (blowout_risk - 0.25) * 10.0)
        all_under_forces.append({
            "name": "Blowout Risk",
            "description": f"{blowout_risk*100:.0f}% chance of blowout — star may sit late",
            "strength": round(strength, 2),
            "direction": "UNDER",
        })
    # --- Force 5: Rest / Fatigue ---
    if rest_factor < 0.95:
        strength = min(1.5, (1.0 - rest_factor) * 20.0)
        all_under_forces.append({
            "name": "Fatigue / Back-to-Back",
            "description": f"Playing on short rest — performance typically drops {(1-rest_factor)*100:.0f}%",
            "strength": round(strength, 2),
            "direction": "UNDER",
        })
    elif rest_factor > 1.01:
        strength = min(1.0, (rest_factor - 1.0) * 50.0)
        all_over_forces.append({
            "name": "Well Rested",
            "description": "Multiple days of rest — typically improves performance",
            "strength": round(strength, 2),
            "direction": "OVER",
        })
    # --- Force 6: Home Court Advantage ---
    if is_home:
        all_over_forces.append({
            "name": "Home Court Advantage",
            "description": "Playing at home — historically +2.5% performance boost",
            "strength": 0.5,
            "direction": "OVER",
        })
    else:
        all_under_forces.append({
            "name": "Road Game",
            "description": "Playing away — historically -1.5% performance penalty",
            "strength": 0.3,
            "direction": "UNDER",
        })
    # --- Force 7: Vegas Spread (Blowout Angle) ---
    # If player's team is a huge favorite, stars may rest in 4th quarter
    if vegas_spread > 10:
        all_under_forces.append({
            "name": "Heavy Favorite — Garbage Time Risk",
            "description": f"Team favored by {vegas_spread:.1f} — stars may sit late",
            "strength": min(1.5, vegas_spread * 0.08),
            "direction": "UNDER",
        })
    elif vegas_spread < -8:
        # Player's team is a big underdog — may get blown out
        all_under_forces.append({
            "name": "Heavy Underdog — Possible Blowout Loss",
            "description": f"Team is {abs(vegas_spread):.1f}-point underdog",
            "strength": min(1.5, abs(vegas_spread) * 0.06),
            "direction": "UNDER",
        })
    # --- Force 8: High Game Total ---
    # High-total game = fast-paced scoring game = more opportunities
    if game_total > 228:
        all_over_forces.append({
            "name": "High-Scoring Game Environment",
            "description": f"Vegas total of {game_total:.0f} — very high-paced game expected",
            "strength": min(1.5, (game_total - 220) * 0.075),
            "direction": "OVER",
        })
    elif game_total < 214 and game_total > 0:
        all_under_forces.append({
            "name": "Low-Scoring Game Environment",
            "description": f"Vegas total of {game_total:.0f} — slow, defensive game expected",
            "strength": min(1.5, (220 - game_total) * 0.075),
            "direction": "UNDER",
        })
    # ============================================================
    # END SECTION: Check Each Force
    # ============================================================
    # ============================================================
    # SECTION: Line Sharpness Detection (W1)
    # Sharp books set lines RIGHT at the player's true average.
    # A line within 3% of the season avg is essentially a coin-flip —
    # penalize over-confidence. Lines 8%+ away are where real edges live.
    # ============================================================
    season_average = player_data.get(f"{stat_type}_avg", None)
    if season_average is not None:
        try:
            season_average = float(season_average)
        except (TypeError, ValueError):
            season_average = None
    line_sharpness_force = detect_line_sharpness(prop_line, season_average, stat_type)
    if line_sharpness_force is not None:
        if line_sharpness_force["direction"] == "OVER":
            all_over_forces.append(line_sharpness_force)
        else:
            all_under_forces.append(line_sharpness_force)
    # ============================================================
    # END SECTION: Line Sharpness Detection
    # ============================================================
    # --- Force 9: Market Consensus (2A) ---
    # When multiple platforms post different lines, consensus shows the "true" line.
    # A prop line far below consensus = OVER value; far above consensus = UNDER value.
    if platform_lines and len(platform_lines) >= 2:
        platform_values = [v for v in (_safe_float(val) for val in platform_lines.values() if val is not None) if v is not None]
        if len(platform_values) >= 2:
            consensus_line = sum(platform_values) / len(platform_values)
            if consensus_line > 0:
                gap_pct = (consensus_line - prop_line) / consensus_line * 100.0
                if gap_pct > 5.0:
                    # Prop line is more than 5% BELOW consensus → OVER value
                    strength = min(2.0, gap_pct / 5.0)
                    all_over_forces.append({
                        "name": "Market Consensus",
                        "description": (
                            f"Line ({prop_line}) is {gap_pct:.1f}% below cross-platform "
                            f"consensus ({consensus_line:.2f}) — potential mispriced line"
                        ),
                        "strength": round(strength, 2),
                        "direction": "OVER",
                    })
                elif gap_pct < -5.0:
                    # Prop line is more than 5% ABOVE consensus → UNDER value
                    strength = min(2.0, abs(gap_pct) / 5.0)
                    all_under_forces.append({
                        "name": "Market Consensus",
                        "description": (
                            f"Line ({prop_line}) is {abs(gap_pct):.1f}% above cross-platform "
                            f"consensus ({consensus_line:.2f}) — line may be inflated"
                        ),
                        "strength": round(strength, 2),
                        "direction": "UNDER",
                    })
    # --- Force 10: Regression to Mean (2F) ---
    # Players running significantly above/below their season average tend to revert.
    # This is one of the most reliable phenomena in sports statistics.
    _recent_form = recent_form_ratio if recent_form_ratio is not None else projection_result.get("recent_form_ratio", None)
    if _recent_form is not None:
        try:
            _recent_form = float(_recent_form)
        except (TypeError, ValueError):
            _recent_form = None
    if _recent_form is not None:
        if _recent_form > 1.20:
            # Running 20%+ above average — regression to mean is likely
            strength = min(1.5, (_recent_form - 1.0) * 3.0)
            all_under_forces.append({
                "name": "Regression Risk",
                "description": (
                    f"Running {(_recent_form - 1.0) * 100:.0f}% above season average — "
                    "regression to the mean is likely"
                ),
                "strength": round(strength, 2),
                "direction": "UNDER",
            })
        elif _recent_form < 0.80:
            # Running 20%+ below average — bounce-back expected
            strength = min(1.2, (1.0 - _recent_form) * 2.5)
            all_over_forces.append({
                "name": "Bounce-Back",
                "description": (
                    f"Running {(1.0 - _recent_form) * 100:.0f}% below season average — "
                    "bounce-back toward average expected"
                ),
                "strength": round(strength, 2),
                "direction": "OVER",
            })
    # ============================================================
    # SECTION: Summarize Forces
    # ============================================================
    # Count and sum strength of over/under forces
    over_count = len(all_over_forces)
    under_count = len(all_under_forces)
    over_total_strength = sum(f["strength"] for f in all_over_forces)
    under_total_strength = sum(f["strength"] for f in all_under_forces)
    # --- Conflict Severity Score (2E) ---
    # Measures how evenly matched the opposing forces are.
    # 1.0 = perfectly balanced (maximum conflict), 0 = one-sided
    if over_total_strength > 0 and under_total_strength > 0:
        conflict_severity = min(over_total_strength, under_total_strength) / max(over_total_strength, under_total_strength)
    else:
        conflict_severity = 0.0
    # Determine the net direction
    if over_total_strength > under_total_strength:
        net_direction = "OVER"
        net_strength = over_total_strength - under_total_strength
    else:
        net_direction = "UNDER"
        net_strength = under_total_strength - over_total_strength
    return {
        "over_forces": all_over_forces,
        "under_forces": all_under_forces,
        "over_count": over_count,
        "under_count": under_count,
        "over_strength": _safe_float(round(over_total_strength, 2), 0.0),
        "under_strength": _safe_float(round(under_total_strength, 2), 0.0),
        "net_direction": net_direction,
        "net_strength": _safe_float(round(net_strength, 2), 0.0),
        "conflict_severity": _safe_float(round(conflict_severity, 3), 0.0),
    }
    # ============================================================
    # END SECTION: Summarize Forces
    # ============================================================
# ============================================================
# SECTION: Closing Line Value (2B)
# ============================================================
def estimate_closing_line_value(current_line, model_projection, hours_to_game=None):
    """
    Estimate the closing line value (CLV) for a prop bet. (2B)
    CLV is the gold standard of sharp betting: did your line beat where
    the market eventually closed? A positive CLV means you got a better
    number than the closing line — a strong indicator of long-term edge.
    BEGINNER NOTE: Lines move as sharp bettors place wagers. The closing
    line is the final line right before tip-off, after all professional
    money has been processed. If you got 24.5 and it closes at 26.0,
    you "beat the closing line" — you had better information than the
    eventual market consensus.
    Args:
        current_line (float): The line you can bet right now
        model_projection (float): Our model's projected value for the stat
        hours_to_game (float or None): Hours until tip-off. When < 2,
            less line movement is expected (market more locked in).
    Returns:
        dict: {
            'estimated_closing_line': float,
            'clv_edge': float,    # positive = you beat the close
            'is_positive_clv': bool,
        }
    Example:
        current_line=24.5, model_projection=27.0, hours_to_game=6
        → estimated_close = 24.5*0.3 + 27.0*0.7 = 26.25
        → clv_edge = 24.5 - 26.25 = -1.75  (negative = you DON'T beat close)
        Flip: current_line=27.0, model_projection=24.5
        → clv_edge = 27.0 - 25.25 = +1.75  (positive CLV — you beat close)
    """
    if current_line <= 0 or model_projection <= 0:
        return {
            "estimated_closing_line": current_line,
            "clv_edge": 0.0,
            "is_positive_clv": False,
        }
    if hours_to_game is not None and hours_to_game < 2:
        # Close to game — less line movement expected
        estimated_close = current_line * 0.6 + model_projection * 0.4
    else:
        # Standard: lines move significantly toward the true value
        estimated_close = current_line * 0.3 + model_projection * 0.7
    # CLV edge: positive = you're getting a better line than where it will close
    clv_edge = current_line - estimated_close
    return {
        "estimated_closing_line": _safe_float(round(estimated_close, 3), 0.0),
        "clv_edge": _safe_float(round(clv_edge, 3), 0.0),
        "is_positive_clv": clv_edge > 0,
    }
# ============================================================
# SECTION: Dynamic Vig (2C)
# ============================================================
def calculate_dynamic_vig(over_odds=None, under_odds=None, platform=None):
    """
    Calculate the dynamic vig percentage for a prop bet. (2C)
    Different platforms have different vig structures:
    - DFS platforms (PrizePicks/Underdog): 0% per-leg vig (profit is in payout structure)
    - Sportsbooks with real odds: actual vig from the juice
    - Default: 2.38% (standard -110/-110 lines)
    BEGINNER NOTE: Vig (or "juice") is the sportsbook's fee. At -110/-110,
    the breakeven is 52.38% (not 50%). Subtracting the vig from our edge
    gives the TRUE edge we need to overcome to be profitable.
    Args:
        over_odds (float or None): American odds on the over (e.g. -110, +120)
        under_odds (float or None): American odds on the under
        platform (str or None): Platform name ('PrizePicks', 'Underdog',
            'DraftKings', etc.)
    Returns:
        float: Vig percentage (0.0 to ~5.0)
    Example:
        calculate_dynamic_vig(platform="PrizePicks") → 0.0
        calculate_dynamic_vig(-110, -110, "DraftKings") → 2.38
        calculate_dynamic_vig(-130, +110, "DraftKings") → ~3.5
    """
    _NO_VIG_PLATFORMS = {"PrizePicks", "Underdog", "Underdog Fantasy"}
    if platform and platform in _NO_VIG_PLATFORMS:
        return 0.0
    if over_odds is not None and under_odds is not None:
        try:
            o = float(over_odds)
            u = float(under_odds)
            # Convert American odds to implied probabilities
            def _implied(odds):
                if odds < 0:
                    return abs(odds) / (abs(odds) + 100.0)
                else:
                    return 100.0 / (odds + 100.0)
            total_implied = _implied(o) + _implied(u)
            # Vig = excess implied probability above 1.0, expressed as a percentage
            return round(max(0.0, (total_implied - 1.0) * 100.0), 3)
        except (ValueError, TypeError):
            pass
    # Fallback: standard -110/-110 vig = 2.38%
    return 2.38
# ============================================================
# SECTION: Avoid List Logic
# Determine if a prop should go on the "avoid" list
# ============================================================
def should_avoid_prop(
    probability_over,
    directional_forces_result,
    edge_percentage,
    stat_standard_deviation,
    stat_average,
    stat_type=None,
    platform=None,
    over_odds=-110,
) -> dict:
    """
    Determine whether a prop pick should be avoided.
    A prop goes on the avoid list if:
    1. No clear edge (< 5% edge in either direction)
    2. High variance stat (too unpredictable)
    3. Conflicting forces (equal OVER and UNDER pressure)
    4. Blowout risk forces are present and strong
    Args:
        probability_over (float): P(over), 0-1
        directional_forces_result (dict): Output of analyze_directional_forces
        edge_percentage (float): Edge %, positive = lean over
        stat_standard_deviation (float): Variability
        stat_average (float): Average for this stat
        stat_type (str, optional): Stat type for low-volume check
        platform (str, optional): Platform name — DFS platforms have 0%
            structural vig on individual legs (payout baked into table).
            Sportsbooks use variable juice from actual odds.
        over_odds (float): American odds on the over side (used for DraftKings
            vig calculation). Default -110.
    Returns:
        tuple: (should_avoid: bool, reasons: list of str)
    Example:
        0.51 probability, conflicting forces → avoid=True,
        reasons=['Insufficient edge (<5%)', 'Conflicting forces']
    """
    avoid_reasons = []  # Collect all reasons to avoid
    # ── Platform-specific vig adjustment ──────────────────────────────────────
    # PrizePicks and Underdog have NO per-leg juice — vig is baked into the
    # multi-leg payout structure, so we apply 0% to individual legs.
    # DraftKings uses actual American odds, so we calculate the real vig.
    _NO_VIG_PLATFORMS = {"PrizePicks", "Underdog", "Underdog Fantasy"}
    if platform and platform in _NO_VIG_PLATFORMS:
        effective_vig = 0.0
    elif platform is None:
        # No platform specified → treat as DFS (no per-leg vig)
        effective_vig = 0.0
    elif over_odds is not None and over_odds != -110:
        # Derive vig from actual DraftKings odds (e.g. -130 → breakeven 56.5%)
        # Vig = implied_prob - 0.5 (excess above fair 50/50)
        try:
            _odds = float(over_odds)
            _implied = abs(_odds) / (abs(_odds) + 100.0) if _odds < 0 else 100.0 / (_odds + 100.0)
            effective_vig = max(0.0, (_implied - 0.5) * 100.0)
        except (ValueError, TypeError):
            effective_vig = VIG_ADJUSTMENT_PCT
    elif over_odds == -110:
        # Standard -110 odds → standard vig
        effective_vig = VIG_ADJUSTMENT_PCT
    else:
        effective_vig = VIG_ADJUSTMENT_PCT
    # Reason 1: Edge too small after vig adjustment
    stat_type_lower = str(stat_type).lower() if stat_type else ""
    vig_adjusted_edge = abs(edge_percentage) - effective_vig
    # Low-volume stats require a larger effective edge due to higher variance.
    if stat_type_lower in LOW_VOLUME_STATS:
        effective_edge = vig_adjusted_edge / LOW_VOLUME_UNCERTAINTY_MULTIPLIER
    else:
        effective_edge = vig_adjusted_edge
    # Use per-stat threshold if available, else fall back to MIN_EDGE_AFTER_VIG
    _stat_min_edge = STAT_EDGE_THRESHOLDS.get(stat_type_lower, MIN_EDGE_AFTER_VIG)
    if effective_edge < _stat_min_edge:
        _vig_adj_display = max(0.0, vig_adjusted_edge)  # show 0 if negative to avoid confusion
        _vig_label = f"{effective_vig:.1f}% vig" if effective_vig > 0 else "no vig (DFS platforms)"
        avoid_reasons.append(
            f"Insufficient edge after vig ({edge_percentage:.1f}% raw → "
            f"{_vig_adj_display:.1f}% after {_vig_label}) — "
            f"below {_stat_min_edge:.1f}% minimum for {stat_type_lower or 'this stat'}"
        )
    # Reason 2: High variance relative to line (too unpredictable)
    if stat_average > 0:
        coefficient_of_variation = stat_standard_deviation / stat_average
        if coefficient_of_variation > HIGH_VARIANCE_CV_THRESHOLD:
            avoid_reasons.append(
                f"High variance stat (CV={coefficient_of_variation:.2f}) — very unpredictable"
            )
    # Reason 3: Conflicting forces (2E enhanced)
    over_strength = directional_forces_result.get("over_strength", 0)
    under_strength = directional_forces_result.get("under_strength", 0)
    conflict_severity = directional_forces_result.get("conflict_severity", 0.0)
    if over_strength > 0 and under_strength > 0:
        if conflict_severity > CONFLICT_SEVERITY_HIGH_THRESHOLD and over_strength > 1.0 and under_strength > 1.0:
            avoid_reasons.append(
                f"High conflict severity ({conflict_severity:.2f}) — strong opposing signals "
                "on both sides with no clear direction"
            )
    # Reason 4: Strong blowout risk force present
    under_forces = directional_forces_result.get("under_forces", [])
    for force in under_forces:
        if "Blowout" in force.get("name", "") and force.get("strength", 0) > 1.0:
            avoid_reasons.append(
                "Strong blowout risk — player may not get full minutes"
            )
            break
    # Zero-Filter Recovery: never flag a prop as "should_avoid".
    # Reasons are retained as informational metadata but do NOT block
    # the prop from rendering on the analysis page.
    should_avoid = False
    return should_avoid, avoid_reasons
# ============================================================
# END SECTION: Avoid List Logic
# ============================================================
# ============================================================
# SECTION: Correlation Detection
# Identify correlated props that come from the same game.
# Correlated props carry additional parlay risk.
# ============================================================
def detect_correlated_props(props_with_results):
    """
    Flag props from the same game as correlated bets.
    When two players from the same game are included in a parlay,
    their outcomes are statistically correlated (same pace, scoring
    environment, and blowout risk). This should be disclosed to
    the user so they can make informed parlay decisions.
    Args:
        props_with_results (list of dict): Analysis results. Each dict
            must contain 'player_team' and 'opponent' keys.
    Returns:
        dict: Mapping of prop index (int) to a correlation warning
              string, or an empty dict if no correlations found.
    Example:
        If props[0] is LeBron (LAL vs GSW) and props[2] is Steph
        (GSW vs LAL), both are flagged with a correlation warning.
    """
    # Build a "game key" for each prop: frozenset of the two teams
    # so LAL-vs-GSW and GSW-vs-LAL map to the same key.
    game_key_to_indices = {}  # game_key → list of prop indices
    for idx, result in enumerate(props_with_results):
        team = result.get("player_team", result.get("team", "")).upper().strip()
        opponent = result.get("opponent", "").upper().strip()
        if team and opponent:
            game_key = frozenset([team, opponent])
        elif team:
            game_key = frozenset([team])
        else:
            continue  # No team info — skip
        if game_key not in game_key_to_indices:
            game_key_to_indices[game_key] = []
        game_key_to_indices[game_key].append(idx)
    # Build the correlation warnings dict
    correlation_warnings = {}
    for game_key, indices in game_key_to_indices.items():
        if len(indices) < 2:
            continue  # Only one prop from this game — no correlation
        teams_str = " vs ".join(sorted(game_key))
        for i in indices:
            correlated_names = [
                props_with_results[j].get("player_name", "?")
                for j in indices if j != i
            ]
            correlated_str = ", ".join(correlated_names)
            correlation_warnings[i] = (
                f"Correlated with {correlated_str} ({teams_str} game) — "
                "same game props share scoring environment and blowout risk"
            )
    return correlation_warnings
# ============================================================
# END SECTION: Correlation Detection
# ============================================================
# ============================================================
# SECTION: Line Sharpness Detection (W1)
# Detect when books have set a "sharp" line right at the player's
# true average (a trap for bettors) vs. a line with real edge.
# ============================================================
def detect_line_sharpness(prop_line, season_average, stat_type="points") -> dict:
    """
    Detect whether a prop line is "sharp" (set close to the true average).
    Sharp lines are set RIGHT at the player's true average, making it
    essentially a 50/50 coin-flip. The engine shouldn't be confident
    on sharp lines — books have accurately priced them.
    Lines set 8%+ away from the average are where real edges exist
    because the book has (intentionally or not) left a gap.
    Args:
        prop_line (float): The betting line (e.g., 24.5)
        season_average (float or None): Player's season average for this stat.
            None means no average available — return None (no force).
        stat_type (str): Stat name for description
    Returns:
        dict or None: A force dict pushing UNDER (sharpness penalty) when
            line is within 3% of average, or None when not applicable.
            Returns an OVER force when line is set far below average (real edge).
    Example:
        season_avg=24.8, line=24.5 → 1.2% below avg → sharp line → UNDER penalty
        season_avg=24.8, line=20.5 → 17.3% below avg → real edge → no penalty (or OVER boost)
    """
    if season_average is None or season_average <= 0 or prop_line <= 0:
        return None  # Can't compute sharpness without a valid average
    # How far is the line from the season average (as percentage)?
    # Positive = line ABOVE average (harder for OVER), negative = line BELOW average
    gap_pct = (prop_line - season_average) / season_average * 100.0
    abs_gap_pct = abs(gap_pct)
    if abs_gap_pct < 3.0:
        # Line is within 3% of season average — this is a SHARP line.
        # Books have set it at the true average → 50/50 coin flip.
        # Penalize confidence by pushing an UNDER force (signal of uncertainty).
        strength = 1.5 - (abs_gap_pct / 3.0) * 1.0  # Closer to avg = stronger penalty
        return {
            "name": "Sharp Line — Books Set at True Average",
            "description": (
                f"Line of {prop_line} is only {abs_gap_pct:.1f}% from season avg "
                f"({season_average:.1f}). Books have accurately priced this — "
                f"edge is minimal."
            ),
            "strength": round(max(0.5, strength), 2),
            "direction": "UNDER",  # Signals caution / reduced confidence
        }
    elif abs_gap_pct >= 8.0:
        # Line is 8%+ away from average — real edge territory.
        # Don't add a force (the Projection vs Line force already captures this),
        # but this confirms the edge is real, not a trap.
        # We return None here because the existing "Model Projection" force handles it.
        return None
    else:
        # Gap between 3% and 8% — moderate zone, no special force needed
        return None
# ============================================================
# END SECTION: Line Sharpness Detection
# ============================================================
# ============================================================
# SECTION: Trap Line Detection (W5)
# Detect when books set a deliberately bait-y line to attract
# public money on the "obvious" side, but hidden factors make
# the obvious side wrong.
# ============================================================
def detect_trap_line(
    prop_line,
    season_average,
    defense_factor,
    rest_factor,
    game_total,
    blowout_risk,
    stat_type="points",
) -> dict:
    """
    Detect whether a prop line is a potential "trap."
    A trap line is deliberately set to attract public money on
    the obvious side, while hidden factors (tough defense + fatigue
    + low total) make the obvious side a loser.
    Two trap patterns:
    1. Line 10%+ below average + multiple negative factors present
       → Looks like easy OVER but the negative factors will kill it
    2. Line 10%+ above average + multiple positive factors present
       → Looks like a tough OVER but the positive factors will carry it
       (though this pattern is less dangerous)
    3. Line set at an exact common hit number (e.g., 24.5 near 25 avg)
       → Book is using a round number trap to attract action
    Args:
        prop_line (float): The betting line
        season_average (float or None): Player's season average
        defense_factor (float): Defensive multiplier (>1 = weak defense)
        rest_factor (float): Rest multiplier (<1 = fatigued)
        game_total (float): Vegas over/under total
        blowout_risk (float): Estimated blowout probability
        stat_type (str): Stat name for description
    Returns:
        dict or None: Trap detection result with:
            'is_trap': bool
            'trap_type': str ('under_trap' | 'over_trap' | 'round_number_trap' | None)
            'warning_message': str
            'confidence_penalty': float (0-15 points to subtract)
            'negative_factors': list of str
            'positive_factors': list of str
    Example:
        Player avg 26 PPG, line 23.5 (9.6% below), opponent top-5 D,
        back-to-back, game total 208 → under_trap detected
    """
    if season_average is None or season_average <= 0 or prop_line <= 0:
        return {"is_trap": False, "trap_type": None, "warning_message": "",
                "confidence_penalty": 0.0, "negative_factors": [], "positive_factors": []}
    gap_pct = (prop_line - season_average) / season_average * 100.0
    # --- Collect negative factors (push performance DOWN) ---
    negative_factors = []
    if defense_factor < 0.95:
        negative_factors.append(f"Tough opponent defense ({(1-defense_factor)*100:.0f}% suppression)")
    if rest_factor < 0.96:
        negative_factors.append("Short rest / back-to-back fatigue")
    if game_total > 0 and game_total < 214:
        negative_factors.append(f"Low Vegas total ({game_total:.0f}) — slow defensive game")
    if blowout_risk > 0.25:
        negative_factors.append(f"High blowout risk ({blowout_risk*100:.0f}%)")
    # --- Collect positive factors (push performance UP) ---
    positive_factors = []
    if defense_factor > 1.05:
        positive_factors.append(f"Weak opponent defense ({(defense_factor-1)*100:.0f}% boost)")
    if rest_factor > 1.005:
        positive_factors.append("Well rested")
    if game_total > 228:
        positive_factors.append(f"High Vegas total ({game_total:.0f}) — fast pace expected")
    if blowout_risk < 0.10:
        positive_factors.append("Very low blowout risk (likely competitive game)")
    # --- Check Trap Pattern 3: Round Number Trap ---
    # BEGINNER NOTE: Books often set lines at round numbers like 24.5 when a
    # player averages ~25. The psychological "round number" attracts action on
    # the OVER, but the line is really at the player's true average (coin-flip).
    # Common round numbers: 5, 10, 15, 20, 25, 30 for points; 5, 10 for rebounds; etc.
    _COMMON_HIT_NUMBERS = {
        "points":    [5, 10, 15, 20, 25, 30, 35, 40],
        "rebounds":  [5, 8, 10, 12, 15],
        "assists":   [5, 8, 10, 12],
        "threes":    [1, 2, 3, 4, 5],
        "steals":    [1, 2, 3],
        "blocks":    [1, 2, 3],
        "turnovers": [2, 3, 4, 5],
        "ftm":       [1, 2, 3, 4, 5, 6],
    }
    stat_type_lower = stat_type.lower() if stat_type else "points"
    common_numbers = _COMMON_HIT_NUMBERS.get(stat_type_lower, [])
    for common_num in common_numbers:
        # Check if the line is set within 0.5 of a common round number
        # AND the season average is near that same round number (within 5%)
        line_near_round = abs(prop_line - common_num) <= 0.5
        avg_near_round = abs(season_average - common_num) / max(1, common_num) < 0.05
        if line_near_round and avg_near_round:
            penalty = 5.0  # Moderate penalty for round number trap
            return {
                "is_trap": True,
                "trap_type": "round_number_trap",
                "warning_message": (
                    f"⚠️ Round Number Trap: Line {prop_line} is set at the common "
                    f"hit number {common_num} (player avg {season_average:.1f}) — "
                    f"books use round numbers to attract action at 50/50 lines."
                ),
                "confidence_penalty": round(penalty, 1),
                "negative_factors": negative_factors,
                "positive_factors": positive_factors,
            }
    # --- Check Trap Pattern 1: Line too LOW + multiple negative factors ---
    # "Bait OVER" trap: line looks like easy over, but negatives will sink it
    if gap_pct <= -10.0 and len(negative_factors) >= 2:
        penalty = min(15.0, 8.0 + len(negative_factors) * 2.0)
        return {
            "is_trap": True,
            "trap_type": "under_trap",
            "warning_message": (
                f"⚠️ Possible Trap Line: Line is {abs(gap_pct):.1f}% BELOW season avg "
                f"({season_average:.1f}) — looks like easy OVER, but {len(negative_factors)} "
                f"negative factors may cancel the edge."
            ),
            "confidence_penalty": round(penalty, 1),
            "negative_factors": negative_factors,
            "positive_factors": positive_factors,
        }
    # --- Check Trap Pattern 2: Line too HIGH + multiple positive factors ---
    # "Bait UNDER" trap: line looks hard to exceed, but positives will carry it
    if gap_pct >= 10.0 and len(positive_factors) >= 2:
        penalty = min(10.0, 5.0 + len(positive_factors) * 1.5)
        return {
            "is_trap": True,
            "trap_type": "over_trap",
            "warning_message": (
                f"⚠️ Possible Trap Line: Line is {gap_pct:.1f}% ABOVE season avg "
                f"({season_average:.1f}) — looks tough to hit, but {len(positive_factors)} "
                f"positive factors may carry the OVER."
            ),
            "confidence_penalty": round(penalty, 1),
            "negative_factors": negative_factors,
            "positive_factors": positive_factors,
        }
    return {
        "is_trap": False,
        "trap_type": None,
        "warning_message": "",
        "confidence_penalty": 0.0,
        "negative_factors": negative_factors,
        "positive_factors": positive_factors,
    }
# ============================================================
# END SECTION: Trap Line Detection
# ============================================================
# ============================================================
# SECTION: Confidence-Adjusted Edge and Coin-Flip Detection
# ============================================================
def calculate_confidence_adjusted_edge(raw_edge_pct, confidence_score):
    """
    Calculate the confidence-adjusted edge by scaling raw edge by confidence.
    BEGINNER NOTE: A 15% raw edge from an unreliable model is worth less than
    a 10% edge from a highly confident model. This function scales the edge
    by the model's confidence level to give a more accurate picture.
    Formula: adjusted_edge = raw_edge * (confidence_score / 100)
    Args:
        raw_edge_pct (float): Raw edge percentage (e.g. 12.5 for 12.5%)
        confidence_score (float): Model confidence score 0-100
    Returns:
        float: Confidence-adjusted edge percentage
    Example:
        calculate_confidence_adjusted_edge(15.0, 60) → 9.0%
        calculate_confidence_adjusted_edge(10.0, 85) → 8.5%
    """
    if confidence_score <= 0:
        return 0.0
    adjusted = raw_edge_pct * (max(0.0, min(100.0, confidence_score)) / 100.0)
    return _safe_float(round(adjusted, 3), 0.0)
def detect_coin_flip(projection, prop_line, stat_std, stat_type=None):
    """
    Detect when a projection is so close to the line that it's essentially a coin flip.
    BEGINNER NOTE: When the model's projection and the betting line are within
    0.3 standard deviations of each other, the outcome is highly uncertain —
    essentially a 50/50 bet that should be avoided regardless of other signals.
    There's no real edge to exploit here.
    Args:
        projection (float): Model's projected stat value
        prop_line (float): The betting line
        stat_std (float): Standard deviation of the stat
        stat_type (str, optional): Stat type for description
    Returns:
        dict: {
            'is_coin_flip': bool,
            'std_devs_from_line': float,
            'message': str,
        }
    Example:
        # Player projects 24.8 points, line is 24.5, std is 6.0
        # Gap = 0.3 / 6.0 = 0.05 std devs → coin flip
        detect_coin_flip(24.8, 24.5, 6.0, 'points')
        → {'is_coin_flip': True, 'std_devs_from_line': 0.05, ...}
    """
    COIN_FLIP_THRESHOLD = 0.3  # Less than 0.3 std devs = coin flip
    if stat_std <= 0 or prop_line <= 0:
        return {"is_coin_flip": False, "std_devs_from_line": 0.0, "message": ""}
    std_devs = abs(projection - prop_line) / stat_std
    if std_devs < COIN_FLIP_THRESHOLD:
        stat_desc = stat_type.title() if stat_type else "Stat"
        msg = (
            f"🪙 COIN FLIP — AVOID: {stat_desc} projection ({projection:.1f}) is only "
            f"{std_devs:.2f}σ from the line ({prop_line}). This is essentially a 50/50 "
            f"flip — no meaningful edge exists at this separation."
        )
        return {
            "is_coin_flip": True,
            "std_devs_from_line": _safe_float(round(std_devs, 3), 0.0),
            "message": msg,
        }
    return {
        "is_coin_flip": False,
        "std_devs_from_line": _safe_float(round(std_devs, 3), 0.0),
        "message": "",
    }
def calculate_weighted_net_force(directional_forces_result):
    """
    Calculate a weighted net force score where force strength matters.
    BEGINNER NOTE: The current system counts forces equally, but a
    "Strong OVER force" from game pace should count more than a
    "Weak OVER force" from home court advantage. This function weights
    forces by their impact magnitude.
    Force strength mapping:
        strength >= 2.0 → "strong" (weight 1.0)
        strength >= 1.0 → "moderate" (weight 0.6)
        strength <  1.0 → "weak" (weight 0.3)
    Args:
        directional_forces_result (dict): Output of analyze_directional_forces
    Returns:
        dict: {
            'weighted_over_score': float,
            'weighted_under_score': float,
            'weighted_net': float (positive = OVER favored),
            'dominant_direction': str ('OVER' | 'UNDER'),
        }
    Example:
        Forces: 1 strong OVER (2.5), 1 weak UNDER (0.4)
        weighted_over = 1.0, weighted_under = 0.3 * 0.4 / 1.0 ≈ 0.12
        → net = 0.88 in favor of OVER
    """
    def _strength_weight(s):
        """Convert force strength to a normalized weight."""
        if s >= 2.0:
            return 1.0   # Strong force
        elif s >= 1.0:
            return 0.6   # Moderate force
        else:
            return 0.3   # Weak force
    over_forces  = directional_forces_result.get("over_forces",  [])
    under_forces = directional_forces_result.get("under_forces", [])
    weighted_over = sum(
        _strength_weight(f.get("strength", 0)) * f.get("strength", 0)
        for f in over_forces
    )
    weighted_under = sum(
        _strength_weight(f.get("strength", 0)) * f.get("strength", 0)
        for f in under_forces
    )
    net = weighted_over - weighted_under
    dominant = "OVER" if net >= 0 else "UNDER"
    return {
        "weighted_over_score": _safe_float(round(weighted_over, 3), 0.0),
        "weighted_under_score": _safe_float(round(weighted_under, 3), 0.0),
        "weighted_net": _safe_float(round(net, 3), 0.0),
        "dominant_direction": dominant,
    }
# ============================================================
# END SECTION: Confidence-Adjusted Edge and Coin-Flip Detection
# ============================================================
# ============================================================
# SECTION: Bet Classification
# Risk flags (conflicting forces, variance, fatigue, regression) are
# separate from classification and feed into the avoid-list system.
# ============================================================
# Thresholds for Uncertain (risk-flag) detection
# These replace the old "Demon" classification thresholds — conflicting
# forces / variance / fatigue / regression are RISK FLAGS, not bet types.
UNCERTAIN_CONFLICT_RATIO_THRESHOLD = 0.80   # Forces within 20% of each other = conflicting
UNCERTAIN_HIGH_VAR_MAX_EDGE        = 8.0    # High-variance stats with edge <8% = uncertain
UNCERTAIN_HIGH_VAR_STATS           = {"threes", "steals", "blocks"}
UNCERTAIN_BLOWOUT_SPREAD_THRESHOLD = 10.0   # Spread >10 pts on a back-to-back = uncertain
UNCERTAIN_HOT_STREAK_RATIO         = 1.25   # Line at 125%+ of season avg = likely regressing
def classify_bet_type(
    probability_over,
    edge_percentage,
    stat_standard_deviation,
    projected_stat,
    prop_line,
    stat_type,
    directional_forces_result,
    rest_days=1,
    vegas_spread=0.0,
    recent_form_ratio=None,
    season_average=None,
    line_source=None,
) -> dict:
    """
    Classify a prop bet and compute risk flags.
    All props return ``"standard"`` bet_type.  Risk flags (conflicting
    forces, high variance, fatigue, regression) are computed independently
    and returned as ``risk_flags`` / ``is_uncertain``.  They do NOT change
    the ``bet_type`` — they feed into the avoid-list system.
    Args:
        probability_over (float): Model P(over), 0–1
        edge_percentage (float): Edge %, positive = lean OVER
        stat_standard_deviation (float): Distribution width
        projected_stat (float): Model's projected value
        prop_line (float): The betting line
        stat_type (str): 'points', 'rebounds', 'threes', etc.
        directional_forces_result (dict): Output of analyze_directional_forces
        rest_days (int): Days since last game (0 = back-to-back)
        vegas_spread (float): Point spread, positive = team favored
        recent_form_ratio (float or None): Recent form (>1 = hot, <1 = cold)
        season_average (float or None): Player's season average for this stat
        line_source (str or None): Where the line came from (e.g. 'PrizePicks',
            'Underdog', 'DraftKings', 'synthetic').
    Returns:
        dict: {
            'bet_type': 'standard',
            'bet_type_emoji': str,
            'bet_type_label': str,
            'risk_flags': list[str],   # conflicting-forces / uncertainty reasons
            'is_uncertain': bool,      # True when any risk flag is triggered
            'reasons': list[str],
            'std_devs_from_line': float,
            'line_verified': bool,
            'line_reliability_warning': str | None,
        }
    """
    stat_type_lower = str(stat_type).lower() if stat_type else ""
    # ── Compute how many std devs the projection is from the line ──
    std_devs_from_line = 0.0
    if stat_standard_deviation > 0 and prop_line > 0:
        std_devs_from_line = (projected_stat - prop_line) / stat_standard_deviation
    # ── LINE RELIABILITY CHECK ──────────────────────────────────────
    line_verified = True
    line_reliability_warning = None
    _line_source_lower = str(line_source).lower() if line_source is not None else ""
    _is_synthetic_source = _line_source_lower in (
        "", "synthetic", "estimated", "default", "none"
    )
    if prop_line <= 0:
        line_verified = False
        line_reliability_warning = (
            "Prop line is zero or missing — cannot verify line reliability"
        )
    elif season_average is not None and season_average > 0 and _is_synthetic_source:
        line_ratio = prop_line / season_average
        if line_ratio < 0.25 or line_ratio > 4.0:
            line_verified = False
            line_reliability_warning = (
                f"Prop line ({prop_line}) appears unreliable relative to season "
                f"average ({season_average:.1f}) — likely a synthetic/default line"
            )
    # ============================================================
    # RISK FLAGS (formerly "Demon check")
    # These four patterns detect structural uncertainty and are attached
    # to the result as `risk_flags` / `is_uncertain`. They do NOT change
    # `bet_type` — they feed into the avoid-list system via callers.
    # ============================================================
    risk_flags = []
    # Pattern 1: Conflicting directional forces (nearly 50/50 split)
    over_strength  = directional_forces_result.get("over_strength",  0)
    under_strength = directional_forces_result.get("under_strength", 0)
    if over_strength > 0 and under_strength > 0:
        conflict_ratio = min(over_strength, under_strength) / max(over_strength, under_strength)
        if conflict_ratio >= UNCERTAIN_CONFLICT_RATIO_THRESHOLD:
            risk_flags.append(
                f"Conflicting forces: OVER ({over_strength:.1f}) vs UNDER ({under_strength:.1f}) "
                f"are nearly balanced ({conflict_ratio*100:.0f}% overlap) — no clear edge direction"
            )
    # Pattern 2: High-variance stat type with low edge
    abs_edge = abs(edge_percentage)
    if stat_type_lower in UNCERTAIN_HIGH_VAR_STATS and abs_edge < UNCERTAIN_HIGH_VAR_MAX_EDGE:
        risk_flags.append(
            f"{stat_type_lower.title()} is a high-variance stat with only {abs_edge:.1f}% edge "
            f"(threshold: {UNCERTAIN_HIGH_VAR_MAX_EDGE:.0f}%) — too unpredictable to bet with low edge"
        )
    # Pattern 3: Back-to-back with large blowout spread
    is_back_to_back = rest_days == 0
    abs_spread = abs(vegas_spread)
    if is_back_to_back and abs_spread > UNCERTAIN_BLOWOUT_SPREAD_THRESHOLD:
        risk_flags.append(
            f"Back-to-back game (rest_days=0) with a {abs_spread:.0f}-pt spread — "
            "blowout + fatigue combo is a significant risk for missing this stat"
        )
    # Pattern 4: Line set at recent hot streak value likely to regress
    if (
        recent_form_ratio is not None
        and recent_form_ratio >= UNCERTAIN_HOT_STREAK_RATIO
        and season_average is not None
        and season_average > 0
        and prop_line > 0
    ):
        line_vs_avg_ratio = prop_line / season_average if season_average > 0 else 1.0
        if line_vs_avg_ratio >= UNCERTAIN_HOT_STREAK_RATIO:
            risk_flags.append(
                f"Line ({prop_line}) inflated to match recent hot streak "
                f"(recent form {recent_form_ratio:.2f}x, season avg {season_average:.1f}) — "
                f"hot streaks regress; line is priced at peak, not true average"
            )
    is_uncertain = len(risk_flags) > 0
    return {
        "bet_type":        "standard",
        "bet_type_emoji":  "",
        "bet_type_label":  "Standard Bet",
        "risk_flags":      risk_flags,
        "is_uncertain":    is_uncertain,
        "reasons":         risk_flags if risk_flags else [],
        "std_devs_from_line": round(std_devs_from_line, 2),
        "line_verified":   line_verified,
        "line_reliability_warning": line_reliability_warning,
    }
def categorize_alt_lines(standard_line, available_lines):
    """
    Categorize alternate sportsbook prop lines relative to the standard line.
    Sportsbooks offer a primary "standard" Over/Under line plus a set of
    alternate lines at higher or lower thresholds.  This function splits
    those alternate lines into two categories:
    * **below_standard** — lines BELOW the standard line.
      These are safer floor bets; the player only needs to exceed a lower
      threshold to win.  High-probability, lower payout.
    * **above_standard** — lines ABOVE the standard line.
      These are high-risk / high-reward bets; the player must exceed a
      higher threshold.  Lower probability, higher payout.
    Analysis should only be triggered on ACTUAL bookmaker lines — never
    on hypothetical or generated lines.  Feed the output of this function
    directly to the statistical analysis pipeline to ensure only real
    sportsbook lines are evaluated.
    Args:
        standard_line (float): The primary median O/U projection set by the
            sportsbook for this player prop (e.g. SGA Points O/U = 31.5).
        available_lines (list of float): The remaining alternate lines
            offered by the bookmaker for the same player prop.  Do NOT
            include the standard_line itself in this list.
    Returns:
        dict: {
            'standard_line':   float,
            'below_standard':  list[float],  # Lines < standard_line, sorted asc
            'above_standard':  list[float],  # Lines > standard_line, sorted asc
        }
    Example:
        # SGA Points — standard 31.5, alternates [28.5, 29.5, 33.5, 36.5]
        result = categorize_alt_lines(31.5, [28.5, 29.5, 33.5, 36.5])
        # → {
        #     'standard_line':  31.5,
        #     'below_standard': [28.5, 29.5],  # below 31.5 → safe floor
        #     'above_standard': [33.5, 36.5],  # above 31.5 → high risk
        # }
        # Paolo Banchero PRA — standard 33.5, alternates [39.5, 41.5, 44.5]
        result = categorize_alt_lines(33.5, [39.5, 41.5, 44.5])
        # → {
        #     'standard_line':  33.5,
        #     'below_standard': [],
        #     'above_standard': [39.5, 41.5, 44.5],
        # }
    """
    if not standard_line or standard_line <= 0:
        return {
            "standard_line":  standard_line,
            "below_standard": [],
            "above_standard": [],
        }
    below_standard = []
    above_standard = []
    for raw in available_lines:
        try:
            line_val = float(raw)
        except (ValueError, TypeError):
            continue  # Skip non-numeric entries
        # Lines below the standard line (safe floor)
        if line_val < standard_line:
            below_standard.append(line_val)
        # Lines above the standard line (high risk/reward)
        elif line_val > standard_line:
            above_standard.append(line_val)
        # Lines exactly equal to the standard are neither (they ARE the standard)
    return {
        "standard_line":  standard_line,
        "below_standard": sorted(below_standard),  # lowest first
        "above_standard": sorted(above_standard),  # lowest first (closest to std first)
    }
# ============================================================
# END SECTION: Bet Classification
# ============================================================
def _normalize_force_strength(raw_value: float, method: str = "ratio") -> float:
    """
    Normalize a raw force-strength signal to the [0, 100] scale.
    Two standardised methods are supported:
    * ``"ratio"``  — interprets *raw_value* as a ratio where 1.0 = neutral.
      Uses ``(factor - 1) * 100`` capped to [0, 100].  Best for
      factors like pace multipliers and matchup multipliers.
    * ``"gap"``    — interprets *raw_value* as an absolute gap (e.g. the
      difference between projected value and prop line measured in the
      same units as the stat).  Uses ``gap / 3`` capped to [0, 100].
      Best for raw numerical differences.
    Args:
        raw_value: The un-normalised signal value.
        method:    ``"ratio"`` (default) or ``"gap"``.
    Returns:
        float: Normalised strength in [0.0, 100.0].
    """
    if method == "gap":
        return min(max(raw_value / 3.0, 0.0), 100.0)
    # Default: "ratio" method
    return min(max((raw_value - 1.0) * 100.0, 0.0), 100.0)
def _reconcile_line_signals(
    sharpness_signal: dict,
    trap_signal: dict,
) -> dict:
    """
    Reconcile conflicting signals from line sharpness (W1) and trap line
    detection (W5).
    When both fire, the higher-confidence signal wins.  If confidence is
    equal the trap signal is given priority (conservative approach: when
    in doubt, don't bet).
    Args:
        sharpness_signal: Dict from ``detect_line_sharpness()`` with at
            minimum ``{"is_sharp": bool, "confidence": float}``.
        trap_signal: Dict from ``detect_trap_line()`` with at minimum
            ``{"is_trap": bool, "confidence": float}``.
    Returns:
        dict:
            ``{"winner": "sharpness" | "trap" | "neutral",
               "is_sharp": bool, "is_trap": bool,
               "note": str}``
    """
    sharp_conf = float(sharpness_signal.get("confidence", 0.0))
    trap_conf  = float(trap_signal.get("confidence",  0.0))
    is_sharp   = bool(sharpness_signal.get("is_sharp", False))
    is_trap    = bool(trap_signal.get("is_trap",   False))
    if not is_sharp and not is_trap:
        return {"winner": "neutral", "is_sharp": False, "is_trap": False,
                "note": "Neither signal active."}
    if is_sharp and not is_trap:
        return {"winner": "sharpness", "is_sharp": True, "is_trap": False,
                "note": "Only sharpness active."}
    if is_trap and not is_sharp:
        return {"winner": "trap", "is_sharp": False, "is_trap": True,
                "note": "Only trap active."}
    # Both fire — resolve by confidence
    if sharp_conf > trap_conf:
        return {
            "winner": "sharpness",
            "is_sharp": True,
            "is_trap": False,
            "note": (
                f"Both W1+W5 fired; sharpness confidence ({sharp_conf:.2f}) "
                f"> trap confidence ({trap_conf:.2f}) — treating as sharp line."
            ),
        }
    else:
        return {
            "winner": "trap",
            "is_sharp": False,
            "is_trap": True,
            "note": (
                f"Both W1+W5 fired; trap confidence ({trap_conf:.2f}) "
                f">= sharpness confidence ({sharp_conf:.2f}) — treating as trap (conservative)."
            ),
        }
# ============================================================
# SECTION: Composite Win Score
# A single 0-100 score combining the strongest win-rate signals
# into one number. Higher score = higher expected historical win
# rate.  Designed as a quick sort key for "what should I bet on?"
#
# Weight allocation reflects empirical NBA prop hit-rate drivers:
#   - Probability in-direction (40%): Strongest single predictor
#   - Confidence score (25%): Aggregates edge, matchup, forces
#   - Edge percentage (15%): Raw model edge vs line
#   - Force alignment (10%): How unanimous the directional forces are
#   - Streak/form bonus (5%): Hot hand / cold adjustment
#   - Risk penalty (5%): De-rate risky picks
# ============================================================
# Weights — must sum to 1.0
_CWS_W_PROBABILITY = 0.40
_CWS_W_CONFIDENCE  = 0.25
_CWS_W_EDGE        = 0.15
_CWS_W_FORCES      = 0.10
_CWS_W_STREAK      = 0.05
_CWS_W_RISK        = 0.05
# Edge % is uncapped in theory; we cap at this value for scoring purposes.
# 20% edge is exceptional for NBA props — anything above is near-certain.
_CWS_MAX_EDGE_PCT = 20.0
# Probability ceiling for normalization.  Most NBA props are 50-75%;
# ≥80% is rare and represents extreme value.  Using 80% as ceiling
# ensures a 70% probability scores ~67/100, not ~40/100.
_CWS_PROB_CEILING = 0.80
def calculate_composite_win_score(
    probability_in_direction,
    confidence_score,
    edge_percentage,
    directional_forces_result=None,
    streak_multiplier=1.0,
    risk_score=5.0,
    is_coin_flip=False,
    should_avoid=False,
):
    """
    Calculate a composite 0-100 score predicting how likely a pick is to win.
    Combines the model's strongest signals — probability, confidence, edge,
    force alignment, streak momentum, and risk — into one number so picks
    can be ranked by expected win rate.
    Higher score = stronger expected historical hit rate.
    Args:
        probability_in_direction (float): Simulation probability in the
            recommended direction (0.0-1.0). This is the single strongest
            predictor of hit rate.
        confidence_score (float): SAFE score (0-100) from
            calculate_confidence_score(). Captures edge quality, matchup,
            and multi-factor agreement.
        edge_percentage (float): Raw model edge vs line (e.g. 12.5 for
            12.5%). Captures how far the projection is from the line.
        directional_forces_result (dict or None): Output of
            analyze_directional_forces(). Used to compute force alignment
            (how one-sided the signals are).
        streak_multiplier (float): 1.05 for hot, 0.95 for cold, 1.0 for
            neutral. From detect_streak().
        risk_score (float): 1-10 risk rating from calculate_risk_score().
            Lower = safer. Inverted for scoring.
        is_coin_flip (bool): True if detect_coin_flip() flagged this prop.
            If True, the score is capped at 25.
        should_avoid (bool): True if the avoid-list flagged this prop.
            If True, the score is capped at 15.
    Returns:
        dict: {
            'composite_win_score': float (0-100),
            'grade': str ('A+', 'A', 'B+', 'B', 'C', 'D', 'F'),
            'grade_label': str (e.g. 'Strong Play', 'Solid Play'),
            'components': dict of individual sub-scores,
        }
    Example:
        probability=0.68, confidence=78, edge=12.5, hot streak
        → composite_win_score ≈ 74 (grade B+)
    """
    # ── Normalize each component to 0-100 ─────────────────────────
    # 1. Probability sub-score (40%): 0.50 → 0, _CWS_PROB_CEILING → 100
    prob_val = max(0.0, min(1.0, float(probability_in_direction)))
    prob_range = _CWS_PROB_CEILING - 0.50
    prob_score = min(100.0, max(0.0, (prob_val - 0.50) / prob_range) * 100.0)
    # 2. Confidence sub-score (25%): pass-through (already 0-100)
    conf_score = max(0.0, min(100.0, float(confidence_score)))
    # 3. Edge sub-score (15%): 0% → 0, _CWS_MAX_EDGE_PCT% → 100
    abs_edge = min(abs(float(edge_percentage)), _CWS_MAX_EDGE_PCT)
    edge_score = (abs_edge / _CWS_MAX_EDGE_PCT) * 100.0
    # 4. Force alignment sub-score (10%): measures unanimity of forces
    force_score = 50.0  # default neutral
    if directional_forces_result:
        over_count  = len(directional_forces_result.get("over_forces", []))
        under_count = len(directional_forces_result.get("under_forces", []))
        total = over_count + under_count
        if total > 0:
            dominant = max(over_count, under_count)
            # Alignment ratio: 1.0 = all forces agree, 0.5 = equal split
            alignment = dominant / total
            # Scale: 0.5 → 0, 1.0 → 100
            force_score = max(0.0, (alignment - 0.5) / 0.5) * 100.0
            # Bonus: more total forces = more evidence
            evidence_bonus = min(20.0, total * 2.0)
            force_score = min(100.0, force_score + evidence_bonus)
    # 5. Streak sub-score (5%): hot=80, neutral=50, cold=20
    streak_val = float(streak_multiplier)
    if streak_val >= 1.03:
        streak_score = 80.0
    elif streak_val <= 0.97:
        streak_score = 20.0
    else:
        streak_score = 50.0
    # 6. Risk sub-score (5%): risk_score 1→100, 10→0 (inverted)
    risk_val = max(1.0, min(10.0, float(risk_score)))
    risk_sub_score = ((10.0 - risk_val) / 9.0) * 100.0
    # ── Weighted combination ──────────────────────────────────────
    raw_score = (
        prob_score   * _CWS_W_PROBABILITY
        + conf_score * _CWS_W_CONFIDENCE
        + edge_score * _CWS_W_EDGE
        + force_score * _CWS_W_FORCES
        + streak_score * _CWS_W_STREAK
        + risk_sub_score * _CWS_W_RISK
    )
    # ── Hard caps for problematic picks ───────────────────────────
    if should_avoid:
        raw_score = min(raw_score, 15.0)
    elif is_coin_flip:
        raw_score = min(raw_score, 25.0)
    composite = _safe_float(round(max(0.0, min(100.0, raw_score)), 1), 0.0)
    # ── Letter grade ──────────────────────────────────────────────
    if composite >= 85:
        grade, label = "A+", "Elite Play"
    elif composite >= 75:
        grade, label = "A", "Strong Play"
    elif composite >= 65:
        grade, label = "B+", "Solid Play"
    elif composite >= 55:
        grade, label = "B", "Decent Play"
    elif composite >= 45:
        grade, label = "C", "Marginal"
    elif composite >= 30:
        grade, label = "D", "Weak"
    else:
        grade, label = "F", "Avoid"
    return {
        "composite_win_score": composite,
        "grade": grade,
        "grade_label": label,
        "components": {
            "probability_score": _safe_float(round(prob_score, 1), 0.0),
            "confidence_score": _safe_float(round(conf_score, 1), 0.0),
            "edge_score": _safe_float(round(edge_score, 1), 0.0),
            "force_alignment_score": _safe_float(round(force_score, 1), 0.0),
            "streak_score": _safe_float(round(streak_score, 1), 0.0),
            "risk_score": _safe_float(round(risk_sub_score, 1), 0.0),
        },
    }
# ============================================================
# END SECTION: Composite Win Score
# ============================================================
# ============================================================
# SECTION: Cross-Platform Edge Detection
# ============================================================
# DFS-specific breakeven probabilities per platform.
# PrizePicks & Underdog have NO per-leg vig — the cost is baked
# into the payout structure.  Traditional sportsbooks charge ~4.5%
# vig via -110 lines (breakeven 52.38%).
_PLATFORM_BREAKEVEN: dict[str, float] = {
    "PrizePicks": 0.50,          # No per-leg vig; 50% breakeven
    "PrizePicks Power": 0.50,    # All-or-nothing; same per-leg breakeven
    "Underdog Fantasy": 0.50,    # No per-leg vig; 50% breakeven
    "DraftKings": 0.5238,        # Standard -110 juice
    "FanDuel": 0.5238,
    "BetMGM": 0.5238,
    "Caesars": 0.5238,
    "PointsBet": 0.5238,
    "DraftKings Pick6": 0.50,    # DFS-style; 50% breakeven
}


def detect_cross_platform_edge(
    platform_lines,
    model_projection,
    simulation_result,
    stat_type=None,
):
    """Detect mispricing opportunities across all betting platforms.

    Given a dict of ``{platform: line}`` and the model's projection +
    simulation output, this function evaluates each platform independently
    and identifies which ones offer the best (and worst) edges.

    Args:
        platform_lines (dict): ``{platform_name: prop_line}`` from all
            available sources (sportsbooks + DFS platforms).
        model_projection (float): Our model's projected value for the stat.
        simulation_result (dict): Output from ``run_enhanced_simulation``,
            must include ``probability_over`` and optionally
            ``simulated_results``.
        stat_type (str or None): Stat type for threshold lookups.

    Returns:
        dict: {
            'platforms': [
                {
                    'platform': str,
                    'line': float,
                    'direction': 'OVER' | 'UNDER',
                    'model_probability': float,
                    'breakeven': float,
                    'edge_pct': float,
                    'has_edge': bool,
                    'edge_rating': str,   # 'Strong' | 'Moderate' | 'Weak' | 'None'
                },
                ...
            ],
            'best_platform': str | None,
            'best_edge_pct': float,
            'worst_platform': str | None,
            'consensus_line': float | None,
            'line_spread': float,          # max(line) - min(line) across platforms
            'mispricing_detected': bool,
        }
    """
    if not platform_lines:
        return {
            "platforms": [],
            "best_platform": None,
            "best_edge_pct": 0.0,
            "worst_platform": None,
            "consensus_line": None,
            "line_spread": 0.0,
            "mispricing_detected": False,
        }

    prob_over_raw = float(simulation_result.get("probability_over", 0.5))
    sim_results_array = simulation_result.get("simulated_results", [])

    all_lines = [float(v) for v in platform_lines.values() if v is not None]
    consensus = sum(all_lines) / len(all_lines) if all_lines else None
    line_spread = max(all_lines) - min(all_lines) if len(all_lines) >= 2 else 0.0

    platform_results = []
    best_plat = None
    best_edge = -999.0
    worst_plat = None
    worst_edge = 999.0

    for plat_name, plat_line in platform_lines.items():
        if plat_line is None or float(plat_line) <= 0:
            continue
        plat_line = float(plat_line)

        # Recompute probability for THIS platform's specific line
        if sim_results_array:
            import numpy as np
            arr = np.array(sim_results_array, dtype=float)
            p_over = float(np.mean(arr > plat_line))
        else:
            p_over = prob_over_raw

        direction = "OVER" if p_over >= 0.5 else "UNDER"
        model_prob = p_over if direction == "OVER" else (1.0 - p_over)

        # Platform-specific breakeven
        breakeven = _PLATFORM_BREAKEVEN.get(plat_name, 0.5238)
        edge_pct = round((model_prob - breakeven) * 100.0, 2)

        # Edge rating thresholds (as multiples of the per-stat minimum edge)
        _STRONG_EDGE_MULTIPLIER = 2.0   # 2× the stat's minimum edge = "Strong"
        _MODERATE_EDGE_MULTIPLIER = 1.0  # 1× the stat's minimum edge = "Moderate"
        min_edge = STAT_EDGE_THRESHOLDS.get(stat_type, 3.0) if stat_type else 3.0
        if edge_pct >= min_edge * _STRONG_EDGE_MULTIPLIER:
            edge_rating = "Strong"
        elif edge_pct >= min_edge * _MODERATE_EDGE_MULTIPLIER:
            edge_rating = "Moderate"
        elif edge_pct > 0:
            edge_rating = "Weak"
        else:
            edge_rating = "None"

        entry = {
            "platform": plat_name,
            "line": plat_line,
            "direction": direction,
            "model_probability": round(model_prob, 4),
            "breakeven": round(breakeven, 4),
            "edge_pct": edge_pct,
            "has_edge": edge_pct > 0,
            "edge_rating": edge_rating,
        }
        platform_results.append(entry)

        if edge_pct > best_edge:
            best_edge = edge_pct
            best_plat = plat_name
        if edge_pct < worst_edge:
            worst_edge = edge_pct
            worst_plat = plat_name

    # Sort by edge descending (best platform first)
    platform_results.sort(key=lambda p: p["edge_pct"], reverse=True)

    mispricing = line_spread >= 1.0 and best_edge > 0 and worst_edge <= 0

    return {
        "platforms": platform_results,
        "best_platform": best_plat,
        "best_edge_pct": _safe_float(best_edge, 0.0),
        "worst_platform": worst_plat,
        "consensus_line": _safe_float(round(consensus, 2), 0.0) if consensus else None,
        "line_spread": _safe_float(round(line_spread, 2), 0.0),
        "mispricing_detected": mispricing,
    }
# ============================================================
# END SECTION: Cross-Platform Edge Detection
# ============================================================
