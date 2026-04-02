# ============================================================
# FILE: engine/platform_line_compare.py
# PURPOSE: Platform Line Comparison Engine
#          When the same prop is available on multiple platforms
#          (PrizePicks, Underdog Fantasy, DraftKings Pick6), lines may differ.
#          This module finds the platform with the BEST line for
#          our recommended direction, maximizing edge via line
#          shopping — a free source of EV with zero model risk.
#
# CONNECTS TO: engine/entry_optimizer.py (EV calculation),
#              pages/6_🧬_Entry_Builder.py (display annotations)
# CONCEPTS COVERED: Line shopping, market inefficiency, EV from
#                   line differences, cross-platform arbitrage
# ============================================================

import math


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

# Minimum line difference to flag as meaningful (noise filter)
MIN_MEANINGFUL_LINE_DIFF = 0.25   # Lines within 0.25 of each other = same line

# Typical stat standard deviations used for edge bonus calculation
# when the actual std is not provided. These are conservative estimates.
DEFAULT_STAT_STD = {
    "points":    6.5,
    "rebounds":  2.8,
    "assists":   2.5,
    "threes":    1.3,
    "steals":    0.8,
    "blocks":    0.7,
    "turnovers": 1.2,
}
DEFAULT_STD_FALLBACK = 4.0   # Used when stat_type not in DEFAULT_STAT_STD

# ============================================================
# END SECTION: Module-Level Constants
# ============================================================


# ============================================================
# SECTION: Internal Helpers
# ============================================================

def _normal_cdf(x):
    """
    Approximation of the standard normal cumulative distribution function.

    Uses the Abramowitz & Stegun approximation (maximum error < 7.5e-8),
    which is accurate enough for edge bonus calculations and requires only
    the standard library.

    Args:
        x (float): Standard normal quantile.

    Returns:
        float: Probability P(Z ≤ x) for Z ~ N(0, 1).
    """
    # Symmetry: CDF(x) = 1 - CDF(-x) for x < 0
    sign = 1.0 if x >= 0 else -1.0
    x = abs(x)

    # Abramowitz & Stegun polynomial approximation (formula 26.2.17)
    t = 1.0 / (1.0 + 0.2316419 * x)
    poly = t * (0.319381530
                + t * (-0.356563782
                       + t * (1.781477937
                              + t * (-1.821255978
                                     + t * 1.330274429))))
    approx = 1.0 - (1.0 / math.sqrt(2.0 * math.pi)) * math.exp(-0.5 * x * x) * poly

    if sign == 1.0:
        return approx
    return 1.0 - approx

# ============================================================
# END SECTION: Internal Helpers
# ============================================================


# ============================================================
# SECTION: Line Comparison & Edge Calculation
# ============================================================

def compare_platform_lines(player_name, stat_type, direction, platform_lines):
    """
    Find the best platform line for a given recommendation direction and
    quantify the edge bonus from line shopping across platforms.

    For OVER picks: a lower line is more favourable (easier threshold to
    exceed). For UNDER picks: a higher line is more favourable.

    The edge_bonus_pct approximates the probability gain (in percentage
    points) from betting the best line vs the worst line, using a
    normal-distribution approximation:

        edge_bonus_pct = (line_spread / stat_std) × 40.0   (capped at 15%)

    Args:
        player_name (str): Player name (used in output for context).
        stat_type (str): Stat category, e.g. 'points', 'threes'.
        direction (str): 'OVER' or 'UNDER'.
        platform_lines (dict): {platform_name: line_value}, e.g.
            {'FanDuel': 24.5, 'DraftKings': 25.0, 'BetMGM': 24.0}

    Returns:
        dict: {
            'player_name': str,
            'stat_type': str,
            'direction': str,
            'best_platform': str,
            'best_line': float,
            'worst_line': float,
            'line_spread': float,
            'edge_bonus_pct': float,
            'all_platforms': list of {'platform': str, 'line': float, 'rank': int},
            'has_meaningful_spread': bool,
        }

    Example:
        result = compare_platform_lines(
            'LeBron James', 'points', 'OVER',
            {'FanDuel': 24.5, 'DraftKings': 25.0}
        )
        # result['best_platform'] → 'FanDuel' (lower line = easier OVER)
        # result['edge_bonus_pct'] → ~3.1
    """
    if not platform_lines:
        return {
            "player_name":           player_name,
            "stat_type":             stat_type,
            "direction":             direction,
            "best_platform":         None,
            "best_line":             None,
            "worst_line":            None,
            "line_spread":           _safe_float(0.0, 0.0),
            "edge_bonus_pct":        _safe_float(0.0, 0.0),
            "all_platforms":         [],
            "has_meaningful_spread": False,
        }

    # Sort platforms by line favourability for the given direction
    # OVER → ascending (lowest line first = best)
    # UNDER → descending (highest line first = best)
    reverse_sort = (direction.upper() == "UNDER")
    sorted_platforms = sorted(
        platform_lines.items(),
        key=lambda kv: kv[1],
        reverse=reverse_sort,
    )

    best_platform, best_line = sorted_platforms[0]
    worst_platform, worst_line = sorted_platforms[-1]

    # line_spread is always a non-negative magnitude
    line_spread = round(abs(best_line - worst_line), 4)

    stat_std = DEFAULT_STAT_STD.get(stat_type.lower(), DEFAULT_STD_FALLBACK)
    if stat_std <= 0:
        stat_std = DEFAULT_STD_FALLBACK
    raw_edge = (line_spread / stat_std) * 40.0
    edge_bonus_pct = round(min(raw_edge, 15.0), 2)

    all_platforms = [
        {"platform": name, "line": line, "rank": rank + 1}
        for rank, (name, line) in enumerate(sorted_platforms)
    ]

    return {
        "player_name":           player_name,
        "stat_type":             stat_type,
        "direction":             direction,
        "best_platform":         best_platform,
        "best_line":             _safe_float(best_line, 0.0) if best_line is not None else None,
        "worst_line":            _safe_float(worst_line, 0.0) if worst_line is not None else None,
        "line_spread":           _safe_float(line_spread, 0.0),
        "edge_bonus_pct":        _safe_float(edge_bonus_pct, 0.0),
        "all_platforms":         all_platforms,
        "has_meaningful_spread": line_spread >= MIN_MEANINGFUL_LINE_DIFF,
    }


def calculate_line_shopping_value(
    platform_lines,
    model_projection,
    stat_std,
    direction,
    entry_fee=10.0,
):
    """
    Quantify how much dollar EV is gained by picking the best platform line
    vs the worst platform line, using a normal-CDF probability model.

    Approximation assumes a single-pick entry at ~3× payout odds so that:
        ev_gain = prob_improvement × 3.0 × entry_fee

    Args:
        platform_lines (dict): {platform_name: line_value}
        model_projection (float): Model's projected stat value (the mean of
            the assumed normal distribution).
        stat_std (float): Standard deviation of the stat (use DEFAULT_STAT_STD
            values if not known precisely).
        direction (str): 'OVER' or 'UNDER'.
        entry_fee (float): Entry fee in dollars. Default 10.0.

    Returns:
        dict: {
            'best_platform': str or None,
            'worst_platform': str or None,
            'best_line': float or None,
            'worst_line': float or None,
            'prob_improvement': float,   # probability gain from line shopping
            'ev_gain_dollars': float,    # expected dollar gain per entry
        }

    Example:
        value = calculate_line_shopping_value(
            {'FanDuel': 24.5, 'DraftKings': 25.5},
            model_projection=26.0,
            stat_std=6.5,
            direction='OVER',
            entry_fee=10.0,
        )
        # value['ev_gain_dollars'] → ~0.46
    """
    empty_result = {
        "best_platform":   None,
        "worst_platform":  None,
        "best_line":       None,
        "worst_line":      None,
        "prob_improvement": _safe_float(0.0, 0.0),
        "ev_gain_dollars": _safe_float(0.0, 0.0),
    }

    if not platform_lines or stat_std <= 0:
        return empty_result

    direction_upper = direction.upper()

    # Best line: lowest for OVER, highest for UNDER
    if direction_upper == "OVER":
        best_platform  = min(platform_lines, key=platform_lines.get)
        worst_platform = max(platform_lines, key=platform_lines.get)
    else:
        best_platform  = max(platform_lines, key=platform_lines.get)
        worst_platform = min(platform_lines, key=platform_lines.get)

    best_line  = platform_lines[best_platform]
    worst_line = platform_lines[worst_platform]

    # Compute P(over/under) at each line using normal CDF
    # Z = (line - projection) / std; P(over) = 1 - CDF(Z)
    if direction_upper == "OVER":
        prob_best  = 1.0 - _normal_cdf((best_line  - model_projection) / stat_std)
        prob_worst = 1.0 - _normal_cdf((worst_line - model_projection) / stat_std)
    else:
        prob_best  = _normal_cdf((best_line  - model_projection) / stat_std)
        prob_worst = _normal_cdf((worst_line - model_projection) / stat_std)

    prob_improvement = round(max(prob_best - prob_worst, 0.0), 4)

    # EV gain on a single-pick ~3× payout entry
    ev_gain_dollars = round(prob_improvement * 3.0 * entry_fee, 4)

    return {
        "best_platform":    best_platform,
        "worst_platform":   worst_platform,
        "best_line":        _safe_float(best_line, 0.0) if best_line is not None else None,
        "worst_line":       _safe_float(worst_line, 0.0) if worst_line is not None else None,
        "prob_improvement": _safe_float(prob_improvement, 0.0),
        "ev_gain_dollars":  _safe_float(ev_gain_dollars, 0.0),
    }


def annotate_picks_with_best_platform(picks, platform_lines_by_pick):
    """
    Annotate a list of pick dicts with best-platform metadata for display in
    the Entry Builder page.

    For each pick, looks up (player_name, stat_type) in platform_lines_by_pick
    and — if data is present — runs compare_platform_lines() to find the
    optimal platform and adds 'best_platform', 'best_line', and
    'line_edge_bonus_pct' keys to the pick dict. Picks without platform data
    are returned unchanged.

    Args:
        picks (list of dict): Analysis result dicts. Each should contain at
            least 'player_name', 'stat_type', and 'direction' keys.
        platform_lines_by_pick (dict): Keyed by (player_name, stat_type)
            tuples. Values are {platform_name: line_value} dicts.
            Example: {('LeBron James', 'points'): {'FanDuel': 24.5, ...}}

    Returns:
        list of dict: Same picks list (mutated in-place) with added keys:
            'best_platform'      – platform name with best line, or None
            'best_line'          – numeric line on that platform, or None
            'line_edge_bonus_pct' – edge bonus percentage, or 0.0

    Example:
        picks = [{'player_name': 'LeBron James', 'stat_type': 'points',
                  'direction': 'OVER', 'confidence': 72}]
        platform_lines = {('LeBron James', 'points'): {'PP': 24.5, 'UD': 25.0}}
        annotated = annotate_picks_with_best_platform(picks, platform_lines)
        # annotated[0]['best_platform'] → 'PP'
    """
    for pick in picks:
        player = pick.get("player_name")
        stat   = pick.get("stat_type")
        direction = pick.get("direction", "OVER")

        lines_for_pick = platform_lines_by_pick.get((player, stat))
        if not lines_for_pick:
            continue

        comparison = compare_platform_lines(player, stat, direction, lines_for_pick)

        pick["best_platform"]       = comparison.get("best_platform")
        pick["best_line"]           = comparison.get("best_line")
        pick["line_edge_bonus_pct"] = comparison.get("edge_bonus_pct", 0.0)

    return picks

# ============================================================
# END SECTION: Line Comparison & Edge Calculation
# ============================================================
