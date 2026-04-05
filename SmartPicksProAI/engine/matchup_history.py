# ============================================================
# FILE: engine/matchup_history.py
# PURPOSE: Player-vs-Team Matchup History Engine
#          Tracks how a specific player has historically performed
#          AGAINST a specific team. Player-vs-team history is one
#          of the strongest predictors of prop bet outcomes, as
#          some players have persistent matchup advantages or
#          disadvantages vs specific teams.
#
# CONNECTS TO: engine/projections.py (matchup adjustment factor),
#              engine/edge_detection.py (Force 9: Historical Matchup)
# CONCEPTS COVERED: Historical matchup analysis, sample-size
#                   weighting, matchup favorability scoring
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

# Minimum number of historical games against the opponent before
# we trust the matchup adjustment (otherwise return neutral 1.0).
MIN_GAMES_FOR_MATCHUP = 5       # < 5 games vs opponent → neutral adjustment

# Maximum adjustment cap: never adjust more than ±20% from baseline
MAX_MATCHUP_ADJUSTMENT = 0.20   # 20% max up or down

# Weights for blending historical vs season average in favorability
MATCHUP_HISTORY_WEIGHT = 0.60   # 60% of the adjustment from matchup history
SEASON_AVG_WEIGHT = 0.40        # 40% from season average (regression to mean)

# Scale factor for ratio-based favorability score.
# Maps avg_vs_team / season_average ratio to 0-100 scale:
#   ratio = 1.0  → raw_score = 50 (neutral)
#   ratio = 1.2  → raw_score = 80 (+20% above avg → strongly favourable)
#   ratio = 0.8  → raw_score = 20 (-20% below avg → strongly unfavourable)
FAVORABILITY_RATIO_SCALE = 150.0

# ============================================================
# END SECTION: Module-Level Constants
# ============================================================


# ============================================================
# SECTION: Internal Helpers
# ============================================================

def _extract_opponent(game_log_row):
    """
    Return the opponent string from a game log row dict.

    Checks common field names in priority order so callers don't need to
    normalise their data before passing it in.

    Args:
        game_log_row (dict): A single game log entry.

    Returns:
        str or None: Opponent team string, or None if not found.
    """
    for key in ("opp", "opponent", "matchup", "vs_team"):
        val = game_log_row.get(key)
        if val:
            return str(val).strip().upper()
    return None


def _extract_stat_value(game_log_row, stat_type):
    """
    Return the numeric stat value from a game log row for the requested stat.

    Checks the raw stat_type key first, then common aliases, so callers can
    pass logs from different data sources without pre-processing.

    Args:
        game_log_row (dict): A single game log entry.
        stat_type (str): Requested stat category (e.g., 'points', 'threes').

    Returns:
        float or None: Stat value, or None if not found / not numeric.
    """
    # Alias map: canonical stat name → list of dict keys to try, in order
    alias_map = {
        "points":              [stat_type, "pts",  "points"],
        "rebounds":            [stat_type, "reb",  "rebounds", "total_reb"],
        "assists":             [stat_type, "ast",  "assists"],
        "threes":              [stat_type, "fg3m", "3pm",  "threes", "three_pointers"],
        "steals":              [stat_type, "stl",  "steals"],
        "blocks":              [stat_type, "blk",  "blocks"],
        "turnovers":           [stat_type, "tov",  "turnovers", "to"],
        "ftm":                 [stat_type, "ftm",  "free_throws_made"],
        "fta":                 [stat_type, "fta",  "free_throws_attempted"],
        "fgm":                 [stat_type, "fgm",  "field_goals_made"],
        "fga":                 [stat_type, "fga",  "field_goals_attempted"],
        "minutes":             [stat_type, "min",  "minutes"],
        "offensive_rebounds":  [stat_type, "oreb", "offensive_rebounds"],
        "defensive_rebounds":  [stat_type, "dreb", "defensive_rebounds"],
        "personal_fouls":      [stat_type, "pf",   "personal_fouls"],
    }

    keys_to_try = alias_map.get(stat_type, [stat_type, stat_type + "_avg"])

    for key in keys_to_try:
        val = game_log_row.get(key)
        if val is not None:
            try:
                return float(val)
            except (TypeError, ValueError):
                continue
    return None


def _std_dev(values):
    """
    Compute population standard deviation for a list of floats.

    Args:
        values (list of float): Numeric values.

    Returns:
        float: Standard deviation, or 0.0 if fewer than 2 values.
    """
    n = len(values)
    if n < 2:
        return 0.0
    mean = sum(values) / n
    variance = sum((v - mean) ** 2 for v in values) / n
    return math.sqrt(variance)

# ============================================================
# END SECTION: Internal Helpers
# ============================================================


# ============================================================
# SECTION: Matchup History Analysis
# ============================================================

def get_player_vs_team_history(player_name, opponent_team, stat_type, game_logs, season_average=None):
    """
    Filter a player's game logs to games vs a specific opponent and compute
    matchup stats for the requested stat type.

    Opponent field discovery: checks 'opp', 'opponent', 'matchup', 'vs_team'.
    Stat value discovery: checks stat_type key first, then common aliases
    (pts / reb / ast / fg3m+3pm / stl / blk / tov).

    Args:
        player_name (str): Display name of the player (used in output only).
        opponent_team (str): Opponent team abbreviation to match, e.g. 'BOS'.
        stat_type (str): Stat category to analyse, e.g. 'points', 'threes'.
        game_logs (list of dict): Full season / career game log rows.

    Returns:
        dict: {
            'player_name': str,
            'opponent_team': str,
            'stat_type': str,
            'games_found': int,
            'avg_vs_team': float or None,
            'std_vs_team': float,
            'sample_size': int,
            'matchup_favorability_score': float (0-100),
            'cold_start': bool,
        }

    Example:
        logs = [{'opp': 'BOS', 'pts': 28}, {'opp': 'BOS', 'pts': 32}, ...]
        result = get_player_vs_team_history('LeBron James', 'BOS', 'points', logs)
        # result['avg_vs_team'] → 30.0
    """
    opp_upper = opponent_team.strip().upper()

    # Filter logs to games played against the target opponent
    matchup_logs = [
        row for row in game_logs
        if _extract_opponent(row) == opp_upper
    ]

    games_found = len(matchup_logs)

    # Cold-start: not enough games to trust the matchup signal
    if games_found < MIN_GAMES_FOR_MATCHUP:
        return {
            "player_name":               player_name,
            "opponent_team":             opponent_team,
            "stat_type":                 stat_type,
            "games_found":               int(games_found),
            "avg_vs_team":               None,
            "std_vs_team":               _safe_float(0.0, 0.0),
            "sample_size":               int(games_found),
            "matchup_favorability_score": _safe_float(50.0, 50.0),
            "cold_start":                True,
        }

    # Extract stat values from filtered logs
    stat_values = [
        v for row in matchup_logs
        for v in [_extract_stat_value(row, stat_type)]
        if v is not None
    ]

    if not stat_values:
        return {
            "player_name":               player_name,
            "opponent_team":             opponent_team,
            "stat_type":                 stat_type,
            "games_found":               int(games_found),
            "avg_vs_team":               None,
            "std_vs_team":               _safe_float(0.0, 0.0),
            "sample_size":               0,
            "matchup_favorability_score": _safe_float(50.0, 50.0),
            "cold_start":                True,
        }

    avg_vs_team = sum(stat_values) / len(stat_values)
    std_vs_team = _std_dev(stat_values)

    # ── Matchup favorability score (0-100) ───────────────────────────────
    # 50 = neutral. Higher = player tends to outperform vs this team.
    # Score is driven by the ratio of avg_vs_team / season_average when
    # the season average is provided, otherwise falls back to the
    # coefficient-of-variation stability approach.

    # Sample-size confidence: asymptotes toward 1.0 with more games
    sample_confidence = 1.0 - (1.0 / (1.0 + (games_found - MIN_GAMES_FOR_MATCHUP) * 0.3))

    # Matchup favorability driven by avg_vs_team / season_average ratio
    if season_average and season_average > 0 and avg_vs_team is not None:
        ratio = avg_vs_team / season_average
        # Map ratio to 0-100 scale using FAVORABILITY_RATIO_SCALE:
        #   ratio=1.0 → 50 (neutral), ratio=1.2 → 80+, ratio=0.8 → 20-
        raw_score = 50.0 + (ratio - 1.0) * FAVORABILITY_RATIO_SCALE
        # Regress toward 50 when sample size is small
        matchup_favorability_score = round(50.0 + (raw_score - 50.0) * sample_confidence, 1)
        matchup_favorability_score = max(0.0, min(100.0, matchup_favorability_score))
    else:
        # No season average provided — fall back to old stability-based approach
        if std_vs_team > 0:
            cv = std_vs_team / avg_vs_team if avg_vs_team != 0 else 1.0
            stability = max(0.0, 1.0 - cv)
        else:
            stability = 1.0
        matchup_favorability_score = round(50.0 + stability * sample_confidence * 25.0, 1)
        matchup_favorability_score = max(0.0, min(100.0, matchup_favorability_score))

    return {
        "player_name":               player_name,
        "opponent_team":             opponent_team,
        "stat_type":                 stat_type,
        "games_found":               int(games_found),
        "avg_vs_team":               _safe_float(round(avg_vs_team, 3), 0.0) if avg_vs_team is not None else None,
        "std_vs_team":               _safe_float(round(std_vs_team, 3), 0.0),
        "sample_size":               int(len(stat_values)),
        "matchup_favorability_score": _safe_float(matchup_favorability_score, 50.0),
        "cold_start":                False,
    }


def calculate_matchup_adjustment(
    player_name,
    opponent_team,
    stat_type,
    game_logs,
    season_average,
):
    """
    Return a multiplicative projection adjustment based on player-vs-team
    historical performance.

    The raw ratio (avg_vs_team / season_average) is blended with 1.0
    (neutral) using MATCHUP_HISTORY_WEIGHT / SEASON_AVG_WEIGHT to prevent
    over-fitting on small samples. The result is capped to
    ±MAX_MATCHUP_ADJUSTMENT of 1.0.

    If fewer than MIN_GAMES_FOR_MATCHUP games exist vs this opponent, or
    season_average is zero, returns 1.0 (neutral — no adjustment).

    Args:
        player_name (str): Player name (used for logging / debug).
        opponent_team (str): Opponent team abbreviation, e.g. 'BOS'.
        stat_type (str): Stat category, e.g. 'points', 'rebounds'.
        game_logs (list of dict): Player's historical game log rows.
        season_average (float): Player's current season average for stat_type.

    Returns:
        float: Multiplicative adjustment factor.
            1.0  = neutral (no adjustment)
            1.08 = 8% boost  (player historically outperforms vs this team)
            0.93 = 7% penalty (player historically underperforms vs this team)

    Example:
        factor = calculate_matchup_adjustment(
            'Jayson Tatum', 'MIA', 'points', logs, season_avg=26.5
        )
        # factor → 1.08 if Tatum averages 28.6 pts vs Miami historically
    """
    if not season_average or season_average == 0.0:
        return 1.0

    history = get_player_vs_team_history(
        player_name, opponent_team, stat_type, game_logs, season_average=season_average
    )

    # Cold start or no usable stat values → neutral
    if history.get("cold_start") or history.get("avg_vs_team") is None:
        return 1.0

    avg_vs_team = history["avg_vs_team"]

    # Raw ratio: how does the player's avg vs this team compare to season avg?
    raw_ratio = avg_vs_team / season_average

    # Blend toward 1.0 (regression to the mean) using the configured weights
    # adjusted_ratio = (raw_ratio × history_weight) + (1.0 × season_weight)
    adjusted_ratio = (raw_ratio * MATCHUP_HISTORY_WEIGHT) + (1.0 * SEASON_AVG_WEIGHT)

    # Cap the adjustment within [1 - MAX, 1 + MAX]
    lower_cap = 1.0 - MAX_MATCHUP_ADJUSTMENT
    upper_cap = 1.0 + MAX_MATCHUP_ADJUSTMENT
    adjusted_ratio = max(lower_cap, min(upper_cap, adjusted_ratio))

    return round(adjusted_ratio, 4)


def get_matchup_force_signal(adjustment_factor):
    """
    Convert a numeric matchup adjustment factor to a human-readable
    directional force signal suitable for the edge detection engine.

    Signal thresholds:
        factor > 1.05  → OVER  (player favoured vs this team)
        factor < 0.95  → UNDER (player disadvantaged vs this team)
        otherwise      → NEUTRAL

    Strength is normalised to [0.0, 1.0]:
        OVER:  min((factor - 1.0) × 5, 1.0)
        UNDER: min((1.0 - factor) × 5, 1.0)
        NEUTRAL: 0.0

    Args:
        adjustment_factor (float): Output from calculate_matchup_adjustment().

    Returns:
        dict: {
            'direction': 'OVER' | 'UNDER' | 'NEUTRAL',
            'strength': float (0.0 – 1.0),
            'label': str,
        }

    Example:
        signal = get_matchup_force_signal(1.12)
        # {'direction': 'OVER', 'strength': 0.6, 'label': 'Moderate OVER matchup edge'}
    """
    if adjustment_factor > 1.05:
        strength = min((adjustment_factor - 1.0) * 5.0, 1.0)
        if strength >= 0.7:
            label = "Strong OVER matchup edge"
        elif strength >= 0.4:
            label = "Moderate OVER matchup edge"
        else:
            label = "Slight OVER matchup edge"
        return {"direction": "OVER", "strength": _safe_float(round(strength, 3), 0.0), "label": label}

    if adjustment_factor < 0.95:
        strength = min((1.0 - adjustment_factor) * 5.0, 1.0)
        if strength >= 0.7:
            label = "Strong UNDER matchup edge"
        elif strength >= 0.4:
            label = "Moderate UNDER matchup edge"
        else:
            label = "Slight UNDER matchup edge"
        return {"direction": "UNDER", "strength": _safe_float(round(strength, 3), 0.0), "label": label}

    return {"direction": "NEUTRAL", "strength": _safe_float(0.0, 0.0), "label": "No significant matchup edge"}

# ============================================================
# END SECTION: Matchup History Analysis
# ============================================================
