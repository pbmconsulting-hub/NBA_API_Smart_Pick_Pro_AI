# engine/rotation_tracker.py
# Lineup and rotation tracking for SmartAI-NBA.
# Detects minutes trends, role changes (starter/bench), and
# provides a minutes trend factor for projection adjustments.
# Standard library only — no numpy/scipy/pandas.

import statistics


# Minutes threshold to be considered a starter
STARTER_MINUTES_THRESHOLD = 25.0

# Significant role change = minutes shift of >= this many minutes
ROLE_CHANGE_THRESHOLD = 6.0

# Weight for recent 5-game average vs season average
RECENT_WEIGHT = 0.60
SEASON_WEIGHT = 0.40


def track_minutes_trend(player_game_logs, window=5):
    """
    Compute rolling average minutes over last N games vs season average.

    Args:
        player_game_logs (list of dict): Game logs with 'MIN' or 'minutes' field
        window (int): Number of recent games to use for rolling average. Default 5.

    Returns:
        dict: {
            'season_avg_minutes': float,
            'recent_avg_minutes': float,
            'trend_direction': str,  # 'up', 'down', 'stable'
            'trend_magnitude': float,  # absolute minutes change
            'games_analyzed': int,
        }
    """
    if not player_game_logs:
        return {
            "season_avg_minutes": 0.0,
            "recent_avg_minutes": 0.0,
            "trend_direction": "stable",
            "trend_magnitude": 0.0,
            "games_analyzed": 0,
        }

    def _parse_min(val):
        """Parse minutes value which may be '32:15' or 32.5 or '32'."""
        if val is None:
            return None
        try:
            s = str(val).strip()
            if ":" in s:
                parts = s.split(":")
                return float(parts[0]) + float(parts[1]) / 60.0
            return float(s)
        except (ValueError, TypeError):
            return None

    all_minutes = []
    for g in player_game_logs:
        m = _parse_min(g.get("MIN") or g.get("minutes") or g.get("min"))
        if m is not None and m > 0:
            all_minutes.append(m)

    if not all_minutes:
        return {
            "season_avg_minutes": 0.0,
            "recent_avg_minutes": 0.0,
            "trend_direction": "stable",
            "trend_magnitude": 0.0,
            "games_analyzed": 0,
        }

    season_avg = sum(all_minutes) / len(all_minutes)
    recent = all_minutes[-window:] if len(all_minutes) >= window else all_minutes
    recent_avg = sum(recent) / len(recent)

    trend_magnitude = recent_avg - season_avg

    if trend_magnitude > 2.0:
        trend_direction = "up"
    elif trend_magnitude < -2.0:
        trend_direction = "down"
    else:
        trend_direction = "stable"

    return {
        "season_avg_minutes": round(season_avg, 2),
        "recent_avg_minutes": round(recent_avg, 2),
        "trend_direction": trend_direction,
        "trend_magnitude": round(trend_magnitude, 2),
        "games_analyzed": len(all_minutes),
    }


def detect_role_change(player_game_logs):
    """
    Detect if a player recently moved from bench to starter or vice versa.

    Compares last 5 games average minutes to the prior 5-15 games average.

    Args:
        player_game_logs (list of dict): Sorted game logs (oldest first)

    Returns:
        dict: {
            'role_change_detected': bool,
            'change_type': str,  # 'bench_to_starter', 'starter_to_bench', 'none'
            'minutes_before': float,
            'minutes_after': float,
            'change_magnitude': float,
        }
    """
    def _parse_min(val):
        if val is None:
            return None
        try:
            s = str(val).strip()
            if ":" in s:
                parts = s.split(":")
                return float(parts[0]) + float(parts[1]) / 60.0
            return float(s)
        except (ValueError, TypeError):
            return None

    all_minutes = []
    for g in player_game_logs:
        m = _parse_min(g.get("MIN") or g.get("minutes") or g.get("min"))
        if m is not None and m >= 0:
            all_minutes.append(m)

    if len(all_minutes) < 8:
        return {
            "role_change_detected": False,
            "change_type": "none",
            "minutes_before": 0.0,
            "minutes_after": 0.0,
            "change_magnitude": 0.0,
        }

    recent = all_minutes[-5:]
    prior = all_minutes[-15:-5] if len(all_minutes) >= 15 else all_minutes[:-5]

    if not prior:
        return {
            "role_change_detected": False,
            "change_type": "none",
            "minutes_before": 0.0,
            "minutes_after": 0.0,
            "change_magnitude": 0.0,
        }

    avg_recent = sum(recent) / len(recent)
    avg_prior = sum(prior) / len(prior)
    change = avg_recent - avg_prior

    if abs(change) < ROLE_CHANGE_THRESHOLD:
        return {
            "role_change_detected": False,
            "change_type": "none",
            "minutes_before": round(avg_prior, 2),
            "minutes_after": round(avg_recent, 2),
            "change_magnitude": round(change, 2),
        }

    if change > 0:
        change_type = "bench_to_starter"
    else:
        change_type = "starter_to_bench"

    return {
        "role_change_detected": True,
        "change_type": change_type,
        "minutes_before": round(avg_prior, 2),
        "minutes_after": round(avg_recent, 2),
        "change_magnitude": round(change, 2),
    }


def get_minutes_trend_factor(player_name, game_logs):
    """
    Return a minutes trend multiplier for use in projections.

    A player trending up in minutes gets a factor > 1.0.
    A player trending down gets a factor < 1.0.

    Args:
        player_name (str): Player's name (used for logging)
        game_logs (list of dict): Game logs for this player

    Returns:
        float: Multiplier (e.g. 1.05 = trending up, 0.92 = trending down, 1.0 = stable)
    """
    if not game_logs:
        return 1.0

    trend = track_minutes_trend(game_logs)
    season_avg = trend["season_avg_minutes"]

    if season_avg <= 0:
        return 1.0

    # Blend recent and season averages
    recent_avg = trend["recent_avg_minutes"]
    blended = RECENT_WEIGHT * recent_avg + SEASON_WEIGHT * season_avg

    factor = blended / season_avg
    # season_avg > 0 is guaranteed by the check above

    # Cap the factor to a reasonable range
    factor = max(0.70, min(1.30, factor))
    return round(factor, 4)


def is_starter(player_data):
    """
    Determine if a player is a starter based on minutes threshold.

    Args:
        player_data (dict): Player stats with 'minutes_avg' or 'min_avg'

    Returns:
        bool: True if player averages >= STARTER_MINUTES_THRESHOLD minutes
    """
    minutes = (
        player_data.get("minutes_avg")
        or player_data.get("min_avg")
        or player_data.get("MIN", 0)
    )
    try:
        return float(minutes or 0) >= STARTER_MINUTES_THRESHOLD
    except (ValueError, TypeError):
        return False


def get_trending_minutes(player_data, game_logs):
    """
    Get the blended minutes projection combining season average and recent trend.

    Args:
        player_data (dict): Player data with season average minutes
        game_logs (list of dict): Recent game logs

    Returns:
        float: Blended minutes projection
    """
    season_avg = float(
        player_data.get("minutes_avg")
        or player_data.get("min_avg")
        or 28.0
    )

    if not game_logs:
        return season_avg

    trend = track_minutes_trend(game_logs)
    recent_avg = trend["recent_avg_minutes"]

    if recent_avg <= 0:
        return season_avg

    blended = RECENT_WEIGHT * recent_avg + SEASON_WEIGHT * season_avg
    return round(blended, 2)


def get_minutes_adjustment(game_logs, window=10):
    """
    Calculate a minutes adjustment multiplier based on recent game log trends.

    BEGINNER NOTE: If a player has been playing significantly more or fewer
    minutes recently vs their season average, their stat projections should
    be scaled accordingly.

    Args:
        game_logs (list of dict): Player's game logs (most recent first or last)
        window (int): Number of recent games to analyze

    Returns:
        float: Multiplier to apply to minute-based projections.
            1.0 = no change, 1.15 = 15% more minutes recently, 0.85 = 15% fewer
    """
    if not game_logs or len(game_logs) < 3:
        return 1.0

    result = track_minutes_trend(game_logs, window=min(window, len(game_logs)))
    season_avg = result.get("season_avg_minutes", 0.0)
    recent_avg = result.get("recent_avg_minutes", 0.0)

    if season_avg < 5.0 or recent_avg < 1.0:
        return 1.0

    ratio = recent_avg / season_avg
    # Cap adjustment at 20% in either direction
    return max(0.80, min(1.20, ratio))
