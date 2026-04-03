"""engine/features/feature_engineering.py – Feature engineering for ML models."""
import math
import datetime
from typing import List, Dict, Any, Optional
from utils.logger import get_logger
from utils.constants import LEAGUE_AVG_PACE, LEAGUE_AVG_DRTG

_logger = get_logger(__name__)

_NBA_CITY_COORDS = {
    "Atlanta": (33.7573, -84.3963), "Boston": (42.3662, -71.0621),
    "Brooklyn": (40.6826, -73.9754), "Charlotte": (35.2251, -80.8392),
    "Chicago": (41.8807, -87.6742), "Cleveland": (41.4965, -81.6882),
    "Dallas": (32.7905, -96.8103), "Denver": (39.7487, -105.0077),
    "Detroit": (42.3410, -83.0553), "Golden State": (37.7680, -122.3877),
    "Houston": (29.7508, -95.3621), "Indiana": (39.7639, -86.1555),
    "LA Clippers": (34.0430, -118.2673), "LA Lakers": (34.0430, -118.2673),
    "Los Angeles": (34.0430, -118.2673),
    "Memphis": (35.1382, -90.0505), "Miami": (25.7814, -80.1870),
    "Milwaukee": (43.0450, -87.9170), "Minnesota": (44.9795, -93.2762),
    "New Orleans": (29.9490, -90.0812), "New York": (40.7505, -73.9934),
    "Oklahoma City": (35.4634, -97.5151), "Orlando": (28.5392, -81.3839),
    "Philadelphia": (39.9012, -75.1720), "Phoenix": (33.4457, -112.0712),
    "Portland": (45.5316, -122.6668), "Sacramento": (38.5802, -121.4997),
    "San Antonio": (29.4270, -98.4375), "Toronto": (43.6435, -79.3791),
    "Utah": (40.7683, -111.9011), "Washington": (38.8981, -77.0209),
}


def calculate_rolling_averages(
    game_logs: List[Dict[str, Any]],
    windows: List[int] = None,
) -> Dict[str, Any]:
    """Compute rolling averages over multiple windows for numeric columns.

    Args:
        game_logs: List of game log dicts sorted chronologically.
        windows: Window sizes (default [3, 5, 10, 20]).

    Returns:
        Dict mapping ``{col}_rolling_{w}`` to the rolling average value.
    """
    if windows is None:
        windows = [3, 5, 10, 20]

    result: Dict[str, Any] = {}
    if not game_logs:
        return result

    # Collect numeric columns
    numeric_keys = [k for k, v in game_logs[0].items() if isinstance(v, (int, float))]

    for col in numeric_keys:
        values = [float(g.get(col, 0)) for g in game_logs]
        for w in windows:
            recent = values[-w:] if len(values) >= w else values
            result[f"{col}_rolling_{w}"] = sum(recent) / len(recent) if recent else 0.0

    return result


def calculate_rest_days(
    schedule: List[Dict[str, Any]],
    game_date: str,
) -> int:
    """Calculate days since the team's last game.

    Args:
        schedule: List of dicts with a ``date`` key (YYYY-MM-DD).
        game_date: Current game date (YYYY-MM-DD).

    Returns:
        Days since last game, or 7 if no prior game found.
    """
    try:
        gd = datetime.date.fromisoformat(game_date)
        past_dates = sorted(
            [datetime.date.fromisoformat(g["date"]) for g in schedule if g["date"] < game_date],
            reverse=True,
        )
        if past_dates:
            return (gd - past_dates[0]).days
    except Exception as exc:
        _logger.debug("calculate_rest_days error: %s", exc)
    return 7


def is_back_to_back(schedule: List[Dict[str, Any]], game_date: str) -> bool:
    """Return True if the team played yesterday.

    Args:
        schedule: List of game dicts with a ``date`` key.
        game_date: Current game date (YYYY-MM-DD).

    Returns:
        True if rest days == 1, False otherwise.
    """
    return calculate_rest_days(schedule, game_date) == 1


def _haversine_miles(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    r = 3958.8
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * r * math.asin(math.sqrt(a))


def calculate_travel_distance(prev_city: str, current_city: str) -> float:
    """Approximate travel miles between two NBA cities.

    Args:
        prev_city: Previous game city name.
        current_city: Current game city name.

    Returns:
        Approximate miles, or 0.0 if city not found.
    """
    c1 = _NBA_CITY_COORDS.get(prev_city)
    c2 = _NBA_CITY_COORDS.get(current_city)
    if c1 is None or c2 is None:
        _logger.debug("City not found: %s or %s", prev_city, current_city)
        return 0.0
    return _haversine_miles(*c1, *c2)


def calculate_stat_differentials(
    team_stats: Dict[str, float],
    opponent_stats: Dict[str, float],
    stat_columns: List[str],
) -> Dict[str, float]:
    """Compute team stat minus opponent stat for each column.

    Args:
        team_stats: Team stat dict.
        opponent_stats: Opponent stat dict.
        stat_columns: List of column names to compute.

    Returns:
        Dict mapping ``{col}_diff`` to the differential.
    """
    return {
        f"{col}_diff": float(team_stats.get(col, 0)) - float(opponent_stats.get(col, 0))
        for col in stat_columns
    }


def calculate_home_away_splits(game_logs: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Separate home and away performance averages.

    Args:
        game_logs: List of game log dicts with a ``location`` key ('HOME'/'AWAY').

    Returns:
        Dict with ``home_avg`` and ``away_avg`` sub-dicts.
    """
    home = [g for g in game_logs if str(g.get("location", "")).upper() in ("HOME", "H", "VS.")]
    away = [g for g in game_logs if str(g.get("location", "")).upper() in ("AWAY", "A", "@")]

    def _avg(logs: List[Dict]) -> Dict[str, float]:
        if not logs:
            return {}
        numeric_keys = [k for k, v in logs[0].items() if isinstance(v, (int, float))]
        return {k: sum(float(g.get(k, 0)) for g in logs) / len(logs) for k in numeric_keys}

    return {"home_avg": _avg(home), "away_avg": _avg(away)}


def calculate_days_rest_factor(rest_days: int) -> float:
    """Return a performance multiplier based on rest days.

    Args:
        rest_days: Days of rest (0 = back-to-back/no rest, 1 = 1 day rest, etc.).

    Returns:
        Multiplier: 0.97 for 0 rest days, 1.00 for 1, 1.02 for 2, 1.03 for 3+.
    """
    if rest_days == 0:
        return 0.97
    if rest_days == 1:
        return 1.00
    if rest_days == 2:
        return 1.02
    return 1.03


def calculate_pace_adjustment(
    team_pace: float,
    opponent_pace: float,
    league_avg_pace: float = LEAGUE_AVG_PACE,
) -> float:
    """Pace adjustment factor for projections.

    Args:
        team_pace: Team's pace rating.
        opponent_pace: Opponent's pace rating.
        league_avg_pace: League average pace (default 100).

    Returns:
        Pace adjustment multiplier.
    """
    if league_avg_pace == 0:
        return 1.0
    combined_pace = (team_pace + opponent_pace) / 2.0
    return combined_pace / league_avg_pace


def calculate_defensive_matchup_factor(
    opp_drtg: float,
    league_avg_drtg: float = LEAGUE_AVG_DRTG,
) -> float:
    """Defensive matchup difficulty adjustment.

    Args:
        opp_drtg: Opponent's defensive rating.
        league_avg_drtg: League average defensive rating (default 110).

    Returns:
        Factor > 1.0 means easier matchup (poor defense), < 1.0 means harder.
    """
    if league_avg_drtg == 0:
        return 1.0
    return opp_drtg / league_avg_drtg


def build_feature_matrix(
    player_data: Dict[str, Any],
    team_data: Dict[str, Any],
    opponent_data: Dict[str, Any],
    game_context: Dict[str, Any],
) -> Dict[str, Any]:
    """Combine all features into a single feature dict/row for ML.

    Args:
        player_data: Player stats dict (with rolling averages, etc.).
        team_data: Team stats dict.
        opponent_data: Opponent stats dict.
        game_context: Game context (rest_days, location, pace, etc.).

    Returns:
        Flat feature dict suitable for ML model input.
    """
    features: Dict[str, Any] = {}

    # Player features
    for k, v in player_data.items():
        if isinstance(v, (int, float)):
            features[f"player_{k}"] = float(v)

    # Team features
    for k, v in team_data.items():
        if isinstance(v, (int, float)):
            features[f"team_{k}"] = float(v)

    # Differential features
    stat_cols = list(set(team_data.keys()) & set(opponent_data.keys()))
    diffs = calculate_stat_differentials(team_data, opponent_data, stat_cols)
    features.update(diffs)

    # Contextual features
    rest_days = int(game_context.get("rest_days", 1))
    features["rest_days"] = rest_days
    features["rest_factor"] = calculate_days_rest_factor(rest_days)
    features["is_home"] = 1 if game_context.get("is_home") else 0
    features["is_back_to_back"] = 1 if rest_days == 0 else 0

    team_pace = float(game_context.get("team_pace", LEAGUE_AVG_PACE))
    opp_pace = float(game_context.get("opponent_pace", LEAGUE_AVG_PACE))
    features["pace_adjustment"] = calculate_pace_adjustment(team_pace, opp_pace)

    opp_drtg = float(game_context.get("opponent_drtg", LEAGUE_AVG_DRTG))
    features["defensive_matchup_factor"] = calculate_defensive_matchup_factor(opp_drtg)

    # Travel fatigue — use utils.geo when team abbreviations are available,
    # fall back to city-name-based calculation within this module.
    travel_fatigue = 1.0
    try:
        prev_team = game_context.get("prev_team")
        curr_team = game_context.get("current_team")
        if prev_team and curr_team:
            from utils.geo import get_travel_distance, get_travel_fatigue_factor
            dist = get_travel_distance(prev_team, curr_team)
            travel_fatigue = get_travel_fatigue_factor(dist)
            features["travel_distance_miles"] = dist
        else:
            prev_city = game_context.get("prev_city")
            curr_city = game_context.get("current_city")
            if prev_city and curr_city:
                dist = calculate_travel_distance(prev_city, curr_city)
                # Derive fatigue factor using the same breakpoints as utils.geo
                if dist < 500:
                    travel_fatigue = 1.00
                elif dist < 1500:
                    travel_fatigue = 0.99
                elif dist < 2500:
                    travel_fatigue = 0.98
                else:
                    travel_fatigue = 0.97
                features["travel_distance_miles"] = dist
    except Exception as exc:
        _logger.debug("travel fatigue calculation failed: %s", exc)
    features["travel_fatigue"] = travel_fatigue

    return features
