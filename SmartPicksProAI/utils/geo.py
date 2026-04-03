"""utils/geo.py – Travel distance and fatigue calculations for NBA teams."""
import math
from utils.logger import get_logger

_logger = get_logger(__name__)

# Approximate arena coordinates (latitude, longitude) for all 30 NBA teams
NBA_ARENA_COORDS = {
    "ATL": (33.7573, -84.3963),
    "BOS": (42.3662, -71.0621),
    "BKN": (40.6826, -73.9754),
    "CHA": (35.2251, -80.8392),
    "CHI": (41.8807, -87.6742),
    "CLE": (41.4965, -81.6882),
    "DAL": (32.7905, -96.8103),
    "DEN": (39.7487, -105.0077),
    "DET": (42.3410, -83.0553),
    "GSW": (37.7680, -122.3877),
    "HOU": (29.7508, -95.3621),
    "IND": (39.7639, -86.1555),
    "LAC": (34.0430, -118.2673),
    "LAL": (34.0430, -118.2673),
    "MEM": (35.1382, -90.0505),
    "MIA": (25.7814, -80.1870),
    "MIL": (43.0450, -87.9170),
    "MIN": (44.9795, -93.2762),
    "NOP": (29.9490, -90.0812),
    "NYK": (40.7505, -73.9934),
    "OKC": (35.4634, -97.5151),
    "ORL": (28.5392, -81.3839),
    "PHI": (39.9012, -75.1720),
    "PHX": (33.4457, -112.0712),
    "POR": (45.5316, -122.6668),
    "SAC": (38.5802, -121.4997),
    "SAS": (29.4270, -98.4375),
    "TOR": (43.6435, -79.3791),
    "UTA": (40.7683, -111.9011),
    "WAS": (38.8981, -77.0209),
}


def haversine_distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Compute great-circle distance in miles between two coordinates.

    Args:
        lat1, lon1: Coordinates of point 1 (degrees).
        lat2, lon2: Coordinates of point 2 (degrees).

    Returns:
        Distance in miles.
    """
    r_miles = 3958.8
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    return 2 * r_miles * math.asin(math.sqrt(a))


def get_travel_distance(team1: str, team2: str) -> float:
    """Return approximate miles between two NBA team arenas.

    Args:
        team1: Team abbreviation (e.g. "LAL").
        team2: Team abbreviation (e.g. "BOS").

    Returns:
        Distance in miles, or 0.0 if either team is unknown.
    """
    coords1 = NBA_ARENA_COORDS.get(team1.upper())
    coords2 = NBA_ARENA_COORDS.get(team2.upper())
    if coords1 is None or coords2 is None:
        _logger.debug("Unknown team in travel distance: %s vs %s", team1, team2)
        return 0.0
    return haversine_distance(*coords1, *coords2)


def get_travel_fatigue_factor(distance_miles: float) -> float:
    """Return a fatigue multiplier based on travel distance.

    Args:
        distance_miles: Distance traveled in miles.

    Returns:
        Multiplier: 1.0 for short trips, down to 0.97 for cross-country.
    """
    if distance_miles < 500:
        return 1.00
    if distance_miles < 1500:
        return 0.99
    if distance_miles < 2500:
        return 0.98
    return 0.97
