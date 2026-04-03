"""engine/features/team_metrics.py – NBA team-level advanced metrics."""
from utils.logger import get_logger

_logger = get_logger(__name__)


def calculate_possessions(fga: float, fta: float, oreb: float, tov: float) -> float:
    """Estimate team possessions.

    Formula: POSS = FGA + 0.44*FTA - OREB + TOV

    Args:
        fga: Field goal attempts.
        fta: Free throw attempts.
        oreb: Offensive rebounds.
        tov: Turnovers.

    Returns:
        Estimated possessions.
    """
    return fga + 0.44 * fta - oreb + tov


def calculate_offensive_rating(pts: float, poss: float) -> float:
    """Points scored per 100 possessions.

    Formula: ORTG = (PTS / POSS) * 100

    Args:
        pts: Points scored.
        poss: Possessions (use calculate_possessions).

    Returns:
        Offensive rating, or 0.0 if poss is zero.
    """
    if poss == 0:
        return 0.0
    return (pts / poss) * 100.0


def calculate_defensive_rating(opp_pts: float, poss: float) -> float:
    """Opponent points allowed per 100 possessions.

    Formula: DRTG = (OPP_PTS / POSS) * 100

    Args:
        opp_pts: Opponent points allowed.
        poss: Possessions.

    Returns:
        Defensive rating, or 0.0 if poss is zero.
    """
    if poss == 0:
        return 0.0
    return (opp_pts / poss) * 100.0


def calculate_net_rating(ortg: float, drtg: float) -> float:
    """Net rating (efficiency differential).

    Formula: NET = ORTG - DRTG

    Args:
        ortg: Offensive rating.
        drtg: Defensive rating.

    Returns:
        Net rating.
    """
    return ortg - drtg


def calculate_pace(poss: float, minutes: float) -> float:
    """Possessions per 48 minutes.

    Formula: PACE = 48 * (POSS / MINUTES)

    Args:
        poss: Possessions in a game.
        minutes: Total minutes played.

    Returns:
        Pace, or 0.0 if minutes is zero.
    """
    if minutes == 0:
        return 0.0
    return 48.0 * (poss / minutes)
