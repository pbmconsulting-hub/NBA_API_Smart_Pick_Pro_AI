"""engine/features/player_metrics.py – NBA player-level advanced metrics."""
from utils.logger import get_logger

_logger = get_logger(__name__)


def calculate_true_shooting(pts: float, fga: float, fta: float) -> float:
    """True shooting percentage.

    Formula: TS% = PTS / (2 * (FGA + 0.44*FTA))

    Args:
        pts: Points scored.
        fga: Field goal attempts.
        fta: Free throw attempts.

    Returns:
        TS% as a decimal (e.g. 0.55), or 0.0 if denominator is zero.
    """
    denom = 2.0 * (fga + 0.44 * fta)
    if denom == 0:
        return 0.0
    return pts / denom


def calculate_usage_rate(
    fga: float,
    fta: float,
    tov: float,
    team_fga: float,
    team_fta: float,
    team_tov: float,
    mp: float,
    team_mp: float,
) -> float:
    """Player usage rate percentage.

    Formula: USG% = 100 * ((FGA + 0.44*FTA + TOV) * (TEAM_MP/5)) /
                    (MP * (TEAM_FGA + 0.44*TEAM_FTA + TEAM_TOV))

    Args:
        fga: Player field goal attempts.
        fta: Player free throw attempts.
        tov: Player turnovers.
        team_fga: Team field goal attempts.
        team_fta: Team free throw attempts.
        team_tov: Team turnovers.
        mp: Player minutes played.
        team_mp: Team total minutes played.

    Returns:
        Usage rate percentage, or 0.0 if denominator is zero.
    """
    numerator = (fga + 0.44 * fta + tov) * (team_mp / 5.0)
    denominator = mp * (team_fga + 0.44 * team_fta + team_tov)
    if denominator == 0:
        return 0.0
    return 100.0 * numerator / denominator


def calculate_per(stats_dict: dict) -> float:
    """Simplified Player Efficiency Rating.

    Uses a condensed PER formula based on common box score stats.

    Args:
        stats_dict: Dict with keys: pts, reb, ast, stl, blk, tov, fga, fgm, fta, ftm, mp.

    Returns:
        Estimated PER value, or 0.0 on error.
    """
    try:
        pts = float(stats_dict.get("pts", 0))
        reb = float(stats_dict.get("reb", 0))
        ast = float(stats_dict.get("ast", 0))
        stl = float(stats_dict.get("stl", 0))
        blk = float(stats_dict.get("blk", 0))
        tov = float(stats_dict.get("tov", 0))
        fga = float(stats_dict.get("fga", 0))
        fgm = float(stats_dict.get("fgm", 0))
        fta = float(stats_dict.get("fta", 0))
        ftm = float(stats_dict.get("ftm", 0))
        mp = float(stats_dict.get("mp", 1)) or 1.0

        positive = pts + reb + ast + stl + blk + 0.5 * (fgm - fga) + 0.5 * (ftm - fta)
        negative = tov
        return (positive - negative) / mp * 36.0
    except Exception as exc:
        _logger.debug("calculate_per error: %s", exc)
        return 0.0


def calculate_assist_percentage(
    ast: float,
    mp: float,
    team_mp: float,
    team_fgm: float,
    fgm: float,
) -> float:
    """Percentage of teammate field goals assisted by this player.

    Formula: AST% = 100 * AST / (((MP/(TEAM_MP/5)) * TEAM_FGM) - FGM)

    Args:
        ast: Player assists.
        mp: Player minutes played.
        team_mp: Team total minutes played.
        team_fgm: Team field goals made.
        fgm: Player field goals made.

    Returns:
        Assist percentage, or 0.0 if denominator is zero.
    """
    if team_mp == 0:
        return 0.0
    denom = (mp / (team_mp / 5.0)) * team_fgm - fgm
    if denom == 0:
        return 0.0
    return 100.0 * ast / denom


def calculate_rebound_percentage(
    reb: float,
    mp: float,
    team_mp: float,
    team_reb: float,
    opp_reb: float,
) -> float:
    """Percentage of available rebounds grabbed by this player.

    Formula: REB% = 100 * (REB * (TEAM_MP/5)) / (MP * (TEAM_REB + OPP_REB))

    Args:
        reb: Player total rebounds.
        mp: Player minutes played.
        team_mp: Team total minutes played.
        team_reb: Team total rebounds.
        opp_reb: Opponent total rebounds.

    Returns:
        Rebound percentage, or 0.0 if denominator is zero.
    """
    if team_mp == 0:
        return 0.0
    denom = mp * (team_reb + opp_reb)
    if denom == 0:
        return 0.0
    return 100.0 * (reb * (team_mp / 5.0)) / denom
