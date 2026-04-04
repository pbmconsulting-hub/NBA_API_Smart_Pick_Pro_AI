"""utils/league_averages.py – Auto-update league-wide pace and DRtg from NBA API.

At startup (or on a schedule), call ``refresh_league_averages()`` to pull
current-season league averages from the NBA API and update the in-memory
constants used by feature engineering and projections.

Falls back to the defaults in ``utils/constants`` and ``config.yaml`` when
the NBA API is unreachable.
"""
import utils.constants as _constants
from utils.logger import get_logger

_logger = get_logger(__name__)


def refresh_league_averages(season: str = None) -> dict:
    """Fetch current league pace and DRtg and update utils.constants in-place.

    Args:
        season: NBA season string (e.g. "2025-26"). Defaults to constants.DEFAULT_SEASON.

    Returns:
        Dict with ``pace`` and ``drtg`` values (whether freshly fetched or fallback).
    """
    season = season or _constants.DEFAULT_SEASON

    pace = _constants.LEAGUE_AVG_PACE
    drtg = _constants.LEAGUE_AVG_DRTG

    try:
        from nba_api.stats.endpoints import leaguedashteamstats
        import time

        # Respect rate limits
        time.sleep(0.6)

        stats = leaguedashteamstats.LeagueDashTeamStats(
            season=season,
            per_mode_detailed="PerGame",
            measure_type_detailed_defense="Base",
        )
        df = stats.get_data_frames()[0]

        if df is not None and not df.empty:
            # League-wide averages are the mean across all 30 teams
            if "PACE" in df.columns:
                pace = round(float(df["PACE"].mean()), 1)
                _constants.LEAGUE_AVG_PACE = pace
                _logger.info("Updated LEAGUE_AVG_PACE to %.1f (season %s)", pace, season)

            if "DEF_RATING" in df.columns:
                drtg = round(float(df["DEF_RATING"].mean()), 1)
                _constants.LEAGUE_AVG_DRTG = drtg
                _logger.info("Updated LEAGUE_AVG_DRTG to %.1f (season %s)", drtg, season)

            return {"pace": pace, "drtg": drtg}

    except ImportError:
        _logger.debug("nba_api not available; using fallback league averages")
    except Exception as exc:
        _logger.warning("Failed to fetch league averages: %s; using fallbacks", exc)

    return {"pace": _constants.LEAGUE_AVG_PACE, "drtg": _constants.LEAGUE_AVG_DRTG}
