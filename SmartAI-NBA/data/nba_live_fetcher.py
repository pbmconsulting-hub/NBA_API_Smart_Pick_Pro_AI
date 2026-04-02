# ============================================================
# FILE: data/nba_live_fetcher.py
# PURPOSE: Centralised NBA API gateway that wraps all available
#          stats.nba.com endpoints via the nba_api Python package.
#          Single source of truth for every NBA data type —
#          box scores, rotations, on/off splits, estimated
#          metrics, play-by-play, shot charts, clutch stats,
#          and more.
#
# CACHING POLICY:
#   - LIVE_TTL  (300 s)  — live game data, today's scoreboard
#   - HIST_TTL (3600 s)  — historical / season data
#
# RATE LIMITING:
#   - Uses the app's existing RateLimiter via get_nba_api_limiter()
#   - Mandatory _NBA_API_CALL_DELAY sleep between every nba_api call
#     to avoid triggering stats.nba.com's IP throttling
#
# GRACEFUL DEGRADATION:
#   - All nba_api imports are wrapped in try/except so the app
#     continues working even without the package installed.
#   - Every function returns an empty dict/list on failure.
#   - Never raises an exception to the caller.
#
# ENDPOINTS COVERED:
#   TIER 1 (critical — feeds projections & simulation):
#     fetch_player_game_logs, fetch_box_score_traditional,
#     fetch_box_score_advanced, fetch_box_score_usage,
#     fetch_player_on_off, fetch_player_estimated_metrics,
#     fetch_player_fantasy_profile, fetch_rotations,
#     fetch_schedule, fetch_todays_scoreboard
#
#   TIER 2 (high value — enrich predictions):
#     fetch_box_score_matchups, fetch_hustle_box_score,
#     fetch_defensive_box_score, fetch_scoring_box_score,
#     fetch_tracking_box_score, fetch_four_factors_box_score,
#     fetch_player_shooting_splits, fetch_shot_chart,
#     fetch_player_clutch_stats, fetch_team_lineups,
#     fetch_team_dashboard, fetch_standings,
#     fetch_team_game_logs, fetch_player_year_over_year
#
#   TIER 3 (reference & context):
#     fetch_player_vs_player, fetch_win_probability,
#     fetch_play_by_play, fetch_game_summary,
#     fetch_league_leaders, fetch_team_streak_finder
# ============================================================

from __future__ import annotations

import datetime
import logging
import time
from typing import Any

try:
    from utils.log_helper import get_logger
    _logger = get_logger(__name__)
except ImportError:
    _logger = logging.getLogger(__name__)

# ── Cache ──────────────────────────────────────────────────────────────────────

_CACHE: dict[str, tuple[Any, float]] = {}

# TTL constants (seconds)
LIVE_TTL: int = 300     # 5 minutes  — live game data / today's scoreboard
HIST_TTL: int = 3600    # 1 hour     — historical / season data

# Mandatory delay between nba_api calls to avoid IP throttling at stats.nba.com
_NBA_API_CALL_DELAY: float = 1.5


def _cache_get(key: str, ttl: int) -> Any | None:
    """Return cached payload if still within *ttl* seconds, else None."""
    entry = _CACHE.get(key)
    if entry is None:
        return None
    payload, ts = entry
    if time.time() - ts > ttl:
        _CACHE.pop(key, None)
        return None
    return payload


def _cache_set(key: str, payload: Any) -> None:
    """Store *payload* in the in-memory cache with the current timestamp."""
    _CACHE[key] = (payload, time.time())


# ── Rate limiter ───────────────────────────────────────────────────────────────

def _get_limiter():
    """Return the global NBA API rate limiter, or None if unavailable."""
    try:
        from utils.rate_limiter import get_nba_api_limiter
        return get_nba_api_limiter()
    except Exception:
        return None


def _check_rate_limit() -> bool:
    """
    Return True if the rate limiter permits a call, False if the circuit
    breaker is open.  Falls back to True (allow) when the limiter is
    unavailable so the app keeps working without utils/rate_limiter.py.
    """
    limiter = _get_limiter()
    if limiter is None:
        return True
    try:
        return limiter.acquire()
    except Exception:
        return True


# ── Game-ID validation ────────────────────────────────────────────────────────

def _is_nba_game_id(game_id: str) -> bool:
    """Return True if *game_id* is a numeric string (e.g. ``'0022401234'``)."""
    return bool(game_id) and game_id.isdigit()


# ── Season helpers ─────────────────────────────────────────────────────────────

def _current_season() -> str:
    """Return the current NBA season string in 'YYYY-YY' format."""
    now = datetime.date.today()
    year = now.year if now.month >= 10 else now.year - 1
    return f"{year}-{str(year + 1)[-2:]}"


def _resolve_season(season: str | None) -> str:
    """Normalise a season argument to 'YYYY-YY' format (e.g. '2025-26')."""
    if season is None:
        return _current_season()
    s = str(season).strip()
    if "-" in s and len(s) >= 6:
        return s
    try:
        y = int(s)
        return f"{y}-{str(y + 1)[-2:]}"
    except (ValueError, TypeError):
        return s


# ── nba_api availability guard ─────────────────────────────────────────────────

try:
    import nba_api  # noqa: F401 — availability check only
    _NBA_API_AVAILABLE = True
except ImportError:
    _NBA_API_AVAILABLE = False


def _parse_resultsets(raw: dict) -> dict:
    """Convert raw ``{resultSets: [{name, headers, rowSet}, ...]}`` to
    ``{name: [dict, ...]}`` so callers can use the same key look-ups as with
    ``get_normalized_dict()``.

    Used as a fallback when ``get_normalized_dict()`` raises an internal
    ``IndexError`` / ``KeyError`` / ``TypeError`` inside *nba_api*.
    """
    result: dict = {}
    for rs in raw.get("resultSets", []):
        name = rs.get("name", "")
        headers = rs.get("headers", [])
        rows = rs.get("rowSet", [])
        if name and headers:
            result[name] = [dict(zip(headers, row)) for row in rows]
    return result


# ══════════════════════════════════════════════════════════════════════════════
# TIER 1 — Critical endpoints (feed projections & simulation directly)
# ══════════════════════════════════════════════════════════════════════════════


def fetch_player_game_logs(
    player_id: int,
    season: str | None = None,
    last_n: int = 0,
) -> list[dict]:
    """
    Return per-game stats for a player in a given season.

    Endpoint: nba_api.stats.endpoints.playergamelog.PlayerGameLog

    Parameters
    ----------
    player_id : int
        NBA player ID.
    season : str | None
        Season string in 'YYYY-YY' format.  Defaults to current season.
    last_n : int
        When > 0, only the most recent *last_n* games are returned.
        When 0 (default), all games in the season are returned.

    Returns
    -------
    list[dict]
        Each dict mirrors the PlayerGameLog DataFrame columns (PTS, REB,
        AST, FG3M, STL, BLK, TOV, MIN, GAME_DATE, MATCHUP, WL …).
        Returns [] on failure.
    """
    season = _resolve_season(season)
    cache_key = f"nlf:player_game_logs:{player_id}:{season}:{last_n}"
    cached = _cache_get(cache_key, HIST_TTL)
    if cached is not None:
        return cached

    if not _NBA_API_AVAILABLE or not _check_rate_limit():
        _logger.warning("fetch_player_game_logs: blocked (nba_api=%s, rate_limit=denied)", _NBA_API_AVAILABLE)
        return []

    try:
        from nba_api.stats.endpoints import playergamelog
        time.sleep(_NBA_API_CALL_DELAY)
        t0 = time.monotonic()
        endpoint = playergamelog.PlayerGameLog(
            player_id=player_id,
            season=season,
            season_type_all_star="Regular Season",
        )
        logs: list[dict] = endpoint.get_data_frames()[0].to_dict("records")
        elapsed = round((time.monotonic() - t0) * 1000, 1)

        if last_n > 0:
            logs = logs[:last_n]

        _cache_set(cache_key, logs)
        _logger.info(
            "fetch_player_game_logs(%s, %s, last_n=%s): %d games in %.1f ms",
            player_id, season, last_n, len(logs), elapsed,
        )
        return logs
    except Exception as exc:
        _logger.warning("fetch_player_game_logs(%s, %s) failed: %s", player_id, season, exc)
        return []


def fetch_box_score_traditional(game_id: str, period: int = 0) -> dict:
    """
    Return the traditional box score for a game.

    Endpoint: nba_api.stats.endpoints.boxscoretraditionalv3.BoxScoreTraditionalV3

    Parameters
    ----------
    game_id : str
        NBA game ID (e.g. "0022401234").
    period : int
        Period filter (0 = full game, 1-4 = quarter, 5+ = OT).

    Returns
    -------
    dict
        Keys: ``player_stats`` (list[dict]), ``team_stats`` (list[dict]).
        Returns {} on failure.
    """
    cache_key = f"nlf:box_score_traditional:{game_id}:{period}"
    cached = _cache_get(cache_key, LIVE_TTL)
    if cached is not None:
        return cached

    if not _is_nba_game_id(game_id):
        _logger.debug("fetch_box_score_traditional: rejected non-numeric game_id=%r", game_id)
        return {}

    if not _NBA_API_AVAILABLE or not _check_rate_limit():
        _logger.warning("fetch_box_score_traditional: blocked (nba_api=%s, rate_limit=denied)", _NBA_API_AVAILABLE)
        return {}

    try:
        from nba_api.stats.endpoints import boxscoretraditionalv3
        time.sleep(_NBA_API_CALL_DELAY)
        t0 = time.monotonic()
        endpoint = boxscoretraditionalv3.BoxScoreTraditionalV3(
            game_id=game_id,
            start_period=period if period > 0 else 0,
            end_period=period if period > 0 else 14,
            start_range=0,
            end_range=28800,
            range_type=0,
        )
        norm = endpoint.get_normalized_dict() or {}
        if not norm:
            _logger.warning("fetch_box_score_traditional: get_normalized_dict() returned None/empty")
        elapsed = round((time.monotonic() - t0) * 1000, 1)

        result = {
            "player_stats": norm.get("PlayerStats", norm.get("playerStats", [])),
            "team_stats": norm.get("TeamStats", norm.get("teamStats", [])),
        }
        _cache_set(cache_key, result)
        _logger.info("fetch_box_score_traditional(%s): ok in %.1f ms", game_id, elapsed)
        return result
    except Exception as exc:
        _logger.warning("fetch_box_score_traditional(%s) failed: %s", game_id, exc)
        return {}


def fetch_box_score_advanced(game_id: str) -> dict:
    """
    Return the advanced box score for a game.

    Delegates to ``data.nba_stats_service.get_advanced_box_score`` for
    consistency with the existing cache layer.

    Endpoint: BoxScoreAdvancedV3

    Parameters
    ----------
    game_id : str
        NBA game ID (e.g. "0022401234").

    Returns
    -------
    dict
        Keys: ``player_stats`` (list[dict]), ``team_stats`` (list[dict]).
        Returns {} on failure.
    """
    cache_key = f"nlf:box_score_advanced:{game_id}"
    cached = _cache_get(cache_key, LIVE_TTL)
    if cached is not None:
        return cached

    if not _is_nba_game_id(game_id):
        _logger.debug("fetch_box_score_advanced: rejected non-numeric game_id=%r", game_id)
        return {}

    try:
        from data.nba_stats_service import get_advanced_box_score
        result = get_advanced_box_score(game_id)
        if result:
            _cache_set(cache_key, result)
        return result
    except Exception as exc:
        _logger.warning("fetch_box_score_advanced(%s) failed: %s", game_id, exc)
        return {}


def fetch_box_score_usage(game_id: str) -> dict:
    """
    Return usage statistics box score for a game (usage rate, touches, etc.).

    Endpoint: nba_api.stats.endpoints.boxscoreusagev3.BoxScoreUsageV3

    Parameters
    ----------
    game_id : str
        NBA game ID.

    Returns
    -------
    dict
        Keys: ``player_stats`` (list[dict]), ``team_stats`` (list[dict]).
        Each player row includes USG_PCT, PCT_FGA, PCT_FGM, etc.
        Returns {} on failure.
    """
    cache_key = f"nlf:box_score_usage:{game_id}"
    cached = _cache_get(cache_key, LIVE_TTL)
    if cached is not None:
        return cached

    if not _is_nba_game_id(game_id):
        _logger.debug("fetch_box_score_usage: rejected non-numeric game_id=%r", game_id)
        return {}

    if not _NBA_API_AVAILABLE or not _check_rate_limit():
        _logger.warning("fetch_box_score_usage: blocked (nba_api=%s, rate_limit=denied)", _NBA_API_AVAILABLE)
        return {}

    try:
        from nba_api.stats.endpoints import boxscoreusagev3
        time.sleep(_NBA_API_CALL_DELAY)
        t0 = time.monotonic()
        endpoint = boxscoreusagev3.BoxScoreUsageV3(game_id=game_id)
        norm = endpoint.get_normalized_dict() or {}
        if not norm:
            _logger.warning("fetch_box_score_usage: get_normalized_dict() returned None/empty")
        elapsed = round((time.monotonic() - t0) * 1000, 1)

        result = {
            "player_stats": norm.get("PlayerStats", norm.get("playerStats", [])),
            "team_stats": norm.get("TeamStats", norm.get("teamStats", [])),
        }
        _cache_set(cache_key, result)
        _logger.info("fetch_box_score_usage(%s): ok in %.1f ms", game_id, elapsed)
        return result
    except Exception as exc:
        _logger.warning("fetch_box_score_usage(%s) failed: %s", game_id, exc)
        return {}


def fetch_player_on_off(team_id: int, season: str | None = None) -> dict:
    """
    Return On/Off court differential stats for all players on a team.

    Uses ``LeagueDashPlayerStats`` with ``per_mode_simple='PerGame'`` and
    ``measure_type_detailed_defense='OnOffCourt'`` via the
    ``teamplayeronoffdetails`` endpoint.

    Endpoint: nba_api.stats.endpoints.teamplayeronoffdetails.TeamPlayerOnOffDetails

    Parameters
    ----------
    team_id : int
        NBA team ID.
    season : str | None
        Season string in 'YYYY-YY' format. Defaults to current season.

    Returns
    -------
    dict
        Keys:
          ``on_court``  — list[dict] of per-player on-court stats
          ``off_court`` — list[dict] of per-player off-court stats
        Each row includes PLAYER_ID, PLAYER_NAME, NET_RATING, PLUS_MINUS, etc.
        Returns {} on failure.
    """
    season = _resolve_season(season)
    cache_key = f"nlf:player_on_off:{team_id}:{season}"
    cached = _cache_get(cache_key, HIST_TTL)
    if cached is not None:
        return cached

    if not _NBA_API_AVAILABLE or not _check_rate_limit():
        _logger.warning("fetch_player_on_off: blocked (nba_api=%s, rate_limit=denied)", _NBA_API_AVAILABLE)
        return {}

    try:
        from nba_api.stats.endpoints import teamplayeronoffdetails
        time.sleep(_NBA_API_CALL_DELAY)
        t0 = time.monotonic()
        endpoint = teamplayeronoffdetails.TeamPlayerOnOffDetails(
            team_id=team_id,
            season=season,
            season_type_all_star="Regular Season",
        )
        norm = endpoint.get_normalized_dict() or {}
        if not norm:
            _logger.warning("fetch_player_on_off: get_normalized_dict() returned None/empty")
        elapsed = round((time.monotonic() - t0) * 1000, 1)

        result = {
            "on_court": norm.get("PlayersOnCourtTeamPlayerOnOffDetails", []),
            "off_court": norm.get("PlayersOffCourtTeamPlayerOnOffDetails", []),
        }
        _cache_set(cache_key, result)
        _logger.info("fetch_player_on_off(%s, %s): ok in %.1f ms", team_id, season, elapsed)
        return result
    except Exception as exc:
        _logger.warning("fetch_player_on_off(%s, %s) failed: %s", team_id, season, exc)
        return {}


def fetch_player_estimated_metrics(season: str | None = None) -> list[dict]:
    """
    Return estimated advanced metrics for all players (pace, w%, ortg, drtg, etc.).

    Endpoint: nba_api.stats.endpoints.playerestimatedmetrics.PlayerEstimatedMetrics

    Parameters
    ----------
    season : str | None
        Season string in 'YYYY-YY' format. Defaults to current season.

    Returns
    -------
    list[dict]
        Each dict contains PLAYER_ID, PLAYER_NAME, GP, W, L, W_PCT,
        MIN, E_OFF_RATING, E_DEF_RATING, E_NET_RATING, E_PACE, E_POSS, etc.
        Returns [] on failure.
    """
    season = _resolve_season(season)
    cache_key = f"nlf:player_estimated_metrics:{season}"
    cached = _cache_get(cache_key, HIST_TTL)
    if cached is not None:
        return cached

    if not _NBA_API_AVAILABLE or not _check_rate_limit():
        _logger.warning("fetch_player_estimated_metrics: blocked (nba_api=%s, rate_limit=denied)", _NBA_API_AVAILABLE)
        return []

    try:
        from nba_api.stats.endpoints import playerestimatedmetrics
        time.sleep(_NBA_API_CALL_DELAY)
        t0 = time.monotonic()
        endpoint = playerestimatedmetrics.PlayerEstimatedMetrics(
            season=season,
            season_type="Regular Season",
        )
        rows: list[dict] = endpoint.get_data_frames()[0].to_dict("records")
        elapsed = round((time.monotonic() - t0) * 1000, 1)

        _cache_set(cache_key, rows)
        _logger.info(
            "fetch_player_estimated_metrics(%s): %d rows in %.1f ms",
            season, len(rows), elapsed,
        )
        return rows
    except Exception as exc:
        _logger.warning("fetch_player_estimated_metrics(%s) failed: %s", season, exc)
        return []


def fetch_player_fantasy_profile(
    player_id: int,
    season: str | None = None,
) -> dict:
    """
    Return fantasy-relevant stat splits for a player (last N games, home/away, etc.).

    Endpoint: nba_api.stats.endpoints.playerdashboardbyyearoveryear.PlayerDashboardByYearOverYear
    (fantasy-profile-equivalent splits from the PlayerDashboardByLastNGames endpoint)

    Parameters
    ----------
    player_id : int
        NBA player ID.
    season : str | None
        Season string in 'YYYY-YY' format. Defaults to current season.

    Returns
    -------
    dict
        Keys: ``last_5`` (list[dict]), ``last_10`` (list[dict]),
              ``last_15`` (list[dict]), ``home`` (list[dict]),
              ``away`` (list[dict]).
        Returns {} on failure.
    """
    season = _resolve_season(season)
    cache_key = f"nlf:player_fantasy_profile:{player_id}:{season}"
    cached = _cache_get(cache_key, HIST_TTL)
    if cached is not None:
        return cached

    if not _NBA_API_AVAILABLE or not _check_rate_limit():
        _logger.warning("fetch_player_fantasy_profile: blocked (nba_api=%s, rate_limit=denied)", _NBA_API_AVAILABLE)
        return {}

    try:
        from nba_api.stats.endpoints import playerdashboardbylastngames
        time.sleep(_NBA_API_CALL_DELAY)
        t0 = time.monotonic()
        endpoint = playerdashboardbylastngames.PlayerDashboardByLastNGames(
            player_id=player_id,
            season=season,
            season_type_all_star="Regular Season",
        )
        frames = endpoint.get_data_frames()
        elapsed = round((time.monotonic() - t0) * 1000, 1)

        # The endpoint returns multiple frames for different last-N windows
        result: dict = {}
        labels = ["overall", "last_5", "last_10", "last_15", "last_20"]
        for i, label in enumerate(labels):
            if i < len(frames) and not frames[i].empty:
                result[label] = frames[i].to_dict("records")
            else:
                result[label] = []

        _cache_set(cache_key, result)
        _logger.info(
            "fetch_player_fantasy_profile(%s, %s): ok in %.1f ms",
            player_id, season, elapsed,
        )
        return result
    except Exception as exc:
        _logger.warning(
            "fetch_player_fantasy_profile(%s, %s) failed: %s", player_id, season, exc
        )
        return {}


def fetch_rotations(game_id: str) -> dict:
    """
    Return in/out rotation data for a game (exact stint times per player).

    Endpoint: nba_api.stats.endpoints.gamerotation.GameRotation

    Parameters
    ----------
    game_id : str
        NBA game ID (e.g. "0022401234").

    Returns
    -------
    dict
        Keys: ``home_team`` (list[dict]), ``away_team`` (list[dict]).
        Each row contains PLAYER_ID, PLAYER_FIRST, PLAYER_LAST,
        IN_TIME_REAL, OUT_TIME_REAL, PT_DIFF, etc.
        Returns {} on failure.
    """
    cache_key = f"nlf:rotations:{game_id}"
    cached = _cache_get(cache_key, LIVE_TTL)
    if cached is not None:
        return cached

    if not _is_nba_game_id(game_id):
        _logger.debug("fetch_rotations: rejected non-numeric game_id=%r", game_id)
        return {}

    if not _NBA_API_AVAILABLE or not _check_rate_limit():
        _logger.warning("fetch_rotations: blocked (nba_api=%s, rate_limit=denied)", _NBA_API_AVAILABLE)
        return {}

    try:
        from nba_api.stats.endpoints import gamerotation
        time.sleep(_NBA_API_CALL_DELAY)
        t0 = time.monotonic()
        endpoint = gamerotation.GameRotation(game_id=game_id)
        norm = endpoint.get_normalized_dict() or {}
        if not norm:
            _logger.warning("fetch_rotations: get_normalized_dict() returned None/empty")
        elapsed = round((time.monotonic() - t0) * 1000, 1)

        result = {
            "home_team": norm.get("HomeTeam", []),
            "away_team": norm.get("AwayTeam", []),
        }
        _cache_set(cache_key, result)
        _logger.info("fetch_rotations(%s): ok in %.1f ms", game_id, elapsed)
        return result
    except Exception as exc:
        _logger.warning("fetch_rotations(%s) failed: %s", game_id, exc)
        return {}


def fetch_schedule(game_date: str | None = None) -> list[dict]:
    """
    Return the game schedule for a given date.

    Endpoint: nba_api.stats.endpoints.scoreboardv2.ScoreboardV2

    Parameters
    ----------
    game_date : str | None
        Date string in 'YYYY-MM-DD' format.  Defaults to today.

    Returns
    -------
    list[dict]
        Each dict contains GAME_ID, HOME_TEAM_ID, VISITOR_TEAM_ID,
        HOME_TEAM_ABBREVIATION, VISITOR_TEAM_ABBREVIATION, GAME_STATUS_TEXT, etc.
        Returns [] on failure.
    """
    if game_date is None:
        game_date = datetime.date.today().strftime("%Y-%m-%d")

    cache_key = f"nlf:schedule:{game_date}"
    cached = _cache_get(cache_key, LIVE_TTL)
    if cached is not None:
        return cached

    if not _NBA_API_AVAILABLE or not _check_rate_limit():
        _logger.warning("fetch_schedule: blocked (nba_api=%s, rate_limit=denied)", _NBA_API_AVAILABLE)
        return []

    try:
        from nba_api.stats.endpoints import scoreboardv2
        time.sleep(_NBA_API_CALL_DELAY)
        t0 = time.monotonic()
        endpoint = scoreboardv2.ScoreboardV2(
            game_date=game_date,
            league_id="00",
        )
        norm = endpoint.get_normalized_dict() or {}
        if not norm:
            _logger.warning("fetch_schedule: get_normalized_dict() returned None/empty")
        elapsed = round((time.monotonic() - t0) * 1000, 1)

        games = norm.get("GameHeader", [])
        _cache_set(cache_key, games)
        _logger.info(
            "fetch_schedule(%s): %d games in %.1f ms", game_date, len(games), elapsed
        )
        return games
    except Exception as exc:
        _logger.warning("fetch_schedule(%s) failed: %s", game_date, exc)
        return []


def fetch_todays_scoreboard() -> dict:
    """
    Return today's full scoreboard including game headers, line scores, etc.

    Endpoint: nba_api.stats.endpoints.scoreboardv2.ScoreboardV2

    Returns
    -------
    dict
        Keys: ``game_header`` (list[dict]), ``line_score`` (list[dict]),
              ``series_standings`` (list[dict]), ``last_meeting`` (list[dict]),
              ``east_conf_standings`` (list[dict]),
              ``west_conf_standings`` (list[dict]).
        Returns {} on failure.
    """
    today = datetime.date.today().strftime("%Y-%m-%d")
    cache_key = f"nlf:todays_scoreboard:{today}"
    cached = _cache_get(cache_key, LIVE_TTL)
    if cached is not None:
        return cached

    if not _NBA_API_AVAILABLE or not _check_rate_limit():
        _logger.warning("fetch_todays_scoreboard: blocked (nba_api=%s, rate_limit=denied)", _NBA_API_AVAILABLE)
        return {}

    try:
        from nba_api.stats.endpoints import scoreboardv2
        time.sleep(_NBA_API_CALL_DELAY)
        t0 = time.monotonic()
        endpoint = scoreboardv2.ScoreboardV2(
            game_date=today,
            league_id="00",
        )
        norm = endpoint.get_normalized_dict() or {}
        if not norm:
            _logger.warning("fetch_todays_scoreboard: get_normalized_dict() returned None/empty")
        elapsed = round((time.monotonic() - t0) * 1000, 1)

        result = {
            "game_header": norm.get("GameHeader", []),
            "line_score": norm.get("LineScore", []),
            "series_standings": norm.get("SeriesStandings", []),
            "last_meeting": norm.get("LastMeeting", []),
            "east_conf_standings": norm.get("EastConfStandingsByDay", []),
            "west_conf_standings": norm.get("WestConfStandingsByDay", []),
        }
        _cache_set(cache_key, result)
        _logger.info("fetch_todays_scoreboard: ok in %.1f ms", elapsed)
        return result
    except Exception as exc:
        _logger.warning("fetch_todays_scoreboard failed: %s", exc)
        return {}


# ══════════════════════════════════════════════════════════════════════════════
# TIER 2 — High-value endpoints (enrich predictions)
# ══════════════════════════════════════════════════════════════════════════════


def fetch_box_score_matchups(game_id: str) -> dict:
    """
    Return defensive matchup data for a game (who guarded whom, etc.).

    Endpoint: nba_api.stats.endpoints.boxscorematchupsv3.BoxScoreMatchupsV3

    Parameters
    ----------
    game_id : str
        NBA game ID.

    Returns
    -------
    dict
        Keys: ``player_stats`` (list[dict]).
        Each row includes PLAYER_ID, PLAYER_NAME, MATCH_UP_PLAYER_ID,
        PARTIAL_POSS, PLAYER_GUARD_DIFF, etc.
        Returns {} on failure.
    """
    cache_key = f"nlf:box_score_matchups:{game_id}"
    cached = _cache_get(cache_key, LIVE_TTL)
    if cached is not None:
        return cached

    if not _is_nba_game_id(game_id):
        _logger.debug("fetch_box_score_matchups: rejected non-numeric game_id=%r", game_id)
        return {}

    if not _NBA_API_AVAILABLE or not _check_rate_limit():
        _logger.warning("fetch_box_score_matchups: blocked (nba_api=%s, rate_limit=denied)", _NBA_API_AVAILABLE)
        return {}

    try:
        from nba_api.stats.endpoints import boxscorematchupsv3
        time.sleep(_NBA_API_CALL_DELAY)
        t0 = time.monotonic()
        endpoint = boxscorematchupsv3.BoxScoreMatchupsV3(game_id=game_id)

        # Try get_normalized_dict first; nba_api's normaliser can raise
        # IndexError on certain game IDs where the response shape is
        # unexpected (e.g. games that haven't started or have no data).
        try:
            norm = endpoint.get_normalized_dict() or {}
        except (IndexError, KeyError, TypeError):
            norm = {}

        # Fallback: parse the raw dict returned by get_dict()
        if not norm:
            try:
                raw = endpoint.get_dict() or {}
                parsed = _parse_resultsets(raw)
                matchups = (
                    parsed.get("MatchUps", parsed.get("matchUps", []))
                    or raw.get("boxScoreMatchups", [])
                )
                if matchups:
                    norm = {"MatchUps": matchups}
            except Exception:
                pass

        if not norm:
            _logger.warning("fetch_box_score_matchups(%s): no data from normalized or raw dict", game_id)
        elapsed = round((time.monotonic() - t0) * 1000, 1)

        result = {
            "player_stats": norm.get("MatchUps", norm.get("matchUps", [])),
        }
        _cache_set(cache_key, result)
        _logger.info("fetch_box_score_matchups(%s): ok in %.1f ms", game_id, elapsed)
        return result
    except Exception as exc:
        _logger.warning("fetch_box_score_matchups(%s) failed: %s", game_id, exc)
        return {}


def fetch_hustle_box_score(game_id: str) -> dict:
    """
    Return hustle statistics box score (charges, deflections, loose balls, etc.).

    Delegates to ``data.nba_stats_service.get_hustle_box_score`` for
    consistency with the existing cache layer.

    Endpoint: BoxScoreHustleV2

    Parameters
    ----------
    game_id : str
        NBA game ID.

    Returns
    -------
    dict
        Keys: ``player_stats`` (list[dict]), ``team_stats`` (list[dict]).
        Returns {} on failure.
    """
    cache_key = f"nlf:hustle_box_score:{game_id}"
    cached = _cache_get(cache_key, LIVE_TTL)
    if cached is not None:
        return cached

    if not _is_nba_game_id(game_id):
        _logger.debug("fetch_hustle_box_score: rejected non-numeric game_id=%r", game_id)
        return {}

    try:
        from data.nba_stats_service import get_hustle_box_score
        result = get_hustle_box_score(game_id)
        if result:
            _cache_set(cache_key, result)
        return result
    except Exception as exc:
        _logger.warning("fetch_hustle_box_score(%s) failed: %s", game_id, exc)
        return {}


def fetch_defensive_box_score(game_id: str) -> dict:
    """
    Return defensive statistics box score for a game.

    Endpoint: nba_api.stats.endpoints.boxscoredefensivev2.BoxScoreDefensiveV2

    Parameters
    ----------
    game_id : str
        NBA game ID.

    Returns
    -------
    dict
        Keys: ``player_stats`` (list[dict]), ``team_stats`` (list[dict]).
        Returns {} on failure.
    """
    cache_key = f"nlf:defensive_box_score:{game_id}"
    cached = _cache_get(cache_key, LIVE_TTL)
    if cached is not None:
        return cached

    if not _is_nba_game_id(game_id):
        _logger.debug("fetch_defensive_box_score: rejected non-numeric game_id=%r", game_id)
        return {}

    if not _NBA_API_AVAILABLE or not _check_rate_limit():
        _logger.warning("fetch_defensive_box_score: blocked (nba_api=%s, rate_limit=denied)", _NBA_API_AVAILABLE)
        return {}

    try:
        from nba_api.stats.endpoints import boxscoredefensivev2
        time.sleep(_NBA_API_CALL_DELAY)
        t0 = time.monotonic()
        endpoint = boxscoredefensivev2.BoxScoreDefensiveV2(game_id=game_id)
        norm = endpoint.get_normalized_dict() or {}
        if not norm:
            _logger.warning("fetch_defensive_box_score: get_normalized_dict() returned None/empty")
        elapsed = round((time.monotonic() - t0) * 1000, 1)

        result = {
            "player_stats": norm.get("PlayerStats", norm.get("playerStats", [])),
            "team_stats": norm.get("TeamStats", norm.get("teamStats", [])),
        }
        _cache_set(cache_key, result)
        _logger.info("fetch_defensive_box_score(%s): ok in %.1f ms", game_id, elapsed)
        return result
    except Exception as exc:
        _logger.warning("fetch_defensive_box_score(%s) failed: %s", game_id, exc)
        return {}


def fetch_scoring_box_score(game_id: str) -> dict:
    """
    Return scoring breakdown box score (field-goal percentages by zone, etc.).

    Endpoint: nba_api.stats.endpoints.boxscorescoringv3.BoxScoreScoringV3

    Parameters
    ----------
    game_id : str
        NBA game ID.

    Returns
    -------
    dict
        Keys: ``player_stats`` (list[dict]), ``team_stats`` (list[dict]).
        Returns {} on failure.
    """
    cache_key = f"nlf:scoring_box_score:{game_id}"
    cached = _cache_get(cache_key, LIVE_TTL)
    if cached is not None:
        return cached

    if not _is_nba_game_id(game_id):
        _logger.debug("fetch_scoring_box_score: rejected non-numeric game_id=%r", game_id)
        return {}

    if not _NBA_API_AVAILABLE or not _check_rate_limit():
        _logger.warning("fetch_scoring_box_score: blocked (nba_api=%s, rate_limit=denied)", _NBA_API_AVAILABLE)
        return {}

    try:
        from nba_api.stats.endpoints import boxscorescoringv3
        time.sleep(_NBA_API_CALL_DELAY)
        t0 = time.monotonic()
        endpoint = boxscorescoringv3.BoxScoreScoringV3(game_id=game_id)
        norm = endpoint.get_normalized_dict() or {}
        if not norm:
            _logger.warning("fetch_scoring_box_score: get_normalized_dict() returned None/empty")
        elapsed = round((time.monotonic() - t0) * 1000, 1)

        result = {
            "player_stats": norm.get("PlayerStats", norm.get("playerStats", [])),
            "team_stats": norm.get("TeamStats", norm.get("teamStats", [])),
        }
        _cache_set(cache_key, result)
        _logger.info("fetch_scoring_box_score(%s): ok in %.1f ms", game_id, elapsed)
        return result
    except Exception as exc:
        _logger.warning("fetch_scoring_box_score(%s) failed: %s", game_id, exc)
        return {}


def fetch_tracking_box_score(game_id: str) -> dict:
    """
    Return player-tracking box score (speed, distance, touches, etc.).

    Delegates to ``data.nba_stats_service.get_player_tracking_box_score``
    for consistency with the existing cache layer.

    Endpoint: BoxScorePlayerTrackV3

    Parameters
    ----------
    game_id : str
        NBA game ID.

    Returns
    -------
    dict
        Keys: ``player_stats`` (list[dict]), ``team_stats`` (list[dict]).
        Returns {} on failure.
    """
    cache_key = f"nlf:tracking_box_score:{game_id}"
    cached = _cache_get(cache_key, LIVE_TTL)
    if cached is not None:
        return cached

    if not _is_nba_game_id(game_id):
        _logger.debug("fetch_tracking_box_score: rejected non-numeric game_id=%r", game_id)
        return {}

    try:
        from data.nba_stats_service import get_player_tracking_box_score
        result = get_player_tracking_box_score(game_id)
        if result:
            _cache_set(cache_key, result)
        return result
    except Exception as exc:
        _logger.warning("fetch_tracking_box_score(%s) failed: %s", game_id, exc)
        return {}


def fetch_four_factors_box_score(game_id: str) -> dict:
    """
    Return four-factors box score (eFG%, TO%, ORB%, FT rate) for a game.

    Endpoint: nba_api.stats.endpoints.boxscorefourfactorsv3.BoxScoreFourFactorsV3

    Parameters
    ----------
    game_id : str
        NBA game ID.

    Returns
    -------
    dict
        Keys: ``player_stats`` (list[dict]), ``team_stats`` (list[dict]).
        Team stats contain EFG_PCT, FTA_RATE, TM_TOV_PCT, OREB_PCT, OPP_EFG_PCT, etc.
        Returns {} on failure.
    """
    cache_key = f"nlf:four_factors_box_score:{game_id}"
    cached = _cache_get(cache_key, LIVE_TTL)
    if cached is not None:
        return cached

    if not _is_nba_game_id(game_id):
        _logger.debug("fetch_four_factors_box_score: rejected non-numeric game_id=%r", game_id)
        return {}

    if not _NBA_API_AVAILABLE or not _check_rate_limit():
        _logger.warning("fetch_four_factors_box_score: blocked (nba_api=%s, rate_limit=denied)", _NBA_API_AVAILABLE)
        return {}

    try:
        from nba_api.stats.endpoints import boxscorefourfactorsv3
        time.sleep(_NBA_API_CALL_DELAY)
        t0 = time.monotonic()
        endpoint = boxscorefourfactorsv3.BoxScoreFourFactorsV3(game_id=game_id)

        try:
            norm = endpoint.get_normalized_dict() or {}
        except (IndexError, KeyError, TypeError):
            norm = {}

        # Fallback: parse the raw dict returned by get_dict()
        if not norm:
            try:
                raw = endpoint.get_dict() or {}
                parsed = _parse_resultsets(raw)
                player_stats = (
                    parsed.get("sqlPlayersFourFactors",
                               parsed.get("PlayerStats",
                                          parsed.get("playerStats", [])))
                    or raw.get("playerStats", [])
                )
                team_stats = (
                    parsed.get("sqlTeamsFourFactors",
                               parsed.get("TeamStats",
                                          parsed.get("teamStats", [])))
                    or raw.get("teamStats", [])
                )
                if player_stats or team_stats:
                    norm = {"PlayerStats": player_stats, "TeamStats": team_stats}
            except Exception:
                pass

        if not norm:
            _logger.warning("fetch_four_factors_box_score(%s): no data from normalized or raw dict", game_id)
        elapsed = round((time.monotonic() - t0) * 1000, 1)

        result = {
            "player_stats": norm.get("PlayerStats", norm.get("playerStats", [])),
            "team_stats": norm.get("TeamStats", norm.get("teamStats", [])),
        }
        _cache_set(cache_key, result)
        _logger.info("fetch_four_factors_box_score(%s): ok in %.1f ms", game_id, elapsed)
        return result
    except Exception as exc:
        _logger.warning("fetch_four_factors_box_score(%s) failed: %s", game_id, exc)
        return {}


def fetch_player_shooting_splits(
    player_id: int,
    season: str | None = None,
) -> dict:
    """
    Return detailed shooting split stats for a player (by zone, distance, etc.).

    Endpoint: nba_api.stats.endpoints.playerdashptshotlog.PlayerDashPtShotLog

    Parameters
    ----------
    player_id : int
        NBA player ID.
    season : str | None
        Season string in 'YYYY-YY' format. Defaults to current season.

    Returns
    -------
    dict
        Keys: ``shot_log`` (list[dict]), ``closest_defender`` (list[dict]).
        Returns {} on failure.
    """
    season = _resolve_season(season)
    cache_key = f"nlf:player_shooting_splits:{player_id}:{season}"
    cached = _cache_get(cache_key, HIST_TTL)
    if cached is not None:
        return cached

    if not _NBA_API_AVAILABLE or not _check_rate_limit():
        _logger.warning("fetch_player_shooting_splits: blocked (nba_api=%s, rate_limit=denied)", _NBA_API_AVAILABLE)
        return {}

    try:
        from nba_api.stats.endpoints import playerdashptshotlog
        time.sleep(_NBA_API_CALL_DELAY)
        t0 = time.monotonic()
        endpoint = playerdashptshotlog.PlayerDashPtShotLog(
            player_id=player_id,
            season=season,
            season_type_all_star="Regular Season",
        )
        frames = endpoint.get_data_frames()
        elapsed = round((time.monotonic() - t0) * 1000, 1)

        result = {
            "shot_log": frames[0].to_dict("records") if len(frames) > 0 and not frames[0].empty else [],
            "closest_defender": frames[1].to_dict("records") if len(frames) > 1 and not frames[1].empty else [],
        }
        _cache_set(cache_key, result)
        _logger.info(
            "fetch_player_shooting_splits(%s, %s): ok in %.1f ms",
            player_id, season, elapsed,
        )
        return result
    except Exception as exc:
        _logger.warning(
            "fetch_player_shooting_splits(%s, %s) failed: %s", player_id, season, exc
        )
        return {}


def fetch_shot_chart(
    player_id: int,
    season: str | None = None,
) -> list[dict]:
    """
    Return shot chart data for a player (x/y coordinates, made/missed, zone, etc.).

    Delegates to ``data.nba_stats_service.get_shot_chart`` for consistency
    with the existing cache layer.

    Endpoint: ShotChartDetail

    Parameters
    ----------
    player_id : int
        NBA player ID.
    season : str | None
        Season string in 'YYYY-YY' format. Defaults to current season.

    Returns
    -------
    list[dict]
        Each dict has LOC_X, LOC_Y, SHOT_MADE_FLAG, SHOT_ZONE_BASIC,
        SHOT_DISTANCE, SHOT_TYPE, etc.
        Returns [] on failure.
    """
    try:
        from data.nba_stats_service import get_shot_chart
        return get_shot_chart(player_id, season)
    except Exception as exc:
        _logger.warning("fetch_shot_chart(%s, %s) failed: %s", player_id, season, exc)
        return []


def fetch_player_clutch_stats(season: str | None = None) -> list[dict]:
    """
    Return clutch-time stats (last 5 min, margin ≤5) for all players.

    Endpoint: nba_api.stats.endpoints.leaguedashplayerclutch.LeagueDashPlayerClutch

    Parameters
    ----------
    season : str | None
        Season string in 'YYYY-YY' format. Defaults to current season.

    Returns
    -------
    list[dict]
        Each dict contains PLAYER_ID, PLAYER_NAME, GP, MIN, FGM, FGA,
        PTS, REB, AST, TOV, PLUS_MINUS, W_PCT, etc.
        Returns [] on failure.
    """
    season = _resolve_season(season)
    cache_key = f"nlf:player_clutch_stats:{season}"
    cached = _cache_get(cache_key, HIST_TTL)
    if cached is not None:
        return cached

    if not _NBA_API_AVAILABLE or not _check_rate_limit():
        _logger.warning("fetch_player_clutch_stats: blocked (nba_api=%s, rate_limit=denied)", _NBA_API_AVAILABLE)
        return []

    try:
        from nba_api.stats.endpoints import leaguedashplayerclutch
        time.sleep(_NBA_API_CALL_DELAY)
        t0 = time.monotonic()
        endpoint = leaguedashplayerclutch.LeagueDashPlayerClutch(
            season=season,
            season_type_all_star="Regular Season",
        )
        rows: list[dict] = endpoint.get_data_frames()[0].to_dict("records")
        elapsed = round((time.monotonic() - t0) * 1000, 1)

        _cache_set(cache_key, rows)
        _logger.info(
            "fetch_player_clutch_stats(%s): %d rows in %.1f ms", season, len(rows), elapsed
        )
        return rows
    except Exception as exc:
        _logger.warning("fetch_player_clutch_stats(%s) failed: %s", season, exc)
        return []


def fetch_team_lineups(
    team_id: int,
    season: str | None = None,
) -> list[dict]:
    """
    Return 5-man lineup stats for a specific team.

    Endpoint: nba_api.stats.endpoints.leaguedashlineups.LeagueDashLineups

    Parameters
    ----------
    team_id : int
        NBA team ID.  Filters the league-wide lineup data to one team.
    season : str | None
        Season string in 'YYYY-YY' format. Defaults to current season.

    Returns
    -------
    list[dict]
        Each dict contains GROUP_ID (lineup string), TEAM_ID, GP, MIN,
        PLUS_MINUS, NET_RATING, OFF_RATING, DEF_RATING, etc.
        Returns [] on failure.
    """
    season = _resolve_season(season)
    cache_key = f"nlf:team_lineups:{team_id}:{season}"
    cached = _cache_get(cache_key, HIST_TTL)
    if cached is not None:
        return cached

    if not _NBA_API_AVAILABLE or not _check_rate_limit():
        _logger.warning("fetch_team_lineups: blocked (nba_api=%s, rate_limit=denied)", _NBA_API_AVAILABLE)
        return []

    try:
        from nba_api.stats.endpoints import leaguedashlineups
        time.sleep(_NBA_API_CALL_DELAY)
        t0 = time.monotonic()
        endpoint = leaguedashlineups.LeagueDashLineups(
            season=season,
            season_type_all_star="Regular Season",
            group_quantity=5,
        )
        rows: list[dict] = endpoint.get_data_frames()[0].to_dict("records")
        elapsed = round((time.monotonic() - t0) * 1000, 1)

        # Filter to the requested team
        team_rows = [r for r in rows if r.get("TEAM_ID") == team_id]

        _cache_set(cache_key, team_rows)
        _logger.info(
            "fetch_team_lineups(%s, %s): %d lineups in %.1f ms",
            team_id, season, len(team_rows), elapsed,
        )
        return team_rows
    except Exception as exc:
        _logger.warning("fetch_team_lineups(%s, %s) failed: %s", team_id, season, exc)
        return []


def fetch_team_dashboard(
    team_id: int,
    season: str | None = None,
) -> dict:
    """
    Return general team dashboard stats (splits by home/away, monthly, etc.).

    Endpoint: nba_api.stats.endpoints.teamdashboardbygeneralsplits.TeamDashboardByGeneralSplits

    Parameters
    ----------
    team_id : int
        NBA team ID.
    season : str | None
        Season string in 'YYYY-YY' format. Defaults to current season.

    Returns
    -------
    dict
        Keys: ``overall`` (list[dict]), ``location`` (list[dict]),
              ``days_rest`` (list[dict]), ``month`` (list[dict]).
        Each row contains W, L, W_PCT, PTS, REB, AST, PLUS_MINUS, etc.
        Returns {} on failure.
    """
    season = _resolve_season(season)
    cache_key = f"nlf:team_dashboard:{team_id}:{season}"
    cached = _cache_get(cache_key, HIST_TTL)
    if cached is not None:
        return cached

    if not _NBA_API_AVAILABLE or not _check_rate_limit():
        _logger.warning("fetch_team_dashboard: blocked (nba_api=%s, rate_limit=denied)", _NBA_API_AVAILABLE)
        return {}

    try:
        from nba_api.stats.endpoints import teamdashboardbygeneralsplits
        time.sleep(_NBA_API_CALL_DELAY)
        t0 = time.monotonic()
        endpoint = teamdashboardbygeneralsplits.TeamDashboardByGeneralSplits(
            team_id=team_id,
            season=season,
            season_type_all_star="Regular Season",
        )
        frames = endpoint.get_data_frames()
        elapsed = round((time.monotonic() - t0) * 1000, 1)

        labels = ["overall", "location", "wins_losses", "days_rest", "month"]
        result: dict = {}
        for i, label in enumerate(labels):
            if i < len(frames) and not frames[i].empty:
                result[label] = frames[i].to_dict("records")
            else:
                result[label] = []

        _cache_set(cache_key, result)
        _logger.info(
            "fetch_team_dashboard(%s, %s): ok in %.1f ms", team_id, season, elapsed
        )
        return result
    except Exception as exc:
        _logger.warning("fetch_team_dashboard(%s, %s) failed: %s", team_id, season, exc)
        return {}


def fetch_standings(season: str | None = None) -> list[dict]:
    """
    Return current league standings.

    Delegates to ``data.nba_stats_service.get_league_standings`` for
    consistency with the existing cache layer.

    Endpoint: LeagueStandingsV3

    Parameters
    ----------
    season : str | None
        Season string in 'YYYY-YY' format. Defaults to current season.

    Returns
    -------
    list[dict]
        Each dict contains TeamID, TeamName, Conference, WINS, LOSSES,
        WinPCT, HOME, ROAD, LAST10, etc.
        Returns [] on failure.
    """
    try:
        from data.nba_stats_service import get_league_standings
        return get_league_standings(season)
    except Exception as exc:
        _logger.warning("fetch_standings(%s) failed: %s", season, exc)
        return []


def fetch_team_game_logs(
    team_id: int,
    season: str | None = None,
    last_n: int = 0,
) -> list[dict]:
    """
    Return per-game stats for a team in a given season.

    Endpoint: nba_api.stats.endpoints.teamgamelog.TeamGameLog

    Parameters
    ----------
    team_id : int
        NBA team ID.
    season : str | None
        Season string in 'YYYY-YY' format. Defaults to current season.
    last_n : int
        When > 0, only the most recent *last_n* games are returned.

    Returns
    -------
    list[dict]
        Each dict contains Game_ID, GAME_DATE, MATCHUP, WL, W, L,
        PTS, FGM, FGA, FG_PCT, FG3M, FG3A, FTM, FTA, REB, AST, etc.
        Returns [] on failure.
    """
    season = _resolve_season(season)
    cache_key = f"nlf:team_game_logs:{team_id}:{season}:{last_n}"
    cached = _cache_get(cache_key, HIST_TTL)
    if cached is not None:
        return cached

    if not _NBA_API_AVAILABLE or not _check_rate_limit():
        _logger.warning("fetch_team_game_logs: blocked (nba_api=%s, rate_limit=denied)", _NBA_API_AVAILABLE)
        return []

    try:
        from nba_api.stats.endpoints import teamgamelog
        time.sleep(_NBA_API_CALL_DELAY)
        t0 = time.monotonic()
        endpoint = teamgamelog.TeamGameLog(
            team_id=team_id,
            season=season,
            season_type_all_star="Regular Season",
        )
        logs: list[dict] = endpoint.get_data_frames()[0].to_dict("records")
        elapsed = round((time.monotonic() - t0) * 1000, 1)

        if last_n > 0:
            logs = logs[:last_n]

        _cache_set(cache_key, logs)
        _logger.info(
            "fetch_team_game_logs(%s, %s, last_n=%s): %d games in %.1f ms",
            team_id, season, last_n, len(logs), elapsed,
        )
        return logs
    except Exception as exc:
        _logger.warning(
            "fetch_team_game_logs(%s, %s) failed: %s", team_id, season, exc
        )
        return []


def fetch_player_year_over_year(player_id: int) -> list[dict]:
    """
    Return year-over-year career stats for a player.

    Endpoint: nba_api.stats.endpoints.playerdashboardbyyearoveryear.PlayerDashboardByYearOverYear

    Parameters
    ----------
    player_id : int
        NBA player ID.

    Returns
    -------
    list[dict]
        Each dict contains GROUP_VALUE (season), GP, MIN, PTS, REB, AST,
        FG_PCT, FG3_PCT, FT_PCT, PLUS_MINUS, etc.
        Returns [] on failure.
    """
    cache_key = f"nlf:player_year_over_year:{player_id}"
    cached = _cache_get(cache_key, HIST_TTL)
    if cached is not None:
        return cached

    if not _NBA_API_AVAILABLE or not _check_rate_limit():
        _logger.warning("fetch_player_year_over_year: blocked (nba_api=%s, rate_limit=denied)", _NBA_API_AVAILABLE)
        return []

    try:
        from nba_api.stats.endpoints import playerdashboardbyyearoveryear
        time.sleep(_NBA_API_CALL_DELAY)
        t0 = time.monotonic()
        endpoint = playerdashboardbyyearoveryear.PlayerDashboardByYearOverYear(
            player_id=player_id,
            season_type_all_star="Regular Season",
        )
        frames = endpoint.get_data_frames()
        elapsed = round((time.monotonic() - t0) * 1000, 1)

        rows = frames[1].to_dict("records") if len(frames) > 1 and not frames[1].empty else []
        _cache_set(cache_key, rows)
        _logger.info(
            "fetch_player_year_over_year(%s): %d seasons in %.1f ms",
            player_id, len(rows), elapsed,
        )
        return rows
    except Exception as exc:
        _logger.warning("fetch_player_year_over_year(%s) failed: %s", player_id, exc)
        return []


# ══════════════════════════════════════════════════════════════════════════════
# TIER 3 — Reference & Context endpoints
# ══════════════════════════════════════════════════════════════════════════════


def fetch_player_vs_player(
    player1_id: int,
    player2_id: int,
    season: str | None = None,
) -> dict:
    """
    Return head-to-head stats for one player when matched up against another.

    Endpoint: nba_api.stats.endpoints.playervsplayer.PlayerVsPlayer

    Parameters
    ----------
    player1_id : int
        NBA player ID for the offensive player.
    player2_id : int
        NBA player ID for the defensive player (the matchup opponent).
    season : str | None
        Season string in 'YYYY-YY' format. Defaults to current season.

    Returns
    -------
    dict
        Keys: ``on_off_court`` (list[dict]), ``shot_distance`` (list[dict]),
              ``player1_shot_areas`` (list[dict]),
              ``player2_shot_areas`` (list[dict]).
        Returns {} on failure.
    """
    season = _resolve_season(season)
    cache_key = f"nlf:player_vs_player:{player1_id}:{player2_id}:{season}"
    cached = _cache_get(cache_key, HIST_TTL)
    if cached is not None:
        return cached

    if not _NBA_API_AVAILABLE or not _check_rate_limit():
        _logger.warning("fetch_player_vs_player: blocked (nba_api=%s, rate_limit=denied)", _NBA_API_AVAILABLE)
        return {}

    try:
        from nba_api.stats.endpoints import playervsplayer
        time.sleep(_NBA_API_CALL_DELAY)
        t0 = time.monotonic()
        endpoint = playervsplayer.PlayerVsPlayer(
            player_id=player1_id,
            vs_player_id=player2_id,
            season=season,
            season_type_all_star="Regular Season",
        )
        frames = endpoint.get_data_frames()
        elapsed = round((time.monotonic() - t0) * 1000, 1)

        labels = ["on_off_court", "shot_distance", "player1_shot_areas", "player2_shot_areas"]
        result: dict = {}
        for i, label in enumerate(labels):
            if i < len(frames) and not frames[i].empty:
                result[label] = frames[i].to_dict("records")
            else:
                result[label] = []

        _cache_set(cache_key, result)
        _logger.info(
            "fetch_player_vs_player(%s, %s, %s): ok in %.1f ms",
            player1_id, player2_id, season, elapsed,
        )
        return result
    except Exception as exc:
        _logger.warning(
            "fetch_player_vs_player(%s, %s) failed: %s", player1_id, player2_id, exc
        )
        return {}


def fetch_win_probability(game_id: str) -> dict:
    """
    Return real-time win probability at each point in the game.

    Endpoint: nba_api.stats.endpoints.winprobabilitypbp.WinProbabilityPBP

    Parameters
    ----------
    game_id : str
        NBA game ID.

    Returns
    -------
    dict
        Keys: ``win_probability`` (list[dict]), ``game_info`` (list[dict]).
        Each win-probability row has HOME_PCT, VISITOR_PCT, PERIOD,
        GAME_CLOCK, etc.
        Returns {} on failure.
    """
    cache_key = f"nlf:win_probability:{game_id}"
    cached = _cache_get(cache_key, LIVE_TTL)
    if cached is not None:
        return cached

    if not _is_nba_game_id(game_id):
        _logger.debug("fetch_win_probability: rejected non-numeric game_id=%r", game_id)
        return {}

    if not _NBA_API_AVAILABLE or not _check_rate_limit():
        _logger.warning("fetch_win_probability: blocked (nba_api=%s, rate_limit=denied)", _NBA_API_AVAILABLE)
        return {}

    try:
        from nba_api.stats.endpoints import winprobabilitypbp
        time.sleep(_NBA_API_CALL_DELAY)
        t0 = time.monotonic()
        endpoint = winprobabilitypbp.WinProbabilityPBP(game_id=game_id)
        norm = endpoint.get_normalized_dict() or {}
        if not norm:
            _logger.warning("fetch_win_probability: get_normalized_dict() returned None/empty")
        elapsed = round((time.monotonic() - t0) * 1000, 1)

        result = {
            "win_probability": norm.get("WinProbPBP", []),
            "game_info": norm.get("GameInfo", []),
        }
        _cache_set(cache_key, result)
        _logger.info("fetch_win_probability(%s): ok in %.1f ms", game_id, elapsed)
        return result
    except Exception as exc:
        _logger.warning("fetch_win_probability(%s) failed: %s", game_id, exc)
        return {}


def fetch_play_by_play(game_id: str) -> list[dict]:
    """
    Return play-by-play events for a game.

    Delegates to ``data.nba_stats_service.get_play_by_play`` for
    consistency with the existing cache layer.

    Endpoint: PlayByPlayV3

    Parameters
    ----------
    game_id : str
        NBA game ID.

    Returns
    -------
    list[dict]
        Each dict contains EVENTNUM, PERIOD, PCTIMESTRING, DESCRIPTION,
        PLAYER1_ID, PLAYER1_NAME, PLAYER2_ID, etc.
        Returns [] on failure.
    """
    if not _is_nba_game_id(game_id):
        _logger.debug("fetch_play_by_play: rejected non-numeric game_id=%r", game_id)
        return []

    try:
        from data.nba_stats_service import get_play_by_play
        return get_play_by_play(game_id)
    except Exception as exc:
        _logger.warning("fetch_play_by_play(%s) failed: %s", game_id, exc)
        return []


def fetch_game_summary(game_id: str) -> dict:
    """
    Return high-level game summary (arena, officials, attendance, etc.).

    Endpoint: nba_api.stats.endpoints.boxscoresummaryv2.BoxScoreSummaryV2

    Parameters
    ----------
    game_id : str
        NBA game ID.

    Returns
    -------
    dict
        Keys: ``game_summary`` (list[dict]), ``other_stats`` (list[dict]),
              ``officials`` (list[dict]), ``inactive_players`` (list[dict]),
              ``game_info`` (list[dict]).
        Returns {} on failure.
    """
    cache_key = f"nlf:game_summary:{game_id}"
    cached = _cache_get(cache_key, LIVE_TTL)
    if cached is not None:
        return cached

    if not _is_nba_game_id(game_id):
        _logger.debug("fetch_game_summary: rejected non-numeric game_id=%r", game_id)
        return {}

    if not _NBA_API_AVAILABLE or not _check_rate_limit():
        _logger.warning("fetch_game_summary: blocked (nba_api=%s, rate_limit=denied)", _NBA_API_AVAILABLE)
        return {}

    try:
        from nba_api.stats.endpoints import boxscoresummaryv2
        time.sleep(_NBA_API_CALL_DELAY)
        t0 = time.monotonic()
        endpoint = boxscoresummaryv2.BoxScoreSummaryV2(game_id=game_id)
        norm = endpoint.get_normalized_dict() or {}
        if not norm:
            _logger.warning("fetch_game_summary: get_normalized_dict() returned None/empty")
        elapsed = round((time.monotonic() - t0) * 1000, 1)

        result = {
            "game_summary": norm.get("GameSummary", []),
            "other_stats": norm.get("OtherStats", []),
            "officials": norm.get("Officials", []),
            "inactive_players": norm.get("InactivePlayers", []),
            "game_info": norm.get("GameInfo", []),
        }
        _cache_set(cache_key, result)
        _logger.info("fetch_game_summary(%s): ok in %.1f ms", game_id, elapsed)
        return result
    except Exception as exc:
        _logger.warning("fetch_game_summary(%s) failed: %s", game_id, exc)
        return {}


def fetch_league_leaders(
    stat_category: str = "PTS",
    season: str | None = None,
) -> list[dict]:
    """
    Return the top players for a given statistical category.

    Endpoint: nba_api.stats.endpoints.leagueleaders.LeagueLeaders

    Parameters
    ----------
    stat_category : str
        Stat to rank by. Common values: 'PTS', 'REB', 'AST', 'STL', 'BLK',
        'FG3M', 'FG_PCT', 'FT_PCT', 'EFF'. Defaults to 'PTS'.
    season : str | None
        Season string in 'YYYY-YY' format. Defaults to current season.

    Returns
    -------
    list[dict]
        Each dict contains PLAYER_ID, PLAYER, TEAM, GP, MIN, RANK, and
        the requested stat column.
        Returns [] on failure.
    """
    season = _resolve_season(season)
    cache_key = f"nlf:league_leaders:{stat_category}:{season}"
    cached = _cache_get(cache_key, HIST_TTL)
    if cached is not None:
        return cached

    if not _NBA_API_AVAILABLE or not _check_rate_limit():
        _logger.warning("fetch_league_leaders: blocked (nba_api=%s, rate_limit=denied)", _NBA_API_AVAILABLE)
        return []

    try:
        from nba_api.stats.endpoints import leagueleaders
        time.sleep(_NBA_API_CALL_DELAY)
        t0 = time.monotonic()
        endpoint = leagueleaders.LeagueLeaders(
            season=season,
            stat_category_abbreviation=stat_category,
            season_type_all_star="Regular Season",
        )
        rows: list[dict] = endpoint.get_data_frames()[0].to_dict("records")
        elapsed = round((time.monotonic() - t0) * 1000, 1)

        _cache_set(cache_key, rows)
        _logger.info(
            "fetch_league_leaders(%s, %s): %d rows in %.1f ms",
            stat_category, season, len(rows), elapsed,
        )
        return rows
    except Exception as exc:
        _logger.warning("fetch_league_leaders(%s, %s) failed: %s", stat_category, season, exc)
        return []


def fetch_team_streak_finder(
    team_id: int,
    season: str | None = None,
) -> list[dict]:
    """
    Return a team's game log to compute streaks and recent form.

    This is a convenience wrapper around ``fetch_team_game_logs`` that
    returns all games in the season (no last_n limit), ordered most-recent
    first.  Callers can compute winning streaks, losing streaks, and rolling
    margin data from the returned list.

    Parameters
    ----------
    team_id : int
        NBA team ID.
    season : str | None
        Season string in 'YYYY-YY' format. Defaults to current season.

    Returns
    -------
    list[dict]
        Full season game log ordered most-recent first.  Same schema as
        ``fetch_team_game_logs``.
        Returns [] on failure.
    """
    return fetch_team_game_logs(team_id=team_id, season=season, last_n=0)
