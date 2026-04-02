# ============================================================
# FILE: data/nba_stats_service.py
# PURPOSE: Centralised service wrapping swar/nba_api endpoints
#          with TTL caching, rate limiting, and graceful
#          degradation.  All imports from nba_api are wrapped in
#          try/except so the app works even if the package is
#          not installed or stats.nba.com is unreachable.
#
# CACHING POLICY:
#   - LIVE_TTL  (300 s)  — standings, play-by-play
#   - HIST_TTL (3600 s)  — game logs, career stats, shots
#
# RATE LIMITING:
#   - Uses the app's existing RateLimiter via get_nba_api_limiter()
#   - Adds a mandatory 1.5 s sleep between every nba_api call to
#     avoid triggering stats.nba.com's aggressive IP throttling
# ============================================================

from __future__ import annotations

import datetime
import logging
import time
from typing import Any

try:
    from utils.logger import get_logger
    _logger = get_logger(__name__)
except ImportError:
    _logger = logging.getLogger(__name__)

# ── Cache ─────────────────────────────────────────────────────────────────────

_CACHE: dict[str, tuple[Any, float]] = {}
LIVE_TTL: int = 300        # 5 minutes  — live/recent data
HIST_TTL: int = 3600       # 1 hour     — historical data

_NBA_API_CALL_DELAY: float = 1.5   # seconds between calls to stats.nba.com


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
    _CACHE[key] = (payload, time.time())


# ── Game-ID validation ────────────────────────────────────────────────────────

def _is_nba_game_id(game_id: str) -> bool:
    """Return True if *game_id* is a numeric string (e.g. ``'0022401234'``)."""
    return bool(game_id) and game_id.isdigit()


# ── Rate limiter ──────────────────────────────────────────────────────────────

def _get_limiter():
    try:
        from utils.rate_limiter import get_nba_api_limiter
        return get_nba_api_limiter()
    except Exception:
        return None


def _check_rate_limit() -> bool:
    """
    Return True if we may proceed, False if the circuit breaker is open.

    Falls back to True (allow) if the rate limiter is unavailable so the
    app continues working even without utils/rate_limiter.py.
    """
    limiter = _get_limiter()
    if limiter is None:
        return True
    try:
        return limiter.acquire()
    except Exception:
        return True


# ── Season helper ─────────────────────────────────────────────────────────────

def _current_season() -> str:
    """Return the current NBA season string in 'YYYY-YY' format."""
    now = datetime.date.today()
    year = now.year if now.month >= 10 else now.year - 1
    return f"{year}-{str(year + 1)[-2:]}"


def _resolve_season(season: str | None) -> str:
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


# ── nba_api availability guard ────────────────────────────────────────────────

try:
    import nba_api  # noqa: F401 – availability check only
    _NBA_API_AVAILABLE = True
except ImportError:
    _NBA_API_AVAILABLE = False


# ══════════════════════════════════════════════════════════════════════════════
# PUBLIC FUNCTIONS
# ══════════════════════════════════════════════════════════════════════════════


def get_all_players(active_only: bool = True) -> list[dict]:
    """
    Return all NBA players from nba_api's CommonAllPlayers endpoint.

    Parameters
    ----------
    active_only : bool
        When True (default) only current-season players are returned.

    Returns
    -------
    list[dict]
        Each dict has: id, full_name, first_name, last_name,
        is_active, team_id, team_abbreviation
    """
    cache_key = f"all_players:{active_only}"
    cached = _cache_get(cache_key, HIST_TTL)
    if cached is not None:
        return cached

    if not _NBA_API_AVAILABLE or not _check_rate_limit():
        _logger.warning("get_all_players: blocked (nba_api=%s, rate_limit=denied)", _NBA_API_AVAILABLE)
        return []

    try:
        from nba_api.stats.endpoints import commonallplayers
        t0 = time.monotonic()
        time.sleep(_NBA_API_CALL_DELAY)
        endpoint = commonallplayers.CommonAllPlayers(
            is_only_current_season=1 if active_only else 0,
            league_id="00",
        )
        _norm_tmp = endpoint.get_normalized_dict() or {}
        if not _norm_tmp:
            _logger.warning("get_all_players: get_normalized_dict() returned None/empty")
        rows = _norm_tmp.get("CommonAllPlayers", [])
        elapsed = round((time.monotonic() - t0) * 1000, 1)

        players = [
            {
                "id": int(r.get("PERSON_ID", 0)),
                "full_name": str(r.get("DISPLAY_FIRST_LAST", "")),
                "first_name": (lambda parts: parts[0] if parts else "")(
                    str(r.get("DISPLAY_FIRST_LAST", "")).split()
                ),
                "last_name": str(r.get("DISPLAY_LAST_COMMA_FIRST", "")).split(",")[0].strip() if r.get("DISPLAY_LAST_COMMA_FIRST") else "",
                "is_active": bool(r.get("ROSTERSTATUS", 0)),
                "team_id": r.get("TEAM_ID"),
                "team_abbreviation": str(r.get("TEAM_ABBREVIATION", "")),
            }
            for r in rows
        ]

        _cache_set(cache_key, players)
        _logger.info("get_all_players: %d players in %.1f ms", len(players), elapsed)
        return players

    except Exception as exc:
        _logger.warning("get_all_players failed: %s", exc)
        return []


def get_player_info(player_id: int) -> dict:
    """
    Return bio / draft / physical info for a single player.

    Uses nba_api's CommonPlayerInfo endpoint.

    Returns
    -------
    dict
        Keys: id, full_name, position, height, weight, country,
              birthdate, draft_year, draft_round, draft_number,
              school, team_id, team_abbreviation, jersey, from_year,
              to_year
        Returns {} on failure.
    """
    cache_key = f"player_info:{player_id}"
    cached = _cache_get(cache_key, HIST_TTL)
    if cached is not None:
        return cached

    if not _NBA_API_AVAILABLE or not _check_rate_limit():
        _logger.warning("get_player_info: blocked (nba_api=%s, rate_limit=denied)", _NBA_API_AVAILABLE)
        return {}

    try:
        from nba_api.stats.endpoints import commonplayerinfo
        t0 = time.monotonic()
        time.sleep(_NBA_API_CALL_DELAY)
        endpoint = commonplayerinfo.CommonPlayerInfo(player_id=player_id)
        norm = endpoint.get_normalized_dict() or {}
        if not norm:
            _logger.warning("get_player_info: get_normalized_dict() returned None/empty")
        rows = norm.get("CommonPlayerInfo", [])
        elapsed = round((time.monotonic() - t0) * 1000, 1)

        if not rows:
            return {}

        r = rows[0]
        info = {
            "id": player_id,
            "full_name": str(r.get("DISPLAY_FIRST_LAST", "")),
            "position": str(r.get("POSITION", "")),
            "height": str(r.get("HEIGHT", "")),
            "weight": str(r.get("WEIGHT", "")),
            "country": str(r.get("COUNTRY", "")),
            "birthdate": str(r.get("BIRTHDATE", "")),
            "draft_year": r.get("DRAFT_YEAR"),
            "draft_round": r.get("DRAFT_ROUND"),
            "draft_number": r.get("DRAFT_NUMBER"),
            "school": str(r.get("SCHOOL", "")),
            "team_id": r.get("TEAM_ID"),
            "team_abbreviation": str(r.get("TEAM_ABBREVIATION", "")),
            "jersey": str(r.get("JERSEY", "")),
            "from_year": r.get("FROM_YEAR"),
            "to_year": r.get("TO_YEAR"),
        }

        _cache_set(cache_key, info)
        _logger.info("get_player_info(%s): ok in %.1f ms", player_id, elapsed)
        return info

    except Exception as exc:
        _logger.warning("get_player_info(%s) failed: %s", player_id, exc)
        return {}


def get_player_game_logs(player_id: int, season: str | None = None) -> list[dict]:
    """
    Return per-game stats for a player in a given season.

    Uses nba_api's PlayerGameLog endpoint.  Results are cached for 1 hour
    (historical data).

    Returns
    -------
    list[dict]
        Each dict mirrors the PlayerGameLog DataFrame columns (PTS, REB,
        AST, FG3M, STL, BLK, TOV, MIN, GAME_DATE, MATCHUP, WL …).
        Returns [] on failure.
    """
    season = _resolve_season(season)
    cache_key = f"player_game_logs:{player_id}:{season}"
    cached = _cache_get(cache_key, HIST_TTL)
    if cached is not None:
        return cached

    if not _NBA_API_AVAILABLE or not _check_rate_limit():
        _logger.warning("get_player_game_logs: blocked (nba_api=%s, rate_limit=denied)", _NBA_API_AVAILABLE)
        return []

    try:
        from nba_api.stats.endpoints import playergamelog
        t0 = time.monotonic()
        time.sleep(_NBA_API_CALL_DELAY)
        endpoint = playergamelog.PlayerGameLog(
            player_id=player_id,
            season=season,
            season_type_all_star="Regular Season",
        )
        logs = endpoint.get_data_frames()[0].to_dict("records")
        elapsed = round((time.monotonic() - t0) * 1000, 1)

        _cache_set(cache_key, logs)
        _logger.info(
            "get_player_game_logs(%s, %s): %d games in %.1f ms",
            player_id, season, len(logs), elapsed,
        )
        return logs

    except Exception as exc:
        _logger.warning("get_player_game_logs(%s, %s) failed: %s", player_id, season, exc)
        return []


def get_player_career_stats(player_id: int) -> dict:
    """
    Return career stats summary for a player.

    Uses nba_api's PlayerCareerStats endpoint.

    Returns
    -------
    dict
        Keys: season_totals_regular_season (list[dict]),
              career_totals_regular_season (list[dict]).
        Returns {} on failure.
    """
    cache_key = f"player_career_stats:{player_id}"
    cached = _cache_get(cache_key, HIST_TTL)
    if cached is not None:
        return cached

    if not _NBA_API_AVAILABLE or not _check_rate_limit():
        _logger.warning("get_player_career_stats: blocked (nba_api=%s, rate_limit=denied)", _NBA_API_AVAILABLE)
        return {}

    try:
        from nba_api.stats.endpoints import playercareerstats
        t0 = time.monotonic()
        time.sleep(_NBA_API_CALL_DELAY)
        endpoint = playercareerstats.PlayerCareerStats(player_id=player_id)
        norm = endpoint.get_normalized_dict() or {}
        if not norm:
            _logger.warning("get_player_career_stats: get_normalized_dict() returned None/empty")
        elapsed = round((time.monotonic() - t0) * 1000, 1)

        result = {
            "season_totals_regular_season": norm.get("SeasonTotalsRegularSeason", []),
            "career_totals_regular_season": norm.get("CareerTotalsRegularSeason", []),
        }

        _cache_set(cache_key, result)
        _logger.info("get_player_career_stats(%s): ok in %.1f ms", player_id, elapsed)
        return result

    except Exception as exc:
        _logger.warning("get_player_career_stats(%s) failed: %s", player_id, exc)
        return {}


def get_player_splits(player_id: int, season: str | None = None) -> dict:
    """
    Return home/away and last-N-games splits for a player.

    Uses nba_api's PlayerDashboardByLastNGames and
    PlayerDashboardByGeneralSplits endpoints.

    Returns
    -------
    dict
        Keys: last_5_games, last_10_games, home, away (each a list[dict]).
        Returns {} on failure.
    """
    season = _resolve_season(season)
    cache_key = f"player_splits:{player_id}:{season}"
    cached = _cache_get(cache_key, HIST_TTL)
    if cached is not None:
        return cached

    if not _NBA_API_AVAILABLE or not _check_rate_limit():
        _logger.warning("get_player_splits: blocked (nba_api=%s, rate_limit=denied)", _NBA_API_AVAILABLE)
        return {}

    result: dict = {}

    try:
        from nba_api.stats.endpoints import (
            playerdashboardbylastngames,
            playerdashboardbygeneralsplits,
        )
        t0 = time.monotonic()

        # Last-N-games splits
        time.sleep(_NBA_API_CALL_DELAY)
        last_n = playerdashboardbylastngames.PlayerDashboardByLastNGames(
            player_id=player_id,
            season=season,
        )
        last_n_norm = last_n.get_normalized_dict() or {}
        result["last_5_games"] = last_n_norm.get("Last5Games", [])
        result["last_10_games"] = last_n_norm.get("Last10Games", [])

        # General (home/away/win/loss) splits
        time.sleep(_NBA_API_CALL_DELAY)
        general = playerdashboardbygeneralsplits.PlayerDashboardByGeneralSplits(
            player_id=player_id,
            season=season,
        )
        general_norm = general.get_normalized_dict() or {}
        home_away = general_norm.get("LocationPlayerDashboard", [])
        result["home"] = [r for r in home_away if r.get("GROUP_VALUE") == "Home"]
        result["away"] = [r for r in home_away if r.get("GROUP_VALUE") == "Road"]

        elapsed = round((time.monotonic() - t0) * 1000, 1)
        _cache_set(cache_key, result)
        _logger.info("get_player_splits(%s, %s): ok in %.1f ms", player_id, season, elapsed)
        return result

    except Exception as exc:
        _logger.warning("get_player_splits(%s, %s) failed: %s", player_id, season, exc)
        return result if result else {}


def get_advanced_box_score(game_id: str) -> dict:
    """
    Return advanced box-score stats (eFG%, TS%, usage%) for a game.

    Uses nba_api's BoxScoreAdvancedV3 endpoint.

    Returns
    -------
    dict
        Keys: player_stats (list[dict]), team_stats (list[dict]).
        Returns {} on failure.
    """
    cache_key = f"advanced_box_score:{game_id}"
    cached = _cache_get(cache_key, LIVE_TTL)
    if cached is not None:
        return cached

    if not _is_nba_game_id(game_id):
        _logger.debug("get_advanced_box_score: rejected non-numeric game_id=%r", game_id)
        return {}

    if not _NBA_API_AVAILABLE or not _check_rate_limit():
        _logger.warning("get_advanced_box_score: blocked (nba_api=%s, rate_limit=denied)", _NBA_API_AVAILABLE)
        return {}

    try:
        from nba_api.stats.endpoints import boxscoreadvancedv3
        t0 = time.monotonic()
        time.sleep(_NBA_API_CALL_DELAY)
        endpoint = boxscoreadvancedv3.BoxScoreAdvancedV3(game_id=game_id)
        norm = endpoint.get_normalized_dict() or {}
        if not norm:
            _logger.warning("get_advanced_box_score: get_normalized_dict() returned None/empty")
        elapsed = round((time.monotonic() - t0) * 1000, 1)

        result = {
            "player_stats": norm.get("PlayerStats", []),
            "team_stats": norm.get("TeamStats", []),
        }

        _cache_set(cache_key, result)
        _logger.info("get_advanced_box_score(%s): ok in %.1f ms", game_id, elapsed)
        return result

    except Exception as exc:
        _logger.warning("get_advanced_box_score(%s) failed: %s", game_id, exc)
        return {}


def get_player_tracking_box_score(game_id: str) -> dict:
    """
    Return player-tracking box-score stats (speed, distance, touches).

    Uses nba_api's BoxScorePlayerTrackV3 endpoint.

    Returns
    -------
    dict
        Keys: player_stats (list[dict]).
        Returns {} on failure.
    """
    cache_key = f"player_tracking_box_score:{game_id}"
    cached = _cache_get(cache_key, LIVE_TTL)
    if cached is not None:
        return cached

    if not _is_nba_game_id(game_id):
        _logger.debug("get_player_tracking_box_score: rejected non-numeric game_id=%r", game_id)
        return {}

    if not _NBA_API_AVAILABLE or not _check_rate_limit():
        _logger.warning("get_player_tracking_box_score: blocked (nba_api=%s, rate_limit=denied)", _NBA_API_AVAILABLE)
        return {}

    try:
        from nba_api.stats.endpoints import boxscoreplayertrackv3
        t0 = time.monotonic()
        time.sleep(_NBA_API_CALL_DELAY)
        endpoint = boxscoreplayertrackv3.BoxScorePlayerTrackV3(game_id=game_id)
        norm = endpoint.get_normalized_dict() or {}
        if not norm:
            _logger.warning("get_player_tracking_box_score: get_normalized_dict() returned None/empty")
        elapsed = round((time.monotonic() - t0) * 1000, 1)

        result = {"player_stats": norm.get("PlayerStats", [])}
        _cache_set(cache_key, result)
        _logger.info(
            "get_player_tracking_box_score(%s): ok in %.1f ms", game_id, elapsed
        )
        return result

    except Exception as exc:
        _logger.warning("get_player_tracking_box_score(%s) failed: %s", game_id, exc)
        return {}


def get_hustle_box_score(game_id: str) -> dict:
    """
    Return hustle-stats box score (deflections, loose balls, charges).

    Uses nba_api's BoxScoreHustleV2 endpoint.

    Returns
    -------
    dict
        Keys: player_stats (list[dict]).
        Returns {} on failure.
    """
    cache_key = f"hustle_box_score:{game_id}"
    cached = _cache_get(cache_key, LIVE_TTL)
    if cached is not None:
        return cached

    if not _is_nba_game_id(game_id):
        _logger.debug("get_hustle_box_score: rejected non-numeric game_id=%r", game_id)
        return {}

    if not _NBA_API_AVAILABLE or not _check_rate_limit():
        _logger.warning("get_hustle_box_score: blocked (nba_api=%s, rate_limit=denied)", _NBA_API_AVAILABLE)
        return {}

    try:
        from nba_api.stats.endpoints import boxscorehustlev2
        t0 = time.monotonic()
        time.sleep(_NBA_API_CALL_DELAY)
        endpoint = boxscorehustlev2.BoxScoreHustleV2(game_id=game_id)
        norm = endpoint.get_normalized_dict() or {}
        if not norm:
            _logger.warning("get_hustle_box_score: get_normalized_dict() returned None/empty")
        elapsed = round((time.monotonic() - t0) * 1000, 1)

        result = {"player_stats": norm.get("PlayerStats", [])}
        _cache_set(cache_key, result)
        _logger.info("get_hustle_box_score(%s): ok in %.1f ms", game_id, elapsed)
        return result

    except Exception as exc:
        _logger.warning("get_hustle_box_score(%s) failed: %s", game_id, exc)
        return {}


def get_defensive_matchup_data(
    season: str | None = None,
    per_mode: str = "PerGame",
) -> list[dict]:
    """
    Return league-wide defensive-matchup data.

    Uses nba_api's LeagueDashPtDefend endpoint.

    Returns
    -------
    list[dict]
        League-wide defender stats rows.  Returns [] on failure.
    """
    season = _resolve_season(season)
    cache_key = f"defensive_matchup:{season}:{per_mode}"
    cached = _cache_get(cache_key, HIST_TTL)
    if cached is not None:
        return cached

    if not _NBA_API_AVAILABLE or not _check_rate_limit():
        _logger.warning("get_defensive_matchup_data: blocked (nba_api=%s, rate_limit=denied)", _NBA_API_AVAILABLE)
        return []

    try:
        from nba_api.stats.endpoints import leaguedashptdefend
        t0 = time.monotonic()
        time.sleep(_NBA_API_CALL_DELAY)
        endpoint = leaguedashptdefend.LeagueDashPtDefend(
            season=season,
            per_mode_simple=per_mode,
        )
        norm = endpoint.get_normalized_dict() or {}
        if not norm:
            _logger.warning("get_defensive_matchup_data: get_normalized_dict() returned None/empty")
        rows = norm.get("LeagueDashPtDefend", [])
        elapsed = round((time.monotonic() - t0) * 1000, 1)

        _cache_set(cache_key, rows)
        _logger.info(
            "get_defensive_matchup_data(%s): %d rows in %.1f ms",
            season, len(rows), elapsed,
        )
        return rows

    except Exception as exc:
        _logger.warning("get_defensive_matchup_data(%s) failed: %s", season, exc)
        return []


def get_shot_chart(player_id: int, season: str | None = None) -> list[dict]:
    """
    Return shot-chart detail for a player in a given season.

    Uses nba_api's ShotChartDetail endpoint.

    Returns
    -------
    list[dict]
        Each dict represents a single shot attempt.  Returns [] on failure.
    """
    season = _resolve_season(season)
    cache_key = f"shot_chart:{player_id}:{season}"
    cached = _cache_get(cache_key, HIST_TTL)
    if cached is not None:
        return cached

    if not _NBA_API_AVAILABLE or not _check_rate_limit():
        _logger.warning("get_shot_chart: blocked (nba_api=%s, rate_limit=denied)", _NBA_API_AVAILABLE)
        return []

    try:
        from nba_api.stats.endpoints import shotchartdetail
        t0 = time.monotonic()
        time.sleep(_NBA_API_CALL_DELAY)
        endpoint = shotchartdetail.ShotChartDetail(
            player_id=player_id,
            team_id=0,
            season_nullable=season,
            context_measure_simple="FGA",
        )
        norm = endpoint.get_normalized_dict() or {}
        if not norm:
            _logger.warning("get_shot_chart: get_normalized_dict() returned None/empty")
        rows = norm.get("Shot_Chart_Detail", [])
        elapsed = round((time.monotonic() - t0) * 1000, 1)

        _cache_set(cache_key, rows)
        _logger.info(
            "get_shot_chart(%s, %s): %d shots in %.1f ms",
            player_id, season, len(rows), elapsed,
        )
        return rows

    except Exception as exc:
        _logger.warning("get_shot_chart(%s, %s) failed: %s", player_id, season, exc)
        return []


def get_lineup_stats(
    season: str | None = None,
    group_quantity: int = 5,
) -> list[dict]:
    """
    Return league-wide lineup stats.

    Uses nba_api's LeagueDashLineups endpoint.

    Returns
    -------
    list[dict]
        Each dict represents one lineup's stats.  Returns [] on failure.
    """
    season = _resolve_season(season)
    cache_key = f"lineup_stats:{season}:{group_quantity}"
    cached = _cache_get(cache_key, HIST_TTL)
    if cached is not None:
        return cached

    if not _NBA_API_AVAILABLE or not _check_rate_limit():
        _logger.warning("get_lineup_stats: blocked (nba_api=%s, rate_limit=denied)", _NBA_API_AVAILABLE)
        return []

    try:
        from nba_api.stats.endpoints import leaguedashlineups
        t0 = time.monotonic()
        time.sleep(_NBA_API_CALL_DELAY)
        endpoint = leaguedashlineups.LeagueDashLineups(
            season=season,
            group_quantity=group_quantity,
        )
        norm = endpoint.get_normalized_dict() or {}
        if not norm:
            _logger.warning("get_lineup_stats: get_normalized_dict() returned None/empty")
        rows = norm.get("Lineups", [])
        elapsed = round((time.monotonic() - t0) * 1000, 1)

        _cache_set(cache_key, rows)
        _logger.info(
            "get_lineup_stats(%s, %d): %d lineups in %.1f ms",
            season, group_quantity, len(rows), elapsed,
        )
        return rows

    except Exception as exc:
        _logger.warning(
            "get_lineup_stats(%s, %d) failed: %s", season, group_quantity, exc
        )
        return []


def get_league_standings(season: str | None = None) -> list[dict]:
    """
    Return current NBA league standings.

    Uses nba_api's LeagueStandingsV3 endpoint.

    Returns
    -------
    list[dict]
        Each dict has: team_abbreviation, conference, conference_rank,
        wins, losses, win_pct, streak, last_10.
        Returns [] on failure.
    """
    season = _resolve_season(season)
    cache_key = f"league_standings:{season}"
    cached = _cache_get(cache_key, LIVE_TTL)
    if cached is not None:
        return cached

    if not _NBA_API_AVAILABLE or not _check_rate_limit():
        _logger.warning("get_league_standings: blocked (nba_api=%s, rate_limit=denied)", _NBA_API_AVAILABLE)
        return []

    try:
        from nba_api.stats.endpoints import leaguestandingsv3
        t0 = time.monotonic()
        time.sleep(_NBA_API_CALL_DELAY)
        endpoint = leaguestandingsv3.LeagueStandingsV3(season=season)
        df = endpoint.get_data_frames()[0]
        elapsed = round((time.monotonic() - t0) * 1000, 1)

        standings = []
        for _, row in df.iterrows():
            standings.append({
                "team_abbreviation": str(row.get("TeamAbbreviation", "")),
                "team_name": f"{row.get('TeamCity', '')} {row.get('TeamName', '')}".strip(),
                "conference": str(row.get("Conference", "")),
                "conference_rank": int(row.get("PlayoffRank", 0)),
                "wins": int(row.get("WINS", 0)),
                "losses": int(row.get("LOSSES", 0)),
                "win_pct": float(row.get("WinPCT", 0.0)),
                "streak": str(row.get("strCurrentStreak", "")),
                "last_10": str(row.get("L10", "")),
            })

        _cache_set(cache_key, standings)
        _logger.info(
            "get_league_standings(%s): %d teams in %.1f ms",
            season, len(standings), elapsed,
        )
        return standings

    except Exception as exc:
        _logger.warning("get_league_standings(%s) failed: %s", season, exc)
        return []


def find_games(
    team_id: int | None = None,
    season: str | None = None,
    date_from: str | None = None,
    date_to: str | None = None,
) -> list[dict]:
    """
    Search for games by team, season, or date range.

    Uses nba_api's LeagueGameFinder endpoint.

    Parameters
    ----------
    team_id : int | None
        Filter by NBA team ID.
    season : str | None
        Season string (e.g. "2024-25").  Defaults to current season.
    date_from : str | None
        Start date in "MM/DD/YYYY" format.
    date_to : str | None
        End date in "MM/DD/YYYY" format.

    Returns
    -------
    list[dict]
        Each dict represents one game row.  Returns [] on failure.
    """
    season = _resolve_season(season)
    cache_key = f"find_games:{team_id}:{season}:{date_from}:{date_to}"
    cached = _cache_get(cache_key, LIVE_TTL)
    if cached is not None:
        return cached

    if not _NBA_API_AVAILABLE or not _check_rate_limit():
        _logger.warning("find_games: blocked (nba_api=%s, rate_limit=denied)", _NBA_API_AVAILABLE)
        return []

    try:
        from nba_api.stats.endpoints import leaguegamefinder
        t0 = time.monotonic()

        kwargs: dict = {"league_id_nullable": "00", "season_nullable": season}
        if team_id is not None:
            kwargs["team_id_nullable"] = team_id
        if date_from:
            kwargs["date_from_nullable"] = date_from
        if date_to:
            kwargs["date_to_nullable"] = date_to

        time.sleep(_NBA_API_CALL_DELAY)
        endpoint = leaguegamefinder.LeagueGameFinder(**kwargs)
        norm = endpoint.get_normalized_dict() or {}
        if not norm:
            _logger.warning("find_games: get_normalized_dict() returned None/empty")
        rows = norm.get("LeagueGameFinderResults", [])
        elapsed = round((time.monotonic() - t0) * 1000, 1)

        _cache_set(cache_key, rows)
        _logger.info(
            "find_games(team=%s, season=%s): %d games in %.1f ms",
            team_id, season, len(rows), elapsed,
        )
        return rows

    except Exception as exc:
        _logger.warning("find_games failed: %s", exc)
        return []


def get_play_by_play(game_id: str) -> list[dict]:
    """
    Return play-by-play data for a game.

    Uses nba_api's PlayByPlayV3 endpoint.

    Returns
    -------
    list[dict]
        Each dict represents one play.  Returns [] on failure.
    """
    cache_key = f"play_by_play:{game_id}"
    cached = _cache_get(cache_key, LIVE_TTL)
    if cached is not None:
        return cached

    if not _is_nba_game_id(game_id):
        _logger.debug("get_play_by_play: rejected non-numeric game_id=%r", game_id)
        return []

    if not _NBA_API_AVAILABLE or not _check_rate_limit():
        _logger.warning("get_play_by_play: blocked (nba_api=%s, rate_limit=denied)", _NBA_API_AVAILABLE)
        return []

    try:
        from nba_api.stats.endpoints import playbyplayv3
        t0 = time.monotonic()
        time.sleep(_NBA_API_CALL_DELAY)
        endpoint = playbyplayv3.PlayByPlayV3(game_id=game_id)
        norm = endpoint.get_normalized_dict() or {}
        if not norm:
            _logger.warning("get_play_by_play: get_normalized_dict() returned None/empty")
        rows = norm.get("PlayByPlay", [])
        elapsed = round((time.monotonic() - t0) * 1000, 1)

        _cache_set(cache_key, rows)
        _logger.info(
            "get_play_by_play(%s): %d plays in %.1f ms", game_id, len(rows), elapsed
        )
        return rows

    except Exception as exc:
        _logger.warning("get_play_by_play(%s) failed: %s", game_id, exc)
        return []


def get_team_roster(team_id: int, season: str | None = None) -> list[dict]:
    """
    Return the roster for a team in a given season.

    Uses nba_api's CommonTeamRoster endpoint.

    Returns
    -------
    list[dict]
        Each dict has: player_id, player_name, position, jersey,
        height, weight, birth_date, exp.
        Returns [] on failure.
    """
    season = _resolve_season(season)
    cache_key = f"team_roster:{team_id}:{season}"
    cached = _cache_get(cache_key, HIST_TTL)
    if cached is not None:
        return cached

    if not _NBA_API_AVAILABLE or not _check_rate_limit():
        _logger.warning("get_team_roster: blocked (nba_api=%s, rate_limit=denied)", _NBA_API_AVAILABLE)
        return []

    try:
        from nba_api.stats.endpoints import commonteamroster
        t0 = time.monotonic()
        time.sleep(_NBA_API_CALL_DELAY)
        endpoint = commonteamroster.CommonTeamRoster(
            team_id=team_id,
            season=season,
        )
        norm = endpoint.get_normalized_dict() or {}
        if not norm:
            _logger.warning("get_team_roster: get_normalized_dict() returned None/empty")
        rows = norm.get("CommonTeamRoster", [])
        elapsed = round((time.monotonic() - t0) * 1000, 1)

        roster = [
            {
                "player_id": r.get("PLAYER_ID"),
                "player_name": str(r.get("PLAYER", "")),
                "position": str(r.get("POSITION", "")),
                "jersey": str(r.get("NUM", "")),
                "height": str(r.get("HEIGHT", "")),
                "weight": str(r.get("WEIGHT", "")),
                "birth_date": str(r.get("BIRTH_DATE", "")),
                "exp": r.get("EXP"),
            }
            for r in rows
        ]

        _cache_set(cache_key, roster)
        _logger.info(
            "get_team_roster(%s, %s): %d players in %.1f ms",
            team_id, season, len(roster), elapsed,
        )
        return roster

    except Exception as exc:
        _logger.warning("get_team_roster(%s, %s) failed: %s", team_id, season, exc)
        return []
