# ============================================================
# FILE: data/advanced_fetcher.py
# PURPOSE: High-level pre-game enrichment pipeline.
#          Given tonight's slate of games, pre-fetches all
#          advanced NBA data (team dashboards, lineups, player
#          metrics, rotation data, standings) in a single
#          coordinated batch and returns it keyed by game_id.
#
# CONNECTS TO:
#   - data/nba_live_fetcher.py — the NBA API gateway
#   - data/nba_data_service.py — public wrappers
#   - pages/9_📡_Data_Feed.py — "Deep Fetch" button
#
# USAGE:
#   from data.advanced_fetcher import enrich_tonights_slate
#   enriched = enrich_tonights_slate(games, progress_callback=st.progress)
#
# DESIGN PRINCIPLES:
#   - Respects rate limits (all calls route through nba_live_fetcher)
#   - Caches aggressively (HIST_TTL for historical, LIVE_TTL for live)
#   - Returns a nested dict keyed by game_id with all enrichment nested
#   - Never raises — any failed fetch returns {} / [] gracefully
#   - Callable from both Streamlit pages and background scripts
# ============================================================

from __future__ import annotations

import logging
import time
from typing import Any, Callable

try:
    from utils.log_helper import get_logger
    _logger = get_logger(__name__)
except ImportError:
    _logger = logging.getLogger(__name__)


# ── Enrichment constants ───────────────────────────────────────────────────────

# Number of recent team games to fetch for form analysis.
# 10 games represents ~2 weeks of NBA play (teams play 3-4 games/week),
# giving a statistically meaningful sample that's recent enough to capture
# current rotation patterns and blowout tendencies.
RECENT_TEAM_GAMES = 10

# Minimum delay between per-game enrichment batches (seconds)
# This is in ADDITION to the per-call delay inside nba_live_fetcher.
_BATCH_INTER_GAME_DELAY: float = 0.2


def enrich_tonights_slate(
    games: list[dict] | None,
    season: str | None = None,
    progress_callback: Callable[[int, int, str], None] | None = None,
) -> dict[str, dict]:
    """
    For each game in tonight's slate, pre-fetch all advanced enrichment data.

    For every game the following data is fetched (all optional/graceful):
    - Home & away team recent game logs (last 10)
    - Home & away team lineups (5-man units)
    - Player estimated metrics for all rostered players
    - Home & away team dashboard (home/away/days-rest splits)
    - Current league standings
    - Today's full scoreboard

    Results are cached aggressively via the nba_live_fetcher TTL cache so
    subsequent calls within the cache window are instant.

    Parameters
    ----------
    games : list[dict]
        Tonight's games as returned by ``get_todays_games()`` /
        ``get_schedule()``.  Each dict must contain at least one of:
        - ``game_id`` or ``GAME_ID`` — for live box-score lookups
        - ``home_team_id`` / ``HOME_TEAM_ID`` — NBA team ID for home side
        - ``away_team_id`` / ``VISITOR_TEAM_ID`` — NBA team ID for away side
    season : str | None
        Season string in 'YYYY-YY' format (e.g. '2025-26').  Defaults to
        the current season as determined by nba_live_fetcher.
    progress_callback : callable | None
        Optional callable invoked after each step:
        ``callback(current_step: int, total_steps: int, message: str)``
        Suitable for Streamlit progress bars.

    Returns
    -------
    dict[str, dict]
        Keyed by ``game_id`` (str).  Each value is a dict with keys:
        - ``game_id``          (str)
        - ``home_team_id``     (int | None)
        - ``away_team_id``     (int | None)
        - ``home_game_logs``   (list[dict])
        - ``away_game_logs``   (list[dict])
        - ``home_lineups``     (list[dict])
        - ``away_lineups``     (list[dict])
        - ``home_dashboard``   (dict)
        - ``away_dashboard``   (dict)
        - ``standings``        (list[dict])
        - ``player_metrics``   (list[dict])
        - ``scoreboard``       (dict)
        - ``fetch_time``       (float) — Unix timestamp of enrichment

    Example
    -------
    games = get_todays_games()
    enriched = enrich_tonights_slate(games)
    heat_logs = enriched.get("0022401234", {}).get("home_game_logs", [])
    """
    if not games:  # handles both None and empty list
        _logger.info("enrich_tonights_slate: no games to enrich")
        return {}

    try:
        from data.nba_data_service import (
            get_team_game_logs,
            get_team_lineups,
            get_team_dashboard,
            get_player_estimated_metrics,
            get_standings_from_nba_api,
            get_todays_scoreboard,
        )
    except Exception as _import_err:
        _logger.warning(
            "enrich_tonights_slate: nba_data_service not available — %s", _import_err
        )
        return {}

    # ── Pre-fetch shared data (once for the whole slate) ──────────────────────
    _total_steps = len(games) * 6 + 3  # 6 per-game + 3 shared
    _step = 0

    def _tick(msg: str) -> None:
        nonlocal _step
        _step += 1
        if progress_callback:
            try:
                progress_callback(_step, _total_steps, msg)
            except Exception as exc:
                _logger.debug("enrich_tonights_slate: progress callback failed — %s", exc)

    # Shared: standings
    _tick("Fetching league standings…")
    standings: list[dict] = []
    try:
        standings = get_standings_from_nba_api(season=season) or []
    except Exception as _exc:
        _logger.debug("enrich_tonights_slate: standings fetch failed — %s", _exc)

    # Shared: player estimated metrics
    _tick("Fetching player estimated metrics…")
    player_metrics: list[dict] = []
    try:
        player_metrics = get_player_estimated_metrics(season=season) or []
    except Exception as _exc:
        _logger.debug("enrich_tonights_slate: estimated metrics fetch failed — %s", _exc)

    # Shared: today's scoreboard
    _tick("Fetching today's scoreboard…")
    scoreboard: dict = {}
    try:
        scoreboard = get_todays_scoreboard() or {}
    except Exception as _exc:
        _logger.debug("enrich_tonights_slate: scoreboard fetch failed — %s", _exc)

    # ── Per-game enrichment ───────────────────────────────────────────────────
    result: dict[str, dict] = {}

    for game in games:
        game_id = str(
            game.get("game_id") or game.get("GAME_ID") or game.get("gameId") or ""
        )
        home_team_id = _resolve_team_id(game, "home")
        away_team_id = _resolve_team_id(game, "away")

        enrichment: dict[str, Any] = {
            "game_id": game_id,
            "home_team_id": home_team_id,
            "away_team_id": away_team_id,
            "home_game_logs": [],
            "away_game_logs": [],
            "home_lineups": [],
            "away_lineups": [],
            "home_dashboard": {},
            "away_dashboard": {},
            "standings": standings,
            "player_metrics": player_metrics,
            "scoreboard": scoreboard,
            "fetch_time": time.time(),
        }

        # Team game logs
        _tick(f"Game {game_id}: fetching home team logs…")
        if home_team_id:
            try:
                enrichment["home_game_logs"] = (
                    get_team_game_logs(home_team_id, season=season, last_n=RECENT_TEAM_GAMES) or []
                )
            except Exception as _exc:
                _logger.debug("enrich: home game logs failed for %s — %s", game_id, _exc)

        _tick(f"Game {game_id}: fetching away team logs…")
        if away_team_id:
            try:
                enrichment["away_game_logs"] = (
                    get_team_game_logs(away_team_id, season=season, last_n=RECENT_TEAM_GAMES) or []
                )
            except Exception as _exc:
                _logger.debug("enrich: away game logs failed for %s — %s", game_id, _exc)

        # Team lineups
        _tick(f"Game {game_id}: fetching lineups…")
        if home_team_id:
            try:
                enrichment["home_lineups"] = get_team_lineups(home_team_id, season=season) or []
            except Exception as _exc:
                _logger.debug("enrich: home lineups failed for %s — %s", game_id, _exc)
        if away_team_id:
            try:
                enrichment["away_lineups"] = get_team_lineups(away_team_id, season=season) or []
            except Exception as _exc:
                _logger.debug("enrich: away lineups failed for %s — %s", game_id, _exc)

        # Team dashboards
        _tick(f"Game {game_id}: fetching team dashboards…")
        if home_team_id:
            try:
                enrichment["home_dashboard"] = get_team_dashboard(home_team_id, season=season) or {}
            except Exception as _exc:
                _logger.debug("enrich: home dashboard failed for %s — %s", game_id, _exc)
        if away_team_id:
            try:
                enrichment["away_dashboard"] = get_team_dashboard(away_team_id, season=season) or {}
            except Exception as _exc:
                _logger.debug("enrich: away dashboard failed for %s — %s", game_id, _exc)

        # Build summary counters for logging
        _log_enrichment_summary(game_id, enrichment)

        result[game_id] = enrichment

        # Small delay between games to keep rate-limit headroom
        time.sleep(_BATCH_INTER_GAME_DELAY)

    _logger.info(
        "enrich_tonights_slate: enriched %d game(s), standings=%d, metrics=%d",
        len(result), len(standings), len(player_metrics),
    )
    return result


def build_enrichment_summary(enriched: dict[str, dict]) -> dict:
    """
    Build a human-readable summary of an enrichment run.

    Parameters
    ----------
    enriched : dict[str, dict]
        Return value of ``enrich_tonights_slate``.

    Returns
    -------
    dict
        Keys:
        - ``games_enriched``      (int)
        - ``game_logs_fetched``   (int)  — total across all teams
        - ``lineups_fetched``     (int)  — total 5-man unit rows
        - ``dashboards_fetched``  (int)  — teams with dashboard data
        - ``standings_rows``      (int)
        - ``player_metrics_rows`` (int)
        - ``scoreboard_available``(bool)
    """
    games_enriched = len(enriched)
    game_logs_fetched = 0
    lineups_fetched = 0
    dashboards_fetched = 0
    standings_rows = 0
    player_metrics_rows = 0
    scoreboard_available = False

    for game_data in enriched.values():
        game_logs_fetched += len(game_data.get("home_game_logs", []))
        game_logs_fetched += len(game_data.get("away_game_logs", []))
        lineups_fetched += len(game_data.get("home_lineups", []))
        lineups_fetched += len(game_data.get("away_lineups", []))
        if game_data.get("home_dashboard"):
            dashboards_fetched += 1
        if game_data.get("away_dashboard"):
            dashboards_fetched += 1
        standings_rows = max(standings_rows, len(game_data.get("standings", [])))
        player_metrics_rows = max(
            player_metrics_rows, len(game_data.get("player_metrics", []))
        )
        if game_data.get("scoreboard"):
            scoreboard_available = True

    return {
        "games_enriched": games_enriched,
        "game_logs_fetched": game_logs_fetched,
        "lineups_fetched": lineups_fetched,
        "dashboards_fetched": dashboards_fetched,
        "standings_rows": standings_rows,
        "player_metrics_rows": player_metrics_rows,
        "scoreboard_available": scoreboard_available,
    }


# ── Internal helpers ───────────────────────────────────────────────────────────

def _resolve_team_id(game: dict, side: str) -> int | None:
    """
    Extract the team ID for *side* ('home' or 'away') from a game dict.

    Handles multiple key-name conventions used across the data layer:
    - ``home_team_id`` / ``HOME_TEAM_ID`` / ``homeTeamId``
    - ``away_team_id`` / ``VISITOR_TEAM_ID`` / ``visitorTeamId``
    """
    if side == "home":
        keys = ("home_team_id", "HOME_TEAM_ID", "homeTeamId", "home_id")
    else:
        keys = ("away_team_id", "VISITOR_TEAM_ID", "visitorTeamId", "away_id")

    for key in keys:
        val = game.get(key)
        if val is not None:
            try:
                return int(val)
            except (TypeError, ValueError):
                pass
    return None


def _log_enrichment_summary(game_id: str, enrichment: dict) -> None:
    """Log a short one-line summary of what was enriched for a single game."""
    home_logs = len(enrichment.get("home_game_logs", []))
    away_logs = len(enrichment.get("away_game_logs", []))
    home_lineups = len(enrichment.get("home_lineups", []))
    away_lineups = len(enrichment.get("away_lineups", []))
    has_home_dash = bool(enrichment.get("home_dashboard"))
    has_away_dash = bool(enrichment.get("away_dashboard"))
    _logger.debug(
        "enrich game=%s: home_logs=%d away_logs=%d home_lineups=%d away_lineups=%d "
        "home_dash=%s away_dash=%s",
        game_id, home_logs, away_logs, home_lineups, away_lineups,
        has_home_dash, has_away_dash,
    )
