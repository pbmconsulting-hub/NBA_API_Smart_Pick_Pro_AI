# ============================================================
# FILE: data/live_data_fetcher.py
# PURPOSE: Real-time NBA data fetcher using the nba_api library.
#          Provides functions to fetch live game data, player
#          stats, injury reports, and other real-time information.
# ============================================================

from __future__ import annotations

import logging
import time
from datetime import datetime, timedelta
from typing import Any

import pandas as pd

_logger = logging.getLogger(__name__)

# ── Rate limiting ───────────────────────────────────────────────────────

_RATE_LIMIT_PAUSE = 0.6  # seconds between API calls
_last_request_time: float = 0.0


def _rate_limit() -> None:
    """Enforce rate limiting between NBA API requests."""
    global _last_request_time
    elapsed = time.time() - _last_request_time
    if elapsed < _RATE_LIMIT_PAUSE:
        time.sleep(_RATE_LIMIT_PAUSE - elapsed)
    _last_request_time = time.time()


# ── Safe imports ────────────────────────────────────────────────────────

try:
    from nba_api.stats.endpoints import (
        ScoreboardV2,
        BoxScoreTraditionalV2,
        LeagueGameLog,
        CommonPlayerInfo,
        PlayerGameLog,
        TeamGameLog,
        LeagueDashTeamStats,
        LeagueDashPlayerStats,
        LeagueStandings,
        PlayerDashboardByGameSplits,
    )
    _NBA_API_AVAILABLE = True
except ImportError:
    _NBA_API_AVAILABLE = False
    _logger.warning("nba_api not installed. Live data fetching disabled.")


def is_available() -> bool:
    """Check if the nba_api library is installed."""
    return _NBA_API_AVAILABLE


# ═══════════════════════════════════════════════════════════════════════════
# Live scoreboard
# ═══════════════════════════════════════════════════════════════════════════


def fetch_todays_scoreboard(game_date: str = "") -> list[dict[str, Any]]:
    """Fetch today's NBA scoreboard.

    Parameters
    ----------
    game_date:
        Date string in ``YYYY-MM-DD`` format.  Defaults to today.

    Returns
    -------
    list[dict]
        List of game dicts with keys: game_id, matchup, home_team,
        away_team, home_score, away_score, status, game_time.
    """
    if not _NBA_API_AVAILABLE:
        _logger.warning("nba_api not available for scoreboard.")
        return []

    if not game_date:
        game_date = datetime.now().strftime("%Y-%m-%d")

    try:
        _rate_limit()
        sb = ScoreboardV2(game_date=game_date)
        header = sb.game_header.get_data_frame()
        line_score = sb.line_score.get_data_frame()

        games: list[dict[str, Any]] = []
        for _, row in header.iterrows():
            gid = row.get("GAME_ID", "")
            status = row.get("GAME_STATUS_TEXT", "")
            home_tid = row.get("HOME_TEAM_ID")
            away_tid = row.get("VISITOR_TEAM_ID")

            home_ls = line_score[line_score["TEAM_ID"] == home_tid]
            away_ls = line_score[line_score["TEAM_ID"] == away_tid]

            home_abbrev = home_ls["TEAM_ABBREVIATION"].values[0] if len(home_ls) else ""
            away_abbrev = away_ls["TEAM_ABBREVIATION"].values[0] if len(away_ls) else ""
            home_pts = int(home_ls["PTS"].values[0]) if len(home_ls) and pd.notna(home_ls["PTS"].values[0]) else None
            away_pts = int(away_ls["PTS"].values[0]) if len(away_ls) and pd.notna(away_ls["PTS"].values[0]) else None

            games.append({
                "game_id": gid,
                "game_date": game_date,
                "matchup": f"{away_abbrev} @ {home_abbrev}",
                "home_team": home_abbrev,
                "away_team": away_abbrev,
                "home_score": home_pts,
                "away_score": away_pts,
                "status": status,
            })
        return games

    except Exception as exc:
        _logger.error("Error fetching scoreboard: %s", exc)
        return []


# ═══════════════════════════════════════════════════════════════════════════
# Box scores
# ═══════════════════════════════════════════════════════════════════════════


def fetch_box_score(game_id: str) -> dict[str, Any]:
    """Fetch box score for a specific game.

    Returns
    -------
    dict with keys ``home_players``, ``away_players`` (list[dict])
    and ``game_id``.
    """
    if not _NBA_API_AVAILABLE:
        return {"game_id": game_id, "home_players": [], "away_players": []}

    try:
        _rate_limit()
        box = BoxScoreTraditionalV2(game_id=game_id)
        players = box.player_stats.get_data_frame()

        result: dict[str, Any] = {
            "game_id": game_id,
            "home_players": [],
            "away_players": [],
        }

        for _, row in players.iterrows():
            player = {
                "player_id": row.get("PLAYER_ID"),
                "player_name": row.get("PLAYER_NAME", ""),
                "team_abbreviation": row.get("TEAM_ABBREVIATION", ""),
                "minutes": row.get("MIN", ""),
                "points": row.get("PTS", 0),
                "rebounds": row.get("REB", 0),
                "assists": row.get("AST", 0),
                "steals": row.get("STL", 0),
                "blocks": row.get("BLK", 0),
                "turnovers": row.get("TO", 0),
                "fg3m": row.get("FG3M", 0),
                "fgm": row.get("FGM", 0),
                "fga": row.get("FGA", 0),
                "ftm": row.get("FTM", 0),
                "fta": row.get("FTA", 0),
            }
            # Use start_period to determine home/away
            start_period = row.get("START_POSITION", "")
            result.setdefault("all_players", []).append(player)

        return result

    except Exception as exc:
        _logger.error("Error fetching box score for %s: %s", game_id, exc)
        return {"game_id": game_id, "home_players": [], "away_players": []}


# ═══════════════════════════════════════════════════════════════════════════
# Player game logs
# ═══════════════════════════════════════════════════════════════════════════


def fetch_player_game_log(
    player_id: int,
    season: str = "2025-26",
    last_n: int = 0,
) -> pd.DataFrame:
    """Fetch a player's game log for the given season.

    Parameters
    ----------
    player_id:
        NBA player ID.
    season:
        Season string (e.g. ``"2025-26"``).
    last_n:
        If > 0, return only the last N games.

    Returns
    -------
    pd.DataFrame
    """
    if not _NBA_API_AVAILABLE:
        return pd.DataFrame()

    try:
        _rate_limit()
        log = PlayerGameLog(
            player_id=player_id,
            season=season,
        )
        df = log.get_data_frames()[0]
        if last_n > 0:
            df = df.head(last_n)
        return df

    except Exception as exc:
        _logger.error("Error fetching game log for player %d: %s", player_id, exc)
        return pd.DataFrame()


# ═══════════════════════════════════════════════════════════════════════════
# Team stats
# ═══════════════════════════════════════════════════════════════════════════


def fetch_team_stats(season: str = "2025-26") -> pd.DataFrame:
    """Fetch league-wide team stats."""
    if not _NBA_API_AVAILABLE:
        return pd.DataFrame()

    try:
        _rate_limit()
        stats = LeagueDashTeamStats(season=season)
        return stats.get_data_frames()[0]
    except Exception as exc:
        _logger.error("Error fetching team stats: %s", exc)
        return pd.DataFrame()


def fetch_team_game_log(
    team_id: int,
    season: str = "2025-26",
    last_n: int = 0,
) -> pd.DataFrame:
    """Fetch a team's game log."""
    if not _NBA_API_AVAILABLE:
        return pd.DataFrame()

    try:
        _rate_limit()
        log = TeamGameLog(team_id=team_id, season=season)
        df = log.get_data_frames()[0]
        if last_n > 0:
            df = df.head(last_n)
        return df
    except Exception as exc:
        _logger.error("Error fetching team game log for %d: %s", team_id, exc)
        return pd.DataFrame()


# ═══════════════════════════════════════════════════════════════════════════
# Standings
# ═══════════════════════════════════════════════════════════════════════════


def fetch_standings(season: str = "2025-26") -> pd.DataFrame:
    """Fetch current NBA standings."""
    if not _NBA_API_AVAILABLE:
        return pd.DataFrame()

    try:
        _rate_limit()
        standings = LeagueStandings(season=season)
        return standings.get_data_frames()[0]
    except Exception as exc:
        _logger.error("Error fetching standings: %s", exc)
        return pd.DataFrame()


# ═══════════════════════════════════════════════════════════════════════════
# Player info
# ═══════════════════════════════════════════════════════════════════════════


def fetch_player_info(player_id: int) -> dict[str, Any]:
    """Fetch basic player info (bio, position, team)."""
    if not _NBA_API_AVAILABLE:
        return {}

    try:
        _rate_limit()
        info = CommonPlayerInfo(player_id=player_id)
        df = info.common_player_info.get_data_frame()
        if df.empty:
            return {}
        row = df.iloc[0]
        return {
            "player_id": int(row.get("PERSON_ID", player_id)),
            "player_name": f"{row.get('FIRST_NAME', '')} {row.get('LAST_NAME', '')}".strip(),
            "team_id": row.get("TEAM_ID"),
            "team_abbreviation": row.get("TEAM_ABBREVIATION", ""),
            "position": row.get("POSITION", ""),
            "height": row.get("HEIGHT", ""),
            "weight": row.get("WEIGHT", ""),
            "jersey": row.get("JERSEY", ""),
            "season_exp": row.get("SEASON_EXP", 0),
        }
    except Exception as exc:
        _logger.error("Error fetching player info for %d: %s", player_id, exc)
        return {}


# ═══════════════════════════════════════════════════════════════════════════
# League-wide player stats
# ═══════════════════════════════════════════════════════════════════════════


def fetch_league_player_stats(
    season: str = "2025-26",
    per_mode: str = "PerGame",
) -> pd.DataFrame:
    """Fetch league-wide per-game player stats.

    Parameters
    ----------
    season:
        NBA season string.
    per_mode:
        ``"PerGame"``, ``"Totals"``, or ``"Per36"``.
    """
    if not _NBA_API_AVAILABLE:
        return pd.DataFrame()

    try:
        _rate_limit()
        stats = LeagueDashPlayerStats(season=season, per_mode_detailed=per_mode)
        return stats.get_data_frames()[0]
    except Exception as exc:
        _logger.error("Error fetching league player stats: %s", exc)
        return pd.DataFrame()


# ═══════════════════════════════════════════════════════════════════════════
# Batch fetching
# ═══════════════════════════════════════════════════════════════════════════


def fetch_yesterday_box_scores() -> list[dict[str, Any]]:
    """Fetch all box scores from yesterday's games.

    Returns a list of box-score dicts (one per game).
    """
    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    games = fetch_todays_scoreboard(game_date=yesterday)

    results: list[dict[str, Any]] = []
    for game in games:
        gid = game.get("game_id", "")
        if gid:
            box = fetch_box_score(gid)
            box["game_info"] = game
            results.append(box)

    _logger.info("Fetched %d box scores from %s", len(results), yesterday)
    return results


def fetch_player_splits(
    player_id: int,
    season: str = "2025-26",
) -> dict[str, pd.DataFrame]:
    """Fetch player game splits (home/away, by month, etc.).

    Returns
    -------
    dict mapping split names to DataFrames.
    """
    if not _NBA_API_AVAILABLE:
        return {}

    try:
        _rate_limit()
        splits = PlayerDashboardByGameSplits(
            player_id=player_id,
            season=season,
        )
        return {
            "by_location": splits.by_location_player_dashboard.get_data_frame(),
            "by_month": splits.by_month_player_dashboard.get_data_frame(),
            "overall": splits.overall_player_dashboard.get_data_frame(),
        }
    except Exception as exc:
        _logger.error("Error fetching splits for player %d: %s", player_id, exc)
        return {}


# ═══════════════════════════════════════════════════════════════════════════
# Bulk refresh helper
# ═══════════════════════════════════════════════════════════════════════════


def refresh_all_data(
    season: str = "2025-26",
    save_path: str | None = None,
) -> dict[str, Any]:
    """Run a comprehensive data refresh.

    Fetches scoreboard, team stats, standings, and league player stats.
    Optionally saves to CSV files.

    Returns
    -------
    dict with keys ``scoreboard``, ``team_stats``, ``standings``,
    ``player_stats`` containing the fetched data.
    """
    _logger.info("Starting full data refresh for season %s", season)

    scoreboard = fetch_todays_scoreboard()
    team_stats = fetch_team_stats(season=season)
    standings = fetch_standings(season=season)
    player_stats = fetch_league_player_stats(season=season)

    result = {
        "scoreboard": scoreboard,
        "team_stats": team_stats,
        "standings": standings,
        "player_stats": player_stats,
        "timestamp": datetime.now().isoformat(),
    }

    if save_path:
        from pathlib import Path

        out_dir = Path(save_path)
        out_dir.mkdir(parents=True, exist_ok=True)

        if not team_stats.empty:
            team_stats.to_csv(out_dir / "team_stats.csv", index=False)
        if not standings.empty:
            standings.to_csv(out_dir / "standings.csv", index=False)
        if not player_stats.empty:
            player_stats.to_csv(out_dir / "player_stats.csv", index=False)

        _logger.info("Saved refresh data to %s", out_dir)

    _logger.info("Full data refresh complete.")
    return result
