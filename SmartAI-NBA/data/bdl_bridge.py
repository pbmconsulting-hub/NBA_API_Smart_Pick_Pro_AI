"""data/bdl_bridge.py – Bridge from BallDontLie API to SmartAI-NBA app format.

Provides adapter functions that:
1. Call the BallDontLie client (engine.scrapers.balldontlie_client)
2. Transform responses to match the format expected by live_data_fetcher,
   nba_live_fetcher, and the rest of the app

This module replaces nba_api as the primary data source for:
- Team records / standings
- Today's games
- Player season averages
- Player per-game stats & game logs
- Injuries
- Box scores, lineups, play-by-play, league leaders

Props fetching (PrizePicks, Underdog, DraftKings) is NOT affected.
"""
from __future__ import annotations

import datetime
import logging
import math
import statistics
import time
from typing import Any

try:
    from utils.logger import get_logger
    _logger = get_logger(__name__)
except Exception:
    _logger = logging.getLogger(__name__)

try:
    from engine.scrapers import balldontlie_client as _bdl
    _BDL_AVAILABLE = True
    if not _bdl.has_api_key():
        _BDL_AVAILABLE = False
        _logger.warning(
            "BallDontLie client imported but BALLDONTLIE_API_KEY is not set — "
            "BDL bridge disabled. Set the key in env or Streamlit secrets."
        )
except ImportError:
    _BDL_AVAILABLE = False
    _logger.debug("balldontlie_client not available; bdl_bridge disabled")

# ── helpers ───────────────────────────────────────────────────────────────────

def _safe_float(val: Any, default: float = 0.0) -> float:
    """Coerce *val* to float, returning *default* on failure."""
    try:
        return float(val) if val is not None else default
    except (ValueError, TypeError):
        return default


def _parse_min(min_str: Any) -> float:
    """Parse BDL minutes ('34:15' or '34' or 34.0) to float."""
    if isinstance(min_str, (int, float)):
        return float(min_str)
    if not min_str:
        return 0.0
    try:
        s = str(min_str)
        if ":" in s:
            parts = s.split(":")
            return float(parts[0]) + float(parts[1]) / 60.0
        return float(s)
    except (ValueError, IndexError):
        return 0.0


# The NBA regular season starts in October.
NBA_SEASON_START_MONTH: int = 10

# Rough normalisation factor for estimating pace from average team scoring:
# pace ≈ (pts_scored + pts_allowed) / PACE_NORMALIZATION_FACTOR
# Derived from the relationship: league-average pace ≈ 100 and average
# combined scoring ≈ 220 → 220/2.2 = 100.
PACE_NORMALIZATION_FACTOR: float = 2.2


def _current_season_int() -> int:
    """Return current NBA season start year (e.g. 2025 for the 2025-26 season)."""
    today = datetime.date.today()
    return today.year if today.month >= NBA_SEASON_START_MONTH else today.year - 1


def _today_et() -> datetime.date:
    """Return today's date anchored to US/Eastern time."""
    try:
        from zoneinfo import ZoneInfo
        _eastern = ZoneInfo("America/New_York")
    except ImportError:
        _eastern = datetime.timezone(datetime.timedelta(hours=-5))
    return datetime.datetime.now(_eastern).date()


def _stdev_safe(values: list[float]) -> float:
    """Compute stdev from a list, returning 0.0 if too few values."""
    if len(values) < 2:
        return 0.0
    try:
        return round(statistics.stdev(values), 2)
    except statistics.StatisticsError:
        return 0.0


# ── Position mapping (BDL → app) ────────────────────────────────────────────

_BDL_POSITION_MAP: dict[str, str] = {
    "G":   "PG",
    "F":   "SF",
    "C":   "C",
    "G-F": "SF",
    "F-G": "SG",
    "F-C": "PF",
    "C-F": "PF",
    "":    "SF",
}

# ── Team abbreviation fallback (BDL uses standard 3-letter codes) ────────────

TEAM_NAME_TO_ABBREVIATION: dict[str, str] = {
    "Atlanta Hawks": "ATL", "Boston Celtics": "BOS", "Brooklyn Nets": "BKN",
    "Charlotte Hornets": "CHA", "Chicago Bulls": "CHI", "Cleveland Cavaliers": "CLE",
    "Dallas Mavericks": "DAL", "Denver Nuggets": "DEN", "Detroit Pistons": "DET",
    "Golden State Warriors": "GSW", "Houston Rockets": "HOU", "Indiana Pacers": "IND",
    "Los Angeles Clippers": "LAC", "Los Angeles Lakers": "LAL", "Memphis Grizzlies": "MEM",
    "Miami Heat": "MIA", "Milwaukee Bucks": "MIL", "Minnesota Timberwolves": "MIN",
    "New Orleans Pelicans": "NOP", "New York Knicks": "NYK", "Oklahoma City Thunder": "OKC",
    "Orlando Magic": "ORL", "Philadelphia 76ers": "PHI", "Phoenix Suns": "PHX",
    "Portland Trail Blazers": "POR", "Sacramento Kings": "SAC", "San Antonio Spurs": "SAS",
    "Toronto Raptors": "TOR", "Utah Jazz": "UTA", "Washington Wizards": "WAS",
}

TEAM_CONFERENCE: dict[str, str] = {
    "ATL": "East", "BOS": "East", "BKN": "East", "CHA": "East",
    "CHI": "East", "CLE": "East", "DET": "East", "IND": "East",
    "MIA": "East", "MIL": "East", "NYK": "East", "ORL": "East",
    "PHI": "East", "TOR": "East", "WAS": "East",
    "DAL": "West", "DEN": "West", "GSW": "West", "HOU": "West",
    "LAC": "West", "LAL": "West", "MEM": "West", "MIN": "West",
    "NOP": "West", "OKC": "West", "PHX": "West", "POR": "West",
    "SAC": "West", "SAS": "West", "UTA": "West",
}


def is_available() -> bool:
    """Return True if the BDL bridge can make API calls.

    Requires both the ``balldontlie_client`` module and a valid API key.
    """
    return _BDL_AVAILABLE


def get_api_status() -> dict:
    """Return a diagnostic dict about BDL API availability.

    Useful for health checks and the Data Feed page.
    """
    if not _BDL_AVAILABLE:
        # _bdl may be undefined if the import itself failed.
        bdl_mod = globals().get("_bdl")
        if bdl_mod is None:
            return {"ok": False, "error": "balldontlie_client module not installed"}
        if not bdl_mod.has_api_key():
            return {"ok": False, "error": "BALLDONTLIE_API_KEY not configured"}
        return {"ok": False, "error": "BDL bridge disabled"}
    try:
        return _bdl.check_api_health()
    except Exception as exc:
        return {"ok": False, "error": str(exc)}


# ═══════════════════════════════════════════════════════════════════════════════
# Team Records / Standings
# ═══════════════════════════════════════════════════════════════════════════════

def fetch_team_records() -> dict[str, dict]:
    """Get team records from BDL standings.

    Returns dict keyed by team abbreviation matching
    ``live_data_fetcher._fetch_team_records()`` format::

        {"LAL": {"wins": 45, "losses": 37, "streak": "W3",
                 "home_record": "25-16", "away_record": "20-21",
                 "conf_rank": 5}, ...}
    """
    if not _BDL_AVAILABLE:
        return {}
    try:
        standings = _bdl.get_standings(season=_current_season_int())
        records: dict[str, dict] = {}
        for entry in standings:
            team = entry.get("team", {})
            abbrev = team.get("abbreviation", "")
            if not abbrev:
                # Try full_name → abbreviation fallback
                abbrev = TEAM_NAME_TO_ABBREVIATION.get(team.get("full_name", ""), "")
            if not abbrev:
                continue
            records[abbrev] = {
                "wins": int(entry.get("wins", 0) or 0),
                "losses": int(entry.get("losses", 0) or 0),
                "streak": str(entry.get("streak", "") or ""),
                "home_record": str(entry.get("home_record", "") or ""),
                "away_record": str(entry.get("road_record", entry.get("away_record", "")) or ""),
                "conf_rank": int(entry.get("conference_rank", entry.get("playoff_rank", 0)) or 0),
            }
        return records
    except Exception as exc:
        _logger.warning("BDL bridge: fetch_team_records failed: %s", exc)
        return {}


# ═══════════════════════════════════════════════════════════════════════════════
# Today's Games
# ═══════════════════════════════════════════════════════════════════════════════

def fetch_todays_games(team_records: dict | None = None) -> list[dict]:
    """Get today's games from BDL in the app's formatted game dict format.

    Each game dict has the same keys as ``_build_formatted_game()`` output.
    """
    if not _BDL_AVAILABLE:
        return []
    try:
        today = _today_et().strftime("%Y-%m-%d")
        games = _bdl.get_games(date=today, per_page=100)

        if team_records is None:
            team_records = fetch_team_records()

        formatted: list[dict] = []
        for game in games:
            home = game.get("home_team", {})
            away = game.get("visitor_team", {})
            home_abbrev = home.get("abbreviation", "")
            away_abbrev = away.get("abbreviation", "")
            if not home_abbrev or not away_abbrev:
                continue

            home_full = home.get("full_name", "")
            away_full = away.get("full_name", "")

            status = str(game.get("status", "") or "")
            game_time = str(game.get("time", "") or "")
            if not game_time and status:
                game_time = status

            home_rec = team_records.get(home_abbrev, {})
            away_rec = team_records.get(away_abbrev, {})

            formatted.append({
                "game_id": str(game.get("id", f"{home_abbrev}_vs_{away_abbrev}")),
                "home_team": home_abbrev,
                "away_team": away_abbrev,
                "home_team_full": f"{home_abbrev} — {home_full}",
                "away_team_full": f"{away_abbrev} — {away_full}",
                "home_team_name": home_full,
                "away_team_name": away_full,
                "vegas_spread": 0.0,
                "game_total": 220.0,
                "game_date": game.get("date", today),
                "game_time_et": game_time or "TBD",
                "arena": "",
                "home_wins": home_rec.get("wins", 0),
                "home_losses": home_rec.get("losses", 0),
                "home_streak": home_rec.get("streak", ""),
                "home_home_record": home_rec.get("home_record", ""),
                "home_conf_rank": home_rec.get("conf_rank", 0),
                "away_wins": away_rec.get("wins", 0),
                "away_losses": away_rec.get("losses", 0),
                "away_streak": away_rec.get("streak", ""),
                "away_away_record": away_rec.get("away_record", ""),
                "away_conf_rank": away_rec.get("conf_rank", 0),
            })
        return formatted
    except Exception as exc:
        _logger.warning("BDL bridge: fetch_todays_games failed: %s", exc)
        return []


# ═══════════════════════════════════════════════════════════════════════════════
# Player Season Averages (single player)
# ═══════════════════════════════════════════════════════════════════════════════

def fetch_player_season_averages(player_id: int, season: int | None = None) -> dict:
    """Return season averages dict for one player (app field names)."""
    if not _BDL_AVAILABLE:
        return {}
    if season is None:
        season = _current_season_int()
    try:
        avgs = _bdl.get_season_averages(player_id=player_id, season=season)
        if not avgs:
            return {}
        a = avgs[0]
        return {
            "minutes_avg": round(_parse_min(a.get("min", 0)), 1),
            "points_avg": round(_safe_float(a.get("pts")), 1),
            "rebounds_avg": round(_safe_float(a.get("reb")), 1),
            "assists_avg": round(_safe_float(a.get("ast")), 1),
            "threes_avg": round(_safe_float(a.get("fg3m")), 1),
            "steals_avg": round(_safe_float(a.get("stl")), 1),
            "blocks_avg": round(_safe_float(a.get("blk")), 1),
            "turnovers_avg": round(_safe_float(a.get("turnover", a.get("tov", 0))), 1),
            "ft_pct": round(_safe_float(a.get("ft_pct")), 3),
            "ftm_avg": round(_safe_float(a.get("ftm")), 1),
            "fta_avg": round(_safe_float(a.get("fta")), 1),
            "fga_avg": round(_safe_float(a.get("fga")), 1),
            "fgm_avg": round(_safe_float(a.get("fgm")), 1),
            "offensive_rebounds_avg": round(_safe_float(a.get("oreb")), 1),
            "defensive_rebounds_avg": round(_safe_float(a.get("dreb")), 1),
            "personal_fouls_avg": round(_safe_float(a.get("pf")), 1),
            "games_played": int(a.get("games_played", 0) or 0),
        }
    except Exception as exc:
        _logger.warning("BDL bridge: fetch_player_season_averages(%s) failed: %s", player_id, exc)
        return {}


# ═══════════════════════════════════════════════════════════════════════════════
# Player Per-Game Stats (for std dev computation & game logs)
# ═══════════════════════════════════════════════════════════════════════════════

def fetch_player_game_stats(player_id: int, season: int | None = None,
                            per_page: int = 100) -> list[dict]:
    """Return raw per-game stat dicts for one player from BDL."""
    if not _BDL_AVAILABLE:
        return []
    if season is None:
        season = _current_season_int()
    try:
        return _bdl.get_stats(player_id=player_id, season=season, per_page=per_page)
    except Exception as exc:
        _logger.warning("BDL bridge: fetch_player_game_stats(%s) failed: %s", player_id, exc)
        return []


def compute_std_devs(game_stats: list[dict]) -> dict[str, float]:
    """Compute standard deviations from BDL per-game stats.

    Returns dict with ``_std`` suffix keys matching the app CSV format.
    """
    if len(game_stats) < 2:
        return {}

    def _vals(key: str) -> list[float]:
        return [_safe_float(g.get(key, 0)) for g in game_stats]

    return {
        "points_std": _stdev_safe(_vals("pts")),
        "rebounds_std": _stdev_safe(_vals("reb")),
        "assists_std": _stdev_safe(_vals("ast")),
        "threes_std": _stdev_safe(_vals("fg3m")),
        "steals_std": _stdev_safe(_vals("stl")),
        "blocks_std": _stdev_safe(_vals("blk")),
        "turnovers_std": _stdev_safe(_vals("turnover")),
        "ftm_std": _stdev_safe(_vals("ftm")),
        "fta_std": _stdev_safe(_vals("fta")),
        "fga_std": _stdev_safe(_vals("fga")),
        "fgm_std": _stdev_safe(_vals("fgm")),
        "offensive_rebounds_std": _stdev_safe(_vals("oreb")),
        "defensive_rebounds_std": _stdev_safe(_vals("dreb")),
        "personal_fouls_std": _stdev_safe(_vals("pf")),
    }


def compute_averages_from_games(game_stats: list[dict]) -> dict[str, float]:
    """Compute season averages locally from per-game BDL stats.

    Useful when ``get_season_averages`` is unavailable or when we already
    have the per-game data in hand and want to avoid an extra API call.
    """
    if not game_stats:
        return {}

    n = len(game_stats)

    def _avg(key: str) -> float:
        total = sum(_safe_float(g.get(key, 0)) for g in game_stats)
        return round(total / n, 1)

    def _avg3(key: str) -> float:
        total = sum(_safe_float(g.get(key, 0)) for g in game_stats)
        return round(total / n, 3)

    return {
        "minutes_avg": _avg("min") if not any(":" in str(g.get("min", "")) for g in game_stats)
                       else round(sum(_parse_min(g.get("min", 0)) for g in game_stats) / n, 1),
        "points_avg": _avg("pts"),
        "rebounds_avg": _avg("reb"),
        "assists_avg": _avg("ast"),
        "threes_avg": _avg("fg3m"),
        "steals_avg": _avg("stl"),
        "blocks_avg": _avg("blk"),
        "turnovers_avg": _avg("turnover"),
        "ft_pct": _avg3("ft_pct"),
        "ftm_avg": _avg("ftm"),
        "fta_avg": _avg("fta"),
        "fga_avg": _avg("fga"),
        "fgm_avg": _avg("fgm"),
        "offensive_rebounds_avg": _avg("oreb"),
        "defensive_rebounds_avg": _avg("dreb"),
        "personal_fouls_avg": _avg("pf"),
        "games_played": n,
    }


# ═══════════════════════════════════════════════════════════════════════════════
# Player Game Log (formatted for app)
# ═══════════════════════════════════════════════════════════════════════════════

def fetch_player_game_log(player_id: int, last_n: int = 20) -> list[dict]:
    """Return game log for a player in the app's ``fetch_player_game_log`` format.

    Each entry::

        {"game_date", "matchup", "win_loss", "minutes", "pts", "reb",
         "ast", "stl", "blk", "tov", "fg3m", "ft_pct"}
    """
    if not _BDL_AVAILABLE:
        return []
    try:
        stats = _bdl.get_stats(
            player_id=player_id,
            season=_current_season_int(),
            per_page=100,
        )
        formatted: list[dict] = []
        for s in stats:
            game = s.get("game", {})
            team = s.get("team", {})
            team_abbrev = team.get("abbreviation", "")

            # Build matchup string
            home_t = game.get("home_team", {})
            away_t = game.get("visitor_team", {})
            is_home = (home_t.get("abbreviation", "") == team_abbrev) if home_t else False
            if is_home:
                opp = away_t.get("abbreviation", "") if away_t else ""
                matchup = f"{team_abbrev} vs. {opp}" if opp else team_abbrev
            else:
                opp = home_t.get("abbreviation", "") if home_t else ""
                matchup = f"{team_abbrev} @ {opp}" if opp else team_abbrev

            # Win/Loss
            home_score = int(game.get("home_team_score", 0) or 0)
            away_score = int(game.get("visitor_team_score", 0) or 0)
            wl = ""
            if home_score > 0 or away_score > 0:
                if is_home:
                    wl = "W" if home_score > away_score else "L"
                else:
                    wl = "W" if away_score > home_score else "L"

            formatted.append({
                "game_date": str(game.get("date", "")),
                "matchup": matchup,
                "win_loss": wl,
                "minutes": _parse_min(s.get("min", 0)),
                "pts": _safe_float(s.get("pts")),
                "reb": _safe_float(s.get("reb")),
                "ast": _safe_float(s.get("ast")),
                "stl": _safe_float(s.get("stl")),
                "blk": _safe_float(s.get("blk")),
                "tov": _safe_float(s.get("turnover", s.get("tov", 0))),
                "fg3m": _safe_float(s.get("fg3m")),
                "ft_pct": _safe_float(s.get("ft_pct")),
            })

        # Sort by date descending (newest first)
        formatted.sort(key=lambda x: x.get("game_date", ""), reverse=True)
        if last_n > 0:
            formatted = formatted[:last_n]
        return formatted
    except Exception as exc:
        _logger.warning("BDL bridge: fetch_player_game_log(%s) failed: %s", player_id, exc)
        return []


# ═══════════════════════════════════════════════════════════════════════════════
# Player Recent Form
# ═══════════════════════════════════════════════════════════════════════════════

def fetch_player_recent_form(player_id: int, last_n_games: int = 10) -> dict:
    """Return recent-form dict matching ``live_data_fetcher.fetch_player_recent_form``."""
    game_log = fetch_player_game_log(player_id, last_n=last_n_games)
    if not game_log:
        return {}

    def safe_avg(vals: list[float]) -> float:
        clean = [v for v in vals if v is not None and isinstance(v, (int, float)) and math.isfinite(v)]
        return round(sum(clean) / len(clean), 1) if clean else 0.0

    pts_list = [g["pts"] for g in game_log]
    reb_list = [g["reb"] for g in game_log]
    ast_list = [g["ast"] for g in game_log]
    fg3m_list = [g["fg3m"] for g in game_log]

    last_5 = pts_list[:5]
    prior_5 = pts_list[5:10]
    last5_avg = safe_avg(last_5)
    prev5_avg = safe_avg(prior_5) if prior_5 else last5_avg

    if prev5_avg > 0:
        if last5_avg >= prev5_avg * 1.1:
            trend = "hot"
        elif last5_avg <= prev5_avg * 0.9:
            trend = "cold"
        else:
            trend = "neutral"
    else:
        trend = "neutral"

    trend_emoji_map = {"hot": "🔥", "cold": "❄️", "neutral": "➡️"}

    # Build game_results list (matches nba_api format keys)
    game_results = []
    for g in game_log:
        game_results.append({
            "date": g["game_date"],
            "matchup": g["matchup"],
            "wl": g["win_loss"],
            "pts": g["pts"],
            "reb": g["reb"],
            "ast": g["ast"],
            "fg3m": g["fg3m"],
            "min": g["minutes"],
        })

    # Build raw-style game dicts (uppercase keys) for backward compat
    raw_games = []
    for g in game_log:
        raw_games.append({
            "GAME_DATE": g["game_date"],
            "MATCHUP": g["matchup"],
            "WL": g["win_loss"],
            "MIN": g["minutes"],
            "PTS": g["pts"],
            "REB": g["reb"],
            "AST": g["ast"],
            "FG3M": g["fg3m"],
            "STL": g.get("stl", 0),
            "BLK": g.get("blk", 0),
            "TOV": g.get("tov", 0),
            "FT_PCT": g.get("ft_pct", 0),
        })

    return {
        "games": raw_games,
        "recent_pts_avg": safe_avg(pts_list),
        "recent_reb_avg": safe_avg(reb_list),
        "recent_ast_avg": safe_avg(ast_list),
        "recent_fg3m_avg": safe_avg(fg3m_list),
        "trend": trend,
        "trend_emoji": trend_emoji_map.get(trend, "➡️"),
        "last_5_pts": last_5,
        "last_5_pts_avg": last5_avg,
        "game_results": game_results,
        "games_played": len(game_log),
    }


# ═══════════════════════════════════════════════════════════════════════════════
# Team Stats (for teams.csv)
# ═══════════════════════════════════════════════════════════════════════════════

def fetch_team_stats_for_csv() -> list[dict]:
    """Build team stat rows for teams.csv from BDL data.

    Combines ``get_teams()`` (basic info) + ``get_standings()`` (W/L) +
    game-score based estimates for pace/ortg/drtg.

    Returns list of dicts with keys:
        team_name, abbreviation, conference, division, pace, ortg, drtg
    """
    if not _BDL_AVAILABLE:
        return []
    try:
        teams = _bdl.get_teams()
        standings = _bdl.get_standings(season=_current_season_int())

        # Build standings lookup
        standings_map: dict[str, dict] = {}
        for entry in standings:
            t = entry.get("team", {})
            abbrev = t.get("abbreviation", "")
            if abbrev:
                standings_map[abbrev] = entry

        # Fetch recent games to estimate pace/ortg/drtg per team
        team_scoring: dict[str, dict] = {}  # abbrev → {scored:[], allowed:[]}
        season = _current_season_int()

        # Get a batch of recent games (all teams)
        recent_games = _bdl.get_games(season=season, per_page=100)
        for g in recent_games:
            ht = g.get("home_team", {})
            at = g.get("visitor_team", {})
            h_abbrev = ht.get("abbreviation", "")
            a_abbrev = at.get("abbreviation", "")
            h_score = _safe_float(g.get("home_team_score", 0))
            a_score = _safe_float(g.get("visitor_team_score", 0))
            if h_score <= 0 and a_score <= 0:
                continue  # Skip unplayed games
            if h_abbrev:
                team_scoring.setdefault(h_abbrev, {"scored": [], "allowed": []})
                team_scoring[h_abbrev]["scored"].append(h_score)
                team_scoring[h_abbrev]["allowed"].append(a_score)
            if a_abbrev:
                team_scoring.setdefault(a_abbrev, {"scored": [], "allowed": []})
                team_scoring[a_abbrev]["scored"].append(a_score)
                team_scoring[a_abbrev]["allowed"].append(h_score)

        formatted: list[dict] = []
        for team in teams:
            abbrev = team.get("abbreviation", "")
            full_name = team.get("full_name", "")
            if not abbrev:
                continue

            conference = TEAM_CONFERENCE.get(abbrev, team.get("conference", "West"))

            # Estimate pace/ortg/drtg from game scoring data
            scoring = team_scoring.get(abbrev, {})
            scored_list = scoring.get("scored", [])
            allowed_list = scoring.get("allowed", [])

            if scored_list:
                avg_scored = sum(scored_list) / len(scored_list)
                avg_allowed = sum(allowed_list) / len(allowed_list)
                # Rough estimates: ortg ≈ ppg (when pace ≈ 100)
                ortg = round(avg_scored, 1)
                drtg = round(avg_allowed, 1)
                pace = round((avg_scored + avg_allowed) / PACE_NORMALIZATION_FACTOR, 1)
            else:
                pace, ortg, drtg = 100.0, 110.0, 110.0

            formatted.append({
                "team_name": full_name,
                "abbreviation": abbrev,
                "conference": conference,
                "division": team.get("division", ""),
                "pace": pace,
                "ortg": ortg,
                "drtg": drtg,
            })

        formatted.sort(key=lambda t: t["team_name"])
        return formatted
    except Exception as exc:
        _logger.warning("BDL bridge: fetch_team_stats_for_csv failed: %s", exc)
        return []


# ═══════════════════════════════════════════════════════════════════════════════
# Injuries
# ═══════════════════════════════════════════════════════════════════════════════

def fetch_injuries(team_id: int | None = None) -> list[dict]:
    """Return injuries from BDL as a list of dicts."""
    if not _BDL_AVAILABLE:
        return []
    try:
        injuries = _bdl.get_injuries(team_id=team_id)
        formatted: list[dict] = []
        for inj in injuries:
            player = inj.get("player", {})
            team = inj.get("team", {})
            formatted.append({
                "player_name": f"{player.get('first_name', '')} {player.get('last_name', '')}".strip(),
                "player_id": player.get("id"),
                "team": team.get("abbreviation", ""),
                "status": inj.get("status", ""),
                "description": inj.get("description", ""),
                "date": inj.get("date", ""),
            })
        return formatted
    except Exception as exc:
        _logger.warning("BDL bridge: fetch_injuries failed: %s", exc)
        return []


# ═══════════════════════════════════════════════════════════════════════════════
# Standings list (for nba_data_service.get_standings())
# ═══════════════════════════════════════════════════════════════════════════════

def fetch_standings_list(season: int | None = None) -> list[dict]:
    """Return standings as a list of dicts with nba_api-style uppercase keys.

    Compatible with the format expected by ``nba_data_service.get_standings()``.
    """
    if not _BDL_AVAILABLE:
        return []
    if season is None:
        season = _current_season_int()
    try:
        standings = _bdl.get_standings(season=season)
        formatted: list[dict] = []
        for entry in standings:
            team = entry.get("team", {})
            wins = int(entry.get("wins", 0) or 0)
            losses = int(entry.get("losses", 0) or 0)
            total = wins + losses
            formatted.append({
                "TeamID": team.get("id", 0),
                "TeamName": team.get("full_name", ""),
                "TeamAbbreviation": team.get("abbreviation", ""),
                "Conference": entry.get("conference", ""),
                "WINS": wins,
                "LOSSES": losses,
                "WinPCT": round(wins / max(1, total), 3),
                "HOME": str(entry.get("home_record", "") or ""),
                "ROAD": str(entry.get("road_record", entry.get("away_record", "")) or ""),
                "PlayoffRank": int(entry.get("conference_rank", entry.get("playoff_rank", 0)) or 0),
                "strCurrentStreak": str(entry.get("streak", "") or ""),
            })
        return formatted
    except Exception as exc:
        _logger.warning("BDL bridge: fetch_standings_list failed: %s", exc)
        return []


# ═══════════════════════════════════════════════════════════════════════════════
# Active Players (basic info from BDL)
# ═══════════════════════════════════════════════════════════════════════════════

def fetch_active_players(team_id: int | None = None, per_page: int = 100) -> list[dict]:
    """Return active players from BDL with basic info.

    Each dict: {player_id, first_name, last_name, name, team_abbreviation, position}
    """
    if not _BDL_AVAILABLE:
        return []
    try:
        players = _bdl.get_active_players(team_id=team_id, per_page=per_page)
        result: list[dict] = []
        for p in players:
            team = p.get("team", {})
            first = p.get("first_name", "")
            last = p.get("last_name", "")
            result.append({
                "player_id": p.get("id"),
                "first_name": first,
                "last_name": last,
                "name": f"{first} {last}".strip(),
                "team_abbreviation": team.get("abbreviation", ""),
                "position": _BDL_POSITION_MAP.get(p.get("position", ""), "SF"),
            })
        return result
    except Exception as exc:
        _logger.warning("BDL bridge: fetch_active_players failed: %s", exc)
        return []


def get_team_id_by_abbreviation(abbrev: str) -> int | None:
    """Look up BDL team ID from a 3-letter abbreviation."""
    if not _BDL_AVAILABLE:
        return None
    try:
        teams = _bdl.get_teams()
        for t in teams:
            if t.get("abbreviation", "") == abbrev:
                return t.get("id")
        return None
    except Exception:
        return None


# ═══════════════════════════════════════════════════════════════════════════════
# League Leaders
# ═══════════════════════════════════════════════════════════════════════════════

def fetch_league_leaders(stat_type: str = "pts", season: int | None = None) -> list[dict]:
    """Return league leaders from BDL."""
    if not _BDL_AVAILABLE:
        return []
    if season is None:
        season = _current_season_int()
    try:
        return _bdl.get_leaders(stat_type=stat_type, season=season)
    except Exception as exc:
        _logger.warning("BDL bridge: fetch_league_leaders failed: %s", exc)
        return []


# ═══════════════════════════════════════════════════════════════════════════════
# Box Scores
# ═══════════════════════════════════════════════════════════════════════════════

def fetch_box_scores_live() -> list:
    """Return live box scores from BDL."""
    if not _BDL_AVAILABLE:
        return []
    try:
        return _bdl.get_box_scores_live()
    except Exception as exc:
        _logger.warning("BDL bridge: fetch_box_scores_live failed: %s", exc)
        return []


def fetch_box_scores(date: str) -> list:
    """Return box scores for a date from BDL."""
    if not _BDL_AVAILABLE:
        return []
    try:
        return _bdl.get_box_scores(date=date)
    except Exception as exc:
        _logger.warning("BDL bridge: fetch_box_scores failed: %s", exc)
        return []


# ═══════════════════════════════════════════════════════════════════════════════
# Lineups & Play-by-Play
# ═══════════════════════════════════════════════════════════════════════════════

def fetch_lineups(game_id: int) -> list:
    """Return lineup data for a game from BDL."""
    if not _BDL_AVAILABLE:
        return []
    try:
        return _bdl.get_lineups(game_id=game_id)
    except Exception as exc:
        _logger.warning("BDL bridge: fetch_lineups failed: %s", exc)
        return []


def fetch_plays(game_id: int | None = None, per_page: int = 100) -> list:
    """Return play-by-play data from BDL."""
    if not _BDL_AVAILABLE:
        return []
    try:
        return _bdl.get_plays(game_id=game_id, per_page=per_page)
    except Exception as exc:
        _logger.warning("BDL bridge: fetch_plays failed: %s", exc)
        return []
