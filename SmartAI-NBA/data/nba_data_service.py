# ============================================================
# FILE: data/nba_data_service.py
# PURPOSE: Thin delegation layer that routes ALL NBA data
#          retrieval through data/etl_data_service.py
#          (SmartPicksProAI database).
#
#          NO live API calls are made from this module.
#          All data originates from the SmartPicksProAI ETL
#          pipeline SQLite database.
#
# DATA SOURCES:
#   1. SmartPicksProAI DB (via etl_data_service.py) — all NBA data
#   2. PrizePicks / Underdog / DraftKings (via sportsbook_service.py
#      and platform_fetcher.py) — props only, unchanged
# ============================================================

from __future__ import annotations

import datetime as _datetime
import json as _json
import logging as _logging
from pathlib import Path as _Path
from typing import Any

try:
    from utils.logger import get_logger
    _logger = get_logger(__name__)
except ImportError:
    _logger = _logging.getLogger(__name__)

# ── ETL data service — sole data source ──────────────────────
import data.etl_data_service as _etl

# ── File-cache helper (for class wrapper) ────────────────────
try:
    from utils.cache import FileCache as _FileCache
    _HAS_FILE_CACHE = True
except ImportError:
    _HAS_FILE_CACHE = False

# ============================================================
# INLINED CONSTANTS (previously imported from live_data_fetcher)
# ============================================================

DATA_DIRECTORY: _Path = _Path(__file__).parent
PLAYERS_CSV_PATH: _Path = DATA_DIRECTORY / "players.csv"
TEAMS_CSV_PATH: _Path = DATA_DIRECTORY / "teams.csv"
DEFENSIVE_RATINGS_CSV_PATH: _Path = DATA_DIRECTORY / "defensive_ratings.csv"
LAST_UPDATED_JSON_PATH: _Path = DATA_DIRECTORY / "last_updated.json"
INJURY_STATUS_JSON_PATH: _Path = DATA_DIRECTORY / "injury_status.json"

API_DELAY_SECONDS: float = 1.5
FALLBACK_POINTS_STD_RATIO: float = 0.3
FALLBACK_REBOUNDS_STD_RATIO: float = 0.4
FALLBACK_ASSISTS_STD_RATIO: float = 0.4
FALLBACK_THREES_STD_RATIO: float = 0.55
FALLBACK_STEALS_STD_RATIO: float = 0.5
FALLBACK_BLOCKS_STD_RATIO: float = 0.6
FALLBACK_TURNOVERS_STD_RATIO: float = 0.4
MIN_MINUTES_THRESHOLD: float = 15.0
GP_ABSENT_THRESHOLD: int = 12
MIN_TEAM_GP_FOR_RECENCY_CHECK: int = 20
HOT_TREND_THRESHOLD: float = 1.1
COLD_TREND_THRESHOLD: float = 0.9
DEFAULT_VEGAS_SPREAD: float = 0.0
DEFAULT_GAME_TOTAL: float = 220.0
ESPN_API_TIMEOUT_SECONDS: int = 10

INACTIVE_INJURY_STATUSES: frozenset = frozenset({
    "Out",
    "Doubtful",
    "Questionable",
    "Injured Reserve",
    "Out (No Recent Games)",
    "Suspended",
    "Not With Team",
    "G League - Two-Way",
    "G League - On Assignment",
    "G League",
})

GTD_INJURY_STATUSES: frozenset = frozenset({
    "GTD",
    "Day-to-Day",
})

TEAM_NAME_TO_ABBREVIATION: dict[str, str] = {
    "Atlanta Hawks": "ATL",
    "Boston Celtics": "BOS",
    "Brooklyn Nets": "BKN",
    "Charlotte Hornets": "CHA",
    "Chicago Bulls": "CHI",
    "Cleveland Cavaliers": "CLE",
    "Dallas Mavericks": "DAL",
    "Denver Nuggets": "DEN",
    "Detroit Pistons": "DET",
    "Golden State Warriors": "GSW",
    "Houston Rockets": "HOU",
    "Indiana Pacers": "IND",
    "Los Angeles Clippers": "LAC",
    "Los Angeles Lakers": "LAL",
    "Memphis Grizzlies": "MEM",
    "Miami Heat": "MIA",
    "Milwaukee Bucks": "MIL",
    "Minnesota Timberwolves": "MIN",
    "New Orleans Pelicans": "NOP",
    "New York Knicks": "NYK",
    "Oklahoma City Thunder": "OKC",
    "Orlando Magic": "ORL",
    "Philadelphia 76ers": "PHI",
    "Phoenix Suns": "PHX",
    "Portland Trail Blazers": "POR",
    "Sacramento Kings": "SAC",
    "San Antonio Spurs": "SAS",
    "Toronto Raptors": "TOR",
    "Utah Jazz": "UTA",
    "Washington Wizards": "WAS",
}

NBA_API_ABBREV_TO_OURS: dict[str, str] = {
    "ATL": "ATL", "BOS": "BOS", "BKN": "BKN", "CHA": "CHA",
    "CHI": "CHI", "CLE": "CLE", "DAL": "DAL", "DEN": "DEN",
    "DET": "DET", "GSW": "GSW", "HOU": "HOU", "IND": "IND",
    "LAC": "LAC", "LAL": "LAL", "MEM": "MEM", "MIA": "MIA",
    "MIL": "MIL", "MIN": "MIN", "NOP": "NOP", "NYK": "NYK",
    "OKC": "OKC", "ORL": "ORL", "PHI": "PHI", "PHX": "PHX",
    "POR": "POR", "SAC": "SAC", "SAS": "SAS", "TOR": "TOR",
    "UTA": "UTA", "WAS": "WAS",
    # Common alternative abbreviations
    "GS": "GSW", "NY": "NYK", "NO": "NOP", "SA": "SAS",
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


# ============================================================
# Utility helpers
# ============================================================

def _nba_today_et() -> _datetime.date:
    """Return today's date anchored to US/Eastern time."""
    try:
        from zoneinfo import ZoneInfo
        _eastern = ZoneInfo("America/New_York")
    except ImportError:
        _eastern = _datetime.timezone(_datetime.timedelta(hours=-5))
    return _datetime.datetime.now(_eastern).date()


def _current_season() -> str:
    """Return the current NBA season string in 'YYYY-YY' format."""
    now = _datetime.date.today()
    year = now.year if now.month >= 10 else now.year - 1
    return f"{year}-{str(year + 1)[-2:]}"


def save_last_updated(data_type: str) -> None:
    """Save the current timestamp to last_updated.json for a given data type."""
    existing: dict = {}
    if LAST_UPDATED_JSON_PATH.exists():
        try:
            with open(LAST_UPDATED_JSON_PATH, "r") as f:
                existing = _json.load(f)
        except Exception:
            existing = {}
    existing[data_type] = _datetime.datetime.now(_datetime.timezone.utc).isoformat()
    existing["is_live"] = True
    try:
        with open(LAST_UPDATED_JSON_PATH, "w") as f:
            _json.dump(existing, f, indent=2)
    except Exception as exc:
        _logger.warning("Could not save timestamp: %s", exc)


def load_last_updated() -> dict:
    """Load all timestamps from last_updated.json."""
    if not LAST_UPDATED_JSON_PATH.exists():
        return {}
    try:
        with open(LAST_UPDATED_JSON_PATH, "r") as f:
            return _json.load(f)
    except Exception:
        return {}


def get_teams_staleness_warning() -> str | None:
    """Return a warning string if team data is stale, or None if fresh."""
    _WARN_DAYS = 7
    _STALE_DAYS = 14
    timestamps = load_last_updated()
    teams_ts_str = timestamps.get("teams")
    if not teams_ts_str:
        return "⚠️ teams.csv has never been updated — run Data Feed → Fetch Team Stats."
    try:
        teams_ts = _datetime.datetime.fromisoformat(str(teams_ts_str))
        _now_utc = _datetime.datetime.now(_datetime.timezone.utc)
        if teams_ts.tzinfo is None:
            teams_ts = teams_ts.replace(tzinfo=_datetime.timezone.utc)
        age_days = (_now_utc - teams_ts).total_seconds() / 86400.0
        if age_days >= _STALE_DAYS:
            return (
                f"🔴 Team data is **{age_days:.0f} days old** — seriously stale! "
                "Go to 📡 Data Feed → Fetch Team Stats to refresh defensive ratings."
            )
        if age_days >= _WARN_DAYS:
            return (
                f"🟡 Team data is **{age_days:.0f} days old**. "
                "Consider refreshing via 📡 Data Feed → Fetch Team Stats."
            )
    except Exception:
        return "⚠️ Could not determine team data age — check last_updated.json."
    return None


# ============================================================
# Sportsbook / props re-exports
# ============================================================

try:
    from data.sportsbook_service import (  # noqa: F401
        get_prizepicks_props,
        get_underdog_props,
        get_draftkings_props,
        get_all_sportsbook_props,
        smart_filter_props,
        parse_alt_lines_from_platform_props,
        enrich_props_with_csv_names,
    )
except ImportError:
    def get_prizepicks_props(*a, **kw):
        return []

    def get_underdog_props(*a, **kw):
        return []

    def get_draftkings_props(*a, **kw):
        return []

    def get_all_sportsbook_props(*a, **kw):
        return {"prizepicks": [], "underdog": [], "draftkings": []}

    def smart_filter_props(*a, **kw):
        return []

    def parse_alt_lines_from_platform_props(*a, **kw):
        return []

    def enrich_props_with_csv_names(*a, **kw):
        return []


# ============================================================
# Public API — Core functions
# ============================================================


def get_todays_games() -> list:
    """Retrieve tonight's NBA games from the SmartPicksProAI database."""
    result = _etl.get_todays_games()
    if not result:
        _logger.debug("get_todays_games: DB returned no games for today")
    return result


def get_todays_players(todays_games, progress_callback=None,
                       precomputed_injury_map=None) -> list:
    """Retrieve players for tonight's games from the DB by team."""
    if not todays_games:
        return []

    all_players: list[dict] = []
    total = len(todays_games)

    # Build a set of team abbreviations from the games
    team_abbrevs: set[str] = set()
    for g in todays_games:
        for key in ("home_team", "away_team", "home_abbrev", "away_abbrev"):
            t = str(g.get(key, "")).upper().strip()
            if t and len(t) <= 4:
                team_abbrevs.add(t)
        # Parse matchup string (e.g. "LAL vs. BOS")
        matchup = g.get("matchup", "")
        if matchup:
            for part in str(matchup).replace("@", "vs.").split("vs."):
                part = part.strip().upper()
                if len(part) == 3:
                    team_abbrevs.add(part)

    if progress_callback:
        progress_callback(0, total, "Loading players from DB…")

    # Get all players from DB, then filter by team
    db_players = _etl.get_all_players()
    for p in db_players:
        p_team = str(p.get("team_abbreviation", "")).upper().strip()
        if p_team in team_abbrevs:
            player_dict = {
                "player_id": p.get("player_id"),
                "name": f"{p.get('first_name', '')} {p.get('last_name', '')}".strip(),
                "team": p.get("team_abbreviation", ""),
                "position": p.get("position", ""),
                "gp": p.get("gp", 0),
                "ppg": p.get("ppg", 0.0),
                "rpg": p.get("rpg", 0.0),
                "apg": p.get("apg", 0.0),
                "spg": p.get("spg", 0.0),
                "bpg": p.get("bpg", 0.0),
                "topg": p.get("topg", 0.0),
                "mpg": p.get("mpg", 0.0),
                "fg3_avg": p.get("fg3_avg", 0.0),
                "ftm_avg": p.get("ftm_avg", 0.0),
                "fta_avg": p.get("fta_avg", 0.0),
                "ft_pct_avg": p.get("ft_pct_avg", 0.0),
                "fgm_avg": p.get("fgm_avg", 0.0),
                "fga_avg": p.get("fga_avg", 0.0),
                "fg_pct_avg": p.get("fg_pct_avg", 0.0),
                "oreb_avg": p.get("oreb_avg", 0.0),
                "dreb_avg": p.get("dreb_avg", 0.0),
                "pf_avg": p.get("pf_avg", 0.0),
                "plus_minus_avg": p.get("plus_minus_avg", 0.0),
                "points_std": p.get("points_std", 0.0),
                "rebounds_std": p.get("rebounds_std", 0.0),
                "assists_std": p.get("assists_std", 0.0),
                "threes_std": p.get("threes_std", 0.0),
            }
            # Apply injury map if provided
            if precomputed_injury_map:
                name = player_dict["name"]
                if name in precomputed_injury_map:
                    player_dict["injury_status"] = precomputed_injury_map[name]
            all_players.append(player_dict)

    if progress_callback:
        progress_callback(total, total, f"Loaded {len(all_players)} players from DB.")

    return all_players


def get_player_recent_form(player_id, last_n_games: int = 10) -> dict:
    """Get a player's recent-form stats from the DB."""
    logs = _etl.get_player_game_logs(player_id, limit=last_n_games)
    if not logs:
        return {}

    def safe_avg(values: list) -> float:
        valid = [v for v in values if v is not None]
        return round(sum(valid) / len(valid), 1) if valid else 0.0

    def parse_min(m) -> float:
        if m is None:
            return 0.0
        m = str(m)
        try:
            if ":" in m:
                parts = m.split(":")
                return float(parts[0]) + float(parts[1]) / 60.0
            return float(m)
        except (ValueError, TypeError):
            return 0.0

    return {
        "games_played": len(logs),
        "ppg": safe_avg([float(g.get("pts", 0) or 0) for g in logs]),
        "rpg": safe_avg([float(g.get("reb", 0) or 0) for g in logs]),
        "apg": safe_avg([float(g.get("ast", 0) or 0) for g in logs]),
        "spg": safe_avg([float(g.get("stl", 0) or 0) for g in logs]),
        "bpg": safe_avg([float(g.get("blk", 0) or 0) for g in logs]),
        "topg": safe_avg([float(g.get("tov", 0) or 0) for g in logs]),
        "mpg": safe_avg([parse_min(g.get("min")) for g in logs]),
        "fg3_avg": safe_avg([float(g.get("fg3m", 0) or 0) for g in logs]),
    }


def get_player_stats(progress_callback=None) -> list:
    """Retrieve all active player season stats from the DB."""
    if progress_callback:
        progress_callback(0, 1, "Loading player stats from DB…")
    result = _etl.get_all_players()
    if progress_callback:
        progress_callback(1, 1, f"Loaded {len(result)} players.")
    return result


def get_team_stats(progress_callback=None) -> list:
    """Retrieve team-level stats from the DB."""
    if progress_callback:
        progress_callback(0, 1, "Loading team stats from DB…")
    result = _etl.get_teams()
    if progress_callback:
        progress_callback(1, 1, f"Loaded {len(result)} teams.")
    return result


def get_defensive_ratings(force: bool = False, progress_callback=None) -> list:
    """Retrieve all defensive-vs-position ratings from the DB."""
    if progress_callback:
        progress_callback(0, 1, "Loading defensive ratings from DB…")
    result = _etl.get_all_defense_vs_position()
    if progress_callback:
        progress_callback(1, 1, f"Loaded {len(result)} defensive rating rows.")
    return result


def get_player_game_log(player_id, last_n_games: int = 20) -> list:
    """Retrieve a player's game log from the DB."""
    result = _etl.get_player_game_logs(player_id, limit=last_n_games)
    if not result:
        _logger.debug("get_player_game_log(%s): DB returned no logs", player_id)
    return result


def get_all_data(progress_callback=None, targeted: bool = False,
                 todays_games=None) -> dict:
    """Orchestrate: games + players + teams from the DB."""
    result: dict[str, Any] = {"games": [], "players": [], "teams": []}
    total = 3
    step = 0

    step += 1
    if progress_callback:
        progress_callback(step, total, "Loading games…")
    result["games"] = todays_games if todays_games else get_todays_games()

    step += 1
    if progress_callback:
        progress_callback(step, total, "Loading players…")
    if result["games"]:
        result["players"] = get_todays_players(result["games"])
    else:
        result["players"] = _etl.get_all_players()

    step += 1
    if progress_callback:
        progress_callback(step, total, "Loading teams…")
    result["teams"] = _etl.get_teams()

    return result


def get_all_todays_data(progress_callback=None) -> dict:
    """One-click: retrieve games + players from the DB for tonight."""
    total = 2
    if progress_callback:
        progress_callback(0, total, "Loading today's games…")
    games = get_todays_games()

    if progress_callback:
        progress_callback(1, total, "Loading today's players…")
    players = get_todays_players(games) if games else []

    if progress_callback:
        progress_callback(2, total, f"Done: {len(games)} games, {len(players)} players.")
    return {"games": games, "players": players}


def get_active_rosters(team_abbrevs=None, progress_callback=None) -> dict:
    """Retrieve active rosters for specified teams from the DB."""
    if not team_abbrevs:
        return {}
    result: dict[str, list] = {}
    # Build a team abbrev → team_id map from the DB
    all_teams = _etl.get_teams()
    abbrev_to_id = {
        str(t.get("abbreviation", "")).upper(): t.get("team_id")
        for t in all_teams
    }
    for abbrev in team_abbrevs:
        abbrev_upper = abbrev.upper()
        team_id = abbrev_to_id.get(abbrev_upper)
        if team_id:
            roster = _etl.get_team_roster(team_id)
            result[abbrev_upper] = [
                f"{r.get('first_name', '')} {r.get('last_name', '')}".strip()
                for r in roster
            ]
        else:
            result[abbrev_upper] = []
    return result


def get_standings(progress_callback=None) -> list:
    """Retrieve current NBA standings from the DB."""
    if progress_callback:
        progress_callback(0, 1, "Loading standings from DB…")
    result = _etl.get_standings()
    if progress_callback:
        progress_callback(1, 1, f"Standings loaded ({len(result)} rows).")
    return result


def get_standings_from_nba_api(season: str | None = None) -> list:
    """Retrieve NBA standings (same DB source as get_standings)."""
    try:
        return _etl.get_standings()
    except Exception as exc:
        _logger.warning("get_standings_from_nba_api failed: %s", exc)
        return []


def get_league_leaders(stat_category: str = "PTS",
                       season: str | None = None) -> list:
    """Return league leaders from the DB."""
    return _etl.get_league_leaders()


# ============================================================
# TIER 1: Game-level wrappers
# ============================================================

def get_player_game_logs_v2(player_id: int, season: str | None = None,
                            last_n: int = 0) -> list:
    """Return per-game stats from the DB."""
    limit = last_n if last_n > 0 else None
    return _etl.get_player_game_logs(player_id, limit=limit)


def get_box_score_traditional(game_id: str, period: int = 0) -> dict:
    """Return traditional box score from the DB (via advanced box score)."""
    result = _etl.get_box_score_advanced(game_id)
    return {"players": result} if result else {}


def get_box_score_advanced(game_id: str) -> dict:
    """Return advanced box score for a game from the DB."""
    result = _etl.get_box_score_advanced(game_id)
    return {"players": result} if result else {}


def get_box_score_usage(game_id: str) -> dict:
    """Return usage box score for a game from the DB."""
    result = _etl.get_box_score_usage(game_id)
    return {"players": result} if result else {}


def get_player_on_off(team_id: int, season: str | None = None) -> dict:
    """On/Off court stats — not yet in DB schema; returns empty."""
    return {}


def get_player_estimated_metrics(season: str | None = None) -> list:
    """Return player estimated advanced metrics from the DB."""
    return _etl.get_player_estimated_metrics(season=season)


def get_player_fantasy_profile(player_id: int,
                               season: str | None = None) -> dict:
    """Build a fantasy-style profile from league dash + estimated metrics."""
    profile = _etl.get_player_season_profile(player_id=player_id, season=season)
    if profile:
        return profile[0]
    return {}


def get_rotations(game_id: str) -> dict:
    """Return rotation (sub-in/out) data for a game from the DB."""
    result = _etl.get_game_rotation(game_id)
    return {"rotations": result} if result else {}


def get_schedule(game_date: str | None = None) -> list:
    """Return the game schedule from the DB."""
    return _etl.get_schedule()


def get_todays_scoreboard() -> dict:
    """Build a scoreboard dict from today's games."""
    games = get_todays_games()
    return {"games": games, "game_count": len(games)}


# ============================================================
# TIER 2: High-value wrappers — powered by SmartPicksProAI DB
# ============================================================

def get_box_score_matchups(game_id: str) -> dict:
    """Return defensive matchup data for a game from the DB."""
    result = _etl.get_box_score_matchups(game_id)
    return {"matchups": result} if result else {}


def get_hustle_box_score(game_id: str) -> dict:
    """Return hustle box score for a game from the DB."""
    result = _etl.get_box_score_hustle(game_id)
    return {"players": result} if result else {}


def get_defensive_box_score(game_id: str) -> dict:
    """Return misc/defensive box score for a game from the DB."""
    result = _etl.get_box_score_misc(game_id)
    return {"players": result} if result else {}


def get_scoring_box_score(game_id: str) -> dict:
    """Return scoring breakdown box score for a game from the DB."""
    result = _etl.get_box_score_scoring(game_id)
    return {"players": result} if result else {}


def get_tracking_box_score(game_id: str) -> dict:
    """Return player tracking stats for a game from the DB."""
    result = _etl.get_player_tracking_stats(game_id)
    return {"players": result} if result else {}


def get_four_factors_box_score(game_id: str) -> dict:
    """Return four-factors box score for a game from the DB."""
    result = _etl.get_box_score_four_factors(game_id)
    return {"players": result} if result else {}


def get_player_shooting_splits(player_id: int,
                               season: str | None = None) -> dict:
    """Return shot chart data as shooting splits for a player."""
    shots = _etl.get_shot_chart(player_id, season=season)
    return {"shots": shots} if shots else {}


def get_shot_chart_v2(player_id: int, season: str | None = None) -> list:
    """Return shot chart data for a player from the DB."""
    return _etl.get_shot_chart(player_id, season=season)


def get_player_clutch_stats(season: str | None = None) -> list:
    """Return player clutch-time stats from the DB."""
    return _etl.get_player_clutch_stats(season=season)


def get_team_lineups(team_id: int, season: str | None = None) -> list:
    """Return lineup data for a team from the DB."""
    return _etl.get_league_lineups(team_id=team_id, season=season)


def get_team_dashboard(team_id: int, season: str | None = None) -> dict:
    """Return team season profile from the DB."""
    result = _etl.get_team_season_profile(team_id=team_id, season=season)
    if result:
        return result[0]
    return {}


def get_team_game_logs(team_id: int, season: str | None = None,
                       last_n: int = 0) -> list:
    """Return team-level game stats from the DB."""
    return _etl.get_team_game_stats(team_id=team_id)


def get_player_year_over_year(player_id: int) -> list:
    """Return year-over-year career stats from the DB."""
    result = _etl.get_player_career(player_id)
    return result if result else []


# ============================================================
# TIER 3: Reference & context — powered by SmartPicksProAI DB
# ============================================================

def get_player_vs_player(player1_id: int, player2_id: int,
                         season: str | None = None) -> dict:
    """Player-vs-player head-to-head — not yet in DB schema."""
    return {}


def get_win_probability(game_id: str) -> dict:
    """Return win probability data for a game from the DB."""
    result = _etl.get_win_probability(game_id)
    return {"events": result} if result else {}


def get_play_by_play_v2(game_id: str) -> list:
    """Return play-by-play events for a game from the DB."""
    return _etl.get_play_by_play(game_id)


def get_game_summary(game_id: str) -> dict:
    """Build a game summary from Games + Team_Game_Stats in the DB."""
    from data.etl_data_service import get_team_game_stats as _get_tgs
    tgs = _get_tgs(game_id=game_id)
    # Also fetch the game info itself
    games = _etl.get_recent_games(limit=500)
    game_info = next((g for g in games if g.get("game_id") == game_id), None)
    return {"game": game_info, "team_stats": tgs}


def get_team_streak_finder(team_id: int,
                           season: str | None = None) -> list:
    """Return team game-by-game stats for streak analysis from the DB."""
    return _etl.get_team_game_stats(team_id=team_id)


# ============================================================
# Utility functions
# ============================================================

def get_player_news(player_name: str | None = None, limit: int = 20) -> list:
    """Player news — not available from DB."""
    return []


def get_game_logs_from_nba_api(player_name: str,
                               season: str | None = None) -> list:
    """Resolve player_name via the DB and fetch game logs."""
    try:
        player = _etl.get_player_by_name(player_name)
        if not player or not player.get("player_id"):
            _logger.debug("get_game_logs_from_nba_api: no player for %r", player_name)
            return []
        return _etl.get_player_game_logs(player["player_id"])
    except Exception as exc:
        _logger.warning("get_game_logs_from_nba_api(%r) failed: %s", player_name, exc)
    return []


def refresh_historical_data_for_tonight(
    games=None, last_n_games: int = 30, progress_callback=None,
) -> dict:
    """No-op — historical data is pre-populated in the DB."""
    return {"players_refreshed": 0, "clv_updated": 0, "errors": 0}


def refresh_all_data(progress_callback=None) -> dict:
    """Refresh all core data sources from the DB."""
    result: dict[str, Any] = {
        "games": [],
        "players": [],
        "team_stats": None,
        "injuries": None,
        "errors": [],
    }
    total_steps = 4
    step = 0

    step += 1
    if progress_callback:
        progress_callback(step, total_steps, "Fetching today's games…")
    try:
        result["games"] = get_todays_games()
    except Exception as exc:
        _logger.error("refresh_all_data — games failed: %s", exc)
        result["errors"].append(f"Games: {exc}")

    step += 1
    if progress_callback:
        progress_callback(step, total_steps, "Fetching players…")
    if result["games"]:
        try:
            result["players"] = get_todays_players(result["games"])
        except Exception as exc:
            _logger.error("refresh_all_data — players failed: %s", exc)
            result["errors"].append(f"Players: {exc}")

    step += 1
    if progress_callback:
        progress_callback(step, total_steps, "Fetching team stats…")
    try:
        result["team_stats"] = _etl.get_teams()
    except Exception as exc:
        _logger.error("refresh_all_data — team stats failed: %s", exc)
        result["errors"].append(f"Team stats: {exc}")

    step += 1
    if progress_callback:
        progress_callback(step, total_steps, "Fetching injury data…")
    try:
        result["injuries"] = _etl.get_injuries()
    except Exception as exc:
        _logger.error("refresh_all_data — injuries failed: %s", exc)
        result["errors"].append(f"Injuries: {exc}")

    _logger.info(
        "refresh_all_data: games=%d, players=%d, errors=%d",
        len(result["games"]),
        len(result["players"]),
        len(result["errors"]),
    )
    return result


def refresh_from_etl(progress_callback=None) -> dict:
    """Incremental ETL update via etl_data_service."""
    return _etl.refresh_data()


def full_refresh_from_etl(season: str | None = None,
                          progress_callback=None) -> dict:
    """Full ETL pull via etl_data_service."""
    return _etl.refresh_data()


def clear_caches() -> None:
    """Clear caches — no-op in DB-only mode."""
    _logger.info("clear_caches: no external caches to clear in DB-only mode")
    try:
        from utils.cache import cache_clear
        cache_clear()
    except Exception:
        pass


def get_cached_roster(team_abbrev: str) -> list:
    """Return the active roster for a team from the DB."""
    all_teams = _etl.get_teams()
    abbrev_to_id = {
        str(t.get("abbreviation", "")).upper(): t.get("team_id")
        for t in all_teams
    }
    team_id = abbrev_to_id.get(team_abbrev.upper())
    if not team_id:
        return []
    roster = _etl.get_team_roster(team_id)
    return [
        f"{r.get('first_name', '')} {r.get('last_name', '')}".strip()
        for r in roster
    ]


# ============================================================
# NBADataService — class-based API wrapper
# ============================================================

class NBADataService:
    """
    Class-based service for NBA data operations.

    Wraps the existing module-level functions in an OOP interface.
    All data comes from the SmartPicksProAI database.
    """

    def __init__(self):
        if _HAS_FILE_CACHE:
            self.cache = _FileCache(cache_dir="cache/service", ttl_hours=1)
        else:
            self.cache = None

    def get_todays_games(self):
        return get_todays_games()

    def get_todays_players(self, games, progress_callback=None,
                           precomputed_injury_map=None):
        return get_todays_players(
            games,
            progress_callback=progress_callback,
            precomputed_injury_map=precomputed_injury_map,
        )

    def get_team_stats(self, progress_callback=None):
        return get_team_stats(progress_callback=progress_callback)

    def get_injuries(self):
        injuries = _etl.get_injuries()
        return {"injuries": injuries, "source": "database"}

    def clear_caches(self):
        clear_caches()

    def refresh_all_data(self, progress_callback=None):
        return refresh_all_data(progress_callback=progress_callback)
