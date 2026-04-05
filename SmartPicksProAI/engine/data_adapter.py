"""
data_adapter.py
---------------
Bridge between SmartPicksProAI's SQLite backend and the engine modules.

Transforms raw database rows (Player_Game_Logs, Players, Teams, etc.) into
the dict structures expected by the engine's projection, simulation, edge
detection, confidence, and explainer modules.

All functions are pure transformations — no database access, no I/O.
They receive the data that the FastAPI endpoint has already queried and
return the reshaped dicts the engine functions require.

Usage::

    from engine.data_adapter import (
        build_engine_player_data,
        build_engine_game_logs,
        build_engine_game_context,
        build_engine_defense_data,
    )
"""

from __future__ import annotations

import math
from typing import Any


# ---------------------------------------------------------------------------
# Stat-key mapping: DB column names → engine expected names
# ---------------------------------------------------------------------------

#: Maps DB column names from Player_Game_Logs to the aliases that the engine
#: modules recognise when looking up stat values.  The engine modules check
#: multiple key aliases (e.g., "pts" or "points"), so we keep the DB names
#: as-is and only add the aliases the engine also needs.
_STAT_AVG_MAP: dict[str, str] = {
    "pts": "points_avg",
    "reb": "rebounds_avg",
    "ast": "assists_avg",
    "fg3m": "threes_avg",
    "stl": "steals_avg",
    "blk": "blocks_avg",
    "tov": "turnovers_avg",
    "ftm": "ftm_avg",
    "fta": "fta_avg",
    "fga": "fga_avg",
    "fgm": "fgm_avg",
    "oreb": "offensive_rebounds_avg",
    "dreb": "defensive_rebounds_avg",
    "pf": "personal_fouls_avg",
}

#: Minute string formats from the DB ("35:42") need conversion to floats.
def _parse_minutes(raw: Any) -> float:
    """Convert a minutes value to a float.

    Handles ``'35:42'`` (mm:ss), ``35.7`` (already float), ``'35'`` (int
    string), and ``None``.
    """
    if raw is None:
        return 0.0
    s = str(raw).strip()
    if not s:
        return 0.0
    if ":" in s:
        parts = s.split(":")
        try:
            return float(parts[0]) + float(parts[1]) / 60.0
        except (ValueError, IndexError):
            return 0.0
    try:
        return float(s)
    except ValueError:
        return 0.0


# ---------------------------------------------------------------------------
# build_engine_player_data
# ---------------------------------------------------------------------------

def build_engine_player_data(
    player_row: dict,
    game_logs: list[dict],
    season_game_count: int | None = None,
) -> dict:
    """Build the ``player_data`` dict expected by ``projections.build_player_projection()``.

    The engine expects keys like ``points_avg``, ``rebounds_avg``, ``position``,
    ``games_played``, ``minutes_avg``.  This function computes season averages
    from the provided game logs and merges them with player metadata.

    Args:
        player_row: A single Players-table row dict (``player_id``,
            ``first_name``, ``last_name``, ``position``, ``team_abbreviation``).
        game_logs: Full list of Player_Game_Logs rows (dicts) for this
            season, ordered by game_date descending (newest first).
        season_game_count: Override for number of games played.  When
            ``None``, ``len(game_logs)`` is used.

    Returns:
        Dict ready to be passed as ``player_data`` to
        ``projections.build_player_projection()``.
    """
    n = season_game_count if season_game_count is not None else len(game_logs)
    n = max(n, 1)  # guard against division by zero

    # Compute season averages from all game logs
    averages: dict[str, float] = {}
    for db_key, engine_key in _STAT_AVG_MAP.items():
        total = sum(float(g.get(db_key) or 0) for g in game_logs)
        averages[engine_key] = round(total / n, 2)

    # Minutes average (special handling for mm:ss format)
    total_min = sum(_parse_minutes(g.get("min")) for g in game_logs)
    averages["minutes_avg"] = round(total_min / n, 2)

    return {
        # Metadata
        "player_id": player_row.get("player_id"),
        "name": (
            f"{player_row.get('first_name', '')} "
            f"{player_row.get('last_name', '')}"
        ).strip(),
        "team": player_row.get("team_abbreviation", ""),
        "position": player_row.get("position", "SF"),
        "games_played": n,
        "gp": n,
        # Season averages (engine-format keys)
        **averages,
    }


# ---------------------------------------------------------------------------
# build_engine_game_logs
# ---------------------------------------------------------------------------

def build_engine_game_logs(
    db_logs: list[dict],
    opponent_abbrevs: dict[str, str] | None = None,
) -> list[dict]:
    """Transform DB game-log rows into the format engine modules expect.

    Engine modules (matchup_history, rotation_tracker, etc.) look for keys
    like ``pts``, ``reb``, ``ast``, ``MIN`` (or ``min``), and an opponent
    key (``opp``, ``opponent``, or ``matchup``).

    The DB already stores ``pts``, ``reb``, ``ast``, etc., so the main work
    is:

    1. Parsing minutes from ``'35:42'`` → ``35.7``.
    2. Adding an ``opp`` field derived from the game's matchup string.
    3. Passing through all original keys untouched.

    Args:
        db_logs: Raw game-log dicts from the DB (with Games-table columns
            joined in, including ``matchup``, ``game_date``, etc.).
        opponent_abbrevs: Optional mapping of ``game_id`` → opponent team
            abbreviation.  When provided, sets the ``opp`` field directly.

    Returns:
        List of enriched game-log dicts.
    """
    result: list[dict] = []
    for row in db_logs:
        entry = dict(row)  # shallow copy

        # Parse minutes to a float and store as both 'MIN' and 'minutes'
        raw_min = entry.get("min", "")
        parsed = _parse_minutes(raw_min)
        entry["MIN"] = parsed
        entry["minutes"] = parsed

        # Derive opponent abbreviation from matchup string or lookup
        if opponent_abbrevs and entry.get("game_id") in opponent_abbrevs:
            entry["opp"] = opponent_abbrevs[entry["game_id"]]
        elif entry.get("matchup"):
            # matchup format: "LAL vs. BOS" or "LAL @ BOS"
            matchup = entry["matchup"]
            parts = matchup.replace(".", "").split()
            if len(parts) >= 3:
                entry["opp"] = parts[-1].upper()
        elif entry.get("away_abbrev") and entry.get("home_abbrev"):
            # If player's team_abbreviation is home, opponent is away
            entry["opp"] = entry.get("away_abbrev", "")

        # Keep game_date as GAME_DATE alias (some engine modules check this)
        if "game_date" in entry and "GAME_DATE" not in entry:
            entry["GAME_DATE"] = entry["game_date"]

        result.append(entry)
    return result


# ---------------------------------------------------------------------------
# build_engine_game_context
# ---------------------------------------------------------------------------

def build_engine_game_context(
    game_row: dict,
    player_team_abbrev: str,
    teams_data: list[dict],
    *,
    vegas_spread: float = 0.0,
    game_total: float = 220.0,
) -> dict:
    """Build the ``game_context`` dict used by edge_detection, simulation, etc.

    Args:
        game_row: A Games-table row dict (``game_id``, ``home_abbrev``,
            ``away_abbrev``, ``game_date``, etc.).
        player_team_abbrev: The abbreviation of the player's team.
        teams_data: All teams as a list of dicts (for pace lookup).
        vegas_spread: Vegas point spread (positive = player's team favored).
        game_total: Vegas over/under total for the game.

    Returns:
        Dict with keys: ``opponent``, ``is_home``, ``rest_days``,
        ``game_total``, ``vegas_spread``, ``game_id``, ``game_date``,
        ``home_team``, ``away_team``.
    """
    home = game_row.get("home_abbrev", "")
    away = game_row.get("away_abbrev", "")
    is_home = player_team_abbrev.upper() == home.upper()
    opponent = away if is_home else home

    return {
        "game_id": game_row.get("game_id"),
        "game_date": game_row.get("game_date"),
        "opponent": opponent,
        "is_home": is_home,
        "rest_days": 1,  # default; caller can override with actual data
        "game_total": game_total,
        "vegas_spread": vegas_spread,
        "home_team": home,
        "away_team": away,
    }


# ---------------------------------------------------------------------------
# build_engine_defense_data
# ---------------------------------------------------------------------------

def build_engine_defense_data(defense_rows: list[dict]) -> list[dict]:
    """Transform Defense_Vs_Position rows into the engine's expected format.

    The engine's ``projections.build_player_projection()`` receives a
    ``defensive_ratings_data`` list where each entry has keys:
    ``team``, ``pos``, ``vs_pts_mult``, ``vs_reb_mult``, ``vs_ast_mult``,
    ``vs_stl_mult``, ``vs_blk_mult``, ``vs_3pm_mult``.

    The DB stores ``team_abbreviation`` instead of ``team``, so we alias it.

    Args:
        defense_rows: Raw rows from the Defense_Vs_Position table.

    Returns:
        List of dicts suitable for the projections engine.
    """
    result: list[dict] = []
    for row in defense_rows:
        entry = dict(row)
        # Alias: engine expects "team" key
        if "team_abbreviation" in entry and "team" not in entry:
            entry["team"] = entry["team_abbreviation"]
        result.append(entry)
    return result


# ---------------------------------------------------------------------------
# build_engine_teams_data
# ---------------------------------------------------------------------------

def build_engine_teams_data(teams_rows: list[dict]) -> list[dict]:
    """Transform Teams rows into engine-expected format.

    The engine's projections module looks up team pace via a ``pace`` key
    and uses ``abbreviation`` for matching.  The DB schema already uses
    these exact names, so this is mostly a pass-through with safety defaults.

    Args:
        teams_rows: Raw rows from the Teams table.

    Returns:
        List of team dicts with guaranteed ``pace``, ``ortg``, ``drtg``.
    """
    result: list[dict] = []
    for row in teams_rows:
        entry = dict(row)
        entry.setdefault("pace", 100.0)
        entry.setdefault("ortg", 110.0)
        entry.setdefault("drtg", 110.0)
        result.append(entry)
    return result


# ---------------------------------------------------------------------------
# build_engine_advanced_context
# ---------------------------------------------------------------------------

def build_engine_advanced_context(
    advanced_rows: list[dict],
    estimated_metrics_row: dict | None = None,
) -> dict:
    """Build the ``advanced_context`` dict for ``projections.build_player_projection()``.

    Aggregates per-game advanced box score stats (USG_PCT, TS_PCT, PIE,
    PACE, OFF_RATING, DEF_RATING) into season averages and merges with
    estimated metrics when available.

    Args:
        advanced_rows: Box_Score_Advanced rows for the player this season.
        estimated_metrics_row: Optional Player_Estimated_Metrics row.

    Returns:
        Dict with keys used by ``_compute_usage_boost()`` and new
        advanced projection adjustments.
    """
    ctx: dict[str, Any] = {}

    if advanced_rows:
        # Compute averages of key advanced stats across all games
        _adv_keys = [
            "usg_pct", "ts_pct", "pie", "pace", "off_rating", "def_rating",
            "net_rating", "ast_pct", "reb_pct", "efg_pct",
        ]
        for key in _adv_keys:
            vals = []
            for r in advanced_rows:
                raw = r.get(key)
                if raw is not None:
                    try:
                        vals.append(float(raw))
                    except (ValueError, TypeError):
                        pass
            if vals:
                ctx[key] = round(sum(vals) / len(vals), 4)

        # Alias for backward compat with _compute_usage_boost
        if "usg_pct" in ctx:
            ctx["usage_pct"] = ctx["usg_pct"]

    # Merge estimated metrics (pre-box-score finalization values)
    if estimated_metrics_row:
        for e_key, ctx_key in [
            ("e_usg_pct", "e_usg_pct"),
            ("e_pace", "e_pace"),
            ("e_net_rating", "e_net_rating"),
            ("e_off_rating", "e_off_rating"),
            ("e_def_rating", "e_def_rating"),
            ("e_ast_ratio", "e_ast_ratio"),
            ("e_reb_pct", "e_reb_pct"),
            ("e_tov_pct", "e_tov_pct"),
        ]:
            raw = estimated_metrics_row.get(e_key)
            if raw is not None:
                try:
                    ctx[ctx_key] = float(raw)
                except (ValueError, TypeError):
                    pass

        # Use estimated usage as fallback when box-score USG unavailable
        if "usage_pct" not in ctx and "e_usg_pct" in ctx:
            ctx["usage_pct"] = ctx["e_usg_pct"]

    return ctx


# ---------------------------------------------------------------------------
# build_engine_hustle_context
# ---------------------------------------------------------------------------

def build_engine_hustle_context(hustle_rows: list[dict]) -> dict:
    """Aggregate hustle stats into a per-game average context dict.

    Hustle stats (deflections, contested shots, screen assists, box outs)
    are strong signals for rebounds, steals, blocks, and assists.

    Args:
        hustle_rows: Box_Score_Hustle rows for the player this season.

    Returns:
        Dict with averaged hustle stats.
    """
    if not hustle_rows:
        return {}

    ctx: dict[str, float] = {}
    _keys = [
        "deflections", "contested_shots", "screen_assists",
        "boxouts", "def_boxouts", "off_boxouts",
        "loose_balls_total", "charges_drawn",
        "boxout_player_rebs",
    ]
    n = len(hustle_rows)
    for key in _keys:
        vals = []
        for r in hustle_rows:
            raw = r.get(key)
            if raw is not None:
                try:
                    vals.append(float(raw))
                except (ValueError, TypeError):
                    pass
        if vals:
            ctx[key] = round(sum(vals) / n, 3)

    return ctx


# ---------------------------------------------------------------------------
# build_engine_clutch_context
# ---------------------------------------------------------------------------

def build_engine_clutch_context(clutch_row: dict | None) -> dict:
    """Transform clutch stats into engine-usable context.

    Clutch stats capture how a player performs in close-game situations
    (last 5 minutes, score within 5 points). Differences from overall
    stats indicate late-game role changes.

    Args:
        clutch_row: A Player_Clutch_Stats row, or None.

    Returns:
        Dict with clutch stat values.
    """
    if not clutch_row:
        return {}

    ctx: dict[str, Any] = {}
    _keys = [
        "gp", "min", "pts", "reb", "ast", "stl", "blk", "tov",
        "fgm", "fga", "fg_pct", "fg3m", "fg3a", "fg3_pct",
        "ftm", "fta", "ft_pct", "plus_minus",
    ]
    for key in _keys:
        raw = clutch_row.get(key)
        if raw is not None:
            try:
                ctx[key] = float(raw)
            except (ValueError, TypeError):
                pass

    return ctx


# ---------------------------------------------------------------------------
# build_engine_matchup_defender_context
# ---------------------------------------------------------------------------

def build_engine_matchup_defender_context(
    matchup_rows: list[dict],
) -> dict:
    """Aggregate individual defender matchup data into a context dict.

    Uses Box_Score_Matchups to identify who primarily guards the player
    and how they perform against that specific defender (not just
    positional averages).

    Args:
        matchup_rows: Box_Score_Matchups rows where the player is the
            offensive player (person_id_off = player_id).

    Returns:
        Dict with primary defender stats and overall matchup quality.
    """
    if not matchup_rows:
        return {}

    # Sort by matchup minutes (descending) to find the primary defender
    sorted_rows = sorted(
        matchup_rows,
        key=lambda r: float(r.get("matchup_min", 0) or 0),
        reverse=True,
    )

    # Primary defender = the one who guards this player the most minutes
    primary = sorted_rows[0] if sorted_rows else None
    if not primary:
        return {}

    total_poss = sum(float(r.get("partial_poss", 0) or 0) for r in sorted_rows)
    total_pts = sum(float(r.get("player_pts", 0) or 0) for r in sorted_rows)
    total_fgm = sum(float(r.get("matchup_fgm", 0) or 0) for r in sorted_rows)
    total_fga = sum(float(r.get("matchup_fga", 0) or 0) for r in sorted_rows)

    ctx: dict[str, Any] = {
        "primary_defender_id": primary.get("person_id_def"),
        "primary_defender_min": float(primary.get("matchup_min", 0) or 0),
        "primary_defender_fg_pct": float(primary.get("matchup_fg_pct", 0) or 0),
        "total_matchup_poss": round(total_poss, 1),
    }

    # Points per possession against all defenders
    if total_poss > 0:
        ctx["pts_per_poss"] = round(total_pts / total_poss, 3)

    # Overall FG% when guarded
    if total_fga > 0:
        ctx["matchup_fg_pct_overall"] = round(total_fgm / total_fga, 3)

    return ctx


# ---------------------------------------------------------------------------
# Convenience: extract stat averages from game logs
# ---------------------------------------------------------------------------

def compute_season_averages(game_logs: list[dict]) -> dict[str, float]:
    """Compute per-stat averages from a list of game-log dicts.

    Returns a dict mapping stat keys (``pts``, ``reb``, ``ast``, etc.) to
    their float averages.  Useful for computing a stat's season average
    before passing it to ``matchup_history.calculate_matchup_adjustment()``.
    """
    if not game_logs:
        return {}
    n = len(game_logs)
    keys = ["pts", "reb", "ast", "fg3m", "stl", "blk", "tov",
            "fgm", "fga", "ftm", "fta", "oreb", "dreb", "pf"]
    return {
        k: round(sum(float(g.get(k) or 0) for g in game_logs) / n, 2)
        for k in keys
    }


# ---------------------------------------------------------------------------
# Stat-type → DB column mapping (for prop analysis)
# ---------------------------------------------------------------------------

#: Maps user-facing stat type names to DB column names and engine keys.
STAT_TYPE_TO_DB_COL: dict[str, str] = {
    "points": "pts",
    "rebounds": "reb",
    "assists": "ast",
    "threes": "fg3m",
    "steals": "stl",
    "blocks": "blk",
    "turnovers": "tov",
    "ftm": "ftm",
    "fta": "fta",
    "fga": "fga",
    "fgm": "fgm",
    "minutes": "min",
    "personal_fouls": "pf",
    "offensive_rebounds": "oreb",
    "defensive_rebounds": "dreb",
}

STAT_TYPE_TO_PROJECTION_KEY: dict[str, str] = {
    "points": "projected_points",
    "rebounds": "projected_rebounds",
    "assists": "projected_assists",
    "threes": "projected_threes",
    "steals": "projected_steals",
    "blocks": "projected_blocks",
    "turnovers": "projected_turnovers",
    "minutes": "projected_minutes",
}


def get_stat_std_from_logs(
    game_logs: list[dict], stat_type: str,
) -> float:
    """Compute the standard deviation of a stat from game logs.

    Args:
        game_logs: Game-log dicts with DB column names.
        stat_type: Engine stat type (e.g., ``'points'``).

    Returns:
        Standard deviation as a float; falls back to 30% of the mean
        if fewer than 3 games.
    """
    col = STAT_TYPE_TO_DB_COL.get(stat_type, stat_type)
    values: list[float] = []
    for g in game_logs:
        raw = g.get(col)
        if raw is not None:
            try:
                values.append(float(raw))
            except (ValueError, TypeError):
                pass

    if col == "min":
        values = [_parse_minutes(g.get("min")) for g in game_logs if g.get("min")]

    n = len(values)
    if n < 3:
        mean = sum(values) / max(n, 1) if values else 0.0
        return max(0.5, mean * 0.30)  # 30% CV fallback

    mean = sum(values) / n
    variance = sum((v - mean) ** 2 for v in values) / (n - 1)
    return max(0.5, math.sqrt(variance))
