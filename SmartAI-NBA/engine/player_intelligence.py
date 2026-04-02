# ============================================================
# FILE: engine/player_intelligence.py
# PURPOSE: Aggregate player data into a comprehensive intelligence
#          summary — hit rates, hot/cold streaks, matchup grades,
#          injury context, line value assessment.
# CONNECTS TO: Neural Analysis page, Prop Scanner, Bet Tracker
# ============================================================

from __future__ import annotations

import logging
import math as _math
from typing import Any

logger = logging.getLogger(__name__)


def _safe_float(value, fallback=0.0):
    """Return *value* as a finite float, or *fallback* if NaN/inf/non-numeric."""
    try:
        v = float(value)
        if _math.isfinite(v):
            return v
        return float(fallback)
    except (ValueError, TypeError):
        return float(fallback)


# ─── Streak & Form Constants ──────────────────────────────────
_STREAK_WINDOW = 5        # games used to define "recent form"
_HOT_HIT_RATE = 0.70      # 70%+ of last N games over = hot
_COLD_HIT_RATE = 0.30     # 30% or fewer = cold
_LONG_STREAK_MIN = 3      # minimum consecutive hits/misses for a meaningful streak

# ─── Matchup Grade Thresholds ────────────────────────────────
# Percentile of opponent defensive rating that sets grade boundaries.
# Lower defensive rating (better defence) → harder matchup.
_GRADE_EASY_THRESHOLD = 0.75   # bottom 25% defensive rating → A (easy)
_GRADE_GOOD_THRESHOLD = 0.50   # 25-50th → B
_GRADE_AVERAGE_THRESHOLD = 0.25  # 50-75th → C
# below 25th percentile → D (toughest defence)

# ─── Availability Status Sets (module-level for reuse) ────────
_OUT_STATUSES = frozenset({
    "out", "injured reserve", "out (no recent games)", "suspended",
    "not with team", "g league - two-way", "g league - on assignment", "g league",
})
_FLAG_STATUSES = frozenset({
    "gtd", "game time decision", "day-to-day", "questionable", "doubtful",
})

# ─── Combo Stat Column Mapping (module-level for reuse) ───────
# Maps stat_type → list of nba_api game-log column names to sum.
_COMBO_STAT_COLUMNS: dict[str, list[str]] = {
    "points_rebounds":            ["PTS", "REB"],
    "points_assists":             ["PTS", "AST"],
    "rebounds_assists":           ["REB", "AST"],
    "points_rebounds_assists":    ["PTS", "REB", "AST"],
    "blocks_steals":              ["BLK", "STL"],
}

# Maps stat_type → list of season-avg field prefixes in player_data.
_COMBO_STAT_COMPONENTS: dict[str, list[str]] = {
    "points_rebounds":            ["points", "rebounds"],
    "points_assists":             ["points", "assists"],
    "rebounds_assists":           ["rebounds", "assists"],
    "points_rebounds_assists":    ["points", "rebounds", "assists"],
    "blocks_steals":              ["blocks", "steals"],
}

# Stat-to-CSV-column mapping for game logs
_STAT_LOG_MAP: dict[str, str] = {
    "points": "PTS",
    "rebounds": "REB",
    "assists": "AST",
    "threes": "FG3M",
    "steals": "STL",
    "blocks": "BLK",
    "turnovers": "TOV",
}


# ============================================================
# SECTION: Recent Form — Hit Rate vs Line
# ============================================================

def get_recent_form_vs_line(
    game_logs: list[dict],
    stat_type: str,
    prop_line: float,
    window: int = _STREAK_WINDOW,
    player_id: int | None = None,
    season: str | None = None,
) -> dict[str, Any]:
    """Return hit-rate and per-game results for *stat_type* vs *prop_line*.

    Each game log entry is expected to have nba_api field names (e.g. "PTS",
    "REB", "AST", "FG3M", "STL", "BLK", "TOV").  Combo stats (e.g.
    "points_rebounds") are summed from their component columns.

    If *game_logs* is empty and *player_id* is provided, this function will
    attempt to fetch logs automatically via
    ``nba_stats_service.get_player_game_logs()``.  This is transparent to the
    caller — if the fetch fails, the function returns the normal empty result.

    Parameters
    ----------
    game_logs : list[dict]
        Pre-fetched game logs.  Pass an empty list (or None-equivalent) to
        trigger automatic fetching when *player_id* is supplied.
    stat_type : str
        Stat type to analyse (e.g. ``"points"``, ``"rebounds"``).
    prop_line : float
        The betting line to compare each game against.
    window : int
        Number of most-recent games to analyse.
    player_id : int | None
        NBA player ID.  When provided and *game_logs* is empty, logs are
        fetched from ``nba_stats_service`` automatically.
    season : str | None
        Season string passed to ``get_player_game_logs()`` when auto-fetching.

    Returns::

        {
            "window": int,              # games analysed
            "hits": int,                # games >= prop_line
            "misses": int,
            "hit_rate": float,          # hits / window  (0.0–1.0)
            "avg_vs_line": float,       # mean stat value over window
            "avg_margin": float,        # mean (value - prop_line)
            "results": list[dict],      # per-game [{date, value, hit, margin}]
            "form_label": str,          # "Hot 🔥", "Cold 🧊", or "Neutral ➡️"
            "streak": dict,             # see _compute_streak()
            "sufficient_data": bool,    # False when window < 3 usable games
        }
    """
    # Auto-fetch logs via nba_stats_service when none are provided
    if not game_logs and player_id is not None:
        game_logs = get_player_game_logs_from_service(player_id, season=season)

    if not game_logs or prop_line <= 0:
        return _empty_form_result(window, prop_line)

    col_names = _resolve_stat_columns(stat_type)
    if not col_names:
        return _empty_form_result(window, prop_line)

    results: list[dict] = []
    for g in game_logs[:window]:
        try:
            value = sum(float(g.get(col, 0) or 0) for col in col_names)
        except (TypeError, ValueError):
            continue
        hit = value >= prop_line
        results.append(
            {
                "date": g.get("GAME_DATE", ""),
                "matchup": g.get("MATCHUP", ""),
                "value": round(value, 1),
                "hit": hit,
                "margin": round(value - prop_line, 1),
            }
        )

    if not results:
        return _empty_form_result(window, prop_line)

    hits = sum(1 for r in results if r["hit"])
    misses = len(results) - hits
    hit_rate = hits / len(results)
    avg_val = sum(r["value"] for r in results) / len(results)
    avg_margin = sum(r["margin"] for r in results) / len(results)

    if hit_rate >= _HOT_HIT_RATE:
        form_label = "Hot 🔥"
    elif hit_rate <= _COLD_HIT_RATE:
        form_label = "Cold 🧊"
    else:
        form_label = "Neutral ➡️"

    return {
        "window": int(window),
        "hits": int(hits),
        "misses": int(misses),
        "hit_rate": _safe_float(hit_rate),
        "avg_vs_line": _safe_float(round(avg_val, 1)),
        "avg_margin": _safe_float(round(avg_margin, 1)),
        "results": results,
        "form_label": form_label,
        "streak": _compute_streak(results),
        "sufficient_data": len(results) >= 3,
    }


def _empty_form_result(window: int, prop_line: float) -> dict[str, Any]:
    return {
        "window": window,
        "hits": 0,
        "misses": 0,
        "hit_rate": 0.0,
        "avg_vs_line": 0.0,
        "avg_margin": 0.0,
        "results": [],
        "form_label": "No Data",
        "streak": {"active": False, "type": "", "count": 0},
        "sufficient_data": False,
    }


def _compute_streak(results: list[dict]) -> dict[str, Any]:
    """Return the current consecutive hit or miss streak from most-recent game."""
    if not results:
        return {"active": False, "type": "", "count": 0}

    streak_type = "hit" if results[0]["hit"] else "miss"
    count = 0
    for r in results:
        if (streak_type == "hit") == r["hit"]:
            count += 1
        else:
            break

    return {
        "active": count >= _LONG_STREAK_MIN,
        "type": streak_type,
        "count": count,
        "label": f"{count}-game {'Over' if streak_type == 'hit' else 'Under'} streak",
    }


# ============================================================
# SECTION: Matchup Grade
# ============================================================

def grade_matchup(
    stat_type: str,
    opponent_def_rating: float | None,
    all_def_ratings: list[float],
) -> dict[str, Any]:
    """Return a letter-grade (A–D) for the matchup difficulty.

    *opponent_def_rating* is the opponent's points-allowed per game for
    *stat_type* (lower = better defence).  *all_def_ratings* is the league
    list used to compute percentile rank.

    Returns::

        {
            "grade": str,           # "A", "B", "C", "D"
            "label": str,           # "Elite Matchup", "Good", "Average", "Tough"
            "percentile": float,    # 0.0–1.0 (rank within league)
            "color_class": str,     # CSS class: "grade-a" / "grade-b" / ...
        }
    """
    if opponent_def_rating is None or not all_def_ratings:
        return {"grade": "N/A", "label": "No Data", "percentile": _safe_float(0.5, 0.5), "color_class": "grade-na"}

    sorted_ratings = sorted(all_def_ratings)
    n = len(sorted_ratings)
    # percentile = fraction of teams with LOWER (better) def rating than opponent
    # Higher opponent def rating = allows more points = easier matchup = higher percentile
    rank = sum(1 for r in sorted_ratings if r < opponent_def_rating)
    percentile = rank / max(n - 1, 1)

    if percentile >= _GRADE_EASY_THRESHOLD:
        return {"grade": "A", "label": "Elite Matchup 🎯", "percentile": _safe_float(percentile, 0.5), "color_class": "grade-a"}
    elif percentile >= _GRADE_GOOD_THRESHOLD:
        return {"grade": "B", "label": "Good Matchup ✅", "percentile": _safe_float(percentile, 0.5), "color_class": "grade-b"}
    elif percentile >= _GRADE_AVERAGE_THRESHOLD:
        return {"grade": "C", "label": "Average Matchup ➡️", "percentile": _safe_float(percentile, 0.5), "color_class": "grade-c"}
    else:
        return {"grade": "D", "label": "Tough Matchup ⚠️", "percentile": _safe_float(percentile, 0.5), "color_class": "grade-d"}


# ============================================================
# SECTION: Injury / Availability Context
# ============================================================

def get_availability_context(
    player_name: str,
    injury_status_map: dict,
) -> dict[str, Any]:
    """Return availability badge data for *player_name*.

    *injury_status_map* is the dict loaded from data/injury_status.json —
    keys are lowercase player names.

    Returns::

        {
            "status": str,          # "Active", "GTD", "Questionable", "Out", etc.
            "badge_class": str,     # CSS class for colour coding
            "badge_label": str,     # Display text  e.g., "🟢 Active"
            "injury_note": str,     # e.g., "Knee – soreness"
            "games_missed": int,
            "return_date": str,
            "is_active": bool,      # False only if Out / IR / suspended
            "is_flagged": bool,     # True for GTD / Day-to-Day / Questionable
        }
    """
    lookup_key = player_name.strip().lower()
    info = injury_status_map.get(lookup_key, {})

    raw_status = (info.get("status") or "Active").strip()
    raw_lower = raw_status.lower()

    if raw_lower in _OUT_STATUSES:
        badge_class = "avail-out"
        badge_label = f"🔴 {raw_status}"
        is_active = False
        is_flagged = False
    elif raw_lower in _FLAG_STATUSES:
        badge_class = "avail-gtd"
        badge_label = f"🟡 {raw_status}"
        is_active = True
        is_flagged = True
    elif raw_lower == "doubtful":
        badge_class = "avail-doubtful"
        badge_label = f"🟠 Doubtful"
        is_active = True
        is_flagged = True
    else:
        badge_class = "avail-active"
        badge_label = "🟢 Active"
        is_active = True
        is_flagged = False

    return {
        "status": raw_status,
        "badge_class": badge_class,
        "badge_label": badge_label,
        "injury_note": info.get("injury_note") or info.get("injury") or "",
        "games_missed": int(_safe_float(info.get("games_missed"), 0)),
        "return_date": info.get("return_date") or "",
        "is_active": is_active,
        "is_flagged": is_flagged,
    }


# ============================================================
# SECTION: Season-vs-Line Value Assessment
# ============================================================

def assess_line_value(
    season_avg: float,
    prop_line: float,
    stat_type: str = "points",
) -> dict[str, Any]:
    """Compare the season average to the prop line and return a value assessment.

    Returns::

        {
            "edge_pct": float,          # (season_avg - prop_line) / prop_line * 100
            "direction": str,           # "OVER" or "UNDER"
            "value_label": str,         # "Great Value", "Slight Value", "Fair", "Slight Under", "Bad Value"
            "value_class": str,         # CSS class
            "season_avg": float,
            "prop_line": float,
            "diff": float,              # season_avg - prop_line
        }
    """
    if prop_line <= 0 or season_avg <= 0:
        return {
            "edge_pct": 0.0,
            "direction": "—",
            "value_label": "No Data",
            "value_class": "val-neutral",
            "season_avg": _safe_float(season_avg),
            "prop_line": _safe_float(prop_line),
            "diff": 0.0,
        }

    diff = season_avg - prop_line
    edge_pct = diff / prop_line * 100.0

    if diff > 0:
        direction = "OVER"
        if edge_pct >= 8:
            label, cls = "Great Value 🎯", "val-great"
        elif edge_pct >= 4:
            label, cls = "Slight Over Edge ✅", "val-good"
        else:
            label, cls = "Fair / Marginal", "val-neutral"
    else:
        direction = "UNDER"
        if edge_pct <= -8:
            label, cls = "Strong Under ⬇️", "val-great"
        elif edge_pct <= -4:
            label, cls = "Slight Under Edge", "val-good"
        else:
            label, cls = "Fair / Marginal", "val-neutral"

    return {
        "edge_pct": _safe_float(round(edge_pct, 1)),
        "direction": direction,
        "value_label": label,
        "value_class": cls,
        "season_avg": _safe_float(round(season_avg, 1)),
        "prop_line": _safe_float(round(prop_line, 1)),
        "diff": _safe_float(round(diff, 1)),
    }


# ============================================================
# SECTION: Comprehensive Player Intelligence Summary
# ============================================================

def get_player_intelligence_summary(
    player_name: str,
    stat_type: str,
    prop_line: float,
    player_data: dict,
    game_logs: list[dict],
    injury_status_map: dict,
    opponent_def_rating: float | None = None,
    all_def_ratings: list[float] | None = None,
) -> dict[str, Any]:
    """Build a single comprehensive intelligence dict for one player+prop.

    This is the primary entry-point called by the Neural Analysis page and
    Prop Scanner to enrich each analysis card with actionable player context.

    Returns::

        {
            "player_name": str,
            "stat_type": str,
            "prop_line": float,
            "availability": dict,           # from get_availability_context()
            "form": dict,                   # from get_recent_form_vs_line()
            "line_value": dict,             # from assess_line_value()
            "matchup_grade": dict,          # from grade_matchup()
            "season_avg": float,
            "alert_flags": list[str],       # high-priority warning messages
            "highlight_flags": list[str],   # positive highlights
        }
    """
    # Season avg for this stat
    avg_key = f"{stat_type}_avg" if "_" not in stat_type else None
    season_avg = 0.0
    if avg_key and player_data:
        try:
            season_avg = float(player_data.get(avg_key, 0) or 0)
        except (TypeError, ValueError):
            season_avg = 0.0
    # For combo stats, sum the components
    if season_avg == 0.0 and "_" in stat_type:
        season_avg = _sum_combo_avg(stat_type, player_data)

    availability = get_availability_context(player_name, injury_status_map)
    form = get_recent_form_vs_line(game_logs, stat_type, prop_line)
    line_value = assess_line_value(season_avg, prop_line, stat_type)
    matchup_grade = grade_matchup(
        stat_type,
        opponent_def_rating,
        all_def_ratings or [],
    )

    alert_flags: list[str] = []
    highlight_flags: list[str] = []

    # Availability alerts
    if not availability["is_active"]:
        alert_flags.append(f"⛔ {availability['badge_label']} — do not bet")
    elif availability["is_flagged"]:
        alert_flags.append(f"⚠️ {availability['badge_label']}: {availability['injury_note']}")

    # Form alerts/highlights
    if form["sufficient_data"]:
        if form["form_label"] == "Hot 🔥":
            highlight_flags.append(
                f"🔥 Hot — {form['hits']}/{form['window']} over in last {form['window']} games"
            )
        elif form["form_label"] == "Cold 🧊":
            alert_flags.append(
                f"🧊 Cold — only {form['hits']}/{form['window']} over in last {form['window']} games"
            )
        streak = form.get("streak", {})
        if streak.get("active") and streak.get("count", 0) >= _LONG_STREAK_MIN:
            if streak["type"] == "hit":
                highlight_flags.append(f"🔥 {streak['label']}")
            else:
                alert_flags.append(f"❄️ {streak['label']}")

    # Line value highlights
    if line_value["value_class"] == "val-great":
        highlight_flags.append(f"💎 {line_value['value_label']} ({line_value['edge_pct']:+.1f}%)")

    # Matchup highlights
    if matchup_grade["grade"] in ("A", "B"):
        highlight_flags.append(f"✅ {matchup_grade['label']}")
    elif matchup_grade["grade"] == "D":
        alert_flags.append(f"⚠️ {matchup_grade['label']}")

    return {
        "player_name": player_name,
        "stat_type": stat_type,
        "prop_line": prop_line,
        "availability": availability,
        "form": form,
        "line_value": line_value,
        "matchup_grade": matchup_grade,
        "season_avg": season_avg,
        "alert_flags": alert_flags,
        "highlight_flags": highlight_flags,
    }


# ============================================================
# SECTION: Streak aggregation across multiple props
# ============================================================

def aggregate_streak_summary(
    all_intel: list[dict],
) -> dict[str, Any]:
    """Summarise hot/cold streaks across a list of intelligence dicts.

    Used by the Neural Analysis status panel to show an at-a-glance streak
    report (e.g. "3 players on hot streaks, 1 cold").

    Returns::

        {
            "hot_count": int,
            "cold_count": int,
            "neutral_count": int,
            "hot_players": list[str],
            "cold_players": list[str],
        }
    """
    hot, cold, neutral = [], [], []
    for intel in all_intel:
        form = intel.get("form", {})
        label = form.get("form_label", "")
        name = intel.get("player_name", "")
        if "Hot" in label:
            hot.append(name)
        elif "Cold" in label:
            cold.append(name)
        else:
            neutral.append(name)
    return {
        "hot_count": len(hot),
        "cold_count": len(cold),
        "neutral_count": len(neutral),
        "hot_players": hot,
        "cold_players": cold,
    }


# ============================================================
# SECTION: Hit-rate table for Prop Scanner Quick Analysis
# ============================================================

def build_quick_analysis_rows(
    props: list[dict],
    players_data: list[dict],
    game_logs_cache: dict[str, list[dict]],
    injury_status_map: dict,
) -> list[dict]:
    """Build a list of quick-analysis rows for the Prop Scanner page.

    Each row contains the original prop fields PLUS intelligence data so the
    Prop Scanner can render edge indicators, injury badges, and form dots
    without calling the full simulation engine.

    *game_logs_cache* maps lowercase player names to their recent game logs.

    Returns a list of dicts with keys:
    ``player_name``, ``team``, ``stat_type``, ``line``, ``platform``,
    ``season_avg``, ``edge_pct``, ``direction``, ``value_label``,
    ``form_results`` (list), ``hit_rate``, ``form_label``,
    ``availability_badge``, ``injury_note``, ``streak_label``.
    """
    from data.data_manager import find_player_by_name  # local import to avoid circular

    rows: list[dict] = []
    for prop in props:
        player_name: str = prop.get("player_name", "")
        stat_type: str = prop.get("stat_type", "")
        try:
            prop_line = float(prop.get("line", 0) or 0)
        except (TypeError, ValueError):
            prop_line = 0.0

        # Look up player stats
        player_data = find_player_by_name(player_name, players_data) or {}

        # Season avg
        avg_key = f"{stat_type}_avg"
        try:
            season_avg = float(player_data.get(avg_key, 0) or 0)
        except (TypeError, ValueError):
            season_avg = 0.0
        if season_avg == 0.0 and "_" in stat_type:
            season_avg = _sum_combo_avg(stat_type, player_data)

        # Game logs
        game_logs = game_logs_cache.get(player_name.strip().lower(), [])

        # Intelligence
        avail = get_availability_context(player_name, injury_status_map)
        form = get_recent_form_vs_line(game_logs, stat_type, prop_line)
        line_val = assess_line_value(season_avg, prop_line, stat_type)

        streak_info = form.get("streak", {})
        streak_label = streak_info.get("label", "") if streak_info.get("active") else ""

        rows.append(
            {
                **prop,
                "season_avg": season_avg,
                "edge_pct": line_val["edge_pct"],
                "direction": line_val["direction"],
                "value_label": line_val["value_label"],
                "value_class": line_val["value_class"],
                "form_results": form.get("results", []),
                "hit_rate": form.get("hit_rate", 0.0),
                "form_label": form.get("form_label", "No Data"),
                "avg_margin": form.get("avg_margin", 0.0),
                "availability_badge": avail["badge_label"],
                "availability_class": avail["badge_class"],
                "injury_note": avail["injury_note"],
                "is_flagged": avail["is_flagged"],
                "streak_label": streak_label,
            }
        )
    return rows


# ============================================================
# SECTION: Internal Helpers
# ============================================================

def _resolve_stat_columns(stat_type: str) -> list[str]:
    """Return the game-log column name(s) for a given stat type."""
    # Simple stats
    if stat_type in _STAT_LOG_MAP:
        return [_STAT_LOG_MAP[stat_type]]

    # Combo stats — use module-level mapping
    if stat_type in _COMBO_STAT_COLUMNS:
        return _COMBO_STAT_COLUMNS[stat_type]

    # Fantasy score — approximate via PTS + REB + AST + STL + BLK (simplified)
    if stat_type.startswith("fantasy_score"):
        return ["PTS", "REB", "AST", "STL", "BLK"]

    return []


def _sum_combo_avg(stat_type: str, player_data: dict) -> float:
    """Sum season averages for a combo stat using module-level component map."""
    components = _COMBO_STAT_COMPONENTS.get(stat_type, [])
    total = 0.0
    for comp in components:
        try:
            total += float(player_data.get(f"{comp}_avg", 0) or 0)
        except (TypeError, ValueError):
            pass
    return round(total, 2)


# ============================================================
# SECTION: nba_stats_service integrations
# ============================================================

def get_player_game_logs_from_service(
    player_id: int,
    season: str | None = None,
) -> list[dict]:
    """
    Fetch game logs via nba_stats_service.get_player_game_logs().

    Intended as a convenient fallback when the caller does not have
    pre-fetched game logs.  Returns [] if the service is unavailable
    or the call fails.

    Parameters
    ----------
    player_id : int
        NBA player ID.
    season : str | None
        Season in "YYYY-YY" format; defaults to current season.

    Returns
    -------
    list[dict]
        Per-game stat dicts with nba_api column names (PTS, REB, …).
    """
    try:
        from data.nba_stats_service import get_player_game_logs
        return get_player_game_logs(player_id, season=season)
    except Exception as exc:
        logger.warning("get_player_game_logs_from_service(%s) failed: %s", player_id, exc)
        return []


def get_player_matchup_grade(
    player_id: int,
    opponent_team_abbrev: str,
    stat_type: str,
    season: str | None = None,
) -> dict[str, Any]:
    """
    Grade a player's upcoming matchup using nba_stats_service defensive data.

    Fetches league-wide defensive-matchup data and grades the opponent's
    defence for *stat_type* on an A–F scale.

    Parameters
    ----------
    player_id : int
        NBA player ID (used for future per-player extension; not used now).
    opponent_team_abbrev : str
        Three-letter abbreviation of the opponent team (e.g. ``"BOS"``).
    stat_type : str
        Prop stat type (e.g. ``"points"``, ``"rebounds"``, ``"assists"``).
    season : str | None
        Season string; defaults to current season.

    Returns
    -------
    dict
        Keys: grade (A/B/C/D/F), label, percentile, color_class,
        source (str).
        Returns a default N/A grade dict if data is unavailable.
    """
    _default = {
        "grade": "N/A",
        "label": "No Data",
        "percentile": 0.5,
        "color_class": "grade-na",
        "source": "nba_stats_service",
    }

    try:
        from data.nba_stats_service import get_defensive_matchup_data
        rows = get_defensive_matchup_data(season=season)
    except Exception as exc:
        logger.warning("get_player_matchup_grade: service call failed: %s", exc)
        return _default

    if not rows:
        return _default

    # Map stat_type → DEFENSE_CATEGORY field values used by LeagueDashPtDefend
    _stat_to_defense_cat: dict[str, str] = {
        "points": "Overall",
        "threes": "3 Pointers",
        "midrange": "Mid-Range",
        "at_rim": "Less Than 6Ft",
        "rebounds": "Overall",
        "assists": "Overall",
    }
    defense_cat = _stat_to_defense_cat.get(stat_type.lower(), "Overall")

    # Filter rows to the relevant defense category
    cat_rows = [r for r in rows if str(r.get("DEFENSE_CATEGORY", "")).strip() == defense_cat]
    if not cat_rows:
        cat_rows = rows  # fall back to all rows if no category match

    # Build opponent rating (points allowed per game) for each team
    team_ratings: dict[str, float] = {}
    for r in cat_rows:
        abbrev = str(r.get("TEAM_ABBREVIATION", "")).upper()
        pts_allowed = _safe_float(r.get("D_PTS", 0), 0.0)
        if abbrev:
            team_ratings[abbrev] = pts_allowed

    all_ratings = list(team_ratings.values())
    opponent_rating = team_ratings.get(opponent_team_abbrev.upper())

    if opponent_rating is None or not all_ratings:
        return _default

    # Reuse grade_matchup to compute the grade
    grade_result = grade_matchup(stat_type, opponent_rating, all_ratings)
    grade_result["source"] = "nba_stats_service"
    return grade_result


def get_player_home_away_splits(
    player_id: int,
    season: str | None = None,
) -> dict[str, Any]:
    """
    Return pre-computed home/away and last-5/last-10 averages from nba_api.

    Wraps nba_stats_service.get_player_splits() and normalises the result
    into a flat dict of averages suitable for direct use in analysis cards.

    Parameters
    ----------
    player_id : int
        NBA player ID.
    season : str | None
        Season string; defaults to current season.

    Returns
    -------
    dict
        Keys: home_pts, home_reb, home_ast, away_pts, away_reb, away_ast,
        last5_pts, last5_reb, last5_ast, last10_pts, last10_reb, last10_ast.
        Returns all-zero dict if data is unavailable.
    """
    _empty: dict[str, Any] = {
        "home_pts": 0.0, "home_reb": 0.0, "home_ast": 0.0,
        "away_pts": 0.0, "away_reb": 0.0, "away_ast": 0.0,
        "last5_pts": 0.0, "last5_reb": 0.0, "last5_ast": 0.0,
        "last10_pts": 0.0, "last10_reb": 0.0, "last10_ast": 0.0,
    }

    try:
        from data.nba_stats_service import get_player_splits
        splits = get_player_splits(player_id, season=season)
    except Exception as exc:
        logger.warning("get_player_home_away_splits(%s) failed: %s", player_id, exc)
        return _empty

    if not splits:
        return _empty

    def _avg(rows: list[dict], key: str) -> float:
        if not rows:
            return 0.0
        return _safe_float(rows[0].get(key, 0), 0.0)

    home_rows = splits.get("home", [])
    away_rows = splits.get("away", [])
    last5_rows = splits.get("last_5_games", [])
    last10_rows = splits.get("last_10_games", [])

    return {
        "home_pts": _avg(home_rows, "PTS"),
        "home_reb": _avg(home_rows, "REB"),
        "home_ast": _avg(home_rows, "AST"),
        "away_pts": _avg(away_rows, "PTS"),
        "away_reb": _avg(away_rows, "REB"),
        "away_ast": _avg(away_rows, "AST"),
        "last5_pts": _avg(last5_rows, "PTS"),
        "last5_reb": _avg(last5_rows, "REB"),
        "last5_ast": _avg(last5_rows, "AST"),
        "last10_pts": _avg(last10_rows, "PTS"),
        "last10_reb": _avg(last10_rows, "REB"),
        "last10_ast": _avg(last10_rows, "AST"),
    }
