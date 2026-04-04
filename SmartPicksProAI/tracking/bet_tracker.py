# ============================================================
# FILE: tracking/bet_tracker.py
# PURPOSE: High-level interface for logging and reviewing bets.
#          Uses database.py for storage, adds business logic.
# ============================================================

import datetime
import logging

_logger = logging.getLogger(__name__)

from tracking.database import (
    insert_bet,
    update_bet_result,
    load_all_bets,
    get_performance_summary,
    get_performance_by_tier,
    get_performance_by_stat,
    get_performance_by_platform,
    insert_analysis_pick,
)

# Valid constants
VALID_DIRECTIONS = {"OVER", "UNDER"}
VALID_RESULTS = {"win", "loss", "push"}
VALID_PLATFORMS = {"PrizePicks", "Underdog Fantasy", "DraftKings Pick6", "FanDuel"}

# Mapping from lowercase frontend keys to canonical platform names.
_PLATFORM_ALIAS: dict[str, str] = {
    "prizepicks": "PrizePicks",
    "underdog": "Underdog Fantasy",
    "draftkings": "DraftKings Pick6",
    "fanduel": "FanDuel",
}


def normalize_platform(raw: str) -> str:
    """Return the canonical platform name, or the original string."""
    return _PLATFORM_ALIAS.get(raw.lower().strip(), raw)


def log_new_bet(
    player_name: str,
    stat_type: str,
    prop_line: float,
    direction: str,
    *,
    bet_date: str = "",
    team: str = "",
    platform: str = "PrizePicks",
    confidence_score: float = 0.0,
    confidence_tier: str = "",
    model_probability: float = 0.0,
    edge_pct: float = 0.0,
    kelly_fraction: float = 0.0,
    recommended_bet: float = 0.0,
    opponent: str = "",
    player_id: int = 0,
    notes: str = "",
    source: str = "manual",
) -> dict:
    """Validate and log a new bet. Returns status dict."""
    if not player_name or not player_name.strip():
        return {"success": False, "error": "Player name is required."}
    if not stat_type:
        return {"success": False, "error": "Stat type is required."}
    direction = direction.upper().strip()
    if direction not in VALID_DIRECTIONS:
        return {"success": False, "error": "Direction must be OVER or UNDER."}
    try:
        prop_line = float(prop_line)
    except (TypeError, ValueError):
        return {"success": False, "error": "Prop line must be a number."}
    if prop_line <= 0:
        return {"success": False, "error": "Prop line must be positive."}

    # Normalize platform alias (e.g. "prizepicks" → "PrizePicks")
    platform = normalize_platform(platform)

    if not bet_date:
        bet_date = datetime.date.today().isoformat()

    bet_id = insert_bet(
        bet_date=bet_date,
        player_name=player_name.strip(),
        stat_type=stat_type,
        prop_line=prop_line,
        direction=direction,
        team=team,
        platform=platform,
        confidence_score=confidence_score,
        confidence_tier=confidence_tier,
        model_probability=model_probability,
        edge_pct=edge_pct,
        kelly_fraction=kelly_fraction,
        recommended_bet=recommended_bet,
        opponent=opponent,
        player_id=player_id,
        notes=notes,
        source=source,
    )
    if bet_id:
        _logger.info("Logged bet #%d: %s %s %s %s", bet_id, player_name, stat_type, direction, prop_line)
        return {"success": True, "bet_id": bet_id}
    return {"success": False, "error": "Database write failed."}


def record_bet_result(bet_id: int, result: str, actual_value: float | None = None) -> dict:
    """Record the result for a bet."""
    result_lower = result.lower().strip()
    if result_lower not in VALID_RESULTS:
        return {"success": False, "error": "Result must be win, loss, or push."}
    success = update_bet_result(bet_id, result_lower, actual_value)
    if success:
        return {"success": True}
    return {"success": False, "error": "Failed to update bet result."}


def auto_log_analysis_bets(analysis_result: dict, *, platform: str = "PrizePicks") -> dict:
    """Auto-log a bet from a /api/picks/analyze response."""
    try:
        player_name = analysis_result.get("player_name", "")
        player_id = analysis_result.get("player_id", 0)
        team = analysis_result.get("team", "")
        opponent = analysis_result.get("opponent", "")
        stat_type = analysis_result.get("stat_type", "")
        prop_line = analysis_result.get("prop_line", 0.0)
        direction = analysis_result.get("direction", "OVER")

        conf = analysis_result.get("confidence", {})
        confidence_score = conf.get("confidence_score", 0.0)
        confidence_tier = conf.get("tier", "")

        model_prob = analysis_result.get("model_probability", 0.0)
        edge = analysis_result.get("edge_pct", 0.0)

        bankroll = analysis_result.get("bankroll", {})
        kelly_frac = bankroll.get("kelly_fraction", 0.0)
        rec_bet = bankroll.get("recommended_bet_size", 0.0)

        explanation = analysis_result.get("explanation", "")

        # Insert into analysis_picks table
        insert_analysis_pick(
            analysis_date=datetime.date.today().isoformat(),
            player_name=player_name,
            stat_type=stat_type,
            prop_line=prop_line,
            direction=direction,
            player_id=player_id,
            team=team,
            opponent=opponent,
            model_probability=model_prob,
            edge_pct=edge,
            confidence_score=confidence_score,
            confidence_tier=confidence_tier,
            kelly_fraction=kelly_frac,
            recommended_bet=rec_bet,
            explanation=explanation,
        )

        # Also log as a bet
        return log_new_bet(
            player_name=player_name,
            stat_type=stat_type,
            prop_line=prop_line,
            direction=direction,
            team=team,
            platform=platform,
            confidence_score=confidence_score,
            confidence_tier=confidence_tier,
            model_probability=model_prob,
            edge_pct=edge,
            kelly_fraction=kelly_frac,
            recommended_bet=rec_bet,
            opponent=opponent,
            player_id=player_id,
            source="analysis",
        )
    except Exception as exc:
        _logger.error("auto_log_analysis_bets failed: %s", exc)
        return {"success": False, "error": str(exc)}


def get_model_performance_stats() -> dict:
    """Get comprehensive model performance stats."""
    return {
        "summary": get_performance_summary(),
        "by_tier": get_performance_by_tier(),
        "by_stat": get_performance_by_stat(),
        "by_platform": get_performance_by_platform(),
    }


# ═══════════════════════════════════════════════════════════════════════════
# Enhanced features — streak tracking, ROI, date-range filtering, export
# ═══════════════════════════════════════════════════════════════════════════


def get_current_streak() -> dict:
    """Calculate the current win/loss streak.

    Returns
    -------
    dict with ``streak_type`` ("win" | "loss" | "none"),
    ``streak_length``, and ``streak_bets`` (list of bet dicts).
    """
    bets = load_all_bets()
    if not bets:
        return {"streak_type": "none", "streak_length": 0, "streak_bets": []}

    # Sort by date descending (most recent first) then by id desc
    resolved = [
        b for b in bets
        if b.get("result") in ("win", "loss")
    ]
    resolved.sort(key=lambda b: (b.get("bet_date", ""), b.get("id", 0)), reverse=True)

    if not resolved:
        return {"streak_type": "none", "streak_length": 0, "streak_bets": []}

    streak_type = resolved[0]["result"]
    streak_bets = []
    for bet in resolved:
        if bet["result"] == streak_type:
            streak_bets.append(bet)
        else:
            break

    return {
        "streak_type": streak_type,
        "streak_length": len(streak_bets),
        "streak_bets": streak_bets,
    }


def get_longest_streaks() -> dict:
    """Find the longest win and loss streaks in bet history.

    Returns
    -------
    dict with ``longest_win`` and ``longest_loss`` (int).
    """
    bets = load_all_bets()
    resolved = [
        b for b in bets
        if b.get("result") in ("win", "loss")
    ]
    resolved.sort(key=lambda b: (b.get("bet_date", ""), b.get("id", 0)))

    longest_win = 0
    longest_loss = 0
    current_type = ""
    current_count = 0

    for bet in resolved:
        if bet["result"] == current_type:
            current_count += 1
        else:
            current_type = bet["result"]
            current_count = 1

        if current_type == "win":
            longest_win = max(longest_win, current_count)
        elif current_type == "loss":
            longest_loss = max(longest_loss, current_count)

    return {"longest_win": longest_win, "longest_loss": longest_loss}


def get_roi_stats(*, bankroll: float = 500.0) -> dict:
    """Calculate return on investment statistics.

    Parameters
    ----------
    bankroll:
        The bankroll used for bet sizing to calculate unit-based ROI.

    Returns
    -------
    dict with ``total_wagered``, ``total_returned``, ``net_profit``,
    ``roi_pct``, ``units_profit``.
    """
    bets = load_all_bets()
    resolved = [b for b in bets if b.get("result") in ("win", "loss", "push")]

    total_wagered = 0.0
    total_returned = 0.0

    for bet in resolved:
        wager = float(bet.get("recommended_bet", 0) or 0)
        if wager <= 0:
            wager = bankroll * 0.02  # Default 2% unit

        total_wagered += wager

        result = bet["result"]
        if result == "win":
            # Assume standard -110 payout (return wager + profit)
            total_returned += wager + (wager * 0.909)
        elif result == "push":
            total_returned += wager  # Return stake

    net_profit = total_returned - total_wagered
    roi_pct = (net_profit / total_wagered * 100) if total_wagered > 0 else 0.0
    unit_size = bankroll * 0.02
    units = (net_profit / unit_size) if unit_size > 0 else 0.0

    return {
        "total_wagered": round(total_wagered, 2),
        "total_returned": round(total_returned, 2),
        "net_profit": round(net_profit, 2),
        "roi_pct": round(roi_pct, 2),
        "units_profit": round(units, 2),
        "num_bets": len(resolved),
    }


def get_bets_by_date_range(
    start_date: str = "",
    end_date: str = "",
) -> list[dict]:
    """Filter bets by date range.

    Parameters
    ----------
    start_date:
        ISO date string (inclusive).  Empty = no lower bound.
    end_date:
        ISO date string (inclusive).  Empty = no upper bound.

    Returns
    -------
    list[dict]
    """
    bets = load_all_bets()
    filtered = []
    for bet in bets:
        bd = bet.get("bet_date", "")
        if start_date and bd < start_date:
            continue
        if end_date and bd > end_date:
            continue
        filtered.append(bet)
    return filtered


def get_daily_summary(days: int = 30) -> list[dict]:
    """Get a daily win/loss summary for the last N days.

    Returns
    -------
    list[dict] with ``date``, ``wins``, ``losses``, ``pushes``,
    ``total``, ``win_rate``.
    """
    cutoff = (datetime.date.today() - datetime.timedelta(days=days)).isoformat()
    bets = get_bets_by_date_range(start_date=cutoff)

    daily: dict[str, dict] = {}
    for bet in bets:
        d = bet.get("bet_date", "")
        if d not in daily:
            daily[d] = {"date": d, "wins": 0, "losses": 0, "pushes": 0, "total": 0}
        result = bet.get("result", "")
        if result == "win":
            daily[d]["wins"] += 1
        elif result == "loss":
            daily[d]["losses"] += 1
        elif result == "push":
            daily[d]["pushes"] += 1
        daily[d]["total"] += 1

    for d in daily.values():
        resolved = d["wins"] + d["losses"]
        d["win_rate"] = round(d["wins"] / resolved, 4) if resolved > 0 else 0.0

    return sorted(daily.values(), key=lambda x: x["date"], reverse=True)


def export_bets_csv(
    start_date: str = "",
    end_date: str = "",
) -> str:
    """Export bets as a CSV string for download.

    Parameters
    ----------
    start_date:
        ISO date string (inclusive).
    end_date:
        ISO date string (inclusive).

    Returns
    -------
    str
        CSV-formatted string.
    """
    bets = get_bets_by_date_range(start_date=start_date, end_date=end_date)
    if not bets:
        return ""

    columns = [
        "id", "bet_date", "player_name", "stat_type", "prop_line",
        "direction", "result", "actual_value", "team", "opponent",
        "platform", "confidence_score", "confidence_tier",
        "model_probability", "edge_pct", "kelly_fraction",
        "recommended_bet", "source", "notes",
    ]

    lines = [",".join(columns)]
    for bet in bets:
        row = []
        for col in columns:
            val = bet.get(col, "")
            # Escape commas in string values
            val_str = str(val) if val is not None else ""
            if "," in val_str:
                val_str = f'"{val_str}"'
            row.append(val_str)
        lines.append(",".join(row))

    return "\n".join(lines)


def get_performance_by_player() -> dict[str, dict]:
    """Get win/loss stats grouped by player name.

    Returns
    -------
    dict mapping player name to ``{"wins", "losses", "total", "win_rate"}``.
    """
    bets = load_all_bets()
    players: dict[str, dict] = {}

    for bet in bets:
        name = bet.get("player_name", "Unknown")
        result = bet.get("result", "")
        if result not in ("win", "loss"):
            continue

        if name not in players:
            players[name] = {"wins": 0, "losses": 0, "total": 0, "win_rate": 0.0}

        players[name]["total"] += 1
        if result == "win":
            players[name]["wins"] += 1
        else:
            players[name]["losses"] += 1

    for data in players.values():
        if data["total"] > 0:
            data["win_rate"] = round(data["wins"] / data["total"], 4)

    return players
