# ============================================================
# FILE: tracking/bet_tracker.py
# PURPOSE: High-level interface for logging and reviewing bets.
#          Uses database.py for storage, adds business logic.
# ============================================================

import datetime
import logging

_logger = logging.getLogger(__name__)

from tracking.database import (
    initialize_database,
    insert_bet,
    update_bet_result,
    load_all_bets,
    get_performance_summary,
    get_performance_by_tier,
    get_performance_by_stat,
    get_performance_by_platform,
    insert_analysis_pick,
    load_analysis_picks,
    delete_bet,
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
        return {"success": False, "error": f"Direction must be OVER or UNDER."}
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
        return {"success": False, "error": f"Result must be win, loss, or push."}
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
