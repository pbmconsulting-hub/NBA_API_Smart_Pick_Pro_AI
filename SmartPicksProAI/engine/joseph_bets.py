# ============================================================
# FILE: engine/joseph_bets.py
# PURPOSE: Bet tracker integration for the Joseph M. Smith
#          AI persona — auto-logging bets and retrieving
#          track record summaries.
# CONNECTS TO: tracking/bet_tracker.py, tracking/database.py,
#              engine/joseph_brain.py
# ============================================================

"""Joseph M. Smith bet tracking integration.

Provides functions for automatically logging Joseph's best bet
recommendations into the bet tracker and retrieving his historical
track record for display in the sidebar widget and studio page.
"""

import logging
from typing import Any

_logger = logging.getLogger(__name__)

try:
    from tracking.bet_tracker import (
        log_new_bet,
        normalize_platform,
    )
    from tracking.database import (
        load_all_bets,
        get_performance_summary,
        get_performance_by_tier,
    )
    _TRACKING_AVAILABLE = True
except ImportError:
    _logger.warning("[JosephBets] Could not import tracking modules")
    _TRACKING_AVAILABLE = False


# ============================================================
# SECTION: Auto-Log Bets
# ============================================================

def joseph_auto_log_bets(
    best_bets: list[dict],
    *,
    platform: str = "PrizePicks",
    source: str = "joseph_ai",
) -> list[dict]:
    """Automatically log a list of Joseph best-bet picks into the tracker.

    Each bet in *best_bets* should contain ``player_name``,
    ``stat_type``, ``prop_line``, ``direction``, and optionally
    ``confidence_tier`` and ``team``.

    Args:
        best_bets: List of best bet dictionaries from
            :func:`engine.joseph_brain.generate_best_bets`.
        platform: DFS / sportsbook platform name.
        source: Source label for tracking (default ``"joseph_ai"``).

    Returns:
        List of result dictionaries with ``player_name``,
        ``logged`` (bool), and ``message``.
    """
    results: list[dict] = []

    if not _TRACKING_AVAILABLE:
        _logger.warning("[JosephBets] Tracking not available — skipping auto-log")
        for bet in best_bets:
            results.append({
                "player_name": bet.get("player_name", "Unknown"),
                "logged": False,
                "message": "Tracking module unavailable.",
            })
        return results

    for bet in best_bets:
        try:
            player_name = bet.get("player_name", "Unknown")
            stat_type = bet.get("stat_type", "points")
            prop_line = float(bet.get("prop_line", 0))
            direction = bet.get("direction", "OVER").upper()
            tier = bet.get("confidence_tier", "Bronze")
            team = bet.get("team", "")

            result = log_new_bet(
                player_name=player_name,
                stat_type=stat_type,
                prop_line=prop_line,
                direction=direction,
                team=team,
                platform=normalize_platform(platform),
                confidence_tier=tier,
                source=source,
            )
            results.append({
                "player_name": player_name,
                "logged": result.get("success", False),
                "message": result.get("message", "Logged successfully."),
            })
        except Exception as exc:
            _logger.error("[JosephBets] Error logging bet for %s: %s",
                          bet.get("player_name", "?"), exc)
            results.append({
                "player_name": bet.get("player_name", "Unknown"),
                "logged": False,
                "message": str(exc),
            })

    return results


# ============================================================
# SECTION: Track Record
# ============================================================

def joseph_get_track_record(
    *,
    limit: int = 100,
) -> dict[str, Any]:
    """Retrieve Joseph's betting track record summary.

    Returns aggregate stats (win/loss/push counts, ROI, win rate)
    and per-tier breakdowns suitable for display in the sidebar
    widget and studio page.

    Args:
        limit: Max number of recent bets to include in detail.

    Returns:
        Dictionary with ``summary``, ``by_tier``, ``recent_bets``,
        and ``joseph_headline``.
    """
    if not _TRACKING_AVAILABLE:
        return _empty_track_record("Tracking module unavailable.")

    try:
        summary = get_performance_summary()
        by_tier = get_performance_by_tier()
        all_bets = load_all_bets()

        # Filter to joseph-sourced bets if possible
        joseph_bets = [
            b for b in all_bets
            if b.get("source") == "joseph_ai"
        ]
        using_all_sources = False
        if not joseph_bets:
            joseph_bets = all_bets
            using_all_sources = True

        recent = joseph_bets[:limit]

        # Build headline
        total = summary.get("total_bets", 0)
        wins = summary.get("wins", 0)
        losses = summary.get("losses", 0)
        win_rate = (wins / total * 100) if total > 0 else 0.0

        source_note = " (all sources)" if using_all_sources else ""

        if total == 0:
            headline = "No bets tracked yet. Let's get started."
        elif win_rate >= 60:
            headline = (
                f"🔥 Joseph is ROLLING — {wins}W-{losses}L "
                f"({win_rate:.0f}% win rate) across {total} tracked bets{source_note}."
            )
        elif win_rate >= 50:
            headline = (
                f"📊 Solid track record — {wins}W-{losses}L "
                f"({win_rate:.0f}%) across {total} bets{source_note}. Grinding profit."
            )
        else:
            headline = (
                f"⚠️ {wins}W-{losses}L ({win_rate:.0f}%) across {total} bets{source_note}. "
                f"Variance happens — trust the process."
            )

        return {
            "summary": summary,
            "by_tier": by_tier,
            "recent_bets": recent,
            "joseph_headline": headline,
        }
    except Exception as exc:
        _logger.error("[JosephBets] joseph_get_track_record error: %s", exc)
        return _empty_track_record(f"Error loading track record: {exc}")


def _empty_track_record(message: str) -> dict[str, Any]:
    return {
        "summary": {},
        "by_tier": {},
        "recent_bets": [],
        "joseph_headline": message,
    }
