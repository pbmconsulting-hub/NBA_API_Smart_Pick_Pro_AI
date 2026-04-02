# ============================================================
# FILE: pages/helpers/bet_tracker_helpers.py
# PURPOSE: Helper functions for the Bet Tracker page.
#          Extracted from pages/11_📈_Bet_Tracker.py to reduce page size.
# ============================================================
import logging

_logger = logging.getLogger(__name__)


def build_tier_performance_rows(tier_perf: dict) -> list:
    """Build table rows for Win Rate by Tier section."""
    tier_order = ["Platinum", "Gold", "Silver", "Bronze"]
    return [
        {
            "Tier": t,
            "Total": tier_perf[t].get("total", 0),
            "Wins":  tier_perf[t].get("wins", 0),
            "Losses": tier_perf[t].get("losses", 0),
            "Win Rate": f"{tier_perf[t].get('win_rate', 0):.1f}%",
        }
        for t in tier_order if t in tier_perf
    ]


def build_platform_performance_rows(plat_perf: dict) -> list:
    """Build table rows for Win Rate by Platform section."""
    return [
        {
            "Platform": p,
            "Total": d.get("total", 0),
            "Wins":  d.get("wins", 0),
            "Win Rate": f"{d.get('win_rate', 0):.1f}%",
        }
        for p, d in plat_perf.items()
    ]


def build_stat_performance_rows(stat_perf: dict) -> list:
    """Build table rows for Win Rate by Stat Type section."""
    return [
        {
            "Stat Type": s.capitalize(),
            "Total": d.get("total", 0),
            "Wins":  d.get("wins", 0),
            "Win Rate": f"{d.get('win_rate', 0):.1f}%",
        }
        for s, d in sorted(stat_perf.items())
    ]


def build_bet_type_performance_rows(bet_type_perf: dict) -> list:
    """Build table rows for Win Rate by Bet Classification section."""
    return [
        {
            "Bet Type":  bt.title(),
            "Total":     d.get("total", 0),
            "Wins":      d.get("wins", 0),
            "Losses":    d.get("losses", 0),
            "Win Rate":  f"{d.get('win_rate', 0):.1f}%",
        }
        for bt, d in sorted(bet_type_perf.items())
    ]


def classify_uncertain_subtype(notes: str) -> str:
    """
    Classify an uncertain pick's risk subtype from its notes string.

    Returns one of: "Conflict", "Variance", "Fatigue", "Regression", "Other".
    """
    notes_lower = (notes or "").lower()
    if "conflict" in notes_lower:
        return "Conflict"
    if "variance" in notes_lower or "high-variance" in notes_lower:
        return "Variance"
    if "fatigue" in notes_lower or "back-to-back" in notes_lower or "blowout" in notes_lower:
        return "Fatigue"
    if "regression" in notes_lower or "hot streak" in notes_lower or "inflated" in notes_lower:
        return "Regression"
    return "Other"


def get_uncertain_subtype_counts(uncertain_bets: list) -> dict:
    """
    Count how many uncertain picks fall into each risk subtype.

    Args:
        uncertain_bets: List of bet dicts with "notes" field.

    Returns:
        Dict mapping subtype name → count (only non-zero counts included).
    """
    counts: dict = {"Conflict": 0, "Variance": 0, "Fatigue": 0, "Regression": 0, "Other": 0}
    for bet in uncertain_bets:
        subtype = classify_uncertain_subtype(bet.get("notes", ""))
        counts[subtype] = counts.get(subtype, 0) + 1
    return {k: v for k, v in counts.items() if v > 0}


def calculate_win_rate(wins: int, total: int) -> float:
    """Calculate win rate percentage, safe against division by zero."""
    if total <= 0:
        return 0.0
    return round(wins / total * 100, 1)


def format_streak(streak: int) -> str:
    """Format a win/loss streak as a human-readable string."""
    if streak > 0:
        return f"🔥 {streak}W streak"
    elif streak < 0:
        return f"❄️ {abs(streak)}L streak"
    return "➡️ No streak"
