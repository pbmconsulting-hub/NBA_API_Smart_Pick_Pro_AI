"""tracking/auto_resolver.py — Auto-resolve pending picks using NBA box scores.

At app startup, queries yesterday's box scores and matches them against
pending picks to automatically record hit/miss results.
"""

import datetime
import logging

_logger = logging.getLogger(__name__)

try:
    from tracking.database import _get_connection
except ImportError:
    _get_connection = None


def _load_pending_picks() -> list[dict]:
    """Return all picks without a result (pending)."""
    if _get_connection is None:
        return []
    try:
        conn = _get_connection()
        cursor = conn.cursor()
        cursor.execute(
            "SELECT bet_id, player_name, stat_type, prop_line, direction, bet_date "
            "FROM bets WHERE result IS NULL OR result = ''"
        )
        rows = cursor.fetchall()
        conn.close()
        return [dict(r) for r in rows]
    except Exception as exc:
        _logger.warning("Failed to load pending picks: %s", exc)
        return []


def _update_pick_result(pick_id: int, result: str, actual_value: float | None = None) -> None:
    """Mark a pick as hit/miss/push with the actual stat value."""
    if _get_connection is None:
        return
    try:
        conn = _get_connection()
        cursor = conn.cursor()
        cursor.execute(
            "UPDATE bets SET result = ?, actual_value = ? WHERE bet_id = ?",
            (result, actual_value, pick_id),
        )
        conn.commit()
        conn.close()
    except Exception as exc:
        _logger.warning("Failed to update pick %d: %s", pick_id, exc)


def _get_box_scores_for_date(date_str: str) -> list[dict]:
    """Fetch box scores for a given date from the NBA API.

    Returns a list of player stat dicts with at least:
    ``player_name``, ``pts``, ``reb``, ``ast``, ``stl``, ``blk``,
    ``tov``, ``fg3m``, ``ftm``.
    """
    try:
        from nba_api.stats.endpoints import ScoreboardV2  # noqa: F811
    except ImportError:
        _logger.info("nba_api not available for auto-resolve.")
        return []

    try:
        sb = ScoreboardV2(game_date=date_str)
        game_headers = sb.get_normalized_dict().get("GameHeader", [])
        if not game_headers:
            return []

        from nba_api.stats.endpoints import BoxScoreTraditionalV2
        all_players = []
        for game in game_headers:
            gid = game.get("GAME_ID")
            if not gid:
                continue
            try:
                box = BoxScoreTraditionalV2(game_id=gid)
                player_stats = box.get_normalized_dict().get("PlayerStats", [])
                for ps in player_stats:
                    all_players.append({
                        "player_name": ps.get("PLAYER_NAME", ""),
                        "pts": float(ps.get("PTS", 0) or 0),
                        "reb": float(ps.get("REB", 0) or 0),
                        "ast": float(ps.get("AST", 0) or 0),
                        "stl": float(ps.get("STL", 0) or 0),
                        "blk": float(ps.get("BLK", 0) or 0),
                        "tov": float(ps.get("TO", 0) or 0),
                        "fg3m": float(ps.get("FG3M", 0) or 0),
                        "ftm": float(ps.get("FTM", 0) or 0),
                    })
            except Exception as box_exc:
                _logger.debug("Box score fetch failed for game %s: %s", gid, box_exc)
        return all_players
    except Exception as exc:
        _logger.warning("Failed to fetch box scores for %s: %s", date_str, exc)
        return []


# Stat type → box score key mapping
_STAT_KEY_MAP = {
    "points": "pts",
    "rebounds": "reb",
    "assists": "ast",
    "steals": "stl",
    "blocks": "blk",
    "turnovers": "tov",
    "threes": "fg3m",
    "fg3m": "fg3m",
    "ftm": "ftm",
}


def _find_player_stat(box_scores: list[dict], player_name: str, stat_type: str) -> float | None:
    """Find a player's stat value in the box score data."""
    stat_key = _STAT_KEY_MAP.get(stat_type.lower())
    if not stat_key:
        return None

    # Normalise player name for fuzzy matching
    target = player_name.lower().strip()
    for ps in box_scores:
        box_name = ps.get("player_name", "").lower().strip()
        if box_name == target or target in box_name or box_name in target:
            return ps.get(stat_key)
    return None


def auto_resolve_pending_picks() -> dict:
    """Query yesterday's box scores, match to pending picks, auto-record results.

    Returns:
        dict with ``resolved`` count and ``errors`` count.
    """
    resolved = 0
    errors = 0

    pending = _load_pending_picks()
    if not pending:
        return {"resolved": 0, "errors": 0, "message": "No pending picks."}

    yesterday = (datetime.date.today() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
    box_scores = _get_box_scores_for_date(yesterday)
    if not box_scores:
        return {"resolved": 0, "errors": 0, "message": "No box scores available for yesterday."}

    for pick in pending:
        try:
            # Only resolve picks from yesterday or earlier
            pick_date = pick.get("bet_date", "")
            if pick_date and pick_date > yesterday:
                continue  # Game hasn't happened yet

            actual = _find_player_stat(
                box_scores,
                pick.get("player_name", ""),
                pick.get("stat_type", ""),
            )
            if actual is None:
                continue  # Player didn't play or name mismatch

            prop_line = float(pick.get("prop_line", 0))
            direction = pick.get("direction", "OVER").upper()

            if actual == prop_line:
                result = "push"
            elif direction == "OVER":
                result = "hit" if actual > prop_line else "miss"
            else:
                result = "hit" if actual < prop_line else "miss"

            _update_pick_result(pick["bet_id"], result, actual_value=actual)
            resolved += 1
        except Exception as exc:
            _logger.warning("Error resolving pick %s: %s", pick.get("bet_id"), exc)
            errors += 1

    _logger.info("Auto-resolved %d picks (%d errors)", resolved, errors)
    return {"resolved": resolved, "errors": errors}
