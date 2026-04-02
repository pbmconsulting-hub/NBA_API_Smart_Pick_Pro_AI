"""
engine/line_movement_mirror.py
Compare flat-file PrizePicks lines vs hierarchy lines to detect
intra-day line movement for NBA props.
"""
import logging

_logger = logging.getLogger(__name__)


def detect_line_movement(flat_props, hierarchy_props):
    """
    Compare two PrizePicks NBA prop sources and find lines that moved.

    The flat file (nba-today.json) includes goblin/demon/standard.
    The hierarchy (current_day/props.json) only has standard lines.
    Comparing the standard lines from both sources reveals intra-day
    line movement.

    Args:
        flat_props (list[dict]): Props from prizepicks-nba-today.json.
        hierarchy_props (list[dict]): Props from hierarchy/current_day/props.json.

    Returns:
        list[dict]: Lines that moved, sorted by largest delta first.
            Each dict has: player_name, stat_type, flat_line, hierarchy_line,
            delta, direction ("UP" or "DOWN").
    """
    # Index hierarchy props by (player_name_lower, stat_type)
    hier_index = {}
    for p in hierarchy_props:
        key = (
            p.get("player_name", "").lower().strip(),
            p.get("stat_type", "").lower().strip(),
        )
        hier_line = p.get("line", 0)
        if hier_line > 0:
            hier_index[key] = hier_line

    movements = []
    for p in flat_props:
        if p.get("odds_type") != "standard":
            continue

        key = (
            p.get("player_name", "").lower().strip(),
            p.get("stat_type", "").lower().strip(),
        )
        hier_line = hier_index.get(key)
        flat_line = p.get("line", 0)

        if hier_line and abs(flat_line - hier_line) >= 0.5:
            movements.append({
                "player_name": p.get("player_name"),
                "stat_type": p.get("stat_type"),
                "flat_line": flat_line,
                "hierarchy_line": hier_line,
                "delta": round(flat_line - hier_line, 1),
                "direction": "UP" if flat_line > hier_line else "DOWN",
            })

    movements.sort(key=lambda m: abs(m["delta"]), reverse=True)
    _logger.info(f"[LineMovement] Detected {len(movements)} NBA line movements.")
    return movements
