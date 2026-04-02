# ============================================================
# FILE: engine/market_movement.py
# PURPOSE: Market Movement Detection Engine
#          Tracks how prop lines change over time and identifies
#          sharp money signals. When prop lines move toward our
#          recommended direction, it confirms our model. When
#          lines move against our recommendation, it's a warning.
#
#          Line movement is one of the strongest real-time signals
#          available — it reflects where "sharp money" (professional
#          bettors) is going.
#
# STORAGE: JSON file (same pattern as clv_tracker.py)
# CONNECTS TO: engine/confidence.py (post-scoring adjustment),
#              engine/clv_tracker.py (line snapshot pattern)
# CONCEPTS COVERED: Line movement, sharp money, steam moves,
#                   market efficiency, confidence adjustment
# ============================================================

import json
import datetime
import math
import os
from pathlib import Path


def _safe_float(value, fallback=0.0):
    """Return *value* as a finite float, or *fallback* if NaN/inf/non-numeric."""
    try:
        v = float(value)
        if math.isfinite(v):
            return v
        return float(fallback)
    except (ValueError, TypeError):
        return float(fallback)


# ============================================================
# SECTION: Module-Level Constants
# ============================================================

# Storage path for line snapshots
_MOVEMENT_JSON_PATH = Path(__file__).parent.parent / "tracking" / "line_movement.json"

# Movement magnitude thresholds (in absolute line units)
MOVEMENT_THRESHOLD_SMALL  = 0.25   # 0.25 change = noise / rounding
MOVEMENT_THRESHOLD_MEDIUM = 0.50   # 0.5 change = meaningful movement
MOVEMENT_THRESHOLD_LARGE  = 1.00   # 1.0+ change = significant sharp money

# Confidence adjustments for confirming vs warning signals
CONFIRMING_ADJUSTMENT_SMALL  = 2.0   # Small confirming movement: +2 pts
CONFIRMING_ADJUSTMENT_MEDIUM = 3.5   # Medium confirming movement: +3.5 pts
CONFIRMING_ADJUSTMENT_LARGE  = 5.0   # Large confirming movement: +5 pts
WARNING_ADJUSTMENT_SMALL     = -3.0  # Small warning movement: -3 pts
WARNING_ADJUSTMENT_MEDIUM    = -5.5  # Medium warning movement: -5.5 pts
WARNING_ADJUSTMENT_LARGE     = -8.0  # Large warning movement: -8 pts

# ============================================================
# END SECTION: Module-Level Constants
# ============================================================


# ============================================================
# SECTION: JSON Storage Helpers
# ============================================================

def _load_movement_data():
    """
    Load line movement data from the JSON storage file.

    Returns:
        dict: Stored snapshots keyed by player/stat/platform key.
    """
    if not _MOVEMENT_JSON_PATH.exists():
        return {}
    try:
        with open(_MOVEMENT_JSON_PATH, "r") as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError):
        return {}


def _save_movement_data(data):
    """
    Persist line movement data to the JSON storage file.

    Args:
        data (dict): Full movement data dict to write.
    """
    os.makedirs(_MOVEMENT_JSON_PATH.parent, exist_ok=True)
    with open(_MOVEMENT_JSON_PATH, "w") as f:
        json.dump(data, f, indent=2)

# ============================================================
# END SECTION: JSON Storage Helpers
# ============================================================


# ============================================================
# SECTION: Line Movement Detection
# ============================================================

def detect_line_movement(player_name, stat_type, initial_line, current_line, model_direction):
    """
    Analyze the movement of a prop line and produce a sharp-money signal.

    Args:
        player_name (str): Player name.
        stat_type (str): Stat category (e.g., 'points').
        initial_line (float): The line at first import (opening line).
        current_line (float): The current line (most recent snapshot).
        model_direction (str): Our recommended direction ('OVER' or 'UNDER').

    Returns:
        dict: {
            'movement_direction': str,          # 'up', 'down', or 'stable'
            'movement_magnitude': float,        # Absolute change in line
            'movement_pct': float,              # % change relative to initial
            'agrees_with_model': bool,          # True if movement confirms model
            'sharp_money_signal': str,          # 'confirming', 'warning', 'neutral'
            'confidence_adjustment': float,     # Pts to add/subtract to confidence
            'interpretation': str,              # Human-readable explanation
        }

    Example:
        detect_line_movement('LeBron James', 'points', 25.5, 24.5, 'OVER')
        # Line moved DOWN for OVER → confirming (easier to hit)
        # Returns: {'movement_direction': 'down', 'agrees_with_model': True, ...}
    """
    movement = current_line - initial_line

    # Determine direction of movement
    if movement > 0.1:
        movement_direction = 'up'
    elif movement < -0.1:
        movement_direction = 'down'
    else:
        movement_direction = 'stable'

    movement_magnitude = abs(movement)
    movement_pct = movement_magnitude / max(0.5, abs(initial_line)) * 100

    # For OVER: line moving DOWN is favorable (bar is lower to clear)
    # For UNDER: line moving UP is favorable (bar is higher, easier to go under)
    model_direction_upper = (model_direction or 'OVER').upper()
    agrees_with_model = (
        (model_direction_upper == 'OVER' and movement < -MOVEMENT_THRESHOLD_SMALL) or
        (model_direction_upper == 'UNDER' and movement > MOVEMENT_THRESHOLD_SMALL)
    )

    # Assign signal strength and confidence adjustment based on magnitude
    if movement_direction == 'stable' or movement_magnitude <= MOVEMENT_THRESHOLD_SMALL:
        sharp_money_signal = 'neutral'
        confidence_adjustment = 0.0
        interpretation = (
            f"Line for {player_name} {stat_type} is stable "
            f"({initial_line} → {current_line}). No sharp money signal."
        )
    elif agrees_with_model:
        # Confirming movement — line moved in our favor
        if movement_magnitude <= MOVEMENT_THRESHOLD_MEDIUM:
            sharp_money_signal = 'confirming'
            confidence_adjustment = CONFIRMING_ADJUSTMENT_SMALL
            size_label = 'small'
        elif movement_magnitude <= MOVEMENT_THRESHOLD_LARGE:
            sharp_money_signal = 'confirming'
            confidence_adjustment = CONFIRMING_ADJUSTMENT_MEDIUM
            size_label = 'medium'
        else:
            sharp_money_signal = 'confirming'
            confidence_adjustment = CONFIRMING_ADJUSTMENT_LARGE
            size_label = 'large'
        interpretation = (
            f"Line moved {movement_direction} ({initial_line} → {current_line}), "
            f"confirming {model_direction_upper} model for {player_name} {stat_type}. "
            f"{size_label.capitalize()} sharp money signal (+{confidence_adjustment:.1f} confidence)."
        )
    else:
        # Warning movement — line moved against our recommendation
        if movement_magnitude <= MOVEMENT_THRESHOLD_MEDIUM:
            sharp_money_signal = 'warning'
            confidence_adjustment = WARNING_ADJUSTMENT_SMALL
            size_label = 'small'
        elif movement_magnitude <= MOVEMENT_THRESHOLD_LARGE:
            sharp_money_signal = 'warning'
            confidence_adjustment = WARNING_ADJUSTMENT_MEDIUM
            size_label = 'medium'
        else:
            sharp_money_signal = 'warning'
            confidence_adjustment = WARNING_ADJUSTMENT_LARGE
            size_label = 'large'
        interpretation = (
            f"Line moved {movement_direction} ({initial_line} → {current_line}), "
            f"WARNING against {model_direction_upper} model for {player_name} {stat_type}. "
            f"{size_label.capitalize()} opposing signal ({confidence_adjustment:.1f} confidence)."
        )

    return {
        'movement_direction':    movement_direction,
        'movement_magnitude':    _safe_float(round(movement_magnitude, 4), 0.0),
        'movement_pct':          _safe_float(round(movement_pct, 2), 0.0),
        'agrees_with_model':     agrees_with_model,
        'sharp_money_signal':    sharp_money_signal,
        'confidence_adjustment': _safe_float(confidence_adjustment, 0.0),
        'interpretation':        interpretation,
    }

# ============================================================
# END SECTION: Line Movement Detection
# ============================================================


# ============================================================
# SECTION: Line Snapshot Tracking
# ============================================================

def track_line_snapshot(player_name, stat_type, platform, line_value):
    """
    Store a timestamped line snapshot for movement detection.

    Args:
        player_name (str): Player name.
        stat_type (str): Stat category.
        platform (str): Platform name (e.g., 'PrizePicks').
        line_value (float): Current line value.

    Returns:
        str: Snapshot key used to retrieve history.

    Example:
        track_line_snapshot('LeBron James', 'points', 'PrizePicks', 25.5)
    """
    key = (
        f"{player_name.lower().replace(' ', '_')}"
        f"_{stat_type}"
        f"_{platform.lower()}"
    )

    snapshot = {
        'timestamp': datetime.datetime.now().isoformat(),
        'line':      float(line_value),
        'platform':  platform,
    }

    data = _load_movement_data()
    if key not in data:
        data[key] = []
    data[key].append(snapshot)

    _save_movement_data(data)
    return key


def get_movement_summary(player_name, stat_type, platform=None):
    """
    Return all line snapshots and computed movement signals.

    Args:
        player_name (str): Player name.
        stat_type (str): Stat category.
        platform (str, optional): Filter to specific platform.

    Returns:
        dict: {
            'has_movement_data': bool,
            'snapshots': list of {timestamp, line, platform},
            'initial_line': float or None,
            'current_line': float or None,
            'movement': float or None,       # current - initial
            'time_span_hours': float or None,
        }
    """
    data = _load_movement_data()

    # Build candidate keys — if platform given, look at that key only;
    # otherwise scan all keys matching player + stat_type prefix
    player_slug = player_name.lower().replace(' ', '_')
    prefix = f"{player_slug}_{stat_type}_"

    if platform:
        candidate_keys = [f"{prefix}{platform.lower()}"]
    else:
        candidate_keys = [k for k in data if k.startswith(prefix)]

    # Merge all matching snapshots across platforms/keys
    all_snapshots = []
    for k in candidate_keys:
        all_snapshots.extend(data.get(k, []))

    # Apply platform filter at snapshot level if needed
    if platform:
        all_snapshots = [s for s in all_snapshots if s.get('platform', '').lower() == platform.lower()]

    if not all_snapshots:
        return {
            'has_movement_data': False,
            'snapshots':         [],
            'initial_line':      None,
            'current_line':      None,
            'movement':          None,
            'time_span_hours':   None,
        }

    # Sort chronologically
    all_snapshots.sort(key=lambda s: s.get('timestamp', ''))

    initial_line  = all_snapshots[0].get('line')
    current_line  = all_snapshots[-1].get('line')
    if initial_line is None or current_line is None:
        return {
            'has_movement_data': False,
            'snapshots':         all_snapshots,
            'initial_line':      initial_line,
            'current_line':      current_line,
            'movement':          None,
            'time_span_hours':   None,
        }
    movement      = current_line - initial_line

    # Compute time span between first and last snapshot
    try:
        t_start = datetime.datetime.fromisoformat(all_snapshots[0]['timestamp'])
        t_end   = datetime.datetime.fromisoformat(all_snapshots[-1]['timestamp'])
        time_span_hours = (t_end - t_start).total_seconds() / 3600.0
    except (ValueError, KeyError):
        time_span_hours = None

    return {
        'has_movement_data': True,
        'snapshots':         all_snapshots,
        'initial_line':      _safe_float(initial_line, 0.0) if initial_line is not None else None,
        'current_line':      _safe_float(current_line, 0.0) if current_line is not None else None,
        'movement':          _safe_float(round(movement, 4), 0.0) if movement is not None else None,
        'time_span_hours':   _safe_float(round(time_span_hours, 2), 0.0) if time_span_hours is not None else None,
    }

# ============================================================
# END SECTION: Line Snapshot Tracking
# ============================================================
