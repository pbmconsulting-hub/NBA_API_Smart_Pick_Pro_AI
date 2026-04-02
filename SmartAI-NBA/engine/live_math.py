# ============================================================
# FILE: engine/live_math.py
# PURPOSE: Pacing Engine & Risk Flags for the Live Sweat
#          dashboard.  Projects a player's final stat line from
#          current in-game stats, minutes played, and pace.
# ============================================================

import logging

try:
    from utils.logger import get_logger
    _logger = get_logger(__name__)
except ImportError:
    _logger = logging.getLogger(__name__)

# ── Constants ─────────────────────────────────────────────────
REGULATION_MINUTES = 48
HALF_MINUTES = 24
MINUTES_PER_QUARTER = 12.0
BLOWOUT_THRESHOLD = 20
FOUL_TROUBLE_THRESHOLD = 3
BLOWOUT_PACE_SLASH = 0.70  # multiply projected pace by 70% in blowouts
OT_EXTRA_MINUTES = 5       # each overtime period adds 5 minutes
TYPICAL_STARTER_MINUTES = 34  # average starter minutes for fallback


def _parse_period(period: str) -> tuple[int, bool]:
    """
    Parse a period string into (period_number, is_overtime).

    Accepts: "1", "Q3", "OT", "OT1", "OT2", "4", etc.
    Returns: (int period_num 1-4+, bool is_ot)
    """
    raw = str(period).upper().replace("Q", "").strip()

    if raw.startswith("OT"):
        ot_num_str = raw[2:].strip()
        ot_num = int(ot_num_str) if ot_num_str.isdigit() else 1
        return (4 + ot_num, True)

    try:
        num = int(raw) if raw.isdigit() else 0
    except (ValueError, TypeError):
        num = 0

    return (num, False)


def calculate_live_pace(
    current_stat: float,
    minutes_played: float,
    target_stat: float,
    live_score_diff: float = 0.0,
    current_fouls: int = 0,
    period: str = "",
    direction: str = "OVER",
) -> dict:
    """
    Project the final stat line and return risk flags.

    Parameters
    ----------
    current_stat : float
        The player's current accumulated stat value (e.g. 14 points).
    minutes_played : float
        Minutes the player has played so far.
    target_stat : float
        The prop line / target the user bet on (e.g. 24.5 points).
    live_score_diff : float
        Absolute score differential (always positive = leading margin).
        A positive value > 20 triggers blowout risk in 3rd/4th quarters.
    current_fouls : int
        The player's current personal foul count.
    period : str
        Current game period indicator (e.g. "1", "2", "3", "4", "Q1", "OT").
    direction : str
        Bet direction — ``"OVER"`` or ``"UNDER"``.  Affects the ``on_pace``
        flag (for UNDER, being *below* target is favourable).

    Returns
    -------
    dict with keys:
        current_stat        – echoed back
        target_stat         – echoed back
        distance            – how many more needed (target − current)
        minutes_played      – echoed back
        minutes_remaining   – estimated minutes left in the game
        pace_per_minute     – current_stat / max(1, minutes_played)
        projected_final     – projected total at game end (adjusted)
        pct_of_target       – projected_final / target as a 0-100 float
        blowout_risk        – True if blowout detected in late quarters
        foul_trouble        – True if ≥3 fouls in the first half
        on_pace             – True when projection favours the bet direction
        cashed              – True when current_stat already hits the target
        direction           – echoed back ("OVER" or "UNDER")
        is_overtime         – True if the game is in an overtime period
    """
    # Sanitise inputs
    current_stat = max(0.0, float(current_stat))
    minutes_played = max(0.0, float(minutes_played))
    target_stat = max(0.01, float(target_stat))  # avoid division by zero
    live_score_diff = abs(float(live_score_diff))
    current_fouls = max(0, int(current_fouls))
    direction = str(direction).upper().strip() if direction else "OVER"
    if direction not in ("OVER", "UNDER"):
        direction = "OVER"

    # Already cashed?
    if direction == "OVER":
        cashed = current_stat >= target_stat
    else:
        # UNDER cashes when the game ends with stat below target — can only
        # truly confirm at Final.  During the game, flag cashed=False; the
        # UI / post-game resolver handles final adjudication.
        cashed = False

    # ── Period & overtime parsing ─────────────────────────────
    period_num, is_overtime = _parse_period(period)

    # Total game minutes (48 regulation + 5 per OT)
    if is_overtime:
        ot_count = max(1, period_num - 4)
        total_game_minutes = REGULATION_MINUTES + (ot_count * OT_EXTRA_MINUTES)
    else:
        total_game_minutes = REGULATION_MINUTES

    # ── Estimated minutes remaining for this player ──────────
    # Players don't play all 48 minutes — use a blend of their actual
    # pace and a typical-starter ceiling.
    if minutes_played > 0:
        # Estimate the player's total minutes allocation based on current
        # minutes-per-quarter pace, but cap at total game minutes.
        mins_per_period = minutes_played / max(1, period_num) if period_num > 0 else minutes_played
        est_total_minutes = min(total_game_minutes, mins_per_period * (total_game_minutes / MINUTES_PER_QUARTER))
        est_total_minutes = max(est_total_minutes, minutes_played)  # never below what's played
    else:
        est_total_minutes = TYPICAL_STARTER_MINUTES

    minutes_remaining = max(0.0, est_total_minutes - minutes_played)

    # ── Per-minute pace ──────────────────────────────────────
    safe_minutes = max(1.0, minutes_played)
    pace_per_minute = current_stat / safe_minutes

    # Projected final = current + (pace × remaining minutes)
    projected_final = current_stat + (pace_per_minute * minutes_remaining)

    # ── Risk flags ────────────────────────────────────────────
    is_second_half = period_num >= 3

    blowout_risk = False
    if is_second_half and live_score_diff > BLOWOUT_THRESHOLD:
        blowout_risk = True
        projected_final = current_stat + (pace_per_minute * minutes_remaining * BLOWOUT_PACE_SLASH)

    is_first_half = minutes_played <= HALF_MINUTES
    foul_trouble = is_first_half and current_fouls >= FOUL_TROUBLE_THRESHOLD

    if foul_trouble:
        projected_final = current_stat + (pace_per_minute * minutes_remaining * 0.85)

    # ── Direction-aware computed fields ───────────────────────
    distance = max(0.0, target_stat - current_stat)

    if direction == "OVER":
        on_pace = projected_final >= target_stat
    else:
        on_pace = projected_final < target_stat

    pct_of_target = min(200.0, (projected_final / target_stat) * 100)

    return {
        "current_stat":      current_stat,
        "target_stat":       target_stat,
        "distance":          round(distance, 1),
        "minutes_played":    round(minutes_played, 1),
        "minutes_remaining": round(minutes_remaining, 1),
        "pace_per_minute":   round(pace_per_minute, 2),
        "projected_final":   round(projected_final, 1),
        "pct_of_target":     round(pct_of_target, 1),
        "blowout_risk":      blowout_risk,
        "foul_trouble":      foul_trouble,
        "on_pace":           on_pace,
        "cashed":            cashed,
        "direction":         direction,
        "is_overtime":       is_overtime,
    }


def pace_color_tier(pct_of_target: float, direction: str = "OVER") -> str:
    """
    Return the CSS class suffix for the progress bar fill color.

    For **OVER** bets:
        * 0-50 %   → ``blue``
        * 51-85 %  → ``orange``
        * 86-99 %  → ``red``   (pulsing)
        * 100 % +  → ``green`` (glowing)

    For **UNDER** bets the tiers are inverted — a low projection is good:
        * ≤ 85 %   → ``green`` (well under)
        * 86-99 %  → ``orange`` (close to hitting)
        * 100 % +  → ``red``   (over the line — losing)
    """
    direction = str(direction).upper().strip() if direction else "OVER"

    if direction == "UNDER":
        if pct_of_target >= 100:
            return "red"
        if pct_of_target >= 86:
            return "orange"
        return "green"

    # OVER (default)
    if pct_of_target >= 100:
        return "green"
    if pct_of_target >= 86:
        return "red"
    if pct_of_target >= 51:
        return "orange"
    return "blue"
