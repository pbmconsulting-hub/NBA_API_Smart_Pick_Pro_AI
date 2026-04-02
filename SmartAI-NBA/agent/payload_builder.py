# ============================================================
# FILE: agent/payload_builder.py
# PURPOSE: Pillar 4 — The Live AI Panic Room: Payload Builder.
#          Compiles granular game-state JSON for the Joseph M.
#          Smith persona engine.  Includes a short-term "grudge"
#          memory buffer (last 3 rants) so the LLM never repeats.
# CONNECTS TO: engine/live_math.py (pacing data),
#              data/live_game_tracker.py (live box-score data),
#              agent/live_persona.py (prompt consumption)
# ============================================================

import logging
from collections import deque

try:
    from utils.logger import get_logger
    _logger = get_logger(__name__)
except ImportError:
    _logger = logging.getLogger(__name__)


# ============================================================
# SECTION: Constants — Game State Classification
# ============================================================

# The 8 canonical game states from the Pillar 4 spec.
# Each maps to a distinct emotional register in the persona prompt.
GAME_STATE_HOOK           = "THE_HOOK"             # 0.5 away from target
GAME_STATE_FREE_THROW     = "FREE_THROW_MERCHANT"  # bad FG%, FT-heavy
GAME_STATE_BENCH_SWEAT    = "BENCH_SWEAT"          # blowout / rotation risk
GAME_STATE_USAGE_FREEZE   = "USAGE_FREEZE_OUT"     # low usage / frozen out
GAME_STATE_GARBAGE_MIRACLE = "GARBAGE_TIME_MIRACLE" # stat-padding in garbage time
GAME_STATE_LOCKER_ROOM    = "LOCKER_ROOM_TRAGEDY"  # injury flag
GAME_STATE_REF_SHOW       = "THE_REF_SHOW"         # foul trouble
GAME_STATE_CLEAN_CASH     = "THE_CLEAN_CASH"       # already cashed

ALL_GAME_STATES = (
    GAME_STATE_HOOK, GAME_STATE_FREE_THROW, GAME_STATE_BENCH_SWEAT,
    GAME_STATE_USAGE_FREEZE, GAME_STATE_GARBAGE_MIRACLE,
    GAME_STATE_LOCKER_ROOM, GAME_STATE_REF_SHOW, GAME_STATE_CLEAN_CASH,
)

# ── Thresholds ─────────────────────────────────────────────
_HOOK_DISTANCE = 1.5       # within 1.5 of target → "The Hook"
_POOR_FG_PCT = 35.0        # below 35% FG → Free Throw Merchant candidate
_HIGH_FT_ATTEMPTS = 6      # 6+ FTA needed for Free Throw Merchant
_LOW_USAGE_MINUTES = 8.0   # < 8 min in a half → Usage Freeze-Out
_GARBAGE_MARGIN = 25       # 25+ point lead → garbage time territory
_GRUDGE_BUFFER_SIZE = 3    # remember last 3 rants


# ============================================================
# SECTION: Grudge Memory Buffer
# ============================================================

class GrudgeBuffer:
    """
    Short-term memory that stores the last N rants so the LLM
    can avoid repeating itself.  Thread-safe via deque.

    Usage
    -----
    >>> buf = GrudgeBuffer(maxlen=3)
    >>> buf.add("rant about free throws")
    >>> buf.get_history()
    ["rant about free throws"]
    """

    def __init__(self, maxlen: int = _GRUDGE_BUFFER_SIZE):
        self._buffer: deque[str] = deque(maxlen=max(1, int(maxlen)))

    def add(self, rant: str) -> None:
        """Push a new rant into the buffer (auto-evicts oldest)."""
        if rant and isinstance(rant, str):
            self._buffer.append(rant.strip())

    def get_history(self) -> list[str]:
        """Return the buffered rants as a plain list (oldest first)."""
        return list(self._buffer)

    def clear(self) -> None:
        """Flush the buffer."""
        self._buffer.clear()

    def __len__(self) -> int:
        return len(self._buffer)


# Module-level singleton so the buffer persists across calls
# within a single Streamlit session / process.
_default_grudge = GrudgeBuffer()


def get_grudge_buffer() -> GrudgeBuffer:
    """Return the module-level grudge buffer singleton."""
    return _default_grudge


# ============================================================
# SECTION: Game-State Classifier
# ============================================================

def classify_game_state(
    pace_result: dict,
    shooting_line: str = "",
    free_throw_line: str = "",
    injury_status: str = "Active",
    score_diff: float = 0.0,
) -> str:
    """
    Classify the current situation into one of the 8 canonical game states.

    Parameters
    ----------
    pace_result : dict
        Output from ``engine.live_math.calculate_live_pace()``.
    shooting_line : str
        Field-goal line, e.g. ``"6/21 FG"`` (optional — parsed for FG%).
    free_throw_line : str
        Free-throw line, e.g. ``"10/12 FTA"`` (optional).
    injury_status : str
        ``"Active"``, ``"Questionable"``, ``"Out"``, etc.
    score_diff : float
        Absolute score differential (positive = leading margin).

    Returns
    -------
    str — one of the ``GAME_STATE_*`` constants.
    """
    if not isinstance(pace_result, dict):
        return GAME_STATE_HOOK

    cashed = pace_result.get("cashed", False)
    distance = pace_result.get("distance", 999)
    blowout_risk = pace_result.get("blowout_risk", False)
    foul_trouble = pace_result.get("foul_trouble", False)
    minutes_played = pace_result.get("minutes_played", 0)

    # 1. Already cashed → THE_CLEAN_CASH
    if cashed:
        return GAME_STATE_CLEAN_CASH

    # 2. Injury flag → LOCKER_ROOM_TRAGEDY
    if str(injury_status).strip().lower() not in ("active", "available", ""):
        return GAME_STATE_LOCKER_ROOM

    # 3. Foul trouble → THE_REF_SHOW
    if foul_trouble:
        return GAME_STATE_REF_SHOW

    # 4. Garbage time (huge margin, second half) → GARBAGE_TIME_MIRACLE
    if blowout_risk and abs(score_diff) >= _GARBAGE_MARGIN:
        return GAME_STATE_GARBAGE_MIRACLE

    # 5. Blowout / bench risk → BENCH_SWEAT
    if blowout_risk:
        return GAME_STATE_BENCH_SWEAT

    # 6. Free Throw Merchant (bad FG%, high FTA)
    fg_pct = _parse_fg_pct(shooting_line)
    ft_attempts = _parse_ft_attempts(free_throw_line)
    if fg_pct is not None and fg_pct < _POOR_FG_PCT and ft_attempts >= _HIGH_FT_ATTEMPTS:
        return GAME_STATE_FREE_THROW

    # 7. Usage freeze-out (low minutes for the half)
    pct_of_target = pace_result.get("pct_of_target", 50)
    if minutes_played > 0 and pct_of_target < 40 and minutes_played < _LOW_USAGE_MINUTES:
        return GAME_STATE_USAGE_FREEZE

    # 8. The Hook (agonisingly close to target)
    if 0 < distance <= _HOOK_DISTANCE:
        return GAME_STATE_HOOK

    # Default fallback — use Hook as the catch-all "sweating" state
    return GAME_STATE_HOOK


def _parse_fg_pct(shooting_line: str) -> float | None:
    """Parse ``'6/21 FG'`` → 28.6.  Returns None on failure."""
    try:
        parts = str(shooting_line).split("/")
        if len(parts) < 2:
            return None
        made = float(parts[0].strip())
        attempted = float(parts[1].split()[0].strip())
        if attempted == 0:
            return 0.0
        return (made / attempted) * 100
    except (ValueError, IndexError):
        return None


def _parse_ft_attempts(free_throw_line: str) -> int:
    """Parse ``'10/12 FTA'`` → 12.  Returns 0 on failure."""
    try:
        parts = str(free_throw_line).split("/")
        if len(parts) < 2:
            return 0
        return int(float(parts[1].split()[0].strip()))
    except (ValueError, IndexError):
        return 0


# ============================================================
# SECTION: Payload Builder
# ============================================================

def build_live_vibe_payload(
    ticket: dict,
    live_stats: dict | None = None,
    game_context: dict | None = None,
    chat_history: list[str] | None = None,
    grudge_buffer: GrudgeBuffer | None = None,
    pace_result: dict | None = None,
) -> dict:
    """
    Build the structured JSON payload that feeds the Joseph persona prompt.

    Parameters
    ----------
    ticket : dict
        The user's active bet.  Expected keys:
        ``player_name``, ``stat_type``, ``line``, ``direction``.
    live_stats : dict or None
        The player's live box-score stats (from ``match_live_player``).
        Keys: ``pts``, ``reb``, ``ast``, ``minutes``, ``fouls``, etc.
    game_context : dict or None
        Live game data (from ``get_game_for_player``).
        Keys: ``home_score``, ``away_score``, ``period``, ``game_clock``,
        ``home_team``, ``away_team``.
    chat_history : list[str] or None
        Explicit rant history override.  If ``None``, uses the
        module-level grudge buffer.
    grudge_buffer : GrudgeBuffer or None
        Custom grudge buffer.  If ``None``, uses the module-level default.
    pace_result : dict or None
        Pre-computed output from ``calculate_live_pace()``.  When provided
        the classifier uses this directly instead of building a synthetic
        pace_result.

    Returns
    -------
    dict — The complete vibe payload with all fields populated.
    """
    ticket = ticket or {}
    live_stats = live_stats or {}
    game_context = game_context or {}

    if grudge_buffer is None:
        grudge_buffer = _default_grudge
    if chat_history is None:
        chat_history = grudge_buffer.get_history()

    # ── Extract ticket fields ─────────────────────────────────
    direction = str(ticket.get("direction", "OVER")).upper()
    stat_type = str(ticket.get("stat_type", "points")).lower()
    line = float(ticket.get("line", 0) or 0)
    player_name = str(ticket.get("player_name", "Unknown"))

    # ── Extract live stat value ───────────────────────────────
    _stat_key_map = {
        "points": "pts", "rebounds": "reb", "assists": "ast",
        "steals": "stl", "blocks": "blk", "turnovers": "tov",
        "threes": "fg3m", "pts": "pts", "reb": "reb", "ast": "ast",
    }
    box_key = _stat_key_map.get(stat_type, stat_type)
    current = float(live_stats.get(box_key, 0) or 0)
    needed = max(0.0, line - current)

    # ── Game context fields ───────────────────────────────────
    home_score = int(game_context.get("home_score", 0) or 0)
    away_score = int(game_context.get("away_score", 0) or 0)
    score_diff = home_score - away_score
    period = str(game_context.get("period", ""))
    clock = str(game_context.get("game_clock", ""))
    clock_display = f"Q{period} {clock}" if period and clock else (period or clock or "")
    opponent = str(
        game_context.get("away_team", "")
        or game_context.get("opponent", "")
        or ""
    )

    # ── Shooting / FT lines ───────────────────────────────────
    # These may come from enriched live_stats if available
    fg_made = int(live_stats.get("fgm", 0) or 0)
    fg_att = int(live_stats.get("fga", 0) or 0)
    ft_made = int(live_stats.get("ftm", 0) or 0)
    ft_att = int(live_stats.get("fta", 0) or 0)
    shooting = f"{fg_made}/{fg_att} FG" if fg_att > 0 else ""
    free_throws = f"{ft_made}/{ft_att} FTA" if ft_att > 0 else ""

    foul_count = int(live_stats.get("fouls", live_stats.get("pf", 0)) or 0)
    injury_status = str(live_stats.get("injury_status", "Active"))

    # ── Build or reuse pace result for classifier ─────────────
    minutes_played = float(live_stats.get("minutes", 0) or 0)
    if pace_result is not None and isinstance(pace_result, dict):
        pace_for_classify = pace_result
    else:
        # UNDER cashing: the game must be over and current < line.
        # Accept Q4/OT/OTn/Final with clock at "0:00", "", "00:00", or "Final".
        _period_lower = str(period).strip().lower()
        _is_late_game = _period_lower in ("4", "q4", "final") or _period_lower.startswith("ot")
        _clock_stripped = str(clock).strip().lower()
        _clock_done = _clock_stripped in ("", "0:00", "00:00", "0.0", "final")
        if direction == "UNDER":
            _under_cashed = current < line and _is_late_game and _clock_done
        else:
            _under_cashed = False

        pace_for_classify = {
            "cashed": (direction == "OVER" and current >= line) or _under_cashed,
            "distance": needed,
            "blowout_risk": abs(score_diff) >= 20 and period in ("3", "4", "Q3", "Q4"),
            "foul_trouble": foul_count >= 3 and minutes_played <= 24,
            "minutes_played": minutes_played,
            "pct_of_target": (current / max(0.01, line)) * 100,
            "direction": direction,
        }

    game_state = classify_game_state(
        pace_result=pace_for_classify,
        shooting_line=shooting,
        free_throw_line=free_throws,
        injury_status=injury_status,
        score_diff=score_diff,
    )

    # ── Minutes remaining (from pace or estimated) ────────────
    minutes_remaining = 0.0
    if pace_result and isinstance(pace_result, dict):
        minutes_remaining = float(pace_result.get("minutes_remaining", 0) or 0)

    # ── Assemble the payload ──────────────────────────────────
    payload = {
        "player_name":          player_name,
        "ticket_type":          direction,
        "stat":                 stat_type.replace("_", " ").title(),
        "line":                 line,
        "current":              current,
        "needed":               round(needed, 1),
        "clock":                clock_display,
        "score_diff":           str(score_diff),
        "opponent":             opponent,
        "shooting":             shooting,
        "free_throws":          free_throws,
        "foul_count":           foul_count,
        "injury_status":        injury_status,
        "minutes_remaining":    round(minutes_remaining, 1),
        "game_state":           game_state,
        "recent_rants_history": list(chat_history),
    }

    return payload
