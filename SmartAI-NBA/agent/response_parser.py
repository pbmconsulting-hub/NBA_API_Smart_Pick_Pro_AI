# ============================================================
# FILE: agent/response_parser.py
# PURPOSE: Pillar 4 — The Live AI Panic Room: Response Parser.
#          Defines and validates the strict JSON output schema
#          for the Joseph persona engine.  Ensures every LLM
#          response is structured for the Streamlit UI.
# CONNECTS TO: agent/live_persona.py (prompt generation),
#              agent/payload_builder.py (grudge buffer update)
# ============================================================

import json
import logging
import re

try:
    from utils.logger import get_logger
    _logger = get_logger(__name__)
except ImportError:
    _logger = logging.getLogger(__name__)


# ============================================================
# SECTION: Vibe Status Enum
# ============================================================

# Valid vibe_status values — the UI uses these to change the
# background glow of the Streamlit container.
VALID_VIBE_STATUSES = ("Panic", "Hype", "Disgust", "Victory", "Sweating")

# Mapping from game state → default vibe status (fallback)
_STATE_TO_DEFAULT_VIBE: dict[str, str] = {
    "THE_HOOK":             "Sweating",
    "FREE_THROW_MERCHANT":  "Disgust",
    "BENCH_SWEAT":          "Panic",
    "USAGE_FREEZE_OUT":     "Panic",
    "GARBAGE_TIME_MIRACLE": "Disgust",
    "LOCKER_ROOM_TRAGEDY":  "Panic",
    "THE_REF_SHOW":         "Panic",
    "THE_CLEAN_CASH":       "Victory",
}


# ============================================================
# SECTION: Output Schema Definition
# ============================================================

# OpenAI-compatible JSON Schema for structured outputs.
VIBE_RESPONSE_SCHEMA = {
    "type": "object",
    "properties": {
        "vibe_status": {
            "type": "string",
            "enum": list(VALID_VIBE_STATUSES),
            "description": (
                "Overall emotional state. The UI will change background "
                "glow based on this value."
            ),
        },
        "ticker_tape_headline": {
            "type": "string",
            "maxLength": 50,
            "description": (
                "Maximum 5 words, ALL CAPS. A punchy headline for the "
                "ticker tape display. Example: 'DYING ON THE HOOK!'"
            ),
        },
        "joseph_rant": {
            "type": "string",
            "description": (
                "The actual dynamic text that will be streamed to the "
                "user. Joseph's dramatic, anchored-in-reality reaction."
            ),
        },
    },
    "required": ["vibe_status", "ticker_tape_headline", "joseph_rant"],
    "additionalProperties": False,
}


# ============================================================
# SECTION: Response Validator & Parser
# ============================================================

def validate_vibe_response(data: dict) -> dict:
    """
    Validate a vibe response dict against the schema.

    Parameters
    ----------
    data : dict
        The parsed response dict.

    Returns
    -------
    dict with keys: ``vibe_status``, ``ticker_tape_headline``, ``joseph_rant``.

    Raises
    ------
    ValueError — if any required field is missing or invalid.
    """
    if not isinstance(data, dict):
        raise ValueError(f"Expected dict, got {type(data).__name__}")

    # ── vibe_status ───────────────────────────────────────────
    vibe_status = str(data.get("vibe_status", "")).strip()
    if vibe_status not in VALID_VIBE_STATUSES:
        raise ValueError(
            f"Invalid vibe_status '{vibe_status}'. "
            f"Must be one of: {VALID_VIBE_STATUSES}"
        )

    # ── ticker_tape_headline ──────────────────────────────────
    headline = str(data.get("ticker_tape_headline", "")).strip()
    if not headline:
        raise ValueError("ticker_tape_headline is required and must not be empty")
    # Enforce ALL CAPS
    headline = headline.upper()
    # Enforce max ~5 words (allow some flexibility — up to 8)
    word_count = len(headline.split())
    if word_count > 8:
        headline = " ".join(headline.split()[:5])

    # ── joseph_rant ───────────────────────────────────────────
    joseph_rant = str(data.get("joseph_rant", "")).strip()
    if not joseph_rant:
        raise ValueError("joseph_rant is required and must not be empty")

    return {
        "vibe_status": vibe_status,
        "ticker_tape_headline": headline,
        "joseph_rant": joseph_rant,
    }


def parse_vibe_response(raw_text: str, game_state: str = "") -> dict:
    """
    Parse a raw LLM text response into a validated vibe response dict.

    Attempts JSON extraction first (for structured output mode),
    then falls back to regex-based extraction.

    Parameters
    ----------
    raw_text : str
        The raw string from the LLM (may contain markdown/code fences).
    game_state : str
        Current game state (used for default vibe_status fallback).

    Returns
    -------
    dict with keys: ``vibe_status``, ``ticker_tape_headline``, ``joseph_rant``.
        Always returns a valid dict — never raises.
    """
    if not raw_text or not isinstance(raw_text, str):
        return _fallback_response(game_state)

    # ── Attempt 1: Direct JSON parse ──────────────────────────
    cleaned = raw_text.strip()

    # Strip markdown code fences (```json ... ```)
    if cleaned.startswith("```"):
        lines = cleaned.split("\n")
        # Remove first line (```json) and last line (```)
        inner_lines = []
        started = False
        for line in lines:
            if line.strip().startswith("```") and not started:
                started = True
                continue
            if line.strip() == "```":
                break
            if started:
                inner_lines.append(line)
        if inner_lines:
            cleaned = "\n".join(inner_lines)

    try:
        parsed = json.loads(cleaned)
        return validate_vibe_response(parsed)
    except (json.JSONDecodeError, ValueError):
        pass

    # ── Attempt 2: Find JSON object in the text ───────────────
    json_match = re.search(r'\{[^{}]*"vibe_status"[^{}]*\}', raw_text, re.DOTALL)
    if json_match:
        try:
            parsed = json.loads(json_match.group(0))
            return validate_vibe_response(parsed)
        except (json.JSONDecodeError, ValueError):
            pass

    # ── Attempt 3: Use the raw text as the rant ───────────────
    _logger.warning("Could not parse structured vibe response — using raw text as rant")
    default_vibe = _STATE_TO_DEFAULT_VIBE.get(game_state, "Sweating")

    # Try to extract a headline from the first line if it's caps
    lines = [l.strip() for l in raw_text.strip().split("\n") if l.strip()]
    headline = "JOSEPH IS SWEATING!"
    rant = raw_text.strip()

    if lines and lines[0].isupper() and len(lines[0].split()) <= 8:
        headline = lines[0]
        rant = " ".join(lines[1:]) if len(lines) > 1 else raw_text.strip()

    return {
        "vibe_status": default_vibe,
        "ticker_tape_headline": headline.upper()[:50],
        "joseph_rant": rant,
    }


def _fallback_response(game_state: str = "") -> dict:
    """Return a safe fallback response when parsing fails completely."""
    vibe = _STATE_TO_DEFAULT_VIBE.get(game_state, "Sweating")
    return {
        "vibe_status": vibe,
        "ticker_tape_headline": "JOSEPH IS SWEATING!",
        "joseph_rant": (
            "Joseph M. Smith is processing the game state — "
            "stand by for a FIRE take!"
        ),
    }


def build_openai_response_format() -> dict:
    """
    Return the ``response_format`` parameter for OpenAI API calls
    using JSON Schema structured outputs.

    Usage
    -----
    >>> client.chat.completions.create(
    ...     model="gpt-4o",
    ...     messages=[...],
    ...     response_format=build_openai_response_format(),
    ... )
    """
    return {
        "type": "json_schema",
        "json_schema": {
            "name": "joseph_vibe_check",
            "strict": True,
            "schema": VIBE_RESPONSE_SCHEMA,
        },
    }


# ============================================================
# SECTION: UI Helpers — CSS Glow Classes & Emojis
# ============================================================

# Vibe → CSS class suffix for the glowing card container
_VIBE_CSS_CLASSES: dict[str, str] = {
    "Panic":    "panic-glow",
    "Hype":     "hype-glow",
    "Disgust":  "disgust-glow",
    "Victory":  "victory-glow",
    "Sweating": "sweating-glow",
}

# Vibe → emoji for UI badges
_VIBE_EMOJIS: dict[str, str] = {
    "Panic":    "🚨",
    "Hype":     "🔥",
    "Disgust":  "🤢",
    "Victory":  "🏆",
    "Sweating": "😰",
}


def generate_vibe_css_class(vibe_status: str) -> str:
    """
    Map a ``vibe_status`` value to its CSS class suffix for the
    Streamlit container glow.

    Parameters
    ----------
    vibe_status : str
        One of ``VALID_VIBE_STATUSES``.

    Returns
    -------
    str — CSS class name like ``"panic-glow"``, ``"victory-glow"``, etc.
    """
    return _VIBE_CSS_CLASSES.get(vibe_status, "sweating-glow")


def get_vibe_emoji(vibe_status: str) -> str:
    """
    Return the emoji associated with a ``vibe_status``.

    Parameters
    ----------
    vibe_status : str
        One of ``VALID_VIBE_STATUSES``.

    Returns
    -------
    str — Single emoji character/string.
    """
    return _VIBE_EMOJIS.get(vibe_status, "😰")
