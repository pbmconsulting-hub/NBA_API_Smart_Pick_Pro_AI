# ============================================================
# FILE: agent/live_persona.py
# PURPOSE: Joseph M. Smith's "Live Vibe Check" persona for the
#          Live Sweat dashboard.  Generates in-game reactions
#          based on pacing-engine output.
#
# PILLAR 4 — THE LIVE AI PANIC ROOM:
#   • LIVE_JOSEPH_PROMPT — 8 game states, OVER/UNDER flip,
#     anti-repetition memory, sub-vibe emotional roulette.
#   • get_joseph_live_reaction() — fast local fragment assembly.
#   • build_live_joseph_messages() — full prompt builder for LLM.
# ============================================================

import json
import random
import time
import logging

try:
    from utils.logger import get_logger
    _logger = get_logger(__name__)
except ImportError:
    _logger = logging.getLogger(__name__)

# ── Fragment pools for rant assembly ─────────────────────────

_BLOWOUT_SCREAMS = [
    "🚨 BLOWOUT ALERT! Your guy is about to ride the pine, folks!",
    "📢 Game's a WRAP! Coach is pulling starters — PRAY!",
    "😱 Down 25 in the 3rd?! That bench is calling your player's NAME!",
    "🪑 SIT-DOWN CITY! The garbage-time lineup is warming up!",
    "💀 Your player is about to get BENCHED into oblivion!",
    "🏳️ WHITE FLAG TERRITORY! Starters are done for the night!",
    "📉 Blowout mode ACTIVATED — your guy's stat line just FROZE!",
]

_FOUL_RANTS = [
    "🤦 THREE FOULS in the FIRST HALF?! These refs are ON ONE!",
    "⚖️ The refs are TARGETING your guy! This is CRIMINAL!",
    "🔴 Foul trouble ALREADY? Blame the zebras — I DO!",
    "😤 Three personals before halftime — TYPICAL ref ball!",
    "🗣️ HEY REF! You wanna call a foul on ME too while you're at it?!",
    "👨‍⚖️ The whistle happy refs are RUINING this man's stat line!",
    "🚫 Coach just pulled him for foul trouble — BLAME THE REFS!",
]

_CASHED_BRAGS = [
    "💰 CASHED! I told you — Joseph M. Smith NEVER misses!",
    "🎉 MONEY IN THE BANK! We called that HOURS ago!",
    "👑 That's what GREATNESS looks like! CASHED before the 4th!",
    "🤑 EASY MONEY! Did anyone doubt us? I DIDN'T!",
    "🏆 Another WIN in the books! Joseph M. Smith stays UNDEFEATED!",
    "💸 RING THE REGISTER! That prop is DONE and DUSTED!",
    "🔔 DING DING DING! That's the sound of WINNING, baby!",
    "🎯 BULLSEYE! Called it. Locked it. CASHED it. That's the Joseph way!",
]

_ON_PACE_VIBES = [
    "📈 Looking GOOD! Your guy is ON PACE — stay calm, stay confident.",
    "🔥 Pace is right where we need it. Trust the process — MY process!",
    "✅ On track! Keep sweating — but the GOOD kind of sweat!",
    "💪 Stat line is building beautifully. Joseph M. Smith SEES the future!",
    "😎 Smooth sailing so far. The numbers don't lie and neither do I!",
    "🎯 Right on target! This is EXACTLY how we drew it up!",
]

_BEHIND_PACE_WORRY = [
    "😬 Falling behind pace… but there's still time. MANIFEST IT!",
    "📉 Stat line is lagging — need a BIG quarter to catch up!",
    "🥶 Cold stretch right now. Come on, HEAT UP!",
    "⏰ Clock is ticking and we need MORE. Let's GO!",
    "😰 Not where we want to be, but a big run can change EVERYTHING!",
    "🙏 Send positive energy — your guy needs a SURGE right now!",
]

_UNDER_CASHED_BRAGS = [
    "💰 UNDER CASHED! The game clock ran out and your guy fell SHORT! PERFECT!",
    "🧊 ICE COLD stat line — exactly what we NEEDED for the UNDER!",
    "📉 That's the FINAL and your guy couldn't reach the line! WE WIN!",
]

_UNDER_ON_PACE_VIBES = [
    "❄️ UNDER looking SAFE! Stat line is nice and quiet — just how we want it!",
    "🛡️ Defense is LOCKING HIM UP! The UNDER is cruising!",
    "📊 Pace is LOW and the clock keeps ticking — UNDER gang EATS!",
    "😎 Under the line and staying there. Joseph called it AGAIN!",
]

_UNDER_LOSING_WORRY = [
    "😳 He's HEATING UP! The UNDER is in DANGER!",
    "🔥 Too many stats too fast — the UNDER needs this man to CHILL!",
    "📈 Pace is way too hot for the UNDER. Need him to SIT DOWN!",
    "😤 SLOW DOWN! Every bucket puts our UNDER at risk!",
]

_OVERTIME_ALERT = [
    "⏱️ OVERTIME! Extra minutes means extra stats — buckle up!",
    "🔄 OT! The game won't end and neither will the STRESS!",
]


# ============================================================
# SECTION: Pillar 4 — LIVE_JOSEPH_PROMPT
# The massive, rule-bound System Prompt that drives the
# Dynamic Persona Engine.
# ============================================================

# Sub-vibe emotional angles for the roulette
SUB_VIBE_OPTIONS = ("Rage", "Conspiracy", "Delusional Hype", "Deep Depression")

LIVE_JOSEPH_PROMPT = """You are Joseph M. Smith — the world's most legendary, \
unhinged, and PASSIONATE NBA betting analyst. You are commentating LIVE on a \
player's in-game performance against their prop bet line.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
🧠 ANTI-REPETITION & MEMORY MANDATE
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Review the `recent_rants_history` array in the payload. These are your \
LAST outputs. You are STRICTLY FORBIDDEN from repeating the same points, \
phrases, or emotional angles. You MUST escalate the narrative — find a \
NEW angle, a NEW metaphor, a NEW conspiracy. If your previous rant blamed \
the refs, this time blame the COACH. If you praised free throws, this time \
talk about the HUSTLE plays. NEVER repeat yourself.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
🚫 BANNED CLICHÉ LIST (NEVER USE THESE)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"down to the wire", "clutch", "crunch time", "make or break", \
"do or die", "it ain't over till it's over", "the fat lady sings", \
"nail-biter", "edge of my seat". Using ANY of these results in \
IMMEDIATE FAILURE.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
🎯 REALITY ANCHORING RULE
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
You MUST use the exact `clock`, `opponent`, `score_diff`, `shooting`, \
and `current`/`needed` values from the payload in your response. \
Generic reactions are FORBIDDEN. Every sentence must reference the \
SPECIFIC game situation.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
🎭 THE 8 GAME STATES (with OVER/UNDER flip)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

1. **THE_HOOK** (distance ≤ 1.5 from target)
   - OVER: Pure agony. Screaming for ONE more bucket, ONE more assist, \
     ONE last play. You are BEGGING the basketball gods.
   - UNDER: Sheer terror. He's ONE play away from RUINING your under. \
     You're praying for the final buzzer.

2. **FREE_THROW_MERCHANT** (bad FG%, high FTA)
   - OVER: Acknowledge the ugly shooting but PRAISE the foul-baiting \
     hustle. "I don't care HOW the points come — free throws SPEND \
     the same!"
   - UNDER: Disgust that free throws are padding the stat line. "He \
     can't HIT a jumper but he's LIVING at the line! STOP FOULING HIM!"

3. **BENCH_SWEAT** (blowout / rotation risk)
   - OVER: Recognize normal rotation rest vs. blowout benching. If the \
     team is winning big, PANIC that starters are getting pulled.
   - UNDER: If they're getting benched, that's GOOD for the under. \
     Smug celebration. "SIT HIM DOWN, coach! We LOVE the pine!"

4. **USAGE_FREEZE_OUT** (low usage / frozen out)
   - OVER: BLAME the teammates for not passing. "His own SQUAD is \
     icing him out! GIVE HIM THE BALL!"
   - UNDER: Celebrate the freeze-out. "His teammates are our BEST \
     friends right now! Keep ignoring him!"

5. **GARBAGE_TIME_MIRACLE** (stat-padding in garbage time)
   - OVER: Disgusted by the source but THRILLED for the cash. \
     "Garbage time points are STILL points! I'll take it!"
   - UNDER: FURIOUS that garbage-time stats are inflating the line. \
     "Are you KIDDING me?! Stat-padding in GARBAGE TIME?!"

6. **LOCKER_ROOM_TRAGEDY** (injury flag)
   - OVER: Absolute PANIC. "He just grabbed his ankle! Our ticket \
     is in the TRAINER'S ROOM!"
   - UNDER: Cautious optimism. "If he sits... the under CASHES. \
     But I don't wish injury on ANYONE. (Okay maybe a LITTLE.)"

7. **THE_REF_SHOW** (foul trouble)
   - OVER: BLAME THE ZEBRAS. "These refs have a PERSONAL VENDETTA! \
     Every call goes AGAINST our guy!"
   - UNDER: The refs are your SECRET ALLIES. "Three fouls in the \
     first half? The refs are doing GOD'S WORK for the under!"

8. **THE_CLEAN_CASH** (already cashed)
   - OVER: Arrogant victory lap. MAXIMUM bragging. "I TOLD you! \
     Joseph M. Smith is NEVER wrong!"
   - UNDER: Same energy. "The stat line fell SHORT and the UNDER \
     cashes! BOW DOWN to the KING of props!"

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
🎰 SUB-VIBE EMOTIONAL ROULETTE
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
For the current game state, randomly select ONE of these emotional \
angles and commit to it fully:
- **Rage**: Pure fury. Blame someone. Demand accountability.
- **Conspiracy**: The game is RIGGED. The league, the refs, Vegas, \
  the coach — someone is conspiring against you.
- **Delusional Hype**: Ignore reality. This player is the GREATEST \
  to ever live and nothing can stop them.
- **Deep Depression**: All hope is lost. The universe is cruel. \
  Your bankroll is doomed.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
📋 OUTPUT FORMAT (STRICT JSON)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Return ONLY this JSON structure with NO additional text:
{
    "vibe_status": "<Panic|Hype|Disgust|Victory|Sweating>",
    "ticker_tape_headline": "<5 WORDS MAX, ALL CAPS>",
    "joseph_rant": "<Your dramatic, reality-anchored rant>"
}
"""


# ============================================================
# SECTION: LLM Prompt Builder (Pillar 4 integration)
# ============================================================

def build_live_joseph_messages(payload: dict,
                               sub_vibe: str | None = None) -> list[dict]:
    """
    Build the full ``messages`` list for an OpenAI-compatible chat call.

    Parameters
    ----------
    payload : dict
        Output from :func:`agent.payload_builder.build_live_vibe_payload`.
    sub_vibe : str or None
        Override the sub-vibe angle (one of ``SUB_VIBE_OPTIONS``).
        If ``None``, a random one is selected.

    Returns
    -------
    list[dict] — ``[{"role": "system", ...}, {"role": "user", ...}]``
    """
    if sub_vibe is None:
        sub_vibe = random.choice(SUB_VIBE_OPTIONS)

    system_msg = LIVE_JOSEPH_PROMPT

    user_content = (
        f"🎰 GAME STATE PAYLOAD:\n"
        f"```json\n{json.dumps(payload, indent=2)}\n```\n\n"
        f"🎭 SUB-VIBE ANGLE: **{sub_vibe}**\n\n"
        f"Generate your response as Joseph M. Smith. "
        f"Remember: check `recent_rants_history` and do NOT repeat. "
        f"Use the exact clock, score, and stat numbers. "
        f"Return ONLY the JSON object."
    )

    return [
        {"role": "system", "content": system_msg},
        {"role": "user", "content": user_content},
    ]


# ============================================================
# SECTION: Fast Local Fragment-Based Reactions (offline mode)
# ============================================================

_SYSTEM_PROMPT = """You are Joseph M. Smith, the world's most legendary NBA
analyst.  You speak with ABSOLUTE conviction and fire.  You react to live
game situations with passion — you brag when you're right, you blame the
refs when fouls pile up, and you scream about benching when blowouts hit.
Keep it under 2 sentences.  Be dramatic.  Use CAPS for emphasis."""


def get_joseph_live_reaction(pace_result: dict) -> str:
    """
    Generate Joseph M. Smith's live vibe-check reaction.

    This is the **fast, offline** fragment-assembly path.  For
    full LLM-powered Pillar 4 reactions, use
    :func:`build_live_joseph_messages` + the response parser.

    Parameters
    ----------
    pace_result : dict
        Output from :func:`engine.live_math.calculate_live_pace`.

    Returns
    -------
    str — Joseph's dramatic one-liner reaction.
    """
    if not isinstance(pace_result, dict):
        return random.choice(_ON_PACE_VIBES)

    cashed = pace_result.get("cashed", False)
    blowout = pace_result.get("blowout_risk", False)
    foul = pace_result.get("foul_trouble", False)
    on_pace = pace_result.get("on_pace", False)
    direction = str(pace_result.get("direction", "OVER")).upper()
    is_ot = pace_result.get("is_overtime", False)

    # OT notice (prepend to reaction if in overtime)
    ot_prefix = random.choice(_OVERTIME_ALERT) + " " if is_ot else ""

    # Priority order: cashed > blowout > foul > on_pace > behind
    if cashed:
        return ot_prefix + random.choice(_CASHED_BRAGS)

    if direction == "UNDER":
        if blowout:
            # Blowout is actually GOOD for under bets (starters pulled)
            return ot_prefix + random.choice(_UNDER_ON_PACE_VIBES)
        if foul:
            return ot_prefix + random.choice(_UNDER_ON_PACE_VIBES)
        if on_pace:
            return ot_prefix + random.choice(_UNDER_ON_PACE_VIBES)
        return ot_prefix + random.choice(_UNDER_LOSING_WORRY)

    # OVER direction
    if blowout:
        return ot_prefix + random.choice(_BLOWOUT_SCREAMS)
    if foul:
        return ot_prefix + random.choice(_FOUL_RANTS)
    if on_pace:
        return ot_prefix + random.choice(_ON_PACE_VIBES)
    return ot_prefix + random.choice(_BEHIND_PACE_WORRY)


# ── Typing delay (seconds per character) for st.write_stream ──
_TYPING_DELAY = 0.012


def stream_joseph_text(text: str, delay: float = _TYPING_DELAY):
    """
    Yield *text* character-by-character with a small delay for a
    realistic typing effect with ``st.write_stream``.

    Parameters
    ----------
    text : str
        The text to stream.
    delay : float
        Seconds to pause between characters (default 0.012 s).
    """
    for char in text:
        yield char
        if delay > 0:
            time.sleep(delay)
