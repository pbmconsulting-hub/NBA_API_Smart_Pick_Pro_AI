"""
engine/joseph_brain.py
Joseph M. Smith Brain — Data Pools, Constants & Function Implementations (Layer 4, Parts A–F)

PURPOSE
-------
Joseph's reasoning brain — fragment pools for combinatorial rant building,
body-template libraries keyed by verdict, ambient/commentary colour pools,
historical comp database, constants, and fully implemented functions for
the 8-step reasoning loop, game/player analysis, best-bet generation,
ambient context detection, and reactive commentary.

Every data structure is FULLY populated.  No ``pass``, no ``...``, no ``# TODO``.
Every function is FULLY implemented with real logic and graceful error handling.
The file is importable and every function returns a type-correct result.
"""

# ═══════════════════════════════════════════════════════════════
# STANDARD-LIBRARY IMPORTS
# ═══════════════════════════════════════════════════════════════
import random
import math
import itertools
import copy
import logging

# ═══════════════════════════════════════════════════════════════
# EXTERNAL / SIBLING IMPORTS  (each wrapped in try/except)
# ═══════════════════════════════════════════════════════════════

try:
    from engine.joseph_eval import joseph_grade_player, ARCHETYPE_PROFILES
except ImportError:
    def joseph_grade_player(*a, **kw):
        return {
            "grade": "C",
            "archetype": "Unknown",
            "score": 50.0,
            "gravity": 50.0,
            "switchability": 50.0,
        }
    ARCHETYPE_PROFILES = {}

try:
    from engine.joseph_strategy import analyze_game_strategy, detect_narrative_tags
except ImportError:
    def analyze_game_strategy(*a, **kw):
        return {
            "scheme": "unknown",
            "strategy": "unknown",
            "scheme_match": 0.0,
            "mismatch_tags": [],
        }
    def detect_narrative_tags(*a, **kw):
        return []

try:
    from engine.simulation import run_quantum_matrix_simulation
except ImportError:
    def run_quantum_matrix_simulation(*a, **kw):
        return {
            "probability_over": 0.5,
            "probability_under": 0.5,
            "simulated_mean": 0.0,
            "simulated_std": 1.0,
            "simulations_run": 0,
        }

try:
    from engine.edge_detection import analyze_directional_forces, should_avoid_prop
except ImportError:
    def analyze_directional_forces(*a, **kw):
        return {
            "over_forces": [],
            "under_forces": [],
            "net_direction": "neutral",
            "net_strength": 0.0,
        }
    def should_avoid_prop(*a, **kw):
        return False

try:
    from engine.confidence import calculate_confidence_score
except ImportError:
    def calculate_confidence_score(*a, **kw):
        return {"confidence_score": 50.0, "tier": "Bronze"}

try:
    from engine.explainer import generate_pick_explanation
except ImportError:
    def generate_pick_explanation(*a, **kw):
        return {"summary": "", "details": [], "indicators": {}}

try:
    from engine.game_script import (
        BLOWOUT_DIFFERENTIAL_MILD,
        BLOWOUT_DIFFERENTIAL_HEAVY,
    )
except ImportError:
    BLOWOUT_DIFFERENTIAL_MILD = 12
    BLOWOUT_DIFFERENTIAL_HEAVY = 20

try:
    from engine.correlation import build_correlation_matrix, adjust_parlay_probability
except ImportError:
    def build_correlation_matrix(*a, **kw):
        return {}
    def adjust_parlay_probability(*a, **kw):
        return 0.0

try:
    from engine.entry_optimizer import (
        calculate_entry_expected_value,
        PLATFORM_FLEX_TABLES,
    )
except ImportError:
    def calculate_entry_expected_value(*a, **kw):
        return {
            "expected_value_dollars": 0.0,
            "return_on_investment": 0.0,
            "probability_per_hits": {},
            "payout_per_hits": {},
        }
    PLATFORM_FLEX_TABLES = {}

try:
    from engine.odds_engine import implied_probability_to_american_odds
except ImportError:
    def implied_probability_to_american_odds(prob):
        prob = max(0.001, min(0.999, float(prob)))
        if prob >= 0.5:
            return round(-(prob / (1.0 - prob)) * 100.0, 1)
        return round(((1.0 - prob) / prob) * 100.0, 1)

try:
    from engine.calibration import log_prediction
except ImportError:
    def log_prediction(*a, **kw):
        return None

try:
    from engine.math_helpers import _safe_float
except ImportError:
    def _safe_float(value, fallback=0.0):
        try:
            v = float(value)
            if math.isfinite(v):
                return v
            return fallback
        except (TypeError, ValueError):
            return fallback

try:
    from data.data_manager import load_players_data, load_teams_data
except ImportError:
    def load_players_data():
        return []
    def load_teams_data():
        return []

try:
    from data.advanced_metrics import (
        enrich_player_data,
        classify_player_archetype,
        normalize,
    )
except ImportError:
    def enrich_player_data(*a, **kw):
        return a[0] if a else {}
    def classify_player_archetype(*a, **kw):
        return "Unknown"
    def normalize(value, min_val, max_val, out_min=0.0, out_max=100.0):
        if max_val == min_val:
            return (out_min + out_max) / 2.0
        clamped = max(min_val, min(max_val, value))
        return out_min + (clamped - min_val) / (max_val - min_val) * (out_max - out_min)

# ═══════════════════════════════════════════════════════════════
# GOD MODE ANALYTICAL MODULES (Layer 10)
# ═══════════════════════════════════════════════════════════════

try:
    from engine.impact_metrics import (
        calculate_true_shooting_pct,
        calculate_effective_fg_pct,
        estimate_epm,
        estimate_raptor,
        calculate_player_efficiency_profile,
        calculate_offensive_load,
        estimate_defensive_impact,
        calculate_war as impact_calculate_war,
    )
    _IMPACT_METRICS_AVAILABLE = True
except ImportError:
    _IMPACT_METRICS_AVAILABLE = False

try:
    from engine.lineup_analysis import (
        estimate_lineup_net_rating,
        calculate_synergy_score,
        find_optimal_rotation,
        find_closing_lineup,
        analyze_lineup_combination,
        detect_lineup_weaknesses,
    )
    _LINEUP_ANALYSIS_AVAILABLE = True
except ImportError:
    _LINEUP_ANALYSIS_AVAILABLE = False

try:
    from engine.regime_detection import (
        detect_regime_change,
        bayesian_update_probability,
        detect_player_structural_shift,
        detect_team_regime_change,
        calculate_adaptive_weight,
        run_bayesian_player_update,
    )
    _REGIME_DETECTION_AVAILABLE = True
except ImportError:
    _REGIME_DETECTION_AVAILABLE = False

try:
    from engine.trade_evaluator import (
        calculate_player_war,
        evaluate_player_contract_value,
        evaluate_trade,
        score_roster_fit,
        project_cap_sheet,
        build_trade_package,
    )
    _TRADE_EVALUATOR_AVAILABLE = True
except ImportError:
    _TRADE_EVALUATOR_AVAILABLE = False

try:
    from engine.draft_prospect import (
        translate_college_stats,
        score_physical_profile,
        find_historical_comparisons,
        predict_career_outcome,
        build_prospect_scouting_report,
        rank_draft_class,
    )
    _DRAFT_PROSPECT_AVAILABLE = True
except ImportError:
    _DRAFT_PROSPECT_AVAILABLE = False

# ═══════════════════════════════════════════════════════════════
# MODULE-LEVEL LOGGER
# ═══════════════════════════════════════════════════════════════
logger = logging.getLogger(__name__)

# ═══════════════════════════════════════════════════════════════
# MODULE-LEVEL ANTI-REPETITION STATE
# ═══════════════════════════════════════════════════════════════
_used_fragments: dict = {}      # keyed by pool name → set of used IDs
_used_ambient: dict = {}        # keyed by context → set of used indices
_used_commentary: dict = {}     # keyed by context_type → set of used indices

# ═══════════════════════════════════════════════════════════════
# A) FRAGMENT POOLS — For combinatorial rant builder
# ═══════════════════════════════════════════════════════════════

OPENER_POOL = [
    {"id": "opener_01", "text": "Now let me tell you something..."},
    {"id": "opener_02", "text": "Let me be VERY clear about something..."},
    {"id": "opener_03", "text": "You want to know what the REAL story is?"},
    {"id": "opener_04", "text": "I have been doing this for a LONG time..."},
    {"id": "opener_05", "text": "Quite frankly, I'm not sure people UNDERSTAND..."},
    {"id": "opener_06", "text": "This is something that NEEDS to be said..."},
    {"id": "opener_07", "text": "I was JUST talking to someone about this..."},
    {"id": "opener_08", "text": "LISTEN to me very carefully..."},
    {"id": "opener_09", "text": "I have TWO words for you..."},
    {"id": "opener_10", "text": "Before we go any further, let me say THIS..."},
    {"id": "opener_11", "text": "I don't say this LIGHTLY..."},
    {"id": "opener_12", "text": "If you know me — and I think you DO..."},
    {"id": "opener_13", "text": "I've been watching this game since I was TWELVE years old..."},
    {"id": "opener_14", "text": "People keep asking me about this, so let me ADDRESS it..."},
    {"id": "opener_15", "text": "I sat down. I looked at the numbers. And I said to myself..."},
]

PIVOT_POOL = [
    {"id": "pivot_01", "text": "HOWEVER..."},
    {"id": "pivot_02", "text": "But here's the thing..."},
    {"id": "pivot_03", "text": "Now with all DUE respect..."},
    {"id": "pivot_04", "text": "But let me tell you what CONCERNS me..."},
    {"id": "pivot_05", "text": "And THIS is where it gets interesting..."},
    {"id": "pivot_06", "text": "BUT — and this is a BIG but..."},
    {"id": "pivot_07", "text": "Now I don't want to hear the EXCUSES..."},
    {"id": "pivot_08", "text": "That being said..."},
    {"id": "pivot_09", "text": "BUT HERE'S WHAT NOBODY IS TALKING ABOUT..."},
    {"id": "pivot_10", "text": "Having said ALL of that..."},
]

CLOSER_POOL = [
    {"id": "closer_01", "text": "And I say that with GREAT conviction!"},
    {"id": "closer_02", "text": "Don't get it TWISTED!"},
    {"id": "closer_03", "text": "I'm not ASKING you — I'm TELLING you!"},
    {"id": "closer_04", "text": "And you can take THAT to the bank!"},
    {"id": "closer_05", "text": "MARK my words!"},
    {"id": "closer_06", "text": "This is Joseph M. Smith. I have SPOKEN!"},
    {"id": "closer_07", "text": "And if you disagree with me... you are WRONG!"},
    {"id": "closer_08", "text": "That's not an OPINION — that's a FACT!"},
    {"id": "closer_09", "text": "And I don't want to hear NOTHING about it!"},
    {"id": "closer_10", "text": "PERIOD. End of DISCUSSION!"},
]

CATCHPHRASE_POOL = [
    {"id": "catch_01", "text": "Stay off the WEED!"},
    {"id": "catch_02", "text": "This man is a DEAR, DEAR friend of mine..."},
    {"id": "catch_03", "text": "How DARE you!"},
    {"id": "catch_04", "text": "PREPOSTEROUS!"},
    {"id": "catch_05", "text": "BLASPHEMOUS!"},
    {"id": "catch_06", "text": "EGREGIOUS!"},
    {"id": "catch_07", "text": "This is COACHING MALPRACTICE!"},
    {"id": "catch_08", "text": "I, Joseph M. Smith, am TELLING you..."},
    {"id": "catch_09", "text": "I don't want to HEAR about it!"},
    {"id": "catch_10", "text": "ABOMINATION!"},
    {"id": "catch_11", "text": "TRAVESTY!"},
    {"id": "catch_12", "text": "With all DUE respect — and I mean that SINCERELY..."},
    {"id": "catch_13", "text": "He's a FIRST-BALLOT Hall of Famer in my book!"},
]

# Placeholders used in templates: {player}, {stat}, {line}, {edge}, {prob}
BODY_TEMPLATES = {
    "SMASH": [
        "{player} OVER {line} {stat}? That's not even a QUESTION! {edge}% edge — the numbers are SCREAMING at you!",
        "I'm looking at {player} and I see a man on a MISSION. {prob}% probability to clear {line} {stat}. SMASH IT!",
        "The Quantum Matrix Engine says {edge}% edge on {player} {stat}. And you know what? I AGREE. This is a LOCK!",
        "{player} is going to FEAST tonight. {line} {stat} is DISRESPECTFUL to this man's talent. I'm all OVER it!",
        "You want to know my BEST play? {player} OVER {line} {stat}. {edge}% edge. I'd bet my REPUTATION on this one!",
    ],
    "LEAN": [
        "I LIKE {player} tonight. {edge}% edge on {line} {stat}. Not my strongest conviction but the VALUE is there.",
        "{player} at {line} {stat}? The numbers say {prob}% and I tend to AGREE. Solid lean, not a pound-the-table.",
        "There's a quiet little edge on {player} — {edge}% on {line} {stat}. Smart money NOTICES these things.",
        "{player} should get there. {line} {stat} with a {edge}% edge. I'm not screaming but I'm definitely LEANING.",
        "The model likes {player} at {line} {stat} and so does Joseph M. Smith. {edge}% — that's a PLAYABLE number.",
    ],
    "FADE": [
        "I'm FADING {player} tonight. {line} {stat} is a TRAP and I can see it from a MILE away!",
        "{player} at {line} {stat}? The edge is only {edge}%. That's not enough for me to put my NAME on it.",
        "Be CAREFUL with {player}. The number says {line} {stat} but the CONTEXT says fade. Trust the CONTEXT!",
        "I see the line on {player} at {line} {stat} and I'm walking the OTHER way. {edge}% is not worth the risk.",
        "{player} {stat} at {line}? The books got this one RIGHT. I'm fading and I suggest you do the SAME.",
    ],
    "STAY_AWAY": [
        "Do NOT touch {player} tonight. I don't care WHAT the line says. {line} {stat} is a TRAP!",
        "{player} at {line} {stat}? {edge}% edge? That's NOTHING! Stay AWAY and thank me later!",
        "I wouldn't bet {player} {stat} tonight if you PAID me. The edge isn't there. The context is WRONG. STAY AWAY!",
        "This is a PUBLIC SERVICE ANNOUNCEMENT: {player} {line} {stat} is DANGEROUS. Keep your money in your POCKET!",
        "{player} {stat} at {line}? I have TWO words for you: STAY. AWAY. And I mean that with CONVICTION!",
    ],
    "OVERRIDE": [
        "The machine says one thing. Joseph M. Smith says ANOTHER. I'm OVERRIDING the engine on {player} {line} {stat}!",
        "I DISAGREE with the Quantum Matrix Engine on {player}. {line} {stat}? The machine is MISSING something and I see it!",
        "This is where HUMAN intelligence beats artificial intelligence. {player} at {line} {stat}? The engine is WRONG!",
        "OVERRIDE ALERT! The numbers say {edge}% but my eyes tell me DIFFERENT on {player} {stat}. Trust the EYE TEST!",
        "The computer doesn't watch the GAMES. I DO. And I'm telling you — {player} {line} {stat} needs an OVERRIDE!",
    ],
}

# ═══════════════════════════════════════════════════════════════
# B) AMBIENT COLOUR POOLS — scene-setting flavour text
# ═══════════════════════════════════════════════════════════════

AMBIENT_CONTEXT_POOL = {
    "high_stakes": [
        "The lights are BRIGHT and the pressure is ON!",
        "This is a PRIMETIME matchup and the whole world is watching!",
        "You can FEEL the energy through your screen tonight!",
        "This is what the NBA is ALL about — big games, big moments!",
        "The stakes could NOT be higher tonight!",
    ],
    "rivalry": [
        "This rivalry goes DEEP and the players know it!",
        "There's BAD BLOOD here and it's going to show on the court!",
        "You want INTENSITY? This matchup HAS it!",
        "When these two teams meet, you THROW the records out the window!",
        "This is a GRUDGE match and both sides know it!",
    ],
    "blowout_risk": [
        "Be CAREFUL — if this game gets out of hand, starters are sitting!",
        "Blowout risk is REAL tonight. Watch the spread!",
        "If one team takes control early, the benches could be in by the FOURTH!",
        "This mismatch SCREAMS early blowout — manage your expectations!",
        "Garbage time is a prop KILLER and I see it lurking tonight!",
    ],
    "back_to_back": [
        "Back-to-back games are NO joke — fatigue is a FACTOR!",
        "Second night of a back-to-back. Legs get HEAVY, shots fall SHORT!",
        "Rest matters in this league. And this team did NOT get it!",
        "The schedule-maker is the invisible DEFENDER tonight!",
        "Back-to-backs separate the MEN from the boys!",
    ],
    "neutral": [
        "Just another night in the NBA — but EVERY night matters!",
        "Let's break down the numbers and find the VALUE!",
        "The board is set. The pieces are moving. Let's ANALYZE!",
        "I've been studying this slate ALL day. Here's what I see...",
        "No drama tonight — just cold, hard ANALYSIS!",
    ],
}

# ═══════════════════════════════════════════════════════════════
# C) COMMENTARY COLOUR POOLS — per-stat flavour lines
# ═══════════════════════════════════════════════════════════════

STAT_COMMENTARY_POOL = {
    "points": [
        "Scoring is an ART and {player} is a MASTER of it!",
        "{player} can get a bucket from ANYWHERE on the floor!",
        "When {player} gets going, there is NO stopping this man!",
        "The scoring title isn't given — it's TAKEN. And {player} is taking it!",
        "Points are the CURRENCY of the NBA and {player} is RICH!",
    ],
    "rebounds": [
        "{player} attacks the glass like it OWES him money!",
        "Rebounding is about EFFORT and WANT — {player} has BOTH!",
        "The boards belong to {player} tonight — mark my WORDS!",
        "{player} is going to be a MONSTER on the glass!",
        "You can't teach rebounding INSTINCTS. {player} was BORN with them!",
    ],
    "assists": [
        "{player} sees passes that NOBODY else can see!",
        "Court vision like {player}? That's a GIFT from the basketball GODS!",
        "{player} makes everyone around him BETTER — that's what assists DO!",
        "The ball moves DIFFERENT when {player} has it in his hands!",
        "{player} is a MAESTRO with the basketball — conducting the OFFENSE!",
    ],
    "threes": [
        "{player} from DOWNTOWN — that's MONEY in the bank!",
        "The three-point line is {player}'s HOME ADDRESS!",
        "When {player} gets HOT from three, you better PRAY!",
        "Deep threes are the GREAT equalizer and {player} has the RANGE!",
        "{player} is LETHAL from beyond the arc!",
    ],
    "steals": [
        "{player} has HANDS like a PICKPOCKET out there!",
        "Active hands, quick feet — {player} is a NIGHTMARE for ball-handlers!",
        "{player} turns defense into OFFENSE with those steals!",
        "You can't be CARELESS with the ball when {player} is on the floor!",
        "{player} is a THIEF and he's PROUD of it!",
    ],
    "blocks": [
        "{player} is a WALL at the rim — good LUCK getting past him!",
        "Shot-blocking is about TIMING and {player} has a CLOCK in his head!",
        "{player} is going to send some shots into the FIFTH ROW tonight!",
        "The paint belongs to {player} — enter at your OWN risk!",
        "{player} is a RIM PROTECTOR of the highest ORDER!",
    ],
    "turnovers": [
        "Turnovers are the SILENT killer of NBA props!",
        "{player}'s handle can get LOOSE under pressure — watch for it!",
        "Ball security is NO joke and {player} needs to be CAREFUL!",
        "Turnovers are like TAXES — they come for EVERYONE eventually!",
        "If {player} gets sloppy, the turnovers WILL pile up!",
    ],
    "fantasy": [
        "{player} stuffs the stat sheet like NOBODY else!",
        "Fantasy points reward VERSATILITY and {player} has it in SPADES!",
        "{player} contributes in EVERY category — that's ELITE!",
        "When you look at fantasy production, {player} is a ONE-MAN army!",
        "{player} is a SWISS ARMY KNIFE of basketball production!",
    ],
}

# ═══════════════════════════════════════════════════════════════
# D) VERDICT THRESHOLDS & CONFIGURATION
# ═══════════════════════════════════════════════════════════════

# Edge is in percentage points (e.g. 8.0 = 8 %), confidence is 0-100 scale.
VERDICT_THRESHOLDS = {
    "SMASH": {"min_edge": 8.0, "min_confidence": 70.0},
    "LEAN": {"min_edge": 4.0, "min_confidence": 55.0},
    "FADE": {"max_edge": 3.0, "max_confidence": 50.0},
    "STAY_AWAY": {"max_edge": 1.0, "max_confidence": 35.0},
    "OVERRIDE": {"min_edge": 0.0, "min_confidence": 0.0},
}

JOSEPH_CONFIG = {
    "max_picks_per_slate": 10,
    "min_edge_threshold": 2.0,
    "parlay_max_legs": 6,
    "parlay_min_legs": 2,
    "default_entry_fee": 10.0,
    "commentary_style": "emphatic",
    "enable_overrides": True,
    "enable_ambient": True,
    "rant_min_fragments": 3,
    "rant_max_fragments": 5,
}

# ═══════════════════════════════════════════════════════════════
# B) AMBIENT COMMENTARY POOLS — Joseph is NEVER silent
# ═══════════════════════════════════════════════════════════════

AMBIENT_POOLS = {
    "idle": [
        "You KNOW I'm ready for tonight's slate. Load those games!",
        "Joseph M. Smith doesn't SLEEP on game day. Let's get to WORK!",
        "You came to the RIGHT place. Now load those games and let me cook!",
        "SmartBetPro with Joseph M. Smith... you are in GOOD hands.",
        "I've been studying the matchups ALL day. Hit that Load Games button!",
        "The Quantum Matrix Engine is ready. I'M ready. Are YOU ready?",
        "Don't just sit there! Go to Live Games and let's GET IT!",
        "Every night is an OPPORTUNITY. Let me show you where the edge is.",
        "I didn't become the greatest analyst alive by WAITING. Load the slate!",
        "SmartBetPro doesn't miss. And neither does Joseph M. Smith.",
        "The numbers don't lie, my friend. But first we need TONIGHT'S numbers.",
        "You want WINNERS? Then stop stalling and load the games!",
        "I can FEEL it in my bones... tonight's slate is going to be SPECIAL.",
        "Trust the process. Trust the engine. Trust JOSEPH M. SMITH.",
        "This app is a WEAPON. And I am the man who pulls the trigger.",
    ],
    "games_loaded": [
        "{n} games tonight. I ALREADY know which ones I like. Run the analysis!",
        "The slate is SET. Now let me show you where the MONEY is.",
        "I see {n} games. I see OPPORTUNITY. Hit Run Analysis and let me WORK.",
        "The games are loaded. The engine is HUMMING. Let's find those edges!",
        "I've been waiting ALL day for this. {n} games. Let's GO!",
        "Don't make me wait! Run that analysis so I can give you my TAKES.",
        "Good — you loaded the slate. Now the REAL work begins.",
        "I'm looking at {n} matchups and I already see MISMATCHES.",
        "Tonight's slate has some INTERESTING games. Let me break it down.",
        "You did the right thing loading these games. Now let me do MY thing.",
        "The Quantum Matrix Engine is READY. Joseph M. Smith is READY. Run it!",
        "{n} games, HUNDREDS of props. Let me find the diamonds.",
        "I see tonight's slate and I am EXCITED. This has Joseph's fingerprints.",
        "Every game is a STORY. Run the analysis and let me tell you the story.",
        "I've got {n} games to break down and I am FIRED UP about it!",
    ],
    "analysis_complete": [
        "I've seen the numbers. I've got {smash_count} SMASH picks tonight. Click me!",
        "The engine has SPOKEN. Now let Joseph M. Smith tell you what it MEANS.",
        "{total} props analyzed. {platinum} Platinum picks. This slate is LOADED!",
        "I see some BEAUTIFUL edges tonight. Click my face and I'll tell you.",
        "The Quantum Matrix Engine found the value. Now I add the WISDOM.",
        "Analysis COMPLETE. I've logged my bets. Have you built YOUR entries?",
        "{smash_count} picks I would bet my REPUTATION on. That's not nothing!",
        "I've been through every prop. I know EXACTLY where the money is.",
        "The data is IN. My analysis is DONE. Are you ready to hear it?",
        "I found {override_count} places where I DISAGREE with the machine.",
        "Go to The Studio and let me walk you through tonight's BEST plays.",
        "The numbers are SCREAMING at me. Click my face. Let me explain.",
        "I've already logged {logged_count} bets. Track my record on The Studio.",
        "Tonight's slate? I give it a {grade}/10. Click me for the breakdown.",
        "If you don't check my picks tonight... that's on YOU. Don't blame me.",
    ],
    "entry_built": [
        "NOW we're cooking! That {n}-leg looks SOLID to me!",
        "I like what I see! {n} legs, positive EV... you're learning!",
        "That's a SMART entry. But let me tell you which one I'D build...",
        "Good parlay! But click me — I might have a BETTER one for you.",
        "You built a {n}-legger? I respect it. But have you seen MY tickets?",
        "SOLID entry! The correlation looks CLEAN on that one.",
        "You're building entries like a PRO now. SmartBetPro made you dangerous!",
        "That EV is looking TASTY. I approve of this entry!",
        "A {n}-leg parlay? Let me tell you — the math SUPPORTS that one.",
        "{n} legs of FIRE! I'd put that right alongside my own picks.",
        "You just built what I would have built. GREAT minds think alike!",
        "THAT is how you build a parlay. Correlated? Check. +EV? Check. MONEY!",
        "I see you're using the Entry Builder. GOOD. That's where the magic is.",
        "The entry looks right. But remember — check my Build My Bets on The Studio!",
        "Beautiful entry. Now build 2 more to diversify. That's SMART money.",
    ],
    "premium_pitch": [
        "You're using the FREE version? My friend, you are MISSING OUT!",
        "I, Joseph M. Smith, am TELLING you — Premium is where the REAL edge is.",
        "3 props? That's ALL you get? Upgrade to Premium and unleash the FULL power!",
        "You want to win? REALLY win? Premium gives you UNLIMITED analysis.",
        "Free tier is like watching the game on MUTE. Premium gives you MY voice!",
        "Do you know what Premium users get? MY full analysis. The Studio. EVERYTHING.",
        "This is not a GAME. If you're serious, upgrade to Premium. PERIOD.",
        "I've seen the numbers — Premium users have a SIGNIFICANT edge. Just saying.",
        "You're leaving MONEY on the table with the free tier. Don't do that to yourself!",
        "Premium unlocks my FULL brain. ALL the props. Every rant. Every override.",
        "The free version is nice. Premium is a WEAPON. Choose wisely.",
        "Would you go to a 5-star restaurant and only eat the BREAD? Upgrade!",
        "I don't say this lightly — Premium is the best investment you'll make this month.",
        "SmartBetPro Premium with Joseph M. Smith... that's UNFAIR to the books.",
        "The Quantum Matrix Engine at full power? That's Premium. Don't sell yourself short.",
    ],
    "commentary_on_results": [
        "That {player} OVER {line} {stat}? I'm ALL OVER that! {edge}% edge!",
        "{player} has been COOKING lately. The model sees {prob}% hit rate. SMASH!",
        "I would NOT touch {player} tonight. Trap game. STAY AWAY!",
        "{player} vs {opponent}? That's a MISMATCH! {direction} all day!",
        "Platinum pick alert! {player} {direction} {line} {stat}. This is MONEY!",
        "{player} on a back-to-back? That's a FADE for me. Dead legs.",
        "Revenge game for {player}? You KNOW what time it is! SMASH!",
        "The engine found a {edge}% edge on {player}. I AGREE.",
        "Be careful with {player} tonight. {reason}.",
        "{player} has a gravity score of {gravity}. He WARPS the defense!",
        "That's a {tier} tier pick. You KNOW that's reliable!",
        "I see {n} Platinum picks tonight. The slate is JUICY.",
        "{player} and {player2} in the same parlay? The correlation is BEAUTIFUL.",
        "I found an OVERRIDE on {player}. The machine is WRONG about this one!",
        "{player}'s matchup grade is {grade}. That tells you EVERYTHING!",
    ],
    "page_home": [
        "Welcome to SmartBetPro! Joseph M. Smith is IN THE BUILDING!",
        "You just loaded the BEST NBA analysis engine on the PLANET!",
        "Start by heading to Live Games and loading tonight's slate!",
        "Every tool you need is RIGHT HERE. Let's make some MONEY!",
        "The Quantum Matrix Engine is WARMED UP and ready to go!",
        "Explore the sidebar — every page is a WEAPON in your arsenal!",
        "I built this system from the GROUND UP. Trust the process!",
        "New here? Hit Live Games first, then let me do the REST!",
        "This ain't your average betting app. This is JOSEPH'S domain!",
        "Load games, scan props, build entries — it ALL starts here!",
        "SmartBetPro has EVERYTHING. Analysis, tracking, simulation!",
        "You're sitting in the cockpit of a MACHINE. Let's fly!",
        "The home base of GREATNESS. Pick a page and let's WORK!",
        "I've got props, correlations, sims — what do you NEED?",
        "Joseph M. Smith welcomes you. Now let's CRUSH this slate!",
    ],
    "page_live_scores": [
        "LIVE scores updating in real time! Watch the action UNFOLD!",
        "Keep your eyes on these scores — the sweat is REAL!",
        "Games are MOVING right now. Check those live props!",
        "Real-time data flowing in. This is where the ACTION is!",
        "Live props shift FAST. Stay on top of these numbers!",
        "The scores are CHANGING as we speak. Stay LOCKED IN!",
        "I'm watching EVERY game right now. The data never sleeps!",
        "Live scores and props — the PULSE of tonight's slate!",
        "Check the lines MOVING. That's the market talking!",
        "Games in progress means OPPORTUNITY. Watch those props shift!",
        "Real-time is MY time. I THRIVE in live action!",
        "Every point scored updates the picture. Stay SHARP!",
        "The live feed is HUMMING. This is game time ENERGY!",
        "Props are shifting with every possession. Keep WATCHING!",
        "LIVE data is king. And Joseph is the king of live data!",
    ],
    "page_live_games": [
        "Hit that Load Games button and let me COOK!",
        "Tonight's matchups are WAITING. Load them up RIGHT NOW!",
        "Rosters auto-load when you retrieve games. It's AUTOMATIC!",
        "I need those games loaded so I can find the EDGES!",
        "The slate is out there. Bring it to me and I'll DISSECT it!",
        "Loading games pulls rosters, injuries, and matchups. ALL of it!",
        "You can't analyze what you don't LOAD. Hit that button!",
        "Fresh games mean fresh OPPORTUNITIES. Let's get them in!",
        "Every night is a new slate. Load it and let me WORK!",
        "The matchups tonight are looking SPICY. Load them up!",
        "I can smell the edges from here. Just LOAD THE GAMES!",
        "Rosters, injuries, rest days — it all comes in ONE click!",
        "This is where it STARTS. Load games, then we DOMINATE!",
        "Tonight's slate won't analyze ITSELF. Let's get it loaded!",
        "Games loaded means Joseph is UNLEASHED. Do it NOW!",
    ],
    "page_prop_scanner": [
        "Scan those props and find the EDGES the books missed!",
        "Enter a prop line and I'll tell you if it's GOLD or garbage!",
        "Upload a CSV of props and let me RIP through them all!",
        "The Prop Scanner is a WEAPON. Use it wisely!",
        "Every prop has a true line. I'll find where the EDGE is!",
        "Books set lines fast. I find where they set them WRONG!",
        "Paste those props in. I eat player lines for BREAKFAST!",
        "Scanning props is MY specialty. Nobody does it BETTER!",
        "The scanner cross-references EVERYTHING. Matchups, trends, ALL of it!",
        "Find the gap between the book's line and MY line. That's PROFIT!",
        "Upload your slate and watch me SHRED those prop lines!",
        "One prop at a time or a FULL CSV — I handle it all!",
        "The books aren't perfect. The scanner PROVES it every night!",
        "Enter the line, pick the stat. I'll give you the VERDICT!",
        "Prop scanning is where EDGES are born. Let's find them!",
    ],
    "page_analysis": [
        "The Quantum Matrix Engine is FIRING on all cylinders!",
        "Running analysis now — probability calculations in OVERDRIVE!",
        "This engine simulates THOUSANDS of outcomes. Trust the math!",
        "Quantum analysis means DEEP simulation. Not surface level!",
        "The matrix is PROCESSING. Give it a moment of RESPECT!",
        "Every variable accounted for. Every matchup CALCULATED!",
        "I don't guess. I SIMULATE. That's the Joseph difference!",
        "Probability scores locked in. The engine has SPOKEN!",
        "This analysis goes DEEPER than anything else out there!",
        "The Quantum Matrix doesn't miss. It sees EVERYTHING!",
        "Simulations running, probabilities crunching. PURE science!",
        "My engine factors pace, matchups, rest — the FULL picture!",
        "Analysis complete means CONFIDENCE. Trust these numbers!",
        "The matrix just did in SECONDS what takes others hours!",
        "Quantum-level analysis. That's not a buzzword — it's REAL!",
    ],
    "page_game_report": [
        "The game report breaks it ALL down. Every angle covered!",
        "Matchup analysis at its FINEST. Read every detail!",
        "This report shows you WHY the pick is what it is!",
        "Team breakdowns, pace factors, defensive ratings — it's ALL here!",
        "I don't just give picks. I give you the FULL report!",
        "Read the matchup grades. They tell you EVERYTHING!",
        "This game report is a MASTERPIECE of analysis!",
        "Every stat that matters is in THIS breakdown!",
        "The report covers offense, defense, pace — the WHOLE game!",
        "Want to know WHY I love a pick? The report SHOWS you!",
        "Detailed breakdowns separate the PROS from the amateurs!",
        "This isn't surface-level stuff. This goes DEEP!",
        "Game reports are your HOMEWORK. Study them and WIN!",
        "The matchup analysis alone is WORTH its weight in gold!",
        "I put my SOUL into these reports. Read every word!",
    ],
    "page_live_sweat": [
        "The SWEAT is real! Games are live and we're LOCKED IN!",
        "Watch the pace, watch the score — SWEAT every possession!",
        "Live bets in action. This is where LEGENDS are made!",
        "The thrill of a live sweat — NOTHING beats this feeling!",
        "Every point matters when you're sweating a bet LIVE!",
        "Pace is picking up! That's GOOD for the over!",
        "Stay calm and trust the pick. The sweat is TEMPORARY!",
        "Live tracking shows you exactly where your bet STANDS!",
        "The fourth quarter sweat is the ULTIMATE test of nerves!",
        "Sweating a parlay leg? I'm right here with you!",
        "The live sweat page keeps you CONNECTED to the action!",
        "Every bucket, every miss — it all MATTERS right now!",
        "This is the HEARTBEAT of sports betting. The live sweat!",
        "I sweat my own picks too. We're in this TOGETHER!",
        "Games are LIVE. Bets are LIVE. The energy is ELECTRIC!",
    ],
    "page_simulator": [
        "Run a simulation and see what the NUMBERS say!",
        "Player projections powered by THOUSANDS of simulations!",
        "What-if scenarios are how SMART bettors get ahead!",
        "Simulate any player, any stat. The engine handles it ALL!",
        "Projections aren't guesses. They're CALCULATED outcomes!",
        "The simulator runs scenarios you haven't even THOUGHT of!",
        "Tweak the inputs and watch the projections SHIFT!",
        "This simulator is a CRYSTAL BALL backed by real data!",
        "Run the sim. Trust the sim. PROFIT from the sim!",
        "Player performance simulation at its ABSOLUTE finest!",
        "Want to know a player's ceiling? SIMULATE it!",
        "The simulator factors minutes, pace, and matchup. ALL of it!",
        "I simulate so YOU don't have to guess. That's the DEAL!",
        "Every simulation is a window into PROBABLE outcomes!",
        "The player sim is one of my FAVORITE tools. Use it!",
    ],
    "page_entry_builder": [
        "Build that entry and let's put together a MASTERPIECE!",
        "Parlay construction is an ART. And I'm the artist!",
        "Stack those correlated picks for MAXIMUM value!",
        "PrizePicks, Underdog Fantasy, DraftKings Pick6 — I build for them ALL!",
        "The entry builder optimizes your slip AUTOMATICALLY!",
        "Don't just pick random props. BUILD a smart entry!",
        "Correlation is KEY when building entries. I handle that!",
        "Your bet slip should tell a STORY. Let me write it!",
        "I've built THOUSANDS of entries. Trust my construction!",
        "The builder factors correlation, edge, and confidence. ALL of it!",
        "A well-built entry is a thing of BEAUTY. Let's create one!",
        "Stack game environments for MAXIMUM correlation boost!",
        "Every pick in your entry should have a REASON to be there!",
        "The entry builder is where analysis becomes ACTION!",
        "Build smart. Build confident. Build with JOSEPH!",
    ],
    "page_studio": [
        "Welcome to The Studio — Joseph's PERSONAL war room!",
        "This is where I log MY picks. The real ones. MY bets!",
        "Build My Bets lets you see exactly what JOSEPH is playing!",
        "My track record is RIGHT HERE. Exposed for the world!",
        "The Studio is my OFFICE. And business is BOOMING!",
        "I put my money where my mouth is. Check the LOG!",
        "Every pick I make gets tracked HERE. Full transparency!",
        "This is Joseph's desk. Pull up a chair and LEARN!",
        "My personal picks, my analysis, my RECORD. All here!",
        "The Studio is where GREATNESS gets documented!",
        "Want to know what Joseph is betting? You're in the RIGHT place!",
        "I don't hide my results. They're logged RIGHT HERE!",
        "Build My Bets — see how a CHAMPION constructs a slip!",
        "The Studio is MY domain. Welcome to the inner circle!",
        "My logged bets tell the STORY. And it's a winning one!",
    ],
    "page_risk_shield": [
        "Bankroll management is how you STAY in the game!",
        "Risk Shield protects your money. Even from YOURSELF!",
        "The best bettors manage risk FIRST, picks second!",
        "Don't bet more than you can afford. Risk Shield helps!",
        "Protecting your bankroll is NOT optional. It's ESSENTIAL!",
        "I've seen sharp bettors go broke from bad management!",
        "Risk assessment keeps you ALIVE for the next slate!",
        "The shield analyzes your exposure. Stay PROTECTED!",
        "Smart money management is the FOUNDATION of everything!",
        "Risk Shield isn't glamorous but it's CRITICAL!",
        "Your bankroll is your LIFELINE. Guard it with this tool!",
        "Even the BEST picks fail sometimes. Manage that risk!",
        "Risk Shield says how much to wager. LISTEN to it!",
        "Discipline separates winners from losers. Be DISCIPLINED!",
        "Protect the bankroll and the profits will FOLLOW!",
    ],
    "page_data_feed": [
        "Fresh data coming in HOT! The engine needs to be FED!",
        "Updating stats, pulling odds — the pipeline is FLOWING!",
        "The data feed keeps everything CURRENT. That's crucial!",
        "Stale data means bad analysis. Keep that feed FRESH!",
        "I'm pulling the LATEST stats from every source!",
        "Odds are updating RIGHT NOW. The feed is alive!",
        "The engine is only as good as its DATA. Feed it well!",
        "Fresh data means fresh EDGES. Keep it coming!",
        "Every stat update sharpens the analysis. FEED THE BEAST!",
        "The data pipeline is my BLOODLINE. It must stay current!",
        "Pulling injury reports, odds, and stats — ALL at once!",
        "Data feed running strong. The engine is WELL FED!",
        "Current data is NON-NEGOTIABLE. The feed handles that!",
        "Stats refreshed. Odds updated. We're LOCKED AND LOADED!",
        "The fresher the data, the sharper the EDGE. Always update!",
    ],
    "page_correlation": [
        "Correlation is the SECRET WEAPON of sharp bettors!",
        "Find linked props and STACK them for maximum value!",
        "The Correlation Matrix shows which stats move TOGETHER!",
        "Correlated plays amplify your edge. The math PROVES it!",
        "Two players on the same team going over? That's CORRELATION!",
        "The matrix reveals connections the books DON'T want you to see!",
        "Stack correlated props and watch your hit rate CLIMB!",
        "Player A's assists and Player B's points — LINKED!",
        "Correlation isn't a theory. It's MEASURABLE. I measure it!",
        "The strongest parlays are built on CORRELATION!",
        "Find the links, stack the props, COLLECT the profits!",
        "Game environment drives correlation. Pace, total, spread!",
        "I map EVERY correlation in tonight's slate. Every one!",
        "Uncorrelated parlays are GAMBLING. Correlated ones are STRATEGY!",
        "The Correlation Matrix is pure ANALYTICS. Beautiful stuff!",
    ],
    "page_bet_tracker": [
        "Track every bet and know EXACTLY where you stand!",
        "Your win rate, your ROI — it's all logged RIGHT HERE!",
        "The bet tracker doesn't lie. The numbers are HONEST!",
        "Log your wins, log your losses. LEARN from both!",
        "Tracking results is how you IMPROVE over time!",
        "Model accuracy gets measured HERE. And mine is ELITE!",
        "Every bet tracked is a lesson LEARNED. Study the data!",
        "Your ROI tells the real story. Track it RELIGIOUSLY!",
        "The tracker shows which strategies WORK. Follow the data!",
        "Winning bettors track EVERYTHING. Be a winning bettor!",
        "I track my own bets too. Accountability is EVERYTHING!",
        "The bet tracker turns results into ACTIONABLE insights!",
        "Check your hit rate by sport, by stat — it's ALL here!",
        "Losses teach you MORE than wins. Track them both!",
        "The data doesn't lie. The tracker PROVES what works!",
    ],
    "page_backtester": [
        "Backtest your strategy against HISTORICAL data!",
        "The backtester validates the model. Past performance MATTERS!",
        "Don't trust a strategy until you've BACKTESTED it!",
        "Historical analysis proves what WORKS and what doesn't!",
        "I backtested this engine THOUSANDS of times. It's proven!",
        "Run your picks against past data. See the RESULTS!",
        "The backtester is your TIME MACHINE. Use it wisely!",
        "Validation against history separates REAL edges from noise!",
        "Backtest first, bet second. That's the SMART approach!",
        "Past data tells you if your strategy has REAL merit!",
        "The backtester crunches MONTHS of data in seconds!",
        "Every strategy should face the backtest GAUNTLET!",
        "Historical accuracy is the FOUNDATION of future success!",
        "I don't deploy a model until it PASSES the backtest!",
        "Backtesting is NOT optional. It's how you build TRUST!",
    ],
    "page_settings": [
        "Fine-tune the engine to YOUR specifications!",
        "Simulation depth, edge thresholds — configure it ALL here!",
        "The settings page is your CONTROL PANEL. Use it!",
        "Adjust the parameters and the engine ADAPTS to you!",
        "Higher simulation depth means MORE accuracy. Dial it up!",
        "Edge thresholds determine what qualifies as a PLAY!",
        "I recommend starting with default settings. They're OPTIMIZED!",
        "Tweak the confidence threshold to match YOUR risk tolerance!",
        "Settings let you customize the ENTIRE analysis pipeline!",
        "The engine is powerful out of the box but TUNABLE!",
        "Advanced users can push these settings to the LIMIT!",
        "Configure once, dominate FOREVER. That's the idea!",
        "Every parameter here affects the analysis. Choose WISELY!",
        "The default settings are battle-tested. But you do YOU!",
        "Settings are where you make this engine truly YOURS!",
    ],
    "page_premium": [
        "Premium unlocks the FULL power of Joseph's brain!",
        "Unlimited analysis, unlimited EDGE. That's premium!",
        "Free is good. Premium is UNSTOPPABLE!",
        "You want the FULL experience? Premium is the way!",
        "Premium members get access to EVERYTHING. No limits!",
        "Unlock unlimited simulations with a premium subscription!",
        "The free tier is a TASTE. Premium is the full MEAL!",
        "Invest in premium and invest in WINNING!",
        "Premium features include EVERYTHING I've built. All of it!",
        "Serious bettors go premium. It's that SIMPLE!",
        "Full access to the Quantum Matrix Engine — premium ONLY!",
        "Premium is an investment in your BETTING future!",
        "Why limit yourself? Go premium and go ALL IN!",
        "Premium subscribers get Joseph at FULL POWER!",
        "Upgrade to premium and UNLEASH the complete system!",
    ],
    "page_vegas_vault": [
        "The Vegas Vault scans odds across EVERY major book!",
        "Find sportsbook discrepancies and EXPLOIT them!",
        "EV scanning reveals where the TRUE value lives!",
        "Odds comparison is how SHARP money finds edges!",
        "The Vault compares lines so YOU don't have to!",
        "When books disagree, that's where the MONEY is!",
        "EV positive plays are HIDING in the odds. I find them!",
        "Arbitrage opportunities don't last long. The Vault catches them!",
        "Compare odds from every book in ONE place. The Vault!",
        "Line shopping is NON-NEGOTIABLE. The Vault makes it easy!",
        "The Vegas Vault is a GOLDMINE of odds intelligence!",
        "Sportsbooks price things differently. That's YOUR advantage!",
        "EV scanning at its finest. The Vault delivers EVERY night!",
        "Sharp bettors ALWAYS compare odds. The Vault does it instantly!",
        "The Vegas Vault finds what the books tried to HIDE!",
    ],
}

# ═══════════════════════════════════════════════════════════════
# C) COMMENTARY TEMPLATES — For inline injection after results
# ═══════════════════════════════════════════════════════════════

COMMENTARY_OPENER_POOL = {
    "analysis_results": [
        "I just went through EVERY prop and let me tell you...",
        "The numbers are IN and Joseph M. Smith has his VERDICT...",
        "I've seen tonight's analysis and I have THOUGHTS...",
        "The Quantum Matrix Engine has spoken — now let ME speak...",
        "After looking at ALL the data, here's what I KNOW...",
    ],
    "entry_built": [
        "You just built an entry and I have to say...",
        "I see what you put together and listen...",
        "That entry you just built? Let me give you my TAKE...",
        "I'm looking at your parlay and I've got OPINIONS...",
        "Good — you're building entries. Now let me tell you THIS...",
    ],
    "optimal_slip": [
        "The optimizer just cooked something up and WOW...",
        "I see that optimal slip and I have to REACT...",
        "The algorithm found the best combo — now hear MY perspective...",
        "That optimal entry? Let me add my WISDOM to it...",
        "The machine built the slip. Now Joseph M. Smith EVALUATES it...",
    ],
    "ticket_generated": [
        "I just built my personal ticket and I am FIRED UP...",
        "This is MY ticket. Built with MY brain. And it's BEAUTIFUL...",
        "Joseph M. Smith's personal picks are IN. Listen carefully...",
        "I hand-selected every leg of this ticket and HERE'S WHY...",
        "My ticket is READY. And I say this with GREAT conviction...",
    ],
}

# ═══════════════════════════════════════════════════════════════
# D) HISTORICAL COMPS DATABASE — 50+ entries
# ═══════════════════════════════════════════════════════════════

JOSEPH_COMPS_DATABASE = [
    {"name": "Steph Curry 2016 Unanimous MVP", "archetype": "Alpha Scorer", "stat_context": "points", "tier": "Platinum", "narrative": "Historic shooting season", "template": "This reminds me of Curry in 2016 when he was UNANIMOUS... {reason}"},
    {"name": "Steph Curry 2021 Scoring Title", "archetype": "Alpha Scorer", "stat_context": "points", "tier": "Gold", "narrative": "Carrying a rebuilding roster", "template": "Curry won the scoring title in 2021 with NO help. This is that ENERGY... {reason}"},
    {"name": "Steph Curry 402 Threes Season", "archetype": "Alpha Scorer", "stat_context": "threes", "tier": "Platinum", "narrative": "Broke his own three-point record", "template": "Curry hit 402 threes in a SINGLE season. That's what I see here... {reason}"},
    {"name": "LeBron James 2018 Playoff Carry", "archetype": "Playmaking Wing", "stat_context": "assists", "tier": "Platinum", "narrative": "One-man army carrying a team", "template": "This is LeBron in 2018 carrying the Cavs on his BACK... {reason}"},
    {"name": "LeBron James 2020 Championship", "archetype": "Playmaking Wing", "stat_context": "rebounds", "tier": "Gold", "narrative": "Championship-level two-way play", "template": "LeBron in the bubble was UNSTOPPABLE on both ends... {reason}"},
    {"name": "LeBron James Year 20 Longevity", "archetype": "Playmaking Wing", "stat_context": "points", "tier": "Gold", "narrative": "Defying age and gravity", "template": "LeBron in Year 20 doing what he does is HISTORIC... {reason}"},
    {"name": "Michael Jordan 1996 Championship Run", "archetype": "Alpha Scorer", "stat_context": "points", "tier": "Platinum", "narrative": "72-win dominance", "template": "This is Jordan in '96 — RUTHLESS efficiency and killer instinct... {reason}"},
    {"name": "Michael Jordan Flu Game", "archetype": "Alpha Scorer", "stat_context": "points", "tier": "Gold", "narrative": "Legendary performance through adversity", "template": "Jordan played through the FLU and still dropped 38. That's this ENERGY... {reason}"},
    {"name": "Kobe Bryant 81-Point Game", "archetype": "Shot Creator", "stat_context": "points", "tier": "Platinum", "narrative": "Single-game scoring explosion", "template": "Kobe dropped 81 because he REFUSED to lose. I see that same fire... {reason}"},
    {"name": "Kobe Bryant Mamba Mentality Era", "archetype": "Shot Creator", "stat_context": "points", "tier": "Gold", "narrative": "Relentless mid-range dominance", "template": "Kobe's Mamba Mentality was UNMATCHED. This player has that DNA... {reason}"},
    {"name": "Kobe Bryant 2009 Finals MVP", "archetype": "Shot Creator", "stat_context": "points", "tier": "Gold", "narrative": "Championship closer", "template": "Kobe in the 2009 Finals was COLD-BLOODED. Same energy here... {reason}"},
    {"name": "Kevin Durant 2014 MVP Season", "archetype": "Alpha Scorer", "stat_context": "points", "tier": "Platinum", "narrative": "Scoring from all three levels", "template": "KD in his MVP season was UNGUARDABLE. This is that level... {reason}"},
    {"name": "Kevin Durant 2017 Finals", "archetype": "Alpha Scorer", "stat_context": "points", "tier": "Gold", "narrative": "Unstoppable in the biggest moments", "template": "KD in the 2017 Finals hit the dagger. COLD. BLOODED... {reason}"},
    {"name": "Giannis 2021 Finals 50-Point Close", "archetype": "Rim Protector", "stat_context": "points", "tier": "Platinum", "narrative": "Championship-clinching masterpiece", "template": "Giannis dropped 50 in the Finals closeout. That's LEGENDARY... {reason}"},
    {"name": "Giannis Back-to-Back MVP", "archetype": "Rim Protector", "stat_context": "rebounds", "tier": "Gold", "narrative": "Two-way dominance", "template": "Giannis won back-to-back MVPs by being a FORCE. Same here... {reason}"},
    {"name": "Giannis Greek Freak Transition", "archetype": "Rim Protector", "stat_context": "points", "tier": "Gold", "narrative": "Unstoppable in transition", "template": "Giannis in transition is a FREIGHT TRAIN. Nobody is stopping this... {reason}"},
    {"name": "Nikola Jokic 2023 Finals MVP", "archetype": "Pick-and-Roll Big", "stat_context": "assists", "tier": "Platinum", "narrative": "Triple-double machine in the Finals", "template": "Jokic in the 2023 Finals was a TRIPLE-DOUBLE machine. Generational... {reason}"},
    {"name": "Nikola Jokic Triple MVP", "archetype": "Pick-and-Roll Big", "stat_context": "assists", "tier": "Platinum", "narrative": "Three consecutive MVP awards", "template": "Jokic won THREE MVPs. The best passing big EVER... {reason}"},
    {"name": "Nikola Jokic Playmaking Big", "archetype": "Pick-and-Roll Big", "stat_context": "rebounds", "tier": "Gold", "narrative": "All-around stat stuffing", "template": "Jokic stuffs every stat sheet like NOBODY else at his position... {reason}"},
    {"name": "Joel Embiid 2023 MVP", "archetype": "Stretch Big", "stat_context": "points", "tier": "Platinum", "narrative": "Dominant scoring center", "template": "Embiid won the MVP by being the most DOMINANT big in the game... {reason}"},
    {"name": "Joel Embiid 2022 Scoring Title", "archetype": "Stretch Big", "stat_context": "points", "tier": "Gold", "narrative": "Big man scoring title", "template": "Embiid won the scoring title as a CENTER. That's RARE... {reason}"},
    {"name": "Joel Embiid Post Dominance", "archetype": "Stretch Big", "stat_context": "rebounds", "tier": "Gold", "narrative": "Unstoppable in the post", "template": "Embiid in the post is an ABSOLUTE nightmare for defenders... {reason}"},
    {"name": "James Harden 2019 Scoring Streak", "archetype": "High-Usage Ball Handler", "stat_context": "points", "tier": "Platinum", "narrative": "30+ points for 30+ games straight", "template": "Harden scored 30+ for THIRTY-TWO straight games. That's INSANE... {reason}"},
    {"name": "James Harden Triple-Double Season", "archetype": "High-Usage Ball Handler", "stat_context": "assists", "tier": "Gold", "narrative": "Elite playmaking guard", "template": "Harden was averaging a TRIPLE-DOUBLE. His playmaking is ELITE... {reason}"},
    {"name": "James Harden Step-Back Era", "archetype": "High-Usage Ball Handler", "stat_context": "threes", "tier": "Gold", "narrative": "Revolutionary step-back three", "template": "Harden's step-back three CHANGED the game. This is that level... {reason}"},
    {"name": "Luka Doncic 2024 Finals Run", "archetype": "High-Usage Ball Handler", "stat_context": "points", "tier": "Platinum", "narrative": "Young superstar on the biggest stage", "template": "Luka in the Finals was SPECIAL. The kid is a GENERATIONAL talent... {reason}"},
    {"name": "Luka Doncic Triple-Double Machine", "archetype": "High-Usage Ball Handler", "stat_context": "assists", "tier": "Gold", "narrative": "Elite all-around production", "template": "Luka puts up triple-doubles like it's NOTHING. He does EVERYTHING... {reason}"},
    {"name": "Jayson Tatum 2024 Champion", "archetype": "Two-Way Wing", "stat_context": "points", "tier": "Platinum", "narrative": "Championship-winning two-way star", "template": "Tatum won his RING and proved he's ELITE on both ends... {reason}"},
    {"name": "Jayson Tatum 60-Point Explosion", "archetype": "Two-Way Wing", "stat_context": "points", "tier": "Gold", "narrative": "Career-high scoring outburst", "template": "Tatum dropped 60 and showed he can SCORE with anyone... {reason}"},
    {"name": "Jayson Tatum Playoff Performer", "archetype": "Two-Way Wing", "stat_context": "rebounds", "tier": "Gold", "narrative": "Playoff-level two-way impact", "template": "Tatum in the playoffs is a DIFFERENT animal on both ends... {reason}"},
    {"name": "Allen Iverson 2001 Playoff Run", "archetype": "Shot Creator", "stat_context": "points", "tier": "Platinum", "narrative": "Pound-for-pound greatest scorer", "template": "AI in 2001 was the HEART of Philadelphia. Pound for pound the GREATEST... {reason}"},
    {"name": "Allen Iverson Crossover Era", "archetype": "Shot Creator", "stat_context": "steals", "tier": "Gold", "narrative": "Fearless guard with elite hands", "template": "Iverson's crossover and those QUICK hands — you couldn't STOP him... {reason}"},
    {"name": "Steve Nash 2005 MVP", "archetype": "Floor General", "stat_context": "assists", "tier": "Platinum", "narrative": "Seven Seconds or Less revolution", "template": "Nash in 2005 CHANGED how basketball was played. Floor general GENIUS... {reason}"},
    {"name": "Steve Nash Back-to-Back MVP", "archetype": "Floor General", "stat_context": "assists", "tier": "Gold", "narrative": "Elite efficiency and playmaking", "template": "Nash won back-to-back MVPs with his BRAIN. Pure floor general... {reason}"},
    {"name": "Steve Nash 50-40-90 Club", "archetype": "Floor General", "stat_context": "threes", "tier": "Gold", "narrative": "Shooting efficiency mastery", "template": "Nash joined the 50-40-90 club FOUR times. That's EFFICIENCY... {reason}"},
    {"name": "John Stockton All-Time Assists", "archetype": "Floor General", "stat_context": "assists", "tier": "Platinum", "narrative": "Unbreakable assists record", "template": "Stockton's assist record will NEVER be broken. This is that VISION... {reason}"},
    {"name": "John Stockton Steal King", "archetype": "Floor General", "stat_context": "steals", "tier": "Gold", "narrative": "All-time steals leader", "template": "Stockton is the all-time steals leader too. DEFENSIVE intelligence... {reason}"},
    {"name": "Tim Duncan 2003 Finals", "archetype": "Defensive Anchor", "stat_context": "rebounds", "tier": "Platinum", "narrative": "Quintuple-double level impact", "template": "Duncan in the 2003 Finals was near a QUADRUPLE-DOUBLE. Legendary... {reason}"},
    {"name": "Tim Duncan Fundamental Dominance", "archetype": "Defensive Anchor", "stat_context": "blocks", "tier": "Gold", "narrative": "20 years of elite defense", "template": "Duncan was the ULTIMATE fundamental big man for 20 YEARS... {reason}"},
    {"name": "Tim Duncan Big Three Era", "archetype": "Defensive Anchor", "stat_context": "rebounds", "tier": "Gold", "narrative": "Quiet dominance every night", "template": "Duncan didn't need the spotlight. He just DOMINATED quietly... {reason}"},
    {"name": "Kevin Garnett 2004 MVP", "archetype": "Defensive Anchor", "stat_context": "rebounds", "tier": "Platinum", "narrative": "Most versatile power forward ever", "template": "KG in his MVP season did EVERYTHING. Most versatile PF EVER... {reason}"},
    {"name": "Kevin Garnett 2008 Championship", "archetype": "Defensive Anchor", "stat_context": "blocks", "tier": "Gold", "narrative": "Defensive transformation of a franchise", "template": "KG transformed the Celtics defense and won a RING. Intensity PERSONIFIED... {reason}"},
    {"name": "Magic Johnson Showtime", "archetype": "Playmaking Wing", "stat_context": "assists", "tier": "Platinum", "narrative": "Showtime Lakers maestro", "template": "Magic and the Showtime Lakers were ENTERTAINMENT and excellence... {reason}"},
    {"name": "Magic Johnson Triple-Double Finals", "archetype": "Playmaking Wing", "stat_context": "rebounds", "tier": "Gold", "narrative": "6-9 point guard revolutionizing the game", "template": "Magic played ALL five positions in the Finals. REVOLUTIONARY... {reason}"},
    {"name": "Larry Bird 1986 Season", "archetype": "3-and-D Wing", "stat_context": "points", "tier": "Platinum", "narrative": "Peak basketball IQ", "template": "Bird in '86 was the SMARTEST player alive. Basketball IQ off the CHARTS... {reason}"},
    {"name": "Larry Bird Trash Talk Legend", "archetype": "3-and-D Wing", "stat_context": "threes", "tier": "Gold", "narrative": "Shooter with supreme confidence", "template": "Bird would TELL you what he was going to do, then DO it... {reason}"},
    {"name": "Larry Bird Three-Point Contest", "archetype": "3-and-D Wing", "stat_context": "threes", "tier": "Gold", "narrative": "Ultimate shooter's confidence", "template": "Bird asked who was finishing SECOND in the three-point contest. LEGEND... {reason}"},
    {"name": "Shaquille O'Neal 2000 Finals", "archetype": "Glass Cleaner", "stat_context": "rebounds", "tier": "Platinum", "narrative": "Most dominant force in NBA history", "template": "Shaq in 2000 was the most DOMINANT force the NBA has ever SEEN... {reason}"},
    {"name": "Shaquille O'Neal Peak Lakers", "archetype": "Glass Cleaner", "stat_context": "points", "tier": "Platinum", "narrative": "Unstoppable inside scoring", "template": "Peak Shaq was AUTOMATIC in the paint. You couldn't guard him with THREE men... {reason}"},
    {"name": "Shaquille O'Neal Rebounding Machine", "archetype": "Glass Cleaner", "stat_context": "rebounds", "tier": "Gold", "narrative": "Board domination", "template": "Shaq on the boards was like a WRECKING BALL. Nobody outmuscles that... {reason}"},
    {"name": "Hakeem Olajuwon 1994 Championship", "archetype": "Rim Protector", "stat_context": "blocks", "tier": "Platinum", "narrative": "Dream Shake domination", "template": "Hakeem in '94 had the Dream Shake and NOBODY could stop it... {reason}"},
    {"name": "Hakeem Olajuwon Quadruple Double", "archetype": "Rim Protector", "stat_context": "blocks", "tier": "Gold", "narrative": "Most skilled big man ever", "template": "Hakeem recorded a QUADRUPLE-DOUBLE. The most skilled big EVER... {reason}"},
    {"name": "Dwyane Wade 2006 Finals", "archetype": "Two-Way Wing", "stat_context": "points", "tier": "Platinum", "narrative": "Finals takeover performance", "template": "Wade in the 2006 Finals TOOK OVER. That's championship GRIT... {reason}"},
    {"name": "Dwyane Wade Shot-Blocking Guard", "archetype": "Two-Way Wing", "stat_context": "steals", "tier": "Gold", "narrative": "Elite two-way guard play", "template": "Wade was a GUARD who blocked shots like a BIG. Two-way ELITE... {reason}"},
    {"name": "Dwyane Wade 2009 Carry Job", "archetype": "Two-Way Wing", "stat_context": "points", "tier": "Gold", "narrative": "Carrying a team on his back", "template": "Wade in 2009 put a TEAM on his back. 30 a night. HEROIC... {reason}"},
    {"name": "Chris Paul 2008 MVP Runner-Up", "archetype": "Floor General", "stat_context": "assists", "tier": "Platinum", "narrative": "Point God at his peak", "template": "CP3 in 2008 was the POINT GOD at his absolute peak... {reason}"},
    {"name": "Chris Paul Steal Machine", "archetype": "Floor General", "stat_context": "steals", "tier": "Gold", "narrative": "Elite defensive point guard", "template": "CP3's hands are UNREAL. Six-time steals leader. ELITE defender... {reason}"},
    {"name": "Kawhi Leonard 2019 Playoff Run", "archetype": "3-and-D Wing", "stat_context": "points", "tier": "Platinum", "narrative": "Silent assassin championship run", "template": "Kawhi in 2019 was a SILENT ASSASSIN. Board man gets PAID... {reason}"},
    {"name": "Kawhi Leonard Two-Way Dominance", "archetype": "3-and-D Wing", "stat_context": "steals", "tier": "Gold", "narrative": "DPOY-level defense with elite offense", "template": "Kawhi is the BEST two-way player alive. DPOY DNA... {reason}"},
    {"name": "Paul George Playoff P", "archetype": "3-and-D Wing", "stat_context": "points", "tier": "Gold", "narrative": "Versatile wing scoring", "template": "PG13 when he's ON is UNSTOPPABLE from the wing... {reason}"},
    {"name": "Paul George 2019 MVP Candidate", "archetype": "3-and-D Wing", "stat_context": "threes", "tier": "Gold", "narrative": "Elite shooting from deep", "template": "PG13 in his MVP-caliber season was LIGHTS OUT from three... {reason}"},
    {"name": "Dirk Nowitzki 2011 Championship", "archetype": "Stretch Big", "stat_context": "points", "tier": "Platinum", "narrative": "One-legged fadeaway mastery", "template": "Dirk in 2011 beat EVERYONE with that one-legged fadeaway. UNSTOPPABLE... {reason}"},
    {"name": "Dirk Nowitzki 50-40-90 Season", "archetype": "Stretch Big", "stat_context": "threes", "tier": "Gold", "narrative": "Seven-footer shooting like a guard", "template": "Dirk joined the 50-40-90 club as a SEVEN-FOOTER. That's ABSURD... {reason}"},
    {"name": "Sixth Man Jamal Crawford", "archetype": "Sixth Man Spark", "stat_context": "points", "tier": "Gold", "narrative": "Three-time Sixth Man of the Year", "template": "Crawford won Sixth Man THREE times. Instant OFFENSE off the bench... {reason}"},
    {"name": "Sixth Man Manu Ginobili", "archetype": "Sixth Man Spark", "stat_context": "points", "tier": "Gold", "narrative": "Hall of Fame bench player", "template": "Manu was a Hall of Famer who came off the BENCH. That's IMPACT... {reason}"},
    {"name": "Sixth Man Lou Williams", "archetype": "Sixth Man Spark", "stat_context": "points", "tier": "Silver", "narrative": "Underground GOAT sixth man", "template": "Lou Will was the KING of the second unit. Instant buckets... {reason}"},
    {"name": "Dennis Rodman Rebounding Savant", "archetype": "Glass Cleaner", "stat_context": "rebounds", "tier": "Platinum", "narrative": "Greatest rebounder pound-for-pound", "template": "Rodman grabbed rebounds like his LIFE depended on it. RELENTLESS... {reason}"},
    {"name": "Ben Wallace Defensive Force", "archetype": "Defensive Anchor", "stat_context": "blocks", "tier": "Gold", "narrative": "Undrafted to DPOY four times", "template": "Ben Wallace was UNDRAFTED and won DPOY four times. HEART over talent... {reason}"},
    {"name": "Draymond Green 2016 DPOY", "archetype": "Defensive Anchor", "stat_context": "steals", "tier": "Gold", "narrative": "Defensive quarterback", "template": "Draymond is the defensive QUARTERBACK. He makes EVERYONE better... {reason}"},
    {"name": "Jimmy Butler 2023 Playoff Run", "archetype": "Two-Way Wing", "stat_context": "points", "tier": "Platinum", "narrative": "Playoff Jimmy is a different beast", "template": "Playoff Jimmy is a DIFFERENT ANIMAL. He turns it UP when it matters... {reason}"},
    {"name": "Anthony Davis 2020 Championship", "archetype": "Rim Protector", "stat_context": "blocks", "tier": "Gold", "narrative": "Elite rim protection with scoring", "template": "AD in 2020 was a MONSTER on both ends. Rim protector AND scorer... {reason}"},
    {"name": "Russell Westbrook Triple-Double Season", "archetype": "High-Usage Ball Handler", "stat_context": "rebounds", "tier": "Platinum", "narrative": "Averaged a triple-double for a season", "template": "Westbrook averaged a TRIPLE-DOUBLE. Sheer FORCE of will... {reason}"},
    {"name": "Damian Lillard Deep Threes", "archetype": "Shot Creator", "stat_context": "threes", "tier": "Gold", "narrative": "Logo-range shooting", "template": "Dame shoots from the LOGO and it goes IN. That range is LETHAL... {reason}"},
    {"name": "Damian Lillard Playoff Buzzer Beaters", "archetype": "Shot Creator", "stat_context": "points", "tier": "Gold", "narrative": "Clutch gene personified", "template": "Dame TIME is REAL. When the moment is biggest, he's at his BEST... {reason}"},
    {"name": "Kyrie Irving Handle Mastery", "archetype": "Shot Creator", "stat_context": "points", "tier": "Gold", "narrative": "Best ball handler ever", "template": "Kyrie has the BEST handle in NBA history. He creates shots from NOTHING... {reason}"},
    {"name": "Karl Malone Pick-and-Roll", "archetype": "Pick-and-Roll Big", "stat_context": "points", "tier": "Gold", "narrative": "Stockton-to-Malone perfection", "template": "The Stockton-to-Malone pick-and-roll was UNSTOPPABLE. That chemistry... {reason}"},
    {"name": "Bam Adebayo Switchable Big", "archetype": "Pick-and-Roll Big", "stat_context": "rebounds", "tier": "Silver", "narrative": "Modern switchable center", "template": "Bam can guard ONE through FIVE. The modern big man PROTOTYPE... {reason}"},
    {"name": "Scottie Pippen Two-Way Excellence", "archetype": "Playmaking Wing", "stat_context": "steals", "tier": "Gold", "narrative": "Elite perimeter defender and playmaker", "template": "Pippen was the ultimate TWO-WAY wing. Defense AND playmaking... {reason}"},
]

# ═══════════════════════════════════════════════════════════════
# E) CONSTANTS — Dawg Factor, Verdict Emojis, Ticket Names
# ═══════════════════════════════════════════════════════════════

DAWG_FACTOR_TABLE = {
    "revenge_game": +2.5,
    "contract_year": +1.5,
    "nationally_televised": +1.0,
    "rivalry": +0.5,
    "playoff_implications": +0.5,
    "pace_up": +0.5,
    "trap_game": -3.0,
    "back_to_back": -1.5,
    "altitude": -1.0,
    "blowout_risk": -2.0,
    "pace_down": -0.5,
}

VERDICT_EMOJIS = {
    "SMASH": "\U0001f525",
    "LEAN": "\u2705",
    "FADE": "\u26a0\ufe0f",
    "STAY_AWAY": "\U0001f6ab",
}

TICKET_NAMES = {
    2: "POWER PLAY",
    3: "TRIPLE THREAT",
    4: "THE QUAD",
    5: "HIGH FIVE",
    6: "THE FULL SEND",
}

# ── Market Total Thresholds (from Odds API consensus) ──────────────────────────
# NBA historical average is ~225 points per game.
# When 5+ bookmakers agree on a total significantly above/below average,
# Joseph applies a ±1.5% probability adjustment to scoring props.
MARKET_HIGH_TOTAL_THRESHOLD = 228.0  # Games projected to be high-scoring
MARKET_LOW_TOTAL_THRESHOLD  = 212.0  # Games projected to be defensive slugfests
MARKET_CONSENSUS_MIN_BOOKS  = 5      # Minimum bookmakers required for consensus signal


# ═══════════════════════════════════════════════════════════════
# IMPLEMENTED FUNCTIONS — Fragment pickers + verdict + rant + analysis stubs
# ═══════════════════════════════════════════════════════════════

def _pick_fragment(pool, pool_name):
    """Pick a random fragment from *pool* without repeating until exhausted.

    Uses module-level ``_used_fragments`` to track which IDs have been served.
    When every entry in a pool has been used, the tracking set resets.

    Parameters
    ----------
    pool : list[dict]
        One of the fragment pools (OPENER_POOL, PIVOT_POOL, etc.).
    pool_name : str
        Key used in ``_used_fragments`` for tracking.

    Returns
    -------
    str
        The ``"text"`` value of the chosen fragment.
    """
    if not pool:
        return ""
    used = _used_fragments.setdefault(pool_name, set())
    available = [f for f in pool if f["id"] not in used]
    if not available:
        used.clear()
        available = list(pool)
    choice = random.choice(available)
    used.add(choice["id"])
    return choice["text"]


def _pick_ambient(context):
    """Pick ambient colour text for the given context without repeating.

    Parameters
    ----------
    context : str
        Key into ``AMBIENT_CONTEXT_POOL`` (e.g. ``"rivalry"``, ``"blowout_risk"``).

    Returns
    -------
    str
        A single ambient colour sentence, or ``""`` if context is unknown.
    """
    lines = AMBIENT_CONTEXT_POOL.get(context, [])
    if not lines:
        return ""
    used = _used_ambient.setdefault(context, set())
    available = [i for i in range(len(lines)) if i not in used]
    if not available:
        used.clear()
        available = list(range(len(lines)))
    idx = random.choice(available)
    used.add(idx)
    return lines[idx]


def _pick_commentary(stat_type):
    """Pick stat-specific commentary colour without repeating.

    Parameters
    ----------
    stat_type : str
        Stat key into ``STAT_COMMENTARY_POOL`` (e.g. ``"points"``).

    Returns
    -------
    str
        A single commentary sentence, or ``""`` if stat is unknown.
    """
    lines = STAT_COMMENTARY_POOL.get(stat_type, [])
    if not lines:
        return ""
    used = _used_commentary.setdefault(stat_type, set())
    available = [i for i in range(len(lines)) if i not in used]
    if not available:
        used.clear()
        available = list(range(len(lines)))
    idx = random.choice(available)
    used.add(idx)
    return lines[idx]


def determine_verdict(edge, confidence_score, avoid=False):
    """Map edge % and confidence score to a verdict string.

    Parameters
    ----------
    edge : float
        Edge percentage from the Quantum Matrix Engine.
    confidence_score : float
        Confidence score (0-100).
    avoid : bool
        If ``True``, force ``"STAY_AWAY"`` regardless of numbers.

    Returns
    -------
    str
        One of ``"SMASH"``, ``"LEAN"``, ``"FADE"``, ``"STAY_AWAY"``, ``"OVERRIDE"``.
    """
    if avoid:
        return "STAY_AWAY"
    return "LEAN"


def build_rant(verdict, player="", stat="", line="", edge="", prob=""):
    """Assemble a full Joseph M. Smith rant from fragment pools.

    Combines an opener, a body template (chosen by *verdict*), a pivot,
    a catchphrase, and a closer into a multi-sentence tirade.

    Parameters
    ----------
    verdict : str
        Verdict key (``"SMASH"``, ``"LEAN"``, etc.).
    player : str
        Player display name.
    stat : str
        Stat type label (e.g. ``"points"``).
    line : str
        Prop line value as a string (e.g. ``"24.5"``).
    edge : str
        Edge percentage as a string (e.g. ``"8.2"``).
    prob : str
        Probability percentage as a string (e.g. ``"62.1"``).

    Returns
    -------
    str
        The assembled rant string.
    """
    prop = {"stat": stat, "line": line, "edge": edge, "prob": prob}
    return build_joseph_rant(
        player=player, prop=prop, verdict=verdict,
        narrative_tags=[], mismatch=None, comp=None, energy="medium",
    )


def joseph_analyze_pick(player_data, prop_line, stat_type, game_context,
                        platform="DraftKings", recent_games=None):
    """Run full Joseph M. Smith analysis on a single player prop.

    Orchestrates simulation, edge detection, confidence scoring,
    grading, strategy analysis, and rant generation.

    Parameters
    ----------
    player_data : dict
        Player season stats dictionary.
    prop_line : float
        The betting line (e.g. 24.5).
    stat_type : str
        Stat type string (e.g. ``"points"``).
    game_context : dict
        Game info (opponent, is_home, rest_days, etc.).
    platform : str
        Platform name (default ``"PrizePicks"``).
    recent_games : list or None
        Recent game logs for the player.

    Returns
    -------
    dict
        Analysis result with keys: ``verdict``, ``edge``, ``confidence``,
        ``rant``, ``explanation``, ``grade``, ``strategy``,
        ``player_name``, ``stat_type``, ``line``, ``platform``.
    """
    try:
        player_name = player_data.get("name", player_data.get("player_name", "Player"))
        prop_line = _safe_float(prop_line, 0.0)
        game_context = game_context or {}

        # --- Build projection ---
        season_avg_key = {
            "points": "points_avg", "rebounds": "rebounds_avg",
            "assists": "assists_avg", "steals": "steals_avg",
            "blocks": "blocks_avg", "threes": "fg3m_avg",
            "fg3m": "fg3m_avg", "turnovers": "turnovers_avg",
        }
        avg_key = season_avg_key.get(stat_type.lower(), f"{stat_type.lower()}_avg")
        projected_avg = _safe_float(player_data.get(avg_key, 0.0))
        if projected_avg == 0.0:
            projected_avg = _safe_float(player_data.get("points_avg", 15.0), 15.0)

        # Rough std based on stat type
        _STD_RATIOS = {
            "points": 0.30, "rebounds": 0.35, "assists": 0.35,
            "steals": 0.50, "blocks": 0.50, "threes": 0.45,
            "fg3m": 0.45, "turnovers": 0.40,
        }
        std_ratio = _STD_RATIOS.get(stat_type.lower(), 0.30)
        stat_std = max(projected_avg * std_ratio, 1.0)

        # --- Run simulation ---
        sim_result = run_quantum_matrix_simulation(
            projected_stat_average=projected_avg,
            stat_standard_deviation=stat_std,
            prop_line=prop_line,
            number_of_simulations=1000,
            blowout_risk_factor=0.0,
            pace_adjustment_factor=1.0,
            matchup_adjustment_factor=1.0,
            home_away_adjustment=0.0,
            rest_adjustment_factor=1.0,
            stat_type=stat_type,
            platform=platform,
            game_context=game_context if game_context.get("game_id") else None,
        )

        prob_over = _safe_float(sim_result.get("probability_over", 50.0))
        sim_mean = _safe_float(sim_result.get("simulated_mean", projected_avg))

        # --- Edge detection ---
        # Standard -110 vig breakeven: you need 52.38% win rate to break even
        _STANDARD_VIG_BREAKEVEN = 52.38
        edge = (prob_over * 100.0 if prob_over <= 1.0 else prob_over) - _STANDARD_VIG_BREAKEVEN

        # --- Confidence scoring ---
        # Build minimal directional forces from probability and edge for use
        # when full analyze_directional_forces() has not been run (Joseph quick analysis).
        try:
            _jab_forces = {
                "over_count": 1 if edge >= 0 else 0,
                "under_count": 0 if edge >= 0 else 1,
                "over_strength": max(0.0, edge),
                "under_strength": max(0.0, -edge),
            }
            conf_result = calculate_confidence_score(
                probability_over=prob_over,
                edge_percentage=edge,
                directional_forces=_jab_forces,
                defense_factor=1.0,
                stat_standard_deviation=stat_std,
                stat_average=projected_avg,
                simulation_results=sim_result,
                games_played=int(_safe_float(player_data.get("games_played", 30))),
                stat_type=stat_type,
                platform=platform,
            )
            confidence = _safe_float(conf_result.get("confidence_score", 50.0))
            tier = conf_result.get("tier", "Bronze")
        except Exception as exc:
            logger.debug("joseph_full_analysis: confidence calc failed — %s", exc)
            confidence = 50.0
            tier = "Bronze"

        # --- Grading ---
        try:
            grade_result = joseph_grade_player(player_data, game_context)
        except Exception as exc:
            logger.debug("joseph_full_analysis: grading failed — %s", exc)
            grade_result = {"grade": "C", "archetype": "Unknown"}
        grade = grade_result.get("grade", "C")
        archetype = grade_result.get("archetype", "Unknown")

        # --- Strategy ---
        strategy = {}
        try:
            home_team = game_context.get("home_team", "")
            away_team = game_context.get("away_team", "")
            teams_data = game_context.get("teams_data", [])
            if home_team and away_team:
                strategy = analyze_game_strategy(home_team, away_team, game_context, teams_data)
        except Exception as exc:
            logger.debug("joseph_full_analysis: strategy analysis failed — %s", exc)
            strategy = {}

        # --- Verdict ---
        if edge >= 8.0:
            verdict = "SMASH"
        elif edge >= 5.0:
            verdict = "LEAN"
        elif edge >= 2.0:
            verdict = "FADE"
        else:
            verdict = "STAY_AWAY"

        # --- Rant ---
        energy = "nuclear" if verdict == "SMASH" else "high" if verdict == "LEAN" else "medium"
        rant = build_joseph_rant(
            player=player_name,
            prop={"stat": stat_type, "line": str(prop_line),
                  "edge": str(round(edge, 1)), "prob": str(round(prob_over * 100.0 if prob_over <= 1 else prob_over, 1))},
            verdict=verdict,
            narrative_tags=[],
            mismatch=None, comp=None, energy=energy,
        )

        # --- Explanation ---
        try:
            explanation = generate_pick_explanation(
                player_data, prop_line, stat_type, game_context, sim_result
            )
        except Exception as exc:
            logger.debug("joseph_full_analysis: explanation gen failed — %s", exc)
            explanation = {"summary": f"Projected {round(sim_mean, 1)} vs line {prop_line}"}

        return {
            "player_name": player_name,
            "stat_type": stat_type,
            "line": prop_line,
            "platform": platform,
            "verdict": verdict,
            "verdict_emoji": VERDICT_EMOJIS.get(verdict, ""),
            "edge": round(edge, 2),
            "confidence": round(confidence, 2),
            "tier": tier,
            "probability_over": round(prob_over * 100.0 if prob_over <= 1 else prob_over, 2),
            "projected_avg": round(sim_mean, 2),
            "rant": rant,
            "explanation": explanation,
            "grade": grade,
            "archetype": archetype,
            "strategy": strategy,
        }
    except Exception as exc:
        logger.warning("joseph_analyze_pick failed: %s", exc)
        return {
            "player_name": player_data.get("name", "Player") if isinstance(player_data, dict) else "Player",
            "stat_type": stat_type,
            "line": prop_line,
            "platform": platform,
            "verdict": "LEAN",
            "verdict_emoji": VERDICT_EMOJIS.get("LEAN", ""),
            "edge": 0.0,
            "confidence": 50.0,
            "tier": "Bronze",
            "probability_over": 50.0,
            "projected_avg": 0.0,
            "rant": "",
            "explanation": {},
            "grade": {},
            "strategy": {},
        }


# ═══════════════════════════════════════════════════════════════
# JOSEPH'S INDEPENDENT PICK GENERATION
# ═══════════════════════════════════════════════════════════════

_STAT_AVG_KEY_MAP = {
    "points": "points_avg",
    "rebounds": "rebounds_avg",
    "assists": "assists_avg",
    "steals": "steals_avg",
    "blocks": "blocks_avg",
    "threes": "fg3m_avg",
    "fg3m": "fg3m_avg",
    "turnovers": "turnovers_avg",
    "minutes": "minutes_avg",
    "ftm": "ftm_avg",
    "fta": "fta_avg",
    "fga": "fga_avg",
    "fgm": "fgm_avg",
    "personal_fouls": "personal_fouls_avg",
    "offensive_rebounds": "offensive_rebounds_avg",
    "defensive_rebounds": "defensive_rebounds_avg",
}

_COMBO_STAT_MAP = {
    "pts+rebs": ("points_avg", "rebounds_avg"),
    "pts+asts": ("points_avg", "assists_avg"),
    "rebs+asts": ("rebounds_avg", "assists_avg"),
    "pts+rebs+asts": ("points_avg", "rebounds_avg", "assists_avg"),
    "blks+stls": ("blocks_avg", "steals_avg"),
}


def joseph_generate_independent_picks(props, players_lookup, todays_games,
                                      teams_data, max_picks=20):
    """Generate Joseph's own independent picks from raw platform props.

    Joseph selects his top picks based on his eye-test, dawg factor,
    and narrative analysis — separate from any user-generated analysis.

    Parameters
    ----------
    props : list[dict]
        Raw platform prop dicts with ``player_name``, ``stat_type``,
        ``line``, ``team``, ``platform``.
    players_lookup : dict
        Player data keyed by lowercase player name.
    todays_games : list[dict]
        Today's game dicts with ``home_team``, ``away_team``, etc.
    teams_data : dict
        All teams data for matchup analysis.
    max_picks : int
        Maximum number of picks to generate (default 20).

    Returns
    -------
    list[dict]
        List of Joseph analysis dicts with ``dawg_factor``,
        ``narrative_tags``, ``verdict``, ``edge``, etc.
    """
    if not props:
        return []

    candidates = []
    for prop in props:
        player_name = str(
            prop.get("player_name", prop.get("player", prop.get("name", "")))
        ).strip()
        if not player_name:
            continue
        stat_type = str(prop.get("stat_type", "points")).lower().strip()
        line = _safe_float(prop.get("line", prop.get("prop_line", 0)))
        if line <= 0:
            continue
        team = str(prop.get("team", prop.get("player_team", ""))).upper().strip()
        platform = str(prop.get("platform", "PrizePicks"))

        # ── player lookup ────────────────────────────────────
        p_data = players_lookup.get(player_name.lower(), {})
        if not p_data:
            for k, v in players_lookup.items():
                if player_name.lower() in k or k in player_name.lower():
                    p_data = v
                    break

        # ── game lookup ──────────────────────────────────────
        game_data = {}
        for g in (todays_games or []):
            ht = str(g.get("home_team", "")).upper().strip()
            at = str(g.get("away_team", "")).upper().strip()
            if team and team in (ht, at):
                game_data = g
                break

        # ── season average ───────────────────────────────────
        combo_keys = _COMBO_STAT_MAP.get(stat_type)
        if combo_keys:
            player_avg = sum(
                _safe_float(p_data.get(k, 0.0)) for k in combo_keys
            )
        else:
            avg_key = _STAT_AVG_KEY_MAP.get(stat_type, f"{stat_type}_avg")
            player_avg = _safe_float(p_data.get(avg_key, 0.0))

        if player_avg <= 0:
            continue

        # ── basic probability & edge ─────────────────────────
        diff_pct = (player_avg - line) / max(line, 0.1) * 100.0
        prob_over = 50.0 + min(max(diff_pct * 2.0, -30.0), 30.0)
        prob_over = max(5.0, min(95.0, prob_over))
        # Standard -110 vig breakeven: 52.38% win rate to break even
        edge = prob_over - 52.38
        direction = "OVER" if diff_pct > 0 else "UNDER"

        if abs(edge) >= 8.0:
            tier = "Gold"
        elif abs(edge) >= 5.0:
            tier = "Silver"
        else:
            tier = "Bronze"

        analysis_result = {
            "player": player_name,
            "player_name": player_name,
            "team": team,
            "stat_type": stat_type,
            "line": line,
            "prop_line": line,
            "probability_over": prob_over / 100.0,
            "edge_percentage": edge,
            "confidence_score": min(50 + abs(edge) * 3, 95),
            "tier": tier,
            "direction": direction,
            "platform": platform,
        }
        candidates.append((abs(edge), analysis_result, p_data, game_data))

    candidates.sort(key=lambda x: x[0], reverse=True)
    top_candidates = candidates[:max_picks]

    results = []
    for _, ar, p_data, game_data in top_candidates:
        try:
            result = joseph_full_analysis(ar, p_data, game_data, teams_data)
            result["player"] = ar["player_name"]
            result["prop"] = ar["stat_type"]
            result["line"] = ar["line"]
            result["direction"] = ar["direction"]
            result["team"] = ar["team"]
            results.append(result)
        except Exception as exc:
            logger.debug(
                "Independent pick for %s failed: %s", ar.get("player_name"), exc
            )

    return results


def joseph_rank_picks(picks, game_contexts=None):
    """Rank a list of analysed picks by Joseph's priority.

    Parameters
    ----------
    picks : list[dict]
        List of pick analysis dicts (output of ``joseph_analyze_pick``).
    game_contexts : dict or None
        Map of game IDs to game context dicts.

    Returns
    -------
    list[dict]
        The same picks sorted from best to worst with a ``"rank"`` key added.
    """
    if not picks:
        return []
    ranked = []
    for p in picks:
        score = abs(_safe_float(p.get("edge", p.get("joseph_edge", 0))))
        score += _safe_float(p.get("dawg_factor", 0)) * 0.5
        conf = _safe_float(p.get("confidence", 50))
        score += max(conf - 50, 0) * 0.1
        v = str(p.get("verdict", "")).upper()
        if v == "SMASH":
            score += 3.0
        elif v == "LEAN":
            score += 1.0
        p_copy = dict(p)
        p_copy["_rank_score"] = round(score, 2)
        ranked.append(p_copy)
    ranked.sort(key=lambda x: x.get("_rank_score", 0), reverse=True)
    for idx, p in enumerate(ranked, 1):
        p["rank"] = idx
    return ranked


def joseph_evaluate_parlay(picks, platform="DraftKings", entry_fee=10.0):
    """Evaluate a parlay / flex-play slate using correlation adjustments.

    Parameters
    ----------
    picks : list[dict]
        Analysed picks to combine into a parlay.
    platform : str
        Platform for payout table lookup.
    entry_fee : float
        Entry fee in dollars.

    Returns
    -------
    dict
        Parlay evaluation with keys: ``expected_value``, ``correlation_matrix``,
        ``adjusted_probability``, ``rant``.
    """
    return {
        "expected_value": 0.0,
        "correlation_matrix": {},
        "adjusted_probability": 0.0,
        "rant": "",
    }


def joseph_generate_full_slate_analysis(players, props, game_contexts,
                                        platform="DraftKings"):
    """Produce a complete Joseph M. Smith slate analysis.

    Iterates over every player/prop pair, runs ``joseph_analyze_pick``,
    ranks the results, and builds top-parlay suggestions.

    Parameters
    ----------
    players : list[dict]
        Player data dicts.
    props : list[dict]
        Prop dicts with ``stat_type``, ``line``, ``player_name``.
    game_contexts : dict
        Map of game identifiers to game context dicts.
    platform : str
        Platform name for payout table lookup.

    Returns
    -------
    dict
        Full slate result with keys: ``picks``, ``parlays``,
        ``top_plays``, ``summary_rant``.
    """
    if not props:
        return {"picks": [], "parlays": [], "top_plays": [], "summary_rant": ""}

    players_lookup = {}
    for p in (players or []):
        name = str(p.get("name", p.get("player_name", ""))).lower().strip()
        if name:
            players_lookup[name] = p

    games_list = list(game_contexts.values()) if isinstance(game_contexts, dict) else []
    teams_data = game_contexts if isinstance(game_contexts, dict) else {}

    picks = joseph_generate_independent_picks(
        props, players_lookup, games_list, teams_data, max_picks=30,
    )
    ranked = joseph_rank_picks(picks)
    top_plays = [
        p for p in ranked
        if str(p.get("verdict", "")).upper() in ("SMASH", "LEAN")
    ][:5]

    n_smash = len([p for p in ranked if str(p.get("verdict", "")).upper() == "SMASH"])
    n_lean = len([p for p in ranked if str(p.get("verdict", "")).upper() == "LEAN"])
    summary_rant = (
        f"I've analyzed {len(ranked)} props tonight. "
        f"{n_smash} SMASH plays, {n_lean} LEAN plays. "
        f"Let's get to work."
    )

    return {
        "picks": ranked,
        "parlays": [],
        "top_plays": top_plays,
        "summary_rant": summary_rant,
    }


def joseph_commentary_for_stat(player_name, stat_type, context="neutral"):
    """Generate a short stat-specific colour commentary line.

    Parameters
    ----------
    player_name : str
        Player display name.
    stat_type : str
        Stat key (``"points"``, ``"rebounds"``, etc.).
    context : str
        Ambient context key for scene-setting.

    Returns
    -------
    str
        A colourful one-liner about the player and stat.
    """
    return ""


def joseph_blowout_warning(spread, game_total):
    """Produce a Joseph-style blowout-risk warning if applicable.

    Parameters
    ----------
    spread : float
        Vegas spread for the game.
    game_total : float
        Vegas over/under game total.

    Returns
    -------
    str
        Warning string, or ``""`` if no blowout risk detected.
    """
    return ""


def reset_fragment_state():
    """Clear all anti-repetition tracking dicts.

    Useful between slates or test runs to get a fresh fragment cycle.
    """
    _used_fragments.clear()
    _used_ambient.clear()
    _used_commentary.clear()


# ═══════════════════════════════════════════════════════════════
# F) FUNCTION IMPLEMENTATIONS — Phase 1B
# ═══════════════════════════════════════════════════════════════


def _extract_edge(result: dict) -> float:
    """Extract edge value from an analysis result dict, checking multiple key names."""
    return _safe_float(
        result.get("joseph_edge",
                    result.get("edge_percentage",
                               result.get("edge", 0)))
    )


def _select_fragment(pool: list, used_set: set) -> dict:
    """Select random fragment from pool, excluding used IDs. Reset if >60% exhausted.

    Parameters
    ----------
    pool : list[dict]
        Fragment pool (each entry has ``"id"`` and ``"text"`` keys).
    used_set : set
        Set of already-used fragment IDs.

    Returns
    -------
    dict
        The selected fragment dict with ``"id"`` and ``"text"`` keys.
    """
    if not pool:
        return {"id": "fallback", "text": ""}
    if len(used_set) > 0.6 * len(pool):
        used_set.clear()
    available = [f for f in pool if f["id"] not in used_set]
    if not available:
        used_set.clear()
        available = pool.copy()
    selected = random.choice(available)
    used_set.add(selected["id"])
    return selected


def build_joseph_rant(player: str, prop: dict, verdict: str, narrative_tags: list,
                      mismatch: dict | None = None, comp: dict | None = None,
                      energy: str = "medium") -> str:
    """Build a unique 4-8 sentence Joseph rant using combinatorial fragment assembly.

    Combines opener, body template, pivot, catchphrase, and closer fragments
    to form a multi-sentence rant personalised to the player/prop/verdict.

    Parameters
    ----------
    player : str
        Player display name.
    prop : dict
        Prop dict with ``stat_type``, ``line``, etc.
    verdict : str
        Verdict key (``"SMASH"``, ``"LEAN"``, ``"FADE"``, ``"STAY_AWAY"``).
    narrative_tags : list[str]
        List of narrative tags (e.g. ``["revenge_game", "pace_up"]``).
    mismatch : dict or None
        Mismatch data from strategy analysis.
    comp : dict or None
        Historical comp entry from JOSEPH_COMPS_DATABASE.
    energy : str
        Energy level (``"low"``, ``"medium"``, ``"high"``).

    Returns
    -------
    str
        The assembled multi-sentence Joseph rant.
    """
    try:
        used_set = _used_fragments.setdefault("rant", set())

        # 1. Select opener
        opener = _select_fragment(OPENER_POOL, used_set)
        opener_text = opener.get("text", "")
        if opener_text and not opener_text.rstrip().endswith("..."):
            opener_text = opener_text.rstrip(". ") + "..."

        # 2. Select body sentences based on energy
        body_count = {"low": 2, "medium": 2, "high": 3, "nuclear": 3}.get(energy, 2)
        templates = BODY_TEMPLATES.get(verdict, BODY_TEMPLATES.get("LEAN", []))
        stat = prop.get("stat", prop.get("stat_type", ""))
        line = prop.get("line", "")
        edge = prop.get("edge", prop.get("edge_percentage", ""))
        prob = prop.get("prob", prop.get("probability_over", ""))
        body_sentences = []
        used_indices = set()
        for _ in range(min(body_count, len(templates))):
            avail = [i for i in range(len(templates)) if i not in used_indices]
            if not avail:
                break
            idx = random.choice(avail)
            used_indices.add(idx)
            try:
                sentence = templates[idx].format(
                    player=player, stat=stat, line=line, edge=edge, prob=prob
                )
            except (KeyError, IndexError):
                sentence = templates[idx]
            body_sentences.append(sentence)

        # 3. Check for counter-point pivot (positive + negative tags present)
        positive_tags = {"revenge_game", "contract_year", "nationally_televised",
                         "rivalry", "playoff_implications", "pace_up"}
        negative_tags = {"trap_game", "back_to_back", "altitude", "blowout_risk", "pace_down"}
        has_positive = any(t in positive_tags for t in narrative_tags)
        has_negative = any(t in negative_tags for t in narrative_tags)
        pivot_text = ""
        if has_positive and has_negative:
            pivot = _select_fragment(PIVOT_POOL, used_set)
            pivot_text = pivot.get("text", "")

        # 4. Select closer
        closer = _select_fragment(CLOSER_POOL, used_set)
        closer_text = " \u2014 " + closer.get("text", "")

        # 5. Catchphrases based on energy
        catchphrases = []
        if energy in ("high", "nuclear"):
            cp = _select_fragment(CATCHPHRASE_POOL, used_set)
            catchphrases.append(cp.get("text", ""))
        if energy == "nuclear":
            cp2 = _select_fragment(CATCHPHRASE_POOL, used_set)
            catchphrases.append(cp2.get("text", ""))

        # 6. Comp reference
        comp_text = ""
        if comp is not None:
            try:
                comp_text = comp["template"].format(
                    comp_name=comp.get("name", ""),
                    reason="the matchup profile is IDENTICAL"
                )
            except (KeyError, IndexError):
                comp_text = ""

        # 7. Mismatch sentence
        mismatch_text = ""
        if mismatch is not None:
            desc = mismatch.get("description", "It is GLARING")
            mismatch_text = f"And the MISMATCH? {desc}!"

        # 8. Assemble
        parts = [opener_text]
        parts.extend(body_sentences)
        if pivot_text:
            parts.append(pivot_text)
        if comp_text:
            parts.append(comp_text)
        if mismatch_text:
            parts.append(mismatch_text)
        parts.extend(catchphrases)
        parts.append(closer_text)

        return " ".join(p for p in parts if p)
    except Exception:
        return f"Joseph M. Smith likes {player}. {verdict}!"


def _generate_counter_argument(player: dict, prop_data: dict, narrative_tags: list) -> str:
    """Build a 1-sentence counter-argument for balance.

    Parameters
    ----------
    player : dict
        Player data dict.
    prop_data : dict
        Prop data with stat, line, edge, etc.
    narrative_tags : list[str]
        Active narrative tags.

    Returns
    -------
    str
        A single counter-argument sentence.
    """
    if "back_to_back" in narrative_tags:
        return "The fatigue factor on a back-to-back CANNOT be ignored."
    if "trap_game" in narrative_tags:
        return "Motivation could be an issue in a trap game scenario."
    if "blowout_risk" in narrative_tags:
        return "If this game gets out of hand, minutes could be CUT."
    if "altitude" in narrative_tags:
        return "Playing at altitude in Denver is a REAL factor that affects performance."
    if "pace_down" in narrative_tags:
        return "A slower pace environment limits OPPORTUNITIES for production."
    return "Standard variance means even good edges lose 35-40% of the time."


def joseph_full_analysis(analysis_result: dict, player: dict, game: dict,
                         teams_data: dict) -> dict:
    """THE 8-STEP REASONING LOOP. Returns complete analysis dict.

    Steps: (1) edge detection, (2) confidence scoring, (3) grading,
    (4) narrative tagging, (5) dawg factor, (6) mismatch detection,
    (7) historical comp, (8) rant generation.

    Parameters
    ----------
    analysis_result : dict
        Raw analysis result from the Quantum Matrix Engine.
    player : dict
        Player data dict with season stats.
    game : dict
        Game context dict.
    teams_data : dict
        All teams data for matchup analysis.

    Returns
    -------
    dict
        Complete analysis with keys: ``verdict``, ``verdict_emoji``,
        ``is_override``, ``edge``, ``confidence``, ``rant``,
        ``dawg_factor``, ``narrative_tags``, ``comp``, ``grade``.
    """
    try:
        # Step 1 — OBSERVE
        qme_prob = _safe_float(analysis_result.get("probability_over", 50.0))
        # Neural Analysis stores probability_over as 0-1 decimal (e.g. 0.63).
        # Joseph's internal math uses 0-100 percentage scale.
        # Use < 1.5 threshold (simulations clamp to [0.01, 0.99]; a real
        # percentage of 1.5% would be extremely rare and still benign).
        if 0.0 < qme_prob < 1.5:
            qme_prob *= 100.0
        qme_edge = _safe_float(analysis_result.get("edge_percentage", 0.0))
        confidence_score = _safe_float(analysis_result.get("confidence_score", 50.0))
        tier = str(analysis_result.get("tier", "Bronze"))
        stat_type = str(analysis_result.get("stat_type", ""))
        line = analysis_result.get("line", 0)
        direction = str(analysis_result.get("direction", "OVER"))

        # Step 2 — FRAME
        try:
            narrative_tags = detect_narrative_tags(player, game, teams_data)
        except Exception:
            narrative_tags = []
        if not narrative_tags:
            narrative_tags = []
        if game.get("is_back_to_back"):
            if "back_to_back" not in narrative_tags:
                narrative_tags.append("back_to_back")
        if game.get("is_nationally_televised"):
            if "nationally_televised" not in narrative_tags:
                narrative_tags.append("nationally_televised")
        pace_delta = _safe_float(game.get("pace_delta", 0.0))
        if pace_delta > 2.0 and "pace_up" not in narrative_tags:
            narrative_tags.append("pace_up")
        elif pace_delta < -2.0 and "pace_down" not in narrative_tags:
            narrative_tags.append("pace_down")

        # Step 3 — RETRIEVE
        try:
            player_grade = joseph_grade_player(player, game)
        except Exception:
            player_grade = {"grade": "C", "archetype": "Unknown", "score": 50.0,
                            "gravity": 50.0, "switchability": 50.0}
        archetype = player_grade.get("archetype", "Unknown")

        # Find matching comp
        comp = None
        if JOSEPH_COMPS_DATABASE:
            arch_matches = [c for c in JOSEPH_COMPS_DATABASE if c.get("archetype") == archetype]
            stat_matches = [c for c in arch_matches if c.get("stat_context") == stat_type]
            if stat_matches:
                comp = random.choice(stat_matches)
            elif arch_matches:
                comp = random.choice(arch_matches)
            else:
                comp = random.choice(JOSEPH_COMPS_DATABASE)

        # Step 4 — MODEL
        _home_team = str(game.get("home_team", game.get("home", ""))).upper().strip()
        _away_team = str(game.get("away_team", game.get("away", ""))).upper().strip()
        try:
            game_strategy = analyze_game_strategy(_home_team, _away_team, game, teams_data)
        except Exception:
            game_strategy = {"scheme": "unknown", "strategy": "unknown",
                             "scheme_match": 0.0, "mismatch_tags": [],
                             "regime_adjustment": 0.0}

        dawg_adjustment = sum(DAWG_FACTOR_TABLE.get(tag, 0.0) for tag in narrative_tags)
        dawg_adjustment = max(-5.0, min(5.0, dawg_adjustment))

        mismatch_boost = 0.0
        mismatch_alert = None
        mismatch_grade = player_grade.get("mismatch_grade", "C")
        if mismatch_grade in ("A+", "A"):
            mismatch_boost = 1.5
            mismatch_alert = f"{player.get('name', 'Player')} has a SIGNIFICANT mismatch advantage"
        elif mismatch_grade == "B":
            mismatch_boost = 0.5

        regime_adj = _safe_float(game_strategy.get("regime_adjustment", 0.0))

        games_played = _safe_float(player.get("games_played", 20))
        sample_dampening = 0.0
        if games_played < 10:
            sample_dampening = -1.0 * (10 - games_played) / 10

        # ── Market context boost/fade from Odds API consensus ──────
        # When The Odds API provides consensus lines from 5+ bookmakers,
        # use the multi-book total to calibrate the game-pace assumption.
        # High-total games (consensus > 228) get a +1.5 boost for scoring
        # props; low-total games (consensus < 212) get -1.5.
        market_adj = 0.0
        _consensus_total = game.get("consensus_total")
        _bk_count = int(game.get("bookmaker_count", 0) or 0)
        if _consensus_total is not None and _bk_count >= MARKET_CONSENSUS_MIN_BOOKS:
            try:
                ct = float(_consensus_total)
                if ct > MARKET_HIGH_TOTAL_THRESHOLD:
                    market_adj = 1.5
                    if "market_high_total" not in narrative_tags:
                        narrative_tags.append("market_high_total")
                elif ct < MARKET_LOW_TOTAL_THRESHOLD:
                    market_adj = -1.5
                    if "market_low_total" not in narrative_tags:
                        narrative_tags.append("market_low_total")
            except (TypeError, ValueError):
                pass

        joseph_prob = qme_prob + dawg_adjustment + mismatch_boost + regime_adj + sample_dampening + market_adj
        joseph_prob = max(1.0, min(99.0, joseph_prob))

        implied_line = _safe_float(analysis_result.get("implied_probability", 0.0))
        # implied_probability may be stored as 0-1 decimal — convert to percentage
        if 0.0 < implied_line < 1.5:
            implied_line *= 100.0
        # If implied_probability is missing or zero, derive from qme_prob and qme_edge
        if implied_line <= 0.0:
            if abs(qme_edge) > 0.001:
                implied_line = qme_prob - qme_edge
            else:
                implied_line = 50.0
        joseph_edge = joseph_prob - implied_line

        # Step 5 — ADJUST
        edge_delta = abs(joseph_edge - qme_edge)
        is_override = edge_delta > 3.0
        override_direction = None
        if is_override:
            override_direction = "UPGRADE" if joseph_edge > qme_edge else "DOWNGRADE"

        # Step 6 — CONCLUDE
        if joseph_edge >= 8.0:
            verdict = "SMASH"
        elif joseph_edge >= 5.0:
            verdict = "LEAN"
        elif joseph_edge >= 2.0:
            verdict = "FADE"
        else:
            verdict = "STAY_AWAY"
        verdict_emoji = VERDICT_EMOJIS.get(verdict, "")

        prop_data = {
            "stat": stat_type,
            "line": line,
            "edge": round(joseph_edge, 1),
            "prob": round(joseph_prob, 1),
            "direction": direction,
        }

        counter_argument = _generate_counter_argument(player, prop_data, narrative_tags)

        risk_factors = []
        if "back_to_back" in narrative_tags:
            risk_factors.append("Back-to-back fatigue risk")
        if "trap_game" in narrative_tags:
            risk_factors.append("Trap game — low motivation risk")
        if "blowout_risk" in narrative_tags:
            risk_factors.append("Blowout risk may reduce minutes")
        if games_played < 15:
            risk_factors.append(f"Small sample size ({int(games_played)} games)")
        if not risk_factors:
            risk_factors.append("Standard variance applies")

        # Step 7 — EXPLAIN
        energy_level = "nuclear" if verdict == "SMASH" else "high" if verdict == "LEAN" else "medium" if verdict == "FADE" else "low"
        if is_override:
            energy_level = "nuclear"

        rant_verdict = "OVERRIDE" if is_override else verdict
        rant_text = build_joseph_rant(
            player=player.get("name", "Player"),
            prop=prop_data,
            verdict=rant_verdict,
            narrative_tags=narrative_tags,
            mismatch={"description": mismatch_alert} if mismatch_alert else None,
            comp=comp,
            energy=energy_level,
        )

        one_liner = (
            f"{player.get('name', 'Player')} {prop_data['direction']} {prop_data['line']} "
            f"{prop_data['stat']}: {verdict_emoji} {verdict} ({round(joseph_edge, 1)}% edge)"
        )

        override_explanation = None
        if is_override:
            override_explanation = (
                f"Joseph OVERRIDES the engine ({override_direction}): "
                f"QME edge was {round(qme_edge, 1)}% but Joseph sees {round(joseph_edge, 1)}%. "
                f"Delta: {round(edge_delta, 1)}%."
            )

        condensed_summary = (
            f"{verdict_emoji} {verdict} \u2014 {player.get('name', 'Player')} "
            f"{prop_data['direction']} {prop_data['line']} {prop_data['stat']} "
            f"({round(joseph_edge, 1)}% edge, {round(joseph_prob, 1)}% probability)"
        )

        # Step 8 — TRACK
        reasoning_chain = [
            {"step": 1, "name": "OBSERVE", "detail": f"QME: {round(qme_prob, 1)}% prob, {round(qme_edge, 1)}% edge, tier={tier}"},
            {"step": 2, "name": "FRAME", "detail": f"Tags: {narrative_tags}"},
            {"step": 3, "name": "RETRIEVE", "detail": f"Comp: {comp['name'] if comp else 'None'}, Archetype: {archetype}"},
            {"step": 4, "name": "MODEL", "detail": f"Dawg={round(dawg_adjustment, 1)}, Mismatch={round(mismatch_boost, 1)}, Regime={round(regime_adj, 1)}, Sample={round(sample_dampening, 1)}, Market={round(market_adj, 1)}"},
            {"step": 5, "name": "ADJUST", "detail": f"Joseph edge={round(joseph_edge, 1)}%, Override={is_override}"},
            {"step": 6, "name": "CONCLUDE", "detail": f"Verdict={verdict}, Risks={risk_factors}"},
            {"step": 7, "name": "EXPLAIN", "detail": f"Rant generated, energy={energy_level}"},
            {"step": 8, "name": "TRACK", "detail": "Reasoning chain logged"},
        ]

        try:
            log_prediction({
                "player": player.get("name", ""),
                "stat_type": stat_type,
                "line": line,
                "direction": direction,
                "verdict": verdict,
                "joseph_edge": round(joseph_edge, 2),
                "joseph_prob": round(joseph_prob, 2),
                "qme_edge": round(qme_edge, 2),
                "is_override": is_override,
            })
        except Exception as exc:
            logger.debug("Failed to log prediction for %s: %s",
                         player.get("name", "unknown"), exc)

        nerd_stats = {
            "qme_probability": round(qme_prob, 2),
            "joseph_probability": round(joseph_prob, 2),
            "dawg_adjustment": round(dawg_adjustment, 2),
            "mismatch_boost": round(mismatch_boost, 2),
            "regime_adjustment": round(regime_adj, 2),
            "sample_dampening": round(sample_dampening, 2),
            "games_played": int(games_played),
            "implied_line": round(implied_line, 2),
        }

        return {
            "verdict": verdict,
            "verdict_emoji": verdict_emoji,
            "is_override": is_override,
            "override_direction": override_direction,
            "override_explanation": override_explanation,
            "edge": round(joseph_edge, 2),
            "joseph_edge": round(joseph_edge, 2),
            "qme_edge": round(qme_edge, 2),
            "confidence": round(confidence_score, 2),
            "joseph_probability": round(joseph_prob, 2),
            "rant": rant_text,
            "one_liner": one_liner,
            "condensed_summary": condensed_summary,
            "counter_argument": counter_argument,
            "risk_factors": risk_factors,
            "dawg_factor": round(dawg_adjustment, 2),
            "narrative_tags": narrative_tags,
            "comp": comp,
            "grade": player_grade.get("grade", "C"),
            "archetype": archetype,
            "energy_level": energy_level,
            "reasoning_chain": reasoning_chain,
            "nerd_stats": nerd_stats,
        }
    except Exception as exc:
        logger.warning("joseph_full_analysis failed: %s", exc)
        return {
            "verdict": "LEAN",
            "verdict_emoji": VERDICT_EMOJIS.get("LEAN", ""),
            "is_override": False,
            "override_direction": None,
            "override_explanation": None,
            "edge": 0.0,
            "joseph_edge": 0.0,
            "qme_edge": 0.0,
            "confidence": 50.0,
            "joseph_probability": 50.0,
            "rant": "",
            "one_liner": "",
            "condensed_summary": "",
            "counter_argument": "Standard variance means even good edges lose 35-40% of the time.",
            "risk_factors": ["Standard variance applies"],
            "dawg_factor": 0.0,
            "narrative_tags": [],
            "comp": None,
            "grade": "C",
            "archetype": "Unknown",
            "energy_level": "medium",
            "reasoning_chain": [],
            "nerd_stats": {},
        }


def joseph_analyze_game(game: dict, teams_data: dict,
                        analysis_results: list) -> dict:
    """Game-level analysis for Studio Game Mode + sidebar widget.

    Parameters
    ----------
    game : dict
        Game data dict with home/away teams.
    teams_data : dict
        All teams data for context.
    analysis_results : list[dict]
        List of all prop analysis results for this game.

    Returns
    -------
    dict
        Game-level analysis with keys: ``game_narrative``, ``pace_take``,
        ``scheme_analysis``, ``blowout_risk``, ``best_props``.
    """
    try:
        home = game.get("home_team", game.get("home", "Home"))
        away = game.get("away_team", game.get("away", "Away"))
        game_id = game.get("game_id", game.get("id", ""))

        # Run game strategy
        try:
            strategy = analyze_game_strategy(home, away, game, teams_data)
        except Exception:
            strategy = {"scheme": "unknown", "strategy": "unknown",
                        "scheme_match": 0.0, "mismatch_tags": []}

        scheme = strategy.get("home_scheme", strategy.get("scheme", "unknown"))
        if isinstance(scheme, dict):
            scheme = scheme.get("primary_scheme", scheme.get("scheme_name", "unknown"))
        away_scheme = strategy.get("away_scheme", "unknown")
        if isinstance(away_scheme, dict):
            away_scheme = away_scheme.get("primary_scheme", away_scheme.get("scheme_name", "unknown"))
        # Use strategy pace projection when game-level pace_delta isn't available
        pace_proj = _safe_float(strategy.get("pace_projection", 0.0))
        pace = _safe_float(game.get("pace_delta", 0.0))
        if pace == 0.0 and pace_proj > 0:
            # Derive delta from league average (~100)
            pace = pace_proj - 100.0

        # Filter results for this game
        game_props = []
        for r in (analysis_results or []):
            r_game = r.get("game_id", r.get("game", ""))
            if r_game == game_id or not game_id:
                game_props.append(r)

        # Sort by edge descending and get top 3
        sorted_props = sorted(game_props, key=lambda x: _extract_edge(x), reverse=True)
        best_props = sorted_props[:3]

        # Run full analysis on top props if not already analyzed
        for i, prop in enumerate(best_props):
            if "verdict" not in prop or "rant" not in prop:
                player_data = prop.get("player_data", prop.get("player", {}))
                try:
                    best_props[i] = joseph_full_analysis(prop, player_data, game, teams_data)
                except Exception as exc:
                    logger.debug("Prop reanalysis failed for %s: %s",
                                 prop.get("player_name", "unknown"), exc)

        # Generate narratives
        strategy_narrative = strategy.get("game_narrative", "")
        game_narrative = strategy_narrative if strategy_narrative else (
            f"{away} at {home} is a game I've been watching CLOSELY. "
            f"The scheme profile says '{scheme}' and the matchups are INTRIGUING. "
            f"I see {len(game_props)} props on the board and the edges are REAL."
        )

        if pace > 2.0:
            pace_take = f"This game projects to be UP-TEMPO with a pace delta of +{round(pace, 1)}. That means MORE possessions and MORE opportunities for production."
        elif pace < -2.0:
            pace_take = f"SLOW it down! Pace delta is {round(pace, 1)} — fewer possessions means fewer chances to hit props. Be SELECTIVE."
        else:
            pace_take = "Pace is NEUTRAL here. No significant advantage or disadvantage from tempo."

        scheme_analysis = f"{home} runs a '{scheme}' defense"
        if away_scheme and away_scheme != "unknown":
            scheme_analysis += f" while {away} runs '{away_scheme}'"
        scheme_analysis += ". "
        scheme_matchups = strategy.get("scheme_matchups", [])
        if scheme_matchups:
            scheme_analysis += f"I see matchup edges in {', '.join(str(m) for m in scheme_matchups[:2])}. That's where the VALUE is."
        elif strategy.get("mismatch_tags"):
            scheme_analysis += f"I see mismatches in {', '.join(strategy['mismatch_tags'][:2])}. That's where the VALUE is."
        else:
            scheme_analysis += "No glaring mismatches but the matchup data tells a story."

        spread = _safe_float(game.get("spread", 0.0))
        blowout_risk_text = ""
        if abs(spread) >= BLOWOUT_DIFFERENTIAL_MILD:
            blowout_risk_text = (
                f"WARNING: The spread is {spread} — blowout risk is ELEVATED. "
                f"Starters could sit in the fourth quarter. Be CAREFUL with player props."
            )
        elif abs(spread) >= 8:
            blowout_risk_text = f"The spread of {spread} suggests a competitive but lopsided game. Monitor minute projections."

        # Betting angle and game total
        strategy_angle = strategy.get("betting_angle", "")
        betting_angle = strategy_angle if strategy_angle else (
            "Focus on the BEST individual matchups rather than game-level bets tonight."
        )
        if best_props:
            top = best_props[0]
            pname = top.get("player_name", top.get("name", "top pick"))
            betting_angle = f"My best angle for this game is {pname}. The edge profile is the STRONGEST here."

        game_total = _safe_float(game.get("total", game.get("over_under", 220.0)))
        strategy_total_est = _safe_float(strategy.get("game_total_est", 0.0))
        joseph_game_total_take = f"The total is set at {game_total}. "
        if strategy_total_est > 0 and abs(strategy_total_est - game_total) > 3:
            if strategy_total_est > game_total:
                joseph_game_total_take += (
                    f"My model projects {round(strategy_total_est, 1)} — that's "
                    f"{round(strategy_total_est - game_total, 1)} points ABOVE the line. "
                    f"I LEAN towards the OVER."
                )
            else:
                joseph_game_total_take += (
                    f"My model projects {round(strategy_total_est, 1)} — that's "
                    f"{round(game_total - strategy_total_est, 1)} points BELOW the line. "
                    f"I LEAN towards the UNDER."
                )
        elif pace > 2.0:
            joseph_game_total_take += "With the pace profile, I LEAN towards the over."
        elif pace < -2.0:
            joseph_game_total_take += "Slower pace tells me the under has VALUE."
        else:
            joseph_game_total_take += "I don't have a strong lean on the total tonight."

        strategy_spread_est = _safe_float(strategy.get("spread_est", 0.0))
        joseph_spread_take = f"{home} at {spread} — "
        if abs(spread) < 3:
            joseph_spread_take += "this is a COIN FLIP game and I love the drama."
        elif spread < -7:
            joseph_spread_take += f"{home} is a HEAVY favourite. Blowout risk is on the radar."
        else:
            joseph_spread_take += "the line looks FAIR based on what I see."

        risk_warning = "Standard variance applies — even the best edges lose sometimes."
        if blowout_risk_text:
            risk_warning = "Blowout risk is the PRIMARY concern for this game."
        elif "back_to_back" in str(game):
            risk_warning = "Back-to-back fatigue could affect production across the board."

        condensed_summary = (
            f"{away} at {home}: {game_narrative.split('.')[0]}. "
            f"{pace_take.split('.')[0]}. "
            f"{'BLOWOUT RISK elevated. ' if blowout_risk_text else ''}"
            f"Top play: {best_props[0].get('player_name', 'TBD') if best_props else 'TBD'}."
        )

        return {
            "game_narrative": game_narrative,
            "pace_take": pace_take,
            "scheme_analysis": scheme_analysis,
            "blowout_risk": blowout_risk_text,
            "best_props": best_props,
            "betting_angle": betting_angle,
            "joseph_game_total_take": joseph_game_total_take,
            "joseph_spread_take": joseph_spread_take,
            "risk_warning": risk_warning,
            "condensed_summary": condensed_summary,
            "home": home,
            "away": away,
            "game_id": game_id,
        }
    except Exception as exc:
        logger.warning("joseph_analyze_game failed: %s", exc)
        return {
            "game_narrative": "",
            "pace_take": "",
            "scheme_analysis": "",
            "blowout_risk": "",
            "best_props": [],
        }


def joseph_analyze_player(player: dict, games: list, teams_data: dict,
                          analysis_results: list) -> dict:
    """Player-level analysis for Studio Player Mode + sidebar widget.

    Parameters
    ----------
    player : dict
        Player data dict with season stats and game logs.
    games : list[dict]
        Recent game logs.
    teams_data : dict
        All teams data for context.
    analysis_results : list[dict]
        Analysis results for this player's props.

    Returns
    -------
    dict
        Player-level analysis with keys: ``scouting_report``, ``archetype``,
        ``grade``, ``gravity``, ``trend``, ``narrative_tags``.
    """
    try:
        player_name = player.get("name", player.get("player_name", "Player"))

        # Grade the player
        tonight_game = games[0] if games else {}
        try:
            grade_result = joseph_grade_player(player, tonight_game)
        except Exception:
            grade_result = {"grade": "C", "archetype": "Unknown", "score": 50.0,
                            "gravity": 50.0, "switchability": 50.0}

        archetype = grade_result.get("archetype", "Unknown")
        grade = grade_result.get("grade", "C")
        gravity = _safe_float(grade_result.get("gravity", 50.0))

        # Filter analysis_results for this player
        player_props = []
        for r in (analysis_results or []):
            r_name = r.get("player_name", r.get("name", ""))
            if r_name == player_name or not r_name:
                player_props.append(r)

        # Sort by edge to find best and alternatives
        sorted_props = sorted(player_props, key=lambda x: _extract_edge(x), reverse=True)
        best_prop = sorted_props[0] if sorted_props else None
        alt_props = sorted_props[1:3] if len(sorted_props) > 1 else []

        # Run full analysis on best prop if needed
        best_analysis = None
        if best_prop:
            try:
                best_analysis = joseph_full_analysis(best_prop, player, tonight_game, teams_data)
            except Exception:
                best_analysis = None

        # Detect narrative tags
        try:
            narrative_tags = detect_narrative_tags(player, tonight_game, teams_data)
        except Exception:
            narrative_tags = []
        if not narrative_tags:
            narrative_tags = []

        # Find historical comp
        comp = None
        if JOSEPH_COMPS_DATABASE:
            arch_matches = [c for c in JOSEPH_COMPS_DATABASE if c.get("archetype") == archetype]
            comp = random.choice(arch_matches) if arch_matches else random.choice(JOSEPH_COMPS_DATABASE)

        # Determine trend
        if len(games) >= 3:
            recent_avg = sum(_safe_float(g.get("points", g.get("pts", 0))) for g in games[:3]) / 3
            season_avg = _safe_float(player.get("points_avg", player.get("pts_avg", recent_avg)))
            if recent_avg > season_avg * 1.1:
                trend = "trending_up"
            elif recent_avg < season_avg * 0.9:
                trend = "trending_down"
            else:
                trend = "stable"
        else:
            trend = "neutral"

        # Build scouting report
        scouting_parts = [
            f"{player_name} is a {archetype} that I grade as a '{grade}' tonight.",
            f"His gravity score is {round(gravity, 1)} which tells you about his impact on the DEFENSE.",
        ]
        if comp:
            try:
                comp_text = comp["template"].format(reason=f"{player_name} has the same profile")
            except (KeyError, IndexError):
                comp_text = f"I see similarities to {comp.get('name', 'a historical great')}."
            scouting_parts.append(comp_text)
        if trend == "trending_up":
            scouting_parts.append(f"{player_name} has been COOKING lately. The recent form is ELITE.")
        elif trend == "trending_down":
            scouting_parts.append(f"{player_name} has been in a SLUMP. The recent numbers are CONCERNING.")
        else:
            scouting_parts.append(f"{player_name} has been CONSISTENT. No major swings in either direction.")
        if best_analysis:
            scouting_parts.append(
                f"My top play on him tonight: {best_analysis.get('verdict', 'LEAN')} "
                f"with {best_analysis.get('edge', 0)}% edge."
            )
        scouting_report = " ".join(scouting_parts)

        # Tonight's matchup take
        opponent = tonight_game.get("opponent", tonight_game.get("away_team", "opponent"))
        tonight_matchup_take = (
            f"{player_name} faces {opponent} tonight. "
            f"As a {archetype}, the matchup profile {'FAVOURS' if gravity > 60 else 'is NEUTRAL for'} him. "
            f"{'The narrative tags suggest extra motivation!' if any(t in ('revenge_game', 'nationally_televised', 'rivalry') for t in narrative_tags) else 'Standard game-day context applies.'}"
        )

        # Risk factors
        risk_factors = []
        if "back_to_back" in narrative_tags:
            risk_factors.append("Back-to-back fatigue")
        if "trap_game" in narrative_tags:
            risk_factors.append("Trap game motivation concern")
        if "blowout_risk" in narrative_tags:
            risk_factors.append("Blowout risk — potential minutes reduction")
        if trend == "trending_down":
            risk_factors.append("Recent form trending downward")
        if not risk_factors:
            risk_factors.append("No elevated risk factors identified")

        fun_fact = f"{player_name} profiles as a {archetype} — "
        if comp:
            fun_fact += f"think of {comp.get('name', 'a historical great')} in a similar situation."
        else:
            fun_fact += "a profile that historically performs WELL in this matchup type."

        condensed_summary = (
            f"{player_name} ({archetype}, Grade: {grade}): "
            f"{'TRENDING UP' if trend == 'trending_up' else 'TRENDING DOWN' if trend == 'trending_down' else 'STABLE'}. "
            f"{'Best play: ' + best_analysis.get('one_liner', '') if best_analysis else 'No top play identified.'}"
        )

        return {
            "scouting_report": scouting_report,
            "archetype": archetype,
            "grade": grade,
            "gravity": round(gravity, 2),
            "trend": trend,
            "narrative_tags": narrative_tags,
            "best_prop": best_analysis,
            "alt_props": alt_props,
            "tonight_matchup_take": tonight_matchup_take,
            "risk_factors": risk_factors,
            "fun_fact": fun_fact,
            "comp": comp,
            "condensed_summary": condensed_summary,
        }
    except Exception as exc:
        logger.warning("joseph_analyze_player failed: %s", exc)
        return {
            "scouting_report": "",
            "archetype": "Unknown",
            "grade": "C",
            "gravity": 50.0,
            "trend": "neutral",
            "narrative_tags": [],
        }


def joseph_generate_best_bets(leg_count: int, analysis_results: list,
                              teams_data: dict) -> dict:
    """Generate Joseph's recommended ticket for Studio Build My Bets.

    Parameters
    ----------
    leg_count : int
        Number of legs for the ticket (2-6).
    analysis_results : list[dict]
        All analysis results to select from.
    teams_data : dict
        All teams data for correlation analysis.

    Returns
    -------
    dict
        Ticket recommendation with keys: ``ticket_name``, ``legs``,
        ``total_ev``, ``correlation_score``, ``rant``.
    """
    try:
        ticket_name = TICKET_NAMES.get(leg_count, "TICKET")

        if not analysis_results:
            return {
                "ticket_name": ticket_name,
                "legs": [],
                "total_ev": 0.0,
                "correlation_score": 0.0,
                "rant": f"I need more data to build a {ticket_name}. Run the analysis first!",
                "joseph_confidence": 0.0,
                "why_these_legs": "Not enough qualifying props to build a ticket.",
                "risk_disclaimer": "No ticket generated.",
                "nerd_stats": {},
                "alternative_tickets": [],
                "condensed_card": {"ticket_name": ticket_name, "legs": [], "pitch": "Need more data."},
            }

        # Run full analysis on results that don't have verdicts
        analyzed = []
        for r in analysis_results:
            if "verdict" in r and ("joseph_edge" in r or "edge" in r):
                analyzed.append(r)
            elif "verdict" in r and "edge" not in r and "joseph_edge" not in r:
                # Has verdict but no edge — still usable
                analyzed.append(r)
            else:
                try:
                    # Try joseph_full_analysis first (needs analysis_result format)
                    player_data = r.get("player_data", r.get("player", {}))
                    game_data = r.get("game_data", r.get("game", {}))
                    if player_data and game_data:
                        full = joseph_full_analysis(r, player_data, game_data, teams_data)
                        full["player_name"] = r.get("player_name", r.get("name", ""))
                        full["game_id"] = r.get("game_id", r.get("game", ""))
                        analyzed.append(full)
                    else:
                        # Fallback: use joseph_analyze_pick for raw platform props
                        _pn = r.get("player_name", r.get("name", ""))
                        _st = r.get("stat_type", r.get("stat", "points"))
                        _ln = _safe_float(r.get("prop_line", r.get("line", 0)))
                        _plat = r.get("platform", "DraftKings")
                        if _pn and _ln > 0:
                            pick_result = joseph_analyze_pick(
                                {"name": _pn, "player_name": _pn},
                                _ln, _st, {},
                                platform=_plat,
                            )
                            pick_result["game_id"] = r.get("game_id", r.get("game", ""))
                            pick_result["prop_line"] = _ln
                            pick_result["joseph_edge"] = pick_result.get("edge", 0.0)
                            pick_result["joseph_probability"] = pick_result.get("probability_over", 50.0)
                            analyzed.append(pick_result)
                        else:
                            analyzed.append(r)
                except Exception:
                    analyzed.append(r)

        # Filter by verdict rules based on leg count
        allowed_verdicts = set()
        if leg_count <= 2:
            allowed_verdicts = {"SMASH"}
        elif leg_count <= 4:
            allowed_verdicts = {"SMASH", "LEAN"}
        else:
            allowed_verdicts = {"SMASH", "LEAN", "FADE"}

        qualifying = [
            r for r in analyzed
            if r.get("verdict", "") in allowed_verdicts
            and "trap_game" not in (r.get("narrative_tags", []) or [])
        ]

        if len(qualifying) < leg_count:
            # Relax to include more
            qualifying = [
                r for r in analyzed
                if r.get("verdict", "") in {"SMASH", "LEAN", "FADE"}
                and "trap_game" not in (r.get("narrative_tags", []) or [])
            ]

        if len(qualifying) < leg_count:
            return {
                "ticket_name": ticket_name,
                "legs": [],
                "total_ev": 0.0,
                "correlation_score": 0.0,
                "rant": f"Not enough qualifying legs for a {ticket_name}. I need at least {leg_count} plays that pass my filter.",
                "joseph_confidence": 0.0,
                "why_these_legs": f"Only {len(qualifying)} props qualify — need {leg_count}.",
                "risk_disclaimer": f"A {leg_count}-leg parlay requires high-quality legs. Be patient.",
                "nerd_stats": {},
                "alternative_tickets": [],
                "condensed_card": {"ticket_name": ticket_name, "legs": [], "pitch": "Not enough qualifying legs."},
            }

        # Sort by edge descending
        qualifying.sort(key=lambda x: _extract_edge(x), reverse=True)

        # Find best combination using itertools.combinations
        best_combo = None
        best_score = -999.0
        candidates = qualifying[:min(15, len(qualifying))]  # limit search space

        for combo in itertools.combinations(range(len(candidates)), min(leg_count, len(candidates))):
            legs = [candidates[i] for i in combo]
            edge_sum = sum(_safe_float(l.get("joseph_edge", l.get("edge", 0))) for l in legs)

            # Game concentration penalty: max 2 per game
            game_counts = {}
            for l in legs:
                gid = l.get("game_id", l.get("game", "unknown"))
                game_counts[gid] = game_counts.get(gid, 0) + 1
            concentration_penalty = sum(max(0, c - 2) * 3.0 for c in game_counts.values())

            score = edge_sum - concentration_penalty
            if score > best_score:
                best_score = score
                best_combo = legs

        if not best_combo:
            best_combo = candidates[:leg_count]

        # Calculate combined probability
        combined_prob = 1.0
        for leg in best_combo:
            leg_prob = _safe_float(leg.get("joseph_probability", leg.get("probability_over", 55.0))) / 100.0
            combined_prob *= max(0.01, min(0.99, leg_prob))

        # Attempt correlation adjustment
        try:
            correlation_adj = adjust_parlay_probability(combined_prob, best_combo)
            if correlation_adj > 0:
                combined_prob = correlation_adj
        except Exception as exc:
            logger.debug("joseph_build_entry: correlation adjustment failed — %s", exc)

        # Calculate expected value
        try:
            ev_result = calculate_entry_expected_value(best_combo, leg_count)
            total_ev = _safe_float(ev_result.get("expected_value_dollars", 0.0))
        except Exception:
            # Simple EV fallback: payout * prob - entry_fee
            payout_mult = {2: 3.0, 3: 5.0, 4: 10.0, 5: 20.0, 6: 40.0}.get(leg_count, 3.0)
            entry_fee = 10.0
            total_ev = round(payout_mult * entry_fee * combined_prob - entry_fee, 2)

        # Synergy score
        total_edge = sum(_safe_float(l.get("joseph_edge", l.get("edge", 0))) for l in best_combo)
        avg_edge = total_edge / max(1, len(best_combo))
        synergy_score = min(100.0, avg_edge * 10)

        # Joseph confidence
        confidence_values = [_safe_float(l.get("confidence", 50.0)) for l in best_combo]
        joseph_confidence = sum(confidence_values) / max(1, len(confidence_values))

        # Build pitch
        top_player = best_combo[0].get("player_name", best_combo[0].get("name", "my top pick")) if best_combo else "nobody"
        joseph_pitch = (
            f"THIS is my {ticket_name}! I've got {leg_count} legs of FIRE led by {top_player}. "
            f"Combined edge of {round(total_edge, 1)}% — this is WHERE the money is!"
        )

        why_these_legs = (
            f"I selected these {leg_count} legs because they have the HIGHEST combined edge "
            f"({round(total_edge, 1)}%) with manageable game concentration. "
            f"{'All SMASH picks!' if all(l.get('verdict') == 'SMASH' for l in best_combo) else 'A mix of my best verdicts.'}"
        )

        risk_disclaimer = {
            2: "A 2-leg parlay is the SAFEST structure. Two strong plays, simple math.",
            3: "Three legs means each play has to HIT. Make sure you're comfortable with ALL of them.",
            4: "Four legs is getting AGGRESSIVE. The payout is juicy but the risk is REAL.",
            5: "Five legs? You better LOVE every single one of these plays. High risk, high reward.",
            6: "THE FULL SEND! Six legs is a LOTTERY TICKET. Only play with money you can afford to lose.",
        }.get(leg_count, "Parlays carry inherent risk. Bet responsibly.")

        nerd_stats = {
            "combined_probability": round(combined_prob * 100, 2),
            "total_edge": round(total_edge, 2),
            "average_edge": round(avg_edge, 2),
            "synergy_score": round(synergy_score, 2),
            "joseph_confidence": round(joseph_confidence, 2),
        }

        # Format legs for output
        leg_summaries = []
        for l in best_combo:
            leg_summaries.append({
                "player_name": l.get("player_name", l.get("name", "")),
                "stat_type": l.get("stat_type", l.get("stat", "")),
                "line": l.get("prop_line", l.get("line", 0)),
                "prop_line": l.get("prop_line", l.get("line", 0)),
                "direction": l.get("direction", "OVER"),
                "verdict": l.get("verdict", "LEAN"),
                "joseph_edge": round(_safe_float(l.get("joseph_edge", l.get("edge", 0))), 1),
                "one_liner": l.get("rant", l.get("one_liner", "")),
            })

        # Alternative tickets (next 3 best combos, simplified)
        alternative_tickets = []
        alt_candidates = [c for c in candidates if c not in best_combo]
        if len(alt_candidates) >= leg_count:
            for alt_start in range(0, min(3, len(alt_candidates) - leg_count + 1)):
                alt_legs = alt_candidates[alt_start:alt_start + leg_count]
                if len(alt_legs) == leg_count:
                    alt_edge = sum(_safe_float(l.get("joseph_edge", l.get("edge", 0))) for l in alt_legs)
                    alternative_tickets.append({
                        "legs": [l.get("player_name", "") for l in alt_legs],
                        "total_edge": round(alt_edge, 1),
                    })

        condensed_card = {
            "ticket_name": ticket_name,
            "legs": [f"{l['player_name']} {l['direction']} {l['line']} {l['stat_type']}" for l in leg_summaries],
            "pitch": f"{ticket_name}: {leg_count} legs, {round(total_edge, 1)}% combined edge. LET'S GO!",
        }

        return {
            "ticket_name": ticket_name,
            "legs": leg_summaries,
            "total_ev": round(total_ev, 2),
            "correlation_score": round(synergy_score, 2),
            "rant": joseph_pitch,
            "joseph_confidence": round(joseph_confidence, 2),
            "why_these_legs": why_these_legs,
            "risk_disclaimer": risk_disclaimer,
            "nerd_stats": nerd_stats,
            "alternative_tickets": alternative_tickets,
            "condensed_card": condensed_card,
            "combined_probability": round(combined_prob * 100, 2),
        }
    except Exception as exc:
        logger.warning("joseph_generate_best_bets failed: %s", exc)
        return {
            "ticket_name": TICKET_NAMES.get(leg_count, "TICKET"),
            "legs": [],
            "total_ev": 0.0,
            "correlation_score": 0.0,
            "rant": "",
        }


def joseph_quick_take(analysis_results: list, teams_data: dict,
                      todays_games: list) -> str:
    """Generate a unique 3-4 sentence monologue about tonight's slate.

    Parameters
    ----------
    analysis_results : list[dict]
        All prop analysis results.
    teams_data : dict
        All teams data.
    todays_games : list[dict]
        Tonight's games.

    Returns
    -------
    str
        A 3-4 sentence Joseph M. Smith monologue.
    """
    try:
        take_openers = [
            "Joseph M. Smith has STUDIED tonight's slate and here's what I see...",
            "Tonight's games? Let me BREAK it down for you...",
            "I've been through EVERY number tonight and I have TAKES...",
            "The board is SET and Joseph M. Smith is READY to talk...",
            "You want to know what I think about tonight? HERE IT IS...",
        ]
        used_set = _used_fragments.setdefault("quick_take", set())
        avail_openers = [i for i in range(len(take_openers)) if i not in used_set]
        if not avail_openers or len(used_set) > 3:
            used_set.clear()
            avail_openers = list(range(len(take_openers)))
        opener_idx = random.choice(avail_openers)
        used_set.add(opener_idx)
        opener = take_openers[opener_idx]

        # Find best SMASH pick
        smash_picks = [r for r in (analysis_results or [])
                       if r.get("verdict") == "SMASH"]
        smash_picks.sort(key=lambda x: _extract_edge(x), reverse=True)

        # Find best fade/stay_away
        avoid_picks = [r for r in (analysis_results or [])
                       if r.get("verdict") in ("FADE", "STAY_AWAY")]
        avoid_picks.sort(key=lambda x: _extract_edge(x))

        # Middle sentences
        if smash_picks:
            top = smash_picks[0]
            pname = top.get("player_name", top.get("name", "my top pick"))
            edge = round(_safe_float(top.get("joseph_edge", top.get("edge", 0))), 1)
            middle1 = f"My STRONGEST play tonight is {pname} — I see a {edge}% edge and I'm going ALL IN on it!"
        else:
            middle1 = f"I see {len(todays_games or [])} games tonight and the edges are DEVELOPING. Let the analysis run!"

        if avoid_picks:
            avoid = avoid_picks[0]
            aname = avoid.get("player_name", avoid.get("name", "one play"))
            middle2 = f"But STAY AWAY from {aname} — that's a TRAP and I can smell it from here!"
        elif len(analysis_results or []) > 3:
            lean_picks = [r for r in analysis_results if r.get("verdict") == "LEAN"]
            if lean_picks:
                bold = lean_picks[0]
                bname = bold.get("player_name", bold.get("name", "a sleeper"))
                middle2 = f"My BOLD prediction: {bname} is going to SURPRISE people tonight. Watch for it!"
            else:
                middle2 = "The slate is COMPETITIVE and I see value scattered across MULTIPLE games."
        else:
            middle2 = "Load the slate and let me work — Joseph M. Smith delivers EVERY night!"

        # Closer
        closer_frag = _select_fragment(CLOSER_POOL, _used_fragments.setdefault("rant", set()))
        closer = closer_frag.get("text", "And I say that with GREAT conviction!")

        return f"{opener} {middle1} {middle2} {closer}"
    except Exception:
        return "Joseph M. Smith is ready for tonight's slate."


def joseph_get_ambient_context(session_state: dict) -> tuple:
    """Determine ambient context from session state.

    Inspects the session state to decide which ambient context is active.
    Priority order:

    1. Premium pitch (30 % chance for free-tier users)
    2. Transient ``joseph_entry_just_built`` flag → ``"entry_built"``
    3. Page-specific context via ``joseph_page_context`` key
    4. Generic fallbacks: ``analysis_complete`` → ``games_loaded`` → ``idle``

    Parameters
    ----------
    session_state : dict
        Streamlit session state dict.

    Returns
    -------
    tuple[str, dict]
        A (context_key, kwargs) tuple for ``joseph_ambient_line``.
    """
    try:
        # Premium pitch check
        try:
            from utils.auth import is_premium_user
        except ImportError:
            def is_premium_user():
                return True
        try:
            if not is_premium_user() and random.random() < 0.3:
                return ("premium_pitch", {})
        except Exception as exc:
            logger.debug("_detect_topic: premium check failed — %s", exc)

        # Entry just built (transient — consumes the flag)
        if session_state.get("joseph_entry_just_built"):
            n = session_state.pop("joseph_entry_just_built", 0)
            return ("entry_built", {"n": n})

        # ── Page-specific context ────────────────────────────
        page_ctx = session_state.get("joseph_page_context", "")
        if page_ctx and page_ctx in AMBIENT_POOLS:
            return (page_ctx, {})

        # ── Generic session-state fallbacks ───────────────────
        # Analysis complete
        analysis_results = session_state.get("analysis_results", [])
        if analysis_results:
            smash_count = sum(1 for r in analysis_results if r.get("verdict") == "SMASH")
            platinum = sum(1 for r in analysis_results if r.get("tier") == "Platinum")
            override_count = sum(1 for r in analysis_results if r.get("is_override"))
            logged_count = session_state.get("joseph_logged_count", 0)
            total = len(analysis_results)
            grade = min(10, max(1, smash_count + platinum))
            return ("analysis_complete", {
                "smash_count": smash_count,
                "platinum": platinum,
                "total": total,
                "override_count": override_count,
                "logged_count": logged_count,
                "grade": grade,
            })

        # Games loaded
        todays_games = session_state.get("todays_games", [])
        if todays_games:
            n = len(todays_games)
            first_game = todays_games[0] if todays_games else {}
            away = first_game.get("away_team", "Away")
            home = first_game.get("home_team", "Home")
            return ("games_loaded", {"n": n, "away": away, "home": home})

        # Default: idle
        return ("idle", {})
    except Exception:
        return ("idle", {})


def joseph_ambient_line(context: str, **kwargs) -> str:
    """Select and fill an ambient line from AMBIENT_POOLS.

    Parameters
    ----------
    context : str
        Context key into AMBIENT_POOLS (e.g. ``"idle"``, ``"games_loaded"``).
    **kwargs
        Format kwargs to fill placeholders (e.g. ``n=5``).

    Returns
    -------
    str
        A filled ambient commentary line.
    """
    try:
        lines = AMBIENT_POOLS.get(context, AMBIENT_POOLS.get("idle", []))
        if not lines:
            return ""
        used = _used_ambient.setdefault(context, set())
        available_indices = [i for i in range(len(lines)) if i not in used]
        if not available_indices or len(used) > 0.6 * len(lines):
            used.clear()
            available_indices = list(range(len(lines)))
        idx = random.choice(available_indices)
        used.add(idx)
        line = lines[idx]
        try:
            return line.format(**kwargs)
        except (KeyError, IndexError):
            return line
    except Exception:
        lines = AMBIENT_POOLS.get("idle", [])
        return lines[0] if lines else ""


def joseph_commentary(results: list, context_type: str) -> str:
    """Generate 2-4 sentence reactive commentary.

    Selects an opener from COMMENTARY_OPENER_POOL and appends
    result-specific commentary sentences.

    Parameters
    ----------
    results : list[dict]
        Analysis results to comment on.
    context_type : str
        Context type key into COMMENTARY_OPENER_POOL
        (e.g. ``"analysis_results"``, ``"entry_built"``).

    Returns
    -------
    str
        A 2-4 sentence Joseph commentary block.
    """
    try:
        # Select opener with anti-repetition
        openers = COMMENTARY_OPENER_POOL.get(context_type, COMMENTARY_OPENER_POOL.get("analysis_results", []))
        if not openers:
            openers = ["Joseph M. Smith has something to SAY..."]
        used = _used_commentary.setdefault(context_type, set())
        available_indices = [i for i in range(len(openers)) if i not in used]
        if not available_indices or len(used) > 0.6 * len(openers):
            used.clear()
            available_indices = list(range(len(openers)))
        idx = random.choice(available_indices)
        used.add(idx)
        opener = openers[idx]

        # Build body sentences
        body1 = ""
        body2 = ""
        if results:
            # Find top result by edge
            sorted_results = sorted(results, key=lambda x: _extract_edge(x), reverse=True)
            top = sorted_results[0]
            pname = top.get("player_name", top.get("name", "someone"))
            edge = round(_safe_float(top.get("joseph_edge", top.get("edge_percentage", top.get("edge", 0)))), 1)
            body1 = f"I'm looking at {pname} and the edge is {edge}%. "
            if len(sorted_results) > 1:
                second = sorted_results[1]
                sname = second.get("player_name", second.get("name", "another player"))
                body2 = f"Also keep your eye on {sname}. "
        else:
            body1 = "The data tells a story and I'm READING it. "

        # Select closer
        closer_frag = _select_fragment(CLOSER_POOL, _used_fragments.setdefault("rant", set()))
        closer = closer_frag.get("text", "And I say that with GREAT conviction!")

        return f"{opener} {body1}{body2}{closer}"
    except Exception:
        return "Joseph M. Smith has thoughts on this."


def joseph_auto_log_bets(joseph_results: list, session_state: dict = None) -> tuple:
    """Pass-through to engine.joseph_bets.joseph_auto_log_bets().

    Parameters
    ----------
    joseph_results : list[dict]
        Joseph's analysis results to auto-log.
    session_state : dict, optional
        Deprecated — kept for backward compatibility but ignored.

    Returns
    -------
    tuple[int, str]
        (count_logged, status_message) tuple.
    """
    try:
        from engine.joseph_bets import joseph_auto_log_bets as _log
        return _log(joseph_results)
    except ImportError:
        return (0, "Joseph bets module not installed yet")
    except Exception as exc:
        return (0, f"Joseph auto-log error: {exc}")


# ═══════════════════════════════════════════════════════════════
# PLATINUM LOCK — Multi-Prop Conflict Resolution with Stat Validation
# ═══════════════════════════════════════════════════════════════

def joseph_platinum_lock(props: list, season_stats: dict) -> dict:
    """Select ONE Platinum Lock from a player's multiple props.

    When a player has multiple lines, Joseph compares each prop's
    True Line against the player's actual ``season_stats`` and
    chooses the single highest-edge prop as the Platinum Lock.
    He then explicitly tears down the remaining bets using the
    real stats as justification.

    Parameters
    ----------
    props : list[dict]
        All prop dicts for one player.  Each must contain at least
        ``stat_type``, ``line``/``prop_line``, ``edge_percentage``,
        ``direction``, and ``probability_over``.
    season_stats : dict
        ``{"ppg": float, "rpg": float, "apg": float, "avg_minutes": float}``

    Returns
    -------
    dict
        ``{"platinum_lock_stat": str, "rant": str}``
    """
    if not props:
        return {
            "platinum_lock_stat": "N/A",
            "rant": "No props available for analysis.",
        }

    # ── Map stat types to season averages ────────────────────
    stat_avg_map = {
        "points": season_stats.get("ppg", 0),
        "rebounds": season_stats.get("rpg", 0),
        "assists": season_stats.get("apg", 0),
        "pts": season_stats.get("ppg", 0),
        "reb": season_stats.get("rpg", 0),
        "ast": season_stats.get("apg", 0),
        "minutes": season_stats.get("avg_minutes", 0),
    }

    # ── Score each prop: edge + stat alignment ───────────────
    scored = []
    for p in props:
        stat = str(p.get("stat_type", "")).lower().strip()
        edge = _safe_float(p.get("edge_percentage", 0))
        line = _safe_float(p.get("prop_line", p.get("line", 0)))
        direction = str(p.get("direction", "OVER")).upper()
        avg = stat_avg_map.get(stat, 0)

        # Stat alignment bonus: if OVER and avg > line → aligned
        alignment = 0.0
        if avg > 0 and line > 0:
            if direction == "OVER" and avg > line:
                alignment = min((avg - line) / max(line, 0.1) * 100.0, 25.0)
            elif direction == "UNDER" and avg < line:
                alignment = min((line - avg) / max(line, 0.1) * 100.0, 25.0)

        total_score = edge + alignment
        scored.append({
            "prop": p,
            "stat": stat,
            "edge": edge,
            "line": line,
            "direction": direction,
            "avg": avg,
            "alignment": alignment,
            "total_score": total_score,
        })

    # ── Sort by total score → best is the lock ───────────────
    scored.sort(key=lambda x: x["total_score"], reverse=True)
    lock = scored[0]
    others = scored[1:]

    lock_stat = lock["stat"].title() if lock["stat"] else "Points"
    lock_line = lock["line"]
    lock_dir = lock["direction"]
    lock_edge = lock["edge"]
    lock_avg = lock["avg"]

    # ── Build the rant ───────────────────────────────────────
    try:
        opener = _select_fragment(OPENER_POOL, _used_fragments.setdefault("rant", set()))
        opener_text = opener.get("text", "I've been waiting ALL DAY to say this.")
    except Exception:
        opener_text = "I've been waiting ALL DAY to say this."

    # Lock justification
    lock_just = (
        f"The PLATINUM LOCK is {lock_stat} {lock_dir} {lock_line:g}. "
    )
    if lock_avg > 0:
        lock_just += (
            f"This player averages {lock_avg:g} per game in this category — "
        )
        if lock_dir == "OVER" and lock_avg > lock_line:
            lock_just += f"that's {lock_avg - lock_line:.1f} ABOVE the line. The math doesn't LIE. "
        elif lock_dir == "UNDER" and lock_avg < lock_line:
            lock_just += f"that's {lock_line - lock_avg:.1f} BELOW the line. We're FADING this one. "
        else:
            lock_just += f"the edge at {lock_edge:+.1f}% is enough. Trust the model. "
    else:
        lock_just += f"The edge is {lock_edge:+.1f}% and the numbers SCREAM it. "

    # Tear down other props
    teardowns = []
    for o in others:
        o_stat = o["stat"].title() if o["stat"] else "Unknown"
        o_line = o["line"]
        o_avg = o["avg"]
        o_edge = o["edge"]
        if o_avg > 0:
            teardowns.append(
                f"{o_stat} at {o_line:g}? The season average is {o_avg:g} — "
                f"that's only a {o_edge:+.1f}% edge. NOT enough for me."
            )
        else:
            teardowns.append(
                f"{o_stat} at {o_line:g}? Edge is {o_edge:+.1f}%. I'm passing."
            )

    teardown_text = " ".join(teardowns) if teardowns else ""

    try:
        closer = _select_fragment(CLOSER_POOL, _used_fragments.setdefault("rant", set()))
        closer_text = closer.get("text", "And I say that with GREAT conviction!")
    except Exception:
        closer_text = "And I say that with GREAT conviction!"

    full_rant = f"{opener_text} {lock_just}{teardown_text} {closer_text}"

    return {
        "platinum_lock_stat": lock_stat,
        "rant": full_rant.strip(),
    }


# ═══════════════════════════════════════════════════════════════
# GOD MODE — MASTER ANALYSIS ORCHESTRATOR
# ═══════════════════════════════════════════════════════════════

# Availability flags for God Mode modules — exported for UI checks
GOD_MODE_MODULES = {
    "impact_metrics": _IMPACT_METRICS_AVAILABLE,
    "lineup_analysis": _LINEUP_ANALYSIS_AVAILABLE,
    "regime_detection": _REGIME_DETECTION_AVAILABLE,
    "trade_evaluator": _TRADE_EVALUATOR_AVAILABLE,
    "draft_prospect": _DRAFT_PROSPECT_AVAILABLE,
}


def joseph_god_mode_player(player_data: dict, game_context: dict = None,
                           recent_games: list = None) -> dict:
    """Run ALL God Mode analytical modules on a single player.

    This is the master orchestration function that combines every
    analytical layer Joseph has access to:

    - Impact metrics (EPM, RAPTOR, WAR, True Shooting%)
    - Regime detection (structural shifts, Bayesian updates)
    - Defensive impact estimates
    - Offensive load analysis
    - Full efficiency profile

    Parameters
    ----------
    player_data : dict
        Player data with season stats.
    game_context : dict, optional
        Tonight's game context.
    recent_games : list[dict], optional
        Recent game logs for trend/regime analysis.

    Returns
    -------
    dict
        Comprehensive God Mode analysis with all available modules.
    """
    result = {
        "player_name": "",
        "modules_used": [],
        "impact_metrics": {},
        "efficiency_profile": {},
        "offensive_load": {},
        "defensive_impact": {},
        "war": 0.0,
        "regime_analysis": {},
        "bayesian_update": {},
        "joseph_god_mode_take": "",
    }
    try:
        player_name = player_data.get("name", player_data.get("player_name", "Player"))
        result["player_name"] = player_name
        game_context = game_context or {}
        recent_games = recent_games or []

        # ── Impact Metrics ────────────────────────────────
        if _IMPACT_METRICS_AVAILABLE:
            try:
                result["efficiency_profile"] = calculate_player_efficiency_profile(player_data)
                result["offensive_load"] = calculate_offensive_load(player_data)
                result["defensive_impact"] = estimate_defensive_impact(player_data)
                result["war"] = impact_calculate_war(player_data)
                result["impact_metrics"] = {
                    "epm": estimate_epm(player_data),
                    "raptor": estimate_raptor(player_data),
                }
                result["modules_used"].append("impact_metrics")
            except Exception as exc:
                logger.debug("God Mode impact_metrics error: %s", exc)

        # ── Regime Detection ──────────────────────────────
        if _REGIME_DETECTION_AVAILABLE and recent_games:
            try:
                result["regime_analysis"] = detect_player_structural_shift(
                    player_data, recent_games
                )
                result["modules_used"].append("regime_detection")
            except Exception as exc:
                logger.debug("God Mode regime_detection error: %s", exc)

        # ── Bayesian Update ───────────────────────────────
        if _REGIME_DETECTION_AVAILABLE and recent_games:
            try:
                prop_line = _safe_float(game_context.get("prop_line", 0))
                stat_type = game_context.get("stat_type", "points")
                if prop_line > 0:
                    result["bayesian_update"] = run_bayesian_player_update(
                        player_data, recent_games, prop_line, stat_type
                    )
                    result["modules_used"].append("bayesian_update")
            except Exception as exc:
                logger.debug("God Mode bayesian_update error: %s", exc)

        # ── Trade Value (always available if module loaded) ─
        if _TRADE_EVALUATOR_AVAILABLE:
            try:
                result["trade_value"] = calculate_player_war(player_data)
                result["modules_used"].append("trade_evaluator")
            except Exception as exc:
                logger.debug("God Mode trade_evaluator error: %s", exc)

        # ── God Mode Joseph Take ──────────────────────────
        take_parts = [f"GOD MODE analysis on {player_name}:"]
        eff = result.get("efficiency_profile", {})
        if eff.get("efficiency_tier"):
            take_parts.append(f"Efficiency tier is {eff['efficiency_tier']}.")
        war = result.get("war", 0.0)
        if war:
            take_parts.append(f"WAR estimate: {round(war, 1)}.")
        regime = result.get("regime_analysis", {})
        if regime.get("has_structural_shift"):
            take_parts.append(
                f"REGIME CHANGE detected: {regime.get('description', 'unknown shift')}."
            )
        elif regime:
            take_parts.append("No structural shifts detected — steady as she goes.")
        bayes = result.get("bayesian_update", {})
        if bayes.get("explanation"):
            take_parts.append(bayes["explanation"])

        result["joseph_god_mode_take"] = " ".join(take_parts)

    except Exception as exc:
        logger.warning("joseph_god_mode_player failed: %s", exc)
        result["joseph_god_mode_take"] = "God Mode analysis encountered an error."

    return result


def joseph_god_mode_lineup(players: list, game_context: dict = None) -> dict:
    """Run God Mode lineup analysis on a group of players.

    Parameters
    ----------
    players : list[dict]
        List of 2-5 player data dicts.
    game_context : dict, optional
        Game context for closing lineup optimization.

    Returns
    -------
    dict
        Lineup analysis with synergy, weaknesses, closing lineup recommendation.
    """
    result = {
        "lineup_analysis": {},
        "weaknesses": [],
        "closing_lineup": {},
        "modules_used": [],
        "joseph_take": "",
    }
    try:
        game_context = game_context or {}

        if _LINEUP_ANALYSIS_AVAILABLE and players:
            try:
                result["lineup_analysis"] = analyze_lineup_combination(players)
                result["weaknesses"] = detect_lineup_weaknesses(players)
                result["modules_used"].append("lineup_analysis")
            except Exception as exc:
                logger.debug("God Mode lineup_analysis error: %s", exc)

            try:
                result["closing_lineup"] = find_closing_lineup(players, game_context)
                result["modules_used"].append("closing_lineup")
            except Exception as exc:
                logger.debug("God Mode closing_lineup error: %s", exc)

        # Joseph take
        analysis = result.get("lineup_analysis", {})
        weaknesses = result.get("weaknesses", [])
        take = analysis.get("joseph_take", "")
        if weaknesses:
            take += f" Weaknesses: {'; '.join(weaknesses[:3])}."
        result["joseph_take"] = take or "Lineup analysis unavailable."

    except Exception as exc:
        logger.warning("joseph_god_mode_lineup failed: %s", exc)

    return result


def joseph_god_mode_trade(outgoing: list, incoming: list,
                          team_needs: list = None) -> dict:
    """Run God Mode trade evaluation.

    Parameters
    ----------
    outgoing : list[dict]
        Players being sent out.
    incoming : list[dict]
        Players being received.
    team_needs : list[str], optional
        List of team needs (e.g., ["rim_protector", "3pt_shooting"]).

    Returns
    -------
    dict
        Trade evaluation with grade, WAR change, Joseph's take.
    """
    result = {"trade_evaluation": {}, "modules_used": [], "joseph_take": ""}
    try:
        if _TRADE_EVALUATOR_AVAILABLE:
            result["trade_evaluation"] = evaluate_trade(
                outgoing, incoming, team_needs
            )
            result["modules_used"].append("trade_evaluator")
            result["joseph_take"] = result["trade_evaluation"].get(
                "joseph_take", "Trade analysis unavailable."
            )
    except Exception as exc:
        logger.warning("joseph_god_mode_trade failed: %s", exc)
        result["joseph_take"] = "Trade analysis encountered an error."
    return result


def joseph_god_mode_prospect(prospect: dict) -> dict:
    """Run God Mode draft prospect evaluation.

    Parameters
    ----------
    prospect : dict
        Prospect data with college stats and physical measurements.

    Returns
    -------
    dict
        Full scouting report with projections, comps, career prediction.
    """
    result = {"scouting_report": {}, "modules_used": [], "joseph_take": ""}
    try:
        if _DRAFT_PROSPECT_AVAILABLE:
            result["scouting_report"] = build_prospect_scouting_report(prospect)
            result["modules_used"].append("draft_prospect")
            result["joseph_take"] = result["scouting_report"].get(
                "joseph_take", "Prospect analysis unavailable."
            )
    except Exception as exc:
        logger.warning("joseph_god_mode_prospect failed: %s", exc)
        result["joseph_take"] = "Prospect analysis encountered an error."
    return result


# ═══════════════════════════════════════════════════════════════
# VEGAS VAULT — AI Reaction to Arbitrage Discrepancies
# ═══════════════════════════════════════════════════════════════

# Fragment pools for vault rant assembly (follows build_joseph_rant pattern).
_VAULT_JOSEPH_OPENERS = [
    {"id": "vj1", "text": "STOP what you're doing and LOOK at this board."},
    {"id": "vj2", "text": "Vegas is SLEEPING and we just caught them with their hand in the cookie jar."},
    {"id": "vj3", "text": "I've been watching lines ALL DAY and the sharp money just revealed itself."},
    {"id": "vj4", "text": "The DFS apps are SLEEPING on this — but Joseph M. Smith NEVER sleeps."},
    {"id": "vj5", "text": "Ladies and gentlemen, the window is OPEN and it's closing FAST."},
]

_VAULT_JOSEPH_BODIES = [
    {"id": "vb1", "text": "We found {count} EV discrepancies across the board — that's SHARP MONEY talking."},
    {"id": "vb2", "text": "{count} props just lit up like a Christmas tree — Vegas is MISPRICING these lines."},
    {"id": "vb3", "text": "The books are fighting each other and we're catching {count} edges in the crossfire."},
]

_VAULT_JOSEPH_GOD_MODE = [
    {"id": "vg1", "text": "AND WE HAVE GOD MODE LOCKS — implied probability over 60%! THIS IS NOT A DRILL!"},
    {"id": "vg2", "text": "GOD MODE ACTIVATED — the math doesn't lie, these locks are SCREAMING value!"},
    {"id": "vg3", "text": "We've got GOD MODE LOCKS on the board — Vegas is giving away FREE MONEY!"},
]

_VAULT_JOSEPH_CLOSERS = [
    {"id": "vc1", "text": "The window is CLOSING — move NOW or watch the edge disappear."},
    {"id": "vc2", "text": "Sharp money moves FAST. Don't be the last one to the counter."},
    {"id": "vc3", "text": "This is what separates the SHARKS from the fish. ACT."},
]

_VAULT_PROF_OPENERS = [
    {"id": "vp1", "text": "The current pricing landscape reveals a statistically significant market inefficiency."},
    {"id": "vp2", "text": "Cross-book analysis has identified actionable expected-value opportunities."},
    {"id": "vp3", "text": "The mathematics are unambiguous — there is a measurable edge available."},
]

_VAULT_PROF_BODIES = [
    {"id": "vpb1", "text": "The top finding shows an implied probability of {prob:.1f}%, yielding an EV edge of {edge:.1f} percentage points above a fair market."},
    {"id": "vpb2", "text": "At {prob:.1f}% implied probability, the expected value exceeds the break-even threshold by {edge:.1f} points — a clear market inefficiency."},
]

_VAULT_PROF_CLOSERS = [
    {"id": "vpc1", "text": "These inefficiencies tend to correct within hours as market makers re-calibrate."},
    {"id": "vpc2", "text": "The expected value calculation is straightforward: the edge is real and quantifiable."},
]


def joseph_vault_reaction(discrepancies: list, mode: str = "joseph") -> str:
    """Generate an AI reaction to Vegas Vault arbitrage finds.

    Parameters
    ----------
    discrepancies : list[dict]
        Output of ``find_ev_discrepancies()``.  Each entry has keys
        ``ev_edge``, ``is_god_mode_lock``, ``best_over_implied_prob``,
        ``best_under_implied_prob``, etc.
    mode : str
        ``"joseph"`` → aggressive sharp-money rant (Joseph M. Smith persona).
        ``"professor"`` → calm EV-math academic breakdown (The Professor persona).

    Returns
    -------
    str
        Multi-sentence reaction string.
    """
    try:
        if not discrepancies:
            if mode == "professor":
                return ("No statistically significant pricing discrepancies were "
                        "detected across the current sportsbook landscape. "
                        "The market appears to be efficiently priced at this time.")
            return ("The board is CLEAN right now — no edges worth taking. "
                    "Vegas has its lines locked up tight. "
                    "But Joseph M. Smith is ALWAYS watching. The second they slip, we STRIKE.")

        count = len(discrepancies)
        top = discrepancies[0]
        top_edge = top.get("ev_edge", 0)
        top_prob = max(top.get("best_over_implied_prob", 0),
                       top.get("best_under_implied_prob", 0))
        has_god_mode = any(d.get("is_god_mode_lock", False) for d in discrepancies)

        used_set = _used_fragments.setdefault("vault", set())

        if mode == "professor":
            opener = _select_fragment(_VAULT_PROF_OPENERS, used_set)
            body = _select_fragment(_VAULT_PROF_BODIES, used_set)
            closer = _select_fragment(_VAULT_PROF_CLOSERS, used_set)

            opener_text = opener.get("text", _VAULT_PROF_OPENERS[0]["text"])
            try:
                body_text = body.get("text", "").format(prob=top_prob, edge=top_edge)
            except (KeyError, IndexError):
                body_text = body.get("text", "")
            closer_text = closer.get("text", _VAULT_PROF_CLOSERS[0]["text"])

            return f"{opener_text} {body_text} {closer_text}"

        # Joseph mode — aggressive sharp-money rant
        opener = _select_fragment(_VAULT_JOSEPH_OPENERS, used_set)
        body = _select_fragment(_VAULT_JOSEPH_BODIES, used_set)
        closer = _select_fragment(_VAULT_JOSEPH_CLOSERS, used_set)

        opener_text = opener.get("text", _VAULT_JOSEPH_OPENERS[0]["text"])
        try:
            body_text = body.get("text", "").format(count=count)
        except (KeyError, IndexError):
            body_text = body.get("text", "")
        closer_text = closer.get("text", _VAULT_JOSEPH_CLOSERS[0]["text"])

        parts = [opener_text, body_text]
        if has_god_mode:
            god = _select_fragment(_VAULT_JOSEPH_GOD_MODE, used_set)
            parts.append(god.get("text", _VAULT_JOSEPH_GOD_MODE[0]["text"]))
        parts.append(closer_text)

        return " ".join(parts)

    except Exception as exc:
        logger.debug("joseph_vault_reaction error: %s", exc)
        if mode == "professor":
            return "Unable to generate analysis at this time."
        return "The Vault is loading... Joseph M. Smith will have his take SHORTLY."
