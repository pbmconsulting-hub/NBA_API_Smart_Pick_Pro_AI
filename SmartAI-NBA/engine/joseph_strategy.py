# ============================================================
# FILE: engine/joseph_strategy.py
# PURPOSE: Game strategy analysis — scheme detection, mismatches, pace
# CONNECTS TO: data/advanced_metrics.py, engine/joseph_eval.py
# ============================================================

import logging
import math
import random

_logger = logging.getLogger(__name__)

try:
    from data.advanced_metrics import normalize, detect_narrative_tags, classify_player_archetype
except ImportError:
    _logger.warning("[JosephStrategy] Could not import from advanced_metrics")

    def normalize(value, min_val, max_val, out_min=0.0, out_max=100.0):
        if max_val == min_val:
            return out_min
        clamped = max(min_val, min(max_val, value))
        return out_min + (clamped - min_val) / (max_val - min_val) * (out_max - out_min)

    def detect_narrative_tags(player, game, teams):
        return []

    def classify_player_archetype(player):
        return "Role Player"

try:
    from engine.math_helpers import _safe_float
except ImportError:
    def _safe_float(value, fallback=0.0):
        try:
            v = float(value)
            if math.isfinite(v):
                return v
            return float(fallback)
        except (ValueError, TypeError):
            return float(fallback)


# ── League Averages ─────────────────────────────────────────
LEAGUE_AVG_PACE = 99.5
LEAGUE_AVG_DEF_RATING = 112.0
LEAGUE_AVG_OFF_RATING = 112.0


# ── Mismatch Rule Condition Helpers ─────────────────────────

def _rule_stretch_big_vs_drop(archetype, player_data, opp_scheme, **kwargs):
    """Stretch Big feasts against drop coverage."""
    return archetype == "Stretch Big" and opp_scheme.get("primary_scheme") == "drop"


def _rule_creator_vs_no_switch(archetype, player_data, opp_scheme, **kwargs):
    """Primary Creator / Alpha Scorer exploits low switch-rate defenses."""
    return (
        archetype in ("Primary Creator", "Alpha Scorer")
        and _safe_float(opp_scheme.get("switch_rate_est", 1)) < 0.4
    )


def _rule_scorer_vs_bottom10_d(archetype, player_data, opp_scheme, **kwargs):
    """Volume scorers thrive against bottom-10 defenses."""
    return (
        archetype in ("Alpha Scorer", "Volume Scorer")
        and _safe_float(opp_scheme.get("def_rating_raw", 112)) > 115
    )


def _rule_floor_general_vs_blitz(archetype, player_data, opp_scheme, **kwargs):
    """Floor Generals pick apart blitz schemes."""
    return archetype == "Floor General" and opp_scheme.get("primary_scheme") == "blitz"


def _rule_3andd_vs_corner3_d(archetype, player_data, opp_scheme, **kwargs):
    """3-and-D Wings exploit weak three-point defense."""
    return (
        archetype == "3-and-D Wing"
        and _safe_float(opp_scheme.get("three_pt_defense", 50)) < 40
    )


def _rule_energy_big_vs_b2b(archetype, player_data, opp_scheme, **kwargs):
    """Energy Bigs dominate when the opponent is on a back-to-back."""
    return archetype == "Energy Big" and kwargs.get("opp_back_to_back", False)


def _rule_sixth_man_vs_weak_bench(archetype, player_data, opp_scheme, **kwargs):
    """Sixth Man types feast against weak opposing bench units."""
    return (
        archetype in ("Sixth Man Spark", "Microwave Scorer")
        and _safe_float(opp_scheme.get("bench_rating", 110)) > 112
    )


def _rule_rim_protector_vs_paint(archetype, player_data, opp_scheme, **kwargs):
    """Rim Protectors shine against paint-heavy offenses."""
    return (
        archetype == "Rim Protector"
        and _safe_float(opp_scheme.get("opp_paint_fg_pct", 0.50)) > 0.54
    )


def _rule_alpha_in_blowout(archetype, player_data, opp_scheme, **kwargs):
    """Alpha Scorers lose value in blowout-risk games (minutes cut)."""
    return archetype == "Alpha Scorer" and kwargs.get("blowout_prob", 0) > 0.35


def _rule_two_way_in_rivalry(archetype, player_data, opp_scheme, **kwargs):
    """Two-Way Forces elevate in rivalry matchups."""
    return (
        archetype == "Two-Way Force"
        and "rivalry" in kwargs.get("narrative_tags", [])
    )


def _rule_microwave_vs_zone(archetype, player_data, opp_scheme, **kwargs):
    """Microwave Scorers exploit zone coverage soft spots."""
    return archetype == "Microwave Scorer" and opp_scheme.get("primary_scheme") == "zone"


def _rule_glue_guy_in_grind(archetype, player_data, opp_scheme, **kwargs):
    """Glue Guys thrive in low-pace grind games."""
    return (
        archetype == "Glue Guy"
        and kwargs.get("pace_label", "") in ("grind", "rock_fight")
    )


# ── Mismatch Rules Table ───────────────────────────────────

MISMATCH_RULES = [
    {
        "name": "Stretch Big vs Drop",
        "condition": _rule_stretch_big_vs_drop,
        "edge_boost": 1.5,
        "direction_bias": None,
        "rant_templates": [
            "{player} is going to PARK himself behind the arc and the {team} defense will DROP. That's a THREE-POINT BUFFET!",
            "You're going to DROP coverage against {player}? That's COACHING MALPRACTICE! He'll shoot you OUT of the gym!",
            "A Stretch Big against drop coverage? I've been doing this since I was TWELVE and I know MONEY when I see it!",
        ],
    },
    {
        "name": "Creator vs No-Switch",
        "condition": _rule_creator_vs_no_switch,
        "edge_boost": 1.0,
        "direction_bias": None,
        "rant_templates": [
            "{player} in the pick and roll against a team that WON'T switch? He's going to COOK all night!",
            "No switching? Against {player}? That's like leaving your FRONT DOOR open and hoping nobody walks in!",
            "{player} sees no switch and he SALIVATES. The mismatch is AUTOMATIC!",
        ],
    },
    {
        "name": "Scorer vs Bottom-10 D",
        "condition": _rule_scorer_vs_bottom10_d,
        "edge_boost": 1.0,
        "direction_bias": None,
        "rant_templates": [
            "You think {team}'s defense can STOP {player}? They're bottom 10 in the LEAGUE! He's going to FEAST!",
            "{player} against a bottom-10 defense? I'm not ASKING you to take this — I'm TELLING you!",
            "A {archetype} against THAT defense? The numbers don't LIE — this is a SMASH!",
        ],
    },
    {
        "name": "Floor General vs Blitz",
        "condition": _rule_floor_general_vs_blitz,
        "edge_boost": 0.8,
        "direction_bias": None,
        "rant_templates": [
            "You're going to BLITZ {player}? He has EYES in the back of his head! Assists are going UP!",
            "A Floor General against a blitz-heavy team? That's {player}'s DREAM matchup!",
            "{player} THRIVES when you send extra pressure. He'll find the open man EVERY time!",
        ],
    },
    {
        "name": "3-and-D vs Corner-3 D",
        "condition": _rule_3andd_vs_corner3_d,
        "edge_boost": 0.8,
        "direction_bias": None,
        "rant_templates": [
            "{player} is going to be WIDE OPEN all night. {team} can't guard the three!",
            "Corner threes against a team that gives up OPEN looks? {player} says THANK YOU!",
            "That's a 3-and-D Wing against a team ranked BOTTOM in three-point defense. MONEY!",
        ],
    },
    {
        "name": "Energy Big vs B2B",
        "condition": _rule_energy_big_vs_b2b,
        "edge_boost": 0.7,
        "direction_bias": None,
        "rant_templates": [
            "FRESH LEGS against DEAD LEGS! {player} is going to DOMINATE the glass tonight!",
            "{team} on a back-to-back? {player} is going to OUT-HUSTLE them from the OPENING tip!",
            "An Energy Big with rest against a tired team? That's a REBOUNDING bonanza!",
        ],
    },
    {
        "name": "Sixth Man vs Weak Bench",
        "condition": _rule_sixth_man_vs_weak_bench,
        "edge_boost": 0.7,
        "direction_bias": None,
        "rant_templates": [
            "{player} is going to EAT against those second-unit SCRUBS!",
            "The bench matchup is where {player} THRIVES. He's going to go OFF!",
            "When {player} checks in and sees THAT bench unit? Oh, it's OVER!",
        ],
    },
    {
        "name": "Rim Protector vs Paint",
        "condition": _rule_rim_protector_vs_paint,
        "edge_boost": 1.0,
        "direction_bias": "UNDER",
        "rant_templates": [
            "GOOD LUCK at the rim with {player} standing there! He's a WALL!",
            "{player} is going to send back EVERYTHING at the rim. Take the UNDER on paint points!",
            "A Rim Protector against a paint-heavy offense? That's BLOCKED SHOTS all night!",
        ],
    },
    {
        "name": "Alpha in Blowout Risk",
        "condition": _rule_alpha_in_blowout,
        "edge_boost": -1.5,
        "direction_bias": "UNDER",
        "rant_templates": [
            "{player} might be on the BENCH by the fourth quarter. Blowout risk is REAL!",
            "When the game is over by Q3, {player} sits. That's a MINUTES problem!",
            "Blowout games KILL star props. {player} won't need to play 35 minutes if they're up 25!",
        ],
    },
    {
        "name": "Two-Way in Rivalry",
        "condition": _rule_two_way_in_rivalry,
        "edge_boost": 0.5,
        "direction_bias": None,
        "rant_templates": [
            "{player} shows up on BOTH ENDS in rivalry games. The intensity goes UP!",
            "Rivalry night? {player} is going to play like it's GAME 7!",
            "Two-Way Forces in rivalry games are DIFFERENT. The effort level is MAXIMUM!",
        ],
    },
    {
        "name": "Microwave vs Zone",
        "condition": _rule_microwave_vs_zone,
        "edge_boost": 0.5,
        "direction_bias": None,
        "rant_templates": [
            "Zone defense can't stop a MICROWAVE! {player} will find the soft spots!",
            "{player} against a zone? He's going to TORCH it from the mid-range!",
            "You play zone against a Microwave Scorer? That's DISRESPECTFUL — and he'll make you PAY!",
        ],
    },
    {
        "name": "Glue Guy in Grind Game",
        "condition": _rule_glue_guy_in_grind,
        "edge_boost": 0.5,
        "direction_bias": None,
        "rant_templates": [
            "This is HIS type of game! Slow, physical, every possession MATTERS!",
            "Glue Guys THRIVE in grind games. {player} does all the LITTLE things!",
            "Low-pace game? {player}'s hustle stats go THROUGH the roof!",
        ],
    },
]


# ── Core Functions ──────────────────────────────────────────

def detect_defensive_scheme(team_data: dict) -> dict:
    """Analyse a team's defensive tendencies and classify their primary scheme.

    Reads pace, defensive rating, and opponent shooting splits to determine
    whether the team primarily plays switch, drop, hedge, blitz, or zone
    defense.  All component ratings are normalised to a 0-100 scale.

    Args:
        team_data: Dictionary of team-level stats.  Expected keys include
            ``pace``, ``def_rating``, ``off_rating``, ``opp_fg3_pct``, and
            ``opp_paint_fg_pct``.  Missing keys fall back to league-average
            defaults.

    Returns:
        A dictionary describing the defensive scheme with keys:
            primary_scheme, switch_rate_est, drop_rate_est, rim_protection,
            perimeter_defense, three_pt_defense, paint_protection,
            transition_defense, def_rating_raw, opp_paint_fg_pct,
            bench_rating.
    """
    try:
        pace = _safe_float(team_data.get("pace", LEAGUE_AVG_PACE), LEAGUE_AVG_PACE)
        def_rating = _safe_float(
            team_data.get("def_rating", LEAGUE_AVG_DEF_RATING), LEAGUE_AVG_DEF_RATING
        )
        off_rating = _safe_float(
            team_data.get("off_rating", LEAGUE_AVG_OFF_RATING), LEAGUE_AVG_OFF_RATING
        )
        opp_fg3_pct = _safe_float(team_data.get("opp_fg3_pct", 0.36), 0.36)
        opp_paint_fg_pct = _safe_float(
            team_data.get("opp_paint_fg_pct", 0.50), 0.50
        )
        bench_rating = _safe_float(team_data.get("bench_rating", 110.0), 110.0)

        # ── Primary scheme determination ────────────────────
        if opp_fg3_pct < 0.34 and opp_paint_fg_pct > 0.52:
            primary_scheme = "switch"
        elif opp_paint_fg_pct < 0.48 and opp_fg3_pct > 0.36:
            primary_scheme = "drop"
        elif def_rating < 108:
            primary_scheme = "hedge"
        elif pace > 102:
            primary_scheme = "blitz"
        else:
            primary_scheme = "zone"

        # ── Component ratings (0-100 scale) ─────────────────
        rim_protection = normalize(1.0 - opp_paint_fg_pct, 0.40, 0.60, 0, 100)
        perimeter_defense = normalize(1.0 - opp_fg3_pct, 0.55, 0.75, 0, 100)
        three_pt_defense = normalize(0.40 - opp_fg3_pct, -0.05, 0.10, 0, 100)
        paint_protection = normalize(0.55 - opp_paint_fg_pct, -0.05, 0.10, 0, 100)
        transition_defense = normalize(115.0 - def_rating, 0, 15, 0, 100)

        # ── Switch / drop rate estimates based on scheme ────
        scheme_rates = {
            "switch": (0.70, 0.10),
            "drop": (0.15, 0.65),
            "hedge": (0.35, 0.30),
            "blitz": (0.25, 0.20),
            "zone": (0.10, 0.25),
        }
        switch_rate_est, drop_rate_est = scheme_rates.get(
            primary_scheme, (0.25, 0.30)
        )

        return {
            "primary_scheme": primary_scheme,
            "switch_rate_est": switch_rate_est,
            "drop_rate_est": drop_rate_est,
            "rim_protection": rim_protection,
            "perimeter_defense": perimeter_defense,
            "three_pt_defense": three_pt_defense,
            "paint_protection": paint_protection,
            "transition_defense": transition_defense,
            "def_rating_raw": def_rating,
            "opp_paint_fg_pct": opp_paint_fg_pct,
            "bench_rating": bench_rating,
        }
    except Exception as exc:
        _logger.error("[JosephStrategy] detect_defensive_scheme error: %s", exc)
        return {
            "primary_scheme": "zone",
            "switch_rate_est": 0.25,
            "drop_rate_est": 0.30,
            "rim_protection": 50.0,
            "perimeter_defense": 50.0,
            "three_pt_defense": 50.0,
            "paint_protection": 50.0,
            "transition_defense": 50.0,
            "def_rating_raw": LEAGUE_AVG_DEF_RATING,
            "opp_paint_fg_pct": 0.50,
            "bench_rating": 110.0,
        }


def apply_mismatch_rules(
    player: dict,
    opp_scheme: dict,
    game_strategy: dict,
    narrative_tags: list,
) -> list:
    """Evaluate every mismatch rule and return triggered edges.

    Each rule's callable ``condition`` is invoked with the player's archetype,
    the raw player dictionary, the opponent's defensive-scheme dictionary, and
    supplementary keyword arguments (narrative_tags, pace_label,
    blowout_prob).  Triggered rules produce a rant string and an edge boost.

    Args:
        player: Player data dictionary.  Must contain ``player_name``,
            ``team``, and ``archetype`` (or ``position`` for fallback
            classification).
        opp_scheme: Opponent defensive scheme dictionary produced by
            :func:`detect_defensive_scheme`.
        game_strategy: Game-level strategy dictionary produced by
            :func:`analyze_game_strategy`.  Supplies ``pace_label``,
            ``blowout_probability``, and related context.
        narrative_tags: List of narrative tag strings (e.g. ``"rivalry"``).

    Returns:
        A list of triggered-mismatch dictionaries, each containing ``name``,
        ``edge_boost``, ``direction_bias``, and ``rant``.
    """
    triggered = []
    try:
        player_name = player.get("player_name", player.get("name", "Unknown"))
        opp_team = player.get("opp_team", player.get("team", "OPP"))
        archetype = player.get("archetype", "")

        if not archetype:
            archetype = classify_player_archetype(player)

        pace_label = game_strategy.get("pace_label", "average")
        blowout_prob = _safe_float(
            game_strategy.get("blowout_probability", 0), 0
        )

        kwargs = {
            "narrative_tags": narrative_tags,
            "pace_label": pace_label,
            "blowout_prob": blowout_prob,
            "opp_back_to_back": game_strategy.get("opp_back_to_back", False),
        }

        for rule in MISMATCH_RULES:
            try:
                condition_fn = rule["condition"]
                if condition_fn(archetype, player, opp_scheme, **kwargs):
                    rant = random.choice(rule["rant_templates"]).format(
                        player=player_name,
                        team=opp_team,
                        archetype=archetype,
                        stat="",
                    )
                    triggered.append(
                        {
                            "name": rule["name"],
                            "edge_boost": rule["edge_boost"],
                            "direction_bias": rule["direction_bias"],
                            "rant": rant,
                        }
                    )
            except Exception as rule_exc:
                _logger.warning(
                    "[JosephStrategy] Rule '%s' failed: %s",
                    rule.get("name", "?"),
                    rule_exc,
                )
    except Exception as exc:
        _logger.error("[JosephStrategy] apply_mismatch_rules error: %s", exc)
    return triggered


def analyze_game_strategy(
    home_team: str,
    away_team: str,
    game: dict,
    teams_data: list,
) -> dict:
    """Build a comprehensive game-strategy profile for a matchup.

    Combines pace projections, point-total estimates, spread calculations,
    blowout / overtime probabilities, and defensive-scheme analysis for both
    teams into a single strategy dictionary used downstream by the evaluation
    engine.

    Args:
        home_team: Home-team abbreviation (e.g. ``"LAL"``).
        away_team: Away-team abbreviation (e.g. ``"BOS"``).
        game: Game-level metadata dictionary (date, venue, etc.).
        teams_data: A list of team dictionaries.  Each dict should include at
            minimum an ``abbreviation`` (or ``team``) key along with
            ``pace``, ``off_rating``, and ``def_rating``.

    Returns:
        A strategy dictionary with pace projection, total estimate, spread
        estimate, blowout / overtime probabilities, garbage-time estimate,
        defensive schemes for both teams, narrative summary, and a betting
        angle string.
    """
    try:
        home_data = _find_team_data(home_team, teams_data)
        away_data = _find_team_data(away_team, teams_data)

        # ── Pace projection ─────────────────────────────────
        home_pace = _safe_float(
            home_data.get("pace", LEAGUE_AVG_PACE), LEAGUE_AVG_PACE
        )
        away_pace = _safe_float(
            away_data.get("pace", LEAGUE_AVG_PACE), LEAGUE_AVG_PACE
        )
        pace_projection = (home_pace + away_pace) / 2.0

        # ── Pace label ──────────────────────────────────────
        if pace_projection > 106:
            pace_label = "shootout"
        elif pace_projection > 102:
            pace_label = "uptempo"
        elif pace_projection > 98:
            pace_label = "average"
        elif pace_projection > 94:
            pace_label = "grind"
        else:
            pace_label = "rock_fight"

        # ── Offensive / defensive ratings ───────────────────
        home_off = _safe_float(
            home_data.get("off_rating", LEAGUE_AVG_OFF_RATING),
            LEAGUE_AVG_OFF_RATING,
        )
        home_def = _safe_float(
            home_data.get("def_rating", LEAGUE_AVG_DEF_RATING),
            LEAGUE_AVG_DEF_RATING,
        )
        away_off = _safe_float(
            away_data.get("off_rating", LEAGUE_AVG_OFF_RATING),
            LEAGUE_AVG_OFF_RATING,
        )
        away_def = _safe_float(
            away_data.get("def_rating", LEAGUE_AVG_DEF_RATING),
            LEAGUE_AVG_DEF_RATING,
        )

        # ── Game total estimate ─────────────────────────────
        game_total_est = pace_projection * (home_off + away_off) / 100.0

        # ── Spread estimate (negative = home favored) ───────
        home_edge = (
            (home_off - away_def) - (away_off - home_def) + 3.5
        )
        spread_est = -home_edge

        # ── Blowout probability ─────────────────────────────
        abs_spread = abs(spread_est)
        if abs_spread > 10:
            blowout_probability = 0.40
        elif abs_spread > 7:
            blowout_probability = 0.25
        elif abs_spread > 4:
            blowout_probability = 0.15
        else:
            blowout_probability = 0.08

        # ── Overtime probability ────────────────────────────
        if abs_spread < 3:
            overtime_probability = 0.06
        elif abs_spread < 5:
            overtime_probability = 0.04
        else:
            overtime_probability = 0.02

        # ── Garbage-time minutes estimate ───────────────────
        garbage_time_minutes_est = blowout_probability * 8.0

        # ── Defensive schemes ───────────────────────────────
        home_scheme = detect_defensive_scheme(home_data)
        away_scheme = detect_defensive_scheme(away_data)

        # ── Game narrative ──────────────────────────────────
        game_narrative = _build_game_narrative(
            home_team,
            away_team,
            pace_label,
            pace_projection,
            spread_est,
            game_total_est,
            blowout_probability,
        )

        # ── Betting angle ───────────────────────────────────
        betting_angle = _determine_betting_angle(
            blowout_probability, pace_label, spread_est
        )

        return {
            "pace_projection": round(pace_projection, 2),
            "pace_label": pace_label,
            "game_total_est": round(game_total_est, 1),
            "spread_est": round(spread_est, 1),
            "blowout_probability": round(blowout_probability, 3),
            "overtime_probability": round(overtime_probability, 3),
            "garbage_time_minutes_est": round(garbage_time_minutes_est, 1),
            "home_scheme": home_scheme,
            "away_scheme": away_scheme,
            "scheme_matchups": [],
            "key_player_matchups": [],
            "game_narrative": game_narrative,
            "betting_angle": betting_angle,
        }
    except Exception as exc:
        _logger.error("[JosephStrategy] analyze_game_strategy error: %s", exc)
        return {
            "pace_projection": LEAGUE_AVG_PACE,
            "pace_label": "average",
            "game_total_est": 224.0,
            "spread_est": 0.0,
            "blowout_probability": 0.08,
            "overtime_probability": 0.04,
            "garbage_time_minutes_est": 0.64,
            "home_scheme": detect_defensive_scheme({}),
            "away_scheme": detect_defensive_scheme({}),
            "scheme_matchups": [],
            "key_player_matchups": [],
            "game_narrative": "Unable to generate game narrative.",
            "betting_angle": "Look for PLAYER PROPS — the game script is neutral",
        }


# ── Private Helpers ─────────────────────────────────────────

def _find_team_data(abbreviation: str, teams_data) -> dict:
    """Look up a team dictionary by abbreviation from the teams data.

    Args:
        abbreviation: Team abbreviation string (e.g. ``"LAL"``).
        teams_data: A list of team dicts **or** a dict keyed by
            abbreviation.  Both formats are accepted so callers
            (e.g. The Studio, which converts to a dict) work
            without conversion.

    Returns:
        The matching team dictionary, or an empty dict if not found.
    """
    try:
        if not teams_data:
            return {}
        abbr_upper = abbreviation.upper()

        # ── Dict path: keyed by abbreviation ────────────────
        if isinstance(teams_data, dict):
            # Try exact key first, then case-insensitive scan
            if abbreviation in teams_data:
                return teams_data[abbreviation]
            for key, val in teams_data.items():
                if key.upper() == abbr_upper:
                    return val
            _logger.warning(
                "[JosephStrategy] Team '%s' not found in teams_data dict",
                abbreviation,
            )
            return {}

        # ── List path: original iteration ───────────────────
        for team in teams_data:
            team_abbr = team.get("abbreviation", team.get("team", ""))
            if team_abbr.upper() == abbr_upper:
                return team
        _logger.warning(
            "[JosephStrategy] Team '%s' not found in teams_data", abbreviation
        )
    except Exception:
        _logger.debug("[JosephStrategy] _find_team_data error for '%s'", abbreviation)
    return {}


def _classify_player_archetype(player: dict) -> str:
    """Rudimentary archetype classification based on available player stats.

    This is a lightweight fallback used when the player dictionary does not
    already carry an ``archetype`` key.  It inspects common stat fields
    (``ppg``, ``apg``, ``rpg``, ``fg3_pct``, ``blk``) to make a best guess.

    Args:
        player: Player data dictionary.

    Returns:
        An archetype string such as ``"Alpha Scorer"`` or ``"Glue Guy"``.
    """
    try:
        ppg = _safe_float(player.get("ppg", player.get("pts", 0)), 0)
        apg = _safe_float(player.get("apg", player.get("ast", 0)), 0)
        rpg = _safe_float(player.get("rpg", player.get("reb", 0)), 0)
        fg3_pct = _safe_float(player.get("fg3_pct", 0), 0)
        blk = _safe_float(player.get("blk", player.get("blocks", 0)), 0)
        stl = _safe_float(player.get("stl", player.get("steals", 0)), 0)
        position = player.get("position", "").upper()

        if ppg >= 25 and apg >= 7:
            return "Primary Creator"
        if ppg >= 25:
            return "Alpha Scorer"
        if ppg >= 18 and apg >= 6:
            return "Floor General"
        if ppg >= 18:
            return "Volume Scorer"
        if apg >= 8:
            return "Floor General"
        if blk >= 2.0 and position in ("C", "PF", "C-PF", "PF-C"):
            return "Rim Protector"
        if fg3_pct >= 0.38 and rpg >= 5 and position in ("SF", "SG", "SF-SG", "SG-SF"):
            return "3-and-D Wing"
        if rpg >= 10 and position in ("C", "PF", "C-PF", "PF-C"):
            if fg3_pct >= 0.33:
                return "Stretch Big"
            return "Energy Big"
        if ppg >= 13 and stl >= 1.5:
            return "Two-Way Force"
        if ppg >= 12 and player.get("starter", True) is False:
            return "Sixth Man Spark"
        if ppg >= 10 and fg3_pct >= 0.37:
            return "Microwave Scorer"
        return "Glue Guy"
    except Exception as exc:
        _logger.warning(
            "[JosephStrategy] _classify_player_archetype error: %s", exc
        )
        return "Glue Guy"


def _build_game_narrative(
    home_team: str,
    away_team: str,
    pace_label: str,
    pace_projection: float,
    spread_est: float,
    game_total_est: float,
    blowout_probability: float,
) -> str:
    """Create a 3-5 sentence narrative preview of the game.

    Args:
        home_team: Home-team abbreviation.
        away_team: Away-team abbreviation.
        pace_label: Pace category string.
        pace_projection: Projected possessions per game.
        spread_est: Estimated point spread (negative = home favored).
        game_total_est: Estimated game total points.
        blowout_probability: Probability of a blowout.

    Returns:
        A multi-sentence narrative string.
    """
    try:
        sentences = []

        # Sentence 1: matchup intro
        sentences.append(
            f"{away_team} visits {home_team} in what projects as a "
            f"{pace_label.replace('_', '-')}-paced contest."
        )

        # Sentence 2: pace detail
        sentences.append(
            f"The combined pace projection sits at {pace_projection:.1f} possessions, "
            f"pointing toward a game total around {game_total_est:.0f} points."
        )

        # Sentence 3: spread context
        if spread_est < -5:
            sentences.append(
                f"{home_team} is a solid favorite with an estimated spread of {spread_est:.1f}."
            )
        elif spread_est > 5:
            sentences.append(
                f"{away_team} comes in as the favorite with {home_team} getting "
                f"{abs(spread_est):.1f} points."
            )
        else:
            sentences.append(
                f"The spread of {spread_est:.1f} signals a closely contested matchup."
            )

        # Sentence 4: blowout risk
        if blowout_probability > 0.30:
            sentences.append(
                "There is a meaningful blowout risk, which could limit star "
                "minutes in the fourth quarter."
            )
        elif blowout_probability > 0.15:
            sentences.append(
                "A moderate blowout chance exists — monitor the game script for "
                "garbage-time implications."
            )
        else:
            sentences.append(
                "Both teams should stay competitive throughout, keeping starters "
                "engaged deep into the game."
            )

        # Sentence 5: wrap-up
        sentences.append(
            "Adjust player prop targets accordingly based on pace and game flow."
        )

        return " ".join(sentences)
    except Exception:
        _logger.debug("[JosephStrategy] _build_game_narrative error")
        return f"{away_team} at {home_team} — game narrative unavailable."


def _determine_betting_angle(
    blowout_probability: float,
    pace_label: str,
    spread_est: float,
) -> str:
    """Select the primary betting angle for this game.

    Args:
        blowout_probability: Probability of a blowout.
        pace_label: Pace category string.
        spread_est: Estimated point spread.

    Returns:
        A single betting-angle recommendation string.
    """
    try:
        if blowout_probability > 0.35:
            return "Watch for GARBAGE TIME — star minutes could be limited"
        if pace_label in ("shootout", "uptempo"):
            return "The TOTAL is the play — this game is going FAST"
        if abs(spread_est) > 7:
            return "Take the DOG — blowout spreads are hard to cover"
        if pace_label in ("grind", "rock_fight"):
            return "UNDER is the angle — expect a defensive battle"
    except Exception:
        _logger.debug("[JosephStrategy] _determine_betting_angle error")
    return "Look for PLAYER PROPS — the game script is neutral"
