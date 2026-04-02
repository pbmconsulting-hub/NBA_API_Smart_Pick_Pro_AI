# ============================================================
# FILE: data/advanced_metrics.py
# PURPOSE: Enrich raw player dicts with God-Mode derived metrics
# CONNECTS TO: data/data_manager.py, data/validators.py, engine/joseph_eval.py
# ============================================================

import math
import os
import datetime
import copy

try:
    from utils.logger import get_logger
    _logger = get_logger(__name__)
except ImportError:
    import logging
    _logger = logging.getLogger(__name__)

# ============================================================
# SECTION: External Imports (graceful fallbacks)
# ============================================================

try:
    from data.data_manager import load_players_data, load_teams_data
except ImportError:
    _logger.warning("[AdvMetrics] Could not import data_manager functions")
    def load_players_data(): return []
    def load_teams_data(): return []

try:
    from engine.math_helpers import _safe_float
except ImportError:
    def _safe_float(value, fallback=0.0):
        """Convert *value* to float; return *fallback* on failure or non-finite."""
        try:
            v = float(value)
            if math.isfinite(v):
                return v
            return float(fallback)
        except (ValueError, TypeError):
            return float(fallback)

# ============================================================
# SECTION: Constants
# ============================================================

REVENGE_MATCHUPS: dict = {
    "LeBron James": ["CLE", "MIA"],
    "James Harden": ["HOU", "BKN", "PHI"],
    "Kevin Durant": ["OKC", "GSW"],
    "Russell Westbrook": ["OKC", "HOU", "WAS", "LAL"],
    "Kyrie Irving": ["CLE", "BOS", "BKN"],
    "Jimmy Butler": ["CHI", "MIN", "PHI"],
    "Paul George": ["IND", "OKC", "LAC"],
    "DeMar DeRozan": ["TOR", "SAS", "CHI"],
    "Kyle Lowry": ["TOR"],
    "Pascal Siakam": ["TOR"],
    "Chris Paul": ["LAC", "HOU", "OKC", "PHX"],
    "Dejounte Murray": ["SAS", "ATL"],
    "Donovan Mitchell": ["UTA"],
    "Rudy Gobert": ["UTA"],
    "Jrue Holiday": ["NOP", "MIL"],
    "Bradley Beal": ["WAS"],
    "Kristaps Porzingis": ["NYK", "DAL"],
    "Tobias Harris": ["LAC", "PHI"],
    "Julius Randle": ["NYK"],
    "Zach LaVine": ["MIN"],
    "D'Angelo Russell": ["LAL", "MIN", "BKN"],
    "Derrick Rose": ["CHI", "MIN", "CLE"],
    "Spencer Dinwiddie": ["BKN", "WAS", "DAL"],
    "Lauri Markkanen": ["CHI", "CLE"],
    "Domantas Sabonis": ["IND"],
    "Tyrese Haliburton": ["SAC"],
    "Malcolm Brogdon": ["MIL", "IND"],
    "Caris LeVert": ["BKN", "IND"],
    "Terry Rozier": ["BOS"],
    "Montrezl Harrell": ["LAC", "LAL", "WAS"],
    "Dennis Schroder": ["ATL", "OKC", "LAL", "BOS"],
    "Lonzo Ball": ["LAL", "NOP"],
    "Brandon Ingram": ["LAL"],
    "Josh Hart": ["LAL", "NOP", "POR"],
    "Jarrett Allen": ["BKN"],
    "Robert Covington": ["PHI", "MIN", "HOU", "POR"],
    "Aaron Gordon": ["ORL"],
    "Nikola Vucevic": ["ORL"],
    "OG Anunoby": ["TOR", "IND"],
    "Immanuel Quickley": ["NYK"],
    "RJ Barrett": ["NYK"],
    "Mikal Bridges": ["PHX", "BKN"],
    "Cameron Johnson": ["PHX", "BKN"],
}

EXPIRING_CONTRACTS_2026: set = {
    "Jimmy Butler", "James Harden", "LeBron James", "Chris Paul",
    "Kyle Lowry", "DeMar DeRozan", "Russell Westbrook", "Bradley Beal",
    "Zach LaVine", "D'Angelo Russell", "Spencer Dinwiddie", "Derrick Rose",
    "Tobias Harris", "Mike Conley", "Al Horford", "Brook Lopez",
    "Bobby Portis", "Donte DiVincenzo", "Gary Trent Jr", "Norman Powell",
    "Tim Hardaway Jr", "Malik Beasley", "Kelly Oubre Jr", "Josh Richardson",
    "Reggie Jackson", "Patrick Beverley", "Kentavious Caldwell-Pope",
    "Buddy Hield", "Bojan Bogdanovic", "Marcus Smart",
}

RIVALRY_PAIRS: set = {
    frozenset(("BOS", "LAL")), frozenset(("NYK", "BKN")), frozenset(("LAL", "LAC")),
    frozenset(("CHI", "DET")), frozenset(("MIA", "NYK")), frozenset(("PHI", "BOS")),
    frozenset(("GSW", "LAL")), frozenset(("GSW", "CLE")), frozenset(("DAL", "HOU")),
    frozenset(("MIL", "CHI")), frozenset(("PHX", "LAL")), frozenset(("DEN", "MIN")),
}

COACH_REVENGE: dict = {
    "Erik Spoelstra": [],
    "Doc Rivers": ["LAC", "PHI"],
    "Mike Budenholzer": ["ATL", "MIL"],
    "Tom Thibodeau": ["CHI", "MIN"],
    "Ty Lue": ["CLE"],
    "Jason Kidd": ["MIL", "BKN"],
    "Steve Nash": ["BKN"],
    "Monty Williams": ["PHX"],
    "Nick Nurse": ["TOR"],
    "Ime Udoka": ["BOS"],
    "Chauncey Billups": [],
    "Nate McMillan": ["IND", "POR"],
}

# "SLC" is kept as an alias — some game feeds use city codes instead of team codes
ALTITUDE_MAP: dict = {"DEN": 1.03, "UTA": 1.015, "SLC": 1.015, "PHX": 1.005}

TIMEZONE_MAP: dict = {
    # Eastern = 0
    "ATL": 0, "BOS": 0, "BKN": 0, "CHA": 0, "CHI": 0, "CLE": 0,
    "DET": 0, "IND": 0, "MIA": 0, "MIL": 0, "NYK": 0, "ORL": 0,
    "PHI": 0, "TOR": 0, "WAS": 0,
    # Central = 1
    "DAL": 1, "HOU": 1, "MEM": 1, "MIN": 1, "NOP": 1, "OKC": 1, "SAS": 1,
    # Mountain = 2
    "DEN": 2, "UTA": 2, "PHX": 2,
    # Pacific = 3
    "GSW": 3, "LAC": 3, "LAL": 3, "POR": 3, "SAC": 3,
}


# ============================================================
# SECTION: Utility Functions
# ============================================================

# Multiplier approximates team-possession share for a single player
_USAGE_RATE_MULTIPLIER = 2.0


def _estimate_usage_rate(fga: float, fta: float, tov: float,
                         minutes: float) -> float:
    """Simplified per-player usage-rate estimate.

    Formula: ``100 * (FGA + 0.44*FTA + TOV) / max(MIN, 1) * multiplier``

    The 2.0 multiplier scales individual possessions into a percentage
    that approximates the share of team possessions used while on court.

    Args:
        fga:     Field-goal attempts per game.
        fta:     Free-throw attempts per game.
        tov:     Turnovers per game.
        minutes: Minutes per game.

    Returns:
        float: Estimated usage rate percentage.
    """
    return 100.0 * (fga + 0.44 * fta + tov) / (max(minutes, 1) * _USAGE_RATE_MULTIPLIER)


def normalize(value: float, min_val: float, max_val: float,
              out_min: float = 0.0, out_max: float = 100.0) -> float:
    """Standard min-max normalization with clamping.

    Args:
        value:   The raw value to normalize.
        min_val: Lower bound of the input range.
        max_val: Upper bound of the input range.
        out_min: Lower bound of the output range.
        out_max: Upper bound of the output range.

    Returns:
        float: The normalized value clamped to [out_min, out_max].
    """
    if max_val == min_val:
        return out_min
    ratio = (value - min_val) / (max_val - min_val)
    result = out_min + ratio * (out_max - out_min)
    return max(out_min, min(out_max, result))


# ============================================================
# SECTION: Classification
# ============================================================

def classify_player_archetype(player: dict) -> str:
    """Classify a player into one of 13 archetypes based on their stats.

    The archetypes are checked in priority order; the first match wins.

    Args:
        player: A player dictionary with stat keys such as
                ``points_avg``, ``assists_avg``, ``rebounds_avg``, etc.

    Returns:
        str: One of 13 archetype labels (e.g. ``"Alpha Scorer"``).
    """
    try:
        pts = _safe_float(player.get("points_avg", 0))
        ast = _safe_float(player.get("assists_avg", 0))
        reb = _safe_float(player.get("rebounds_avg", 0))
        stl = _safe_float(player.get("steals_avg", 0))
        blk = _safe_float(player.get("blocks_avg", 0))
        tov = _safe_float(player.get("turnovers_avg", 0))
        fg3_pct = _safe_float(player.get("fg3_pct", 0))
        fg3a = _safe_float(player.get("fg3a", 0))
        fga = _safe_float(player.get("fga", 0))
        fta = _safe_float(player.get("fta", 0))
        minutes = _safe_float(player.get("minutes_avg", 0))
        dreb = _safe_float(player.get("defensive_rebounds_avg", 0))

        position = player.get("position", "").upper()
        starter = player.get("starter", True)

        stocks = stl + blk

        # Simplified usage-rate estimate
        usage_rate_est = _estimate_usage_rate(fga, fta, tov, minutes)

        # Assist-to-turnover ratio
        assist_to_turnover = ast / max(tov, 0.5)

        # 1. Alpha Scorer
        if pts > 25 and usage_rate_est > 30:
            return "Alpha Scorer"

        # 2. Primary Creator
        if usage_rate_est > 25 and ast > 5.5:
            return "Primary Creator"

        # 3. Volume Scorer
        if pts > 20 and usage_rate_est > 26:
            return "Volume Scorer"

        # 4. Floor General
        if ast > 7.5 and assist_to_turnover > 2.5:
            return "Floor General"

        # 5. 3-and-D Wing
        if fg3_pct > 0.355 and position in ("SG", "SF") and stocks > 1.5:
            return "3-and-D Wing"

        # 6. Stretch Big
        if position in ("C", "PF") and fg3a > 2.5:
            return "Stretch Big"

        # 7. Rim Protector
        if position in ("C", "PF") and blk > 1.5:
            return "Rim Protector"

        # 8. Two-Way Force
        if pts > 15 and stocks > 2.0:
            return "Two-Way Force"

        # 9. Energy Big
        if position in ("C", "PF") and reb > 8 and stocks > 1.5:
            return "Energy Big"

        # 10. Sixth Man Spark
        if not starter and pts > 13:
            return "Sixth Man Spark"

        # 11. Microwave Scorer
        if not starter and pts > 10 and fg3a > 2:
            return "Microwave Scorer"

        # 12. Glue Guy
        if ast > 3 and reb > 4 and stocks > 1.5:
            return "Glue Guy"

        # 13. Fallback
        return "Role Player"

    except Exception as exc:
        _logger.warning("[AdvMetrics] classify_player_archetype error: %s", exc)
        return "Role Player"


# ============================================================
# SECTION: Narrative Tags
# ============================================================

def detect_narrative_tags(player: dict, game: dict, teams: dict) -> list:
    """Detect contextual narrative tags for a player/game combination.

    Each tag check is wrapped in its own try/except so that a single
    failure never prevents other tags from being detected.

    Args:
        player: Player dictionary with at least ``name`` and ``team``.
        game:   Game dictionary with keys like ``home_team``, ``away_team``,
                ``spread``, ``broadcast``, ``city``, ``coach``, etc.
        teams:  Mapping of team abbreviations to team info dicts (with
                ``pace``, ``wins``, ``losses``, etc.).

    Returns:
        list: A list of string tags (e.g. ``["revenge_game", "rivalry"]``).
    """
    tags: list = []
    player_name = player.get("name", "")
    player_team = player.get("team", "")
    home_team = game.get("home_team", "")
    away_team = game.get("away_team", "")
    opponent = away_team if player_team == home_team else home_team

    # 1. revenge_game
    try:
        former_teams = REVENGE_MATCHUPS.get(player_name, [])
        if former_teams and opponent in former_teams:
            tags.append("revenge_game")
    except Exception as exc:
        _logger.debug("[AdvMetrics] revenge_game check failed: %s", exc)

    # 2. contract_year
    try:
        if player_name in EXPIRING_CONTRACTS_2026:
            tags.append("contract_year")
    except Exception as exc:
        _logger.debug("[AdvMetrics] contract_year check failed: %s", exc)

    # 3. trap_game
    try:
        spread = _safe_float(game.get("spread", 0))
        if spread >= 7:
            tags.append("trap_game")
    except Exception as exc:
        _logger.debug("[AdvMetrics] trap_game check failed: %s", exc)

    # 4. nationally_televised
    try:
        broadcast = str(game.get("broadcast", "")).upper()
        if any(net in broadcast for net in ("ESPN", "TNT", "ABC")):
            tags.append("nationally_televised")
    except Exception as exc:
        _logger.debug("[AdvMetrics] nationally_televised check failed: %s", exc)

    # 5. rivalry
    try:
        pair = frozenset((home_team, away_team))
        if pair in RIVALRY_PAIRS:
            tags.append("rivalry")
    except Exception as exc:
        _logger.debug("[AdvMetrics] rivalry check failed: %s", exc)

    # 6. back_to_back
    try:
        rest_days = int(_safe_float(game.get("rest_days", 1)))
        if rest_days == 0:
            tags.append("back_to_back")
    except Exception as exc:
        _logger.debug("[AdvMetrics] back_to_back check failed: %s", exc)

    # 7. altitude
    try:
        city = str(game.get("city", "")).strip()
        if city in ("Denver", "Salt Lake City"):
            tags.append("altitude")
    except Exception as exc:
        _logger.debug("[AdvMetrics] altitude check failed: %s", exc)

    # 8. playoff_implications
    try:
        home_info = teams.get(home_team, {})
        away_info = teams.get(away_team, {})
        home_wins = _safe_float(home_info.get("wins", 0))
        home_losses = _safe_float(home_info.get("losses", 0))
        away_wins = _safe_float(away_info.get("wins", 0))
        away_losses = _safe_float(away_info.get("losses", 0))
        home_total = home_wins + home_losses
        away_total = away_wins + away_losses
        home_pct = home_wins / max(home_total, 1)
        away_pct = away_wins / max(away_total, 1)
        if 0.40 <= home_pct <= 0.60 and 0.40 <= away_pct <= 0.60:
            tags.append("playoff_implications")
    except Exception as exc:
        _logger.debug("[AdvMetrics] playoff_implications check failed: %s", exc)

    # 9. blowout_risk
    try:
        spread_val = _safe_float(game.get("spread", 0))
        if abs(spread_val) > 10:
            tags.append("blowout_risk")
    except Exception as exc:
        _logger.debug("[AdvMetrics] blowout_risk check failed: %s", exc)

    # 10. pace_up
    try:
        home_pace = _safe_float(teams.get(home_team, {}).get("pace", 100))
        away_pace = _safe_float(teams.get(away_team, {}).get("pace", 100))
        pace_sum = home_pace + away_pace
        if pace_sum > 202:
            tags.append("pace_up")
    except Exception as exc:
        _logger.debug("[AdvMetrics] pace_up check failed: %s", exc)

    # 11. pace_down
    try:
        home_pace = _safe_float(teams.get(home_team, {}).get("pace", 100))
        away_pace = _safe_float(teams.get(away_team, {}).get("pace", 100))
        pace_sum = home_pace + away_pace
        if pace_sum < 194:
            tags.append("pace_down")
    except Exception as exc:
        _logger.debug("[AdvMetrics] pace_down check failed: %s", exc)

    # 12. revenge_coach
    try:
        coach = str(game.get("coach", ""))
        former = COACH_REVENGE.get(coach, [])
        if former and opponent in former:
            tags.append("revenge_coach")
    except Exception as exc:
        _logger.debug("[AdvMetrics] revenge_coach check failed: %s", exc)

    return tags


# ============================================================
# SECTION: God-Mode Enrichment
# ============================================================

def enrich_player_god_mode(player: dict, games: list, teams: dict) -> dict:
    """Enrich a player dict with advanced God-Mode metrics.

    Returns an enriched **copy** of the player dict — the original is
    never mutated.  If the player's prop dict has no ``line`` or the
    line is zero the function bails out early with ``enriched=False``.

    Args:
        player: Raw player dict (must contain stat averages and a
                ``prop`` sub-dict with ``line``).
        games:  List of game dicts relevant to this player/slate.
        teams:  Mapping of team abbreviation → team info dict.

    Returns:
        dict: A deep copy of *player* with additional computed keys.
    """
    try:
        enriched = copy.deepcopy(player)

        # ---- Kill switch: no prop line → skip enrichment ----
        # Check top-level "line" first, then nested "prop.line".
        line = _safe_float(enriched.get("line", 0))
        if line == 0:
            prop = enriched.get("prop", {})
            if isinstance(prop, dict):
                line = _safe_float(prop.get("line", 0))
        if line == 0:
            enriched["enriched"] = False
            return enriched

        # ---- Pull base stats ----
        pts = _safe_float(enriched.get("points_avg", 0))
        ast = _safe_float(enriched.get("assists_avg", 0))
        reb = _safe_float(enriched.get("rebounds_avg", 0))
        stl = _safe_float(enriched.get("steals_avg", 0))
        blk = _safe_float(enriched.get("blocks_avg", 0))
        tov = _safe_float(enriched.get("turnovers_avg", 0))
        fg3_pct = _safe_float(enriched.get("fg3_pct", 0))
        fg3a = _safe_float(enriched.get("fg3a", 0))
        fga = _safe_float(enriched.get("fga", 0))
        fta = _safe_float(enriched.get("fta", 0))
        minutes = _safe_float(enriched.get("minutes_avg", 0))
        dreb = _safe_float(enriched.get("defensive_rebounds_avg", 0))
        position = enriched.get("position", "").upper()
        player_team = enriched.get("team", "")

        # ============================================================
        # Offensive Metrics
        # ============================================================

        # Usage rate estimate (simplified)
        usage_rate_est = _estimate_usage_rate(fga, fta, tov, minutes)
        enriched["usage_rate_est"] = round(usage_rate_est, 2)

        # True shooting percentage
        ts_denom = 2.0 * (fga + 0.44 * fta)
        true_shooting_pct = (pts / ts_denom) if ts_denom > 0 else 0.0
        enriched["true_shooting_pct"] = round(true_shooting_pct, 4)

        # Points per possession estimate
        poss_est = fga + 0.44 * fta + tov
        points_per_possession_est = pts / max(poss_est, 1)
        enriched["points_per_possession_est"] = round(points_per_possession_est, 3)

        # Assist-to-turnover ratio
        assist_to_turnover = ast / max(tov, 0.5)
        enriched["assist_to_turnover"] = round(assist_to_turnover, 2)

        # Three-point volume (attempts per game)
        enriched["three_point_volume"] = round(fg3a, 1)

        # Free-throw rate
        free_throw_rate = fta / max(fga, 1)
        enriched["free_throw_rate"] = round(free_throw_rate, 3)

        # Gravity score (0-100): composite of scoring volume, 3PT threat,
        # and free-throw drawing
        raw_gravity = (
            normalize(pts, 5, 35, 0, 40)
            + normalize(fg3a, 0, 10, 0, 30)
            + normalize(free_throw_rate, 0, 0.50, 0, 15)
            + normalize(usage_rate_est, 15, 35, 0, 15)
        )
        enriched["gravity_score"] = round(max(0.0, min(100.0, raw_gravity)), 1)

        # ============================================================
        # Defensive Proxies
        # ============================================================

        stocks = stl + blk
        enriched["stocks"] = round(stocks, 2)

        # Defensive activity (per-36)
        defensive_activity = (stl + blk + dreb) / max(minutes, 1) * 36.0
        enriched["defensive_activity"] = round(defensive_activity, 2)

        # Switchability score (0-100): positional versatility proxy
        pos_flex = 0.0
        if position in ("SF", "PF"):
            pos_flex = 25.0
        elif position in ("SG",):
            pos_flex = 15.0
        elif position in ("C",):
            pos_flex = 5.0
        elif position in ("PG",):
            pos_flex = 10.0

        raw_switchability = (
            pos_flex
            + normalize(stocks, 0, 4, 0, 35)
            + normalize(defensive_activity, 0, 8, 0, 25)
            + normalize(reb, 0, 12, 0, 15)
        )
        enriched["switchability_score"] = round(
            max(0.0, min(100.0, raw_switchability)), 1
        )

        # ============================================================
        # Contextual Metrics
        # ============================================================

        # Use the first game in the list as the primary context
        game = games[0] if games else {}

        # Rest days
        rest_days = int(_safe_float(game.get("rest_days", 1)))
        enriched["rest_days"] = rest_days
        enriched["back_to_back"] = rest_days == 0

        # Altitude factor
        home_team = game.get("home_team", "")
        enriched["altitude_factor"] = ALTITUDE_MAP.get(home_team, 1.0)

        # Travel distance tier based on timezone differential
        away_team = game.get("away_team", "")
        opponent = away_team if player_team == home_team else home_team
        tz_player = TIMEZONE_MAP.get(player_team, 0)
        tz_game = TIMEZONE_MAP.get(home_team, 0)
        tz_diff = abs(tz_player - tz_game)
        if tz_diff == 0:
            enriched["travel_distance_tier"] = "local"
        elif tz_diff == 1:
            enriched["travel_distance_tier"] = "short"
        elif tz_diff == 2:
            enriched["travel_distance_tier"] = "medium"
        else:
            enriched["travel_distance_tier"] = "cross_country"

        # Archetype
        enriched["archetype"] = classify_player_archetype(enriched)

        # Narrative tags
        enriched["narrative_tags"] = detect_narrative_tags(enriched, game, teams)

        # ============================================================
        # Trajectory Metrics
        # ============================================================

        age = int(_safe_float(enriched.get("age", 26)))
        enriched["age"] = age
        enriched["prime_window"] = 25 <= age <= 31
        enriched["decline_risk"] = age > 32 and true_shooting_pct < 0.50

        # Mark as enriched
        enriched["enriched"] = True
        return enriched

    except Exception as exc:
        _logger.error("[AdvMetrics] enrich_player_god_mode error: %s", exc)
        safe = copy.deepcopy(player) if isinstance(player, dict) else {}
        safe["enriched"] = False
        return safe
