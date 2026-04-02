# ============================================================
# FILE: engine/impact_metrics.py
# PURPOSE: Advanced impact metrics for NBA player evaluation —
#          True Shooting, eFG%, EPM / RAPTOR approximations,
#          efficiency profiles, offensive load, defensive impact,
#          and WAR estimates.
# CONNECTS TO: engine/math_helpers.py, data/advanced_metrics.py,
#              engine/joseph_eval.py
# ============================================================

"""Advanced impact metrics for NBA player evaluation.

Provides box-score-derived approximations of modern basketball
analytics metrics including True Shooting percentage, Effective
Field Goal percentage, Estimated Plus-Minus (EPM), RAPTOR, WAR,
offensive load, and defensive impact ratings.

All public functions are wrapped in try/except blocks and return
type-correct fallback values on error so that callers never
receive unexpected types.
"""

import logging
import math

_logger = logging.getLogger(__name__)

# ============================================================
# SECTION: External Imports (graceful fallbacks)
# ============================================================

try:
    from engine.math_helpers import _safe_float
except ImportError:
    _logger.warning("[ImpactMetrics] Could not import _safe_float from math_helpers")

    def _safe_float(value, fallback=0.0):
        """Convert *value* to float; return *fallback* on failure or non-finite."""
        try:
            v = float(value)
            if math.isfinite(v):
                return v
            return float(fallback)
        except (ValueError, TypeError):
            return float(fallback)

try:
    from data.advanced_metrics import normalize
except ImportError:
    _logger.warning("[ImpactMetrics] Could not import normalize from advanced_metrics")

    def normalize(value, min_val, max_val, out_min=0.0, out_max=100.0):
        """Standard min-max normalization with clamping."""
        if max_val == min_val:
            return out_min
        ratio = (value - min_val) / (max_val - min_val)
        result = out_min + ratio * (out_max - out_min)
        return max(out_min, min(out_max, result))


# ============================================================
# SECTION: Constants — League Averages & Baselines
# ============================================================

LEAGUE_AVG_TS = 0.572
LEAGUE_AVG_EFG = 0.543
LEAGUE_AVG_USAGE = 20.0
LEAGUE_AVG_AST_RATE = 16.5
LEAGUE_AVG_TOV_RATE = 12.5
LEAGUE_AVG_REB_RATE = 10.0
LEAGUE_AVG_STL_RATE = 1.5
LEAGUE_AVG_BLK_RATE = 1.0
LEAGUE_AVG_OFF_RATING = 112.0
LEAGUE_AVG_DEF_RATING = 112.0
LEAGUE_AVG_PACE = 100.0

# Position-based defensive adjustment multipliers (offense, defense)
_POSITION_ADJUSTMENTS: dict = {
    "C":  (-0.8, 1.5),
    "PF": (-0.4, 0.8),
    "SF": (0.0, 0.2),
    "SG": (0.3, -0.3),
    "PG": (0.5, -0.6),
}

# Replacement-level baseline per 100 possessions
_REPLACEMENT_LEVEL_PER_100 = -2.0

# Minutes in a full season for WAR scaling
_FULL_SEASON_MINUTES = 2000


# ============================================================
# SECTION: Simple Shooting Metrics
# ============================================================

def calculate_true_shooting_pct(pts: float, fga: float, fta: float) -> float:
    """Calculate True Shooting Percentage.

    Formula: ``TS% = PTS / (2 * (FGA + 0.44 * FTA))``

    Args:
        pts: Points scored.
        fga: Field goal attempts.
        fta: Free throw attempts.

    Returns:
        True Shooting percentage as a float in [0, 1].
        Returns 0.0 on invalid input or division by zero.
    """
    try:
        pts = _safe_float(pts)
        fga = _safe_float(fga)
        fta = _safe_float(fta)
        denominator = 2.0 * (fga + 0.44 * fta)
        if denominator <= 0:
            return 0.0
        return max(0.0, min(1.0, pts / denominator))
    except Exception:
        _logger.debug("[ImpactMetrics] calculate_true_shooting_pct error")
        return 0.0


def calculate_effective_fg_pct(fgm: float, fga: float, fg3m: float) -> float:
    """Calculate Effective Field Goal Percentage.

    Formula: ``eFG% = (FGM + 0.5 * FG3M) / FGA``

    Args:
        fgm: Field goals made.
        fga: Field goal attempts.
        fg3m: Three-point field goals made.

    Returns:
        Effective FG percentage as a float in [0, 1].
        Returns 0.0 on invalid input or division by zero.
    """
    try:
        fgm = _safe_float(fgm)
        fga = _safe_float(fga)
        fg3m = _safe_float(fg3m)
        if fga <= 0:
            return 0.0
        return max(0.0, min(1.0, (fgm + 0.5 * fg3m) / fga))
    except Exception:
        _logger.debug("[ImpactMetrics] calculate_effective_fg_pct error")
        return 0.0


# ============================================================
# SECTION: Internal Rate Helpers
# ============================================================

def _estimate_possessions(fga: float, fta: float, oreb: float,
                          tov: float) -> float:
    """Approximate individual possessions used.

    Uses the standard NBA possession estimate:
    ``FGA + 0.44 * FTA - OREB + TOV``
    """
    return max(fga + 0.44 * fta - oreb + tov, 1.0)


def _extract_rates(player_data: dict) -> dict:
    """Pull common per-game stats and derive per-100-possession rates.

    Returns a dict of raw stats and rate-based metrics used across
    multiple public functions.
    """
    pts = _safe_float(player_data.get("points_avg", 0))
    ast = _safe_float(player_data.get("assists_avg", 0))
    reb = _safe_float(player_data.get("rebounds_avg", 0))
    oreb = _safe_float(player_data.get("offensive_rebounds_avg", 0))
    dreb = _safe_float(player_data.get("defensive_rebounds_avg", 0))
    stl = _safe_float(player_data.get("steals_avg", 0))
    blk = _safe_float(player_data.get("blocks_avg", 0))
    tov = _safe_float(player_data.get("turnovers_avg", 0))
    minutes = _safe_float(player_data.get("minutes_avg", 0))
    fga = _safe_float(player_data.get("fga", 0))
    fgm = _safe_float(player_data.get("fgm", 0))
    fta = _safe_float(player_data.get("fta", 0))
    fg3m = _safe_float(player_data.get("fg3m", 0))
    fg3a = _safe_float(player_data.get("fg3a", 0))
    fg3_pct = _safe_float(player_data.get("fg3_pct", 0))
    position = str(player_data.get("position", "SF")).upper().strip()

    # Per-game possession estimate
    poss = _estimate_possessions(fga, fta, oreb, tov)

    # Scale factor to normalise to per-100 possessions
    per_100 = 100.0 / max(poss, 1.0)

    # Usage rate estimate
    usage_denom = max(minutes, 1.0) * 2.0
    usage_rate = 100.0 * (fga + 0.44 * fta + tov) / usage_denom

    # Shooting efficiencies
    ts_pct = calculate_true_shooting_pct(pts, fga, fta)
    efg_pct = calculate_effective_fg_pct(fgm, fga, fg3m)

    # Rate metrics (per 100 possessions)
    ast_rate = ast * per_100
    tov_rate = tov * per_100
    reb_rate = reb * per_100
    stl_rate = stl * per_100
    blk_rate = blk * per_100

    return {
        "pts": pts, "ast": ast, "reb": reb, "oreb": oreb, "dreb": dreb,
        "stl": stl, "blk": blk, "tov": tov, "minutes": minutes,
        "fga": fga, "fgm": fgm, "fta": fta, "fg3m": fg3m, "fg3a": fg3a,
        "fg3_pct": fg3_pct, "position": position,
        "poss": poss, "per_100": per_100,
        "usage_rate": usage_rate,
        "ts_pct": ts_pct, "efg_pct": efg_pct,
        "ast_rate": ast_rate, "tov_rate": tov_rate,
        "reb_rate": reb_rate, "stl_rate": stl_rate, "blk_rate": blk_rate,
    }


# ============================================================
# SECTION: Estimated Plus-Minus (EPM)
# ============================================================

def _default_epm() -> dict:
    """Return a safe fallback EPM dictionary."""
    return {
        "offensive_epm": 0.0,
        "defensive_epm": 0.0,
        "total_epm": 0.0,
        "percentile": 50.0,
    }


def estimate_epm(player_data: dict) -> dict:
    """Estimate Plus-Minus approximation using box-score stats.

    Blends scoring efficiency, assist rate, turnover rate, rebound
    rate, and steal/block rates into an offensive and defensive
    EPM estimate, normalised to a per-100-possessions scale.
    Position-based adjustments shift the baseline for bigs versus
    guards.

    Args:
        player_data: Player statistics dictionary.

    Returns:
        Dictionary with keys ``offensive_epm``, ``defensive_epm``,
        ``total_epm``, and ``percentile``.
    """
    try:
        r = _extract_rates(player_data)

        pos_off_adj, pos_def_adj = _POSITION_ADJUSTMENTS.get(
            r["position"], (0.0, 0.0)
        )

        # --- Offensive EPM components ---
        # Scoring efficiency differential vs league average
        ts_diff = (r["ts_pct"] - LEAGUE_AVG_TS) * 10.0
        # Assist contribution above average
        ast_diff = (r["ast_rate"] - LEAGUE_AVG_AST_RATE) * 0.15
        # Turnover penalty
        tov_penalty = (r["tov_rate"] - LEAGUE_AVG_TOV_RATE) * -0.12
        # Usage-weighted volume bonus
        usage_factor = (r["usage_rate"] - LEAGUE_AVG_USAGE) * 0.05

        offensive_epm = (
            ts_diff * 0.40
            + ast_diff * 0.25
            + tov_penalty * 0.20
            + usage_factor * 0.15
            + pos_off_adj
        )

        # --- Defensive EPM components ---
        stl_diff = (r["stl_rate"] - LEAGUE_AVG_STL_RATE) * 0.3
        blk_diff = (r["blk_rate"] - LEAGUE_AVG_BLK_RATE) * 0.25
        dreb_contrib = (r["dreb"] - 3.5) * 0.1
        stocks_bonus = (r["stl"] + r["blk"]) * 0.15

        defensive_epm = (
            stl_diff * 0.30
            + blk_diff * 0.30
            + dreb_contrib * 0.20
            + stocks_bonus * 0.20
            + pos_def_adj
        )

        total_epm = offensive_epm + defensive_epm

        # Percentile mapping: map total EPM to a 0-100 percentile
        # Typical EPM range for NBA players is roughly -5.0 to +8.0
        percentile = normalize(total_epm, -5.0, 8.0, 0.0, 100.0)

        return {
            "offensive_epm": round(offensive_epm, 2),
            "defensive_epm": round(defensive_epm, 2),
            "total_epm": round(total_epm, 2),
            "percentile": round(percentile, 1),
        }
    except Exception:
        _logger.exception("[ImpactMetrics] estimate_epm error")
        return _default_epm()


# ============================================================
# SECTION: Estimated RAPTOR
# ============================================================

def _default_raptor() -> dict:
    """Return a safe fallback RAPTOR dictionary."""
    return {
        "raptor_offense": 0.0,
        "raptor_defense": 0.0,
        "raptor_total": 0.0,
        "war": 0.0,
    }


def estimate_raptor(player_data: dict) -> dict:
    """Box-score RAPTOR approximation.

    Combines offensive and defensive contributions into a
    per-100-possession RAPTOR estimate and derives an
    approximate Wins Above Replacement (WAR).

    WAR formula: ``RAPTOR_total * minutes_pct * 82 / 2000``

    Args:
        player_data: Player statistics dictionary.

    Returns:
        Dictionary with keys ``raptor_offense``, ``raptor_defense``,
        ``raptor_total``, and ``war``.
    """
    try:
        r = _extract_rates(player_data)

        # --- Offensive RAPTOR ---
        # Efficiency above/below league average
        ts_component = (r["ts_pct"] - LEAGUE_AVG_TS) * 12.0
        efg_component = (r["efg_pct"] - LEAGUE_AVG_EFG) * 6.0
        # Playmaking
        ast_component = (r["ast_rate"] - LEAGUE_AVG_AST_RATE) * 0.12
        tov_component = (r["tov_rate"] - LEAGUE_AVG_TOV_RATE) * -0.08
        # Volume / usage scaling — higher usage amplifies both good and bad
        usage_scale = max(r["usage_rate"] / LEAGUE_AVG_USAGE, 0.5)

        raptor_offense = (
            (ts_component * 0.35 + efg_component * 0.20
             + ast_component * 0.25 + tov_component * 0.20) * usage_scale
        )

        # --- Defensive RAPTOR ---
        stl_component = (r["stl"] - 0.8) * 0.6
        blk_component = (r["blk"] - 0.4) * 0.5
        dreb_component = (r["dreb"] - 3.0) * 0.15
        # Position adjustment for defense
        _, pos_def_adj = _POSITION_ADJUSTMENTS.get(r["position"], (0.0, 0.0))

        raptor_defense = (
            stl_component * 0.30
            + blk_component * 0.30
            + dreb_component * 0.20
            + pos_def_adj * 0.20
        )

        raptor_total = raptor_offense + raptor_defense

        # WAR estimate: RAPTOR * (minutes / full-season minutes) * 82 / 2000
        minutes_pct = r["minutes"] * 82.0 / _FULL_SEASON_MINUTES
        war = raptor_total * minutes_pct * 82.0 / _FULL_SEASON_MINUTES

        return {
            "raptor_offense": round(raptor_offense, 2),
            "raptor_defense": round(raptor_defense, 2),
            "raptor_total": round(raptor_total, 2),
            "war": round(war, 2),
        }
    except Exception:
        _logger.exception("[ImpactMetrics] estimate_raptor error")
        return _default_raptor()


# ============================================================
# SECTION: Player Efficiency Profile
# ============================================================

def _classify_efficiency_tier(percentile: float) -> str:
    """Map a 0-100 percentile to a human-readable tier label.

    Tiers:
        * ``"Elite"``         — top 10 %  (≥ 90)
        * ``"Above Average"`` — 65-89
        * ``"Average"``       — 40-64
        * ``"Below Average"`` — 20-39
        * ``"Poor"``          — < 20
    """
    if percentile >= 90.0:
        return "Elite"
    if percentile >= 65.0:
        return "Above Average"
    if percentile >= 40.0:
        return "Average"
    if percentile >= 20.0:
        return "Below Average"
    return "Poor"


def _default_efficiency_profile() -> dict:
    """Return a safe fallback efficiency profile dictionary."""
    return {
        "ts_pct": 0.0,
        "efg_pct": 0.0,
        "usage_rate": 0.0,
        "assist_rate": 0.0,
        "turnover_rate": 0.0,
        "rebound_rate": 0.0,
        "steal_rate": 0.0,
        "block_rate": 0.0,
        "estimated_epm": _default_epm(),
        "estimated_raptor": _default_raptor(),
        "efficiency_tier": "Poor",
    }


def calculate_player_efficiency_profile(player_data: dict) -> dict:
    """Build an all-in-one efficiency profile for a player.

    Combines shooting metrics, rate stats, EPM, RAPTOR, and an
    overall efficiency tier into a single dictionary.

    Args:
        player_data: Player statistics dictionary.

    Returns:
        Dictionary with keys ``ts_pct``, ``efg_pct``, ``usage_rate``,
        ``assist_rate``, ``turnover_rate``, ``rebound_rate``,
        ``steal_rate``, ``block_rate``, ``estimated_epm``,
        ``estimated_raptor``, and ``efficiency_tier``.
    """
    try:
        r = _extract_rates(player_data)
        epm = estimate_epm(player_data)
        raptor = estimate_raptor(player_data)

        # Composite percentile from EPM percentile and RAPTOR-derived score
        raptor_pctile = normalize(raptor["raptor_total"], -4.0, 6.0, 0.0, 100.0)
        composite_pctile = 0.5 * epm["percentile"] + 0.5 * raptor_pctile

        return {
            "ts_pct": round(r["ts_pct"], 3),
            "efg_pct": round(r["efg_pct"], 3),
            "usage_rate": round(r["usage_rate"], 1),
            "assist_rate": round(r["ast_rate"], 1),
            "turnover_rate": round(r["tov_rate"], 1),
            "rebound_rate": round(r["reb_rate"], 1),
            "steal_rate": round(r["stl_rate"], 1),
            "block_rate": round(r["blk_rate"], 1),
            "estimated_epm": epm,
            "estimated_raptor": raptor,
            "efficiency_tier": _classify_efficiency_tier(composite_pctile),
        }
    except Exception:
        _logger.exception("[ImpactMetrics] calculate_player_efficiency_profile error")
        return _default_efficiency_profile()


# ============================================================
# SECTION: Offensive Load
# ============================================================

def _default_offensive_load() -> dict:
    """Return a safe fallback offensive load dictionary."""
    return {
        "load_score": 0.0,
        "self_created_rate": 0.0,
        "assisted_rate_est": 0.0,
        "gravity_adjusted_load": 0.0,
    }


def calculate_offensive_load(player_data: dict) -> dict:
    """Estimate a player's offensive creation burden.

    Combines usage rate, self-creation proxies, and three-point
    gravity into a load score scaled 0-100.

    Args:
        player_data: Player statistics dictionary.

    Returns:
        Dictionary with keys ``load_score``, ``self_created_rate``,
        ``assisted_rate_est``, and ``gravity_adjusted_load``.
    """
    try:
        r = _extract_rates(player_data)

        # Self-created scoring proxy: unassisted shots are estimated as
        # a function of usage and assist-to-turnover ratio.
        ast_tov_ratio = r["ast"] / max(r["tov"], 0.5)

        # Higher usage + lower assist ratio → more self-creation
        self_created_rate = normalize(
            r["usage_rate"] * 0.6 + (1.0 / max(ast_tov_ratio, 0.3)) * 8.0,
            0, 30, 0.0, 1.0,
        )

        # Assisted rate is the complement
        assisted_rate_est = max(0.0, min(1.0, 1.0 - self_created_rate))

        # Three-point gravity factor
        three_pt_gravity = r["fg3a"] * r["fg3_pct"]
        gravity_norm = normalize(three_pt_gravity, 0, 4, 0, 1.0)

        # Gravity-adjusted load scales the raw load up for players who
        # also stretch the floor
        raw_load = normalize(r["usage_rate"], 10, 40, 0, 100)
        gravity_adjusted_load = raw_load * (1.0 + 0.15 * gravity_norm)
        gravity_adjusted_load = max(0.0, min(100.0, gravity_adjusted_load))

        # Final load score blends usage, self-creation, and scoring volume
        volume_component = normalize(r["pts"], 0, 35, 0, 100)
        creation_component = normalize(r["ast"], 0, 12, 0, 100)

        load_score = (
            0.35 * raw_load
            + 0.25 * (self_created_rate * 100.0)
            + 0.20 * volume_component
            + 0.20 * creation_component
        )
        load_score = max(0.0, min(100.0, load_score))

        return {
            "load_score": round(load_score, 1),
            "self_created_rate": round(self_created_rate, 3),
            "assisted_rate_est": round(assisted_rate_est, 3),
            "gravity_adjusted_load": round(gravity_adjusted_load, 1),
        }
    except Exception:
        _logger.exception("[ImpactMetrics] calculate_offensive_load error")
        return _default_offensive_load()


# ============================================================
# SECTION: Defensive Impact
# ============================================================

def _grade_defense(score: float) -> str:
    """Convert a 0-100 defensive score to a letter grade.

    Grade boundaries mirror those in ``engine/joseph_eval.py``.
    """
    if score >= 90:
        return "A+"
    if score >= 80:
        return "A"
    if score >= 70:
        return "B+"
    if score >= 60:
        return "B"
    if score >= 50:
        return "C+"
    if score >= 40:
        return "C"
    if score >= 30:
        return "D"
    return "F"


def _default_defensive_impact() -> dict:
    """Return a safe fallback defensive impact dictionary."""
    return {
        "def_rating_est": LEAGUE_AVG_DEF_RATING,
        "contest_rate_est": 0.0,
        "deflection_rate_est": 0.0,
        "rim_protection_est": 0.0,
        "perimeter_defense_est": 0.0,
        "overall_defensive_grade": "F",
    }


def estimate_defensive_impact(player_data: dict) -> dict:
    """Estimate a bundle of defensive metrics from box-score data.

    Since contest and deflection data are not available in standard
    box scores, proxies are derived from steals, blocks, defensive
    rebounds, and position information.

    Args:
        player_data: Player statistics dictionary.

    Returns:
        Dictionary with keys ``def_rating_est``, ``contest_rate_est``,
        ``deflection_rate_est``, ``rim_protection_est``,
        ``perimeter_defense_est``, and ``overall_defensive_grade``.
    """
    try:
        r = _extract_rates(player_data)

        # --- Defensive rating estimate ---
        # Start from league average and adjust based on stocks and rebounds
        stl_adj = (r["stl"] - 0.8) * -1.2
        blk_adj = (r["blk"] - 0.4) * -1.5
        dreb_adj = (r["dreb"] - 3.5) * -0.4
        _, pos_def_adj = _POSITION_ADJUSTMENTS.get(r["position"], (0.0, 0.0))
        # Negative pos_def_adj means worse defense for guards — invert for rating
        pos_rating_adj = pos_def_adj * -0.5

        def_rating_est = LEAGUE_AVG_DEF_RATING + stl_adj + blk_adj + dreb_adj + pos_rating_adj

        # --- Contest rate proxy ---
        # Bigs contest more shots; guards contest fewer but deflect more
        if r["position"] in ("C", "PF"):
            contest_rate_est = normalize(r["blk"] * 2.5 + r["dreb"] * 0.3, 0, 8, 0, 100)
        else:
            contest_rate_est = normalize(r["stl"] * 2.0 + r["blk"] * 1.5, 0, 5, 0, 100)

        # --- Deflection rate proxy ---
        # Steals strongly correlate with deflections
        deflection_rate_est = normalize(r["stl"] * 3.0 + r["blk"] * 0.5, 0, 6, 0, 100)

        # --- Rim protection proxy ---
        if r["position"] in ("C", "PF"):
            rim_protection_est = normalize(r["blk"], 0, 3, 0, 100)
        else:
            # Wings/guards rarely protect the rim
            rim_protection_est = normalize(r["blk"], 0, 1.5, 0, 50)

        # --- Perimeter defense proxy ---
        if r["position"] in ("PG", "SG", "SF"):
            perimeter_defense_est = normalize(r["stl"], 0, 2.5, 0, 100)
        else:
            perimeter_defense_est = normalize(r["stl"], 0, 1.5, 0, 60)

        # --- Overall defensive grade ---
        composite = (
            0.25 * normalize(LEAGUE_AVG_DEF_RATING - def_rating_est + LEAGUE_AVG_DEF_RATING,
                             100, 120, 0, 100)
            + 0.20 * contest_rate_est
            + 0.20 * deflection_rate_est
            + 0.20 * rim_protection_est
            + 0.15 * perimeter_defense_est
        )
        composite = max(0.0, min(100.0, composite))

        return {
            "def_rating_est": round(def_rating_est, 1),
            "contest_rate_est": round(contest_rate_est, 1),
            "deflection_rate_est": round(deflection_rate_est, 1),
            "rim_protection_est": round(rim_protection_est, 1),
            "perimeter_defense_est": round(perimeter_defense_est, 1),
            "overall_defensive_grade": _grade_defense(composite),
        }
    except Exception:
        _logger.exception("[ImpactMetrics] estimate_defensive_impact error")
        return _default_defensive_impact()


# ============================================================
# SECTION: Wins Above Replacement (WAR)
# ============================================================

def calculate_war(player_data: dict) -> float:
    """Estimate Wins Above Replacement for a player.

    Combines offensive and defensive contributions (via RAPTOR)
    and scales to an 82-game season.

    Args:
        player_data: Player statistics dictionary.

    Returns:
        Estimated WAR as a float.  Returns 0.0 on error.
    """
    try:
        raptor = estimate_raptor(player_data)
        minutes = _safe_float(player_data.get("minutes_avg", 0))

        raptor_total = _safe_float(raptor.get("raptor_total", 0))
        above_replacement = raptor_total - _REPLACEMENT_LEVEL_PER_100

        # Season minutes projection
        season_minutes = minutes * 82.0
        minutes_pct = season_minutes / _FULL_SEASON_MINUTES

        # WAR: marginal value over replacement scaled to wins
        # ~2500 marginal points ≈ 1 win;  per-100-poss → per-minute scaling
        war = above_replacement * minutes_pct * 82.0 / _FULL_SEASON_MINUTES

        return round(war, 2)
    except Exception:
        _logger.exception("[ImpactMetrics] calculate_war error")
        return 0.0
