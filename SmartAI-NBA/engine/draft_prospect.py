# ============================================================
# FILE: engine/draft_prospect.py
# PURPOSE: Draft prospect evaluation — college-to-NBA stat
#          translation, physical profiling, historical comps,
#          career-outcome prediction, and scouting reports.
# CONNECTS TO: engine/joseph_eval.py, data/advanced_metrics.py,
#              engine/math_helpers.py
# ============================================================

import logging
import math

_logger = logging.getLogger(__name__)

# ------------------------------------------------------------------
# External imports (graceful fallbacks)
# ------------------------------------------------------------------

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

try:
    from data.advanced_metrics import normalize
except ImportError:
    _logger.warning("[DraftProspect] Could not import normalize from advanced_metrics")

    def normalize(value, min_val, max_val, out_min=0.0, out_max=100.0):
        if max_val == min_val:
            return out_min
        clamped = max(min_val, min(max_val, value))
        return out_min + (clamped - min_val) / (max_val - min_val) * (out_max - out_min)

try:
    from engine.joseph_eval import letter_grade
except ImportError:
    _logger.warning("[DraftProspect] Could not import letter_grade from joseph_eval")

    def letter_grade(score: float) -> str:
        try:
            score = float(score)
        except (TypeError, ValueError):
            return "F"
        if score >= 95:
            return "A+"
        if score >= 90:
            return "A"
        if score >= 85:
            return "A-"
        if score >= 80:
            return "B+"
        if score >= 75:
            return "B"
        if score >= 70:
            return "B-"
        if score >= 65:
            return "C+"
        if score >= 60:
            return "C"
        if score >= 55:
            return "C-"
        if score >= 50:
            return "D+"
        if score >= 45:
            return "D"
        if score >= 40:
            return "D-"
        return "F"


# ============================================================
# SECTION: Constants
# ============================================================

CONFERENCE_STRENGTH: dict = {
    "SEC": 1.00,
    "Big Ten": 0.98,
    "Big 12": 0.98,
    "ACC": 0.97,
    "Big East": 0.95,
    "Pac-12": 0.94,
    "AAC": 0.90,
    "Mountain West": 0.88,
    "WCC": 0.87,
    "MVC": 0.86,
    "A-10": 0.86,
    "Colonial": 0.84,
    "Horizon": 0.84,
    "mid-major": 0.85,
    "international": 0.92,
    "unknown": 0.85,
}

NBA_POSITION_AVERAGES: dict = {
    "PG": {
        "height_inches": 75, "weight": 195, "wingspan_inches": 79,
        "standing_reach": 100, "lane_agility": 10.8, "sprint_3qtr": 3.15,
        "vertical_leap": 38,
    },
    "SG": {
        "height_inches": 77, "weight": 205, "wingspan_inches": 81,
        "standing_reach": 102, "lane_agility": 11.0, "sprint_3qtr": 3.18,
        "vertical_leap": 37,
    },
    "SF": {
        "height_inches": 79, "weight": 220, "wingspan_inches": 84,
        "standing_reach": 105, "lane_agility": 11.2, "sprint_3qtr": 3.22,
        "vertical_leap": 36,
    },
    "PF": {
        "height_inches": 81, "weight": 235, "wingspan_inches": 86,
        "standing_reach": 108, "lane_agility": 11.5, "sprint_3qtr": 3.28,
        "vertical_leap": 34,
    },
    "C": {
        "height_inches": 83, "weight": 250, "wingspan_inches": 89,
        "standing_reach": 112, "lane_agility": 11.8, "sprint_3qtr": 3.35,
        "vertical_leap": 32,
    },
}

HISTORICAL_COMP_DATABASE: list = [
    # Guards — scoring-focused
    {"name": "Steph Curry archetype", "position": "PG", "ppg": 24, "rpg": 5, "apg": 7, "spg": 1.6, "bpg": 0.2, "fg_pct": 0.47, "three_pct": 0.43, "height_inches": 75, "wingspan_inches": 79, "career_outcome": "Franchise Player"},
    {"name": "Damian Lillard archetype", "position": "PG", "ppg": 25, "rpg": 4, "apg": 7, "spg": 1.0, "bpg": 0.3, "fg_pct": 0.44, "three_pct": 0.37, "height_inches": 74, "wingspan_inches": 79, "career_outcome": "All-Star"},
    {"name": "Chris Paul archetype", "position": "PG", "ppg": 18, "rpg": 4, "apg": 10, "spg": 2.2, "bpg": 0.1, "fg_pct": 0.47, "three_pct": 0.37, "height_inches": 72, "wingspan_inches": 77, "career_outcome": "Franchise Player"},
    {"name": "Jrue Holiday archetype", "position": "PG", "ppg": 17, "rpg": 4, "apg": 7, "spg": 1.5, "bpg": 0.4, "fg_pct": 0.45, "three_pct": 0.35, "height_inches": 76, "wingspan_inches": 81, "career_outcome": "All-Star"},
    {"name": "Kyle Lowry archetype", "position": "PG", "ppg": 15, "rpg": 4, "apg": 7, "spg": 1.4, "bpg": 0.3, "fg_pct": 0.43, "three_pct": 0.36, "height_inches": 72, "wingspan_inches": 78, "career_outcome": "Quality Starter"},
    {"name": "Marcus Smart archetype", "position": "PG", "ppg": 12, "rpg": 4, "apg": 5, "spg": 1.5, "bpg": 0.4, "fg_pct": 0.40, "three_pct": 0.33, "height_inches": 76, "wingspan_inches": 82, "career_outcome": "Quality Starter"},
    {"name": "Klay Thompson archetype", "position": "SG", "ppg": 20, "rpg": 4, "apg": 2, "spg": 1.0, "bpg": 0.5, "fg_pct": 0.46, "three_pct": 0.42, "height_inches": 78, "wingspan_inches": 82, "career_outcome": "All-Star"},
    {"name": "Devin Booker archetype", "position": "SG", "ppg": 26, "rpg": 5, "apg": 5, "spg": 1.0, "bpg": 0.3, "fg_pct": 0.47, "three_pct": 0.36, "height_inches": 78, "wingspan_inches": 80, "career_outcome": "All-Star"},
    {"name": "Bradley Beal archetype", "position": "SG", "ppg": 23, "rpg": 4, "apg": 5, "spg": 1.2, "bpg": 0.4, "fg_pct": 0.46, "three_pct": 0.37, "height_inches": 77, "wingspan_inches": 82, "career_outcome": "All-Star"},
    {"name": "Danny Green archetype", "position": "SG", "ppg": 9, "rpg": 4, "apg": 1, "spg": 1.0, "bpg": 0.8, "fg_pct": 0.43, "three_pct": 0.40, "height_inches": 78, "wingspan_inches": 83, "career_outcome": "Rotation Player"},
    # Wings
    {"name": "LeBron James archetype", "position": "SF", "ppg": 27, "rpg": 7, "apg": 7, "spg": 1.5, "bpg": 0.8, "fg_pct": 0.50, "three_pct": 0.35, "height_inches": 81, "wingspan_inches": 84, "career_outcome": "Franchise Player"},
    {"name": "Kawhi Leonard archetype", "position": "SF", "ppg": 24, "rpg": 7, "apg": 4, "spg": 1.8, "bpg": 0.5, "fg_pct": 0.49, "three_pct": 0.38, "height_inches": 79, "wingspan_inches": 88, "career_outcome": "Franchise Player"},
    {"name": "Paul George archetype", "position": "SF", "ppg": 22, "rpg": 7, "apg": 4, "spg": 1.5, "bpg": 0.4, "fg_pct": 0.44, "three_pct": 0.38, "height_inches": 81, "wingspan_inches": 83, "career_outcome": "All-Star"},
    {"name": "Jayson Tatum archetype", "position": "SF", "ppg": 25, "rpg": 8, "apg": 4, "spg": 1.1, "bpg": 0.7, "fg_pct": 0.45, "three_pct": 0.37, "height_inches": 80, "wingspan_inches": 84, "career_outcome": "All-Star"},
    {"name": "Robert Covington archetype", "position": "SF", "ppg": 12, "rpg": 6, "apg": 1, "spg": 1.5, "bpg": 1.2, "fg_pct": 0.42, "three_pct": 0.36, "height_inches": 81, "wingspan_inches": 87, "career_outcome": "Rotation Player"},
    {"name": "Rudy Gay archetype", "position": "SF", "ppg": 17, "rpg": 6, "apg": 2, "spg": 1.2, "bpg": 0.5, "fg_pct": 0.44, "three_pct": 0.34, "height_inches": 80, "wingspan_inches": 84, "career_outcome": "Quality Starter"},
    {"name": "Michael Kidd-Gilchrist archetype", "position": "SF", "ppg": 9, "rpg": 5, "apg": 1, "spg": 0.8, "bpg": 0.4, "fg_pct": 0.44, "three_pct": 0.20, "height_inches": 79, "wingspan_inches": 86, "career_outcome": "Bust"},
    # Forwards / Bigs
    {"name": "Giannis Antetokounmpo archetype", "position": "PF", "ppg": 28, "rpg": 11, "apg": 6, "spg": 1.2, "bpg": 1.4, "fg_pct": 0.55, "three_pct": 0.29, "height_inches": 83, "wingspan_inches": 90, "career_outcome": "Franchise Player"},
    {"name": "Kevin Durant archetype", "position": "PF", "ppg": 27, "rpg": 7, "apg": 5, "spg": 1.1, "bpg": 1.5, "fg_pct": 0.50, "three_pct": 0.38, "height_inches": 82, "wingspan_inches": 89, "career_outcome": "Franchise Player"},
    {"name": "Pascal Siakam archetype", "position": "PF", "ppg": 20, "rpg": 7, "apg": 4, "spg": 1.0, "bpg": 0.7, "fg_pct": 0.47, "three_pct": 0.33, "height_inches": 81, "wingspan_inches": 86, "career_outcome": "All-Star"},
    {"name": "Draymond Green archetype", "position": "PF", "ppg": 9, "rpg": 7, "apg": 6, "spg": 1.4, "bpg": 1.0, "fg_pct": 0.44, "three_pct": 0.32, "height_inches": 78, "wingspan_inches": 84, "career_outcome": "Quality Starter"},
    {"name": "Jabari Parker archetype", "position": "PF", "ppg": 14, "rpg": 5, "apg": 2, "spg": 0.6, "bpg": 0.3, "fg_pct": 0.48, "three_pct": 0.35, "height_inches": 80, "wingspan_inches": 82, "career_outcome": "Bust"},
    {"name": "Aaron Gordon archetype", "position": "PF", "ppg": 15, "rpg": 7, "apg": 3, "spg": 0.8, "bpg": 0.6, "fg_pct": 0.46, "three_pct": 0.33, "height_inches": 80, "wingspan_inches": 84, "career_outcome": "Quality Starter"},
    # Centers
    {"name": "Joel Embiid archetype", "position": "C", "ppg": 28, "rpg": 11, "apg": 4, "spg": 1.0, "bpg": 1.7, "fg_pct": 0.50, "three_pct": 0.33, "height_inches": 84, "wingspan_inches": 90, "career_outcome": "Franchise Player"},
    {"name": "Nikola Jokic archetype", "position": "C", "ppg": 25, "rpg": 11, "apg": 9, "spg": 1.3, "bpg": 0.7, "fg_pct": 0.56, "three_pct": 0.35, "height_inches": 83, "wingspan_inches": 86, "career_outcome": "Franchise Player"},
    {"name": "Rudy Gobert archetype", "position": "C", "ppg": 14, "rpg": 13, "apg": 1, "spg": 0.7, "bpg": 2.3, "fg_pct": 0.66, "three_pct": 0.00, "height_inches": 85, "wingspan_inches": 92, "career_outcome": "All-Star"},
    {"name": "Myles Turner archetype", "position": "C", "ppg": 13, "rpg": 7, "apg": 1, "spg": 0.7, "bpg": 2.5, "fg_pct": 0.49, "three_pct": 0.35, "height_inches": 83, "wingspan_inches": 88, "career_outcome": "Quality Starter"},
    {"name": "Brook Lopez archetype", "position": "C", "ppg": 15, "rpg": 5, "apg": 2, "spg": 0.5, "bpg": 1.7, "fg_pct": 0.49, "three_pct": 0.34, "height_inches": 84, "wingspan_inches": 87, "career_outcome": "Quality Starter"},
    {"name": "Bismack Biyombo archetype", "position": "C", "ppg": 5, "rpg": 5, "apg": 0.5, "spg": 0.3, "bpg": 1.2, "fg_pct": 0.52, "three_pct": 0.00, "height_inches": 81, "wingspan_inches": 88, "career_outcome": "Bench Player"},
    {"name": "Hasheem Thabeet archetype", "position": "C", "ppg": 3, "rpg": 3, "apg": 0.2, "spg": 0.2, "bpg": 1.0, "fg_pct": 0.44, "three_pct": 0.00, "height_inches": 87, "wingspan_inches": 93, "career_outcome": "Bust"},
]

CAREER_TIER_THRESHOLDS: dict = {
    "Franchise Player": 85,
    "All-Star": 72,
    "Quality Starter": 58,
    "Rotation Player": 44,
    "Bench Player": 30,
    "Bust": 0,
}

# College-to-NBA stat discount factors (college stats are inflated)
_COLLEGE_TO_NBA_DISCOUNT: dict = {
    "ppg": 0.58,
    "rpg": 0.72,
    "apg": 0.62,
    "spg": 0.55,
    "bpg": 0.50,
    "fg_pct": 0.94,
    "three_pct": 0.90,
}


# ============================================================
# SECTION: translate_college_stats
# ============================================================

def translate_college_stats(prospect: dict) -> dict:
    """Convert college per-game stats to projected NBA statistics.

    Applies conference strength multiplier, age adjustment
    (younger prospects receive a higher ceiling factor), and
    per-40-minute normalization before applying college-to-NBA
    discount factors.

    Args:
        prospect: Dictionary with college stat keys such as
            ``ppg``, ``rpg``, ``apg``, ``spg``, ``bpg``,
            ``fg_pct``, ``three_pct``, ``minutes_per_game``,
            ``conference``, ``age``.

    Returns:
        Dictionary with projected NBA stats and meta-factors:
        ``projected_ppg``, ``projected_rpg``, ``projected_apg``,
        ``projected_spg``, ``projected_bpg``, ``projected_fg_pct``,
        ``projected_3pt_pct``, ``conference_factor``, ``age_factor``,
        ``translation_confidence``.
    """
    try:
        # Raw college stats
        ppg = _safe_float(prospect.get("ppg", 0))
        rpg = _safe_float(prospect.get("rpg", 0))
        apg = _safe_float(prospect.get("apg", 0))
        spg = _safe_float(prospect.get("spg", 0))
        bpg = _safe_float(prospect.get("bpg", 0))
        fg_pct = _safe_float(prospect.get("fg_pct", 0))
        three_pct = _safe_float(prospect.get("three_pct", 0))
        mpg = _safe_float(prospect.get("minutes_per_game", 30))
        conference = str(prospect.get("conference", "unknown")).strip()
        age = _safe_float(prospect.get("age", 21))

        # Per-40 normalization
        per40_factor = 40.0 / max(mpg, 1.0)
        ppg_40 = ppg * per40_factor
        rpg_40 = rpg * per40_factor
        apg_40 = apg * per40_factor
        spg_40 = spg * per40_factor
        bpg_40 = bpg * per40_factor

        # Conference strength
        conference_factor = CONFERENCE_STRENGTH.get(
            conference, CONFERENCE_STRENGTH.get("unknown", 0.85)
        )

        # Age factor: younger prospects get a ceiling bump
        # 18-year-old → 1.12, 19 → 1.08, 20 → 1.04, 21 → 1.0, 22 → 0.96, 23+ → 0.92
        if age <= 18:
            age_factor = 1.12
        elif age <= 19:
            age_factor = 1.08
        elif age <= 20:
            age_factor = 1.04
        elif age <= 21:
            age_factor = 1.00
        elif age <= 22:
            age_factor = 0.96
        else:
            age_factor = 0.92

        # Apply discounts, conference factor, and age factor
        projected_ppg = ppg_40 * _COLLEGE_TO_NBA_DISCOUNT["ppg"] * conference_factor * age_factor
        projected_rpg = rpg_40 * _COLLEGE_TO_NBA_DISCOUNT["rpg"] * conference_factor * age_factor
        projected_apg = apg_40 * _COLLEGE_TO_NBA_DISCOUNT["apg"] * conference_factor * age_factor
        projected_spg = spg_40 * _COLLEGE_TO_NBA_DISCOUNT["spg"] * conference_factor * age_factor
        projected_bpg = bpg_40 * _COLLEGE_TO_NBA_DISCOUNT["bpg"] * conference_factor * age_factor
        projected_fg_pct = fg_pct * _COLLEGE_TO_NBA_DISCOUNT["fg_pct"]
        projected_3pt_pct = three_pct * _COLLEGE_TO_NBA_DISCOUNT["three_pct"]

        # Confidence is higher when we have more data points present
        stat_keys = ["ppg", "rpg", "apg", "spg", "bpg", "fg_pct", "three_pct",
                     "minutes_per_game", "conference", "age"]
        present_count = sum(1 for k in stat_keys if prospect.get(k) is not None)
        translation_confidence = round(present_count / len(stat_keys), 2)

        return {
            "projected_ppg": round(projected_ppg, 1),
            "projected_rpg": round(projected_rpg, 1),
            "projected_apg": round(projected_apg, 1),
            "projected_spg": round(projected_spg, 1),
            "projected_bpg": round(projected_bpg, 1),
            "projected_fg_pct": round(projected_fg_pct, 3),
            "projected_3pt_pct": round(projected_3pt_pct, 3),
            "conference_factor": conference_factor,
            "age_factor": age_factor,
            "translation_confidence": translation_confidence,
        }

    except Exception:
        _logger.exception("[DraftProspect] Error translating college stats for %s",
                          prospect.get("name", "unknown"))
        return {
            "projected_ppg": 0.0, "projected_rpg": 0.0, "projected_apg": 0.0,
            "projected_spg": 0.0, "projected_bpg": 0.0, "projected_fg_pct": 0.0,
            "projected_3pt_pct": 0.0, "conference_factor": 0.85,
            "age_factor": 1.0, "translation_confidence": 0.0,
        }


# ============================================================
# SECTION: score_physical_profile
# ============================================================

def _infer_position(height_inches: float) -> str:
    """Infer a likely NBA position from height alone."""
    if height_inches <= 75:
        return "PG"
    if height_inches <= 77:
        return "SG"
    if height_inches <= 80:
        return "SF"
    if height_inches <= 82:
        return "PF"
    return "C"


def score_physical_profile(prospect: dict) -> dict:
    """Grade a prospect's physical measurements relative to their position.

    Generates sub-scores for length, athleticism, and lateral
    quickness, then assigns an overall letter grade.

    Args:
        prospect: Dictionary with physical measurement keys such as
            ``height_inches``, ``weight``, ``wingspan_inches``,
            ``standing_reach``, ``lane_agility``, ``sprint_3qrt``,
            ``vertical_leap``, ``position``.

    Returns:
        Dictionary with ``physical_grade``, ``length_score``,
        ``athleticism_score``, ``lateral_quickness_score``,
        ``size_for_position``, ``physical_comparison``.
    """
    try:
        height = _safe_float(prospect.get("height_inches", 0))
        weight = _safe_float(prospect.get("weight", 0))
        wingspan = _safe_float(prospect.get("wingspan_inches", 0))
        reach = _safe_float(prospect.get("standing_reach", 0))
        lane_agility = _safe_float(prospect.get("lane_agility", 0))
        sprint = _safe_float(prospect.get("sprint_3qrt", 0))
        vert = _safe_float(prospect.get("vertical_leap", 0))
        position = str(prospect.get("position", "")).upper().strip()

        if not position or position not in NBA_POSITION_AVERAGES:
            position = _infer_position(height) if height > 0 else "SF"

        avg = NBA_POSITION_AVERAGES[position]

        # Length score: wingspan relative to height + standing reach vs avg
        wingspan_diff = (wingspan - avg["wingspan_inches"]) if wingspan > 0 else 0
        reach_diff = (reach - avg["standing_reach"]) if reach > 0 else 0
        length_score = max(0.0, min(100.0,
            50.0
            + wingspan_diff * 5.0
            + reach_diff * 3.0
        ))

        # Athleticism: vertical leap and sprint
        vert_component = normalize(vert, 25, 44, 0, 100) if vert > 0 else 50.0
        sprint_component = normalize(avg["sprint_3qrt"] + 0.3 - sprint, -0.2, 0.5, 0, 100) if sprint > 0 else 50.0
        athleticism_score = max(0.0, min(100.0,
            0.55 * vert_component + 0.45 * sprint_component
        ))

        # Lateral quickness: lane agility (lower is better)
        if lane_agility > 0:
            lateral_quickness_score = max(0.0, min(100.0,
                normalize(avg["lane_agility"] + 0.8 - lane_agility, -0.5, 1.5, 0, 100)
            ))
        else:
            lateral_quickness_score = 50.0

        # Size for position
        height_diff = height - avg["height_inches"] if height > 0 else 0
        weight_diff = weight - avg["weight"] if weight > 0 else 0
        if height_diff >= 2 and weight_diff >= 10:
            size_for_position = "Elite size for position"
        elif height_diff >= 1 or weight_diff >= 5:
            size_for_position = "Above-average size"
        elif height_diff <= -2 or weight_diff <= -15:
            size_for_position = "Undersized for position"
        elif height_diff <= -1 or weight_diff <= -5:
            size_for_position = "Slightly undersized"
        else:
            size_for_position = "Average size for position"

        # Overall physical grade
        physical_score = (
            0.35 * length_score
            + 0.35 * athleticism_score
            + 0.30 * lateral_quickness_score
        )

        # Physical comparison descriptor
        if physical_score >= 85:
            physical_comparison = "Elite NBA-ready athlete"
        elif physical_score >= 70:
            physical_comparison = "Above-average physical tools"
        elif physical_score >= 55:
            physical_comparison = "Solid but unspectacular physical profile"
        elif physical_score >= 40:
            physical_comparison = "Below-average athleticism — relies on skill"
        else:
            physical_comparison = "Physical limitations may cap upside"

        return {
            "physical_grade": letter_grade(physical_score),
            "length_score": round(length_score, 1),
            "athleticism_score": round(athleticism_score, 1),
            "lateral_quickness_score": round(lateral_quickness_score, 1),
            "size_for_position": size_for_position,
            "physical_comparison": physical_comparison,
        }

    except Exception:
        _logger.exception("[DraftProspect] Error scoring physical profile for %s",
                          prospect.get("name", "unknown"))
        return {
            "physical_grade": "F",
            "length_score": 0.0,
            "athleticism_score": 0.0,
            "lateral_quickness_score": 0.0,
            "size_for_position": "Unknown",
            "physical_comparison": "Insufficient data",
        }


# ============================================================
# SECTION: find_historical_comparisons
# ============================================================

def _weighted_euclidean_distance(vec_a: list, vec_b: list, weights: list) -> float:
    """Compute weighted Euclidean distance between two vectors."""
    total = 0.0
    for a, b, w in zip(vec_a, vec_b, weights):
        total += w * (a - b) ** 2
    return math.sqrt(total)


def _normalize_stat(value: float, min_val: float, max_val: float) -> float:
    """Normalize a stat to [0, 1] range for comparison."""
    if max_val == min_val:
        return 0.5
    return max(0.0, min(1.0, (value - min_val) / (max_val - min_val)))


def find_historical_comparisons(prospect: dict, n_comps: int = 3) -> list:
    """Find the most statistically and physically similar historical archetypes.

    Uses a weighted Euclidean distance on a normalized stat vector
    covering scoring, rebounding, assists, defensive stats, shooting
    efficiency, and physical measurements.

    Args:
        prospect: Dictionary with NBA-projected or raw stats including
            ``ppg``, ``rpg``, ``apg``, ``spg``, ``bpg``, ``fg_pct``,
            ``three_pct``, ``height_inches``, ``wingspan_inches``.
        n_comps: Number of comparisons to return (default 3).

    Returns:
        List of dicts, each containing ``comp_name``,
        ``similarity_score`` (0-100), ``career_outcome``, and
        ``comparison_basis``.
    """
    try:
        # Use projected stats if available, fall back to raw
        ppg = _safe_float(prospect.get("projected_ppg", prospect.get("ppg", 0)))
        rpg = _safe_float(prospect.get("projected_rpg", prospect.get("rpg", 0)))
        apg = _safe_float(prospect.get("projected_apg", prospect.get("apg", 0)))
        spg = _safe_float(prospect.get("projected_spg", prospect.get("spg", 0)))
        bpg = _safe_float(prospect.get("projected_bpg", prospect.get("bpg", 0)))
        fg_pct = _safe_float(prospect.get("projected_fg_pct", prospect.get("fg_pct", 0)))
        three_pct = _safe_float(prospect.get("projected_3pt_pct", prospect.get("three_pct", 0)))
        height = _safe_float(prospect.get("height_inches", 0))
        wingspan = _safe_float(prospect.get("wingspan_inches", 0))

        # Normalization ranges (min, max) for each dimension
        ranges = [
            (0, 30),   # ppg
            (0, 14),   # rpg
            (0, 11),   # apg
            (0, 2.5),  # spg
            (0, 2.5),  # bpg
            (0.38, 0.58),  # fg_pct
            (0.0, 0.45),   # three_pct
            (70, 88),  # height
            (74, 94),  # wingspan
        ]
        weights = [1.5, 1.2, 1.3, 0.8, 0.8, 1.0, 1.0, 0.7, 0.7]

        prospect_vec = [
            _normalize_stat(ppg, *ranges[0]),
            _normalize_stat(rpg, *ranges[1]),
            _normalize_stat(apg, *ranges[2]),
            _normalize_stat(spg, *ranges[3]),
            _normalize_stat(bpg, *ranges[4]),
            _normalize_stat(fg_pct, *ranges[5]),
            _normalize_stat(three_pct, *ranges[6]),
            _normalize_stat(height, *ranges[7]),
            _normalize_stat(wingspan, *ranges[8]),
        ]

        comparisons = []
        for comp in HISTORICAL_COMP_DATABASE:
            comp_vec = [
                _normalize_stat(comp["ppg"], *ranges[0]),
                _normalize_stat(comp["rpg"], *ranges[1]),
                _normalize_stat(comp["apg"], *ranges[2]),
                _normalize_stat(comp["spg"], *ranges[3]),
                _normalize_stat(comp["bpg"], *ranges[4]),
                _normalize_stat(comp["fg_pct"], *ranges[5]),
                _normalize_stat(comp["three_pct"], *ranges[6]),
                _normalize_stat(comp["height_inches"], *ranges[7]),
                _normalize_stat(comp["wingspan_inches"], *ranges[8]),
            ]

            dist = _weighted_euclidean_distance(prospect_vec, comp_vec, weights)
            # Convert distance to a 0-100 similarity score
            max_possible_dist = math.sqrt(sum(w for w in weights))
            similarity = max(0.0, min(100.0, 100.0 * (1.0 - dist / max_possible_dist)))

            # Build a description of what dimensions matched most
            dim_names = ["scoring", "rebounding", "playmaking", "steals",
                         "shot-blocking", "FG efficiency", "3PT shooting",
                         "height", "wingspan"]
            diffs = []
            for i, name in enumerate(dim_names):
                d = abs(prospect_vec[i] - comp_vec[i]) * weights[i]
                diffs.append((d, name))
            diffs.sort(key=lambda x: x[0])
            top_matches = [name for _, name in diffs[:3]]
            comparison_basis = "Similar " + ", ".join(top_matches)

            comparisons.append({
                "comp_name": comp["name"],
                "similarity_score": round(similarity, 1),
                "career_outcome": comp["career_outcome"],
                "comparison_basis": comparison_basis,
            })

        comparisons.sort(key=lambda c: c["similarity_score"], reverse=True)
        return comparisons[:max(1, n_comps)]

    except Exception:
        _logger.exception("[DraftProspect] Error finding historical comparisons for %s",
                          prospect.get("name", "unknown"))
        return [{
            "comp_name": "Unknown",
            "similarity_score": 0.0,
            "career_outcome": "Unknown",
            "comparison_basis": "Insufficient data for comparison",
        }]


# ============================================================
# SECTION: predict_career_outcome
# ============================================================

def predict_career_outcome(prospect: dict) -> dict:
    """Predict the most likely career outcome tier for a draft prospect.

    Combines projected NBA stats, physical profile, age, and
    conference strength into a composite score, then classifies
    the prospect into one of six career tiers.

    Args:
        prospect: Dictionary with prospect data including stats,
            physical measurements, ``conference``, and ``age``.

    Returns:
        Dictionary with ``predicted_tier``, ``tier_probability``,
        ``bust_probability``, ``star_probability``, ``ceiling``,
        ``floor``, ``confidence``.
    """
    try:
        # Get translated stats
        translated = translate_college_stats(prospect)
        physical = score_physical_profile(prospect)

        proj_ppg = _safe_float(translated.get("projected_ppg", 0))
        proj_rpg = _safe_float(translated.get("projected_rpg", 0))
        proj_apg = _safe_float(translated.get("projected_apg", 0))
        proj_spg = _safe_float(translated.get("projected_spg", 0))
        proj_bpg = _safe_float(translated.get("projected_bpg", 0))
        conf_factor = _safe_float(translated.get("conference_factor", 0.85))
        age_factor = _safe_float(translated.get("age_factor", 1.0))
        translation_conf = _safe_float(translated.get("translation_confidence", 0.5))

        # Physical sub-scores
        length_score = _safe_float(physical.get("length_score", 50))
        athleticism_score = _safe_float(physical.get("athleticism_score", 50))
        lateral_score = _safe_float(physical.get("lateral_quickness_score", 50))

        # Stat-based score (0-100)
        scoring_component = normalize(proj_ppg, 0, 22, 0, 100)
        rebounding_component = normalize(proj_rpg, 0, 10, 0, 100)
        playmaking_component = normalize(proj_apg, 0, 7, 0, 100)
        defense_component = normalize(proj_spg + proj_bpg, 0, 3, 0, 100)

        stat_score = (
            0.35 * scoring_component
            + 0.20 * rebounding_component
            + 0.25 * playmaking_component
            + 0.20 * defense_component
        )

        # Physical score
        phys_score = (
            0.35 * length_score
            + 0.35 * athleticism_score
            + 0.30 * lateral_score
        )

        # Conference and age adjustments
        conf_bonus = (conf_factor - 0.85) * 100  # ranges roughly -0 to +15
        age_bonus = (age_factor - 1.0) * 50       # ranges roughly -4 to +6

        # Composite score
        composite = (
            0.50 * stat_score
            + 0.30 * phys_score
            + 0.10 * max(0, min(100, 50 + conf_bonus))
            + 0.10 * max(0, min(100, 50 + age_bonus))
        )
        composite = max(0.0, min(100.0, composite))

        # Determine tier
        predicted_tier = "Bust"
        for tier, threshold in sorted(CAREER_TIER_THRESHOLDS.items(),
                                       key=lambda x: x[1], reverse=True):
            if composite >= threshold:
                predicted_tier = tier
                break

        # Probabilities
        tier_probability = max(0.20, min(0.85,
            0.40 + (translation_conf - 0.5) * 0.4 + (composite - 50) * 0.005
        ))

        star_probability = max(0.0, min(1.0,
            normalize(composite, 50, 90, 0.0, 0.80)
        ))

        bust_probability = max(0.0, min(1.0,
            normalize(100 - composite, 30, 80, 0.05, 0.70)
        ))
        # Younger players have slightly lower bust risk
        bust_probability = max(0.0, bust_probability - (age_factor - 1.0) * 0.15)

        # Ceiling / floor descriptions
        tier_order = ["Bust", "Bench Player", "Rotation Player",
                      "Quality Starter", "All-Star", "Franchise Player"]
        tier_idx = tier_order.index(predicted_tier) if predicted_tier in tier_order else 0

        ceiling_idx = min(len(tier_order) - 1, tier_idx + 1)
        floor_idx = max(0, tier_idx - 1)
        ceiling = tier_order[ceiling_idx]
        floor = tier_order[floor_idx]

        confidence = max(0.1, min(0.95, translation_conf * 0.6 + 0.3))

        return {
            "predicted_tier": predicted_tier,
            "tier_probability": round(tier_probability, 2),
            "bust_probability": round(bust_probability, 2),
            "star_probability": round(star_probability, 2),
            "ceiling": ceiling,
            "floor": floor,
            "confidence": round(confidence, 2),
        }

    except Exception:
        _logger.exception("[DraftProspect] Error predicting career outcome for %s",
                          prospect.get("name", "unknown"))
        return {
            "predicted_tier": "Unknown",
            "tier_probability": 0.0,
            "bust_probability": 0.5,
            "star_probability": 0.0,
            "ceiling": "Unknown",
            "floor": "Unknown",
            "confidence": 0.0,
        }


# ============================================================
# SECTION: build_prospect_scouting_report
# ============================================================

def _identify_strengths(translated: dict, physical: dict, prospect: dict) -> list:
    """Identify top strengths from stats and physical profile."""
    strengths = []
    if _safe_float(translated.get("projected_ppg", 0)) >= 15:
        strengths.append("High-volume scoring ability")
    if _safe_float(translated.get("projected_apg", 0)) >= 5:
        strengths.append("Elite playmaking vision")
    if _safe_float(translated.get("projected_rpg", 0)) >= 7:
        strengths.append("Strong rebounder")
    if _safe_float(translated.get("projected_spg", 0)) >= 1.2:
        strengths.append("Disruptive perimeter defender")
    if _safe_float(translated.get("projected_bpg", 0)) >= 1.5:
        strengths.append("Rim protection presence")
    if _safe_float(translated.get("projected_3pt_pct", 0)) >= 0.36:
        strengths.append("Reliable three-point shooter")
    if _safe_float(translated.get("projected_fg_pct", 0)) >= 0.48:
        strengths.append("Efficient scorer inside the arc")
    if _safe_float(physical.get("length_score", 0)) >= 70:
        strengths.append("Exceptional length for position")
    if _safe_float(physical.get("athleticism_score", 0)) >= 70:
        strengths.append("Plus athleticism")
    if _safe_float(physical.get("lateral_quickness_score", 0)) >= 70:
        strengths.append("Quick lateral movement — switchable defender")
    age = _safe_float(prospect.get("age", 21))
    if age <= 19:
        strengths.append("Youth and development upside")
    return strengths if strengths else ["Well-rounded skill set"]


def _identify_weaknesses(translated: dict, physical: dict, prospect: dict) -> list:
    """Identify weaknesses from stats and physical profile."""
    weaknesses = []
    if _safe_float(translated.get("projected_3pt_pct", 0)) < 0.30:
        weaknesses.append("Limited three-point range")
    if _safe_float(translated.get("projected_apg", 0)) < 2:
        weaknesses.append("Minimal playmaking ability")
    if _safe_float(translated.get("projected_spg", 0)) + _safe_float(translated.get("projected_bpg", 0)) < 0.8:
        weaknesses.append("Low defensive activity")
    if _safe_float(physical.get("length_score", 50)) < 35:
        weaknesses.append("Short wingspan limits defensive versatility")
    if _safe_float(physical.get("athleticism_score", 50)) < 35:
        weaknesses.append("Below-average athleticism")
    if _safe_float(physical.get("lateral_quickness_score", 50)) < 35:
        weaknesses.append("Poor lateral mobility — vulnerable in switches")
    age = _safe_float(prospect.get("age", 21))
    if age >= 23:
        weaknesses.append("Older prospect — limited development runway")
    conf_factor = _safe_float(translated.get("conference_factor", 0.85))
    if conf_factor < 0.88:
        weaknesses.append("Competition level raises translation risk")
    return weaknesses if weaknesses else ["No glaring weaknesses identified"]


def _suggest_development_areas(translated: dict, physical: dict) -> list:
    """Suggest areas for development based on stat/physical gaps."""
    areas = []
    if _safe_float(translated.get("projected_3pt_pct", 0)) < 0.35:
        areas.append("Improve catch-and-shoot consistency from three")
    if _safe_float(translated.get("projected_apg", 0)) < 3:
        areas.append("Develop secondary playmaking skills")
    if _safe_float(physical.get("lateral_quickness_score", 50)) < 50:
        areas.append("Improve lateral footwork and on-ball defense")
    if _safe_float(translated.get("projected_fg_pct", 0)) < 0.44:
        areas.append("Refine finishing at the rim through contact")
    if _safe_float(physical.get("athleticism_score", 50)) < 50:
        areas.append("Strength and conditioning program for explosiveness")
    if _safe_float(translated.get("projected_spg", 0)) < 0.8:
        areas.append("Work on active hands and anticipation on defense")
    return areas if areas else ["Continue refining overall game"]


def _generate_joseph_take(prospect: dict, prediction: dict, comps: list) -> str:
    """Generate a one-sentence scouting take in Joseph's voice."""
    name = str(prospect.get("name", "This prospect"))
    tier = prediction.get("predicted_tier", "Unknown")
    bust_prob = _safe_float(prediction.get("bust_probability", 0.5))
    top_comp = comps[0]["comp_name"] if comps else "an unknown archetype"

    if tier in ("Franchise Player", "All-Star"):
        return (f"{name} projects as a {tier} with shades of {top_comp}. "
                f"This is a premium talent worth a top selection.")
    if tier == "Quality Starter":
        return (f"{name} has the profile of a solid {tier}, comparable to {top_comp}. "
                f"Low bust risk makes this a safe pick with starter upside.")
    if bust_prob >= 0.45:
        return (f"{name} is a high-variance prospect — the upside is real, "
                f"but a {bust_prob:.0%} bust probability demands caution.")
    return (f"{name} projects as a {tier} with a comp to {top_comp}. "
            f"A useful NBA player, but unlikely to move the needle as a franchise piece.")


def build_prospect_scouting_report(prospect: dict) -> dict:
    """Build a comprehensive scouting report for a draft prospect.

    Combines stat translation, physical profiling, historical
    comparisons, career prediction, and editorial analysis into
    a single scouting report dictionary.

    Args:
        prospect: Dictionary with college stats, physical measurements,
            ``name``, ``position``, ``conference``, ``age``, etc.

    Returns:
        Dictionary with ``overall_grade``, ``projected_nba_stats``,
        ``physical_profile``, ``historical_comps``, ``career_prediction``,
        ``strengths``, ``weaknesses``, ``development_areas``,
        ``joseph_take``.
    """
    try:
        translated = translate_college_stats(prospect)
        physical = score_physical_profile(prospect)
        comps = find_historical_comparisons({**prospect, **translated})
        prediction = predict_career_outcome(prospect)

        strengths = _identify_strengths(translated, physical, prospect)
        weaknesses = _identify_weaknesses(translated, physical, prospect)
        dev_areas = _suggest_development_areas(translated, physical)
        joseph_take = _generate_joseph_take(prospect, prediction, comps)

        # Overall grade: blend of stat projection, physical, and career prediction
        stat_score = (
            normalize(_safe_float(translated.get("projected_ppg", 0)), 0, 20, 0, 100) * 0.30
            + normalize(_safe_float(translated.get("projected_rpg", 0)), 0, 8, 0, 100) * 0.15
            + normalize(_safe_float(translated.get("projected_apg", 0)), 0, 6, 0, 100) * 0.20
            + normalize(
                _safe_float(translated.get("projected_spg", 0))
                + _safe_float(translated.get("projected_bpg", 0)),
                0, 3, 0, 100
            ) * 0.15
            + normalize(_safe_float(translated.get("projected_fg_pct", 0)), 0.38, 0.52, 0, 100) * 0.10
            + normalize(_safe_float(translated.get("projected_3pt_pct", 0)), 0.28, 0.42, 0, 100) * 0.10
        )

        phys_numeric = (
            _safe_float(physical.get("length_score", 50)) * 0.35
            + _safe_float(physical.get("athleticism_score", 50)) * 0.35
            + _safe_float(physical.get("lateral_quickness_score", 50)) * 0.30
        )

        tier_score_map = {
            "Franchise Player": 95, "All-Star": 80, "Quality Starter": 65,
            "Rotation Player": 50, "Bench Player": 35, "Bust": 15, "Unknown": 30,
        }
        tier_score = tier_score_map.get(prediction.get("predicted_tier", "Unknown"), 30)

        overall_numeric = 0.40 * stat_score + 0.25 * phys_numeric + 0.35 * tier_score
        overall_numeric = max(0.0, min(100.0, overall_numeric))

        return {
            "overall_grade": letter_grade(overall_numeric),
            "projected_nba_stats": translated,
            "physical_profile": physical,
            "historical_comps": comps,
            "career_prediction": prediction,
            "strengths": strengths,
            "weaknesses": weaknesses,
            "development_areas": dev_areas,
            "joseph_take": joseph_take,
        }

    except Exception:
        _logger.exception("[DraftProspect] Error building scouting report for %s",
                          prospect.get("name", "unknown"))
        return {
            "overall_grade": "F",
            "projected_nba_stats": {},
            "physical_profile": {},
            "historical_comps": [],
            "career_prediction": {},
            "strengths": [],
            "weaknesses": [],
            "development_areas": [],
            "joseph_take": "Unable to generate scouting report due to data error.",
        }


# ============================================================
# SECTION: rank_draft_class
# ============================================================

def rank_draft_class(prospects: list) -> list:
    """Rank a class of draft prospects by composite score.

    For each prospect, builds a scouting report and computes
    ceiling, floor, and bust-risk scores, then sorts by the
    overall composite score in descending order.

    Args:
        prospects: List of prospect dictionaries.

    Returns:
        Sorted list of dicts, each containing ``rank``, ``name``,
        ``overall_score``, ``ceiling_score``, ``floor_score``,
        ``bust_risk``.
    """
    try:
        ranked = []
        for prospect in prospects:
            report = build_prospect_scouting_report(prospect)
            prediction = report.get("career_prediction", {})
            translated = report.get("projected_nba_stats", {})
            physical = report.get("physical_profile", {})

            # Overall score derived from the report grade
            grade_str = report.get("overall_grade", "F")
            grade_to_score = {
                "A+": 98, "A": 93, "A-": 88, "B+": 83, "B": 78,
                "B-": 73, "C+": 68, "C": 63, "C-": 58, "D+": 53,
                "D": 48, "D-": 43, "F": 30,
            }
            overall_score = grade_to_score.get(grade_str, 30)

            # Ceiling: boost for youth and elite physical tools
            age_factor = _safe_float(translated.get("age_factor", 1.0))
            ceiling_bump = (age_factor - 0.92) * 50  # up to ~10 pts for young
            athleticism = _safe_float(physical.get("athleticism_score", 50))
            ceiling_score = max(0.0, min(100.0,
                overall_score + ceiling_bump + (athleticism - 50) * 0.15
            ))

            # Floor: penalize bust risk
            bust_prob = _safe_float(prediction.get("bust_probability", 0.5))
            floor_score = max(0.0, min(100.0,
                overall_score * (1.0 - bust_prob * 0.4)
            ))

            ranked.append({
                "rank": 0,
                "name": str(prospect.get("name", "Unknown")),
                "overall_score": round(overall_score, 1),
                "ceiling_score": round(ceiling_score, 1),
                "floor_score": round(floor_score, 1),
                "bust_risk": round(bust_prob, 2),
            })

        ranked.sort(key=lambda p: p["overall_score"], reverse=True)
        for i, entry in enumerate(ranked, start=1):
            entry["rank"] = i

        return ranked

    except Exception:
        _logger.exception("[DraftProspect] Error ranking draft class")
        return []
