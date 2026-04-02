# ============================================================
# FILE: engine/lineup_analysis.py
# PURPOSE: Lineup and combination analysis — synergy scoring,
#          net rating estimation, optimal rotation distribution,
#          closing-lineup selection, and weakness detection.
# CONNECTS TO: engine/math_helpers.py, data/advanced_metrics.py,
#              engine/joseph_strategy.py, engine/impact_metrics.py
# ============================================================

"""Lineup and combination analysis for NBA roster evaluation.

Provides functions for estimating multi-player net ratings,
computing pairwise synergy scores, finding optimal minute
distributions across a rotation, selecting crunch-time closing
lineups, and detecting structural lineup weaknesses (spacing,
rim protection, ball handling).

All public functions are wrapped in try/except blocks and return
type-correct fallback values on error so that callers never
receive unexpected types.
"""

import logging
import math
import itertools

_logger = logging.getLogger(__name__)

# ============================================================
# SECTION: External Imports (graceful fallbacks)
# ============================================================

try:
    from engine.math_helpers import _safe_float
except ImportError:
    _logger.warning("[LineupAnalysis] Could not import _safe_float from math_helpers")

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
    from data.advanced_metrics import normalize, classify_player_archetype
except ImportError:
    _logger.warning("[LineupAnalysis] Could not import from advanced_metrics")

    def normalize(value, min_val, max_val, out_min=0.0, out_max=100.0):
        """Standard min-max normalization with clamping."""
        if max_val == min_val:
            return out_min
        ratio = (value - min_val) / (max_val - min_val)
        result = out_min + ratio * (out_max - out_min)
        return max(out_min, min(out_max, result))

    def classify_player_archetype(player):
        return "Role Player"


# ============================================================
# SECTION: Constants
# ============================================================

LEAGUE_AVG_OFF_RATING = 112.0
LEAGUE_AVG_DEF_RATING = 112.0
LEAGUE_AVG_PACE = 99.5

# Archetype synergy matrix — maps frozenset pairs to bonus points (0-20).
# Higher values indicate archetypes that complement each other on the floor.
ARCHETYPE_SYNERGY_MATRIX: dict[frozenset[str], int] = {
    frozenset(("Floor General", "Rim Protector")):   18,
    frozenset(("Floor General", "3-and-D Wing")):    16,
    frozenset(("Floor General", "Alpha Scorer")):    14,
    frozenset(("Floor General", "Stretch Big")):     15,
    frozenset(("Floor General", "Energy Big")):      12,
    frozenset(("Primary Creator", "Rim Protector")): 17,
    frozenset(("Primary Creator", "3-and-D Wing")):  15,
    frozenset(("Primary Creator", "Stretch Big")):   14,
    frozenset(("Alpha Scorer", "Rim Protector")):    13,
    frozenset(("Alpha Scorer", "3-and-D Wing")):     14,
    frozenset(("Alpha Scorer", "Energy Big")):       11,
    frozenset(("Alpha Scorer", "Stretch Big")):      12,
    frozenset(("Alpha Scorer", "Floor General")):    14,
    frozenset(("3-and-D Wing", "Rim Protector")):    16,
    frozenset(("3-and-D Wing", "Stretch Big")):      10,
    frozenset(("3-and-D Wing", "Energy Big")):       12,
    frozenset(("Stretch Big", "Rim Protector")):      8,
    frozenset(("Two-Way Force", "Floor General")):   15,
    frozenset(("Two-Way Force", "3-and-D Wing")):    13,
    frozenset(("Two-Way Force", "Rim Protector")):   14,
    frozenset(("Two-Way Force", "Stretch Big")):     11,
    frozenset(("Sixth Man Spark", "Floor General")): 10,
    frozenset(("Sixth Man Spark", "Rim Protector")): 9,
    frozenset(("Microwave Scorer", "Floor General")):10,
    frozenset(("Glue Guy", "Alpha Scorer")):         12,
    frozenset(("Glue Guy", "Primary Creator")):      11,
    frozenset(("Glue Guy", "Rim Protector")):        10,
    frozenset(("Volume Scorer", "Rim Protector")):   12,
    frozenset(("Volume Scorer", "3-and-D Wing")):    13,
    frozenset(("Volume Scorer", "Floor General")):   11,
    frozenset(("Energy Big", "Rim Protector")):       9,
}

# Penalty applied when two players share the same position
POSITION_OVERLAP_PENALTY: float = 8.0

# Weights for closing-lineup scoring (must sum conceptually to ~1.0 influence)
CLUTCH_WEIGHT_FACTORS: dict = {
    "scoring":      0.35,
    "defense":      0.25,
    "ball_handling": 0.20,
    "experience":   0.20,
}

# Position groups for complementarity checks
_GUARD_POSITIONS = {"PG", "SG"}
_WING_POSITIONS = {"SF"}
_BIG_POSITIONS = {"PF", "C"}

# Maximum / minimum minutes constraints for rotation optimisation
_MAX_PLAYER_MINUTES = 38.0
_MIN_ACTIVE_MINUTES = 8.0

# Archetypes considered spacers (floor-spacing threat from three)
_SPACER_ARCHETYPES = {"3-and-D Wing", "Stretch Big", "Microwave Scorer"}

# Archetypes considered rim protectors
_RIM_PROTECTOR_ARCHETYPES = {"Rim Protector", "Energy Big"}

# Archetypes considered primary ball handlers
_BALL_HANDLER_ARCHETYPES = {"Floor General", "Primary Creator"}


# ============================================================
# SECTION: Internal Helpers
# ============================================================

def _get_player_name(player: dict) -> str:
    """Extract player name from a player dict with alias fallback."""
    return player.get("player_name", player.get("name", "Unknown"))


def _get_position(player: dict) -> str:
    """Extract uppercase position string from a player dict."""
    pos = player.get("position", "SF")
    return str(pos).upper().strip() if pos else "SF"


def _get_archetype(player: dict) -> str:
    """Return the player's archetype, computing it if absent."""
    arch = player.get("archetype", "")
    if arch:
        return arch
    try:
        return classify_player_archetype(player)
    except Exception:
        return "Role Player"


def _get_stat(player: dict, *keys, fallback=0.0) -> float:
    """Try multiple keys in order, returning the first valid float."""
    for key in keys:
        val = player.get(key)
        if val is not None:
            return _safe_float(val, fallback)
    return float(fallback)


# ============================================================
# SECTION: Synergy Scoring
# ============================================================

def calculate_synergy_score(player_a: dict, player_b: dict) -> dict:
    """Compute pairwise synergy between two players.

    Evaluates archetype complementarity (e.g. Floor General + Rim Protector
    yields high synergy), position overlap penalties, and complementary
    skill identification.

    Args:
        player_a: First player dictionary.
        player_b: Second player dictionary.

    Returns:
        Dictionary with keys ``synergy_score`` (0-100), ``complementary_skills``,
        ``overlap_penalties``, and ``archetype_fit``.
    """
    try:
        arch_a = _get_archetype(player_a)
        arch_b = _get_archetype(player_b)
        pos_a = _get_position(player_a)
        pos_b = _get_position(player_b)

        # ── Archetype bonus ────────────────────────────────
        pair_key = frozenset((arch_a, arch_b))
        archetype_bonus = ARCHETYPE_SYNERGY_MATRIX.get(pair_key, 5)

        # Same archetype = lower synergy (redundant skill sets)
        if arch_a == arch_b:
            archetype_bonus = max(0, archetype_bonus - 6)

        # ── Position complementarity ───────────────────────
        overlap_penalties: list[str] = []
        position_score = 10  # baseline

        if pos_a == pos_b:
            overlap_penalties.append(
                f"Position overlap: both play {pos_a} (-{POSITION_OVERLAP_PENALTY:.0f})"
            )
            position_score -= POSITION_OVERLAP_PENALTY
        else:
            # Guard + Big pairing is ideal
            a_group = ("guard" if pos_a in _GUARD_POSITIONS
                       else "big" if pos_a in _BIG_POSITIONS else "wing")
            b_group = ("guard" if pos_b in _GUARD_POSITIONS
                       else "big" if pos_b in _BIG_POSITIONS else "wing")
            if {a_group, b_group} == {"guard", "big"}:
                position_score += 5
            elif a_group != b_group:
                position_score += 2

        # ── Complementary skills detection ─────────────────
        complementary_skills: list[str] = []

        # Shooting complementarity
        fg3_a = _get_stat(player_a, "fg3_pct", fallback=0.0)
        fg3_b = _get_stat(player_b, "fg3_pct", fallback=0.0)
        if fg3_a > 0.36 or fg3_b > 0.36:
            complementary_skills.append("Floor spacing")

        # Playmaking + scoring synergy
        ast_a = _get_stat(player_a, "assists_avg", "apg", "ast", fallback=0.0)
        ast_b = _get_stat(player_b, "assists_avg", "apg", "ast", fallback=0.0)
        pts_a = _get_stat(player_a, "points_avg", "ppg", "pts", fallback=0.0)
        pts_b = _get_stat(player_b, "points_avg", "ppg", "pts", fallback=0.0)

        if (ast_a > 6 and pts_b > 20) or (ast_b > 6 and pts_a > 20):
            complementary_skills.append("Playmaker-scorer connection")

        # Defensive anchoring
        blk_a = _get_stat(player_a, "blocks_avg", "blk", "blocks", fallback=0.0)
        blk_b = _get_stat(player_b, "blocks_avg", "blk", "blocks", fallback=0.0)
        stl_a = _get_stat(player_a, "steals_avg", "stl", "steals", fallback=0.0)
        stl_b = _get_stat(player_b, "steals_avg", "stl", "steals", fallback=0.0)

        if (blk_a > 1.5 and stl_b > 1.2) or (blk_b > 1.5 and stl_a > 1.2):
            complementary_skills.append("Interior-perimeter defensive pairing")

        # Rebounding balance
        reb_a = _get_stat(player_a, "rebounds_avg", "rpg", "reb", fallback=0.0)
        reb_b = _get_stat(player_b, "rebounds_avg", "rpg", "reb", fallback=0.0)
        if reb_a > 8 or reb_b > 8:
            complementary_skills.append("Glass presence")

        # ── Skill overlap penalties ────────────────────────
        # Two high-usage players competing for touches
        usage_a = _get_stat(player_a, "usage_rate", "usg_pct", fallback=20.0)
        usage_b = _get_stat(player_b, "usage_rate", "usg_pct", fallback=20.0)
        if usage_a > 28 and usage_b > 28:
            overlap_penalties.append("Dual high-usage: touch competition")
            position_score -= 4

        # ── Archetype fit label ────────────────────────────
        if archetype_bonus >= 15:
            archetype_fit = "Elite"
        elif archetype_bonus >= 11:
            archetype_fit = "Strong"
        elif archetype_bonus >= 7:
            archetype_fit = "Moderate"
        elif archetype_bonus >= 4:
            archetype_fit = "Marginal"
        else:
            archetype_fit = "Poor"

        # ── Final score ────────────────────────────────────
        raw_score = (archetype_bonus * 3.5) + (position_score * 1.5)
        # Complementary skills give small bonus
        raw_score += len(complementary_skills) * 3.0
        synergy_score = max(0.0, min(100.0, raw_score))

        return {
            "synergy_score": round(synergy_score, 1),
            "complementary_skills": complementary_skills,
            "overlap_penalties": overlap_penalties,
            "archetype_fit": archetype_fit,
        }
    except Exception as exc:
        _logger.error("[LineupAnalysis] calculate_synergy_score error: %s", exc)
        return {
            "synergy_score": 25.0,
            "complementary_skills": [],
            "overlap_penalties": ["Error computing synergy"],
            "archetype_fit": "Unknown",
        }


# ============================================================
# SECTION: Net Rating Estimation
# ============================================================

def estimate_lineup_net_rating(players: list[dict], teams_data=None) -> dict:
    """Estimate net rating for a group of 2-5 players.

    Blends individual offensive and defensive ratings with pairwise
    synergy adjustments and an estimated pace factor.  The ``teams_data``
    parameter (optional dict keyed by team abbreviation) supplies
    team-level pace and ratings when available.

    Args:
        players: List of 2-5 player dictionaries.
        teams_data: Optional mapping of team abbreviation to team stats.

    Returns:
        Dictionary with ``offensive_rating``, ``defensive_rating``,
        ``net_rating``, ``pace_factor``, and ``minutes_overlap_est``.
    """
    try:
        if not players or len(players) < 2:
            return {
                "offensive_rating": LEAGUE_AVG_OFF_RATING,
                "defensive_rating": LEAGUE_AVG_DEF_RATING,
                "net_rating": 0.0,
                "pace_factor": 1.0,
                "minutes_overlap_est": 0.0,
            }

        n = min(len(players), 5)
        active = players[:n]
        teams_data = teams_data or {}

        # ── Per-player offensive / defensive contributions ──
        off_ratings: list[float] = []
        def_ratings: list[float] = []
        minutes_list: list[float] = []

        for p in active:
            pts = _get_stat(p, "points_avg", "ppg", "pts", fallback=12.0)
            ast = _get_stat(p, "assists_avg", "apg", "ast", fallback=2.0)
            tov = _get_stat(p, "turnovers_avg", "tov", fallback=1.5)
            fg_pct = _get_stat(p, "fg_pct", fallback=0.45)
            fta = _get_stat(p, "fta", fallback=3.0)

            # Rough individual offensive rating proxy
            scoring_eff = pts * (0.5 + fg_pct)
            playmaking = ast * 2.5
            turnover_cost = tov * 2.0
            ft_contrib = fta * 0.44
            p_off = LEAGUE_AVG_OFF_RATING + (scoring_eff + playmaking + ft_contrib - turnover_cost - 12.0) * 0.6

            blk = _get_stat(p, "blocks_avg", "blk", "blocks", fallback=0.3)
            stl = _get_stat(p, "steals_avg", "stl", "steals", fallback=0.7)
            reb = _get_stat(p, "rebounds_avg", "rpg", "reb", fallback=4.0)

            # Rough individual defensive rating proxy (lower is better)
            def_contrib = (blk * 2.5 + stl * 2.0 + reb * 0.5)
            p_def = LEAGUE_AVG_DEF_RATING - (def_contrib - 4.0) * 0.6

            off_ratings.append(p_off)
            def_ratings.append(p_def)
            minutes_list.append(_get_stat(p, "minutes_avg", "min", fallback=24.0))

        # ── Team-level pace adjustment ─────────────────────
        team_paces: list[float] = []
        for p in active:
            team_abbr = p.get("team", "")
            if team_abbr and team_abbr in teams_data:
                tp = _safe_float(teams_data[team_abbr].get("pace", LEAGUE_AVG_PACE),
                                 LEAGUE_AVG_PACE)
                team_paces.append(tp)
        avg_pace = (sum(team_paces) / len(team_paces)) if team_paces else LEAGUE_AVG_PACE
        pace_factor = avg_pace / LEAGUE_AVG_PACE

        # ── Aggregate ratings ──────────────────────────────
        base_off = sum(off_ratings) / n
        base_def = sum(def_ratings) / n

        # ── Synergy adjustment from pairwise scores ────────
        total_synergy_bonus = 0.0
        pair_count = 0
        for i in range(n):
            for j in range(i + 1, n):
                syn = calculate_synergy_score(active[i], active[j])
                total_synergy_bonus += (syn["synergy_score"] - 50.0) / 50.0
                pair_count += 1

        if pair_count > 0:
            avg_synergy_adj = total_synergy_bonus / pair_count
        else:
            avg_synergy_adj = 0.0

        # Synergy pushes offense up and defense down (better) by up to ~3 pts
        synergy_impact = avg_synergy_adj * 3.0
        final_off = base_off + synergy_impact * 0.6
        final_def = base_def - synergy_impact * 0.4

        net_rating = final_off - final_def

        # ── Minutes overlap estimate ───────────────────────
        if minutes_list:
            min_minutes = min(minutes_list)
            minutes_overlap_est = min_minutes * 0.65
        else:
            minutes_overlap_est = 0.0

        return {
            "offensive_rating": round(final_off, 2),
            "defensive_rating": round(final_def, 2),
            "net_rating": round(net_rating, 2),
            "pace_factor": round(pace_factor, 3),
            "minutes_overlap_est": round(minutes_overlap_est, 1),
        }
    except Exception as exc:
        _logger.error("[LineupAnalysis] estimate_lineup_net_rating error: %s", exc)
        return {
            "offensive_rating": LEAGUE_AVG_OFF_RATING,
            "defensive_rating": LEAGUE_AVG_DEF_RATING,
            "net_rating": 0.0,
            "pace_factor": 1.0,
            "minutes_overlap_est": 0.0,
        }


# ============================================================
# SECTION: Optimal Rotation
# ============================================================

def find_optimal_rotation(roster: list[dict], target_minutes: int = 240) -> dict:
    """Distribute *target_minutes* across a roster to maximise projected net rating.

    Uses a greedy approach: rank players by an efficiency proxy, allocate
    minutes proportionally, then clamp to [8, 38] per active player.
    Players below the efficiency threshold are marked inactive.

    Args:
        roster: List of 8-15 player dictionaries.
        target_minutes: Total team minutes to allocate (default 240 for
            a regulation game with 5 on the floor × 48 min).

    Returns:
        Dictionary with ``rotation`` (list of player-minute-role dicts)
        and ``projected_net_rating``.
    """
    try:
        if not roster:
            return {"rotation": [], "projected_net_rating": 0.0}

        target_minutes = max(48, min(target_minutes, 300))

        # ── Compute efficiency proxy for each player ───────
        scored_players: list[tuple[float, dict]] = []
        for p in roster:
            pts = _get_stat(p, "points_avg", "ppg", "pts", fallback=5.0)
            ast = _get_stat(p, "assists_avg", "apg", "ast", fallback=1.0)
            reb = _get_stat(p, "rebounds_avg", "rpg", "reb", fallback=2.0)
            stl = _get_stat(p, "steals_avg", "stl", "steals", fallback=0.5)
            blk = _get_stat(p, "blocks_avg", "blk", "blocks", fallback=0.3)
            tov = _get_stat(p, "turnovers_avg", "tov", fallback=1.0)
            fg_pct = _get_stat(p, "fg_pct", fallback=0.44)
            mins = _get_stat(p, "minutes_avg", "min", fallback=15.0)

            # Composite efficiency index (higher = more valuable per minute)
            eff = (pts * 1.0 + ast * 1.5 + reb * 0.8 + stl * 2.0
                   + blk * 2.0 - tov * 1.5 + fg_pct * 10.0)
            # Slight bonus for players already logging heavy minutes (role signal)
            eff += min(mins, 36) * 0.1

            scored_players.append((eff, p))

        # Sort descending by efficiency
        scored_players.sort(key=lambda x: x[0], reverse=True)

        # ── Determine active players (top 9 or fewer) ─────
        max_active = min(len(scored_players), 10)
        # At least 8 if available
        min_active = min(len(scored_players), 8)
        # Use up to 10 players; trim low-efficiency tail
        if max_active > min_active:
            threshold = scored_players[min_active - 1][0] * 0.45
            active_count = min_active
            for idx in range(min_active, max_active):
                if scored_players[idx][0] >= threshold:
                    active_count = idx + 1
                else:
                    break
        else:
            active_count = min_active

        active = scored_players[:active_count]
        total_eff = sum(e for e, _ in active)

        if total_eff <= 0:
            total_eff = float(active_count)

        # ── Proportional allocation ────────────────────────
        raw_alloc: list[tuple[float, float, dict]] = []
        for eff, p in active:
            share = eff / total_eff
            raw_mins = share * target_minutes
            raw_alloc.append((eff, raw_mins, p))

        # ── Clamp to [_MIN_ACTIVE_MINUTES, _MAX_PLAYER_MINUTES] ──
        clamped: list[tuple[float, float, dict]] = []
        overflow = 0.0
        for eff, mins, p in raw_alloc:
            if mins > _MAX_PLAYER_MINUTES:
                overflow += mins - _MAX_PLAYER_MINUTES
                mins = _MAX_PLAYER_MINUTES
            elif mins < _MIN_ACTIVE_MINUTES:
                overflow -= (_MIN_ACTIVE_MINUTES - mins)
                mins = _MIN_ACTIVE_MINUTES
            clamped.append((eff, mins, p))

        # Redistribute overflow across uncapped players
        if abs(overflow) > 0.5:
            uncapped_indices = [
                i for i, (_, m, _) in enumerate(clamped)
                if _MIN_ACTIVE_MINUTES < m < _MAX_PLAYER_MINUTES
            ]
            if uncapped_indices:
                per_player = overflow / len(uncapped_indices)
                redistributed: list[tuple[float, float, dict]] = []
                for i, (eff, mins, p) in enumerate(clamped):
                    if i in uncapped_indices:
                        adjusted = max(_MIN_ACTIVE_MINUTES,
                                       min(_MAX_PLAYER_MINUTES, mins + per_player))
                        redistributed.append((eff, adjusted, p))
                    else:
                        redistributed.append((eff, mins, p))
                clamped = redistributed

        # ── Normalise so total equals target_minutes exactly ──
        allocated_total = sum(m for _, m, _ in clamped)
        if allocated_total > 0 and abs(allocated_total - target_minutes) > 0.1:
            scale = target_minutes / allocated_total
            clamped = [
                (eff, max(_MIN_ACTIVE_MINUTES,
                          min(_MAX_PLAYER_MINUTES, mins * scale)), p)
                for eff, mins, p in clamped
            ]

        # ── Assign roles ──────────────────────────────────
        rotation: list[dict] = []
        for rank, (eff, mins, p) in enumerate(clamped):
            if rank < 5:
                role = "Starter"
            elif rank < 8:
                role = "Key Reserve"
            else:
                role = "Rotation Player"
            rotation.append({
                "player": _get_player_name(p),
                "minutes": round(mins, 1),
                "role": role,
            })

        # ── Projected net rating ──────────────────────────
        starters = [p for _, _, p in clamped[:5]]
        if len(starters) >= 2:
            nr = estimate_lineup_net_rating(starters)
            projected_net_rating = nr["net_rating"]
        else:
            projected_net_rating = 0.0

        return {
            "rotation": rotation,
            "projected_net_rating": round(projected_net_rating, 2),
        }
    except Exception as exc:
        _logger.error("[LineupAnalysis] find_optimal_rotation error: %s", exc)
        return {"rotation": [], "projected_net_rating": 0.0}


# ============================================================
# SECTION: Closing Lineup Selection
# ============================================================

def find_closing_lineup(roster: list[dict], game_context: dict) -> dict:
    """Select the best 5-man closing lineup for crunch time.

    Weights each candidate by clutch scoring ability, defensive
    versatility, ball-handling reliability, and experience proxy
    (minutes played as a rough experience signal).

    Args:
        roster: Full roster list (8-15 players).
        game_context: Dictionary with optional keys ``score_differential``
            (int), ``time_remaining`` (float, minutes), ``opponent_off_rating``
            (float).

    Returns:
        Dictionary with ``closing_five`` (list of player name strings),
        ``closing_score`` (float), ``strengths`` (list), and
        ``weaknesses`` (list).
    """
    try:
        if not roster or len(roster) < 5:
            names = [_get_player_name(p) for p in (roster or [])]
            return {
                "closing_five": names,
                "closing_score": 0.0,
                "strengths": [],
                "weaknesses": ["Insufficient roster size"],
            }

        game_context = game_context or {}
        score_diff = _safe_float(game_context.get("score_differential", 0), 0)
        opp_off = _safe_float(game_context.get("opponent_off_rating", LEAGUE_AVG_OFF_RATING),
                              LEAGUE_AVG_OFF_RATING)

        # Increase defensive weighting when trailing or facing elite offense
        defense_multiplier = 1.0
        if score_diff < -5 or opp_off > 115:
            defense_multiplier = 1.3

        # ── Score each player for closing suitability ──────
        player_scores: list[tuple[float, dict]] = []
        for p in roster:
            pts = _get_stat(p, "points_avg", "ppg", "pts", fallback=8.0)
            ast = _get_stat(p, "assists_avg", "apg", "ast", fallback=1.5)
            stl = _get_stat(p, "steals_avg", "stl", "steals", fallback=0.5)
            blk = _get_stat(p, "blocks_avg", "blk", "blocks", fallback=0.3)
            tov = _get_stat(p, "turnovers_avg", "tov", fallback=1.5)
            mins = _get_stat(p, "minutes_avg", "min", fallback=15.0)
            fg_pct = _get_stat(p, "fg_pct", fallback=0.44)
            ft_pct = _get_stat(p, "ft_pct", fallback=0.75)

            # Clutch scoring: points efficiency weighted by FT (late-game FTs matter)
            scoring = (pts * fg_pct + ft_pct * 5.0) * CLUTCH_WEIGHT_FACTORS["scoring"]

            # Defensive versatility: stocks minus liability
            defense = ((stl * 2.0 + blk * 2.5) * defense_multiplier
                       * CLUTCH_WEIGHT_FACTORS["defense"])

            # Ball handling: assists vs turnovers
            ball_handling = (ast * 1.5 - tov * 2.0) * CLUTCH_WEIGHT_FACTORS["ball_handling"]

            # Experience proxy: minutes played (veterans play more)
            experience = normalize(mins, 10.0, 36.0, 0.0, 10.0) * CLUTCH_WEIGHT_FACTORS["experience"]

            total = scoring + defense + ball_handling + experience
            player_scores.append((total, p))

        # Sort descending and pick top 5
        player_scores.sort(key=lambda x: x[0], reverse=True)
        closing_five_data = player_scores[:5]

        closing_names = [_get_player_name(p) for _, p in closing_five_data]
        closing_score = sum(s for s, _ in closing_five_data)
        closing_players = [p for _, p in closing_five_data]

        # ── Identify strengths ─────────────────────────────
        strengths: list[str] = []
        avg_pts = sum(_get_stat(p, "points_avg", "ppg", "pts", fallback=0)
                      for p in closing_players) / 5
        if avg_pts > 16:
            strengths.append("Elite scoring firepower")
        elif avg_pts > 12:
            strengths.append("Solid scoring depth")

        total_stocks = sum(
            _get_stat(p, "steals_avg", "stl", "steals", fallback=0)
            + _get_stat(p, "blocks_avg", "blk", "blocks", fallback=0)
            for p in closing_players
        )
        if total_stocks > 8:
            strengths.append("Disruptive defensive unit")

        handler_count = sum(
            1 for p in closing_players
            if _get_archetype(p) in _BALL_HANDLER_ARCHETYPES
        )
        if handler_count >= 2:
            strengths.append("Multiple ball-handling options")

        spacer_count = sum(
            1 for p in closing_players
            if _get_stat(p, "fg3_pct", fallback=0) > 0.35
        )
        if spacer_count >= 3:
            strengths.append("Excellent floor spacing")

        # ── Identify weaknesses via detect_lineup_weaknesses ──
        weaknesses = detect_lineup_weaknesses(closing_players)
        if not weaknesses:
            strengths.append("No critical weaknesses detected")

        return {
            "closing_five": closing_names,
            "closing_score": round(closing_score, 2),
            "strengths": strengths,
            "weaknesses": weaknesses,
        }
    except Exception as exc:
        _logger.error("[LineupAnalysis] find_closing_lineup error: %s", exc)
        return {
            "closing_five": [],
            "closing_score": 0.0,
            "strengths": [],
            "weaknesses": ["Error computing closing lineup"],
        }


# ============================================================
# SECTION: Full Combination Analysis
# ============================================================

def analyze_lineup_combination(players: list[dict]) -> dict:
    """Full analysis of a 2-5 player combination.

    Computes all pairwise synergy scores, aggregated net rating
    estimate, floor spacing score (three-point shooting density),
    rim protection presence, ball-handling assessment, and
    defensive versatility.  Includes a ``joseph_take`` text blurb
    summarising the lineup quality.

    Args:
        players: List of 2-5 player dictionaries.

    Returns:
        Dictionary with ``net_rating_est``, ``synergy_matrix``,
        ``spacing_score``, ``rim_protection``, ``ball_handling``,
        ``versatility_score``, and ``joseph_take``.
    """
    try:
        if not players or len(players) < 2:
            return {
                "net_rating_est": 0.0,
                "synergy_matrix": {},
                "spacing_score": 0.0,
                "rim_protection": 0.0,
                "ball_handling": 0.0,
                "versatility_score": 0.0,
                "joseph_take": "Need at least 2 players to analyze a lineup.",
            }

        active = players[:5]
        n = len(active)
        names = [_get_player_name(p) for p in active]

        # ── Net rating estimate ────────────────────────────
        nr = estimate_lineup_net_rating(active)
        net_rating_est = nr["net_rating"]

        # ── Pairwise synergy matrix ────────────────────────
        synergy_matrix: dict[str, dict[str, float]] = {}
        synergy_scores: list[float] = []
        for i in range(n):
            name_i = names[i]
            synergy_matrix[name_i] = {}
            for j in range(n):
                if i == j:
                    synergy_matrix[name_i][names[j]] = 0.0
                elif j > i:
                    syn = calculate_synergy_score(active[i], active[j])
                    score = syn["synergy_score"]
                    synergy_matrix[name_i][names[j]] = score
                    synergy_scores.append(score)
                else:
                    synergy_matrix[name_i][names[j]] = synergy_matrix[names[j]][name_i]

        avg_synergy = (sum(synergy_scores) / len(synergy_scores)) if synergy_scores else 50.0

        # ── Spacing score ──────────────────────────────────
        spacer_count = 0
        total_fg3_pct = 0.0
        for p in active:
            fg3 = _get_stat(p, "fg3_pct", fallback=0.0)
            fg3a = _get_stat(p, "fg3a", fallback=0.0)
            total_fg3_pct += fg3
            if fg3 > 0.34 and fg3a > 2.0:
                spacer_count += 1
            elif _get_archetype(p) in _SPACER_ARCHETYPES:
                spacer_count += 1

        # Spacing 0-100: based on count of spacers and average 3PT%
        avg_fg3 = total_fg3_pct / n if n > 0 else 0.0
        spacing_score = normalize(spacer_count, 0, 5, 0, 60) + normalize(avg_fg3, 0.30, 0.40, 0, 40)
        spacing_score = max(0.0, min(100.0, spacing_score))

        # ── Rim protection ─────────────────────────────────
        total_blk = sum(_get_stat(p, "blocks_avg", "blk", "blocks", fallback=0)
                        for p in active)
        rim_arch_count = sum(1 for p in active if _get_archetype(p) in _RIM_PROTECTOR_ARCHETYPES)
        rim_protection = normalize(total_blk, 0, 5, 0, 60) + normalize(rim_arch_count, 0, 2, 0, 40)
        rim_protection = max(0.0, min(100.0, rim_protection))

        # ── Ball handling ──────────────────────────────────
        total_ast = sum(_get_stat(p, "assists_avg", "apg", "ast", fallback=0)
                        for p in active)
        total_tov = sum(_get_stat(p, "turnovers_avg", "tov", fallback=0)
                        for p in active)
        handler_count = sum(1 for p in active if _get_archetype(p) in _BALL_HANDLER_ARCHETYPES)
        ast_tov = (total_ast / max(total_tov, 0.1))
        ball_handling = normalize(ast_tov, 1.0, 3.0, 0, 50) + normalize(handler_count, 0, 3, 0, 50)
        ball_handling = max(0.0, min(100.0, ball_handling))

        # ── Versatility score ──────────────────────────────
        positions = set(_get_position(p) for p in active)
        archetypes = set(_get_archetype(p) for p in active)
        pos_diversity = normalize(len(positions), 1, 5, 0, 50)
        arch_diversity = normalize(len(archetypes), 1, 5, 0, 50)
        versatility_score = max(0.0, min(100.0, pos_diversity + arch_diversity))

        # ── Joseph take ────────────────────────────────────
        joseph_take = _build_lineup_joseph_take(
            names, net_rating_est, avg_synergy, spacing_score,
            rim_protection, ball_handling, versatility_score,
        )

        return {
            "net_rating_est": round(net_rating_est, 2),
            "synergy_matrix": synergy_matrix,
            "spacing_score": round(spacing_score, 1),
            "rim_protection": round(rim_protection, 1),
            "ball_handling": round(ball_handling, 1),
            "versatility_score": round(versatility_score, 1),
            "joseph_take": joseph_take,
        }
    except Exception as exc:
        _logger.error("[LineupAnalysis] analyze_lineup_combination error: %s", exc)
        return {
            "net_rating_est": 0.0,
            "synergy_matrix": {},
            "spacing_score": 0.0,
            "rim_protection": 0.0,
            "ball_handling": 0.0,
            "versatility_score": 0.0,
            "joseph_take": "Could not analyze this lineup combination.",
        }


def _build_lineup_joseph_take(
    names: list[str],
    net_rating: float,
    avg_synergy: float,
    spacing: float,
    rim_protection: float,
    ball_handling: float,
    versatility: float,
) -> str:
    """Generate a Joseph-style text blurb for a lineup combination."""
    parts: list[str] = []
    if not names:
        return "Could not analyze this lineup — no player names provided."
    lineup_str = ", ".join(names[:-1]) + f" and {names[-1]}" if len(names) > 1 else names[0]

    # Overall verdict
    if net_rating > 5:
        parts.append(f"This {lineup_str} lineup is ELITE — a +{net_rating:.1f} net rating projection!")
    elif net_rating > 2:
        parts.append(f"The {lineup_str} group projects as a STRONG unit at +{net_rating:.1f}.")
    elif net_rating > 0:
        parts.append(f"{lineup_str} together should be a net positive at +{net_rating:.1f}.")
    elif net_rating > -2:
        parts.append(f"{lineup_str} are roughly neutral at {net_rating:.1f} — proceed with caution.")
    else:
        parts.append(f"I'm CONCERNED about {lineup_str} together — projecting {net_rating:.1f} net rating.")

    # Synergy commentary
    if avg_synergy > 70:
        parts.append("The synergy between these players is OFF THE CHARTS!")
    elif avg_synergy > 55:
        parts.append("Good complementary fit — these guys know how to play together.")
    elif avg_synergy < 35:
        parts.append("The fit is ROUGH — too much skill overlap, not enough complementarity.")

    # Highlight key dimension
    best_dim = max(
        [("spacing", spacing), ("rim protection", rim_protection),
         ("ball handling", ball_handling), ("versatility", versatility)],
        key=lambda x: x[1],
    )
    if best_dim[1] > 70:
        parts.append(f"Their {best_dim[0]} is the STRENGTH of this unit ({best_dim[1]:.0f}/100).")

    # Flag worst dimension
    worst_dim = min(
        [("spacing", spacing), ("rim protection", rim_protection),
         ("ball handling", ball_handling), ("versatility", versatility)],
        key=lambda x: x[1],
    )
    if worst_dim[1] < 35:
        parts.append(f"Watch out for {worst_dim[0]} — that's a WEAKNESS at {worst_dim[1]:.0f}/100.")

    return " ".join(parts)


# ============================================================
# SECTION: Weakness Detection
# ============================================================

def detect_lineup_weaknesses(players: list[dict]) -> list[str]:
    """Identify structural weaknesses in a lineup combination.

    Checks for missing rim protection, lack of floor spacing, absence
    of a primary ball handler, defensive liability players, and size
    mismatches.

    Args:
        players: List of 2-5 player dictionaries.

    Returns:
        List of human-readable weakness description strings.
        Empty list when no weaknesses are detected.
    """
    try:
        if not players:
            return ["Empty lineup — no players to evaluate"]

        weaknesses: list[str] = []
        active = players[:5]
        n = len(active)

        archetypes = [_get_archetype(p) for p in active]
        positions = [_get_position(p) for p in active]

        # ── No rim protector ──────────────────────────────
        total_blk = sum(_get_stat(p, "blocks_avg", "blk", "blocks", fallback=0)
                        for p in active)
        has_rim_protector = any(a in _RIM_PROTECTOR_ARCHETYPES for a in archetypes)
        if not has_rim_protector and total_blk < 1.5:
            weaknesses.append(
                "No rim protection: lineup lacks a shot-blocking presence "
                f"(total blocks avg: {total_blk:.1f})"
            )

        # ── No floor spacer ───────────────────────────────
        spacer_count = 0
        for p in active:
            fg3 = _get_stat(p, "fg3_pct", fallback=0.0)
            fg3a = _get_stat(p, "fg3a", fallback=0.0)
            if (fg3 > 0.34 and fg3a > 2.0) or _get_archetype(p) in _SPACER_ARCHETYPES:
                spacer_count += 1
        if spacer_count == 0:
            weaknesses.append(
                "No floor spacing: no reliable three-point shooter in the lineup"
            )
        elif spacer_count == 1 and n >= 4:
            weaknesses.append(
                "Limited floor spacing: only one three-point threat — "
                "opponents can pack the paint"
            )

        # ── No primary ball handler ───────────────────────
        handler_count = sum(1 for a in archetypes if a in _BALL_HANDLER_ARCHETYPES)
        total_ast = sum(_get_stat(p, "assists_avg", "apg", "ast", fallback=0)
                        for p in active)
        if handler_count == 0 and total_ast < 8:
            weaknesses.append(
                "No primary ball handler: lineup lacks a playmaker "
                f"(total assists avg: {total_ast:.1f})"
            )

        # ── Defensive liability ───────────────────────────
        for p in active:
            stl = _get_stat(p, "steals_avg", "stl", "steals", fallback=0.5)
            blk = _get_stat(p, "blocks_avg", "blk", "blocks", fallback=0.3)
            pts = _get_stat(p, "points_avg", "ppg", "pts", fallback=10.0)
            stocks = stl + blk
            # High-usage player with very low defensive contribution
            if pts > 18 and stocks < 0.8:
                weaknesses.append(
                    f"Defensive liability: {_get_player_name(p)} scores "
                    f"{pts:.1f} PPG but only {stocks:.1f} combined stocks"
                )

        # ── Size mismatch — too many smalls ───────────────
        big_count = sum(1 for pos in positions if pos in _BIG_POSITIONS)
        guard_count = sum(1 for pos in positions if pos in _GUARD_POSITIONS)
        if big_count == 0 and n >= 3:
            weaknesses.append(
                "Size mismatch: no true big (PF/C) in lineup — "
                "vulnerable on the boards and in the paint"
            )
        if guard_count == 0 and n >= 3:
            weaknesses.append(
                "Size mismatch: no true guard (PG/SG) in lineup — "
                "vulnerable to quick, perimeter-oriented attacks"
            )

        # ── Too many overlapping positions ────────────────
        from collections import Counter
        pos_counts = Counter(positions)
        for pos, count in pos_counts.items():
            if count >= 3:
                weaknesses.append(
                    f"Position logjam: {count} players at {pos} — "
                    f"creates minutes and spacing conflicts"
                )

        # ── Turnover-prone lineup ─────────────────────────
        total_tov = sum(_get_stat(p, "turnovers_avg", "tov", fallback=0)
                        for p in active)
        if total_tov > n * 2.5:
            weaknesses.append(
                f"Turnover-prone: lineup averages {total_tov:.1f} combined turnovers "
                f"({total_tov / n:.1f} per player)"
            )

        return weaknesses
    except Exception as exc:
        _logger.error("[LineupAnalysis] detect_lineup_weaknesses error: %s", exc)
        return ["Error detecting lineup weaknesses"]
