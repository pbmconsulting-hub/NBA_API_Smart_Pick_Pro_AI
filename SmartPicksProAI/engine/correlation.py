# ============================================================
# FILE: engine/correlation.py
# PURPOSE: Player correlation modeling for parlay construction.
#          Models positive/negative teammate correlations, game-level
#          correlations from pace/total, and usage cannibalization.
# CONNECTS TO: engine/entry_optimizer.py, pages/4 (Entry Builder)
# CONCEPTS COVERED: Pearson correlation, Gaussian copula (simplified),
#                   usage rate, game environment correlation
# ============================================================
# BEGINNER NOTE: Correlation in parlays matters because if two players
# are positively correlated (e.g. both benefit from a fast-paced game),
# the joint probability of both going over is HIGHER than multiplying
# their individual probabilities. Negative correlation means one player's
# success hurts the other's chances (usage competition).
import math
import statistics
def _safe_float(value, fallback=0.0):
    """Return *value* as a finite float, or *fallback* if NaN/inf/non-numeric."""
    try:
        v = float(value)
        if math.isfinite(v):
            return v
        return float(fallback)
    except (ValueError, TypeError):
        return float(fallback)
# Maximum correlation adjustment magnitude (conservative cap)
MAX_CORRELATION_ADJUSTMENT = 0.15  # 15% max adjustment to joint probability
# Recency decay factor for weighted Pearson correlation (4D).
# Each game older than the most recent decays by this factor.
# 0.9 means a game from 2 games ago gets 0.81x weight, 3 games ago = 0.729x.
RECENCY_DECAY_FACTOR = 0.9
# Cross-stat teammate correlations (4A).
# These model how two different stats from different teammates co-move.
# Keyed by (stat1, stat2) sorted alphabetically for canonical lookup.
CROSS_STAT_CORRELATIONS = {
    ("assists", "points"):        0.12,   # Scorer + playmaker synergy
    ("points", "rebounds"):      -0.05,   # Slight negative (different roles)
    ("points", "threes"):         0.25,   # Strong positive (threes contribute to points)
    ("assists", "rebounds"):      0.03,   # Near-independent
    ("assists", "threes"):        0.08,   # Positive (assist to 3-point shooter)
    ("blocks", "rebounds"):       0.18,   # Positive (big man stats)
    ("assists", "steals"):        0.10,   # Active hands correlate with court vision
    ("points", "turnovers"):      0.15,   # High usage → more of both
    ("assists", "turnovers"):     0.20,   # Ball-handler gets both
    ("blocks", "points"):        -0.08,   # Big man vs scorer role separation
    ("points", "steals"):         0.05,   # Mild positive (active players)
    ("rebounds", "steals"):       0.02,   # Minimal
    ("blocks", "turnovers"):      0.05,   # Minimal
    ("rebounds", "turnovers"):    0.04,   # Low usage bigs vs high usage guards
    ("steals", "threes"):         0.06,   # Active guards correlation
    ("blocks", "steals"):         0.08,   # Both defensive stats
    ("rebounds", "threes"):      -0.10,   # Big man vs perimeter role
    ("steals", "turnovers"):      0.12,   # Both guard/ball-handler stats
    ("assists", "blocks"):       -0.05,   # Guard vs big separation
    ("threes", "turnovers"):      0.08,   # Perimeter usage correlation
}
# Position-based correlation adjustments (4B).
# Two players at the same position often compete for usage (negative).
# Complementary positions have positive correlation.
# Keyed by (pos1, pos2) sorted alphabetically.
POSITION_CORRELATION_ADJUSTMENTS = {
    ("PG", "PG"):  -0.08,   # Two PGs: usage competition
    ("PG", "SG"):   0.05,   # Backcourt synergy
    ("PG", "SF"):   0.02,   # Mild positive
    ("PF", "PG"):   0.03,   # Mild positive
    ("C", "PG"):    0.10,   # Pick-and-roll connection
    ("SG", "SG"):  -0.06,   # Slight competition
    ("SF", "SG"):   0.03,   # Mild positive
    ("PF", "SG"):   0.01,   # Near-independent
    ("C", "SG"):    0.06,   # Backcourt-frontcourt synergy
    ("SF", "SF"):  -0.04,   # Slight competition
    ("PF", "SF"):   0.03,   # Mild positive
    ("C", "SF"):    0.04,   # Mild positive
    ("PF", "PF"):  -0.08,   # Big-man competition
    ("C", "PF"):   -0.10,   # Heavy frontcourt overlap
    ("C", "C"):    -0.12,   # Big-man competition (rebounds especially)
}
# Platform-specific default implied probabilities
# BEGINNER NOTE: These are the breakeven win rates for each platform's payout structure
PLATFORM_IMPLIED_PROB = {
    "PrizePicks":        0.526,
    "Underdog Fantasy":  0.500,
    "DraftKings Pick6":  0.5238,
    # Backward-compat aliases
    "Underdog":          0.500,
    "DraftKings":        0.5238,
    "default":           0.5238,
}
def calculate_pearson_correlation(values_a, values_b):
    """
    Calculate the Pearson correlation coefficient between two lists of values.
    BEGINNER NOTE: Pearson correlation ranges from -1 (perfectly opposite)
    to +1 (perfectly aligned). 0 = no linear relationship.
    We use this to see if two players' stats move together.
    Args:
        values_a (list of float): Player A's stat values across games
        values_b (list of float): Player B's stat values across games
    Returns:
        float: Pearson r (-1 to 1), or 0.0 if insufficient data
    """
    if len(values_a) < 3 or len(values_b) < 3:
        return 0.0
    n = min(len(values_a), len(values_b))
    if n < 3:
        return 0.0
    a = values_a[:n]
    b = values_b[:n]
    try:
        mean_a = statistics.mean(a)
        mean_b = statistics.mean(b)
        num = sum((a[i] - mean_a) * (b[i] - mean_b) for i in range(n))
        den_a = math.sqrt(sum((v - mean_a) ** 2 for v in a))
        den_b = math.sqrt(sum((v - mean_b) ** 2 for v in b))
        if den_a < 1e-9 or den_b < 1e-9:
            return 0.0
        r = num / (den_a * den_b)
        return max(-1.0, min(1.0, r))
    except (ValueError, ZeroDivisionError, statistics.StatisticsError):
        return 0.0
def calculate_player_correlation(player1_logs, player2_logs, stat_type):
    """
    Compute recency-weighted empirical correlation between two players' stats. (4D)
    Uses exponentially decaying weights (newest game = 1.0, each older game
    decays by 0.9x) so recent games matter more. Requires minimum 8 shared
    games. Blends empirical result with the heuristic prior (60/40) to
    prevent wild empirical values from small samples dominating.
    Args:
        player1_logs (list of dict): Game logs for player 1 (must have 'GAME_DATE')
        player2_logs (list of dict): Game logs for player 2
        stat_type (str): Stat to correlate ('points', 'rebounds', etc.)
    Returns:
        float: Blended Pearson correlation coefficient (-1 to 1)
    """
    stat_key_map = {
        "points": "PTS", "rebounds": "REB", "assists": "AST",
        "steals": "STL", "blocks": "BLK", "threes": "FG3M",
        "turnovers": "TOV",
    }
    api_key = stat_key_map.get(stat_type.lower(), stat_type.upper())
    def _get_date_stat_map(logs, key):
        result = {}
        for g in (logs or []):
            date = g.get("GAME_DATE", g.get("game_date", ""))
            if date:
                try:
                    val = float(g.get(key, 0) or 0)
                    result[date] = val
                except (ValueError, TypeError):
                    pass
        return result
    p1_map = _get_date_stat_map(player1_logs, api_key)
    p2_map = _get_date_stat_map(player2_logs, api_key)
    shared_dates = sorted(set(p1_map.keys()) & set(p2_map.keys()))
    # 4D: Require minimum 8 shared games for reliable empirical correlation
    if len(shared_dates) < 8:
        return 0.0
    a = [p1_map[d] for d in shared_dates]
    b = [p2_map[d] for d in shared_dates]
    # 4D: Recency weights — most recent game = 1.0, each older decays by 0.9x
    n = len(shared_dates)
    weights = [RECENCY_DECAY_FACTOR ** (n - 1 - i) for i in range(n)]  # oldest first
    total_w = sum(weights)
    # Weighted means
    mean_a = sum(w * v for w, v in zip(weights, a)) / total_w
    mean_b = sum(w * v for w, v in zip(weights, b)) / total_w
    # Weighted Pearson correlation
    num = sum(w * (ai - mean_a) * (bi - mean_b) for w, ai, bi in zip(weights, a, b))
    den_a = math.sqrt(max(0.0, sum(w * (ai - mean_a) ** 2 for w, ai in zip(weights, a))))
    den_b = math.sqrt(max(0.0, sum(w * (bi - mean_b) ** 2 for w, bi in zip(weights, b))))
    if den_a < 1e-9 or den_b < 1e-9:
        empirical_corr = 0.0
    else:
        empirical_corr = max(-1.0, min(1.0, num / (den_a * den_b)))
    # 4D: Blend with heuristic prior (60% empirical + 40% heuristic)
    heuristic_prior = get_teammate_correlation(stat_type)
    blended = 0.6 * empirical_corr + 0.4 * heuristic_prior
    return round(max(-1.0, min(1.0, blended)), 4)
def calculate_game_environment_correlation(game_total, stat_type):
    """
    Estimate how a game's over/under total affects players' stat probabilities.
    BEGINNER NOTE: High-scoring (high total) games benefit scorers and assist
    men. Low-total games tend to suppress counting stats across the board.
    This is a "game-environment" correlation factor.
    Args:
        game_total (float or None): The game's over/under total (e.g. 228.5)
        stat_type (str): The prop stat type ('points', 'assists', etc.)
    Returns:
        float: Correlation boost/penalty per player in this game (-MAX_CORRELATION_ADJUSTMENT to +MAX_CORRELATION_ADJUSTMENT)
    """
    if game_total is None:
        return 0.0
    # BEGINNER NOTE: League average game total is ~220-225. High totals (230+)
    # create positive environment for scorers; low totals (210-) hurt them.
    LEAGUE_AVG_TOTAL = 222.0
    deviation = game_total - LEAGUE_AVG_TOTAL
    # Scale: ±10 pts from average → ±0.05 correlation factor
    factor = deviation * 0.005
    # Defense/hustle stats are less affected by game total
    low_impact_stats = {"steals", "blocks", "turnovers"}
    if stat_type.lower() in low_impact_stats:
        factor *= 0.3
    return max(-0.12, min(0.12, factor))
def calculate_usage_cannibalization(player1_usage_rate, player2_usage_rate, stat_type, is_teammate):
    """
    Model negative correlation where one player's high usage reduces teammate's.
    BEGINNER NOTE: A team's total possessions are finite. If Player A uses 30%
    of possessions, Player B has fewer left. When both players have high usage
    rates and play the same position, there's "cannibalization" — if A goes
    huge (high usage game), B likely gets fewer touches.
    Args:
        player1_usage_rate (float): Player 1 usage rate (0-1, e.g. 0.28 for 28%)
        player2_usage_rate (float): Player 2 usage rate (0-1)
        stat_type (str): Stat type being analyzed
        is_teammate (bool): Whether the two players are on the same team
    Returns:
        float: Negative correlation factor (negative means their props are negatively correlated)
    """
    if not is_teammate:
        return 0.0
    # Only scoring-related stats have usage competition
    scoring_stats = {"points", "assists", "threes", "fantasy_score"}
    if stat_type.lower() not in scoring_stats:
        return 0.0
    # Combined usage > 50% = significant cannibalization risk
    combined = (player1_usage_rate or 0.20) + (player2_usage_rate or 0.20)
    if combined > 0.50:
        # Strong negative correlation when total usage is high
        cannibalization = -(combined - 0.50) * 0.4
        return max(-MAX_CORRELATION_ADJUSTMENT, cannibalization)
    return 0.0
def get_teammate_correlation(stat_type, stat_type2=None, game_total=None):
    """
    Return estimated correlation for two teammates' prop outcomes based on stat type.
    Uses empirically-grounded values based on NBA analytics research.
    Same-team same-stat correlations are typically negative (usage competition),
    while cross-stat correlations can be positive (assists and points are linked).
    BEGINNER NOTE: Points is highly competitive between teammates (negative),
    while rebounds can be complementary (one's miss can become another's board).
    Assists correlate positively with scorers (need each other).
    Game total scaling: higher-total games increase positive correlations as
    the fast pace benefits multiple players simultaneously.
    Args:
        stat_type (str): Prop stat type
        game_total (float or None): Vegas game total, used to scale correlations.
            Higher totals amplify correlations (good for scorers, pacers).
    Returns:
        float: Estimated correlation (-1 to 1)
    """
    # 4A: Cross-stat lookup when two different stat types are provided
    if stat_type2 is not None and stat_type2.lower() != stat_type.lower():
        s1, s2 = sorted([stat_type.lower(), stat_type2.lower()])
        cross_corr = CROSS_STAT_CORRELATIONS.get((s1, s2), 0.0)
        # Apply same game total scaling
        if game_total is not None:
            LEAGUE_AVG = 222.0
            deviation = (game_total - LEAGUE_AVG) / LEAGUE_AVG
            effect = max(-0.03, min(0.03, deviation * 0.10))
            cross_corr = cross_corr + abs(cross_corr) * effect if cross_corr >= 0 else cross_corr * (1.0 - effect)
        return round(max(-1.0, min(1.0, cross_corr)), 4)
    # BEGINNER NOTE: These values are based on published NBA analytics research
    # on same-team prop correlations. Key findings:
    # - Same-team same-stat (points vs points): ≈ -0.12 (shot competition)
    # - Same-team different-stat (pts vs ast): ≈ +0.08 (playmaker/scorer symbiosis)
    # - Opponent same-stat: ≈ +0.03 (high-scoring matchups boost both)
    # Source: empirical NBA prop betting correlation research
    heuristics = {
        "points":               -0.12,   # Shot competition between teammates
        "rebounds":              0.05,   # Team rebounding correlates slightly
        "assists":               0.08,   # Playmakers help each other's efficiency
        "threes":               -0.08,   # 3PT competition (similar role players)
        "steals":                0.04,   # Team defense effort correlates
        "blocks":                0.03,   # Independent, slight team defense link
        "turnovers":             0.06,   # High-usage → more TOV for both stars
        "fantasy_score":        -0.06,   # Overall mild negative
        "points_rebounds":       0.01,   # Near-zero
        "points_assists":        0.08,   # Positive (scorer and playmaker feed each other)
        "rebounds_assists":      0.04,   # Slight positive
        "points_rebounds_assists": 0.02, # Near-zero (complex combo)
    }
    base_corr = heuristics.get(stat_type.lower(), 0.0)
    # Game total scaling: high-total games (230+) increase positive correlations
    # as fast pace creates more opportunities for ALL players simultaneously
    if game_total is not None:
        LEAGUE_AVG = 222.0
        deviation = (game_total - LEAGUE_AVG) / LEAGUE_AVG  # normalized
        # Positive deviation increases positive correlations, reduces negative ones
        # Effect is small (max ±0.03 for extreme game totals)
        game_total_effect = deviation * 0.10
        game_total_effect = max(-0.03, min(0.03, game_total_effect))
        # Apply: for positive base corr, boost further; for negative, reduce magnitude
        if base_corr >= 0:
            base_corr = base_corr + abs(base_corr) * game_total_effect
        else:
            base_corr = base_corr * (1.0 - game_total_effect)
    return round(max(-1.0, min(1.0, base_corr)), 4)
def get_position_correlation_adjustment(pos1, pos2):
    """
    Return the additive correlation adjustment for two players' positions. (4B)
    Playing-position affects how much teammates' stats co-move.
    Two point guards split ball-handling duties (negative), while
    a PG and C benefit from pick-and-roll synergy (positive).
    BEGINNER NOTE: Position-based adjustments are added ON TOP of the
    stat-type-based teammate correlation. They represent the structural
    relationship between the two players' roles on the court.
    Args:
        pos1 (str): Position of player 1 ('PG', 'SG', 'SF', 'PF', 'C')
        pos2 (str): Position of player 2
    Returns:
        float: Additive adjustment to apply to base teammate correlation
    Example:
        get_position_correlation_adjustment("PG", "C") → 0.10  (pick-and-roll)
        get_position_correlation_adjustment("C", "C")  → -0.12 (rebounding competition)
    """
    # Normalize positions (remove suffixes like G, F, G/F, etc.)
    def _normalize_pos(p):
        p = str(p).upper().strip()
        # Map common variants to canonical positions
        _pos_map = {
            "G": "PG", "F": "SF", "C": "C",
            "G/F": "SG", "F/C": "PF", "C/F": "PF",
        }
        return _pos_map.get(p, p)
    p1 = _normalize_pos(pos1)
    p2 = _normalize_pos(pos2)
    # Use sorted tuple for symmetric lookup
    key = tuple(sorted([p1, p2]))
    return POSITION_CORRELATION_ADJUSTMENTS.get(key, 0.0)
def get_within_player_cross_stat_correlation(stat1, stat2):
    """
    Return the within-player cross-stat correlation for multi-stat parlays.
    BEGINNER NOTE: A player's stats within the same game are NOT independent.
    If LeBron has a huge scoring night (high PTS), he likely also has more
    assists (both driven by heavy usage/minutes). This within-player correlation
    must be accounted for in multi-stat (PRA, etc.) parlay analysis.
    Common cross-stat correlations (based on NBA data):
    - PTS <-> AST: +0.35 (both driven by offensive role/minutes)
    - PTS <-> REB: +0.25 (high-minute players get more of both)
    - REB <-> BLK: +0.20 (big men who rebound also block)
    - PTS <-> STL: +0.15 (active offensive players create steals on D)
    - PTS <-> TOV: +0.30 (high usage = more scoring AND more turnovers)
    Args:
        stat1 (str): First stat ('points', 'rebounds', etc.)
        stat2 (str): Second stat
    Returns:
        float: Within-player cross-stat correlation (-1 to 1)
    """
    # Normalize to lowercase and sort for lookup
    s1, s2 = stat1.lower(), stat2.lower()
    key = tuple(sorted([s1, s2]))
    # Empirically-grounded within-player cross-stat correlations
    CROSS_STAT_CORR = {
        ("assists", "points"):        0.35,   # Both driven by offensive role
        ("points", "rebounds"):       0.25,   # High-minute players get both
        ("blocks", "rebounds"):       0.20,   # Big man overlap
        ("assists", "rebounds"):      0.10,   # Point-forward overlap
        ("points", "steals"):         0.15,   # Active on both ends
        ("points", "turnovers"):      0.30,   # High usage = scoring + TOV
        ("assists", "turnovers"):     0.25,   # Playmakers commit turnovers
        ("blocks", "steals"):         0.08,   # Both defensive stats, modest corr
        ("points", "threes"):         0.45,   # Volume shooters hit many 3s
        ("assists", "threes"):        0.12,   # Passers sometimes shoot too
        ("rebounds", "steals"):       0.05,   # Minimal overlap
        ("blocks", "turnovers"):      0.05,   # Minimal
    }
    return CROSS_STAT_CORR.get(key, 0.0)
def adjust_parlay_probability(individual_probs, correlation_matrix):
    """
    Adjust the joint probability of a parlay using a simplified Gaussian copula.
    BEGINNER NOTE: For independent props, parlay probability = p1 * p2 * p3 * ...
    But correlated props need adjustment. Positive correlation means the joint
    probability is HIGHER than the product. Negative correlation = LOWER.
    We use a simplified linear correction capped at ±15%.
    Args:
        individual_probs (list of float): Individual win probabilities for each leg
        correlation_matrix (list of list of float): Square matrix of pairwise correlations
            e.g. [[1.0, 0.1], [0.1, 1.0]] for two props with 0.1 correlation
    Returns:
        float: Adjusted joint probability (0.0 to 1.0)
    Example:
        Two players, p=[0.65, 0.60], correlation=0.2
        → base prob: 0.39, correlation boost: ~+0.01 → 0.40
    """
    if not individual_probs:
        return 0.0
    if len(individual_probs) == 1:
        return individual_probs[0]
    # Base: independent product
    base_prob = 1.0
    for p in individual_probs:
        base_prob *= max(0.001, min(0.999, p))
    n = len(individual_probs)
    if not correlation_matrix or len(correlation_matrix) < n:
        return base_prob
    # Compute weighted average pairwise correlation
    total_corr = 0.0
    pair_count = 0
    for i in range(n):
        for j in range(i + 1, n):
            try:
                c = float(correlation_matrix[i][j])
            except (IndexError, TypeError, ValueError):
                c = 0.0
            total_corr += c
            pair_count += 1
    if pair_count == 0:
        return base_prob
    avg_corr = total_corr / pair_count
    # BEGINNER NOTE: Adjustment formula (simplified linear copula correction):
    # positive corr → multiply base_prob by (1 + corr * avg_prob_factor)
    # negative corr → multiply base_prob by (1 - |corr| * avg_prob_factor)
    avg_prob = sum(individual_probs) / n
    # The adjustment is proportional to both correlation and how extreme the probs are
    prob_factor = avg_prob * (1 - avg_prob)  # maximized at p=0.5
    # BEGINNER NOTE: Scale factor of 2.0 normalizes the prob_factor (max 0.25 at p=0.5)
    # so that full correlation (r=1) with average probability gives a meaningful adjustment.
    # Without this factor, the adjustment would be too small to matter.
    _COPULA_SCALE = 2.0
    adjustment = avg_corr * prob_factor * _COPULA_SCALE
    # Cap adjustment to MAX_CORRELATION_ADJUSTMENT
    adjustment = max(-MAX_CORRELATION_ADJUSTMENT, min(MAX_CORRELATION_ADJUSTMENT, adjustment))
    adjusted = base_prob * (1.0 + adjustment)
    return max(0.0001, min(0.9999, adjusted))
def build_correlation_matrix(picks, game_logs_by_player=None):
    """
    Build a pairwise correlation matrix for a list of picks.
    For each pair of picks, compute correlation using historical game log data
    if available, otherwise use heuristic values.
    BEGINNER NOTE: The matrix entry [i][j] is the correlation between pick i
    and pick j. Same player = 1.0. Teammates = heuristic or empirical.
    Opponents = game-environment correlation.
    Args:
        picks (list of dict): Pick dicts with 'player_name', 'team', 'stat_type'
        game_logs_by_player (dict or None): {player_name: [game_log_dicts]}
    Returns:
        list of list of float: n×n correlation matrix
    """
    n = len(picks)
    if n == 0:
        return []
    # Identity matrix as baseline
    matrix = [[0.0] * n for _ in range(n)]
    for i in range(n):
        matrix[i][i] = 1.0
    for i in range(n):
        for j in range(i + 1, n):
            p1 = picks[i]
            p2 = picks[j]
            p1_name = str(p1.get("player_name", "")).lower()
            p2_name = str(p2.get("player_name", "")).lower()
            p1_team = str(p1.get("team", "")).upper()
            p2_team = str(p2.get("team", "")).upper()
            stat1   = str(p1.get("stat_type", "points")).lower()
            stat2   = str(p2.get("stat_type", "points")).lower()
            # Same player — identical prop (high correlation)
            if p1_name == p2_name:
                corr = 0.85
            # Teammates
            elif p1_team and p2_team and p1_team == p2_team:
                # Try empirical first
                if game_logs_by_player:
                    logs1 = game_logs_by_player.get(p1_name, [])
                    logs2 = game_logs_by_player.get(p2_name, [])
                    if stat1 == stat2 and len(logs1) >= 5 and len(logs2) >= 5:
                        corr = calculate_player_correlation(logs1, logs2, stat1)
                    else:
                        corr = get_teammate_correlation(stat1)
                else:
                    corr = get_teammate_correlation(stat1)
            else:
                # 4C: Opponent correlation — not 0; they share game environment
                p1_stat = str(p1.get("stat_type", "")).lower()
                p2_stat = str(p2.get("stat_type", "")).lower()
                game_total_ctx = None
                spread_ctx = 0.0
                # Try to get game context from pick dicts
                game_total_ctx = p1.get("game_total") or p2.get("game_total")
                spread_ctx = float(p1.get("vegas_spread", 0.0) or 0.0)
                gt = float(game_total_ctx) if game_total_ctx else 225.0
                if gt >= 230.0:
                    if p1_stat == "points" and p2_stat == "points":
                        corr = 0.10  # Both scorers in high-total game benefit
                    else:
                        corr = 0.06  # General pace benefit
                elif abs(spread_ctx) > 8.0:
                    # Heavy favorite/underdog — star of favored team may sit
                    corr = -0.08 * (abs(spread_ctx) / 15.0)
                    corr = max(-0.12, corr)
                else:
                    corr = 0.0
            # Cap
            corr = max(-MAX_CORRELATION_ADJUSTMENT, min(MAX_CORRELATION_ADJUSTMENT, corr))
            matrix[i][j] = corr
            matrix[j][i] = corr
    return matrix
def get_correlation_summary(picks, correlation_matrix):
    """
    Produce a human-readable summary of correlation risk for a set of picks.
    Args:
        picks (list of dict): Picks with player_name, team, stat_type
        correlation_matrix (list of list of float): Pairwise correlation matrix
    Returns:
        dict: {
            'risk_level': str,      # 'low', 'medium', 'high'
            'avg_correlation': float,
            'correlated_pairs': list of dict,
            'description': str,
        }
    """
    n = len(picks)
    if n < 2 or not correlation_matrix:
        return {
            "risk_level": "low",
            "avg_correlation": 0.0,
            "correlated_pairs": [],
            "description": "Single pick — no correlation risk.",
        }
    pairs = []
    total_corr = 0.0
    pair_count = 0
    for i in range(n):
        for j in range(i + 1, n):
            try:
                c = float(correlation_matrix[i][j])
            except (IndexError, TypeError, ValueError):
                c = 0.0
            total_corr += c
            pair_count += 1
            if abs(c) > 0.05:
                pairs.append({
                    "player1": picks[i].get("player_name", "?"),
                    "player2": picks[j].get("player_name", "?"),
                    "correlation": round(c, 3),
                    "direction": "positive" if c > 0 else "negative",
                })
    avg = total_corr / max(pair_count, 1)
    if avg > 0.10:
        risk_level = "high"
        desc = "⚠️ High positive correlation — these players likely go over/under together. Parlay probability is boosted but also means all can fail together."
    elif avg < -0.05:
        risk_level = "medium"
        desc = "⚡ Negative correlation detected — some players compete for usage. If one goes huge, another may underperform."
    elif abs(avg) < 0.03:
        risk_level = "low"
        desc = "✅ Low correlation — picks are relatively independent. Good for parlay construction."
    else:
        risk_level = "medium"
        desc = "ℹ️ Moderate correlation — some relationship between picks. Consider diversifying."
    return {
        "risk_level": risk_level,
        "avg_correlation": _safe_float(round(avg, 4), 0.0),
        "correlated_pairs": pairs,
        "description": desc,
    }
def get_correlation_confidence(picks, correlation_matrix):
    """
    Measure how safe a parlay's correlation structure is. (4E)
    High positive correlations between picks = correlated failure risk.
    All independent picks = higher confidence.
    Mix of positive and negative = moderate confidence.
    BEGINNER NOTE: In a parlay, if all picks are correlated (they all
    go over or all go under together), you're essentially making one
    big bet. Diversification — picks from different games and different
    stat types — reduces this risk.
    Args:
        picks (list of dict): Picks with 'player_name', 'team', 'stat_type', 'game'
        correlation_matrix (list of list of float): n×n pairwise correlations
    Returns:
        dict: {
            'correlation_confidence': float 0-100,
            'correlation_risk_level': str ('low', 'medium', 'high'),
            'diversification_score': float 0-1 (unique games / total picks),
        }
    Example:
        4 picks all from same game, corr≈0.10 → low diversification,
        lower correlation_confidence
    """
    n = len(picks)
    if n == 0:
        return {"correlation_confidence": 50.0, "correlation_risk_level": "medium",
                "diversification_score": 1.0}
    if n == 1:
        return {"correlation_confidence": 75.0, "correlation_risk_level": "low",
                "diversification_score": 1.0}
    # Diversification: unique games / total picks
    unique_games = len(set(
        frozenset([str(p.get("team", "")), str(p.get("opponent", ""))])
        for p in picks
        if p.get("team") or p.get("opponent")
    ))
    diversification_score = min(1.0, unique_games / max(1, n))
    # Average pairwise absolute correlation
    total_corr = 0.0
    pair_count = 0
    for i in range(n):
        for j in range(i + 1, n):
            try:
                c = float(correlation_matrix[i][j])
            except (IndexError, TypeError, ValueError):
                c = 0.0
            total_corr += abs(c)
            pair_count += 1
    avg_abs_corr = total_corr / max(1, pair_count)
    # Correlation confidence: inversely proportional to average correlation
    # High avg correlation (0.12) = lower confidence; near 0 = higher confidence
    corr_confidence = max(0.0, min(100.0, 75.0 - avg_abs_corr * 300.0))
    # Boost for diversification
    corr_confidence += diversification_score * 20.0
    corr_confidence = min(100.0, corr_confidence)
    if corr_confidence >= 65:
        risk_level = "low"
    elif corr_confidence >= 40:
        risk_level = "medium"
    else:
        risk_level = "high"
    return {
        "correlation_confidence": _safe_float(round(corr_confidence, 1), 50.0),
        "correlation_risk_level": risk_level,
        "diversification_score": _safe_float(round(diversification_score, 3), 0.0),
    }
def correlation_adjusted_kelly(picks, bankroll, correlation_matrix):
    """
    Calculate correlation-adjusted Kelly Criterion bet sizing. (4F)
    Standard Kelly is optimal for independent bets. When picks are
    positively correlated, concentration risk justifies a smaller bet.
    We reduce the Kelly fraction by `1.0 - max_abs_corr * 0.5`.
    BEGINNER NOTE: Kelly Criterion says "bet a fraction of your bankroll
    proportional to your edge divided by the odds." With correlated picks,
    we reduce the bet size because a correlated loss hurts more than
    independent losses do. Think of it as portfolio diversification.
    Args:
        picks (list of dict): Picks, each with:
            'win_probability' (float): P(win), 0-1
            'odds_decimal' (float): Decimal odds (e.g. 1.91 for -110)
        bankroll (float): Total bankroll in dollars
        correlation_matrix (list of list of float): n×n pairwise correlations
    Returns:
        dict: {
            'kelly_fraction': float,
            'recommended_bet': float,
            'correlation_discount': float,
        }
    Example:
        picks=[{'win_probability': 0.60, 'odds_decimal': 1.91}],
        bankroll=1000, correlation_matrix=[[1.0]]
        → kelly_fraction ≈ 0.157, recommended_bet ≈ $157
    """
    if not picks or bankroll <= 0:
        return {"kelly_fraction": 0.0, "recommended_bet": 0.0, "correlation_discount": 1.0}
    n = len(picks)
    total_kelly = 0.0
    for pick in picks:
        p = float(pick.get("win_probability", 0.5))
        b = float(pick.get("odds_decimal", 1.91)) - 1.0  # net odds (profit per $1 stake)
        q = 1.0 - p
        if b > 0:
            kelly = (p * b - q) / b
            total_kelly += max(0.0, kelly)
    avg_kelly = total_kelly / max(1, n)
    # Correlation discount: reduce by max absolute pairwise correlation * 0.5
    max_abs_corr = 0.0
    for i in range(n):
        for j in range(i + 1, n):
            try:
                c = abs(float(correlation_matrix[i][j]))
                max_abs_corr = max(max_abs_corr, c)
            except (IndexError, TypeError, ValueError):
                pass
    correlation_discount = max(0.1, 1.0 - max_abs_corr * 0.5)
    adjusted_kelly = avg_kelly * correlation_discount
    # Apply a safety cap (never bet more than 25% of bankroll on a single decision)
    adjusted_kelly = min(0.25, max(0.0, adjusted_kelly))
    return {
        "kelly_fraction": _safe_float(round(adjusted_kelly, 4), 0.0),
        "recommended_bet": _safe_float(round(adjusted_kelly * bankroll, 2), 0.0),
        "correlation_discount": _safe_float(round(correlation_discount, 4), 1.0),
    }
# ============================================================
# SECTION: Simulation-Array Pearson Correlation
# ============================================================
def pearson_sim_correlation(array_a, array_b):
    """
    Pearson correlation between two Monte Carlo simulation arrays.
    Thin wrapper around :func:`calculate_pearson_correlation` that
    rounds the result to four decimal places — matching the precision
    expected by the Correlation Matrix page.
    Args:
        array_a (list of float): Player A simulation results
            (typically 1,000 values from the QME engine).
        array_b (list of float): Player B simulation results.
    Returns:
        float: Pearson *r* (−1.0 … +1.0), rounded to 4 dp.
            Returns 0.0 when fewer than 3 paired observations exist.
    """
    r = calculate_pearson_correlation(array_a, array_b)
    return round(r, 4)
