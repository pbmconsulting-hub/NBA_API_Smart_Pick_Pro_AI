# ============================================================
# FILE: engine/entry_optimizer.py
# PURPOSE: Build optimal parlay entries for major sportsbooks
#          (PrizePicks, Underdog Fantasy, DraftKings Pick6).
#          Calculates exact EV (expected value) for each entry.
# CONNECTS TO: edge_detection.py (picks), math_helpers.py (math)
# CONCEPTS COVERED: Combinatorics, expected value, parlay math
# ============================================================

# Standard library imports only
import math        # For combinations and calculations
import itertools   # For generating combinations of picks

try:
    from utils.log_helper import get_logger
    _logger = get_logger(__name__)
except ImportError:
    import logging
    _logger = logging.getLogger(__name__)

# Math helpers needed for Flex EV calculation in Feature 10
try:
    from engine.math_helpers import calculate_flex_ev   # Flex EV with partial-win probabilities
except ImportError:
    calculate_flex_ev = None  # Graceful fallback: flex EV uses calculate_entry_expected_value

try:
    from engine.correlation import build_correlation_matrix, adjust_parlay_probability, get_correlation_summary
    _CORRELATION_AVAILABLE = True
except ImportError:
    _CORRELATION_AVAILABLE = False


# ============================================================
# SECTION: Platform Payout Tables
# These are the actual payout multipliers for each platform.
# BEGINNER NOTE: "payout table" = if you pick N games and hit K,
# here's your multiplier on your entry fee.
# ============================================================

# PrizePicks Flex Play payout table: {picks: {hits: payout_multiplier}}
# "Flex" means you can win even without hitting all picks
# LAST_VERIFIED: 2025-26 season (March 2026)
# WARNING: Payout tables change frequently. Verify at prizepicks.com before each season.
PRIZEPICKS_FLEX_PAYOUT_TABLE = {
    3: {3: 2.25, 2: 1.25, 1: 0.0, 0: 0.0},   # 3-pick flex (1-of-3 no longer pays)
    4: {4: 5.0, 3: 1.50, 2: 0.40, 1: 0.0, 0: 0.0},  # 4-pick flex
    5: {5: 10.0, 4: 2.0, 3: 0.40, 2: 0.0, 1: 0.0, 0: 0.0},  # 5-pick flex
    6: {6: 25.0, 5: 2.0, 4: 0.40, 3: 0.0, 2: 0.0, 1: 0.0, 0: 0.0},  # 6-pick flex
}

# PrizePicks Power Play: ALL picks must hit (no partial wins)
# LAST_VERIFIED: 2025-26 season (March 2026)
# WARNING: Payout tables change frequently. Verify at prizepicks.com before each season.
PRIZEPICKS_POWER_PAYOUT_TABLE = {
    2: {2: 3.0},   # 2-pick power: 3x payout
    3: {3: 5.0},   # 3-pick power: 5x payout
    4: {4: 10.0},  # 4-pick power: 10x payout
    5: {5: 20.0},  # 5-pick power: 20x payout
    6: {6: 40.0},  # 6-pick power: 40x payout
}

# Underdog Fantasy Flex payout table
# LAST_VERIFIED: 2025-26 season (March 2026)
# WARNING: Payout tables change frequently. Verify at underdogfantasy.com before each season.
UNDERDOG_FLEX_PAYOUT_TABLE = {
    3: {3: 2.25, 2: 1.20, 1: 0.0, 0: 0.0},
    4: {4: 5.0, 3: 1.50, 2: 0.0, 1: 0.0, 0: 0.0},
    5: {5: 10.0, 4: 2.0, 3: 0.50, 2: 0.0, 1: 0.0, 0: 0.0},
    6: {6: 25.0, 5: 2.5, 4: 0.40, 3: 0.0, 2: 0.0, 1: 0.0, 0: 0.0},
}

# DraftKings Pick6 payout table (estimated — DK pools vary)
# LAST_VERIFIED: 2025-26 season (March 2026)
# WARNING: Payout tables change frequently. Verify at draftkings.com/pick6 before each season.
DRAFTKINGS_PICK6_PAYOUT_TABLE = {
    3: {3: 2.50, 2: 0.0, 1: 0.0, 0: 0.0},
    4: {4: 5.0, 3: 0.0, 2: 0.0, 1: 0.0, 0: 0.0},
    5: {5: 10.0, 4: 1.5, 3: 0.0, 2: 0.0, 1: 0.0, 0: 0.0},
    6: {6: 25.0, 5: 2.0, 4: 0.0, 3: 0.0, 2: 0.0, 1: 0.0, 0: 0.0},
}

# Standard sportsbook parlay payout table (all legs must hit).
# Based on standard -110 per-leg juice.  All-or-nothing structure.
SPORTSBOOK_PARLAY_TABLE = {
    2: {2: 2.64, 1: 0.0, 0: 0.0},
    3: {3: 5.96, 2: 0.0, 1: 0.0, 0: 0.0},
    4: {4: 12.28, 3: 0.0, 2: 0.0, 1: 0.0, 0: 0.0},
    5: {5: 24.35, 4: 0.0, 3: 0.0, 2: 0.0, 1: 0.0, 0: 0.0},
    6: {6: 47.77, 5: 0.0, 4: 0.0, 3: 0.0, 2: 0.0, 1: 0.0, 0: 0.0},
}

# Map platform names to their payout tables
PLATFORM_FLEX_TABLES = {
    "PrizePicks": PRIZEPICKS_FLEX_PAYOUT_TABLE,
    "Underdog Fantasy": UNDERDOG_FLEX_PAYOUT_TABLE,
    "DraftKings Pick6": SPORTSBOOK_PARLAY_TABLE,
    # Backward-compat aliases
    "Underdog": UNDERDOG_FLEX_PAYOUT_TABLE,
    "DraftKings": SPORTSBOOK_PARLAY_TABLE,
}

# ============================================================
# END SECTION: Platform Payout Tables
# ============================================================


def _compute_flex_ev(pick_probabilities, payout_table, entry_fee):
    """
    Internal helper: compute flex EV using calculate_flex_ev when available,
    falling back to calculate_entry_expected_value if calculate_flex_ev was
    not importable (e.g., running without the full engine package).

    Returns a dict with at minimum an 'ev_dollars' key.
    """
    if calculate_flex_ev is not None:
        return calculate_flex_ev(pick_probabilities, payout_table, entry_fee)
    # Fallback: map calculate_entry_expected_value output to flex-ev format
    result = calculate_entry_expected_value(pick_probabilities, payout_table, entry_fee)
    return {
        "ev_dollars": result.get("expected_value_dollars", 0.0),
        "roi": result.get("return_on_investment", 0.0),
        "all_hit_prob": 0.0,
        "prob_at_least_one_miss": 1.0,
    }


# ============================================================
# SECTION: Expected Value Calculator
# ============================================================

def calculate_entry_expected_value(
    pick_probabilities,
    payout_table,
    entry_fee,
):
    """
    Calculate the expected value (EV) of a parlay entry.

    EV = Sum of (probability of each outcome × payout for that outcome)
    EV > 0 means the bet is profitable on average.
    EV < 0 means the house edge wins.

    Args:
        pick_probabilities (list of float): P(over) for each pick
            e.g., [0.62, 0.58, 0.71] for a 3-pick entry
        payout_table (dict): Payout multipliers {hits: multiplier}
        entry_fee (float): Dollar amount bet (e.g., 10.00)

    Returns:
        dict: {
            'expected_value_dollars': float (positive = profitable),
            'return_on_investment': float (e.g., 0.15 = 15% ROI),
            'probability_per_hits': dict {hits: probability},
            'payout_per_hits': dict {hits: payout_dollars},
        }

    Example:
        3-pick entry, probs=[0.62, 0.58, 0.71], $10 entry fee
        → might give EV = $1.23 (12.3% ROI) — profitable!
    """
    number_of_picks = len(pick_probabilities)

    if number_of_picks == 0:
        return {
            "expected_value_dollars": 0.0,
            "return_on_investment": 0.0,
            "probability_per_hits": {},
            "payout_per_hits": {},
        }

    # ============================================================
    # SECTION: Calculate Probability of Each Hit Count
    # BEGINNER NOTE: This uses the binomial distribution.
    # We calculate P(exactly k picks win out of n total)
    # using the formula: C(n,k) * p^k * (1-p)^(n-k)
    # But since probabilities differ per pick, we sum over
    # all combinations of which k picks win.
    # ============================================================

    # Build probabilities for all possible hit counts (0 to n)
    probability_for_hit_count = {}

    # Loop through each possible number of hits (0, 1, 2, ..., n)
    for hit_count in range(number_of_picks + 1):
        # Sum probabilities over ALL combinations of `hit_count` wins
        total_prob_for_this_hit_count = 0.0

        # itertools.combinations gives us all ways to choose
        # `hit_count` picks out of `number_of_picks` total
        # BEGINNER NOTE: If picks are [A, B, C] and hit_count=2,
        # combinations gives us: (A,B), (A,C), (B,C)
        pick_indices = list(range(number_of_picks))

        for winning_indices in itertools.combinations(pick_indices, hit_count):
            # Calculate P(exactly these picks win, all others lose)
            # = product of P(win) for winners × P(lose) for losers
            combination_probability = 1.0
            winning_indices_set = set(winning_indices)

            for pick_index in range(number_of_picks):
                pick_probability = pick_probabilities[pick_index]
                if pick_index in winning_indices_set:
                    combination_probability *= pick_probability  # Win
                else:
                    combination_probability *= (1.0 - pick_probability)  # Lose

            total_prob_for_this_hit_count += combination_probability

        probability_for_hit_count[hit_count] = total_prob_for_this_hit_count

    # ============================================================
    # END SECTION: Calculate Probability of Each Hit Count
    # ============================================================

    # ============================================================
    # SECTION: Calculate EV Using Payout Table
    # ============================================================

    total_expected_value = 0.0
    payout_per_hits = {}

    for hit_count, probability in probability_for_hit_count.items():
        # Look up the payout multiplier for this hit count
        # Default to 0 if not in table (unspecified = no payout)
        payout_multiplier = payout_table.get(hit_count, 0.0)
        payout_dollars = payout_multiplier * entry_fee
        payout_per_hits[hit_count] = round(payout_dollars, 2)

        # Add this outcome's contribution to expected value
        total_expected_value += probability * payout_dollars

    # Net EV subtracts the cost of the entry fee
    net_expected_value = total_expected_value - entry_fee

    # ROI = net EV as a fraction of the entry fee
    return_on_investment = net_expected_value / entry_fee if entry_fee > 0 else 0.0

    return {
        "expected_value_dollars": round(net_expected_value, 2),
        "return_on_investment": round(return_on_investment, 4),
        "probability_per_hits": {k: round(v, 4) for k, v in probability_for_hit_count.items()},
        "payout_per_hits": payout_per_hits,
        "total_expected_return": round(total_expected_value, 2),
    }

    # ============================================================
    # END SECTION: Calculate EV Using Payout Table
    # ============================================================


# ============================================================
# SECTION: Optimal Entry Builder
# ============================================================

def build_optimal_entries(
    analyzed_picks,
    platform,
    entry_size,
    entry_fee,
    max_entries_to_show,
):
    """
    Find the best combination of picks for a given entry size.

    Sorts all possible combinations of picks by Expected Value
    and returns the top entries.

    Args:
        analyzed_picks (list of dict): All analyzed props, each with:
            'player_name', 'stat_type', 'line', 'probability_over',
            'direction', 'confidence_score', 'edge_percentage'
        platform (str): sportsbook name (e.g., 'FanDuel', 'DraftKings')
        entry_size (int): Number of picks per entry (2-6)
        entry_fee (float): Dollar amount per entry
        max_entries_to_show (int): How many top entries to return

    Returns:
        list of dict: Top entries sorted by EV, each containing:
            'picks': list of pick dicts,
            'ev_result': EV calculation result,
            'combined_confidence': float average confidence
    """
    # Filter to only picks with a clear direction and good confidence
    # We only want picks where our model has a meaningful edge
    qualifying_picks = [
        pick for pick in analyzed_picks
        if abs(pick.get("edge_percentage", 0)) >= 3.0  # At least 3% edge
        and pick.get("confidence_score", 0) >= 40.0    # At least Bronze tier
    ]

    # Pre-sort picks by win probability (descending) so the combinatorial
    # search encounters the highest-EV combinations first. This improves
    # branch-and-bound effectiveness and ensures greedy pre-filtering
    # retains the most promising candidates.
    qualifying_picks = sorted(
        qualifying_picks,
        key=lambda p: (
            p.get("probability_over", 0.5)
            if p.get("direction", "OVER") == "OVER"
            else 1.0 - p.get("probability_over", 0.5)
        ),
        reverse=True,
    )

    # Complexity: O(C(n, k)) where n=picks, k=entry_size
    # For large pick pools (> 30), use a greedy pre-filter to reduce the
    # candidate pool to the top ~25 picks before running full combinatorial
    # search. This prevents combinatorial explosion while preserving quality.
    _LARGE_POOL_THRESHOLD = 30
    _GREEDY_POOL_SIZE = 25
    if len(qualifying_picks) > _LARGE_POOL_THRESHOLD:
        qualifying_picks = qualifying_picks[:_GREEDY_POOL_SIZE]

    # Get the payout table for this platform and entry size
    platform_flex_table = PLATFORM_FLEX_TABLES.get(platform, SPORTSBOOK_PARLAY_TABLE)
    payout_table_for_size = platform_flex_table.get(entry_size, {})

    if not qualifying_picks or not payout_table_for_size:
        return []  # Nothing to build

    # Cap entry size to what we have available
    actual_entry_size = min(entry_size, len(qualifying_picks))

    # ============================================================
    # SECTION: Generate and Score All Combinations
    # ============================================================

    all_entries_with_scores = []  # Store all combos with their EVs

    # Generate all combinations of `entry_size` picks from qualifying_picks
    # BEGINNER NOTE: itertools.combinations([A,B,C,D], 3) gives
    # (A,B,C), (A,B,D), (A,C,D), (B,C,D) — all 3-pick combos
    pick_index_list = list(range(len(qualifying_picks)))

    for combo_indices in itertools.combinations(pick_index_list, actual_entry_size):
        # Get the actual pick dictionaries for this combination
        combo_picks = [qualifying_picks[i] for i in combo_indices]

        # Extract probabilities: use P(over) if direction=OVER, else P(under)
        pick_probabilities = []
        for pick in combo_picks:
            if pick.get("direction", "OVER") == "OVER":
                prob = pick.get("probability_over", 0.5)
            else:
                # If we're betting UNDER, the probability is 1 - P(over)
                prob = 1.0 - pick.get("probability_over", 0.5)
            pick_probabilities.append(prob)

        # Calculate EV for this combination
        ev_result = calculate_entry_expected_value(
            pick_probabilities,
            payout_table_for_size,
            entry_fee,
        )

        # W2: Apply correlation discount to expected value
        # If multiple legs share a game, their outcomes are correlated.
        correlation_risk = calculate_correlation_risk(combo_picks)
        discount = correlation_risk["discount_multiplier"]
        if discount < 1.0:
            # Scale down the EV to reflect correlation penalty.
            # Shallow copy is safe here because we only modify top-level float fields,
            # not the nested probability_per_hits / payout_per_hits dicts.
            discounted_ev = ev_result["expected_value_dollars"] * discount
            discounted_return = ev_result["total_expected_return"] * discount
            discounted_roi = discounted_ev / entry_fee if entry_fee > 0 else 0.0
            ev_result = dict(ev_result)  # copy
            ev_result["expected_value_dollars"] = round(discounted_ev, 2)
            ev_result["total_expected_return"] = round(discounted_return, 2)
            ev_result["return_on_investment"] = round(discounted_roi, 4)
            ev_result["correlation_discount_applied"] = True
        else:
            ev_result = dict(ev_result)
            ev_result["correlation_discount_applied"] = False

        # Average confidence score of all picks in this combo
        average_confidence = sum(
            p.get("confidence_score", 50) for p in combo_picks
        ) / len(combo_picks)

        # W9: Identify the weakest link in this combo
        weakest = identify_weakest_link(combo_picks)
        weakest_prob = 0.5
        weakest_label = ""
        if weakest:
            wp = weakest.get("probability_over", 0.5)
            weakest_prob = wp if weakest.get("direction", "OVER") == "OVER" else 1.0 - wp
            weakest_label = (
                f"{weakest.get('player_name','?')} "
                f"{weakest.get('stat_type','').capitalize()} "
                f"({weakest_prob*100:.0f}%)"
            )

        all_entries_with_scores.append({
            "picks": combo_picks,
            "ev_result": ev_result,
            "combined_confidence": round(average_confidence, 1),
            "pick_probabilities": pick_probabilities,
            "correlation_risk": correlation_risk,
            "weakest_link": weakest,
            "weakest_link_probability": round(weakest_prob, 4),
            "weakest_link_label": weakest_label,
        })

    # ============================================================
    # END SECTION: Generate and Score All Combinations
    # ============================================================

    # Sort by expected value (highest EV first)
    # BEGINNER NOTE: sorted() with key= lets us sort by any field
    # reverse=True means descending order (highest first)
    all_entries_with_scores.sort(
        key=lambda entry: entry["ev_result"]["expected_value_dollars"],
        reverse=True
    )

    # Return the top N entries
    return all_entries_with_scores[:max_entries_to_show]


# ============================================================
# SECTION: Correlation Risk Calculator (W2)
# When multiple legs come from the SAME game, all are affected
# simultaneously by game-level events (blowout, OT, foul trouble).
# This is a hidden parlay risk that must be disclosed and penalized.
# ============================================================

def calculate_correlation_risk(selected_picks):
    """
    Calculate the correlation risk discount for a set of picks. (W2)

    When 2+ picks come from the same game, their outcomes are
    correlated — a blowout or overtime affects ALL of them.
    We apply a probability discount to account for this hidden risk.

    When engine.correlation is available, also computes a full
    pairwise correlation matrix and summary via build_correlation_matrix
    and get_correlation_summary.

    Args:
        selected_picks (list of dict): Picks to check, each with
            'player_team' (or 'team') and 'opponent' keys.

    Returns:
        dict: {
            'discount_multiplier': float  (1.0 = no discount, 0.88 = 12% penalty)
            'max_same_game_picks': int    (highest count from any single game)
            'game_groups': dict           {game_key: [player_names]}
            'warnings': list of str       (human-readable warnings)
            'correlation_level': str      ('none', 'low', 'high')
            'correlation_matrix': list    (n×n matrix, if available)
            'correlation_summary': dict   (risk summary, if available)
        }

    Example:
        3 picks from LAL vs GSW → discount_multiplier=0.88, level='high'
    """
    # Group picks by game (same two teams)
    game_groups = {}  # game_key (frozenset of teams) → list of pick dicts

    for pick in selected_picks:
        team = pick.get("player_team", pick.get("team", "")).upper().strip()
        opponent = pick.get("opponent", "").upper().strip()
        if team and opponent:
            game_key = frozenset([team, opponent])
        elif team:
            game_key = frozenset([team])
        else:
            continue
        game_key_str = " vs ".join(sorted(game_key))
        if game_key_str not in game_groups:
            game_groups[game_key_str] = []
        game_groups[game_key_str].append(pick)

    # Find the game with the most picks
    max_same_game = max((len(v) for v in game_groups.values()), default=0)

    # Build warnings and determine discount
    warnings = []
    correlation_level = "none"
    discount_multiplier = 1.0

    for game_key_str, picks_in_game in game_groups.items():
        n = len(picks_in_game)
        if n < 2:
            continue  # Single pick from a game — no correlation

        names = [p.get("player_name", "?") for p in picks_in_game]
        names_str = ", ".join(names)

        if n >= 3:
            # 12% penalty for 3+ picks from same game
            discount_multiplier = min(discount_multiplier, 0.88)
            correlation_level = "high"
            warnings.append(
                f"🚨 HIGH CORRELATION: {n} picks from {game_key_str} "
                f"({names_str}) — 12% EV penalty applied. "
                f"A blowout or foul-out affects ALL {n} legs simultaneously."
            )
        else:
            # 5% penalty for 2 picks from same game
            discount_multiplier = min(discount_multiplier, 0.95)
            if correlation_level != "high":
                correlation_level = "low"
            warnings.append(
                f"⚠️ CORRELATION: 2 picks from {game_key_str} "
                f"({names_str}) — 5% EV penalty applied. "
                f"Same-game events (blowout, OT) affect both legs."
            )

    result = {
        "discount_multiplier": round(discount_multiplier, 4),
        "max_same_game_picks": max_same_game,
        "game_groups": {k: [p.get("player_name", "?") for p in v]
                        for k, v in game_groups.items()},
        "warnings": warnings,
        "correlation_level": correlation_level,
        "correlation_matrix": None,
        "correlation_summary": None,
    }

    # Enhanced correlation analysis using engine.correlation when available
    if _CORRELATION_AVAILABLE and selected_picks:
        try:
            corr_matrix = build_correlation_matrix(selected_picks)
            corr_summary = get_correlation_summary(selected_picks, corr_matrix)
            result["correlation_matrix"] = corr_matrix
            result["correlation_summary"] = corr_summary
            # Escalate correlation_level based on matrix analysis risk level
            risk = corr_summary.get("risk_level", "low")
            if risk == "high":
                result["correlation_level"] = "high"
            elif risk == "medium" and correlation_level == "none":
                result["correlation_level"] = "low"
        except Exception as exc:
            _logger.debug("analyze_correlation_risk: matrix analysis failed — %s", exc)
            pass  # Fallback to game-grouping result above

    return result

# ============================================================
# END SECTION: Correlation Risk Calculator
# ============================================================


# ============================================================
# SECTION: Weakest Link Detection + Swap Suggestions (W9)
# The entry is only as strong as its weakest pick.
# Identify it, then suggest better alternatives.
# ============================================================

def identify_weakest_link(picks):
    """
    Find the weakest pick in an entry by probability. (W9)

    The weakest link is the pick with the LOWEST win probability,
    since a parlay fails if any single leg misses.

    Args:
        picks (list of dict): Entry picks, each with 'probability_over',
            'direction', 'player_name', 'stat_type', 'line'

    Returns:
        dict or None: The weakest pick dict, or None if picks is empty.
    """
    if not picks:
        return None

    def _win_prob(pick):
        """Get the win probability for the betted direction."""
        p = pick.get("probability_over", 0.5)
        return p if pick.get("direction", "OVER") == "OVER" else 1.0 - p

    return min(picks, key=_win_prob)


def suggest_swap(weakest_pick, available_picks, entry_picks):
    """
    Suggest a stronger alternative for the weakest pick in an entry. (W9)

    Finds the available pick (not already in the entry) with the
    highest win probability that could replace the weakest link.

    Args:
        weakest_pick (dict): The lowest-probability pick in the entry
        available_picks (list of dict): All qualifying picks
        entry_picks (list of dict): Current entry picks (to exclude from suggestions)

    Returns:
        dict or None: The best swap candidate, or None if no better option exists.
            Includes the pick dict plus a 'swap_reason' key explaining the swap.
    """
    # Collect player+stat combos already in the entry (to avoid duplicates)
    in_entry = set()
    for p in entry_picks:
        key = (p.get("player_name", ""), p.get("stat_type", ""))
        in_entry.add(key)

    weakest_prob = (
        weakest_pick.get("probability_over", 0.5)
        if weakest_pick.get("direction", "OVER") == "OVER"
        else 1.0 - weakest_pick.get("probability_over", 0.5)
    )

    best_candidate = None
    best_prob = weakest_prob  # Only suggest if it's actually better

    for pick in available_picks:
        key = (pick.get("player_name", ""), pick.get("stat_type", ""))
        if key in in_entry:
            continue  # Already in the entry

        p = pick.get("probability_over", 0.5)
        win_prob = p if pick.get("direction", "OVER") == "OVER" else 1.0 - p

        if win_prob > best_prob:
            best_prob = win_prob
            best_candidate = pick

    if best_candidate is None:
        return None

    improvement = (best_prob - weakest_prob) * 100.0
    best_candidate = dict(best_candidate)  # copy to avoid mutating original
    best_candidate["swap_reason"] = (
        f"Replaces {weakest_pick.get('player_name','?')} "
        f"{weakest_pick.get('stat_type','').capitalize()} "
        f"({weakest_prob*100:.0f}% → {best_prob*100:.0f}%, "
        f"+{improvement:.1f}% stronger)"
    )
    return best_candidate

# ============================================================
# END SECTION: Weakest Link Detection + Swap Suggestions
# ============================================================


def format_ev_display(ev_result, entry_fee):
    """
    Format EV results for display in the UI.

    Args:
        ev_result (dict): Output from calculate_entry_expected_value
        entry_fee (float): Entry fee amount

    Returns:
        dict: Human-readable display values
    """
    ev_dollars = ev_result.get("expected_value_dollars", 0)
    roi = ev_result.get("return_on_investment", 0)
    probability_per_hits = ev_result.get("probability_per_hits", {})

    # Format as percentage for display
    roi_percentage = roi * 100.0

    # Determine if EV is positive or negative
    ev_label = f"+${ev_dollars:.2f}" if ev_dollars >= 0 else f"-${abs(ev_dollars):.2f}"
    roi_label = f"+{roi_percentage:.1f}%" if roi >= 0 else f"{roi_percentage:.1f}%"

    return {
        "ev_label": ev_label,
        "roi_label": roi_label,
        "is_positive_ev": ev_dollars > 0,
        "probability_per_hits": probability_per_hits,
    }

# ============================================================
# END SECTION: Optimal Entry Builder
# ============================================================


# ============================================================
# SECTION: Flex vs Power Play Optimizer (Feature 10)
# Determines whether a Power or Flex entry maximizes EV for
# a given set of pick probabilities on sportsbooks. Includes
# a binary-search breakeven calculator so users know the
# exact probability threshold where Power becomes better.
# ============================================================

def optimize_play_type(pick_probabilities, entry_size, platform='DraftKings'):
    """
    Determine whether Flex or Power play is better for a given set of picks.

    Calculates EV for both Flex and Power, recommending the play type that
    maximizes expected value given the specific pick probabilities.

    Args:
        pick_probabilities (list of float): Win probabilities for each pick (0-1).
            Must have 2-6 elements.
        entry_size (int): Number of picks (must match len(pick_probabilities)).
        platform (str): Platform name. Default 'DraftKings'.
            Only PrizePicks supports both Flex and Power.

    Returns:
        dict: {
            'recommended_play_type': str,  # 'Power', 'Flex', or 'Either'
            'flex_ev': float,              # EV in dollars per $10 entry
            'power_ev': float,             # EV in dollars per $10 entry
            'ev_difference': float,        # power_ev - flex_ev
            'min_probability': float,      # Lowest probability leg
            'avg_probability': float,      # Average probability across legs
            'reasoning': str,              # Human-readable explanation
        }

    Example:
        optimize_play_type([0.68, 0.71, 0.65], entry_size=3, platform='DraftKings')
        # All probs >= 65% → Power recommended
        # Returns: {'recommended_play_type': 'Power', 'power_ev': 2.5, ...}
    """
    entry_fee = 10.0   # Standardise at $10 for comparison

    min_prob = min(pick_probabilities) if pick_probabilities else 0.5
    avg_prob = sum(pick_probabilities) / len(pick_probabilities) if pick_probabilities else 0.5

    # Sportsbooks don't offer Power play (keep PrizePicks for backward compat)
    if platform not in ('PrizePicks',):
        flex_table = PLATFORM_FLEX_TABLES.get(platform, SPORTSBOOK_PARLAY_TABLE)
        flex_payout_for_size = flex_table.get(entry_size, {})
        flex_result = _compute_flex_ev(pick_probabilities, flex_payout_for_size, entry_fee)
        return {
            'recommended_play_type': 'Flex',
            'flex_ev':               flex_result['ev_dollars'],
            'power_ev':              None,
            'ev_difference':         None,
            'min_probability':       round(min_prob, 4),
            'avg_probability':       round(avg_prob, 4),
            'reasoning':             f"{platform} does not offer Power play. Flex only.",
        }

    # ---- PrizePicks: compare Flex vs Power ----
    flex_payout_for_size  = PRIZEPICKS_FLEX_PAYOUT_TABLE.get(entry_size, {})
    power_payout_for_size = PRIZEPICKS_POWER_PAYOUT_TABLE.get(entry_size, {})

    flex_result = _compute_flex_ev(pick_probabilities, flex_payout_for_size, entry_fee)
    flex_ev = flex_result['ev_dollars']

    # Power EV: only paid when ALL picks hit
    if power_payout_for_size:
        power_multiplier = power_payout_for_size.get(entry_size, 0.0)
        all_hit_prob = 1.0
        for p in pick_probabilities:
            all_hit_prob *= p
        power_ev = round(all_hit_prob * power_multiplier * entry_fee - entry_fee, 2)
    else:
        power_ev = -entry_fee   # No payout table → automatic loss

    ev_difference = round(power_ev - flex_ev, 2)

    # ---- Recommendation rules ----
    if min_prob >= 0.65 and power_ev > flex_ev:
        recommended = 'Power'
        reasoning = (
            f"All legs ≥ 65% (min={min_prob*100:.0f}%) and Power EV "
            f"(${power_ev:.2f}) beats Flex EV (${flex_ev:.2f}). "
            "High-confidence lineup favours all-or-nothing payout."
        )
    elif avg_prob < 0.60 or min_prob < 0.55:
        recommended = 'Flex'
        reasoning = (
            f"Average probability {avg_prob*100:.0f}% or min probability "
            f"{min_prob*100:.0f}% is below threshold for Power. "
            "Flex partial-win safety net has better EV here."
        )
    elif abs(ev_difference) < 0.50:
        recommended = 'Either'
        reasoning = (
            f"EV difference between Power (${power_ev:.2f}) and Flex "
            f"(${flex_ev:.2f}) is less than $0.50 — essentially a toss-up. "
            "Either play type is defensible."
        )
    elif power_ev > flex_ev:
        recommended = 'Power'
        reasoning = (
            f"Power EV (${power_ev:.2f}) exceeds Flex EV (${flex_ev:.2f}) "
            f"by ${ev_difference:.2f} with avg prob {avg_prob*100:.0f}%."
        )
    else:
        recommended = 'Flex'
        reasoning = (
            f"Flex EV (${flex_ev:.2f}) exceeds Power EV (${power_ev:.2f}). "
            "Partial-win payouts more valuable at current probabilities."
        )

    return {
        'recommended_play_type': recommended,
        'flex_ev':               flex_ev,
        'power_ev':              power_ev,
        'ev_difference':         ev_difference,
        'min_probability':       round(min_prob, 4),
        'avg_probability':       round(avg_prob, 4),
        'reasoning':             reasoning,
    }


def calculate_flex_vs_power_breakeven(pick_probabilities, entry_size):
    """
    Find the probability threshold where Power becomes more +EV than Flex.

    Computes BOTH the uniform breakeven (traditional threshold analysis)
    AND the actual heterogeneous EV comparison using the real pick
    probabilities, so users get an accurate recommendation even when
    picks have varying probabilities (e.g. [0.55, 0.55, 0.90]).

    Args:
        pick_probabilities (list of float): Actual win probabilities for
            each pick.  Used for the real EV comparison.
        entry_size (int): Number of picks.

    Returns:
        dict: {
            'breakeven_probability': float,  # Uniform prob where Power = Flex EV
            'current_min_prob': float,       # Actual minimum probability in entry
            'power_better_above': float,     # Clear threshold (breakeven + buffer)
            'actual_power_ev': float,        # Real Power EV with actual probabilities
            'actual_flex_ev': float,         # Real Flex EV with actual probabilities
            'power_is_better_actual': bool,  # True when real EVs favour Power
            'interpretation': str,
        }

    Example:
        result = calculate_flex_vs_power_breakeven([0.62, 0.67, 0.64], entry_size=3)
        # result['breakeven_probability'] → ~0.68 for a 3-pick entry
    """
    entry_fee = 10.0

    flex_payout  = PRIZEPICKS_FLEX_PAYOUT_TABLE.get(entry_size, {})
    power_payout = PRIZEPICKS_POWER_PAYOUT_TABLE.get(entry_size, {})
    power_multiplier = power_payout.get(entry_size, 0.0)

    def _ev_diff(p_test):
        """power_ev - flex_ev at uniform probability p_test."""
        # Power EV
        all_hit = p_test ** entry_size
        p_ev = all_hit * power_multiplier * entry_fee - entry_fee
        # Flex EV
        uniform_probs = [p_test] * entry_size
        f_ev = _compute_flex_ev(uniform_probs, flex_payout, entry_fee)['ev_dollars']
        return p_ev - f_ev

    # Binary search for breakeven in [0.50, 0.99]
    lo, hi = 0.50, 0.99
    breakeven = 0.75   # sensible default if search fails

    for _ in range(20):
        mid = (lo + hi) / 2.0
        if _ev_diff(mid) < 0:
            lo = mid   # Power still losing → need higher probability
        else:
            hi = mid   # Power winning → breakeven is lower
            breakeven = mid

    breakeven = round(breakeven, 4)
    current_min_prob = min(pick_probabilities) if pick_probabilities else 0.5
    power_better_above = min(0.99, breakeven + 0.02)   # 2% safety buffer

    # ── Actual heterogeneous EV comparison ────────────────────────────────────
    # Unlike the uniform-probability breakeven search above, this uses the
    # REAL probabilities so picks like [0.55, 0.55, 0.90] are handled correctly
    # (that entry may favour Power even though the weakest leg is below breakeven).
    actual_power_ev = 0.0
    actual_flex_ev  = 0.0
    if pick_probabilities and power_multiplier > 0 and flex_payout:
        try:
            # Power: all legs must hit
            p_all_hit = 1.0
            for p in pick_probabilities:
                p_all_hit *= p
            actual_power_ev = round(
                p_all_hit * power_multiplier * entry_fee - entry_fee, 2
            )
            # Flex: use actual probabilities (not uniform)
            flex_result = _compute_flex_ev(list(pick_probabilities), flex_payout, entry_fee)
            actual_flex_ev = round(flex_result.get('ev_dollars', 0.0), 2)
        except Exception as exc:
            _logger.debug("power_vs_flex: actual EV calc failed — %s", exc)
            pass  # Fall back to zeros; uniform analysis still valid
    power_is_better_actual = actual_power_ev > actual_flex_ev

    def _ev_str(ev):
        """Format an EV value with sign and dollar sign (e.g. '+$3.61', '-$1.20')."""
        sign = "+" if ev >= 0 else "-"
        return f"{sign}${abs(ev):.2f}"

    if power_is_better_actual:
        interpretation = (
            f"Based on your actual pick probabilities, Power play has better EV "
            f"({_ev_str(actual_power_ev)} vs {_ev_str(actual_flex_ev)} Flex). "
            f"Uniform breakeven threshold: {breakeven*100:.0f}%."
        )
    elif current_min_prob >= power_better_above:
        interpretation = (
            f"Your weakest leg ({current_min_prob*100:.0f}%) is above the "
            f"{power_better_above*100:.0f}% threshold — Power play is recommended "
            f"(uniform analysis)."
        )
    elif current_min_prob >= breakeven:
        interpretation = (
            f"Your weakest leg ({current_min_prob*100:.0f}%) is near the breakeven "
            f"({breakeven*100:.0f}%). Power play is marginal — consider Flex for safety. "
            f"Actual EV: Power {_ev_str(actual_power_ev)}, Flex {_ev_str(actual_flex_ev)}."
        )
    else:
        interpretation = (
            f"Breakeven at {breakeven*100:.0f}% uniform probability. "
            f"Your weakest leg ({current_min_prob*100:.0f}%) is below this — "
            f"Flex play has better expected value "
            f"(actual EV: Power {_ev_str(actual_power_ev)}, Flex {_ev_str(actual_flex_ev)})."
        )

    return {
        'breakeven_probability':  breakeven,
        'current_min_prob':       round(current_min_prob, 4),
        'power_better_above':     round(power_better_above, 4),
        'actual_power_ev':        actual_power_ev,
        'actual_flex_ev':         actual_flex_ev,
        'power_is_better_actual': power_is_better_actual,
        'interpretation':         interpretation,
    }


def build_optimal_entries_with_play_type(
    picks,
    entry_sizes=None,
    platform="DraftKings",
    entry_fee=10.0,
    min_edge_pct=3.0,
    min_confidence_score=40.0,
    max_entries_to_show=10,
):
    """
    Build optimal entries and annotate each with Flex vs Power recommendation.

    Wraps build_optimal_entries() and adds play-type optimization to each result.

    Args:
        picks (list of dict): Same as build_optimal_entries().
        entry_sizes (list of int, optional): Entry sizes to consider.
        platform (str): Platform name.
        entry_fee (float): Entry fee per bet.
        min_edge_pct (float): Minimum edge percentage filter.
        min_confidence_score (float): Minimum confidence score filter.
        max_entries_to_show (int): Maximum entries to return.

    Returns:
        list of dict: Same as build_optimal_entries() output, with additional fields:
            'flex_ev': float,
            'power_ev': float,
            'recommended_play_type': str,
            'play_type_reasoning': str,
            'breakeven_probability': float or None,

    Example:
        entries = build_optimal_entries_with_play_type(picks, platform='DraftKings')
        for e in entries:
            print(f"{e['recommended_play_type']}: Flex={e['flex_ev']:.2f}, Power={e['power_ev']:.2f}")
    """
    if entry_sizes is None:
        entry_sizes = [3, 4, 5, 6]

    # Filter picks to meet minimum quality thresholds before building entries
    qualifying_picks = [
        p for p in picks
        if abs(p.get('edge_percentage', 0)) >= min_edge_pct
        and p.get('confidence_score', 0) >= min_confidence_score
    ]

    all_entries = []

    for size in entry_sizes:
        raw_entries = build_optimal_entries(
            qualifying_picks,
            platform=platform,
            entry_size=size,
            entry_fee=entry_fee,
            max_entries_to_show=max_entries_to_show,
        )
        all_entries.extend(raw_entries)

    # Sort combined results by EV descending, then trim to max_entries_to_show
    all_entries.sort(
        key=lambda e: e.get('ev_result', {}).get('expected_value_dollars', 0),
        reverse=True,
    )
    all_entries = all_entries[:max_entries_to_show]

    # Annotate each entry with Flex vs Power recommendation
    annotated = []
    for entry in all_entries:
        entry_picks = entry.get('picks', [])
        n = len(entry_picks)

        # Reconstruct per-pick win probabilities (same logic as build_optimal_entries)
        pick_probabilities = []
        for pick in entry_picks:
            p = pick.get('probability_over', 0.5)
            win_prob = p if pick.get('direction', 'OVER') == 'OVER' else 1.0 - p
            pick_probabilities.append(win_prob)

        if platform != 'PrizePicks' or n < 2:
            # Non-PrizePicks or too few picks → Flex only
            play_type_result = {
                'recommended_play_type': 'Flex',
                'flex_ev':               entry.get('ev_result', {}).get('expected_value_dollars', 0.0),
                'power_ev':              None,
                'reasoning':             f"{platform} does not support Power play or insufficient picks.",
            }
            breakeven_prob = None
        else:
            play_type_result = optimize_play_type(pick_probabilities, n, platform)
            breakeven_data  = calculate_flex_vs_power_breakeven(pick_probabilities, n)
            breakeven_prob  = breakeven_data.get('breakeven_probability')

        annotated_entry = dict(entry)
        annotated_entry['flex_ev']                = play_type_result.get('flex_ev')
        annotated_entry['power_ev']               = play_type_result.get('power_ev')
        annotated_entry['recommended_play_type']  = play_type_result['recommended_play_type']
        annotated_entry['play_type_reasoning']    = play_type_result.get('reasoning', '')
        annotated_entry['breakeven_probability']  = breakeven_prob if platform == 'PrizePicks' else None
        annotated.append(annotated_entry)

    return annotated

# ============================================================
# END SECTION: Flex vs Power Play Optimizer
# ============================================================


# ============================================================
# SECTION: Enhanced Correlation Modeling (GAP 3)
# ============================================================

# Scaling factor for the bivariate normal probability adjustment.
# For p near 0.5, (p_i-0.5)*(p_j-0.5) ≈ 0.25 max, so
# CORRELATION_SCALING_FACTOR * 0.25 = 1.0 maximum per-pair adjustment.
CORRELATION_SCALING_FACTOR = 4.0


def get_correlation_coefficient(player1, player2, stat1, stat2, game_info=None):
    """
    Calculate correlation coefficient between two player prop picks.

    Models three types of correlation:
    1. Positive: teammates in high-pace/high-total game
    2. Negative: teammates competing for same usage (same stat type)
    3. Independence: players in different games

    Args:
        player1 (dict): First player pick with 'team', 'player_name'
        player2 (dict): Second player pick with 'team', 'player_name'
        stat1 (str): Stat type for player1 (e.g. 'points')
        stat2 (str): Stat type for player2 (e.g. 'assists')
        game_info (dict or None): Game info with 'game_total', 'home_team', 'away_team'

    Returns:
        float: Correlation coefficient (-1 to 1)
            0 = independent, positive = correlated, negative = anti-correlated
    """
    if game_info is None:
        game_info = {}

    team1 = str(player1.get("player_team", player1.get("team", ""))).upper().strip()
    team2 = str(player2.get("player_team", player2.get("team", ""))).upper().strip()

    # Cross-game picks are independent
    if not team1 or not team2:
        return 0.0

    # Check if in the same game (same teams or opponents)
    opponent1 = str(player1.get("opponent", "")).upper().strip()
    opponent2 = str(player2.get("opponent", "")).upper().strip()

    same_game = (
        (team1 == team2) or
        (team1 == opponent2) or
        (team2 == opponent1) or
        (opponent1 and opponent1 == opponent2)
    )

    if not same_game:
        return 0.0  # Different games → independent

    # Both players are in the same game
    are_teammates = (team1 == team2)
    game_total = float(game_info.get("game_total", 220.0) or 220.0)

    # High-total games create positive correlation for all players in the game
    game_total_bonus = 0.0
    if game_total > 235:
        game_total_bonus = 0.12
    elif game_total > 228:
        game_total_bonus = 0.08
    elif game_total > 222:
        game_total_bonus = 0.04

    if are_teammates:
        # Teammates with same stat type: usage competition → negative correlation
        usage_stats = {"points", "assists", "field_goals_made", "threes"}
        if stat1 in usage_stats and stat2 in usage_stats and stat1 == stat2:
            # Same stat type for teammates: negative correlation (usage competition)
            return round(-0.15 - game_total_bonus * 0.3, 4)
        elif stat1 == "rebounds" and stat2 == "rebounds":
            return round(-0.10, 4)  # Rebound competition
        else:
            # Different stats or complementary stats: mild positive correlation
            return round(0.05 + game_total_bonus, 4)
    else:
        # Opponents: game-level correlation for counting stats
        counting_stats = {"points", "rebounds", "assists", "threes"}
        if stat1 in counting_stats and stat2 in counting_stats:
            # Positive correlation from game pace/total
            return round(game_total_bonus, 4)
        return 0.0


def calculate_parlay_probability_with_correlation(picks, games=None):
    """
    Calculate parlay win probability accounting for correlation between picks.

    Adjusts the naive product P1 * P2 * P3 * ... using correlation coefficients.
    Uses a simplified covariance adjustment (Gaussian copula approximation):
    P_adjusted = P_naive * product(1 + rho_ij * adjustment_factor)

    Args:
        picks (list of dict): Each pick with 'win_probability', 'player_name',
            'player_team' (or 'team'), 'opponent', 'stat_type'
        games (list of dict or None): Today's game info list with 'game_total',
            'home_team', 'away_team'

    Returns:
        dict: {
            'naive_probability': float,
            'correlated_probability': float,
            'correlation_matrix': list of list,
            'max_correlation': float,
            'correlation_warnings': list of str,
        }
    """
    if games is None:
        games = []

    if not picks:
        return {
            "naive_probability": 0.0,
            "correlated_probability": 0.0,
            "correlation_matrix": [],
            "max_correlation": 0.0,
            "correlation_warnings": [],
        }

    n = len(picks)

    # Build game info lookup by team
    game_lookup = {}
    for g in games:
        home = str(g.get("home_team", "")).upper()
        away = str(g.get("away_team", "")).upper()
        if home:
            game_lookup[home] = g
        if away:
            game_lookup[away] = g

    # Naive probability (independent)
    naive_prob = 1.0
    for p in picks:
        naive_prob *= float(p.get("win_probability", 0.5))

    if n == 1:
        return {
            "naive_probability": round(naive_prob, 6),
            "correlated_probability": round(naive_prob, 6),
            "correlation_matrix": [[1.0]],
            "max_correlation": 0.0,
            "correlation_warnings": [],
        }

    # Build correlation matrix and compute adjustment
    corr_matrix = [[0.0] * n for _ in range(n)]
    max_corr = 0.0
    warnings = []
    total_adjustment = 1.0

    for i in range(n):
        corr_matrix[i][i] = 1.0
        for j in range(i + 1, n):
            # Find game context for these two players
            team_i = str(picks[i].get("player_team", picks[i].get("team", ""))).upper()
            game_info = game_lookup.get(team_i, {})

            rho = get_correlation_coefficient(
                picks[i], picks[j],
                picks[i].get("stat_type", ""),
                picks[j].get("stat_type", ""),
                game_info,
            )
            corr_matrix[i][j] = rho
            corr_matrix[j][i] = rho

            if abs(rho) > abs(max_corr):
                max_corr = rho

            # Adjust probability based on correlation
            # Positive correlation → slight boost (correlated wins help each other)
            # Negative correlation → penalty (anti-correlated hurts parlay)
            p_i = float(picks[i].get("win_probability", 0.5))
            p_j = float(picks[j].get("win_probability", 0.5))
            # Simplified bivariate normal adjustment factor.
            # The coefficient scales the correlation's impact on probabilities:
            # for p_i, p_j near 0.5, (p_i-0.5)*(p_j-0.5) ≈ 0.25 max, so
            # CORRELATION_SCALING_FACTOR * 0.25 = 1.0 maximum adjustment per pair.
            adjustment = 1.0 + rho * (p_i - 0.5) * (p_j - 0.5) * CORRELATION_SCALING_FACTOR
            adjustment = max(0.7, min(1.3, adjustment))
            total_adjustment *= adjustment

            if abs(rho) >= 0.10:
                name_i = picks[i].get("player_name", "?")
                name_j = picks[j].get("player_name", "?")
                direction = "positively" if rho > 0 else "negatively"
                warnings.append(
                    f"{name_i} & {name_j} are {direction} correlated (ρ={rho:+.2f})"
                )

    correlated_prob = min(1.0, max(0.0, naive_prob * total_adjustment))

    return {
        "naive_probability": round(naive_prob, 6),
        "correlated_probability": round(correlated_prob, 6),
        "correlation_matrix": corr_matrix,
        "max_correlation": round(max_corr, 4),
        "correlation_warnings": warnings,
    }

# ============================================================
# END SECTION: Enhanced Correlation Modeling (GAP 3)
# ============================================================
