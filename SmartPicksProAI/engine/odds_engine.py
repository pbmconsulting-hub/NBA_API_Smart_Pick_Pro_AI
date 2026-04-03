# engine/odds_engine.py
# Odds and implied probability calculations for prop betting.
# Standard library only — no numpy/scipy/pandas.
import math
import itertools
def _safe_float(value, fallback=0.0):
    """Return *value* if it is a finite float, otherwise *fallback*.
    Prevents NaN or ±inf from leaking out of odds/Kelly calculations
    into the UI layer.
    """
    try:
        v = float(value)
        if math.isfinite(v):
            return v
        return float(fallback)
    except (ValueError, TypeError):
        return float(fallback)
# Minimum combined overround (sum of both sides' implied probabilities)
# required for devig to produce stable results. Normal two-sided markets
# have overround ≥ 1.0; extreme positive odds on both sides (e.g.,
# +10000/+10000) yield overround ~0.02 — dividing by such a small
# value amplifies rounding errors into meaningless fair probabilities.
MIN_VALID_OVERROUND = 0.01
def american_odds_to_implied_probability(odds):
    """
    Convert American odds to implied probability.
    Args:
        odds (int or float): American odds (e.g. -110, +150, -200)
    Returns:
        float: Implied probability (0.0 to 1.0)
    Examples:
        -110  → 0.5238 (52.38% breakeven)
        +150  → 0.4000 (40.00%)
        -200  → 0.6667 (66.67%)
    """
    try:
        odds = float(odds)
        # American odds of exactly 0 are invalid (valid range: <= -100 or >= +100).
        # Return the standard -110 breakeven to avoid returning 1.0 (certainty).
        if odds == 0:
            return 0.5238
        if odds < 0:
            return round(abs(odds) / (abs(odds) + 100.0), 6)
        else:
            return round(100.0 / (odds + 100.0), 6)
    except (ValueError, TypeError):
        return 0.5238  # Default to -110 breakeven
def implied_probability_to_american_odds(prob):
    """
    Convert implied probability to American odds.
    Args:
        prob (float): Implied probability (0.0 to 1.0)
    Returns:
        float: American odds (negative for favorites, positive for underdogs)
    Examples:
        0.5238 → -110
        0.40   → +150
    """
    try:
        prob = float(prob)
        prob = max(0.001, min(0.999, prob))
        if prob >= 0.5:
            return round(-(prob / (1.0 - prob)) * 100.0, 1)
        else:
            return round(((1.0 - prob) / prob) * 100.0, 1)
    except (ValueError, TypeError):
        return -110.0
def calculate_breakeven_probability(odds):
    """
    Calculate the exact breakeven probability needed to profit at these odds.
    This is the same as implied probability for a single side.
    At breakeven, EV = 0.
    Args:
        odds (int or float): American odds
    Returns:
        float: Breakeven probability (0.0 to 1.0)
    Example:
        -110 → 0.5238 (need to win 52.38% of the time to break even)
    """
    return american_odds_to_implied_probability(odds)
def get_vig_percentage(odds_side1, odds_side2=None):
    """
    Calculate the vig (juice) percentage on a two-sided market.
    If only one side is provided, assumes the other side is the standard
    -110 complement (not a mirror of side 1), which is the correct
    assumption for most NBA prop markets.
    Args:
        odds_side1 (float): American odds for side 1 (e.g. -110)
        odds_side2 (float or None): American odds for side 2 (e.g. -110)
            If None, defaults to -110 (standard market complement).
    Returns:
        float: Vig as a percentage (e.g. 0.0476 = 4.76% for a -110/-110 market)
    Example:
        get_vig_percentage(-110, -110) → 0.0476
        get_vig_percentage(-120)       → uses -110 as complement, not -120 mirror
    """
    try:
        p1 = american_odds_to_implied_probability(float(odds_side1))
        if odds_side2 is None:
            # FIXED: Default to -110 (standard complement) not a mirror of side 1
            # This is the correct assumption for most DraftKings prop markets
            p2 = american_odds_to_implied_probability(-110.0)
        else:
            p2 = american_odds_to_implied_probability(float(odds_side2))
        overround = p1 + p2
        return round(max(0.0, overround - 1.0), 6)
    except (ValueError, TypeError):
        return 0.0476  # Default -110/-110 vig
def calculate_true_edge(model_probability, odds):
    """
    Compute the true betting edge as model_probability minus implied probability.
    This is the correct edge calculation — NOT model_prob - 0.50.
    At -110 odds, the implied probability is 52.38%, so you need
    a model probability above 52.38% just to break even.
    Args:
        model_probability (float): Model's estimated win probability (0.0 to 1.0)
        odds (int or float): American odds on the bet (e.g. -110, +100)
    Returns:
        float: True edge as a decimal (e.g. 0.05 = 5% edge)
            Positive = value bet, Negative = no value
    Example:
        calculate_true_edge(0.60, -110) → 0.0762 (7.62% true edge)
        calculate_true_edge(0.52, -110) → -0.0038 (no edge at -110)
    """
    try:
        implied = american_odds_to_implied_probability(float(odds))
        return round(float(model_probability) - implied, 6)
    except (ValueError, TypeError):
        return 0.0
def calculate_expected_value_with_odds(model_probability, odds, stake=1.0):
    """
    Calculate the true expected value of a bet accounting for juice.
    EV = (p_win * net_win) - (p_lose * stake)
    where net_win is derived from the actual odds.
    Args:
        model_probability (float): Model's win probability (0.0 to 1.0).
            Values outside [0, 1] are silently clamped.
        odds (int or float): American odds (e.g. -110, +150)
        stake (float): Amount wagered. Default 1.0
    Returns:
        float: Expected value in stake units.
            Positive = profitable bet, Negative = losing bet
    Example:
        calculate_expected_value_with_odds(0.60, -110, stake=100) → 9.09
    """
    try:
        p = max(0.0, min(1.0, float(model_probability)))
        stake = max(0.0, float(stake))
        odds = float(odds)
        net_win = odds_to_payout_multiplier(odds) * stake - stake
        ev = p * net_win - (1.0 - p) * stake
        # Guard against float overflow from extreme odds (e.g., +99900)
        if not math.isfinite(ev):
            return 0.0
        return round(ev, 4)
    except (ValueError, TypeError, OverflowError):
        return 0.0
def devig_probabilities(over_odds, under_odds):
    """
    Remove the bookmaker's vig to get the true fair probabilities for both sides.
    Uses the multiplicative (proportional) devig method, which is considered
    more accurate than the additive method for NBA props.
    BEGINNER NOTE: When a book offers -110 on both sides, the implied
    probabilities sum to 1.048 (not 1.0). This 4.8% excess is the vig.
    Devigging scales both probabilities down proportionally so they sum to 1.0.
    The resulting "fair" probabilities are the book's true assessment.
    Args:
        over_odds (float): American odds for the Over side (e.g. -115)
        under_odds (float): American odds for the Under side (e.g. -105)
    Returns:
        tuple: (fair_over_prob, fair_under_prob) — both float, sum to 1.0
    Examples:
        devig_probabilities(-110, -110) → (0.5, 0.5)
        devig_probabilities(-120, +100) → (0.545, 0.455) approximately
    """
    try:
        p_over_raw  = american_odds_to_implied_probability(float(over_odds))
        p_under_raw = american_odds_to_implied_probability(float(under_odds))
        overround = p_over_raw + p_under_raw
        # Guard: when both sides carry extreme positive odds (e.g., +10000/+10000),
        # each implied probability is ~0.01, making overround ~0.02.  Dividing by
        # such a small overround amplifies rounding errors.  A 0.01 threshold safely
        # rejects degenerate markets while still allowing any realistic two-sided
        # market (normal markets have overround ≥ 1.0).
        if overround < MIN_VALID_OVERROUND:
            return (0.5, 0.5)
        # Multiplicative devig: divide each side by the overround
        fair_over  = p_over_raw  / overround
        fair_under = p_under_raw / overround
        return (round(fair_over, 6), round(fair_under, 6))
    except (ValueError, TypeError):
        return (0.5, 0.5)
def calculate_half_kelly_ev(model_probability, odds, bankroll=1.0):
    """
    Calculate the Half Kelly Criterion stake for a bet.
    BEGINNER NOTE: The Kelly Criterion tells you the optimal fraction of
    your bankroll to bet to maximize long-run growth. Half Kelly (50% of
    the Kelly fraction) reduces variance while capturing most of the edge.
    Formula:
        Kelly fraction f = (b*p - q) / b
        where b = net payout ratio, p = win prob, q = 1-p
        Half Kelly = f / 2
    Args:
        model_probability (float): Model's win probability (0-1)
        odds (float): American odds on the bet (e.g. -110, +150)
        bankroll (float): Total bankroll. Default 1.0 (returns fraction).
    Returns:
        dict: {
            'kelly_fraction': float (full Kelly as fraction of bankroll),
            'half_kelly_fraction': float (recommended 50% Kelly fraction),
            'half_kelly_stake': float (dollar amount at given bankroll),
            'ev': float (expected value per unit staked),
        }
    Example:
        calculate_half_kelly_ev(0.62, -110, bankroll=1000)
        → stake about $44.50 (4.45% of $1000 bankroll)
    """
    try:
        p = max(0.001, min(0.999, float(model_probability)))
        q = 1.0 - p
        b = odds_to_payout_multiplier(float(odds)) - 1.0  # Net payout ratio
        if b <= 0:
            return {"kelly_fraction": 0.0, "half_kelly_fraction": 0.0,
                    "half_kelly_stake": 0.0, "ev": 0.0}
        # Kelly fraction
        kelly_f = (b * p - q) / b
        # Negative Kelly → no edge → don't bet
        kelly_f = max(0.0, kelly_f)
        half_kelly = kelly_f / 2.0
        ev = p * b - q  # Expected value per unit staked
        return {
            "kelly_fraction": round(kelly_f, 6),
            "half_kelly_fraction": round(half_kelly, 6),
            "half_kelly_stake": round(half_kelly * float(bankroll), 2),
            "ev": round(ev, 6),
        }
    except (ValueError, TypeError):
        return {"kelly_fraction": 0.0, "half_kelly_fraction": 0.0,
                "half_kelly_stake": 0.0, "ev": 0.0}
def calculate_fractional_kelly(model_prob, book_odds, multiplier=0.25):
    """
    Calculate the Fractional Kelly Criterion wager.
    Full Kelly fraction: f = (b*p - q) / b
    where p = win probability, b = net payout ratio, q = 1 - p.
    The result is scaled by ``multiplier`` (e.g. 0.25 for Quarter Kelly).
    Negative-EV situations are clamped to a $0.00 wager.
    Divide-by-zero and degenerate inputs are handled gracefully.
    Args:
        model_prob (float): Model's win probability (0.0 to 1.0).
        book_odds (float): American odds on the bet (e.g. -110, +150).
        multiplier (float): Kelly fraction multiplier (0.0 to 1.0).
            Default 0.25 (Quarter Kelly, recommended for retail bettors).
    Returns:
        dict: {
            'kelly_fraction': float (full Kelly as fraction of bankroll),
            'fractional_kelly': float (fraction after applying multiplier),
            'multiplier': float (the multiplier used),
            'ev_per_unit': float (expected value per $1 staked),
            'edge': float (model_prob minus implied probability),
        }
        All fractions are 0.0 when the bet has no mathematical edge.
    Examples:
        calculate_fractional_kelly(0.62, -110) → ~2.8% fractional Kelly
        calculate_fractional_kelly(0.40, -110) → 0.0 (negative EV)
    """
    try:
        p = float(model_prob)
        p = max(0.001, min(0.999, p))
        q = 1.0 - p
        mult = max(0.0, min(1.0, float(multiplier)))
        payout = odds_to_payout_multiplier(float(book_odds))
        b = payout - 1.0  # net payout ratio
        # Guard: non-positive net payout → no sensible bet
        if b <= 0.0:
            return {
                "kelly_fraction": 0.0,
                "fractional_kelly": 0.0,
                "multiplier": mult,
                "ev_per_unit": 0.0,
                "edge": 0.0,
            }
        full_kelly = (b * p - q) / b
        ev_per_unit = p * b - q
        implied = american_odds_to_implied_probability(float(book_odds))
        edge = p - implied
        # Negative Kelly → no edge → clamp to zero
        if full_kelly <= 0.0:
            return {
                "kelly_fraction": 0.0,
                "fractional_kelly": 0.0,
                "multiplier": mult,
                "ev_per_unit": round(ev_per_unit, 6),
                "edge": round(edge, 6),
            }
        fractional = full_kelly * mult
        return {
            "kelly_fraction": _safe_float(round(full_kelly, 6)),
            "fractional_kelly": _safe_float(round(fractional, 6)),
            "multiplier": mult,
            "ev_per_unit": _safe_float(round(ev_per_unit, 6)),
            "edge": _safe_float(round(edge, 6)),
        }
    except (ValueError, TypeError, ZeroDivisionError):
        return {
            "kelly_fraction": 0.0,
            "fractional_kelly": 0.0,
            "multiplier": float(multiplier) if multiplier else 0.25,
            "ev_per_unit": 0.0,
            "edge": 0.0,
        }
def calculate_fair_odds_from_simulation(sim_array, target_line, direction="OVER"):
    """
    Calculate fair-value American odds from a Monte Carlo simulation array.
    Counts the proportion of simulated outcomes that satisfy the target
    condition and converts the resulting probability to American odds.
    Args:
        sim_array (list of float): Raw simulated stat values from the
            Monte Carlo engine (typically 1,000 runs).
        target_line (float): The prop line to evaluate against.
        direction (str): ``"OVER"`` counts results > target_line;
            ``"UNDER"`` counts results <= target_line.
    Returns:
        dict: {
            'win_probability': float (0.0 to 1.0),
            'fair_odds': float (American odds, e.g. -150 or +130),
            'target_line': float,
            'direction': str,
            'sample_size': int,
        }
        Returns +100/0.50 defaults when the array is empty.
    Examples:
        calculate_fair_odds_from_simulation([20, 22, 18, 25, 19], 19.5, "OVER")
        → win_probability ~0.60, fair_odds ~ -150
    """
    try:
        if not sim_array:
            return {
                "win_probability": 0.5,
                "fair_odds": 100.0,
                "target_line": float(target_line),
                "direction": direction.upper(),
                "sample_size": 0,
            }
        target = float(target_line)
        n = len(sim_array)
        d = direction.upper().strip()
        if d == "UNDER":
            hits = sum(1 for v in sim_array if v <= target)
        else:
            hits = sum(1 for v in sim_array if v > target)
        raw_prob = hits / n
        # Clamp to (0.01, 0.99) to avoid infinite or undefined odds
        prob = max(0.01, min(0.99, raw_prob))
        fair_odds = implied_probability_to_american_odds(prob)
        return {
            "win_probability": _safe_float(round(prob, 6), 0.5),
            "fair_odds": _safe_float(fair_odds, 100.0),
            "target_line": _safe_float(target),
            "direction": d,
            "sample_size": n,
        }
    except (ValueError, TypeError, ZeroDivisionError):
        return {
            "win_probability": 0.5,
            "fair_odds": 100.0,
            "target_line": float(target_line) if target_line else 0.0,
            "direction": direction.upper() if direction else "OVER",
            "sample_size": 0,
        }
# Backward-compatible alias (deprecated name)
calculate_synthetic_odds = calculate_fair_odds_from_simulation
# ── DFS / Sportsbook Fixed-Payout Tables ─────────────────────────────────
# These are the actual payout multipliers for each platform.
# Used by calculate_dfs_ev() to compute expected value against the real
# payouts that players receive.
# Standard sportsbook parlay payout table (all legs must hit).
_SPORTSBOOK_PARLAY_PAYOUTS = {
    2: {2: 2.64, 1: 0.0, 0: 0.0},
    3: {3: 5.96, 2: 0.0, 1: 0.0, 0: 0.0},
    4: {4: 12.28, 3: 0.0, 2: 0.0, 1: 0.0, 0: 0.0},
    5: {5: 24.35, 4: 0.0, 3: 0.0, 2: 0.0, 1: 0.0, 0: 0.0},
    6: {6: 47.77, 5: 0.0, 4: 0.0, 3: 0.0, 2: 0.0, 1: 0.0, 0: 0.0},
}
DFS_PAYOUT_TABLES = {
    "PrizePicks": {
        3: {3: 2.25, 2: 1.25, 1: 0.0, 0: 0.0},
        4: {4: 5.0, 3: 1.50, 2: 0.40, 1: 0.0, 0: 0.0},
        5: {5: 10.0, 4: 2.0, 3: 0.40, 2: 0.0, 1: 0.0, 0: 0.0},
        6: {6: 25.0, 5: 2.0, 4: 0.40, 3: 0.0, 2: 0.0, 1: 0.0, 0: 0.0},
    },
    "Underdog Fantasy": {
        3: {3: 2.25, 2: 1.20, 1: 0.0, 0: 0.0},
        4: {4: 5.0, 3: 1.50, 2: 0.0, 1: 0.0, 0: 0.0},
        5: {5: 10.0, 4: 2.0, 3: 0.50, 2: 0.0, 1: 0.0, 0: 0.0},
        6: {6: 25.0, 5: 2.5, 4: 0.40, 3: 0.0, 2: 0.0, 1: 0.0, 0: 0.0},
    },
    "DraftKings Pick6": _SPORTSBOOK_PARLAY_PAYOUTS,
    # Backward-compat aliases
    "Underdog": {
        3: {3: 2.25, 2: 1.20, 1: 0.0, 0: 0.0},
        4: {4: 5.0, 3: 1.50, 2: 0.0, 1: 0.0, 0: 0.0},
        5: {5: 10.0, 4: 2.0, 3: 0.50, 2: 0.0, 1: 0.0, 0: 0.0},
        6: {6: 25.0, 5: 2.5, 4: 0.40, 3: 0.0, 2: 0.0, 1: 0.0, 0: 0.0},
    },
    "DraftKings": _SPORTSBOOK_PARLAY_PAYOUTS,
}
def calculate_dfs_breakeven_probability(platform="DraftKings", pick_count=3):
    """
    Calculate the per-leg breakeven probability for a DFS flex entry.
    In DFS parlays the house edge is baked into the payout table, not into
    per-line juice.  The breakeven probability is the per-leg win rate at
    which the expected payout exactly equals the entry fee ($1).
    For an N-pick flex where hitting all N pays ``M×``, the break-even
    per-leg probability is approximately ``(1/M)^(1/N)``.  This function
    also accounts for partial-win payouts (flex) by summing the weighted
    payout across all hit counts.
    Args:
        platform (str): Sportsbook name (e.g., ``"PrizePicks"``, ``"DraftKings"``).
        pick_count (int): Number of picks in the entry (3–6).
    Returns:
        dict: {
            'breakeven_per_leg': float  (per-leg prob needed to break even),
            'all_hit_payout': float     (payout multiplier when all legs hit),
            'platform': str,
            'pick_count': int,
        }
    """
    table = DFS_PAYOUT_TABLES.get(platform, DFS_PAYOUT_TABLES["PrizePicks"])
    tier = table.get(pick_count)
    if not tier:
        return {
            "breakeven_per_leg": 0.50,
            "all_hit_payout": 1.0,
            "platform": platform,
            "pick_count": pick_count,
        }
    all_hit_payout = tier.get(pick_count, 1.0)
    # Binary search for the per-leg probability p where E[payout] = 1.0
    lo, hi = 0.01, 0.99
    for _ in range(64):
        mid = (lo + hi) / 2.0
        ev = _dfs_flex_ev_at_prob(mid, pick_count, tier)
        if ev < 1.0:
            lo = mid
        else:
            hi = mid
    breakeven = round((lo + hi) / 2.0, 6)
    return {
        "breakeven_per_leg": _safe_float(breakeven, 0.5),
        "all_hit_payout": _safe_float(all_hit_payout, 1.0),
        "platform": platform,
        "pick_count": pick_count,
    }
def _dfs_flex_ev_at_prob(per_leg_prob, n, tier):
    """Expected payout for a DFS flex entry when each leg wins with *per_leg_prob*."""
    p = max(0.001, min(0.999, per_leg_prob))
    q = 1.0 - p
    ev = 0.0
    for hits in range(n + 1):
        multiplier = tier.get(hits, 0.0)
        if multiplier <= 0:
            continue
        # Binomial: C(n, hits) * p^hits * q^(n-hits)
        ev += math.comb(n, hits) * (p ** hits) * (q ** (n - hits)) * multiplier
    return ev
def calculate_dfs_ev(leg_probabilities, platform="DraftKings", pick_count=None, entry_fee=1.0):
    """
    Calculate the expected value of a DFS flex entry using the platform's
    actual payout table — NOT sportsbook singles math.
    This is the correct EV for sportsbook parlays and DFS flex entries.
    The calculation sums the probability-weighted payout for
    every possible number of correct legs (0 through N).
    Args:
        leg_probabilities (list[float]): Per-leg model probabilities
            (each 0.0–1.0), e.g. ``[0.62, 0.58, 0.71]`` for a 3-pick.
        platform (str): Sportsbook name (e.g., ``"PrizePicks"``, ``"DraftKings"``).
        pick_count (int or None): Override the number of picks (defaults to
            ``len(leg_probabilities)``).
        entry_fee (float): Dollar amount of the entry (default $1.00).
    Returns:
        dict: {
            'expected_value': float    (EV in dollars),
            'expected_payout': float   (expected gross return),
            'roi_pct': float           (return on investment %),
            'all_hit_prob': float      (probability every leg hits),
            'platform': str,
            'pick_count': int,
        }
    """
    if not leg_probabilities:
        return {"expected_value": 0.0, "expected_payout": 0.0, "roi_pct": 0.0,
                "all_hit_prob": 0.0, "platform": platform, "pick_count": 0}
    n = pick_count or len(leg_probabilities)
    table = DFS_PAYOUT_TABLES.get(platform, DFS_PAYOUT_TABLES["PrizePicks"])
    tier = table.get(n)
    if not tier:
        return {"expected_value": 0.0, "expected_payout": 0.0, "roi_pct": 0.0,
                "all_hit_prob": 0.0, "platform": platform, "pick_count": n}
    probs = [max(0.001, min(0.999, float(p))) for p in leg_probabilities[:n]]
    # Enumerate all 2^n outcomes
    expected_payout = 0.0
    all_hit_prob = 1.0
    for p in probs:
        all_hit_prob *= p
    for mask in range(1 << n):
        hits = bin(mask).count("1")
        multiplier = tier.get(hits, 0.0)
        if multiplier <= 0:
            continue
        # Probability of this exact outcome
        outcome_prob = 1.0
        for i in range(n):
            if mask & (1 << i):
                outcome_prob *= probs[i]
            else:
                outcome_prob *= (1.0 - probs[i])
        expected_payout += outcome_prob * multiplier
    expected_payout_dollars = expected_payout * float(entry_fee)
    ev = expected_payout_dollars - float(entry_fee)
    roi = (ev / float(entry_fee)) * 100.0 if float(entry_fee) > 0 else 0.0
    return {
        "expected_value": _safe_float(round(ev, 4)),
        "expected_payout": _safe_float(round(expected_payout_dollars, 4)),
        "roi_pct": _safe_float(round(roi, 2)),
        "all_hit_prob": _safe_float(round(all_hit_prob, 6)),
        "platform": platform,
        "pick_count": n,
    }
def calculate_dfs_parlay_ev_from_sim(
    model_probability,
    platform="DraftKings",
    direction="OVER",
):
    """
    Compute per-leg DFS metrics from a simulation probability.
    This is the **Phase 2 Fixed-Payout Quant** bridge between the Monte
    Carlo simulation array and the DFS flex payout tables.  Given a
    model probability for one leg, it returns:
    * The DFS breakeven probability for each flex tier (3-6 pick)
    * Whether this leg *beats* each tier's breakeven
    * The per-leg EV contribution for each tier
    * Fractional Kelly sizing against the platform's effective odds
    This replaces the old sportsbook-singles EV approach with math that
    reflects the actual DFS payout structure.
    Args:
        model_probability (float): Model's win probability for the
            chosen direction (0.0–1.0), as output by the Monte Carlo
            simulation (e.g. ``simulation_output["probability_over"]``).
        platform (str): ``"PrizePicks"``, ``"Underdog"``, or
            ``"DraftKings"``.  Default ``"PrizePicks"``.
        direction (str): ``"OVER"`` or ``"UNDER"`` — used only for
            labelling; the math is symmetric.
    Returns:
        dict: {
            'model_probability': float,
            'platform': str,
            'direction': str,
            'tiers': {
                3: {'breakeven': float, 'beats_breakeven': bool,
                    'edge_vs_breakeven': float, 'all_hit_payout': float},
                4: { ... },
                5: { ... },
                6: { ... },
            },
            'best_tier': int or None  (tier with largest edge, or None),
            'kelly_fraction': float   (fractional Kelly for best tier),
        }
    Example:
        >>> calculate_dfs_parlay_ev_from_sim(0.62, "PrizePicks")
        {'model_probability': 0.62, 'tiers': {3: {'breakeven': 0.55, ...}}, ...}
    """
    try:
        p = max(0.001, min(0.999, float(model_probability)))
    except (ValueError, TypeError):
        p = 0.5
    tiers = {}
    best_tier = None
    best_edge = -999.0
    for pick_count in (3, 4, 5, 6):
        be_result = calculate_dfs_breakeven_probability(platform, pick_count)
        be_prob = be_result.get("breakeven_per_leg", 0.5)
        all_hit_payout = be_result.get("all_hit_payout", 1.0)
        edge = round(p - be_prob, 6)
        beats = p > be_prob
        tiers[pick_count] = {
            "breakeven": _safe_float(round(be_prob, 6), 0.5),
            "beats_breakeven": beats,
            "edge_vs_breakeven": _safe_float(round(edge, 6)),
            "all_hit_payout": _safe_float(all_hit_payout, 1.0),
        }
        if beats and edge > best_edge:
            best_edge = edge
            best_tier = pick_count
    # Kelly sizing against the best tier's effective odds
    kelly_frac = 0.0
    if best_tier is not None:
        tier_payout = tiers[best_tier]["all_hit_payout"]
        if tier_payout > 1.0:
            # Convert all-hit payout to effective American odds for Kelly
            effective_odds = implied_probability_to_american_odds(
                tiers[best_tier]["breakeven"]
            )
            kelly_result = calculate_fractional_kelly(p, effective_odds, 0.25)
            kelly_frac = kelly_result.get("fractional_kelly", 0.0)
    return {
        "model_probability": _safe_float(round(p, 6), 0.5),
        "platform": platform,
        "direction": direction.upper() if direction else "OVER",
        "tiers": tiers,
        "best_tier": best_tier,
        "kelly_fraction": _safe_float(round(kelly_frac, 6)),
    }
def generate_optimal_slip(filtered_props_list, platform="DraftKings"):
    """
    Generate optimal 2-to-5-man slips from a list of analysed props.
    Uses ``itertools.combinations`` to evaluate every combination of
    2 through 5 props.  Each combination is scored by its cumulative
    expected value against the platform's fixed payout table.
    Intra-game correlation is heavily penalised (same-game picks
    share variance, reducing true EV).
    Args:
        filtered_props_list (list of dict): Each dict must contain at
            minimum:
                - ``probability_over`` (float)
                - ``direction`` (str, ``"OVER"`` or ``"UNDER"``)
                - ``player_name`` (str)
                - ``stat_type`` (str)
                - ``player_team`` or ``team`` (str)
                - ``opponent`` (str, optional)
                - ``confidence_score`` (float, optional)
                - ``edge_percentage`` (float, optional)
        platform (str): Sportsbook name (e.g., ``"PrizePicks"``,
            ``"DraftKings"``).  Default ``"DraftKings"``.
    Returns:
        list of dict: Top slips sorted by cumulative EV (descending),
            each containing:
                - ``slip_size``: int (2-5)
                - ``picks``: list of pick dicts
                - ``cumulative_ev``: float (expected profit per $1)
                - ``combined_probability``: float
                - ``correlation_penalty``: float (1.0 = no penalty)
                - ``fair_odds``: float (American odds of the slip)
                - ``dfs_leg_edges``: list of (dict or None) per leg — each
                  dict has ``beats_breakeven``, ``edge_vs_breakeven``,
                  ``breakeven``, ``probability``; None when DFS data absent
                - ``dfs_legs_beat_breakeven``: int — count of legs that beat
                  the slip-tier's DFS breakeven
                - ``dfs_avg_edge``: float — mean per-leg edge vs breakeven
    """
    try:
        from engine.entry_optimizer import (
            PLATFORM_FLEX_TABLES,
            PRIZEPICKS_POWER_PAYOUT_TABLE,
            calculate_entry_expected_value,
        )
    except ImportError:
        return []
    if not filtered_props_list or len(filtered_props_list) < 2:
        return []
    payout_tables = PLATFORM_FLEX_TABLES.get(platform, PLATFORM_FLEX_TABLES.get("DraftKings", {}))
    power_table = PRIZEPICKS_POWER_PAYOUT_TABLE if platform == "PrizePicks" else {}
    all_slips = []
    # Cap the pool to keep combinatorics tractable
    pool = sorted(
        filtered_props_list,
        key=lambda p: abs(p.get("edge_percentage", 0)),
        reverse=True,
    )[:25]
    for slip_size in range(2, min(6, len(pool) + 1)):
        table = payout_tables.get(slip_size)
        # Fall back to power table for 2-leg (PrizePicks has no 2-leg flex)
        if not table and slip_size in power_table:
            table = power_table[slip_size]
        if not table:
            continue
        for combo in itertools.combinations(range(len(pool)), slip_size):
            picks = [pool[i] for i in combo]
            # ── enforce unique players ──
            seen_players = set()
            skip = False
            for pk in picks:
                pname = pk.get("player_name", "").lower().strip()
                if pname in seen_players:
                    skip = True
                    break
                seen_players.add(pname)
            if skip:
                continue
            # ── compute directional probabilities ──
            probs = []
            for pk in picks:
                if pk.get("direction", "OVER") == "OVER":
                    probs.append(max(0.01, min(0.99, pk.get("probability_over", 0.5))))
                else:
                    probs.append(max(0.01, min(0.99, 1.0 - pk.get("probability_over", 0.5))))
            # ── intra-game correlation penalty ──
            game_counts = {}
            for pk in picks:
                team = (pk.get("player_team") or pk.get("team", "")).upper().strip()
                opp = pk.get("opponent", "").upper().strip()
                key = frozenset([team, opp]) if (team and opp) else frozenset([team])
                game_counts[key] = game_counts.get(key, 0) + 1
            correlation_penalty = 1.0
            for cnt in game_counts.values():
                if cnt >= 3:
                    # 15% penalty — 3+ correlated legs share game-level variance
                    correlation_penalty *= 0.85
                elif cnt == 2:
                    # 7% penalty — 2 same-game legs share moderate variance
                    correlation_penalty *= 0.93
            # ── cumulative EV ──
            ev_result = calculate_entry_expected_value(probs, table, 1.0)
            raw_ev = ev_result.get("expected_value_dollars", 0.0)
            cumulative_ev = raw_ev * correlation_penalty
            combined_prob = 1.0
            for pr in probs:
                combined_prob *= pr
            fair_slip_odds = implied_probability_to_american_odds(
                max(0.001, min(0.999, combined_prob))
            )
            # ── per-leg DFS breakeven edge (Phase 4) ──
            # If picks carry dfs_parlay_ev from Phase 2, compute which
            # legs beat the breakeven for this slip's tier and the avg edge.
            dfs_leg_edges = []
            for pk, p_dir_prob in zip(picks, probs):
                parlay_data = pk.get("dfs_parlay_ev")
                if parlay_data:
                    tier_data = parlay_data.get("tiers", {}).get(slip_size) or \
                                parlay_data.get("tiers", {}).get(str(slip_size))
                    if tier_data:
                        dfs_leg_edges.append({
                            "beats_breakeven": tier_data.get("beats_breakeven", False),
                            "edge_vs_breakeven": _safe_float(
                                tier_data.get("edge_vs_breakeven", 0), 0
                            ),
                            "breakeven": _safe_float(
                                tier_data.get("breakeven", 0.5), 0.5
                            ),
                            "probability": round(p_dir_prob, 4),
                        })
                    else:
                        dfs_leg_edges.append(None)
                else:
                    dfs_leg_edges.append(None)
            # Aggregate DFS metrics for the slip
            _valid_edges = [e for e in dfs_leg_edges if e is not None]
            legs_beat_be = sum(1 for e in _valid_edges if e["beats_breakeven"])
            avg_dfs_edge = (
                round(sum(e["edge_vs_breakeven"] for e in _valid_edges) / len(_valid_edges), 6)
                if _valid_edges else 0.0
            )
            all_slips.append({
                "slip_size": slip_size,
                "picks": picks,
                "cumulative_ev": round(cumulative_ev, 4),
                "combined_probability": round(combined_prob, 6),
                "correlation_penalty": round(correlation_penalty, 4),
                "fair_odds": fair_slip_odds,
                "dfs_leg_edges": dfs_leg_edges,
                "dfs_legs_beat_breakeven": legs_beat_be,
                "dfs_avg_edge": _safe_float(avg_dfs_edge),
            })
    # Sort by cumulative EV descending, return top 10
    all_slips.sort(key=lambda s: s["cumulative_ev"], reverse=True)
    return all_slips[:10]
def odds_to_payout_multiplier(american_odds):
    """
    Convert American odds to a gross payout multiplier.
    The payout multiplier is the total return per unit staked (including stake).
    Args:
        american_odds (int or float): American odds (e.g. -110, +150)
    Returns:
        float: Gross payout multiplier (e.g. 1.909 for -110, 2.50 for +150)
    Examples:
        -110 → 1.9091 (bet $110, get back $210 total → 1.909x)
        +150 → 2.5    (bet $100, get back $250 total → 2.5x)
        -200 → 1.5    (bet $200, get back $300 total → 1.5x)
    """
    try:
        odds = float(american_odds)
        # American odds of exactly 0 are invalid — return default -110 payout.
        if odds == 0:
            return 1.9091
        if odds < 0:
            return round(1.0 + (100.0 / abs(odds)), 6)
        else:
            return round(1.0 + (odds / 100.0), 6)
    except (ValueError, TypeError):
        return 1.9091  # Default -110 payout
