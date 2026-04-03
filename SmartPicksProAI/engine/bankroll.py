# ============================================================
# FILE: engine/bankroll.py
# PURPOSE: Kelly Criterion Bankroll Management
#          Implements the Kelly Criterion for optimal bet sizing.
#          Tells the user exactly how much to bet on each entry
#          to maximize long-term bankroll growth while minimizing
#          risk of ruin.
#
#          Full Kelly = (p*b - (1-p)) / b
#          Quarter Kelly = Full Kelly / 4 (conservative, recommended)
#
#          Kelly Criterion assumes the edge is real. If the model
#          edge is not validated (see clv_tracker.py), use even
#          more conservative sizing.
#
# CONNECTS TO: engine/entry_optimizer.py (EV values),
#              pages/6_🧬_Entry_Builder.py (bankroll UI)
# CONCEPTS COVERED: Kelly Criterion, bankroll management,
#                   risk of ruin, expected value, sizing
# ============================================================

import math


def _safe_float(value, fallback=0.0):
    """Return *value* if it is a finite float, otherwise *fallback*.

    Last-line-of-defence guard that prevents NaN or ±inf from leaking
    out of the bankroll engine into downstream UI / allocation code.
    """
    try:
        v = float(value)
        if math.isfinite(v):
            return v
        return float(fallback)
    except (ValueError, TypeError):
        return float(fallback)


# ============================================================
# SECTION: Kelly Constants
# ============================================================

# Kelly fraction modes and their divisors.
# Full Kelly (divisor=1) is theoretically optimal but practically too aggressive.
# Quarter Kelly (divisor=4) is recommended for retail sports bettors.
KELLY_DIVISORS = {
    "full":    1,    # 100% of Kelly — theoretical maximum growth, high variance
    "half":    2,    # 50% of Kelly — reduced variance, popular among professionals
    "quarter": 4,    # 25% of Kelly — conservative, recommended for retail bettors
    "eighth":  8,    # 12.5% of Kelly — very conservative, good for high-variance props
}

# Maximum bankroll percentage per single entry (hard cap regardless of Kelly).
MAX_PER_ENTRY_FRACTION = 0.05   # Never bet more than 5% on one entry

# Maximum total bankroll exposure per session (all entries combined).
MAX_SESSION_FRACTION = 0.25     # Never risk more than 25% of bankroll in one session

# Minimum Kelly fraction to be worth betting.
MIN_KELLY_THRESHOLD = 0.001     # Below 0.1% of bankroll → skip the bet

# ============================================================
# END SECTION: Kelly Constants
# ============================================================


# ============================================================
# SECTION: Core Kelly Calculation
# ============================================================

def calculate_kelly_fraction(win_probability, payout_multiplier, kelly_fraction_mode='quarter'):
    """
    Compute the optimal Kelly fraction for a bet.

    Full Kelly formula: f = (p*b - (1-p)) / b
    where p = win probability, b = net payout multiplier (payout - 1).

    Args:
        win_probability (float): Probability of winning (0-1).
        payout_multiplier (float): Gross payout multiplier (e.g., 3.0 for 3x return).
        kelly_fraction_mode (str): One of 'full', 'half', 'quarter', 'eighth'.
            Default 'quarter' (recommended for retail bettors).

    Returns:
        float: Fraction of bankroll to bet (0.0 = don't bet).
            Capped at MAX_PER_ENTRY_FRACTION.
            Returns 0.0 if Kelly is negative (no edge).

    Example:
        # 62% win prob, 3x payout multiplier, quarter Kelly
        calculate_kelly_fraction(0.62, 3.0) → ~0.028 (bet 2.8% of bankroll)
    """
    try:
        p = float(win_probability)
        gross = float(payout_multiplier)

        # Guard against degenerate inputs
        if p <= 0.0 or p >= 1.0 or gross <= 1.0:
            return 0.0

        # Net payout per unit risked
        b = gross - 1.0

        # Full Kelly fraction
        full_kelly = (p * b - (1.0 - p)) / b

        # Negative Kelly means no mathematical edge — don't bet
        if full_kelly <= 0.0:
            return 0.0

        divisor = KELLY_DIVISORS.get(kelly_fraction_mode, 4)
        fractional_kelly = full_kelly / divisor

        # Hard cap: never exceed MAX_PER_ENTRY_FRACTION of bankroll
        return _safe_float(round(min(fractional_kelly, MAX_PER_ENTRY_FRACTION), 6), 0.0)

    except Exception:
        return 0.0


# ============================================================
# END SECTION: Core Kelly Calculation
# ============================================================


# ============================================================
# SECTION: Multi-Entry Bankroll Allocation
# ============================================================

def get_bankroll_allocation(entries, bankroll, kelly_fraction_mode='quarter'):
    """
    Allocate bankroll across multiple entries using Kelly Criterion.

    Higher-EV entries get proportionally larger allocations.
    Total allocation is capped at MAX_SESSION_FRACTION of bankroll.

    Args:
        entries (list of dict): Each entry should have:
            - 'win_probability': float
            - 'payout_multiplier': float (gross payout)
            - 'ev': float (expected value in dollars, optional)
            - 'entry_label': str (display name, optional)
        bankroll (float): Total available bankroll in dollars.
        kelly_fraction_mode (str): Kelly sizing mode. Default 'quarter'.

    Returns:
        list of dict: Same entries with added fields:
            - 'recommended_bet': float (dollars to bet)
            - 'kelly_fraction': float (fraction of bankroll)
            - 'expected_profit': float (dollars expected profit)
            - 'kelly_mode': str

    Example:
        entries = [{'win_probability': 0.62, 'payout_multiplier': 3.0}]
        get_bankroll_allocation(entries, bankroll=500) →
        [{'recommended_bet': 14.0, 'kelly_fraction': 0.028, ...}]
    """
    try:
        bankroll = float(bankroll)
        if bankroll <= 0.0 or not entries:
            return entries

        # Step 1: compute raw Kelly fraction and raw dollar bet for each entry
        fractions = []
        for entry in entries:
            p = float(entry.get('win_probability', 0.5))
            gross = float(entry.get('payout_multiplier', 1.0))
            kf = calculate_kelly_fraction(p, gross, kelly_fraction_mode)
            fractions.append(kf)

        raw_bets = [kf * bankroll for kf in fractions]
        total_raw = sum(raw_bets)

        # Step 2: scale down if total exceeds session cap
        session_cap = MAX_SESSION_FRACTION * bankroll
        scale = 1.0
        if total_raw > session_cap and total_raw > 0.0:
            scale = session_cap / total_raw

        # Step 3: build result list
        result = []
        for i, entry in enumerate(entries):
            kf = fractions[i]
            scaled_bet = raw_bets[i] * scale

            # Only include if above minimum threshold
            if kf >= MIN_KELLY_THRESHOLD:
                recommended_bet = max(1.0, round(scaled_bet, 2))
            else:
                recommended_bet = 0.0

            p = float(entry.get('win_probability', 0.5))
            gross = float(entry.get('payout_multiplier', 1.0))
            expected_profit = recommended_bet * (p * gross - 1.0)

            enriched = dict(entry)
            enriched['recommended_bet'] = _safe_float(recommended_bet, 0.0)
            enriched['kelly_fraction'] = _safe_float(round(kf * scale, 6), 0.0)
            enriched['expected_profit'] = _safe_float(round(expected_profit, 2), 0.0)
            enriched['kelly_mode'] = kelly_fraction_mode
            result.append(enriched)

        return result

    except Exception:
        return entries


# ============================================================
# END SECTION: Multi-Entry Bankroll Allocation
# ============================================================


# ============================================================
# SECTION: Session Risk Summary
# ============================================================

def get_session_risk_summary(entries, bankroll):
    """
    Return a comprehensive risk summary for the current session.

    Args:
        entries (list of dict): Entries with 'win_probability', 'payout_multiplier',
            'recommended_bet' (from get_bankroll_allocation output).
        bankroll (float): Total available bankroll.

    Returns:
        dict: {
            'total_at_risk': float,          # Total dollars at risk
            'total_at_risk_pct': float,      # As % of bankroll
            'expected_profit': float,        # Expected session profit
            'prob_positive_session': float,  # P(at least 1 entry wins)
            'worst_case_loss': float,        # All entries miss
            'best_case_gain': float,         # All entries win
            'risk_of_ruin_estimate': float,  # Rough estimate over 100 sessions
            'num_entries': int,
        }

    Example:
        summary = get_session_risk_summary(entries, bankroll=500)
        print(f"At risk: ${summary['total_at_risk']:.2f}")
    """
    try:
        bankroll = float(bankroll)
        if not entries or bankroll <= 0.0:
            return {
                'total_at_risk': 0.0,
                'total_at_risk_pct': 0.0,
                'expected_profit': 0.0,
                'prob_positive_session': 0.0,
                'worst_case_loss': 0.0,
                'best_case_gain': 0.0,
                'risk_of_ruin_estimate': 0.0,
                'num_entries': 0,
            }

        total_at_risk = sum(float(e.get('recommended_bet', 0.0)) for e in entries)
        expected_profit = sum(float(e.get('expected_profit', 0.0)) for e in entries)

        # P(at least one entry wins) = 1 - P(all entries lose)
        prob_all_lose = 1.0
        for e in entries:
            p_win = float(e.get('win_probability', 0.5))
            prob_all_lose *= max(0.0, min(1.0, 1.0 - p_win))
        prob_positive_session = 1.0 - prob_all_lose

        worst_case_loss = total_at_risk

        best_case_gain = sum(
            float(e.get('recommended_bet', 0.0)) * float(e.get('payout_multiplier', 1.0))
            for e in entries
        ) - total_at_risk

        # Rough risk-of-ruin estimate over 100 sessions.
        # This uses a simplified Kelly-based approximation:
        #   P(ruin) ≈ exp(-2 * edge_per_session * N_sessions / bankroll)
        # This is derived from the gambler's ruin problem with a continuous
        # random walk assumption. It underestimates true ruin probability
        # (it ignores variance and assumes reinvestment), so treat as a
        # lower bound. A formal simulation would be more accurate.
        # Reference: Thorp (1997), "The Kelly Criterion in Blackjack,
        #            Sports Betting and the Stock Market."
        if expected_profit <= 0.0:
            risk_of_ruin_estimate = 1.0
        else:
            exponent = -2.0 * expected_profit * 100.0 / bankroll
            risk_of_ruin_estimate = min(1.0, max(0.0, math.exp(exponent)))

        return {
            'total_at_risk': _safe_float(round(total_at_risk, 2), 0.0),
            'total_at_risk_pct': _safe_float(round(total_at_risk / bankroll, 4) if bankroll > 0 else 0.0, 0.0),
            'expected_profit': _safe_float(round(expected_profit, 2), 0.0),
            'prob_positive_session': _safe_float(round(prob_positive_session, 4), 0.0),
            'worst_case_loss': _safe_float(round(worst_case_loss, 2), 0.0),
            'best_case_gain': _safe_float(round(best_case_gain, 2), 0.0),
            'risk_of_ruin_estimate': _safe_float(round(risk_of_ruin_estimate, 4), 0.0),
            'num_entries': len(entries),
        }

    except Exception:
        return {
            'total_at_risk': 0.0,
            'total_at_risk_pct': 0.0,
            'expected_profit': 0.0,
            'prob_positive_session': 0.0,
            'worst_case_loss': 0.0,
            'best_case_gain': 0.0,
            'risk_of_ruin_estimate': 0.0,
            'num_entries': 0,
        }


# ============================================================
# END SECTION: Session Risk Summary
# ============================================================


def odds_to_payout_multiplier(american_odds):
    """
    Convert American odds to a gross payout multiplier for Kelly sizing.

    Delegates to engine.odds_engine for a single implementation.

    Args:
        american_odds (int or float): American odds (e.g. -110, +150)

    Returns:
        float: Gross payout multiplier (e.g. 1.909 for -110)
    """
    try:
        from engine.odds_engine import odds_to_payout_multiplier as _impl
        return _impl(american_odds)
    except ImportError:
        try:
            odds = float(american_odds)
            if odds == 0:
                return 1.9091  # Invalid odds value; fall back to default -110 payout
            if odds < 0:
                return round(1.0 + (100.0 / abs(odds)), 6)
            else:
                return round(1.0 + (odds / 100.0), 6)
        except (ValueError, TypeError):
            return 1.9091  # Default -110 payout
