"""
engine/arbitrage_matcher.py
---------------------------
Triangulates sportsbook player-prop lines across multiple books to
surface EV discrepancies for the Vegas Vault dashboard.

Public API
----------
find_ev_discrepancies(sportsbook_props) -> list[dict]
"""

import re
import logging

try:
    from utils.logger import get_logger
    _logger = get_logger(__name__)
except ImportError:
    _logger = logging.getLogger(__name__)

def calculate_implied_probability(american_odds: float) -> float:
    """Convert American odds to implied probability percentage."""
    odds = float(american_odds)
    if odds == 0:
        return 52.38  # Invalid odds; default to -110 breakeven
    if odds < 0:
        return abs(odds) / (abs(odds) + 100.0) * 100
    return 100.0 / (odds + 100.0) * 100

try:
    from engine.odds_engine import devig_probabilities, get_vig_percentage
    _DEVIG_AVAILABLE = True
except ImportError:
    _DEVIG_AVAILABLE = False
    _logger.warning("devig_probabilities not available; fair probability fields will use fallbacks.")
    def devig_probabilities(over_odds, under_odds):
        return (0.5, 0.5)
    def get_vig_percentage(odds_side1, odds_side2=None):
        return 0.0476


# ── Name-normalisation helpers ────────────────────────────────────────────────

_PUNCT_RE = re.compile(r"[^a-z\s]")


def _normalise_name(name: str) -> str:
    """Lower-case, strip, remove punctuation."""
    return _PUNCT_RE.sub("", name.strip().lower()).strip()


def _names_match(a: str, b: str) -> bool:
    """Return True when two player names refer to the same person.

    Handles exact matches and abbreviated first names such as
    ``"L. James"`` vs ``"LeBron James"`` using a token-match ratio:
    if the last name matches exactly AND the first initial matches,
    treat as the same player.
    """
    na = _normalise_name(a)
    nb = _normalise_name(b)
    if na == nb:
        return True

    tokens_a = na.split()
    tokens_b = nb.split()
    if len(tokens_a) < 2 or len(tokens_b) < 2:
        return False

    # Last name must match exactly.
    if tokens_a[-1] != tokens_b[-1]:
        return False

    # First initial must match.
    if tokens_a[0][0] == tokens_b[0][0]:
        return True

    return False


# ── Canonical-name grouping key ───────────────────────────────────────────────

def _group_key(name: str, stat_type: str, line: float) -> tuple:
    return (_normalise_name(name), stat_type.strip().lower(), float(line))


# ── Public API ────────────────────────────────────────────────────────────────

def find_ev_discrepancies(sportsbook_props: list) -> list:
    """Find EV discrepancies across sportsbook player-prop lines.

    Parameters
    ----------
    sportsbook_props : list[dict]
        Raw output of ``get_player_props()``.  Each dict has keys:
        ``player_name``, ``stat_type``, ``line``, ``platform``,
        ``over_odds``, ``under_odds``.

    Returns
    -------
    list[dict]
        Discrepancies sorted by *ev_edge* descending.  Only includes
        props with ``ev_edge >= 7.0``.
    """
    if not sportsbook_props:
        return []

    # ── 1. Build groups by (normalised_name, stat_type, line) ─────────
    # We need to merge names like "LeBron James" and "L. James".
    # First pass: collect canonical names per normalised name.
    canonical_names: dict[str, str] = {}  # normalised -> longest original
    groups: dict[tuple, list] = {}

    for prop in sportsbook_props:
        try:
            name = prop.get("player_name", "")
            stat = prop.get("stat_type", "")
            line = prop.get("line")
            if not name or not stat or line is None:
                continue
            line = float(line)
        except (TypeError, ValueError):
            continue

        norm = _normalise_name(name)

        # Track best (longest) display name.
        if norm not in canonical_names or len(name) > len(canonical_names[norm]):
            canonical_names[norm] = name

        key = _group_key(name, stat, line)
        groups.setdefault(key, []).append(prop)

    # Second pass: merge groups whose normalised names match via
    # _names_match (handles abbreviated first names).
    merged_groups: dict[tuple, list] = {}
    norm_to_merged: dict[str, str] = {}  # maps normalised name -> canonical norm

    for key, props in groups.items():
        norm_name, stat, line = key
        # Check if we already have a merged key for this name.
        merged_norm = norm_to_merged.get(norm_name)
        if merged_norm is None:
            # Check against all existing merged norms.
            for existing_norm in list(norm_to_merged.values()):
                if _names_match(norm_name, existing_norm):
                    merged_norm = existing_norm
                    break
            if merged_norm is None:
                merged_norm = norm_name
            norm_to_merged[norm_name] = merged_norm

        merged_key = (merged_norm, stat, line)
        merged_groups.setdefault(merged_key, []).extend(props)

        # Also update canonical display name.
        orig = canonical_names.get(norm_name, "")
        existing = canonical_names.get(merged_norm, "")
        if len(orig) > len(existing):
            canonical_names[merged_norm] = orig

    # ── 2. For each group, find best Over/Under across books ──────────
    discrepancies = []

    for (norm_name, stat, line), props in merged_groups.items():
        if len(props) < 1:
            continue

        best_over_odds = None
        best_over_book = ""
        best_under_odds = None
        best_under_book = ""
        book_set = set()

        for p in props:
            platform = p.get("platform", "Unknown")
            book_set.add(platform)

            over = p.get("over_odds")
            under = p.get("under_odds")

            # Best Over = highest implied probability = most aggressive pricing
            # (most negative odds for favourites, or lowest positive odds).
            # We compare implied probabilities to select the strongest signal.
            if over is not None:
                try:
                    over_int = int(over)
                    if best_over_odds is None:
                        best_over_odds = over_int
                        best_over_book = platform
                    else:
                        over_ip = calculate_implied_probability(over_int)
                        best_ip = calculate_implied_probability(best_over_odds)
                        if over_ip > best_ip:
                            best_over_odds = over_int
                            best_over_book = platform
                except (TypeError, ValueError):
                    pass

            # Best Under = highest implied probability = most aggressive pricing.
            if under is not None:
                try:
                    under_int = int(under)
                    if best_under_odds is None:
                        best_under_odds = under_int
                        best_under_book = platform
                    else:
                        under_ip = calculate_implied_probability(under_int)
                        best_uip = calculate_implied_probability(best_under_odds)
                        if under_ip > best_uip:
                            best_under_odds = under_int
                            best_under_book = platform
                except (TypeError, ValueError):
                    pass

        if best_over_odds is None and best_under_odds is None:
            continue

        # ── 3. Calculate implied probabilities ────────────────────────
        over_prob = 0.0
        under_prob = 0.0
        if best_over_odds is not None:
            over_prob = calculate_implied_probability(best_over_odds)
        if best_under_odds is not None:
            under_prob = calculate_implied_probability(best_under_odds)

        # ── 3b. Devig: compute fair (vig-free) probabilities ─────────
        # For each book in the group, devig its over/under pair and
        # track the highest fair probability per side.  This removes
        # bookmaker juice and gives a truer picture of the edge.
        best_fair_over = 0.0
        best_fair_under = 0.0
        best_fair_over_book = ""
        best_fair_under_book = ""
        consensus_fair_over_sum = 0.0
        consensus_fair_under_sum = 0.0
        devig_count = 0

        if _DEVIG_AVAILABLE:
            for p in props:
                p_over = p.get("over_odds")
                p_under = p.get("under_odds")
                if p_over is None or p_under is None:
                    continue
                try:
                    fair_o, fair_u = devig_probabilities(int(p_over), int(p_under))
                    fair_o_pct = round(fair_o * 100.0, 2)
                    fair_u_pct = round(fair_u * 100.0, 2)
                    platform = p.get("platform", "Unknown")
                    if fair_o_pct > best_fair_over:
                        best_fair_over = fair_o_pct
                        best_fair_over_book = platform
                    if fair_u_pct > best_fair_under:
                        best_fair_under = fair_u_pct
                        best_fair_under_book = platform
                    consensus_fair_over_sum += fair_o_pct
                    consensus_fair_under_sum += fair_u_pct
                    devig_count += 1
                except (TypeError, ValueError):
                    pass

        # Consensus fair probability: average devigged prob across all books.
        # This is the market's best estimate of the true probability.
        consensus_fair_over = round(consensus_fair_over_sum / max(1, devig_count), 2)
        consensus_fair_under = round(consensus_fair_under_sum / max(1, devig_count), 2)

        # ── 4. EV edge = max implied prob - 50 ───────────────────────
        max_prob = max(over_prob, under_prob)
        ev_edge = round(max_prob - 50.0, 2)

        # ── 5. Filter: ev_edge >= 7.0 ────────────────────────────────
        if ev_edge < 7.0:
            continue

        # ── 6. God Mode Lock: any side >= 60% implied ────────────────
        is_god_mode = over_prob >= 60.0 or under_prob >= 60.0

        # ── 6b. Recommended side and fair edge ────────────────────────
        # Recommend the side with the higher fair (devigged) probability.
        # Fall back to raw implied if devig not available.
        if best_fair_over > 0 or best_fair_under > 0:
            if best_fair_over >= best_fair_under:
                rec_side = "OVER"
                rec_book = best_fair_over_book or best_over_book
                fair_prob = best_fair_over
            else:
                rec_side = "UNDER"
                rec_book = best_fair_under_book or best_under_book
                fair_prob = best_fair_under
        else:
            if over_prob >= under_prob:
                rec_side = "OVER"
                rec_book = best_over_book
                fair_prob = over_prob
            else:
                rec_side = "UNDER"
                rec_book = best_under_book
                fair_prob = under_prob

        # True edge after vig removal — the real signal.
        true_ev_edge = round(fair_prob - 50.0, 2) if fair_prob > 0 else ev_edge

        # Vig % on the recommended side's best odds.
        rec_odds = best_over_odds if rec_side == "OVER" else best_under_odds
        comp_odds = best_under_odds if rec_side == "OVER" else best_over_odds
        try:
            vig = round(get_vig_percentage(rec_odds, comp_odds) * 100.0, 2) if (rec_odds and comp_odds) else 0.0
        except (TypeError, ValueError, ZeroDivisionError):
            vig = 0.0

        display_name = canonical_names.get(norm_name, norm_name)

        # ── 6c. Kelly fraction — optimal bet sizing ───────────────────
        # Quarter-Kelly is the standard for retail bettors. This tells
        # users *how much* to bet on each edge, not just *what* to bet.
        kelly = calculate_kelly_fraction_for_edge(fair_prob, rec_odds)

        # ── 6d. Convergence score — multi-book agreement strength ─────
        conv = calculate_convergence_score(len(book_set), devig_count, vig)

        # ── 6e. Edge quality grade ────────────────────────────────────
        grade_info = grade_edge_quality(
            ev_edge, true_ev_edge, len(book_set), is_god_mode, kelly, vig,
        )

        discrepancies.append({
            "player_name": display_name,
            "stat_type": stat,
            "true_line": line,
            "best_over_odds": best_over_odds if best_over_odds is not None else 0,
            "best_over_book": best_over_book,
            "best_under_odds": best_under_odds if best_under_odds is not None else 0,
            "best_under_book": best_under_book,
            "best_over_implied_prob": round(over_prob, 2),
            "best_under_implied_prob": round(under_prob, 2),
            "ev_edge": ev_edge,
            "is_god_mode_lock": is_god_mode,
            "book_count": len(book_set),
            # ── Devig-enhanced fields ─────────────────────────────────
            "recommended_side": rec_side,
            "recommended_book": rec_book,
            "fair_probability": round(fair_prob, 2),
            "true_ev_edge": true_ev_edge,
            "fair_over_prob": round(best_fair_over, 2),
            "fair_under_prob": round(best_fair_under, 2),
            "consensus_fair_over": consensus_fair_over,
            "consensus_fair_under": consensus_fair_under,
            "vig_pct": vig,
            # ── Win-rate enhancement fields ───────────────────────────
            "kelly_fraction": kelly,
            "convergence_score": conv,
            "edge_grade": grade_info["grade"],
            "edge_grade_label": grade_info["label"],
        })

    # ── 7. Sort by ev_edge descending ─────────────────────────────────
    discrepancies.sort(key=lambda d: d["ev_edge"], reverse=True)

    _logger.info("find_ev_discrepancies: %d props passed filter.", len(discrepancies))
    return discrepancies


# ============================================================
# SECTION: Kelly Criterion Bet Sizing for Vegas Vault
# ============================================================

def calculate_kelly_fraction_for_edge(fair_probability_pct, american_odds):
    """Compute Quarter-Kelly fraction for a Vegas Vault discrepancy.

    Uses the standard Kelly Criterion formula:
        Full Kelly f = (p * b - q) / b
    where p = win probability, q = 1 - p, b = net payout ratio.
    Quarter-Kelly is f / 4 (recommended for retail bettors).

    Parameters
    ----------
    fair_probability_pct : float
        Devigged fair win probability (0-100 scale, e.g. 62.5).
    american_odds : int or float or None
        American odds on the recommended side (e.g. -145, +130).

    Returns
    -------
    float
        Quarter-Kelly fraction of bankroll to wager (0.0 to 0.05).
        Capped at 5% max to prevent over-betting.
        Returns 0.0 if inputs are invalid or edge is negative.
    """
    try:
        if american_odds is None or fair_probability_pct is None:
            return 0.0
        p = float(fair_probability_pct) / 100.0
        if p <= 0 or p >= 1:
            return 0.0
        odds = float(american_odds)
        # Net payout ratio: how much you win per $1 wagered
        if odds > 0:
            b = odds / 100.0
        elif odds < 0:
            b = 100.0 / abs(odds)
        else:
            return 0.0
        q = 1.0 - p
        # Full Kelly
        full_kelly = (p * b - q) / b if b > 0 else 0.0
        if full_kelly <= 0:
            return 0.0
        # Quarter Kelly (conservative)
        quarter_kelly = full_kelly / 4.0
        # Cap at 5% of bankroll
        return round(min(quarter_kelly, 0.05), 4)
    except (TypeError, ValueError, ZeroDivisionError):
        return 0.0


# ============================================================
# END SECTION: Kelly Criterion Bet Sizing for Vegas Vault
# ============================================================


# ============================================================
# SECTION: Convergence Score — Multi-Book Agreement
# ============================================================

def calculate_convergence_score(book_count, devig_count, vig_pct):
    """Score how well multiple books converge on confirming this edge.

    A higher convergence score means the edge is more likely real
    (not a mispricing by one stale book).

    Components:
        - Book breadth (0-40): More books = more confirmation
        - Devig depth (0-30): More deviggable pairs = better fair-prob estimate
        - Vig tightness (0-30): Lower vig = sharper line = stronger signal

    Parameters
    ----------
    book_count : int
        Number of distinct sportsbooks pricing this prop.
    devig_count : int
        Number of books with both over/under odds (deviggable pairs).
    vig_pct : float
        Vig percentage on the recommended side (0-20 typical).

    Returns
    -------
    int
        Convergence score 0-100. Higher = more reliable edge.
    """
    try:
        # Book breadth: 1 book = 0, 2 = 13, 3 = 27, 4 = 33, 5+ = 40
        breadth = min(40, int(max(0, book_count - 1)) * 13)

        # Devig depth: 0 deviggable = 0, 1 = 10, 2 = 20, 3+ = 30
        depth = min(30, int(max(0, devig_count)) * 10)

        # Vig tightness: 0% vig = 30, 5% = 15, 10%+ = 0
        vig_val = max(0.0, float(vig_pct))
        tightness = max(0, int(30 - vig_val * 3))

        return min(100, breadth + depth + tightness)
    except (TypeError, ValueError):
        return 0


# ============================================================
# END SECTION: Convergence Score
# ============================================================


# ============================================================
# SECTION: Edge Quality Grade
# ============================================================

# Grade thresholds — determines the A/B/C/D/F grade.
_GRADE_THRESHOLDS = [
    (85, "A+", "Elite Edge"),
    (70, "A",  "Premium Edge"),
    (55, "B+", "Strong Edge"),
    (40, "B",  "Solid Edge"),
    (25, "C",  "Moderate Edge"),
    (10, "D",  "Marginal Edge"),
    (0,  "F",  "Weak Edge"),
]


def grade_edge_quality(ev_edge, true_ev_edge, book_count, is_god_mode,
                       kelly_fraction, vig_pct):
    """Assign an A-F quality grade to a Vegas Vault discrepancy.

    Combines multiple signals into a 0-100 quality score and maps it
    to a letter grade for quick visual decision-making.

    Score components (0-100):
        - EV edge magnitude (0-30): Larger raw edge = better
        - True edge magnitude (0-25): Devigged edge = real signal
        - Book confirmation (0-15): More books = more reliable
        - God Mode bonus (0-10): High-probability lock
        - Kelly sizing (0-10): Positive Kelly = real +EV
        - Vig penalty (0 to -10): High vig = noisy signal

    Parameters
    ----------
    ev_edge : float
        Raw EV edge percentage (7.0+).
    true_ev_edge : float
        Devigged true edge percentage.
    book_count : int
        Number of sportsbooks pricing this prop.
    is_god_mode : bool
        Whether any side has >= 60% implied probability.
    kelly_fraction : float
        Quarter-Kelly fraction (0.0 to 0.05).
    vig_pct : float
        Vig percentage on the recommended line.

    Returns
    -------
    dict
        {'grade': str, 'label': str, 'score': int}
    """
    try:
        # EV edge: 7% → 9, 10% → 15, 15% → 25, 20%+ → 30
        ev_score = min(30, max(0, int((float(ev_edge) - 5.0) * 2.0 + 5)))

        # True edge: 5% → 6, 8% → 15, 12% → 25
        true_score = min(25, max(0, int((float(true_ev_edge) - 3.0) * 3.0)))

        # Book confirmation: 1 → 5, 2 → 10, 3+ → 15
        book_score = min(15, max(0, int(book_count) * 5))

        # God Mode bonus
        god_bonus = 10 if is_god_mode else 0

        # Kelly sizing: 0 = 0, 0.01 → 4, 0.02 → 8, 0.025+ → 10
        kelly_score = min(10, int(float(kelly_fraction) * 400))

        # Vig penalty: 0% = 0, 5% = -5, 10%+ = -10
        vig_penalty = min(10, max(0, int(float(vig_pct))))

        total = ev_score + true_score + book_score + god_bonus + kelly_score - vig_penalty
        total = max(0, min(100, total))

        for threshold, grade, label in _GRADE_THRESHOLDS:
            if total >= threshold:
                return {"grade": grade, "label": label, "score": total}

        return {"grade": "F", "label": "Weak Edge", "score": 0}
    except (TypeError, ValueError):
        return {"grade": "?", "label": "Unknown", "score": 0}


# ============================================================
# END SECTION: Edge Quality Grade
# ============================================================

