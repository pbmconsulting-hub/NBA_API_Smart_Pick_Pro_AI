# ============================================================
# FILE: engine/stat_distributions.py
# PURPOSE: Proper statistical distributions for NBA stat modeling.
#          NBA stats are NOT normally distributed — this module
#          provides the correct distribution for each stat type.
#
#          Key insight: blocks, steals, turnovers are discrete counts
#          best modeled by Poisson. Points/rebounds/assists follow a
#          negative binomial (over-dispersed) distribution.
#          Three-pointers are zero-inflated Poisson or Poisson.
#
# CONNECTS TO: engine/simulation.py, engine/math_helpers.py
# CONCEPTS COVERED: Poisson distribution, negative binomial,
#                   distribution selection, over-dispersion
# ============================================================

# Standard library only — no numpy/scipy/pandas
import math

# ============================================================
# SECTION: Distribution Type Mapping
# ============================================================

# BEGINNER NOTE: Different NBA stats follow different statistical
# distributions. Using the wrong distribution leads to wrong probabilities.
# Poisson: good for rare events (0, 1, 2, 3...) like blocks and steals
# Normal (Gaussian): OK for high-count continuous-ish stats like points
# Negative Binomial: better than Poisson for over-dispersed counts

# Distribution types
DIST_POISSON = "poisson"
DIST_NORMAL = "normal"
DIST_NEGBINOM = "negbinom"  # Negative binomial (over-dispersed)

# Stat → distribution mapping
# These are based on published NBA analytics research:
# - Low-count discrete stats (steals, blocks, turnovers): Poisson
# - Three-pointers: Poisson/NegBin hybrid (zero-inflated)
# - Points, rebounds, assists: Normal or NegBin (both work well)
_STAT_DISTRIBUTION_MAP = {
    "steals":           DIST_POISSON,
    "stl":              DIST_POISSON,
    "blocks":           DIST_POISSON,
    "blk":              DIST_POISSON,
    "turnovers":        DIST_POISSON,
    "tov":              DIST_POISSON,
    "threes":           DIST_POISSON,   # FG3M — Poisson fits discrete shot counts
    "fg3m":             DIST_POISSON,
    "points":           DIST_NORMAL,    # Normal is acceptable for points
    "pts":              DIST_NORMAL,
    "rebounds":         DIST_NEGBINOM,  # Over-dispersed for bigs
    "reb":              DIST_NEGBINOM,
    "assists":          DIST_NORMAL,    # Guards are consistent, Normal works
    "ast":              DIST_NORMAL,
}

# Default distribution for unknown stats
_DEFAULT_DISTRIBUTION = DIST_NORMAL


def get_distribution_type(stat_type):
    """
    Return the recommended statistical distribution type for a given NBA stat.

    BEGINNER NOTE: Choosing the right distribution is critical for accurate
    probability estimates. Using a normal distribution for blocks (which
    average 0.5-1.5 per game) underestimates the probability of 0-block
    games and overestimates the chance of fractional blocks (impossible).

    Args:
        stat_type (str): Stat category, e.g. 'points', 'blocks', 'threes'

    Returns:
        str: One of DIST_POISSON, DIST_NORMAL, DIST_NEGBINOM

    Examples:
        get_distribution_type('blocks')    → 'poisson'
        get_distribution_type('points')    → 'normal'
        get_distribution_type('rebounds')  → 'negbinom'
    """
    if not stat_type:
        return _DEFAULT_DISTRIBUTION
    return _STAT_DISTRIBUTION_MAP.get(stat_type.lower(), _DEFAULT_DISTRIBUTION)


# ============================================================
# END SECTION: Distribution Type Mapping
# ============================================================


# ============================================================
# SECTION: Poisson Probability Functions
# ============================================================

def poisson_pmf(k, lam):
    """
    Poisson probability mass function: P(X = k) given rate lambda.

    BEGINNER NOTE: Poisson distribution models the number of times
    a rare event happens in a fixed period. Perfect for blocks (rare)
    and steals — "how many times does an event occur in a game?"

    Args:
        k (int): Number of events (must be >= 0)
        lam (float): Average rate (lambda), must be > 0

    Returns:
        float: Probability P(X = k)

    Example:
        # Player averages 1.2 blocks, what's P(exactly 2 blocks)?
        poisson_pmf(2, 1.2) ≈ 0.2169
    """
    if lam <= 0 or k < 0:
        # Poisson is only defined for lambda > 0 and k >= 0
        # A zero-rate Poisson has all probability mass at k=0, but that's
        # only a limit — for lambda=0 we return the degenerate case.
        return 1.0 if (k == 0 and lam == 0) else 0.0
    try:
        # P(k) = (e^-λ × λ^k) / k!
        log_prob = -lam + k * math.log(lam) - sum(math.log(i) for i in range(1, k + 1))
        return math.exp(log_prob)
    except (ValueError, OverflowError):
        return 0.0


def poisson_over_probability(line, lam):
    """
    P(X > line) using the Poisson distribution.

    Uses the complement: P(X > line) = 1 - P(X <= floor(line))

    BEGINNER NOTE: For a player averaging 1.5 steals (λ=1.5),
    what's the probability they get MORE than 1.5 steals?
    Since steals are integers, that means P(X >= 2).

    Args:
        line (float): The prop line (e.g., 1.5 steals)
        lam (float): Average rate (Poisson lambda)

    Returns:
        float: P(X > line), clamped to [0, 1]

    Example:
        poisson_over_probability(1.5, 1.8) ≈ 0.537
        (player averages 1.8 blocks, line is 1.5)
    """
    if lam <= 0:
        return 0.0 if line >= 0 else 1.0

    # P(X > line) = 1 - P(X <= floor(line))
    threshold = int(math.floor(line))

    # Sum P(X=0), P(X=1), ..., P(X=threshold)
    cumulative = 0.0
    for k in range(threshold + 1):
        cumulative += poisson_pmf(k, lam)

    return max(0.0, min(1.0, 1.0 - cumulative))


# ============================================================
# END SECTION: Poisson Probability Functions
# ============================================================


# ============================================================
# SECTION: Negative Binomial Distribution
# ============================================================

def negbinom_over_probability(line, mean, std):
    """
    P(X > line) using the Negative Binomial distribution.

    The Negative Binomial generalizes Poisson to allow over-dispersion
    (variance > mean). This is appropriate for rebounds and other stats
    where the variance exceeds what Poisson would predict.

    BEGINNER NOTE: In basketball, some nights a big gets 20 boards and
    some nights only 5 — more variance than Poisson predicts. The
    Negative Binomial adds a "dispersion" parameter to capture this.

    Parameterization:
        r = mean² / (variance - mean)   [number of successes]
        p = mean / variance              [probability of success]
        P(X > line) = 1 - CDF(floor(line))

    CDF is computed via the regularized incomplete beta function
    approximated using a continued fraction expansion.

    Args:
        line (float): The prop line
        mean (float): Player's projected mean for this stat
        std (float): Projected standard deviation

    Returns:
        float: P(X > line), clamped to [0, 1]

    Example:
        negbinom_over_probability(8.5, 9.2, 3.5) → ~0.58
    """
    if mean <= 0 or std <= 0:
        return 0.5

    variance = std * std

    # If variance ≤ mean, Negative Binomial isn't appropriate (use Poisson or Normal)
    # Fall back to a normal approximation in that case
    if variance <= mean:
        return _normal_over_probability(line, mean, std)

    # Negative binomial parameters
    # r = mean^2 / (var - mean), p = mean / var
    try:
        r = (mean * mean) / (variance - mean)
        p = mean / variance  # probability parameter (0 < p < 1)

        if r <= 0 or p <= 0 or p >= 1:
            return _normal_over_probability(line, mean, std)

        r = max(0.1, min(1000.0, r))
        p = max(0.001, min(0.999, p))

    except (ValueError, ZeroDivisionError):
        return _normal_over_probability(line, mean, std)

    # Compute P(X <= floor(line)) via cumulative NegBinomial PMF
    threshold = int(math.floor(max(-1, line)))

    cumulative = 0.0
    max_k = max(threshold + 1, int(mean + 10 * std))

    for k in range(min(threshold + 1, max_k + 1)):
        try:
            # P(X=k) for NegBin(r, p): uses log-gamma for stability
            log_binom_coeff = _log_gamma(r + k) - _log_gamma(r) - _log_gamma(k + 1)
            log_pmf = log_binom_coeff + r * math.log(p) + k * math.log(1.0 - p)
            cumulative += math.exp(log_pmf)
        except (ValueError, OverflowError):
            break

    return max(0.0, min(1.0, 1.0 - cumulative))


def _log_gamma(x):
    """
    Compute log(Gamma(x)) using Stirling's approximation for large x,
    or direct computation for small x.

    BEGINNER NOTE: Gamma(n) = (n-1)! for integers, so this lets us
    compute log-factorials without overflow.

    Args:
        x (float): Positive real number

    Returns:
        float: log(Gamma(x))
    """
    if x <= 0:
        raise ValueError("log_gamma requires positive argument")
    if x < 0.5:
        # Use reflection formula: Gamma(x)*Gamma(1-x) = pi/sin(pi*x)
        return math.log(math.pi / math.sin(math.pi * x)) - _log_gamma(1.0 - x)
    # Lanczos approximation coefficients (g=7, n=9)
    g = 7
    c = [
        0.99999999999980993,
        676.5203681218851,
        -1259.1392167224028,
        771.32342877765313,
        -176.61502916214059,
        12.507343278686905,
        -0.13857109526572012,
        9.9843695780195716e-6,
        1.5056327351493116e-7,
    ]
    x -= 1
    base = x + g + 0.5
    s = c[0] + sum(c[i] / (x + i) for i in range(1, len(c)))
    return 0.5 * math.log(2 * math.pi) + (x + 0.5) * math.log(base) - base + math.log(s)


def _normal_over_probability(line, mean, std):
    """
    Compute P(X > line) for a normal distribution (fallback).

    Args:
        line (float): The prop line
        mean (float): Distribution mean
        std (float): Distribution standard deviation

    Returns:
        float: P(X > line), clamped to [0, 1]
    """
    if std <= 0:
        return 1.0 if mean > line else 0.0
    z = (line - mean) / std
    return max(0.0, min(1.0, 1.0 - 0.5 * (1.0 + math.erf(z / math.sqrt(2.0)))))


# ============================================================
# END SECTION: Negative Binomial Distribution
# ============================================================


# ============================================================
# SECTION: Unified Over-Probability Function
# ============================================================

def get_over_probability(mean, std, line, stat_type):
    """
    Calculate P(stat > line) using the appropriate distribution for the stat type.

    Automatically selects between Normal, Poisson, and Negative Binomial
    based on the stat type using `get_distribution_type()`.

    BEGINNER NOTE: This is the main function to call. It handles the
    distribution selection automatically — just pass the stat name and
    let the engine figure out which math to use.

    Args:
        mean (float): Player's projected mean for this stat
        std (float): Projected standard deviation (used for Normal/NegBin)
        line (float): The prop line (e.g., 1.5 steals, 24.5 points)
        stat_type (str): Stat name: 'points', 'blocks', 'rebounds', etc.

    Returns:
        float: P(X > line), clamped to [0.01, 0.99]

    Examples:
        get_over_probability(1.8, 1.2, 1.5, 'blocks')    → Poisson path
        get_over_probability(25.0, 5.5, 24.5, 'points')   → Normal path
        get_over_probability(9.2, 3.5, 8.5, 'rebounds')   → NegBinom path
    """
    if mean <= 0:
        return 0.01  # Can't go over a positive line with 0 mean

    dist = get_distribution_type(stat_type)

    if dist == DIST_POISSON:
        prob = poisson_over_probability(line, mean)
    elif dist == DIST_NEGBINOM:
        prob = negbinom_over_probability(line, mean, std)
    else:
        # Normal distribution
        prob = _normal_over_probability(line, mean, std)

    # Clamp to prevent extreme values
    return max(0.01, min(0.99, prob))


# ============================================================
# END SECTION: Unified Over-Probability Function
# ============================================================


# ============================================================
# SECTION: Distribution Statistics
# ============================================================

def get_distribution_mean_and_variance(mean, std, stat_type):
    """
    Return the (theoretical_mean, theoretical_variance) for a stat's
    chosen distribution, given the input mean and std.

    Useful for diagnostics and model validation.

    Args:
        mean (float): Projected mean for this stat
        std (float): Projected standard deviation
        stat_type (str): Stat category

    Returns:
        tuple: (mean, variance) — both floats
    """
    dist = get_distribution_type(stat_type)
    variance = std * std if std > 0 else mean  # Poisson: variance = mean

    if dist == DIST_POISSON:
        # Poisson: E[X] = λ, Var[X] = λ
        return (mean, mean)
    elif dist == DIST_NEGBINOM:
        # NegBinom: E[X] = r*(1-p)/p, Var[X] = r*(1-p)/p^2
        # We just return the provided mean and std^2 (they define our NegBin)
        return (mean, variance)
    else:
        # Normal
        return (mean, variance)


# ============================================================
# END SECTION: Distribution Statistics
# ============================================================
