# ============================================================
# FILE: engine/math_helpers.py
# PURPOSE: All mathematical and statistical functions needed
#          by the SmartBetPro NBA engine — built from scratch using
#          ONLY Python's standard library (math, statistics).
#          No numpy, no scipy, no pandas.
# CONNECTS TO: simulation.py, projections.py, confidence.py,
#              edge_detection.py
# CONCEPTS COVERED: Normal distribution, Poisson distribution,
#                   standard deviation, z-scores, percentiles,
#                   probability calculations
# ============================================================

# Import only standard-library modules (these ship with Python)
import math        # math.sqrt, math.exp, math.pi, math.erf, etc.
import statistics  # statistics.mean, statistics.stdev
import random      # random.gauss for Quantum Matrix Engine 5.6 sampling


# ============================================================
# SECTION: Safe Float Conversion
# ============================================================

def _safe_float(value, fallback=0.0):
    """Return *value* as a finite float, or *fallback* if NaN/inf/non-numeric."""
    try:
        v = float(value)
        if math.isfinite(v):
            return v
        return float(fallback)
    except (ValueError, TypeError):
        return float(fallback)


# ============================================================
# SECTION: Normal Distribution Helpers
# A normal distribution (bell curve) is the most common
# statistical shape. We use it to model player stat variability.
# ============================================================

def calculate_normal_cdf(value, mean, standard_deviation):
    """
    Calculate the probability that a normally-distributed
    random variable is LESS THAN OR EQUAL TO `value`.

    This is the "cumulative distribution function" (CDF).
    Think of it as: given a player averages 25 points with
    some spread, what % of games do they score <= 24.5?

    Args:
        value (float): The threshold we're checking (e.g., 24.5)
        mean (float): The average (center of the bell curve)
        standard_deviation (float): How spread out the curve is

    Returns:
        float: Probability between 0.0 and 1.0

    Example:
        If LeBron averages 24.8 pts with std 6.2, and the line
        is 24.5, then P(score <= 24.5) ≈ 0.48 (just under 50%)
    """
    # Guard against zero or negative standard deviation
    # (A player can't have zero variability — games differ)
    if standard_deviation <= 0:
        # If no variability, either certainly under or certainly over
        if value >= mean:
            return 1.0  # Always under or at line
        else:
            return 0.0  # Always over

    # BEGINNER NOTE: The z-score tells us how many standard
    # deviations away from the mean our value is.
    # z = 0  means value == mean (50% probability)
    # z = 1  means value is 1 std above mean (~84% probability)
    # z = -1 means value is 1 std below mean (~16% probability)
    z_score = (value - mean) / standard_deviation

    # BEGINNER NOTE: math.erf is the "error function" — a
    # mathematical tool that lets us compute normal probabilities
    # using only Python's math module. The formula below converts
    # z-score to a probability (0 to 1).
    # This is equivalent to scipy.stats.norm.cdf(value, mean, std)
    probability = 0.5 * (1.0 + math.erf(z_score / math.sqrt(2.0)))

    return probability


def calculate_probability_over_line(mean, standard_deviation, line):
    """
    Calculate the probability that a player EXCEEDS a prop line.

    This is the core of our prediction: "What is the chance
    LeBron scores MORE THAN 24.5 points tonight?"

    Args:
        mean (float): Player's projected stat average
        standard_deviation (float): Variability of that stat
        line (float): The prop line to beat

    Returns:
        float: Probability (0.0 to 1.0) of going OVER the line

    Example:
        LeBron projects 25.8 pts with std 6.2, line is 24.5
        → returns ~0.58 (58% chance to go over)
    """
    # P(over) = 1 - P(under or equal)
    # Because all probabilities must sum to 1
    probability_under = calculate_normal_cdf(line, mean, standard_deviation)
    probability_over = 1.0 - probability_under

    return probability_over


# ============================================================
# END SECTION: Normal Distribution Helpers
# ============================================================


# ============================================================
# SECTION: Poisson Distribution Helpers
# Poisson models count events (like assists or steals) that
# happen with a known average rate. Great for low-count stats.
# ============================================================

def calculate_poisson_probability(count, average_rate):
    """
    Calculate the probability of exactly `count` events
    given an average rate, using the Poisson distribution.

    Args:
        count (int): The exact number of events (e.g., 3 assists)
        average_rate (float): Average events per game (e.g., 5.6)

    Returns:
        float: Probability of exactly `count` events

    Example:
        If a player averages 2.1 steals, what's P(exactly 3)?
        → calculate_poisson_probability(3, 2.1) ≈ 0.189
    """
    # Guard: count must be a non-negative integer
    if count < 0:
        return 0.0
    if average_rate <= 0:
        # If average is 0, only P(0 events) = 1, everything else = 0
        return 1.0 if count == 0 else 0.0

    # BEGINNER NOTE: The Poisson formula is:
    # P(k events) = (e^(-λ) * λ^k) / k!
    # Where λ (lambda) = average_rate, k = count
    # math.factorial(k) computes k! (e.g., 5! = 120)
    try:
        probability = (
            (math.exp(-average_rate) * (average_rate ** count))
            / math.factorial(count)
        )
    except OverflowError:
        # For very large counts, factorial overflows → probability ≈ 0
        probability = 0.0

    return probability


def calculate_poisson_over_probability(line, average_rate):
    """
    Calculate the probability of exceeding `line` using Poisson.

    Uses the complement method: P(X > line) = 1 - P(X <= floor(line)).
    Summing from 0 to floor(line) is more efficient and numerically stable
    than summing the upper tail, especially when line < average_rate (fewer
    terms) and avoids the need for an arbitrary upper ceiling cutoff.

    Args:
        line (float): The prop line (e.g., 4.5 assists)
        average_rate (float): Player's average for this stat

    Returns:
        float: Probability of exceeding the line (clamped to [0.0, 1.0])

    Example:
        If a player averages 5.0 assists, what's P(X > 4.5)?
        → calculate_poisson_over_probability(4.5, 5.0) ≈ 0.559
    """
    # Guard: degenerate rate → only X = 0 is possible
    # P(X > line) = 1.0 if line < 0, else 0.0
    if average_rate <= 0:
        return 1.0 if line < 0 else 0.0

    # Complement method: P(X > threshold) = 1 - P(X <= threshold)
    # where threshold = floor(line)
    # BEGINNER NOTE: Summing 0..threshold (the UNDER side) is faster when
    # line < mean, and avoids choosing an arbitrary upper ceiling.
    threshold = math.floor(line)

    # For negative lines, threshold < 0, so range is empty → P(X <= -1) = 0
    # → P(X > -1) = 1.0, which is correct since all Poisson values are >= 0.
    cumulative_under = 0.0
    for k in range(threshold + 1):
        cumulative_under += calculate_poisson_probability(k, average_rate)

    return max(0.0, min(1.0, 1.0 - cumulative_under))


# ============================================================
# END SECTION: Poisson Distribution Helpers
# ============================================================


# ============================================================
# SECTION: Descriptive Statistics
# Functions to summarize a list of numbers (e.g., simulation
# results or historical game logs).
# ============================================================

def calculate_mean(numbers_list):
    """
    Calculate the arithmetic mean (average) of a list of numbers.

    Args:
        numbers_list (list of float): The numbers to average

    Returns:
        float: The mean, or 0.0 if the list is empty

    Example:
        calculate_mean([20, 25, 30, 15]) → 22.5
    """
    if not numbers_list:
        return 0.0  # Avoid division by zero on empty list

    # Add up all values and divide by the count
    total = sum(numbers_list)
    count = len(numbers_list)
    return total / count


def calculate_standard_deviation(numbers_list):
    """
    Calculate how spread out a list of numbers is.
    A higher std means more variability (unpredictable player).

    Uses the sample standard deviation formula (divides by N-1)
    which is more accurate for small sample sizes.

    Args:
        numbers_list (list of float): Data points (game scores)

    Returns:
        float: Standard deviation, or 0.0 if fewer than 2 values

    Example:
        calculate_standard_deviation([20, 25, 30, 15]) → 6.45
    """
    if len(numbers_list) < 2:
        return 0.0  # Need at least 2 data points for variability

    # Use Python's built-in statistics module for accuracy
    # statistics.stdev uses the sample formula (N-1 denominator)
    return statistics.stdev(numbers_list)


def calculate_percentile(numbers_list, percentile):
    """
    Find the value at a given percentile in a sorted list.

    Args:
        numbers_list (list of float): Unsorted data points
        percentile (float): 0-100, e.g., 25 = 25th percentile

    Returns:
        float: The value at that percentile

    Example:
        calculate_percentile([10,20,30,40,50], 25) → 20
        (position = (25/100)*(5-1) = 1.0 → exact index 1 → sorted[1] = 20)
        calculate_percentile([10,20,30,40,50], 40) → 26
        (position = 1.6, interpolates: 20*0.4 + 30*0.6 = 26)

    Edge case — single-element list:
        When the list has exactly 1 element, position = 0 for ALL
        percentile values (0th through 100th), so both
        calculate_percentile([42], 0) and calculate_percentile([42], 100)
        return 42. This is mathematically correct: for a single-point
        distribution every percentile is that one value. Do NOT
        "fix" this behaviour — it is intentional.
    """
    if not numbers_list:
        return 0.0  # Nothing to compute

    # Sort a copy (don't modify the original list)
    sorted_list = sorted(numbers_list)
    total_count = len(sorted_list)

    # Calculate the exact position in the sorted list
    # BEGINNER NOTE: index = (percentile / 100) * (N - 1)
    # This gives a fractional position we can interpolate between
    position = (percentile / 100.0) * (total_count - 1)

    # Get the integer index below and above our position
    lower_index = int(math.floor(position))
    upper_index = int(math.ceil(position))

    # If they're the same (exact integer position), return that value
    if lower_index == upper_index:
        return sorted_list[lower_index]

    # Otherwise, interpolate (average) between the two surrounding values
    # The fraction tells us how far between the two we are
    fraction = position - lower_index
    interpolated_value = (
        sorted_list[lower_index] * (1.0 - fraction)
        + sorted_list[upper_index] * fraction
    )
    return interpolated_value


def calculate_median(numbers_list):
    """
    Find the middle value of a list (50th percentile).
    Less sensitive to outliers than the mean.

    Args:
        numbers_list (list of float): Data points

    Returns:
        float: The median value
    """
    return calculate_percentile(numbers_list, 50)


# ============================================================
# END SECTION: Descriptive Statistics
# ============================================================


# ============================================================
# SECTION: Edge and Probability Utilities
# These helpers translate raw probabilities into
# meaningful edge percentages and labels.
# ============================================================

def calculate_edge_percentage(probability_over, implied_probability=None):
    """
    Convert a probability (0-1) into an "edge" value showing
    how much better than random our prediction is vs the market.

    When implied_probability is provided (e.g. 0.5238 for -110 odds),
    edge = (probability - implied_probability) * 100.
    When not provided, defaults to 0.5238 (the -110 breakeven standard).

    BEGINNER NOTE: At -110 odds (standard American bet), you need to win
    52.38% of the time just to break even. So the real edge is your
    probability minus 52.38%, not minus 50%.

    Args:
        probability_over (float): Probability of going over (0-1)
        implied_probability (float or None): Market implied probability.
            If None, defaults to 0.5238 (breakeven for -110 odds).

    Returns:
        float: Edge in percentage points

    Examples:
        calculate_edge_percentage(0.63)          → +10.62% (vs -110 breakeven)
        calculate_edge_percentage(0.63, 0.5238)  → +10.62% (explicit -110 baseline)
        calculate_edge_percentage(0.63, 0.50)    → +13.0%  (vs 50/50 baseline)
    """
    baseline = implied_probability if implied_probability is not None else 0.5238
    edge = (probability_over - baseline) * 100.0
    return edge


# Platform-specific implied probability baselines.
# BEGINNER NOTE: Standard sportsbooks have -110 vig on individual legs,
# giving a 52.38% breakeven. DFS platforms (PrizePicks, Underdog) have
# NO per-leg vig — the house edge is baked into the multi-leg payout table.
_PLATFORM_BASELINE_PROBS = {
    "fanduel":          0.5238,
    "draftkings":       0.5238,
    "betmgm":           0.5238,
    "caesars":          0.5238,
    "fanatics":         0.5238,
    "espn bet":         0.5238,
    "hard rock bet":    0.5238,
    "betrivers":        0.5238,
    "dk":               0.5238,
    # Backward-compat DFS entries
    "prizepicks":       0.50,
    "prize picks":      0.50,
    "underdog":         0.50,
    "underdog fantasy": 0.50,
}
_DEFAULT_PLATFORM_BASELINE = 0.5238  # Conservative default (DraftKings-style)


def calculate_platform_edge_percentage(probability_over, platform=None, over_odds=None):
    """
    Calculate edge percentage using the correct baseline for each platform.

    BEGINNER NOTE: On PrizePicks/Underdog, the breakeven is 50% (no juice
    on individual legs). On DraftKings it's 52.38% (-110). Using the wrong
    baseline gives you wrong edge numbers.

    Args:
        probability_over (float): Model's P(over), 0-1
        platform (str or None): Platform name ('PrizePicks', 'DraftKings', etc.)
            When None, defaults to -110 (-52.38%) breakeven.
        over_odds (float or None): Actual American odds from DraftKings.
            When provided, overrides the platform baseline with the true
            implied probability derived from the actual odds.

    Returns:
        float: Edge in percentage points

    Examples:
        calculate_platform_edge_percentage(0.63, 'FanDuel') → +10.62% (vs 52.38%)
        calculate_platform_edge_percentage(0.63, 'DraftKings') → +10.62% (vs 52.38%)
        calculate_platform_edge_percentage(0.63, 'DraftKings', -120) → true edge vs -120
    """
    # If actual odds are provided, use the precise implied probability
    if over_odds is not None:
        try:
            odds_float = float(over_odds)
            if odds_float < 0:
                baseline = abs(odds_float) / (abs(odds_float) + 100.0)
            elif odds_float + 100.0 > 0:
                baseline = 100.0 / (odds_float + 100.0)
            else:
                baseline = _DEFAULT_PLATFORM_BASELINE
        except (ValueError, TypeError):
            baseline = _DEFAULT_PLATFORM_BASELINE
    elif platform:
        baseline = _PLATFORM_BASELINE_PROBS.get(platform.lower().strip(), _DEFAULT_PLATFORM_BASELINE)
    else:
        baseline = _DEFAULT_PLATFORM_BASELINE

    return (probability_over - baseline) * 100.0


def probability_standard_error(p, n_simulations):
    """
    Compute the standard error of an estimated probability.

    BEGINNER NOTE: When we run 2000 simulations and 1200 go over the line
    (60% probability), the true probability has uncertainty around it.
    The standard error tells us how precise that 60% estimate is.
    With more simulations, the SE gets smaller (more precise).

    Formula: SE = sqrt(p * (1 - p) / n)

    Args:
        p (float): Estimated probability (0-1)
        n_simulations (int): Number of simulations run

    Returns:
        float: Standard error (e.g. 0.011 means ±1.1% uncertainty)

    Example:
        probability_standard_error(0.60, 2000) → ~0.011 (±1.1%)
        probability_standard_error(0.60, 500)  → ~0.022 (±2.2%)
    """
    if n_simulations <= 0 or p <= 0 or p >= 1:
        return 0.0
    return math.sqrt(p * (1.0 - p) / n_simulations)


def probability_confidence_interval(p, n, alpha=0.05):
    """
    Compute the Wilson score confidence interval for a probability estimate.

    The Wilson interval is preferred over the naive normal approximation
    because it remains accurate even for extreme probabilities (near 0 or 1)
    where the normal approximation breaks down.

    BEGINNER NOTE: If our simulation says 60% probability, the Wilson
    interval gives us a range like [58%, 62%] at 95% confidence.
    A wider interval means we're less certain about the exact probability.

    Args:
        p (float): Estimated probability (0-1)
        n (int): Number of observations (simulations run)
        alpha (float): Significance level. Default 0.05 → 95% CI.

    Returns:
        tuple: (lower, upper) — the 95% confidence interval bounds

    Example:
        probability_confidence_interval(0.60, 2000) → (~0.578, ~0.622)
    """
    if n <= 0 or p < 0 or p > 1:
        return (max(0.0, p - 0.05), min(1.0, p + 0.05))

    # z-score for alpha/2 (1.96 for alpha=0.05)
    # BEGINNER NOTE: 1.96 corresponds to the 97.5th percentile of the
    # standard normal distribution (the two-tail 95% confidence level).
    # We hardcode this to avoid needing scipy.stats.
    if alpha <= 0.01:
        z = 2.576  # 99% CI
    elif alpha <= 0.05:
        z = 1.960  # 95% CI
    elif alpha <= 0.10:
        z = 1.645  # 90% CI
    else:
        z = 1.282  # 80% CI

    z2 = z * z
    n_float = float(n)
    p_float = float(p)

    # Wilson score interval formula
    # center = (p + z²/2n) / (1 + z²/n)
    # half_width = z * sqrt(p(1-p)/n + z²/4n²) / (1 + z²/n)
    denom = 1.0 + z2 / n_float
    center = (p_float + z2 / (2.0 * n_float)) / denom
    half_width = (z / denom) * math.sqrt(
        (p_float * (1.0 - p_float) / n_float) + (z2 / (4.0 * n_float * n_float))
    )

    lower = max(0.0, center - half_width)
    upper = min(1.0, center + half_width)
    return (round(lower, 6), round(upper, 6))


def clamp_probability(probability):
    """
    Ensure a probability stays between 0.01 and 0.99.
    We never want to say something is 100% certain.

    Args:
        probability (float): Any probability value

    Returns:
        float: Clamped between 0.01 and 0.99
    """
    return max(0.01, min(0.99, probability))


def round_to_decimal(value, decimal_places):
    """
    Round a number to a specified number of decimal places.

    Args:
        value (float): Number to round
        decimal_places (int): How many decimal places to keep

    Returns:
        float: Rounded value

    Example:
        round_to_decimal(3.14159, 2) → 3.14
    """
    multiplier = 10 ** decimal_places
    return math.floor(value * multiplier + 0.5) / multiplier


def sample_from_normal_distribution(mean, standard_deviation):
    """
    Draw a single random sample from a normal distribution.
    Used in Quantum Matrix Engine 5.6 simulation to simulate one game's result.

    Args:
        mean (float): Center of the distribution
        standard_deviation (float): Spread of the distribution

    Returns:
        float: A random value, most likely near the mean

    Example:
        If mean=25.0 and std=6.0, might return 22.3 or 28.7
        Most results will be within 1-2 standard deviations
    """
    # Guard against invalid std
    if standard_deviation <= 0:
        return mean

    # random.gauss draws from a normal distribution
    # BEGINNER NOTE: This is the core randomness of Quantum Matrix Engine 5.6!
    # Each call gives a different number, simulating one game
    raw_sample = random.gauss(mean, standard_deviation)

    # Stats can't be negative (can't score -5 points)
    return max(0.0, raw_sample)


def sample_skew_normal(mean, standard_deviation, skew_param=0.0):
    """
    Draw a single random sample from a skew-normal distribution.

    NBA stats are right-skewed (hard floor at 0, occasional explosion games
    that pull the distribution right). Standard normal sampling underestimates
    the probability of big games and overestimates the floor.

    This implementation uses the standard composition method:
        1. Draw two independent standard normals u, v
        2. If skew_param == 0, return the standard normal (reduces to Gaussian)
        3. Else, compute the skew-normal variate using the sign trick

    Reference: Azzalini (1985) — "A class of distributions which includes
    the normal ones", Scandinavian Journal of Statistics.

    Args:
        mean (float): Center of the distribution (projected stat average)
        standard_deviation (float): Spread of the distribution
        skew_param (float): Skewness parameter α.
            0.0  = symmetric normal (no skew)
            > 0  = right skew (long tail toward larger values — NBA typical)
            < 0  = left skew (rare for sports stats)
            Practical range for NBA: 0.5–1.5

    Returns:
        float: A random sample, clamped to ≥ 0.0 (stats can't be negative)

    Example:
        Points: sample_skew_normal(22.0, 5.5, skew_param=0.8)
        Threes: sample_skew_normal(2.5, 1.5, skew_param=1.2)
    """
    if standard_deviation <= 0:
        return max(0.0, mean)

    if abs(skew_param) < 1e-9:
        # No skew: fall back to standard normal
        return max(0.0, random.gauss(mean, standard_deviation))

    # Skew-normal composition:
    # delta = skew_param / sqrt(1 + skew_param^2)
    # Compute two independent standard normals
    delta = skew_param / math.sqrt(1.0 + skew_param * skew_param)
    u0 = random.gauss(0.0, 1.0)
    v  = random.gauss(0.0, 1.0)

    # The skew-normal variate z (zero mean, unit variance):
    z = delta * abs(u0) + math.sqrt(1.0 - delta * delta) * v

    # Scale to the desired mean and std
    # Note: The skew-normal mean is mu + omega * delta * sqrt(2/pi)
    # We shift so that the output is centred at `mean` exactly.
    skew_mean_shift = delta * math.sqrt(2.0 / math.pi)
    raw_sample = mean + standard_deviation * (z - skew_mean_shift)

    return max(0.0, raw_sample)


# Default skew parameters by NBA stat type (C5)
# These reflect the right-skewed nature of each stat category.
# Higher skew_param = more right-skewed = more explosion-game probability.
STAT_SKEW_PARAMS = {
    "points":    0.8,   # Moderate right skew (star can always go nuclear)
    "rebounds":  0.6,   # Moderate skew (rebounding is position-dependent)
    "assists":   0.5,   # Mild skew (playmakers are somewhat consistent)
    "threes":    1.2,   # Highly right-skewed (shooting is very streaky)
    "steals":    1.0,   # Right-skewed (rare stat, can spike)
    "blocks":    1.0,   # Right-skewed (rare stat, can spike)
    "turnovers": 0.4,   # Mild skew (more predictable for high-usage players)
}


def get_stat_skew_param(stat_type):
    """
    Return the default skew parameter for a given stat type. (C5)

    Args:
        stat_type (str): e.g. 'points', 'threes', 'rebounds'

    Returns:
        float: Skew parameter α for sample_skew_normal()
    """
    return STAT_SKEW_PARAMS.get(stat_type.lower() if stat_type else "", 0.6)


# ============================================================
# SECTION: KDE Sampling from Game Logs (C11)
# When a player has 15+ recent game log entries, build a
# non-parametric Kernel Density Estimate and sample from it
# instead of using the parametric skew-normal distribution.
# This captures player-specific distribution shapes — e.g., a
# player who tends to score either 15 or 30 but never 22.
# ============================================================

# Minimum game log entries required to use KDE instead of skew-normal
KDE_MIN_GAME_LOGS = 15


def _kde_bandwidth(data):
    """
    Estimate KDE bandwidth using Silverman's rule of thumb.

    Bandwidth h = 1.06 * std * n^(-1/5)
    This is a standard automatic bandwidth selector for unimodal data.

    Args:
        data (list of float): Sample data points

    Returns:
        float: Bandwidth h (always > 0)
    """
    n = len(data)
    if n < 2:
        return 1.0
    try:
        std = statistics.stdev(data)
    except statistics.StatisticsError:
        std = 1.0
    if std < 1e-6:
        return 0.5
    return 1.06 * std * (n ** (-0.2))


def sample_from_kde(game_log_values, bandwidth=None):
    """
    Draw a single sample from a Kernel Density Estimate built from game logs. (C11)

    Algorithm (simple KDE sampling):
    1. Pick a random data point from the game logs (uniform selection)
    2. Add Gaussian noise with std = bandwidth
    3. Clamp result to >= 0.0 (stats can't be negative)

    This preserves the empirical distribution shape — if a player has a
    bimodal distribution (either goes big or goes quiet), the KDE captures
    that, unlike a parametric normal/skew-normal which would smooth it out.

    Args:
        game_log_values (list of float): Recent game stat values
            e.g., [22.0, 15.0, 31.0, 8.0, 27.0, ...]
            Must have at least KDE_MIN_GAME_LOGS (15) entries.
        bandwidth (float, optional): KDE bandwidth (kernel width).
            When None, computed automatically via Silverman's rule.

    Returns:
        float: A sampled value >= 0.0, or None if insufficient data.

    Example:
        Player has recent logs [8, 15, 32, 11, 28, 14, 9, 33, 12, 27, 10, 30, 13, 25, 16]
        → sample_from_kde(...) might return ~10.8 or ~29.3 (bimodal-shaped)
    """
    if not game_log_values or len(game_log_values) < KDE_MIN_GAME_LOGS:
        return None  # Caller should fall back to skew-normal

    if bandwidth is None:
        bandwidth = _kde_bandwidth(game_log_values)

    # Pick a random kernel center from the data, then perturb
    center = random.choice(game_log_values)
    noise = random.gauss(0.0, bandwidth)
    return max(0.0, center + noise)


def should_use_kde(game_log_values):
    """
    Return True if there are enough game logs to use KDE sampling. (C11)

    Args:
        game_log_values (list of float): Recent game stat values

    Returns:
        bool: True if len(game_log_values) >= KDE_MIN_GAME_LOGS
    """
    return bool(game_log_values) and len(game_log_values) >= KDE_MIN_GAME_LOGS

# ============================================================
# END SECTION: KDE Sampling from Game Logs
# ============================================================


# ============================================================
# END SECTION: Edge and Probability Utilities
# ============================================================


# ============================================================
# SECTION: Flex EV and Correlation Helpers (W2, W9)
# Additional math utilities for the entry optimizer.
# ============================================================

def calculate_flex_ev(pick_probabilities, payout_table, entry_fee):
    """
    Calculate the expected value (EV) of a flex-play entry. (W9)

    Convenience wrapper around the full EV calculation, returning
    just the essential numbers for quick comparison across entries.

    Args:
        pick_probabilities (list of float): Win probability for each pick (0-1)
        payout_table (dict): {hits: multiplier} for this entry size
        entry_fee (float): Dollar amount bet

    Returns:
        dict: {
            'ev_dollars': float (net EV),
            'roi': float (net EV / entry_fee),
            'all_hit_prob': float (P of hitting all picks),
            'prob_at_least_one_miss': float (P of missing at least one)
        }

    Example:
        6 picks at 62% each, $10 entry → ev_dollars, roi, all-hit-prob
    """
    import itertools as _itertools

    n = len(pick_probabilities)
    if n == 0:
        return {"ev_dollars": 0.0, "roi": 0.0, "all_hit_prob": 0.0,
                "prob_at_least_one_miss": 1.0}

    # --- Probability of exactly k hits ---
    prob_for_k = {}
    for k in range(n + 1):
        total_p = 0.0
        for winning_idx in _itertools.combinations(range(n), k):
            winning_set = set(winning_idx)
            combo_p = 1.0
            for i in range(n):
                combo_p *= (pick_probabilities[i] if i in winning_set
                            else 1.0 - pick_probabilities[i])
            total_p += combo_p
        prob_for_k[k] = total_p

    # --- Expected return ---
    total_return = sum(prob_for_k[k] * payout_table.get(k, 0.0) * entry_fee
                       for k in range(n + 1))
    net_ev = total_return - entry_fee
    roi = net_ev / entry_fee if entry_fee > 0 else 0.0

    # --- All-hit probability ---
    all_hit_prob = prob_for_k.get(n, 0.0)

    return {
        "ev_dollars": round(net_ev, 2),
        "roi": round(roi, 4),
        "all_hit_prob": round(all_hit_prob, 6),
        "prob_at_least_one_miss": round(1.0 - all_hit_prob, 6),
    }


def calculate_correlation_discount(same_game_pick_count):
    """
    Compute the EV discount multiplier for correlated same-game picks. (W2)

    When picks share a game, their outcomes are correlated — a blowout
    or overtime hit affects ALL legs simultaneously.

    Args:
        same_game_pick_count (int): How many picks from the same game

    Returns:
        float: Multiplier to apply to entry EV (1.0 = no discount)

    Example:
        2 picks same game → 0.95 (5% penalty)
        3+ picks same game → 0.88 (12% penalty)
    """
    if same_game_pick_count <= 1:
        return 1.0   # No correlation discount
    elif same_game_pick_count == 2:
        return 0.95  # 5% penalty — moderate correlation
    else:
        return 0.88  # 12% penalty — high correlation (3+ same-game legs)

# ============================================================
# END SECTION: Flex EV and Correlation Helpers
# ============================================================


# ============================================================
# SECTION: Stat-Specific Distribution Samplers (Feature 8)
# Poisson-like sampler for low-count discrete stats (steals,
# blocks, turnovers) and a zero-inflated sampler for three-
# pointers — both built from first principles, no scipy.
# ============================================================

def sample_poisson_like(mean, game_log_values=None):
    """
    Sample from a Poisson-like discrete distribution for low-count stats.

    Used for steals, blocks, and turnovers — stats where the outcome is
    discrete (0, 1, 2, 3...) and the Poisson distribution models the
    zero-inflated count nature better than a continuous normal.

    Implements the inverse CDF method using cumulative Poisson probabilities
    computed from first principles (no scipy needed).

    Args:
        mean (float): Expected value (lambda parameter for Poisson).
        game_log_values (list of float, optional): Historical game log for
            empirical calibration. If provided and mean deviates significantly
            from log mean, the log mean takes precedence.

    Returns:
        float: A sampled integer value (as float) ≥ 0.

    Example:
        # Player averages 1.2 steals per game
        sample_poisson_like(1.2) → might return 0.0, 1.0, 2.0, 3.0, etc.
    """
    # Use empirical mean from game logs when enough history is available
    if game_log_values and len(game_log_values) >= 10:
        lam = sum(game_log_values) / len(game_log_values)
    else:
        lam = float(mean)

    # Clamp lambda to a sensible range (Poisson requires λ > 0)
    lam = max(0.1, min(20.0, lam))

    # Build cumulative Poisson CDF using calculate_poisson_probability()
    # We go up to max_k to ensure the CDF reaches ≈ 1.0
    max_k = max(20, int(lam * 3) + 10)
    cdf = 0.0
    u = random.random()

    for k in range(max_k + 1):
        cdf += calculate_poisson_probability(k, lam)
        if cdf >= u:
            return float(k)

    # Fallback: return max_k if u never crossed (numerical edge case)
    return float(max_k)


def sample_zero_inflated(mean, std, zero_probability, game_log_values=None):
    """
    Sample from a zero-inflated distribution for three-pointers.

    Three-pointers have a "zero mass" component — many games with 0 threes,
    then a right-skewed tail of high-three games. This models that structure.

    With probability zero_probability, returns 0 (zero-inflation component).
    Otherwise samples from a shifted distribution representing active-three-game.

    Args:
        mean (float): Overall mean (including zero games).
        std (float): Overall standard deviation (including zero games).
        zero_probability (float): Fraction of games with 0 of this stat.
        game_log_values (list of float, optional): Historical game log values.

    Returns:
        float: Sampled stat value ≥ 0 (rounded to nearest 0.5).

    Example:
        # Player averages 2.3 threes, has 0 threes in 20% of games
        sample_zero_inflated(2.3, 1.8, 0.20) → 0, 1.5, 2.0, 3.0, 4.5, etc.
    """
    # Zero-inflation component: with probability zero_probability, player has 0
    if random.random() < zero_probability:
        return 0.0

    # Non-zero game: shift the mean upward to reflect conditional distribution
    # E[X | X > 0] = mean / P(X > 0)
    conditional_mean = mean / max(0.001, 1.0 - zero_probability)

    # Right-skewed draw for the active-three-pointer game
    raw = sample_skew_normal(conditional_mean, std * 1.1, skew_param=2.0)

    # Round to nearest 0.5 (threes are whole numbers; 0.5 granularity is fine)
    # Clamp to >= 0.0: the conditional skew-normal can produce negative raw values,
    # but the zero-inflation component above already accounts for actual 0-three games.
    # Using max(0.5, ...) was incorrect — it prevented the non-zero path from ever
    # returning 0.0, which artificially inflated three-point predictions for players
    # whose conditional distribution has significant mass near zero.
    rounded = round(raw * 2.0) / 2.0
    return max(0.0, rounded)


def estimate_zero_probability(game_log_values, stat_type):
    """
    Estimate the probability of recording 0 for a stat from game logs.

    Used by the zero-inflated distribution (sample_zero_inflated) to set
    the zero-inflation parameter from the player's actual game history.

    Args:
        game_log_values (list of float): Historical game values for this stat.
        stat_type (str): Stat category for default fallbacks.
            'threes'/'fg3m': higher zero probability
            'steals'/'blocks': higher zero probability
            Others: lower zero probability

    Returns:
        float: Estimated zero probability (0.0-1.0).
            Minimum 0.05, maximum 0.80.
            Returns a stat-type default if fewer than 5 game logs.

    Example:
        # Player had 0 threes in 6 of 30 games
        estimate_zero_probability([0,2,3,0,1,4,0,2,1,3,...], 'threes') → 0.20
    """
    # Stat-type default fallbacks (used when insufficient game log data)
    stat_key = (stat_type or '').lower()
    if stat_key in ('threes', 'fg3m'):
        default_zero_prob = 0.15
    elif stat_key == 'steals':
        default_zero_prob = 0.25
    elif stat_key == 'blocks':
        default_zero_prob = 0.30
    else:
        default_zero_prob = 0.05

    # Use empirical rate when enough history is available
    if game_log_values and len(game_log_values) >= 5:
        zero_count = sum(1 for v in game_log_values if v < 0.5)
        empirical_rate = zero_count / len(game_log_values)
        # Clamp to reasonable bounds
        return max(0.05, min(0.80, empirical_rate))

    return default_zero_prob

# ============================================================
# END SECTION: Stat-Specific Distribution Samplers
# ============================================================
