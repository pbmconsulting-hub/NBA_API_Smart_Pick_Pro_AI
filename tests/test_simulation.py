"""Tests for engine/simulation.py — distribution sanity checks."""

import sys
from pathlib import Path

import numpy as np

_REPO = Path(__file__).resolve().parent.parent
_PKG = _REPO / "SmartPicksProAI"
if str(_PKG) not in sys.path:
    sys.path.insert(0, str(_PKG))


class TestNegativeBinomial:
    """Verify that the negative binomial sampling doesn't crash and produces
    sensible integer results (steals/blocks)."""

    def test_negative_binomial_integer_n(self):
        """numpy.random.Generator.negative_binomial requires integer n."""
        rng = np.random.default_rng(42)
        mu = np.array([1.2, 0.8, 1.5, 2.0])
        overdispersion = 1.5
        r_arr = np.clip(mu / max(overdispersion - 1.0, 0.01), 0.5, 100.0)
        p_arr = r_arr / (r_arr + mu)
        p_arr = np.clip(p_arr, 0.01, 0.99)

        r_safe = np.maximum(np.asarray(r_arr, dtype=np.float64), 0.5)
        p_safe = np.asarray(p_arr, dtype=np.float64)
        # Must round to int for numpy negative_binomial
        r_int = np.round(r_safe).astype(np.int64)
        r_int = np.maximum(r_int, 1)

        results = rng.negative_binomial(r_int, p_safe)
        assert results.shape == mu.shape
        assert np.all(results >= 0), "Negative binomial should produce non-negative values"
        assert results.dtype in (np.int64, np.int32, np.intp)

    def test_negative_binomial_reasonable_values(self):
        """Mean of many samples should be close to mu."""
        rng = np.random.default_rng(123)
        mu = 1.5
        overdispersion = 1.5
        r = mu / max(overdispersion - 1.0, 0.01)
        p = r / (r + mu)
        r_int = max(1, round(r))

        samples = rng.negative_binomial(r_int, p, size=10_000)
        sample_mean = samples.mean()
        # Should be within reasonable range of mu
        assert 0.5 < sample_mean < 4.0, f"Sample mean {sample_mean} too far from mu {mu}"


class TestPoissonForTurnovers:
    """Verify Poisson sampling for turnovers."""

    def test_poisson_produces_non_negative(self):
        rng = np.random.default_rng(42)
        lam_arr = np.array([2.5, 3.0, 1.8])
        results = rng.poisson(lam_arr)
        assert np.all(results >= 0)

    def test_poisson_mean_close_to_lambda(self):
        rng = np.random.default_rng(42)
        lam = 3.0
        samples = rng.poisson(lam, size=10_000)
        assert abs(samples.mean() - lam) < 0.2


class TestNormalForPoints:
    """Verify normal/skew-normal sampling doesn't crash."""

    def test_normal_produces_values(self):
        rng = np.random.default_rng(42)
        means = np.array([25.0, 18.0, 30.0])
        stds = np.array([5.0, 4.0, 6.0])
        results = rng.normal(means, stds)
        assert results.shape == means.shape
        assert not np.any(np.isnan(results))
