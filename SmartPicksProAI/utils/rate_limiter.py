# utils/rate_limiter.py
# Rate limiting and circuit breaker for NBA API and platform API calls.
# Protects against 429 rate limit errors and cascading failures.
# Standard library only — no numpy/scipy/pandas.

import time
import datetime
from collections import deque

try:
    from utils.logger import get_logger
    _logger = get_logger(__name__)
except ImportError:
    import logging
    _logger = logging.getLogger(__name__)


class RateLimiter:
    """
    Token-bucket-style rate limiter with circuit breaker pattern.

    Tracks request counts per minute and per hour, and implements
    a circuit breaker that opens when an endpoint fails repeatedly.
    """

    def __init__(self, max_requests_per_minute=20, max_requests_per_hour=200):
        """
        Initialize the rate limiter.

        Args:
            max_requests_per_minute (int): Max requests allowed per 60 seconds
            max_requests_per_hour (int): Max requests allowed per 3600 seconds
        """
        self.max_per_minute = max_requests_per_minute
        self.max_per_hour = max_requests_per_hour

        # Sliding window of request timestamps (global, across all endpoints)
        self._request_times = deque()

        # Per-endpoint failure tracking for circuit breaker
        self._failure_times = {}   # endpoint → deque of failure timestamps
        self._circuit_open = {}    # endpoint → bool

        # Constants for circuit breaker
        self._circuit_failure_threshold = 5   # Open after N failures
        self._circuit_window_seconds = 600    # Within this many seconds
        self._circuit_reset_seconds = 120     # Auto-reset after this long

    def acquire(self):
        """
        Block/sleep until it's safe to make a request.

        Checks both per-minute and per-hour rate limits.
        Sleeps if either limit would be exceeded.

        Returns:
            bool: Always True (returns when safe to proceed)
        """
        while True:
            now = time.time()

            # Clean up old timestamps
            minute_cutoff = now - 60
            hour_cutoff = now - 3600

            while self._request_times and self._request_times[0] < hour_cutoff:
                self._request_times.popleft()

            # Count recent requests
            per_minute = sum(1 for t in self._request_times if t >= minute_cutoff)
            per_hour = len(self._request_times)

            if per_minute >= self.max_per_minute:
                # Find oldest request in the past minute and wait until it expires
                oldest_in_minute = next(
                    (t for t in self._request_times if t >= minute_cutoff), now
                )
                wait = max(0.1, oldest_in_minute + 60 - now + 0.1)
                _logger.debug("Rate limit (per-minute): sleeping %.1fs", wait)
                time.sleep(min(wait, 5.0))
                continue

            if per_hour >= self.max_per_hour:
                oldest = self._request_times[0] if self._request_times else now
                wait = max(0.1, oldest + 3600 - now + 0.1)
                _logger.warning("Rate limit (per-hour): sleeping %.0fs", min(wait, 30))
                time.sleep(min(wait, 30.0))
                continue

            # Safe to proceed
            self._request_times.append(now)
            return True

    def record_request(self, endpoint):
        """
        Record that a successful request was made to an endpoint.

        Tracks per-endpoint request counts for diagnostics. The global
        rate-limit enforcement is handled by acquire(); this method provides
        a lightweight audit trail of which endpoints are being called.

        Args:
            endpoint (str): The API endpoint or identifier
        """
        if endpoint not in self._failure_times:
            # Initialise the failure deque so is_circuit_open() works immediately
            self._failure_times[endpoint] = deque()

    def record_failure(self, endpoint):
        """
        Record a failure for an endpoint (used by circuit breaker).

        Args:
            endpoint (str): The API endpoint or identifier
        """
        now = time.time()
        if endpoint not in self._failure_times:
            self._failure_times[endpoint] = deque()

        self._failure_times[endpoint].append(now)

        # Clean old failures outside the window
        cutoff = now - self._circuit_window_seconds
        while self._failure_times[endpoint] and self._failure_times[endpoint][0] < cutoff:
            self._failure_times[endpoint].popleft()

        # Open circuit if too many failures
        if len(self._failure_times[endpoint]) >= self._circuit_failure_threshold:
            if not self._circuit_open.get(endpoint, False):
                _logger.warning(
                    "Circuit breaker OPEN for endpoint: %s (%d failures in %ds)",
                    endpoint,
                    len(self._failure_times[endpoint]),
                    self._circuit_window_seconds,
                )
            self._circuit_open[endpoint] = True

    def handle_429_response(self, response=None, retry_after=None):
        """
        Handle a 429 (rate limit) response.

        Parses Retry-After header and sleeps accordingly.

        Args:
            response: HTTP response object with headers (or None)
            retry_after (int or None): Override retry-after seconds

        Returns:
            bool: True if should retry, False if should give up
        """
        wait_seconds = 60  # Default wait

        if retry_after is not None:
            wait_seconds = int(retry_after)
        elif response is not None:
            try:
                wait_seconds = int(response.headers.get("Retry-After", 60))
            except (AttributeError, ValueError, TypeError):
                wait_seconds = 60

        wait_seconds = max(1, min(wait_seconds, 300))  # Clamp 1-300 seconds
        _logger.warning("429 rate limit hit — waiting %ds before retry", wait_seconds)
        time.sleep(wait_seconds)
        return True

    def is_circuit_open(self, endpoint):
        """
        Check if the circuit breaker is open for an endpoint.

        Auto-resets the circuit if enough time has passed since the last failure.

        Args:
            endpoint (str): API endpoint identifier

        Returns:
            bool: True if circuit is open (should skip calls to this endpoint)
        """
        if not self._circuit_open.get(endpoint, False):
            return False

        # Check for auto-reset
        failures = self._failure_times.get(endpoint, deque())
        if failures:
            last_failure = failures[-1]
            if time.time() - last_failure > self._circuit_reset_seconds:
                self.reset_circuit(endpoint)
                return False

        return True

    def reset_circuit(self, endpoint):
        """
        Manually reset the circuit breaker for an endpoint.

        Args:
            endpoint (str): API endpoint identifier
        """
        self._circuit_open[endpoint] = False
        if endpoint in self._failure_times:
            self._failure_times[endpoint].clear()
        _logger.info("Circuit breaker RESET for endpoint: %s", endpoint)


# Global singleton rate limiter instances
_nba_api_limiter = RateLimiter(max_requests_per_minute=15, max_requests_per_hour=150)
_platform_limiter = RateLimiter(max_requests_per_minute=20, max_requests_per_hour=200)


def get_nba_api_limiter():
    """Get the global NBA API rate limiter."""
    return _nba_api_limiter


def get_platform_limiter():
    """Get the global platform API rate limiter."""
    return _platform_limiter
