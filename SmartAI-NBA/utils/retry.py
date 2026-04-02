"""
Unified retry logic with exponential backoff and circuit breaker pattern.
"""

import time
from functools import wraps
from typing import Callable, Any, List, Union, Optional
from datetime import datetime, timedelta

from utils.logger import get_logger

logger = get_logger(__name__)


def retry_with_backoff(
    retries: int = 3,
    initial_delay: float = 2.0,
    backoff_factor: float = 2.0,
    max_delay: float = 30.0,
    exceptions: Union[List[type], tuple] = (Exception,),
):
    """
    Decorator for retrying functions with exponential backoff.

    Args:
        retries: Number of retry attempts.
        initial_delay: Initial delay in seconds.
        backoff_factor: Multiplier for delay on each retry.
        max_delay: Maximum delay between retries.
        exceptions: Exception types to catch and retry.
    """
    if isinstance(exceptions, list):
        exceptions = tuple(exceptions)

    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            delay = initial_delay
            last_exception = None

            for attempt in range(retries + 1):
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    last_exception = e
                    if attempt < retries:
                        wait_time = min(delay, max_delay)
                        logger.warning(
                            "Retry %d/%d for %s: %s. Waiting %.1fs...",
                            attempt + 1,
                            retries,
                            func.__name__,
                            e,
                            wait_time,
                        )
                        time.sleep(wait_time)
                        delay *= backoff_factor
                    else:
                        logger.error(
                            "All %d retries failed for %s",
                            retries,
                            func.__name__,
                        )
                        raise

        return wrapper

    return decorator


class CircuitBreaker:
    """
    Circuit breaker pattern to prevent repeated calls to failing endpoints.
    Opens after *failure_threshold* failures and stays open for *timeout*
    seconds before allowing a single test call (half-open state).
    """

    def __init__(self, name: str, failure_threshold: int = 3, timeout: int = 60):
        self.name = name
        self.failure_threshold = failure_threshold
        self.timeout = timeout
        self.failures = 0
        self.last_failure_time: Optional[datetime] = None
        self.is_open = False

    def call(self, func: Callable, *args, **kwargs) -> Any:
        """Execute function with circuit breaker protection."""
        if self.is_open:
            if (
                self.last_failure_time
                and datetime.now() - self.last_failure_time
                > timedelta(seconds=self.timeout)
            ):
                logger.info(
                    "Circuit breaker '%s' resetting after timeout", self.name
                )
                self.is_open = False
                self.failures = 0
            else:
                raise RuntimeError(
                    f"Circuit breaker '{self.name}' is open — "
                    f"skipping {func.__name__}"
                )

        try:
            result = func(*args, **kwargs)
            self.failures = 0
            return result
        except Exception as exc:
            self.failures += 1
            self.last_failure_time = datetime.now()
            if self.failures >= self.failure_threshold:
                self.is_open = True
                logger.error(
                    "Circuit breaker '%s' opened after %d failures",
                    self.name,
                    self.failures,
                )
            raise exc

    def reset(self):
        """Reset circuit breaker manually."""
        self.is_open = False
        self.failures = 0
        self.last_failure_time = None
        logger.info("Circuit breaker '%s' manually reset", self.name)
