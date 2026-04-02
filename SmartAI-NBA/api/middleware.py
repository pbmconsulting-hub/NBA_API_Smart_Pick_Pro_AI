"""api/middleware.py – Custom middleware for SmartAI-NBA API."""
import time
from utils.logger import get_logger

_logger = get_logger(__name__)

try:
    from starlette.middleware.base import BaseHTTPMiddleware
    from starlette.requests import Request
    _STARLETTE_AVAILABLE = True
except ImportError:
    _STARLETTE_AVAILABLE = False

if _STARLETTE_AVAILABLE:
    class TimingMiddleware(BaseHTTPMiddleware):
        """Middleware that measures request processing time and adds X-Process-Time header."""

        async def dispatch(self, request: Request, call_next):
            """Process request and add timing header.

            Args:
                request: Incoming HTTP request.
                call_next: Next middleware or route handler.

            Returns:
                HTTP response with X-Process-Time header.
            """
            start = time.perf_counter()
            response = await call_next(request)
            elapsed = time.perf_counter() - start
            response.headers["X-Process-Time"] = f"{elapsed:.4f}"
            _logger.debug("%s %s %.4fs", request.method, request.url.path, elapsed)
            return response
else:
    class TimingMiddleware:  # type: ignore[no-redef]
        """No-op fallback when starlette is not installed."""
        def __init__(self, app=None):
            self.app = app
            _logger.warning(
                "TimingMiddleware loaded without starlette — "
                "request timing is disabled. Install starlette/fastapi to enable."
            )
