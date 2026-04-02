"""api/routes/health.py – Health check endpoint."""
import datetime
from utils.logger import get_logger

_logger = get_logger(__name__)

try:
    from fastapi import APIRouter
    router = APIRouter(tags=["health"])
    _FASTAPI_AVAILABLE = True
except ImportError:
    _FASTAPI_AVAILABLE = False
    router = None

if _FASTAPI_AVAILABLE:
    @router.get("/health")
    async def health_check():
        """Return API health status.

        Returns:
            JSON with ``status``, ``timestamp``, and ``version``.
            Status is ``healthy`` or ``degraded``.
        """
        details = {}
        degraded = False

        # Check cache accessibility
        try:
            from utils.cache import cache_get, cache_set
            cache_set("_health_probe", 1, tier="live")
            val = cache_get("_health_probe", tier="live")
            if val != 1:
                details["cache"] = "stale"
                degraded = True
            else:
                details["cache"] = "ok"
        except Exception as exc:
            details["cache"] = f"error: {exc}"
            degraded = True

        # Check nba_api reachability (lightweight)
        try:
            import importlib
            importlib.import_module("nba_api")
            details["nba_api"] = "ok"
        except ImportError:
            details["nba_api"] = "not_installed"
            degraded = True

        ts = datetime.datetime.now(datetime.timezone.utc).isoformat()

        if degraded:
            return {"status": "degraded", "timestamp": ts, "details": details}
        return {"status": "healthy", "timestamp": ts, "version": "1.0.0"}
