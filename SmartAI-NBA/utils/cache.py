"""utils/cache.py – Tiered TTL in-memory cache + file-based cache."""
import json
import hashlib
import time
import threading
from functools import wraps
from pathlib import Path
from typing import Any, Optional, Dict
from utils.logger import get_logger

_logger = get_logger(__name__)

CACHE_TIERS = {
    "live": 30,
    "odds": 120,
    "stats": 900,
    "projections": 3600,
    "static": 86400,
}

_store: dict = {}
_lock = threading.Lock()


def cache_get(key: str, tier: str = "stats"):
    """Retrieve a value from cache if not expired.

    Args:
        key: Cache key.
        tier: One of the CACHE_TIERS keys (determines TTL).

    Returns:
        Cached value or None if missing/expired.
    """
    with _lock:
        entry = _store.get(key)
        if entry is None:
            return None
        ttl = CACHE_TIERS.get(tier, 900)
        if time.time() - entry["ts"] > ttl:
            _store.pop(key, None)
            return None
        return entry["value"]


def cache_set(key: str, value, tier: str = "stats") -> None:
    """Store a value in cache.

    Args:
        key: Cache key.
        value: Value to store.
        tier: Cache tier (controls TTL on retrieval).
    """
    with _lock:
        _store[key] = {"value": value, "ts": time.time(), "tier": tier}


def cache_invalidate(key: str) -> None:
    """Remove a single key from cache.

    Args:
        key: Cache key to remove.
    """
    with _lock:
        _store.pop(key, None)


def cache_clear() -> int:
    """Remove all keys from the in-memory cache.

    Returns:
        Number of keys removed.
    """
    with _lock:
        count = len(_store)
        _store.clear()
        return count


def cache_clear_tier(tier: str) -> int:
    """Remove all keys belonging to a specific tier.

    Args:
        tier: Cache tier name.

    Returns:
        Number of keys removed.
    """
    with _lock:
        keys = [k for k, v in _store.items() if v.get("tier") == tier]
        for k in keys:
            _store.pop(k, None)
        return len(keys)


def get_cache_stats() -> dict:
    """Return cache statistics.

    Returns:
        Dict with total keys and per-tier counts.
    """
    with _lock:
        tier_counts: dict = {}
        now = time.time()
        expired = 0
        for entry in _store.values():
            t = entry.get("tier", "unknown")
            tier_counts[t] = tier_counts.get(t, 0) + 1
            ttl = CACHE_TIERS.get(t, 900)
            if now - entry["ts"] > ttl:
                expired += 1
        return {"total": len(_store), "tiers": tier_counts, "expired": expired}


# ── File-based cache with TTL ────────────────────────────────


class FileCache:
    """File-based cache with TTL support."""

    def __init__(self, cache_dir: str = "cache", ttl_hours: int = 1):
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.ttl = ttl_hours * 3600

    def _get_cache_key(self, key: str) -> str:
        """Generate hash for cache key."""
        return hashlib.md5(key.encode()).hexdigest()

    def _get_cache_path(self, cache_key: str) -> Path:
        return self.cache_dir / f"{cache_key}.json"

    def get(self, key: str) -> Optional[Any]:
        """Get value from cache if not expired."""
        cache_key = self._get_cache_key(key)
        cache_path = self._get_cache_path(cache_key)

        if cache_path.exists():
            try:
                with open(cache_path, "r", encoding="utf-8") as f:
                    data = json.load(f)

                if time.time() - data["timestamp"] < self.ttl:
                    _logger.debug("Cache hit for %s...", key[:50])
                    return data["value"]
                else:
                    _logger.debug("Cache expired for %s...", key[:50])
                    cache_path.unlink()
            except Exception as exc:
                _logger.warning("Failed to read cache: %s", exc)

        return None

    def set(self, key: str, value: Any) -> None:
        """Set value in cache."""
        cache_key = self._get_cache_key(key)
        cache_path = self._get_cache_path(cache_key)

        try:
            data = {
                "timestamp": time.time(),
                "key": key,
                "value": value,
            }
            with open(cache_path, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2, default=str)
            _logger.debug("Cached %s...", key[:50])
        except Exception as exc:
            _logger.warning("Failed to write cache: %s", exc)

    def clear(self, key: Optional[str] = None) -> None:
        """Clear specific key or entire cache."""
        if key:
            cache_key = self._get_cache_key(key)
            cache_path = self._get_cache_path(cache_key)
            if cache_path.exists():
                cache_path.unlink()
                _logger.debug("Cleared cache for %s...", key[:50])
        else:
            count = 0
            for file in self.cache_dir.glob("*.json"):
                file.unlink()
                count += 1
            _logger.info("Cleared entire cache (%d files)", count)

    def get_stats(self) -> Dict[str, Any]:
        """Get cache statistics."""
        files = list(self.cache_dir.glob("*.json"))
        return {
            "total_files": len(files),
            "cache_dir": str(self.cache_dir),
            "ttl_hours": self.ttl / 3600,
        }


# Global file-cache instance
_data_cache = FileCache()


def cached(ttl_hours: int = 1):
    """Decorator for caching function results to disk."""

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            key = f"{func.__module__}.{func.__name__}:{args!s}:{kwargs!s}"
            result = _data_cache.get(key)
            if result is None:
                result = func(*args, **kwargs)
                _data_cache.set(key, result)
            return result

        return wrapper

    return decorator
