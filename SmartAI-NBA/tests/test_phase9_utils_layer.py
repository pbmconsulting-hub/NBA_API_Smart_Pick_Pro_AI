# ============================================================
# FILE: tests/test_phase9_utils_layer.py
# PURPOSE: Phase 9 — Unit tests for utility layer modules
#   9A: utils/headers.py — header rotation and required fields
#   9B: utils/retry.py — retry decorator and circuit breaker
#   9C: utils/cache.py — tiered in-memory and file-based cache
#   9D: utils/logger.py — structured logging configuration
# ============================================================
import logging
import math
import os
import shutil
import sys
import time
import tempfile
import unittest
from unittest.mock import MagicMock, patch


# ============================================================
# 9A: utils/headers.py
# ============================================================

class TestHeadersUserAgent(unittest.TestCase):
    """Verify get_random_user_agent returns one of the known agents."""

    def test_returns_string(self):
        from utils.headers import get_random_user_agent
        ua = get_random_user_agent()
        self.assertIsInstance(ua, str)
        self.assertGreater(len(ua), 10)

    def test_from_known_list(self):
        from utils.headers import get_random_user_agent, USER_AGENTS
        ua = get_random_user_agent()
        self.assertIn(ua, USER_AGENTS)

    def test_user_agents_list_nonempty(self):
        from utils.headers import USER_AGENTS
        self.assertGreater(len(USER_AGENTS), 0)


class TestNBAHeaders(unittest.TestCase):
    """Verify get_nba_headers returns required keys."""

    def test_required_keys(self):
        from utils.headers import get_nba_headers
        h = get_nba_headers()
        for key in ("User-Agent", "Accept", "Referer", "Origin"):
            self.assertIn(key, h, f"Missing key: {key}")

    def test_nba_referer(self):
        from utils.headers import get_nba_headers
        h = get_nba_headers()
        self.assertIn("nba.com", h["Referer"])

    def test_cors_headers(self):
        from utils.headers import get_nba_headers
        h = get_nba_headers()
        self.assertIn("Sec-Fetch-Mode", h)
        self.assertEqual(h["Sec-Fetch-Mode"], "cors")


class TestCDNHeaders(unittest.TestCase):
    """Verify get_cdn_headers returns required keys."""

    def test_required_keys(self):
        from utils.headers import get_cdn_headers
        h = get_cdn_headers()
        for key in ("User-Agent", "Accept", "Referer"):
            self.assertIn(key, h)

    def test_origin_nba(self):
        from utils.headers import get_cdn_headers
        h = get_cdn_headers()
        self.assertIn("nba.com", h["Origin"])


class TestESPNHeaders(unittest.TestCase):
    """Verify get_espn_headers returns required keys."""

    def test_required_keys(self):
        from utils.headers import get_espn_headers
        h = get_espn_headers()
        self.assertIn("User-Agent", h)
        self.assertIn("Accept", h)

    def test_espn_referer(self):
        from utils.headers import get_espn_headers
        h = get_espn_headers()
        self.assertIn("espn.com", h["Referer"])


class TestUnderdogHeaders(unittest.TestCase):
    """Verify get_underdog_headers returns required keys."""

    def test_required_keys(self):
        from utils.headers import get_underdog_headers
        h = get_underdog_headers()
        self.assertIn("User-Agent", h)
        self.assertIn("Origin", h)

    def test_underdog_origin(self):
        from utils.headers import get_underdog_headers
        h = get_underdog_headers()
        self.assertIn("underdogfantasy.com", h["Origin"])


class TestOddsAPIHeaders(unittest.TestCase):
    """Verify get_odds_api_headers returns required keys."""

    def test_required_keys(self):
        from utils.headers import get_odds_api_headers
        h = get_odds_api_headers()
        self.assertIn("User-Agent", h)
        self.assertIn("Accept", h)

    def test_user_agent_identifies_app(self):
        from utils.headers import get_odds_api_headers
        h = get_odds_api_headers()
        self.assertIn("SmartAI", h["User-Agent"])


# ============================================================
# 9B: utils/retry.py — retry_with_backoff decorator
# ============================================================

class TestRetryWithBackoff(unittest.TestCase):
    """Verify retry_with_backoff decorator behaviour."""

    def test_success_no_retry(self):
        from utils.retry import retry_with_backoff

        call_count = 0

        @retry_with_backoff(retries=3, initial_delay=0.01)
        def succeed():
            nonlocal call_count
            call_count += 1
            return "ok"

        self.assertEqual(succeed(), "ok")
        self.assertEqual(call_count, 1)

    def test_retries_on_failure(self):
        from utils.retry import retry_with_backoff

        call_count = 0

        @retry_with_backoff(retries=2, initial_delay=0.01)
        def fail_then_succeed():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise ValueError("boom")
            return "ok"

        self.assertEqual(fail_then_succeed(), "ok")
        self.assertEqual(call_count, 3)

    def test_raises_after_exhausting_retries(self):
        from utils.retry import retry_with_backoff

        @retry_with_backoff(retries=1, initial_delay=0.01)
        def always_fail():
            raise RuntimeError("oops")

        with self.assertRaises(RuntimeError):
            always_fail()

    def test_only_retries_specified_exceptions(self):
        from utils.retry import retry_with_backoff

        @retry_with_backoff(retries=3, initial_delay=0.01, exceptions=(ValueError,))
        def raise_type_error():
            raise TypeError("wrong type")

        with self.assertRaises(TypeError):
            raise_type_error()

    def test_respects_max_delay(self):
        from utils.retry import retry_with_backoff

        @retry_with_backoff(retries=1, initial_delay=100.0, max_delay=0.01)
        def fail_once():
            fail_once._calls = getattr(fail_once, "_calls", 0) + 1
            if fail_once._calls < 2:
                raise ValueError("fail")
            return "ok"

        start = time.time()
        fail_once()
        elapsed = time.time() - start
        self.assertLess(elapsed, 2.0)

    def test_preserves_function_name(self):
        from utils.retry import retry_with_backoff

        @retry_with_backoff()
        def my_function():
            pass

        self.assertEqual(my_function.__name__, "my_function")

    def test_exceptions_as_list(self):
        from utils.retry import retry_with_backoff

        call_count = 0

        @retry_with_backoff(retries=1, initial_delay=0.01, exceptions=[ValueError])
        def fail_once():
            nonlocal call_count
            call_count += 1
            if call_count < 2:
                raise ValueError("boom")
            return "done"

        self.assertEqual(fail_once(), "done")


# ============================================================
# 9B: utils/retry.py — CircuitBreaker
# ============================================================

class TestCircuitBreaker(unittest.TestCase):
    """Verify CircuitBreaker state transitions."""

    def test_starts_closed(self):
        from utils.retry import CircuitBreaker
        cb = CircuitBreaker("test", failure_threshold=3, timeout=60)
        self.assertFalse(cb.is_open)

    def test_successful_call_resets_failures(self):
        from utils.retry import CircuitBreaker
        cb = CircuitBreaker("test", failure_threshold=3, timeout=60)
        result = cb.call(lambda: "ok")
        self.assertEqual(result, "ok")
        self.assertEqual(cb.failures, 0)

    def test_opens_after_threshold_failures(self):
        from utils.retry import CircuitBreaker
        cb = CircuitBreaker("test", failure_threshold=2, timeout=60)

        for _ in range(2):
            with self.assertRaises(ValueError):
                cb.call(self._raise_value_error)

        self.assertTrue(cb.is_open)

    def test_open_circuit_raises_runtime_error(self):
        from utils.retry import CircuitBreaker
        cb = CircuitBreaker("test", failure_threshold=1, timeout=9999)

        with self.assertRaises(ValueError):
            cb.call(self._raise_value_error)

        self.assertTrue(cb.is_open)

        with self.assertRaises(RuntimeError):
            cb.call(lambda: "should not run")

    def test_resets_after_timeout(self):
        from utils.retry import CircuitBreaker
        cb = CircuitBreaker("test", failure_threshold=1, timeout=0)

        with self.assertRaises(ValueError):
            cb.call(self._raise_value_error)

        self.assertTrue(cb.is_open)

        # After timeout=0 seconds, it should reset on next call
        result = cb.call(lambda: "recovered")
        self.assertEqual(result, "recovered")
        self.assertFalse(cb.is_open)

    def test_manual_reset(self):
        from utils.retry import CircuitBreaker
        cb = CircuitBreaker("test", failure_threshold=1, timeout=9999)

        with self.assertRaises(ValueError):
            cb.call(self._raise_value_error)

        self.assertTrue(cb.is_open)
        cb.reset()
        self.assertFalse(cb.is_open)
        self.assertEqual(cb.failures, 0)

    def test_success_after_failures_resets_count(self):
        from utils.retry import CircuitBreaker
        cb = CircuitBreaker("test", failure_threshold=3, timeout=60)

        with self.assertRaises(ValueError):
            cb.call(self._raise_value_error)

        self.assertEqual(cb.failures, 1)

        cb.call(lambda: "success")
        self.assertEqual(cb.failures, 0)

    @staticmethod
    def _raise_value_error():
        raise ValueError("test error")


# ============================================================
# 9C: utils/cache.py — tiered in-memory cache
# ============================================================

class TestInMemoryCache(unittest.TestCase):
    """Verify tiered in-memory cache get/set/invalidate/clear."""

    def setUp(self):
        from utils.cache import _store
        _store.clear()

    def tearDown(self):
        from utils.cache import _store
        _store.clear()

    def test_cache_set_and_get(self):
        from utils.cache import cache_set, cache_get
        cache_set("key1", "value1", tier="stats")
        self.assertEqual(cache_get("key1", tier="stats"), "value1")

    def test_cache_miss_returns_none(self):
        from utils.cache import cache_get
        self.assertIsNone(cache_get("nonexistent"))

    def test_cache_invalidate(self):
        from utils.cache import cache_set, cache_get, cache_invalidate
        cache_set("key1", "value1")
        cache_invalidate("key1")
        self.assertIsNone(cache_get("key1"))

    def test_cache_clear_tier(self):
        from utils.cache import cache_set, cache_get, cache_clear_tier
        cache_set("a", 1, tier="live")
        cache_set("b", 2, tier="live")
        cache_set("c", 3, tier="stats")
        removed = cache_clear_tier("live")
        self.assertEqual(removed, 2)
        self.assertIsNone(cache_get("a", tier="live"))
        self.assertEqual(cache_get("c", tier="stats"), 3)

    def test_cache_expiry(self):
        from utils.cache import cache_set, cache_get, _store
        cache_set("k", "v", tier="live")
        # Manually expire by backdating timestamp
        _store["k"]["ts"] = time.time() - 100  # live TTL is 30s
        self.assertIsNone(cache_get("k", tier="live"))

    def test_get_cache_stats(self):
        from utils.cache import cache_set, get_cache_stats
        cache_set("x", 1, tier="odds")
        cache_set("y", 2, tier="odds")
        stats = get_cache_stats()
        self.assertEqual(stats["total"], 2)
        self.assertEqual(stats["tiers"]["odds"], 2)

    def test_cache_tiers_defined(self):
        from utils.cache import CACHE_TIERS
        expected = {"live", "odds", "stats", "projections", "static"}
        self.assertEqual(set(CACHE_TIERS.keys()), expected)

    def test_default_tier_is_stats(self):
        from utils.cache import cache_set, cache_get
        cache_set("key", "val")
        # stats has 900s TTL, so should be accessible
        self.assertEqual(cache_get("key"), "val")


# ============================================================
# 9C: utils/cache.py — FileCache
# ============================================================

class TestFileCache(unittest.TestCase):
    """Verify FileCache set/get/clear and TTL expiry."""

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def _make_cache(self, ttl_hours=1):
        from utils.cache import FileCache
        return FileCache(cache_dir=self.tmpdir, ttl_hours=ttl_hours)

    def test_set_and_get(self):
        fc = self._make_cache()
        fc.set("hello", {"data": [1, 2, 3]})
        result = fc.get("hello")
        self.assertEqual(result, {"data": [1, 2, 3]})

    def test_get_returns_none_on_miss(self):
        fc = self._make_cache()
        self.assertIsNone(fc.get("nonexistent"))

    def test_clear_specific_key(self):
        fc = self._make_cache()
        fc.set("k1", "v1")
        fc.set("k2", "v2")
        fc.clear(key="k1")
        self.assertIsNone(fc.get("k1"))
        self.assertEqual(fc.get("k2"), "v2")

    def test_clear_all(self):
        fc = self._make_cache()
        fc.set("k1", "v1")
        fc.set("k2", "v2")
        fc.clear()
        self.assertIsNone(fc.get("k1"))
        self.assertIsNone(fc.get("k2"))

    def test_ttl_expiry(self):
        fc = self._make_cache(ttl_hours=0)
        fc.set("expired", "data")
        # ttl_hours=0 means ttl=0 seconds, so everything is expired
        time.sleep(0.05)
        self.assertIsNone(fc.get("expired"))

    def test_get_stats(self):
        fc = self._make_cache()
        fc.set("a", 1)
        fc.set("b", 2)
        stats = fc.get_stats()
        self.assertEqual(stats["total_files"], 2)
        self.assertEqual(stats["ttl_hours"], 1.0)

    def test_handles_corrupt_cache_file(self):
        fc = self._make_cache()
        fc.set("key", "value")
        # Corrupt the file
        cache_key = fc._get_cache_key("key")
        cache_path = fc._get_cache_path(cache_key)
        with open(cache_path, "w") as f:
            f.write("not json{{{")
        # Should return None gracefully
        self.assertIsNone(fc.get("key"))

    def test_string_values(self):
        fc = self._make_cache()
        fc.set("str", "hello world")
        self.assertEqual(fc.get("str"), "hello world")

    def test_list_values(self):
        fc = self._make_cache()
        fc.set("list", [1, 2, 3])
        self.assertEqual(fc.get("list"), [1, 2, 3])

    def test_nested_dict_values(self):
        fc = self._make_cache()
        fc.set("nested", {"a": {"b": [1, 2]}})
        self.assertEqual(fc.get("nested"), {"a": {"b": [1, 2]}})


# ============================================================
# 9C: utils/cache.py — @cached decorator
# ============================================================

class TestCachedDecorator(unittest.TestCase):
    """Verify @cached decorator caches function results."""

    def setUp(self):
        from utils.cache import _data_cache
        self._original_cache_dir = _data_cache.cache_dir
        self.tmpdir = tempfile.mkdtemp()
        _data_cache.cache_dir = __import__("pathlib").Path(self.tmpdir)

    def tearDown(self):
        from utils.cache import _data_cache
        _data_cache.cache_dir = self._original_cache_dir
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_caches_result(self):
        from utils.cache import cached

        call_count = 0

        @cached(ttl_hours=1)
        def expensive():
            nonlocal call_count
            call_count += 1
            return 42

        result1 = expensive()
        result2 = expensive()
        self.assertEqual(result1, 42)
        self.assertEqual(result2, 42)
        self.assertEqual(call_count, 1)

    def test_preserves_function_name(self):
        from utils.cache import cached

        @cached()
        def my_func():
            return 1

        self.assertEqual(my_func.__name__, "my_func")


# ============================================================
# 9D: utils/logger.py — structured logging
# ============================================================

class TestGetLogger(unittest.TestCase):
    """Verify get_logger returns properly namespaced loggers."""

    def test_returns_logger(self):
        from utils.logger import get_logger
        log = get_logger("test_module")
        self.assertIsInstance(log, logging.Logger)

    def test_prefixed_name(self):
        from utils.logger import get_logger
        log = get_logger("my_module")
        self.assertTrue(log.name.startswith("smartai_nba."))

    def test_already_prefixed_name(self):
        from utils.logger import get_logger
        log = get_logger("smartai_nba.existing")
        self.assertEqual(log.name, "smartai_nba.existing")

    def test_root_logger_exists(self):
        from utils.logger import get_logger
        get_logger("any_module")
        root = logging.getLogger("smartai_nba")
        self.assertTrue(len(root.handlers) > 0)


class TestSetupLogging(unittest.TestCase):
    """Verify setup_logging configures the root logger."""

    def test_returns_logger(self):
        from utils.logger import setup_logging
        log = setup_logging(log_level="WARNING")
        self.assertIsInstance(log, logging.Logger)

    def test_custom_log_file(self):
        from utils.logger import setup_logging
        tmpdir = tempfile.mkdtemp()
        try:
            log_file = os.path.join(tmpdir, "test.log")
            setup_logging(log_level="DEBUG", log_file=log_file)
            log = logging.getLogger("smartai_nba")
            log.info("test message")
            self.assertTrue(os.path.exists(log_file))
        finally:
            shutil.rmtree(tmpdir, ignore_errors=True)


if __name__ == "__main__":
    unittest.main()
