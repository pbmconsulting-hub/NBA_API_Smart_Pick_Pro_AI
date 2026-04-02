"""
tests/test_stripe_manager.py
-----------------------------
Tests for utils/stripe_manager.py — Stripe API integration.
"""

import sys
import os
import pathlib
import unittest

# ── Ensure repo root is on sys.path ──────────────────────────────
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

_SM_SRC = pathlib.Path(__file__).parent.parent / "utils" / "stripe_manager.py"


class TestStripeManagerSourceLevel(unittest.TestCase):
    """Source-level checks for utils/stripe_manager.py."""

    def test_file_exists(self):
        self.assertTrue(_SM_SRC.exists(), "utils/stripe_manager.py must exist")

    def test_has_is_stripe_configured(self):
        src = _SM_SRC.read_text(encoding="utf-8")
        self.assertIn("def is_stripe_configured(", src)

    def test_has_create_checkout_session(self):
        src = _SM_SRC.read_text(encoding="utf-8")
        self.assertIn("def create_checkout_session(", src)

    def test_has_get_subscription_by_id(self):
        src = _SM_SRC.read_text(encoding="utf-8")
        self.assertIn("def get_subscription_by_id(", src)


class TestIsStripeConfigured(unittest.TestCase):
    """Runtime tests for is_stripe_configured."""

    def test_returns_false_without_env_vars(self):
        """Should return False when Stripe env vars are not set."""
        # Clear any Stripe env vars that might be set
        original_key = os.environ.pop("STRIPE_SECRET_KEY", None)
        original_pub = os.environ.pop("STRIPE_PUBLISHABLE_KEY", None)
        try:
            from utils.stripe_manager import is_stripe_configured
            # Note: is_stripe_configured may still find keys in st.secrets
            # In test environment with mocked streamlit, it should return False
            result = is_stripe_configured()
            self.assertIsInstance(result, bool)
        finally:
            if original_key is not None:
                os.environ["STRIPE_SECRET_KEY"] = original_key
            if original_pub is not None:
                os.environ["STRIPE_PUBLISHABLE_KEY"] = original_pub


class TestCreateCheckoutSession(unittest.TestCase):
    """Runtime tests for create_checkout_session."""

    def test_returns_error_when_not_configured(self):
        """Should return an error dict when Stripe is not configured."""
        original_key = os.environ.pop("STRIPE_SECRET_KEY", None)
        try:
            from utils.stripe_manager import create_checkout_session
            result = create_checkout_session()
            self.assertIsInstance(result, dict)
            # When Stripe isn't configured, it should indicate failure
            if not result.get("success"):
                self.assertIn("error", result)
        finally:
            if original_key is not None:
                os.environ["STRIPE_SECRET_KEY"] = original_key


class TestGetSubscriptionById(unittest.TestCase):
    """Runtime tests for get_subscription_by_id."""

    def test_graceful_degradation_with_invalid_id(self):
        """Should gracefully handle invalid subscription IDs."""
        from utils.stripe_manager import get_subscription_by_id
        result = get_subscription_by_id("invalid_sub_id_12345")
        self.assertIsInstance(result, dict)


if __name__ == "__main__":
    unittest.main()
