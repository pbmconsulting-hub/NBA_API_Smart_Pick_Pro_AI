"""
tests/test_auth.py
------------------
Tests for utils/auth.py — session-based premium authentication.
"""

import sys
import os
import pathlib
import unittest

# ── Ensure repo root is on sys.path ──────────────────────────────
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

_AUTH_SRC = pathlib.Path(__file__).parent.parent / "utils" / "auth.py"


class TestAuthSourceLevel(unittest.TestCase):
    """Source-level checks for utils/auth.py."""

    def test_file_exists(self):
        self.assertTrue(_AUTH_SRC.exists(), "utils/auth.py must exist")

    def test_has_is_premium_user(self):
        src = _AUTH_SRC.read_text(encoding="utf-8")
        self.assertIn("def is_premium_user(", src)

    def test_has_handle_checkout_redirect(self):
        src = _AUTH_SRC.read_text(encoding="utf-8")
        self.assertIn("def handle_checkout_redirect(", src)

    def test_has_production_bypass_logic(self):
        """Must check SMARTAI_PRODUCTION env var."""
        src = _AUTH_SRC.read_text(encoding="utf-8")
        self.assertIn("SMARTAI_PRODUCTION", src)

    def test_has_startup_warning(self):
        """Must warn when STRIPE_SECRET_KEY is set without SMARTAI_PRODUCTION."""
        src = _AUTH_SRC.read_text(encoding="utf-8")
        self.assertIn("STRIPE_SECRET_KEY is set but SMARTAI_PRODUCTION", src)


class TestIsPremiumUser(unittest.TestCase):
    """Runtime tests for is_premium_user."""

    def test_returns_true_when_not_production(self):
        """Without SMARTAI_PRODUCTION set, should return True (dev mode)."""
        original = os.environ.pop("SMARTAI_PRODUCTION", None)
        try:
            from utils.auth import is_premium_user
            result = is_premium_user()
            self.assertTrue(result)
        finally:
            if original is not None:
                os.environ["SMARTAI_PRODUCTION"] = original

    def test_returns_true_when_stripe_not_configured(self):
        """When Stripe is not configured, should return True (free mode)."""
        original_prod = os.environ.pop("SMARTAI_PRODUCTION", None)
        original_key = os.environ.pop("STRIPE_SECRET_KEY", None)
        try:
            from utils.auth import is_premium_user
            result = is_premium_user()
            self.assertTrue(result)
        finally:
            if original_prod is not None:
                os.environ["SMARTAI_PRODUCTION"] = original_prod
            if original_key is not None:
                os.environ["STRIPE_SECRET_KEY"] = original_key


class TestHandleCheckoutRedirect(unittest.TestCase):
    """Runtime tests for handle_checkout_redirect."""

    def test_returns_false_with_no_session_id(self):
        """Should return False when no session_id in query params."""
        import streamlit as st
        # Ensure no session_id in query params
        if hasattr(st, 'query_params') and isinstance(st.query_params, dict):
            st.query_params.pop("session_id", None)
        from utils.auth import handle_checkout_redirect
        result = handle_checkout_redirect()
        self.assertFalse(result)


if __name__ == "__main__":
    unittest.main()
