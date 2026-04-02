"""
tests/conftest.py
-----------------
Shared test fixtures for pytest.
Mocks streamlit globally and provides a fresh in-memory SQLite database.
"""

import sys
import os
import types
import sqlite3

# ── Ensure repo root is on sys.path ──────────────────────────────
_PROJECT_ROOT = os.path.join(os.path.dirname(__file__), "..")
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)


# ── Mock streamlit before any app code imports it ────────────────
# This prevents Streamlit from starting a server during tests.
#
# NOTE: types.SimpleNamespace does NOT support the context-manager
# protocol (Python looks up __enter__/__exit__ on the *type*, not
# the instance).  We use a proper class for anything that may appear
# in a ``with`` statement.


class _MockCtxManager:
    """Minimal mock that supports the context manager protocol."""

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return None


class _MockSidebar(_MockCtxManager):
    """Mock for ``st.sidebar`` — supports attribute access and ``with`` protocol."""

    @staticmethod
    def markdown(*a, **kw):
        return None

    @staticmethod
    def expander(*a, **kw):
        return _MockCtxManager()

    @staticmethod
    def caption(*a, **kw):
        return None


_mock_st = types.ModuleType("streamlit")
_mock_st.cache_data = lambda *a, **kw: (lambda f: f) if not a else a[0]
_mock_st.cache_resource = lambda *a, **kw: (lambda f: f) if not a else a[0]
_mock_st.session_state = {}
_mock_st.secrets = {}
_mock_st.set_page_config = lambda **kw: None
_mock_st.markdown = lambda *a, **kw: None
_mock_st.write = lambda *a, **kw: None
_mock_st.warning = lambda *a, **kw: None
_mock_st.error = lambda *a, **kw: None
_mock_st.info = lambda *a, **kw: None
_mock_st.success = lambda *a, **kw: None
_mock_st.spinner = lambda *a, **kw: _MockCtxManager()
_mock_st.expander = lambda *a, **kw: _MockCtxManager()
_mock_st.sidebar = _MockSidebar()
_mock_st.query_params = {}
_mock_st.rerun = lambda: None
_mock_st.columns = lambda *a, **kw: [_MockCtxManager()]
_mock_st.popover = lambda *a, **kw: _MockCtxManager()
_mock_st.number_input = lambda *a, **kw: 0
_mock_st.slider = lambda *a, **kw: 0
_mock_st.caption = lambda *a, **kw: None
_mock_st.divider = lambda: None

if "streamlit" not in sys.modules:
    sys.modules["streamlit"] = _mock_st
