"""
tests/test_nba_injury_pdf.py
-----------------------------
Comprehensive tests for the data/nba_injury_pdf package and its
integration into RosterEngine.

All HTTP requests are mocked — no live NBA CDN calls are made.
"""

import datetime
import os
import sys
import unittest
from unittest.mock import MagicMock, patch

try:
    import pandas as pd
    _HAS_PANDAS = True
except ModuleNotFoundError:
    pd = None
    _HAS_PANDAS = False

# ── Ensure repo root is on sys.path ──────────────────────────────────────────
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

# Streamlit is already mocked by conftest.py; guard for direct invocation.
if "streamlit" not in sys.modules:
    _mock_st = MagicMock()
    _mock_st.session_state = {}
    _mock_st.cache_data = lambda *a, **kw: (lambda f: f)
    _mock_st.secrets = {}
    sys.modules["streamlit"] = _mock_st


# =============================================================================
# Section 1: URL generation
# =============================================================================

@unittest.skipUnless(_HAS_PANDAS, "pandas not installed")
class TestURLGeneration(unittest.TestCase):
    """Tests for data/nba_injury_pdf/_url.py generate_report_url()."""

    def setUp(self):
        from data.nba_injury_pdf._url import generate_report_url
        self.generate_report_url = generate_report_url

    # ── Test 1: legacy format (no minutes) ───────────────────────────────────
    def test_legacy_format_no_minutes(self):
        """Pre-cutoff timestamps must use %I%p format (e.g. '05PM', no minutes)."""
        ts = datetime.datetime(2025, 11, 15, 17, 0)
        url = self.generate_report_url(ts)
        self.assertIn("05PM", url)
        self.assertNotIn("05_00PM", url)

    # ── Test 2: new format (with minutes) ────────────────────────────────────
    def test_new_format_with_minutes(self):
        """Post-cutoff timestamps must use %I_%M%p format (e.g. '05_00PM')."""
        ts = datetime.datetime(2026, 3, 25, 17, 0)
        url = self.generate_report_url(ts)
        self.assertIn("05_00PM", url)

    # ── Test 3: correct date slug format ─────────────────────────────────────
    def test_date_slug_format(self):
        """The date portion of the URL must be in YYYY-MM-DD format."""
        ts = datetime.datetime(2026, 3, 25, 17, 0)
        url = self.generate_report_url(ts)
        self.assertIn("2026-03-25", url)

    def test_url_contains_base_stem(self):
        """URL must start with the NBA CDN base path."""
        ts = datetime.datetime(2026, 1, 10, 13, 30)
        url = self.generate_report_url(ts)
        self.assertTrue(url.startswith("https://ak-static.cms.nba.com/referee/injury/"))

    def test_url_ends_with_pdf(self):
        """URL must end with .pdf."""
        ts = datetime.datetime(2026, 1, 10, 13, 30)
        url = self.generate_report_url(ts)
        self.assertTrue(url.endswith(".pdf"))


# =============================================================================
# Section 2: DataFrame cleaning
# =============================================================================

@unittest.skipUnless(_HAS_PANDAS, "pandas not installed")
class TestCleaner(unittest.TestCase):
    """Tests for data/nba_injury_pdf/_cleaner.py clean_injury_report()."""

    def setUp(self):
        from data.nba_injury_pdf._cleaner import clean_injury_report
        self.clean = clean_injury_report

    def _make_df(self, rows):
        """Helper: build a DataFrame from a list of dicts using EXPECTED_COLUMNS."""
        cols = ["Game Date", "Game Time", "Matchup", "Team", "Player Name",
                "Current Status", "Reason"]
        return pd.DataFrame(rows, columns=cols)

    # ── Test 4: forward-fill ──────────────────────────────────────────────────
    def test_forward_fill(self):
        """Blank Game Date/Time/Matchup/Team on 2nd row must be filled from 1st."""
        rows = [
            {
                "Game Date": "2026-03-25", "Game Time": "7:30 PM",
                "Matchup": "LAL @ BOS", "Team": "Los Angeles Lakers",
                "Player Name": "LeBron James", "Current Status": "Out",
                "Reason": "Left Knee",
            },
            {
                "Game Date": "", "Game Time": "", "Matchup": "", "Team": "",
                "Player Name": "Anthony Davis", "Current Status": "GTD",
                "Reason": "Back",
            },
        ]
        df = self._make_df(rows)
        result = self.clean(df)
        self.assertEqual(result.iloc[1]["Game Date"], "2026-03-25")
        self.assertEqual(result.iloc[1]["Game Time"], "7:30 PM")
        self.assertEqual(result.iloc[1]["Matchup"], "LAL @ BOS")
        self.assertEqual(result.iloc[1]["Team"], "Los Angeles Lakers")

    # ── Test 5: 2-line multiline stitch ──────────────────────────────────────
    def test_multiline_stitch_two_rows(self):
        """Two-row split reason must be merged into one row."""
        rows = [
            {
                "Game Date": "2026-03-25", "Game Time": "7:30 PM",
                "Matchup": "LAL @ BOS", "Team": "Los Angeles Lakers",
                "Player Name": "LeBron James", "Current Status": "Out",
                "Reason": "Injury/Illness - Left Knee;",
            },
            {
                "Game Date": "", "Game Time": "", "Matchup": "", "Team": "",
                "Player Name": "", "Current Status": "",
                "Reason": "Soreness",
            },
        ]
        df = self._make_df(rows)
        result = self.clean(df)
        self.assertEqual(len(result), 1)
        self.assertIn("Soreness", result.iloc[0]["Reason"])
        self.assertIn("Left Knee", result.iloc[0]["Reason"])

    # ── Test 6: 3-line multiline stitch ──────────────────────────────────────
    def test_multiline_stitch_three_rows(self):
        """Three-row split reason must collapse to one row."""
        rows = [
            {
                "Game Date": "2026-03-25", "Game Time": "7:30 PM",
                "Matchup": "LAL @ BOS", "Team": "Los Angeles Lakers",
                "Player Name": "Anthony Davis", "Current Status": "Out",
                "Reason": "Part A",
            },
            {
                "Game Date": "", "Game Time": "", "Matchup": "", "Team": "",
                "Player Name": "", "Current Status": "",
                "Reason": "Part B",
            },
            {
                "Game Date": "", "Game Time": "", "Matchup": "", "Team": "",
                "Player Name": "", "Current Status": "",
                "Reason": "Part C",
            },
        ]
        df = self._make_df(rows)
        result = self.clean(df)
        self.assertEqual(len(result), 1)
        combined = result.iloc[0]["Reason"]
        self.assertIn("Part A", combined)
        self.assertIn("Part B", combined)
        self.assertIn("Part C", combined)

    # ── Test 7: NOT YET SUBMITTED removal ────────────────────────────────────
    def test_not_yet_submitted_removed(self):
        """Rows with Reason == 'NOT YET SUBMITTED' must be dropped."""
        rows = [
            {
                "Game Date": "2026-03-25", "Game Time": "7:30 PM",
                "Matchup": "LAL @ BOS", "Team": "Los Angeles Lakers",
                "Player Name": "LeBron James", "Current Status": "Out",
                "Reason": "NOT YET SUBMITTED",
            },
            {
                "Game Date": "2026-03-25", "Game Time": "7:30 PM",
                "Matchup": "LAL @ BOS", "Team": "Los Angeles Lakers",
                "Player Name": "Anthony Davis", "Current Status": "GTD",
                "Reason": "Back",
            },
        ]
        df = self._make_df(rows)
        result = self.clean(df)
        self.assertEqual(len(result), 1)
        self.assertEqual(result.iloc[0]["Player Name"], "Anthony Davis")


# =============================================================================
# Section 3: Column validation
# =============================================================================

@unittest.skipUnless(_HAS_PANDAS, "pandas not installed")
class TestColumnValidation(unittest.TestCase):
    """Tests for data/nba_injury_pdf/_parser.py validate_columns()."""

    def setUp(self):
        from data.nba_injury_pdf._parser import validate_columns
        from data.nba_injury_pdf._constants import EXPECTED_COLUMNS
        self.validate_columns = validate_columns
        self.expected = EXPECTED_COLUMNS

    # ── Test 8: all expected columns present ─────────────────────────────────
    def test_valid_columns_returns_true(self):
        """DataFrame with all expected columns must return True."""
        df = pd.DataFrame(columns=self.expected)
        self.assertTrue(self.validate_columns(df))

    # ── Test 9: missing column returns False ─────────────────────────────────
    def test_missing_column_returns_false(self):
        """DataFrame missing one expected column must return False."""
        cols = [c for c in self.expected if c != "Current Status"]
        df = pd.DataFrame(columns=cols)
        self.assertFalse(self.validate_columns(df))


# =============================================================================
# Section 4: RosterEngine._fetch_official_pdf_injuries()
# =============================================================================

@unittest.skipUnless(_HAS_PANDAS, "pandas not installed")
class TestRosterEnginePdfSource(unittest.TestCase):
    """Integration tests for RosterEngine._fetch_official_pdf_injuries()."""

    def _make_engine(self):
        from data.roster_engine import RosterEngine
        return RosterEngine()

    # ── Test 10: happy path ───────────────────────────────────────────────────
    def test_happy_path_two_players(self):
        """Method must return dict with 2 entries when get_report() succeeds."""
        mock_df = pd.DataFrame(
            [
                {
                    "Game Date": "2026-03-25",
                    "Game Time": "7:30 PM",
                    "Matchup": "LAL @ BOS",
                    "Team": "Los Angeles Lakers",
                    "Player Name": "LeBron James",
                    "Current Status": "Out",
                    "Reason": "Left Knee; Soreness",
                },
                {
                    "Game Date": "2026-03-25",
                    "Game Time": "7:30 PM",
                    "Matchup": "LAL @ BOS",
                    "Team": "Los Angeles Lakers",
                    "Player Name": "Anthony Davis",
                    "Current Status": "Questionable",
                    "Reason": "Back",
                },
            ]
        )
        engine = self._make_engine()
        with patch("data.roster_engine.RosterEngine._fetch_official_pdf_injuries",
                   wraps=engine._fetch_official_pdf_injuries):
            with patch("data.nba_injury_pdf.get_report", return_value=mock_df):
                result = engine._fetch_official_pdf_injuries()

        self.assertEqual(len(result), 2)

        lebron_key = "lebron james"
        self.assertIn(lebron_key, result)
        self.assertEqual(result[lebron_key]["status"], "Out")
        self.assertEqual(result[lebron_key]["source"], "nba-official-pdf")
        self.assertIn("Knee", result[lebron_key]["injury"])

        ad_key = "anthony davis"
        self.assertIn(ad_key, result)
        self.assertEqual(result[ad_key]["status"], "Questionable")

    # ── Test 11: ImportError graceful fallback ────────────────────────────────
    def test_import_error_returns_empty(self):
        """ImportError when importing get_report must return {}."""
        engine = self._make_engine()
        with patch.dict("sys.modules", {"data.nba_injury_pdf": None}):
            result = engine._fetch_official_pdf_injuries()
        self.assertEqual(result, {})

    # ── Test 12: empty DataFrame returns {} ──────────────────────────────────
    def test_empty_dataframe_returns_empty(self):
        """Empty DataFrame from get_report must return {}."""
        from data.nba_injury_pdf._constants import EXPECTED_COLUMNS
        empty_df = pd.DataFrame(columns=EXPECTED_COLUMNS)
        engine = self._make_engine()
        with patch("data.nba_injury_pdf.get_report", return_value=empty_df):
            result = engine._fetch_official_pdf_injuries()
        self.assertEqual(result, {})


# =============================================================================
# Section 5: RosterEngine.refresh() calls Source 0
# =============================================================================

@unittest.skipUnless(_HAS_PANDAS, "pandas not installed")
class TestRosterEngineRefreshSource0(unittest.TestCase):
    """Verify that refresh() calls _fetch_official_pdf_injuries and uses results."""

    # ── Test 13: Source 0 called and present in injury_map ───────────────────
    def test_refresh_calls_source0_and_merges(self):
        """Source 0 results must be present in _injury_map after refresh()."""
        from data.roster_engine import RosterEngine

        pdf_data = {
            "lebron james": {
                "status": "Out",
                "injury": "Left Knee",
                "team": "LAL",
                "return_date": "",
                "source": "nba-official-pdf",
                "game_date": "2026-03-25",
                "game_time": "7:30 PM",
                "matchup": "LAL @ BOS",
            }
        }

        engine = RosterEngine()
        with patch.object(engine, "_fetch_official_pdf_injuries", return_value=pdf_data) as mock_src0, \
             patch.object(engine, "_fetch_nba_api_injuries", return_value={}), \
             patch.object(engine, "_fetch_nba_api_rosters"):
            engine.refresh(["LAL"])

        mock_src0.assert_called_once()
        self.assertIn("lebron james", engine._injury_map)
        self.assertEqual(engine._injury_map["lebron james"]["source"], "nba-official-pdf")


# =============================================================================
# Section 6: check_report_available
# =============================================================================

@unittest.skipUnless(_HAS_PANDAS, "pandas not installed")
class TestCheckReportAvailable(unittest.TestCase):
    """Tests for data/nba_injury_pdf/report.py check_report_available()."""

    def setUp(self):
        from data.nba_injury_pdf.report import check_report_available
        self.check = check_report_available

    # ── Test 14: HEAD 200 → True ──────────────────────────────────────────────
    def test_head_200_returns_true(self):
        """HTTP 200 response must return True."""
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        with patch("requests.head", return_value=mock_resp):
            result = self.check(datetime.datetime(2026, 3, 25, 17, 0))
        self.assertTrue(result)

    # ── Test 15: HEAD 404 → False ─────────────────────────────────────────────
    def test_head_404_returns_false(self):
        """HTTP 404 response must return False."""
        mock_resp = MagicMock()
        mock_resp.status_code = 404
        with patch("requests.head", return_value=mock_resp):
            result = self.check(datetime.datetime(2026, 3, 25, 17, 0))
        self.assertFalse(result)


if __name__ == "__main__":
    unittest.main()
