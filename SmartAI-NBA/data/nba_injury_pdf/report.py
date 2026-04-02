# ============================================================
# FILE: data/nba_injury_pdf/report.py
# PURPOSE: Public API for the Official NBA Injury Report PDF parser.
#          Provides auto-discovery of the latest report, availability
#          checks, and URL generation.
# ============================================================

import datetime

import pandas as pd

from data.nba_injury_pdf._cleaner import clean_injury_report
from data.nba_injury_pdf._constants import EXPECTED_COLUMNS
from data.nba_injury_pdf._exceptions import DataValidationError, URLRetrievalError
from data.nba_injury_pdf._parser import extract_tables_from_pdf, fetch_pdf_bytes, validate_columns
from data.nba_injury_pdf._url import generate_report_url

try:
    from utils.logger import get_logger
    _logger = get_logger(__name__)
except ImportError:
    import logging
    _logger = logging.getLogger(__name__)

# Known NBA injury report publication times in Eastern Time (hour, minute)
REPORT_TIMES_ET = [
    (17, 0),
    (13, 30),
    (11, 30),
    (9, 30),
]


def get_report(
    timestamp: datetime.datetime | None = None,
    auto_discover: bool = True,
) -> pd.DataFrame:
    """Fetch the latest official NBA Injury Report PDF as a DataFrame.

    When *auto_discover* is ``True`` and no *timestamp* is supplied the
    function probes each known publication time (newest first, skipping
    future times) for today, then falls back to yesterday's 5 PM report if
    all attempts fail.  An empty DataFrame (with the correct columns) is
    returned when nothing is found.

    When *auto_discover* is ``False`` and a *timestamp* is supplied the
    function fetches exactly that report and raises on failure.

    Args:
        timestamp:     Specific datetime to fetch.  Optional.
        auto_discover: When ``True`` (default) silently tries all known
                       report times instead of raising on failure.

    Returns:
        A cleaned ``pd.DataFrame`` with columns matching ``EXPECTED_COLUMNS``.

    Raises:
        URLRetrievalError:   If the PDF cannot be downloaded and
                             *auto_discover* is ``False``.
        DataValidationError: If the PDF cannot be parsed and
                             *auto_discover* is ``False``.
    """
    _empty = pd.DataFrame(columns=EXPECTED_COLUMNS)

    if timestamp is not None:
        if auto_discover:
            try:
                return _fetch_and_parse(timestamp)
            except (URLRetrievalError, DataValidationError):
                return _empty
        return _fetch_and_parse(timestamp)

    if not auto_discover:
        return _empty

    # Auto-discover: try today's known publication times in reverse order
    # (17:00 first, i.e. most recent)
    now_et = _now_et()
    today_date = now_et.date()

    for hour, minute in REPORT_TIMES_ET:
        candidate = datetime.datetime(
            today_date.year, today_date.month, today_date.day,
            hour, minute, 0,
        )
        if candidate > now_et:
            # Skip future times — the report won't be published yet
            continue
        try:
            df = _fetch_and_parse(candidate)
            if not df.empty:
                _logger.info(
                    f"NBA Injury PDF: found report at {candidate.strftime('%H:%M ET')}"
                )
                return df
        except (URLRetrievalError, DataValidationError) as exc:
            _logger.debug(f"NBA Injury PDF: {exc}")

    # Fallback: yesterday's 5 PM report
    yesterday = today_date - datetime.timedelta(days=1)
    fallback_ts = datetime.datetime(yesterday.year, yesterday.month, yesterday.day, 17, 0)
    try:
        df = _fetch_and_parse(fallback_ts)
        if not df.empty:
            _logger.info("NBA Injury PDF: using yesterday's 5 PM report as fallback")
            return df
    except (URLRetrievalError, DataValidationError) as exc:
        _logger.debug(f"NBA Injury PDF fallback: {exc}")

    _logger.info("NBA Injury PDF: no report found for today or yesterday")
    return _empty


def check_report_available(timestamp: datetime.datetime) -> bool:
    """Check whether an injury report PDF exists for the given datetime.

    Performs an HTTP HEAD request against the NBA CDN URL.

    Args:
        timestamp: The datetime whose report URL should be checked.

    Returns:
        ``True`` if the server responds with HTTP 200, ``False`` otherwise.
    """
    import requests as _requests
    from data.nba_injury_pdf._constants import REQUEST_HEADERS

    url = generate_report_url(timestamp)
    try:
        resp = _requests.head(url, headers=REQUEST_HEADERS, timeout=10)
        return resp.status_code == 200
    except Exception:
        return False


def generate_url(timestamp: datetime.datetime) -> str:
    """Return the PDF URL for a given datetime.

    Thin passthrough to :func:`~_url.generate_report_url`.

    Args:
        timestamp: The datetime for which to build the URL.

    Returns:
        The full URL string for the injury report PDF.
    """
    return generate_report_url(timestamp)


# ============================================================
# Internal helpers
# ============================================================

def _fetch_and_parse(timestamp: datetime.datetime) -> pd.DataFrame:
    """Fetch a PDF, parse it, validate columns, and clean the result.

    Args:
        timestamp: The exact datetime for which to retrieve the report.

    Returns:
        A cleaned ``pd.DataFrame``.

    Raises:
        URLRetrievalError:   On download failure.
        DataValidationError: On parse failure or missing expected columns.
    """
    url = generate_report_url(timestamp)
    pdf_bytes = fetch_pdf_bytes(url)
    df = extract_tables_from_pdf(pdf_bytes)
    if not validate_columns(df):
        raise DataValidationError(
            f"Unexpected columns in PDF from {url}. "
            f"Got: {list(df.columns)}; "
            f"Expected: {EXPECTED_COLUMNS}"
        )
    return clean_injury_report(df)


def _now_et() -> datetime.datetime:
    """Return the current datetime in US/Eastern time (naive, for simplicity)."""
    # Use UTC and subtract 4 or 5 hours.  For the purpose of selecting which
    # report times to probe this is accurate enough without a pytz dependency.
    try:
        import zoneinfo
        et_zone = zoneinfo.ZoneInfo("America/New_York")
        return datetime.datetime.now(tz=et_zone).replace(tzinfo=None)
    except Exception:
        # Fallback: UTC-5 (EST, conservative)
        utc_now = datetime.datetime.utcnow()
        return utc_now - datetime.timedelta(hours=5)
