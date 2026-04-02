# ============================================================
# FILE: data/nba_injury_pdf/_url.py
# PURPOSE: Generate the PDF URL for a given datetime, handling
#          the NBA CDN legacy vs new time-format change.
# ============================================================

import datetime

from data.nba_injury_pdf._constants import (
    URL_STEM,
    _LEGACY_FORMAT_CUTOFF,
    _NEW_FORMAT_START,
)


def generate_report_url(timestamp: datetime.datetime) -> str:
    """Build the full PDF URL for a given datetime.

    The NBA changed the time portion of the URL slug in late 2025:
      - Before 2025-12-19 15:30  → legacy  ``%I%p``    (e.g. ``03PM``)
      - After  2025-12-22 09:00  → new     ``%I_%M%p``  (e.g. ``03_00PM``)
      - The short gap between uses the new format.

    Args:
        timestamp: The datetime for which the report URL should be generated.

    Returns:
        The full URL string pointing to the injury report PDF.
    """
    date_str = timestamp.strftime("%Y-%m-%d")

    if timestamp < _LEGACY_FORMAT_CUTOFF:
        # Legacy format: no minutes (e.g. "03PM")
        time_str = timestamp.strftime("%I%p")
    else:
        # New format: with minutes (e.g. "03_00PM")
        time_str = timestamp.strftime("%I_%M%p")

    slug = f"{date_str}_{time_str}"
    return URL_STEM.format(slug=slug)
