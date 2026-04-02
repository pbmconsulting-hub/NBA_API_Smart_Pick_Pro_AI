# ============================================================
# FILE: data/nba_injury_pdf/_constants.py
# PURPOSE: Module-level constants for the NBA Injury Report PDF parser.
# ============================================================

import datetime

# HTTP headers for NBA CDN requests
REQUEST_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "application/pdf,*/*",
}

# URL template for the official NBA Injury Report PDF
URL_STEM = "https://ak-static.cms.nba.com/referee/injury/Injury-Report_{slug}.pdf"

# Expected column headers in the parsed PDF table
EXPECTED_COLUMNS = [
    "Game Date",
    "Game Time",
    "Matchup",
    "Team",
    "Player Name",
    "Current Status",
    "Reason",
]

# Season date boundaries keyed by season code (e.g. "2425" = 2024-25 season)
SEASON_DATES = {
    "2122": {
        "reg_start":     datetime.datetime(2021, 10, 19),
        "reg_end":       datetime.datetime(2022, 4, 10),
        "playoff_start": datetime.datetime(2022, 4, 16),
        "playoff_end":   datetime.datetime(2022, 6, 20),
    },
    "2223": {
        "reg_start":     datetime.datetime(2022, 10, 18),
        "reg_end":       datetime.datetime(2023, 4, 9),
        "playoff_start": datetime.datetime(2023, 4, 15),
        "playoff_end":   datetime.datetime(2023, 6, 18),
    },
    "2324": {
        "reg_start":     datetime.datetime(2023, 10, 24),
        "reg_end":       datetime.datetime(2024, 4, 14),
        "playoff_start": datetime.datetime(2024, 4, 20),
        "playoff_end":   datetime.datetime(2024, 6, 25),
    },
    "2425": {
        "reg_start":     datetime.datetime(2024, 10, 22),
        "reg_end":       datetime.datetime(2025, 4, 13),
        "playoff_start": datetime.datetime(2025, 4, 19),
        "playoff_end":   datetime.datetime(2025, 6, 25),
    },
    "2526": {
        "reg_start":     datetime.datetime(2025, 10, 21),
        "reg_end":       datetime.datetime(2026, 4, 12),
        "playoff_start": datetime.datetime(2026, 4, 18),
        "playoff_end":   datetime.datetime(2026, 6, 24),
    },
}

# URL time-format change boundaries.
# Before this datetime: use legacy "%I%p" format  (e.g. "03PM", no minutes)
_LEGACY_FORMAT_CUTOFF = datetime.datetime(2025, 12, 19, 15, 30)
# After this datetime:  use new "%I_%M%p" format  (e.g. "03_00PM", with minutes).
# The gap between _LEGACY_FORMAT_CUTOFF and _NEW_FORMAT_START also uses the new
# format.  _NEW_FORMAT_START is retained for documentation and potential future
# use (e.g. when season-specific boundary detection is needed).
_NEW_FORMAT_START = datetime.datetime(2025, 12, 22, 9, 0)
