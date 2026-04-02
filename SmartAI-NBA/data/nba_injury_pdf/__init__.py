# ============================================================
# FILE: data/nba_injury_pdf/__init__.py
# PURPOSE: Package init — export the three public API functions.
# ============================================================

from data.nba_injury_pdf.report import (
    check_report_available,
    generate_url,
    get_report,
)

__all__ = ["get_report", "check_report_available", "generate_url"]
