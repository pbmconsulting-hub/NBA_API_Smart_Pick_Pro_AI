# ============================================================
# FILE: data/nba_injury_pdf/_exceptions.py
# PURPOSE: Custom exceptions for the NBA Injury Report PDF parser.
# ============================================================


class InjuryReportError(Exception):
    """Base exception for all NBA Injury Report PDF errors."""


class URLRetrievalError(InjuryReportError):
    """Raised when the PDF cannot be downloaded from the NBA CDN.

    Attributes:
        url:    The URL that failed.
        reason: Human-readable description of the failure.
    """

    def __init__(self, url: str, reason: str):
        self.url = url
        self.reason = reason
        super().__init__(f"Failed to retrieve '{url}': {reason}")


class DataValidationError(InjuryReportError):
    """Raised when the PDF cannot be parsed or fails column validation."""
