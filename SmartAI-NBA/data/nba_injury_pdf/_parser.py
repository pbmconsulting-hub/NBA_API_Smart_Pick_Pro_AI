# ============================================================
# FILE: data/nba_injury_pdf/_parser.py
# PURPOSE: Download and extract tabular data from the official
#          NBA Injury Report PDF using pdfplumber (pure Python).
# ============================================================

import io

import pandas as pd
import requests

from data.nba_injury_pdf._constants import EXPECTED_COLUMNS, REQUEST_HEADERS
from data.nba_injury_pdf._exceptions import DataValidationError, URLRetrievalError

# Wrap pdfplumber import so the app degrades gracefully if the package is absent.
try:
    import pdfplumber as _pdfplumber
    _PDFPLUMBER_AVAILABLE = True
except ImportError:
    _pdfplumber = None  # type: ignore[assignment]
    _PDFPLUMBER_AVAILABLE = False


def fetch_pdf_bytes(url: str, timeout: int = 15) -> bytes:
    """Download the PDF from the NBA CDN.

    Args:
        url:     Full URL to the injury report PDF.
        timeout: Request timeout in seconds.

    Returns:
        Raw PDF bytes.

    Raises:
        URLRetrievalError: If the request fails (non-200 status or network error).
    """
    try:
        resp = requests.get(url, headers=REQUEST_HEADERS, timeout=timeout)
        if resp.status_code != 200:
            raise URLRetrievalError(url, f"HTTP {resp.status_code}")
        return resp.content
    except URLRetrievalError:
        raise
    except Exception as exc:
        raise URLRetrievalError(url, str(exc)) from exc


def extract_tables_from_pdf(pdf_bytes: bytes) -> pd.DataFrame:
    """Parse all tables from a PDF and return a unified DataFrame.

    Uses ``pdfplumber`` with line-based extraction settings that work well
    with the NBA's injury report layout.

    First row of the first table is used as the column header.  Repeated
    header rows on subsequent pages are discarded automatically.

    Args:
        pdf_bytes: Raw bytes of the PDF.

    Returns:
        A ``pd.DataFrame`` containing all rows from all pages.

    Raises:
        DataValidationError: If ``pdfplumber`` is not installed, or if the
                             PDF contains no extractable tables.
    """
    if not _PDFPLUMBER_AVAILABLE:
        raise DataValidationError(
            "pdfplumber is not installed. "
            "Run `pip install pdfplumber` to enable PDF parsing."
        )

    table_settings = {
        "vertical_strategy": "lines",
        "horizontal_strategy": "lines",
        "snap_tolerance": 5,
        "join_tolerance": 5,
    }

    header: list | None = None
    all_rows: list[list] = []

    with _pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        for page in pdf.pages:
            tables = page.extract_tables(table_settings)
            for table in (tables or []):
                if not table:
                    continue
                for row in table:
                    # Normalise cell values: None → ""
                    cleaned = [str(cell).strip() if cell is not None else "" for cell in row]

                    if header is None:
                        # First non-empty row across the whole document = header
                        if any(cleaned):
                            header = cleaned
                    else:
                        # Skip repeated header rows
                        if cleaned == header:
                            continue
                        # Handle column count mismatches from merged cells
                        if len(cleaned) < len(header):
                            cleaned += [""] * (len(header) - len(cleaned))
                        elif len(cleaned) > len(header):
                            cleaned = cleaned[: len(header)]
                        all_rows.append(cleaned)

    if header is None or not all_rows:
        raise DataValidationError(
            "No tables found in the PDF. "
            "The document may be empty or in an unexpected format."
        )

    return pd.DataFrame(all_rows, columns=header)


def validate_columns(df: pd.DataFrame) -> bool:
    """Check that all expected columns are present in the DataFrame.

    Args:
        df: The DataFrame to validate.

    Returns:
        ``True`` if all ``EXPECTED_COLUMNS`` are present; ``False`` otherwise.
    """
    return all(col in df.columns for col in EXPECTED_COLUMNS)
