# ATTRIBUTION

## mxufc29/nbainjuries (MIT License)

This package (`data/nba_injury_pdf/`) is adapted from the open-source project
[mxufc29/nbainjuries](https://github.com/mxufc29/nbainjuries), released under
the MIT License.

### What was adapted

- **URL generation logic** (`_url.py`): The algorithm for building the
  `Injury-Report_{date}_{time}.pdf` URL slug, including the season date
  boundaries and the legacy vs. new time-format change boundaries, was adapted
  from `nbainjuries._url` and `nbainjuries._constants`.
- **Season date boundaries** (`_constants.py`): The `SEASON_DATES` dict
  (regular season + playoff start/end datetimes for 2021-22 through 2025-26)
  was adapted from the same library.
- **Multiline reason stitching algorithm** (`_cleaner.py`):
  The logic for detecting and merging continuation rows (rows with a `Reason`
  but no `Player Name` or `Current Status`) was adapted from
  `nbainjuries._util.__clean_injrep()`.

### What was rewritten

- **PDF parser** (`_parser.py`): The original library uses `tabula-py` (a
  Java-based PDF parser requiring a JVM) with hardcoded pixel coordinates per
  season layout.  This implementation uses **`pdfplumber`** (pure Python, no
  Java dependency) with line-based extraction settings, making it suitable for
  Streamlit Cloud and other lightweight Python environments.
- **Public API** (`report.py`): Completely rewritten to match this project's
  conventions (try/except imports, `_logger`, auto-discover logic with ET
  timezone awareness, graceful empty-DataFrame fallback).
- **Exception hierarchy** (`_exceptions.py`): New custom exceptions
  (`InjuryReportError`, `URLRetrievalError`, `DataValidationError`) replacing
  the original library's error handling.
- **`RosterEngine` integration** (`data/roster_engine.py`): Source 0
  wiring and severity-based merge are entirely original to this project.

---

### Original MIT License

```
MIT License

Copyright (c) 2022 mxufc29

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
```
