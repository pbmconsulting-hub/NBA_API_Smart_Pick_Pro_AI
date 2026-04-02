# Third-Party Licenses

## mxufc29/nbainjuries

**Repository:** https://github.com/mxufc29/nbainjuries  
**License:** MIT  
**Used in:** `data/nba_injury_pdf/`

### Summary of adaptation

The `data/nba_injury_pdf/` package is adapted from the
[mxufc29/nbainjuries](https://github.com/mxufc29/nbainjuries) open-source
library.  The following elements were adapted:

| Element | Source file(s) | Notes |
|---|---|---|
| URL generation algorithm | `_url.py`, `_constants.py` | Season date boundaries and legacy/new URL format detection |
| Season date boundaries | `_constants.py` | `SEASON_DATES` dict covering 2021-22 through 2025-26 |
| Multiline reason stitching | `_cleaner.py` | Algorithm for merging split PDF rows |

The following elements were **rewritten from scratch**:

| Element | What changed |
|---|---|
| PDF parser | Replaced `tabula-py` (Java) with `pdfplumber` (pure Python); no hardcoded pixel coordinates |
| Public API | Rewritten to match this project's conventions (auto-discovery, graceful fallback, ET timezone) |
| Exception hierarchy | New `InjuryReportError` / `URLRetrievalError` / `DataValidationError` classes |
| `RosterEngine` integration | Source 0 wiring and severity-based merge are original to this project |

---

### Full MIT License Text

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
