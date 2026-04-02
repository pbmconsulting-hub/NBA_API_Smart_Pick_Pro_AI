# ============================================================
# FILE: data/nba_injury_pdf/_cleaner.py
# PURPOSE: Clean and normalise the raw DataFrame extracted from
#          the official NBA Injury Report PDF.
#
# Adapted from mxufc29/nbainjuries _util.__clean_injrep() (MIT License).
# See data/nba_injury_pdf/ATTRIBUTION.md for attribution details.
# ============================================================

import pandas as pd


def clean_injury_report(df: pd.DataFrame) -> pd.DataFrame:
    """Clean a raw injury-report DataFrame returned by the PDF parser.

    Applies five normalisation steps in order:

    1. **Forward-fill** ``Game Date``, ``Game Time``, ``Matchup``, and
       ``Team`` columns — the PDF only prints these once per game section.
    2. **Remove** rows whose ``Reason`` equals ``"NOT YET SUBMITTED"``.
    3. **Stitch** multiline reason cells — long reasons are sometimes split
       across two or three PDF rows; those continuation rows are merged.
    4. **Drop** rows where ``Player Name`` is NaN or empty (blank spacers).
    5. **Strip** leading/trailing whitespace from all string columns.

    Args:
        df: Raw DataFrame produced by :func:`~_parser.extract_tables_from_pdf`.

    Returns:
        A cleaned copy of the DataFrame (original is not modified).
    """
    df = df.copy()

    # ── Step 1: forward-fill game-context columns ─────────────────
    _ffill_cols = ["Game Date", "Game Time", "Matchup", "Team"]
    for col in _ffill_cols:
        if col in df.columns:
            df[col] = df[col].replace("", pd.NA).ffill()

    # ── Step 2: remove "NOT YET SUBMITTED" placeholder rows ───────
    if "Reason" in df.columns:
        df = df[df["Reason"].str.strip() != "NOT YET SUBMITTED"].copy()

    # ── Step 3: stitch multiline reasons ──────────────────────────
    df = _stitch_multiline_reasons(df)

    # ── Step 4: drop rows with no player name ─────────────────────
    if "Player Name" in df.columns:
        df = df[
            df["Player Name"].notna()
            & (df["Player Name"].str.strip() != "")
        ].copy()

    # ── Step 5: strip whitespace from all string columns ──────────
    for col in df.select_dtypes(include=["object", "string"]).columns:
        df[col] = df[col].astype(str).str.strip()
        df[col] = df[col].replace("nan", "")

    df = df.reset_index(drop=True)
    return df


def _stitch_multiline_reasons(df: pd.DataFrame) -> pd.DataFrame:
    """Merge continuation rows whose Reason text was split across PDF lines.

    A row is treated as a *continuation* when it has a non-empty ``Reason``
    value but both ``Player Name`` and ``Current Status`` are blank/NaN.
    The continuation row's Reason text is appended (space-separated) to the
    most-recent full player row, then the continuation row is dropped.

    Args:
        df: DataFrame that has already had "NOT YET SUBMITTED" rows removed.

    Returns:
        The DataFrame with continuation rows merged and dropped.
    """
    if "Reason" not in df.columns:
        return df

    rows_to_drop: list[int] = []
    last_valid_idx: int | None = None

    for idx, row in df.iterrows():
        player_name = str(row.get("Player Name", "") or "").strip()
        status      = str(row.get("Current Status", "") or "").strip()
        reason      = str(row.get("Reason", "") or "").strip()

        is_continuation = (
            reason != ""
            and player_name == ""
            and status == ""
        )

        if is_continuation and last_valid_idx is not None:
            existing_reason = str(df.at[last_valid_idx, "Reason"] or "").strip()
            df.at[last_valid_idx, "Reason"] = (
                f"{existing_reason} {reason}".strip()
            )
            rows_to_drop.append(idx)
        elif player_name:
            last_valid_idx = idx

    if rows_to_drop:
        df = df.drop(index=rows_to_drop).copy()

    return df
