"""
utils.py
--------
Shared utility functions used by multiple backend modules.

Consolidates common logic that was previously duplicated across
``initial_pull.py`` and ``data_updater.py``.
"""

from __future__ import annotations

import logging
import sqlite3

import pandas as pd

logger = logging.getLogger(__name__)


def parse_matchup_abbreviations(matchup: str) -> tuple[str | None, str | None]:
    """Parse home and away team abbreviations from an NBA matchup string.

    Handles the two standard formats returned by the NBA API:

    - ``'LAL vs. BOS'`` → ``('LAL', 'BOS')`` — left is home.
    - ``'LAL @ BOS'``   → ``('BOS', 'LAL')`` — right is home.

    Args:
        matchup: The raw matchup string from the API.

    Returns:
        A ``(home_abbrev, away_abbrev)`` tuple.  Both values are ``None``
        if the format is unrecognised.
    """
    if " vs. " in matchup:
        parts = matchup.split(" vs. ", 1)
        return parts[0].strip(), parts[1].strip()
    if " @ " in matchup:
        parts = matchup.split(" @ ", 1)
        # left team is away, right team is home
        return parts[1].strip(), parts[0].strip()
    return None, None


def upsert_dataframe(
    df: pd.DataFrame,
    table_name: str,
    conn: sqlite3.Connection,
) -> None:
    """Upsert a DataFrame into a table using ``INSERT OR REPLACE``.

    This centralises the repeated pattern of building an ``INSERT OR REPLACE``
    statement from a DataFrame's columns and executing it in bulk.

    Args:
        df: DataFrame whose columns match the target table's columns.
        table_name: SQLite table name.
        conn: Open SQLite connection.
    """
    if df.empty:
        logger.info("%s: no rows to upsert.", table_name)
        return
    cursor = conn.cursor()
    cols = list(df.columns)
    placeholders = ", ".join("?" for _ in cols)
    col_names = ", ".join(cols)
    sql = f"INSERT OR REPLACE INTO {table_name} ({col_names}) VALUES ({placeholders})"
    cursor.executemany(sql, df.itertuples(index=False, name=None))
    logger.info("%s: upserted %d rows.", table_name, len(df))


def get_new_rows(
    df: pd.DataFrame,
    existing: pd.DataFrame,
    on_cols: list[str],
) -> pd.DataFrame:
    """Return rows from *df* that don't exist in *existing* by *on_cols*.

    Uses a left-merge with an indicator column to identify rows in *df* that
    have no matching key combination in *existing*.

    Args:
        df: Candidate rows to check.
        existing: DataFrame of already-stored key columns.
        on_cols: Column names to join on.

    Returns:
        A copy of the rows from *df* that are not present in *existing*.
    """
    if existing.empty:
        return df
    merged = df.merge(existing, on=on_cols, how="left", indicator=True)
    return df[merged["_merge"] == "left_only"].copy()
