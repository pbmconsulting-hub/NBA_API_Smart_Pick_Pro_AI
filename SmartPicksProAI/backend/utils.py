"""
utils.py
--------
Shared utility functions used by multiple backend modules.

Consolidates common logic that was previously duplicated across
``initial_pull.py`` and ``data_updater.py``.
"""

from __future__ import annotations


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
