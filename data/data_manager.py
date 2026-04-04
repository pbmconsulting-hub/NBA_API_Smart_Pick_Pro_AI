# ============================================================
# FILE: data/data_manager.py
# PURPOSE: Centralised data management for SmartPicksProAI.
#          Handles loading, caching, and updating seed data files
#          and provides a unified interface for data access.
# ============================================================

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import pandas as pd

_logger = logging.getLogger(__name__)

# ── Data directory paths ────────────────────────────────────────────────

DATA_DIR = Path(__file__).resolve().parent
PKG_DATA_DIR = DATA_DIR.parent / "SmartPicksProAI" / "data"
RAW_DIR = PKG_DATA_DIR / "raw" if PKG_DATA_DIR.exists() else DATA_DIR / "raw"
PROCESSED_DIR = PKG_DATA_DIR / "processed" if PKG_DATA_DIR.exists() else DATA_DIR / "processed"
ML_READY_DIR = PKG_DATA_DIR / "ml_ready" if PKG_DATA_DIR.exists() else DATA_DIR / "ml_ready"

# ── Seed data files ─────────────────────────────────────────────────────

_SEED_FILES = {
    "teams": DATA_DIR / "teams.csv",
    "defensive_ratings": DATA_DIR / "defensive_ratings.csv",
    "sample_players": DATA_DIR / "sample_players.csv",
    "sample_props": DATA_DIR / "sample_props.csv",
}

# ── In-memory cache ─────────────────────────────────────────────────────

_cache: dict[str, pd.DataFrame] = {}


def _ensure_directories() -> None:
    """Create data subdirectories if they don't exist."""
    for d in (RAW_DIR, PROCESSED_DIR, ML_READY_DIR):
        d.mkdir(parents=True, exist_ok=True)


def load_csv(name: str, *, force_reload: bool = False) -> pd.DataFrame:
    """Load a named seed CSV into a DataFrame.

    Parameters
    ----------
    name:
        Key from ``_SEED_FILES`` (e.g. ``"teams"``, ``"defensive_ratings"``).
    force_reload:
        Bypass the in-memory cache.

    Returns
    -------
    pd.DataFrame
        The loaded data, or an empty DataFrame if the file is missing.
    """
    if not force_reload and name in _cache:
        return _cache[name].copy()

    path = _SEED_FILES.get(name)
    if path is None:
        _logger.warning("Unknown seed file: %s", name)
        return pd.DataFrame()

    if not path.exists():
        _logger.warning("Seed file not found: %s", path)
        return pd.DataFrame()

    try:
        df = pd.read_csv(path)
        _cache[name] = df
        _logger.info("Loaded %s: %d rows from %s", name, len(df), path)
        return df.copy()
    except Exception as exc:
        _logger.error("Error loading %s: %s", path, exc)
        return pd.DataFrame()


def get_teams() -> pd.DataFrame:
    """Load the teams reference data."""
    return load_csv("teams")


def get_defensive_ratings() -> pd.DataFrame:
    """Load team defensive ratings."""
    return load_csv("defensive_ratings")


def get_sample_players() -> pd.DataFrame:
    """Load sample player data for testing."""
    return load_csv("sample_players")


def get_sample_props() -> pd.DataFrame:
    """Load sample prop lines for testing."""
    return load_csv("sample_props")


def get_team_by_abbreviation(abbrev: str) -> dict[str, Any]:
    """Look up a single team by abbreviation.

    Returns an empty dict if not found.
    """
    teams = get_teams()
    if teams.empty:
        return {}
    match = teams[teams["team_abbreviation"] == abbrev.upper()]
    if match.empty:
        return {}
    return match.iloc[0].to_dict()


def get_defensive_rating(team_abbrev: str) -> float | None:
    """Return the defensive rating for a team, or None."""
    ratings = get_defensive_ratings()
    if ratings.empty:
        return None
    match = ratings[ratings["team_abbreviation"] == team_abbrev.upper()]
    if match.empty:
        return None
    return float(match.iloc[0]["defensive_rating"])


def save_dataframe(
    df: pd.DataFrame,
    name: str,
    directory: str = "processed",
) -> Path | None:
    """Save a DataFrame as CSV in the specified data subdirectory.

    Parameters
    ----------
    df:
        Data to save.
    name:
        Filename (without extension).
    directory:
        One of ``"raw"``, ``"processed"``, ``"ml_ready"``.

    Returns
    -------
    Path | None
        The path to the saved file, or None on failure.
    """
    dir_map = {"raw": RAW_DIR, "processed": PROCESSED_DIR, "ml_ready": ML_READY_DIR}
    target_dir = dir_map.get(directory)
    if target_dir is None:
        _logger.error("Unknown directory: %s", directory)
        return None

    _ensure_directories()
    path = target_dir / f"{name}.csv"
    try:
        df.to_csv(path, index=False)
        _logger.info("Saved %d rows to %s", len(df), path)
        return path
    except Exception as exc:
        _logger.error("Error saving %s: %s", path, exc)
        return None


def list_data_files(directory: str = "processed") -> list[str]:
    """List CSV files in a data subdirectory."""
    dir_map = {"raw": RAW_DIR, "processed": PROCESSED_DIR, "ml_ready": ML_READY_DIR}
    target_dir = dir_map.get(directory)
    if target_dir is None or not target_dir.exists():
        return []
    return sorted(f.name for f in target_dir.glob("*.csv"))


def clear_cache() -> None:
    """Clear the in-memory data cache."""
    _cache.clear()
    _logger.info("Data cache cleared.")
