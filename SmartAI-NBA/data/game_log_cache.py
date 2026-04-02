# data/game_log_cache.py
# Persistent cache for player game logs across browser sessions.
# Stores game logs to a JSON file so they survive page refreshes.
# Standard library only — no numpy/scipy/pandas.

import json
import os
import datetime


_CACHE_FILE = os.path.join(os.path.dirname(__file__), "game_logs_cache.json")
_CACHE_TTL_HOURS = 24  # Cache is stale after 24 hours


def save_game_logs_to_cache(player_name, game_logs):
    """
    Save a player's game logs to the persistent JSON cache.

    Args:
        player_name (str): Player's name (used as cache key)
        game_logs (list of dict): Game log entries to cache

    Returns:
        bool: True if saved successfully, False on error
    """
    if not player_name or not game_logs:
        return False

    try:
        cache = _load_cache_file()
        key = player_name.strip().lower()
        cache[key] = {
            "player_name": player_name,
            "game_logs": game_logs,
            "cached_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        }
        _write_cache_file(cache)
        return True
    except Exception:
        return False


def load_game_logs_from_cache(player_name):
    """
    Load a player's game logs from the persistent JSON cache.

    Args:
        player_name (str): Player's name

    Returns:
        tuple: (game_logs, is_stale)
            game_logs: list of dict (empty list if not cached)
            is_stale: bool (True if cache is older than TTL)
    """
    if not player_name:
        return [], True

    try:
        cache = _load_cache_file()
        key = player_name.strip().lower()
        entry = cache.get(key)

        if not entry:
            return [], True

        game_logs = entry.get("game_logs", [])
        cached_at_str = entry.get("cached_at", "")

        is_stale = True
        if cached_at_str:
            try:
                cached_at = datetime.datetime.fromisoformat(cached_at_str)
                now_utc = datetime.datetime.now(datetime.timezone.utc)
                if cached_at.tzinfo is None:
                    cached_at = cached_at.replace(tzinfo=datetime.timezone.utc)
                age = now_utc - cached_at
                is_stale = age.total_seconds() > _CACHE_TTL_HOURS * 3600
            except (ValueError, TypeError):
                is_stale = True

        return game_logs, is_stale

    except Exception:
        return [], True


def get_all_cached_players():
    """
    Get a list of all player names currently in the cache.

    Returns:
        list of str: Player names with cached game logs
    """
    try:
        cache = _load_cache_file()
        return [entry.get("player_name", key) for key, entry in cache.items()]
    except Exception:
        return []


def is_cache_stale(player_name):
    """
    Check if a player's cached game logs are stale.

    Args:
        player_name (str): Player's name

    Returns:
        bool: True if cache is stale or missing
    """
    _, is_stale = load_game_logs_from_cache(player_name)
    return is_stale


def get_player_game_logs_from_etl(player_name_or_id, limit=None):
    """
    Retrieve game logs from the ETL SQLite database.

    Parameters
    ----------
    player_name_or_id : str or int
        Player name (fuzzy matched) or integer player ID.
    limit : int | None
        Maximum number of recent games to return.

    Returns
    -------
    list[dict] : Game log entries, or empty list if not available.
    """
    try:
        from data.etl_data_service import (
            get_player_game_logs,
            get_player_by_id,
            get_player_by_name,
        )

        if isinstance(player_name_or_id, int) or (
            isinstance(player_name_or_id, str) and player_name_or_id.isdigit()
        ):
            return get_player_game_logs(int(player_name_or_id), limit=limit)

        # Name-based lookup — resolve to player_id first
        player = get_player_by_name(str(player_name_or_id))
        if player is None:
            return []
        return get_player_game_logs(player["player_id"], limit=limit)
    except Exception:
        return []


def _load_cache_file():
    """Load the raw cache JSON file.

    Also cleans up any stale ``.tmp`` file left behind by a crashed
    ``_write_cache_file`` call — a lingering ``.tmp`` is harmless but
    wastes disk space and could confuse operators.
    """
    _tmp = _CACHE_FILE + ".tmp"
    if os.path.exists(_tmp):
        try:
            os.remove(_tmp)
        except OSError:
            pass
    if not os.path.exists(_CACHE_FILE):
        return {}
    try:
        with open(_CACHE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError):
        return {}


def _write_cache_file(cache):
    """Write the cache dict to disk."""
    tmp_path = _CACHE_FILE + ".tmp"
    try:
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(cache, f, ensure_ascii=False)
        os.replace(tmp_path, _CACHE_FILE)
    except OSError:
        try:
            os.remove(tmp_path)
        except OSError:
            pass
