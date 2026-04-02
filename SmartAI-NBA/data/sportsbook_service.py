# ============================================================
# FILE: data/sportsbook_service.py
# PURPOSE: Thin delegation layer that routes all sportsbook /
#          player-prop retrieval through data/platform_fetcher.py.
#
#          platform_fetcher.py fetches live prop lines directly
#          from PrizePicks, Underdog Fantasy, and DraftKings
#          Pick6 (via The Odds API).
#
# PLATFORMS:
#   - PrizePicks  : public JSON API, no key required
#   - Underdog    : public JSON API, no key required
#   - DraftKings  : via The Odds API (free key, 500 req/month)
#
# USAGE:
#   from data.sportsbook_service import get_all_sportsbook_props
#   props = get_all_sportsbook_props()
# ============================================================

import logging as _logging

try:
    from utils.logger import get_logger
    _logger = get_logger(__name__)
except ImportError:
    _logger = _logging.getLogger(__name__)

# ── Re-export every public symbol from platform_fetcher ──────
from data.platform_fetcher import (            # noqa: F401 – re-exports
    build_cross_platform_comparison,
    recommend_best_platform,
    match_platform_player_to_csv,
    enrich_props_with_csv_names,
    find_new_players_from_props,
    extract_active_players_from_props,
    cross_reference_with_player_data,
    get_platform_confirmed_injuries,
    summarize_props_by_platform,
    quarantine_props,
    smart_filter_props,
    parse_alt_lines_from_platform_props,
    # Async infrastructure
    AIOHTTP_AVAILABLE,
    _ASYNC_SEMAPHORE_LIMIT,
    fetch_all_platforms_async as get_all_sportsbooks_async,
    # Quarantine constants
    QUARANTINE_ODDS_FLOOR,
    QUARANTINE_ODDS_CEILING,
    _EQUILIBRIUM_ODDS,
)

# Import platform_fetcher functions under private names for
# get_* wrapper compatibility.
from data.platform_fetcher import (
    fetch_prizepicks_props as _pf_prizepicks,
    fetch_underdog_props as _pf_underdog,
    fetch_draftkings_props as _pf_draftkings,
    fetch_all_platform_props as _pf_all,
)


# ============================================================
# Public API — name-compatible wrappers
# ============================================================

def get_prizepicks_props(league="NBA"):
    """Fetch live PrizePicks NBA prop lines."""
    return _pf_prizepicks(league=league)


def get_underdog_props(league="NBA"):
    """Fetch live Underdog Fantasy NBA prop lines."""
    return _pf_underdog(league=league)


def get_draftkings_props(api_key=None):
    """Fetch DraftKings Pick6 NBA prop lines via The Odds API."""
    return _pf_draftkings(api_key=api_key)


def get_all_sportsbook_props(
    include_prizepicks=True,
    include_underdog=True,
    include_draftkings=True,
    odds_api_key=None,
    progress_callback=None,
):
    """
    Fetch live prop lines from PrizePicks, Underdog Fantasy, and
    DraftKings Pick6 (via The Odds API).

    Args:
        include_prizepicks: Include PrizePicks props.
        include_underdog:   Include Underdog Fantasy props.
        include_draftkings: Include DraftKings Pick6 props.
        odds_api_key:       Odds API key for DraftKings.
        progress_callback:  Optional callable(current, total, message).

    Returns:
        list[dict]: Merged prop line dicts from all platforms.
    """
    return _pf_all(
        include_prizepicks=include_prizepicks,
        include_underdog=include_underdog,
        include_draftkings=include_draftkings,
        odds_api_key=odds_api_key,
        progress_callback=progress_callback,
    )
