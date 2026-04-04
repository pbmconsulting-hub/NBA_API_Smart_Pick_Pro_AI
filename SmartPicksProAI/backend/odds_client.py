"""
odds_client.py
--------------
Integration with The Odds API (free tier) to auto-populate player prop
lines for today's NBA games.

The free tier provides 500 requests/month.  This client caches results
in the ``Prop_Lines`` table so repeated calls during the same day do not
burn extra quota.

Environment variable:
    ODDS_API_KEY  — your API key from https://the-odds-api.com

Usage::

    from odds_client import fetch_todays_odds, get_cached_player_lines

    # Pull live odds into the database
    n = fetch_todays_odds(conn)

    # Retrieve cached lines for a player
    lines = get_cached_player_lines(conn, player_id=203999, stat_type="points")
"""

import logging
import os
import sqlite3
from datetime import date, datetime, timezone
from typing import Optional

import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# The Odds API base URL and settings
_BASE_URL = "https://api.the-odds-api.com"
_SPORT = "basketball_nba"
_REGIONS = "us"
_ODDS_FORMAT = "american"
_REQUEST_TIMEOUT = 15

# Mapping from The Odds API market names to our internal stat_type names
_MARKET_TO_STAT: dict[str, str] = {
    "player_points": "points",
    "player_rebounds": "rebounds",
    "player_assists": "assists",
    "player_threes": "threes",
    "player_blocks": "blocks",
    "player_steals": "steals",
    "player_turnovers": "turnovers",
    "player_points_rebounds_assists": "points_rebounds_assists",
    "player_points_rebounds": "points_rebounds",
    "player_points_assists": "points_assists",
    "player_rebounds_assists": "rebounds_assists",
    "player_blocks_steals": "blocks_steals",
}

# The prop markets we want to fetch
_PROP_MARKETS = list(_MARKET_TO_STAT.keys())

# Schema for caching odds
CREATE_PROP_LINES = """
CREATE TABLE IF NOT EXISTS Prop_Lines (
    player_name     TEXT    NOT NULL,
    player_id       INTEGER,
    stat_type       TEXT    NOT NULL,
    line            REAL    NOT NULL,
    over_price      INTEGER,
    under_price     INTEGER,
    bookmaker       TEXT    NOT NULL,
    game_date       TEXT    NOT NULL,
    fetched_at      TEXT    NOT NULL,
    PRIMARY KEY (player_name, stat_type, bookmaker, game_date)
);
"""

CREATE_PROP_LINES_INDEX = (
    "CREATE INDEX IF NOT EXISTS idx_prop_lines_player "
    "ON Prop_Lines (player_id, stat_type, game_date)"
)


def ensure_prop_lines_table(conn: sqlite3.Connection) -> None:
    """Create the Prop_Lines table if it does not exist."""
    conn.execute(CREATE_PROP_LINES)
    conn.execute(CREATE_PROP_LINES_INDEX)


# ---------------------------------------------------------------------------
# Player-name → player_id resolution
# ---------------------------------------------------------------------------


def _build_name_to_id_map(conn: sqlite3.Connection) -> dict[str, int]:
    """Build a lookup from lower-cased full_name to player_id."""
    rows = conn.execute(
        "SELECT player_id, full_name FROM Players WHERE full_name IS NOT NULL"
    ).fetchall()
    mapping: dict[str, int] = {}
    for r in rows:
        pid = r[0] if isinstance(r, (list, tuple)) else r["player_id"]
        name = r[1] if isinstance(r, (list, tuple)) else r["full_name"]
        if name:
            mapping[name.strip().lower()] = pid
    return mapping


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def get_api_key() -> Optional[str]:
    """Return the Odds API key from the environment, or ``None``."""
    return os.environ.get("ODDS_API_KEY")


def fetch_todays_odds(conn: sqlite3.Connection) -> int:
    """Fetch player prop odds for today's NBA games and cache them.

    Returns the number of prop-line rows inserted/updated.  If the API
    key is missing or the API returns an error this function logs a
    warning and returns 0 — it never raises.
    """
    api_key = get_api_key()
    if not api_key:
        logger.warning(
            "ODDS_API_KEY not set — skipping live odds fetch.  "
            "Set the environment variable to enable auto-populated prop lines."
        )
        return 0

    ensure_prop_lines_table(conn)
    name_map = _build_name_to_id_map(conn)
    today_str = date.today().isoformat()
    now_ts = datetime.now(timezone.utc).isoformat()

    # Step 1: Get today's NBA event IDs
    try:
        events = _fetch_events(api_key)
    except Exception:
        logger.exception("Failed to fetch NBA events from The Odds API.")
        return 0

    if not events:
        logger.info("No NBA events returned for today.")
        return 0

    # Step 2: For each event, fetch player prop markets
    total_inserted = 0
    for event in events:
        event_id = event.get("id", "")
        for market_key in _PROP_MARKETS:
            try:
                outcomes = _fetch_player_props(api_key, event_id, market_key)
            except Exception:
                logger.warning(
                    "Failed to fetch %s for event %s — skipping.",
                    market_key, event_id,
                )
                continue

            stat_type = _MARKET_TO_STAT.get(market_key, market_key)
            for row in outcomes:
                player_name = row["player_name"]
                player_id = name_map.get(player_name.strip().lower())
                try:
                    conn.execute(
                        """
                        INSERT OR REPLACE INTO Prop_Lines
                            (player_name, player_id, stat_type, line,
                             over_price, under_price, bookmaker,
                             game_date, fetched_at)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            player_name,
                            player_id,
                            stat_type,
                            row["line"],
                            row.get("over_price"),
                            row.get("under_price"),
                            row["bookmaker"],
                            today_str,
                            now_ts,
                        ),
                    )
                    total_inserted += 1
                except sqlite3.Error:
                    logger.warning(
                        "DB insert failed for %s / %s / %s",
                        player_name, stat_type, row["bookmaker"],
                    )

    conn.commit()
    logger.info(
        "Odds client: cached %d prop-line rows for %s.", total_inserted, today_str
    )
    return total_inserted


def get_cached_player_lines(
    conn: sqlite3.Connection,
    player_id: int,
    stat_type: str,
    game_date: Optional[str] = None,
) -> dict[str, float]:
    """Return a ``{bookmaker: line}`` dict for a player + stat from cache.

    This is the data structure expected by :func:`edge_detection.analyze_directional_forces`
    as the ``platform_lines`` parameter.
    """
    if game_date is None:
        game_date = date.today().isoformat()

    rows = conn.execute(
        """
        SELECT bookmaker, line FROM Prop_Lines
        WHERE player_id = ? AND stat_type = ? AND game_date = ?
        """,
        (player_id, stat_type, game_date),
    ).fetchall()

    result: dict[str, float] = {}
    for r in rows:
        bk = r[0] if isinstance(r, (list, tuple)) else r["bookmaker"]
        ln = r[1] if isinstance(r, (list, tuple)) else r["line"]
        result[bk] = float(ln)
    return result


def get_consensus_line(
    conn: sqlite3.Connection,
    player_id: int,
    stat_type: str,
    game_date: Optional[str] = None,
) -> Optional[float]:
    """Return the average (consensus) line across all bookmakers, or ``None``."""
    lines = get_cached_player_lines(conn, player_id, stat_type, game_date)
    if not lines:
        return None
    return sum(lines.values()) / len(lines)


# ---------------------------------------------------------------------------
# Internal HTTP helpers
# ---------------------------------------------------------------------------


def _fetch_events(api_key: str) -> list[dict]:
    """Fetch today's NBA game events."""
    url = f"{_BASE_URL}/v4/sports/{_SPORT}/events"
    params = {
        "apiKey": api_key,
        "dateFormat": "iso",
    }
    resp = requests.get(url, params=params, timeout=_REQUEST_TIMEOUT)
    resp.raise_for_status()
    events = resp.json()
    # Filter to today's events
    today_str = date.today().isoformat()
    return [
        e for e in events
        if e.get("commence_time", "").startswith(today_str)
    ]


def _fetch_player_props(
    api_key: str, event_id: str, market_key: str
) -> list[dict]:
    """Fetch player prop outcomes for one event + market.

    Returns a list of dicts, each with:
        player_name, line, over_price, under_price, bookmaker
    """
    url = f"{_BASE_URL}/v4/sports/{_SPORT}/events/{event_id}/odds"
    params = {
        "apiKey": api_key,
        "regions": _REGIONS,
        "markets": market_key,
        "oddsFormat": _ODDS_FORMAT,
    }
    resp = requests.get(url, params=params, timeout=_REQUEST_TIMEOUT)
    resp.raise_for_status()
    data = resp.json()

    results: list[dict] = []
    for bookmaker in data.get("bookmakers", []):
        bk_name = bookmaker.get("title", bookmaker.get("key", "unknown"))
        for market in bookmaker.get("markets", []):
            if market.get("key") != market_key:
                continue
            # Group outcomes by player description
            player_outcomes: dict[str, dict] = {}
            for outcome in market.get("outcomes", []):
                desc = outcome.get("description", "")
                if not desc:
                    continue
                if desc not in player_outcomes:
                    player_outcomes[desc] = {
                        "player_name": desc,
                        "line": None,
                        "over_price": None,
                        "under_price": None,
                        "bookmaker": bk_name,
                    }
                point = outcome.get("point")
                price = outcome.get("price")
                name = outcome.get("name", "").lower()
                if point is not None:
                    player_outcomes[desc]["line"] = float(point)
                if name == "over":
                    player_outcomes[desc]["over_price"] = price
                elif name == "under":
                    player_outcomes[desc]["under_price"] = price

            for po in player_outcomes.values():
                if po["line"] is not None:
                    results.append(po)

    return results
