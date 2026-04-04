"""
prizepicks_client.py
--------------------
Integration with the PrizePicks public API to fetch NBA player prop
projections for today's games.

PrizePicks exposes a public JSON endpoint that lists available
projections grouped by league.  No API key is required for reading
public projections.

Environment variable (optional):
    PRIZEPICKS_API_URL  — override the default API base URL.

Usage::

    from prizepicks_client import fetch_prizepicks_props, get_prizepicks_lines

    # Pull today's PrizePicks projections into the database
    n = fetch_prizepicks_props(conn)

    # Retrieve cached lines for a player
    lines = get_prizepicks_lines(conn, player_id=203999, stat_type="points")
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

_DEFAULT_API_URL = "https://api.prizepicks.com"
_REQUEST_TIMEOUT = 15

# PrizePicks stat type → our internal stat_type mapping
_PP_STAT_TO_INTERNAL: dict[str, str] = {
    "Points": "points",
    "Rebounds": "rebounds",
    "Assists": "assists",
    "3-Point Made": "threes",
    "3-Pt Made": "threes",
    "Blocked Shots": "blocks",
    "Blocks": "blocks",
    "Steals": "steals",
    "Turnovers": "turnovers",
    "Pts+Rebs+Asts": "points_rebounds_assists",
    "Pts+Asts": "points_assists",
    "Pts+Rebs": "points_rebounds",
    "Rebs+Asts": "rebounds_assists",
    "Blks+Stls": "blocks_steals",
    "Fantasy Score": "fantasy_score_pp",
    "Free Throws Made": "ftm",
    "FT Made": "ftm",
}

# NBA league ID on PrizePicks (NBA = 7)
_NBA_LEAGUE_ID = 7

# DFS_Prop_Lines table DDL (shared with underdog_client.py)
CREATE_DFS_PROP_LINES = """
CREATE TABLE IF NOT EXISTS DFS_Prop_Lines (
    player_name     TEXT    NOT NULL,
    player_id       INTEGER,
    stat_type       TEXT    NOT NULL,
    line            REAL    NOT NULL,
    platform        TEXT    NOT NULL,
    pick_type       TEXT    DEFAULT 'standard',
    game_date       TEXT    NOT NULL,
    fetched_at      TEXT    NOT NULL,
    PRIMARY KEY (player_name, stat_type, platform, pick_type, game_date)
);
"""

CREATE_DFS_PROP_LINES_INDEX = (
    "CREATE INDEX IF NOT EXISTS idx_dfs_prop_lines_player "
    "ON DFS_Prop_Lines (player_id, stat_type, platform, game_date)"
)


def ensure_dfs_prop_lines_table(conn: sqlite3.Connection) -> None:
    """Create the DFS_Prop_Lines table if it does not exist."""
    conn.execute(CREATE_DFS_PROP_LINES)
    conn.execute(CREATE_DFS_PROP_LINES_INDEX)


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


def get_api_url() -> str:
    """Return the PrizePicks API base URL."""
    return os.environ.get("PRIZEPICKS_API_URL", _DEFAULT_API_URL)


def fetch_prizepicks_props(conn: sqlite3.Connection) -> int:
    """Fetch PrizePicks NBA projections and cache them in DFS_Prop_Lines.

    Returns the number of rows inserted/updated.  On error this function
    logs a warning and returns 0 — it never raises.
    """
    ensure_dfs_prop_lines_table(conn)
    name_map = _build_name_to_id_map(conn)
    today_str = date.today().isoformat()
    now_ts = datetime.now(timezone.utc).isoformat()
    base_url = get_api_url()

    try:
        projections = _fetch_projections(base_url)
    except Exception:
        logger.exception("Failed to fetch PrizePicks projections.")
        return 0

    if not projections:
        logger.info("PrizePicks: no NBA projections returned.")
        return 0

    total_inserted = 0
    for proj in projections:
        player_name = proj.get("player_name", "")
        if not player_name:
            continue

        stat_label = proj.get("stat_type", "")
        stat_type = _PP_STAT_TO_INTERNAL.get(stat_label)
        if stat_type is None:
            continue

        line = proj.get("line")
        if line is None or float(line) <= 0:
            continue

        player_id = name_map.get(player_name.strip().lower())
        pick_type = proj.get("pick_type", "standard")

        try:
            conn.execute(
                """
                INSERT OR REPLACE INTO DFS_Prop_Lines
                    (player_name, player_id, stat_type, line,
                     platform, pick_type, game_date, fetched_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    player_name,
                    player_id,
                    stat_type,
                    float(line),
                    "PrizePicks",
                    pick_type,
                    today_str,
                    now_ts,
                ),
            )
            total_inserted += 1
        except sqlite3.Error:
            logger.warning(
                "DB insert failed for PrizePicks %s / %s",
                player_name,
                stat_type,
            )

    conn.commit()
    logger.info(
        "PrizePicks client: cached %d prop-line rows for %s.",
        total_inserted,
        today_str,
    )
    return total_inserted


def get_prizepicks_lines(
    conn: sqlite3.Connection,
    player_id: int,
    stat_type: str,
    game_date: Optional[str] = None,
) -> dict[str, float]:
    """Return ``{pick_type: line}`` for a player on PrizePicks.

    Example return: ``{"standard": 24.5, "demon": 26.5}``
    """
    if game_date is None:
        game_date = date.today().isoformat()

    rows = conn.execute(
        """
        SELECT pick_type, line FROM DFS_Prop_Lines
        WHERE player_id = ? AND stat_type = ? AND platform = 'PrizePicks'
              AND game_date = ?
        """,
        (player_id, stat_type, game_date),
    ).fetchall()

    result: dict[str, float] = {}
    for r in rows:
        pt = r[0] if isinstance(r, (list, tuple)) else r["pick_type"]
        ln = r[1] if isinstance(r, (list, tuple)) else r["line"]
        result[pt] = float(ln)
    return result


def get_prizepicks_consensus_line(
    conn: sqlite3.Connection,
    player_id: int,
    stat_type: str,
    game_date: Optional[str] = None,
) -> Optional[float]:
    """Return the standard PrizePicks line, or ``None``."""
    lines = get_prizepicks_lines(conn, player_id, stat_type, game_date)
    return lines.get("standard")


# ---------------------------------------------------------------------------
# Internal HTTP helpers
# ---------------------------------------------------------------------------


def _fetch_projections(base_url: str) -> list[dict]:
    """Fetch NBA projections from the PrizePicks public API.

    Returns a flat list of dicts with keys:
        player_name, stat_type, line, pick_type
    """
    url = f"{base_url}/projections"
    params = {
        "league_id": _NBA_LEAGUE_ID,
        "per_page": 250,
        "single_stat": "true",
    }
    headers = {
        "Accept": "application/json",
        "User-Agent": "SmartPicksProAI/1.0",
    }

    resp = requests.get(url, params=params, headers=headers, timeout=_REQUEST_TIMEOUT)
    resp.raise_for_status()
    payload = resp.json()

    # PrizePicks returns a JSON:API-style payload with ``data`` and ``included``.
    # ``included`` contains player objects; ``data`` contains projection objects.
    included = payload.get("included", [])
    data = payload.get("data", [])

    # Build a player lookup: id → player name
    player_lookup: dict[str, str] = {}
    for item in included:
        if item.get("type") == "new_player":
            pid = item.get("id", "")
            attrs = item.get("attributes", {})
            name = attrs.get("display_name") or attrs.get("name", "")
            if pid and name:
                player_lookup[str(pid)] = name

    results: list[dict] = []
    for item in data:
        if item.get("type") != "projection":
            continue
        attrs = item.get("attributes", {})
        relationships = item.get("relationships", {})

        # Resolve player name from relationships
        player_rel = relationships.get("new_player", {}).get("data", {})
        player_pp_id = str(player_rel.get("id", ""))
        player_name = player_lookup.get(player_pp_id, "")
        if not player_name:
            continue

        stat_label = attrs.get("stat_type", "")
        line_score = attrs.get("line_score")
        if line_score is None:
            continue

        # Pick type: standard (flex) vs demon vs goblin
        pick_type = attrs.get("projection_type", "standard") or "standard"

        results.append({
            "player_name": player_name,
            "stat_type": stat_label,
            "line": float(line_score),
            "pick_type": pick_type,
        })

    return results
