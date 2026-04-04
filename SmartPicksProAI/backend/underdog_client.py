"""
underdog_client.py
------------------
Integration with the Underdog Fantasy public API to fetch NBA player
prop projections (pick'em lines) for today's games.

Underdog Fantasy exposes a public JSON endpoint for their pick'em
slate.  No API key is required for reading the public slate.

Environment variable (optional):
    UNDERDOG_API_URL  — override the default API base URL.

Usage::

    from underdog_client import fetch_underdog_props, get_underdog_lines

    # Pull today's Underdog projections into the database
    n = fetch_underdog_props(conn)

    # Retrieve cached lines for a player
    lines = get_underdog_lines(conn, player_id=203999, stat_type="points")
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

_DEFAULT_API_URL = "https://api.underdogfantasy.com"
_REQUEST_TIMEOUT = 15

# Underdog stat type → our internal stat_type mapping
_UD_STAT_TO_INTERNAL: dict[str, str] = {
    "points": "points",
    "Points": "points",
    "rebounds": "rebounds",
    "Rebounds": "rebounds",
    "assists": "assists",
    "Assists": "assists",
    "three_pointers_made": "threes",
    "3-Pointers Made": "threes",
    "3PM": "threes",
    "blocked_shots": "blocks",
    "Blocks": "blocks",
    "steals": "steals",
    "Steals": "steals",
    "turnovers": "turnovers",
    "Turnovers": "turnovers",
    "pts_rebs_asts": "points_rebounds_assists",
    "Pts+Rebs+Asts": "points_rebounds_assists",
    "pts_asts": "points_assists",
    "Pts+Asts": "points_assists",
    "pts_rebs": "points_rebounds",
    "Pts+Rebs": "points_rebounds",
    "rebs_asts": "rebounds_assists",
    "Rebs+Asts": "rebounds_assists",
    "blks_stls": "blocks_steals",
    "Blks+Stls": "blocks_steals",
    "fantasy_points": "fantasy_score_ud",
    "Fantasy Points": "fantasy_score_ud",
    "free_throws_made": "ftm",
    "Free Throws Made": "ftm",
    "FTM": "ftm",
    "double_doubles": "double_double",
    "Double Doubles": "double_double",
}


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
# DFS table helper — reuses the same table as prizepicks_client.py
# ---------------------------------------------------------------------------


def _ensure_dfs_table(conn: sqlite3.Connection) -> None:
    """Create DFS_Prop_Lines if needed (idempotent)."""
    try:
        from prizepicks_client import ensure_dfs_prop_lines_table
        ensure_dfs_prop_lines_table(conn)
    except ImportError:
        # Fallback: create inline
        conn.execute("""
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
            )
        """)
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_dfs_prop_lines_player "
            "ON DFS_Prop_Lines (player_id, stat_type, platform, game_date)"
        )


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def get_api_url() -> str:
    """Return the Underdog Fantasy API base URL."""
    return os.environ.get("UNDERDOG_API_URL", _DEFAULT_API_URL)


def fetch_underdog_props(conn: sqlite3.Connection) -> int:
    """Fetch Underdog Fantasy NBA pick'em lines and cache them.

    Returns the number of rows inserted/updated.  On error this function
    logs a warning and returns 0 — it never raises.
    """
    _ensure_dfs_table(conn)
    name_map = _build_name_to_id_map(conn)
    today_str = date.today().isoformat()
    now_ts = datetime.now(timezone.utc).isoformat()
    base_url = get_api_url()

    try:
        picks = _fetch_pickem_lines(base_url)
    except Exception:
        logger.exception("Failed to fetch Underdog Fantasy pick'em lines.")
        return 0

    if not picks:
        logger.info("Underdog Fantasy: no NBA pick'em lines returned.")
        return 0

    total_inserted = 0
    for pick in picks:
        player_name = pick.get("player_name", "")
        if not player_name:
            continue

        stat_label = pick.get("stat_type", "")
        stat_type = _UD_STAT_TO_INTERNAL.get(stat_label)
        if stat_type is None:
            continue

        line = pick.get("line")
        if line is None or float(line) <= 0:
            continue

        player_id = name_map.get(player_name.strip().lower())
        pick_type = pick.get("pick_type", "standard")

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
                    "Underdog Fantasy",
                    pick_type,
                    today_str,
                    now_ts,
                ),
            )
            total_inserted += 1
        except sqlite3.Error:
            logger.warning(
                "DB insert failed for Underdog %s / %s",
                player_name,
                stat_type,
            )

    conn.commit()
    logger.info(
        "Underdog client: cached %d prop-line rows for %s.",
        total_inserted,
        today_str,
    )
    return total_inserted


def get_underdog_lines(
    conn: sqlite3.Connection,
    player_id: int,
    stat_type: str,
    game_date: Optional[str] = None,
) -> dict[str, float]:
    """Return ``{pick_type: line}`` for a player on Underdog Fantasy.

    Example return: ``{"standard": 24.5, "higher": 26.5, "lower": 22.5}``
    """
    if game_date is None:
        game_date = date.today().isoformat()

    rows = conn.execute(
        """
        SELECT pick_type, line FROM DFS_Prop_Lines
        WHERE player_id = ? AND stat_type = ?
              AND platform = 'Underdog Fantasy'
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


def get_underdog_consensus_line(
    conn: sqlite3.Connection,
    player_id: int,
    stat_type: str,
    game_date: Optional[str] = None,
) -> Optional[float]:
    """Return the standard Underdog line, or ``None``."""
    lines = get_underdog_lines(conn, player_id, stat_type, game_date)
    return lines.get("standard")


# ---------------------------------------------------------------------------
# Internal HTTP helpers
# ---------------------------------------------------------------------------


def _fetch_pickem_lines(base_url: str) -> list[dict]:
    """Fetch NBA pick'em lines from the Underdog Fantasy public API.

    Returns a flat list of dicts with keys:
        player_name, stat_type, line, pick_type
    """
    url = f"{base_url}/v1/pick_em/lines"
    headers = {
        "Accept": "application/json",
        "User-Agent": "SmartPicksProAI/1.0",
    }

    resp = requests.get(url, headers=headers, timeout=_REQUEST_TIMEOUT)
    resp.raise_for_status()
    payload = resp.json()

    # Underdog returns a payload with:
    #   "appearances" — player appearance objects (player_id → player info)
    #   "over_under_lines" — the actual pick'em lines
    #   "players" — player info lookup
    appearances = payload.get("appearances", [])
    over_under_lines = payload.get("over_under_lines", [])
    players = payload.get("players", [])

    # Build player lookup: appearance_id → player_name
    player_lookup: dict[str, str] = {}
    player_id_to_name: dict[str, str] = {}

    for p in players:
        p_id = str(p.get("id", ""))
        first = p.get("first_name", "")
        last = p.get("last_name", "")
        sport = str(p.get("sport_id", "")).lower()
        # Only include NBA players
        if first and last and sport in ("nba", ""):
            player_id_to_name[p_id] = f"{first} {last}"

    for app in appearances:
        app_id = str(app.get("id", ""))
        p_id = str(app.get("player_id", ""))
        match_type = app.get("match_type", "")
        # Only NBA single-player appearances
        if match_type == "single" and p_id in player_id_to_name:
            player_lookup[app_id] = player_id_to_name[p_id]

    results: list[dict] = []
    for line_obj in over_under_lines:
        app_id = str(line_obj.get("appearance_id", ""))
        player_name = player_lookup.get(app_id)
        if not player_name:
            continue

        # Prefer "stat" key; fall back to "stat_type" (API schema varies by version)
        stat_label = line_obj.get("stat", "") or line_obj.get("stat_type", "")
        stat_value = line_obj.get("stat_value")
        if stat_value is None:
            continue

        pick_type = line_obj.get("projection_type", "standard") or "standard"

        results.append({
            "player_name": player_name,
            "stat_type": stat_label,
            "line": float(stat_value),
            "pick_type": pick_type,
        })

    return results
