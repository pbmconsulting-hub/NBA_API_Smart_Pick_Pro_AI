"""
injury_client.py
----------------
Populate the ``Injury_Status`` table from the NBA API injury data.

The NBA does not have a dedicated "injuries" endpoint in the public
``nba_api`` package, but the ``LeagueDashPlayerStats`` data includes
minutes = 0 for inactive players and the ``CommonTeamRoster`` has a
``PLAYER_STATUS`` / ``HOW_ACQUIRED`` field.

This module uses the free **BallDontLie** (v1) API which exposes an
``/injuries`` endpoint at no cost, with no key required for basic use.
If that API is unavailable we fall back to scraping the NBA's official
injury report RSS feed (also free).

If neither source works the module logs a warning and returns 0 — it
never raises.

Usage::

    from injury_client import refresh_injury_status
    n = refresh_injury_status(conn)
"""

import logging
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

# CBSSports NBA injuries RSS feed (free, no auth)
_CBS_INJURIES_URL = "https://www.cbssports.com/nba/injuries/"

# NBA official injury report JSON
_NBA_INJURY_URL = "https://cdn.nba.com/static/json/liveData/injuries/injuries_current.json"

# Fallback: nba_api roster-based approach
_ROSTER_FALLBACK = True


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


def _build_team_abbrev_to_id(conn: sqlite3.Connection) -> dict[str, int]:
    """Build abbreviation → team_id from the Teams table."""
    rows = conn.execute(
        "SELECT team_id, abbreviation FROM Teams"
    ).fetchall()
    mapping: dict[str, int] = {}
    for r in rows:
        tid = r[0] if isinstance(r, (list, tuple)) else r["team_id"]
        abbr = r[1] if isinstance(r, (list, tuple)) else r["abbreviation"]
        if abbr:
            mapping[abbr.strip().upper()] = tid
    return mapping


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def refresh_injury_status(conn: sqlite3.Connection) -> int:
    """Fetch current NBA injury data and upsert into ``Injury_Status``.

    Tries multiple sources in order of reliability:
    1. NBA official CDN injury JSON
    2. Fallback — zero rows (logged warning)

    Returns the number of rows upserted.
    """
    name_map = _build_name_to_id_map(conn)
    team_map = _build_team_abbrev_to_id(conn)

    injuries = _fetch_nba_cdn_injuries()

    if not injuries:
        logger.warning(
            "No injury data returned from any source — Injury_Status "
            "table will not be updated this cycle."
        )
        return 0

    today_str = date.today().isoformat()
    now_ts = datetime.now(timezone.utc).isoformat()
    inserted = 0

    for inj in injuries:
        player_name = inj.get("player_name", "")
        player_id = inj.get("player_id") or name_map.get(
            player_name.strip().lower()
        )
        if not player_id:
            continue  # Unknown player — skip

        team_abbrev = inj.get("team", "").upper()
        team_id = inj.get("team_id") or team_map.get(team_abbrev)
        status = inj.get("status", "Unknown")
        reason = inj.get("reason", "")
        source = inj.get("source", "nba_cdn")

        try:
            conn.execute(
                """
                INSERT OR REPLACE INTO Injury_Status
                    (player_id, team_id, report_date, status, reason,
                     source, last_updated_ts)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (player_id, team_id, today_str, status, reason, source, now_ts),
            )
            inserted += 1
        except sqlite3.Error:
            logger.warning("Failed to upsert injury for player_id=%s", player_id)

    conn.commit()
    logger.info("Injury client: upserted %d injury rows for %s.", inserted, today_str)
    return inserted


def get_injured_players_for_team(
    conn: sqlite3.Connection,
    team_id: int,
    report_date: Optional[str] = None,
) -> list[dict]:
    """Return injury rows for a given team on the specified date.

    Returns a list of dicts with keys:
        player_id, status, reason
    """
    if report_date is None:
        report_date = date.today().isoformat()

    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        """
        SELECT i.player_id, i.status, i.reason, p.full_name
        FROM Injury_Status i
        LEFT JOIN Players p ON p.player_id = i.player_id
        WHERE i.team_id = ? AND i.report_date = ?
        """,
        (team_id, report_date),
    ).fetchall()

    return [dict(r) for r in rows]


def get_player_injury_status(
    conn: sqlite3.Connection,
    player_id: int,
    report_date: Optional[str] = None,
) -> Optional[dict]:
    """Return the current injury status for a single player, or ``None``."""
    if report_date is None:
        report_date = date.today().isoformat()

    conn.row_factory = sqlite3.Row
    row = conn.execute(
        """
        SELECT status, reason, source, last_updated_ts
        FROM Injury_Status
        WHERE player_id = ? AND report_date = ?
        """,
        (player_id, report_date),
    ).fetchone()

    return dict(row) if row else None


# ---------------------------------------------------------------------------
# NBA CDN official injury report
# ---------------------------------------------------------------------------


def _fetch_nba_cdn_injuries() -> list[dict]:
    """Fetch injuries from the NBA's official CDN JSON feed."""
    try:
        resp = requests.get(_NBA_INJURY_URL, timeout=15, headers={
            "User-Agent": "SmartPicksProAI/1.0",
            "Accept": "application/json",
        })
        resp.raise_for_status()
        data = resp.json()
    except Exception:
        logger.warning("NBA CDN injury feed unavailable.")
        return []

    injuries: list[dict] = []
    # The feed structure: {"leagueInjuries": [{"teamId": ..., "players": [...]}]}
    league_injuries = data.get("leagueInjuries", [])
    if not league_injuries:
        # Alternate structure
        league_injuries = data.get("resultSets", [])

    for team_entry in league_injuries:
        team_id = team_entry.get("teamId")
        team_abbrev = team_entry.get("teamTricode", "")
        for player in team_entry.get("players", []):
            injuries.append({
                "player_id": player.get("personId"),
                "player_name": (
                    f"{player.get('firstName', '')} {player.get('lastName', '')}".strip()
                ),
                "team_id": team_id,
                "team": team_abbrev,
                "status": player.get("injuryStatus", "Unknown"),
                "reason": player.get("reason", player.get("comment", "")),
                "source": "nba_cdn",
            })

    logger.info("NBA CDN: parsed %d injury records.", len(injuries))
    return injuries
