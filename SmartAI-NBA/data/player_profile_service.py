# ============================================================
# FILE: data/player_profile_service.py
# PURPOSE: Enrich betting data with real NBA player context
#          and season stats.  Returns headshot URLs, position,
#          team logo URLs, next opponent, and season averages
#          (PPG, RPG, APG, Minutes) for use in the Player
#          Spotlight modal and Trading-Card grid.
#
# USAGE:
#   from data.player_profile_service import enrich_player_data
#   vitals = enrich_player_data("LeBron James", players_data, todays_games)
# ============================================================

import html as _html
import logging as _logging

_logger = _logging.getLogger(__name__)

# ── Player ID lookup cache ───────────────────────────────────
# Maps player_name.lower() → NBA player ID (int).
# Populated on first call to get_headshot_url() / get_player_id(),
# and pre-seeded with marquee players for test compatibility and performance.
_PLAYER_ID_CACHE: dict = {}

# Pre-seeded player IDs for the most common NBA players.
# This ensures headshots work immediately without any API call,
# and provides backward compatibility for tests that import _KNOWN_PLAYER_IDS.
_KNOWN_PLAYER_IDS: dict[str, int] = {
    "lebron james": 2544,
    "stephen curry": 201939,
    "kevin durant": 201142,
    "giannis antetokounmpo": 203507,
    "luka doncic": 1629029,
    "jayson tatum": 1628369,
    "nikola jokic": 203999,
    "joel embiid": 203954,
    "anthony davis": 203076,
    "damian lillard": 203081,
    "devin booker": 1626164,
    "jimmy butler": 202710,
    "anthony edwards": 1630162,
    "shai gilgeous-alexander": 1628983,
    "ja morant": 1629630,
    "donovan mitchell": 1628378,
    "trae young": 1629027,
    "paolo banchero": 1631094,
    "tyrese haliburton": 1630169,
    "de'aaron fox": 1628368,
    "jalen brunson": 1628973,
    "chet holmgren": 1631096,
    "victor wembanyama": 1641705,
    "lamelo ball": 1630163,
    "zion williamson": 1629627,
    "bam adebayo": 1628389,
    "karl-anthony towns": 1626157,
    "domantas sabonis": 1627734,
    "pascal siakam": 1627783,
    "lauri markkanen": 1628374,
    "tyrese maxey": 1630178,
    "jaylen brown": 1627759,
    "jalen williams": 1631114,
    "alperen sengun": 1630578,
    "cade cunningham": 1630595,
    "franz wagner": 1630532,
    "scottie barnes": 1630567,
    "mikal bridges": 1628969,
    "desmond bane": 1630217,
    "tyler herro": 1629639,
    "james harden": 201935,
    "paul george": 202331,
    "kawhi leonard": 202695,
    "kyrie irving": 202681,
    "josh giddey": 1630581,
    "jalen green": 1630224,
    "cam thomas": 1630560,
    "dyson daniels": 1631097,
    "keegan murray": 1631099,
}

# Pre-seed the lookup cache with the known IDs
_PLAYER_ID_CACHE.update(_KNOWN_PLAYER_IDS)

# ── NBA CDN helpers ─────────────────────────────────────────
# The official NBA CDN serves headshots at a predictable URL
# keyed by a numeric player ID.


def _build_nba_static_lookup() -> dict:
    """
    Build a name→id lookup from nba_api's local static players list.

    This is a LOCAL file read — no network call — so it always works
    even when stats.nba.com is rate-limited or blocked.  It covers
    every player who has ever played in the NBA (4000+ entries).

    Returns:
        dict: {player_name.lower(): player_id (int)}  or {} on failure.
    """
    try:
        from nba_api.stats.static import players as _nba_players_static
        all_players = _nba_players_static.get_players()
        return {p["full_name"].lower(): int(p["id"]) for p in all_players}
    except Exception:
        return {}


def _build_dynamic_player_lookup() -> dict:
    """
    Build a name→id lookup from nba_stats_service.get_all_players().

    This uses the CommonAllPlayers endpoint (with caching) to return
    a fully up-to-date active-player list.  Falls back to {} on failure
    so callers can transparently fall through to the static lookup.

    Returns:
        dict: {player_name.lower(): player_id (int)}  or {} on failure.
    """
    try:
        from data.nba_stats_service import get_all_players
        players = get_all_players(active_only=True)
        return {p["full_name"].lower(): int(p["id"]) for p in players if p.get("id")}
    except Exception:
        return {}


def get_player_id(player_name: str) -> int | None:
    """
    Return the NBA player ID (int) for headshot URL construction.

    Lookup order:
    1. Module-level cache (instant, no I/O)
    2. API-NBA API (if configured)
    3. nba_api local static player list (always fast, covers all players)

    The result is cached so subsequent calls for the same player are free.

    Parameters
    ----------
    player_name : str
        Full player name (case-insensitive).

    Returns
    -------
    int or None
        NBA CDN player ID, or None if not found.
    """
    key = str(player_name).lower().strip()

    # 1. Cache hit
    if key in _PLAYER_ID_CACHE:
        return _PLAYER_ID_CACHE[key]

    pid = None

    # 2. Dynamic lookup via nba_stats_service (CommonAllPlayers — cached)
    if not pid:
        try:
            dynamic_lookup = _build_dynamic_player_lookup()
            pid = dynamic_lookup.get(key)
            if not pid:
                parts = key.split()
                if len(parts) >= 2:
                    pid = next(
                        (v for k, v in dynamic_lookup.items()
                         if parts[0] in k and parts[-1] in k),
                        None,
                    )
        except Exception as exc:
            _logger.debug("get_player_id: dynamic lookup failed for %r — %s", key, exc)

    # 3. nba_api local static list (no network call, covers all-time players)
    if not pid:
        try:
            static_lookup = _build_nba_static_lookup()
            pid = static_lookup.get(key)
            if not pid:
                # Try partial match: first + last name
                parts = key.split()
                if len(parts) >= 2:
                    pid = next(
                        (v for k, v in static_lookup.items()
                         if parts[0] in k and parts[-1] in k),
                        None,
                    )
        except Exception as exc:
            _logger.debug("get_player_id: static lookup failed for %r — %s", key, exc)

    # Cache result (even None) to avoid repeated lookups
    _PLAYER_ID_CACHE[key] = pid
    return pid


def get_headshot_url(player_name: str) -> str:
    """Return the NBA CDN headshot URL for a player.

    Uses dynamic player ID lookup — covers ALL NBA players, not just
    a hardcoded list. Falls back to a generic silhouette if the player
    ID cannot be found.

    Parameters
    ----------
    player_name : str
        Full player name (case-insensitive).

    Returns
    -------
    str
        URL string pointing to a player headshot image.
    """
    pid = get_player_id(player_name)
    if pid:
        return (
            f"https://cdn.nba.com/headshots/nba/latest/1040x760/{pid}.png"
        )
    return "https://cdn.nba.com/headshots/nba/latest/1040x760/fallback.png"


# ── Team logo CDN ───────────────────────────────────────────
_NBA_TEAM_LOGO_TMPL = (
    "https://cdn.nba.com/logos/nba/{team_id}/global/L/logo.svg"
)

_TEAM_ABBREV_TO_ID: dict[str, int] = {
    "ATL": 1610612737, "BOS": 1610612738, "BKN": 1610612751,
    "CHA": 1610612766, "CHI": 1610612741, "CLE": 1610612739,
    "DAL": 1610612742, "DEN": 1610612743, "DET": 1610612765,
    "GSW": 1610612744, "HOU": 1610612745, "IND": 1610612754,
    "LAC": 1610612746, "LAL": 1610612747, "MEM": 1610612763,
    "MIA": 1610612748, "MIL": 1610612749, "MIN": 1610612750,
    "NOP": 1610612740, "NYK": 1610612752, "OKC": 1610612760,
    "ORL": 1610612753, "PHI": 1610612755, "PHX": 1610612756,
    "POR": 1610612757, "SAC": 1610612758, "SAS": 1610612759,
    "TOR": 1610612761, "UTA": 1610612762, "WAS": 1610612764,
}


def get_team_logo_url(team_abbrev: str) -> str:
    """Return the NBA CDN team logo SVG URL.

    Parameters
    ----------
    team_abbrev : str
        Three-letter NBA team abbreviation (e.g. ``"LAL"``).

    Returns
    -------
    str
        URL string pointing to a team logo SVG.
    """
    tid = _TEAM_ABBREV_TO_ID.get(str(team_abbrev).upper().strip(), 0)
    if tid:
        return _NBA_TEAM_LOGO_TMPL.format(team_id=tid)
    return ""


def _find_next_opponent(player_team: str, todays_games: list) -> str:
    """Derive the next opponent from today's game list.

    Parameters
    ----------
    player_team : str
        Player's team abbreviation.
    todays_games : list[dict]
        List of today's game dicts (keys: ``home_team``, ``away_team``).

    Returns
    -------
    str
        Opponent abbreviation or ``"TBD"`` if not found.
    """
    if not todays_games or not player_team:
        return "TBD"
    abbrev = str(player_team).upper().strip()
    for g in todays_games:
        home = str(g.get("home_team", "")).upper().strip()
        away = str(g.get("away_team", "")).upper().strip()
        if abbrev == home:
            return away
        if abbrev == away:
            return home
    return "TBD"


def _extract_season_stats(player_data: dict) -> dict:
    """Pull season averages from the players CSV row.

    The data_manager's ``load_players_data`` returns dicts whose
    keys vary by source (``"ppg"`` or ``"pts_per_game"`` etc.).
    This helper normalises to a stable shape.

    Parameters
    ----------
    player_data : dict
        A single player record from ``load_players_data()``.

    Returns
    -------
    dict
        ``{"ppg": float, "rpg": float, "apg": float, "avg_minutes": float}``
    """
    def _f(val):
        try:
            return round(float(val), 1)
        except (TypeError, ValueError):
            return 0.0

    ppg = _f(
        player_data.get("ppg")
        or player_data.get("pts_per_game")
        or player_data.get("points_avg")
        or player_data.get("points", 0)
    )
    rpg = _f(
        player_data.get("rpg")
        or player_data.get("reb_per_game")
        or player_data.get("rebounds_avg")
        or player_data.get("rebounds", 0)
    )
    apg = _f(
        player_data.get("apg")
        or player_data.get("ast_per_game")
        or player_data.get("assists_avg")
        or player_data.get("assists", 0)
    )
    avg_min = _f(
        player_data.get("avg_minutes")
        or player_data.get("min_per_game")
        or player_data.get("minutes_avg")
        or player_data.get("minutes", 0)
    )
    return {"ppg": ppg, "rpg": rpg, "apg": apg, "avg_minutes": avg_min}


def get_player_bio(player_name: str) -> dict:
    """Enrich a player's profile with bio/draft data from nba_stats_service.

    Resolves the player's NBA ID and then calls
    ``nba_stats_service.get_player_info()`` for height, weight, draft info,
    country, school, and jersey number.

    Parameters
    ----------
    player_name : str
        Full player name (case-insensitive).

    Returns
    -------
    dict
        Keys: position, height, weight, country, birthdate,
        draft_year, draft_round, draft_number, school, jersey.
        Returns {} if the player ID cannot be resolved or the
        nba_stats_service call fails.
    """
    pid = get_player_id(player_name)
    if not pid:
        return {}

    try:
        from data.nba_stats_service import get_player_info
        info = get_player_info(pid)
        return {
            "position": info.get("position", ""),
            "height": info.get("height", ""),
            "weight": info.get("weight", ""),
            "country": info.get("country", ""),
            "birthdate": info.get("birthdate", ""),
            "draft_year": info.get("draft_year"),
            "draft_round": info.get("draft_round"),
            "draft_number": info.get("draft_number"),
            "school": info.get("school", ""),
            "jersey": info.get("jersey", ""),
        }
    except Exception:
        return {}


def enrich_player_data(
    player_name: str,
    players_data: list,
    todays_games: list | None = None,
) -> dict:
    """Build a vitals dict for the Player Spotlight modal.

    Parameters
    ----------
    player_name : str
        Full player name.
    players_data : list[dict]
        Full players dataset from ``load_players_data()``.
    todays_games : list[dict] | None
        Today's game list for opponent resolution.

    Returns
    -------
    dict
        Keys: ``headshot_url``, ``position``, ``team``,
        ``team_logo_url``, ``next_opponent``, ``season_stats``.
    """
    safe_name = _html.escape(str(player_name))
    key = str(player_name).lower().strip()

    # Find player in dataset
    player_row: dict = {}
    if isinstance(players_data, list):
        for p in players_data:
            if str(p.get("name", "")).lower().strip() == key:
                player_row = p
                break
    elif isinstance(players_data, dict):
        player_row = players_data.get(player_name, {})

    team = str(player_row.get("team", player_row.get("team_abbrev", ""))).upper().strip()
    position = str(player_row.get("position", player_row.get("pos", ""))).strip()

    # If player_id is in the CSV row, cache it for future headshot lookups
    csv_pid = player_row.get("player_id")
    if csv_pid and not _PLAYER_ID_CACHE.get(key):
        try:
            _PLAYER_ID_CACHE[key] = int(csv_pid)
        except (ValueError, TypeError):
            pass

    return {
        "player_name": safe_name,
        "headshot_url": get_headshot_url(player_name),
        "position": position or "N/A",
        "team": team or "N/A",
        "team_logo_url": get_team_logo_url(team),
        "next_opponent": _find_next_opponent(team, todays_games or []),
        "season_stats": _extract_season_stats(player_row),
    }

