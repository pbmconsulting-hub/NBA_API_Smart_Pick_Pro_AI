"""engine/scrapers/basketball_ref_scraper.py – Basketball Reference scraper.

Uses requests + beautifulsoup4. Rate-limited (3s delay) with exponential backoff retry.
Replaces the basketball_reference_web_scraper package dependency.
"""
import time
import random
from utils.logger import get_logger

_logger = get_logger(__name__)

_BASE_URL = "https://www.basketball-reference.com"
_DELAY = 3.0  # seconds between requests
_MAX_RETRIES = 3
_USER_AGENT = (
    "Mozilla/5.0 (compatible; SmartAI-NBA/1.0; +https://github.com/pbmconsulting-hub/SmartAI-NBA)"
)

try:
    import requests
    from bs4 import BeautifulSoup
    _DEPS_AVAILABLE = True
except ImportError:
    _DEPS_AVAILABLE = False
    _logger.debug("requests/beautifulsoup4 not installed; basketball_ref_scraper unavailable")


def _fetch(url: str) -> str:
    """Fetch a URL with rate limiting and exponential backoff.

    Args:
        url: Target URL.

    Returns:
        Response text, or empty string on failure.
    """
    if not _DEPS_AVAILABLE:
        _logger.warning("requests/beautifulsoup4 not available")
        return ""

    time.sleep(_DELAY)
    for attempt in range(_MAX_RETRIES):
        try:
            resp = requests.get(url, headers={"User-Agent": _USER_AGENT}, timeout=15)
            resp.raise_for_status()
            return resp.text
        except Exception as exc:
            wait = (2 ** attempt) + random.uniform(0, 1)
            _logger.debug("Fetch attempt %d failed for %s: %s — retrying in %.1fs", attempt + 1, url, exc, wait)
            time.sleep(wait)

    _logger.error("All %d fetch attempts failed for %s", _MAX_RETRIES, url)
    return ""


def _player_url_slug(player_name: str) -> str:
    """Convert a player name to a Basketball Reference URL slug.

    Args:
        player_name: Full player name (e.g. "LeBron James").

    Returns:
        URL slug (e.g. "j/jamesle01").
    """
    parts = player_name.lower().split()
    if len(parts) < 2:
        return ""
    last = parts[-1][:5]
    first = parts[0][:2]
    slug = f"{last[0]}/{last}{first}01"
    return slug


def get_player_game_log(player_name: str, season: str) -> list:
    """Scrape a player's game log for a given season.

    Args:
        player_name: Full player name.
        season: Season string (e.g. "2024").

    Returns:
        List of game log dicts, or empty list on failure.
    """
    slug = _player_url_slug(player_name)
    if not slug:
        return []

    url = f"{_BASE_URL}/players/{slug}/gamelog/{season}"
    html = _fetch(url)
    if not html:
        return []

    rows = []
    try:
        soup = BeautifulSoup(html, "lxml")
        table = soup.find("table", {"id": "pgl_basic"})
        if table is None:
            return []

        headers = [th.get_text(strip=True) for th in table.find("thead").find_all("th")]
        for tr in table.find("tbody").find_all("tr"):
            if tr.get("class") and "thead" in tr.get("class", []):
                continue
            cells = tr.find_all(["td", "th"])
            if len(cells) < 5:
                continue
            row = {headers[i]: cells[i].get_text(strip=True) for i in range(min(len(headers), len(cells)))}
            if row.get("Rk") and row["Rk"].isdigit():
                rows.append(row)
    except Exception as exc:
        _logger.error("parse error for %s game log: %s", player_name, exc)

    return rows


def get_player_season_stats(player_name: str, season: str) -> dict:
    """Scrape season averages for a player.

    Args:
        player_name: Full player name.
        season: Season string (e.g. "2024").

    Returns:
        Dict of stat averages, or empty dict on failure.
    """
    slug = _player_url_slug(player_name)
    if not slug:
        return {}

    url = f"{_BASE_URL}/players/{slug}.html"
    html = _fetch(url)
    if not html:
        return {}

    try:
        soup = BeautifulSoup(html, "lxml")
        table = soup.find("table", {"id": "per_game"})
        if table is None:
            return {}

        headers = [th.get_text(strip=True) for th in table.find("thead").find_all("th")]
        for tr in table.find("tbody").find_all("tr"):
            cells = tr.find_all(["td", "th"])
            if not cells:
                continue
            row = {headers[i]: cells[i].get_text(strip=True) for i in range(min(len(headers), len(cells)))}
            if str(season) in row.get("Season", ""):
                return row
    except Exception as exc:
        _logger.error("parse error for %s season stats: %s", player_name, exc)

    return {}


def get_team_standings(season: str) -> list:
    """Scrape NBA standings for a given season.

    Args:
        season: Season string (e.g. "2024").

    Returns:
        List of team standing dicts.
    """
    url = f"{_BASE_URL}/leagues/NBA_{season}_standings.html"
    html = _fetch(url)
    if not html:
        return []

    standings = []
    try:
        soup = BeautifulSoup(html, "lxml")
        for conf in ["divs_standings_E", "divs_standings_W"]:
            table = soup.find("table", {"id": conf})
            if table is None:
                continue
            headers = [th.get_text(strip=True) for th in table.find("thead").find_all("th")]
            for tr in table.find("tbody").find_all("tr"):
                if tr.get("class") and "thead" in tr.get("class", []):
                    continue
                cells = tr.find_all(["td", "th"])
                if len(cells) < 3:
                    continue
                row = {headers[i]: cells[i].get_text(strip=True) for i in range(min(len(headers), len(cells)))}
                if row:
                    standings.append(row)
    except Exception as exc:
        _logger.error("standings parse error for season %s: %s", season, exc)

    return standings


def get_player_box_scores_for_date(date_str: str) -> dict:
    """Scrape all player box scores for a specific date from Basketball Reference.

    This function serves as the Tier 2 fallback for bet resolution in
    ``tracking/bet_tracker.py``, replacing the removed
    ``basketball_reference_web_scraper`` package.

    URL pattern: /friv/dailyleaders.fcgi?month=M&day=D&year=YYYY

    Args:
        date_str: Date in YYYY-MM-DD format.

    Returns:
        Dict mapping lowercased player name to stat dict, or empty dict on failure.
        Stat keys: ``pts``, ``reb``, ``ast``, ``stl``, ``blk``, ``tov``,
        ``fg3m``, ``fg3a``, ``fgm``, ``fga``, ``ftm``, ``fta``,
        ``oreb``, ``dreb``, ``pf``, ``minutes``.
    """
    try:
        import datetime as _dt
        target = _dt.datetime.strptime(date_str, "%Y-%m-%d")
    except ValueError as exc:
        _logger.error("Invalid date_str '%s': %s", date_str, exc)
        return {}

    url = (
        f"{_BASE_URL}/friv/dailyleaders.fcgi"
        f"?month={target.month}&day={target.day}&year={target.year}"
    )
    html = _fetch(url)
    if not html:
        return {}

    lookup: dict = {}
    try:
        soup = BeautifulSoup(html, "lxml")
        table = soup.find("table", {"id": "stats"})
        if table is None:
            _logger.debug("No 'stats' table found for date %s", date_str)
            return {}

        headers = [th.get("data-stat", th.get_text(strip=True)) for th in table.find("thead").find_all("th")]

        for tr in table.find("tbody").find_all("tr"):
            if tr.get("class") and "thead" in tr.get("class", []):
                continue
            cells = tr.find_all(["td", "th"])
            if len(cells) < 5:
                continue

            row = {}
            for i, cell in enumerate(cells):
                if i < len(headers):
                    row[headers[i]] = cell.get_text(strip=True)

            pname = str(row.get("player", "")).lower().strip()
            # Remove asterisk suffix used for HOF players on BBRef
            pname = pname.rstrip("*").strip()
            if not pname:
                continue

            def _f(key: str, default: float = 0.0) -> float:
                try:
                    return float(row.get(key) or default)
                except (ValueError, TypeError):
                    return default

            oreb = _f("orb")
            dreb = _f("drb")
            mins_str = row.get("mp", "0")
            try:
                if ":" in str(mins_str):
                    parts = str(mins_str).split(":")
                    minutes = float(parts[0]) + float(parts[1]) / 60.0
                else:
                    minutes = float(mins_str or 0)
            except (ValueError, TypeError):
                minutes = 0.0

            lookup[pname] = {
                "pts":     _f("pts"),
                "reb":     oreb + dreb,
                "ast":     _f("ast"),
                "stl":     _f("stl"),
                "blk":     _f("blk"),
                "tov":     _f("tov"),
                "fg3m":    _f("fg3"),
                "fg3a":    _f("fg3a"),
                "fgm":     _f("fg"),
                "fga":     _f("fga"),
                "ftm":     _f("ft"),
                "fta":     _f("fta"),
                "oreb":    oreb,
                "dreb":    dreb,
                "pf":      _f("pf"),
                "minutes": round(minutes, 2),
            }
    except Exception as exc:
        _logger.error("parse error for daily leaders on %s: %s", date_str, exc)

    return lookup
