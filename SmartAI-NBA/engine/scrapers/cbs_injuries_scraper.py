"""engine/scrapers/cbs_injuries_scraper.py – CBS Sports NBA injury report scraper."""
import time
import random
from utils.logger import get_logger

_logger = get_logger(__name__)

_CBS_INJURY_URL = "https://www.cbssports.com/nba/injuries/"
_DELAY = 3.0
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
    _logger.debug("requests/beautifulsoup4 not installed; cbs_injuries_scraper unavailable")


def _fetch(url: str) -> str:
    """Fetch a URL with rate limiting and retry logic.

    Args:
        url: Target URL.

    Returns:
        HTML text or empty string.
    """
    if not _DEPS_AVAILABLE:
        return ""

    time.sleep(_DELAY)
    for attempt in range(_MAX_RETRIES):
        try:
            resp = requests.get(url, headers={"User-Agent": _USER_AGENT}, timeout=15)
            resp.raise_for_status()
            return resp.text
        except Exception as exc:
            wait = (2 ** attempt) + random.uniform(0, 1)
            _logger.debug("Fetch attempt %d failed: %s — retry in %.1fs", attempt + 1, exc, wait)
            time.sleep(wait)
    _logger.error("All fetch attempts failed for %s", url)
    return ""


def get_injury_report() -> list:
    """Scrape the CBS Sports NBA injury page.

    Returns:
        List of dicts with keys: ``player``, ``team``, ``status``, ``injury``, ``date``.
    """
    html = _fetch(_CBS_INJURY_URL)
    if not html:
        return []

    injuries = []
    try:
        soup = BeautifulSoup(html, "lxml")
        # CBS Sports uses a table or div structure per team
        for team_section in soup.find_all("div", class_=lambda c: c and "TeamCard" in c):
            team_name = team_section.find("a", class_=lambda c: c and "team-name" in (c or ""))
            team = team_name.get_text(strip=True) if team_name else "Unknown"

            rows = team_section.find_all("tr")
            for row in rows:
                cells = row.find_all("td")
                if len(cells) < 3:
                    continue
                player = cells[0].get_text(strip=True)
                status = cells[1].get_text(strip=True)
                injury = cells[2].get_text(strip=True)
                date = cells[3].get_text(strip=True) if len(cells) > 3 else ""
                if player:
                    injuries.append({
                        "player": player,
                        "team": team,
                        "status": status,
                        "injury": injury,
                        "date": date,
                    })
    except Exception as exc:
        _logger.error("CBS injury parse error: %s", exc)

    return injuries
