"""engine/scrapers/transactions_scraper.py – NBA transactions scraper (ESPN/CBS)."""
import time
import random
import datetime
from utils.logger import get_logger

_logger = get_logger(__name__)

_ESPN_TRANSACTIONS_URL = "https://www.espn.com/nba/transactions"
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
    _logger.debug("requests/beautifulsoup4 not installed; transactions_scraper unavailable")


def _fetch(url: str) -> str:
    """Fetch a URL with rate limiting and retry.

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


def get_recent_transactions(days: int = 7) -> list:
    """Scrape recent NBA transactions from ESPN.

    Args:
        days: How many days back to look.

    Returns:
        List of transaction dicts with keys: ``date``, ``team``, ``description``, ``type``.
    """
    html = _fetch(_ESPN_TRANSACTIONS_URL)
    if not html:
        return []

    transactions = []
    cutoff = datetime.date.today() - datetime.timedelta(days=days)

    try:
        soup = BeautifulSoup(html, "lxml")
        # ESPN renders transactions in tables grouped by date
        current_date = None
        for element in soup.find_all(["h2", "tr"]):
            tag = element.name
            if tag == "h2":
                date_text = element.get_text(strip=True)
                try:
                    current_date = datetime.datetime.strptime(date_text, "%B %d, %Y").date()
                except ValueError:
                    current_date = None
                continue

            if tag == "tr" and current_date and current_date >= cutoff:
                cells = element.find_all("td")
                if len(cells) >= 2:
                    team = cells[0].get_text(strip=True)
                    description = cells[1].get_text(strip=True)
                    # Classify type heuristically
                    desc_lower = description.lower()
                    if "trade" in desc_lower:
                        tx_type = "trade"
                    elif "sign" in desc_lower or "contract" in desc_lower:
                        tx_type = "signing"
                    elif "waiv" in desc_lower or "release" in desc_lower:
                        tx_type = "waiver"
                    else:
                        tx_type = "other"

                    transactions.append({
                        "date": current_date.isoformat(),
                        "team": team,
                        "description": description,
                        "type": tx_type,
                    })
    except Exception as exc:
        _logger.error("ESPN transactions parse error: %s", exc)

    return transactions
