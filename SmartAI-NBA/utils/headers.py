"""
Unified headers for all API requests to avoid 403 errors.
Rotates user agents and includes all necessary browser headers.
"""

import random
from typing import Dict

# User agents to rotate and avoid detection
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) "
    "Gecko/20100101 Firefox/121.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 "
    "(KHTML, like Gecko) Version/17.0 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36 Edg/119.0.0.0",
]


def get_random_user_agent() -> str:
    """Return random user agent to avoid detection."""
    return random.choice(USER_AGENTS)


def get_nba_headers() -> Dict[str, str]:
    """Headers for NBA stats API (stats.nba.com)."""
    return {
        "User-Agent": get_random_user_agent(),
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Referer": "https://www.nba.com/",
        "Origin": "https://www.nba.com",
        "Connection": "keep-alive",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-site",
        "Pragma": "no-cache",
        "Cache-Control": "no-cache",
    }


def get_cdn_headers() -> Dict[str, str]:
    """Headers for NBA CDN (cdn.nba.com)."""
    return {
        "User-Agent": get_random_user_agent(),
        "Accept": "application/json",
        "Referer": "https://www.nba.com/",
        "Origin": "https://www.nba.com",
    }


def get_espn_headers() -> Dict[str, str]:
    """Headers for ESPN API."""
    return {
        "User-Agent": get_random_user_agent(),
        "Accept": "application/json",
        "Referer": "https://www.espn.com/",
    }


def get_underdog_headers() -> Dict[str, str]:
    """Headers for Underdog Fantasy API."""
    return {
        "User-Agent": get_random_user_agent(),
        "Accept": "application/json",
        "Origin": "https://underdogfantasy.com",
        "Referer": "https://underdogfantasy.com/",
    }


def get_odds_api_headers() -> Dict[str, str]:
    """Headers for The Odds API."""
    return {
        "User-Agent": "SmartAI-NBA/1.0",
        "Accept": "application/json",
    }
