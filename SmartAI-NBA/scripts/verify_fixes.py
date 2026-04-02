"""
Verification script to ensure all fixes are working.
Run this after implementing all changes.

Usage:
    python scripts/verify_fixes.py           # Run all checks
    python scripts/verify_fixes.py --quick   # Run offline-only checks
"""

import sys
import os
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import logging
logging.basicConfig(level=logging.INFO)


# ── Offline checks (no network required) ────────────────────

def verify_nba_api_version():
    """Verify nba_api is installed and key endpoints are available"""
    from importlib.metadata import version as pkg_version
    ver = pkg_version("nba_api")
    print(f"✓ nba_api version: {ver}")

    # Test endpoints actually used by the codebase
    from nba_api.stats.endpoints import ScoreboardV2, LeagueStandingsV3
    print("✓ ScoreboardV2 import successful")
    print("✓ LeagueStandingsV3 import successful")
    return True


def verify_utils():
    """Verify utility modules are working"""
    from utils.headers import (
        get_nba_headers, get_cdn_headers, get_espn_headers,
        get_underdog_headers, get_odds_api_headers,
    )
    from utils.retry import retry_with_backoff, CircuitBreaker
    from utils.cache import FileCache, cache_get, cache_set, cache_invalidate
    from utils.logger import get_logger

    # Headers — all five endpoint header helpers
    for name, fn in [
        ("NBA", get_nba_headers),
        ("CDN", get_cdn_headers),
        ("ESPN", get_espn_headers),
        ("Underdog", get_underdog_headers),
        ("Odds API", get_odds_api_headers),
    ]:
        headers = fn()
        assert "User-Agent" in headers, f"{name} headers missing User-Agent"
    print("✓ All 5 header helpers working")

    # Retry decorator
    call_count = 0

    @retry_with_backoff(retries=1, initial_delay=0.01)
    def _retry_test():
        nonlocal call_count
        call_count += 1
        if call_count < 2:
            raise ValueError("transient")
        return "ok"

    assert _retry_test() == "ok"
    print("✓ retry_with_backoff decorator working")

    # Circuit breaker
    cb = CircuitBreaker("verify", failure_threshold=2, timeout=0)
    assert cb.call(lambda: 42) == 42
    cb.reset()
    print("✓ CircuitBreaker working")

    # File-based cache
    import tempfile, shutil
    tmpdir = tempfile.mkdtemp()
    try:
        cache = FileCache(cache_dir=tmpdir, ttl_hours=1)
        cache.set("test", "value")
        assert cache.get("test") == "value"
        cache.clear()
        assert cache.get("test") is None
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)
    print("✓ FileCache module working")

    # In-memory cache
    cache_set("verify_key", 123, tier="stats")
    assert cache_get("verify_key", tier="stats") == 123
    cache_invalidate("verify_key")
    assert cache_get("verify_key") is None
    print("✓ In-memory tiered cache working")

    # Logger
    log = get_logger("verify_test")
    assert log.name.startswith("smartai_nba.")
    print("✓ Logger module working")

    return True


def verify_player_id_cache():
    """Verify PlayerIDCache fuzzy matching and overrides"""
    from data.player_id_cache import PlayerIDCache

    cache = PlayerIDCache()

    # Verify team map
    team_map = cache._get_team_map()
    assert len(team_map) == 30, f"Expected 30 teams, got {len(team_map)}"
    print(f"✓ Team map has {len(team_map)} teams")

    # Verify name normalization
    assert cache._normalize_name("LeBron James Jr.") == "lebron james"
    assert cache._normalize_name("P.J. Tucker") == "pj tucker"
    print("✓ Name normalization working")

    # Verify overrides loaded
    overrides = cache.list_overrides()
    print(f"✓ Player ID overrides loaded: {len(overrides)} entries")

    # Verify missing player detection
    missing = cache.get_missing_players(["Definitely Not A Player"])
    assert "Definitely Not A Player" in missing
    print("✓ Missing player detection working")

    return True


def verify_data_package():
    """Verify data package exports"""
    from data import NBADataService, RosterEngine, PlayerIDCache

    # NBADataService
    svc = NBADataService()
    assert hasattr(svc, "get_todays_games")
    assert hasattr(svc, "get_todays_players")
    assert hasattr(svc, "clear_caches")
    assert hasattr(svc, "refresh_all_data")
    print("✓ NBADataService class has all expected methods")

    # Module-level functions preserved
    from data.nba_data_service import (
        get_todays_games, get_todays_players, get_team_stats,
        clear_caches, refresh_all_data,
    )
    print("✓ Module-level functions preserved")

    # PlayerIDCache
    cache = PlayerIDCache()
    assert callable(cache.get_player_id)
    assert callable(cache.add_override)
    print("✓ PlayerIDCache instantiates correctly")

    return True


def verify_player_id_overrides_file():
    """Verify player_id_overrides.json is valid"""
    import json
    fp = Path(__file__).parent.parent / "data" / "player_id_overrides.json"
    assert fp.exists(), "player_id_overrides.json not found"

    with open(fp, "r") as f:
        data = json.load(f)
    assert isinstance(data, dict)

    for name, entry in data.items():
        if isinstance(entry, dict):
            assert "id" in entry, f"Override for {name} missing 'id'"
    print(f"✓ player_id_overrides.json valid ({len(data)} entries)")
    return True


# ── Online checks (require network) ─────────────────────────

def verify_roster_engine():
    """Verify roster engine can fetch injuries"""
    from data.roster_engine import RosterEngine

    engine = RosterEngine()
    engine.refresh()

    injury_map = getattr(engine, '_injury_map', {})
    print(f"✓ Injuries fetched: {len(injury_map)} players")
    return True


def verify_live_data_fetcher():
    """Verify live data fetcher can get games"""
    from data.live_data_fetcher import fetch_todays_games

    games = fetch_todays_games()

    print(f"✓ Games fetched: {len(games)}")
    for game in games:
        print(f"  - {game.get('away_team', '?')} @ {game.get('home_team', '?')}")

    return len(games) > 0


def verify_platform_fetcher():
    """Verify platform fetcher can get props"""
    from data.platform_fetcher import fetch_prizepicks_props, fetch_underdog_props

    # Test PrizePicks
    pp_props = fetch_prizepicks_props()
    print(f"✓ PrizePicks props: {len(pp_props)}")

    # Test Underdog
    ud_props = fetch_underdog_props()
    print(f"✓ Underdog props: {len(ud_props)}")

    return True


def verify_player_filtering():
    """Verify Coby White is not filtered"""
    from data.nba_data_service import NBADataService

    service = NBADataService()
    games = service.get_todays_games()

    if not games:
        print("⚠ No games today, skipping player verification")
        return True

    players = service.get_todays_players(games)

    # Check for Coby White
    coby = [p for p in players if 'Coby White' in p.get('name', '')]

    if coby:
        print(f"✓ Coby White found in players ({len(coby)} entries)")
        print(f"  - Status: {coby[0].get('injury_status', 'Unknown')}")
        return True
    else:
        print("⚠ Coby White not found in players (may not be playing today)")
        return True


def main():
    """Run all verification checks"""
    quick_mode = "--quick" in sys.argv

    print("\n" + "=" * 56)
    print("  SMART-AI NBA VERIFICATION SUITE")
    if quick_mode:
        print("  (quick mode — offline checks only)")
    print("=" * 56 + "\n")

    # Offline checks always run
    checks = [
        ("NBA API Version", verify_nba_api_version),
        ("Utility Modules", verify_utils),
        ("Player ID Cache", verify_player_id_cache),
        ("Data Package Exports", verify_data_package),
        ("Player ID Overrides File", verify_player_id_overrides_file),
    ]

    # Online checks only in full mode
    if not quick_mode:
        checks += [
            ("Roster Engine", verify_roster_engine),
            ("Live Data Fetcher", verify_live_data_fetcher),
            ("Platform Fetcher", verify_platform_fetcher),
            ("Player Filtering", verify_player_filtering),
        ]

    passed = 0
    failed = 0

    for name, func in checks:
        print(f"\n--- Testing {name} ---")
        try:
            if func():
                passed += 1
                print(f"✅ {name}: PASSED")
            else:
                failed += 1
                print(f"❌ {name}: FAILED")
        except Exception as e:
            failed += 1
            print(f"❌ {name}: ERROR - {e}")

    print("\n" + "=" * 56)
    print(f"  RESULTS: {passed} passed, {failed} failed")
    print("=" * 56)

    return failed == 0


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
