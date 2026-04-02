# ============================================================
# FILE: tests/test_prizepicks_mirror.py
# PURPOSE: Tests for the PrizePicks data mirror integration in
#          data/platform_fetcher.py — mirror URL constants,
#          mirror prop parser, and odds_type (goblin/demon/standard)
#          capture from both the mirror and the live API parser.
# ============================================================
import unittest
from unittest.mock import MagicMock, patch


class TestPrizePicksMirrorConstants(unittest.TestCase):
    """Verify the mirror URL constants exist and point at the right host."""

    def setUp(self):
        try:
            from data.platform_fetcher import (
                PRIZEPICKS_MIRROR_TODAY_URL,
                PRIZEPICKS_MIRROR_TOMORROW_URL,
            )
            self.today_url = PRIZEPICKS_MIRROR_TODAY_URL
            self.tomorrow_url = PRIZEPICKS_MIRROR_TOMORROW_URL
        except ImportError as exc:
            self.skipTest(f"platform_fetcher not importable: {exc}")

    def test_today_url_contains_enkday(self):
        self.assertIn("enkday", self.today_url.lower())

    def test_today_url_contains_nba_today(self):
        self.assertIn("prizepicks-nba-today", self.today_url)

    def test_tomorrow_url_contains_nba_tomorrow(self):
        self.assertIn("prizepicks-nba-tomorrow", self.tomorrow_url)

    def test_today_url_is_raw_github(self):
        self.assertIn(
            "raw.githubusercontent.com",
            self.today_url,
            "Mirror URL should use raw.githubusercontent.com",
        )


class TestParsePrizePicksMirrorProps(unittest.TestCase):
    """Unit tests for _parse_prizepicks_mirror_props()."""

    def setUp(self):
        try:
            from data.platform_fetcher import _parse_prizepicks_mirror_props
            self._parse = _parse_prizepicks_mirror_props
        except ImportError as exc:
            self.skipTest(f"platform_fetcher not importable: {exc}")

    def _make_item(self, **kwargs):
        defaults = {
            "player": "LeBron James",
            "stat": "Points",
            "line": 24.5,
            "oddsType": "standard",
            "teamCode": "LAL",
            "startDateCST": "2026-03-25",
        }
        defaults.update(kwargs)
        return defaults

    def test_basic_standard_prop(self):
        data = {"props": [self._make_item()]}
        result = self._parse(data, "2026-03-25", "2026-03-25T00:00:00+00:00")
        self.assertEqual(len(result), 1)
        prop = result[0]
        self.assertEqual(prop["player_name"], "LeBron James")
        self.assertEqual(prop["line"], 24.5)
        self.assertEqual(prop["odds_type"], "standard")
        self.assertEqual(prop["platform"], "PrizePicks")

    def test_goblin_odds_type_captured(self):
        data = {"props": [self._make_item(oddsType="goblin", line=22.5)]}
        result = self._parse(data, "2026-03-25", "2026-03-25T00:00:00+00:00")
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["odds_type"], "goblin")

    def test_demon_odds_type_captured(self):
        data = {"props": [self._make_item(oddsType="demon", line=26.5)]}
        result = self._parse(data, "2026-03-25", "2026-03-25T00:00:00+00:00")
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["odds_type"], "demon")

    def test_odds_type_lowercased(self):
        """oddsType values from the mirror should be normalized to lowercase."""
        data = {"props": [self._make_item(oddsType="GOBLIN")]}
        result = self._parse(data, "2026-03-25", "2026-03-25T00:00:00+00:00")
        self.assertEqual(result[0]["odds_type"], "goblin")

    def test_missing_player_skipped(self):
        data = {"props": [self._make_item(player="")]}
        result = self._parse(data, "2026-03-25", "2026-03-25T00:00:00+00:00")
        self.assertEqual(len(result), 0)

    def test_zero_line_skipped(self):
        data = {"props": [self._make_item(line=0)]}
        result = self._parse(data, "2026-03-25", "2026-03-25T00:00:00+00:00")
        self.assertEqual(len(result), 0)

    def test_negative_line_skipped(self):
        data = {"props": [self._make_item(line=-1.5)]}
        result = self._parse(data, "2026-03-25", "2026-03-25T00:00:00+00:00")
        self.assertEqual(len(result), 0)

    def test_non_numeric_line_skipped(self):
        data = {"props": [self._make_item(line="N/A")]}
        result = self._parse(data, "2026-03-25", "2026-03-25T00:00:00+00:00")
        self.assertEqual(len(result), 0)

    def test_team_code_uppercased(self):
        data = {"props": [self._make_item(teamCode="lal")]}
        result = self._parse(data, "2026-03-25", "2026-03-25T00:00:00+00:00")
        self.assertEqual(result[0]["team"], "LAL")

    def test_uses_team_fallback_when_no_teamcode(self):
        item = self._make_item()
        item.pop("teamCode", None)
        item["Team"] = "lakers"
        data = {"props": [item]}
        result = self._parse(data, "2026-03-25", "2026-03-25T00:00:00+00:00")
        self.assertEqual(result[0]["team"], "LAKERS")

    def test_game_date_from_startDateCST(self):
        data = {"props": [self._make_item(startDateCST="2026-03-26")]}
        result = self._parse(data, "2026-03-25", "2026-03-25T00:00:00+00:00")
        self.assertEqual(result[0]["game_date"], "2026-03-26")

    def test_game_date_falls_back_to_today(self):
        item = self._make_item()
        item.pop("startDateCST", None)
        data = {"props": [item]}
        result = self._parse(data, "2026-03-25", "2026-03-25T00:00:00+00:00")
        self.assertEqual(result[0]["game_date"], "2026-03-25")

    def test_empty_props_list(self):
        result = self._parse({"props": []}, "2026-03-25", "2026-03-25T00:00:00+00:00")
        self.assertEqual(result, [])

    def test_missing_props_key(self):
        result = self._parse({}, "2026-03-25", "2026-03-25T00:00:00+00:00")
        self.assertEqual(result, [])

    def test_missing_oddtype_defaults_to_standard(self):
        item = self._make_item()
        item.pop("oddsType", None)
        data = {"props": [item]}
        result = self._parse(data, "2026-03-25", "2026-03-25T00:00:00+00:00")
        self.assertEqual(result[0]["odds_type"], "standard")

    def test_stat_type_normalized(self):
        data = {"props": [self._make_item(stat="3-Point Made")]}
        result = self._parse(data, "2026-03-25", "2026-03-25T00:00:00+00:00")
        self.assertEqual(result[0]["stat_type"], "threes")

    def test_multiple_props_returned(self):
        data = {"props": [
            self._make_item(player="Player A", oddsType="standard"),
            self._make_item(player="Player B", oddsType="goblin"),
            self._make_item(player="Player C", oddsType="demon"),
        ]}
        result = self._parse(data, "2026-03-25", "2026-03-25T00:00:00+00:00")
        self.assertEqual(len(result), 3)
        types = [p["odds_type"] for p in result]
        self.assertIn("goblin", types)
        self.assertIn("demon", types)
        self.assertIn("standard", types)


class TestFetchPrizePicksPropsFromMirror(unittest.TestCase):
    """Tests for fetch_prizepicks_props_from_mirror()."""

    def setUp(self):
        try:
            from data import platform_fetcher
            self._module = platform_fetcher
        except ImportError as exc:
            self.skipTest(f"platform_fetcher not importable: {exc}")

    def _make_mirror_response(self, props):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"props": props}
        mock_resp.raise_for_status.return_value = None
        return mock_resp

    def test_returns_list_on_success(self):
        props_data = [
            {"player": "LeBron James", "stat": "Points", "line": 25.5,
             "oddsType": "standard", "teamCode": "LAL", "startDateCST": "2026-03-25"},
        ]
        with patch.object(self._module, "_fetch_with_retry",
                          return_value=self._make_mirror_response(props_data)), \
             patch.object(self._module, "_cache_get", return_value=None), \
             patch.object(self._module, "_cache_set"):
            result = self._module.fetch_prizepicks_props_from_mirror()
        self.assertIsInstance(result, list)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["odds_type"], "standard")

    def test_returns_empty_list_on_network_failure(self):
        with patch.object(self._module, "_fetch_with_retry", return_value=None), \
             patch.object(self._module, "_cache_get", return_value=None):
            result = self._module.fetch_prizepicks_props_from_mirror()
        self.assertEqual(result, [])

    def test_goblin_props_included(self):
        props_data = [
            {"player": "Stephen Curry", "stat": "Points", "line": 25.5,
             "oddsType": "goblin", "teamCode": "GSW", "startDateCST": "2026-03-25"},
        ]
        with patch.object(self._module, "_fetch_with_retry",
                          return_value=self._make_mirror_response(props_data)), \
             patch.object(self._module, "_cache_get", return_value=None), \
             patch.object(self._module, "_cache_set"):
            result = self._module.fetch_prizepicks_props_from_mirror()
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["odds_type"], "goblin")

    def test_demon_props_included(self):
        props_data = [
            {"player": "Nikola Jokic", "stat": "Points", "line": 32.5,
             "oddsType": "demon", "teamCode": "DEN", "startDateCST": "2026-03-25"},
        ]
        with patch.object(self._module, "_fetch_with_retry",
                          return_value=self._make_mirror_response(props_data)), \
             patch.object(self._module, "_cache_get", return_value=None), \
             patch.object(self._module, "_cache_set"):
            result = self._module.fetch_prizepicks_props_from_mirror()
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["odds_type"], "demon")

    def test_uses_cache_when_available(self):
        cached_data = {"props": [
            {"player": "Cached Player", "stat": "Points", "line": 20.0,
             "oddsType": "standard", "teamCode": "BOS"},
        ]}
        with patch.object(self._module, "_cache_get", return_value=cached_data), \
             patch.object(self._module, "_fetch_with_retry") as mock_fetch:
            result = self._module.fetch_prizepicks_props_from_mirror()
        mock_fetch.assert_not_called()
        self.assertEqual(len(result), 1)

    def test_include_tomorrow_fetches_two_urls(self):
        props_data = [
            {"player": "Player A", "stat": "Points", "line": 20.0,
             "oddsType": "standard", "teamCode": "MIA"},
        ]
        with patch.object(self._module, "_fetch_with_retry",
                          return_value=self._make_mirror_response(props_data)), \
             patch.object(self._module, "_cache_get", return_value=None), \
             patch.object(self._module, "_cache_set"):
            result = self._module.fetch_prizepicks_props_from_mirror(
                include_tomorrow=True
            )
        # Should aggregate today + tomorrow props
        self.assertEqual(len(result), 2)


class TestFetchPrizePicksPropsOddsType(unittest.TestCase):
    """Verify fetch_prizepicks_props() includes odds_type from the live API."""

    def setUp(self):
        try:
            from data import platform_fetcher
            self._module = platform_fetcher
        except ImportError as exc:
            self.skipTest(f"platform_fetcher not importable: {exc}")

    def _make_api_response(self, projections, included=None):
        """Build a minimal PrizePicks API response dict."""
        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "data": projections,
            "included": included or [],
        }
        mock_resp.raise_for_status.return_value = None
        return mock_resp

    def _make_projection(self, player_id, stat_type, line_score,
                         odds_type="standard", league="NBA"):
        return {
            "type": "projection",
            "id": "proj1",
            "attributes": {
                "stat_type": stat_type,
                "line_score": line_score,
                "odds_type": odds_type,
                "league": league,
            },
            "relationships": {
                "new_player": {"data": {"id": player_id}}
            },
        }

    def _make_player(self, player_id, name, team):
        return {
            "type": "new_player",
            "id": player_id,
            "attributes": {"name": name, "team": team},
        }

    def test_live_api_captures_standard_odds_type(self):
        """Live API path captures odds_type='standard' when mirror is empty."""
        proj = self._make_projection("p1", "Points", 25.5, odds_type="standard")
        player = self._make_player("p1", "LeBron James", "LAL")

        with patch.object(self._module, "fetch_prizepicks_props_from_mirror",
                          return_value=[]), \
             patch.object(self._module, "_fetch_with_retry",
                          return_value=self._make_api_response([proj], [player])), \
             patch.object(self._module, "_cache_get", return_value=None), \
             patch.object(self._module, "_cache_set"):
            result = self._module.fetch_prizepicks_props()

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["odds_type"], "standard")

    def test_live_api_captures_goblin_odds_type(self):
        """Live API path captures odds_type='goblin'."""
        proj = self._make_projection("p1", "Points", 22.5, odds_type="goblin")
        player = self._make_player("p1", "Stephen Curry", "GSW")

        with patch.object(self._module, "fetch_prizepicks_props_from_mirror",
                          return_value=[]), \
             patch.object(self._module, "_fetch_with_retry",
                          return_value=self._make_api_response([proj], [player])), \
             patch.object(self._module, "_cache_get", return_value=None), \
             patch.object(self._module, "_cache_set"):
            result = self._module.fetch_prizepicks_props()

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["odds_type"], "goblin")

    def test_live_api_captures_demon_odds_type(self):
        """Live API path captures odds_type='demon'."""
        proj = self._make_projection("p1", "Points", 30.5, odds_type="demon")
        player = self._make_player("p1", "Nikola Jokic", "DEN")

        with patch.object(self._module, "fetch_prizepicks_props_from_mirror",
                          return_value=[]), \
             patch.object(self._module, "_fetch_with_retry",
                          return_value=self._make_api_response([proj], [player])), \
             patch.object(self._module, "_cache_get", return_value=None), \
             patch.object(self._module, "_cache_set"):
            result = self._module.fetch_prizepicks_props()

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["odds_type"], "demon")

    def test_mirror_used_when_available(self):
        """fetch_prizepicks_props() returns mirror data when the mirror succeeds."""
        mirror_props = [
            {"player_name": "Mirror Player", "stat_type": "points",
             "line": 20.0, "odds_type": "goblin", "platform": "PrizePicks",
             "team": "MIA", "game_date": "2026-03-25",
             "fetched_at": "2026-03-25T00:00:00+00:00",
             "over_odds": -110, "under_odds": -110},
        ]
        with patch.object(self._module, "fetch_prizepicks_props_from_mirror",
                          return_value=mirror_props):
            result = self._module.fetch_prizepicks_props()

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["odds_type"], "goblin")
        self.assertEqual(result[0]["player_name"], "Mirror Player")

    def test_fallback_to_live_api_when_mirror_empty(self):
        """fetch_prizepicks_props() falls back to live API when mirror returns []."""
        proj = self._make_projection("p1", "Points", 25.5, odds_type="standard")
        player = self._make_player("p1", "LeBron James", "LAL")

        with patch.object(self._module, "fetch_prizepicks_props_from_mirror",
                          return_value=[]), \
             patch.object(self._module, "_fetch_with_retry",
                          return_value=self._make_api_response([proj], [player])), \
             patch.object(self._module, "_cache_get", return_value=None), \
             patch.object(self._module, "_cache_set"):
            result = self._module.fetch_prizepicks_props()

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["player_name"], "LeBron James")

    def test_live_api_default_odds_type_when_missing(self):
        """If the live API omits odds_type, defaults to 'standard'."""
        proj = self._make_projection("p1", "Points", 25.5, odds_type="standard")
        # Remove odds_type from attributes
        del proj["attributes"]["odds_type"]
        player = self._make_player("p1", "LeBron James", "LAL")

        with patch.object(self._module, "fetch_prizepicks_props_from_mirror",
                          return_value=[]), \
             patch.object(self._module, "_fetch_with_retry",
                          return_value=self._make_api_response([proj], [player])), \
             patch.object(self._module, "_cache_get", return_value=None), \
             patch.object(self._module, "_cache_set"):
            result = self._module.fetch_prizepicks_props()

        self.assertEqual(result[0]["odds_type"], "standard")


class TestAsyncFetchPrizePicks(unittest.TestCase):
    """Tests that the async PrizePicks fetcher mirrors the sync strategy:
    mirror first, then live API fallback, with odds_type populated."""

    def setUp(self):
        try:
            from data import platform_fetcher
            self._module = platform_fetcher
        except ImportError as exc:
            self.skipTest(f"platform_fetcher not importable: {exc}")

    def _run(self, coro):
        """Run a coroutine synchronously for testing."""
        import asyncio
        loop = asyncio.new_event_loop()
        try:
            return loop.run_until_complete(coro)
        finally:
            loop.close()

    def test_async_uses_mirror_when_available(self):
        """_async_fetch_prizepicks returns mirror data (with odds_type) when available."""
        mirror_props = [
            {"player_name": "Mirror Player", "stat_type": "points",
             "line": 20.0, "odds_type": "goblin", "platform": "PrizePicks",
             "team": "MIA", "game_date": "2026-03-25",
             "fetched_at": "2026-03-25T00:00:00+00:00",
             "over_odds": -110, "under_odds": -110},
        ]
        with patch.object(self._module, "fetch_prizepicks_props_from_mirror",
                          return_value=mirror_props):
            coro = self._module._async_fetch_prizepicks(MagicMock(), MagicMock())
            result = self._run(coro)

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["odds_type"], "goblin")
        self.assertEqual(result[0]["player_name"], "Mirror Player")

    def test_async_includes_odds_type_in_live_api_fallback(self):
        """_async_fetch_prizepicks populates odds_type from live API when mirror is empty."""
        proj = {
            "type": "projection",
            "id": "proj1",
            "attributes": {
                "stat_type": "Points",
                "line_score": 25.5,
                "odds_type": "demon",
                "league": "NBA",
            },
            "relationships": {
                "new_player": {"data": {"id": "p1"}}
            },
        }
        player = {
            "type": "new_player",
            "id": "p1",
            "attributes": {"name": "Nikola Jokic", "team": "DEN"},
        }

        mock_resp_data = {"data": [proj], "included": [player]}

        async def fake_fetch_json(session, url, headers=None, params=None):
            return mock_resp_data

        import asyncio

        async def run():
            semaphore = asyncio.Semaphore(1)
            with patch.object(self._module, "fetch_prizepicks_props_from_mirror",
                               return_value=[]), \
                 patch.object(self._module, "_async_fetch_json",
                               side_effect=fake_fetch_json):
                return await self._module._async_fetch_prizepicks(MagicMock(), semaphore)

        result = self._run(run())
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["odds_type"], "demon")
        self.assertEqual(result[0]["player_name"], "Nikola Jokic")

    def test_async_returns_empty_when_mirror_empty_and_api_fails(self):
        """_async_fetch_prizepicks returns [] when both mirror and live API fail."""
        async def fake_fetch_json(session, url, headers=None, params=None):
            return None

        import asyncio

        async def run():
            semaphore = asyncio.Semaphore(1)
            with patch.object(self._module, "fetch_prizepicks_props_from_mirror",
                               return_value=[]), \
                 patch.object(self._module, "_async_fetch_json",
                               side_effect=fake_fetch_json):
                return await self._module._async_fetch_prizepicks(MagicMock(), semaphore)

        result = self._run(run())
        self.assertEqual(result, [])

    def test_async_goblin_from_mirror(self):
        """_async_fetch_prizepicks passes through goblin lines from mirror."""
        mirror_props = [
            {"player_name": "Stephen Curry", "stat_type": "threes",
             "line": 3.5, "odds_type": "goblin", "platform": "PrizePicks",
             "team": "GSW", "game_date": "2026-03-25",
             "fetched_at": "2026-03-25T00:00:00+00:00",
             "over_odds": -110, "under_odds": -110},
            {"player_name": "Stephen Curry", "stat_type": "threes",
             "line": 5.5, "odds_type": "demon", "platform": "PrizePicks",
             "team": "GSW", "game_date": "2026-03-25",
             "fetched_at": "2026-03-25T00:00:00+00:00",
             "over_odds": -110, "under_odds": -110},
        ]
        with patch.object(self._module, "fetch_prizepicks_props_from_mirror",
                          return_value=mirror_props):
            coro = self._module._async_fetch_prizepicks(MagicMock(), MagicMock())
            result = self._run(coro)

        self.assertEqual(len(result), 2)
        types = {p["odds_type"] for p in result}
        self.assertIn("goblin", types)
        self.assertIn("demon", types)


if __name__ == "__main__":
    unittest.main()
