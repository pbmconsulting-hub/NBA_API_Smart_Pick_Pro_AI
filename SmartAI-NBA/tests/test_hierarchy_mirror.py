# ============================================================
# FILE: tests/test_hierarchy_mirror.py
# PURPOSE: Tests for the NBA hierarchy mirror integration in
#          data/platform_fetcher.py — hierarchy parser, fetcher,
#          team fetcher, archive fetcher, slate grouping, and
#          same-game grouping.
# ============================================================
import unittest
from unittest.mock import MagicMock, patch


class TestNBATeamSlugs(unittest.TestCase):
    """Verify NBA_TEAM_SLUGS contains all 30 NBA teams."""

    def setUp(self):
        try:
            from data.platform_fetcher import NBA_TEAM_SLUGS
            self.slugs = NBA_TEAM_SLUGS
        except ImportError as exc:
            self.skipTest(f"platform_fetcher not importable: {exc}")

    def test_has_30_teams(self):
        self.assertEqual(len(self.slugs), 30)

    def test_all_values_are_strings(self):
        for abbrev, slug in self.slugs.items():
            self.assertIsInstance(abbrev, str)
            self.assertIsInstance(slug, str)

    def test_known_teams_present(self):
        known = ["LAL", "GSW", "BOS", "MIA", "CHI", "NYK", "DAL", "SAS"]
        for team in known:
            self.assertIn(team, self.slugs, f"{team} missing from NBA_TEAM_SLUGS")

    def test_lal_slug(self):
        self.assertEqual(self.slugs["LAL"], "los-angeles-lakers")

    def test_gsw_slug(self):
        self.assertEqual(self.slugs["GSW"], "golden-state-warriors")


class TestParseHierarchyProps(unittest.TestCase):
    """Unit tests for _parse_hierarchy_props()."""

    def setUp(self):
        try:
            from data.platform_fetcher import _parse_hierarchy_props
            self._parse = _parse_hierarchy_props
        except ImportError as exc:
            self.skipTest(f"platform_fetcher not importable: {exc}")

    def _make_prop(self, **overrides):
        base = {
            "playerId": "p1",
            "stat": "Points",
            "line": 25.5,
            "teamCode": "LAL",
            "opponentCode": "GSW",
            "startDateCST": "2026-03-25",
            "startTime": "7:30 PM ET",
            "gameId": "game123",
        }
        base.update(overrides)
        return base

    def _make_players(self):
        return {"p1": {"playerName": "LeBron James", "playerId": "p1"}}

    def test_parses_standard_prop(self):
        props = [self._make_prop()]
        result = self._parse(props, self._make_players(), "2026-03-25", "now")
        self.assertEqual(len(result), 1)
        p = result[0]
        self.assertEqual(p["player_name"], "LeBron James")
        self.assertEqual(p["line"], 25.5)
        self.assertEqual(p["team"], "LAL")
        self.assertEqual(p["opponent"], "GSW")
        self.assertEqual(p["game_id"], "game123")
        self.assertEqual(p["start_time"], "7:30 PM ET")
        self.assertEqual(p["source"], "hierarchy")
        self.assertEqual(p["odds_type"], "standard")
        self.assertEqual(p["platform"], "PrizePicks")

    def test_skips_missing_player_name(self):
        """Props without a player name (no playerId match, no 'player' field) are skipped."""
        props = [self._make_prop()]
        result = self._parse(props, {}, "2026-03-25", "now")
        # playerId "p1" not in lookup, no 'player' field → skip
        self.assertEqual(len(result), 0)

    def test_uses_player_field_as_fallback(self):
        """Falls back to the 'player' field if playerId not in lookup."""
        props = [self._make_prop(player="Anthony Davis")]
        result = self._parse(props, {}, "2026-03-25", "now")
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["player_name"], "Anthony Davis")

    def test_skips_zero_line(self):
        props = [self._make_prop(line=0)]
        result = self._parse(props, self._make_players(), "2026-03-25", "now")
        self.assertEqual(len(result), 0)

    def test_skips_negative_line(self):
        props = [self._make_prop(line=-5.0)]
        result = self._parse(props, self._make_players(), "2026-03-25", "now")
        self.assertEqual(len(result), 0)

    def test_skips_invalid_line(self):
        props = [self._make_prop(line="N/A")]
        result = self._parse(props, self._make_players(), "2026-03-25", "now")
        self.assertEqual(len(result), 0)

    def test_odds_type_is_standard(self):
        props = [self._make_prop()]
        result = self._parse(props, self._make_players(), "2026-03-25", "now")
        self.assertEqual(result[0]["odds_type"], "standard")

    def test_includes_game_id(self):
        props = [self._make_prop(gameId="abc999")]
        result = self._parse(props, self._make_players(), "2026-03-25", "now")
        self.assertEqual(result[0]["game_id"], "abc999")

    def test_includes_opponent(self):
        props = [self._make_prop(opponentCode="BOS")]
        result = self._parse(props, self._make_players(), "2026-03-25", "now")
        self.assertEqual(result[0]["opponent"], "BOS")

    def test_includes_start_time(self):
        props = [self._make_prop(startTime="8:00 PM ET")]
        result = self._parse(props, self._make_players(), "2026-03-25", "now")
        self.assertEqual(result[0]["start_time"], "8:00 PM ET")

    def test_includes_source_hierarchy(self):
        props = [self._make_prop()]
        result = self._parse(props, self._make_players(), "2026-03-25", "now")
        self.assertEqual(result[0]["source"], "hierarchy")

    def test_uses_today_as_date_fallback(self):
        prop = self._make_prop()
        del prop["startDateCST"]
        result = self._parse([prop], self._make_players(), "2026-01-01", "now")
        self.assertEqual(result[0]["game_date"], "2026-01-01")

    def test_multiple_props(self):
        props = [
            self._make_prop(playerId="p1", stat="Points", line=25.5),
            self._make_prop(playerId="p2", stat="Rebounds", line=8.5),
        ]
        players = {
            "p1": {"playerName": "LeBron James"},
            "p2": {"playerName": "Anthony Davis"},
        }
        result = self._parse(props, players, "2026-03-25", "now")
        self.assertEqual(len(result), 2)


class TestFetchPrizePicksHierarchyData(unittest.TestCase):
    """Tests for fetch_prizepicks_hierarchy_data()."""

    def setUp(self):
        try:
            from data import platform_fetcher
            self._module = platform_fetcher
        except ImportError as exc:
            self.skipTest(f"platform_fetcher not importable: {exc}")

    def test_returns_dict_with_expected_keys(self):
        mock_resp = MagicMock()
        mock_resp.json.return_value = []
        mock_resp.raise_for_status.return_value = None

        with patch.object(self._module, "_fetch_with_retry", return_value=mock_resp), \
             patch.object(self._module, "_cache_get", return_value=None), \
             patch.object(self._module, "_cache_set"):
            result = self._module.fetch_prizepicks_hierarchy_data()

        self.assertIsInstance(result, dict)
        for key in ("games", "teams", "players", "props", "slates", "raw_props"):
            self.assertIn(key, result)

    def test_handles_network_failure(self):
        with patch.object(self._module, "_fetch_with_retry", return_value=None), \
             patch.object(self._module, "_cache_get", return_value=None):
            result = self._module.fetch_prizepicks_hierarchy_data()

        self.assertIsInstance(result, dict)
        self.assertEqual(result.get("props", []), [])

    def test_returns_empty_dict_when_requests_unavailable(self):
        original = self._module.REQUESTS_AVAILABLE
        try:
            self._module.REQUESTS_AVAILABLE = False
            result = self._module.fetch_prizepicks_hierarchy_data()
            self.assertEqual(result, {})
        finally:
            self._module.REQUESTS_AVAILABLE = original

    def test_parses_props_from_raw_props(self):
        raw_props = [{"player": "LeBron James", "stat": "Points", "line": 25.5}]

        def fake_fetch(url, **kwargs):
            mock = MagicMock()
            mock.raise_for_status.return_value = None
            if "props" in url:
                mock.json.return_value = raw_props
            else:
                mock.json.return_value = []
            return mock

        with patch.object(self._module, "_fetch_with_retry", side_effect=fake_fetch), \
             patch.object(self._module, "_cache_get", return_value=None), \
             patch.object(self._module, "_cache_set"):
            result = self._module.fetch_prizepicks_hierarchy_data()

        self.assertIsInstance(result["props"], list)


class TestFetchTeamPropsFromMirror(unittest.TestCase):
    """Tests for fetch_team_props_from_mirror()."""

    def setUp(self):
        try:
            from data import platform_fetcher
            self._module = platform_fetcher
        except ImportError as exc:
            self.skipTest(f"platform_fetcher not importable: {exc}")

    def test_returns_empty_for_unknown_abbreviation(self):
        result = self._module.fetch_team_props_from_mirror("XYZ")
        self.assertEqual(result, [])

    def test_fetches_correct_url_for_lal(self):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"props": []}
        mock_resp.raise_for_status.return_value = None

        with patch.object(self._module, "_fetch_with_retry", return_value=mock_resp) as mock_fetch, \
             patch.object(self._module, "_cache_get", return_value=None), \
             patch.object(self._module, "_cache_set"):
            self._module.fetch_team_props_from_mirror("LAL")

        called_url = mock_fetch.call_args[0][0]
        self.assertIn("los-angeles-lakers.json", called_url)

    def test_fetches_correct_url_for_gsw(self):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"props": []}
        mock_resp.raise_for_status.return_value = None

        with patch.object(self._module, "_fetch_with_retry", return_value=mock_resp) as mock_fetch, \
             patch.object(self._module, "_cache_get", return_value=None), \
             patch.object(self._module, "_cache_set"):
            self._module.fetch_team_props_from_mirror("GSW")

        called_url = mock_fetch.call_args[0][0]
        self.assertIn("golden-state-warriors.json", called_url)

    def test_case_insensitive_lookup(self):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"props": []}
        mock_resp.raise_for_status.return_value = None

        with patch.object(self._module, "_fetch_with_retry", return_value=mock_resp) as mock_fetch, \
             patch.object(self._module, "_cache_get", return_value=None), \
             patch.object(self._module, "_cache_set"):
            self._module.fetch_team_props_from_mirror("lal")

        called_url = mock_fetch.call_args[0][0]
        self.assertIn("los-angeles-lakers.json", called_url)

    def test_returns_empty_on_network_failure(self):
        with patch.object(self._module, "_fetch_with_retry", return_value=None), \
             patch.object(self._module, "_cache_get", return_value=None):
            result = self._module.fetch_team_props_from_mirror("BOS")
        self.assertEqual(result, [])

    def test_returns_list_of_props(self):
        mock_resp = MagicMock()
        mock_resp.raise_for_status.return_value = None
        mock_resp.json.return_value = {
            "props": [
                {"player": "Jayson Tatum", "stat": "Points", "line": 27.5,
                 "oddsType": "standard", "teamCode": "BOS"}
            ]
        }

        with patch.object(self._module, "_fetch_with_retry", return_value=mock_resp), \
             patch.object(self._module, "_cache_get", return_value=None), \
             patch.object(self._module, "_cache_set"):
            result = self._module.fetch_team_props_from_mirror("BOS")

        self.assertIsInstance(result, list)
        self.assertGreater(len(result), 0)
        self.assertEqual(result[0]["player_name"], "Jayson Tatum")


class TestFetchArchivedNbaProps(unittest.TestCase):
    """Tests for fetch_archived_nba_props()."""

    def setUp(self):
        try:
            from data import platform_fetcher
            self._module = platform_fetcher
        except ImportError as exc:
            self.skipTest(f"platform_fetcher not importable: {exc}")

    def test_returns_empty_on_network_failure(self):
        with patch.object(self._module, "_fetch_with_retry", return_value=None), \
             patch.object(self._module, "_cache_get", return_value=None):
            result = self._module.fetch_archived_nba_props("2026-03-01")
        self.assertEqual(result, [])

    def test_parses_hierarchy_list_format(self):
        raw = [{"player": "LeBron James", "stat": "Points", "line": 25.5}]
        mock_resp = MagicMock()
        mock_resp.raise_for_status.return_value = None
        mock_resp.json.return_value = raw

        with patch.object(self._module, "_fetch_with_retry", return_value=mock_resp), \
             patch.object(self._module, "_cache_get", return_value=None), \
             patch.object(self._module, "_cache_set"):
            result = self._module.fetch_archived_nba_props("2026-03-01")

        self.assertIsInstance(result, list)
        self.assertGreater(len(result), 0)
        self.assertEqual(result[0]["player_name"], "LeBron James")

    def test_parses_flat_dict_format(self):
        flat = {"props": [
            {"player": "LeBron James", "stat": "Points", "line": 25.5,
             "oddsType": "standard", "teamCode": "LAL"}
        ]}
        mock_resp = MagicMock()
        mock_resp.raise_for_status.return_value = None
        mock_resp.json.return_value = flat

        with patch.object(self._module, "_fetch_with_retry", return_value=mock_resp), \
             patch.object(self._module, "_cache_get", return_value=None), \
             patch.object(self._module, "_cache_set"):
            result = self._module.fetch_archived_nba_props("2026-03-01")

        self.assertIsInstance(result, list)
        self.assertGreater(len(result), 0)

    def test_uses_correct_date_in_url(self):
        mock_resp = MagicMock()
        mock_resp.raise_for_status.return_value = None
        mock_resp.json.return_value = []

        with patch.object(self._module, "_fetch_with_retry", return_value=mock_resp) as mock_fetch, \
             patch.object(self._module, "_cache_get", return_value=None), \
             patch.object(self._module, "_cache_set"):
            self._module.fetch_archived_nba_props("2026-01-15")

        called_url = mock_fetch.call_args[0][0]
        self.assertIn("2026-01-15", called_url)
        self.assertIn("archive", called_url)

    def test_returns_empty_for_unknown_format(self):
        mock_resp = MagicMock()
        mock_resp.raise_for_status.return_value = None
        mock_resp.json.return_value = {"something": "unexpected"}

        with patch.object(self._module, "_fetch_with_retry", return_value=mock_resp), \
             patch.object(self._module, "_cache_get", return_value=None), \
             patch.object(self._module, "_cache_set"):
            result = self._module.fetch_archived_nba_props("2026-03-01")

        self.assertEqual(result, [])


class TestGetSlateGroupedProps(unittest.TestCase):
    """Tests for get_slate_grouped_props()."""

    def setUp(self):
        try:
            from data import platform_fetcher
            self._module = platform_fetcher
        except ImportError as exc:
            self.skipTest(f"platform_fetcher not importable: {exc}")

    def test_returns_early_late_dict(self):
        hierarchy_data = {
            "games": [{"gameId": "g1", "slate": "Early"}],
            "props": [{"player_name": "LeBron", "game_id": "g1", "stat_type": "points", "line": 25.5}],
        }
        with patch.object(self._module, "fetch_prizepicks_hierarchy_data",
                          return_value=hierarchy_data):
            result = self._module.get_slate_grouped_props()

        self.assertIn("Early", result)
        self.assertIn("Late", result)

    def test_routes_props_to_correct_slate(self):
        hierarchy_data = {
            "games": [
                {"gameId": "g1", "slate": "Early"},
                {"gameId": "g2", "slate": "Late"},
            ],
            "props": [
                {"player_name": "Player A", "game_id": "g1", "stat_type": "points", "line": 20.5},
                {"player_name": "Player B", "game_id": "g2", "stat_type": "points", "line": 18.5},
            ],
        }
        with patch.object(self._module, "fetch_prizepicks_hierarchy_data",
                          return_value=hierarchy_data):
            result = self._module.get_slate_grouped_props()

        self.assertEqual(len(result["Early"]), 1)
        self.assertEqual(result["Early"][0]["player_name"], "Player A")
        self.assertEqual(len(result["Late"]), 1)
        self.assertEqual(result["Late"][0]["player_name"], "Player B")

    def test_defaults_unknown_game_id_to_late(self):
        hierarchy_data = {
            "games": [],
            "props": [{"player_name": "Player X", "game_id": "unknown_id", "stat_type": "points", "line": 15.0}],
        }
        with patch.object(self._module, "fetch_prizepicks_hierarchy_data",
                          return_value=hierarchy_data):
            result = self._module.get_slate_grouped_props()

        self.assertEqual(len(result["Late"]), 1)


class TestGetSameGamePropGroups(unittest.TestCase):
    """Tests for get_same_game_prop_groups()."""

    def setUp(self):
        try:
            from data import platform_fetcher
            self._module = platform_fetcher
        except ImportError as exc:
            self.skipTest(f"platform_fetcher not importable: {exc}")

    def test_groups_by_game_id(self):
        hierarchy_data = {
            "props": [
                {"player_name": "Player A", "game_id": "g1", "stat_type": "points", "line": 20.0},
                {"player_name": "Player B", "game_id": "g1", "stat_type": "points", "line": 18.0},
                {"player_name": "Player C", "game_id": "g2", "stat_type": "assists", "line": 6.5},
            ],
        }
        with patch.object(self._module, "fetch_prizepicks_hierarchy_data",
                          return_value=hierarchy_data):
            result = self._module.get_same_game_prop_groups()

        self.assertIn("g1", result)
        self.assertIn("g2", result)
        self.assertEqual(len(result["g1"]), 2)
        self.assertEqual(len(result["g2"]), 1)

    def test_returns_dict(self):
        with patch.object(self._module, "fetch_prizepicks_hierarchy_data",
                          return_value={"props": []}):
            result = self._module.get_same_game_prop_groups()
        self.assertIsInstance(result, dict)

    def test_props_without_game_id_go_to_unknown(self):
        hierarchy_data = {
            "props": [{"player_name": "Player X", "stat_type": "points", "line": 20.0}],
        }
        with patch.object(self._module, "fetch_prizepicks_hierarchy_data",
                          return_value=hierarchy_data):
            result = self._module.get_same_game_prop_groups()

        self.assertIn("unknown", result)


if __name__ == "__main__":
    unittest.main()
