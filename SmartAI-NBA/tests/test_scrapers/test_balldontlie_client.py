"""tests/test_scrapers/test_balldontlie_client.py – Unit tests for the BallDontLie API client."""
from __future__ import annotations

import time
from unittest.mock import MagicMock, patch

import pytest


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_response(json_data: dict, status_code: int = 200) -> MagicMock:
    """Build a mock requests.Response."""
    resp = MagicMock()
    resp.status_code = status_code
    resp.json.return_value = json_data
    if status_code >= 400:
        from requests.exceptions import HTTPError
        resp.raise_for_status.side_effect = HTTPError(response=resp)
    else:
        resp.raise_for_status.return_value = None
    return resp


# ---------------------------------------------------------------------------
# Module constants
# ---------------------------------------------------------------------------

class TestModuleConstants:
    def test_base_url(self):
        import engine.scrapers.balldontlie_client as mod
        assert mod._BASE_URL == "https://api.balldontlie.io"

    def test_delay(self):
        import engine.scrapers.balldontlie_client as mod
        assert mod._DELAY >= 1.0

    def test_ttl_constants(self):
        import engine.scrapers.balldontlie_client as mod
        assert mod.LIVE_TTL == 300
        assert mod.HIST_TTL == 3600


# ---------------------------------------------------------------------------
# API key loading
# ---------------------------------------------------------------------------

class TestLoadApiKey:
    def test_loads_from_env(self, monkeypatch):
        monkeypatch.setenv("BALLDONTLIE_API_KEY", "test-key-123")
        import engine.scrapers.balldontlie_client as mod
        assert mod._load_api_key() == "test-key-123"

    def test_returns_empty_when_missing(self, monkeypatch):
        monkeypatch.delenv("BALLDONTLIE_API_KEY", raising=False)
        import engine.scrapers.balldontlie_client as mod
        # patch streamlit to raise so it falls through to empty string
        with patch.dict("sys.modules", {"streamlit": None}):
            key = mod._load_api_key()
        assert isinstance(key, str)

    def test_authorization_header_set(self, monkeypatch):
        monkeypatch.setenv("BALLDONTLIE_API_KEY", "mykey")
        import engine.scrapers.balldontlie_client as mod
        captured = {}

        def fake_get(url, params, headers, timeout):
            captured["headers"] = headers
            return _make_response({"data": []})

        with patch("engine.scrapers.balldontlie_client.requests") as mock_req:
            mock_req.get.side_effect = fake_get
            mod._get("/nba/v1/players")

        assert captured["headers"].get("Authorization") == "mykey"

    def test_no_authorization_header_when_key_empty(self, monkeypatch):
        monkeypatch.delenv("BALLDONTLIE_API_KEY", raising=False)
        import engine.scrapers.balldontlie_client as mod
        captured = {}

        def fake_get(url, params, headers, timeout):
            captured["headers"] = headers
            return _make_response({"data": []})

        with patch("engine.scrapers.balldontlie_client.requests") as mock_req, \
             patch.object(mod, "_load_api_key", return_value=""):
            mock_req.get.side_effect = fake_get
            mod._get("/nba/v1/players")

        assert "Authorization" not in captured["headers"]


# ---------------------------------------------------------------------------
# has_api_key
# ---------------------------------------------------------------------------

class TestHasApiKey:
    def test_returns_true_when_key_set(self, monkeypatch):
        monkeypatch.setenv("BALLDONTLIE_API_KEY", "test-key")
        import engine.scrapers.balldontlie_client as mod
        assert mod.has_api_key() is True

    def test_returns_false_when_key_missing(self, monkeypatch):
        monkeypatch.delenv("BALLDONTLIE_API_KEY", raising=False)
        import engine.scrapers.balldontlie_client as mod
        with patch.object(mod, "_load_api_key", return_value=""):
            assert mod.has_api_key() is False


# ---------------------------------------------------------------------------
# Error handling
# ---------------------------------------------------------------------------

class TestErrorHandling:
    def test_returns_empty_dict_on_401(self, monkeypatch):
        import engine.scrapers.balldontlie_client as mod
        resp = _make_response({}, status_code=401)
        resp.raise_for_status.side_effect = None  # we check status_code first
        with patch("engine.scrapers.balldontlie_client.requests") as mock_req:
            mock_req.get.return_value = resp
            result = mod._get("/nba/v1/players")
        assert result == {}

    def test_returns_empty_dict_on_403(self, monkeypatch):
        import engine.scrapers.balldontlie_client as mod
        resp = _make_response({}, status_code=403)
        resp.raise_for_status.side_effect = None
        with patch("engine.scrapers.balldontlie_client.requests") as mock_req:
            mock_req.get.return_value = resp
            result = mod._get("/nba/v1/players")
        assert result == {}

    def test_returns_empty_dict_on_404(self, monkeypatch):
        import engine.scrapers.balldontlie_client as mod
        resp = _make_response({}, status_code=404)
        resp.raise_for_status.side_effect = None
        with patch("engine.scrapers.balldontlie_client.requests") as mock_req:
            mock_req.get.return_value = resp
            result = mod._get("/nba/v1/players")
        assert result == {}

    def test_returns_empty_dict_on_429(self, monkeypatch):
        import engine.scrapers.balldontlie_client as mod
        resp = _make_response({}, status_code=429)
        resp.raise_for_status.side_effect = None
        with patch("engine.scrapers.balldontlie_client.requests") as mock_req:
            mock_req.get.return_value = resp
            result = mod._get("/nba/v1/players")
        assert result == {}

    def test_returns_empty_dict_on_500(self, monkeypatch):
        import engine.scrapers.balldontlie_client as mod
        with patch("engine.scrapers.balldontlie_client.requests") as mock_req:
            mock_req.get.return_value = _make_response({}, status_code=500)
            result = mod._get("/nba/v1/players")
        assert result == {}

    def test_returns_empty_dict_on_network_exception(self):
        import engine.scrapers.balldontlie_client as mod
        with patch("engine.scrapers.balldontlie_client.requests") as mock_req:
            mock_req.get.side_effect = ConnectionError("timeout")
            result = mod._get("/nba/v1/players")
        assert result == {}

    def test_graceful_degradation_when_requests_unavailable(self):
        import engine.scrapers.balldontlie_client as mod
        original = mod._REQUESTS_AVAILABLE
        mod._REQUESTS_AVAILABLE = False
        try:
            assert mod._get("/nba/v1/players") == {}
        finally:
            mod._REQUESTS_AVAILABLE = original


# ---------------------------------------------------------------------------
# Caching helpers
# ---------------------------------------------------------------------------

class TestCaching:
    def setup_method(self):
        import engine.scrapers.balldontlie_client as mod
        mod._CACHE.clear()

    def test_cache_set_and_get(self):
        import engine.scrapers.balldontlie_client as mod
        mod._cache_set("key1", [1, 2, 3])
        assert mod._cache_get("key1", ttl=60) == [1, 2, 3]

    def test_cache_miss_returns_none(self):
        import engine.scrapers.balldontlie_client as mod
        assert mod._cache_get("nonexistent", ttl=60) is None

    def test_cache_expiry(self):
        import engine.scrapers.balldontlie_client as mod
        mod._cache_set("expiring", "value")
        # Manually back-date the timestamp to force expiry
        mod._CACHE["expiring"] = ("value", time.time() - 3700)
        assert mod._cache_get("expiring", ttl=3600) is None

    def test_endpoint_result_is_cached(self):
        import engine.scrapers.balldontlie_client as mod
        mod._CACHE.clear()
        with patch("engine.scrapers.balldontlie_client.requests") as mock_req:
            mock_req.get.return_value = _make_response({"data": [{"id": 1}]})
            result1 = mod.get_teams()
            result2 = mod.get_teams()  # should come from cache
        assert mock_req.get.call_count == 1
        assert result1 == result2 == [{"id": 1}]


# ---------------------------------------------------------------------------
# Teams
# ---------------------------------------------------------------------------

class TestTeams:
    def setup_method(self):
        import engine.scrapers.balldontlie_client as mod
        mod._CACHE.clear()

    def test_get_teams_returns_list(self):
        import engine.scrapers.balldontlie_client as mod
        payload = {"data": [{"id": 1, "full_name": "Boston Celtics"}]}
        with patch("engine.scrapers.balldontlie_client.requests") as mock_req:
            mock_req.get.return_value = _make_response(payload)
            result = mod.get_teams()
        assert isinstance(result, list)
        assert result[0]["full_name"] == "Boston Celtics"

    def test_get_teams_conference_param(self):
        import engine.scrapers.balldontlie_client as mod
        with patch("engine.scrapers.balldontlie_client.requests") as mock_req:
            mock_req.get.return_value = _make_response({"data": []})
            mod.get_teams(conference="East")
        call_kwargs = mock_req.get.call_args
        assert call_kwargs[1]["params"]["conference"] == "East"

    def test_get_team_returns_dict(self):
        import engine.scrapers.balldontlie_client as mod
        payload = {"data": {"id": 1, "full_name": "Boston Celtics"}}
        with patch("engine.scrapers.balldontlie_client.requests") as mock_req:
            mock_req.get.return_value = _make_response(payload)
            result = mod.get_team(1)
        assert isinstance(result, dict)
        assert result["id"] == 1

    def test_get_teams_returns_empty_on_failure(self):
        import engine.scrapers.balldontlie_client as mod
        with patch("engine.scrapers.balldontlie_client.requests") as mock_req:
            mock_req.get.side_effect = Exception("network error")
            result = mod.get_teams()
        assert result == []

    def test_get_team_returns_empty_on_failure(self):
        import engine.scrapers.balldontlie_client as mod
        with patch("engine.scrapers.balldontlie_client.requests") as mock_req:
            mock_req.get.side_effect = Exception("network error")
            result = mod.get_team(999)
        assert result == {}


# ---------------------------------------------------------------------------
# Players
# ---------------------------------------------------------------------------

class TestPlayers:
    def setup_method(self):
        import engine.scrapers.balldontlie_client as mod
        mod._CACHE.clear()

    def test_get_players_returns_list(self):
        import engine.scrapers.balldontlie_client as mod
        payload = {"data": [{"id": 237, "first_name": "LeBron", "last_name": "James"}]}
        with patch("engine.scrapers.balldontlie_client.requests") as mock_req:
            mock_req.get.return_value = _make_response(payload)
            result = mod.get_players(search="LeBron")
        assert isinstance(result, list)
        assert result[0]["first_name"] == "LeBron"

    def test_get_active_players_returns_list(self):
        import engine.scrapers.balldontlie_client as mod
        with patch("engine.scrapers.balldontlie_client.requests") as mock_req:
            mock_req.get.return_value = _make_response({"data": [{"id": 237}]})
            result = mod.get_active_players()
        assert isinstance(result, list)

    def test_get_player_returns_dict(self):
        import engine.scrapers.balldontlie_client as mod
        payload = {"data": {"id": 237, "first_name": "LeBron"}}
        with patch("engine.scrapers.balldontlie_client.requests") as mock_req:
            mock_req.get.return_value = _make_response(payload)
            result = mod.get_player(237)
        assert isinstance(result, dict)

    def test_search_players_alias(self):
        import engine.scrapers.balldontlie_client as mod
        with patch.object(mod, "get_players", return_value=[{"id": 1}]) as mock_gp:
            result = mod.search_players("James")
        mock_gp.assert_called_once_with(search="James")
        assert result == [{"id": 1}]

    def test_get_players_returns_empty_on_failure(self):
        import engine.scrapers.balldontlie_client as mod
        with patch("engine.scrapers.balldontlie_client.requests") as mock_req:
            mock_req.get.side_effect = Exception("error")
            assert mod.get_players() == []


# ---------------------------------------------------------------------------
# Games
# ---------------------------------------------------------------------------

class TestGames:
    def setup_method(self):
        import engine.scrapers.balldontlie_client as mod
        mod._CACHE.clear()

    def test_get_games_returns_list(self):
        import engine.scrapers.balldontlie_client as mod
        payload = {"data": [{"id": 1001}]}
        with patch("engine.scrapers.balldontlie_client.requests") as mock_req:
            mock_req.get.return_value = _make_response(payload)
            result = mod.get_games(date="2024-01-15")
        assert isinstance(result, list)

    def test_get_game_returns_dict(self):
        import engine.scrapers.balldontlie_client as mod
        payload = {"data": {"id": 1001, "home_team_score": 110}}
        with patch("engine.scrapers.balldontlie_client.requests") as mock_req:
            mock_req.get.return_value = _make_response(payload)
            result = mod.get_game(1001)
        assert isinstance(result, dict)
        assert result["home_team_score"] == 110

    def test_get_games_formats_params_with_array_notation(self):
        import engine.scrapers.balldontlie_client as mod
        with patch("engine.scrapers.balldontlie_client.requests") as mock_req:
            mock_req.get.return_value = _make_response({"data": []})
            mod.get_games(date="2024-01-15", season=2024, team_id=2)
        params = mock_req.get.call_args[1]["params"]
        assert params.get("dates[]") == "2024-01-15"
        assert params.get("seasons[]") == 2024
        assert params.get("team_ids[]") == 2

    def test_get_games_returns_empty_on_failure(self):
        import engine.scrapers.balldontlie_client as mod
        with patch("engine.scrapers.balldontlie_client.requests") as mock_req:
            mock_req.get.side_effect = Exception("error")
            assert mod.get_games() == []


# ---------------------------------------------------------------------------
# Stats
# ---------------------------------------------------------------------------

class TestStats:
    def setup_method(self):
        import engine.scrapers.balldontlie_client as mod
        mod._CACHE.clear()

    def test_get_stats_returns_list(self):
        import engine.scrapers.balldontlie_client as mod
        with patch("engine.scrapers.balldontlie_client.requests") as mock_req:
            mock_req.get.return_value = _make_response({"data": [{"pts": 30}]})
            result = mod.get_stats(player_id=237)
        assert isinstance(result, list)

    def test_get_advanced_stats_returns_list(self):
        import engine.scrapers.balldontlie_client as mod
        with patch("engine.scrapers.balldontlie_client.requests") as mock_req:
            mock_req.get.return_value = _make_response({"data": [{"pie": 0.18}]})
            result = mod.get_advanced_stats(player_id=237)
        assert isinstance(result, list)

    def test_get_season_averages_returns_list(self):
        import engine.scrapers.balldontlie_client as mod
        payload = {"data": [{"pts": 27.1, "reb": 7.4}]}
        with patch("engine.scrapers.balldontlie_client.requests") as mock_req:
            mock_req.get.return_value = _make_response(payload)
            result = mod.get_season_averages(237, 2024)
        assert isinstance(result, list)
        assert result[0]["pts"] == 27.1

    def test_get_player_stats_legacy_alias(self):
        import engine.scrapers.balldontlie_client as mod
        with patch.object(mod, "get_season_averages", return_value=[{"pts": 30}]) as mock_sa:
            result = mod.get_player_stats(237, 2024)
        mock_sa.assert_called_once_with(237, 2024)
        assert result == [{"pts": 30}]


# ---------------------------------------------------------------------------
# Standings & Leaders
# ---------------------------------------------------------------------------

class TestStandingsLeaders:
    def setup_method(self):
        import engine.scrapers.balldontlie_client as mod
        mod._CACHE.clear()

    def test_get_standings_returns_list(self):
        import engine.scrapers.balldontlie_client as mod
        with patch("engine.scrapers.balldontlie_client.requests") as mock_req:
            mock_req.get.return_value = _make_response({"data": [{"wins": 50}]})
            result = mod.get_standings(season=2024)
        assert isinstance(result, list)

    def test_get_leaders_returns_list(self):
        import engine.scrapers.balldontlie_client as mod
        with patch("engine.scrapers.balldontlie_client.requests") as mock_req:
            mock_req.get.return_value = _make_response({"data": [{"player_id": 237}]})
            result = mod.get_leaders("pts", season=2024)
        assert isinstance(result, list)

    def test_get_leaders_passes_stat_type(self):
        import engine.scrapers.balldontlie_client as mod
        with patch("engine.scrapers.balldontlie_client.requests") as mock_req:
            mock_req.get.return_value = _make_response({"data": []})
            mod.get_leaders("reb")
        params = mock_req.get.call_args[1]["params"]
        assert params["stat_type"] == "reb"


# ---------------------------------------------------------------------------
# Injuries
# ---------------------------------------------------------------------------

class TestInjuries:
    def setup_method(self):
        import engine.scrapers.balldontlie_client as mod
        mod._CACHE.clear()

    def test_get_injuries_returns_list(self):
        import engine.scrapers.balldontlie_client as mod
        with patch("engine.scrapers.balldontlie_client.requests") as mock_req:
            mock_req.get.return_value = _make_response({"data": [{"status": "Out"}]})
            result = mod.get_injuries()
        assert isinstance(result, list)

    def test_get_injuries_uses_live_ttl(self):
        import engine.scrapers.balldontlie_client as mod
        # First call populates cache; second call should not hit network
        with patch("engine.scrapers.balldontlie_client.requests") as mock_req:
            mock_req.get.return_value = _make_response({"data": [{"status": "Out"}]})
            mod.get_injuries()
            mod.get_injuries()
        assert mock_req.get.call_count == 1


# ---------------------------------------------------------------------------
# Box Scores
# ---------------------------------------------------------------------------

class TestBoxScores:
    def setup_method(self):
        import engine.scrapers.balldontlie_client as mod
        mod._CACHE.clear()

    def test_get_box_scores_live_returns_list(self):
        import engine.scrapers.balldontlie_client as mod
        with patch("engine.scrapers.balldontlie_client.requests") as mock_req:
            mock_req.get.return_value = _make_response({"data": [{"game_id": 1}]})
            result = mod.get_box_scores_live()
        assert isinstance(result, list)

    def test_get_box_scores_returns_list(self):
        import engine.scrapers.balldontlie_client as mod
        with patch("engine.scrapers.balldontlie_client.requests") as mock_req:
            mock_req.get.return_value = _make_response({"data": [{"game_id": 2}]})
            result = mod.get_box_scores("2024-01-15")
        assert isinstance(result, list)


# ---------------------------------------------------------------------------
# Odds (v2)
# ---------------------------------------------------------------------------

class TestOdds:
    def setup_method(self):
        import engine.scrapers.balldontlie_client as mod
        mod._CACHE.clear()

    def test_get_odds_returns_list(self):
        import engine.scrapers.balldontlie_client as mod
        with patch("engine.scrapers.balldontlie_client.requests") as mock_req:
            mock_req.get.return_value = _make_response({"data": [{"moneyline": -110}]})
            result = mod.get_odds(date="2024-01-15")
        assert isinstance(result, list)

    def test_get_odds_uses_v1_path(self):
        import engine.scrapers.balldontlie_client as mod
        with patch("engine.scrapers.balldontlie_client.requests") as mock_req:
            mock_req.get.return_value = _make_response({"data": []})
            mod.get_odds(date="2024-01-15")
        url = mock_req.get.call_args[0][0]
        assert "/nba/v1/odds" in url

    def test_get_player_props_returns_list(self):
        import engine.scrapers.balldontlie_client as mod
        with patch("engine.scrapers.balldontlie_client.requests") as mock_req:
            mock_req.get.return_value = _make_response({"data": [{"line": 25.5}]})
            result = mod.get_player_props(game_id=1001, player_id=237, prop_type="pts")
        assert isinstance(result, list)

    def test_get_player_props_uses_v1_path(self):
        import engine.scrapers.balldontlie_client as mod
        with patch("engine.scrapers.balldontlie_client.requests") as mock_req:
            mock_req.get.return_value = _make_response({"data": []})
            mod.get_player_props(game_id=1001)
        url = mock_req.get.call_args[0][0]
        assert "/nba/v1/player_props" in url


# ---------------------------------------------------------------------------
# Lineups & Play-by-Play
# ---------------------------------------------------------------------------

class TestLineupsPlays:
    def setup_method(self):
        import engine.scrapers.balldontlie_client as mod
        mod._CACHE.clear()

    def test_get_lineups_returns_list(self):
        import engine.scrapers.balldontlie_client as mod
        with patch("engine.scrapers.balldontlie_client.requests") as mock_req:
            mock_req.get.return_value = _make_response({"data": [{"lineup": []}]})
            result = mod.get_lineups(game_id=1001)
        assert isinstance(result, list)

    def test_get_plays_returns_list(self):
        import engine.scrapers.balldontlie_client as mod
        with patch("engine.scrapers.balldontlie_client.requests") as mock_req:
            mock_req.get.return_value = _make_response({"data": [{"description": "shot"}]})
            result = mod.get_plays(game_id=1001)
        assert isinstance(result, list)

    def test_get_lineups_returns_empty_on_failure(self):
        import engine.scrapers.balldontlie_client as mod
        with patch("engine.scrapers.balldontlie_client.requests") as mock_req:
            mock_req.get.side_effect = Exception("error")
            assert mod.get_lineups(1001) == []

    def test_get_plays_returns_empty_on_failure(self):
        import engine.scrapers.balldontlie_client as mod
        with patch("engine.scrapers.balldontlie_client.requests") as mock_req:
            mock_req.get.side_effect = Exception("error")
            assert mod.get_plays() == []


# ---------------------------------------------------------------------------
# check_api_health
# ---------------------------------------------------------------------------

class TestCheckApiHealth:
    def test_health_ok(self, monkeypatch):
        monkeypatch.setenv("BALLDONTLIE_API_KEY", "test-key")
        import engine.scrapers.balldontlie_client as mod
        with patch("engine.scrapers.balldontlie_client.requests") as mock_req:
            mock_req.get.return_value = _make_response({"data": [{"id": 1}]})
            result = mod.check_api_health()
        assert result["ok"] is True
        assert result["has_api_key"] is True
        assert result["status_code"] == 200

    def test_health_no_api_key(self, monkeypatch):
        monkeypatch.delenv("BALLDONTLIE_API_KEY", raising=False)
        import engine.scrapers.balldontlie_client as mod
        with patch.object(mod, "_load_api_key", return_value=""):
            result = mod.check_api_health()
        assert result["ok"] is False
        assert result["has_api_key"] is False
        assert "not configured" in result["error"]

    def test_health_bad_key(self, monkeypatch):
        monkeypatch.setenv("BALLDONTLIE_API_KEY", "bad-key")
        import engine.scrapers.balldontlie_client as mod
        resp = _make_response({}, status_code=401)
        resp.raise_for_status.side_effect = None
        with patch("engine.scrapers.balldontlie_client.requests") as mock_req:
            mock_req.get.return_value = resp
            result = mod.check_api_health()
        assert result["ok"] is False
        assert result["status_code"] == 401
        assert "invalid" in result["error"].lower()

    def test_health_connection_error(self, monkeypatch):
        monkeypatch.setenv("BALLDONTLIE_API_KEY", "test-key")
        import engine.scrapers.balldontlie_client as mod
        import requests as real_requests
        with patch("engine.scrapers.balldontlie_client.requests") as mock_req:
            mock_req.get.side_effect = real_requests.exceptions.ConnectionError("fail")
            mock_req.exceptions = real_requests.exceptions
            result = mod.check_api_health()
        assert result["ok"] is False
        assert "connect" in result["error"].lower()

    def test_health_requests_unavailable(self):
        import engine.scrapers.balldontlie_client as mod
        original = mod._REQUESTS_AVAILABLE
        mod._REQUESTS_AVAILABLE = False
        try:
            result = mod.check_api_health()
            assert result["ok"] is False
            assert "not installed" in result["error"]
        finally:
            mod._REQUESTS_AVAILABLE = original
