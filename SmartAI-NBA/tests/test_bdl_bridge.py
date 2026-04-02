"""tests/test_bdl_bridge.py – Unit tests for the BallDontLie bridge module.

Tests the adapter layer that transforms BDL API responses into the
app's expected format. All BDL calls are mocked so no network access
is needed.
"""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest


# ---------------------------------------------------------------------------
# Module importability
# ---------------------------------------------------------------------------

class TestBdlBridgeImport:
    def test_import(self):
        """data.bdl_bridge must be importable."""
        import data.bdl_bridge  # noqa: F401

    def test_is_available_callable(self):
        from data.bdl_bridge import is_available
        assert callable(is_available)

    def test_get_api_status_callable(self):
        from data.bdl_bridge import get_api_status
        assert callable(get_api_status)

    def test_position_map(self):
        from data.bdl_bridge import _BDL_POSITION_MAP
        assert _BDL_POSITION_MAP["G"] == "PG"
        assert _BDL_POSITION_MAP["F"] == "SF"
        assert _BDL_POSITION_MAP["C"] == "C"
        assert _BDL_POSITION_MAP[""] == "SF"


# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------

class TestHelpers:
    def test_safe_float_normal(self):
        from data.bdl_bridge import _safe_float
        assert _safe_float(3.5) == 3.5
        assert _safe_float("2.1") == 2.1
        assert _safe_float(0) == 0.0

    def test_safe_float_none(self):
        from data.bdl_bridge import _safe_float
        assert _safe_float(None) == 0.0

    def test_safe_float_invalid(self):
        from data.bdl_bridge import _safe_float
        assert _safe_float("abc") == 0.0
        assert _safe_float("abc", 5.0) == 5.0

    def test_parse_min_float(self):
        from data.bdl_bridge import _parse_min
        assert _parse_min(34.0) == 34.0

    def test_parse_min_int(self):
        from data.bdl_bridge import _parse_min
        assert _parse_min(30) == 30.0

    def test_parse_min_string(self):
        from data.bdl_bridge import _parse_min
        assert _parse_min("34") == 34.0

    def test_parse_min_colon_format(self):
        from data.bdl_bridge import _parse_min
        result = _parse_min("34:30")
        assert abs(result - 34.5) < 0.01

    def test_parse_min_empty(self):
        from data.bdl_bridge import _parse_min
        assert _parse_min("") == 0.0
        assert _parse_min(None) == 0.0

    def test_stdev_safe_too_few(self):
        from data.bdl_bridge import _stdev_safe
        assert _stdev_safe([5.0]) == 0.0
        assert _stdev_safe([]) == 0.0

    def test_stdev_safe_normal(self):
        from data.bdl_bridge import _stdev_safe
        result = _stdev_safe([10.0, 20.0, 30.0])
        assert result > 0

    def test_current_season_int(self):
        from data.bdl_bridge import _current_season_int
        season = _current_season_int()
        assert isinstance(season, int)
        assert 2020 <= season <= 2030

    def test_today_et(self):
        from data.bdl_bridge import _today_et
        import datetime
        result = _today_et()
        assert isinstance(result, datetime.date)


# ---------------------------------------------------------------------------
# Team records / standings
# ---------------------------------------------------------------------------

@patch("data.bdl_bridge._BDL_AVAILABLE", True)
class TestFetchTeamRecords:
    @patch("data.bdl_bridge._bdl")
    def test_returns_formatted_records(self, mock_bdl):
        mock_bdl.get_standings.return_value = [
            {
                "team": {"abbreviation": "LAL", "full_name": "Los Angeles Lakers"},
                "wins": 45,
                "losses": 37,
                "streak": "W3",
                "home_record": "25-16",
                "road_record": "20-21",
                "conference_rank": 5,
            },
        ]
        from data.bdl_bridge import fetch_team_records
        result = fetch_team_records()
        assert "LAL" in result
        assert result["LAL"]["wins"] == 45
        assert result["LAL"]["losses"] == 37
        assert result["LAL"]["streak"] == "W3"
        assert result["LAL"]["conf_rank"] == 5

    @patch("data.bdl_bridge._bdl")
    def test_returns_empty_on_failure(self, mock_bdl):
        mock_bdl.get_standings.side_effect = Exception("API error")
        from data.bdl_bridge import fetch_team_records
        result = fetch_team_records()
        assert result == {}


# ---------------------------------------------------------------------------
# Today's games
# ---------------------------------------------------------------------------

@patch("data.bdl_bridge._BDL_AVAILABLE", True)
class TestFetchTodaysGames:
    @patch("data.bdl_bridge._bdl")
    @patch("data.bdl_bridge._today_et")
    def test_returns_formatted_games(self, mock_today, mock_bdl):
        import datetime
        mock_today.return_value = datetime.date(2026, 3, 6)
        mock_bdl.get_games.return_value = [
            {
                "id": 12345,
                "home_team": {"abbreviation": "LAL", "full_name": "Los Angeles Lakers"},
                "visitor_team": {"abbreviation": "GSW", "full_name": "Golden State Warriors"},
                "status": "7:30 pm ET",
                "time": "7:30 PM ET",
                "date": "2026-03-06",
            }
        ]
        from data.bdl_bridge import fetch_todays_games
        games = fetch_todays_games(team_records={})
        assert len(games) == 1
        g = games[0]
        assert g["home_team"] == "LAL"
        assert g["away_team"] == "GSW"
        assert g["game_id"] == "12345"
        assert "home_team_full" in g
        assert "away_team_full" in g
        assert "vegas_spread" in g
        assert "game_total" in g

    @patch("data.bdl_bridge._bdl")
    @patch("data.bdl_bridge._today_et")
    def test_returns_empty_on_failure(self, mock_today, mock_bdl):
        import datetime
        mock_today.return_value = datetime.date(2026, 3, 6)
        mock_bdl.get_games.side_effect = Exception("API error")
        from data.bdl_bridge import fetch_todays_games
        result = fetch_todays_games()
        assert result == []


# ---------------------------------------------------------------------------
# Player season averages
# ---------------------------------------------------------------------------

@patch("data.bdl_bridge._BDL_AVAILABLE", True)
class TestFetchPlayerSeasonAverages:
    @patch("data.bdl_bridge._bdl")
    def test_returns_formatted_averages(self, mock_bdl):
        mock_bdl.get_season_averages.return_value = [
            {
                "min": "34:30",
                "pts": 27.5,
                "reb": 7.2,
                "ast": 8.3,
                "fg3m": 2.1,
                "stl": 1.3,
                "blk": 0.5,
                "turnover": 3.1,
                "ft_pct": 0.750,
                "ftm": 5.5,
                "fta": 7.3,
                "fga": 20.1,
                "fgm": 10.2,
                "oreb": 1.1,
                "dreb": 6.1,
                "pf": 2.3,
                "games_played": 60,
            }
        ]
        from data.bdl_bridge import fetch_player_season_averages
        result = fetch_player_season_averages(237)
        assert result["points_avg"] == 27.5
        assert result["rebounds_avg"] == 7.2
        assert result["assists_avg"] == 8.3
        assert result["threes_avg"] == 2.1
        assert result["games_played"] == 60

    @patch("data.bdl_bridge._bdl")
    def test_returns_empty_on_no_data(self, mock_bdl):
        mock_bdl.get_season_averages.return_value = []
        from data.bdl_bridge import fetch_player_season_averages
        result = fetch_player_season_averages(237)
        assert result == {}


# ---------------------------------------------------------------------------
# Std dev computation
# ---------------------------------------------------------------------------

class TestComputeStdDevs:
    def test_computes_from_games(self):
        from data.bdl_bridge import compute_std_devs
        games = [
            {"pts": 20, "reb": 5, "ast": 3, "fg3m": 2, "stl": 1, "blk": 0,
             "turnover": 2, "ftm": 4, "fta": 5, "fga": 15, "fgm": 8,
             "oreb": 1, "dreb": 4, "pf": 3},
            {"pts": 30, "reb": 10, "ast": 8, "fg3m": 4, "stl": 3, "blk": 2,
             "turnover": 1, "ftm": 6, "fta": 7, "fga": 20, "fgm": 12,
             "oreb": 2, "dreb": 8, "pf": 1},
        ]
        result = compute_std_devs(games)
        assert "points_std" in result
        assert "rebounds_std" in result
        assert result["points_std"] > 0

    def test_returns_empty_for_single_game(self):
        from data.bdl_bridge import compute_std_devs
        result = compute_std_devs([{"pts": 20}])
        assert result == {}


# ---------------------------------------------------------------------------
# Player game log
# ---------------------------------------------------------------------------

@patch("data.bdl_bridge._BDL_AVAILABLE", True)
class TestFetchPlayerGameLog:
    @patch("data.bdl_bridge._bdl")
    def test_returns_formatted_log(self, mock_bdl):
        mock_bdl.get_stats.return_value = [
            {
                "game": {
                    "date": "2026-03-05",
                    "home_team": {"abbreviation": "LAL"},
                    "visitor_team": {"abbreviation": "GSW"},
                    "home_team_score": 110,
                    "visitor_team_score": 105,
                },
                "team": {"abbreviation": "LAL"},
                "min": 34,
                "pts": 28,
                "reb": 7,
                "ast": 5,
                "stl": 2,
                "blk": 1,
                "turnover": 3,
                "fg3m": 3,
                "ft_pct": 0.85,
            }
        ]
        from data.bdl_bridge import fetch_player_game_log
        result = fetch_player_game_log(237, last_n=10)
        assert len(result) == 1
        g = result[0]
        assert g["pts"] == 28.0
        assert g["game_date"] == "2026-03-05"
        assert g["win_loss"] == "W"
        assert "LAL vs. GSW" in g["matchup"]

    @patch("data.bdl_bridge._bdl")
    def test_returns_empty_on_failure(self, mock_bdl):
        mock_bdl.get_stats.side_effect = Exception("API error")
        from data.bdl_bridge import fetch_player_game_log
        result = fetch_player_game_log(237)
        assert result == []


# ---------------------------------------------------------------------------
# Player recent form
# ---------------------------------------------------------------------------

class TestFetchPlayerRecentForm:
    @patch("data.bdl_bridge.fetch_player_game_log")
    def test_returns_formatted_form(self, mock_log):
        mock_log.return_value = [
            {"game_date": f"2026-03-{5-i:02d}", "matchup": "LAL vs GSW",
             "win_loss": "W", "minutes": 34, "pts": 25 + i, "reb": 7,
             "ast": 5, "stl": 1, "blk": 0, "tov": 2, "fg3m": 3, "ft_pct": 0.8}
            for i in range(10)
        ]
        from data.bdl_bridge import fetch_player_recent_form
        result = fetch_player_recent_form(237, last_n_games=10)
        assert "trend" in result
        assert result["trend"] in ("hot", "cold", "neutral")
        assert "recent_pts_avg" in result
        assert "game_results" in result
        assert len(result["game_results"]) == 10
        assert "games" in result
        assert result["games_played"] == 10

    @patch("data.bdl_bridge.fetch_player_game_log")
    def test_returns_empty_on_no_games(self, mock_log):
        mock_log.return_value = []
        from data.bdl_bridge import fetch_player_recent_form
        result = fetch_player_recent_form(237)
        assert result == {}


# ---------------------------------------------------------------------------
# Team stats for CSV
# ---------------------------------------------------------------------------

@patch("data.bdl_bridge._BDL_AVAILABLE", True)
class TestFetchTeamStatsForCsv:
    @patch("data.bdl_bridge._bdl")
    def test_returns_formatted_teams(self, mock_bdl):
        mock_bdl.get_teams.return_value = [
            {"abbreviation": "LAL", "full_name": "Los Angeles Lakers", "division": "Pacific"},
        ]
        mock_bdl.get_standings.return_value = []
        mock_bdl.get_games.return_value = []
        from data.bdl_bridge import fetch_team_stats_for_csv
        result = fetch_team_stats_for_csv()
        assert len(result) == 1
        t = result[0]
        assert t["abbreviation"] == "LAL"
        assert "pace" in t
        assert "ortg" in t
        assert "drtg" in t


# ---------------------------------------------------------------------------
# Standings list
# ---------------------------------------------------------------------------

@patch("data.bdl_bridge._BDL_AVAILABLE", True)
class TestFetchStandingsList:
    @patch("data.bdl_bridge._bdl")
    def test_returns_formatted_standings(self, mock_bdl):
        mock_bdl.get_standings.return_value = [
            {
                "team": {"id": 1, "full_name": "Los Angeles Lakers", "abbreviation": "LAL"},
                "wins": 45,
                "losses": 37,
                "conference": "West",
                "conference_rank": 5,
                "home_record": "25-16",
                "road_record": "20-21",
                "streak": "W3",
            },
        ]
        from data.bdl_bridge import fetch_standings_list
        result = fetch_standings_list()
        assert len(result) == 1
        s = result[0]
        assert s["WINS"] == 45
        assert s["TeamAbbreviation"] == "LAL"
        assert s["PlayoffRank"] == 5


# ---------------------------------------------------------------------------
# Injuries
# ---------------------------------------------------------------------------

@patch("data.bdl_bridge._BDL_AVAILABLE", True)
class TestFetchInjuries:
    @patch("data.bdl_bridge._bdl")
    def test_returns_formatted_injuries(self, mock_bdl):
        mock_bdl.get_injuries.return_value = [
            {
                "player": {"id": 237, "first_name": "LeBron", "last_name": "James"},
                "team": {"abbreviation": "LAL"},
                "status": "Questionable",
                "description": "Ankle soreness",
                "date": "2026-03-05",
            },
        ]
        from data.bdl_bridge import fetch_injuries
        result = fetch_injuries()
        assert len(result) == 1
        inj = result[0]
        assert inj["player_name"] == "LeBron James"
        assert inj["status"] == "Questionable"


# ---------------------------------------------------------------------------
# Active players
# ---------------------------------------------------------------------------

@patch("data.bdl_bridge._BDL_AVAILABLE", True)
class TestFetchActivePlayers:
    @patch("data.bdl_bridge._bdl")
    def test_returns_formatted_players(self, mock_bdl):
        mock_bdl.get_active_players.return_value = [
            {
                "id": 237,
                "first_name": "LeBron",
                "last_name": "James",
                "team": {"abbreviation": "LAL"},
                "position": "F",
            },
        ]
        from data.bdl_bridge import fetch_active_players
        result = fetch_active_players()
        assert len(result) == 1
        p = result[0]
        assert p["name"] == "LeBron James"
        assert p["position"] == "SF"  # F maps to SF


# ---------------------------------------------------------------------------
# Compute averages from games
# ---------------------------------------------------------------------------

class TestComputeAveragesFromGames:
    def test_computes_averages(self):
        from data.bdl_bridge import compute_averages_from_games
        games = [
            {"pts": 20, "reb": 5, "ast": 3, "fg3m": 2, "stl": 1, "blk": 0,
             "turnover": 2, "ftm": 4, "fta": 5, "fga": 15, "fgm": 8,
             "oreb": 1, "dreb": 4, "pf": 3, "ft_pct": 0.80, "min": 30},
            {"pts": 30, "reb": 10, "ast": 8, "fg3m": 4, "stl": 3, "blk": 2,
             "turnover": 1, "ftm": 6, "fta": 7, "fga": 20, "fgm": 12,
             "oreb": 2, "dreb": 8, "pf": 1, "ft_pct": 0.90, "min": 36},
        ]
        result = compute_averages_from_games(games)
        assert result["points_avg"] == 25.0
        assert result["rebounds_avg"] == 7.5
        assert result["games_played"] == 2

    def test_empty_games(self):
        from data.bdl_bridge import compute_averages_from_games
        result = compute_averages_from_games([])
        assert result == {}


# ---------------------------------------------------------------------------
# Box scores & lineups
# ---------------------------------------------------------------------------

@patch("data.bdl_bridge._BDL_AVAILABLE", True)
class TestBoxScoresAndLineups:
    @patch("data.bdl_bridge._bdl")
    def test_fetch_box_scores_live(self, mock_bdl):
        mock_bdl.get_box_scores_live.return_value = [{"game_id": 1}]
        from data.bdl_bridge import fetch_box_scores_live
        result = fetch_box_scores_live()
        assert len(result) == 1

    @patch("data.bdl_bridge._bdl")
    def test_fetch_box_scores(self, mock_bdl):
        mock_bdl.get_box_scores.return_value = [{"game_id": 1}]
        from data.bdl_bridge import fetch_box_scores
        result = fetch_box_scores("2026-03-06")
        assert len(result) == 1

    @patch("data.bdl_bridge._bdl")
    def test_fetch_lineups(self, mock_bdl):
        mock_bdl.get_lineups.return_value = [{"player": "LeBron"}]
        from data.bdl_bridge import fetch_lineups
        result = fetch_lineups(12345)
        assert len(result) == 1

    @patch("data.bdl_bridge._bdl")
    def test_fetch_plays(self, mock_bdl):
        mock_bdl.get_plays.return_value = [{"play_type": "shot"}]
        from data.bdl_bridge import fetch_plays
        result = fetch_plays(12345)
        assert len(result) == 1


# ---------------------------------------------------------------------------
# Integration with nba_data_service
# ---------------------------------------------------------------------------

class TestNbaDataServiceBdlIntegration:
    """Verify nba_data_service routes to ETL database."""

    def test_nba_data_service_imports_etl(self):
        """nba_data_service should import etl_data_service successfully."""
        import data.nba_data_service as nds
        # The module should use _etl for DB access
        assert hasattr(nds, '_etl')

    def test_get_todays_games_callable(self):
        from data.nba_data_service import get_todays_games
        assert callable(get_todays_games)

    def test_get_standings_callable(self):
        from data.nba_data_service import get_standings
        assert callable(get_standings)

    def test_get_player_game_log_callable(self):
        from data.nba_data_service import get_player_game_log
        assert callable(get_player_game_log)

    def test_get_player_recent_form_callable(self):
        from data.nba_data_service import get_player_recent_form
        assert callable(get_player_recent_form)

    def test_get_league_leaders_callable(self):
        from data.nba_data_service import get_league_leaders
        assert callable(get_league_leaders)
