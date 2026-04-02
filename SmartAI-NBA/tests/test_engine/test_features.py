"""tests/test_engine/test_features.py – Unit tests for engine/features modules."""
import pytest


class TestTeamMetrics:
    def test_calculate_possessions(self):
        from engine.features.team_metrics import calculate_possessions
        result = calculate_possessions(fga=80, fta=20, oreb=10, tov=12)
        expected = 80 + 0.44 * 20 - 10 + 12
        assert abs(result - expected) < 0.001

    def test_calculate_offensive_rating(self):
        from engine.features.team_metrics import calculate_offensive_rating
        result = calculate_offensive_rating(pts=110, poss=100)
        assert abs(result - 110.0) < 0.001

    def test_calculate_offensive_rating_zero_poss(self):
        from engine.features.team_metrics import calculate_offensive_rating
        assert calculate_offensive_rating(pts=110, poss=0) == 0.0

    def test_calculate_defensive_rating(self):
        from engine.features.team_metrics import calculate_defensive_rating
        result = calculate_defensive_rating(opp_pts=105, poss=100)
        assert abs(result - 105.0) < 0.001

    def test_calculate_net_rating(self):
        from engine.features.team_metrics import calculate_net_rating
        result = calculate_net_rating(ortg=115.0, drtg=108.0)
        assert abs(result - 7.0) < 0.001

    def test_calculate_pace(self):
        from engine.features.team_metrics import calculate_pace
        result = calculate_pace(poss=96, minutes=240)
        assert abs(result - 48 * (96 / 240)) < 0.001

    def test_calculate_pace_zero_minutes(self):
        from engine.features.team_metrics import calculate_pace
        assert calculate_pace(poss=96, minutes=0) == 0.0


class TestPlayerMetrics:
    def test_calculate_true_shooting(self):
        from engine.features.player_metrics import calculate_true_shooting
        result = calculate_true_shooting(pts=25, fga=15, fta=5)
        denom = 2 * (15 + 0.44 * 5)
        assert abs(result - 25 / denom) < 0.001

    def test_calculate_true_shooting_zero_attempts(self):
        from engine.features.player_metrics import calculate_true_shooting
        assert calculate_true_shooting(pts=0, fga=0, fta=0) == 0.0

    def test_calculate_usage_rate(self):
        from engine.features.player_metrics import calculate_usage_rate
        result = calculate_usage_rate(
            fga=15, fta=5, tov=2,
            team_fga=80, team_fta=20, team_tov=12,
            mp=30, team_mp=240,
        )
        assert 0 < result < 50  # reasonable USG% range

    def test_calculate_per(self):
        from engine.features.player_metrics import calculate_per
        stats = {"pts": 25, "reb": 8, "ast": 7, "stl": 1.5, "blk": 0.5,
                 "tov": 3, "fga": 18, "fgm": 10, "fta": 6, "ftm": 5, "mp": 35}
        result = calculate_per(stats)
        assert isinstance(result, float)
        assert result > 0

    def test_calculate_assist_percentage(self):
        from engine.features.player_metrics import calculate_assist_percentage
        result = calculate_assist_percentage(ast=8, mp=30, team_mp=240, team_fgm=40, fgm=8)
        assert 0 <= result <= 100

    def test_calculate_rebound_percentage(self):
        from engine.features.player_metrics import calculate_rebound_percentage
        result = calculate_rebound_percentage(reb=9, mp=30, team_mp=240, team_reb=42, opp_reb=38)
        assert 0 <= result <= 100


class TestFeatureEngineering:
    def test_calculate_rolling_averages(self):
        from engine.features.feature_engineering import calculate_rolling_averages
        logs = [{"pts": i * 2 + 10} for i in range(15)]
        result = calculate_rolling_averages(logs, windows=[3, 5])
        assert "pts_rolling_3" in result
        assert "pts_rolling_5" in result

    def test_calculate_rest_days(self):
        from engine.features.feature_engineering import calculate_rest_days
        schedule = [{"date": "2025-01-10"}, {"date": "2025-01-12"}]
        days = calculate_rest_days(schedule, "2025-01-14")
        assert days == 2

    def test_is_back_to_back(self):
        from engine.features.feature_engineering import is_back_to_back
        schedule = [{"date": "2025-01-13"}]
        assert is_back_to_back(schedule, "2025-01-14") is True
        assert is_back_to_back(schedule, "2025-01-16") is False

    def test_calculate_travel_distance(self):
        from engine.features.feature_engineering import calculate_travel_distance
        dist = calculate_travel_distance("Boston", "Los Angeles")
        # Boston to LA is roughly 2600 miles
        assert dist > 2000

    def test_calculate_days_rest_factor(self):
        from engine.features.feature_engineering import calculate_days_rest_factor
        assert calculate_days_rest_factor(0) == 0.97
        assert calculate_days_rest_factor(1) == 1.00
        assert calculate_days_rest_factor(2) == 1.02
        assert calculate_days_rest_factor(5) == 1.03

    def test_build_feature_matrix(self):
        from engine.features.feature_engineering import build_feature_matrix
        result = build_feature_matrix(
            {"pts_avg": 20.0},
            {"pace": 100.0},
            {"pace": 98.0},
            {"rest_days": 2, "is_home": True, "team_pace": 100.0, "opponent_pace": 98.0},
        )
        assert "rest_days" in result
        assert "is_home" in result
        assert result["rest_factor"] == 1.02

    def test_build_feature_matrix_includes_travel_with_team_abbrevs(self):
        """build_feature_matrix must use utils.geo when team abbreviations are given."""
        from engine.features.feature_engineering import build_feature_matrix
        result = build_feature_matrix(
            {},
            {},
            {},
            {"prev_team": "BOS", "current_team": "LAL"},
        )
        assert "travel_fatigue" in result
        assert "travel_distance_miles" in result
        assert result["travel_distance_miles"] > 2000  # Boston → LA

    def test_build_feature_matrix_includes_travel_with_city_names(self):
        """build_feature_matrix falls back to city-name travel when no team abbrevs."""
        from engine.features.feature_engineering import build_feature_matrix
        result = build_feature_matrix(
            {},
            {},
            {},
            {"prev_city": "Boston", "current_city": "Los Angeles"},
        )
        assert "travel_fatigue" in result
        assert "travel_distance_miles" in result

    def test_build_feature_matrix_uses_constants_defaults(self):
        """Default pace/DRTG must come from utils.constants."""
        from engine.features.feature_engineering import build_feature_matrix
        from utils.constants import LEAGUE_AVG_PACE, LEAGUE_AVG_DRTG
        result = build_feature_matrix({}, {}, {}, {})
        # pace_adjustment with league-avg pace for both teams should be ~1.0
        assert abs(result["pace_adjustment"] - 1.0) < 0.01
        # defensive_matchup_factor with league-avg DRTG should be 1.0
        assert abs(result["defensive_matchup_factor"] - 1.0) < 0.01

    def test_constants_league_averages_imported(self):
        """LEAGUE_AVG_PACE and LEAGUE_AVG_DRTG must be imported from utils.constants."""
        import engine.features.feature_engineering as fe
        from utils.constants import LEAGUE_AVG_PACE, LEAGUE_AVG_DRTG
        assert fe.LEAGUE_AVG_PACE == LEAGUE_AVG_PACE
        assert fe.LEAGUE_AVG_DRTG == LEAGUE_AVG_DRTG
