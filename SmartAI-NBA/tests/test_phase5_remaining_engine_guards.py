# ============================================================
# FILE: tests/test_phase5_remaining_engine_guards.py
# PURPOSE: Phase 5 — Remaining Engine Files NaN/inf Guards
#   5A: engine/correlation.py _safe_float + return dict guards
#   5B: engine/ensemble.py _safe_float + return dict guards
#   5C: engine/game_script.py _safe_float + return dict guards
#   5D: engine/minutes_model.py _safe_float + return dict guards
#   5E: engine/player_intelligence.py _safe_float + return dict guards
#   5F: engine/market_movement.py _safe_float + return dict guards
#   5G: engine/matchup_history.py _safe_float + return dict guards
#   5H: engine/platform_line_compare.py _safe_float + return dict guards
# ============================================================
import math
import sys
import unittest
from unittest.mock import MagicMock


def _ensure_streamlit_mock():
    """Inject a lightweight streamlit mock if not installed."""
    if "streamlit" not in sys.modules:
        mock_st = MagicMock()
        mock_st.session_state = {}
        mock_st.cache_data = lambda **kw: (lambda f: f)
        sys.modules["streamlit"] = mock_st


# ============================================================
# 5A: engine/correlation.py
# ============================================================

class TestCorrelationSafeFloat(unittest.TestCase):
    """Verify _safe_float helper exists and behaves correctly."""

    def setUp(self):
        from engine.correlation import _safe_float
        self.sf = _safe_float

    def test_finite_passes(self):
        self.assertEqual(self.sf(42.0), 42.0)

    def test_nan_returns_fallback(self):
        self.assertEqual(self.sf(float("nan"), 0.0), 0.0)

    def test_inf_returns_fallback(self):
        self.assertEqual(self.sf(float("inf"), 1.0), 1.0)

    def test_none_returns_fallback(self):
        self.assertEqual(self.sf(None, 5.0), 5.0)

    def test_string_returns_fallback(self):
        self.assertEqual(self.sf("abc", 0.0), 0.0)


class TestCorrelationSummaryGuards(unittest.TestCase):
    """Verify get_correlation_summary return dict has finite avg_correlation."""

    def test_summary_avg_finite(self):
        from engine.correlation import get_correlation_summary
        picks = [
            {"player_name": "A", "stat_type": "points", "team": "LAL", "game_id": "g1"},
            {"player_name": "B", "stat_type": "rebounds", "team": "BOS", "game_id": "g2"},
        ]
        matrix = [[1.0, 0.1], [0.1, 1.0]]
        result = get_correlation_summary(picks, matrix)
        self.assertTrue(math.isfinite(result["avg_correlation"]))


class TestCorrelationConfidenceGuards(unittest.TestCase):
    """Verify get_correlation_confidence return dict has finite scalars."""

    def test_confidence_finite(self):
        from engine.correlation import get_correlation_confidence
        picks = [
            {"player_name": "A", "stat_type": "points", "team": "LAL", "game_id": "g1"},
            {"player_name": "B", "stat_type": "rebounds", "team": "BOS", "game_id": "g2"},
        ]
        matrix = [[1.0, 0.05], [0.05, 1.0]]
        result = get_correlation_confidence(picks, matrix)
        self.assertTrue(math.isfinite(result["correlation_confidence"]))
        self.assertTrue(math.isfinite(result["diversification_score"]))


class TestCorrelationAdjustedKellyGuards(unittest.TestCase):
    """Verify correlation_adjusted_kelly return dict has finite scalars."""

    def test_kelly_finite(self):
        from engine.correlation import correlation_adjusted_kelly
        picks = [{"win_probability": 0.60, "odds_decimal": 1.91}]
        result = correlation_adjusted_kelly(picks, 1000.0, [[1.0]])
        self.assertTrue(math.isfinite(result["kelly_fraction"]))
        self.assertTrue(math.isfinite(result["recommended_bet"]))
        self.assertTrue(math.isfinite(result["correlation_discount"]))

    def test_empty_picks_safe(self):
        from engine.correlation import correlation_adjusted_kelly
        result = correlation_adjusted_kelly([], 1000.0, [])
        self.assertEqual(result["kelly_fraction"], 0.0)
        self.assertEqual(result["recommended_bet"], 0.0)


# ============================================================
# 5B: engine/ensemble.py
# ============================================================

class TestEnsembleSafeFloat(unittest.TestCase):
    """Verify _safe_float helper exists and behaves correctly."""

    def setUp(self):
        from engine.ensemble import _safe_float
        self.sf = _safe_float

    def test_finite_passes(self):
        self.assertEqual(self.sf(42.0), 42.0)

    def test_nan_returns_fallback(self):
        self.assertEqual(self.sf(float("nan"), 0.0), 0.0)

    def test_inf_returns_fallback(self):
        self.assertEqual(self.sf(float("inf"), 1.0), 1.0)


class TestEnsembleProjectionGuards(unittest.TestCase):
    """Verify get_ensemble_projection returns finite scalars."""

    def test_standard_projection_finite(self):
        from engine.ensemble import get_ensemble_projection
        player_data = {
            "name": "Test Player",
            "points_avg": 24.0,
            "points_std": 5.0,
            "games_played": 40,
        }
        game_context = {
            "opponent_team": "BOS",
            "is_home": True,
            "pace_factor": 1.0,
            "defense_factor": 1.0,
        }
        result = get_ensemble_projection(player_data, game_context)
        self.assertTrue(math.isfinite(result["ensemble_projection"]))
        self.assertTrue(math.isfinite(result["ensemble_std"]))
        self.assertTrue(math.isfinite(result["confidence_adjustment"]))


class TestModelDisagreementGuards(unittest.TestCase):
    """Verify calculate_model_disagreement returns finite scalars."""

    def test_disagreement_finite(self):
        from engine.ensemble import calculate_model_disagreement
        outputs = [
            {"projection": 24.0, "variance": 4.0, "weight_hint": 1.0, "model": "a"},
            {"projection": 26.0, "variance": 5.0, "weight_hint": 0.8, "model": "b"},
        ]
        result = calculate_model_disagreement(outputs)
        self.assertTrue(math.isfinite(result["disagreement_score"]))
        self.assertTrue(math.isfinite(result["max_divergence"]))
        self.assertTrue(math.isfinite(result["confidence_penalty"]))

    def test_empty_models_finite(self):
        from engine.ensemble import calculate_model_disagreement
        result = calculate_model_disagreement([])
        self.assertTrue(math.isfinite(result["disagreement_score"]))


# ============================================================
# 5C: engine/game_script.py
# ============================================================

class TestGameScriptSafeFloat(unittest.TestCase):
    """Verify _safe_float helper exists and behaves correctly."""

    def setUp(self):
        from engine.game_script import _safe_float
        self.sf = _safe_float

    def test_finite_passes(self):
        self.assertEqual(self.sf(42.0), 42.0)

    def test_nan_returns_fallback(self):
        self.assertEqual(self.sf(float("nan"), 0.0), 0.0)

    def test_inf_returns_fallback(self):
        self.assertEqual(self.sf(float("inf"), 1.0), 1.0)


class TestBlendSimulationGuards(unittest.TestCase):
    """Verify blend_with_flat_simulation returns finite scalars."""

    def test_blend_result_finite(self):
        from engine.game_script import blend_with_flat_simulation
        gs = {"mean": 24.5, "std": 5.0, "simulated_values": []}
        flat = {"mean": 25.0, "std": 4.5}
        result = blend_with_flat_simulation(gs, flat)
        self.assertTrue(math.isfinite(result["blended_mean"]))
        self.assertTrue(math.isfinite(result["blended_std"]))
        self.assertTrue(math.isfinite(result["game_script_mean"]))
        self.assertTrue(math.isfinite(result["flat_mean"]))
        self.assertTrue(math.isfinite(result["blend_weight"]))


# ============================================================
# 5D: engine/minutes_model.py
# ============================================================

class TestMinutesModelSafeFloat(unittest.TestCase):
    """Verify _safe_float helper exists and behaves correctly."""

    def setUp(self):
        from engine.minutes_model import _safe_float
        self.sf = _safe_float

    def test_finite_passes(self):
        self.assertEqual(self.sf(30.0), 30.0)

    def test_nan_returns_fallback(self):
        self.assertEqual(self.sf(float("nan"), 28.0), 28.0)

    def test_inf_returns_fallback(self):
        self.assertEqual(self.sf(float("inf"), 0.0), 0.0)


class TestProjectMinutesGuards(unittest.TestCase):
    """Verify project_player_minutes returns finite scalars."""

    def test_standard_projection_finite(self):
        from engine.minutes_model import project_player_minutes
        player_data = {
            "name": "Test Player",
            "minutes_avg": 34.0,
            "minutes_std": 4.0,
            "games_played": 40,
        }
        game_context = {
            "opponent_team": "BOS",
            "is_home": True,
            "vegas_spread": -3.5,
            "game_total": 220.0,
        }
        result = project_player_minutes(player_data, game_context)
        self.assertTrue(math.isfinite(result["projected_minutes"]))
        self.assertTrue(math.isfinite(result["minutes_std"]))
        self.assertTrue(math.isfinite(result["minutes_floor"]))
        self.assertTrue(math.isfinite(result["minutes_ceiling"]))
        self.assertTrue(math.isfinite(result["base_minutes"]))


# ============================================================
# 5E: engine/player_intelligence.py
# ============================================================

class TestPlayerIntelligenceSafeFloat(unittest.TestCase):
    """Verify _safe_float helper exists and behaves correctly."""

    def setUp(self):
        from engine.player_intelligence import _safe_float
        self.sf = _safe_float

    def test_finite_passes(self):
        self.assertEqual(self.sf(0.75), 0.75)

    def test_nan_returns_fallback(self):
        self.assertEqual(self.sf(float("nan"), 0.0), 0.0)

    def test_inf_returns_fallback(self):
        self.assertEqual(self.sf(float("inf"), 0.5), 0.5)


class TestRecentFormGuards(unittest.TestCase):
    """Verify get_recent_form_vs_line returns finite scalars."""

    def test_form_result_finite(self):
        from engine.player_intelligence import get_recent_form_vs_line
        logs = [
            {"date": "2025-01-01", "points": 28, "minutes": 34},
            {"date": "2025-01-02", "points": 22, "minutes": 32},
            {"date": "2025-01-03", "points": 30, "minutes": 36},
            {"date": "2025-01-04", "points": 18, "minutes": 30},
            {"date": "2025-01-05", "points": 25, "minutes": 33},
        ]
        result = get_recent_form_vs_line(logs, "points", 24.5, window=5)
        self.assertTrue(math.isfinite(result["hit_rate"]))
        self.assertTrue(math.isfinite(result["avg_vs_line"]))
        self.assertTrue(math.isfinite(result["avg_margin"]))


class TestAssessLineValueGuards(unittest.TestCase):
    """Verify assess_line_value returns finite scalars."""

    def test_line_value_finite(self):
        from engine.player_intelligence import assess_line_value
        result = assess_line_value(24.0, 22.5, "points")
        self.assertTrue(math.isfinite(result["edge_pct"]))
        self.assertTrue(math.isfinite(result["season_avg"]))
        self.assertTrue(math.isfinite(result["prop_line"]))
        self.assertTrue(math.isfinite(result["diff"]))


# ============================================================
# 5F: engine/market_movement.py
# ============================================================

class TestMarketMovementSafeFloat(unittest.TestCase):
    """Verify _safe_float helper exists and behaves correctly."""

    def setUp(self):
        from engine.market_movement import _safe_float
        self.sf = _safe_float

    def test_finite_passes(self):
        self.assertEqual(self.sf(2.5), 2.5)

    def test_nan_returns_fallback(self):
        self.assertEqual(self.sf(float("nan"), 0.0), 0.0)

    def test_inf_returns_fallback(self):
        self.assertEqual(self.sf(float("inf"), 0.0), 0.0)


class TestDetectLineMovementGuards(unittest.TestCase):
    """Verify detect_line_movement returns finite scalars."""

    def test_movement_result_finite(self):
        from engine.market_movement import detect_line_movement
        result = detect_line_movement(
            "LeBron James", "points", 24.5, 25.5, "OVER"
        )
        self.assertTrue(math.isfinite(result["movement_magnitude"]))
        self.assertTrue(math.isfinite(result["movement_pct"]))
        self.assertTrue(math.isfinite(result["confidence_adjustment"]))


# ============================================================
# 5G: engine/matchup_history.py
# ============================================================

class TestMatchupHistorySafeFloat(unittest.TestCase):
    """Verify _safe_float helper exists and behaves correctly."""

    def setUp(self):
        from engine.matchup_history import _safe_float
        self.sf = _safe_float

    def test_finite_passes(self):
        self.assertEqual(self.sf(50.0), 50.0)

    def test_nan_returns_fallback(self):
        self.assertEqual(self.sf(float("nan"), 50.0), 50.0)

    def test_inf_returns_fallback(self):
        self.assertEqual(self.sf(float("inf"), 0.0), 0.0)


class TestMatchupHistoryGuards(unittest.TestCase):
    """Verify get_player_vs_team_history returns finite scalars."""

    def test_no_logs_returns_finite(self):
        from engine.matchup_history import get_player_vs_team_history
        result = get_player_vs_team_history("LeBron James", "BOS", "points", [])
        self.assertTrue(math.isfinite(result.get("matchup_favorability_score", 50.0)))

    def test_with_logs_returns_finite(self):
        from engine.matchup_history import get_player_vs_team_history
        logs = [
            {"date": "2025-01-01", "opponent": "BOS", "points": 28, "minutes": 34},
            {"date": "2025-01-15", "opponent": "BOS", "points": 32, "minutes": 36},
            {"date": "2025-02-01", "opponent": "BOS", "points": 24, "minutes": 32},
        ]
        result = get_player_vs_team_history("LeBron James", "BOS", "points", logs, season_average=26.0)
        if result.get("avg_vs_team") is not None:
            self.assertTrue(math.isfinite(result["avg_vs_team"]))
        self.assertTrue(math.isfinite(result["matchup_favorability_score"]))


class TestMatchupForceSignalGuards(unittest.TestCase):
    """Verify get_matchup_force_signal returns finite strength."""

    def test_positive_adjustment_finite(self):
        from engine.matchup_history import get_matchup_force_signal
        result = get_matchup_force_signal(1.15)
        self.assertTrue(math.isfinite(result["strength"]))

    def test_negative_adjustment_finite(self):
        from engine.matchup_history import get_matchup_force_signal
        result = get_matchup_force_signal(0.85)
        self.assertTrue(math.isfinite(result["strength"]))

    def test_neutral_adjustment(self):
        from engine.matchup_history import get_matchup_force_signal
        result = get_matchup_force_signal(1.0)
        # Neutral adjustment may return None or a dict with strength=0
        if result is not None:
            self.assertTrue(math.isfinite(result["strength"]))
            self.assertEqual(result["strength"], 0.0)


# ============================================================
# 5H: engine/platform_line_compare.py
# ============================================================

class TestPlatformLineCompareSafeFloat(unittest.TestCase):
    """Verify _safe_float helper exists and behaves correctly."""

    def setUp(self):
        from engine.platform_line_compare import _safe_float
        self.sf = _safe_float

    def test_finite_passes(self):
        self.assertEqual(self.sf(24.5), 24.5)

    def test_nan_returns_fallback(self):
        self.assertEqual(self.sf(float("nan"), 0.0), 0.0)

    def test_inf_returns_fallback(self):
        self.assertEqual(self.sf(float("inf"), 0.0), 0.0)


class TestComparePlatformLinesGuards(unittest.TestCase):
    """Verify compare_platform_lines returns finite scalars."""

    def test_single_platform_finite(self):
        from engine.platform_line_compare import compare_platform_lines
        lines = {"PrizePicks": 24.5}
        result = compare_platform_lines("LeBron James", "points", "OVER", lines)
        if result.get("line_spread") is not None:
            self.assertTrue(math.isfinite(result["line_spread"]))
        if result.get("edge_bonus_pct") is not None:
            self.assertTrue(math.isfinite(result["edge_bonus_pct"]))

    def test_multi_platform_finite(self):
        from engine.platform_line_compare import compare_platform_lines
        lines = {"PrizePicks": 24.5, "Underdog": 25.0, "DraftKings": 24.0}
        result = compare_platform_lines("LeBron James", "points", "OVER", lines)
        if result.get("line_spread") is not None:
            self.assertTrue(math.isfinite(result["line_spread"]))
        if result.get("edge_bonus_pct") is not None:
            self.assertTrue(math.isfinite(result["edge_bonus_pct"]))


if __name__ == "__main__":
    unittest.main()
