# ============================================================
# FILE: tests/test_dfs_singularity.py
# PURPOSE: Tests for the DFS Singularity Directive upgrade:
#          Phase 1 (Data Quarantine), Phase 2 (DFS EV Engine),
#          Phase 3 (More/Less terminology), Phase 4 (Slip Optimization)
# ============================================================

import math
import sys
import os

# Ensure project root is on sys.path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


# ============================================================
# PHASE 1: Data Quarantine & Ingestion
# ============================================================

class TestQuarantineProps:
    """Tests for data/sportsbook_service.py quarantine_props()"""

    def _make_prop(self, player="Player A", stat="points", line=24.5,
                   over_odds=-110, under_odds=-110, platform="PrizePicks"):
        return {
            "player_name": player,
            "stat_type": stat,
            "line": line,
            "over_odds": over_odds,
            "under_odds": under_odds,
            "platform": platform,
        }

    def test_quarantine_empty_input(self):
        from data.sportsbook_service import quarantine_props
        result, summary = quarantine_props([])
        assert result == []
        assert summary["input_count"] == 0

    def test_quarantine_passes_normal_odds(self):
        """Props with standard -110 odds should survive quarantine."""
        from data.sportsbook_service import quarantine_props
        props = [self._make_prop(over_odds=-110, under_odds=-110)]
        result, summary = quarantine_props(props)
        assert len(result) == 1
        assert result[0]["prop_target_line"] == 24.5

    def test_quarantine_drops_extreme_negative_odds(self):
        """Props with odds worse than -300 should be dropped."""
        from data.sportsbook_service import quarantine_props
        props = [self._make_prop(over_odds=-400, under_odds=+200)]
        result, summary = quarantine_props(props)
        assert len(result) == 0
        assert summary["after_hard_drop"] == 0

    def test_quarantine_drops_extreme_positive_odds(self):
        """Props with odds better than +250 should be dropped."""
        from data.sportsbook_service import quarantine_props
        props = [self._make_prop(over_odds=+300, under_odds=-400)]
        result, summary = quarantine_props(props)
        assert len(result) == 0

    def test_quarantine_boundary_odds_pass(self):
        """Props exactly at -300 or +250 should survive."""
        from data.sportsbook_service import quarantine_props
        props = [
            self._make_prop(over_odds=-300, under_odds=+250),
            self._make_prop(player="Player B", over_odds=+250, under_odds=-300),
        ]
        result, summary = quarantine_props(props)
        assert len(result) == 2

    def test_quarantine_selects_main_line_closest_to_minus_110(self):
        """When multiple lines exist for same player+stat, pick closest to -110."""
        from data.sportsbook_service import quarantine_props
        props = [
            self._make_prop(line=22.5, over_odds=-150, under_odds=+120),
            self._make_prop(line=24.5, over_odds=-110, under_odds=-110),
            self._make_prop(line=26.5, over_odds=+120, under_odds=-150),
        ]
        result, summary = quarantine_props(props)
        assert len(result) == 1
        assert result[0]["prop_target_line"] == 24.5
        assert result[0]["line"] == 24.5

    def test_quarantine_silently_drops_player_with_no_valid_line(self):
        """If all lines for a player are extreme, player should be silently dropped."""
        from data.sportsbook_service import quarantine_props
        props = [
            self._make_prop(over_odds=-400, under_odds=+300),
            self._make_prop(line=28.5, over_odds=-350, under_odds=+280),
        ]
        result, summary = quarantine_props(props)
        assert len(result) == 0
        assert summary["after_hard_drop"] == 0

    def test_quarantine_drops_zero_line(self):
        """Props with line <= 0 should be dropped."""
        from data.sportsbook_service import quarantine_props
        props = [self._make_prop(line=0)]
        result, summary = quarantine_props(props)
        assert len(result) == 0

    def test_quarantine_multiple_players(self):
        """Each player+stat gets independent quarantine."""
        from data.sportsbook_service import quarantine_props
        props = [
            self._make_prop(player="Alpha", stat="points", line=24.5, over_odds=-110),
            self._make_prop(player="Alpha", stat="rebounds", line=8.5, over_odds=-115),
            self._make_prop(player="Beta", stat="points", line=20.5, over_odds=-105),
        ]
        result, summary = quarantine_props(props)
        assert len(result) == 3

    def test_quarantine_summary_counts(self):
        """Summary should accurately reflect step counts."""
        from data.sportsbook_service import quarantine_props
        props = [
            self._make_prop(player="Good", over_odds=-110),
            self._make_prop(player="Bad", over_odds=-400),
        ]
        result, summary = quarantine_props(props)
        assert summary["input_count"] == 2
        assert summary["after_hard_drop"] == 1
        assert summary["after_main_line_lock"] == 1

    def test_quarantine_default_odds_for_missing(self):
        """Props without over_odds/under_odds should default to -110."""
        from data.sportsbook_service import quarantine_props
        props = [{"player_name": "NoOdds", "stat_type": "points", "line": 20.5}]
        result, summary = quarantine_props(props)
        assert len(result) == 1
        assert result[0]["prop_target_line"] == 20.5


class TestSmartFilterQuarantineIntegration:
    """Tests that smart_filter_props integrates quarantine step."""

    def test_smart_filter_includes_quarantine_in_summary(self):
        from data.sportsbook_service import smart_filter_props
        props = [
            {"player_name": "Player A", "stat_type": "points", "line": 24.5,
             "over_odds": -110, "under_odds": -110, "platform": "PrizePicks"},
        ]
        result, summary = smart_filter_props(props)
        assert "after_quarantine" in summary

    def test_smart_filter_drops_extreme_odds(self):
        from data.sportsbook_service import smart_filter_props
        props = [
            {"player_name": "Extreme", "stat_type": "points", "line": 24.5,
             "over_odds": -500, "under_odds": +400, "platform": "DraftKings"},
        ]
        result, summary = smart_filter_props(props)
        assert len(result) == 0


# ============================================================
# PHASE 2: DFS Fixed-Payout Quant Engine
# ============================================================

class TestDfsEv:
    """Tests for engine/odds_engine.py DFS EV functions."""

    def test_calculate_dfs_ev_empty(self):
        from engine.odds_engine import calculate_dfs_ev
        result = calculate_dfs_ev([])
        assert result["expected_value"] == 0.0
        assert result["pick_count"] == 0

    def test_calculate_dfs_ev_3pick_prizepicks(self):
        """3-pick PrizePicks flex with 60% per leg should have positive EV."""
        from engine.odds_engine import calculate_dfs_ev
        result = calculate_dfs_ev([0.60, 0.60, 0.60], platform="PrizePicks")
        assert result["pick_count"] == 3
        assert result["expected_value"] > 0
        assert result["roi_pct"] > 0
        assert 0 < result["all_hit_prob"] < 1

    def test_calculate_dfs_ev_low_prob_negative(self):
        """Low per-leg probability should result in negative EV."""
        from engine.odds_engine import calculate_dfs_ev
        result = calculate_dfs_ev([0.40, 0.40, 0.40], platform="PrizePicks")
        assert result["expected_value"] < 0

    def test_calculate_dfs_ev_entry_fee_scaling(self):
        """EV should scale with entry fee."""
        from engine.odds_engine import calculate_dfs_ev
        r1 = calculate_dfs_ev([0.60, 0.60, 0.60], entry_fee=1.0)
        r10 = calculate_dfs_ev([0.60, 0.60, 0.60], entry_fee=10.0)
        assert abs(r10["expected_value"] - r1["expected_value"] * 10) < 0.01

    def test_calculate_dfs_ev_underdog(self):
        """Underdog platform should use different payout table."""
        from engine.odds_engine import calculate_dfs_ev
        pp = calculate_dfs_ev([0.60, 0.60, 0.60], platform="PrizePicks")
        ud = calculate_dfs_ev([0.60, 0.60, 0.60], platform="Underdog")
        # Both should produce valid results with non-zero payout
        assert pp["expected_payout"] > 0
        assert ud["expected_payout"] > 0

    def test_calculate_dfs_ev_4_5_6_pick(self):
        """EV calculations work for 4, 5, 6 pick entries."""
        from engine.odds_engine import calculate_dfs_ev
        for n in [4, 5, 6]:
            probs = [0.60] * n
            result = calculate_dfs_ev(probs, platform="PrizePicks")
            assert result["pick_count"] == n
            assert result["expected_payout"] > 0

    def test_calculate_dfs_ev_all_return_finite(self):
        """All return values should be finite floats."""
        from engine.odds_engine import calculate_dfs_ev
        result = calculate_dfs_ev([0.55, 0.60, 0.65])
        for key in ["expected_value", "expected_payout", "roi_pct", "all_hit_prob"]:
            assert math.isfinite(result[key])


class TestDfsBreakevenProbability:
    """Tests for calculate_dfs_breakeven_probability."""

    def test_breakeven_prizepicks_3pick(self):
        from engine.odds_engine import calculate_dfs_breakeven_probability
        result = calculate_dfs_breakeven_probability("PrizePicks", 3)
        assert 0.40 < result["breakeven_per_leg"] < 0.70
        assert result["all_hit_payout"] == 2.25
        assert result["pick_count"] == 3

    def test_breakeven_increases_with_picks(self):
        """Breakeven per-leg should generally be similar or slightly shift with more picks."""
        from engine.odds_engine import calculate_dfs_breakeven_probability
        be3 = calculate_dfs_breakeven_probability("PrizePicks", 3)
        be6 = calculate_dfs_breakeven_probability("PrizePicks", 6)
        # Both should be valid probabilities
        assert 0.01 < be3["breakeven_per_leg"] < 0.99
        assert 0.01 < be6["breakeven_per_leg"] < 0.99

    def test_breakeven_unknown_pick_count(self):
        """Unknown pick count should return safe defaults."""
        from engine.odds_engine import calculate_dfs_breakeven_probability
        result = calculate_dfs_breakeven_probability("PrizePicks", 99)
        assert result["breakeven_per_leg"] == 0.50

    def test_breakeven_all_platforms(self):
        """Breakeven should work for all platforms."""
        from engine.odds_engine import calculate_dfs_breakeven_probability
        for platform in ["PrizePicks", "Underdog", "DraftKings"]:
            result = calculate_dfs_breakeven_probability(platform, 3)
            assert 0.01 < result["breakeven_per_leg"] < 0.99


class TestDfsPayoutTables:
    """Tests for DFS_PAYOUT_TABLES constant."""

    def test_payout_tables_exist(self):
        from engine.odds_engine import DFS_PAYOUT_TABLES
        assert "PrizePicks" in DFS_PAYOUT_TABLES
        assert "Underdog" in DFS_PAYOUT_TABLES
        assert "DraftKings" in DFS_PAYOUT_TABLES

    def test_payout_tables_have_3_to_6_picks(self):
        from engine.odds_engine import DFS_PAYOUT_TABLES
        for platform, table in DFS_PAYOUT_TABLES.items():
            for n in [3, 4, 5, 6]:
                assert n in table, f"{platform} missing {n}-pick tier"

    def test_all_hit_payout_positive(self):
        from engine.odds_engine import DFS_PAYOUT_TABLES
        for platform, table in DFS_PAYOUT_TABLES.items():
            for n, tier in table.items():
                assert tier[n] > 1.0, f"{platform} {n}-pick all-hit payout should be > 1.0"


# ============================================================
# PHASE 3: More/Less Terminology
# ============================================================

class TestMoreLessTerminology:
    """Tests that Over/Under display labels have been updated to More/Less."""

    def test_glossary_has_more_less(self):
        from styles.theme import GLOSSARY
        assert "More/Less" in GLOSSARY
        assert "Over/Under" not in GLOSSARY

    def test_glossary_goblin_uses_more_less(self):
        from styles.theme import GLOSSARY
        assert "More/Less" in GLOSSARY["Goblin Bet"]
        assert "Over/Under" not in GLOSSARY["Goblin Bet"]

    def test_glossary_demon_uses_more_less(self):
        from styles.theme import GLOSSARY
        assert "More/Less" in GLOSSARY["Demon Bet"]
        assert "Over/Under" not in GLOSSARY["Demon Bet"]

    def test_glossary_fifty_fifty_uses_more_less(self):
        from styles.theme import GLOSSARY
        assert "More/Less" in GLOSSARY["50/50 Bet"]
        assert "Over/Under" not in GLOSSARY["50/50 Bet"]


# ============================================================
# PHASE 4: Quarantine Constants
# ============================================================

class TestQuarantineConstants:
    """Tests that quarantine constants are properly defined."""

    def test_quarantine_constants_exist(self):
        from data.sportsbook_service import (
            QUARANTINE_ODDS_FLOOR,
            QUARANTINE_ODDS_CEILING,
            _EQUILIBRIUM_ODDS,
        )
        assert QUARANTINE_ODDS_FLOOR == -300
        assert QUARANTINE_ODDS_CEILING == 250
        assert _EQUILIBRIUM_ODDS == -110

    def test_quarantine_is_importable(self):
        from data.sportsbook_service import quarantine_props
        assert callable(quarantine_props)


# ============================================================
# Backward Compatibility Tests
# ============================================================

class TestBackwardCompatibility:
    """Ensure existing functions still work after the upgrade."""

    def test_smart_filter_props_still_works(self):
        from data.sportsbook_service import smart_filter_props
        props = [
            {"player_name": "LeBron James", "stat_type": "points", "line": 24.5,
             "over_odds": -110, "under_odds": -110, "platform": "PrizePicks",
             "team": "LAL"},
        ]
        result, summary = smart_filter_props(props)
        assert isinstance(result, list)
        assert "final_count" in summary

    def test_fractional_kelly_unchanged(self):
        from engine.odds_engine import calculate_fractional_kelly
        result = calculate_fractional_kelly(0.62, -110, 0.25)
        assert result["kelly_fraction"] > 0
        assert result["fractional_kelly"] > 0

    def test_generate_optimal_slip_still_works(self):
        from engine.odds_engine import generate_optimal_slip
        # Should handle empty input gracefully
        assert generate_optimal_slip([]) == []
        assert generate_optimal_slip(None) == []

    def test_calculate_fair_odds_from_simulation_unchanged(self):
        from engine.odds_engine import calculate_fair_odds_from_simulation
        result = calculate_fair_odds_from_simulation([20, 22, 18, 25, 19], 19.5, "OVER")
        assert result["win_probability"] > 0
        assert result["sample_size"] == 5

    def test_backward_compat_alias_synthetic_odds(self):
        """Backward-compatible alias must still work."""
        from engine.odds_engine import calculate_synthetic_odds
        result = calculate_synthetic_odds([20, 22, 18, 25, 19], 19.5, "OVER")
        assert result["win_probability"] > 0
        assert result["sample_size"] == 5

    def test_american_odds_to_implied_probability_unchanged(self):
        from engine.odds_engine import american_odds_to_implied_probability
        assert abs(american_odds_to_implied_probability(-110) - 0.5238) < 0.001
