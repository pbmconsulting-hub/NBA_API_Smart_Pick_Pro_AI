"""Tests for engine/joseph_bets.py — Layer 8 Joseph Bet Tracker Integration."""

import sys
import os
import unittest
from unittest.mock import patch, MagicMock

# Ensure repo root is on the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


# ============================================================
# Import Tests
# ============================================================

class TestJosephBetsImport(unittest.TestCase):
    """Verify all public functions are importable."""

    def test_joseph_auto_log_bets_importable(self):
        from engine.joseph_bets import joseph_auto_log_bets
        self.assertTrue(callable(joseph_auto_log_bets))

    def test_joseph_get_track_record_importable(self):
        from engine.joseph_bets import joseph_get_track_record
        self.assertTrue(callable(joseph_get_track_record))

    def test_joseph_get_accuracy_by_verdict_importable(self):
        from engine.joseph_bets import joseph_get_accuracy_by_verdict
        self.assertTrue(callable(joseph_get_accuracy_by_verdict))

    def test_joseph_get_override_accuracy_importable(self):
        from engine.joseph_bets import joseph_get_override_accuracy
        self.assertTrue(callable(joseph_get_override_accuracy))

    def test_is_joseph_bet_importable(self):
        from engine.joseph_bets import _is_joseph_bet
        self.assertTrue(callable(_is_joseph_bet))


# ============================================================
# _is_joseph_bet helper
# ============================================================

class TestIsJosephBet(unittest.TestCase):
    """Test the _is_joseph_bet helper."""

    def setUp(self):
        from engine.joseph_bets import _is_joseph_bet
        self.fn = _is_joseph_bet

    def test_platform_match(self):
        self.assertTrue(self.fn({"platform": "Joseph M. Smith", "notes": ""}))

    def test_notes_match(self):
        self.assertTrue(self.fn({"platform": "Other", "notes": "Pick by Joseph M. Smith today"}))

    def test_no_match(self):
        self.assertFalse(self.fn({"platform": "PrizePicks", "notes": "regular bet"}))

    def test_empty_bet(self):
        self.assertFalse(self.fn({}))

    def test_notes_and_platform(self):
        self.assertTrue(self.fn({"platform": "Joseph M. Smith", "notes": "Joseph M. Smith SMASH"}))


# ============================================================
# joseph_auto_log_bets
# ============================================================

class TestJosephAutoLogBets(unittest.TestCase):
    """Test joseph_auto_log_bets."""

    def setUp(self):
        from engine.joseph_bets import joseph_auto_log_bets
        self.fn = joseph_auto_log_bets

    def test_empty_input_returns_zero(self):
        count, msg = self.fn([])
        self.assertEqual(count, 0)
        self.assertIn("0", msg)

    def test_none_input_returns_zero(self):
        count, msg = self.fn(None)
        self.assertEqual(count, 0)

    @patch("engine.joseph_bets.load_all_bets", return_value=[])
    @patch("engine.joseph_bets.log_new_bet", return_value=(True, "OK"))
    def test_logs_smash_pick(self, mock_log, mock_load):
        results = [{
            "player_name": "LeBron James",
            "stat_type": "points",
            "line": 25.5,
            "direction": "OVER",
            "verdict": "SMASH",
            "confidence_pct": 85,
            "joseph_edge": 12.5,
            "nerd_stats": {"joseph_probability": 0.72},
            "player_team": "LAL",
            "one_liner": "King is rolling",
        }]
        count, msg = self.fn(results)
        self.assertEqual(count, 1)
        self.assertIn("1", msg)
        mock_log.assert_called_once()
        call_kwargs = mock_log.call_args[1]
        self.assertEqual(call_kwargs["player_name"], "LeBron James")
        self.assertEqual(call_kwargs["platform"], "Joseph M. Smith")
        self.assertEqual(call_kwargs["bet_type"], "joseph_pick")
        self.assertEqual(call_kwargs["auto_logged"], 1)

    @patch("engine.joseph_bets.load_all_bets", return_value=[])
    @patch("engine.joseph_bets.log_new_bet", return_value=(True, "OK"))
    def test_logs_lean_pick(self, mock_log, mock_load):
        results = [{
            "player_name": "Steph Curry",
            "stat_type": "threes",
            "line": 3.5,
            "direction": "OVER",
            "verdict": "LEAN",
            "confidence_pct": 60,
            "joseph_edge": 5.0,
            "nerd_stats": {},
            "player_team": "GSW",
            "one_liner": "Chef is cooking",
        }]
        count, msg = self.fn(results)
        self.assertEqual(count, 1)

    @patch("engine.joseph_bets.load_all_bets", return_value=[])
    @patch("engine.joseph_bets.log_new_bet", return_value=(True, "OK"))
    def test_skips_non_smash_lean(self, mock_log, mock_load):
        results = [{
            "player_name": "Test Player",
            "stat_type": "points",
            "line": 20.0,
            "direction": "OVER",
            "verdict": "SKIP",
        }]
        count, msg = self.fn(results)
        self.assertEqual(count, 0)
        mock_log.assert_not_called()

    @patch("engine.joseph_bets.load_all_bets", return_value=[])
    @patch("engine.joseph_bets.log_new_bet", return_value=(True, "OK"))
    def test_multiple_picks_logged(self, mock_log, mock_load):
        results = [
            {"player_name": "A", "stat_type": "points", "line": 20.0,
             "direction": "OVER", "verdict": "SMASH"},
            {"player_name": "B", "stat_type": "assists", "line": 7.5,
             "direction": "UNDER", "verdict": "LEAN"},
            {"player_name": "C", "stat_type": "rebounds", "line": 10.5,
             "direction": "OVER", "verdict": "FADE"},
        ]
        count, msg = self.fn(results)
        self.assertEqual(count, 2)
        self.assertEqual(mock_log.call_count, 2)

    @patch("engine.joseph_bets.load_all_bets")
    @patch("engine.joseph_bets.log_new_bet", return_value=(True, "OK"))
    def test_dedup_skips_existing(self, mock_log, mock_load):
        import datetime
        today = datetime.date.today().isoformat()
        mock_load.return_value = [{
            "bet_date": today,
            "player_name": "LeBron James",
            "stat_type": "points",
            "prop_line": 25.5,
        }]
        results = [{
            "player_name": "LeBron James",
            "stat_type": "points",
            "line": 25.5,
            "direction": "OVER",
            "verdict": "SMASH",
        }]
        count, msg = self.fn(results)
        self.assertEqual(count, 0)
        mock_log.assert_not_called()

    @patch("engine.joseph_bets.load_all_bets", side_effect=Exception("DB error"))
    def test_error_returns_zero_and_msg(self, mock_load):
        count, msg = self.fn([{"verdict": "SMASH", "player_name": "X",
                               "stat_type": "points", "line": 10,
                               "direction": "OVER"}])
        self.assertEqual(count, 0)
        self.assertIn("Error", msg)

    @patch("engine.joseph_bets.load_all_bets", return_value=[])
    @patch("engine.joseph_bets.log_new_bet", return_value=(False, "DB fail"))
    def test_failed_log_not_counted(self, mock_log, mock_load):
        results = [{
            "player_name": "A",
            "stat_type": "points",
            "line": 20.0,
            "direction": "OVER",
            "verdict": "SMASH",
        }]
        count, msg = self.fn(results)
        self.assertEqual(count, 0)

    @patch("engine.joseph_bets.load_all_bets", return_value=[])
    @patch("engine.joseph_bets.log_new_bet", return_value=(True, "OK"))
    def test_notes_contain_joseph(self, mock_log, mock_load):
        results = [{
            "player_name": "Test",
            "stat_type": "points",
            "line": 20.0,
            "direction": "OVER",
            "verdict": "SMASH",
            "one_liner": "He's hot",
        }]
        self.fn(results)
        notes = mock_log.call_args[1]["notes"]
        self.assertIn("Joseph M. Smith", notes)
        self.assertIn("SMASH", notes)
        self.assertIn("He's hot", notes)

    @patch("engine.joseph_bets.load_all_bets", return_value=[])
    @patch("engine.joseph_bets.log_new_bet", return_value=(True, "OK"))
    def test_missing_optional_fields_default(self, mock_log, mock_load):
        results = [{
            "player_name": "Test",
            "stat_type": "points",
            "line": 20.0,
            "direction": "OVER",
            "verdict": "SMASH",
        }]
        count, msg = self.fn(results)
        self.assertEqual(count, 1)
        kw = mock_log.call_args[1]
        self.assertEqual(kw["confidence_score"], 0.0)
        self.assertEqual(kw["probability_over"], 0.0)
        self.assertEqual(kw["edge_percentage"], 0.0)

    def test_return_type(self):
        count, msg = self.fn([])
        self.assertIsInstance(count, int)
        self.assertIsInstance(msg, str)


# ============================================================
# joseph_get_track_record
# ============================================================

class TestJosephGetTrackRecord(unittest.TestCase):
    """Test joseph_get_track_record."""

    def setUp(self):
        from engine.joseph_bets import joseph_get_track_record
        self.fn = joseph_get_track_record

    @patch("engine.joseph_bets.load_all_bets", return_value=[])
    def test_empty_bets_returns_zeroes(self, mock_load):
        result = self.fn()
        self.assertEqual(result["total"], 0)
        self.assertEqual(result["wins"], 0)
        self.assertEqual(result["losses"], 0)
        self.assertEqual(result["pending"], 0)
        self.assertEqual(result["win_rate"], 0.0)
        self.assertEqual(result["streak"], 0)

    @patch("engine.joseph_bets.load_all_bets")
    def test_basic_stats(self, mock_load):
        mock_load.return_value = [
            {"platform": "Joseph M. Smith", "result": "Win", "tier": "SMASH",
             "edge_percentage": 10, "confidence_score": 80,
             "player_name": "A", "stat_type": "pts", "notes": ""},
            {"platform": "Joseph M. Smith", "result": "Loss", "tier": "LEAN",
             "edge_percentage": 5, "confidence_score": 90,
             "player_name": "B", "stat_type": "reb", "notes": ""},
            {"platform": "Joseph M. Smith", "result": "", "tier": "SMASH",
             "edge_percentage": 0, "confidence_score": 0,
             "player_name": "C", "stat_type": "ast", "notes": ""},
        ]
        rec = self.fn()
        self.assertEqual(rec["total"], 3)
        self.assertEqual(rec["wins"], 1)
        self.assertEqual(rec["losses"], 1)
        self.assertEqual(rec["pending"], 1)
        self.assertEqual(rec["win_rate"], 50.0)

    @patch("engine.joseph_bets.load_all_bets")
    def test_streak_positive(self, mock_load):
        mock_load.return_value = [
            {"platform": "Joseph M. Smith", "result": "W", "tier": "SMASH",
             "edge_percentage": 0, "confidence_score": 0,
             "player_name": "A", "stat_type": "pts", "notes": ""},
            {"platform": "Joseph M. Smith", "result": "Win", "tier": "SMASH",
             "edge_percentage": 0, "confidence_score": 0,
             "player_name": "B", "stat_type": "pts", "notes": ""},
            {"platform": "Joseph M. Smith", "result": "Loss", "tier": "LEAN",
             "edge_percentage": 0, "confidence_score": 0,
             "player_name": "C", "stat_type": "pts", "notes": ""},
        ]
        rec = self.fn()
        self.assertEqual(rec["streak"], 2)

    @patch("engine.joseph_bets.load_all_bets")
    def test_streak_negative(self, mock_load):
        mock_load.return_value = [
            {"platform": "Joseph M. Smith", "result": "L", "tier": "LEAN",
             "edge_percentage": 0, "confidence_score": 0,
             "player_name": "A", "stat_type": "pts", "notes": ""},
            {"platform": "Joseph M. Smith", "result": "Loss", "tier": "LEAN",
             "edge_percentage": 0, "confidence_score": 0,
             "player_name": "B", "stat_type": "pts", "notes": ""},
            {"platform": "Joseph M. Smith", "result": "Win", "tier": "SMASH",
             "edge_percentage": 0, "confidence_score": 0,
             "player_name": "C", "stat_type": "pts", "notes": ""},
        ]
        rec = self.fn()
        self.assertEqual(rec["streak"], -2)

    @patch("engine.joseph_bets.load_all_bets")
    def test_accuracy_by_verdict(self, mock_load):
        mock_load.return_value = [
            {"platform": "Joseph M. Smith", "result": "Win", "tier": "SMASH",
             "edge_percentage": 10, "confidence_score": 80,
             "player_name": "A", "stat_type": "pts", "notes": ""},
            {"platform": "Joseph M. Smith", "result": "Win", "tier": "SMASH",
             "edge_percentage": 8, "confidence_score": 75,
             "player_name": "B", "stat_type": "pts", "notes": ""},
            {"platform": "Joseph M. Smith", "result": "Loss", "tier": "LEAN",
             "edge_percentage": 5, "confidence_score": 90,
             "player_name": "C", "stat_type": "reb", "notes": ""},
        ]
        rec = self.fn()
        self.assertEqual(rec["accuracy_by_verdict"]["SMASH"]["total"], 2)
        self.assertEqual(rec["accuracy_by_verdict"]["SMASH"]["wins"], 2)
        self.assertEqual(rec["accuracy_by_verdict"]["SMASH"]["win_rate"], 100.0)
        self.assertEqual(rec["accuracy_by_verdict"]["LEAN"]["total"], 1)
        self.assertEqual(rec["accuracy_by_verdict"]["LEAN"]["losses"], 1)

    @patch("engine.joseph_bets.load_all_bets")
    def test_best_pick(self, mock_load):
        mock_load.return_value = [
            {"platform": "Joseph M. Smith", "result": "Win", "tier": "SMASH",
             "edge_percentage": 15, "confidence_score": 80,
             "player_name": "LeBron", "stat_type": "points", "notes": ""},
            {"platform": "Joseph M. Smith", "result": "Win", "tier": "LEAN",
             "edge_percentage": 5, "confidence_score": 60,
             "player_name": "Curry", "stat_type": "threes", "notes": ""},
        ]
        rec = self.fn()
        self.assertIn("LeBron", rec["best_pick"])
        self.assertIn("15.0", rec["best_pick"])

    @patch("engine.joseph_bets.load_all_bets")
    def test_worst_pick(self, mock_load):
        mock_load.return_value = [
            {"platform": "Joseph M. Smith", "result": "Loss", "tier": "SMASH",
             "edge_percentage": 10, "confidence_score": 95,
             "player_name": "BadPick", "stat_type": "points", "notes": ""},
            {"platform": "Joseph M. Smith", "result": "Loss", "tier": "LEAN",
             "edge_percentage": 5, "confidence_score": 60,
             "player_name": "OkayPick", "stat_type": "rebounds", "notes": ""},
        ]
        rec = self.fn()
        self.assertIn("BadPick", rec["worst_pick"])
        self.assertIn("95", rec["worst_pick"])

    @patch("engine.joseph_bets.load_all_bets")
    def test_roi_estimate(self, mock_load):
        # 3 wins, 1 loss, 4 total => (3*0.8 - 1) / 4 * 100 = (2.4-1)/4*100 = 35.0
        mock_load.return_value = [
            {"platform": "Joseph M. Smith", "result": "Win", "tier": "SMASH",
             "edge_percentage": 0, "confidence_score": 0,
             "player_name": "A", "stat_type": "pts", "notes": ""},
            {"platform": "Joseph M. Smith", "result": "Win", "tier": "SMASH",
             "edge_percentage": 0, "confidence_score": 0,
             "player_name": "B", "stat_type": "pts", "notes": ""},
            {"platform": "Joseph M. Smith", "result": "Win", "tier": "LEAN",
             "edge_percentage": 0, "confidence_score": 0,
             "player_name": "C", "stat_type": "pts", "notes": ""},
            {"platform": "Joseph M. Smith", "result": "Loss", "tier": "LEAN",
             "edge_percentage": 0, "confidence_score": 0,
             "player_name": "D", "stat_type": "pts", "notes": ""},
        ]
        rec = self.fn()
        self.assertAlmostEqual(rec["roi_estimate"], 35.0, places=1)

    @patch("engine.joseph_bets.load_all_bets")
    def test_filters_non_joseph_bets(self, mock_load):
        mock_load.return_value = [
            {"platform": "Joseph M. Smith", "result": "Win", "tier": "SMASH",
             "edge_percentage": 10, "confidence_score": 80,
             "player_name": "A", "stat_type": "pts", "notes": ""},
            {"platform": "PrizePicks", "result": "Win", "tier": "Gold",
             "edge_percentage": 10, "confidence_score": 80,
             "player_name": "B", "stat_type": "pts", "notes": "regular bet"},
        ]
        rec = self.fn()
        self.assertEqual(rec["total"], 1)

    @patch("engine.joseph_bets.load_all_bets", side_effect=Exception("DB down"))
    def test_error_returns_zeroes(self, mock_load):
        rec = self.fn()
        self.assertEqual(rec["total"], 0)
        self.assertEqual(rec["wins"], 0)

    @patch("engine.joseph_bets.load_all_bets")
    def test_pending_recognized(self, mock_load):
        mock_load.return_value = [
            {"platform": "Joseph M. Smith", "result": None, "tier": "SMASH",
             "edge_percentage": 0, "confidence_score": 0,
             "player_name": "A", "stat_type": "pts", "notes": ""},
            {"platform": "Joseph M. Smith", "result": "Pending", "tier": "LEAN",
             "edge_percentage": 0, "confidence_score": 0,
             "player_name": "B", "stat_type": "pts", "notes": ""},
        ]
        rec = self.fn()
        self.assertEqual(rec["pending"], 2)
        self.assertEqual(rec["wins"], 0)
        self.assertEqual(rec["losses"], 0)

    @patch("engine.joseph_bets.load_all_bets")
    def test_notes_based_identification(self, mock_load):
        mock_load.return_value = [
            {"platform": "SmartAI", "result": "Win", "tier": "SMASH",
             "edge_percentage": 5, "confidence_score": 70,
             "player_name": "X", "stat_type": "pts",
             "notes": "🎙️ Joseph M. Smith — SMASH — hot take"},
        ]
        rec = self.fn()
        self.assertEqual(rec["total"], 1)
        self.assertEqual(rec["wins"], 1)

    def test_return_type(self):
        with patch("engine.joseph_bets.load_all_bets", return_value=[]):
            rec = self.fn()
        self.assertIsInstance(rec, dict)
        for key in ("total", "wins", "losses", "pending", "win_rate",
                     "roi_estimate", "accuracy_by_verdict", "streak",
                     "best_pick", "worst_pick"):
            self.assertIn(key, rec)


# ============================================================
# joseph_get_accuracy_by_verdict
# ============================================================

class TestJosephGetAccuracyByVerdict(unittest.TestCase):
    """Test joseph_get_accuracy_by_verdict."""

    def setUp(self):
        from engine.joseph_bets import joseph_get_accuracy_by_verdict
        self.fn = joseph_get_accuracy_by_verdict

    @patch("engine.joseph_bets.load_all_bets", return_value=[])
    def test_empty_returns_structure(self, mock_load):
        result = self.fn()
        self.assertIn("SMASH", result)
        self.assertIn("LEAN", result)
        self.assertEqual(result["SMASH"]["total"], 0)
        self.assertEqual(result["LEAN"]["total"], 0)

    @patch("engine.joseph_bets.load_all_bets")
    def test_counts_correct(self, mock_load):
        mock_load.return_value = [
            {"platform": "Joseph M. Smith", "result": "Win", "tier": "SMASH",
             "notes": ""},
            {"platform": "Joseph M. Smith", "result": "Win", "tier": "SMASH",
             "notes": ""},
            {"platform": "Joseph M. Smith", "result": "Loss", "tier": "SMASH",
             "notes": ""},
            {"platform": "Joseph M. Smith", "result": "Win", "tier": "LEAN",
             "notes": ""},
        ]
        result = self.fn()
        self.assertEqual(result["SMASH"]["total"], 3)
        self.assertEqual(result["SMASH"]["wins"], 2)
        self.assertAlmostEqual(result["SMASH"]["pct"], 66.7, places=1)
        self.assertEqual(result["LEAN"]["total"], 1)
        self.assertEqual(result["LEAN"]["wins"], 1)
        self.assertAlmostEqual(result["LEAN"]["pct"], 100.0, places=1)

    @patch("engine.joseph_bets.load_all_bets", side_effect=Exception("fail"))
    def test_error_returns_zeroes(self, mock_load):
        result = self.fn()
        self.assertEqual(result["SMASH"]["total"], 0)
        self.assertEqual(result["LEAN"]["total"], 0)

    @patch("engine.joseph_bets.load_all_bets")
    def test_pending_not_counted_as_win(self, mock_load):
        mock_load.return_value = [
            {"platform": "Joseph M. Smith", "result": "", "tier": "SMASH",
             "notes": ""},
            {"platform": "Joseph M. Smith", "result": "Win", "tier": "SMASH",
             "notes": ""},
        ]
        result = self.fn()
        self.assertEqual(result["SMASH"]["total"], 2)
        self.assertEqual(result["SMASH"]["wins"], 1)
        self.assertAlmostEqual(result["SMASH"]["pct"], 100.0, places=1)

    def test_return_keys(self):
        with patch("engine.joseph_bets.load_all_bets", return_value=[]):
            result = self.fn()
        for verdict in ("SMASH", "LEAN"):
            for key in ("total", "wins", "pct"):
                self.assertIn(key, result[verdict])


# ============================================================
# joseph_get_override_accuracy
# ============================================================

class TestJosephGetOverrideAccuracy(unittest.TestCase):
    """Test joseph_get_override_accuracy."""

    def setUp(self):
        from engine.joseph_bets import joseph_get_override_accuracy
        self.fn = joseph_get_override_accuracy

    @patch("engine.joseph_bets.load_all_bets", return_value=[])
    def test_no_overrides_msg(self, mock_load):
        result = self.fn()
        self.assertEqual(result["overrides_total"], 0)
        self.assertEqual(result["summary"], "No overrides recorded yet.")

    @patch("engine.joseph_bets.load_all_bets")
    def test_overrides_calculated(self, mock_load):
        mock_load.return_value = [
            {"platform": "Joseph M. Smith", "result": "Win",
             "notes": "OVERRIDE — Joseph disagrees", "tier": "SMASH"},
            {"platform": "Joseph M. Smith", "result": "Loss",
             "notes": "OVERRIDE — Joseph disagrees", "tier": "LEAN"},
            {"platform": "Joseph M. Smith", "result": "Win",
             "notes": "normal pick regular", "tier": "SMASH"},
        ]
        result = self.fn()
        self.assertEqual(result["overrides_total"], 2)
        self.assertEqual(result["overrides_correct"], 1)
        self.assertAlmostEqual(result["override_accuracy"], 50.0, places=1)
        self.assertIn("50.0%", result["summary"])

    @patch("engine.joseph_bets.load_all_bets")
    def test_case_insensitive_override_check(self, mock_load):
        mock_load.return_value = [
            {"platform": "Joseph M. Smith", "result": "Win",
             "notes": "override detected", "tier": "SMASH"},
        ]
        result = self.fn()
        self.assertEqual(result["overrides_total"], 1)

    @patch("engine.joseph_bets.load_all_bets", side_effect=Exception("fail"))
    def test_error_returns_zeroes(self, mock_load):
        result = self.fn()
        self.assertEqual(result["overrides_total"], 0)
        self.assertIn("Error", result["summary"])

    @patch("engine.joseph_bets.load_all_bets")
    def test_only_joseph_bets_counted(self, mock_load):
        mock_load.return_value = [
            {"platform": "Joseph M. Smith", "result": "Win",
             "notes": "OVERRIDE — Joseph disagrees", "tier": "SMASH"},
            {"platform": "PrizePicks", "result": "Win",
             "notes": "OVERRIDE by system", "tier": "Gold"},
        ]
        result = self.fn()
        self.assertEqual(result["overrides_total"], 1)

    def test_return_keys(self):
        with patch("engine.joseph_bets.load_all_bets", return_value=[]):
            result = self.fn()
        for key in ("overrides_total", "overrides_correct",
                     "override_accuracy", "summary"):
            self.assertIn(key, result)


if __name__ == "__main__":
    unittest.main()
