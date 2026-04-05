"""Tests for tracking/auto_resolver.py — name matching and result logic."""

import sys
from pathlib import Path

# Ensure SmartPicksProAI package root is importable
_REPO = Path(__file__).resolve().parent.parent
_PKG = _REPO / "SmartPicksProAI"
if str(_PKG) not in sys.path:
    sys.path.insert(0, str(_PKG))

from tracking.auto_resolver import _name_similarity, _find_player_stat  # noqa: E402


# ---------------------------------------------------------------------------
# Name similarity tests
# ---------------------------------------------------------------------------

class TestNameSimilarity:
    def test_exact_match(self):
        assert _name_similarity("LeBron James", "LeBron James") == 1.0

    def test_case_insensitive(self):
        assert _name_similarity("lebron james", "LeBron James") == 1.0

    def test_jr_suffix(self):
        """'Jaren Jackson' vs 'Jaren Jackson Jr.' should be above 0.85."""
        score = _name_similarity("Jaren Jackson", "Jaren Jackson Jr.")
        assert score >= 0.85, f"Score {score} is below 0.85 threshold"

    def test_accented_characters(self):
        """'Nikola Jokic' vs 'Nikola Jokić' should be above 0.85."""
        score = _name_similarity("Nikola Jokic", "Nikola Jokić")
        assert score >= 0.85, f"Score {score} is below 0.85 threshold"

    def test_completely_different_names(self):
        score = _name_similarity("LeBron James", "Stephen Curry")
        assert score < 0.85

    def test_leading_trailing_spaces(self):
        assert _name_similarity("  LeBron James  ", "LeBron James") == 1.0


# ---------------------------------------------------------------------------
# find_player_stat tests
# ---------------------------------------------------------------------------

_BOX_SCORES = [
    {"player_name": "LeBron James", "pts": 30.0, "reb": 8.0, "ast": 7.0,
     "stl": 1.0, "blk": 1.0, "tov": 3.0, "fg3m": 2.0, "ftm": 5.0},
    {"player_name": "Nikola Jokić", "pts": 26.0, "reb": 12.0, "ast": 9.0,
     "stl": 2.0, "blk": 0.0, "tov": 4.0, "fg3m": 1.0, "ftm": 3.0},
    {"player_name": "Jaren Jackson Jr.", "pts": 22.0, "reb": 6.0, "ast": 1.0,
     "stl": 0.0, "blk": 3.0, "tov": 2.0, "fg3m": 2.0, "ftm": 4.0},
]


class TestFindPlayerStat:
    def test_exact_name(self):
        assert _find_player_stat(_BOX_SCORES, "LeBron James", "points") == 30.0

    def test_fuzzy_jr(self):
        """Should match 'Jaren Jackson' to 'Jaren Jackson Jr.'"""
        assert _find_player_stat(_BOX_SCORES, "Jaren Jackson", "blocks") == 3.0

    def test_fuzzy_accent(self):
        """Should match 'Nikola Jokic' to 'Nikola Jokić'"""
        assert _find_player_stat(_BOX_SCORES, "Nikola Jokic", "rebounds") == 12.0

    def test_unknown_stat_type(self):
        assert _find_player_stat(_BOX_SCORES, "LeBron James", "dunks") is None

    def test_unknown_player(self):
        assert _find_player_stat(_BOX_SCORES, "Nonexistent Player", "points") is None


# ---------------------------------------------------------------------------
# Result logic tests (OVER/UNDER/push)
# ---------------------------------------------------------------------------

class TestResultLogic:
    """Test the OVER/UNDER/push logic from auto_resolve_pending_picks."""

    @staticmethod
    def _determine_result(actual: float, prop_line: float, direction: str) -> str:
        if actual == prop_line:
            return "push"
        elif direction == "OVER":
            return "win" if actual > prop_line else "loss"
        else:
            return "win" if actual < prop_line else "loss"

    def test_over_hit(self):
        assert self._determine_result(25.0, 20.5, "OVER") == "win"

    def test_over_miss(self):
        assert self._determine_result(18.0, 20.5, "OVER") == "loss"

    def test_under_hit(self):
        assert self._determine_result(18.0, 20.5, "UNDER") == "win"

    def test_under_miss(self):
        assert self._determine_result(25.0, 20.5, "UNDER") == "loss"

    def test_push(self):
        assert self._determine_result(20.5, 20.5, "OVER") == "push"
        assert self._determine_result(20.5, 20.5, "UNDER") == "push"
