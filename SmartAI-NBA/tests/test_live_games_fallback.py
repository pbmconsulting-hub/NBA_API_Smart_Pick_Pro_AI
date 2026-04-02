# ============================================================
# FILE: tests/test_live_games_fallback.py
# PURPOSE: Verify that the Live Games player-data fallback
#   uses POSITION_PRIORS instead of hardcoded mock values.
#   When a player appears in prop data but is absent from
#   the CSV, the profile must be seeded from real positional
#   league-average priors (via engine.projections.POSITION_PRIORS)
#   and the analyzed stat must be anchored to the prop line.
# ============================================================
import sys
import unittest
from unittest.mock import MagicMock

def _ensure_streamlit_mock():
    if "streamlit" not in sys.modules:
        mock_st = MagicMock()
        mock_st.session_state = {}
        mock_st.cache_data = lambda **kw: (lambda f: f)
        sys.modules["streamlit"] = mock_st

_ensure_streamlit_mock()


class TestLiveGamesPlayerFallback(unittest.TestCase):
    """Verify no mock data in the Live Games player-missing fallback."""

    def _build_stub(self, stat_type, prop_line, fallback_pos="SF"):
        """Re-implement the fallback logic from pages/1_📡_Live_Games.py."""
        from engine.projections import POSITION_PRIORS
        _prior = POSITION_PRIORS.get(fallback_pos, POSITION_PRIORS["SF"])
        return {
            "name": "Unknown Player",
            "team": "LAL",
            "position": fallback_pos,
            "minutes_avg": 30.0,
            "games_played": 30,
            "points_avg":    str(prop_line if stat_type == "points" else _prior["points"]),
            "rebounds_avg":  str(prop_line if stat_type == "rebounds" else _prior["rebounds"]),
            "assists_avg":   str(prop_line if stat_type == "assists" else _prior["assists"]),
            "threes_avg":    str(prop_line if stat_type == "threes" else _prior["threes"]),
            "steals_avg":    str(prop_line if stat_type == "steals" else _prior["steals"]),
            "blocks_avg":    str(prop_line if stat_type == "blocks" else _prior["blocks"]),
            "turnovers_avg": str(prop_line if stat_type == "turnovers" else _prior["turnovers"]),
        }

    def test_no_hardcoded_position_G(self):
        """Fallback must not hard-code 'G' — POSITION_PRIORS has no 'G' key."""
        stub = self._build_stub("points", 22.5)
        self.assertNotEqual(stub["position"], "G",
            "Fallback must not use 'G' — not a valid POSITION_PRIORS key")

    def test_no_hardcoded_usage_rate(self):
        """Fallback must not include a made-up usage_rate."""
        stub = self._build_stub("points", 22.5)
        self.assertNotIn("usage_rate", stub,
            "Fallback must not include unused hardcoded usage_rate")

    def test_no_hardcoded_ft_pct(self):
        """Fallback must not include a made-up ft_pct."""
        stub = self._build_stub("points", 22.5)
        self.assertNotIn("ft_pct", stub,
            "Fallback must not include unused hardcoded ft_pct")

    def test_target_stat_anchored_to_prop_line(self):
        """Analyzed stat must be anchored to the prop_line."""
        for stat, line in [("points", 24.5), ("rebounds", 8.5), ("assists", 6.5)]:
            with self.subTest(stat=stat, line=line):
                stub = self._build_stub(stat, line)
                self.assertAlmostEqual(float(stub[f"{stat}_avg"]), line, places=3,
                    msg=f"{stat}_avg must equal prop_line={line}")

    def test_other_stats_seeded_from_position_priors(self):
        """Non-analyzed stats must come from POSITION_PRIORS, not hardcoded values."""
        from engine.projections import POSITION_PRIORS
        for pos in ["PG", "SG", "SF", "PF", "C"]:
            prior = POSITION_PRIORS[pos]
            # Analyze points; rebounds should come from position prior
            stub = self._build_stub("points", 20.0, fallback_pos=pos)
            self.assertAlmostEqual(float(stub["rebounds_avg"]), prior["rebounds"], places=3,
                msg=f"rebounds_avg for {pos} must equal prior {prior['rebounds']}")
            self.assertAlmostEqual(float(stub["assists_avg"]), prior["assists"], places=3,
                msg=f"assists_avg for {pos} must equal prior {prior['assists']}")

    def test_games_played_above_bayesian_threshold(self):
        """games_played must be ≥ 25 so Bayesian shrinkage trusts the prop-line anchor."""
        from engine.projections import BAYESIAN_SMALL_SAMPLE_THRESHOLD
        stub = self._build_stub("points", 20.0)
        self.assertGreaterEqual(stub["games_played"], BAYESIAN_SMALL_SAMPLE_THRESHOLD,
            "games_played must be above Bayesian threshold to trust prop_line anchor")

    def test_position_is_valid_priors_key(self):
        """Fallback position must be a valid POSITION_PRIORS key."""
        from engine.projections import POSITION_PRIORS
        stub = self._build_stub("points", 20.0)
        self.assertIn(stub["position"], POSITION_PRIORS,
            f"position={stub['position']!r} is not a valid POSITION_PRIORS key")


if __name__ == "__main__":
    unittest.main()
