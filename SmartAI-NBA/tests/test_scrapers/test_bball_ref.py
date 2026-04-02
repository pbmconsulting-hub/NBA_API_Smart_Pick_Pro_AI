"""tests/test_scrapers/test_bball_ref.py – Tests for Basketball Reference scraper."""
import pytest
from unittest.mock import patch, MagicMock


class TestPlayerUrlSlug:
    def test_slug_generation(self):
        from engine.scrapers.basketball_ref_scraper import _player_url_slug
        slug = _player_url_slug("LeBron James")
        assert "jamesle" in slug
        assert slug.startswith("j/")

    def test_slug_single_name(self):
        from engine.scrapers.basketball_ref_scraper import _player_url_slug
        slug = _player_url_slug("Madonna")
        assert slug == ""

    def test_slug_three_names(self):
        from engine.scrapers.basketball_ref_scraper import _player_url_slug
        slug = _player_url_slug("Karl Anthony Towns")
        assert "/" in slug


class TestFetchWithMock:
    def test_get_player_game_log_no_deps(self):
        """Should return empty list when deps unavailable."""
        import engine.scrapers.basketball_ref_scraper as mod
        original = mod._DEPS_AVAILABLE
        mod._DEPS_AVAILABLE = False
        try:
            result = mod.get_player_game_log("LeBron James", "2024")
            assert result == []
        finally:
            mod._DEPS_AVAILABLE = original

    def test_get_player_season_stats_no_deps(self):
        import engine.scrapers.basketball_ref_scraper as mod
        original = mod._DEPS_AVAILABLE
        mod._DEPS_AVAILABLE = False
        try:
            result = mod.get_player_season_stats("LeBron James", "2024")
            assert result == {}
        finally:
            mod._DEPS_AVAILABLE = original

    def test_get_team_standings_no_deps(self):
        import engine.scrapers.basketball_ref_scraper as mod
        original = mod._DEPS_AVAILABLE
        mod._DEPS_AVAILABLE = False
        try:
            result = mod.get_team_standings("2024")
            assert result == []
        finally:
            mod._DEPS_AVAILABLE = original

    def test_get_player_box_scores_for_date_no_deps(self):
        """Should return empty dict when deps unavailable."""
        import engine.scrapers.basketball_ref_scraper as mod
        original = mod._DEPS_AVAILABLE
        mod._DEPS_AVAILABLE = False
        try:
            result = mod.get_player_box_scores_for_date("2025-01-15")
            assert result == {}
        finally:
            mod._DEPS_AVAILABLE = original

    def test_get_player_box_scores_for_date_invalid_date(self):
        """Should return empty dict for an invalid date string."""
        from engine.scrapers.basketball_ref_scraper import get_player_box_scores_for_date
        result = get_player_box_scores_for_date("not-a-date")
        assert result == {}

    def test_get_player_box_scores_for_date_returns_dict(self):
        """Function must always return a dict."""
        import engine.scrapers.basketball_ref_scraper as mod
        # Force deps to False so no network call is made
        original = mod._DEPS_AVAILABLE
        mod._DEPS_AVAILABLE = False
        try:
            result = mod.get_player_box_scores_for_date("2025-01-15")
            assert isinstance(result, dict)
        finally:
            mod._DEPS_AVAILABLE = original
