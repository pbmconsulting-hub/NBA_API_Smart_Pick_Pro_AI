"""engine/predict/predictor.py – Load saved models and generate predictions."""
import os
import sqlite3
from utils.logger import get_logger

_logger = get_logger(__name__)

_SAVED_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "models", "saved")
_DB_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "backend", "smartpicks.db"
)


def _load_best_model(stat_type: str):
    """Load the best available saved model for a stat type.

    Args:
        stat_type: Target stat (e.g. "pts").

    Returns:
        Loaded model instance, or None if not found.
    """
    try:
        import joblib
        from tracking.model_performance import get_best_model

        best_name = get_best_model(stat_type)
        path = os.path.join(_SAVED_DIR, f"{best_name}_{stat_type}.joblib")
        if os.path.exists(path):
            model = joblib.load(path)
            _logger.debug("Loaded model %s for %s", best_name, stat_type)
            return model

        # Fallback: try any available model
        for fname in os.listdir(_SAVED_DIR) if os.path.isdir(_SAVED_DIR) else []:
            if fname.endswith(f"_{stat_type}.joblib"):
                model = joblib.load(os.path.join(_SAVED_DIR, fname))
                _logger.debug("Loaded fallback model %s", fname)
                return model
    except Exception as exc:
        _logger.debug("_load_best_model failed: %s", exc)
    return None


def _fetch_player_data(player_name: str) -> tuple:
    """Look up a player's recent game logs from the DB and compute rolling averages.

    Args:
        player_name: Player's full name.

    Returns:
        Tuple of (player_stats dict, game_logs list).
    """
    try:
        from engine.features.feature_engineering import calculate_rolling_averages

        if not os.path.exists(_DB_PATH):
            return {}, []
        conn = sqlite3.connect(_DB_PATH)
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        cur.execute("""
            SELECT pgl.*
            FROM Player_Game_Logs pgl
            JOIN Players p ON pgl.player_id = p.player_id
            LEFT JOIN Games g ON pgl.game_id = g.game_id
            WHERE p.full_name = ?
            ORDER BY g.game_date ASC
            LIMIT 50
        """, (player_name,))
        rows = [dict(r) for r in cur.fetchall()]
        conn.close()

        if not rows:
            return {}, []

        # Compute rolling averages over recent game logs
        rolling = calculate_rolling_averages(rows)

        # Also include the most recent game's raw stats as season-context
        latest = rows[-1]
        for k, v in latest.items():
            if isinstance(v, (int, float)):
                rolling.setdefault(k, float(v))

        return rolling, rows
    except Exception as exc:
        _logger.debug("_fetch_player_data failed for %s: %s", player_name, exc)
        return {}, []


def _fetch_team_data(player_name: str) -> tuple:
    """Look up team and opponent stats for a player from the DB.

    Args:
        player_name: Player's full name.

    Returns:
        Tuple of (team_data dict, opponent_team_abbrev str or None).
    """
    try:
        if not os.path.exists(_DB_PATH):
            return {}, None
        conn = sqlite3.connect(_DB_PATH)
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        cur.execute("""
            SELECT t.*
            FROM Players p
            JOIN Teams t ON p.team_id = t.team_id
            WHERE p.full_name = ?
            LIMIT 1
        """, (player_name,))
        row = cur.fetchone()
        conn.close()
        if row:
            return {k: row[k] for k in row.keys() if isinstance(row[k], (int, float))}, None
        return {}, None
    except Exception as exc:
        _logger.debug("_fetch_team_data failed for %s: %s", player_name, exc)
        return {}, None


def _fetch_opponent_data(opponent_abbrev: str) -> dict:
    """Look up opponent team stats from the DB.

    Args:
        opponent_abbrev: Opponent team abbreviation (e.g. "BOS").

    Returns:
        Dict of opponent team stats.
    """
    if not opponent_abbrev:
        return {}
    try:
        if not os.path.exists(_DB_PATH):
            return {}
        conn = sqlite3.connect(_DB_PATH)
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        cur.execute("""
            SELECT * FROM Teams WHERE abbreviation = ? LIMIT 1
        """, (opponent_abbrev,))
        row = cur.fetchone()
        conn.close()
        if row:
            return {k: row[k] for k in row.keys() if isinstance(row[k], (int, float))}
        return {}
    except Exception as exc:
        _logger.debug("_fetch_opponent_data failed for %s: %s", opponent_abbrev, exc)
        return {}


def predict_player_stat(
    player_name: str,
    stat_type: str,
    game_context: dict,
) -> dict:
    """Generate a prediction for a player stat.

    Args:
        player_name: Player's full name.
        stat_type: Stat to predict (e.g. "pts", "reb", "ast").
        game_context: Dict with game context (rest_days, is_home, opponent_drtg, etc.).

    Returns:
        Dict with ``player_name``, ``stat_type``, ``prediction``, ``confidence_interval``.
    """
    result = {
        "player_name": player_name,
        "stat_type": stat_type,
        "prediction": None,
        "confidence_interval": None,
        "source": "unavailable",
    }

    try:
        from engine.features.feature_engineering import build_feature_matrix
        import numpy as np

        # Look up actual player/team data from the database
        player_data, game_logs = _fetch_player_data(player_name)
        team_data, opp_abbrev = _fetch_team_data(player_name)
        # Allow game_context to supply opponent abbreviation
        opp_abbrev = game_context.get("opponent_abbrev", opp_abbrev)
        opponent_data = _fetch_opponent_data(opp_abbrev)

        # Enrich game_context with game_logs for streak/usage trend features
        enriched_ctx = dict(game_context)
        enriched_ctx.setdefault("game_logs", game_logs)

        # Enrich with matchup history if opponent is known
        if opp_abbrev and game_logs:
            try:
                from engine.matchup_history import get_player_vs_team_history
                # Map short stat types to matchup_history's stat type names
                stat_map = {
                    "pts": "points", "reb": "rebounds", "ast": "assists",
                    "stl": "steals", "blk": "blocks", "tov": "turnovers",
                    "fg3m": "threes", "ftm": "ftm", "fta": "fta",
                    "fgm": "fgm", "fga": "fga", "min": "minutes",
                    "oreb": "offensive_rebounds", "dreb": "defensive_rebounds",
                    "pf": "personal_fouls",
                }
                matchup_stat = stat_map.get(stat_type, stat_type)
                history = get_player_vs_team_history(
                    player_name, opp_abbrev, matchup_stat, game_logs
                )
                if history and not history.get("cold_start"):
                    enriched_ctx["matchup_avg_vs_team"] = history.get("avg_vs_team")
                    enriched_ctx["matchup_favorability_score"] = history.get("matchup_favorability_score")
            except Exception as exc:
                _logger.debug("matchup enrichment failed: %s", exc)

        features = build_feature_matrix(player_data, team_data, opponent_data, enriched_ctx)
        X = np.array([list(features.values())], dtype=float)

        model = _load_best_model(stat_type)
        if model is not None:
            preds = model.predict(X)
            prediction = float(preds[0]) if hasattr(preds, "__len__") else float(preds)
            result["prediction"] = round(prediction, 2)
            result["confidence_interval"] = (
                round(prediction * 0.85, 2),
                round(prediction * 1.15, 2),
            )
            result["source"] = "ml_model"
        else:
            # Statistical fallback: return a baseline value
            _STAT_DEFAULTS = {"pts": 15.0, "reb": 5.0, "ast": 4.0, "stl": 1.0, "blk": 0.5}
            result["prediction"] = _STAT_DEFAULTS.get(stat_type, 5.0)
            result["confidence_interval"] = None
            result["source"] = "default_fallback"
    except Exception as exc:
        _logger.debug("predict_player_stat failed for %s/%s: %s", player_name, stat_type, exc)
        result["source"] = "error"

    return result
