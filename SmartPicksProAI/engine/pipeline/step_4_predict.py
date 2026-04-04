"""engine/pipeline/step_4_predict.py – Phase 4: Generate predictions.

Now passes real game-context features to the predictor and predicts all
eight trained stat types (pts, reb, ast, stl, blk, tov, fg3m, ftm).
"""
from utils.logger import get_logger

_logger = get_logger(__name__)

_MAX_PLAYERS = 50
_ALL_STATS = ["pts", "reb", "ast", "stl", "blk", "tov", "fg3m", "ftm"]


def _build_game_context(row) -> dict:
    """Extract a game-context dict from an enriched player-feature row.

    Args:
        row: A pandas Series (one player-game) from the feature DataFrame.

    Returns:
        Dict understood by ``predict_player_stat`` and ``build_feature_matrix``.
    """
    ctx = {}
    _safe = lambda k, default=0: float(row.get(k, default) or default)

    ctx["rest_days"] = int(_safe("rest_days", 1))
    ctx["is_home"] = bool(int(_safe("is_home", 0)))
    ctx["team_pace"] = _safe("team_pace", 100.0)
    ctx["opponent_pace"] = _safe("opp_pace", 100.0)
    ctx["opponent_drtg"] = _safe("opp_drtg", 110.0)

    # Optional enrichment fields (present if step_3 succeeded)
    for key in ("pace_adjustment", "defensive_matchup_factor", "rest_factor",
                "dvp_vs_pts_mult", "dvp_vs_reb_mult", "dvp_vs_ast_mult",
                "dvp_vs_stl_mult", "dvp_vs_blk_mult", "dvp_vs_3pm_mult"):
        val = row.get(key)
        if val is not None:
            ctx[key] = float(val)

    return ctx


def run(context: dict) -> dict:
    """Run predictions using saved ML models.

    Args:
        context: Pipeline context with ``feature_data``.

    Returns:
        Updated context with ``predictions`` key.
    """
    predictions = []
    feature_data = context.get("feature_data", {})

    try:
        from engine.predict.predictor import predict_player_stat

        player_df = feature_data.get("player_features")
        if player_df is not None:
            try:
                import pandas as pd
                df = pd.DataFrame(player_df) if isinstance(player_df, list) else player_df

                # Deduplicate to one row per player (latest game)
                if "player_name" in df.columns and "game_date" in df.columns:
                    latest = (
                        df.sort_values("game_date", ascending=False)
                        .drop_duplicates(subset="player_name", keep="first")
                    )
                else:
                    latest = df

                for _, row in latest.head(context.get("max_players", _MAX_PLAYERS)).iterrows():
                    player_name = row.get("player_name") or row.get("name", "Unknown")
                    game_ctx = _build_game_context(row)

                    for stat in _ALL_STATS:
                        try:
                            result = predict_player_stat(str(player_name), stat, game_ctx)
                            predictions.append(result)
                        except Exception as exc:
                            _logger.debug("predict %s/%s failed: %s", player_name, stat, exc)
            except Exception as exc:
                _logger.debug("DataFrame iteration failed: %s", exc)
    except ImportError as exc:
        _logger.debug("predictor not available: %s", exc)

    _logger.info("Generated %d predictions", len(predictions))
    context["predictions"] = predictions
    return context
