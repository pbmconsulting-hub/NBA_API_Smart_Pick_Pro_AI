"""api/routes/players.py – Player stats and projections endpoint."""
from utils.logger import get_logger

_logger = get_logger(__name__)

try:
    from fastapi import APIRouter, HTTPException
    router = APIRouter(prefix="/players", tags=["players"])
    _FASTAPI_AVAILABLE = True
except ImportError:
    _FASTAPI_AVAILABLE = False
    router = None

if _FASTAPI_AVAILABLE:
    @router.get("/{name}/stats")
    async def get_player_stats(name: str):
        """Return player stats and projections.

        Args:
            name: Player name (URL-encoded).

        Returns:
            Dict with player name, stats, and projections.
        """
        result = {"player": name, "stats": {}, "projections": {}, "source": "unavailable"}

        try:
            from data.nba_data_service import get_player_stats as _get_all_player_stats
            all_stats = _get_all_player_stats() or []
            # Filter to the requested player (case-insensitive partial match)
            name_lower = name.lower()
            matched = [
                s for s in all_stats
                if name_lower in str(s.get("PLAYER_NAME", s.get("player_name", ""))).lower()
            ]
            if matched:
                result["stats"] = matched[0]
                result["source"] = "nba_api"
        except Exception as exc:
            _logger.debug("get_player_stats failed: %s", exc)

        # Generate projections for key stats
        try:
            from engine.predict.predictor import predict_player_stat
            for stat in ["pts", "reb", "ast"]:
                proj = predict_player_stat(name, stat, {})
                result["projections"][stat] = proj.get("prediction")
        except Exception as exc:
            _logger.debug("projections failed: %s", exc)

        if not result["stats"] and not result["projections"]:
            raise HTTPException(status_code=404, detail=f"Player '{name}' not found")

        return result
