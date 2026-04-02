"""
Data module for SmartAI-NBA.

Provides high-level access to NBA data services, roster management,
and player ID resolution.
"""

try:
    from data.nba_data_service import NBADataService
except ImportError:
    pass

try:
    from data.roster_engine import RosterEngine
except ImportError:
    pass

try:
    from data.player_id_cache import PlayerIDCache
except ImportError:
    pass

__all__ = [
    "NBADataService",
    "RosterEngine",
    "PlayerIDCache",
]