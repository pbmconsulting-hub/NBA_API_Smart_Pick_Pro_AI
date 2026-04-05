"""
1_🏀_Todays_Games — Today's NBA games with live scores and matchup details.

Streamlit native multi-page file.  Delegates to the SmartPicksProAI backend
API for game data.
"""

import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
for _p in (_ROOT, _ROOT / "SmartPicksProAI", _ROOT / "SmartPicksProAI" / "frontend"):
    if str(_p) not in sys.path:
        sys.path.insert(0, str(_p))

import streamlit as st  # noqa: E402

st.set_page_config(page_title="Today's Games", page_icon="🏀", layout="wide")

try:
    from SmartPicksProAI.utils.components import inject_joseph_floating  # noqa: E402
except ImportError:
    def inject_joseph_floating() -> None:  # type: ignore[misc]
        pass

try:
    from SmartPicksProAI.frontend.api_service import (
        get_todays_games,
        get_recent_games,
    )
    _API = True
except ImportError:
    _API = False

st.title("🏀 Today's Games")
st.caption("Live scores, matchups, and game details for today's NBA slate.")

if not _API:
    st.error("Backend API unavailable — start the backend first.")
    st.stop()

# ── Today's Games Grid ──────────────────────────────────────────────────
games = get_todays_games()

if games:
    st.markdown(f"### {len(games)} Games Today")
    for game in games:
        matchup = game.get("matchup", "TBD")
        status = game.get("status", "")
        home_team = game.get("home_team", "")
        away_team = game.get("away_team", "")
        home_score = game.get("home_score")
        away_score = game.get("away_score")
        game_time = game.get("game_time", "")
        arena = game.get("arena", "")

        with st.container():
            cols = st.columns([3, 1, 1, 1])

            # Matchup info
            with cols[0]:
                st.markdown(f"#### 🏀 {matchup}")
                if arena:
                    st.caption(f"📍 {arena}")
                if game_time:
                    st.caption(f"🕐 {game_time}")

            # Scores
            with cols[1]:
                if home_score is not None:
                    st.metric(home_team or "Home", home_score)
                else:
                    st.metric(home_team or "Home", "—")

            with cols[2]:
                if away_score is not None:
                    st.metric(away_team or "Away", away_score)
                else:
                    st.metric(away_team or "Away", "—")

            # Status
            with cols[3]:
                if status:
                    if "Final" in str(status):
                        st.success(status)
                    elif "progress" in str(status).lower() or "live" in str(status).lower():
                        st.warning(f"🔴 {status}")
                    else:
                        st.info(status)
            st.divider()
else:
    st.info("No games scheduled for today. Check back later!")

# ── Recent Games ────────────────────────────────────────────────────────
st.markdown("---")
st.markdown("## 🕐 Recent Games")

recent = get_recent_games()
if recent:
    for game in recent[:15]:
        matchup = game.get("matchup", "TBD")
        gdate = game.get("game_date", "")
        home_score = game.get("home_score")
        away_score = game.get("away_score")
        score_str = ""
        if home_score is not None and away_score is not None:
            score_str = f" | {home_score} – {away_score}"
        st.markdown(f"🏀 **{matchup}**{score_str} — {gdate}")
else:
    st.info("No recent game data available.")

# ── Floating Joseph widget ──────────────────────────────────────────────
inject_joseph_floating()
