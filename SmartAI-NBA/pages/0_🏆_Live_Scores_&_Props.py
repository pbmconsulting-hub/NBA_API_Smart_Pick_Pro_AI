# ============================================================
# FILE: pages/0_🏆_Live_Scores_&_Props.py
# PURPOSE: Live NBA scoreboard, tonight's stats leaders, and
#          season leaders — the first page in the sidebar.
# DESIGN:  Quantum Design System (QDS) — dark futuristic theme
# ============================================================

import streamlit as st
import datetime
import os
import time

from styles.theme import get_global_css, get_qds_css

try:
    from utils.logger import get_logger
    _logger = get_logger(__name__)
except ImportError:
    import logging
    _logger = logging.getLogger(__name__)

# ============================================================
# SECTION: Page Setup
# ============================================================

st.set_page_config(
    page_title="Live Scores — SmartBetPro NBA",
    page_icon="🏆",
    layout="wide",
)

st.markdown(get_global_css(), unsafe_allow_html=True)
st.markdown(get_qds_css(), unsafe_allow_html=True)

# ── Joseph M. Smith Floating Widget ───────────────────────────
from utils.components import render_joseph_hero_banner, inject_joseph_floating
render_joseph_hero_banner()
st.session_state["joseph_page_context"] = "page_live_scores"
inject_joseph_floating()

st.title("🏆 Live Scores & Stats Leaders")
st.markdown(
    f"**{datetime.date.today().strftime('%A, %B %d, %Y')}** — Real-time NBA scores and leaderboards."
)

with st.expander("📖 How to Use This Page", expanded=False):
    st.markdown("""
    ### Live Scores & Stats Leaders
    
    This page shows **real-time NBA scores** and **statistical leaderboards** for the current season.
    
    **Live Scores**
    - Automatically refreshed from the NBA data feed
    - Shows current score, quarter, and time remaining for active games
    - Completed games show final scores
    
    **Season Leaders**
    - View top performers across key stats (points, rebounds, assists, etc.)
    - Updated automatically when you refresh player data on the Data Feed page
    
    💡 **Pro Tips:**
    - Use this page to quickly scout which players are hot before placing bets
    - Check if a player's recent performance matches their season averages
    - Compare leaders across stat categories for correlation insights
    """)

st.divider()

# ============================================================
# END SECTION: Page Setup
# ============================================================


# ============================================================
# SECTION: Auto-Refresh Toggle
# ============================================================

col_refresh, col_interval, _ = st.columns([1, 1, 3])
with col_refresh:
    auto_refresh = st.toggle("🔄 Auto-Refresh (30s)", value=False)
with col_interval:
    if auto_refresh:
        st.caption("Page will refresh every 30 seconds while live games are in progress.")

# ============================================================
# END SECTION: Auto-Refresh Toggle
# ============================================================


# ============================================================
# SECTION: Load Live Scoreboard
# ============================================================

def _get_live_scores():
    """
    Attempt to retrieve live/today's game scores.

    Returns:
        list of dict: Game score data, or empty list on failure.
    """
    return []


def _status_badge(status_text):
    """Return a colored HTML badge for game status."""
    s = str(status_text).upper().strip()
    if "FINAL" in s:
        return "🏁 Final"
    if any(kw in s for kw in ("QTR", "HALF", "OT", "Q1", "Q2", "Q3", "Q4")):
        return "🔴 LIVE"
    return "🗓️ Scheduled"


with st.spinner("🏆 Loading live scores..."):
    live_games = _get_live_scores()

# ============================================================
# END SECTION: Load Live Scoreboard
# ============================================================


# ============================================================
# SECTION: Live Scoreboard Display
# ============================================================

st.subheader("🏀 Live Scoreboard")


def _get_quarter_scores(game_id: str) -> dict:
    """
    Attempt to retrieve per-quarter (period) line score for a given game_id.

    Returns dict with keys 'away_q' and 'home_q' each being a list of
    quarter scores (indices 0-3 = Q1-Q4, index 4 = OT if present).
    """
    result = {"away_q": [], "home_q": []}

    def _parse_v3_periods(response):
        """Parse V3 normalized dict into (away_q, home_q) lists."""
        game = response.get("game", {})
        away_periods = game.get("awayTeam", {}).get("periods", [])
        home_periods = game.get("homeTeam", {}).get("periods", [])
        away_q = [int(p.get("score", 0)) for p in away_periods
                  if p.get("periodType", "REGULAR") == "REGULAR"]
        home_q = [int(p.get("score", 0)) for p in home_periods
                  if p.get("periodType", "REGULAR") == "REGULAR"]
        return away_q, home_q

    # ── Attempt 1: Direct requests call to V3 endpoint ──────────────
    # (nba_api library no longer used — direct HTTP call for quarter scores)

    # ── Attempt 2: Direct requests call to V3 endpoint ────────
    try:
        import requests as _req
        _headers = {
            "Host": "stats.nba.com",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
            "x-nba-stats-origin": "stats",
            "x-nba-stats-token": "true",
            "Connection": "keep-alive",
            "Referer": "https://www.nba.com/",
            "Origin": "https://www.nba.com",
        }
        _url = f"https://stats.nba.com/stats/boxscoresummaryv3?GameID={game_id}"
        _resp = _req.get(_url, headers=_headers, timeout=8)
        _resp.raise_for_status()
        _data = _resp.json()
        away_q, home_q = _parse_v3_periods(_data)
        if away_q or home_q:
            result["away_q"] = away_q
            result["home_q"] = home_q
            return result
    except Exception:
        pass

    return result


def _quarter_table_html(away: str, home: str, away_q: list, home_q: list,
                        away_total: int, home_total: int) -> str:
    """Build a compact quarter-by-quarter score table as HTML."""
    max_q = max(len(away_q), len(home_q), 4)
    header_cells = "".join(
        f'<th style="padding:4px 8px;color:#8a9bb8;font-size:0.75rem;">'
        f'{"OT" if i >= 4 else f"Q{i+1}"}</th>'
        for i in range(max_q)
    ) + '<th style="padding:4px 10px;color:#ff5e00;font-size:0.8rem;font-weight:700;">TOT</th>'

    def _cells(scores, total, opp_total):
        cells = ""
        for i in range(max_q):
            v = scores[i] if i < len(scores) else "–"
            cells += f'<td style="padding:4px 8px;color:#c0d0e8;font-size:0.78rem;text-align:center;">{v}</td>'
        c = "#00ff9d" if total > opp_total else ("#ff6b6b" if total < opp_total else "#c0d0e8")
        cells += f'<td style="padding:4px 10px;color:{c};font-weight:700;font-size:0.85rem;text-align:center;">{total}</td>'
        return cells

    rows = (
        f'<tr><td style="padding:4px 10px;color:#c0d0e8;font-weight:700;font-size:0.8rem;">{away}</td>'
        f'{_cells(away_q, away_total, home_total)}</tr>'
        f'<tr><td style="padding:4px 10px;color:#c0d0e8;font-weight:700;font-size:0.8rem;">{home}</td>'
        f'{_cells(home_q, home_total, away_total)}</tr>'
    )
    return (
        f'<table style="width:100%;border-collapse:collapse;margin-top:8px;'
        f'background:rgba(0,0,0,0.2);border-radius:6px;">'
        f'<thead><tr><th style="padding:4px 10px;"></th>{header_cells}</tr></thead>'
        f'<tbody>{rows}</tbody></table>'
    )


if live_games:
    cols = st.columns(min(len(live_games), 3))
    for idx, game in enumerate(live_games):
        col = cols[idx % 3]
        with col:
            badge = _status_badge(game.get("status", ""))
            home = game.get("home_team", "?")
            away = game.get("away_team", "?")
            h_pts = game.get("home_score", 0)
            a_pts = game.get("away_score", 0)
            clock = game.get("clock", "")
            period = game.get("period", "")
            period_txt = f"Q{period}" if period else ""
            clock_txt = f"{period_txt} {clock}".strip() if (period_txt or clock) else ""

            score_color_home = "#00ff9d" if h_pts > a_pts else "#c8d8f0"
            score_color_away = "#00ff9d" if a_pts > h_pts else "#c8d8f0"

            # Retrieve quarter-by-quarter scores if game_id available
            game_id = game.get("game_id", "")
            quarter_html = ""
            if game_id:
                qscores = _get_quarter_scores(game_id)
                away_q = qscores.get("away_q", [])
                home_q = qscores.get("home_q", [])
                if away_q or home_q:
                    quarter_html = _quarter_table_html(away, home, away_q, home_q, a_pts, h_pts)

            st.markdown(
                f"""
                <div style="
                    background: rgba(20,25,43,0.85);
                    border: 1px solid rgba(0,240,255,0.20);
                    border-radius: 12px;
                    padding: 16px 20px;
                    margin-bottom: 12px;
                    text-align: center;
                ">
                    <div style="font-size:0.75rem;color:#8a9bb8;margin-bottom:6px;">{badge}</div>
                    <div style="display:flex;justify-content:space-between;align-items:center;gap:8px;">
                        <div style="flex:1;text-align:right;">
                            <div style="font-size:1.1rem;font-weight:700;color:#c8d8f0;">{away}</div>
                            <div style="font-size:2rem;font-weight:900;color:{score_color_away};">{a_pts}</div>
                        </div>
                        <div style="font-size:1.2rem;color:#ff5e00;font-weight:800;padding:0 8px;">@</div>
                        <div style="flex:1;text-align:left;">
                            <div style="font-size:1.1rem;font-weight:700;color:#c8d8f0;">{home}</div>
                            <div style="font-size:2rem;font-weight:900;color:{score_color_home};">{h_pts}</div>
                        </div>
                    </div>
                    {f'<div style="font-size:0.8rem;color:#00f0ff;margin-top:6px;">{clock_txt}</div>' if clock_txt else ''}
                    {quarter_html}
                </div>
                """,
                unsafe_allow_html=True,
            )
else:
    st.info(
        "📡 No live scores available right now.\n\n"
        "This may be because:\n"
        "- No games are currently scheduled or in progress\n"
        "- The data feed is temporarily unavailable\n\n"
        "Check back when games are live!"
    )

# ============================================================
# END SECTION: Live Scoreboard Display
# ============================================================

st.divider()

# ============================================================
# SECTION: Tonight's Stats Leaders
# ============================================================

st.subheader("📊 Tonight's Stats Leaders")

def _get_tonights_leaders():
    """
    Attempt to get tonight's top performers from live box scores.
    Returns a dict with lists of top players per category.
    """
    leaders = {"points": [], "rebounds": [], "assists": [], "threes": []}
    # Live stats leaders come from session state (populated by Neural Analysis or API-NBA)
    try:
        players_live = st.session_state.get("players_data", [])
        for player in players_live:
            name = player.get("name", "")
            team = player.get("team", "")
            pts = float(player.get("points_avg", 0) or 0)
            reb = float(player.get("rebounds_avg", 0) or 0)
            ast = float(player.get("assists_avg", 0) or 0)
            fg3m = float(player.get("threes_avg", 0) or 0)
            if name:
                leaders["points"].append({"player": name, "team": team, "value": pts})
                leaders["rebounds"].append({"player": name, "team": team, "value": reb})
                leaders["assists"].append({"player": name, "team": team, "value": ast})
                leaders["threes"].append({"player": name, "team": team, "value": fg3m})
    except Exception:
        pass
    # Sort and return top 5
    for cat in leaders:
        leaders[cat] = sorted(leaders[cat], key=lambda x: x["value"], reverse=True)[:5]
    return leaders


if live_games:
    with st.spinner("Loading tonight's stats leaders..."):
        tonights_leaders = _get_tonights_leaders()

    _CAT_LABELS = {
        "points": "🏀 Points",
        "rebounds": "💪 Rebounds",
        "assists": "🎯 Assists",
        "threes": "3️⃣ 3-Pointers",
    }

    has_leaders = any(tonights_leaders.get(cat) for cat in _CAT_LABELS)
    if has_leaders:
        lcols = st.columns(4)
        for ci, (cat, label) in enumerate(_CAT_LABELS.items()):
            with lcols[ci]:
                st.markdown(f"**{label}**")
                leaders_list = tonights_leaders.get(cat, [])
                if leaders_list:
                    for rank, entry in enumerate(leaders_list, 1):
                        st.markdown(
                            f"{rank}. **{entry['player']}** ({entry['team']}) — `{entry['value']}`"
                        )
                else:
                    st.caption("No data yet")
    else:
        st.info("Tonight's stats will appear once games are in progress.")
else:
    st.info("Stats leaders will be available once tonight's games begin.")

# ============================================================
# END SECTION: Tonight's Stats Leaders
# ============================================================

st.divider()

# ============================================================
# SECTION: Season Leaders
# ============================================================

st.subheader("🏆 Season Leaders")

players_data = st.session_state.get("players_data", [])
if not players_data:
    # Try loading from disk
    try:
        from data.data_manager import load_players_data
        players_data = load_players_data()
        if players_data:
            st.session_state["players_data"] = players_data
    except Exception:
        pass

if players_data:
    def _top_n(data, stat_key, n=5):
        ranked = sorted(
            [p for p in data if p.get(stat_key, 0)],
            key=lambda p: p.get(stat_key, 0),
            reverse=True,
        )
        return ranked[:n]

    _SEASON_CATS = [
        ("season_pts_avg", "🏀 PPG"),
        ("season_reb_avg", "💪 RPG"),
        ("season_ast_avg", "🎯 APG"),
    ]

    scols = st.columns(len(_SEASON_CATS))
    for ci, (stat_key, label) in enumerate(_SEASON_CATS):
        with scols[ci]:
            st.markdown(f"**{label}**")
            top_players = _top_n(players_data, stat_key, n=5)
            if top_players:
                for rank, player in enumerate(top_players, 1):
                    name = player.get("name", "Unknown")
                    team = player.get("team", "")
                    val = player.get(stat_key, 0)
                    st.markdown(
                        f"{rank}. **{name}** ({team}) — `{val:.1f}`"
                    )
            else:
                st.caption(f"No {label} data available")
else:
    st.info(
        "💡 Season leaders will appear here once player data is loaded. "
        "Go to **📡 Live Games** and click **Auto-Load Tonight's Games** to load player data."
    )

# ============================================================
# END SECTION: Season Leaders
# ============================================================

st.divider()

# ============================================================
# SECTION: Prop Tracker Widget
# Shows how active props from tonight are tracking vs their lines
# based on live box-score stats.
# ============================================================

st.subheader("📌 Live Prop Tracker")
st.caption(
    "See how tonight's active props are tracking against their lines in real time. "
    "Go to **🔬 Prop Scanner** and **⚡ Neural Analysis** to set up your props first."
)

_active_props = st.session_state.get("analysis_results", [])
_analysis_top = [p for p in _active_props if not p.get("should_avoid", False)][:20]

if not live_games:
    st.info("📡 Prop tracker activates once live games are in progress.")
elif not _analysis_top:
    st.info(
        "📌 No active props to track yet. "
        "Run **⚡ Neural Analysis** to generate top picks, then come back here to monitor them live."
    )
else:
    # Build a lookup: player_name → box-score stats from live games
    _live_player_stats: dict = {}
    # Live box score data comes from live_games (API-NBA API)
    try:
        for _g in live_games:
            _ht = _g.get("home_team", "")
            _at = _g.get("away_team", "")
            _hs = int(_g.get("home_score", 0) or 0)
            _as = int(_g.get("away_score", 0) or 0)
            # Player-level live stats come from session state if available
        # Also check session state players_data for season averages as proxy
        for _pl in st.session_state.get("players_data", []):
            _pn = _pl.get("name", "").strip()
            if _pn:
                _live_player_stats[_pn.lower()] = {
                    "points": float(_pl.get("points_avg", 0) or 0),
                    "rebounds": float(_pl.get("rebounds_avg", 0) or 0),
                    "assists": float(_pl.get("assists_avg", 0) or 0),
                    "threes": float(_pl.get("threes_avg", 0) or 0),
                    "steals": float(_pl.get("steals_avg", 0) or 0),
                    "blocks": float(_pl.get("blocks_avg", 0) or 0),
                }
    except Exception:
        pass

    _tracked = 0
    _tracker_html = ""
    for _prop in _analysis_top:
        _pname = _prop.get("player_name", "")
        _stat = _prop.get("stat_type", "").lower()
        _line = float(_prop.get("line", 0))
        _dir = _prop.get("direction", "OVER")
        _tier = _prop.get("tier_emoji", "") + _prop.get("tier", "")

        _live = _live_player_stats.get(_pname.lower(), {})
        _cur = _live.get(_stat, None)
        if _cur is None:
            continue

        _tracked += 1
        _diff = _cur - _line
        if _dir == "OVER":
            _hitting = _cur > _line
            _status_txt = f"ON TRACK ↑" if _hitting else "BEHIND ↓"
            _status_clr = "#00ff9d" if _hitting else "#ff6b6b"
        else:
            _hitting = _cur < _line
            _status_txt = f"ON TRACK ↓" if _hitting else "OVER PACE ↑"
            _status_clr = "#00ff9d" if _hitting else "#ff6b6b"

        # progress bar: pct of line reached
        _pct = min(100, round((_cur / _line * 100) if _line > 0 else 0))
        _bar_color = "#00ff9d" if _hitting else "#ff6b6b"
        import html as _html_mod
        _tracker_html += (
            f'<div style="background:#14192b;border-radius:6px;padding:10px 14px;'
            f'margin-bottom:8px;border-left:3px solid {_bar_color};">'
            f'<div style="display:flex;justify-content:space-between;align-items:center;">'
            f'<div><span style="color:#c0d0e8;font-weight:700;">{_html_mod.escape(_pname)}</span>'
            f'&nbsp;<span style="color:#8a9bb8;font-size:0.8rem;">{_stat.title()} {_dir} {_line}</span>'
            f'&nbsp;<span style="color:#8a9bb8;font-size:0.75rem;">{_tier}</span></div>'
            f'<div style="color:{_bar_color};font-weight:700;font-size:0.9rem;">{_status_txt}</div>'
            f'</div>'
            f'<div style="display:flex;align-items:center;gap:10px;margin-top:6px;">'
            f'<div style="flex:1;height:6px;background:#1a2035;border-radius:3px;">'
            f'<div style="width:{_pct}%;height:6px;background:{_bar_color};border-radius:3px;"></div></div>'
            f'<div style="color:#c0d0e8;font-size:0.85rem;white-space:nowrap;">'
            f'<strong style="color:{_bar_color};">{_cur}</strong> / {_line} '
            f'({_diff:+.1f})</div>'
            f'</div>'
            f'</div>'
        )

    if _tracked == 0:
        st.info(
            "📊 No live box-score data found for your active props yet. "
            "Stats appear once games tip off."
        )
    else:
        st.markdown(f"Tracking **{_tracked}** active prop(s) from tonight's analysis:")
        st.markdown(_tracker_html, unsafe_allow_html=True)

# ============================================================
# END SECTION: Prop Tracker Widget
# ============================================================


# ============================================================
# ============================================================
# SECTION: Auto-Refresh Handler
# ============================================================

if auto_refresh:
    time.sleep(30)
    st.rerun()

# ============================================================
# END SECTION: Auto-Refresh Handler
# ============================================================
