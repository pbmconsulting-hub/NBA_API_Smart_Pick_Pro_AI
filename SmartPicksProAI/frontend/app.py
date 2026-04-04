"""
app.py
------
Streamlit dashboard for SmartPicksProAI — Smart Pick Pro AI Edition.

Quantum Edge dark theme with glassmorphism, neon cyan/green glow,
Orbitron headings, and Joseph M Smith branded assets.

Includes: Prop Analyzer, Bet Tracker, Pick History, and full NBA stats.

Start the dashboard::

    cd SmartPicksProAI/frontend
    streamlit run app.py
"""

import sys
import os
import datetime
import pandas as pd
import streamlit as st

from typing import Any
from collections.abc import Callable
from pathlib import Path

# ── Ensure SmartPicksProAI package root is on sys.path ──────
_FRONTEND_DIR = Path(__file__).resolve().parent
_PACKAGE_ROOT = _FRONTEND_DIR.parent
if str(_PACKAGE_ROOT) not in sys.path:
    sys.path.insert(0, str(_PACKAGE_ROOT))

from styles.theme import (
    get_global_css,
    get_hero_banner_html,
    get_sidebar_avatar_html,
    get_sidebar_brand_html,
    get_summary_cards_html,
    get_tier_badge_html,
    get_verdict_banner_html,
)
from tracking.database import initialize_database as _init_tracker_db
from tracking.bet_tracker import (
    auto_log_analysis_bets,
    get_model_performance_stats,
    log_new_bet,
    normalize_platform,
    record_bet_result,
    VALID_PLATFORMS,
)
from tracking.database import (
    load_all_bets,
    load_analysis_picks,
    get_performance_by_tier,
)

from api_service import (
    analyze_prop,
    get_defense_vs_position,
    get_game_box_score,
    get_game_rotation,
    get_league_dash_players,
    get_league_dash_teams,
    get_league_leaders,
    get_pick_history,
    get_play_by_play,
    get_player_advanced,
    get_player_bio,
    get_player_career,
    get_player_clutch,
    get_player_hustle,
    get_player_last5,
    get_player_matchups,
    get_player_projection,
    get_player_scoring,
    get_player_shot_chart,
    get_player_tracking,
    get_player_usage,
    get_recent_games,
    get_schedule,
    get_standings,
    get_team_clutch,
    get_team_details,
    get_team_estimated_metrics,
    get_team_hustle,
    get_team_roster,
    get_team_stats,
    get_teams,
    get_todays_games,
    get_win_probability,
    save_pick,
    search_players,
    trigger_refresh,
    update_pick_result,
)

# ═══════════════════════════════════════════════════════════════════════════
# Page configuration & constants
# ═══════════════════════════════════════════════════════════════════════════

MAX_GAME_COLUMNS = 4
MAX_RECENT_GAMES = 20
MAX_SEARCH_RESULTS = 10

# Example bankroll used for display purposes only (not financial advice).
EXAMPLE_BANKROLL = 500.0

st.set_page_config(
    page_title="Smart Pick Pro — NBA Edition",
    page_icon="🏀",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Initialize bet tracker database
_init_tracker_db()

# ═══════════════════════════════════════════════════════════════════════════
# Session-state navigation
# ═══════════════════════════════════════════════════════════════════════════

_DEFAULT_STATE: dict[str, Any] = {
    "page": "home",
    "selected_game_id": None,
    "selected_player_id": None,
    "selected_team_id": None,
    "game_context": {},
}
for _key, _default in _DEFAULT_STATE.items():
    if _key not in st.session_state:
        st.session_state[_key] = _default


def _nav(page: str, **kwargs) -> None:
    """Navigate to a page, setting any additional session state keys."""
    st.session_state.page = page
    for k, v in kwargs.items():
        st.session_state[k] = v


# ═══════════════════════════════════════════════════════════════════════════
# Quantum Edge Dark Theme (from styles/theme.py)
# ═══════════════════════════════════════════════════════════════════════════

st.markdown(get_global_css(), unsafe_allow_html=True)


# ═══════════════════════════════════════════════════════════════════════════
# Helpers
# ═══════════════════════════════════════════════════════════════════════════

def _show_df(
    data: list[dict[str, Any]] | pd.DataFrame,
    columns: list[str] | None = None,
    height: int | None = None,
) -> None:
    """Display data as a styled dataframe."""
    if not data:
        st.markdown('<div class="empty-state">No data available.</div>',
                    unsafe_allow_html=True)
        return
    df = pd.DataFrame(data) if isinstance(data, list) else data
    if columns:
        columns = [c for c in columns if c in df.columns]
        if columns:
            df = df[columns]
    kwargs = {"use_container_width": True, "hide_index": True}
    if height:
        kwargs["height"] = height
    st.dataframe(df, **kwargs)


def _player_button(
    player_id: int,
    name: str,
    position: str | None = None,
    team: str | None = None,
    key_prefix: str = "",
) -> None:
    """Render a clickable button for a player that navigates to their profile."""
    label_parts = [name]
    if position:
        label_parts.append(f"({position})")
    if team:
        label_parts.append(f"· {team}")
    label = " ".join(label_parts)
    if st.button(f"👤 {label}", key=f"{key_prefix}_p_{player_id}",
                 use_container_width=True):
        _nav("player_profile", selected_player_id=player_id)
        st.rerun()


def _game_button(game: dict[str, Any], key_prefix: str = "") -> None:
    """Render a clickable game card button."""
    matchup = game.get("matchup", "TBD")
    home_score = game.get("home_score")
    away_score = game.get("away_score")
    game_date = game.get("game_date", "")
    gid = game.get("game_id", "")

    if home_score is not None and away_score is not None:
        label = f"🏀 {matchup}  |  {home_score} – {away_score}  |  {game_date}"
    else:
        label = f"🏀 {matchup}  |  {game_date}"

    if st.button(label, key=f"{key_prefix}_g_{gid}", use_container_width=True):
        _nav("game_detail", selected_game_id=gid, game_context=game)
        st.rerun()


# ═══════════════════════════════════════════════════════════════════════════
# Sidebar — Smart Pick Pro branded nav + Joseph M Smith avatar
# ═══════════════════════════════════════════════════════════════════════════

with st.sidebar:
    # Logo / Brand
    st.markdown(get_sidebar_brand_html(), unsafe_allow_html=True)

    # Joseph M Smith avatar
    st.markdown(get_sidebar_avatar_html(), unsafe_allow_html=True)
    st.divider()

    # ── Navigation ────────────────────────────────────────────
    st.markdown(
        '<div class="section-hdr">Navigation</div>',
        unsafe_allow_html=True,
    )

    nav_items = [
        ("🏠  Home", "home"),
        ("🎯  Prop Analyzer", "prop_analyzer"),
        ("📈  Bet Tracker", "bet_tracker"),
        ("📋  Pick History", "pick_history"),
        ("🏆  Standings", "standings"),
        ("🏟️  Teams", "teams_browse"),
        ("📊  Leaders & Stats", "leaders"),
        ("🛡️  Defense vs Position", "defense"),
        ("🗓️  Schedule", "more"),
    ]
    for label, page_key in nav_items:
        if st.button(label, key=f"nav_{page_key}", use_container_width=True):
            _nav(page_key)
            st.rerun()

    st.divider()

    # ── Player Quick Search ───────────────────────────────────
    st.markdown(
        '<div class="section-hdr">Quick Player Search</div>',
        unsafe_allow_html=True,
    )
    sidebar_search = st.text_input(
        "Search player",
        placeholder="e.g. LeBron, Curry …",
        key="sidebar_search",
        label_visibility="collapsed",
    )
    if sidebar_search.strip():
        results = search_players(sidebar_search.strip())
        if results:
            for r in results[:8]:
                pid = r["player_id"]
                nm = r.get("full_name", "")
                pos = r.get("position", "")
                tm = r.get("team_abbreviation", "")
                btn_label = f"{nm}"
                if pos:
                    btn_label += f" ({pos})"
                if tm:
                    btn_label += f" · {tm}"
                if st.button(btn_label, key=f"sb_p_{pid}",
                             use_container_width=True):
                    _nav("player_profile", selected_player_id=pid)
                    st.rerun()
        else:
            st.caption("No players found.")

    st.divider()

    # ── Admin ─────────────────────────────────────────────────
    st.markdown(
        '<div class="section-hdr">Admin</div>',
        unsafe_allow_html=True,
    )
    if st.button("🔄 Sync Latest Data", use_container_width=True,
                 key="admin_sync"):
        with st.spinner("Syncing with NBA API…"):
            result = trigger_refresh()
        if result.get("status") == "success":
            st.success(result.get("message", "Refresh complete."))
            for fn in [get_todays_games, get_player_last5, search_players,
                       get_teams, get_team_roster, get_team_stats,
                       get_defense_vs_position, get_standings,
                       get_league_leaders, get_recent_games]:
                fn.clear()
        else:
            st.error(f"Failed: {result.get('message', 'Unknown error')}")

    st.divider()
    st.markdown(
        '<div class="sidebar-engine-label">'
        '⚡ Powered by Quantum Matrix Engine 5.6</div>',
        unsafe_allow_html=True,
    )

# ═══════════════════════════════════════════════════════════════════════════
# Page functions
# ═══════════════════════════════════════════════════════════════════════════


# ─────────────────────────────────────────────────────────────────────────
# PAGE: HOME
# ─────────────────────────────────────────────────────────────────────────

def _page_home() -> None:
    # Hero banner
    st.markdown(get_hero_banner_html(), unsafe_allow_html=True)

    st.title("🏀 Smart Pick Pro AI")
    st.caption("Quantum AI Prop Intelligence — click any game or player to explore")

    # ── Today's Games ─────────────────────────────────────────
    st.markdown('<div class="section-hdr">Today\'s Matchups</div>',
                unsafe_allow_html=True)

    games = get_todays_games()
    if games:
        cols = st.columns(min(len(games), MAX_GAME_COLUMNS))
        for idx, game in enumerate(games):
            with cols[idx % len(cols)]:
                _game_button(game, key_prefix="today")
    else:
        st.info("No games scheduled for today.")

    st.divider()

    # ── Recent Games ──────────────────────────────────────────
    st.markdown('<div class="section-hdr">Recent Games</div>',
                unsafe_allow_html=True)

    recent = get_recent_games()
    if recent:
        # Show as clickable list
        for idx, game in enumerate(recent[:MAX_RECENT_GAMES]):
            _game_button(game, key_prefix="recent")
    else:
        st.info("No recent game data available.")

    st.divider()

    # ── Quick Player Search ───────────────────────────────────
    st.markdown('<div class="section-hdr">Player Lookup</div>',
                unsafe_allow_html=True)
    st.caption("Search for any player to view their complete profile.")

    search_col, id_col = st.columns([3, 1])
    with search_col:
        player_query = st.text_input(
            "Search by name",
            placeholder="e.g. LeBron, Curry, Jokic …",
            key="home_search",
        )
    with id_col:
        player_id_direct = st.number_input(
            "Player ID",
            min_value=0, value=0, step=1,
            key="home_pid",
        )

    if player_query.strip():
        results = search_players(player_query.strip())
        if results:
            for r in results[:MAX_SEARCH_RESULTS]:
                _player_button(
                    r["player_id"],
                    r.get("full_name", ""),
                    r.get("position"),
                    r.get("team_abbreviation"),
                    key_prefix="hs",
                )
        else:
            st.warning("No players found.")
    elif player_id_direct > 0:
        if st.button("Open Player Profile", key="home_open_pid"):
            _nav("player_profile", selected_player_id=player_id_direct)
            st.rerun()


# ─────────────────────────────────────────────────────────────────────────
# PAGE: GAME DETAIL
# ─────────────────────────────────────────────────────────────────────────

def _page_game_detail() -> None:
    gid = st.session_state.selected_game_id
    ctx = st.session_state.game_context or {}

    if st.button("← Back to Home", key="back_home_gd"):
        _nav("home")
        st.rerun()

    matchup = ctx.get("matchup", gid or "Game Detail")
    home_score = ctx.get("home_score")
    away_score = ctx.get("away_score")
    game_date = ctx.get("game_date", "")

    st.title(f"🏀 {matchup}")

    # Score + date header
    header_parts = []
    if game_date:
        header_parts.append(game_date)
    if home_score is not None and away_score is not None:
        header_parts.append(f"**{home_score} – {away_score}**")
    if header_parts:
        st.caption(" · ".join(header_parts))

    if not gid:
        st.warning("No game selected.")
    else:
        # ── Team info from context ────────────────────────────
        home_tid = ctx.get("home_team_id")
        away_tid = ctx.get("away_team_id")
        home_abbrev = ctx.get("home_abbrev", "")
        away_abbrev = ctx.get("away_abbrev", "")

        # Team stats comparison
        teams_data = get_teams()
        team_lookup = {t["team_id"]: t for t in teams_data} if teams_data else {}

        if home_tid and away_tid:
            home_team = team_lookup.get(home_tid, {})
            away_team = team_lookup.get(away_tid, {})

            if home_team or away_team:
                st.markdown('<div class="section-hdr">Team Comparison</div>',
                            unsafe_allow_html=True)
                st.caption(
                    "**Pace** = possessions per game (higher = faster).  "
                    "**ORtg** = points scored per 100 possessions (higher = better offense).  "
                    "**DRtg** = points allowed per 100 possessions (lower = better defense)."
                )
                c1, c2 = st.columns(2)
                with c1:
                    ht_name = home_team.get("team_name", home_abbrev)
                    st.markdown(f"#### 🏠 {home_abbrev} — {ht_name}")
                    m1 = st.columns(3)
                    m1[0].metric("Pace", home_team.get("pace", "N/A"))
                    m1[1].metric("ORtg", home_team.get("ortg", "N/A"))
                    m1[2].metric("DRtg", home_team.get("drtg", "N/A"))
                with c2:
                    at_name = away_team.get("team_name", away_abbrev)
                    st.markdown(f"#### ✈️ {away_abbrev} — {at_name}")
                    m2 = st.columns(3)
                    m2[0].metric("Pace", away_team.get("pace", "N/A"))
                    m2[1].metric("ORtg", away_team.get("ortg", "N/A"))
                    m2[2].metric("DRtg", away_team.get("drtg", "N/A"))

        st.divider()

        # ── Tabs for game data ────────────────────────────────
        tab_box, tab_rosters, tab_pbp, tab_wp, tab_rot = st.tabs([
            "📊 Box Score",
            "👥 Rosters (click players)",
            "📝 Play-by-Play",
            "📈 Win Probability",
            "🔄 Rotation",
        ])

        with tab_box:
            box = get_game_box_score(gid)
            if box:
                # Split by team
                teams_in_box = sorted(
                    set(p.get("team_abbreviation", "") for p in box)
                )
                for team_abbr in teams_in_box:
                    st.markdown(f"#### {team_abbr}")
                    team_players = [p for p in box
                                    if p.get("team_abbreviation") == team_abbr]
                    _show_df(team_players, [
                        "full_name", "position", "pts", "reb", "ast",
                        "stl", "blk", "tov", "fgm", "fga", "fg_pct",
                        "fg3m", "fg3a", "fg3_pct", "ftm", "fta", "ft_pct",
                        "oreb", "dreb", "pf", "plus_minus", "min", "wl",
                    ])
                    # Clickable player names
                    for p in team_players:
                        _player_button(
                            p["player_id"],
                            p.get("full_name", ""),
                            p.get("position"),
                            key_prefix=f"box_{team_abbr}",
                        )
                    st.divider()
            else:
                st.info("No box score data for this game.")

        with tab_rosters:
            if home_tid:
                st.markdown(f"#### 🏠 {home_abbrev} Roster")
                h_roster = get_team_roster(home_tid)
                if h_roster:
                    for p in h_roster:
                        _player_button(
                            p["player_id"],
                            p.get("full_name", ""),
                            p.get("position"),
                            key_prefix="hr",
                        )
                else:
                    st.info("No roster data.")
                st.divider()

            if away_tid:
                st.markdown(f"#### ✈️ {away_abbrev} Roster")
                a_roster = get_team_roster(away_tid)
                if a_roster:
                    for p in a_roster:
                        _player_button(
                            p["player_id"],
                            p.get("full_name", ""),
                            p.get("position"),
                            key_prefix="ar",
                        )
                else:
                    st.info("No roster data.")

        with tab_pbp:
            pbp = get_play_by_play(gid)
            if pbp:
                _show_df(pbp, [
                    "period", "clock", "description", "action_type",
                    "sub_type", "player_name", "team_tricode",
                    "score_home", "score_away", "shot_result",
                    "shot_distance",
                ], height=500)
            else:
                st.info("No play-by-play data.")

        with tab_wp:
            wp = get_win_probability(gid)
            if wp:
                df_wp = pd.DataFrame(wp)
                if "home_pct" in df_wp.columns:
                    st.line_chart(
                        df_wp.set_index("event_num")[
                            ["home_pct", "visitor_pct"]
                        ],
                        use_container_width=True,
                    )
                st.divider()
                _show_df(wp, [
                    "event_num", "home_pct", "visitor_pct",
                    "home_pts", "visitor_pts",
                    "home_score_margin", "period", "description",
                ], height=400)
            else:
                st.info("No win probability data.")

        with tab_rot:
            rot = get_game_rotation(gid)
            if rot:
                _show_df(rot, [
                    "full_name", "team_abbrev", "in_time_real",
                    "out_time_real", "player_pts", "pt_diff", "usg_pct",
                ], height=500)
            else:
                st.info("No rotation data.")


# ─────────────────────────────────────────────────────────────────────────
# PAGE: PLAYER PROFILE
# ─────────────────────────────────────────────────────────────────────────

def _page_player_profile() -> None:
    pid = st.session_state.selected_player_id

    if st.button("← Back", key="back_from_player"):
        _nav("home")
        st.rerun()

    if not pid:
        st.warning("No player selected.")
    else:
        pid = int(pid)

        # ── Header: Name + Bio summary ────────────────────────
        bio = get_player_bio(pid)
        last5 = get_player_last5(pid)

        player_name = ""
        if last5:
            player_name = (
                f"{last5.get('first_name', '')} {last5.get('last_name', '')}"
            ).strip()
        if not player_name and bio:
            player_name = bio.get("player_name", f"Player #{pid}")
        if not player_name:
            player_name = f"Player #{pid}"

        st.title(f"👤 {player_name}")

        # Quick bio metrics
        if bio:
            bio_cols = st.columns(8)
            bio_cols[0].metric("Height", bio.get("player_height", "N/A"))
            bio_cols[1].metric(
                "Weight",
                f"{bio.get('player_weight', 'N/A')} lbs"
                if bio.get("player_weight") else "N/A",
            )
            bio_cols[2].metric("Age", bio.get("age", "N/A"))
            bio_cols[3].metric("College", bio.get("college", "N/A"))
            bio_cols[4].metric("Country", bio.get("country", "N/A"))
            bio_cols[5].metric(
                "Experience",
                f"{bio.get('seasons')} yrs"
                if bio.get("seasons") is not None
                else "N/A",
            )
            bio_cols[6].metric("GP", bio.get("gp", "N/A"))
            bio_cols[7].metric(
                "USG%",
                f"{(bio.get('usg_pct') or 0):.1%}",
            )

        # Last 5 averages hero row
        if last5:
            avgs = last5.get("averages", {})
            st.markdown('<div class="section-hdr">Last 5 Games Average</div>',
                        unsafe_allow_html=True)
            a_cols = st.columns(8)
            stat_map = {
                "pts": "PTS", "reb": "REB", "ast": "AST", "blk": "BLK",
                "stl": "STL", "tov": "TOV", "fg_pct": "FG%",
                "plus_minus": "+/-",
            }
            for i, (k, lbl) in enumerate(stat_map.items()):
                val = avgs.get(k, 0.0)
                if k in ("fg_pct",):
                    a_cols[i].metric(lbl, f"{(val or 0):.1%}")
                else:
                    a_cols[i].metric(lbl, val)

        st.divider()

        # ── Detailed tabs ─────────────────────────────────────
        (p_t_last5, p_t_career, p_t_adv, p_t_scoring, p_t_usage,
         p_t_shots, p_t_tracking, p_t_clutch, p_t_hustle,
         p_t_matchups) = st.tabs([
            "📊 Last 5 Games",
            "📈 Career Stats",
            "🧠 Advanced",
            "🎯 Scoring",
            "⚡ Usage",
            "🏀 Shot Chart",
            "🏃 Tracking",
            "🔥 Clutch",
            "💪 Hustle",
            "⚔️ Matchups",
        ])

        with p_t_last5:
            if last5 and last5.get("games"):
                _show_df(last5["games"], [
                    "game_date", "matchup", "wl", "pts", "reb", "ast",
                    "blk", "stl", "tov", "fgm", "fga", "fg_pct",
                    "fg3m", "fg3a", "fg3_pct", "ftm", "fta", "ft_pct",
                    "oreb", "dreb", "pf", "plus_minus", "min",
                ])
            else:
                st.info("No recent game data.")

        with p_t_career:
            st.caption("Season-by-season totals across the player's entire NBA career.")
            career = get_player_career(pid)
            if career:
                _show_df(career, [
                    "season_id", "team_abbreviation", "player_age", "gp",
                    "gs", "min", "pts", "reb", "ast", "stl", "blk",
                    "tov", "fgm", "fga", "fg_pct", "fg3m", "fg3a",
                    "fg3_pct", "ftm", "fta", "ft_pct", "oreb", "dreb",
                    "pf",
                ])
            else:
                st.info("No career data.")

        with p_t_adv:
            with st.expander("ℹ️ What do advanced stats mean?", expanded=False):
                st.markdown("""
| Stat | Meaning |
|------|---------|
| **ORtg** | Offensive Rating — points produced per 100 possessions |
| **DRtg** | Defensive Rating — points allowed per 100 possessions (lower = better) |
| **Net Rtg** | ORtg − DRtg; positive = outscoring opponents while on court |
| **TS%** | True Shooting % — shooting efficiency including FT and 3PT |
| **eFG%** | Effective FG% — adjusts for 3PT being worth more |
| **USG%** | Usage Rate — % of team possessions used while on court |
| **AST%** | Assist % — % of teammate FGs assisted while on court |
| **TOV Ratio** | Turnovers per 100 possessions |
| **OREB% / DREB%** | Offensive / Defensive rebound % |
| **Pace** | Possessions per 48 minutes |
| **PIE** | Player Impact Estimate — overall contribution (higher = better) |
                """)
            adv = get_player_advanced(pid)
            if adv:
                _show_df(adv, [
                    "game_date", "matchup", "minutes", "off_rating",
                    "def_rating", "net_rating", "ts_pct", "efg_pct",
                    "usg_pct", "ast_pct", "oreb_pct", "dreb_pct",
                    "reb_pct", "tov_ratio", "pace", "pie",
                ])
            else:
                st.info("No advanced data.")

        with p_t_scoring:
            st.caption("How this player's points are distributed — 2PT vs 3PT, paint vs midrange, assisted vs unassisted.")
            scoring = get_player_scoring(pid)
            if scoring:
                _show_df(scoring, [
                    "game_date", "matchup", "minutes",
                    "pct_fga_2pt", "pct_fga_3pt", "pct_pts_2pt",
                    "pct_pts_3pt", "pct_pts_fast_break", "pct_pts_ft",
                    "pct_pts_paint", "pct_pts_off_tov",
                    "pct_assisted_fgm", "pct_unassisted_fgm",
                ])
            else:
                st.info("No scoring data.")

        with p_t_usage:
            st.caption("Usage shows what % of the team's actions this player is responsible for while on the court.")
            usage = get_player_usage(pid)
            if usage:
                _show_df(usage, [
                    "game_date", "matchup", "minutes", "usg_pct",
                    "pct_fgm", "pct_fga", "pct_fg3m", "pct_fg3a",
                    "pct_ftm", "pct_fta", "pct_oreb", "pct_dreb",
                    "pct_reb", "pct_ast", "pct_tov", "pct_stl",
                    "pct_blk", "pct_pts",
                ])
            else:
                st.info("No usage data.")

        with p_t_shots:
            st.caption("Every field goal attempt plotted by court location, zone, and distance.")
            shots = get_player_shot_chart(pid)
            if shots:
                df_shots = pd.DataFrame(shots)
                if "shot_zone_basic" in df_shots.columns:
                    st.markdown(
                        '<div class="section-hdr">Shot Zone Summary</div>',
                        unsafe_allow_html=True,
                    )
                    zone_summary = (
                        df_shots.groupby("shot_zone_basic")
                        .agg(
                            attempts=("shot_attempted_flag", "sum"),
                            makes=("shot_made_flag", "sum"),
                        )
                        .reset_index()
                    )
                    zone_summary["fg_pct"] = (
                        zone_summary["makes"] / zone_summary["attempts"]
                    ).round(3)
                    st.dataframe(
                        zone_summary.sort_values("attempts", ascending=False),
                        use_container_width=True,
                        hide_index=True,
                    )
                st.caption(f"Showing {len(shots)} shot attempts")
                _show_df(shots, [
                    "game_date", "period", "event_type", "action_type",
                    "shot_type", "shot_zone_basic", "shot_zone_area",
                    "shot_distance", "shot_made_flag", "loc_x", "loc_y",
                ], height=400)
            else:
                st.info("No shot chart data.")

        with p_t_tracking:
            st.caption("Player-tracking data from NBA cameras — speed, distance, touches, and shot contest rates.")
            tracking = get_player_tracking(pid)
            if tracking:
                _show_df(tracking, [
                    "game_date", "matchup", "minutes", "speed",
                    "distance", "touches", "passes", "assists",
                    "contested_fg_made", "contested_fg_attempted",
                    "contested_fg_pct", "uncontested_fg_made",
                    "uncontested_fg_attempted", "uncontested_fg_pct",
                    "defended_at_rim_fg_made",
                    "defended_at_rim_fg_attempted",
                    "defended_at_rim_fg_pct",
                ])
            else:
                st.info("No tracking data.")

        with p_t_clutch:
            st.caption("Performance in clutch time — the last 5 minutes when the score is within 5 points.")
            clutch = get_player_clutch(pid)
            if clutch:
                _show_df(clutch, [
                    "season", "team_abbreviation", "gp", "min", "pts",
                    "reb", "ast", "stl", "blk", "tov", "fg_pct",
                    "fg3_pct", "ft_pct", "plus_minus",
                ])
            else:
                st.info("No clutch data.")

        with p_t_hustle:
            st.caption("Effort plays that don't show up in traditional stats — deflections, loose balls, contested shots, screens.")
            hustle = get_player_hustle(pid)
            if hustle:
                _show_df(hustle, [
                    "season", "team_abbreviation", "gp", "min",
                    "contested_shots", "contested_shots_2pt",
                    "contested_shots_3pt", "deflections", "charges_drawn",
                    "screen_assists", "screen_ast_pts", "loose_balls",
                    "off_boxouts", "def_boxouts", "boxouts",
                ])
            else:
                st.info("No hustle data.")

        with p_t_matchups:
            st.caption("Head-to-head defensive matchup data — who guarded this player and how they performed.")
            matchups = get_player_matchups(pid)
            if matchups:
                _show_df(matchups, [
                    "game_date", "game_matchup", "defender_name",
                    "matchup_min", "partial_poss", "player_pts",
                    "matchup_fgm", "matchup_fga", "matchup_fg_pct",
                    "matchup_fg3m", "matchup_fg3a", "matchup_fg3_pct",
                    "matchup_ast", "matchup_tov", "matchup_blk",
                    "switches_on",
                ])
            else:
                st.info("No matchup data.")


# ─────────────────────────────────────────────────────────────────────────
# PAGE: STANDINGS
# ─────────────────────────────────────────────────────────────────────────

def _page_standings() -> None:
    st.title("🏆 League Standings")

    with st.expander("ℹ️ Understanding League Standings", expanded=False):
        st.markdown("""
**League Standings** show how every NBA team ranks in their conference.
The top 6 teams in each conference automatically qualify for the playoffs,
while seeds 7–10 compete in the **Play-In Tournament**.

| Column | Meaning |
|--------|---------|
| **Playoff Rank** | Team's seed in their conference (1 = best) |
| **W / L** | Wins and losses |
| **Win%** | Win percentage — wins ÷ total games played |
| **Home / Road** | Record at home vs. on the road (e.g. "25-5") |
| **L10** | Record over the last 10 games — shows recent form |
| **Streak** | Current winning or losing streak (e.g. "W3" = 3 straight wins) |
| **GB** | Games Back — how many games behind the conference leader |
| **PPG** | Points Per Game scored |
| **Opp PPG** | Opponent Points Per Game — points allowed |
| **Diff** | Point differential (PPG − Opp PPG); positive = outscoring opponents |
        """)

    standings_data = get_standings()
    if standings_data:
        east = [s for s in standings_data if s.get("conference") == "East"]
        west = [s for s in standings_data if s.get("conference") == "West"]

        col_e, col_w = st.columns(2)
        standing_cols = [
            "playoff_rank", "abbreviation", "team_name", "wins", "losses",
            "win_pct", "home", "road", "l10", "str_current_streak",
            "conference_games_back", "points_pg", "opp_points_pg",
            "diff_points_pg",
        ]

        with col_e:
            st.markdown("### 🔵 Eastern Conference")
            if east:
                _show_df(east, standing_cols, height=550)

        with col_w:
            st.markdown("### 🟠 Western Conference")
            if west:
                _show_df(west, standing_cols, height=550)

        st.divider()
        st.markdown("### 📋 Full Standings Detail")
        st.caption("Scroll right to see all columns including division records and vs-conference breakdowns.")
        _show_df(standings_data, height=500)
    else:
        st.info("No standings data available. Run a data sync to populate.")


# ─────────────────────────────────────────────────────────────────────────
# PAGE: TEAMS BROWSE
# ─────────────────────────────────────────────────────────────────────────

def _page_teams_browse() -> None:
    st.title("🏟️ Teams")

    all_teams = get_teams()
    if all_teams:
        st.caption("Select a team to view their roster, game stats, clutch/hustle metrics, synergy play types, and defense vs position.")
        # Group by conference
        east_teams = [t for t in all_teams if t.get("conference") == "East"]
        west_teams = [t for t in all_teams if t.get("conference") == "West"]

        ce, cw = st.columns(2)
        with ce:
            st.markdown("### 🔵 Eastern Conference")
            for t in east_teams:
                if st.button(
                    f"🏟️ {t['abbreviation']} — {t['team_name']}",
                    key=f"tb_e_{t['team_id']}",
                    use_container_width=True,
                ):
                    _nav("team_detail", selected_team_id=t["team_id"])
                    st.rerun()

        with cw:
            st.markdown("### 🟠 Western Conference")
            for t in west_teams:
                if st.button(
                    f"🏟️ {t['abbreviation']} — {t['team_name']}",
                    key=f"tb_w_{t['team_id']}",
                    use_container_width=True,
                ):
                    _nav("team_detail", selected_team_id=t["team_id"])
                    st.rerun()
    else:
        st.info("No teams loaded yet.")


# ─────────────────────────────────────────────────────────────────────────
# PAGE: TEAM DETAIL
# ─────────────────────────────────────────────────────────────────────────

def _page_team_detail() -> None:
    tid = st.session_state.selected_team_id

    if st.button("← Back to Teams", key="back_teams"):
        _nav("teams_browse")
        st.rerun()

    if not tid:
        st.warning("No team selected.")
    else:
        all_teams = get_teams()
        team_data = next((t for t in all_teams if t["team_id"] == tid), {})
        abbrev = team_data.get("abbreviation", "")

        st.title(f"🏟️ {abbrev} — {team_data.get('team_name', '')}")

        # Overview metrics
        ov = st.columns(5)
        ov[0].metric("Conference", team_data.get("conference", "N/A"))
        ov[1].metric("Division", team_data.get("division", "N/A"))
        ov[2].metric("Pace", team_data.get("pace", "N/A"))
        ov[3].metric("ORtg", team_data.get("ortg", "N/A"))
        ov[4].metric("DRtg", team_data.get("drtg", "N/A"))

        st.divider()

        (t_tab_roster, t_tab_games, t_tab_details,
         t_tab_clutch, t_tab_hustle, t_tab_metrics, t_tab_dvp) = st.tabs([
            "👥 Roster (click players)",
            "📊 Recent Games",
            "🏢 Details",
            "🔥 Clutch",
            "💪 Hustle",
            "📈 Metrics",
            "🛡️ Def vs Pos",
        ])

        with t_tab_roster:
            roster = get_team_roster(tid)
            if roster:
                for p in roster:
                    _player_button(
                        p["player_id"],
                        p.get("full_name", ""),
                        p.get("position"),
                        key_prefix=f"tr_{tid}",
                    )
            else:
                st.info("No roster data.")

        with t_tab_games:
            team_games = get_team_stats(tid, last_n=20)
            if team_games:
                _show_df(team_games, [
                    "game_date", "matchup", "points_scored",
                    "points_allowed", "pace_est", "ortg_est", "drtg_est",
                ])
            else:
                st.info("No game stats.")

        with t_tab_details:
            details = get_team_details(tid)
            if details:
                dc = st.columns(3)
                dc[0].metric("Arena", details.get("arena", "N/A"))
                dc[1].metric(
                    "Capacity",
                    f"{details['arena_capacity']:,}"
                    if details.get("arena_capacity") is not None else "N/A",
                )
                dc[2].metric("Founded", details.get("year_founded", "N/A"))
                dc2 = st.columns(3)
                dc2[0].metric("Coach", details.get("head_coach", "N/A"))
                dc2[1].metric("GM", details.get("general_manager", "N/A"))
                dc2[2].metric("Owner", details.get("owner", "N/A"))
            else:
                st.info("No team details.")

        with t_tab_clutch:
            st.caption("Team performance in clutch time — last 5 min when score is within 5 points.")
            t_clutch = get_team_clutch(tid)
            if t_clutch:
                _show_df(t_clutch, [
                    "season", "gp", "w", "l", "w_pct", "pts", "reb",
                    "ast", "stl", "blk", "tov", "fg_pct", "fg3_pct",
                    "ft_pct", "plus_minus",
                ])
            else:
                st.info("No clutch data.")

        with t_tab_hustle:
            st.caption("Effort metrics — deflections, contested shots, loose balls recovered, box-outs.")
            t_hustle = get_team_hustle(tid)
            if t_hustle:
                _show_df(t_hustle, [
                    "season", "contested_shots", "deflections",
                    "charges_drawn", "screen_assists", "loose_balls",
                    "off_boxouts", "def_boxouts", "boxouts",
                ])
            else:
                st.info("No hustle data.")

        with t_tab_metrics:
            st.caption("NBA's estimated advanced metrics — ORtg, DRtg, Net Rtg, Pace derived from league-wide tracking data.")
            t_metrics = get_team_estimated_metrics(tid)
            if t_metrics:
                _show_df(t_metrics, [
                    "season", "gp", "w", "l", "w_pct",
                    "e_off_rating", "e_def_rating", "e_net_rating",
                    "e_pace", "e_reb_pct", "e_tm_tov_pct",
                ])
            else:
                st.info("No estimated metrics.")

        with t_tab_dvp:
            if abbrev:
                dvp = get_defense_vs_position(abbrev)
                if dvp:
                    st.caption(
                        f"How **{abbrev}** defends each position. "
                        "Multiplier > 1.0 = weaker defense (allows more than avg). "
                        "< 1.0 = tougher defense (allows less than avg)."
                    )
                    _show_df(dvp, [
                        "pos", "vs_pts_mult", "vs_reb_mult",
                        "vs_ast_mult", "vs_stl_mult", "vs_blk_mult",
                        "vs_3pm_mult",
                    ])
                else:
                    st.info("No defense-vs-position data.")


# ─────────────────────────────────────────────────────────────────────────
# PAGE: LEADERS & STATS
# ─────────────────────────────────────────────────────────────────────────

def _page_leaders() -> None:
    st.title("📊 League Leaders & Season Stats")

    with st.expander("ℹ️ Understanding Leaders & Stats", expanded=False):
        st.markdown("""
This section gives you three views into NBA performance:

**🏅 League Leaders** — The top players ranked by overall efficiency.
The "EFF" (Efficiency) rating is a simple formula:
`(PTS + REB + AST + STL + BLK) − (Missed FG + Missed FT + TOV)`.
Higher = better overall production.

**👤 Season Player Stats** — Per-game averages for every player this season.
Use this to compare any two players head-to-head across all box-score stats.

**🏟️ Season Team Stats** — Per-game averages for every team this season.
Great for spotting the best offensive teams (high PTS), defensive teams
(low Opp PTS), or efficient shooting teams (high FG%).

| Key Stat | Meaning |
|----------|---------|
| **GP** | Games played |
| **MIN** | Minutes per game |
| **FG%** | Field goal percentage — overall shooting accuracy |
| **FG3%** | Three-point percentage |
| **FT%** | Free throw percentage |
| **+/−** | Plus-minus — team's net score while this player is on court |
| **Fantasy PTS** | NBA Fantasy points (standard scoring) |
| **DD2 / TD3** | Double-doubles / Triple-doubles this season |
        """)

    l_tab_leaders, l_tab_players, l_tab_teams = st.tabs([
        "🏅 League Leaders",
        "👤 Season Player Stats",
        "🏟️ Season Team Stats",
    ])

    with l_tab_leaders:
        st.caption("Top players ranked by efficiency. Click any name to open their full profile.")
        leaders = get_league_leaders()
        if leaders:
            _show_df(leaders, [
                "rank", "full_name", "position", "team_abbreviation",
                "gp", "min", "pts", "reb", "ast", "stl", "blk",
                "tov", "fg_pct", "fg3_pct", "ft_pct", "eff",
            ], height=600)
            # Clickable player list
            st.markdown('<div class="section-hdr">Click a player</div>',
                        unsafe_allow_html=True)
            for ldr in leaders[:25]:
                _player_button(
                    ldr.get("player_id"),
                    ldr.get("full_name", ""),
                    ldr.get("position"),
                    ldr.get("team_abbreviation"),
                    key_prefix="ldr",
                )
        else:
            st.info("No league leaders data.")

    with l_tab_players:
        st.caption(
            "Season per-game averages for every player. "
            "Sort by any column header to find leaders in a specific stat."
        )
        dash_players = get_league_dash_players()
        if dash_players:
            _show_df(dash_players, [
                "full_name", "position", "team_abbreviation", "season",
                "gp", "w", "l", "min", "pts", "reb", "ast", "stl",
                "blk", "tov", "fg_pct", "fg3_pct", "ft_pct",
                "plus_minus", "nba_fantasy_pts", "dd2", "td3",
            ], height=600)
        else:
            st.info("No season player stats.")

    with l_tab_teams:
        st.caption(
            "Season per-game averages for every team. "
            "Compare offensive and defensive performance across the league."
        )
        dash_teams = get_league_dash_teams()
        if dash_teams:
            _show_df(dash_teams, [
                "abbreviation", "team_name", "season", "gp", "w", "l",
                "w_pct", "pts", "reb", "ast", "stl", "blk", "tov",
                "fg_pct", "fg3_pct", "ft_pct", "plus_minus",
            ], height=600)
        else:
            st.info("No season team stats.")


# ─────────────────────────────────────────────────────────────────────────
# PAGE: DEFENSE VS POSITION
# ─────────────────────────────────────────────────────────────────────────

def _page_defense() -> None:
    st.title("🛡️ Defense vs Position")

    with st.expander("ℹ️ Understanding Defense vs Position", expanded=False):
        st.markdown("""
**Defense vs Position (DVP)** reveals how well each team defends against
players at each position (PG, SG, SF, PF, C).  This is one of the most
valuable tools for **fantasy basketball**, **DFS**, and **betting props**.

### How to read the multipliers

Every stat gets a **multiplier** relative to the league average:

| Multiplier | Meaning | Example |
|------------|---------|---------|
| **1.00** | League average — no advantage or disadvantage | — |
| **> 1.00** | Team allows **more** than average (weaker defense) | 1.15 = allows 15% more |
| **< 1.00** | Team allows **less** than average (tougher defense) | 0.85 = allows 15% less |

### Stat columns explained

| Column | Stat | What it tells you |
|--------|------|-------------------|
| **vs_pts_mult** | Points | How many points this position scores against them |
| **vs_reb_mult** | Rebounds | How many rebounds this position grabs against them |
| **vs_ast_mult** | Assists | How many assists this position records against them |
| **vs_stl_mult** | Steals | How many steals this position gets against them |
| **vs_blk_mult** | Blocks | How many blocks this position gets against them |
| **vs_3pm_mult** | 3-Pointers Made | How many threes this position makes against them |

### 💡 How to use this

**Example:** If Boston has a `vs_pts_mult` of **1.20** for the **PG**
position, that means point guards score **20% more** against Boston than the
league average.  A PG averaging 20 PPG would be projected for ~24 PPG vs
Boston.

**Look for multipliers > 1.10** to find favourable matchups, and
**< 0.90** to identify tough matchups to avoid.
        """)

    dvp_teams = get_teams()
    if dvp_teams:
        # ── Position filter at the top ────────────────────────
        pos_filter = st.selectbox(
            "Filter by position",
            options=["All Positions", "PG", "SG", "SF", "PF", "C"],
            key="dvp_pos_filter",
        )

        selected_dvp = st.selectbox(
            "Select a team (or All Teams)",
            options=["All Teams"] + [
                t["abbreviation"] for t in dvp_teams
            ],
            key="dvp_select",
        )

        display_cols = [
            "team", "pos", "vs_pts_mult", "vs_reb_mult",
            "vs_ast_mult", "vs_stl_mult", "vs_blk_mult",
            "vs_3pm_mult",
        ]

        if selected_dvp == "All Teams":
            all_dvp = []
            for t in dvp_teams:
                positions = get_defense_vs_position(t["abbreviation"])
                for p in positions:
                    p["team"] = t["abbreviation"]
                    all_dvp.append(p)
            if all_dvp:
                df_dvp = pd.DataFrame(all_dvp)
                if pos_filter != "All Positions":
                    df_dvp = df_dvp[df_dvp["pos"] == pos_filter]

                if not df_dvp.empty:
                    # Summary: best & worst matchups
                    st.markdown('<div class="section-hdr">Quick Insights</div>',
                                unsafe_allow_html=True)
                    for stat, label in [
                        ("vs_pts_mult", "Points"),
                        ("vs_reb_mult", "Rebounds"),
                        ("vs_ast_mult", "Assists"),
                        ("vs_3pm_mult", "3-Pointers"),
                    ]:
                        if stat in df_dvp.columns and df_dvp[stat].notna().any():
                            valid = df_dvp[df_dvp[stat].notna()]
                            best = valid.loc[valid[stat].idxmax()]
                            worst = valid.loc[valid[stat].idxmin()]
                            c1, c2 = st.columns(2)
                            c1.metric(
                                f"🟢 Easiest for {label}",
                                f"{best['team']} vs {best['pos']}",
                                f"{best[stat]:.2f}x",
                            )
                            c2.metric(
                                f"🔴 Toughest for {label}",
                                f"{worst['team']} vs {worst['pos']}",
                                f"{worst[stat]:.2f}x",
                                delta_color="inverse",
                            )

                    st.divider()
                    st.markdown('<div class="section-hdr">Full Table</div>',
                                unsafe_allow_html=True)
                    st.caption(
                        "Sort by any column to find the best/worst matchups. "
                        "🟢 > 1.0 = weaker defense (good matchup)  ·  "
                        "🔴 < 1.0 = tougher defense (bad matchup)"
                    )
                    avail_cols = [c for c in display_cols if c in df_dvp.columns]
                    _show_df(df_dvp[avail_cols].to_dict("records"), avail_cols, height=600)
                else:
                    st.info("No data for the selected position.")
            else:
                st.info("No defense-vs-position data available.")
        else:
            positions = get_defense_vs_position(selected_dvp)
            if positions:
                if pos_filter != "All Positions":
                    positions = [p for p in positions if p.get("pos") == pos_filter]
                if positions:
                    st.caption(
                        f"**{selected_dvp}** defense multipliers by position. "
                        "Values > 1.0 = allows more than average (weaker). "
                        "Values < 1.0 = allows less (tougher)."
                    )
                    single_cols = [
                        "pos", "vs_pts_mult", "vs_reb_mult",
                        "vs_ast_mult", "vs_stl_mult", "vs_blk_mult",
                        "vs_3pm_mult",
                    ]
                    _show_df(positions, single_cols)
                else:
                    st.info("No data for the selected position.")
            else:
                st.info("No data for this team.")
    else:
        st.info("No teams loaded.")


# ─────────────────────────────────────────────────────────────────────────
# PAGE: SCHEDULE
# ─────────────────────────────────────────────────────────────────────────

def _page_more() -> None:
    st.title("🗓️ NBA Schedule")

    schedule = get_schedule()
    if schedule:
        _show_df(schedule, [
            "game_date", "game_status_text", "home_team_tricode",
            "away_team_tricode", "home_team_score", "away_team_score",
            "arena_name", "arena_city", "game_id",
        ], height=600)
    else:
        st.info("No schedule data.")


# ═══════════════════════════════════════════════════════════════════════════
# Prop Analyzer page  (engine-powered)
# ═══════════════════════════════════════════════════════════════════════════


_ANALYSIS_STAT_TYPES: list[str] = [
    "points", "rebounds", "assists", "threes",
    "steals", "blocks", "turnovers",
]

_TIER_COLORS: dict[str, str] = {
    "Platinum": "#E5E4E2",
    "Gold": "#FFD700",
    "Silver": "#C0C0C0",
    "Bronze": "#CD7F32",
    "Avoid": "#FF4444",
}


def _page_prop_analyzer() -> None:
    """Interactive prop-analysis page powered by the engine modules."""

    st.title("🎯 Prop Analyzer")
    st.caption(
        "Enter a player prop to get an AI-powered analysis with projection, "
        "simulation, edge detection, and confidence scoring."
    )

    # ── Sidebar inputs ──────────────────────────────────────────────
    with st.sidebar:
        st.subheader("🔍 Prop Setup")

        # Player search
        query = st.text_input("Search player", key="prop_player_search")
        selected_player: dict | None = None
        if query and len(query) >= 2:
            results = search_players(query)
            if results:
                options = {
                    f"{p.get('full_name', p.get('first_name', '') + ' ' + p.get('last_name', ''))} "
                    f"({p.get('team_abbreviation', '???')})": p
                    for p in results[:MAX_SEARCH_RESULTS]
                }
                choice = st.selectbox("Select player", list(options.keys()), key="prop_player_select")
                selected_player = options.get(choice)
            else:
                st.info("No players found.")

        stat_type = st.selectbox("Stat type", _ANALYSIS_STAT_TYPES, key="prop_stat_type")
        prop_line = st.number_input("Prop line", min_value=0.5, value=20.5, step=0.5, key="prop_line_input")

        st.divider()
        st.subheader("⚙️ Game Context")
        opponent = st.text_input(
            "Opponent (abbrev, e.g. BOS)",
            key="prop_opponent",
            help="Leave blank to auto-detect from today's schedule.",
        ).strip().upper() or None
        vegas_spread = st.number_input("Vegas spread", value=0.0, step=0.5, key="prop_spread")
        game_total = st.number_input("Game total (O/U)", value=220.0, step=0.5, key="prop_total")
        platform = st.selectbox(
            "Platform",
            ["prizepicks", "underdog", "draftkings", "fanduel"],
            key="prop_platform",
        )

        run_analysis = st.button("🚀 Analyze Prop", type="primary", use_container_width=True)

    # ── Main content area ───────────────────────────────────────────
    if not run_analysis:
        st.info("👈 Configure a prop in the sidebar and click **Analyze Prop** to begin.")
        return

    if not selected_player:
        st.warning("Please search for and select a player first.")
        return

    player_id = selected_player.get("player_id")
    if not player_id:
        st.error("Selected player has no ID.")
        return

    player_name = (
        selected_player.get("full_name")
        or f"{selected_player.get('first_name', '')} {selected_player.get('last_name', '')}".strip()
    )

    with st.spinner(f"Analyzing {player_name} — {stat_type} {prop_line}…"):
        result = analyze_prop(
            player_id=int(player_id),
            stat_type=stat_type,
            prop_line=float(prop_line),
            opponent=opponent,
            vegas_spread=float(vegas_spread),
            game_total=float(game_total),
            platform=platform,
        )

    if result.get("status") == "error":
        st.error(f"Analysis failed: {result.get('message', 'Unknown error')}")
        return

    if "confidence" not in result:
        st.error("Unexpected response from the analysis engine.")
        return

    # ── Auto-log to bet tracker ──────────────────────────────
    try:
        auto_log_analysis_bets(result, platform=platform)
    except Exception:
        pass  # Never block UI for tracking errors

    # ── Extract core data ───────────────────────────────────────────
    conf = result.get("confidence", {})
    tier = conf.get("tier", "Bronze")
    tier_emoji = conf.get("tier_emoji", "🥉")
    score = conf.get("confidence_score", 0)
    direction = result.get("direction", "OVER")
    model_prob = result.get("model_probability", 0.5)
    edge = result.get("edge_pct", 0.0)
    explanation = result.get("explanation", {})
    proj = result.get("projection", {})
    sim = result.get("simulation", {})
    forces = result.get("forces", {})
    bankroll = result.get("bankroll", {})
    regime = result.get("regime", {})
    kelly_frac = bankroll.get("kelly_fraction", 0.0)
    regime_changed = regime.get("regime_changed", False)
    regime_dir = regime.get("direction", "stable")
    team_abbrev = result.get("team", "???")
    opp_abbrev = result.get("opponent", "???")

    # ── Player Result Card ──────────────────────────────────────────
    st.markdown('<div class="player-result-card">', unsafe_allow_html=True)

    # Card header: player name, matchup info, tier badge
    dir_icon = "🟢" if direction == "OVER" else "🔴"
    st.markdown(
        f"""<div class="player-card-header">
            <div>
                <div class="player-card-name">{tier_emoji} {player_name}</div>
                <div class="player-card-meta">
                    {team_abbrev} vs {opp_abbrev}  ·  {stat_type.upper()} {direction} {prop_line}  ·  {platform.title()}
                </div>
            </div>
            <div class="player-card-tier">
                {get_tier_badge_html(tier)}
            </div>
        </div>""",
        unsafe_allow_html=True,
    )

    st.markdown('<div class="player-card-body">', unsafe_allow_html=True)

    # ── Joseph M Smith's Verdict ────────────────────────────────────
    verdict_text = explanation.get("verdict", "")
    tldr = explanation.get("tldr", "")
    if verdict_text:
        st.markdown(get_verdict_banner_html(verdict_text), unsafe_allow_html=True)
    elif tldr:
        st.markdown(get_verdict_banner_html(tldr), unsafe_allow_html=True)

    # ── Quick Metrics Row ───────────────────────────────────────────
    qm = st.columns([1, 1, 1, 1, 1])
    qm[0].metric("Confidence", f"{score:.0f}/100", delta=tier)
    qm[1].metric("Win Prob", f"{model_prob:.1%}")
    qm[2].metric("Edge", f"{edge:+.1f}%")
    qm[3].metric("Direction", f"{dir_icon} {direction}")
    kelly_pct = bankroll.get("recommended_pct", "0.00%")
    qm[4].metric("Kelly Size", kelly_pct)

    # ── Tabbed Sections ─────────────────────────────────────────────
    tab_info, tab_pred, tab_bet = st.tabs([
        "📋 Player Info & Stats",
        "🔮 Predictions & Analysis",
        "💰 Bet Sizing & Verdict",
    ])

    # ══════════════════════════════════════════════════════════════
    # TAB 1: Player Info & Stats
    # ══════════════════════════════════════════════════════════════
    with tab_info:
        # Matchup summary
        st.subheader(f"🏀 {player_name} — {team_abbrev} vs {opp_abbrev}")

        info_c1, info_c2 = st.columns(2)

        with info_c1:
            st.markdown("**Projection Factors**")
            proj_metrics = {
                "Projected Points": proj.get("projected_points"),
                "Projected Rebounds": proj.get("projected_rebounds"),
                "Projected Assists": proj.get("projected_assists"),
                "Projected Threes": proj.get("projected_threes"),
                "Projected Steals": proj.get("projected_steals"),
                "Projected Blocks": proj.get("projected_blocks"),
            }
            for label, val in proj_metrics.items():
                if val is not None:
                    st.text(f"  {label}: {val}")

        with info_c2:
            st.markdown("**Context Adjustments**")
            ctx_metrics = {
                "Pace Factor": proj.get("pace_factor"),
                "Defense Factor": proj.get("defense_factor"),
                "Home/Away Factor": proj.get("home_away_factor"),
                "Rest Factor": proj.get("rest_factor"),
                "Blowout Risk": proj.get("blowout_risk"),
            }
            for label, val in ctx_metrics.items():
                if val is not None:
                    st.text(f"  {label}: {val}")

        # Matchup History
        matchup = result.get("matchup_history", {})
        if matchup and "error" not in matchup:
            st.divider()
            st.markdown("**Matchup History**")
            if matchup.get("cold_start"):
                st.info(
                    f"Fewer than 5 games vs {opp_abbrev} — matchup adjustment is neutral."
                )
            else:
                mh_cols = st.columns([1, 1, 1, 1])
                avg_vs = matchup.get("avg_vs_team")
                mh_cols[0].metric(
                    "Avg vs Team",
                    f"{avg_vs:.1f}" if avg_vs is not None else "N/A",
                )
                mh_cols[1].metric("Games Found", matchup.get("games_found", 0))
                fav = matchup.get("matchup_favorability_score", 50)
                mh_cols[2].metric(
                    "Favorability",
                    f"{fav:.0f}/100",
                    delta="Favorable" if fav > 55 else ("Unfavorable" if fav < 45 else "Neutral"),
                    delta_color="normal" if fav > 55 else ("inverse" if fav < 45 else "off"),
                )
                adj = matchup.get("adjustment_factor", 1.0)
                mh_cols[3].metric(
                    "Projection Adj",
                    f"{adj:.2f}x",
                    delta=f"{(adj - 1) * 100:+.1f}%" if adj != 1.0 else "None",
                    delta_color="normal" if adj > 1.0 else ("inverse" if adj < 1.0 else "off"),
                )

        # Rotation / Minutes
        rotation_data = result.get("rotation", {})
        if rotation_data and "error" not in rotation_data:
            st.divider()
            st.markdown("**Minutes & Rotation**")
            rt_cols = st.columns([1, 1, 1])
            min_adj = rotation_data.get("minutes_adjustment", 1.0)
            rt_cols[0].metric(
                "Minutes Adj",
                f"{min_adj:.2f}x",
                delta=f"{(min_adj - 1) * 100:+.1f}%" if min_adj != 1.0 else "Stable",
                delta_color="normal" if min_adj > 1.0 else ("inverse" if min_adj < 1.0 else "off"),
            )
            role_changed = rotation_data.get("role_change_detected", False)
            if role_changed:
                change_type = rotation_data.get("change_type", "none").replace("_", " → ").title()
                rt_cols[1].metric("Role Change", f"⚠️ {change_type}")
                rt_cols[2].metric(
                    "Minutes Shift",
                    f"{rotation_data.get('change_magnitude', 0):+.1f} min",
                    delta=f"{rotation_data.get('minutes_before', 0):.0f} → "
                          f"{rotation_data.get('minutes_after', 0):.0f}",
                )
            else:
                rt_cols[1].metric("Role Change", "✅ None")
                rt_cols[2].metric("Status", "Stable rotation")

        # Player Efficiency
        eff = result.get("efficiency", {})
        if eff and "error" not in eff:
            st.divider()
            st.markdown("**Player Efficiency Profile**")
            eff_cols = st.columns([1, 1, 1, 1])
            eff_cols[0].metric("True Shooting", f"{eff.get('ts_pct', 0):.1%}")
            eff_cols[1].metric("eFG%", f"{eff.get('efg_pct', 0):.1%}")
            eff_cols[2].metric("Usage Rate", f"{eff.get('usage_rate', 0):.1f}%")
            eff_cols[3].metric("Efficiency Tier", eff.get("efficiency_tier", "N/A"))

            epm = eff.get("estimated_epm", {})
            raptor = eff.get("estimated_raptor", {})
            if epm or raptor:
                adv_cols = st.columns([1, 1, 1, 1])
                if epm:
                    adv_cols[0].metric("EPM Total", f"{epm.get('total', 0):+.1f}")
                    adv_cols[1].metric("EPM Percentile", f"{epm.get('percentile', 50):.0f}th")
                if raptor:
                    adv_cols[2].metric("RAPTOR Total", f"{raptor.get('raptor_total', 0):+.1f}")
                    adv_cols[3].metric("Est. WAR", f"{raptor.get('war', 0):.1f}")

    # ══════════════════════════════════════════════════════════════
    # TAB 2: Predictions & Analysis
    # ══════════════════════════════════════════════════════════════
    with tab_pred:
        # TL;DR
        if tldr:
            st.info(f"**TL;DR:** {tldr}")

        # Simulation
        st.subheader("🎲 Simulation Results")
        sim_cols = st.columns([1, 1, 1, 1])
        sim_cols[0].metric("Simulated Mean", f"{sim.get('simulated_mean', 0):.1f}")
        sim_cols[1].metric("P(Over)", f"{sim.get('probability_over', 0):.1%}")
        sim_cols[2].metric(
            "90% CI",
            f"{sim.get('ci_90_low', 0):.1f} – {sim.get('ci_90_high', 0):.1f}",
        )
        sim_cols[3].metric("Sims Run", sim.get("simulations_run", 0))

        sim_detail_cols = st.columns([1, 1, 1])
        sim_detail_cols[0].metric("10th %ile", f"{sim.get('percentile_10', 0):.1f}")
        sim_detail_cols[1].metric("50th %ile", f"{sim.get('percentile_50', 0):.1f}")
        sim_detail_cols[2].metric("90th %ile", f"{sim.get('percentile_90', 0):.1f}")

        st.divider()

        # Directional Forces
        st.subheader("⚡ Directional Forces")
        over_forces = forces.get("over_forces", [])
        under_forces = forces.get("under_forces", [])

        force_c1, force_c2 = st.columns(2)
        with force_c1:
            st.markdown("**🟢 OVER Forces**")
            if over_forces:
                for f in over_forces:
                    name = f.get("name", f.get("force_name", "Unknown"))
                    strength = f.get("strength", f.get("magnitude", 0))
                    st.text(f"  ↑ {name}: {strength:.2f}")
            else:
                st.caption("No OVER forces detected.")

        with force_c2:
            st.markdown("**🔴 UNDER Forces**")
            if under_forces:
                for f in under_forces:
                    name = f.get("name", f.get("force_name", "Unknown"))
                    strength = f.get("strength", f.get("magnitude", 0))
                    st.text(f"  ↓ {name}: {strength:.2f}")
            else:
                st.caption("No UNDER forces detected.")

        # Game Script
        game_script = result.get("game_script", {})
        if game_script and "error" not in game_script:
            st.divider()
            st.subheader("🎬 Game Script Simulation")
            gs_cols = st.columns([1, 1, 1, 1])
            gs_cols[0].metric(
                "Blended Mean",
                f"{game_script.get('blended_mean', 0):.1f}",
                delta=f"Script: {game_script.get('game_script_mean', 0):.1f}",
            )
            gs_cols[1].metric("Flat Mean", f"{game_script.get('flat_mean', 0):.1f}")
            gs_cols[2].metric("Blowout Rate", f"{game_script.get('blowout_game_rate', 0):.1%}")
            gs_cols[3].metric("Player Tier", game_script.get("player_tier", "rotation").title())
            st.caption(
                f"Blend: {game_script.get('blend_weight', 0.3):.0%} game-script "
                f"+ {1 - game_script.get('blend_weight', 0.3):.0%} flat simulation."
            )

        # Distribution Cross-Check
        dist_check = result.get("distribution_check", {})
        if dist_check and "error" not in dist_check:
            st.divider()
            st.subheader("📐 Distribution Cross-Check")
            dc_cols = st.columns([1, 1, 1])
            dc_cols[0].metric(
                "Analytical P(Over)",
                f"{dist_check.get('analytical_probability', 0):.1%}",
            )
            dc_cols[1].metric(
                "Monte Carlo P(Over)",
                f"{dist_check.get('monte_carlo_probability', 0):.1%}",
            )
            delta_val = dist_check.get("delta", 0)
            dc_cols[2].metric(
                "Delta",
                f"{delta_val:.1%}",
                delta="Agreement" if delta_val < 0.05 else "Divergence",
                delta_color="off" if delta_val < 0.05 else "inverse",
            )
            if delta_val >= 0.05:
                st.caption(
                    "⚠️ Analytical and Monte Carlo probabilities differ by ≥5%.  "
                    "This may indicate unusual stat distribution or high variance."
                )

        # Full Explanation
        with st.expander("📝 Full Explanation", expanded=False):
            for key in [
                "average_vs_line", "matchup_explanation", "pace_explanation",
                "home_away_explanation", "rest_explanation", "vegas_explanation",
                "projection_explanation", "simulation_narrative", "forces_summary",
                "recent_form_explanation", "verdict",
            ]:
                text = explanation.get(key)
                if text:
                    st.markdown(f"**{key.replace('_', ' ').title()}:** {text}")

    # ══════════════════════════════════════════════════════════════
    # TAB 3: Bet Sizing & Verdict
    # ══════════════════════════════════════════════════════════════
    with tab_bet:
        # Joseph M Smith's verdict (prominent in this tab)
        if verdict_text:
            st.markdown(get_verdict_banner_html(verdict_text), unsafe_allow_html=True)

        st.subheader("💰 Bankroll & Sizing")
        bet_cols = st.columns([1, 1, 1, 1])
        bet_cols[0].metric(
            "Kelly Sizing",
            bankroll.get("recommended_pct", "0.00%"),
            delta=bankroll.get("kelly_mode", "quarter").title(),
        )
        regime_label = f"{'⚠️ ' if regime_changed else '✅ '}{regime_dir.title()}"
        bet_cols[1].metric(
            "Regime",
            regime_label,
            delta=f"Magnitude: {regime.get('magnitude', 0.0):.1f}" if regime_changed else "Stable",
            delta_color="off" if not regime_changed else ("normal" if regime_dir == "up" else "inverse"),
        )
        bet_cols[2].metric(
            "Payout",
            f"{bankroll.get('payout_multiplier', 1.909):.3f}x",
        )
        if kelly_frac > 0:
            example_bet = round(kelly_frac * EXAMPLE_BANKROLL, 2)
            bet_cols[3].metric(f"${EXAMPLE_BANKROLL:.0f} Bankroll →", f"${example_bet:.2f}")
        else:
            bet_cols[3].metric(f"${EXAMPLE_BANKROLL:.0f} Bankroll →", "No bet")

        # Risk factors
        risk_factors = explanation.get("risk_factors", [])
        if risk_factors:
            st.divider()
            st.subheader("⚠️ Risk Factors")
            for rf in risk_factors:
                st.warning(rf)

        avoid_reasons = conf.get("avoid_reasons", [])
        if avoid_reasons:
            st.error("**Avoid Reasons:** " + " • ".join(avoid_reasons))

        # Save Pick button
        st.divider()
        if st.button("💾 Save Pick", key="save_pick_btn", use_container_width=True):
            save_data = {
                "player_id": int(player_id),
                "player_name": player_name,
                "team": result.get("team", ""),
                "opponent": result.get("opponent", ""),
                "stat_type": stat_type,
                "prop_line": float(prop_line),
                "direction": direction,
                "model_probability": model_prob,
                "edge_pct": edge,
                "confidence_score": score,
                "tier": tier,
                "kelly_fraction": kelly_frac,
                "recommended_bet": round(kelly_frac * EXAMPLE_BANKROLL, 2),
                "regime_flag": regime_dir,
                "platform": platform,
                "vegas_spread": float(vegas_spread),
                "game_total": float(game_total),
            }
            save_result = save_pick(save_data)
            if save_result.get("status") == "saved":
                st.success(f"✅ Pick saved (ID: {save_result.get('pick_id')})")
            else:
                st.error(f"Failed to save: {save_result.get('message', 'Unknown error')}")

    # Close the card divs
    st.markdown('</div></div>', unsafe_allow_html=True)


# ═══════════════════════════════════════════════════════════════════════════
# Pick History page (Phase 3)
# ═══════════════════════════════════════════════════════════════════════════

_TIER_EMOJI: dict[str, str] = {
    "Platinum": "💎",
    "Gold": "🥇",
    "Silver": "🥈",
    "Bronze": "🥉",
    "Avoid": "⛔",
}

_RESULT_EMOJI: dict[str, str] = {
    "hit": "✅",
    "miss": "❌",
    "push": "➖",
}


def _page_pick_history() -> None:
    """Display saved picks with performance tracking."""

    st.title("📋 Pick History")
    st.caption("Track your saved prop analyses and record outcomes.")

    picks = get_pick_history(limit=100)

    if not picks:
        st.info("No saved picks yet.  Use the **Prop Analyzer** to analyze and save picks.")
        return

    # ── Summary stats ───────────────────────────────────────────────
    total = len(picks)
    hits = sum(1 for p in picks if p.get("result") == "hit")
    misses = sum(1 for p in picks if p.get("result") == "miss")
    pushes = sum(1 for p in picks if p.get("result") == "push")
    pending = total - hits - misses - pushes
    decided = hits + misses
    win_rate = (hits / decided * 100) if decided > 0 else 0.0

    sum_cols = st.columns(6)
    sum_cols[0].metric("Total Picks", total)
    sum_cols[1].metric("Hits", f"✅ {hits}")
    sum_cols[2].metric("Misses", f"❌ {misses}")
    sum_cols[3].metric("Pushes", f"➖ {pushes}")
    sum_cols[4].metric("Pending", f"⏳ {pending}")
    sum_cols[5].metric("Win Rate", f"{win_rate:.1f}%" if decided > 0 else "N/A")

    # ── Tier breakdown ──────────────────────────────────────────────
    if decided > 0:
        st.divider()
        st.subheader("📊 Performance by Tier")
        tiers_seen: dict[str, dict[str, int]] = {}
        for p in picks:
            t = p.get("tier", "Bronze")
            if t not in tiers_seen:
                tiers_seen[t] = {"hits": 0, "misses": 0, "total": 0}
            tiers_seen[t]["total"] += 1
            if p.get("result") == "hit":
                tiers_seen[t]["hits"] += 1
            elif p.get("result") == "miss":
                tiers_seen[t]["misses"] += 1

        tier_cols = st.columns(min(len(tiers_seen), 5))
        sorted_tiers = sorted(
            tiers_seen.items(),
            key=lambda x: -x[1].get("total", 0),
        )
        for i, (tier_name, counts) in enumerate(sorted_tiers):
            tier_decided = counts["hits"] + counts["misses"]
            tier_wr = (counts["hits"] / tier_decided * 100) if tier_decided > 0 else 0.0
            emoji = _TIER_EMOJI.get(tier_name, "🥉")
            tier_cols[i % len(tier_cols)].metric(
                f"{emoji} {tier_name}",
                f"{tier_wr:.0f}% WR" if tier_decided > 0 else "N/A",
                delta=f"{counts['hits']}/{tier_decided} decided" if tier_decided > 0 else f"{counts['total']} pending",
            )

    st.divider()

    # ── Pick table ──────────────────────────────────────────────────
    st.subheader("🗂️ All Picks")

    for pick in picks:
        pick_id = pick.get("pick_id", "?")
        p_name = pick.get("player_name", "Unknown")
        p_tier = pick.get("tier", "Bronze")
        p_dir = pick.get("direction", "?")
        p_stat = pick.get("stat_type", "?")
        p_line = pick.get("prop_line", 0)
        p_conf = pick.get("confidence_score", 0)
        p_edge = pick.get("edge_pct", 0)
        p_kelly = pick.get("kelly_fraction", 0)
        p_result = pick.get("result")
        p_date = pick.get("pick_date", "?")
        p_opp = pick.get("opponent", "?")
        p_regime = pick.get("regime_flag", "stable")

        tier_emoji = _TIER_EMOJI.get(p_tier, "🥉")
        result_emoji = _RESULT_EMOJI.get(p_result, "⏳") if p_result else "⏳"
        dir_icon = "🟢" if p_dir == "OVER" else "🔴"

        with st.expander(
            f"{result_emoji} {tier_emoji} **{p_name}** — {p_stat.upper()} "
            f"{dir_icon} {p_dir} {p_line}  •  vs {p_opp}  •  {p_date}",
            expanded=False,
        ):
            det_cols = st.columns([1, 1, 1, 1, 1])
            det_cols[0].metric("Confidence", f"{p_conf:.0f}/100")
            det_cols[1].metric("Edge", f"{p_edge:+.1f}%")
            det_cols[2].metric("Kelly", f"{p_kelly * 100:.2f}%")
            det_cols[3].metric("Regime", p_regime.title())
            det_cols[4].metric("Platform", pick.get("platform", "?").title())

            if not p_result:
                st.caption("Record outcome:")
                res_cols = st.columns(4)
                if res_cols[0].button("✅ Hit", key=f"hit_{pick_id}"):
                    update_pick_result(int(pick_id), "hit")
                    st.rerun()
                if res_cols[1].button("❌ Miss", key=f"miss_{pick_id}"):
                    update_pick_result(int(pick_id), "miss")
                    st.rerun()
                if res_cols[2].button("➖ Push", key=f"push_{pick_id}"):
                    update_pick_result(int(pick_id), "push")
                    st.rerun()
            else:
                actual = pick.get("actual_value")
                if actual is not None:
                    st.text(f"  Actual value: {actual}")


# ─────────────────────────────────────────────────────────────────────────
# PAGE: BET TRACKER
# ─────────────────────────────────────────────────────────────────────────

_TRACKER_STAT_TYPES = [
    "points", "rebounds", "assists", "threes",
    "steals", "blocks", "turnovers",
    "pts+reb", "pts+ast", "reb+ast", "pts+reb+ast",
]


def _page_bet_tracker() -> None:
    """Full bet tracker with summary, logging, and results management."""
    st.title("📈 Bet Tracker")
    st.caption("Track model performance • Log bets • Record results")

    # ── Summary Cards ─────────────────────────────────────────
    stats = get_model_performance_stats()
    summary = stats.get("summary", {})
    st.markdown(
        get_summary_cards_html(
            total_bets=summary.get("total_bets", 0),
            wins=summary.get("wins", 0),
            losses=summary.get("losses", 0),
            pushes=summary.get("pushes", 0),
            pending=summary.get("pending", 0),
            win_rate=summary.get("win_rate", 0.0),
        ),
        unsafe_allow_html=True,
    )

    # ── Tabs ──────────────────────────────────────────────────
    tab_bets, tab_log, tab_perf = st.tabs([
        "📋 My Bets", "➕ Log Bet", "📊 Performance",
    ])

    # ── Tab: My Bets ──────────────────────────────────────────
    with tab_bets:
        bets = load_all_bets(limit=100)
        if not bets:
            st.info("No bets logged yet. Use the **Log Bet** tab or run the **Prop Analyzer**.")
        else:
            for bet in bets:
                bid = bet.get("bet_id", "?")
                name = bet.get("player_name", "Unknown")
                stat = bet.get("stat_type", "?")
                line = bet.get("prop_line", 0)
                direction = bet.get("direction", "?")
                tier = bet.get("confidence_tier", "")
                score = bet.get("confidence_score", 0)
                edge = bet.get("edge_pct", 0)
                result_val = bet.get("result")
                bet_date = bet.get("bet_date", "?")
                opp = bet.get("opponent", "?")
                plat = bet.get("platform", "?")
                src = bet.get("source", "manual")

                dir_icon = "🟢" if direction == "OVER" else "🔴"
                tier_emoji = {"Platinum": "💎", "Gold": "🥇", "Silver": "🥈", "Bronze": "🥉"}.get(tier, "")
                result_emoji = {"win": "✅", "loss": "❌", "push": "➖"}.get(result_val, "⏳")

                with st.expander(
                    f"{result_emoji} {tier_emoji} **{name}** — {stat.upper()} "
                    f"{dir_icon} {direction} {line}  •  vs {opp}  •  {bet_date}",
                    expanded=False,
                ):
                    det = st.columns([1, 1, 1, 1, 1])
                    det[0].metric("Confidence", f"{score:.0f}/100")
                    det[1].metric("Edge", f"{edge:+.1f}%")
                    det[2].metric("Platform", plat)
                    det[3].metric("Tier", tier or "—")
                    det[4].metric("Source", src.title())

                    if not result_val:
                        st.caption("Record outcome:")
                        res = st.columns(4)
                        if res[0].button("✅ Win", key=f"bt_win_{bid}"):
                            record_bet_result(int(bid), "win")
                            st.rerun()
                        if res[1].button("❌ Loss", key=f"bt_loss_{bid}"):
                            record_bet_result(int(bid), "loss")
                            st.rerun()
                        if res[2].button("➖ Push", key=f"bt_push_{bid}"):
                            record_bet_result(int(bid), "push")
                            st.rerun()
                    else:
                        actual = bet.get("actual_value")
                        if actual is not None:
                            st.text(f"  Actual value: {actual}")

    # ── Tab: Log Bet ──────────────────────────────────────────
    with tab_log:
        st.subheader("➕ Log a New Bet")
        with st.form("log_bet_form", clear_on_submit=True):
            fc1, fc2 = st.columns(2)
            with fc1:
                lb_player = st.text_input("Player Name", placeholder="e.g. LeBron James")
                lb_stat = st.selectbox("Stat Type", _TRACKER_STAT_TYPES)
                lb_line = st.number_input("Prop Line", min_value=0.5, step=0.5, value=20.0)
            with fc2:
                lb_direction = st.selectbox("Direction", ["OVER", "UNDER"])
                lb_platform = st.selectbox("Platform", sorted(VALID_PLATFORMS))
                lb_opponent = st.text_input("Opponent (optional)", placeholder="e.g. BOS")

            lb_notes = st.text_input("Notes (optional)")
            submitted = st.form_submit_button("📝 Log Bet", use_container_width=True)

        if submitted:
            if not lb_player or not lb_player.strip():
                st.error("Player name is required.")
            else:
                res = log_new_bet(
                    player_name=lb_player,
                    stat_type=lb_stat,
                    prop_line=float(lb_line),
                    direction=lb_direction,
                    platform=lb_platform,
                    opponent=lb_opponent,
                    notes=lb_notes,
                    source="manual",
                )
                if res.get("success"):
                    st.success(f"Bet logged! (ID: {res['bet_id']})")
                else:
                    st.error(res.get("error", "Failed to log bet."))

    # ── Tab: Performance ──────────────────────────────────────
    with tab_perf:
        st.subheader("📊 Performance by Tier")
        tier_data = stats.get("by_tier", [])
        if tier_data:
            tc = st.columns(min(len(tier_data), 4))
            for i, t in enumerate(tier_data):
                tier_name = t.get("tier", "?")
                wr = t.get("win_rate", 0)
                decided = t.get("wins", 0) + t.get("losses", 0)
                emoji = {"Platinum": "💎", "Gold": "🥇", "Silver": "🥈", "Bronze": "🥉"}.get(tier_name, "🥉")
                tc[i % len(tc)].metric(
                    f"{emoji} {tier_name}",
                    f"{wr:.0f}% WR" if decided > 0 else "N/A",
                    delta=f"{t.get('wins', 0)}/{decided} decided" if decided > 0 else f"{t.get('total', 0)} pending",
                )
        else:
            st.info("No tier data yet.")

        st.divider()
        st.subheader("📊 Performance by Stat Type")
        stat_data = stats.get("by_stat", [])
        if stat_data:
            _show_df(stat_data, columns=["stat_type", "total", "wins", "losses", "pending", "win_rate"])
        else:
            st.info("No stat data yet.")

        st.divider()
        st.subheader("📊 Performance by Platform")
        plat_data = stats.get("by_platform", [])
        if plat_data:
            _show_df(plat_data, columns=["platform", "total", "wins", "losses", "pending", "win_rate"])
        else:
            st.info("No platform data yet.")


# ═══════════════════════════════════════════════════════════════════════════
# Page router
# ═══════════════════════════════════════════════════════════════════════════

_PAGE_DISPATCH: dict[str, Callable[[], None]] = {
    "home": _page_home,
    "game_detail": _page_game_detail,
    "player_profile": _page_player_profile,
    "standings": _page_standings,
    "teams_browse": _page_teams_browse,
    "team_detail": _page_team_detail,
    "leaders": _page_leaders,
    "defense": _page_defense,
    "more": _page_more,
    "prop_analyzer": _page_prop_analyzer,
    "pick_history": _page_pick_history,
    "bet_tracker": _page_bet_tracker,
}

_page_fn = _PAGE_DISPATCH.get(st.session_state.page)
if _page_fn is not None:
    _page_fn()
else:
    st.warning(f"Unknown page: {st.session_state.page}")
    _page_home()
