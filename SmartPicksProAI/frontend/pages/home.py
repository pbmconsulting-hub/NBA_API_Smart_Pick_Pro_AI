"""Home page."""
import streamlit as st
from pages._shared import (
    nav, game_button, player_button, TIER_EMOJI, TIER_COLORS,
    MAX_GAME_COLUMNS, MAX_RECENT_GAMES, MAX_SEARCH_RESULTS,
)
from styles.theme import get_hero_banner_html
from api_service import (
    get_todays_games, get_todays_slate, get_recent_games, search_players,
)


def render() -> None:
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
                game_button(game, key_prefix="today")
    else:
        st.info("No games scheduled for today.")

    st.divider()

    # ── Today's Best AI Picks (F2: interactive cards) ─────────
    st.markdown('<div class="section-hdr">🤖 Today\'s Best AI Picks</div>',
                unsafe_allow_html=True)
    st.caption("Top Platinum & Gold picks auto-generated from today's slate.")

    slate = get_todays_slate(top_n=5)
    top_picks = slate.get("picks", [])
    if top_picks:
        for idx, pick in enumerate(top_picks[:5]):
            p_name = pick.get("player_name", "Unknown")
            p_stat = pick.get("stat_type", "?")
            p_line = pick.get("prop_line", 0)
            p_dir = pick.get("direction", "OVER")
            p_tier = pick.get("tier", "Bronze")
            p_conf = pick.get("confidence_score", 0)
            p_edge = pick.get("edge_pct", 0.0)
            p_team = pick.get("team", "?")
            p_opp = pick.get("opponent", "?")
            tier_emoji = TIER_EMOJI.get(p_tier, "🥉")
            tier_color = TIER_COLORS.get(p_tier, "#C0C0C0")
            dir_icon = "🟢" if p_dir == "OVER" else "🔴"

            # Interactive card (F2)
            with st.container():
                st.markdown(
                    f'<div style="height:4px;background:{tier_color};'
                    f'border-radius:2px;margin-bottom:4px"></div>',
                    unsafe_allow_html=True,
                )
                card_cols = st.columns([3, 1, 1, 1])
                card_cols[0].markdown(
                    f"**{tier_emoji} {p_name}** ({p_team} vs {p_opp})<br>"
                    f"{p_stat.upper()} {dir_icon} {p_dir} {p_line}",
                    unsafe_allow_html=True,
                )
                card_cols[1].metric("Confidence", f"{p_conf:.0f}")
                card_cols[2].metric("Edge", f"{p_edge:+.1f}%")
                if card_cols[3].button("🎯 Analyze", key=f"slate_analyze_{idx}"):
                    st.session_state["auto_prop_player_id"] = pick.get("player_id")
                    st.session_state["auto_prop_stat"] = p_stat
                    st.session_state["auto_prop_line"] = p_line
                    st.session_state["auto_prop_platform"] = pick.get("platform", "prizepicks")
                    nav("prop_analyzer")
                    st.rerun()
        if slate.get("games_scanned"):
            st.caption(
                f"Scanned {slate.get('games_scanned', 0)} games, "
                f"{slate.get('players_scanned', 0)} players"
            )
    else:
        st.info("No AI picks available yet — run the slate builder or wait for today's games.")

    st.divider()

    # ── Recent Games ──────────────────────────────────────────
    st.markdown('<div class="section-hdr">Recent Games</div>',
                unsafe_allow_html=True)

    recent = get_recent_games()
    if recent:
        for idx, game in enumerate(recent[:MAX_RECENT_GAMES]):
            game_button(game, key_prefix="recent")
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
                player_button(
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
            nav("player_profile", selected_player_id=player_id_direct)
            st.rerun()
