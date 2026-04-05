"""Studio page — Joseph M. Smith AI Analyst Broadcast Desk.

Internal frontend module for ``SmartPicksProAI/frontend/app.py``.
Mirrors the root-level ``pages/9_🎙️_The_Studio.py`` for the internal
modular router.
"""

import streamlit as st

from pages._shared import TIER_EMOJI

# ── Optional imports (graceful fallbacks) ────────────────────────────────

try:
    from pages.helpers.joseph_live_desk import (  # type: ignore[import-untyped]
        inject_live_desk_css,
        render_joseph_desk_header,
        render_full_analysis,
    )
    _DESK_AVAILABLE = True
except ImportError:
    _DESK_AVAILABLE = False

try:
    from engine.joseph_brain import (
        joseph_full_analysis,
        generate_best_bets,
        get_ambient_lines,
    )
    _BRAIN_AVAILABLE = True
except ImportError:
    _BRAIN_AVAILABLE = False

try:
    from engine.joseph_bets import joseph_get_track_record
    _BETS_AVAILABLE = True
except ImportError:
    _BETS_AVAILABLE = False

try:
    from engine.joseph_tickets import build_joseph_ticket
    _TICKETS_AVAILABLE = True
except ImportError:
    _TICKETS_AVAILABLE = False

try:
    from api_service import search_players, analyze_prop
    _API_AVAILABLE = True
except ImportError:
    _API_AVAILABLE = False


# =========================================================================
# render()
# =========================================================================

def render() -> None:  # noqa: C901 — long but straightforward tabbed UI
    """Render the Joseph M. Smith broadcast desk page."""

    if _DESK_AVAILABLE:
        inject_live_desk_css()

    # ── Header ──────────────────────────────────────────────────
    st.markdown(
        '<div class="joseph-desk">',
        unsafe_allow_html=True,
    )
    if _DESK_AVAILABLE:
        render_joseph_desk_header()
    else:
        st.markdown("## 🎙️ Joseph M. Smith — The Studio")
        st.caption("AI Analyst • Smart Pick Pro")
    st.markdown("</div>", unsafe_allow_html=True)

    # ── Tabs ────────────────────────────────────────────────────
    tab_analysis, tab_best_bets, tab_tickets, tab_record = st.tabs([
        "🔬 Live Analysis",
        "🔥 Best Bets",
        "🎫 Ticket Builder",
        "📈 Track Record",
    ])

    # ────────────────────────────────────────────────────────────
    # TAB 1: Live Analysis
    # ────────────────────────────────────────────────────────────
    with tab_analysis:
        st.markdown("### 🔬 Ask Joseph — Live Analysis")

        if not _API_AVAILABLE:
            st.warning(
                "Backend API unavailable — start the backend to use live analysis. "
                "(`cd SmartPicksProAI/backend && python -m uvicorn api:app --port 8098`)"
            )
        else:
            col_input, col_result = st.columns([1, 2])

            with col_input:
                player_query = st.text_input(
                    "Search Player", placeholder="e.g. LeBron James",
                    key="studio_player",
                )
                stat_type = st.selectbox(
                    "Stat Type",
                    ["points", "rebounds", "assists", "threes", "steals",
                     "blocks", "points_rebounds_assists", "points_rebounds",
                     "points_assists", "rebounds_assists", "fantasy_score_pp"],
                    key="studio_stat",
                )
                prop_line = st.number_input(
                    "Prop Line", min_value=0.0, step=0.5, value=20.0,
                    key="studio_line",
                )
                run_btn = st.button("🎙️ Run Analysis", type="primary",
                                    key="studio_run")

            with col_result:
                if run_btn and player_query:
                    with st.spinner("Joseph is breaking this down..."):
                        players = search_players(player_query)
                        if not players:
                            st.error(f"No players found matching '{player_query}'.")
                        else:
                            player = players[0] if isinstance(players, list) else players
                            player_name = player.get(
                                "full_name", player.get("player_name", player_query),
                            )
                            result = analyze_prop(
                                player_name=player_name,
                                stat_type=stat_type,
                                prop_line=prop_line,
                            )
                            if result and "error" not in result:
                                if _BRAIN_AVAILABLE:
                                    analysis = joseph_full_analysis(
                                        player=player,
                                        prop_line=prop_line,
                                        stat_type=stat_type,
                                        simulation_result=result.get("simulation"),
                                        confidence_data={
                                            "tier": result.get("tier", "Bronze"),
                                            "score": result.get("confidence_score", 50),
                                        },
                                    )
                                    if _DESK_AVAILABLE:
                                        render_full_analysis(analysis)
                                    else:
                                        st.json(analysis)
                                else:
                                    st.json(result)
                            elif result:
                                st.error(result.get("error", "Analysis failed."))
                            else:
                                st.error("No response from the analysis engine.")

    # ────────────────────────────────────────────────────────────
    # TAB 2: Best Bets
    # ────────────────────────────────────────────────────────────
    with tab_best_bets:
        st.markdown("### 🔥 Joseph's Best Bets")

        if not _BRAIN_AVAILABLE:
            st.info("Joseph's brain module is not available.")
        else:
            st.caption(
                "Best bets are generated from today's analysis results. "
                "Run analyses in the Live Analysis tab first, or use the "
                "Analysis page to build a slate."
            )
            slate_results = st.session_state.get("joseph_slate_results", [])
            if slate_results:
                best = generate_best_bets(slate_results, max_bets=5)
                for i, bet in enumerate(best, 1):
                    with st.container():
                        cols = st.columns([3, 1, 1, 1])
                        cols[0].markdown(
                            f"**{i}. {bet['player_name']}** — "
                            f"{bet['stat_type']} {bet['direction']} {bet['prop_line']}"
                        )
                        cols[1].metric("Confidence", f"{bet['confidence_score']:.0f}")
                        cols[2].metric("Tier", bet["confidence_tier"])
                        cols[3].metric("Score", f"{bet['composite_score']:.0f}")
                        note = bet.get("joseph_note", "")
                        if note:
                            st.caption(note)
                        st.divider()
            else:
                st.info(
                    "No slate results in session yet. Run analyses first "
                    "and results will appear here."
                )

            lines = get_ambient_lines(2)
            for line in lines:
                st.markdown(
                    f'<div style="font-style:italic;color:#888;font-size:0.8rem;'
                    f'margin:4px 0;">"{line}"</div>',
                    unsafe_allow_html=True,
                )

    # ────────────────────────────────────────────────────────────
    # TAB 3: Ticket Builder
    # ────────────────────────────────────────────────────────────
    with tab_tickets:
        st.markdown("### 🎫 Joseph's Ticket Builder")

        if not _TICKETS_AVAILABLE:
            st.info("Ticket builder module is not available.")
        else:
            st.caption(
                "Build a parlay ticket from your analysis results. "
                "Joseph will grade the ticket and warn about correlated legs."
            )
            slate_results = st.session_state.get("joseph_slate_results", [])
            if slate_results:
                max_legs = st.slider("Max Legs", 2, 6, 4, key="studio_legs")
                min_conf = st.slider(
                    "Min Confidence", 40.0, 90.0, 60.0, step=5.0,
                    key="studio_conf",
                )
                if st.button("🎫 Build Ticket", type="primary",
                             key="studio_build"):
                    with st.spinner("Building ticket..."):
                        ticket = build_joseph_ticket(
                            slate_results, max_legs=max_legs,
                            min_confidence=min_conf,
                        )
                    if ticket.get("legs"):
                        st.success(
                            f"**{ticket['leg_count']}-Leg Ticket** | "
                            f"Grade: {ticket['ticket_grade']} | "
                            f"Implied Prob: {ticket['implied_probability']:.1%}"
                        )
                        for leg in ticket["legs"]:
                            st.markdown(
                                f"- **{leg['player_name']}** {leg['stat_type']} "
                                f"{leg['direction']} {leg['prop_line']} "
                                f"(Conf: {leg['confidence_score']:.0f}, "
                                f"{leg['confidence_tier']})"
                            )
                        warn = ticket.get("correlation_warning", "")
                        if warn:
                            st.warning(warn)
                        pitch = ticket.get("joseph_pitch", "")
                        if pitch:
                            st.info(f"🎙️ {pitch}")
                    else:
                        st.warning(ticket.get("joseph_pitch", "No eligible legs."))
            else:
                st.info("Run analyses first to populate the ticket builder.")

    # ────────────────────────────────────────────────────────────
    # TAB 4: Track Record
    # ────────────────────────────────────────────────────────────
    with tab_record:
        st.markdown("### 📈 Joseph's Track Record")

        if not _BETS_AVAILABLE:
            st.info("Bet tracking module is not available.")
        else:
            record = joseph_get_track_record(limit=50)
            headline = record.get("joseph_headline", "")
            if headline:
                st.markdown(f"**{headline}**")

            summary = record.get("summary", {})
            total = summary.get("total_bets", 0)

            if total > 0:
                cols = st.columns(4)
                cols[0].metric("Total Bets", total)
                cols[1].metric("Wins", summary.get("wins", 0))
                cols[2].metric("Losses", summary.get("losses", 0))
                win_rate = summary.get("wins", 0) / total * 100 if total else 0
                cols[3].metric("Win Rate", f"{win_rate:.1f}%")

                by_tier = record.get("by_tier", {})
                if by_tier:
                    st.markdown("**Performance by Tier**")
                    for tier_name, tier_data in by_tier.items():
                        if isinstance(tier_data, dict):
                            icon = TIER_EMOJI.get(tier_name, "")
                            t_total = tier_data.get("total", 0)
                            t_wins = tier_data.get("wins", 0)
                            t_rate = (t_wins / t_total * 100) if t_total > 0 else 0
                            st.markdown(
                                f"- {icon} **{tier_name}**: "
                                f"{t_wins}/{t_total} ({t_rate:.0f}%)"
                            )

                recent = record.get("recent_bets", [])
                if recent:
                    st.markdown("**Recent Bets**")
                    for bet in recent[:20]:
                        result_emoji = {
                            "win": "✅", "loss": "❌", "push": "➖",
                        }.get(bet.get("result", ""), "⏳")
                        st.markdown(
                            f"{result_emoji} {bet.get('player_name', '?')} — "
                            f"{bet.get('stat_type', '?')} "
                            f"{bet.get('direction', '?')} "
                            f"{bet.get('prop_line', '?')} "
                            f"({bet.get('confidence_tier', '')})"
                        )
            else:
                st.info("No bets tracked yet. Run analyses to generate picks.")
