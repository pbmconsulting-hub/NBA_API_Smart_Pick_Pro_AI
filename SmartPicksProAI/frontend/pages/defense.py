"""Defense vs Position page."""
import pandas as pd
import streamlit as st
from pages._shared import show_df
from api_service import get_teams, get_defense_vs_position


def render() -> None:
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
        pos_filter = st.selectbox(
            "Filter by position",
            options=["All Positions", "PG", "SG", "SF", "PF", "C"],
            key="dvp_pos_filter",
        )

        selected_dvp = st.selectbox(
            "Select a team (or All Teams)",
            options=["All Teams"] + [t["abbreviation"] for t in dvp_teams],
            key="dvp_select",
        )

        display_cols = [
            "team", "pos", "vs_pts_mult", "vs_reb_mult",
            "vs_ast_mult", "vs_stl_mult", "vs_blk_mult",
            "vs_3pm_mult",
        ]

        if selected_dvp == "All Teams":
            _dvp_cache_key = "dvp_all_teams_cache"
            if _dvp_cache_key not in st.session_state:
                all_dvp: list[dict] = []
                with st.spinner("Loading defense data for all teams…"):
                    for t in dvp_teams:
                        positions = get_defense_vs_position(t["abbreviation"])
                        for p in positions:
                            p["team"] = t["abbreviation"]
                            all_dvp.append(p)
                st.session_state[_dvp_cache_key] = all_dvp
            all_dvp = st.session_state[_dvp_cache_key]
            if all_dvp:
                df_dvp = pd.DataFrame(all_dvp)
                if pos_filter != "All Positions":
                    df_dvp = df_dvp[df_dvp["pos"] == pos_filter]

                if not df_dvp.empty:
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
                    show_df(df_dvp[avail_cols].to_dict("records"), avail_cols, height=600)
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
                    show_df(positions, single_cols)
                else:
                    st.info("No data for the selected position.")
            else:
                st.info("No data for this team.")
    else:
        st.info("No teams loaded.")
