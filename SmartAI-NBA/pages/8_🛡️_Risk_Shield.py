# ============================================================
# FILE: pages/8_🛡️_Risk_Shield.py
# PURPOSE: Display all props that should be avoided and explain
#          WHY each one is a bad bet. Helps the user avoid traps.
# CONNECTS TO: analysis results in session state
# CONCEPTS COVERED: Filtering, explanations, data display
# ============================================================

import streamlit as st  # Main UI framework

# ============================================================
# SECTION: Page Setup
# ============================================================

st.set_page_config(
    page_title="Risk Shield — SmartBetPro NBA",
    page_icon="🛡️",
    layout="wide",
)

# ─── Inject Global CSS Theme ──────────────────────────────────
from styles.theme import get_global_css, get_education_box_html
st.markdown(get_global_css(), unsafe_allow_html=True)

# ── Joseph M. Smith Hero Banner & Floating Widget ─────────────
from utils.components import render_joseph_hero_banner, inject_joseph_floating
st.session_state["joseph_page_context"] = "page_risk_shield"
inject_joseph_floating()

# ── Premium Gate ───────────────────────────────────────────────
from utils.premium_gate import premium_gate
if not premium_gate("Risk Shield"):
    st.stop()
# ── End Premium Gate ───────────────────────────────────────────

st.title("🛡️ Risk Shield")
st.markdown(
    "These props have been flagged as **high-risk or low-edge** by the model. "
    "Understand WHY to make better decisions."
)

with st.expander("📖 How to Use This Page", expanded=False):
    st.markdown("""
    ### Risk Shield — Protecting Your Bankroll
    
    The Risk Shield automatically flags props that **failed the model's quality checks**. These are picks you should avoid.
    
    **How It Works**
    1. Run analysis on the **⚡ Quantum Analysis Matrix** page
    2. The Risk Shield automatically collects all flagged props
    3. Each flagged pick shows WHY it was flagged (low edge, trap line, high variance, etc.)
    
    **Understanding Red Flags**
    - 🚩 **Low Edge**: Model probability is near 50/50 — essentially a coin flip
    - 🚩 **Trap Line**: Vegas set this line to bait bettors to the wrong side
    - 🚩 **Sharp Line**: The line matches the player's exact average — no value
    - 🚩 **High Variance**: The player's game-to-game stats are too unpredictable
    - 🚩 **Low Confidence**: Contradictory signals — the model isn't sure
    
    💡 **Pro Tips:**
    - If a pick appears here, skip it — even if the potential payout looks attractive
    - Check the "What would make this playable?" section for each flagged pick
    - Use this alongside the Entry Builder to avoid putting risky picks in parlays
    """)

st.divider()

st.markdown(get_education_box_html(
    "📖 Why We Avoid These Picks",
    """
    The Risk Shield flags props that have one or more red flags:<br><br>
    🚩 <strong>Low edge</strong>: The model's probability is too close to 50/50 — no real edge.<br>
    🚩 <strong>Trap line</strong>: The line is set at an unusual position to attract bettors to the wrong side.<br>
    🚩 <strong>Sharp line</strong>: Vegas has priced this line at the player's exact average — no value.<br>
    🚩 <strong>High variance</strong>: The player's stats are too unpredictable to bet with confidence.<br>
    🚩 <strong>Low confidence</strong>: Too many contradictory signals — the model isn't sure.<br><br>
    <em>Rule of thumb: If a pick is on this list, it's not worth the risk regardless of how tempting it looks.</em>
    """
), unsafe_allow_html=True)

# ============================================================
# END SECTION: Page Setup
# ============================================================

# ============================================================
# SECTION: Load Analysis Results
# ============================================================

analysis_results = st.session_state.get("analysis_results", [])

if not analysis_results:
    st.warning(
        "⚠️ No analysis results yet. Go to **⚡ Neural Analysis** and run analysis first!"
    )
    st.stop()

# ============================================================
# END SECTION: Load Analysis Results
# ============================================================

# ============================================================
# SECTION: Build Avoid List
# Separate picks into avoided vs non-avoided
# ============================================================

# Get all props that should be avoided
avoided_props = [
    result for result in analysis_results
    if result.get("should_avoid", False)
]

# Get low-edge props (edge below 5% but not explicitly avoided)
low_edge_props = [
    result for result in analysis_results
    if not result.get("should_avoid", False)
    and abs(result.get("edge_percentage", 0)) < 5.0
]

# Get conflicting direction props (over_count ≈ under_count)
conflicting_props = []
for result in analysis_results:
    forces = result.get("forces", {})
    over_strength = forces.get("over_strength", 0)
    under_strength = forces.get("under_strength", 0)
    total = over_strength + under_strength
    if total > 0:
        # If both sides are within 25% of each other, it's conflicting
        if min(over_strength, under_strength) / max(over_strength, under_strength) > 0.75:
            if result not in avoided_props:
                conflicting_props.append(result)

# ============================================================
# END SECTION: Build Avoid List
# ============================================================

# ============================================================
# SECTION: Summary Metrics
# ============================================================

col1, col2, col3, col4 = st.columns(4)

col1.metric(
    "🚫 Explicitly Avoided",
    len(avoided_props),
    help="Props that triggered avoid criteria in the model",
)
col2.metric(
    "⚠️ Low Edge",
    len(low_edge_props),
    help="Props with less than 5% edge — near coin flip",
)
col3.metric(
    "🔀 Conflicting Forces",
    len(conflicting_props),
    help="Props where OVER and UNDER forces are nearly equal",
)
col4.metric(
    "✅ Recommended Picks",
    len(analysis_results) - len(avoided_props),
    help="Props that passed the avoid criteria",
)

# ============================================================
# END SECTION: Summary Metrics
# ============================================================

st.divider()

# ============================================================
# SECTION: Avoided Props Detail
# ============================================================

if avoided_props:
    st.subheader(f"🚫 Explicitly Avoided Picks ({len(avoided_props)})")
    st.markdown(
        "These props were flagged by the model for specific reasons. "
        "Understanding why helps you become a better bettor."
    )

    for result in avoided_props:
        player = result.get("player_name", "Unknown")
        stat = result.get("stat_type", "").capitalize()
        line = result.get("line", 0)
        platform = result.get("platform", "")
        edge = result.get("edge_percentage", 0)
        confidence = result.get("confidence_score", 0)
        prob = result.get("probability_over", 0.5)
        avoid_reasons = result.get("avoid_reasons", [])

        # ── Risk Score Bar ─────────────────────────────────────────────
        # Risk = inverse of edge/confidence composite
        _risk_score = max(0, min(100, round(100 - abs(edge) * 2.5 - confidence * 0.3)))
        _risk_color = "#ff4444" if _risk_score > 70 else ("#ffcc00" if _risk_score > 40 else "#00ff9d")
        _risk_label = "HIGH RISK 🔴" if _risk_score > 70 else ("MEDIUM RISK 🟡" if _risk_score > 40 else "LOW RISK 🟢")

        with st.container():
            _rc1, _rc2 = st.columns([3, 1])
            with _rc1:
                st.markdown(f"#### 🚫 {player} — {stat} ({line}) | {platform}")
            with _rc2:
                st.markdown(
                    f'<div style="text-align:right;">'
                    f'<span style="color:{_risk_color};font-weight:700;font-size:0.85rem;">{_risk_label}</span>'
                    f'</div>'
                    f'<div style="height:6px;background:#1a2035;border-radius:3px;margin-top:4px;">'
                    f'<div style="width:{_risk_score}%;height:6px;background:{_risk_color};border-radius:3px;"></div>'
                    f'</div>'
                    f'<div style="font-size:0.72rem;color:#8b949e;text-align:right;">Risk Score: {_risk_score}/100</div>',
                    unsafe_allow_html=True,
                )

            reason_col, stats_col = st.columns([2, 1])

            with reason_col:
                st.markdown("**Why to Avoid:**")
                for reason in avoid_reasons:
                    st.markdown(f"  - ❌ {reason}")

                # ── "What would make this playable?" ──────────────────────
                playable_hints = []
                if abs(edge) < 5:
                    playable_hints.append(f"Edge needs to reach ≥5% (currently {edge:.1f}%) — line would need to move to make this a value bet.")
                if confidence < 55:
                    playable_hints.append(f"Confidence score needs to reach ≥55 (currently {confidence:.0f}) — more consistent recent form would help.")
                forces = result.get("forces", {})
                over_strength = forces.get("over_strength", 0)
                under_strength = forces.get("under_strength", 0)
                if abs(over_strength - under_strength) < 5:
                    playable_hints.append("OVER/UNDER forces are nearly balanced — wait for a clearer directional signal.")
                if playable_hints:
                    with st.expander("🤔 What would make this playable?", expanded=False):
                        for hint in playable_hints:
                            st.markdown(f"  💡 {hint}")

            with stats_col:
                st.caption(f"Probability Over: {prob*100:.1f}%")
                st.caption(f"Edge: {edge:.1f}%")
                st.caption(f"Confidence Score: {confidence:.0f}/100")

            over_count = forces.get("over_count", 0)
            under_count = forces.get("under_count", 0)
            if over_count > 0 and under_count > 0:
                st.caption(
                    f"Forces: {over_count} OVER ({forces.get('over_strength', 0):.1f} strength) vs "
                    f"{under_count} UNDER ({forces.get('under_strength', 0):.1f} strength)"
                )

        st.markdown("---")

else:
    st.success("✅ No props explicitly flagged for avoidance — all picks passed!")

# ============================================================
# END SECTION: Avoided Props Detail
# ============================================================

# ============================================================
# SECTION: Low Edge Warnings
# ============================================================

if low_edge_props:
    st.divider()
    st.subheader(f"⚠️ Low-Edge Caution Zone ({len(low_edge_props)})")
    st.markdown(
        "These picks have less than **5% edge**. They're essentially coin flips. "
        "We suggest avoiding them unless you have other reasons to bet them."
    )

    # Display as a compact table
    low_edge_rows = []
    for result in low_edge_props:
        low_edge_rows.append({
            "Player": result.get("player_name", ""),
            "Stat": result.get("stat_type", "").capitalize(),
            "Line": result.get("line", 0),
            "Platform": result.get("platform", ""),
            "P(Over)": f"{result.get('probability_over', 0.5)*100:.1f}%",
            "Edge": f"{result.get('edge_percentage', 0):.1f}%",
            "Tier": f"{result.get('tier_emoji','')}{result.get('tier','')}",
        })

    st.dataframe(low_edge_rows, width="stretch", hide_index=True)

# ============================================================
# END SECTION: Low Edge Warnings
# ============================================================

# ============================================================
# SECTION: Conflicting Forces Section
# ============================================================

if conflicting_props:
    st.divider()
    st.subheader(f"🔀 Conflicting Forces ({len(conflicting_props)})")
    st.markdown(
        "These picks have nearly equal OVER and UNDER forces — the model is uncertain. "
        "Proceed with caution."
    )

    for result in conflicting_props:
        player = result.get("player_name", "Unknown")
        stat = result.get("stat_type", "").capitalize()
        line = result.get("line", 0)
        forces = result.get("forces", {})

        with st.expander(f"🔀 {player} — {stat} {line}"):
            over_forces = forces.get("over_forces", [])
            under_forces = forces.get("under_forces", [])

            force_col1, force_col2 = st.columns(2)
            with force_col1:
                st.markdown("**OVER forces:**")
                for f in over_forces:
                    st.caption(f"⬆️ {f['name']} (strength: {f['strength']:.1f})")
            with force_col2:
                st.markdown("**UNDER forces:**")
                for f in under_forces:
                    st.caption(f"⬇️ {f['name']} (strength: {f['strength']:.1f})")

            st.caption(
                f"Net: {forces.get('net_direction','')} by {forces.get('net_strength',0):.1f} — barely any edge"
            )

# ============================================================
# END SECTION: Conflicting Forces Section
# ============================================================

# ============================================================
# SECTION: Avoid List Education
# ============================================================

st.divider()
with st.expander("📚 Understanding Why Picks Get Avoided"):
    st.markdown("""
    ### Why Props End Up on the Avoid List

    The model avoids props when it detects one or more of these conditions:

    ---

    #### 1. 🪙 Insufficient Edge (< 5%)
    The model's probability is within 5% of 50% (the fair coin flip line).
    At this level, the house edge makes it unprofitable long-term.

    #### 2. 📉 High Variance / Unpredictable Stat
    Some stats (like steals, blocks, 3-pointers) are inherently random.
    A player who averages 2.1 steals might have 0 in a game or 6.
    When variability is > 55% of the average, projections are unreliable.

    #### 3. 🔀 Conflicting Forces
    When OVER forces and UNDER forces are nearly equal in strength,
    it means there's no clear directional edge. Both sides cancel out.

    #### 4. ⚠️ Blowout Risk
    When a game is likely to be a blowout (large spread), stars get
    rested in garbage time. This kills your over bets!

    ---

    **Remember:** The avoid list isn't saying these are 0% — it's saying
    the edge isn't large enough to justify the risk. Save your bankroll
    for the Platinum and Gold tier picks!
    """)

# ============================================================
# END SECTION: Avoid List Education
# ============================================================
