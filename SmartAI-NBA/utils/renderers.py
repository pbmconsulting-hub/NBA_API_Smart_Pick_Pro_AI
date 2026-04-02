# ============================================================
# FILE: utils/renderers.py
# PURPOSE: High-capacity HTML Matrix Compiler for the Neural
#          Analysis page. Compiles up to 500 prop-analysis
#          result dicts into a single HTML string with CSS Grid
#          layout for injection via st.markdown().
#
# USAGE:
#   from utils.renderers import compile_card_matrix
#   html = compile_card_matrix(results)
#   st.markdown(html, unsafe_allow_html=True)
#
# DESIGN:
#   - One single HTML string (no per-card st.container calls)
#   - CSS Grid auto-fill layout scales from mobile → ultrawide
#   - Staggered fade-in-up animation for visual waterfall
#   - Prominently displays True More/Less Line
# ============================================================

import html as _html
import os as _os
import base64 as _base64
import re as _re

try:
    from utils.log_helper import get_logger as _get_logger
    _logger = _get_logger(__name__)
except ImportError:
    import logging as _logging
    _logger = _logging.getLogger(__name__)

from styles.theme import QUANTUM_CARD_MATRIX_CSS, UNIFIED_PLAYER_CARD_CSS, get_team_colors

try:
    from data.player_profile_service import get_headshot_url as _get_headshot_url
except ImportError:  # pragma: no cover
    def _get_headshot_url(name):  # type: ignore[misc]
        return "https://cdn.nba.com/headshots/nba/latest/1040x760/fallback.png"


# ── Joseph M Smith avatar (cached base64) ────────────────────
_JOSEPH_AVATAR_B64: str | None = None


def _get_joseph_avatar_b64() -> str:
    """Return base64-encoded Joseph M Smith avatar, cached after first load."""
    global _JOSEPH_AVATAR_B64
    if _JOSEPH_AVATAR_B64 is not None:
        return _JOSEPH_AVATAR_B64
    _candidates = [
        _os.path.join(_os.path.dirname(_os.path.abspath(__file__)), "..", "Joseph M Smith Avatar.png"),
        _os.path.join(_os.getcwd(), "Joseph M Smith Avatar.png"),
        _os.path.join(_os.path.dirname(_os.path.abspath(__file__)), "..", "assets", "Joseph M Smith Avatar.png"),
    ]
    for path in _candidates:
        norm = _os.path.normpath(path)
        if _os.path.isfile(norm):
            try:
                with open(norm, "rb") as fh:
                    _JOSEPH_AVATAR_B64 = _base64.b64encode(fh.read()).decode("utf-8")
                    return _JOSEPH_AVATAR_B64
            except Exception as exc:
                _logger.debug("_load_joseph_avatar: read failed for %s — %s", norm, exc)
    _JOSEPH_AVATAR_B64 = ""
    return _JOSEPH_AVATAR_B64


# Fallback silhouette SVG for missing headshots
_FALLBACK_SVG = (
    "data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' "
    "width='48' height='48' viewBox='0 0 48 48'%3E"
    "%3Ccircle cx='24' cy='24' r='24' fill='%23141a2d'/%3E"
    "%3Ccircle cx='24' cy='18' r='8' fill='%23475569'/%3E"
    "%3Cellipse cx='24' cy='40' rx='13' ry='10' fill='%23475569'/%3E"
    "%3C/svg%3E"
)


def _escape(value):
    """Safely HTML-escape a value, returning empty string for None."""
    if value is None:
        return ""
    return _html.escape(str(value))


def _build_context_metrics(result):
    """Build 4-item context metrics (Situational, Matchup, Form, Edge)."""
    stat_type = (result.get("stat_type", "points") or "points").lower()
    avg_map = {
        "points": "season_pts_avg", "rebounds": "season_reb_avg",
        "assists": "season_ast_avg", "threes": "season_threes_avg",
    }
    season_avg = float(result.get(avg_map.get(stat_type, ""), 0) or 0)
    line = float(result.get("line", result.get("prop_line", 0)) or 0)
    edge_pct = float(result.get("edge_percentage", 0) or 0)
    defense_f = float(result.get("overall_adjustment", 1.0) or 1.0)
    form_ratio = result.get("recent_form_ratio")

    # Situational: season avg vs line
    line_diff = round(line - season_avg, 1)
    sit_val = (
        f"Avg {season_avg:.1f} / Line {line:g} "
        f"({'▲' if line_diff > 0 else '▼'}{abs(line_diff):.1f})"
    ) if season_avg > 0 else f"Line {line:g}"

    # Archetype matchup
    matchup_val = (
        f"{'Favorable' if defense_f < 1.0 else 'Tough'} "
        f"({defense_f:.2f}x)"
    )

    # Form
    if form_ratio is not None:
        try:
            fr = float(form_ratio)
            form_label = "Hot 🔥" if fr > 1.05 else ("Cold 🧊" if fr < 0.95 else "Neutral")
            form_val = f"{fr:.2f}x ({form_label})"
        except (ValueError, TypeError):
            form_val = "N/A"
    else:
        form_val = "N/A"

    # Edge vs line
    edge_sign = "+" if edge_pct >= 0 else ""
    edge_val = f"{edge_sign}{edge_pct:.1f}%"

    return [
        ("📊 Situational", sit_val),
        ("🛡️ Matchup", matchup_val),
        ("🔥 Form", form_val),
        ("⚡ Edge", edge_val),
    ]


def _build_bonus_factors(result):
    """Build bonus factor strings from forces, traps, and notes."""
    bonus = []
    direction = (result.get("direction", "OVER") or "OVER").upper()
    forces = result.get("forces", {}) or {}
    forces_to_show = (forces.get("over_forces", []) if direction == "OVER"
                      else forces.get("under_forces", [])) or []
    for f in forces_to_show[:4]:
        if not isinstance(f, dict):
            continue
        lbl = f.get("name", f.get("label", f.get("factor", "")))
        desc = f.get("description", f.get("detail", ""))
        if lbl:
            bonus.append(f"{lbl}" + (f" — {desc}" if desc else ""))

    trap = result.get("trap_line_result", {}) or {}
    if trap.get("is_trap"):
        bonus.append(f"⚠️ {trap.get('warning_message', 'Possible trap line')}")

    ls_force = result.get("line_sharpness_force")
    if ls_force:
        bonus.append(f"📐 Sharp line: {ls_force.get('description', '')}")

    for note in (result.get("teammate_out_notes", []) or [])[:2]:
        bonus.append(f"👥 {note}")

    return bonus[:6]


def _build_single_card_html(result, index=0):
    """
    Build a single Quantum Card HTML string from an analysis result dict.

    Renders the **Combined** view merging the QDS prop card and the matrix
    card into one unified display:  identity row (headshot + name + SAFE
    Score), True Line, confidence bar, primary metrics, distribution
    percentiles, context metrics grid, directional forces, score
    breakdown bars, and bonus factors.

    Args:
        result (dict): A single prop analysis result from the engine.
        index (int): The card's position index (used for stagger delay).

    Returns:
        str: The HTML string for this single card.
    """
    player_name = _escape(result.get("player_name", "Unknown"))
    stat_type = _escape(result.get("stat_type", ""))
    team = _escape(result.get("player_team", result.get("team", "")))
    platform = _escape(result.get("platform", ""))
    tier = result.get("tier", "Bronze")
    tier_lower = tier.lower() if tier else "bronze"

    # True Line — the verified More/Less line
    prop_line = result.get("prop_line", result.get("line", 0))
    try:
        true_line = float(prop_line)
        true_line_display = f"{true_line:g}"
    except (ValueError, TypeError):
        true_line = 0
        true_line_display = "—"

    # Confidence / probability / edge
    confidence = result.get("confidence_score", 0)
    try:
        confidence = float(confidence)
    except (ValueError, TypeError):
        confidence = 0
    prob_over = result.get("probability_over", 0)
    try:
        prob_pct = f"{float(prob_over) * 100:.1f}%"
    except (ValueError, TypeError):
        prob_pct = "—"
    edge = result.get("edge_percentage", result.get("edge", 0))
    try:
        edge_display = f"{float(edge):+.1f}%"
    except (ValueError, TypeError):
        edge_display = "—"

    # ── Distribution & EV metrics ────────────────────────────────
    p10 = result.get("percentile_10", 0) or 0
    p50 = result.get("percentile_50", 0) or 0
    p90 = result.get("percentile_90", 0) or 0
    std_dev = result.get("simulated_std", result.get("std_dev", 0)) or 0
    adj_proj = result.get("adjusted_projection", 0) or 0
    try:
        p10_d = f"{float(p10):.1f}"
    except (ValueError, TypeError):
        p10_d = "—"
    try:
        p50_d = f"{float(p50):.1f}"
    except (ValueError, TypeError):
        p50_d = "—"
    try:
        p90_d = f"{float(p90):.1f}"
    except (ValueError, TypeError):
        p90_d = "—"
    try:
        std_d = f"{float(std_dev):.1f}"
    except (ValueError, TypeError):
        std_d = "—"
    try:
        proj_d = f"{float(adj_proj):.1f}"
    except (ValueError, TypeError):
        proj_d = "—"

    # Prediction text
    prediction = _escape(result.get("prediction", ""))

    prediction_html = ""
    if prediction:
        prediction_html = (
            f'<div class="qcm-prediction qcm-prediction-neutral">'
            f'⚪ {prediction}</div>'
        )

    # Stagger delay: 20ms per card, capped at 2s for 100 cards
    delay_ms = min(index * 20, 2000)

    # Direction label
    direction = result.get("direction", "")
    if not direction:
        try:
            direction = "OVER" if prob_over and float(prob_over) >= 0.5 else "UNDER"
        except (ValueError, TypeError):
            direction = "OVER"
    direction_escaped = _escape(direction.upper())

    # ── Player headshot ──────────────────────────────────────────
    player_id = result.get("player_id", "") or ""
    if player_id:
        headshot_url = f"https://cdn.nba.com/headshots/nba/latest/1040x760/{player_id}.png"
    else:
        # Fallback: resolve headshot from player name via profile service
        _raw_name = result.get("player_name", "")
        headshot_url = _get_headshot_url(_raw_name) if _raw_name else ""
    if headshot_url:
        img_html = (
            f'<img class="qcm-headshot qcm-headshot-{tier_lower}" '
            f'src="{headshot_url}" '
            f'onerror="this.onerror=null;this.src=\'{_FALLBACK_SVG}\'" '
            f'alt="{player_name}">'
        )
    else:
        img_html = (
            f'<img class="qcm-headshot qcm-headshot-{tier_lower}" '
            f'src="{_FALLBACK_SVG}" alt="{player_name}">'
        )

    # ── Team badge ────────────────────────────────────────────────
    team_primary, _ = get_team_colors(team)
    team_badge_html = (
        f'<span class="qcm-team-badge" '
        f'style="background:{team_primary};">{team}</span>'
    ) if team else ""

    # ── SAFE Score (X.X / 10) ─────────────────────────────────────
    safe_score = round(min(10.0, confidence / 10.0), 1)
    safe_score_str = f"{safe_score:.1f}"

    # ── Prop description ──────────────────────────────────────────
    _stat_emoji = {
        "points": "🏀", "rebounds": "📊", "assists": "🎯",
        "threes": "🎯", "steals": "⚡", "blocks": "🛡️", "turnovers": "❌",
    }
    stat_lower = (result.get("stat_type", "") or "").lower()
    dir_label = "More" if direction.upper() == "OVER" else "Less"
    stat_emoji = _stat_emoji.get(stat_lower, "🏀")
    prop_text = f"{stat_emoji} {dir_label} {true_line_display} {_escape(stat_type.replace('_', ' ').title())}"

    # ── Confidence bar ─────────────────────────────────────────────
    conf_pct = max(0.0, min(100.0, confidence))
    if conf_pct >= 80:
        conf_color = "#00f0ff"
    elif conf_pct >= 65:
        conf_color = "#FFD700"
    elif conf_pct >= 50:
        conf_color = "#00b4ff"
    else:
        conf_color = "#94A3B8"

    conf_bar_html = (
        f'<div class="qcm-conf-bar-wrap">'
        f'<div class="qcm-conf-bar-header">'
        f'<span>Confidence</span>'
        f'<span class="qcm-conf-bar-pct" style="color:{conf_color};">{conf_pct:.0f}%</span>'
        f'</div>'
        f'<div class="qcm-conf-bar-track">'
        f'<div class="qcm-conf-bar-fill" style="width:{conf_pct}%;'
        f'background:linear-gradient(90deg,{conf_color},{conf_color}dd);"></div>'
        f'</div>'
        f'</div>'
    )

    # ── Forces HTML (individual force lists) ─────────────────────
    forces = result.get("forces", {}) or {}
    over_forces = forces.get("over_forces", []) or []
    under_forces = forces.get("under_forces", []) or []

    def _force_items(force_list):
        if not force_list:
            return '<span class="qcm-force-none">None</span>'
        parts = []
        for f in force_list:
            if not isinstance(f, dict):
                continue
            strength = max(1, min(5, round(float(f.get("strength", 1)))))
            stars = "⭐" * strength
            name = _escape(str(f.get("name", "") or ""))
            parts.append(f'<div class="qcm-force-item">{stars} {name}</div>')
        return "".join(parts) if parts else '<span class="qcm-force-none">None</span>'

    forces_html = (
        '<div class="qcm-forces">'
        '<div class="qcm-forces-col qcm-forces-over">'
        '<div class="qcm-forces-label">▲ OVER</div>'
        f'{_force_items(over_forces)}'
        '</div>'
        '<div class="qcm-forces-col qcm-forces-under">'
        '<div class="qcm-forces-label">▼ UNDER</div>'
        f'{_force_items(under_forces)}'
        '</div>'
        '</div>'
    )

    # ── Score breakdown bars ─────────────────────────────────────
    breakdown = result.get("score_breakdown", {}) or {}
    breakdown_html = ""
    if breakdown:
        bars = []
        for factor, score in breakdown.items():
            try:
                score_f = float(score or 0)
            except (ValueError, TypeError):
                continue
            label = _escape(
                factor.replace("_score", "").replace("_", " ").title()
            )
            bar_w = min(100, max(0, score_f))
            if bar_w >= 70:
                bar_c = "#00f0ff"
            elif bar_w >= 40:
                bar_c = "#ff5e00"
            else:
                bar_c = "#ff4444"
            bars.append(
                f'<div class="qcm-breakdown-row">'
                f'<span class="qcm-breakdown-label">{label}</span>'
                f'<span class="qcm-breakdown-score">{score_f:.0f}</span>'
                f'<div class="qcm-breakdown-track">'
                f'<div class="qcm-breakdown-fill" style="width:{bar_w:.1f}%;background:{bar_c};"></div>'
                f'</div></div>'
            )
        breakdown_html = '<div class="qcm-breakdown">' + "".join(bars) + '</div>'

    # ── Kelly TARGET ALLOCATION metric ──────────────────────────
    wager_html = ""
    try:
        import streamlit as _st_mod
        from engine.odds_engine import calculate_fractional_kelly
        _dir = direction.upper() if direction else "OVER"
        _odds = (result.get("over_odds", -110)
                 if _dir == "OVER"
                 else result.get("under_odds", -110))
        _prob = (float(prob_over)
                 if _dir == "OVER"
                 else (1.0 - float(prob_over)))
        _bk = float(_st_mod.session_state.get("total_bankroll", 1000.0))
        _km = float(_st_mod.session_state.get("kelly_multiplier", 0.25))
        _kr = calculate_fractional_kelly(_prob, _odds, _km)
        _wager = round(_kr["fractional_kelly"] * _bk, 2)
        if _wager > 0:
            wager_html = (
                f'<div class="qcm-metric">'
                f'<div class="qcm-metric-val" style="color:#00C6FF;">${_wager:,.0f}</div>'
                f'<div class="qcm-metric-lbl">Wager</div>'
                f'</div>'
            )
    except Exception as exc:
        _logger.debug("build_unified_player_card_html: wager calc failed — %s", exc)

    # ── Context metrics grid (Situational / Matchup / Form / Edge) ─
    context_metrics = _build_context_metrics(result)
    ctx_cards = ""
    for ctx_label, ctx_value in context_metrics:
        safe_label = _escape(ctx_label)
        safe_value = _escape(str(ctx_value))
        ctx_cards += (
            f'<div class="qcm-context-card">'
            f'<div class="qcm-context-label">{safe_label}</div>'
            f'<div class="qcm-context-value">{safe_value}</div>'
            f'</div>'
        )
    context_grid_html = f'<div class="qcm-context-grid">{ctx_cards}</div>'

    # ── Bonus factors ─────────────────────────────────────────────
    bonus_factors = _build_bonus_factors(result)
    bonus_html = ""
    if bonus_factors:
        items = ""
        for bf in bonus_factors:
            items += (
                f'<div class="qcm-bonus-item">'
                f'<span class="qcm-bonus-icon">✓</span>'
                f'<span>{_escape(bf)}</span>'
                f'</div>'
            )
        bonus_html = (
            f'<div class="qcm-bonus">'
            f'<div class="qcm-bonus-title">Key Factors</div>'
            f'{items}'
            f'</div>'
        )

    return f"""<div class="qcm-card" style="animation-delay:{delay_ms}ms;">
  <div class="qcm-card-header">
    <span class="qcm-stat-type">{_escape(stat_type.replace('_', ' '))} <span class="qcm-team">· {team}</span> <span class="qcm-platform">· {platform}</span></span>
    <span class="qcm-tier-badge qcm-tier-{tier_lower}">{_escape(tier)}</span>
  </div>
  <div class="qcm-identity">
    {img_html}
    <div class="qcm-identity-info">
      <div class="qcm-identity-name">{player_name}{team_badge_html}</div>
      <div class="qcm-identity-prop">{prop_text}</div>
    </div>
    <div class="qcm-safe-score">
      <div class="qcm-safe-score-label">SAFE Score™</div>
      <div class="qcm-safe-score-value">{safe_score_str}<span>/10</span></div>
    </div>
  </div>
  <div class="qcm-true-line-row">
    <span class="qcm-true-line-label">True Line ({direction_escaped})</span>
    <span class="qcm-true-line-value">{true_line_display}</span>
  </div>
  {prediction_html}
  {conf_bar_html}
  <div class="qcm-metrics">
    <div class="qcm-metric">
      <div class="qcm-metric-val">{prob_pct}</div>
      <div class="qcm-metric-lbl">Prob</div>
    </div>
    <div class="qcm-metric">
      <div class="qcm-metric-val">{confidence:.0f}</div>
      <div class="qcm-metric-lbl">SAFE</div>
    </div>
    <div class="qcm-metric">
      <div class="qcm-metric-val">{edge_display}</div>
      <div class="qcm-metric-lbl">Edge</div>
    </div>
    {wager_html}
  </div>
  <div class="qcm-dist-row">
    <div class="qcm-dist-cell"><div class="qcm-dist-val">{p10_d}</div><div class="qcm-dist-lbl">P10</div></div>
    <div class="qcm-dist-cell qcm-dist-median"><div class="qcm-dist-val">{p50_d}</div><div class="qcm-dist-lbl">MED</div></div>
    <div class="qcm-dist-cell"><div class="qcm-dist-val">{p90_d}</div><div class="qcm-dist-lbl">P90</div></div>
    <div class="qcm-dist-cell"><div class="qcm-dist-val">{std_d}</div><div class="qcm-dist-lbl">σ</div></div>
    <div class="qcm-dist-cell qcm-dist-proj"><div class="qcm-dist-val">{proj_d}</div><div class="qcm-dist-lbl">Proj</div></div>
  </div>
  {context_grid_html}
  {forces_html}
  {breakdown_html}
  {bonus_html}
</div>"""


def compile_card_matrix(results, max_cards=None):
    """
    Compile a list of prop-analysis result dicts into a single HTML string
    with CSS Grid layout for high-capacity rendering.

    This function iterates through *all* results (or up to *max_cards* if
    specified) and wraps each in the Quantum Card HTML template. All cards
    are joined into one ``master_html_string`` for injection via a single
    ``st.markdown(html, unsafe_allow_html=True)`` call.

    Args:
        results (list[dict]): Prop analysis results from the engine.
            Each dict should contain keys like ``player_name``,
            ``stat_type``, ``prop_line``, ``tier``, ``confidence_score``,
            ``probability_over``, ``edge_percentage``, ``bet_type``,
            ``prediction``.
        max_cards (int or None): Maximum number of cards to render.
            Default None renders ALL results.

    Returns:
        str: A single HTML string containing all cards wrapped in a CSS
             Grid container, preceded by the Quantum Card Matrix CSS.
    """
    if not results:
        return (
            f"<style>{QUANTUM_CARD_MATRIX_CSS}</style>"
            '<div style="text-align:center;color:#64748b;padding:40px;">'
            "No analysis results to display.</div>"
        )

    # Render all results (or cap at max_cards when explicitly provided)
    display_results = results if max_cards is None else results[:max_cards]

    # Build all card HTML strings
    card_strings = [
        _build_single_card_html(r, idx)
        for idx, r in enumerate(display_results)
    ]

    # Join into a single master HTML string with CSS Grid wrapper
    master_html = (
        f"<style>{QUANTUM_CARD_MATRIX_CSS}</style>"
        f'<div class="qcm-grid">'
        f'{"".join(card_strings)}'
        f"</div>"
    )

    # Add count banner if results were truncated
    total = len(results)
    shown = len(display_results)
    if total > shown:
        master_html += (
            f'<div style="text-align:center;color:#64748b;font-size:0.78rem;'
            f'padding:12px 0;font-family:\'JetBrains Mono\',monospace;">'
            f"Showing top {shown} of {total} props for rendering stability."
            f"</div>"
        )

    return master_html


def build_horizontal_card_html(result, accent_color="#00f0ff"):
    """
    Build a horizontal (wide) version of the combined Quantum Card.

    This is the wide-layout variant used in sections like Best Single Bets,
    50/50 picks, and Uncertain Props.  It displays the same information as
    the vertical card but arranged in a horizontal flow: identity + metrics
    on top, distribution + forces + breakdown + context side-by-side below.

    Args:
        result (dict): A single prop analysis result from the engine.
        accent_color (str): Primary accent CSS colour for the left border.

    Returns:
        str: The HTML string for this horizontal card.
    """
    player_name = _escape(result.get("player_name", "Unknown"))
    stat_type = _escape(result.get("stat_type", ""))
    team = _escape(result.get("player_team", result.get("team", "")))
    platform = _escape(result.get("platform", ""))
    tier = result.get("tier", "Bronze")
    tier_lower = tier.lower() if tier else "bronze"

    # Prop line
    prop_line = result.get("prop_line", result.get("line", 0))
    try:
        true_line = float(prop_line)
        true_line_display = f"{true_line:g}"
    except (ValueError, TypeError):
        true_line = 0
        true_line_display = "—"

    # Confidence / probability / edge
    confidence = result.get("confidence_score", 0)
    try:
        confidence = float(confidence)
    except (ValueError, TypeError):
        confidence = 0
    prob_over = result.get("probability_over", 0)
    try:
        prob_pct = f"{float(prob_over) * 100:.1f}%"
    except (ValueError, TypeError):
        prob_pct = "—"
    edge = result.get("edge_percentage", result.get("edge", 0))
    try:
        edge_val = float(edge)
        edge_display = f"{edge_val:+.1f}%"
    except (ValueError, TypeError):
        edge_val = 0
        edge_display = "—"

    # Direction
    direction = result.get("direction", "")
    if not direction:
        try:
            direction = "OVER" if prob_over and float(prob_over) >= 0.5 else "UNDER"
        except (ValueError, TypeError):
            direction = "OVER"
    direction_escaped = _escape(direction.upper())

    # SAFE Score
    safe_score = round(min(10.0, confidence / 10.0), 1)
    safe_score_str = f"{safe_score:.1f}"

    # Prob for direction
    try:
        prob_dir = float(prob_over) if direction.upper() == "OVER" else (1.0 - float(prob_over))
    except (ValueError, TypeError):
        prob_dir = 0.5

    # Projection
    adj_proj = result.get("adjusted_projection", 0) or 0
    try:
        proj_d = f"{float(adj_proj):.1f}"
    except (ValueError, TypeError):
        proj_d = "—"

    # Headshot
    player_id = result.get("player_id", "") or ""
    if player_id:
        headshot_url = f"https://cdn.nba.com/headshots/nba/latest/1040x760/{player_id}.png"
    else:
        _raw_name = result.get("player_name", "")
        headshot_url = _get_headshot_url(_raw_name) if _raw_name else ""
    if headshot_url:
        img_html = (
            f'<img class="qcm-headshot qcm-headshot-{tier_lower}" '
            f'src="{headshot_url}" '
            f'onerror="this.onerror=null;this.src=\'{_FALLBACK_SVG}\'" '
            f'alt="{player_name}">'
        )
    else:
        img_html = (
            f'<img class="qcm-headshot qcm-headshot-{tier_lower}" '
            f'src="{_FALLBACK_SVG}" alt="{player_name}">'
        )

    # Team badge
    team_primary, _ = get_team_colors(team)
    team_badge_html = (
        f'<span class="qcm-team-badge" '
        f'style="background:{team_primary};">{team}</span>'
    ) if team else ""

    # Prop description
    _stat_emoji = {
        "points": "🏀", "rebounds": "📊", "assists": "🎯",
        "threes": "🎯", "steals": "⚡", "blocks": "🛡️", "turnovers": "❌",
    }
    stat_lower = (result.get("stat_type", "") or "").lower()
    dir_label = "More" if direction.upper() == "OVER" else "Less"
    stat_emoji = _stat_emoji.get(stat_lower, "🏀")
    prop_text = f"{stat_emoji} {dir_label} {true_line_display} {_escape(stat_type.replace('_', ' ').title())}"

    # Confidence bar
    conf_pct = max(0.0, min(100.0, confidence))
    if conf_pct >= 80:
        conf_color = "#00f0ff"
    elif conf_pct >= 65:
        conf_color = "#FFD700"
    elif conf_pct >= 50:
        conf_color = "#00b4ff"
    else:
        conf_color = "#94A3B8"

    # Distribution
    p10 = result.get("percentile_10", 0) or 0
    p50 = result.get("percentile_50", 0) or 0
    p90 = result.get("percentile_90", 0) or 0
    std_dev = result.get("simulated_std", result.get("std_dev", 0)) or 0

    def _format_dist(v):
        """Format a distribution value as '%.1f' or '—' when falsy."""
        try:
            return f"{float(v):.1f}" if v else "—"
        except (ValueError, TypeError):
            return "—"

    p10_d, p50_d, p90_d, std_d = (
        _format_dist(p10), _format_dist(p50),
        _format_dist(p90), _format_dist(std_dev),
    )

    # Forces
    forces = result.get("forces", {}) or {}
    over_forces = forces.get("over_forces", []) or []
    under_forces = forces.get("under_forces", []) or []

    def _force_items(force_list):
        if not force_list:
            return '<span class="qcm-force-none">None</span>'
        parts = []
        for f in force_list:
            if not isinstance(f, dict):
                continue
            strength = max(1, min(5, round(float(f.get("strength", 1)))))
            stars = "⭐" * strength
            name = _escape(str(f.get("name", "") or ""))
            parts.append(f'<div class="qcm-force-item">{stars} {name}</div>')
        return "".join(parts) if parts else '<span class="qcm-force-none">None</span>'

    # Breakdown bars
    breakdown = result.get("score_breakdown", {}) or {}
    bars_html = ""
    if breakdown:
        bars = []
        for factor, score in breakdown.items():
            try:
                score_f = float(score or 0)
            except (ValueError, TypeError):
                continue
            label = _escape(factor.replace("_score", "").replace("_", " ").title())
            bar_w = min(100, max(0, score_f))
            bar_c = "#00f0ff" if bar_w >= 70 else ("#ff5e00" if bar_w >= 40 else "#ff4444")
            bars.append(
                f'<div class="qcm-breakdown-row">'
                f'<span class="qcm-breakdown-label">{label}</span>'
                f'<span class="qcm-breakdown-score">{score_f:.0f}</span>'
                f'<div class="qcm-breakdown-track">'
                f'<div class="qcm-breakdown-fill" style="width:{bar_w:.1f}%;background:{bar_c};"></div>'
                f'</div></div>'
            )
        bars_html = '<div class="qcm-breakdown">' + "".join(bars) + '</div>'

    # Context metrics
    context_metrics = _build_context_metrics(result)
    ctx_cards = ""
    for ctx_label, ctx_value in context_metrics:
        ctx_cards += (
            f'<div class="qcm-context-card">'
            f'<div class="qcm-context-label">{_escape(ctx_label)}</div>'
            f'<div class="qcm-context-value">{_escape(str(ctx_value))}</div>'
            f'</div>'
        )

    # Bonus factors
    bonus_factors = _build_bonus_factors(result)
    bonus_items = ""
    for bf in bonus_factors:
        bonus_items += (
            f'<div class="qcm-bonus-item">'
            f'<span class="qcm-bonus-icon">✓</span>'
            f'<span>{_escape(bf)}</span>'
            f'</div>'
        )

    # Kelly wager
    wager_html = ""
    try:
        import streamlit as _st_mod
        from engine.odds_engine import calculate_fractional_kelly
        _dir = direction.upper() if direction else "OVER"
        _odds = (result.get("over_odds", -110)
                 if _dir == "OVER"
                 else result.get("under_odds", -110))
        _prob = prob_dir
        _bk = float(_st_mod.session_state.get("total_bankroll", 1000.0))
        _km = float(_st_mod.session_state.get("kelly_multiplier", 0.25))
        _kr = calculate_fractional_kelly(_prob, _odds, _km)
        _wager = round(_kr["fractional_kelly"] * _bk, 2)
        if _wager > 0:
            wager_html = (
                f'<div class="qcm-h-metric">'
                f'<div class="qcm-metric-val" style="color:#00C6FF;">${_wager:,.0f}</div>'
                f'<div class="qcm-metric-lbl">Wager</div>'
                f'</div>'
            )
    except Exception as exc:
        _logger.debug("build_horizontal_player_card_html: wager calc failed — %s", exc)

    # Build the full horizontal card
    return (
        f'<div class="qcm-h-card" data-tier="{_escape(tier)}" style="border-left-color:{accent_color};">'
        # ── TOP ROW: Identity + True Line + Metrics ──────────────
        f'<div class="qcm-h-top">'
        # Left: headshot + name + prop
        f'<div class="qcm-h-left">'
        f'{img_html}'
        f'<div>'
        f'<div class="qcm-identity-name">{player_name}{team_badge_html} '
        f'<span class="qcm-tier-badge qcm-tier-{tier_lower}" style="font-size:0.60rem;">{_escape(tier)}</span></div>'
        f'<div class="qcm-identity-prop">{prop_text}</div>'
        f'<div style="font-size:0.68rem;color:#64748b;margin-top:2px;">'
        f'{_escape(stat_type.replace("_", " "))} · {team}'
        + (f' · {platform}' if platform else '') +
        f'</div>'
        f'</div>'
        f'</div>'
        # Center: True Line + confidence bar
        f'<div class="qcm-h-center">'
        f'<div class="qcm-true-line-row" style="margin-bottom:6px;">'
        f'<span class="qcm-true-line-label">True Line ({direction_escaped})</span>'
        f'<span class="qcm-true-line-value">{true_line_display}</span>'
        f'</div>'
        f'<div class="qcm-conf-bar-wrap" style="margin-bottom:6px;">'
        f'<div class="qcm-conf-bar-header">'
        f'<span>Confidence</span>'
        f'<span class="qcm-conf-bar-pct" style="color:{conf_color};">{conf_pct:.0f}%</span>'
        f'</div>'
        f'<div class="qcm-conf-bar-track">'
        f'<div class="qcm-conf-bar-fill" style="width:{conf_pct}%;'
        f'background:linear-gradient(90deg,{conf_color},{conf_color}dd);"></div>'
        f'</div></div>'
        # Metrics strip
        f'<div class="qcm-h-metrics-strip">'
        f'<div class="qcm-h-metric"><div class="qcm-metric-val">{prob_pct}</div><div class="qcm-metric-lbl">Prob</div></div>'
        f'<div class="qcm-h-metric"><div class="qcm-metric-val">{confidence:.0f}</div><div class="qcm-metric-lbl">SAFE</div></div>'
        f'<div class="qcm-h-metric"><div class="qcm-metric-val">{edge_display}</div><div class="qcm-metric-lbl">Edge</div></div>'
        f'<div class="qcm-h-metric"><div class="qcm-metric-val">{proj_d}</div><div class="qcm-metric-lbl">Proj</div></div>'
        + wager_html +
        f'</div>'
        f'</div>'
        # Right: SAFE Score
        f'<div class="qcm-h-right">'
        f'<div class="qcm-safe-score">'
        f'<div class="qcm-safe-score-label">SAFE Score™</div>'
        f'<div class="qcm-safe-score-value">{safe_score_str}<span>/10</span></div>'
        f'</div>'
        f'<div class="qcm-prob-pill" style="background:{accent_color};">'
        f'P({direction_escaped.title()}): {prob_dir*100:.0f}%</div>'
        f'</div>'
        f'</div>'
        # ── BOTTOM ROW: 3-column detail ─────────────────────────
        f'<div class="qcm-h-bottom">'
        # Col 1: Distribution + Context Grid
        f'<div class="qcm-h-col">'
        f'<div class="qcm-dist-row" style="margin-bottom:6px;">'
        f'<div class="qcm-dist-cell"><div class="qcm-dist-val">{p10_d}</div><div class="qcm-dist-lbl">P10</div></div>'
        f'<div class="qcm-dist-cell qcm-dist-median"><div class="qcm-dist-val">{p50_d}</div><div class="qcm-dist-lbl">MED</div></div>'
        f'<div class="qcm-dist-cell"><div class="qcm-dist-val">{p90_d}</div><div class="qcm-dist-lbl">P90</div></div>'
        f'<div class="qcm-dist-cell"><div class="qcm-dist-val">{std_d}</div><div class="qcm-dist-lbl">σ</div></div>'
        f'<div class="qcm-dist-cell qcm-dist-proj"><div class="qcm-dist-val">{proj_d}</div><div class="qcm-dist-lbl">Proj</div></div>'
        f'</div>'
        f'<div class="qcm-context-grid">{ctx_cards}</div>'
        f'</div>'
        # Col 2: Forces
        f'<div class="qcm-h-col">'
        f'<div class="qcm-forces" style="margin-bottom:6px;">'
        f'<div class="qcm-forces-col qcm-forces-over">'
        f'<div class="qcm-forces-label">▲ OVER</div>'
        f'{_force_items(over_forces)}'
        f'</div>'
        f'<div class="qcm-forces-col qcm-forces-under">'
        f'<div class="qcm-forces-label">▼ UNDER</div>'
        f'{_force_items(under_forces)}'
        f'</div>'
        f'</div>'
        # Bonus factors
        + (
            f'<div class="qcm-bonus">'
            f'<div class="qcm-bonus-title">Key Factors</div>'
            f'{bonus_items}'
            f'</div>'
            if bonus_items else ''
        ) +
        f'</div>'
        # Col 3: Breakdown bars
        f'<div class="qcm-h-col-narrow">'
        f'{bars_html}'
        f'</div>'
        f'</div>'
        f'</div>'
    )


# =====================================================================
# Unified Expandable Player Cards
# =====================================================================

_FALLBACK_HEADSHOT = (
    "https://cdn.nba.com/headshots/nba/latest/1040x760/fallback.png"
)


def _build_unified_player_header(player_name, vitals):
    """Build the always-visible summary header for a unified player card.

    Args:
        player_name (str): Display name.
        vitals (dict): ``{"headshot_url", "position", "team",
            "next_opponent", "season_stats": {"ppg","rpg","apg"}}``.

    Returns:
        str: HTML for the ``<summary>`` content (not the tag itself).
    """
    safe_name = _escape(player_name)
    headshot = _escape(vitals.get("headshot_url", "") or "")
    position = _escape(vitals.get("position", "N/A"))
    team = _escape(vitals.get("team", "N/A"))
    opponent = _escape(vitals.get("next_opponent", "TBD"))
    stats = vitals.get("season_stats") or {}
    ppg = stats.get("ppg", 0.0)
    rpg = stats.get("rpg", 0.0)
    apg = stats.get("apg", 0.0)

    team_primary, _ = get_team_colors(team)
    team_badge = (
        f'<span class="upc-team-badge" style="background:{team_primary};">'
        f'{team}</span>'
    ) if team and team != "N/A" else ""

    return (
        f'<img class="upc-headshot" src="{headshot}" alt="{safe_name}" '
        f'onerror="this.onerror=null;this.src=\'{_FALLBACK_HEADSHOT}\'">'
        f'<div class="upc-identity">'
        f'<div class="upc-player-name">{safe_name}{team_badge}</div>'
        f'<div class="upc-subtitle">{position} · {team} vs {opponent}</div>'
        f'<div class="upc-stats">'
        f'<span class="upc-stat-pill">{ppg} PPG</span>'
        f'<span class="upc-stat-pill">{rpg} RPG</span>'
        f'<span class="upc-stat-pill">{apg} APG</span>'
        f'</div>'
        f'</div>'
    )


def build_unified_player_card_html(player_name, vitals, props,
                                   joseph_opinion=None):
    """Build a single expandable unified player card.

    The card displays a compact identity header (headshot, name, team,
    season stats) and expands on click to reveal all prop analysis cards
    for this player.  Each expanded card includes the Joseph M Smith
    avatar with an "Ask Joseph M Smith" button that toggles his
    Platinum Lock opinion inline.

    Args:
        player_name (str): Display name.
        vitals (dict): Player vitals from ``group_props_by_player``.
        props (list[dict]): Prop analysis results for this player.
        joseph_opinion (dict | None): Pre-computed Joseph opinion from
            ``joseph_platinum_lock``.  Keys: ``platinum_lock_stat``,
            ``rant``.  When *None*, the button is hidden.

    Returns:
        str: HTML ``<details>`` element for one player.
    """
    header_html = _build_unified_player_header(player_name, vitals)
    prop_count = len(props)
    prop_label = f"{prop_count} prop{'s' if prop_count != 1 else ''}"

    # Unique DOM id for this player's Joseph response panel
    _safe_id = _re.sub(r'[^a-zA-Z0-9_-]', '_', player_name)

    # Joseph M Smith avatar footer for expanded cards
    _j_b64 = _get_joseph_avatar_b64()
    if _j_b64 and joseph_opinion:
        lock_stat = _html.escape(str(joseph_opinion.get("platinum_lock_stat", "N/A")))
        rant_text = _html.escape(str(joseph_opinion.get("rant", "")))
        joseph_html = (
            f'<div class="upc-joseph-row" onclick="toggleJoseph(\'{_safe_id}\')">'
            f'<img class="upc-joseph-avatar" '
            f'src="data:image/png;base64,{_j_b64}" alt="Joseph M Smith">'
            f'<span class="upc-joseph-label">🎙️ Ask Joseph M Smith</span>'
            f'</div>'
            f'<div id="joseph-resp-{_safe_id}" class="upc-joseph-response" style="display:none;">'
            f'<div class="upc-joseph-resp-header">'
            f'<img class="upc-joseph-resp-avatar" '
            f'src="data:image/png;base64,{_j_b64}" alt="Joseph M Smith">'
            f'<div class="upc-joseph-resp-title">'
            f'<span class="upc-joseph-resp-name">Joseph M. Smith</span>'
            f'<span class="upc-joseph-resp-role">🟢 LIVE — NBA Analyst</span>'
            f'</div>'
            f'</div>'
            f'<div class="upc-joseph-resp-lock">💎 PLATINUM LOCK: {lock_stat}</div>'
            f'<div class="upc-joseph-resp-rant">{rant_text}</div>'
            f'</div>'
        )
    elif _j_b64:
        joseph_html = (
            '<div class="upc-joseph-row" style="opacity:0.5;cursor:default;">'
            f'<img class="upc-joseph-avatar" '
            f'src="data:image/png;base64,{_j_b64}" alt="Joseph M Smith">'
            '<span class="upc-joseph-label">🎙️ Ask Joseph M Smith</span>'
            '</div>'
        )
    else:
        joseph_html = ""

    # Build individual prop cards inside the body
    card_strings = [_build_single_card_html(p, i) for i, p in enumerate(props)]
    body_html = (
        f'<div class="upc-body">'
        f'<div class="qcm-grid">{"".join(card_strings)}</div>'
        f'{joseph_html}'
        f'</div>'
    )

    return (
        f'<details class="upc-card">'
        f'<summary>'
        f'{header_html}'
        f'<div class="upc-summary-right">'
        f'<span class="upc-prop-count">{prop_label}</span>'
        f'<span class="upc-chevron">▼</span>'
        f'</div>'
        f'</summary>'
        f'{body_html}'
        f'</details>'
    )


def compile_unified_card_matrix(grouped_players, joseph_opinions=None):
    """Compile all players into a unified expandable card matrix.

    Each player gets one expandable card containing their identity header
    and all associated prop analysis cards.

    Args:
        grouped_players (dict): Mapping of ``player_name`` →
            ``{"vitals": dict, "props": list[dict]}`` from
            :func:`utils.data_grouper.group_props_by_player`.
        joseph_opinions (dict | None): Mapping of ``player_name`` →
            ``{"platinum_lock_stat": str, "rant": str}`` from
            :func:`engine.joseph_brain.joseph_platinum_lock`.

    Returns:
        str: A single HTML string with all unified player cards,
             preceded by both the QCM and Unified Player Card CSS.
    """
    if not grouped_players:
        return (
            f"<style>{QUANTUM_CARD_MATRIX_CSS}{UNIFIED_PLAYER_CARD_CSS}</style>"
            '<div style="text-align:center;color:#64748b;padding:40px;">'
            "No analysis results to display.</div>"
        )

    opinions = joseph_opinions or {}
    cards = []
    for name, data in grouped_players.items():
        vitals = data.get("vitals", {})
        props = data.get("props", [])
        if props:
            cards.append(build_unified_player_card_html(
                name, vitals, props,
                joseph_opinion=opinions.get(name),
            ))

    # JavaScript toggle function for Ask Joseph M Smith response panels.
    _toggle_js = (
        "<script>"
        "function toggleJoseph(id){"
        "var p=document.getElementById('joseph-resp-'+id);"
        "if(p){p.style.display=p.style.display==='none'?'block':'none';}"
        "}"
        "</script>"
    )

    return (
        f"<style>{QUANTUM_CARD_MATRIX_CSS}{UNIFIED_PLAYER_CARD_CSS}</style>"
        f'<div class="upc-grid">{"".join(cards)}</div>'
        f'{_toggle_js}'
    )
