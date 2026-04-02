# ============================================================
# FILE: pages/5b_🔮_Player_Simulator.py
# PURPOSE: Player Simulator — run Quantum Matrix Engine 5.6 simulations for
#          selected players to project their full stat line and
#          surface "dark horse" upside picks for tonight's slate.
# CONNECTS TO: engine/ (simulation, projections), data_manager.py
# ============================================================

import streamlit as st
import html as _html
import logging as _logging

try:
    from utils.logger import get_logger as _get_logger
    _logger = _get_logger(__name__)
except ImportError:
    _logger = _logging.getLogger(__name__)

from data.data_manager import (
    load_players_data,
    load_teams_data,
    load_defensive_ratings_data,
)
from engine.projections import build_player_projection, get_stat_standard_deviation
from engine.simulation import run_quantum_matrix_simulation

# ─── Page config ────────────────────────────────────────────
st.set_page_config(
    page_title="Player Simulator — SmartBetPro NBA",
    page_icon="🔮",
    layout="wide",
)

from styles.theme import get_global_css, get_qds_css
st.markdown(get_global_css(), unsafe_allow_html=True)
st.markdown(get_qds_css(), unsafe_allow_html=True)

# ── Joseph M. Smith Hero Banner & Floating Widget ─────────────
from utils.components import render_joseph_hero_banner, inject_joseph_floating
st.session_state["joseph_page_context"] = "page_simulator"
inject_joseph_floating()

# ── Premium Gate ───────────────────────────────────────────────
from utils.premium_gate import premium_gate
if not premium_gate("Player Simulator"):
    st.stop()
# ── End Premium Gate ───────────────────────────────────────────

# ─── Header ─────────────────────────────────────────────────
st.markdown(
    '<h2 style="font-family:\'Orbitron\',sans-serif;color:#00ffd5;margin-bottom:4px;">'
    '🔮 Player Simulator</h2>'
    '<p style="color:#a0b4d0;margin-top:0;">'
    'Select active players and simulate their full stat line for tonight\'s game. '
    'Identifies <strong style="color:#ff5e00;">dark horse</strong> upside picks automatically.</p>',
    unsafe_allow_html=True,
)

with st.expander("📖 How to Use the Player Simulator", expanded=False):
    st.markdown("""
### What This Page Does
The Player Simulator runs the **Quantum Matrix Engine 5.6** Monte Carlo
simulation for any player you select, projecting their **full stat line**
(points, rebounds, assists, steals, blocks, turnovers, and threes) for
tonight's game.  It automatically flags **dark horse** upside picks —
stat categories where a player's simulated ceiling is meaningfully above
the market line.

### Step-by-Step Workflow
1. **Load Tonight's Games** — visit **📡 Live Games** first and click
   *Auto-Load Tonight's Games* so the simulator knows which players are active.
   If you skip this step, all players in the database will appear — but
   projections will be less accurate without tonight's game context.
2. **Select a Player** — use the sidebar dropdown to pick a player from
   tonight's slate.
3. **Review Context** — the simulator auto-detects the opponent, home/away
   status, game total, and spread for accurate projections.
4. **Run Simulation** — click **Simulate** to generate thousands of
   Monte Carlo iterations per stat category.
5. **Read Results** — each stat shows a distribution chart plus summary
   metrics (median, mean, floor, ceiling, standard deviation).

### Understanding the Output
| Metric | Meaning |
|--------|---------|
| **Median** | The 50th-percentile outcome — what the player hits "most nights" |
| **Mean** | Average across all simulated outcomes (slightly different from median if skewed) |
| **Floor (10th pctile)** | The low-end — what the player produces on a bad night |
| **Ceiling (90th pctile)** | The high-end — what the player hits when everything clicks |
| **Std Dev** | How much the outcome varies night-to-night; higher = more volatile |
| **🟠 Dark Horse** | The ceiling is well above the current prop line — upside opportunity |

### Tips
- 💡 Use the simulator to **pressure-test a prop** before locking it — if the
  median is near the line, the bet is close to a coin flip.
- 💡 Players with **high standard deviation** are better OVER plays; low-variance
  players are safer UNDER plays.
- 💡 The simulator accounts for opponent defensive rating, pace, and game total —
  results change game-to-game even for the same player.
- 💡 Compare simulator output to the lines on **🔬 Prop Scanner** or **⚡ Quantum
  Analysis Matrix** to spot discrepancies.
""")

# ─── Load data ───────────────────────────────────────────────
players_data = load_players_data()
teams_data = load_teams_data()
defensive_ratings_data = load_defensive_ratings_data()
todays_games = st.session_state.get("todays_games", [])

# ─── Derive tonight's playing teams ─────────────────────────
playing_teams: set = set()
for game in todays_games:
    playing_teams.add(game.get("home_team", "").upper())
    playing_teams.add(game.get("away_team", "").upper())
playing_teams.discard("")

if playing_teams:
    tonight_players = [p for p in players_data if p.get("team", "").upper() in playing_teams]
else:
    tonight_players = players_data  # Fallback — no games loaded yet

if not tonight_players:
    st.warning(
        "⚠️ No player data loaded. Go to **📡 Live Games** and click "
        "**Auto-Load Tonight's Games** first."
    )
    st.stop()

if not playing_teams:
    st.warning(
        "⚠️ No tonight's games configured. Go to **📡 Live Games** and load tonight's slate "
        "to restrict simulation to active players only."
    )

# ─── Helper: find opponent for a player ─────────────────────
def _find_opponent(team: str, games: list) -> str:
    for g in games:
        if g.get("home_team", "").upper() == team.upper():
            return g.get("away_team", "")
        if g.get("away_team", "").upper() == team.upper():
            return g.get("home_team", "")
    return ""


def _find_game_context(team: str, games: list) -> dict:
    try:
        from data.player_profile_service import _TEAM_ABBREV_TO_ID as _TID
    except Exception:
        _TID = {}

    for g in games:
        home = g.get("home_team", "").upper()
        away = g.get("away_team", "").upper()

        _vs_raw = g.get("vegas_spread")
        try:
            _vs_val = float(_vs_raw) if _vs_raw is not None else 0.0
        except (TypeError, ValueError):
            _vs_val = 0.0
        _gt_raw = g.get("game_total")
        try:
            _gt_val = float(_gt_raw) if _gt_raw is not None else 220.0
        except (TypeError, ValueError):
            _gt_val = 220.0

        game_id = str(g.get("game_id") or g.get("GAME_ID") or "")
        home_team_id = g.get("home_team_id") or g.get("HOME_TEAM_ID") or _TID.get(home)
        away_team_id = g.get("away_team_id") or g.get("VISITOR_TEAM_ID") or _TID.get(away)

        if team.upper() == home:
            return {
                "opponent": g.get("away_team", ""),
                "is_home": True,
                "game_total": _gt_val,
                "vegas_spread": _vs_val,
                "rest_days": 2,
                "game_id": game_id,
                "home_team": home,
                "away_team": away,
                "home_team_id": home_team_id,
                "away_team_id": away_team_id,
            }
        if team.upper() == away:
            return {
                "opponent": g.get("home_team", ""),
                "is_home": False,
                "game_total": _gt_val,
                "vegas_spread": -_vs_val,
                "rest_days": 2,
                "game_id": game_id,
                "home_team": home,
                "away_team": away,
                "home_team_id": home_team_id,
                "away_team_id": away_team_id,
            }
    return {
        "opponent": "", "is_home": True, "game_total": 220.0, "vegas_spread": 0.0,
        "rest_days": 2, "game_id": "", "home_team": "", "away_team": "",
        "home_team_id": None, "away_team_id": None,
    }


# ─── Stat types to simulate ──────────────────────────────────
_STAT_TYPES = ["points", "rebounds", "assists", "threes", "steals", "blocks", "turnovers"]
_STAT_EMOJI = {
    "points": "🏀", "rebounds": "📊", "assists": "🎯",
    "threes": "🎯", "steals": "⚡", "blocks": "🛡️", "turnovers": "❌",
}

# ─── Controls ────────────────────────────────────────────────
player_names_sorted = sorted(p.get("name", "") for p in tonight_players if p.get("name"))

col_select, col_depth, col_dark_horse = st.columns([3, 1, 1])
with col_select:
    selected_names = st.multiselect(
        "Select players to simulate",
        player_names_sorted,
        max_selections=10,
        placeholder="Search for a player…",
    )
with col_depth:
    sim_depth = st.slider("Simulation depth", min_value=500, max_value=5000, value=2000, step=500)
with col_dark_horse:
    run_dark_horse = st.button(
        "🌑 Dark Horse Finder",
        width="stretch",
        help="Scan ALL tonight's players and rank by upside vs season average",
    )

# ── Mode toggle ────────────────────────────────────────────────
_sim_mode = st.radio(
    "Mode",
    options=["🔮 Standard Simulation", "📊 Compare Mode", "🎛️ Scenario Builder"],
    horizontal=True,
    key="sim_mode_radio",
)
_compare_mode = "Compare" in _sim_mode
_scenario_mode = "Scenario" in _sim_mode

# ── Scenario Builder controls ──────────────────────────────────
_scenario_overrides: dict = {}
if _scenario_mode and selected_names:
    with st.expander("🎛️ Scenario Builder — Adjust Game Parameters", expanded=True):
        st.markdown(
            "Override the default game environment for this simulation. "
            "Useful for 'what if' analysis (e.g., if the pace is faster, or a star opponent is out)."
        )
        _sc1, _sc2, _sc3, _sc4 = st.columns(4)
        with _sc1:
            _sce_total = st.slider("Game O/U Total", 195.0, 260.0, 220.0, 0.5, key="sce_total")
        with _sc2:
            _sce_spread = st.slider("Home Spread", -20.0, 20.0, 0.0, 0.5, key="sce_spread")
        with _sc3:
            _sce_rest = st.selectbox("Rest Days", [0, 1, 2, 3, 4], index=2, key="sce_rest")
        with _sc4:
            _sce_def_adj = st.slider("Opponent Defense Adj (%)", -30, 30, 0, 1, key="sce_def")
        _scenario_overrides = {
            "game_total": _sce_total,
            "vegas_spread": _sce_spread,
            "rest_days": _sce_rest,
            "def_adj": _sce_def_adj / 100.0,
        }
        st.caption(
            f"Scenario: O/U {_sce_total} | Spread {_sce_spread:+.1f} | "
            f"Rest {_sce_rest}d | Def {_sce_def_adj:+d}%"
        )

_sim_btn_col1, _sim_btn_col2 = st.columns([2, 3])
with _sim_btn_col1:
    run_sim = st.button(
        "🚀 Run Simulation",
        type="primary",
        use_container_width=True,
        disabled=not selected_names,
    )
with _sim_btn_col2:
    _load_logs_btn = st.button(
        "🔄 Get Game Logs",
        use_container_width=True,
        help="Load the last 20 games per player for more accurate simulation",
        disabled=not selected_names,
    )

# ── On-demand API-NBA game log retrieval ──────────────────────
if _load_logs_btn and selected_names:
    _gl_progress = st.progress(0, text="Getting game logs from API-NBA…")
    _gl_loaded = 0
    _gl_errors  = 0
    for _gl_idx, _gl_pname in enumerate(selected_names):
        _gl_pdata = next(
            (p for p in tonight_players if p.get("name") == _gl_pname), None
        )
        _gl_player_id = _gl_pdata.get("player_id", "") if _gl_pdata else ""
        _gl_progress.progress(
            (_gl_idx + 1) / len(selected_names),
            text=f"Getting logs for {_gl_pname}…",
        )
        if _gl_player_id:
            try:
                from data.nba_data_service import get_player_game_log as _ldf_gl
                from data.nba_data_service import get_player_recent_form as _ldf_form
                from data.game_log_cache import save_game_logs_to_cache as _gl_save
                _logs = _ldf_gl(_gl_player_id, last_n_games=20)
                if _logs:
                    _gl_save(_gl_pname, _logs)
                    _gl_loaded += 1
                # Also get recent form trend for the player
                try:
                    _form = _ldf_form(_gl_player_id, last_n_games=10)
                    if _form and _gl_pdata:
                        _gl_pdata["recent_form_games"] = _form.get("game_results", [])
                        _gl_pdata["recent_trend"] = _form.get("trend", "neutral")
                        _gl_pdata["recent_trend_emoji"] = _form.get("trend_emoji", "➡️")
                except Exception as _form_exc:
                    _logger.warning("Recent form request failed for %s: %s", _gl_pname, _form_exc)
            except Exception as _gl_exc:
                _logger.warning("Game log request failed for %s: %s", _gl_pname, _gl_exc)
                _gl_errors += 1
        else:
            _gl_errors += 1
    _gl_progress.empty()
    if _gl_loaded:
        st.success(
            f"✅ Game logs loaded for **{_gl_loaded}** player(s). "
            "Re-run simulation to use the fresh data."
        )
    if _gl_errors:
        st.warning(
            f"⚠️ Could not load logs for {_gl_errors} player(s) — "
            "player IDs may be missing. Run a Smart Update on the Data Feed page first."
        )

st.divider()


# ─── Simulation runner ───────────────────────────────────────
def _simulate_player(player_data: dict, sim_depth: int, todays_games: list,
                     scenario_overrides: dict | None = None) -> dict:
    """Run simulation for all stat types for one player."""
    team = player_data.get("team", "")
    ctx = _find_game_context(team, todays_games)
    # Apply scenario overrides if provided
    if scenario_overrides:
        ctx = dict(ctx)
        for k in ("game_total", "vegas_spread", "rest_days"):
            if k in scenario_overrides:
                ctx[k] = scenario_overrides[k]
    results = {}
    for stat in _STAT_TYPES:
        # Look up player estimated metrics from Deep Fetch enrichment (if available)
        _sim_adv_ctx: dict | None = None
        try:
            _sim_enr = st.session_state.get("advanced_enrichment", {}).get(
                ctx.get("game_id", ""), {}
            )
            _sim_metrics = _sim_enr.get("player_metrics", [])
            _sim_pid = player_data.get("player_id") or player_data.get("id")
            _sim_pname = str(player_data.get("name", "")).lower()
            for _mm in _sim_metrics:
                _mmid = _mm.get("PLAYER_ID") or _mm.get("playerId")
                _mmname = str(_mm.get("PLAYER_NAME") or _mm.get("playerName") or "").lower()
                if (_sim_pid and _mmid and int(_sim_pid) == int(_mmid)) or (
                    _sim_pname and _mmname and _sim_pname in _mmname
                ):
                    _usg = _mm.get("E_USG_PCT") or _mm.get("USG_PCT") or _mm.get("usage_pct")
                    if _usg is not None:
                        try:
                            _usg_f = float(_usg)
                            _sim_adv_ctx = {"usage_pct": _usg_f / 100.0 if _usg_f > 1.0 else _usg_f}
                        except (TypeError, ValueError):
                            pass
                    break
        except Exception:
            pass

        projection = build_player_projection(
            player_data=player_data,
            opponent_team_abbreviation=ctx.get("opponent", ""),
            is_home_game=ctx.get("is_home", True),
            rest_days=ctx.get("rest_days", 2),
            game_total=ctx.get("game_total", 220.0),
            defensive_ratings_data=defensive_ratings_data,
            teams_data=teams_data,
            vegas_spread=ctx.get("vegas_spread", 0.0),
            advanced_context=_sim_adv_ctx,
        )
        projected_val = projection.get(
            f"projected_{stat}",
            float(player_data.get(f"{stat}_avg", 0) or 0),
        )
        # Apply scenario defense adjustment
        if scenario_overrides and "def_adj" in scenario_overrides:
            projected_val = projected_val * (1.0 + scenario_overrides["def_adj"])
        stat_std = get_stat_standard_deviation(player_data, stat)
        sim_out = run_quantum_matrix_simulation(
            projected_stat_average=projected_val,
            stat_standard_deviation=stat_std,
            prop_line=projected_val,  # Use projected as line for percentile calculation
            number_of_simulations=sim_depth,
            blowout_risk_factor=projection.get("blowout_risk", 0.15),
            pace_adjustment_factor=projection.get("pace_factor", 1.0),
            matchup_adjustment_factor=projection.get("defense_factor", 1.0),
            home_away_adjustment=projection.get("home_away_factor", 0.0),
            rest_adjustment_factor=projection.get("rest_factor", 1.0),
            stat_type=stat,
            game_context=ctx if ctx.get("game_id") else None,
        )
        season_avg = float(player_data.get(f"{stat}_avg", 0) or 0)
        p90 = sim_out.get("percentile_90", projected_val)
        upside_ratio = (p90 / season_avg) if season_avg > 0.5 else 1.0
        results[stat] = {
            "projected": round(sim_out.get("simulated_mean", projected_val), 1),
            "p10": round(sim_out.get("percentile_10", 0), 1),
            "p50": round(sim_out.get("percentile_50", projected_val), 1),
            "p90": round(p90, 1),
            "season_avg": round(season_avg, 1),
            "upside_ratio": round(upside_ratio, 2),
        }
    return {
        "player": player_data,
        "context": ctx,
        "stats": results,
    }


def _render_betting_recommendations(sim_result: dict, is_dark_horse: bool = False):
    """Render a Betting Recommendations section based on simulation output.

    Compares projected values against the season average (used as a proxy
    for typical prop lines) and shows suggested OVER/UNDER picks with the
    projected edge for each stat.  For dark horse players it also explains
    WHY they are flagged as a dark horse opportunity.
    """
    player_data = sim_result["player"]
    ctx = sim_result["context"]
    stats = sim_result["stats"]
    player_name = player_data.get("name", "")

    # Build a lookup of live platform prop lines from session state.
    # current_props is a list of prop dicts with keys: player_name, stat_type, line, platform.
    # We index by (normalized_player_name, stat_type) for fast lookup.
    live_props_lookup = {}
    for prop in st.session_state.get("current_props", []):
        prop_player = prop.get("player_name", "").strip().lower()
        prop_stat = prop.get("stat_type", "").strip().lower()
        if prop_player and prop_stat:
            key = (prop_player, prop_stat)
            # Prefer the first (highest-priority) match; do not overwrite
            if key not in live_props_lookup:
                live_props_lookup[key] = prop

    rec_rows = ""
    for stat in _STAT_TYPES:
        s = stats.get(stat, {})
        projected = s.get("projected", 0)
        season_avg = s.get("season_avg", 0)
        p10 = s.get("p10", 0)
        p90 = s.get("p90", 0)
        upside = s.get("upside_ratio", 1.0)

        if season_avg < 0.5:
            continue  # Skip stats where the player has essentially no production

        # ── Resolve prop line: live platform data > season-avg proxy ──
        live_key = (player_name.strip().lower(), stat.strip().lower())
        live_prop = live_props_lookup.get(live_key)
        if live_prop is not None:
            prop_line = float(live_prop.get("line", 0))
            live_platform = live_prop.get("platform", "")
        else:
            prop_line = round(season_avg * 2) / 2  # Round to nearest 0.5 (typical book format)
            live_platform = ""

        if prop_line <= 0:
            continue  # Guard against bad line values
        edge_pct = round((projected - prop_line) / max(prop_line, 0.1) * 100, 1)
        direction = "OVER" if projected >= prop_line else "UNDER"
        dir_color = "#00ff9d" if direction == "OVER" else "#ff5e00"
        edge_label = f"{edge_pct:+.1f}%"
        emoji = _STAT_EMOJI.get(stat, "📊")

        # Only show meaningful edges (≥ 3%)
        if abs(edge_pct) < 3.0:
            continue

        upside_tag = ""
        if upside >= 1.5:
            upside_tag = (
                '<span style="background:#ff5e00;color:#0a0f1a;padding:1px 6px;'
                'border-radius:3px;font-size:0.72rem;font-weight:700;margin-left:6px;">'
                '🌑 DARK HORSE UPSIDE</span>'
            )

        rec_rows += (
            f'<tr style="border-bottom:1px solid rgba(255,255,255,0.05);">'
            f'<td style="padding:6px 8px;color:#c0d0e8;">{emoji} {_html.escape(stat.replace("_", " ").title())}</td>'
            f'<td style="padding:6px 8px;color:#ff5e00;font-weight:700;">{projected}</td>'
            f'<td style="padding:6px 8px;color:#8a9bb8;">{prop_line}'
            + (f'<br><span style="color:#5a8fa8;font-size:0.72rem;">{_html.escape(live_platform)}</span>' if live_platform else
               '<br><span style="color:#5a5a5a;font-size:0.72rem;">est.</span>')
            + f'</td>'
            f'<td style="padding:6px 8px;color:{dir_color};font-weight:700;">'
            f'{direction} {upside_tag}</td>'
            f'<td style="padding:6px 8px;color:{dir_color};">{edge_label}</td>'
            f'<td style="padding:6px 8px;color:#8a9bb8;font-size:0.8rem;">'
            f'{p10}–{p90}</td>'
            f'</tr>'
        )

    if not rec_rows:
        return  # Nothing actionable to show

    # Dark horse explanation block
    dark_horse_explain = ""
    if is_dark_horse:
        opponent = ctx.get("opponent", "?")
        game_total = ctx.get("game_total", 220)
        is_home = ctx.get("is_home", True)
        loc_label = "home" if is_home else "away"
        dark_horse_stats = [
            stat for stat in _STAT_TYPES
            if stats.get(stat, {}).get("upside_ratio", 0) >= 1.5
            and stats.get(stat, {}).get("season_avg", 0) > 0.5
        ]
        dh_stat_str = ", ".join(s.replace("_", " ").title() for s in dark_horse_stats)
        dark_horse_explain = (
            f'<div style="margin-bottom:12px;padding:10px 14px;'
            f'background:rgba(255,94,0,0.1);border-radius:6px;border-left:3px solid #ff5e00;">'
            f'<div style="color:#ff5e00;font-weight:700;font-size:0.88rem;margin-bottom:4px;">'
            f'🌑 Why Dark Horse?</div>'
            f'<div style="color:#c0d0e8;font-size:0.84rem;line-height:1.6;">'
            f'{_html.escape(player_data.get("name", ""))} is flagged as a <strong style="color:#ff5e00;">dark horse</strong> '
            f'for {dh_stat_str or "key stats"} — the 90th-percentile projection is ≥ 1.5× the season average. '
            f'Playing <em>{loc_label}</em> vs <strong>{_html.escape(opponent)}</strong> '
            f'in a game with an O/U of <strong>{game_total:.0f}</strong>. '
            f'This combination of matchup and pace context creates significant upside risk.</div>'
            f'</div>'
        )

    html_out = (
        f'<div style="background:#0f1424;border-radius:8px;padding:16px 18px;'
        f'margin-top:6px;margin-bottom:16px;border-top:2px solid #00ff9d;">'
        + dark_horse_explain +
        f'<div style="overflow-x:auto;">'
        f'<table style="width:100%;border-collapse:collapse;font-size:0.86rem;">'
        f'<thead><tr style="border-bottom:1px solid rgba(0,255,157,0.3);">'
        f'<th style="padding:6px 8px;text-align:left;color:#00ff9d;">Stat</th>'
        f'<th style="padding:6px 8px;text-align:left;color:#ff5e00;">Projected</th>'
        f'<th style="padding:6px 8px;text-align:left;color:#8a9bb8;">Prop Line</th>'
        f'<th style="padding:6px 8px;text-align:left;color:#00ff9d;">Pick</th>'
        f'<th style="padding:6px 8px;text-align:left;color:#c0d0e8;">Edge</th>'
        f'<th style="padding:6px 8px;text-align:left;color:#8a9bb8;">10–90th Range</th>'
        f'</tr></thead>'
        f'<tbody>{rec_rows}</tbody>'
        f'</table></div>'
        f'<div style="margin-top:10px;font-size:0.75rem;color:#8a9bb8;">'
        f'ℹ️ Methodology: projections based on Quantum Matrix Engine 5.6 simulation using '
        f'matchup-adjusted season averages, pace, rest, and home/away factors. '
        f'Prop line ≈ season average rounded to nearest 0.5.</div>'
        f'</div>'
    )
    _exp_label = (
        f"💡 Betting Recommendations — {player_name}" if player_name
        else "💡 Betting Recommendations"
    )
    with st.expander(_exp_label, expanded=True):
        st.markdown(html_out, unsafe_allow_html=True)


def _render_sim_card(sim_result: dict):
    """Render a styled simulation result card for one player."""
    player_data = sim_result["player"]
    ctx = sim_result["context"]
    stats = sim_result["stats"]

    player_name = player_data.get("name", "")
    team = player_data.get("team", "")
    opponent = ctx.get("opponent", "?")
    player_id = player_data.get("player_id", "")

    # Dark horse check: any stat with upside_ratio ≥ 1.5 (90th pct ≥ 150% of season avg)
    is_dark_horse = any(
        s["upside_ratio"] >= 1.5 for s in stats.values() if s["season_avg"] > 0.5
    )

    headshot_url = (
        f"https://cdn.nba.com/headshots/nba/latest/1040x760/{player_id}.png"
        if player_id else ""
    )
    fallback_svg = (
        "data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' "
        "width='60' height='60' viewBox='0 0 60 60'%3E"
        "%3Ccircle cx='30' cy='30' r='30' fill='%23141a2d'/%3E"
        "%3Ccircle cx='30' cy='22' r='10' fill='%23a0b4d0'/%3E"
        "%3Cellipse cx='30' cy='50' rx='16' ry='12' fill='%23a0b4d0'/%3E"
        "%3C/svg%3E"
    )
    img_src = headshot_url if headshot_url else fallback_svg
    safe_name = _html.escape(player_name)
    safe_team = _html.escape(team)
    safe_opp = _html.escape(opponent)

    dark_horse_badge = (
        '<span style="background:#ff5e00;color:#0a0f1a;padding:3px 10px;border-radius:4px;'
        'font-size:0.78rem;font-weight:700;margin-left:8px;">🌑 DARK HORSE</span>'
        if is_dark_horse else ""
    )

    # Build stat rows
    stat_rows = ""
    for stat in _STAT_TYPES:
        s = stats.get(stat, {})
        proj = s.get("projected", 0)
        p10 = s.get("p10", 0)
        p50 = s.get("p50", 0)
        p90 = s.get("p90", 0)
        avg = s.get("season_avg", 0)
        ratio = s.get("upside_ratio", 1.0)
        emoji = _STAT_EMOJI.get(stat, "📊")
        # Upside highlight
        upside_color = "#ff5e00" if ratio >= 1.5 else ("#00ff9d" if ratio >= 1.2 else "#c0d0e8")
        stat_label = _html.escape(stat.replace("_", " ").title())
        stat_rows += (
            f'<tr style="border-bottom:1px solid rgba(255,255,255,0.05);">'
            f'<td style="padding:6px 8px;color:#c0d0e8;">{emoji} {stat_label}</td>'
            f'<td style="padding:6px 8px;color:{upside_color};font-weight:700;">{proj}</td>'
            f'<td style="padding:6px 8px;color:#8a9bb8;">{p10}</td>'
            f'<td style="padding:6px 8px;color:#c0d0e8;">{p50}</td>'
            f'<td style="padding:6px 8px;color:#00ff9d;">{p90}</td>'
            f'<td style="padding:6px 8px;color:#8a9bb8;">{avg}</td>'
            f'</tr>'
        )

    card_html = f"""
<div style="background:#14192b;border-radius:8px;padding:20px;margin-bottom:20px;
            border-top:3px solid #ff5e00;">
    <div style="display:flex;align-items:center;gap:12px;margin-bottom:16px;">
        <img src="{img_src}"
             onerror="this.onerror=null;this.src='{fallback_svg}'"
             style="width:64px;height:64px;border-radius:50%;object-fit:cover;
                    border:2px solid #ff5e00;flex-shrink:0;"
             alt="{safe_name}">
        <div>
            <div style="font-family:'Orbitron',sans-serif;color:#ff5e00;font-size:1.1rem;font-weight:700;">
                {safe_name}
                <span style="font-size:0.8rem;background:rgba(255,255,255,0.1);padding:2px 6px;
                             border-radius:4px;color:white;font-family:inherit;">{safe_team}</span>
                {dark_horse_badge}
            </div>
            <div style="color:#c0d0e8;font-size:0.9rem;margin-top:4px;">
                vs <strong style="color:white;">{safe_opp}</strong>
                {'&nbsp;🏠 Home' if ctx.get('is_home') else '&nbsp;✈️ Away'}
                &nbsp;| Game Total: {ctx.get('game_total', 220):.0f}
            </div>
        </div>
    </div>
    <div style="overflow-x:auto;">
        <table style="width:100%;border-collapse:collapse;font-size:0.88rem;">
            <thead>
                <tr style="border-bottom:1px solid rgba(255,94,0,0.4);">
                    <th style="padding:6px 8px;text-align:left;color:#ff5e00;">Stat</th>
                    <th style="padding:6px 8px;text-align:left;color:#ff5e00;">Projected</th>
                    <th style="padding:6px 8px;text-align:left;color:#8a9bb8;">10th%</th>
                    <th style="padding:6px 8px;text-align:left;color:#c0d0e8;">Median</th>
                    <th style="padding:6px 8px;text-align:left;color:#00ff9d;">90th%</th>
                    <th style="padding:6px 8px;text-align:left;color:#8a9bb8;">Season Avg</th>
                </tr>
            </thead>
            <tbody>
                {stat_rows}
            </tbody>
        </table>
    </div>
</div>
"""
    st.markdown(card_html, unsafe_allow_html=True)


# ─── Run simulation for selected players ────────────────────
if run_sim and selected_names:
    _mode_label = " (Scenario)" if _scenario_mode else (" (Compare)" if _compare_mode else "")
    st.subheader(f"📊 Simulation Results — {len(selected_names)} Player(s){_mode_label}")

    try:
        with st.spinner("🔮 Running Quantum Matrix Engine 5.6 simulations…"):
            _all_sim_results = []
            for pname in selected_names:
                pdata = next((p for p in tonight_players if p.get("name") == pname), None)
                if pdata is None:
                    st.warning(f"⚠️ Could not find data for **{pname}**.")
                    continue
                sim_result = _simulate_player(
                    pdata, sim_depth, todays_games,
                    scenario_overrides=_scenario_overrides if _scenario_mode else None,
                )
                _all_sim_results.append(sim_result)
    except Exception as _sim_err:
        _sim_err_str = str(_sim_err)
        if "WebSocketClosedError" not in _sim_err_str and "StreamClosedError" not in _sim_err_str:
            st.error(f"❌ Simulation failed: {_sim_err}")
        _all_sim_results = []

    if _compare_mode and len(_all_sim_results) >= 2:
        # ── Compare Mode: side-by-side table for all selected players ──
        st.markdown("**📊 Compare Mode — Side-by-Side Stat Projections**")
        _cmp_header = ["Stat"] + [r["player"].get("name", "") for r in _all_sim_results]
        _cmp_rows = []
        for _stat in _STAT_TYPES:
            _row = {"Stat": f"{_STAT_EMOJI.get(_stat, '📊')} {_stat.title()}"}
            for _r in _all_sim_results:
                _s = _r["stats"].get(_stat, {})
                _row[_r["player"].get("name", "")] = (
                    f"{_s.get('projected', 0)} "
                    f"({_s.get('p10', 0)}–{_s.get('p90', 0)})"
                )
            _cmp_rows.append(_row)
        st.dataframe(_cmp_rows, width="stretch", hide_index=True)
        st.caption("Format: Projected (10th pct – 90th pct)")

        # Also render individual cards
        for sim_result in _all_sim_results:
            _render_sim_card(sim_result)
            _is_dh = any(
                s["upside_ratio"] >= 1.5
                for s in sim_result["stats"].values()
                if s["season_avg"] > 0.5
            )
            _render_betting_recommendations(sim_result, is_dark_horse=_is_dh)
    else:
        # Standard / Scenario: render full cards + game log overlay
        for sim_result in _all_sim_results:
            _render_sim_card(sim_result)
            _is_dh = any(
                s["upside_ratio"] >= 1.5
                for s in sim_result["stats"].values()
                if s["season_avg"] > 0.5
            )
            _render_betting_recommendations(sim_result, is_dark_horse=_is_dh)

            # ── Historical Game Log Overlay ────────────────────────────
            _pdata = sim_result["player"]
            _pname_log = _pdata.get("name", "")
            _recent_games = _pdata.get("recent_form_games", [])

            # ── Recent Form Trend Badge ────────────────────────────────
            _trend = _pdata.get("recent_trend", "")
            _trend_emoji = _pdata.get("recent_trend_emoji", "")
            if _trend and _trend != "neutral":
                _trend_colors = {"hot": "#00ff9d", "cold": "#ff6b6b"}
                _tc = _trend_colors.get(_trend, "#8b949e")
                st.markdown(
                    f'<div style="display:inline-block;background:rgba(0,0,0,0.3);'
                    f'border:1px solid {_tc};border-radius:6px;padding:4px 12px;'
                    f'font-size:0.82rem;margin-bottom:8px;">'
                    f'{_trend_emoji} <strong style="color:{_tc};">'
                    f'{_pname_log} is {_trend.upper()}</strong>'
                    f' — Recent form trend based on last 10 games'
                    f'</div>',
                    unsafe_allow_html=True,
                )

            # Also check game log cache (populated by historical data refresh)
            _cached_logs = []
            try:
                from data.game_log_cache import load_game_logs_from_cache as _load_cache
                _cached_logs, _cache_stale = _load_cache(_pname_log)
            except Exception:
                _cached_logs, _cache_stale = [], True

            _game_logs_for_display = _recent_games or _cached_logs

            if _game_logs_for_display:
                with st.expander(
                    f"📅 {_pname_log} — Last {len(_game_logs_for_display)} Game Log",
                    expanded=False,
                ):
                    _log_rows = []
                    for _gi, _g in enumerate(_game_logs_for_display[:15], start=1):
                        def _to_float(val):
                            try:
                                return float(val)
                            except (TypeError, ValueError):
                                return None
                        _log_rows.append({
                            "Game": f"G-{_gi}",
                            "Date": _g.get("game_date", ""),
                            "Opp": _g.get("matchup", _g.get("opp", "")),
                            "PTS": _to_float(_g.get("pts")),
                            "REB": _to_float(_g.get("reb")),
                            "AST": _to_float(_g.get("ast")),
                            "3PM": _to_float(_g.get("fg3m")),
                            "STL": _to_float(_g.get("stl")),
                            "BLK": _to_float(_g.get("blk")),
                            "TOV": _to_float(_g.get("tov")),
                        })
                    st.dataframe(_log_rows, width="stretch", hide_index=True)

                # ── Matchup History vs Tonight's Opponent ─────────────────
                _tonight_ctx = sim_result.get("context", {})
                _opponent = _tonight_ctx.get("opponent", "")
                if _opponent and _game_logs_for_display:
                    try:
                        from engine.matchup_history import (
                            get_player_vs_team_history,
                            get_matchup_force_signal,
                        )
                        # Normalise game log keys to what matchup_history expects
                        _norm_logs = []
                        for _gl in _game_logs_for_display:
                            _norm_logs.append({
                                "opp": _gl.get("matchup", _gl.get("opp", "")),
                                "opponent": _gl.get("matchup", _gl.get("opp", "")),
                                "pts": _gl.get("pts", 0),
                                "reb": _gl.get("reb", 0),
                                "ast": _gl.get("ast", 0),
                                "fg3m": _gl.get("fg3m", 0),
                                "stl": _gl.get("stl", 0),
                                "blk": _gl.get("blk", 0),
                                "tov": _gl.get("tov", 0),
                            })

                        _mh_rows = []
                        for _mh_stat in ["points", "rebounds", "assists", "threes"]:
                            _season_avg = float(
                                _pdata.get(
                                    {"points": "points_avg", "rebounds": "rebounds_avg",
                                     "assists": "assists_avg", "threes": "threes_avg"}[_mh_stat],
                                    0,
                                ) or 0
                            )
                            if _season_avg < 0.5:
                                continue
                            _mh = get_player_vs_team_history(
                                _pname_log, _opponent, _mh_stat, _norm_logs,
                                season_average=_season_avg,
                            )
                            if _mh.get("games_found", 0) < 1:
                                continue
                            _avg_vs = _mh.get("avg_vs_team")
                            _sig = get_matchup_force_signal(
                                (_avg_vs / _season_avg) if _avg_vs and _season_avg else 1.0
                            )
                            _mh_rows.append({
                                "Stat": _mh_stat.title(),
                                "Games vs Opp": _mh["games_found"],
                                "Avg vs Opp": f"{_avg_vs:.1f}" if _avg_vs is not None else "—",
                                "Season Avg": f"{_season_avg:.1f}",
                                "Δ vs Avg": (
                                    f"{(_avg_vs - _season_avg):+.1f}"
                                    if _avg_vs is not None else "—"
                                ),
                                "Signal": _sig.get("label", ""),
                            })

                        if _mh_rows:
                            st.markdown(
                                f"**📊 Matchup History vs {_opponent}:**"
                            )
                            st.dataframe(_mh_rows, hide_index=True, use_container_width=True)

                    except Exception as _mh_exc:
                        pass  # Matchup history is optional — never break the page

            elif _scenario_mode:
                st.caption(
                    f"ℹ️ No recent game log stored for {_pname_log}. "
                    "Scenario simulation uses season averages + your overrides."
                )


# ─── Dark Horse Finder ───────────────────────────────────────
if run_dark_horse:
    st.subheader("🌑 Dark Horse Finder — All Tonight's Players")
    st.caption(
        "Players ranked by their highest 90th-percentile upside ratio "
        "(90th pct projection ÷ season average). Ratio ≥ 1.5 = Dark Horse."
    )

    try:
        with st.spinner("🔮 Scanning all tonight's players for dark horses…"):
            dark_horses = []
            for pdata in tonight_players:
                if not pdata.get("name"):
                    continue
                sim_result = _simulate_player(pdata, min(sim_depth, 1000), todays_games)
                stats = sim_result["stats"]
                # Compute max upside ratio across all meaningful stats
                best_ratio = max(
                    (s["upside_ratio"] for s in stats.values() if s["season_avg"] > 0.5),
                    default=1.0,
                )
                best_stat = max(
                    ((stat, s["upside_ratio"]) for stat, s in stats.items() if s["season_avg"] > 0.5),
                    key=lambda x: x[1],
                    default=("points", 1.0),
                )
                dark_horses.append({
                    "player": pdata,
                    "context": sim_result["context"],
                    "stats": stats,
                    "best_ratio": best_ratio,
                    "best_stat": best_stat[0],
                    "best_p90": stats.get(best_stat[0], {}).get("p90", 0),
                    "best_avg": stats.get(best_stat[0], {}).get("season_avg", 0),
                })

            # Sort by upside ratio descending
            dark_horses.sort(key=lambda x: x["best_ratio"], reverse=True)
    except Exception as _dh_err:
        _dh_err_str = str(_dh_err)
        if "WebSocketClosedError" not in _dh_err_str and "StreamClosedError" not in _dh_err_str:
            st.error(f"❌ Dark Horse scan failed: {_dh_err}")
        dark_horses = []

    # Show top 10 dark horses
    st.markdown(f"**Top Dark Horses Tonight (out of {len(dark_horses)} players):**")
    for rank, dh in enumerate(dark_horses[:10], start=1):
        pdata = dh["player"]
        pname = pdata.get("name", "")
        team = pdata.get("team", "")
        opp = dh["context"].get("opponent", "?")
        ratio = dh["best_ratio"]
        stat = dh["best_stat"]
        p90 = dh["best_p90"]
        avg = dh["best_avg"]
        emoji = _STAT_EMOJI.get(stat, "📊")
        is_dh = ratio >= 1.5
        badge = "🌑 DARK HORSE" if is_dh else "📈 Upside"
        color = "#ff5e00" if is_dh else "#00ff9d"
        st.markdown(
            f'<div style="background:#14192b;border-radius:6px;padding:10px 14px;'
            f'margin-bottom:8px;border-left:3px solid {color};">'
            f'<span style="color:{color};font-weight:700;margin-right:8px;">#{rank} {badge}</span>'
            f'<strong style="color:white;">{_html.escape(pname)}</strong>'
            f'<span style="color:#8a9bb8;margin:0 6px;">{_html.escape(team)} vs {_html.escape(opp)}</span>'
            f'<span style="color:#c0d0e8;">'
            f'{emoji} {stat.title()} — 90th pct: <strong style="color:{color};">{p90}</strong>'
            f' vs avg {avg} (upside {ratio:.2f}x)</span>'
            f'</div>',
            unsafe_allow_html=True,
        )

    st.divider()
    st.markdown("**Full simulation cards for top 3 dark horses:**")
    for dh in dark_horses[:3]:
        _dh_pname = dh["player"].get("name", "Unknown Player")
        _dh_ratio = dh["best_ratio"]
        _dh_badge = "🌑 DARK HORSE" if _dh_ratio >= 1.5 else "📈 Upside"
        with st.expander(
            f"{_dh_badge} — {_dh_pname} (Upside {_dh_ratio:.2f}×)",
            expanded=True,
        ):
            sim_full = _simulate_player(dh["player"], sim_depth, todays_games)
            _render_sim_card(sim_full)
            _is_dh_full = any(
                s["upside_ratio"] >= 1.5
                for s in sim_full["stats"].values()
                if s["season_avg"] > 0.5
            )
            _render_betting_recommendations(sim_full, is_dark_horse=_is_dh_full)
