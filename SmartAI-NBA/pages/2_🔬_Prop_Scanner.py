# ============================================================
# FILE: pages/2_📥_Import_Props.py
# PURPOSE: Allow users to enter prop lines manually or upload
#          a CSV file. Stores props in session state for Analysis.
# CONNECTS TO: data_manager.py (load/save), Analysis page
# CONCEPTS COVERED: Forms, file upload, data tables, CSV parsing
# ============================================================

import streamlit as st
import datetime

from data.data_manager import (
    load_players_data,
    load_props_data,
    load_props_from_session,
    save_props_to_session,
    get_all_player_names,
    parse_props_from_csv_text,
    get_csv_template,
    find_player_by_name,
    enrich_prop_with_player_data,
    validate_props_against_roster,
    get_player_status,
    load_injury_status,
)
from data.platform_mappings import (
    normalize_stat_type,
    detect_platform_from_stat_names,
    COMBO_STATS,
    FANTASY_SCORING,
)
try:
    from engine import SIMPLE_STAT_TYPES as _SIMPLE_STAT_TYPES
except ImportError:
    _SIMPLE_STAT_TYPES = frozenset({
        "points", "rebounds", "assists", "threes", "steals", "blocks", "turnovers",
    })

# ============================================================
# SECTION: Page Setup
# ============================================================

st.set_page_config(
    page_title="Prop Scanner — SmartBetPro NBA",
    page_icon="🔬",
    layout="wide",
)

# ─── Inject Global CSS Theme ──────────────────────────────────
from styles.theme import get_global_css, get_education_box_html
st.markdown(get_global_css(), unsafe_allow_html=True)

# ── Joseph M. Smith Hero Banner & Floating Widget ─────────────
from utils.components import render_joseph_hero_banner, inject_joseph_floating
st.session_state["joseph_page_context"] = "page_prop_scanner"
inject_joseph_floating()

# ── Premium Status (partial gate — some features restricted) ──
from utils.auth import is_premium_user as _is_premium_user
try:
    from utils.stripe_manager import _PREMIUM_PAGE_PATH as _PREM_PATH
except Exception:
    _PREM_PATH = "/14_%F0%9F%92%8E_Subscription_Level"
_FREE_PROP_LIMIT = 5   # Free users can manually enter up to 5 props
_user_is_premium = _is_premium_user()

# ─── Custom CSS ───────────────────────────────────────────────
st.markdown("""
<style>
/* Platform color badges */
.plat-draftkings { background:#2b6cb0; color:#bee3f8; padding:2px 8px; border-radius:4px; font-size:0.8rem; font-weight:700; }
.plat-default    { background:#1a2035; color:#c0d0e8; padding:2px 8px; border-radius:4px; font-size:0.8rem; font-weight:700; border:1px solid rgba(0,240,255,0.20); }
/* Team pill */
.team-pill { background:rgba(0,240,255,0.12); color:#fff; border:1px solid rgba(0,240,255,0.30); padding:1px 6px; border-radius:4px; font-size:0.8rem; font-weight:700; }
/* Line context: high/low */
.line-high { color:#ff6b6b; font-size:0.75rem; }
.line-low  { color:#00ff9d; font-size:0.75rem; }
.line-ok   { color:#8a9bb8; font-size:0.75rem; }
</style>
""", unsafe_allow_html=True)

st.title("🔬 Prop Scanner")
st.markdown("Enter prop lines manually, upload a CSV, or **load live lines** directly from the platforms!")

with st.expander("📖 How to Use This Page", expanded=False):
    st.markdown("""
    ### Prop Scanner — Three Ways to Load Props
    
    **Option 1: Manual Entry**
    - Use the form to enter individual props (player name, stat type, line)
    - Good for adding specific props not available on platforms
    
    **Option 2: CSV Upload**
    - Download the template, fill in your props, upload the file
    - Best for bulk entry or importing from your own research
    
    **Option 3: Get Live Platform Lines**
    - Go to the **📡 Live Games** page and click **📊 Get Live Props & Analyze**
    - Retrieves real live lines from all major sportsbooks via The Odds API
    
    💡 **Pro Tips:**
    - Load live lines for the most accurate analysis
    - Use the filter/sort options to focus on specific players or stat types
    """)

st.divider()

st.markdown(get_education_box_html(
    "📖 What is a Prop Bet?",
    """
    <strong>Prop Bet</strong>: A bet on whether a player will exceed (Over) or fall short of (Under) a 
    statistical threshold set by the platform.<br><br>
    Example: "LeBron James Points OVER 24.5" — you win if LeBron scores 25 or more points.<br><br>
    <strong>How to read a prop line</strong>: The number (24.5) is the threshold. 
    Always bet OVER or UNDER — never equal (that's a push/tie).<br><br>
    <strong>Platforms</strong>: PrizePicks, Underdog Fantasy, DraftKings Pick6 — each platform may have different line values.
    """
), unsafe_allow_html=True)

# ============================================================
# END SECTION: Page Setup
# ============================================================

# ============================================================
# SECTION: Load Available Data
# ============================================================

players_data = load_players_data()
all_player_names = get_all_player_names(players_data)

# Simple stats + combo + fantasy stat types
valid_stat_types = (
    sorted(_SIMPLE_STAT_TYPES)
    + sorted(COMBO_STATS.keys())
    + sorted(FANTASY_SCORING.keys())
    + ["double_double", "triple_double"]
)
valid_platforms = [
    "PrizePicks", "Underdog Fantasy", "DraftKings Pick6",
]

# ── Import platform service (optional — app works without it) ──
try:
    from data.sportsbook_service import (
        get_all_sportsbook_props,
        build_cross_platform_comparison,
        recommend_best_platform,
        summarize_props_by_platform,
        find_new_players_from_props,
        extract_active_players_from_props,
    )
    from data.data_manager import (
        save_platform_props_to_session,
        load_platform_props_from_session,
        save_platform_props_to_csv,
    )
    _SPORTSBOOK_SERVICE_AVAILABLE = True
except ImportError:
    _SPORTSBOOK_SERVICE_AVAILABLE = False

# ============================================================
# END SECTION: Load Available Data
# ============================================================

# ============================================================
# SECTION: Get Live Props
# One-click button to pull live lines from all major sportsbooks
# (via The Odds API) and populate the prop list.
# ============================================================

st.subheader("🔄 Get Live Props")

# ── Free tier: disable live platform loading ──────────────────
if not _user_is_premium:
    st.markdown(
        '<div style="background:rgba(255,94,0,0.08);border:1px solid rgba(255,94,0,0.25);'
        'border-radius:10px;padding:12px 16px;margin-bottom:8px;">'
        '<span style="color:#ff9d00;font-weight:600;">🔒 Premium Feature</span>'
        f' — Live platform loading (all major sportsbooks) requires a '
        f'<a href="{_PREM_PATH}" style="color:#ff5e00;font-weight:700;">Premium subscription</a>. '
        'You can still enter up to 5 props manually below.</div>',
        unsafe_allow_html=True,
    )
elif _SPORTSBOOK_SERVICE_AVAILABLE:
    _dk_on = st.session_state.get("load_draftkings_enabled", True)
    _dk_key = st.session_state.get("odds_api_key", "").strip()

    # Platform checkboxes
    _ps_pp_col, _ps_ud_col, _ps_dk_col = st.columns(3)
    with _ps_pp_col:
        _pp_on = st.checkbox("🟢 PrizePicks", value=True, key="scanner_pp_checkbox")
    with _ps_ud_col:
        _ud_on = st.checkbox("🟡 Underdog Fantasy", value=True, key="scanner_ud_checkbox")
    with _ps_dk_col:
        _dk_cb_on = st.checkbox("🔵 DraftKings Pick6", value=_dk_on and bool(_dk_key), key="scanner_dk_checkbox",
                                disabled=not (_dk_on and bool(_dk_key)),
                                help="Requires Odds API key — configure on ⚙️ Settings page." if not (_dk_on and bool(_dk_key)) else "")

    # Show which platforms are enabled
    _enabled_names = []
    if _pp_on:
        _enabled_names.append("PrizePicks")
    if _ud_on:
        _enabled_names.append("Underdog Fantasy")
    if _dk_cb_on:
        _enabled_names.append("DraftKings Pick6")

    st.markdown(
        f"Get tonight's live prop lines from: **{', '.join(_enabled_names) if _enabled_names else 'no platforms enabled'}**. "
        "Configure platforms on the [⚙️ Settings](/Settings) page."
    )

    _live_col1, _live_col2 = st.columns([2, 3])

    with _live_col1:
        _do_load = st.button(
            "🔄 Get Live Props",
            type="primary",
            width="stretch",
            help="Pull tonight's live prop lines from all enabled platforms.",
            disabled=not _enabled_names,
        )

    with _live_col2:
        # Show cached platform props info if available
        _cached = load_platform_props_from_session(st.session_state)
        if _cached:
            _cached_summary = summarize_props_by_platform(_cached)
            _retrieved_at = _cached[0].get("retrieved_at", "unknown time") if _cached else ""
            st.info(
                f"📦 **{len(_cached)} props cached** "
                f"({', '.join(f'{p}: {c}' for p, c in _cached_summary.items())}) "
                f"— retrieved at {_retrieved_at[:16] if _retrieved_at else 'unknown'}"
            )

    if _do_load:
        _pb = st.progress(0, text="Starting platform load...")

        def _scanner_progress(current, total, msg):
            pct = int((current / max(total, 1)) * 100)
            _pb.progress(pct, text=msg)

        try:
            with st.spinner("Loading live props..."):
                _live_props = get_all_sportsbook_props(
                    include_prizepicks=_pp_on,
                    include_underdog=_ud_on,
                    include_draftkings=_dk_cb_on,
                    odds_api_key=_dk_key or None,
                    progress_callback=_scanner_progress,
                )

            _pb.progress(100, text="Done!")

            if _live_props:
                save_platform_props_to_session(_live_props, st.session_state)
                save_platform_props_to_csv(_live_props)
                save_props_to_session(_live_props, st.session_state)
                _lsummary = summarize_props_by_platform(_live_props)
                st.success(
                    f"✅ Loaded **{len(_live_props)} live props**: "
                    + ", ".join(f"**{p}** ({c})" for p, c in _lsummary.items())
                )
                # Warn if any props reference players not in our database
                _missing = find_new_players_from_props(_live_props, players_data)
                if _missing:
                    st.warning(
                        f"⚠️ **{len(_missing)} player(s)** from platform props are not in your "
                        f"local database: {', '.join(_missing[:5])}"
                        + (f" and {len(_missing) - 5} more" if len(_missing) > 5 else "")
                        + ". Run a **Smart Update** on the 📡 Data Feed page to add their stats."
                    )
                st.rerun()  # Refresh so the current_props table shows the new data
            else:
                st.warning(
                    "⚠️ No live props retrieved. Check your internet connection."
                )
        except Exception as _load_err:
            _err_str = str(_load_err)
            if "WebSocketClosedError" not in _err_str and "StreamClosedError" not in _err_str:
                st.error(f"❌ Failed to load live props: {_load_err}")
        finally:
            try:
                _pb.empty()
            except Exception:
                pass

    # ── Cross-Platform Comparison Table ───────────────────────────
    _platform_props = load_platform_props_from_session(st.session_state)
    if _platform_props:
        comparison = build_cross_platform_comparison(_platform_props)
        # Only show players that appear on 2+ platforms (most useful to compare)
        multi_platform = {
            key: lines for key, lines in comparison.items()
            if len(lines) >= 2
        }

        if multi_platform:
            with st.expander(
                f"📊 Cross-Platform Line Comparison ({len(multi_platform)} player+stat combos)",
                expanded=False,
            ):
                st.markdown(
                    "Lines available on **multiple platforms** — compare to find the best bet. "
                    "**OVER**: lower line is better. **UNDER**: higher line is better."
                )

                # Build comparison table rows
                _comp_rows = []
                for (player_name, stat_type), lines in sorted(multi_platform.items()):
                    row = {
                        "Player": player_name,
                        "Stat": stat_type,
                    }
                    # Add columns for each sportsbook
                    for platform in valid_platforms:
                        row[platform] = lines.get(platform, "—")

                    # Calculate spread (max - min line)
                    numeric_lines = [v for v in lines.values() if isinstance(v, (int, float))]
                    if len(numeric_lines) >= 2:
                        spread = round(max(numeric_lines) - min(numeric_lines), 1)
                        row["Spread"] = spread
                        # Best for OVER = lowest line
                        best_over_plat = min(lines, key=lambda p: lines[p])
                        row["Best OVER"] = f"{best_over_plat} ({lines[best_over_plat]})"
                        # Best for UNDER = highest line
                        best_under_plat = max(lines, key=lambda p: lines[p])
                        row["Best UNDER"] = f"{best_under_plat} ({lines[best_under_plat]})"
                    else:
                        row["Spread"] = "—"
                        row["Best OVER"] = "—"
                        row["Best UNDER"] = "—"

                    _comp_rows.append(row)

                if _comp_rows:
                    st.dataframe(_comp_rows, width="stretch", hide_index=True)
                    st.caption(
                        "💡 **Best OVER** = platform with the lowest line (easiest to beat). "
                        "**Best UNDER** = platform with the highest line (most room). "
                        "**Spread** = difference between highest and lowest line."
                    )

else:
    st.info(
        "ℹ️ Live prop loading requires the `requests` library. "
        "Run `pip install requests` to enable this feature."
    )

st.divider()

# ============================================================
# END SECTION: Get Live Props
# ============================================================

# ============================================================
# SECTION: Current Props Table
# ============================================================

current_props = load_props_from_session(st.session_state)



# Load persisted injury status for warning display (no API call needed)
injury_status_map = load_injury_status()

# ── Injury-status classification of all current props ─────────────
_UNAVAILABLE_STATUSES = {"Out", "Doubtful", "Injured Reserve"}
_GTD_STATUSES = {"GTD", "Questionable", "Day-to-Day"}

_unavailable_props = []
_gtd_props = []
_healthy_props = []

for _p in current_props:
    _pname = _p.get("player_name", "")
    _si = get_player_status(_pname, injury_status_map)
    _pstatus = _si.get("status", "Active")
    if _pstatus in _UNAVAILABLE_STATUSES:
        _unavailable_props.append((_p, _pstatus, _si.get("injury_note", "")))
    elif _pstatus in _GTD_STATUSES:
        _gtd_props.append((_p, _pstatus, _si.get("injury_note", "")))
    else:
        _healthy_props.append((_p, _pstatus, _si.get("injury_note", "")))

# ── Toggle: show injured players anyway ───────────────────────────
_show_injured = st.toggle(
    "👁️ Show injured players anyway (Out/Doubtful)",
    value=False,
    help="By default, players confirmed Out or Doubtful are hidden. Enable this to see all props.",
)

# ── Summary banner for removed props ──────────────────────────────
if _unavailable_props and not _show_injured:
    st.error(
        f"⚠️ **{len(_unavailable_props)} prop(s) hidden** — player(s) are confirmed "
        f"**Out or Doubtful**: "
        + ", ".join(f"**{p.get('player_name','')}**" for p, _, _ in _unavailable_props)
        + ". Enable *'Show injured players anyway'* to view them."
    )

# ── Determine which props to display ──────────────────────────────
if _show_injured:
    _display_props_raw = current_props
    _display_props_enriched = [enrich_prop_with_player_data(p, players_data) for p in _display_props_raw]
else:
    # Only non-unavailable props
    _safe_props = [p for p, _, _ in _healthy_props + _gtd_props]
    _display_props_raw = _safe_props
    _display_props_enriched = [enrich_prop_with_player_data(p, players_data) for p in _safe_props]

st.subheader(f"📋 Current Props ({len(_display_props_enriched)} displayed / {len(current_props)} total)")

# ── Smart Scan Pre-Filter Bar ─────────────────────────────────────
if _display_props_enriched:
    with st.expander("🔍 Smart Scan — Pre-Filter Props", expanded=False):
        st.markdown(
            "Narrow down to the most promising props before running Neural Analysis. "
            "Filters apply to the table below and carry into the analysis run."
        )
        _sf1, _sf2, _sf3, _sf4 = st.columns(4)
        with _sf1:
            _filter_platform = st.multiselect(
                "Platform",
                options=valid_platforms,
                default=[],
                placeholder="All platforms",
                key="scan_platform_filter",
            )
        with _sf2:
            _filter_stat = st.multiselect(
                "Stat Type",
                options=sorted({p.get("stat_type", "").capitalize() for p in _display_props_enriched if p.get("stat_type")}),
                default=[],
                placeholder="All stats",
                key="scan_stat_filter",
            )
        with _sf3:
            _filter_line_max = st.slider(
                "Max Line Value",
                min_value=0.0,
                max_value=60.0,
                value=60.0,
                step=0.5,
                key="scan_line_max",
            )
        with _sf4:
            _filter_healthy_only = st.toggle(
                "Healthy Players Only",
                value=True,
                key="scan_healthy_filter",
                help="Hide GTD/Out players from Smart Scan results",
            )

        # Apply Smart Scan filters
        _scanned = _display_props_enriched
        if _filter_platform:
            _scanned = [p for p in _scanned if p.get("platform", "") in _filter_platform]
        if _filter_stat:
            _scanned = [p for p in _scanned if p.get("stat_type", "").capitalize() in _filter_stat]
        _scanned = [p for p in _scanned if float(p.get("line", 0)) <= _filter_line_max]
        if _filter_healthy_only:
            _healthy_names = {p.get("player_name", "") for p, _, _ in _healthy_props}
            _scanned = [p for p in _scanned if p.get("player_name", "") in _healthy_names]

        st.caption(f"**Smart Scan result: {len(_scanned)} props** match your filters (out of {len(_display_props_enriched)} displayed).")
        if _scanned:
            # Line comparison visual: show line vs season avg as a bar
            _cmp_rows = []
            for _sp in _scanned[:30]:
                _sn = _sp.get("player_name", "")
                _st_t = _sp.get("stat_type", "").capitalize()
                _ln = float(_sp.get("line", 0))
                _sa_map = {
                    "Points": _sp.get("season_pts_avg", 0),
                    "Rebounds": _sp.get("season_reb_avg", 0),
                    "Assists": _sp.get("season_ast_avg", 0),
                }
                _avg = float(_sa_map.get(_st_t, 0) or 0)
                _diff_pct = round((_ln - _avg) / _avg * 100, 1) if _avg > 0 else 0
                _vs = f"+{_diff_pct:.1f}%" if _diff_pct >= 0 else f"{_diff_pct:.1f}%"
                _cmp_rows.append({
                    "Player": _sn,
                    "Stat": _st_t,
                    "Line": _ln,
                    "Line Type": {
                        "goblin": "🟢 Goblin",
                        "demon":  "🔴 Demon",
                    }.get(_sp.get("odds_type", "standard"), "⚪ Standard"),
                    "Season Avg": round(_avg, 1) if _avg else "—",
                    "Line vs Avg": _vs,
                    "Platform": _sp.get("platform", ""),
                })
            st.dataframe(
                _cmp_rows,
                width="stretch",
                hide_index=True,
                column_config={
                    "Line": st.column_config.NumberColumn(format="%.1f"),
                    "Season Avg": st.column_config.NumberColumn(format="%.1f"),
                    "Line vs Avg": st.column_config.TextColumn(),
                },
            )

            # Bulk edit: update lines for scanned props
            with st.expander("✏️ Bulk Edit Lines", expanded=False):
                st.markdown("Adjust prop lines for the Smart Scan results in bulk:")
                _bulk_adjustments = {}
                for _bi, _bp in enumerate(_scanned[:10]):
                    _bname = _bp.get("player_name", "")
                    _bstat = _bp.get("stat_type", "")
                    _bline = float(_bp.get("line", 0))
                    _bc1, _bc2, _bc3 = st.columns([3, 2, 1])
                    with _bc1:
                        st.markdown(f"**{_bname}** — {_bstat.title()}")
                    with _bc2:
                        _new_line = st.number_input(
                            "Line",
                            min_value=0.0,
                            max_value=100.0,
                            value=_bline,
                            step=0.5,
                            key=f"bulk_line_{_bi}",
                            label_visibility="collapsed",
                        )
                    with _bc3:
                        if _new_line != _bline:
                            _bulk_adjustments[(_bname, _bstat)] = _new_line

                if _bulk_adjustments and st.button("💾 Apply Bulk Edits", type="primary", key="bulk_apply"):
                    _updated = []
                    for _raw in current_props:
                        _k = (_raw.get("player_name", ""), _raw.get("stat_type", ""))
                        if _k in _bulk_adjustments:
                            _raw = dict(_raw)
                            _raw["line"] = _bulk_adjustments[_k]
                        _updated.append(_raw)
                    save_props_to_session(_updated, st.session_state)
                    st.success(f"✅ Updated {len(_bulk_adjustments)} prop line(s)!")
                    st.rerun()

if _display_props_enriched:
    # Build display rows with season averages, line context, and injury status
    display_rows = []
    for i, prop in enumerate(_display_props_enriched):
        player_name = prop.get("player_name", "")
        team = prop.get("player_team", prop.get("team", ""))
        stat = prop.get("stat_type", "").capitalize()
        line = prop.get("line", 0)
        platform = prop.get("platform", "")
        pts = prop.get("season_pts_avg", 0)
        reb = prop.get("season_reb_avg", 0)
        ast = prop.get("season_ast_avg", 0)
        line_diff = prop.get("line_vs_avg_pct", 0)

        # Season avg for this stat type
        stat_key = stat.lower()
        stat_avg_map = {"points": pts, "rebounds": reb, "assists": ast}
        season_avg = stat_avg_map.get(stat_key, 0)

        # Line context
        if line_diff > 10:
            line_ctx = f"↑{line_diff:.0f}% above avg"
        elif line_diff < -10:
            line_ctx = f"↓{abs(line_diff):.0f}% below avg"
        else:
            line_ctx = "near avg"

        # Injury / availability status badge
        status_info = get_player_status(player_name, injury_status_map)
        player_status = status_info.get("status", "Active")
        status_emoji = {
            "Out": "🔴", "Injured Reserve": "🔴", "Doubtful": "🔴",
            "Questionable": "🟡", "GTD": "🟡", "Day-to-Day": "🟡",
            "Active": "🟢", "Probable": "🟢",
        }.get(player_status, "⚪")

        display_rows.append({
            "#": i + 1,
            "Player": player_name,
            "Status": f"{status_emoji} {player_status}",
            "Team": team,
            "Stat": stat,
            "Line": line,
            "Line Type": {
                "goblin": "🟢 Goblin",
                "demon":  "🔴 Demon",
            }.get(prop.get("odds_type", "standard"), "⚪ Standard"),
            "Season Avg": round(season_avg, 1) if season_avg else "—",
            "Line Context": line_ctx if season_avg else "—",
            "Value Signal": (
                "🔥 Low Line" if season_avg and line_diff < -12 else
                "⚠️ High Line" if season_avg and line_diff > 15 else
                "✅ Fair" if season_avg else "—"
            ),
            "Platform": platform,
            "Date": prop.get("game_date", ""),
        })

    # ── Quick Value Summary Banner ─────────────────────────────────
    _low_count  = sum(1 for r in display_rows if r.get("Value Signal", "").startswith("🔥"))
    _high_count = sum(1 for r in display_rows if r.get("Value Signal", "").startswith("⚠️"))
    _fair_count = sum(1 for r in display_rows if r.get("Value Signal", "").startswith("✅"))
    if _low_count + _high_count + _fair_count > 0:
        st.markdown(
            f'<div style="background:rgba(0,240,255,0.05);border:1px solid rgba(0,240,255,0.15);'
            f'border-radius:6px;padding:8px 14px;margin-bottom:8px;font-size:0.83rem;color:#c0d0e8;">'
            f'📊 <strong>Line Value Summary:</strong> &nbsp; '
            f'<span style="color:#00ff9d;font-weight:700;">🔥 {_low_count} Low (OVER value)</span> &nbsp;·&nbsp; '
            f'<span style="color:#69b4ff;font-weight:600;">✅ {_fair_count} Fair</span> &nbsp;·&nbsp; '
            f'<span style="color:#ff9966;font-weight:700;">⚠️ {_high_count} High (UNDER value)</span>'
            f'</div>',
            unsafe_allow_html=True,
        )

    st.dataframe(
        display_rows,
        width="stretch",
        hide_index=True,
        column_config={
            "Line": st.column_config.NumberColumn(format="%.1f"),
            "Season Avg": st.column_config.NumberColumn(format="%.1f"),
        },
    )

    # Show GTD / Questionable warnings
    gtd_warnings = []
    for p, pstatus, note in _gtd_props:
        pname = p.get("player_name", "")
        gtd_warnings.append(
            f"⚠️ **{pname}** is **{pstatus}**" + (f" — {note}" if note else "")
        )
    # If showing injured, also show Out/Doubtful warnings
    if _show_injured:
        for p, pstatus, note in _unavailable_props:
            pname = p.get("player_name", "")
            gtd_warnings.append(
                f"⛔ **{pname}** is **{pstatus}**" + (f" — {note}" if note else "")
            )

    if gtd_warnings:
        with st.expander(
            f"🏥 Availability Alerts ({len(gtd_warnings)} player(s)) — click to expand",
            expanded=True,
        ):
            for warning in gtd_warnings:
                if warning.startswith("⛔"):
                    st.error(warning)
                else:
                    st.warning(warning)

    col_clear, col_load_sample, _ = st.columns([1, 1, 3])
    with col_clear:
        if st.button("🗑️ Clear All Props"):
            st.session_state["current_props"] = []
            st.session_state["analysis_results"] = []
            st.rerun()
    with col_load_sample:
        if st.button("📦 Load Props from CSV"):
            saved_props = load_props_data()
            if saved_props:
                save_props_to_session(saved_props, st.session_state)
                st.success(f"Loaded {len(saved_props)} props from props.csv!")
            else:
                st.info("No props found. Go to **📡 Data Feed** to load live data first.")
            st.rerun()

    # Roster validation table
    if players_data:
        from styles.theme import get_roster_health_html
        validation = validate_props_against_roster(current_props, players_data)
        total_v = validation["total"]
        matched_count_v = validation["matched_count"]

        # Show OUT warnings from the validation result (these are post-processed by data_manager)
        out_in_validation = [
            item for item in validation["matched"] + validation["fuzzy_matched"]
            if item.get("out_warning")
        ]
        if out_in_validation:
            for item in out_in_validation:
                st.error(item["out_warning"])

        if total_v > 0:
            with st.expander(
                f"🧬 Roster Health: {matched_count_v}/{total_v} props matched "
                f"({int(matched_count_v/max(total_v,1)*100)}%) — click to see details",
                expanded=(len(validation["unmatched"]) > 0),
            ):
                st.markdown(
                    get_roster_health_html(
                        validation["matched"],
                        validation["fuzzy_matched"],
                        validation["unmatched"],
                    ),
                    unsafe_allow_html=True,
                )

else:
    if current_props:
        st.info(
            f"No props to display. "
            f"{'All ' + str(len(current_props)) + ' prop(s) are hidden (players are Out/Doubtful). '  if _unavailable_props and not _show_injured else ''}"
            "Use the toggle above to show injured players."
        )
    else:
        st.info("No props loaded. Use the forms below to add props.")

# ============================================================
# END SECTION: Current Props Table
# ============================================================

st.divider()

# ============================================================
# SECTION: How to Get Prop Lines
# ============================================================

st.info(
    "💡 **To get prop lines:** Use the **📡 Live Games** page — click "
    "**📊 Get Live Props & Analyze** or **⚡ One-Click Setup** to load "
    "real live lines from all major sportsbooks."
)

# ============================================================
# END SECTION: How to Get Prop Lines
# ============================================================

st.divider()

# ============================================================
# SECTION: Manual Entry Form
# ============================================================

st.subheader("✏️ Add Props Manually")
st.markdown("Enter one prop at a time. Click **Add Prop** to save each one.")

with st.form("manual_prop_entry", clear_on_submit=True):
    col1, col2, col3, col4 = st.columns([3, 1, 1, 2])

    with col1:
        selected_player = st.selectbox(
            "Player Name *",
            options=["— Type or select —"] + all_player_names,
        )
        custom_player_name = st.text_input(
            "Or type player name:",
            placeholder="e.g., LeBron James",
        )

    with col2:
        stat_type_selection = st.selectbox("Stat Type *", options=valid_stat_types)

    with col3:
        prop_line_value = st.number_input(
            "Line *",
            min_value=0.0, max_value=100.0,
            value=24.5, step=0.5,
        )

    with col4:
        platform_selection = st.selectbox("Platform *", options=valid_platforms)

    col5, col6, col7 = st.columns([2, 2, 3])
    with col5:
        team_input = st.text_input("Team (optional)", placeholder="e.g., LAL")
    with col6:
        game_date_input = st.date_input("Game Date", value=datetime.date.today())

    add_prop_button = st.form_submit_button(
        "➕ Add Prop",
        width="stretch",
        type="primary",
    )

if add_prop_button:
    if custom_player_name.strip():
        final_player_name = custom_player_name.strip()
    elif selected_player != "— Type or select —":
        final_player_name = selected_player
    else:
        final_player_name = ""

    if not final_player_name:
        st.error("Please enter or select a player name.")
    elif prop_line_value <= 0:
        st.error("Prop line must be greater than 0.")
    else:
        # Auto-fill team from player database if not provided
        auto_team = team_input.strip().upper() if team_input else ""
        if not auto_team:
            player_lookup = find_player_by_name(players_data, final_player_name)
            if player_lookup:
                auto_team = player_lookup.get("team", "")

        new_prop = {
            "player_name": final_player_name,
            "team": auto_team,
            "stat_type": stat_type_selection,
            "line": prop_line_value,
            "platform": platform_selection,
            "game_date": game_date_input.isoformat(),
        }

        current_props_for_update = load_props_from_session(st.session_state)

        # ── Free tier prop limit ───────────────────────────────────
        if not _user_is_premium and len(current_props_for_update) >= _FREE_PROP_LIMIT:
            st.warning(
                f"⚠️ Free plan is limited to **{_FREE_PROP_LIMIT} props**. "
                f"Remove a prop first, or [**upgrade to Premium**]({_PREM_PATH}) "
                "for unlimited props."
            )
        else:
            current_props_for_update.append(new_prop)
            save_props_to_session(current_props_for_update, st.session_state)
            st.success(f"✅ Added: {final_player_name} ({auto_team}) | {stat_type_selection} | {prop_line_value} | {platform_selection}")
            st.rerun()

# ============================================================
# END SECTION: Manual Entry Form
# ============================================================

st.divider()

# ============================================================
# SECTION: CSV Upload
# ============================================================

st.subheader("📤 Upload Props CSV")

# ── Free tier: disable CSV upload ────────────────────────────
if not _user_is_premium:
    st.markdown(
        '<div style="background:rgba(255,94,0,0.08);border:1px solid rgba(255,94,0,0.25);'
        'border-radius:10px;padding:12px 16px;">'
        '<span style="color:#ff9d00;font-weight:600;">🔒 Premium Feature</span>'
        f' — CSV upload requires a '
        f'<a href="{_PREM_PATH}" style="color:#ff5e00;font-weight:700;">Premium subscription</a>.'
        '</div>',
        unsafe_allow_html=True,
    )
else:
    st.markdown("**Required CSV format:**")
    st.code(
        "player_name,team,stat_type,line,platform,game_date\n"
        "LeBron James,LAL,points,24.5,PrizePicks,2026-03-05\n"
        "Stephen Curry,GSW,threes,3.5,DraftKings Pick6,2026-03-05",
        language="csv",
    )

    template_csv = get_csv_template()
    st.download_button(
        label="⬇️ Download CSV Template",
        data=template_csv,
        file_name="props_template.csv",
        mime="text/csv",
    )

    st.markdown("---")

    uploaded_file = st.file_uploader(
        "Upload your props CSV file",
        type=["csv"],
    )

    if uploaded_file is not None:
        file_content = uploaded_file.read().decode("utf-8")
        parsed_props, parse_errors = parse_props_from_csv_text(file_content)

        if parse_errors:
            for error in parse_errors:
                st.warning(f"⚠️ {error}")

        if parsed_props:
            # Auto-detect platform and normalize stat types through platform mappings
            raw_stat_names = [p.get("stat_type", "") for p in parsed_props]
            detected_platform = detect_platform_from_stat_names(raw_stat_names)
            if detected_platform:
                st.info(f"🔍 Auto-detected platform: **{detected_platform}**")

            for p in parsed_props:
                raw_stat = p.get("stat_type", "")
                platform_hint = p.get("platform", detected_platform or "")
                normalized = normalize_stat_type(raw_stat, platform_hint)
                if normalized != raw_stat:
                    p["stat_type"] = normalized

            st.success(f"✅ Parsed {len(parsed_props)} props from upload!")

            # Auto-enrich with player data
            enriched_preview = [enrich_prop_with_player_data(p, players_data) for p in parsed_props]
            preview_rows = [
                {
                    "Player": p.get("player_name", ""),
                    "Team": p.get("player_team", p.get("team", "")),
                    "Stat": p.get("stat_type", ""),
                    "Line": p.get("line", ""),
                    "Season Avg": round(p.get("season_pts_avg", 0), 1) if p.get("stat_type") == "points" else "—",
                    "Platform": p.get("platform", ""),
                }
                for p in enriched_preview[:10]
            ]
            st.markdown("**Preview:**")
            st.dataframe(preview_rows, width="stretch", hide_index=True)

            if len(parsed_props) > 10:
                st.caption(f"... and {len(parsed_props) - 10} more")

            col_replace, col_add, col_cancel = st.columns([1, 1, 2])
            with col_replace:
                if st.button("🔄 Replace All Props", type="primary"):
                    save_props_to_session(parsed_props, st.session_state)
                    st.success(f"Replaced all props with {len(parsed_props)} from upload!")
                    st.rerun()
            with col_add:
                if st.button("➕ Add to Existing"):
                    existing = load_props_from_session(st.session_state)
                    combined = existing + parsed_props
                    save_props_to_session(combined, st.session_state)
                    st.success(f"Added {len(parsed_props)} props. Total: {len(combined)}")
                    st.rerun()
        else:
            st.error("No valid props found in the uploaded file.")

# ============================================================
# END SECTION: CSV Upload
# ============================================================

st.divider()

# ============================================================
# SECTION: Quick Add Multiple Props
# ============================================================

st.subheader("⚡ Quick Add (Paste CSV data)")
st.markdown("Paste prop lines directly as CSV text:")

quick_add_text = st.text_area(
    "Paste CSV data here",
    placeholder="player_name,team,stat_type,line,platform\nLeBron James,LAL,points,24.5,PrizePicks\nStephen Curry,GSW,threes,3.5,DraftKings Pick6",
    height=150,
)

if st.button("⚡ Parse & Add Props") and quick_add_text.strip():
    parsed_props_quick, errors_quick = parse_props_from_csv_text(quick_add_text)

    for error in errors_quick:
        st.warning(f"⚠️ {error}")

    if parsed_props_quick:
        # Normalize stat types through platform mappings
        for p in parsed_props_quick:
            raw_stat = p.get("stat_type", "")
            platform_hint = p.get("platform", "")
            normalized = normalize_stat_type(raw_stat, platform_hint)
            if normalized != raw_stat:
                p["stat_type"] = normalized

        existing = load_props_from_session(st.session_state)
        combined = existing + parsed_props_quick
        save_props_to_session(combined, st.session_state)
        st.success(f"✅ Added {len(parsed_props_quick)} props! Total: {len(combined)}")
        st.rerun()
    else:
        st.error("Could not parse any props from the input.")

# ============================================================
# END SECTION: Quick Add Multiple Props
# ============================================================

st.divider()

# ============================================================
# SECTION: Quick Analysis Panel
# Shows edge indicators, injury status, and recent form for
# all currently loaded props without running the full simulation.
# This gives users an instant snapshot to prioritise which props
# are worth sending to Neural Analysis.
# ============================================================

st.subheader("⚡ Quick Analysis — Loaded Props")

_qa_props = load_props_from_session(st.session_state)

if not _qa_props:
    st.info(
        "No props loaded yet. Load live props above, add them manually, "
        "or upload a CSV to populate this panel."
    )
else:
    # Try to load player intelligence helpers
    try:
        from engine.player_intelligence import (
            build_quick_analysis_rows,
            aggregate_streak_summary,
        )
        from styles.theme import get_player_intel_css, get_form_dots_html
        _QA_AVAILABLE = True
    except ImportError:
        _QA_AVAILABLE = False

    if not _QA_AVAILABLE:
        st.info(f"ℹ️ {len(_qa_props)} props loaded. Quick Analysis unavailable (player_intelligence module not found).")
    else:
        # Inject CSS once
        st.markdown(get_player_intel_css(), unsafe_allow_html=True)

        # Load supporting data
        _qa_injury_map = load_injury_status()
        _qa_game_logs_cache = st.session_state.get("game_logs_cache", {})

        with st.spinner("Building quick analysis..."):
            _qa_rows = build_quick_analysis_rows(
                props=_qa_props,
                players_data=players_data,
                game_logs_cache=_qa_game_logs_cache,
                injury_status_map=_qa_injury_map,
            )

        if not _qa_rows:
            st.warning("No analysis rows generated.")
        else:
            # ── Summary metrics ────────────────────────────────────────
            _all_intel_stubs = [
                {"player_name": r["player_name"], "form": {"form_label": r["form_label"]}}
                for r in _qa_rows
            ]
            _streak_summary = aggregate_streak_summary(_all_intel_stubs)

            _col_h, _col_c, _col_n = st.columns(3)
            with _col_h:
                st.metric("🔥 Hot Players", _streak_summary["hot_count"],
                          help="Players hitting the over in 70%+ of last 5 games")
            with _col_c:
                st.metric("🧊 Cold Players", _streak_summary["cold_count"],
                          help="Players hitting the over in 30% or fewer of last 5 games")
            with _col_n:
                _flagged_count = sum(1 for r in _qa_rows if r.get("is_flagged"))
                st.metric("⚠️ Injury Flagged", _flagged_count,
                          help="Players with GTD / Day-to-Day / Questionable status")

            # ── Filters ────────────────────────────────────────────────
            _qa_filter_col1, _qa_filter_col2, _qa_filter_col3 = st.columns(3)
            with _qa_filter_col1:
                _qa_sort_by = st.selectbox(
                    "Sort by",
                    ["Edge % (Best first)", "Hit Rate (Best first)", "Player Name"],
                    key="qa_sort_by",
                )
            with _qa_filter_col2:
                _qa_form_filter = st.selectbox(
                    "Form filter",
                    ["All", "Hot only 🔥", "Cold only 🧊", "No injury flags"],
                    key="qa_form_filter",
                )
            with _qa_filter_col3:
                _qa_stat_filter = st.selectbox(
                    "Stat type",
                    ["All"] + sorted(set(r.get("stat_type", "") for r in _qa_rows)),
                    key="qa_stat_filter",
                )

            # Apply filters
            _filtered_rows = _qa_rows[:]
            if _qa_form_filter == "Hot only 🔥":
                _filtered_rows = [r for r in _filtered_rows if "Hot" in r.get("form_label", "")]
            elif _qa_form_filter == "Cold only 🧊":
                _filtered_rows = [r for r in _filtered_rows if "Cold" in r.get("form_label", "")]
            elif _qa_form_filter == "No injury flags":
                _filtered_rows = [r for r in _filtered_rows if not r.get("is_flagged")]
            if _qa_stat_filter != "All":
                _filtered_rows = [r for r in _filtered_rows if r.get("stat_type") == _qa_stat_filter]

            # Apply sort
            if _qa_sort_by == "Edge % (Best first)":
                _filtered_rows.sort(key=lambda r: abs(r.get("edge_pct", 0)), reverse=True)
            elif _qa_sort_by == "Hit Rate (Best first)":
                _filtered_rows.sort(key=lambda r: r.get("hit_rate", 0), reverse=True)
            else:
                _filtered_rows.sort(key=lambda r: r.get("player_name", ""))

            st.caption(f"Showing {len(_filtered_rows)} of {len(_qa_rows)} props")

            # ── Quick Analysis rows ────────────────────────────────────
            import html as _html_mod
            for _qrow in _filtered_rows:
                _qp = _qrow.get("player_name", "")
                _qs = _qrow.get("stat_type", "")
                _ql = _qrow.get("line", 0)
                _qavg = _qrow.get("season_avg", 0.0)
                _qedge = _qrow.get("edge_pct", 0.0)
                _qdir = _qrow.get("direction", "—")
                _qhr = _qrow.get("hit_rate", 0.0)
                _qfl = _qrow.get("form_label", "No Data")
                _qform = _qrow.get("form_results", [])
                _qavail = _qrow.get("availability_badge", "🟢 Active")
                _qavail_cls = _qrow.get("availability_class", "avail-active")
                _qinj = _qrow.get("injury_note", "")
                _qstreak = _qrow.get("streak_label", "")
                _qplat = _qrow.get("platform", "")
                _qteam = _qrow.get("team", _qrow.get("player_team", ""))

                # Edge colour
                _edge_css = "qa-edge-pos" if _qedge >= 4 else "qa-edge-neg" if _qedge <= -4 else "qa-edge-neu"
                _edge_sign = "+" if _qedge >= 0 else ""

                # Form label colour
                _flbl_css = "form-label-hot" if "Hot" in _qfl else "form-label-cold" if "Cold" in _qfl else "form-label-neutral"

                # Dots HTML
                _dots_html = get_form_dots_html(_qform, window=5, prop_line=float(_ql or 0))

                # Availability badge HTML
                _avail_html = (
                    f'<span class="avail-badge {_qavail_cls}"'
                    + (f' title="{_html_mod.escape(_qinj)}"' if _qinj else "")
                    + f">{_qavail}</span>"
                )

                # Platform badge
                _plat_lower = _qplat.lower() if _qplat else "default"
                _plat_html = f'<span class="plat-{_plat_lower}">{_qplat}</span>' if _qplat else ""

                # Streak badge
                _streak_html = ""
                if _qstreak:
                    _sbanner = "streak-banner-hot" if "Over" in _qstreak else "streak-banner-cold"
                    _streak_html = (
                        f'<span class="{_sbanner}" style="padding:1px 8px;border-radius:4px;">'
                        f'{_html_mod.escape(_qstreak)}</span>'
                    )

                # Row HTML
                _avg_html = (
                    f'<span style="color:#8a9bb8;font-size:0.75rem;"> (avg {_qavg:.1f})</span>'
                    if _qavg > 0 else ""
                )
                _row_html = (
                    f'<div class="qa-row">'
                    f'<div><span class="qa-player">{_html_mod.escape(_qp)}</span>'
                    f' <span class="qa-stat">{_html_mod.escape(_qteam)}</span> {_plat_html}</div>'
                    f'<div><span class="qa-stat">{_html_mod.escape(_qs.replace("_"," ").title())}</span>'
                    f' <span class="qa-line">{_ql}</span>{_avg_html}</div>'
                    f'<div><span class="qa-edge {_edge_css}">{_edge_sign}{_qedge:.1f}% {_qdir}</span></div>'
                    f'<div style="display:flex;align-items:center;gap:6px;">'
                    f'{_dots_html}<span class="{_flbl_css}">{int(_qhr*100)}% over</span></div>'
                    f'<div>{_avail_html}</div>'
                    + (f'<div>{_streak_html}</div>' if _streak_html else "")
                    + f'</div>'
                )
                st.markdown(_row_html, unsafe_allow_html=True)

            # ── Hot/Cold summary footer ────────────────────────────────
            if _streak_summary["hot_players"]:
                st.success(
                    f"🔥 **Hot players:** {', '.join(_streak_summary['hot_players'][:8])}"
                )
            if _streak_summary["cold_players"]:
                st.warning(
                    f"🧊 **Cold players:** {', '.join(_streak_summary['cold_players'][:8])}"
                )

# ============================================================
# END SECTION: Quick Analysis Panel
# ============================================================

