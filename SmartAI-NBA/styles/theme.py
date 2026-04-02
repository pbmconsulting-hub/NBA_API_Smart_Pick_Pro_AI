# ============================================================
# FILE: styles/theme.py
# PURPOSE: All CSS/HTML generators for the SmartBetPro NBA UI.
#          Provides a futuristic "AI Neural Network Lab" bright
#          theme with glassmorphism cards, animated glow
#          effects, and NBA team colors on a clean light
#          AI-lab background for maximum readability.
# BRAND:   SmartBetPro NBA by Quantum Matrix Engine 5.6
# USAGE:
#   from styles.theme import get_global_css, get_player_card_html
#   st.markdown(get_global_css(), unsafe_allow_html=True)
# ============================================================

# Standard library only — no new dependencies
import base64 as _base64
import datetime as _datetime
import functools as _functools
import html as _html
import logging as _logging
import math as _math
import os as _os

_logger_theme = _logging.getLogger(__name__)

# ── Centralised logo paths ──────────────────────────────────────
GOBLIN_LOGO_PATH = _os.path.join("assets", "New_Goblin_Logo.png")
DEMON_LOGO_PATH  = _os.path.join("assets", "New_Demon_Logo.png")
GOLD_LOGO_PATH   = _os.path.join("assets", "NewGold_Logo.png")


@_functools.lru_cache(maxsize=32)
def _load_logo_b64(logo_path):
    """Load and base64-encode a logo file, cached per path."""
    try:
        with open(logo_path, "rb") as _f:
            return _base64.b64encode(_f.read()).decode()
    except OSError as _e:
        _logger_theme.warning("Could not load logo file '%s': %s", logo_path, _e)
        return None


def get_logo_img_tag(logo_path, width=20, alt="logo"):
    """Return an inline <img> tag for use in st.markdown HTML."""
    if _os.path.exists(logo_path):
        _b64 = _load_logo_b64(logo_path)
        if _b64:
            _safe_alt = _html.escape(str(alt))
            return f'<img src="data:image/png;base64,{_b64}" width="{width}" alt="{_safe_alt}" style="vertical-align:middle;">'
    _logger_theme.debug("Logo not found at '%s', using alt text '%s'", logo_path, alt)
    return _html.escape(str(alt))


# ============================================================
# SECTION: Glossary
# Plain-English explanations for betting / AI terms shown
# via tooltips and education boxes throughout the UI.
# ============================================================

GLOSSARY = {
    "Monte Carlo Simulation": (
        "We simulate thousands of possible game outcomes using the player's historical "
        "stats, matchup data, and current conditions. The percentage you see reflects "
        "how often the player hit the target across all those simulations."
    ),
    "Edge": (
        "The difference between what we think will happen and what the betting line "
        "implies. A positive edge means our model sees more value than the sportsbook "
        "is offering — the bigger the edge, the better the opportunity."
    ),
    "Confidence Score": (
        "A 0–100 rating of how sure the model is about this pick. It combines sample "
        "size, consistency, matchup clarity, and simulation stability. Above 70 = "
        "high confidence; below 50 = proceed with caution."
    ),
    "Expected Value (EV)": (
        "How much money you'd expect to make (or lose) per dollar bet over time if "
        "you placed this wager repeatedly under the same conditions. Positive EV bets "
        "are profitable long-term even if any single bet can lose."
    ),
    "Coefficient of Variation": (
        "A measure of how consistent a player is. It compares the spread of their "
        "game-to-game stats to their average. Low CV = very consistent; high CV = "
        "boom-or-bust performer who is harder to predict."
    ),
    "Prop Bet": (
        "A bet on whether a player will exceed or fall short of a statistical threshold "
        "(the 'line') set by the platform — for example, scoring more or fewer than "
        "24.5 points in a game."
    ),
    "More/Less": (
        "Betting on whether a stat will be higher (More) or lower (Less) than the "
        "line. Our model calculates the true probability of each side so you can "
        "spot when the line is mis-priced."
    ),
    "Line": (
        "The threshold set by the platform for a specific player stat. If the line "
        "for LeBron points is 24.5, you bet whether he scores more (More) or fewer "
        "(Less) than that number."
    ),
    "Parlay": (
        "A multi-pick bet where ALL selections must be correct to win. The payout "
        "multiplies across legs but so does the risk — one wrong pick loses everything. "
        "Stick to high-confidence legs when building parlays."
    ),
    "Bankroll": (
        "The total amount of money set aside exclusively for betting. Good bankroll "
        "management means never risking more than 1–5% on a single bet, so a losing "
        "streak doesn't wipe you out before the edge plays out."
    ),
    "Goblin Bet": (
        "A PrizePicks More/Less prop with boosted payout odds — typically a harder "
        "line to hit but with a higher reward. The model evaluates Goblin lines the "
        "same way as standard props so you can see the true probability."
    ),
    "Demon Bet": (
        "A PrizePicks More/Less prop at reduced payout odds — usually an easier "
        "line but with a lower reward. Our analysis still applies full simulation "
        "so you know whether the edge justifies the smaller payout."
    ),
    "50/50 Bet": (
        "A PrizePicks More/Less prop where the platform treats both sides as equally "
        "likely. Our model often finds an edge even on 50/50 lines because our "
        "simulation uses real matchup data rather than symmetric pricing."
    ),
}


# ============================================================
# END SECTION: Glossary
# ============================================================


# ============================================================
# SECTION: NBA Team Colors
# Primary and secondary hex colors for each franchise.
# ============================================================

# Maps team abbreviation → (primary_color, secondary_color)
_TEAM_COLORS = {
    "ATL": ("#C8102E", "#FDB927"),
    "BOS": ("#007A33", "#BA9653"),
    "BKN": ("#000000", "#FFFFFF"),
    "CHA": ("#1D1160", "#00788C"),
    "CHI": ("#CE1141", "#000000"),
    "CLE": ("#860038", "#FDBB30"),
    "DAL": ("#00538C", "#002B5E"),
    "DEN": ("#0E2240", "#FEC524"),
    "DET": ("#C8102E", "#1D42BA"),
    "GSW": ("#1D428A", "#FFC72C"),
    "HOU": ("#CE1141", "#000000"),
    "IND": ("#002D62", "#FDBB30"),
    "LAC": ("#C8102E", "#1D428A"),
    "LAL": ("#552583", "#FDB927"),
    "MEM": ("#5D76A9", "#12173F"),
    "MIA": ("#98002E", "#F9A01B"),
    "MIL": ("#00471B", "#EEE1C6"),
    "MIN": ("#0C2340", "#236192"),
    "NOP": ("#0C2340", "#C8102E"),
    "NYK": ("#006BB6", "#F58426"),
    "OKC": ("#007AC1", "#EF3B24"),
    "ORL": ("#0077C0", "#C4CED4"),
    "PHI": ("#006BB6", "#ED174C"),
    "PHX": ("#1D1160", "#E56020"),
    "POR": ("#E03A3E", "#000000"),
    "SAC": ("#5A2D81", "#63727A"),
    "SAS": ("#C4CED4", "#000000"),
    "TOR": ("#CE1141", "#000000"),
    "UTA": ("#002B5C", "#00471B"),
    "WAS": ("#002B5C", "#E31837"),
}

_DEFAULT_TEAM_COLORS = ("#00f0ff", "#0a1a2e")


def get_team_colors(team_abbrev):
    """
    Return (primary, secondary) hex colors for an NBA team.

    Args:
        team_abbrev (str): 3-letter team abbreviation (e.g., 'LAL')

    Returns:
        tuple: (primary_color, secondary_color) hex strings
    """
    return _TEAM_COLORS.get(team_abbrev.upper() if team_abbrev else "", _DEFAULT_TEAM_COLORS)


# ============================================================
# END SECTION: NBA Team Colors
# ============================================================


# ============================================================
# SECTION: Global CSS Theme
# Injected once per page via st.markdown(unsafe_allow_html=True)
# ============================================================

def get_global_css():
    """
    Return the full CSS string for the SmartBetPro NBA Quantum Edge dark theme.

    Implements a dark futuristic "AI command center" theme with:
    - Deep space dark backgrounds (#0a0f1a) with radial gradient
    - Glassmorphism cards with neon cyan glow borders on dark
    - Neon orange (#ff5e00) primary + holographic cyan (#00f0ff) secondary accents
    - Electric green (#00ff9d) for success, neon purple (#c800ff) tertiary
    - Orbitron font for headings, Montserrat for body
    - Animated holographic shimmer overlays
    - Futuristic tier badges with neon glow effects
    - Pulsing live-indicator dot animation with cyan glow
    - Monospace terminal readout class
    - Smooth hover transitions with lift + increased glow
    - Sidebar "Powered by Quantum Matrix Engine 5.6" branding with neon accent
    - Custom dark scrollbar with cyan thumb

    Returns:
        str: Full <style>...</style> block ready for st.markdown()
    """
    return """
<style>
/* ─── Google Fonts ────────────────────────────────────────── */
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800;900&family=JetBrains+Mono:wght@400;500;600;700;800&family=Orbitron:wght@400;700;800;900&family=Montserrat:wght@400;600;700&display=swap');

/* ─── Keyframe Animations ─────────────────────────────────── */
@keyframes borderGlow {
    0%, 100% { box-shadow: 0 0 12px rgba(0,240,255,0.15),
                            0 4px 24px rgba(0,240,255,0.07); }
    50%       { box-shadow: 0 0 28px rgba(0,240,255,0.35),
                            0 4px 30px rgba(0,240,255,0.15); }
}
@keyframes pulse-platinum {
    0%, 100% { box-shadow: 0 0 10px rgba(0,240,255,0.30); }
    50%       { box-shadow: 0 0 24px rgba(0,240,255,0.60); }
}
@keyframes pulse-gold {
    0%, 100% { box-shadow: 0 0 10px rgba(255,94,0,0.35); }
    50%       { box-shadow: 0 0 24px rgba(255,94,0,0.65); }
}
@keyframes live-dot-pulse {
    0%, 100% { opacity: 1; transform: scale(1); }
    50%       { opacity: 0.4; transform: scale(1.35); }
}
@keyframes fadeInUp {
    from { opacity: 0; transform: translateY(10px); }
    to   { opacity: 1; transform: translateY(0); }
}
@keyframes headerShimmer {
    0%   { background-position: 0% 50%; }
    50%  { background-position: 100% 50%; }
    100% { background-position: 0% 50%; }
}
@keyframes hologramEffect {
    0%   { background-position: -200% center; }
    100% { background-position: 200% center; }
}
/* ── "The Pulse" — live-indicator glow dot ─────────────────── */
@keyframes thePulse {
    0%, 100% { box-shadow: 0 0 4px 1px rgba(0,255,157,0.60); opacity: 1; }
    50%      { box-shadow: 0 0 12px 4px rgba(0,255,157,0.90); opacity: 0.7; }
}
/* ── State-aware fade-in-up: plays once, no thrash on rerun ── */
@keyframes ssFadeInUp {
    from { opacity: 0; transform: translateY(16px); }
    to   { opacity: 1; transform: translateY(0); }
}

/* ═══════════════════════════════════════════════════════════
   PILLAR 1 — Streamlit Chrome Obliteration
   ═══════════════════════════════════════════════════════════ */
#MainMenu { visibility: hidden !important; }
header[data-testid="stHeader"] { display: none !important; }
footer { display: none !important; }
.stDeployButton { display: none !important; }
.block-container { padding-top: 1rem !important; }

/* ─── Base / Body ─────────────────────────────────────────── */
html, body, [class*="css"] {
    font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
    font-size: 16px;
    color: #c8d8f0;
    background-color: #070A13;
}
/* Deep obsidian background with radial gradient */
.stApp {
    background-color: #070A13;
    background-image:
        radial-gradient(ellipse at 20% 20%, rgba(0,240,255,0.04) 0%, transparent 50%),
        radial-gradient(ellipse at 80% 80%, rgba(200,0,255,0.03) 0%, transparent 50%),
        radial-gradient(ellipse at center, #0d1220 0%, #070A13 100%);
}
/* Override stAppViewContainer for institutional dark gradient */
[data-testid="stAppViewContainer"] {
    background: radial-gradient(ellipse at 30% 10%, rgba(0,240,255,0.03) 0%, transparent 45%),
                radial-gradient(ellipse at 70% 90%, rgba(200,0,255,0.025) 0%, transparent 50%),
                radial-gradient(ellipse at center, #0a0e18 0%, #070A13 100%);
}

/* All JetBrains Mono / monospace text gets tabular-nums for alignment */
[style*="JetBrains"], .stat-readout, .prob-value, .edge-badge,
.dist-p10, .dist-p50, .dist-p90, .dist-label,
.summary-value, .status-card-value, .nba-stat-number,
.verdict-confidence, .hero-subtext, .hero-date,
code, pre, .monospace {
    font-variant-numeric: tabular-nums !important;
}

/* Streamlit text defaults on dark background */
[data-testid="stMarkdownContainer"] p,
[data-testid="stMarkdownContainer"] li,
[data-testid="stMarkdownContainer"] span,
[data-testid="stCaptionContainer"],
.stTextInput label,
.stSelectbox label,
.stSlider label,
.stCheckbox label,
.stRadio label {
    color: #c8d8f0 !important;
    font-size: 1rem !important;
}
[data-testid="stMarkdownContainer"] h1,
[data-testid="stMarkdownContainer"] h2,
[data-testid="stMarkdownContainer"] h3,
[data-testid="stMarkdownContainer"] h4,
[data-testid="stMarkdownContainer"] h5,
[data-testid="stMarkdownContainer"] h6,
.stHeadingWithActionElements,
h1, h2, h3, h4, h5, h6 {
    color: #00f0ff !important;
    font-family: 'Orbitron', sans-serif !important;
    letter-spacing: 0.05em;
}

/* Custom scrollbar — ultra-thin dark track, cyan thumb */
::-webkit-scrollbar { width: 4px; height: 4px; }
::-webkit-scrollbar-track { background: rgba(7,10,19,0.9); }
::-webkit-scrollbar-thumb { background: rgba(0,240,255,0.30); border-radius: 2px; }
::-webkit-scrollbar-thumb:hover { background: rgba(0,240,255,0.55); }

/* ─── Sidebar — enhanced dark panel with neon border ─────── */
/* min-width: 280px ensures emoji + full page titles are always readable */
[data-testid="stSidebar"] {
    background: #060910 !important;
    border-right: 1px solid rgba(0,240,255,0.20) !important;
    box-shadow: 2px 0 20px rgba(0,240,255,0.05) !important;
    min-width: 280px !important;
}
[data-testid="stSidebar"] p,
[data-testid="stSidebar"] span,
[data-testid="stSidebar"] div,
[data-testid="stSidebar"] label,
[data-testid="stSidebar"] a {
    color: #c0d0e8 !important;
}
[data-testid="stSidebar"] .stPageLink,
[data-testid="stSidebar"] [data-testid="stSidebarNavLink"] {
    font-size: 0.9rem !important;
    white-space: nowrap !important;
    overflow: visible !important;
    text-overflow: unset !important;
}
[data-testid="stSidebar"]::after {
    content: "⚡ Powered by Quantum Matrix Engine 5.6";
    display: block;
    position: fixed;
    bottom: 18px;
    left: 0;
    width: 100%;
    padding: 0 20px;
    box-sizing: border-box;
    text-align: center;
    font-size: 0.68rem;
    font-family: 'JetBrains Mono', 'Courier New', monospace;
    font-weight: 700;
    color: rgba(0,240,255,0.70) !important;
    letter-spacing: 0.08em;
    pointer-events: none;
    text-shadow: 0 0 8px rgba(0,240,255,0.5);
}

/* ─── Sidebar Logo — enlarged per branding directive ─────── */
[data-testid="stLogo"] {
    max-width: none !important;
    width: auto !important;
    height: auto !important;
}
[data-testid="stLogo"] img,
[data-testid="stSidebarHeader"] img {
    width: 100% !important;
    max-width: 220px !important;
    height: auto !important;
    object-fit: contain !important;
    transform: scale(1.2);
    margin-left: -5px;
}

/* ─── Streamlit native elements on dark bg ───────────────── */
/* Metric glassmorphic card treatment */
[data-testid="stMetric"] {
    background: rgba(15,23,42,0.55);
    border: 1px solid rgba(255,255,255,0.05);
    border-radius: 14px;
    padding: 18px 20px;
    backdrop-filter: blur(12px);
    -webkit-backdrop-filter: blur(12px);
    box-shadow: 0 0 20px rgba(0,240,255,0.04), 0 4px 20px rgba(0,0,0,0.30);
    transition: border-color 0.25s ease, box-shadow 0.25s ease, transform 0.25s ease;
}
[data-testid="stMetric"]:hover {
    border-color: rgba(0,240,255,0.20);
    box-shadow: 0 0 28px rgba(0,240,255,0.10), 0 6px 24px rgba(0,0,0,0.40);
    transform: translateY(-3px);
}
[data-testid="stMetricValue"] { color: rgba(255,255,255,0.95) !important; font-size: 1.4rem !important; font-family: 'JetBrains Mono', 'Courier New', monospace !important; font-variant-numeric: tabular-nums !important; }
[data-testid="stMetricLabel"] { color: #94A3B8 !important; font-size: 0.82rem !important; text-transform: uppercase; letter-spacing: 0.08em; font-family: 'Inter', sans-serif !important; }
[data-testid="stMetricDelta"] { font-family: 'JetBrains Mono', monospace !important; font-variant-numeric: tabular-nums !important; }

/* ═══════════════════════════════════════════════════════════
   PILLAR 2 — Terminal-Style Alert Overrides
   ═══════════════════════════════════════════════════════════ */
.stAlert { background: rgba(15,23,42,0.90) !important; border-radius: 8px !important; border: none !important; color: #e0eeff !important; font-size: 0.95rem !important; padding: 14px 18px !important; backdrop-filter: blur(8px) !important; -webkit-backdrop-filter: blur(8px) !important; }
/* st.error → red neon left-border */
[data-testid="stAlert"][data-baseweb*="negative"],
div[data-testid="stNotification"][data-type="error"],
.stAlert .st-emotion-cache-1gulkj5 {
    border-left: 3px solid #ef4444 !important;
    background: rgba(239,68,68,0.06) !important;
}
/* st.warning → amber neon left-border */
[data-testid="stAlert"][data-baseweb*="warning"],
div[data-testid="stNotification"][data-type="warning"] {
    border-left: 3px solid #f59e0b !important;
    background: rgba(245,158,11,0.06) !important;
}
/* st.success → green neon left-border */
[data-testid="stAlert"][data-baseweb*="positive"],
div[data-testid="stNotification"][data-type="success"] {
    border-left: 3px solid #00ff9d !important;
    background: rgba(0,255,157,0.05) !important;
}
/* st.info → cyan neon left-border */
[data-testid="stAlert"][data-baseweb*="informational"],
div[data-testid="stNotification"][data-type="info"] {
    border-left: 3px solid #00f0ff !important;
    background: rgba(0,240,255,0.05) !important;
}

/* "The Pulse" — animated live-indicator dot */
.the-pulse {
    display: inline-block;
    width: 8px;
    height: 8px;
    border-radius: 50%;
    background: #00ff9d;
    animation: thePulse 1.8s ease-in-out infinite;
    vertical-align: middle;
    margin-right: 6px;
    flex-shrink: 0;
}
/* State-aware fade: only plays once, no thrash on Streamlit reruns */
.ss-fade-in-up {
    animation: ssFadeInUp 0.4s ease both;
}
.stExpander { background: rgba(13,18,32,0.80) !important; border: 1px solid rgba(0,240,255,0.15) !important; border-radius: 12px !important; }
.stExpander summary, .stExpander [data-testid="stExpanderToggleIcon"] + span { color: #e0eeff !important; font-size: 1rem !important; font-weight: 600 !important; }
button[kind="primary"] {
    background: linear-gradient(135deg, #00ffd5, #00b4ff) !important;
    color: #070A13 !important;
    border: none !important;
    font-family: 'Inter', sans-serif !important;
    font-weight: 700 !important;
    letter-spacing: 0.04em !important;
    box-shadow: 0 0 16px rgba(0,255,213,0.30) !important;
    transition: transform 0.2s ease, box-shadow 0.2s ease !important;
}
button[kind="primary"]:hover {
    transform: translateY(-2px) !important;
    box-shadow: 0 0 28px rgba(0,255,213,0.50), 0 6px 20px rgba(0,0,0,0.4) !important;
}
/* Secondary / default buttons — tactile hover */
.stButton > button {
    transition: transform 0.2s ease, box-shadow 0.2s ease, border-color 0.2s ease !important;
}
.stButton > button:hover {
    transform: translateY(-2px) !important;
    box-shadow: 0 4px 20px rgba(0,240,255,0.15) !important;
}
/* Tab labels */
[data-testid="stTab"] button {
    font-size: 1rem !important;
    font-weight: 600 !important;
    color: #c8d8f0 !important;
}
[data-testid="stTab"] button[aria-selected="true"] {
    color: #00f0ff !important;
    border-bottom: 2px solid #00f0ff !important;
}
/* Dataframe / table text — terminal look */
[data-testid="stDataFrame"] {
    border: none !important;
    border-radius: 12px !important;
    overflow: hidden !important;
}
[data-testid="stDataFrame"] td {
    font-size: 0.92rem !important;
    color: #e0eeff !important;
    font-family: 'JetBrains Mono', 'Courier New', monospace !important;
    font-variant-numeric: tabular-nums !important;
    border-color: rgba(0,240,255,0.06) !important;
    transition: border-color 0.15s ease !important;
}
[data-testid="stDataFrame"] th {
    font-size: 0.75rem !important;
    color: #94A3B8 !important;
    text-transform: uppercase !important;
    letter-spacing: 1px !important;
    font-family: 'Inter', sans-serif !important;
    font-weight: 700 !important;
    background: rgba(7,10,19,0.90) !important;
    border-color: rgba(0,240,255,0.08) !important;
}
/* Row hover glow — neon-cyan highlight */
[data-testid="stDataFrame"] tr:hover td {
    border-bottom-color: rgba(0,240,255,0.30) !important;
    background: rgba(0,240,255,0.06) !important;
}
/* Strip native table borders */
[data-testid="stDataFrame"] table { border-collapse: collapse !important; }
[data-testid="stDataFrame"] th,
[data-testid="stDataFrame"] td { border: none !important; }
.stDataFrame, .stTable { background: rgba(15,23,42,0.85) !important; color: #e0eeff !important; }

/* ─── Tier Badges ─────────────────────────────────────────── */
.tier-platinum {
    background: linear-gradient(135deg, rgba(0,240,255,0.12), rgba(0,255,157,0.08));
    border: 1px solid rgba(0,240,255,0.35);
    color: #00f0ff;
    padding: 4px 12px;
    border-radius: 8px;
    font-weight: 700;
    font-size: 0.78rem;
    text-transform: uppercase;
    letter-spacing: 0.1em;
    animation: pulse-platinum 2.5s ease-in-out infinite;
}
.tier-gold {
    background: linear-gradient(135deg, rgba(255,94,0,0.12), rgba(255,215,0,0.08));
    border: 1px solid rgba(255,94,0,0.35);
    color: #ff5e00;
    padding: 4px 12px;
    border-radius: 8px;
    font-weight: 700;
    font-size: 0.78rem;
    text-transform: uppercase;
    letter-spacing: 0.1em;
    animation: pulse-gold 2.5s ease-in-out infinite;
}
.tier-silver {
    background: linear-gradient(135deg, rgba(148,163,184,0.12), rgba(200,216,240,0.08));
    border: 1px solid rgba(148,163,184,0.25);
    color: #94A3B8;
    padding: 4px 12px;
    border-radius: 8px;
    font-weight: 700;
    font-size: 0.78rem;
    text-transform: uppercase;
    letter-spacing: 0.1em;
}
/* ─── State-aware fade-in-up — plays once on initial load ─── */
.qds-fade-in {
    animation: ssFadeInUp 0.5s ease both;
    animation-fill-mode: both;
}

/* ─── Analysis Card (smartai-card) ───────────────────────── */
.smartai-card {
    background: rgba(13,18,32,0.85);
    border: 1px solid rgba(0,240,255,0.15);
    border-radius: 16px;
    padding: 20px 24px;
    margin-bottom: 18px;
    backdrop-filter: blur(12px);
    -webkit-backdrop-filter: blur(12px);
    box-shadow: 0 0 20px rgba(0,240,255,0.08), 0 4px 24px rgba(0,0,0,0.4);
    animation: borderGlow 3.5s ease-in-out infinite,
               fadeInUp 0.35s ease both;
    transition: border-color 0.2s ease, transform 0.2s ease, box-shadow 0.2s ease;
    position: relative;
    overflow: hidden;
}
.smartai-card::before {
    content: '';
    position: absolute;
    top: 0; left: 0; right: 0; height: 3px;
    background: linear-gradient(90deg, #00f0ff, #00ff9d, #ff5e00, #c800ff, #00f0ff);
    background-size: 200% 100%;
    animation: headerShimmer 4s ease infinite;
    opacity: 0.9;
}
.smartai-card:hover {
    border-color: rgba(0,240,255,0.40);
    transform: translateY(-5px);
    box-shadow: 0 0 30px rgba(0,240,255,0.20), 0 8px 32px rgba(0,0,0,0.5);
}

/* ─── Neural Header ───────────────────────────────────────── */
.neural-header {
    background: linear-gradient(135deg, #070A13 0%, #0d1a2e 50%, #070A13 100%);
    border: 1px solid rgba(0,240,255,0.30);
    border-radius: 16px;
    padding: 24px 30px;
    margin-bottom: 20px;
    backdrop-filter: blur(10px);
    -webkit-backdrop-filter: blur(10px);
    position: relative;
    overflow: hidden;
    text-align: center;
    box-shadow: 0 0 30px rgba(0,240,255,0.12), 0 4px 24px rgba(0,0,0,0.5);
}
.neural-header::before {
    content: '';
    position: absolute;
    top: 0; left: 0; right: 0; height: 3px;
    background: linear-gradient(90deg, #00f0ff, #00ff9d, #ff5e00, #c800ff, #00f0ff);
    background-size: 200% 100%;
    animation: headerShimmer 3s linear infinite;
}
.neural-header-title {
    font-size: 2rem;
    font-weight: 900;
    font-family: 'Orbitron', sans-serif;
    background: linear-gradient(135deg, #00f0ff, #00ff9d);
    -webkit-background-clip: text;
    -webkit-text-fill-color: transparent;
    background-clip: text;
    letter-spacing: 0.05em;
    line-height: 1.15;
    text-shadow: none;
    filter: drop-shadow(0 0 12px rgba(0,240,255,0.5));
}
.neural-header-subtitle {
    font-size: 0.88rem;
    color: rgba(192,208,232,0.80);
    margin-top: 6px;
    font-family: 'JetBrains Mono', 'Courier New', monospace;
    letter-spacing: 0.06em;
}
.circuit-dot {
    display: inline-block;
    width: 8px; height: 8px;
    border-radius: 50%;
    background: #00f0ff;
    margin: 0 6px;
    vertical-align: middle;
    animation: live-dot-pulse 1.8s ease-in-out infinite;
    box-shadow: 0 0 8px rgba(0,240,255,0.9);
}

/* ─── Smart Pick Pro Hero Header ─────────────────────────── */
/* Used on the main app.py page to display logo + app name.  */
.spp-hero-header {
    display: flex;
    align-items: center;
    gap: 22px;
    text-align: left;
}
/* Logo circle thumbnail inside the hero header */
.spp-hero-logo {
    max-width: 80%;
    height: auto;
    object-fit: contain;
    border-radius: 50%;
    box-shadow: 0 0 18px rgba(0,240,255,0.35), 0 0 8px rgba(200,16,46,0.25);
    flex-shrink: 0;
}
/* "NBA EDITION" red label shown below "Smart Pick Pro" */
.nba-edition-label {
    font-size: 1.05rem;
    letter-spacing: 0.22em;
    color: #C8102E;
    font-family: 'Bebas Neue', 'Oswald', monospace;
    font-weight: 700;
    margin-top: 4px;
    text-shadow: 0 0 10px rgba(200,16,46,0.45);
}

/* ─── Player Name & Team Pill ─────────────────────────────── */
.player-name {
    font-size: 1.3rem;
    font-weight: 800;
    font-family: 'Orbitron', sans-serif;
    color: rgba(255,255,255,0.95);
    letter-spacing: 0.02em;
}
.team-pill {
    display: inline-block;
    padding: 2px 9px;
    border-radius: 6px;
    font-weight: 700;
    font-size: 0.8rem;
    color: #fff;
    background: rgba(0,240,255,0.15);
    margin-left: 8px;
    vertical-align: middle;
    border: 1px solid rgba(0,240,255,0.35);
    box-shadow: 0 0 8px rgba(0,240,255,0.15);
}
.position-tag {
    color: #b0bec5;
    font-size: 0.82rem;
    margin-left: 8px;
    vertical-align: middle;
}

/* ─── Tier Badges ─────────────────────────────────────────── */
.tier-badge {
    display: inline-block;
    padding: 5px 14px;
    border-radius: 20px;
    font-weight: 800;
    font-size: 0.9rem;
    font-family: 'Orbitron', sans-serif;
    letter-spacing: 0.05em;
    text-transform: uppercase;
    position: relative;
    transition: box-shadow 0.2s ease, transform 0.2s ease;
}
.tier-badge:hover {
    transform: scale(1.04);
}
.tier-platinum {
    background: linear-gradient(135deg, #d4d8e0, #ffffff, #b0b8c8, #ffffff, #d4d8e0);
    background-size: 300% 100%;
    color: #1a2035;
    border: 1px solid rgba(255,255,255,0.50);
    box-shadow: 0 0 18px rgba(255,255,255,0.25), 0 0 6px rgba(0,240,255,0.20);
    text-shadow: 0 1px 2px rgba(0,0,0,0.15);
    animation: pulse-platinum 2.5s infinite, nba-shimmer-platinum 3s linear infinite;
}
.tier-gold {
    background: linear-gradient(135deg, #a67c00, #ffd700, #c9a800, #ffd700, #a67c00);
    background-size: 300% 100%;
    color: #2a1800;
    border: 1px solid rgba(255,215,0,0.55);
    box-shadow: 0 0 16px rgba(255,215,0,0.30), 0 0 6px rgba(255,160,0,0.20);
    text-shadow: 0 1px 2px rgba(0,0,0,0.20);
    animation: pulse-gold 2.8s infinite, nba-gold-gleam 4s ease-in-out infinite;
}
.tier-silver {
    background: linear-gradient(135deg, #8a8e96, #c0c0c0, #a8acb4, #c0c0c0, #8a8e96);
    background-size: 300% 100%;
    color: #1a1f30;
    border: 1px solid rgba(192,192,192,0.45);
    box-shadow: 0 0 12px rgba(192,192,192,0.20);
    text-shadow: 0 1px 1px rgba(0,0,0,0.15);
    animation: nba-silver-sheen 3.5s linear infinite;
}
.tier-bronze {
    background: linear-gradient(135deg, #8B4513, #CD7F32, #a0652a, #CD7F32, #8B4513);
    background-size: 300% 100%;
    color: #fff;
    border: 1px solid rgba(205,127,50,0.50);
    box-shadow: 0 0 10px rgba(205,127,50,0.25);
    animation: nba-bronze-pulse 2.5s ease-in-out infinite;
}

/* ─── AI Verdict Card ─────────────────────────────────────── */
.verdict-bet {
    background: rgba(0,255,157,0.06);
    border: 2px solid rgba(0,255,157,0.45);
    border-radius: 14px;
    padding: 16px 20px;
    animation: borderGlow 2.5s ease-in-out infinite;
    box-shadow: 0 0 20px rgba(0,255,157,0.10);
}
.verdict-avoid {
    background: rgba(239,68,68,0.07);
    border: 2px solid rgba(239,68,68,0.45);
    border-radius: 14px;
    padding: 16px 20px;
    box-shadow: 0 0 20px rgba(239,68,68,0.10);
}
.verdict-risky {
    background: rgba(255,94,0,0.07);
    border: 2px solid rgba(255,94,0,0.40);
    border-radius: 14px;
    padding: 16px 20px;
    box-shadow: 0 0 20px rgba(255,94,0,0.10);
}
.verdict-label {
    font-size: 1.4rem;
    font-weight: 900;
    font-family: 'Orbitron', sans-serif;
    letter-spacing: 0.10em;
    text-transform: uppercase;
}
.verdict-label-bet   { color: #00ff9d; text-shadow: 0 0 10px rgba(0,255,157,0.6); }
.verdict-label-avoid { color: #ff4444; text-shadow: 0 0 10px rgba(239,68,68,0.6); }
.verdict-label-risky { color: #ff5e00; text-shadow: 0 0 10px rgba(255,94,0,0.6); }
.verdict-confidence {
    font-size: 0.8rem;
    color: #b0bec5;
    font-family: 'JetBrains Mono', 'Courier New', monospace;
    margin-top: 4px;
}
.verdict-explanation {
    font-size: 0.9rem;
    color: rgba(192,208,232,0.90);
    margin-top: 10px;
    line-height: 1.55;
    border-top: 1px solid rgba(0,240,255,0.12);
    padding-top: 10px;
}

/* ─── Stat Readout (monospace terminal) ───────────────────── */
.stat-readout {
    font-family: 'JetBrains Mono', 'Courier New', monospace;
    background: rgba(0,200,255,0.05);
    border: 1px solid rgba(0,240,255,0.15);
    border-radius: 8px;
    padding: 8px 14px;
    margin: 4px 0;
    display: flex;
    justify-content: space-between;
    align-items: center;
    transition: background 0.2s ease;
}
.stat-readout:hover {
    background: rgba(0,200,255,0.10);
}
.stat-readout-label {
    color: #b0bec5;
    font-size: 0.8rem;
    text-transform: uppercase;
    letter-spacing: 0.07em;
}
.stat-readout-value {
    color: #00f0ff;
    font-size: 1rem;
    font-weight: 700;
    text-shadow: 0 0 6px rgba(0,240,255,0.5);
}
.stat-readout-context {
    color: #b0bec5;
    font-size: 0.75rem;
    margin-left: 10px;
}

/* ─── Education Box ───────────────────────────────────────── */
.education-box {
    background: rgba(13,18,32,0.70);
    border: 1px solid rgba(0,240,255,0.18);
    border-radius: 12px;
    padding: 14px 18px;
    margin: 10px 0;
    transition: background 0.2s ease;
    backdrop-filter: blur(8px);
    -webkit-backdrop-filter: blur(8px);
}
.education-box:hover {
    background: rgba(13,18,32,0.90);
}
.education-box-title {
    font-size: 0.88rem;
    font-weight: 700;
    color: #00f0ff;
    display: flex;
    align-items: center;
    gap: 7px;
    cursor: pointer;
    user-select: none;
    text-shadow: 0 0 6px rgba(0,240,255,0.4);
}
.education-box-content {
    font-size: 0.84rem;
    color: rgba(192,208,232,0.90);
    margin-top: 9px;
    line-height: 1.6;
    border-top: 1px solid rgba(0,240,255,0.12);
    padding-top: 9px;
}

/* ─── Progress Ring ───────────────────────────────────────── */
.progress-ring-wrap {
    display: inline-flex;
    flex-direction: column;
    align-items: center;
    gap: 4px;
}
.progress-ring-label {
    font-size: 0.72rem;
    color: #b0bec5;
    text-transform: uppercase;
    letter-spacing: 0.08em;
    font-family: 'JetBrains Mono', 'Courier New', monospace;
}

/* ─── Signal Strength Bar ─────────────────────────────────── */
.signal-bar-wrap {
    display: inline-flex;
    align-items: flex-end;
    gap: 3px;
    height: 22px;
    vertical-align: middle;
}
.signal-bar-seg {
    width: 7px;
    border-radius: 2px;
    background: rgba(0,240,255,0.12);
    transition: background 0.2s ease;
}
.signal-bar-seg.active {
    background: linear-gradient(180deg, #00f0ff, #00c8ff);
    box-shadow: 0 0 4px rgba(0,240,255,0.5);
}
.signal-strength-label {
    font-size: 0.72rem;
    color: #b0bec5;
    font-family: 'JetBrains Mono', 'Courier New', monospace;
    margin-left: 6px;
    vertical-align: middle;
}

/* ─── Inline Tooltip ──────────────────────────────────────── */
.edu-tooltip {
    position: relative;
    display: inline-block;
    border-bottom: 1px dashed rgba(0,240,255,0.6);
    color: #00f0ff;
    cursor: help;
    font-weight: 600;
}
.edu-tooltip .tooltip-text {
    visibility: hidden;
    opacity: 0;
    width: 260px;
    background: rgba(7,10,19,0.97);
    border: 1px solid rgba(0,240,255,0.35);
    color: #e2e8f0;
    font-size: 0.8rem;
    font-weight: 400;
    line-height: 1.5;
    border-radius: 10px;
    padding: 10px 14px;
    position: absolute;
    z-index: 999;
    bottom: 130%;
    left: 50%;
    transform: translateX(-50%);
    transition: opacity 0.2s ease;
    box-shadow: 0 4px 20px rgba(0,240,255,0.15), 0 4px 16px rgba(0,0,0,0.5);
    pointer-events: none;
}
.edu-tooltip:hover .tooltip-text {
    visibility: visible;
    opacity: 1;
}

/* ─── Platform & Stat Badges ──────────────────────────────── */
.platform-badge {
    display: inline-block;
    padding: 3px 9px;
    border-radius: 5px;
    font-size: 0.78rem;
    font-weight: 700;
    transition: opacity 0.2s ease;
}
.stat-chip {
    display: inline-block;
    background: rgba(0,240,255,0.07);
    border: 1px solid rgba(0,240,255,0.20);
    border-radius: 8px;
    padding: 4px 11px;
    margin-right: 6px;
    margin-top: 4px;
    color: rgba(255,255,255,0.90);
    font-size: 0.83rem;
    font-weight: 600;
    transition: background 0.2s ease;
}
.stat-chip:hover {
    background: rgba(0,240,255,0.14);
}
.stat-label { color: #b0bec5; font-size: 0.72rem; }

/* ─── Probability Gauge ───────────────────────────────────── */
.prob-gauge-wrap {
    background: rgba(13,18,32,0.80);
    border-radius: 10px;
    height: 16px;
    overflow: hidden;
    margin-top: 6px;
    border: 1px solid rgba(0,240,255,0.12);
}
.prob-gauge-fill-over {
    background: linear-gradient(90deg, #00f0ff, #00e7ff, #00ff9d);
    height: 100%;
    border-radius: 10px;
    transition: width 0.5s ease;
    box-shadow: 0 0 10px rgba(0,240,255,0.50);
}
.prob-gauge-fill-under {
    background: linear-gradient(90deg, #dc2626, #f87171);
    height: 100%;
    border-radius: 10px;
    transition: width 0.5s ease;
    box-shadow: 0 0 8px rgba(220,38,38,0.45);
}
.prob-value {
    font-size: 1.15rem;
    font-weight: 800;
    color: rgba(255,255,255,0.95);
    font-family: 'JetBrains Mono', 'Courier New', monospace;
}
.edge-badge {
    padding: 2px 8px;
    border-radius: 6px;
    font-size: 0.82rem;
    font-weight: 700;
    font-family: 'JetBrains Mono', 'Courier New', monospace;
}
.edge-positive { background: rgba(0,255,157,0.10); color: #00ff9d; border: 1px solid rgba(0,255,157,0.35); text-shadow: 0 0 6px rgba(0,255,157,0.4); }
.edge-negative { background: rgba(220,38,38,0.10); color: #ff6b6b; border: 1px solid rgba(220,38,38,0.35); }

/* ─── Direction Badge ─────────────────────────────────────── */
.dir-over {
    background: rgba(0,255,157,0.10);
    color: #00ff9d;
    padding: 4px 12px;
    border-radius: 14px;
    font-weight: 800;
    font-size: 0.9rem;
    border: 1px solid rgba(0,255,157,0.35);
    font-family: 'JetBrains Mono', 'Courier New', monospace;
    text-shadow: 0 0 6px rgba(0,255,157,0.5);
}
.dir-under {
    background: rgba(220,38,38,0.10);
    color: #ff6b6b;
    padding: 4px 12px;
    border-radius: 14px;
    font-weight: 800;
    font-size: 0.9rem;
    border: 1px solid rgba(220,38,38,0.35);
    font-family: 'JetBrains Mono', 'Courier New', monospace;
}

/* ─── Force Bar ───────────────────────────────────────────── */
.force-bar-wrap {
    display: flex;
    height: 10px;
    border-radius: 5px;
    overflow: hidden;
    background: rgba(13,18,32,0.80);
    margin-top: 5px;
    border: 1px solid rgba(0,240,255,0.10);
}
.force-bar-over  { background: linear-gradient(90deg, #00f0ff, #00ff9d); }
.force-bar-under { background: linear-gradient(90deg, #dc2626, #f87171); }

/* ─── Distribution Range ──────────────────────────────────── */
.dist-range-wrap { text-align: right; }
.dist-p10  { color: #ff6b6b; font-size: 0.82rem; font-weight: 700; font-family: 'JetBrains Mono', 'Courier New', monospace; }
.dist-p50  { color: rgba(255,255,255,0.95); font-size: 0.9rem; font-weight: 800; font-family: 'JetBrains Mono', 'Courier New', monospace; }
.dist-p90  { color: #00f0ff; font-size: 0.82rem; font-weight: 700; font-family: 'JetBrains Mono', 'Courier New', monospace; }
.dist-sep  { color: #4a5568; font-size: 0.82rem; margin: 0 3px; }
.dist-label { color: #b0bec5; font-size: 0.7rem; font-family: 'JetBrains Mono', 'Courier New', monospace; }

/* ─── Form Dots ───────────────────────────────────────────── */
.form-dot-over  { display:inline-block; width:11px; height:11px; border-radius:50%;
                  background:#00ff9d; box-shadow:0 0 6px rgba(0,255,157,0.70);
                  margin:1px; vertical-align:middle; }
.form-dot-under { display:inline-block; width:11px; height:11px; border-radius:50%;
                  background:#ef4444; box-shadow:0 0 6px rgba(239,68,68,0.65);
                  margin:1px; vertical-align:middle; }

/* ─── Summary Cards ───────────────────────────────────────── */
.summary-card {
    background: rgba(13,18,32,0.85);
    border: 1px solid rgba(0,240,255,0.15);
    border-radius: 12px;
    padding: 16px 20px;
    text-align: center;
    backdrop-filter: blur(10px);
    -webkit-backdrop-filter: blur(10px);
    box-shadow: 0 0 18px rgba(0,240,255,0.07), 0 4px 16px rgba(0,0,0,0.4);
    transition: border-color 0.2s ease, box-shadow 0.2s ease, transform 0.2s ease;
}
.summary-card:hover {
    border-color: rgba(0,240,255,0.35);
    box-shadow: 0 0 28px rgba(0,240,255,0.18), 0 6px 24px rgba(0,0,0,0.5);
    transform: translateY(-3px);
}
.summary-value {
    font-size: 2rem;
    font-weight: 800;
    color: rgba(255,255,255,0.95);
    line-height: 1.1;
    font-family: 'JetBrains Mono', 'Courier New', monospace;
}
.summary-label {
    font-size: 0.75rem;
    color: #b0bec5;
    text-transform: uppercase;
    letter-spacing: 1.2px;
    margin-top: 5px;
}

/* ─── Best Bets Card ──────────────────────────────────────── */
.best-bet-card {
    background: rgba(13,18,32,0.85);
    border: 1px solid rgba(0,240,255,0.18);
    border-radius: 14px;
    padding: 16px 20px;
    margin-bottom: 10px;
    position: relative;
    backdrop-filter: blur(10px);
    -webkit-backdrop-filter: blur(10px);
    box-shadow: 0 0 16px rgba(0,240,255,0.06), 0 4px 16px rgba(0,0,0,0.4);
    transition: border-color 0.2s ease, transform 0.2s ease, box-shadow 0.2s ease;
}
.best-bet-card:hover {
    border-color: rgba(0,240,255,0.40);
    transform: translateX(3px);
    box-shadow: 0 0 24px rgba(0,240,255,0.15), 0 6px 20px rgba(0,0,0,0.5);
}
.best-bet-rank {
    position: absolute;
    top: -10px; left: 16px;
    background: linear-gradient(135deg, #ff5e00, #00f0ff);
    color: #ffffff;
    font-weight: 900;
    font-size: 0.75rem;
    padding: 2px 10px;
    border-radius: 10px;
    font-family: 'JetBrains Mono', 'Courier New', monospace;
    box-shadow: 0 0 10px rgba(255,94,0,0.4);
}

/* ─── Roster Health ───────────────────────────────────────── */
.health-matched {
    display: inline-block;
    background: rgba(0,255,157,0.08);
    border: 1px solid rgba(0,255,157,0.35);
    color: #00ff9d;
    padding: 2px 9px;
    border-radius: 6px;
    font-size: 0.78rem;
    font-weight: 700;
    margin: 2px;
}
.health-fuzzy {
    display: inline-block;
    background: rgba(255,94,0,0.08);
    border: 1px solid rgba(255,94,0,0.40);
    color: #ff9d4d;
    padding: 2px 9px;
    border-radius: 6px;
    font-size: 0.78rem;
    font-weight: 700;
    margin: 2px;
    cursor: help;
}
.health-unmatched {
    display: inline-block;
    background: rgba(220,38,38,0.08);
    border: 1px solid rgba(220,38,38,0.35);
    color: #ff6b6b;
    padding: 2px 9px;
    border-radius: 6px;
    font-size: 0.78rem;
    font-weight: 700;
    margin: 2px;
}

/* ─── Live / Sample Badge ─────────────────────────────────── */
.live-badge {
    display: inline-block;
    background: rgba(0,255,157,0.10);
    color: #00ff9d;
    padding: 3px 10px;
    border-radius: 12px;
    font-size: 0.8rem;
    font-weight: 700;
    border: 1px solid rgba(0,255,157,0.35);
    text-shadow: 0 0 6px rgba(0,255,157,0.4);
}
.live-badge::before {
    content: '';
    display: inline-block;
    width: 7px; height: 7px;
    border-radius: 50%;
    background: #00ff9d;
    margin-right: 6px;
    vertical-align: middle;
    animation: live-dot-pulse 1.5s ease-in-out infinite;
    box-shadow: 0 0 6px rgba(0,255,157,0.7);
}
.sample-badge {
    display: inline-block;
    background: rgba(255,94,0,0.10);
    color: #ff9d4d;
    padding: 3px 10px;
    border-radius: 12px;
    font-size: 0.8rem;
    font-weight: 700;
    border: 1px solid rgba(255,94,0,0.35);
}

/* ─── Correlation Warning ─────────────────────────────────── */
.corr-warning {
    background: rgba(245,158,11,0.10);
    border: 1px solid rgba(245,158,11,0.32);
    border-radius: 8px;
    padding: 8px 14px;
    color: #facc15;
    font-size: 0.83rem;
    margin-top: 8px;
}

/* ─── Player Analysis Card ────────────────────────────────── */
.player-analysis-card {
    background: rgba(13,18,32,0.85);
    border: 1px solid rgba(0,240,255,0.15);
    border-radius: 16px;
    padding: 20px 24px;
    margin-bottom: 18px;
    backdrop-filter: blur(12px);
    -webkit-backdrop-filter: blur(12px);
    box-shadow: 0 0 20px rgba(0,240,255,0.07), 0 4px 24px rgba(0,0,0,0.4);
    animation: borderGlow 3.5s ease-in-out infinite,
               fadeInUp 0.3s ease both;
    transition: border-color 0.2s ease, transform 0.2s ease, box-shadow 0.2s ease;
    position: relative;
    overflow: hidden;
}
.player-analysis-card::before {
    content: '';
    position: absolute;
    top: 0; left: 0; right: 0; height: 3px;
    background: linear-gradient(90deg, #00f0ff, #00ff9d, #ff5e00, #c800ff, #00f0ff);
    background-size: 200% 100%;
    animation: headerShimmer 4s ease infinite;
    opacity: 0.9;
}
.player-analysis-card:hover {
    border-color: rgba(0,240,255,0.40);
    transform: translateY(-5px);
    box-shadow: 0 0 30px rgba(0,240,255,0.18), 0 8px 32px rgba(0,0,0,0.5);
}
.add-to-slip-btn {
    background: linear-gradient(135deg, #ff5e00, #ff8c00);
    color: #ffffff;
    border: none;
    border-radius: 8px;
    padding: 6px 14px;
    font-weight: 800;
    font-size: 0.8rem;
    cursor: pointer;
    font-family: 'JetBrains Mono', 'Courier New', monospace;
    transition: opacity 0.2s ease, transform 0.2s ease;
    box-shadow: 0 0 12px rgba(255,94,0,0.40);
}
.add-to-slip-btn:hover {
    opacity: 0.88;
    transform: scale(1.03);
}

/* ═══════════════════════════════════════════════════════════
   NBA THEME PRESENCE — Authentic NBA look layered on top of
   the Quantum Edge dark theme. Adds sports-broadcast energy
   without replacing any existing QDS styling.
   ═══════════════════════════════════════════════════════════ */

/* ─── NBA Sports Fonts ─────────────────────────────────────
   Bebas Neue gives the ESPN/TNT scoreboard feel for numbers.
   Used via class .nba-stat-number or .nba-score-display.   */
@import url('https://fonts.googleapis.com/css2?family=Bebas+Neue&family=Oswald:wght@400;600;700&display=swap');

/* ─── NBA Keyframe Animations ─────────────────────────────── */
@keyframes nba-live-pulse {
    0%, 100% { box-shadow: 0 0 0 0 rgba(200,16,46,0.7); opacity: 1; }
    50%       { box-shadow: 0 0 0 8px rgba(200,16,46,0); opacity: 0.85; }
}
@keyframes nba-shimmer-platinum {
    0%   { background-position: -300% center; }
    100% { background-position: 300% center; }
}
@keyframes nba-gold-gleam {
    0%, 80%, 100% { filter: brightness(1); }
    40%            { filter: brightness(1.35) drop-shadow(0 0 6px #FFD700); }
}
@keyframes nba-silver-sheen {
    0%   { background-position: -200% 0; }
    100% { background-position: 200% 0; }
}
@keyframes nba-bronze-pulse {
    0%, 100% { box-shadow: 0 0 8px rgba(205,127,50,0.30); }
    50%       { box-shadow: 0 0 18px rgba(205,127,50,0.65); }
}
@keyframes analysis-spin {
    0%   { transform: rotate(0deg); }
    100% { transform: rotate(360deg); }
}
@keyframes data-stream {
    0%   { background-position: 0 0; }
    100% { background-position: 0 -100px; }
}
@keyframes card-flip-in {
    0%   { opacity: 0; transform: rotateY(-90deg) scale(0.95); }
    100% { opacity: 1; transform: rotateY(0deg) scale(1); }
}
@keyframes fade-in-up {
    from { opacity: 0; transform: translateY(16px); }
    to   { opacity: 1; transform: translateY(0); }
}
@keyframes slide-in-left {
    from { opacity: 0; transform: translateX(-24px); }
    to   { opacity: 1; transform: translateX(0); }
}
@keyframes slide-in-right {
    from { opacity: 0; transform: translateX(24px); }
    to   { opacity: 1; transform: translateX(0); }
}
@keyframes count-up-glow {
    0%   { text-shadow: 0 0 0 transparent; }
    50%  { text-shadow: 0 0 16px rgba(0,240,255,0.7); }
    100% { text-shadow: 0 0 6px rgba(0,240,255,0.3); }
}
@keyframes freshness-pulse-green {
    0%, 100% { box-shadow: 0 0 0 0 rgba(0,255,157,0.6); }
    50%       { box-shadow: 0 0 0 5px rgba(0,255,157,0); }
}
@keyframes freshness-pulse-yellow {
    0%, 100% { box-shadow: 0 0 0 0 rgba(255,200,0,0.6); }
    50%       { box-shadow: 0 0 0 5px rgba(255,200,0,0); }
}
@keyframes freshness-pulse-red {
    0%, 100% { box-shadow: 0 0 0 0 rgba(200,16,46,0.6); }
    50%       { box-shadow: 0 0 0 5px rgba(200,16,46,0); }
}

/* ─── NBA Game-Day Banner ─────────────────────────────────── */
/* Usage: <div class="nba-game-day-banner">GAME DAY</div>      */
.nba-game-day-banner {
    border-top: 3px solid transparent;
    border-image: linear-gradient(90deg, #C8102E 0%, #FFFFFF 33%, #1D428A 66%, #C8102E 100%) 1;
    background: rgba(7,10,19,0.92);
    border-radius: 0 0 10px 10px;
    padding: 12px 24px;
    text-align: center;
    font-family: 'Bebas Neue', 'Orbitron', sans-serif;
    font-size: 1.4rem;
    letter-spacing: 0.25em;
    color: #FFFFFF;
    text-shadow: 0 0 12px rgba(200,16,46,0.8), 0 0 24px rgba(29,66,138,0.5);
    position: relative;
    overflow: hidden;
}
.nba-game-day-banner::before {
    content: '🏀';
    margin-right: 12px;
}
.nba-game-day-banner::after {
    content: '🏀';
    margin-left: 12px;
}

/* ─── NBA Stat Highlight Card ─────────────────────────────── */
/* Usage: <div class="nba-stat-highlight"><span class="nba-stat-number">24.5</span><span class="nba-stat-label">PPG</span></div> */
.nba-stat-highlight {
    display: inline-flex;
    flex-direction: column;
    align-items: flex-start;
    border-left: 4px solid #C8102E;
    padding: 8px 16px 8px 14px;
    background: rgba(200,16,46,0.06);
    border-radius: 0 10px 10px 0;
    margin: 4px 8px;
    transition: border-color 0.2s ease, background 0.2s ease;
}
.nba-stat-highlight:hover {
    border-color: #00f0ff;
    background: rgba(0,240,255,0.06);
}
.nba-stat-number {
    font-family: 'Bebas Neue', 'Oswald', sans-serif;
    font-size: 2.2rem;
    font-weight: 700;
    color: #FFFFFF;
    line-height: 1;
    letter-spacing: 0.03em;
}
.nba-stat-label {
    font-family: 'Oswald', 'Montserrat', sans-serif;
    font-size: 0.72rem;
    font-weight: 600;
    color: rgba(192,208,232,0.75);
    letter-spacing: 0.12em;
    text-transform: uppercase;
    margin-top: 2px;
}

/* ─── LIVE Game Badge ─────────────────────────────────────── */
/* Usage: <span class="game-live-badge">LIVE</span>            */
.game-live-badge {
    display: inline-flex;
    align-items: center;
    gap: 6px;
    background: #C8102E;
    color: #FFFFFF;
    font-family: 'Bebas Neue', 'Orbitron', sans-serif;
    font-size: 0.82rem;
    font-weight: 700;
    letter-spacing: 0.12em;
    padding: 3px 10px;
    border-radius: 4px;
    animation: nba-live-pulse 1.5s ease-in-out infinite;
    vertical-align: middle;
}
.game-live-badge::before {
    content: '';
    display: inline-block;
    width: 7px;
    height: 7px;
    border-radius: 50%;
    background: #FFFFFF;
    animation: live-dot-pulse 1.2s ease-in-out infinite;
}

/* ─── Sidebar NBA Branding ────────────────────────────────── */
/* Adds "🏀 Smart Pick Pro — NBA Edition" below the   */
/* existing "⚡ Powered by Quantum Matrix Engine 5.6" text.   */
[data-testid="stSidebar"]::before {
    content: "🏀 Smart Pick Pro · NBA Edition";
    display: block;
    position: fixed;
    bottom: 44px;
    left: 0;
    width: 100%;
    padding: 0 20px;
    box-sizing: border-box;
    text-align: center;
    font-size: 0.62rem;
    font-family: 'Bebas Neue', 'Oswald', 'Courier New', monospace;
    font-weight: 700;
    letter-spacing: 0.14em;
    color: rgba(200,16,46,0.80) !important;
    pointer-events: none;
    text-shadow: 0 0 8px rgba(200,16,46,0.35);
}

/* ─── Half-Court Arc Watermark ────────────────────────────── */
/* Subtle basketball court arc on main content background     */
.stApp::after {
    content: '';
    display: block;
    position: fixed;
    bottom: -120px;
    right: -120px;
    width: 360px;
    height: 360px;
    border-radius: 50%;
    border: 40px solid rgba(200,16,46,0.04);
    pointer-events: none;
    z-index: 0;
}

/* ═══════════════════════════════════════════════════════════
   PREMIUM ENHANCEMENTS — State-of-the-art UI polish
   ═══════════════════════════════════════════════════════════ */

/* ─── Enhanced Tier Badges ────────────────────────────────── */
/* Platinum — metallic white-silver with outer glow */
.tier-platinum {
    background: linear-gradient(
        135deg,
        #d4d8e0 0%, #ffffff 30%, #b0b8c8 50%, #ffffff 70%, #d4d8e0 100%
    ) !important;
    background-size: 300% 100% !important;
    color: #1a2035 !important;
    -webkit-background-clip: unset !important;
    -webkit-text-fill-color: #1a2035 !important;
    animation: pulse-platinum 2.5s infinite, nba-shimmer-platinum 3s linear infinite !important;
}
/* Gold — polished metal gleam */
.tier-gold {
    background: linear-gradient(
        135deg,
        #a67c00 0%, #ffd700 30%, #c9a800 50%, #ffd700 70%, #a67c00 100%
    ) !important;
    background-size: 300% 100% !important;
    color: #2a1800 !important;
    animation: pulse-gold 2.8s infinite, nba-gold-gleam 4s ease-in-out infinite !important;
}
/* Silver — metallic sheen */
.tier-silver {
    background: linear-gradient(
        105deg,
        #8a8e96 0%, #c0c0c0 40%, #d0d4dc 50%, #c0c0c0 60%, #8a8e96 100%
    ) !important;
    background-size: 300% 100% !important;
    color: #1a1f30 !important;
    animation: nba-silver-sheen 3s linear infinite !important;
}
/* Bronze — warm metallic pulse */
.tier-bronze {
    animation: nba-bronze-pulse 2.5s ease-in-out infinite !important;
}

/* ─── Analysis Loading Animation ─────────────────────────── */
/* Usage: <div class="analysis-loading"></div>                */
.analysis-loading {
    display: inline-block;
    width: 40px;
    height: 40px;
    border: 4px solid rgba(0,240,255,0.15);
    border-top-color: #00f0ff;
    border-radius: 50%;
    animation: analysis-spin 0.9s linear infinite;
    vertical-align: middle;
    margin: 0 10px;
}

/* ─── Data Stream Effect ──────────────────────────────────── */
.data-stream {
    background-image: repeating-linear-gradient(
        0deg,
        transparent,
        transparent 3px,
        rgba(0,240,255,0.03) 3px,
        rgba(0,240,255,0.03) 4px
    );
    background-size: 100% 100px;
    animation: data-stream 2s linear infinite;
}

/* ─── Pick Reveal Animation ───────────────────────────────── */
.pick-reveal {
    animation: card-flip-in 0.45s cubic-bezier(0.25, 0.46, 0.45, 0.94) both;
    perspective: 800px;
}

/* ─── Fade / Slide Page Transitions ─────────────────────────
   Apply these classes to content sections for smooth entry.  */
.fade-in-up    { animation: fade-in-up    0.4s ease both; }
.slide-in-left  { animation: slide-in-left  0.35s ease both; }
.slide-in-right { animation: slide-in-right 0.35s ease both; }

/* ─── Premium Metric Card ────────────────────────────────── */
/* Usage: <div class="premium-metric-card">...</div>          */
.premium-metric-card {
    background: rgba(13,18,32,0.90);
    border: 1px solid rgba(0,240,255,0.18);
    border-radius: 16px;
    padding: 22px 26px;
    backdrop-filter: blur(16px);
    -webkit-backdrop-filter: blur(16px);
    box-shadow:
        0 0 0 1px rgba(0,240,255,0.06) inset,
        0 0 24px rgba(0,240,255,0.10),
        0 8px 32px rgba(0,0,0,0.45);
    transition: transform 0.25s ease, box-shadow 0.25s ease, border-color 0.25s ease;
    animation: count-up-glow 1.5s ease 0.2s both;
    position: relative;
    overflow: hidden;
}
.premium-metric-card::before {
    content: '';
    position: absolute;
    top: 0; left: 0; right: 0; height: 2px;
    background: linear-gradient(90deg, #C8102E, #00f0ff, #1D428A, #00ff9d, #C8102E);
    background-size: 200% 100%;
    animation: headerShimmer 3s ease infinite;
}
.premium-metric-card:hover {
    transform: translateY(-6px) scale(1.012);
    border-color: rgba(0,240,255,0.38);
    box-shadow:
        0 0 0 1px rgba(0,240,255,0.10) inset,
        0 0 36px rgba(0,240,255,0.20),
        0 12px 40px rgba(0,0,0,0.55);
}

/* ─── Smart Tooltip ───────────────────────────────────────── */
/* Usage: <span class="smart-tooltip-wrap">hover<span class="smart-tooltip">tip text</span></span> */
.smart-tooltip-wrap {
    position: relative;
    cursor: help;
}
.smart-tooltip {
    visibility: hidden;
    opacity: 0;
    max-width: 300px;
    min-width: 140px;
    background: rgba(7,10,19,0.97);
    border: 1px solid rgba(0,240,255,0.35);
    border-radius: 10px;
    padding: 10px 14px;
    font-size: 0.82rem;
    color: #c8d8f0;
    line-height: 1.5;
    position: absolute;
    z-index: 9999;
    bottom: calc(100% + 8px);
    left: 50%;
    transform: translateX(-50%) translateY(4px);
    box-shadow: 0 0 16px rgba(0,240,255,0.18), 0 4px 20px rgba(0,0,0,0.6);
    backdrop-filter: blur(12px);
    -webkit-backdrop-filter: blur(12px);
    transition: opacity 0.18s ease, visibility 0.18s ease, transform 0.18s ease;
    pointer-events: none;
}
.smart-tooltip::after {
    content: '';
    position: absolute;
    top: 100%;
    left: 50%;
    transform: translateX(-50%);
    border: 6px solid transparent;
    border-top-color: rgba(0,240,255,0.35);
}
.smart-tooltip-wrap:hover .smart-tooltip {
    visibility: visible;
    opacity: 1;
    transform: translateX(-50%) translateY(0);
}

/* ─── Data Freshness Badge ────────────────────────────────── */
/* Usage (HTML): <span class="data-freshness-badge fresh">● FRESH</span>
   Usage (HTML): <span class="data-freshness-badge stale">● STALE</span>
   Usage (HTML): <span class="data-freshness-badge outdated">● OUTDATED</span>
   In page files: inject via st.markdown(f'<span class="data-freshness-badge fresh">● FRESH</span>', unsafe_allow_html=True) */
.data-freshness-badge {
    display: inline-flex;
    align-items: center;
    gap: 5px;
    font-size: 0.72rem;
    font-weight: 700;
    font-family: 'Oswald', 'Courier New', monospace;
    letter-spacing: 0.10em;
    padding: 3px 10px;
    border-radius: 20px;
    vertical-align: middle;
}
.data-freshness-badge.fresh {
    background: rgba(0,255,157,0.10);
    color: #00ff9d;
    border: 1px solid rgba(0,255,157,0.35);
    animation: freshness-pulse-green 2s ease-in-out infinite;
}
.data-freshness-badge.stale {
    background: rgba(255,200,0,0.10);
    color: #ffc800;
    border: 1px solid rgba(255,200,0,0.35);
    animation: freshness-pulse-yellow 2.5s ease-in-out infinite;
}
.data-freshness-badge.outdated {
    background: rgba(200,16,46,0.10);
    color: #ff4d6a;
    border: 1px solid rgba(200,16,46,0.35);
    animation: freshness-pulse-red 1.8s ease-in-out infinite;
}

/* ─── Enhanced Scrollbar ──────────────────────────────────── */
::-webkit-scrollbar { width: 4px; height: 4px; }
::-webkit-scrollbar-track {
    background: rgba(7,10,19,0.95);
    border-radius: 2px;
}
::-webkit-scrollbar-thumb {
    background: linear-gradient(180deg, rgba(0,240,255,0.40), rgba(0,240,255,0.20));
    border-radius: 2px;
}
::-webkit-scrollbar-thumb:hover {
    background: linear-gradient(180deg, rgba(0,240,255,0.70), rgba(0,240,255,0.40));
}
/* Custom text selection colors */
::selection {
    background: rgba(0,240,255,0.25);
    color: #ffffff;
}
::-moz-selection {
    background: rgba(0,240,255,0.25);
    color: #ffffff;
}
/* Focus styles for inputs */
input:focus, textarea:focus, select:focus,
[data-testid="stTextInput"] input:focus,
[data-testid="stTextArea"] textarea:focus {
    outline: none !important;
    box-shadow: 0 0 0 2px rgba(0,240,255,0.40) !important;
    border-color: rgba(0,240,255,0.55) !important;
}

/* ─── Print-Ready Styles ──────────────────────────────────── */
@media print {
    /* Hide sidebar, navigation, and interactive controls */
    [data-testid="stSidebar"],
    [data-testid="stHeader"],
    [data-testid="stToolbar"],
    .stButton, .stDownloadButton,
    [data-testid="stSidebarNav"] { display: none !important; }
    /* Light background + dark text for paper readability */
    html, body, .stApp, [class*="css"] {
        background: #ffffff !important;
        color: #111111 !important;
    }
    .smartai-card, .premium-metric-card {
        background: #f5f5f5 !important;
        border: 1px solid #cccccc !important;
        box-shadow: none !important;
    }
    /* Expand all content area */
    section[data-testid="stMain"], .main .block-container {
        max-width: 100% !important;
        padding: 0 !important;
    }
}

/* ─── Responsive — Mobile Touch-Ups ──────────────────────── */
@media (max-width: 768px) {
    html, body, [class*="css"] { font-size: 14px !important; }
    .neural-header-title { font-size: 1.4rem !important; }
    .smartai-card, .premium-metric-card { padding: 14px 16px !important; }
    .nba-stat-number { font-size: 1.7rem !important; }
    /* Larger touch targets for buttons */
    button, .stButton > button {
        min-height: 44px !important;
        padding: 10px 16px !important;
    }
    /* Stack metrics in fewer columns on small screens */
    [data-testid="stMetricValue"] { font-size: 1.2rem !important; }
}

/* ─── Premium animated gradient border — Neural Header ─── */
.neural-header {
    position: relative;
}
.neural-header::before {
    content: '';
    position: absolute;
    top: 0; left: 0; right: 0; height: 3px;
    background: linear-gradient(90deg, #00f0ff, #00ff9d, #ff5e00, #c800ff, #00f0ff);
    background-size: 200% 100%;
    animation: headerShimmer 4s ease infinite;
}

/* ─── Enhanced glassmorphism card ─────────────────────── */
.glass-card {
    background: rgba(13, 18, 40, 0.75);
    border: 1px solid rgba(0,240,255,0.20);
    border-radius: 16px;
    padding: 20px 24px;
    margin-bottom: 18px;
    backdrop-filter: blur(20px);
    -webkit-backdrop-filter: blur(20px);
    box-shadow: 0 8px 32px rgba(0,0,0,0.4), 0 0 24px rgba(0,240,255,0.06);
    transition: border-color 0.25s ease, box-shadow 0.25s ease, transform 0.25s ease;
}
.glass-card:hover {
    border-color: rgba(0,240,255,0.40);
    box-shadow: 0 12px 40px rgba(0,0,0,0.5), 0 0 36px rgba(0,240,255,0.14);
    transform: translateY(-4px);
}

/* ─── AI Processing Spinner ───────────────────────────── */
@keyframes aiSpin {
    0% { transform: rotate(0deg); filter: hue-rotate(0deg); }
    100% { transform: rotate(360deg); filter: hue-rotate(360deg); }
}
.ai-spinner {
    width: 40px; height: 40px;
    border: 3px solid rgba(0,240,255,0.1);
    border-top: 3px solid #00f0ff;
    border-radius: 50%;
    animation: aiSpin 1s linear infinite;
    margin: 0 auto;
}

/* ─── Platinum Tier Shimmer ───────────────────────────── */
@keyframes platinumShimmer {
    0% { background-position: -200% center; }
    100% { background-position: 200% center; }
}
/* NOTE: .tier-platinum is now a metallic badge — no text-clip needed */
</style>
"""


# ============================================================
# END SECTION: Global CSS Theme
# ============================================================


# ============================================================
# SECTION: Premium Footer HTML
# ============================================================

def get_premium_footer_html() -> str:
    """Return a premium footer HTML block with responsible gambling info."""
    return '''<div style="text-align:center;padding:16px 0 8px 0;margin-top:24px;border-top:1px solid rgba(0,240,255,0.1);">
    <p style="color:#8a9bb8;font-size:0.75rem;margin:0;">
        Powered by <strong style="color:#00f0ff;">SmartBetPro AI™</strong> &nbsp;|&nbsp;
        For entertainment &amp; educational purposes only &nbsp;|&nbsp; 
        <span style="color:#ff6b6b;">Not financial advice. Bet responsibly. 21+</span>
    </p>
    <p style="color:#5a6b8a;font-size:0.7rem;margin:4px 0 0 0;">
        Problem gambling help: <strong>1-800-GAMBLER (1-800-426-2537)</strong> |
        <a href="https://www.ncpgambling.org" target="_blank" rel="noopener noreferrer"
           aria-label="National Council on Problem Gambling" style="color:#8a9bb8;">National Council on Problem Gambling</a> |
        <a href="https://www.begambleaware.org" target="_blank" rel="noopener noreferrer"
           aria-label="BeGambleAware organisation" style="color:#8a9bb8;">BeGambleAware</a>
    </p>
</div>'''

# ============================================================
# END SECTION: Premium Footer HTML
# ============================================================


# ============================================================
# SECTION: HTML Component Generators
# Each function returns a self-contained HTML snippet.
# ============================================================

def get_tier_badge_html(tier, tier_emoji=None):
    """
    Return styled HTML for a tier badge.

    Args:
        tier (str): 'Platinum', 'Gold', 'Silver', or 'Bronze'
        tier_emoji (str, optional): Override emoji (default per tier)

    Returns:
        str: HTML span with appropriate CSS class
    """
    tier_emojis = {
        "Platinum": "💎",
        "Gold": "🥇",
        "Silver": "🥈",
        "Bronze": "🥉",
    }
    emoji = tier_emoji or tier_emojis.get(tier, "🏅")
    css_class = f"tier-badge tier-{tier.lower()}"
    return f'<span class="{css_class}">{emoji} {tier}</span>'


def get_probability_gauge_html(probability, direction):
    """
    Return a styled probability progress bar.

    Args:
        probability (float): Raw P(over), 0.0–1.0
        direction (str): 'OVER' or 'UNDER'

    Returns:
        str: HTML div containing the gauge bar
    """
    if direction == "OVER":
        display_pct = probability * 100
        fill_class = "prob-gauge-fill-over"
    else:
        display_pct = (1.0 - probability) * 100
        fill_class = "prob-gauge-fill-under"
    bar_width = int(min(100, max(0, display_pct)))
    return f"""<div class="prob-gauge-wrap">
  <div class="{fill_class}" style="width:{bar_width}%;"></div>
</div>"""


def get_stat_pill_html(label, value, emoji=""):
    """
    Return a styled stat pill chip.

    Args:
        label (str): Stat label (e.g., 'PPG')
        value: Stat value (e.g., 24.8)
        emoji (str): Optional emoji prefix

    Returns:
        str: HTML span
    """
    prefix = f"{emoji} " if emoji else ""
    return f'<span class="stat-chip">{prefix}<strong>{value}</strong> <span class="stat-label">{label}</span></span>'


def get_force_bar_html(over_strength, under_strength, over_count, under_count):
    """
    Return a proportional green/red force bar showing OVER vs UNDER pressure.

    Args:
        over_strength (float): Total strength of OVER forces
        under_strength (float): Total strength of UNDER forces
        over_count (int): Number of OVER forces
        under_count (int): Number of UNDER forces

    Returns:
        str: HTML div with the force bar
    """
    total = (over_strength + under_strength) or 1.0
    over_pct = int(over_strength / total * 100)
    under_pct = 100 - over_pct
    return f"""<div class="force-bar-wrap">
  <div class="force-bar-over" style="width:{over_pct}%;"></div>
  <div class="force-bar-under" style="width:{under_pct}%;"></div>
</div>
<div style="display:flex;justify-content:space-between;font-size:0.72rem;color:#b0bec5;margin-top:3px;">
  <span>⬆️ OVER ({over_count})</span>
  <span>UNDER ({under_count}) ⬇️</span>
</div>"""


def get_distribution_range_html(p10, p50, p90):
    """
    Return a styled distribution range display (10th / 50th / 90th pct).

    Args:
        p10 (float): 10th percentile value
        p50 (float): 50th percentile (median)
        p90 (float): 90th percentile value

    Returns:
        str: HTML span block
    """
    return f"""<div class="dist-range-wrap">
  <span class="dist-p10">{p10:.1f}</span>
  <span class="dist-sep">—</span>
  <span class="dist-p50">{p50:.1f}</span>
  <span class="dist-sep">—</span>
  <span class="dist-p90">{p90:.1f}</span>
  <div class="dist-label">10th / 50th / 90th pct</div>
</div>"""


def get_player_card_html(result):
    """
    Build the complete styled analysis card HTML for one prop result.

    The card header now includes a player headshot loaded from the NBA CDN
    (``https://cdn.nba.com/headshots/nba/latest/1040x760/{player_id}.png``).
    An ``onerror`` handler falls back to a generic silhouette when the image
    is unavailable (e.g. sample / unknown player IDs).

    Args:
        result (dict): Full analysis result dict from the simulation loop.
            Expected keys: player_name, stat_type, line, direction, tier,
            tier_emoji, probability_over, edge_percentage, confidence_score,
            platform, player_team, player_position, season_pts_avg,
            player_id (optional NBA player ID for headshot), etc.

    Returns:
        str: Complete HTML string for the card
    """
    player = result.get("player_name", "Unknown")
    stat = result.get("stat_type", "").capitalize()
    line = result.get("line", 0)
    direction = result.get("direction", "OVER")
    tier = result.get("tier", "Bronze")
    tier_emoji = result.get("tier_emoji", "🥉")
    prob_over = result.get("probability_over", 0.5)
    edge = result.get("edge_percentage", 0)
    confidence = result.get("confidence_score", 50)
    platform = result.get("platform", "")
    team = result.get("player_team", result.get("team", ""))
    position = result.get("player_position", result.get("position", ""))
    proj = result.get("adjusted_projection", 0)
    player_id = result.get("player_id", "")

    pts_avg = result.get("season_pts_avg", result.get("points_avg", 0))
    reb_avg = result.get("season_reb_avg", result.get("rebounds_avg", 0))
    ast_avg = result.get("season_ast_avg", result.get("assists_avg", 0))

    prob_pct = prob_over * 100 if direction == "OVER" else (1 - prob_over) * 100
    direction_arrow = "⬆️" if direction == "OVER" else "⬇️"

    tier_class = f"tier-badge tier-{tier.lower()}"
    dir_class = "dir-over" if direction == "OVER" else "dir-under"

    platform_colors = {
        "PrizePicks": "rgba(39,103,73,0.9)",
        "Underdog Fantasy": "rgba(85,60,154,0.9)",
        "DraftKings Pick6": "rgba(43,108,176,0.9)",
        # Backward-compat aliases
        "Underdog": "rgba(85,60,154,0.9)",
        "DraftKings": "rgba(43,108,176,0.9)",
    }
    plat_color = platform_colors.get(platform, "rgba(45,55,72,0.9)")

    fill_class = "prob-gauge-fill-over" if direction == "OVER" else "prob-gauge-fill-under"
    bar_width = int(min(100, max(0, prob_pct)))

    primary_color, secondary_color = get_team_colors(team)

    # Team badge
    team_badge = f'<span class="team-pill" style="background:{secondary_color};">{team}</span>' if team else ""
    position_tag = f'<span class="position-tag">{position}</span>' if position else ""

    # Player headshot from NBA CDN with fallback silhouette
    NBA_CDN_BASE = "https://cdn.nba.com/headshots/nba/latest/1040x760"
    FALLBACK_HEADSHOT = f"{NBA_CDN_BASE}/fallback.png"
    if player_id:
        headshot_url = f"{NBA_CDN_BASE}/{player_id}.png"
        headshot_html = (
            f'<img src="{headshot_url}" '
            f'onerror="this.onerror=null;this.src=\'{FALLBACK_HEADSHOT}\';" '
            f'style="width:60px;height:44px;border-radius:8px;object-fit:cover;'
            f'margin-right:12px;flex-shrink:0;background:#1a2035;" '
            f'alt="{player}">'
        )
    else:
        headshot_html = (
            f'<div style="width:60px;height:44px;border-radius:8px;'
            f'background:#1a2035;margin-right:12px;flex-shrink:0;'
            f'display:flex;align-items:center;justify-content:center;'
            f'font-size:1.6rem;">🏀</div>'
        )

    # Stat pills
    stat_pills = ""
    if pts_avg:
        stat_pills += get_stat_pill_html("PPG", f"{pts_avg:.1f}", "🏀")
    if reb_avg:
        stat_pills += get_stat_pill_html("RPG", f"{reb_avg:.1f}", "📊")
    if ast_avg:
        stat_pills += get_stat_pill_html("APG", f"{ast_avg:.1f}", "🎯")
    if proj:
        stat_pills += get_stat_pill_html("Proj", f"{proj:.1f}", "📐")

    # Edge badge
    edge_class = "edge-positive" if edge >= 0 else "edge-negative"
    edge_sign = "+" if edge >= 0 else ""
    edge_html = f'<span class="edge-badge {edge_class}">{edge_sign}{edge:.1f}% edge</span>'

    # Confidence color
    conf_color = "#00ff9d" if confidence >= 70 else "#ff9d4d" if confidence >= 50 else "#ff6b6b"

    # Force bar
    over_forces = result.get("forces", {}).get("over_forces", [])
    under_forces = result.get("forces", {}).get("under_forces", [])
    total_over_strength = sum(f.get("strength", 1) for f in over_forces)
    total_under_strength = sum(f.get("strength", 1) for f in under_forces)
    force_bar = get_force_bar_html(
        total_over_strength, total_under_strength,
        len(over_forces), len(under_forces)
    )

    # Distribution range
    p10 = result.get("percentile_10", 0)
    p50 = result.get("percentile_50", 0)
    p90 = result.get("percentile_90", 0)
    dist_range = get_distribution_range_html(p10, p50, p90)

    # Opponent
    opponent = result.get("opponent", "")
    matchup_html = f'<span style="color:#b0bec5;font-size:0.82rem;">vs {opponent}</span>' if opponent else ""

    # Line context
    line_vs_avg = result.get("line_vs_avg_pct", 0)
    if line_vs_avg != 0:
        line_ctx = f"Line is {abs(line_vs_avg):.0f}% {'above' if line_vs_avg > 0 else 'below'} season avg"
        line_ctx_html = f'<span style="color:#b0bec5;font-size:0.78rem;font-style:italic;">{line_ctx}</span>'
    else:
        line_ctx_html = ""

    # Recent form dots
    recent_results = result.get("recent_form_results", [])
    form_html = ""
    if recent_results:
        stat_map = {"points": "pts", "rebounds": "reb", "assists": "ast",
                    "threes": "fg3m", "steals": "stl", "blocks": "blk", "turnovers": "tov"}
        mapped_key = stat_map.get(stat.lower(), "pts")
        dots = ""
        for g in recent_results[:5]:
            val = g.get(mapped_key, g.get("pts", 0))
            dot_cls = "form-dot-over" if val >= line else "form-dot-under"
            dots += f'<span class="{dot_cls}"></span>'
        form_html = f"""<div style="margin-right:16px;">
      <div style="color:#b0bec5;font-size:0.72rem;text-transform:uppercase;letter-spacing:1px;margin-bottom:3px;">Last 5</div>
      {dots}
    </div>"""

    # Correlation warning (passed in from caller if applicable)
    corr_warning = result.get("_correlation_warning", "")
    corr_html = f'<div class="corr-warning">⚠️ {corr_warning}</div>' if corr_warning else ""

    return f"""
<div class="smartai-card" style="border-top-color:{primary_color};">
  <!-- Header: Headshot + Player name + team + tier -->
  <div style="display:flex;justify-content:space-between;align-items:flex-start;flex-wrap:wrap;gap:8px;">
    <div style="display:flex;align-items:center;">
      {headshot_html}
      <div>
        <span class="player-name">{player}</span>
        <div style="margin-top:2px;">{team_badge} {position_tag}</div>
      </div>
    </div>
    <span class="{tier_class}">{tier_emoji} {tier}</span>
  </div>

  <!-- Subheader: Platform + stat + line + matchup -->
  <div style="margin-top:9px;display:flex;gap:8px;flex-wrap:wrap;align-items:center;">
    <span class="platform-badge" style="background:{plat_color};color:#fff;">{platform}</span>
    <span style="color:#b0bec5;font-size:0.9rem;">{stat} &nbsp;·&nbsp; Line: <strong style="color:rgba(255,255,255,0.95);">{line}</strong></span>
    {matchup_html}
    {line_ctx_html}
  </div>

  <!-- Stat pills -->
  {f'<div style="margin-top:10px;">{stat_pills}</div>' if stat_pills else ""}

  <!-- Probability gauge -->
  <div style="margin-top:14px;">
    <div style="display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:6px;margin-bottom:4px;">
      <span class="{dir_class}">{direction_arrow} {direction}</span>
      <span class="prob-value">{prob_pct:.1f}%</span>
      {edge_html}
      <span style="color:#b0bec5;font-size:0.82rem;">Confidence: <strong style="color:{conf_color};">{confidence:.0f}/100</strong></span>
    </div>
    <div class="prob-gauge-wrap">
      <div class="{fill_class}" style="width:{bar_width}%;"></div>
    </div>
  </div>

  <!-- Form + Force bar + Distribution -->
  <div style="margin-top:12px;display:flex;gap:16px;align-items:flex-start;flex-wrap:wrap;">
    {form_html}
    <div style="flex:1;min-width:160px;">
      <div style="color:#b0bec5;font-size:0.72rem;text-transform:uppercase;letter-spacing:1px;margin-bottom:3px;">Over/Under Forces</div>
      {force_bar}
    </div>
    <div style="min-width:110px;">
      <div style="color:#b0bec5;font-size:0.72rem;text-transform:uppercase;letter-spacing:1px;margin-bottom:3px;">Range</div>
      {dist_range}
    </div>
  </div>
  {corr_html}
</div>
"""


def get_best_bets_section_html(best_bets):
    """
    Return HTML for the "Best Bets" ranked summary section.

    Args:
        best_bets (list of dict): Top analysis results (ranked)

    Returns:
        str: HTML string for the best-bets section
    """
    if not best_bets:
        return ""

    rank_emojis = [get_logo_img_tag("assets/NewGold_Logo.png", width=16, alt="#1"), "🥈", "🥉", "4️⃣", "5️⃣"]
    rows = []
    for i, bet in enumerate(best_bets[:5]):
        emoji = rank_emojis[i] if i < len(rank_emojis) else f"{i+1}."
        player = bet.get("player_name", "")
        stat = bet.get("stat_type", "").capitalize()
        line = bet.get("line", 0)
        direction = bet.get("direction", "OVER")
        prob_over = bet.get("probability_over", 0.5)
        prob_pct = prob_over * 100 if direction == "OVER" else (1 - prob_over) * 100
        edge = bet.get("edge_percentage", 0)
        tier = bet.get("tier", "")
        tier_class = f"tier-badge tier-{tier.lower()}"
        tier_emoji = bet.get("tier_emoji", "")
        platform = bet.get("platform", "")
        rec = bet.get("recommendation", "")
        dir_class = "dir-over" if direction == "OVER" else "dir-under"
        arrow = "⬆️" if direction == "OVER" else "⬇️"
        edge_sign = "+" if edge >= 0 else ""

        rows.append(f"""
<div class="best-bet-card">
  <div class="best-bet-rank">{emoji} #{i+1}</div>
  <div style="display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:8px;margin-top:6px;">
    <div>
      <strong style="color:rgba(255,255,255,0.95);font-size:1.05rem;">{player}</strong>
      <span style="color:#b0bec5;font-size:0.88rem;margin-left:8px;">{stat} {line}</span>
      <span style="color:#b0bec5;font-size:0.8rem;margin-left:6px;">{platform}</span>
    </div>
    <div style="display:flex;gap:8px;align-items:center;">
      <span class="{dir_class}">{arrow} {direction}</span>
      <span class="{tier_class}">{tier_emoji} {tier}</span>
    </div>
  </div>
  <div style="margin-top:6px;display:flex;gap:10px;align-items:center;flex-wrap:wrap;">
    <span style="color:rgba(255,255,255,0.95);font-weight:700;font-family:'JetBrains Mono','Courier New',monospace;">{prob_pct:.1f}%</span>
    <span style="color:#00ff9d;font-size:0.82rem;font-family:'JetBrains Mono','Courier New',monospace;">{edge_sign}{edge:.1f}% edge</span>
    <span style="color:#b0bec5;font-size:0.82rem;font-style:italic;">{rec}</span>
  </div>
</div>
""")

    cards_html = "\n".join(rows)
    return f"""
<div style="background:rgba(13,18,32,0.85);
            border:1px solid rgba(0,240,255,0.18);border-radius:16px;padding:20px 24px;margin-bottom:20px;
            backdrop-filter:blur(10px);-webkit-backdrop-filter:blur(10px);
            box-shadow:0 0 20px rgba(0,240,255,0.08),0 4px 24px rgba(0,0,0,0.4);">
  <div style="font-size:1.15rem;font-weight:800;color:rgba(255,255,255,0.95);margin-bottom:14px;font-family:'Orbitron',sans-serif;letter-spacing:0.05em;">
    🏆 Best Bets Today
    <span style="font-size:0.8rem;font-weight:400;color:#b0bec5;margin-left:10px;font-family:'Montserrat',sans-serif;">Ranked by confidence score</span>
  </div>
  {cards_html}
</div>
"""


def get_roster_health_html(matched, fuzzy_matched, unmatched):
    """
    Return HTML showing prop-to-roster matching status.

    Three categories:
    - ✅ Matched players (green) — definitive match
    - ⚠️ Fuzzy matched (yellow) — probable match with suggestion
    - ❌ Unmatched (red) — no match found, closest suggestion shown

    Args:
        matched (list of dict): Items from validate_props_against_roster()['matched']
            Each has 'prop' (dict) and 'matched_name' (str)
        fuzzy_matched (list of dict): Items from ['fuzzy_matched']
            Each has 'prop', 'matched_name', 'suggestion'
        unmatched (list of dict): Items from ['unmatched']
            Each has 'prop' (dict) and 'suggestion' (str or None)

    Returns:
        str: HTML string for the roster health section
    """
    sections = []

    if matched:
        chips = " ".join(
            f'<span class="health-matched">✅ {item["prop"].get("player_name", "")}</span>'
            for item in matched
        )
        sections.append(f"""
<div style="margin-bottom:12px;">
  <div style="color:#00ff9d;font-size:0.8rem;font-weight:700;text-transform:uppercase;
              letter-spacing:1px;margin-bottom:6px;">
    ✅ Matched ({len(matched)})
  </div>
  <div style="display:flex;flex-wrap:wrap;gap:4px;">{chips}</div>
</div>""")

    if fuzzy_matched:
        chips = " ".join(
            f'<span class="health-fuzzy" title="{item.get("suggestion","")}">'
            f'⚠️ {item["prop"].get("player_name", "")}'
            f'<span style="font-size:0.7rem;opacity:0.8;"> → {item.get("matched_name","")}</span>'
            f'</span>'
            for item in fuzzy_matched
        )
        sections.append(f"""
<div style="margin-bottom:12px;">
  <div style="color:#ff9d4d;font-size:0.8rem;font-weight:700;text-transform:uppercase;
              letter-spacing:1px;margin-bottom:6px;">
    ⚠️ Fuzzy Matched ({len(fuzzy_matched)}) — using closest match
  </div>
  <div style="display:flex;flex-wrap:wrap;gap:4px;">{chips}</div>
</div>""")

    if unmatched:
        chips = " ".join(
            f'<span class="health-unmatched" title="Closest: {item.get("suggestion") or "none"}">'
            f'❌ {item["prop"].get("player_name", "")}'
            + (f'<span style="font-size:0.7rem;opacity:0.7;"> (suggest: {item["suggestion"]})</span>'
               if item.get("suggestion") else "")
            + "</span>"
            for item in unmatched
        )
        sections.append(f"""
<div style="margin-bottom:12px;">
  <div style="color:#ff6b6b;font-size:0.8rem;font-weight:700;text-transform:uppercase;
              letter-spacing:1px;margin-bottom:6px;">
    ❌ Unmatched ({len(unmatched)}) — will use fallback data
  </div>
  <div style="display:flex;flex-wrap:wrap;gap:4px;">{chips}</div>
</div>""")

    if not sections:
        return '<div style="color:#00ff9d;">✅ All props matched to the player database.</div>'

    inner = "\n".join(sections)
    total = len(matched) + len(fuzzy_matched) + len(unmatched)
    match_pct = int((len(matched) + len(fuzzy_matched)) / max(total, 1) * 100)
    return f"""
<div style="background:rgba(13,18,32,0.85);border:1px solid rgba(0,240,255,0.15);
            border-radius:12px;padding:16px 20px;margin-bottom:16px;
            backdrop-filter:blur(10px);-webkit-backdrop-filter:blur(10px);
            box-shadow:0 0 18px rgba(0,240,255,0.07),0 4px 16px rgba(0,0,0,0.4);">
  <div style="font-size:1rem;font-weight:700;color:rgba(255,255,255,0.95);margin-bottom:12px;">
    🧬 Roster Health Check
    <span style="font-size:0.8rem;font-weight:400;color:#b0bec5;margin-left:8px;">
      {len(matched) + len(fuzzy_matched)}/{total} matched ({match_pct}%)
    </span>
  </div>
  {inner}
  <div style="font-size:0.75rem;color:#b0bec5;margin-top:4px;">
    💡 Add fuzzy-matched names to the alias map for exact matching next time.
    Unmatched props use the prop line as the baseline projection.
  </div>
</div>"""


def get_platform_badge_html(platform):
    """
    Return a styled platform badge for sportsbook platforms.

    Args:
        platform (str): Platform name

    Returns:
        str: HTML span with platform-specific gradient and styling
    """
    platform_styles = {
        "PrizePicks": (
            "background:linear-gradient(135deg,#276749,#48bb78);color:#f0fff4;"
        ),
        "Underdog Fantasy": (
            "background:linear-gradient(135deg,#44337a,#805ad5);color:#e9d8fd;"
        ),
        "DraftKings Pick6": (
            "background:linear-gradient(135deg,#1a202c,#2b6cb0);color:#bee3f8;"
        ),
        # Backward-compat aliases
        "Underdog": (
            "background:linear-gradient(135deg,#44337a,#805ad5);color:#e9d8fd;"
        ),
        "DraftKings": (
            "background:linear-gradient(135deg,#1a202c,#2b6cb0);color:#bee3f8;"
        ),
    }
    style = platform_styles.get(
        platform,
        "background:rgba(45,55,72,0.9);color:#e2e8f0;",
    )
    return (
        f'<span style="{style}padding:3px 10px;border-radius:6px;'
        f'font-size:0.8rem;font-weight:700;display:inline-block;">{platform}</span>'
    )

# ============================================================
# END SECTION: HTML Component Generators
# ============================================================


# ============================================================
# SECTION: New AI Neural Network Lab Components
# Additional HTML generators for the SmartBetPro NBA UI.
# ============================================================

def get_neural_header_html(title, subtitle):
    """
    Return a glowing circuit-decoration header for page/section headings.

    Args:
        title (str): Main heading text (rendered with gradient)
        subtitle (str): Secondary line shown in monospace below the title

    Returns:
        str: HTML string for the neural header block
    """
    return f"""
<div class="neural-header">
  <div class="neural-header-title">{title}</div>
  <div class="neural-header-subtitle">
    <span class="circuit-dot"></span>
    {subtitle}
    <span class="circuit-dot"></span>
  </div>
</div>
"""


def get_ai_verdict_card_html(verdict, confidence, explanation):
    """
    Return a styled AI verdict card showing BET / AVOID / RISKY.

    Args:
        verdict (str): 'BET', 'AVOID', or 'RISKY'
        confidence (float): Confidence score 0–100
        explanation (str): Plain-English rationale for the verdict

    Returns:
        str: HTML string for the verdict card
    """
    verdict_upper = verdict.upper()
    icons = {"BET": "✅", "AVOID": "🚫", "RISKY": "⚠️"}
    icon = icons.get(verdict_upper, "🔍")
    css_class_map = {"BET": "verdict-bet", "AVOID": "verdict-avoid", "RISKY": "verdict-risky"}
    label_class_map = {"BET": "verdict-label-bet", "AVOID": "verdict-label-avoid", "RISKY": "verdict-label-risky"}
    card_class = css_class_map.get(verdict_upper, "verdict-risky")
    label_class = label_class_map.get(verdict_upper, "verdict-label-risky")
    conf_bar_color = "#00ff9d" if confidence >= 70 else "#ff9d4d" if confidence >= 50 else "#ff6b6b"
    bar_width = int(min(100, max(0, confidence)))
    return f"""
<div class="{card_class}">
  <div style="display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:8px;">
    <span class="verdict-label {label_class}">{icon} {verdict_upper}</span>
    <div style="text-align:right;">
      <div class="verdict-confidence">CONFIDENCE</div>
      <div style="font-size:1.1rem;font-weight:800;color:{conf_bar_color};
                  font-family:'JetBrains Mono','Courier New',monospace;">{confidence:.0f}/100</div>
    </div>
  </div>
  <div style="margin-top:8px;background:rgba(13,18,32,0.80);border-radius:6px;height:6px;overflow:hidden;">
    <div style="width:{bar_width}%;height:100%;background:{conf_bar_color};
                border-radius:6px;box-shadow:0 0 8px {conf_bar_color};
                transition:width 0.5s ease;"></div>
  </div>
  <div class="verdict-explanation">{explanation}</div>
</div>
"""


def get_player_analysis_card_html(result, show_add_button=True):
    """
    Return a redesigned player analysis card with the neural network theme.

    Wraps the core stat data in the .player-analysis-card container with
    an optional '+ Add to Slip' button. Internally re-uses the proven
    layout from get_player_card_html but applies the updated CSS class.

    Args:
        result (dict): Full analysis result dict (same schema as get_player_card_html)
        show_add_button (bool): Whether to render the '+ Add to Slip' button

    Returns:
        str: HTML string for the player analysis card
    """
    player = result.get("player_name", "Unknown")
    stat = result.get("stat_type", "").capitalize()
    line = result.get("line", 0)
    direction = result.get("direction", "OVER")
    tier = result.get("tier", "Bronze")
    tier_emoji = result.get("tier_emoji", "🥉")
    prob_over = result.get("probability_over", 0.5)
    edge = result.get("edge_percentage", 0)
    confidence = result.get("confidence_score", 50)
    platform = result.get("platform", "")
    team = result.get("player_team", result.get("team", ""))
    position = result.get("player_position", result.get("position", ""))
    proj = result.get("adjusted_projection", 0)

    pts_avg = result.get("season_pts_avg", result.get("points_avg", 0))
    reb_avg = result.get("season_reb_avg", result.get("rebounds_avg", 0))
    ast_avg = result.get("season_ast_avg", result.get("assists_avg", 0))

    prob_pct = prob_over * 100 if direction == "OVER" else (1 - prob_over) * 100
    direction_arrow = "⬆️" if direction == "OVER" else "⬇️"
    tier_class = f"tier-badge tier-{tier.lower()}"
    dir_class = "dir-over" if direction == "OVER" else "dir-under"

    platform_colors = {
        "PrizePicks": "rgba(0,120,70,0.85)",
        "Underdog Fantasy": "rgba(85,60,154,0.85)",
        "DraftKings Pick6": "rgba(43,108,176,0.85)",
        # Backward-compat aliases
        "Underdog": "rgba(85,60,154,0.85)",
        "DraftKings": "rgba(43,108,176,0.85)",
    }
    plat_color = platform_colors.get(platform, "rgba(30,40,60,0.85)")
    fill_class = "prob-gauge-fill-over" if direction == "OVER" else "prob-gauge-fill-under"
    bar_width = int(min(100, max(0, prob_pct)))
    primary_color, secondary_color = get_team_colors(team)

    team_badge = (
        f'<span class="team-pill" style="background:rgba(0,212,255,0.15);">{team}</span>'
        if team else ""
    )
    position_tag = f'<span class="position-tag">{position}</span>' if position else ""

    stat_pills = ""
    if pts_avg:
        stat_pills += get_stat_pill_html("PPG", f"{pts_avg:.1f}", "🏀")
    if reb_avg:
        stat_pills += get_stat_pill_html("RPG", f"{reb_avg:.1f}", "📊")
    if ast_avg:
        stat_pills += get_stat_pill_html("APG", f"{ast_avg:.1f}", "🎯")
    if proj:
        stat_pills += get_stat_pill_html("Proj", f"{proj:.1f}", "📐")

    conf_color = "#00ff9d" if confidence >= 70 else "#ff9d4d" if confidence >= 50 else "#ff6b6b"

    edge_class = "edge-positive" if edge >= 0 else "edge-negative"
    edge_sign = "+" if edge >= 0 else ""
    edge_html = f'<span class="edge-badge {edge_class}">{edge_sign}{edge:.1f}% edge</span>'

    over_forces = result.get("forces", {}).get("over_forces", [])
    under_forces = result.get("forces", {}).get("under_forces", [])
    total_over_strength = sum(f.get("strength", 1) for f in over_forces)
    total_under_strength = sum(f.get("strength", 1) for f in under_forces)
    force_bar = get_force_bar_html(
        total_over_strength, total_under_strength,
        len(over_forces), len(under_forces)
    )

    p10 = result.get("percentile_10", 0)
    p50 = result.get("percentile_50", 0)
    p90 = result.get("percentile_90", 0)
    dist_range = get_distribution_range_html(p10, p50, p90)

    opponent = result.get("opponent", "")
    matchup_html = f'<span style="color:#b0bec5;font-size:0.82rem;">vs {opponent}</span>' if opponent else ""

    add_btn = (
        '<button class="add-to-slip-btn">＋ Add to Slip</button>'
        if show_add_button else ""
    )

    corr_warning = result.get("_correlation_warning", "")
    corr_html = f'<div class="corr-warning">⚠️ {corr_warning}</div>' if corr_warning else ""

    return f"""
<div class="player-analysis-card" style="border-top-color:{primary_color};">
  <div style="display:flex;justify-content:space-between;align-items:flex-start;flex-wrap:wrap;gap:8px;">
    <div>
      <span class="player-name">{player}</span>
      {team_badge}
      {position_tag}
    </div>
    <div style="display:flex;gap:8px;align-items:center;flex-wrap:wrap;">
      <span class="{tier_class}">{tier_emoji} {tier}</span>
      {add_btn}
    </div>
  </div>
  <div style="margin-top:9px;display:flex;gap:8px;flex-wrap:wrap;align-items:center;">
    <span class="platform-badge" style="background:{plat_color};color:#fff;">{platform}</span>
    <span style="color:#b0bec5;font-size:0.9rem;">{stat} &nbsp;·&nbsp;
      Line: <strong style="color:rgba(255,255,255,0.95);">{line}</strong></span>
    {matchup_html}
  </div>
  {f'<div style="margin-top:10px;">{stat_pills}</div>' if stat_pills else ""}
  <div style="margin-top:14px;">
    <div style="display:flex;justify-content:space-between;align-items:center;
                flex-wrap:wrap;gap:6px;margin-bottom:4px;">
      <span class="{dir_class}">{direction_arrow} {direction}</span>
      <span class="prob-value">{prob_pct:.1f}%</span>
      {edge_html}
      <span style="color:#b0bec5;font-size:0.82rem;">
        Confidence: <strong style="color:{conf_color};">{confidence:.0f}/100</strong>
      </span>
    </div>
    <div class="prob-gauge-wrap">
      <div class="{fill_class}" style="width:{bar_width}%;"></div>
    </div>
  </div>
  <div style="margin-top:12px;display:flex;gap:16px;align-items:flex-start;flex-wrap:wrap;">
    <div style="flex:1;min-width:160px;">
      <div style="color:#b0bec5;font-size:0.72rem;text-transform:uppercase;
                  letter-spacing:1px;margin-bottom:3px;">Over/Under Forces</div>
      {force_bar}
    </div>
    <div style="min-width:110px;">
      <div style="color:#b0bec5;font-size:0.72rem;text-transform:uppercase;
                  letter-spacing:1px;margin-bottom:3px;">Range</div>
      {dist_range}
    </div>
  </div>
  {corr_html}
</div>
"""


def get_stat_readout_html(label, value, context):
    """
    Return a monospace terminal-style stat readout row.

    Args:
        label (str): Stat name shown on the left (e.g., 'Season AVG')
        value: The stat value to display prominently (e.g., 24.8)
        context (str): Short contextual note shown on the right (e.g., 'last 10 games')

    Returns:
        str: HTML string for the stat readout
    """
    return f"""
<div class="stat-readout">
  <span class="stat-readout-label">{label}</span>
  <span>
    <span class="stat-readout-value">{value}</span>
    <span class="stat-readout-context">{context}</span>
  </span>
</div>
"""


def get_education_box_html(title, content):
    """
    Return a collapsible education info box.

    The box is rendered open by default using an HTML <details> element
    so it works without JavaScript in Streamlit's sandboxed environment.

    Args:
        title (str): Box heading (e.g., 'What is Edge?')
        content (str): Explanation text rendered inside the box

    Returns:
        str: HTML string for the education box
    """
    return f"""
<details class="education-box" open>
  <summary class="education-box-title">
    <span>💡</span> {title}
  </summary>
  <div class="education-box-content">{content}</div>
</details>
"""


def get_progress_ring_html(percentage, label):
    """
    Return an SVG circular confidence/progress ring indicator.

    Args:
        percentage (float): Value 0–100 to fill the ring
        label (str): Text label displayed below the ring

    Returns:
        str: HTML string containing inline SVG ring and label
    """
    pct = max(0.0, min(100.0, float(percentage)))
    radius = 28
    circumference = 2 * _math.pi * radius
    filled = circumference * pct / 100.0
    gap = circumference - filled

    if pct >= 70:
        ring_color = "#00ff9d"
    elif pct >= 50:
        ring_color = "#ff9d4d"
    else:
        ring_color = "#ff6b6b"

    return f"""
<div class="progress-ring-wrap">
  <svg width="72" height="72" viewBox="0 0 72 72">
    <circle cx="36" cy="36" r="{radius}"
            fill="none" stroke="rgba(0,240,255,0.12)" stroke-width="6"/>
    <circle cx="36" cy="36" r="{radius}"
            fill="none" stroke="{ring_color}" stroke-width="6"
            stroke-linecap="round"
            stroke-dasharray="{filled:.2f} {gap:.2f}"
            stroke-dashoffset="{circumference * 0.25:.2f}"
            style="filter:drop-shadow(0 0 4px {ring_color});transition:stroke-dasharray 0.5s ease;"/>
    <text x="36" y="40" text-anchor="middle"
          font-family="'JetBrains Mono','Courier New',monospace"
          font-size="13" font-weight="700"
          fill="{ring_color}">{pct:.0f}%</text>
  </svg>
  <span class="progress-ring-label">{label}</span>
</div>
"""


def get_signal_strength_bar_html(strength, label):
    """
    Return a WiFi-style signal-strength bar (5 segments).

    Args:
        strength (float): Signal level 0.0–1.0 (or 0–100)
        label (str): Text label displayed next to the bars

    Returns:
        str: HTML string for the signal strength indicator
    """
    # Normalise to 0–1
    if strength > 1:
        strength = strength / 100.0
    strength = max(0.0, min(1.0, strength))
    active_bars = round(strength * 5)

    heights = [8, 12, 16, 20, 24]  # px heights for each bar segment
    bars_html = ""
    for i, h in enumerate(heights):
        active_class = "active" if i < active_bars else ""
        bars_html += (
            f'<div class="signal-bar-seg {active_class}" '
            f'style="height:{h}px;"></div>'
        )

    return f"""
<span>
  <span class="signal-bar-wrap">{bars_html}</span>
  <span class="signal-strength-label">{label}</span>
</span>
"""


def get_education_tooltip_html(term, explanation):
    """
    Return an inline hover tooltip for a betting/AI term.

    The tooltip uses pure CSS (no JavaScript) and works within
    Streamlit's sandboxed HTML environment.

    Args:
        term (str): The term to underline and make hoverable
        explanation (str): Plain-English explanation shown on hover

    Returns:
        str: HTML string with the tooltip span
    """
    safe_explanation = _html.escape(str(explanation))
    return (
        f'<span class="edu-tooltip">{term}'
        f'<span class="tooltip-text">{safe_explanation}</span>'
        f'</span>'
    )


# ============================================================
# END SECTION: New AI Neural Network Lab Components
# ============================================================


# ============================================================
# SECTION: QDS Game Report Generator
# Produces a fully self-contained HTML game-betting report
# using the Quantum Design System (QDS) visual language:
#   - Dark card panels, teal neon accents, glassmorphism
#   - Collapsible sections with chevron animation
#   - Animated confidence / probability bars (fill on open)
#   - SAFE Score™ prop cards with per-metric breakdowns
#   - Entry Strategy Matrix (Pick 2/3/5)
#   - Framework Logic and Final Word sections
# Designed for st.components.v1.html() embedding.
# ============================================================

# ── Static CSS for the QDS report (no f-string — braces are literal) ────────
_QDS_REPORT_CSS = """
<style>
@import url('https://fonts.googleapis.com/css2?family=Orbitron:wght@400;500;700&family=Montserrat:wght@300;400;600;700&display=swap');

:root {
  --qds-primary: #00ffd5;
  --qds-primary-dark: #00ccaa;
  --qds-primary-light: rgba(0,255,213,0.15);
  --qds-bg: #0a101f;
  --qds-card: #141a2d;
  --qds-card-hover: #1a2238;
  --qds-accent: #00b4ff;
  --qds-accent-light: rgba(0,180,255,0.15);
  --qds-text-light: #f0f4ff;
  --qds-text-muted: #b0bec5;
  --qds-text-dark: #0a101f;
  --qds-success: #00ff88;
  --qds-warning: #ffcc00;
  --qds-info: #00a3ff;
  --qds-danger: #ff3860;
  --qds-neon-shadow: 0 0 10px rgba(0,255,213,0.5);
  --qds-neon-glow: 0 0 15px rgba(0,255,213,0.7);
}
*{box-sizing:border-box;margin:0;padding:0;}
html{scroll-behavior:smooth;}
body{
  font-family:'Montserrat',sans-serif;
  background:var(--qds-bg);
  color:var(--qds-text-light);
  line-height:1.6;
  overflow-x:hidden;
  background-image:
    radial-gradient(circle at 10% 20%,rgba(0,180,255,0.05) 0%,transparent 20%),
    radial-gradient(circle at 90% 80%,rgba(0,255,213,0.05) 0%,transparent 20%);
  background-attachment:fixed;
  padding:0 0 40px;
}
h1,h2,h3,h4{font-family:'Orbitron',sans-serif;letter-spacing:0.5px;font-weight:700;color:var(--qds-text-light);}
.qds-container{max-width:1100px;margin:0 auto;padding:0 16px;}

/* ── Header ── */
.qds-report-header{text-align:center;padding:24px 0 16px;position:relative;overflow:hidden;}
.qds-report-header::before{
  content:'';position:absolute;top:0;left:0;right:0;height:3px;
  background:linear-gradient(90deg,transparent,var(--qds-primary),var(--qds-accent),transparent);
}
.qds-report-title{display:flex;flex-direction:column;align-items:center;gap:8px;margin-bottom:18px;}
.qds-report-title-icon{color:var(--qds-primary);font-size:22px;}
.qds-report-title-text{
  font-size:clamp(1.4rem,4vw,2rem);
  background:linear-gradient(90deg,var(--qds-primary),var(--qds-accent));
  -webkit-background-clip:text;background-clip:text;color:transparent;
  text-shadow:0 0 10px rgba(0,255,213,0.3);
}
.qds-game-info-container{display:flex;flex-direction:column;gap:14px;margin-bottom:24px;align-items:center;}
.qds-game-teams{
  display:flex;align-items:center;justify-content:center;gap:14px;padding:14px 20px;
  border-radius:12px;background:rgba(10,16,31,0.7);
  border:1px solid rgba(0,255,213,0.1);box-shadow:var(--qds-neon-shadow);
  flex-wrap:wrap;width:100%;max-width:600px;
}
.qds-team-container{display:flex;flex-direction:column;align-items:center;gap:6px;flex:1;}
.qds-team-brand{display:flex;align-items:center;gap:8px;}
.qds-team-logo{width:38px;height:38px;object-fit:contain;filter:drop-shadow(0 0 5px rgba(0,180,255,0.3));}
.qds-team-name-txt{font-weight:700;font-size:0.95rem;font-family:'Orbitron',sans-serif;white-space:nowrap;}
.qds-vs-separator{
  font-size:1rem;font-weight:700;color:var(--qds-text-light);padding:4px 14px;
  border-radius:50px;background:rgba(0,180,255,0.1);font-family:'Orbitron',sans-serif;
}
.qds-team-record{font-size:0.82rem;color:var(--qds-text-muted);display:flex;align-items:center;gap:5px;}
.qds-game-meta{display:flex;flex-direction:column;gap:10px;align-items:center;width:100%;max-width:600px;}
.qds-game-date{
  font-size:0.9rem;color:var(--qds-text-light);
  background:linear-gradient(90deg,rgba(0,255,213,0.08) 0%,rgba(0,180,255,0.08) 100%);
  padding:8px 16px;border-radius:50px;display:flex;align-items:center;gap:8px;
  border:1px dashed var(--qds-primary);width:100%;justify-content:center;
}
.qds-framework{
  display:inline-flex;align-items:center;gap:10px;
  background:linear-gradient(90deg,rgba(0,255,213,0.08) 0%,rgba(0,180,255,0.08) 100%);
  padding:8px 18px;border-radius:50px;font-size:0.82rem;
  border:1px solid rgba(0,255,213,0.3);font-family:'Orbitron',sans-serif;
  letter-spacing:0.5px;width:100%;justify-content:center;
}

/* ── Collapsible ── */
.qds-collapsible{
  background:var(--qds-card);border-radius:12px;margin-bottom:18px;
  overflow:hidden;box-shadow:0 5px 15px rgba(0,0,0,0.25);
  border:1px solid rgba(0,255,213,0.1);
}
.qds-collapsible-header{
  padding:14px 18px;cursor:pointer;display:flex;justify-content:space-between;
  align-items:center;
  background:linear-gradient(90deg,rgba(0,255,213,0.04) 0%,rgba(0,180,255,0.04) 100%);
  position:relative;
}
.qds-collapsible-header::after{
  content:'';position:absolute;bottom:0;left:0;right:0;height:1px;
  background:linear-gradient(90deg,transparent,rgba(0,255,213,0.3),transparent);
}
.qds-collapsible-title{
  display:flex;align-items:center;gap:10px;font-size:1rem;font-weight:600;
  color:var(--qds-primary);margin:0;font-family:'Orbitron',sans-serif;
}
.qds-collapsible-icon{transition:all 0.3s ease;color:var(--qds-accent);}
.qds-collapsible.open .qds-collapsible-icon{transform:rotate(180deg);color:var(--qds-primary);}
.qds-collapsible-content{padding:0 18px;max-height:0;overflow:hidden;transition:max-height 0.5s cubic-bezier(0.4,0,0.2,1);}
.qds-collapsible.open .qds-collapsible-content{padding:18px;max-height:5000px;}

/* ── Team Cards ── */
.qds-team-cards{display:flex;flex-direction:column;gap:16px;margin-bottom:20px;}
.qds-team-card{background:rgba(20,26,45,0.7);border-radius:12px;padding:18px;border-left:4px solid var(--qds-primary);}
.qds-team-header{display:flex;align-items:center;gap:12px;margin-bottom:12px;}
.qds-stat-row{display:flex;flex-wrap:wrap;margin-bottom:10px;align-items:flex-start;gap:4px;}
.qds-stat-icon{color:var(--qds-primary);font-size:0.75rem;margin-top:3px;}
.qds-stat-label{font-weight:600;color:var(--qds-primary);font-size:0.82rem;min-width:75px;display:flex;align-items:center;gap:4px;}
.qds-stat-value{font-size:0.82rem;color:var(--qds-text-light);flex:1;}

/* ── Section Title ── */
.qds-section-title{display:flex;align-items:center;gap:10px;color:var(--qds-primary);margin-bottom:12px;}
.qds-matchup-text{
  font-size:0.9rem;line-height:1.65;margin-bottom:12px;padding-left:14px;
  border-left:2px solid var(--qds-primary);position:relative;
}

/* ── Prop Cards ── */
.qds-prop-card{
  background:var(--qds-card);border-radius:12px;padding:18px;margin-bottom:20px;
  position:relative;overflow:hidden;border-top:3px solid var(--qds-primary);
}
.qds-prop-badge{
  position:absolute;top:14px;right:14px;background:var(--qds-primary);
  color:var(--qds-text-dark);padding:4px 10px;border-radius:4px;
  font-size:0.72rem;font-weight:700;text-transform:uppercase;
  font-family:'Orbitron',sans-serif;z-index:2;
}
.qds-prop-header{display:flex;flex-direction:column;gap:12px;margin-bottom:18px;}
@media(min-width:480px){.qds-prop-header{flex-direction:row;align-items:flex-start;}}
.qds-player-img{
  width:68px;height:68px;border-radius:50%;border:3px solid var(--qds-primary);
  object-fit:cover;align-self:center;flex-shrink:0;
  background:#1a2238;
}
.qds-player-info{flex:1;}
.qds-player-name{font-size:1.1rem;margin:0;color:var(--qds-primary);display:flex;align-items:center;gap:8px;flex-wrap:wrap;}
.qds-player-team-badge{font-size:0.78rem;background:rgba(255,255,255,0.08);padding:2px 7px;border-radius:4px;}
.qds-player-prop{font-size:0.95rem;color:var(--qds-text-light);margin-top:7px;font-weight:600;display:flex;align-items:center;gap:6px;}
.qds-prop-emoji{font-size:1.1rem;}
.qds-safe-score{display:flex;flex-wrap:wrap;align-items:center;gap:8px;margin-top:12px;}
.qds-score-value{
  font-weight:700;font-size:1.05rem;color:var(--qds-primary);
  background:rgba(0,255,213,0.1);padding:4px 12px;border-radius:50px;
  display:flex;align-items:center;gap:6px;
}
.qds-score-label{font-size:0.85rem;color:var(--qds-text-muted);}
.qds-confidence-tier{display:inline-flex;align-items:center;gap:5px;font-size:0.82rem;padding:4px 10px;border-radius:50px;}
.qds-tier-diamond{background:rgba(0,255,213,0.12);color:var(--qds-primary);border:1px solid var(--qds-primary);box-shadow:0 0 8px rgba(0,255,213,0.5);}
.qds-tier-lock{background:rgba(255,204,0,0.12);color:var(--qds-warning);border:1px solid var(--qds-warning);box-shadow:0 0 8px rgba(255,204,0,0.5);}
.qds-tier-check{background:rgba(0,163,255,0.12);color:var(--qds-info);border:1px solid var(--qds-info);box-shadow:0 0 8px rgba(0,163,255,0.4);}
.qds-tier-caution{background:rgba(255,94,0,0.10);color:#ff5e00;border:1px solid #ff5e00;box-shadow:0 0 8px rgba(255,94,0,0.4);}

/* ── Metrics Grid ── */
.qds-metrics-grid{display:grid;grid-template-columns:1fr;gap:12px;margin:16px 0;}
@media(min-width:560px){.qds-metrics-grid{grid-template-columns:repeat(auto-fill,minmax(220px,1fr));}}
.qds-metric-item{background:rgba(20,26,45,0.75);padding:13px;border-radius:8px;border-left:3px solid var(--qds-primary);}
.qds-metric-header{display:flex;align-items:center;gap:8px;margin-bottom:10px;}
.qds-metric-name{font-size:0.82rem;font-weight:600;color:var(--qds-primary);flex:1;}
.qds-metric-score{font-weight:700;color:var(--qds-primary);background:rgba(0,255,213,0.1);padding:2px 7px;border-radius:4px;font-size:0.82rem;}
.qds-metric-justification{font-size:0.82rem;color:var(--qds-text-light);line-height:1.55;}
.qds-stat-badge{display:inline-block;background:rgba(0,255,213,0.1);color:var(--qds-primary);border:1px solid rgba(0,255,213,0.3);border-radius:4px;padding:2px 7px;font-size:0.78rem;font-weight:600;margin-right:4px;margin-bottom:3px;}

/* ── Bonus Factors ── */
.qds-bonus-factors{margin-top:16px;padding-top:13px;border-top:1px dashed rgba(255,255,255,0.08);}
.qds-bonus-title{font-size:0.85rem;color:var(--qds-primary);margin-bottom:10px;display:flex;align-items:center;gap:6px;}
.qds-bonus-item{display:flex;align-items:flex-start;gap:8px;margin-bottom:10px;}
.qds-bonus-icon{color:var(--qds-primary);font-size:0.78rem;margin-top:3px;}
.qds-bonus-text{font-size:0.82rem;color:var(--qds-text-light);flex:1;line-height:1.5;}

/* ── Confidence Bars ── */
.qds-confidence-bars{margin:16px 0;}
.qds-confidence-bar{height:9px;background:#1a2238;border-radius:5px;margin-bottom:10px;overflow:hidden;}
.qds-confidence-fill{
  height:100%;
  background:linear-gradient(90deg,var(--qds-primary),var(--qds-accent));
  width:0;border-radius:5px;
  transition:width 1.5s cubic-bezier(0.4,0,0.2,1);
}
/* Color-coded confidence fill variants (Platinum/Gold/Silver/Bronze) */
.qds-conf-fill-high{background:linear-gradient(90deg,#00ffd5,#00ff88)!important;}
.qds-conf-fill-mid{background:linear-gradient(90deg,#ffcc00,#ff9500)!important;}
.qds-conf-fill-low{background:linear-gradient(90deg,#00b4ff,#0070cc)!important;}
.qds-conf-fill-very-low{background:linear-gradient(90deg,#ff5e00,#ff3860)!important;}
.qds-confidence-labels{display:flex;justify-content:space-between;font-size:0.85rem;color:var(--qds-text-muted);margin-bottom:14px;}
.qds-confidence-name{display:flex;align-items:center;gap:7px;}
/* ── Verdict paragraph ── */
.qds-prop-verdict{
  margin-top:12px;padding:10px 14px;font-size:0.83rem;line-height:1.6;
  background:rgba(0,255,213,0.04);border-left:3px solid var(--qds-primary);
  border-radius:0 6px 6px 0;color:var(--qds-text-light);font-style:italic;
}

/* ── Strategy Table ── */
.qds-strategy-table{width:100%;border-collapse:collapse;margin-top:16px;font-size:0.85rem;background:var(--qds-card);border-radius:8px;overflow:hidden;}
.qds-strategy-table th{text-align:left;padding:11px 14px;color:var(--qds-primary);border-bottom:1px solid rgba(255,255,255,0.08);background:rgba(0,255,213,0.04);}
.qds-strategy-table td{padding:11px 14px;color:var(--qds-text-light);border-bottom:1px solid rgba(255,255,255,0.05);vertical-align:middle;}
.qds-strategy-table tr:last-child td{border-bottom:none;}
.qds-strategy-pick{display:flex;flex-direction:column;gap:5px;margin:3px 0;}
.qds-strategy-player{font-weight:600;color:var(--qds-primary);display:flex;align-items:center;gap:7px;}
.qds-strategy-prop{font-size:0.8rem;color:var(--qds-text-muted);padding-left:18px;}
.qds-strategy-tag{background:rgba(0,255,213,0.1);color:var(--qds-primary);border:1px solid rgba(0,255,213,0.3);padding:3px 10px;border-radius:50px;font-size:0.78rem;white-space:nowrap;}

/* ── Framework Logic ── */
.qds-logic-item{display:flex;align-items:flex-start;gap:10px;margin-bottom:13px;padding:13px;background:rgba(20,26,45,0.75);border-radius:8px;}
.qds-logic-icon{color:var(--qds-primary);font-size:1rem;margin-top:2px;}
.qds-logic-text{font-size:0.9rem;color:var(--qds-text-light);flex:1;}
.qds-logic-text strong{color:var(--qds-primary);font-weight:600;}

/* ── Final Word ── */
.qds-final-word{background:var(--qds-card);border-radius:12px;padding:18px;border-left:3px solid var(--qds-primary);}
.qds-final-text{font-size:0.95rem;color:var(--qds-text-light);line-height:1.65;margin-bottom:18px;font-style:italic;}
.qds-cta{display:flex;align-items:center;gap:10px;margin-bottom:13px;color:var(--qds-primary);}
.qds-cta-steps{display:flex;flex-direction:column;gap:10px;}
.qds-cta-step{display:flex;align-items:flex-start;gap:10px;}
.qds-cta-text{flex:1;font-size:0.9rem;}

/* ── Empty State ── */
.qds-empty{text-align:center;padding:40px 20px;color:var(--qds-text-muted);}
.qds-empty-icon{font-size:2.2rem;color:var(--qds-primary);display:block;margin-bottom:12px;}

@media(min-width:768px){
  .qds-team-cards{flex-direction:row;}
  .qds-team-card{flex:1;}
  .qds-game-meta{flex-direction:row;}
}
</style>"""

# ── Static JS for QDS report ────────────────────────────────────────────────
_QDS_REPORT_JS = """
<script>
function qdsToggle(id){
  var el=document.getElementById(id);
  el.classList.toggle('open');
  if(el.classList.contains('open')) qdsAnimateBars();
}
function qdsAnimateBars(){
  document.querySelectorAll('.qds-confidence-fill').forEach(function(bar){
    var w=bar.getAttribute('data-width');
    bar.style.width='0';
    setTimeout(function(){bar.style.width=w;},80);
  });
}
document.addEventListener('DOMContentLoaded',function(){
  document.querySelectorAll('.qds-collapsible').forEach(function(s){s.classList.add('open');});
  setTimeout(qdsAnimateBars,400);
});
</script>"""


def get_game_report_html(game=None, analysis_results=None):
    """
    Generate a complete QDS-styled NBA game betting report as a self-contained HTML document.

    Produces a fully interactive report with collapsible sections, animated confidence
    bars, SAFE Score™ prop cards with per-metric breakdowns, team analysis, and entry
    strategy matrix. Designed for embedding via st.components.v1.html().

    Args:
        game (dict|None): Game dict from session state with home_team, away_team, records
        analysis_results (list|None): Analysis result dicts from Neural Analysis engine

    Returns:
        str: Complete self-contained HTML document with embedded CSS and JS
    """
    NBA_CDN = "https://cdn.nba.com/headshots/nba/latest/1040x760"
    ESPN_NBA = "https://a.espncdn.com/i/teamlogos/nba/500"

    # Confidence thresholds for color-coded bar fills
    _CONF_HIGH  = 80   # ≥ 80%  → cyan  gradient (Platinum / High)
    _CONF_MID   = 60   # ≥ 60%  → gold  gradient (Gold / Moderate)
    _CONF_LOW   = 40   # ≥ 40%  → blue  gradient (Silver / Lower)

    # ── Data Prep ─────────────────────────────────────────────
    results = sorted(
        analysis_results or [],
        key=lambda x: x.get("confidence_score", 0),
        reverse=True,
    )
    top_picks = results[:3]
    today_str = _datetime.date.today().strftime("%B %d, %Y")

    # ── Game Data ─────────────────────────────────────────────
    if game:
        home = game.get("home_team", "HOME")
        away = game.get("away_team", "AWAY")
        hw = game.get("home_wins")
        hl = game.get("home_losses")
        aw = game.get("away_wins")
        al = game.get("away_losses")
        home_record = f"{hw}-{hl}" if (hw is not None and hl is not None and (hw > 0 or hl > 0)) else "N/A"
        away_record  = f"{aw}-{al}" if (aw is not None and al is not None and (aw > 0 or al > 0)) else "N/A"
    else:
        home, away = "HOME", "AWAY"
        home_record = away_record = "N/A"

    home_color, _ = get_team_colors(home)
    away_color, _  = get_team_colors(away)
    home_logo = f"{ESPN_NBA}/{home.lower()}.png"
    away_logo = f"{ESPN_NBA}/{away.lower()}.png"

    # ── Tier Mappings ─────────────────────────────────────────
    TIER = {
        "Platinum": {"icon": "gem",   "label": "95%+ Confidence", "css": "qds-tier-diamond"},
        "Gold":     {"icon": "lock",  "label": "90% Confidence",  "css": "qds-tier-lock"},
        "Silver":   {"icon": "check", "label": "85% Confidence",  "css": "qds-tier-check"},
        "Bronze":   {"icon": "star",  "label": "80% Confidence",  "css": "qds-tier-caution"},
    }
    BADGE = [("QUANTUM PICK", "bolt"), ("STRONG PICK", "lock"), ("SAFE PICK", "check")]
    STAT_EMOJI = {
        "points": "🏀", "rebounds": "📊", "assists": "🎯",
        "threes": "🎯", "steals": "⚡", "blocks": "🛡️", "turnovers": "❌",
    }

    def _ss(conf):
        """Convert 0-100 confidence to 0-10 SAFE Score."""
        return round(min(10.0, conf / 10.0), 1)

    def _prop_pct(pick):
        """Return the relevant hit-probability percentage for a pick (0-100 float)."""
        prob_over = pick.get("probability_over", 0.5)
        direction = pick.get("direction", "OVER")
        return prob_over * 100 if direction == "OVER" else (1 - prob_over) * 100

    def _badge(text, color):
        return (
            f'<span class="qds-stat-badge" style="border-color:{color};color:{color};">'
            f'{_html.escape(str(text))}</span>'
        )

    def _force_items(pick, max_n=2):
        over_f  = pick.get("forces", {}).get("over_forces",  [])
        under_f = pick.get("forces", {}).get("under_forces", [])
        items = (over_f + under_f)[:max_n]
        html_out = ""
        for f in items:
            lbl  = f.get("label", f.get("factor", ""))
            desc = f.get("description", f.get("detail", ""))
            if lbl:
                html_out += (
                    f'<div class="qds-bonus-item">'
                    f'<i class="fas fa-circle-check qds-bonus-icon"></i>'
                    f'<div class="qds-bonus-text"><strong>{_html.escape(str(lbl))}</strong>'
                    f'{f" — {_html.escape(str(desc))}" if desc else ""}'
                    f'</div></div>'
                )
        if not html_out:
            edge = pick.get("edge_percentage", 0)
            prob_pct = int(_prop_pct(pick))
            sign = "+" if edge >= 0 else ""
            html_out = (
                f'<div class="qds-bonus-item">'
                f'<i class="fas fa-circle-check qds-bonus-icon"></i>'
                f'<div class="qds-bonus-text">'
                f'<strong>{sign}{edge:.1f}% edge vs implied probability</strong>'
                f' — AI model shows {prob_pct}% hit rate across 1,000+ simulations'
                f'</div></div>'
            )
        return html_out

    # ── Confidence Bars ───────────────────────────────────────
    conf_bars = ""
    for pick in top_picks:
        player    = pick.get("player_name", "Player")
        stat      = pick.get("stat_type", "stat").capitalize()
        line      = pick.get("line", 0)
        direction = pick.get("direction", "OVER")
        prob_pct  = int(_prop_pct(pick))
        tier      = pick.get("tier", "Silver")
        td        = TIER.get(tier, TIER["Silver"])
        # Color-code the fill based on confidence level
        fill_class = (
            "qds-conf-fill-high"     if prob_pct >= _CONF_HIGH else
            "qds-conf-fill-mid"      if prob_pct >= _CONF_MID  else
            "qds-conf-fill-low"      if prob_pct >= _CONF_LOW  else
            "qds-conf-fill-very-low"
        )
        conf_bars += (
            f'<div class="qds-confidence-bar">'
            f'<div class="qds-confidence-fill {fill_class}" data-width="{prob_pct}%"></div></div>'
            f'<div class="qds-confidence-labels">'
            f'<span class="qds-confidence-name">'
            f'<i class="fas fa-{td["icon"]}" style="color:var(--qds-primary);"></i>'
            f'<span>{_html.escape(player)} &nbsp;{direction} {line} {stat}</span>'
            f'</span><span>{prob_pct}%</span></div>'
        )
    if not conf_bars:
        conf_bars = (
            '<p class="qds-empty">'
            '<i class="fas fa-robot qds-empty-icon"></i>'
            'Run Neural Analysis to see confidence rankings.</p>'
        )

    # ── Prop Cards ────────────────────────────────────────────
    prop_cards = ""
    for idx, pick in enumerate(top_picks):
        player    = pick.get("player_name", "Unknown")
        stat      = pick.get("stat_type", "points").capitalize()
        line      = pick.get("line", 0)
        direction = pick.get("direction", "OVER")
        tier      = pick.get("tier", "Silver")
        prob_over = pick.get("probability_over", 0.5)
        edge      = pick.get("edge_percentage", 0)
        conf      = pick.get("confidence_score", 75)
        platform  = pick.get("platform", "")
        team      = pick.get("player_team", pick.get("team", ""))
        player_id = pick.get("player_id", "")

        pts_avg = pick.get("season_pts_avg", pick.get("points_avg", 0))
        reb_avg = pick.get("season_reb_avg", pick.get("rebounds_avg", 0))
        ast_avg = pick.get("season_ast_avg", pick.get("assists_avg", 0))
        proj    = pick.get("adjusted_projection", 0)

        prob_pct  = _prop_pct(pick)
        ss        = _ss(conf)
        edge_sign = "+" if edge >= 0 else ""

        td = TIER.get(tier, TIER["Silver"])
        bl, bi = BADGE[idx] if idx < len(BADGE) else ("PICK", "check")

        hs_url  = f"{NBA_CDN}/{player_id}.png" if player_id and str(player_id).strip() else f"{NBA_CDN}/fallback.png"
        hs_fall = f"{NBA_CDN}/fallback.png"
        prop_emoji = STAT_EMOJI.get(stat.lower(), "📊")
        tcolor, _ = get_team_colors(team)

        # Stat badges for metric card
        sbadges = ""
        if pts_avg: sbadges += _badge(f"{pts_avg:.1f} PPG", "#00ffd5")
        if reb_avg: sbadges += _badge(f"{reb_avg:.1f} RPG", "#00ffd5")
        if ast_avg: sbadges += _badge(f"{ast_avg:.1f} APG", "#00ffd5")
        if proj:    sbadges += _badge(f"{proj:.1f} Proj",   "#00b4ff")
        if not sbadges:
            sbadges = f'<span style="color:var(--qds-text-muted);">Stats for {_html.escape(player)}</span>'

        team_badge_html = ""
        if team:
            team_badge_html = (
                f'<span class="qds-player-team-badge" '
                f'style="border:1px solid {tcolor};color:{tcolor};">'
                f'{_html.escape(team)}</span>'
            )
        plat_html = (
            f'<span style="font-size:0.8rem;color:var(--qds-text-muted);margin-left:6px;">'
            f'{_html.escape(platform)}</span>'
            if platform else ""
        )

        # Extract plain-English verdict for the prop card footer
        pick_verdict = (pick.get("explanation") or {}).get("verdict") or pick.get("recommendation", "")
        verdict_html = (
            f'<p class="qds-prop-verdict">{_html.escape(str(pick_verdict))}</p>'
            if pick_verdict else ""
        )

        prop_cards += f"""
<div class="qds-prop-card">
  <div class="qds-prop-badge"><i class="fas fa-{bi}"></i> {bl}</div>
  <div class="qds-prop-header">
    <img src="{hs_url}" onerror="this.onerror=null;this.src='{hs_fall}';"
         class="qds-player-img" alt="{_html.escape(player)}" loading="lazy" width="68" height="68">
    <div class="qds-player-info">
      <h3 class="qds-player-name">{_html.escape(player)} {team_badge_html}</h3>
      <div class="qds-player-prop">
        <span class="qds-prop-emoji">{prop_emoji}</span>
        <span><span style="color:{'var(--qds-success)' if direction == 'OVER' else '#ff5e00'};font-weight:700;">{direction}</span> {line} {stat}</span>{plat_html}
      </div>
      <div class="qds-safe-score">
        <span class="qds-score-value"><i class="fas fa-shield-alt"></i> {ss:.1f} / 10</span>
        <span class="qds-score-label">SAFE Score™</span>
        <span class="qds-confidence-tier {td['css']}">
          <i class="fas fa-{td['icon']}"></i> <span>{td['label']}</span>
        </span>
      </div>
    </div>
  </div>
  <div class="qds-metrics-grid">
    <div class="qds-metric-item">
      <div class="qds-metric-header">
        <i class="fas fa-chart-line"></i>
        <span class="qds-metric-name">Season Stats</span>
        <span class="qds-metric-score">{min(9.8, ss + 0.1):.1f}</span>
      </div>
      <p class="qds-metric-justification">{sbadges}</p>
    </div>
    <div class="qds-metric-item">
      <div class="qds-metric-header">
        <i class="fas fa-chess"></i>
        <span class="qds-metric-name">Matchup Edge</span>
        <span class="qds-metric-score">{ss:.1f}</span>
      </div>
      <p class="qds-metric-justification">
        {edge_sign}{edge:.1f}% edge vs posted line. Model sees favorable conditions
        for {direction.lower()} {line} {stat.lower()}.
      </p>
    </div>
    <div class="qds-metric-item">
      <div class="qds-metric-header">
        <i class="fas fa-brain"></i>
        <span class="qds-metric-name">AI Model Signal</span>
        <span class="qds-metric-score">{ss:.1f}</span>
      </div>
      <p class="qds-metric-justification">
        Monte Carlo simulation: <strong style="color:var(--qds-primary);">{int(prob_pct)}%
        hit rate</strong> across 1,000+ game scenarios.
      </p>
    </div>
    <div class="qds-metric-item">
      <div class="qds-metric-header">
        <i class="fas fa-shield-alt"></i>
        <span class="qds-metric-name">Confidence</span>
        <span class="qds-metric-score">{conf:.0f}/100</span>
      </div>
      <p class="qds-metric-justification">
        Quantum Matrix Engine 5.6 rating integrating sample size, matchup clarity, and simulation stability.
      </p>
    </div>
  </div>
  <div class="qds-bonus-factors">
    <div class="qds-bonus-title"><i class="fas fa-star"></i> Key Supporting Factors:</div>
    {_force_items(pick)}
  </div>{verdict_html}
  <!-- Always-open full breakdown panel -->
  <div style="background:rgba(13,18,32,0.7);border-radius:6px;padding:12px 15px;margin-top:10px;border:1px solid rgba(255,94,0,0.12);">
    <div style="color:#ff5e00;font-weight:600;font-size:0.8rem;margin-bottom:10px;">📊 Distribution</div>
    <div style="display:grid;grid-template-columns:repeat(4,1fr);gap:8px;margin-bottom:10px;">
      <div style="text-align:center;padding:6px;background:rgba(7,10,19,0.6);border-radius:5px;">
        <div style="color:#b0bec5;font-size:0.7rem;">10th pct</div>
        <div style="color:#ff5e00;font-weight:700;font-size:0.85rem;">{pick.get("percentile_10", 0):.1f}</div>
      </div>
      <div style="text-align:center;padding:6px;background:rgba(7,10,19,0.6);border-radius:5px;">
        <div style="color:#b0bec5;font-size:0.7rem;">Median</div>
        <div style="color:var(--qds-primary);font-weight:700;font-size:0.85rem;">{pick.get("percentile_50", 0):.1f}</div>
      </div>
      <div style="text-align:center;padding:6px;background:rgba(7,10,19,0.6);border-radius:5px;">
        <div style="color:#b0bec5;font-size:0.7rem;">90th pct</div>
        <div style="color:#ff5e00;font-weight:700;font-size:0.85rem;">{pick.get("percentile_90", 0):.1f}</div>
      </div>
      <div style="text-align:center;padding:6px;background:rgba(7,10,19,0.6);border-radius:5px;">
        <div style="color:#b0bec5;font-size:0.7rem;">Std Dev</div>
        <div style="color:white;font-weight:700;font-size:0.85rem;">{pick.get("simulated_std", 0):.1f}</div>
      </div>
    </div>
    <div style="display:grid;grid-template-columns:1fr 1fr;gap:10px;">
      <div style="padding:8px;background:rgba(0,240,255,0.04);border-radius:5px;border-left:2px solid var(--qds-primary);">
        <div style="color:var(--qds-primary);font-size:0.75rem;font-weight:600;margin-bottom:4px;">🔵 Forces OVER</div>
        {_force_items(pick, max_n=2) if pick.get("forces", {}).get("over_forces") else '<span style="color:#b0bec5;font-size:0.75rem;">None detected</span>'}
      </div>
      <div style="padding:8px;background:rgba(255,94,0,0.04);border-radius:5px;border-left:2px solid #ff5e00;">
        <div style="color:#ff5e00;font-size:0.75rem;font-weight:600;margin-bottom:4px;">🔴 Forces UNDER</div>
        {_force_items(pick, max_n=2) if pick.get("forces", {}).get("under_forces") else '<span style="color:#b0bec5;font-size:0.75rem;">None detected</span>'}
      </div>
    </div>
  </div>
</div>"""

    if not prop_cards:
        prop_cards = (
            '<div class="qds-empty">'
            '<i class="fas fa-robot qds-empty-icon"></i>'
            '<p style="font-size:1rem;margin-bottom:8px;">No analysis results available yet.</p>'
            '<p style="font-size:0.85rem;">Go to <strong style="color:var(--qds-primary);">'
            '⚡ Neural Analysis</strong> to generate prop predictions.</p></div>'
        )

    # ── Strategy Matrix ───────────────────────────────────────
    strategy_rows = ""
    if len(top_picks) >= 2:
        matrix = []
        avg2 = sum(p.get("confidence_score", 75) for p in top_picks[:2]) / 2 / 10
        matrix.append(("Pick 2", "fire", "danger",  top_picks[:2], "Power Play",  f"{avg2:.2f}"))
        if len(top_picks) >= 3:
            avg3 = sum(p.get("confidence_score", 75) for p in top_picks[:3]) / 3 / 10
            matrix.append(("Pick 3", "lock",        "success", top_picks[:3], "Flex Core",   f"{avg3:.2f}"))
            matrix.append(("Pick 5", "layer-group", "warning", top_picks[:3], "Stack Build", f"{avg3:.2f}"))

        for combo, icon, color_name, picks, strategy, avg_ss in matrix:
            picks_html = ""
            for j, p in enumerate(picks[:2]):
                pname = _html.escape(p.get("player_name", ""))
                pstat = p.get("stat_type", "").capitalize()
                pline = p.get("line", 0)
                pdir  = p.get("direction", "OVER")
                ptier = p.get("tier", "Silver")
                ptd   = TIER.get(ptier, TIER["Silver"])
                picks_html += (
                    f'<div class="qds-strategy-pick">'
                    f'<span class="qds-strategy-player">'
                    f'<i class="fas fa-{ptd["icon"]}" style="color:var(--qds-primary);"></i>'
                    f' {pname}</span>'
                    f'<span class="qds-strategy-prop">{pdir} {pline} {pstat}</span></div>'
                )
                if j == 0:
                    picks_html += '<span style="color:var(--qds-text-muted);font-size:0.85rem;padding:3px 0;display:block;">+</span>'
            strategy_rows += (
                f'<tr>'
                f'<td><i class="fas fa-{icon}" style="color:var(--qds-{color_name});margin-right:6px;"></i>{combo}</td>'
                f'<td>{picks_html}</td>'
                f'<td style="font-family:\'Courier New\',monospace;color:var(--qds-primary);font-weight:700;">{avg_ss}</td>'
                f'<td><span class="qds-strategy-tag">{strategy}</span></td>'
                f'</tr>'
            )

    if not strategy_rows:
        strategy_rows = (
            '<tr><td colspan="4" class="qds-empty" style="padding:24px;">'
            'Run Neural Analysis to populate strategy recommendations.</td></tr>'
        )

    # ── Team Player Badges ────────────────────────────────────
    home_players = [r for r in results if r.get("player_team", "").upper() == home.upper()]
    away_players = [r for r in results if r.get("player_team", "").upper() == away.upper()]

    def _player_badges(players, color, max_n=3):
        seen = set(); out = ""
        for p in players[:max_n]:
            name = p.get("player_name", "")
            pts  = p.get("season_pts_avg", p.get("points_avg", 0))
            if name and name not in seen:
                seen.add(name)
                label = f"{name} ({pts:.0f} PPG)" if pts else name
                out += _badge(label, color)
        return out or "—"

    home_pbadges = _player_badges(home_players, home_color)
    away_pbadges = _player_badges(away_players, away_color)

    # ── Final Word ────────────────────────────────────────────
    pick_summaries = []
    for p in top_picks[:3]:
        pname = p.get("player_name", "")
        pdir  = p.get("direction", "OVER")
        pline = p.get("line", 0)
        pstat = p.get("stat_type", "").capitalize()
        ppct  = int(_prop_pct(p))
        pick_summaries.append(f"{pname} {pdir} {pline} {pstat} ({ppct}%)")

    primary  = pick_summaries[0] if pick_summaries else "—"
    second   = " + ".join(pick_summaries[1:]) if len(pick_summaries) > 1 else "—"
    pick2txt = " + ".join(pick_summaries[:2]) if len(pick_summaries) >= 2 else primary

    matchup_label = f"{away} @ {home}" if game else "Tonight's Matchup"

    # ── Assemble Full HTML ────────────────────────────────────
    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>SmartBetPro NBA — {_html.escape(matchup_label)} Report</title>
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
  <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.4.0/css/all.min.css">
  {_QDS_REPORT_CSS}
</head>
<body>
<div class="qds-container">

  <!-- ── Header ── -->
  <header class="qds-report-header">
    <div class="qds-report-title">
      <i class="fas fa-robot qds-report-title-icon"></i>
      <h1 class="qds-report-title-text">{_html.escape(away)} vs {_html.escape(home)}</h1>
    </div>
    <div class="qds-game-info-container">
      <div class="qds-game-teams">
        <div class="qds-team-container">
          <div class="qds-team-brand">
            <img src="{away_logo}" onerror="this.style.display='none';"
                 class="qds-team-logo" alt="{_html.escape(away)}" loading="lazy" width="38" height="38">
            <span class="qds-team-name-txt" style="color:{away_color};">{_html.escape(away)}</span>
          </div>
          <div class="qds-team-record"><i class="fas fa-flag"></i><span>{away_record}</span></div>
        </div>
        <span class="qds-vs-separator">VS</span>
        <div class="qds-team-container">
          <div class="qds-team-brand">
            <img src="{home_logo}" onerror="this.style.display='none';"
                 class="qds-team-logo" alt="{_html.escape(home)}" loading="lazy" width="38" height="38">
            <span class="qds-team-name-txt" style="color:{home_color};">{_html.escape(home)}</span>
          </div>
          <div class="qds-team-record"><i class="fas fa-trophy"></i><span>{home_record}</span></div>
        </div>
      </div>
      <div class="qds-game-meta">
        <span class="qds-game-date">
          <i class="far fa-calendar-alt"></i> {today_str}
        </span>
        <div class="qds-framework">
          <i class="fas fa-brain"></i>
          <span>SAFE SCORE™ AI · QUANTUM MATRIX ENGINE 5.6</span>
          <i class="fas fa-atom"></i>
        </div>
      </div>
    </div>
  </header>

  <main>

    <!-- ── Team Analysis ── -->
    <div class="qds-collapsible open" id="qdsTeams">
      <div class="qds-collapsible-header" onclick="qdsToggle('qdsTeams')">
        <h2 class="qds-collapsible-title">
          <i class="fas fa-network-wired"></i> TEAM MATCHUP BREAKDOWN
        </h2>
        <i class="fas fa-chevron-down qds-collapsible-icon"></i>
      </div>
      <div class="qds-collapsible-content">
        <div class="qds-team-cards">
          <div class="qds-team-card" style="border-left-color:{away_color};">
            <div class="qds-team-header">
              <img src="{away_logo}" onerror="this.style.display='none';"
                   class="qds-team-logo" alt="{_html.escape(away)}" loading="lazy" width="38" height="38">
              <div>
                <h3 class="qds-team-name-txt" style="color:{away_color};font-size:1.05rem;">{_html.escape(away)}</h3>
                <div class="qds-team-record"><i class="fas fa-flag"></i><span>{away_record}</span></div>
              </div>
            </div>
            <div>
              <div class="qds-stat-row">
                <i class="fas fa-users qds-stat-icon"></i>
                <span class="qds-stat-label">Key Players:</span>
                <span class="qds-stat-value">{away_pbadges if away_pbadges != "—" else "Load analysis for player data"}</span>
              </div>
            </div>
          </div>
          <div class="qds-team-card" style="border-left-color:{home_color};">
            <div class="qds-team-header">
              <img src="{home_logo}" onerror="this.style.display='none';"
                   class="qds-team-logo" alt="{_html.escape(home)}" loading="lazy" width="38" height="38">
              <div>
                <h3 class="qds-team-name-txt" style="color:{home_color};font-size:1.05rem;">{_html.escape(home)}</h3>
                <div class="qds-team-record"><i class="fas fa-trophy"></i><span>{home_record}</span></div>
              </div>
            </div>
            <div>
              <div class="qds-stat-row">
                <i class="fas fa-users qds-stat-icon"></i>
                <span class="qds-stat-label">Key Players:</span>
                <span class="qds-stat-value">{home_pbadges if home_pbadges != "—" else "Load analysis for player data"}</span>
              </div>
            </div>
          </div>
        </div>
        <div>
          <h3 class="qds-section-title"><i class="fas fa-chart-network"></i> KEY MATCHUP INSIGHTS</h3>
          <p class="qds-matchup-text">
            SmartBetPro's Quantum Matrix Engine 5.6 has run 1,000+ Monte Carlo simulations for this matchup.
            The top-ranked props below reflect the strongest signal-to-noise ratio across all analysed players —
            each selected based on edge vs the posted line, recent form, and matchup-specific factors.
          </p>
          <p class="qds-matchup-text">
            All picks carry a SAFE Score™ of 8.0+ and have been validated against the current season sample.
            Focus on the <strong style="color:var(--qds-primary);">Quantum Pick</strong> for single-leg entries
            and use the Strategy Matrix below to build optimal multi-leg combinations.
          </p>
        </div>
      </div>
    </div>

    <!-- ── Top Prop Bets ── -->
    <div class="qds-collapsible open" id="qdsProps">
      <div class="qds-collapsible-header" onclick="qdsToggle('qdsProps')">
        <h2 class="qds-collapsible-title">
          <i class="fas fa-magnifying-glass-chart"></i> TOP PROP BETS (SAFE SCORE™ RANKED)
        </h2>
        <i class="fas fa-chevron-down qds-collapsible-icon"></i>
      </div>
      <div class="qds-collapsible-content">
        <div class="qds-confidence-bars">{conf_bars}</div>
        {prop_cards}
      </div>
    </div>

    <!-- ── Entry Strategy Matrix ── -->
    <div class="qds-collapsible open" id="qdsStrategy">
      <div class="qds-collapsible-header" onclick="qdsToggle('qdsStrategy')">
        <h2 class="qds-collapsible-title">
          <i class="fas fa-chess-board"></i> ENTRY STRATEGY MATRIX
        </h2>
        <i class="fas fa-chevron-down qds-collapsible-icon"></i>
      </div>
      <div class="qds-collapsible-content">
        <table class="qds-strategy-table">
          <thead>
            <tr>
              <th>Combo</th><th>Picks</th><th>SAFE Avg</th><th>Strategy</th>
            </tr>
          </thead>
          <tbody>{strategy_rows}</tbody>
        </table>
      </div>
    </div>

    <!-- ── Framework Logic ── -->
    <div class="qds-collapsible open" id="qdsFramework">
      <div class="qds-collapsible-header" onclick="qdsToggle('qdsFramework')">
        <h2 class="qds-collapsible-title">
          <i class="fas fa-sitemap"></i> WHY THIS WORKS — FRAMEWORK LOGIC
        </h2>
        <i class="fas fa-chevron-down qds-collapsible-icon"></i>
      </div>
      <div class="qds-collapsible-content">
        <div class="qds-logic-item">
          <i class="fas fa-check qds-logic-icon"></i>
          <div class="qds-logic-text">
            <strong>SAFE Score™ Weighted System</strong> — Balances volatility with matchup intelligence
            via a proprietary algorithm analysing confidence, edge %, form, and situational factors.
          </div>
        </div>
        <div class="qds-logic-item">
          <i class="fas fa-project-diagram qds-logic-icon"></i>
          <div class="qds-logic-text">
            <strong>Causal-Driven Picks Only</strong> — No trend chasing. Every pick has layered
            justification with clear cause-effect relationships backed by Monte Carlo simulation.
          </div>
        </div>
        <div class="qds-logic-item">
          <i class="fas fa-chess-queen qds-logic-icon"></i>
          <div class="qds-logic-text">
            <strong>Multi-Lens Value</strong> — Combines projection delta + narrative context +
            edge % for maximum signal. We identify when the market hasn't adjusted to recent form.
          </div>
        </div>
        <div class="qds-logic-item">
          <i class="fas fa-layer-group qds-logic-icon"></i>
          <div class="qds-logic-text">
            <strong>Confidence Buckets</strong> — Tier system (Platinum / Gold / Silver / Bronze)
            maps directly to optimal entry formats (2, 3, 5) based on risk tolerance.
          </div>
        </div>
        <div class="qds-logic-item">
          <i class="fas fa-network-wired qds-logic-icon"></i>
          <div class="qds-logic-text">
            <strong>Stack Matrix Synergy</strong> — Complementary picks in the same game create
            correlated upside while the SAFE Score maintains strong individual probabilities.
          </div>
        </div>
      </div>
    </div>

    <!-- ── Final Word ── -->
    <div class="qds-collapsible open" id="qdsFinal">
      <div class="qds-collapsible-header" onclick="qdsToggle('qdsFinal')">
        <h2 class="qds-collapsible-title">
          <i class="fas fa-bullseye"></i> FINAL WORD FROM SMARTBETPRO NBA
        </h2>
        <i class="fas fa-chevron-down qds-collapsible-icon"></i>
      </div>
      <div class="qds-collapsible-content">
        <div class="qds-final-word">
          <p class="qds-final-text">
            "These aren't locks — they're engineered plays. Built with matchup logic, stress-tested
            through 1,000+ Monte Carlo simulations, and reinforced with real market edge.
            The Quantum Matrix Engine 5.6 has identified {len(top_picks)} high-probability props for
            {_html.escape(matchup_label)}, each with a SAFE Score™ of {_ss(top_picks[0].get('confidence_score', 75)) if top_picks else '—'}/10 or better.
            Play disciplined, size appropriately, and trust the process."
          </p>
          <div class="qds-cta">
            <i class="fas fa-rocket qds-cta-icon"></i>
            <span>Recommended Play Strategy:</span>
          </div>
          <div class="qds-cta-steps">
            <div class="qds-cta-step">
              <i class="fas fa-check qds-cta-icon"></i>
              <span class="qds-cta-text">
                <strong>Primary Play:</strong> {_html.escape(primary)}
              </span>
            </div>
            <div class="qds-cta-step">
              <i class="fas fa-check qds-cta-icon"></i>
              <span class="qds-cta-text">
                <strong>Multi-Leg:</strong> {_html.escape(pick2txt)} as a 2-leg entry
              </span>
            </div>
            <div class="qds-cta-step">
              <i class="fas fa-check qds-cta-icon"></i>
              <span class="qds-cta-text">
                <strong>Full Stack:</strong> {_html.escape(second if second != "—" else "See Strategy Matrix above for 3-leg recommendations")}
              </span>
            </div>
          </div>
        </div>
      </div>
    </div>

  </main>
</div>
{_QDS_REPORT_JS}
</body>
</html>"""

    return html


# ============================================================
# END SECTION: QDS Game Report Generator
# ============================================================
# ============================================================
# SECTION: QDS Neural Analysis HTML Generators
# Individual reusable building blocks for the Neural Analysis
# page redesign using the Quantum Design System visual language.
# All functions return self-contained HTML strings suitable for
# st.markdown(unsafe_allow_html=True) injection.
# ============================================================

# Shared QDS CSS injected once per page (lightweight variant —
# does not duplicate the full report CSS).
_QDS_NA_CSS = """
<style>
@import url('https://fonts.googleapis.com/css2?family=Orbitron:wght@400;600;700&family=Montserrat:wght@300;400;600;700&display=swap');
:root{
  --qds-primary:#00ffd5;--qds-bg:#0a101f;--qds-card:#141a2d;
  --qds-accent:#00b4ff;--qds-text-light:#f0f4ff;--qds-text-muted:#a0b4d0;
  --qds-success:#00ff88;--qds-warning:#ffcc00;--qds-danger:#ff3860;
  --qds-neon-shadow:0 0 10px rgba(0,255,213,0.5);
}
.qds-na-card{background:var(--qds-card);border-radius:12px;padding:18px;
  margin-bottom:18px;border-top:3px solid var(--qds-primary);
  box-shadow:var(--qds-neon-shadow);}
.qds-na-badge{display:inline-block;padding:3px 10px;border-radius:4px;
  font-size:0.72rem;font-weight:700;text-transform:uppercase;
  font-family:'Orbitron',sans-serif;letter-spacing:0.5px;}
.qds-na-player-name{font-family:'Orbitron',sans-serif;font-size:1.05rem;
  font-weight:700;color:var(--qds-text-light);margin-bottom:4px;}
.qds-na-prop-desc{color:var(--qds-primary);font-size:1.1rem;font-weight:600;
  margin-bottom:10px;}
.qds-na-score{font-family:'Orbitron',sans-serif;font-size:1.6rem;font-weight:700;
  color:var(--qds-primary);text-shadow:0 0 8px rgba(0,255,213,0.5);}
.qds-na-metrics-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(130px,1fr));
  gap:10px;margin:12px 0;}
.qds-na-metric-card{background:rgba(10,16,31,0.7);border-radius:8px;padding:10px;
  border:1px solid rgba(0,255,213,0.12);text-align:center;}
.qds-na-metric-label{font-size:0.7rem;color:var(--qds-text-muted);
  text-transform:uppercase;letter-spacing:0.5px;margin-bottom:4px;}
.qds-na-metric-value{font-size:1rem;font-weight:700;color:var(--qds-text-light);}
.qds-na-conf-bar-wrap{margin:4px 0 12px;}
.qds-na-conf-bar-label{font-size:0.8rem;color:var(--qds-text-muted);
  margin-bottom:3px;display:flex;justify-content:space-between;}
.qds-na-conf-bar-track{height:10px;background:rgba(255,255,255,0.08);
  border-radius:5px;overflow:hidden;}
.qds-na-conf-bar-fill{height:100%;border-radius:5px;
  background:linear-gradient(90deg,var(--qds-primary),var(--qds-accent));
  transition:width 0.5s ease;}
.qds-na-bonus-item{display:flex;align-items:flex-start;gap:8px;
  font-size:0.82rem;color:var(--qds-text-light);margin-bottom:5px;}
.qds-na-bonus-icon{color:var(--qds-success);margin-top:2px;flex-shrink:0;}
.qds-na-team-badge{display:inline-block;padding:2px 8px;border-radius:4px;
  font-size:0.72rem;font-weight:700;margin-left:6px;vertical-align:middle;}
.qds-na-header{text-align:center;padding:20px;border-radius:12px;
  background:linear-gradient(135deg,rgba(0,255,213,0.06) 0%,rgba(0,180,255,0.06) 100%);
  border:1px solid rgba(0,255,213,0.15);margin-bottom:18px;}
.qds-na-matchup{display:flex;align-items:center;justify-content:center;
  gap:16px;flex-wrap:wrap;padding:14px;border-radius:10px;
  background:rgba(10,16,31,0.6);border:1px solid rgba(0,255,213,0.1);}
.qds-na-team-block{display:flex;flex-direction:column;align-items:center;gap:4px;}
.qds-na-team-logo{width:44px;height:44px;object-fit:contain;
  filter:drop-shadow(0 0 6px rgba(0,180,255,0.4));}
.qds-na-team-abbrev{font-family:'Orbitron',sans-serif;font-weight:700;
  font-size:0.9rem;color:var(--qds-text-light);}
.qds-na-vs{font-family:'Orbitron',sans-serif;font-size:0.9rem;
  color:var(--qds-text-muted);padding:4px 12px;
  background:rgba(0,180,255,0.1);border-radius:20px;}
.qds-na-strategy-table{width:100%;border-collapse:collapse;font-size:0.85rem;
  color:var(--qds-text-light);}
.qds-na-strategy-table th{background:rgba(0,255,213,0.08);color:var(--qds-primary);
  padding:8px 12px;text-align:left;font-family:'Orbitron',sans-serif;
  font-size:0.72rem;letter-spacing:0.5px;border-bottom:1px solid rgba(0,255,213,0.2);}
.qds-na-strategy-table td{padding:8px 12px;
  border-bottom:1px solid rgba(255,255,255,0.05);vertical-align:top;}
.qds-na-strategy-table tr:hover td{background:rgba(0,255,213,0.03);}
.qds-na-logic-item{display:flex;align-items:flex-start;gap:10px;
  padding:8px 0;border-bottom:1px solid rgba(255,255,255,0.04);}
.qds-na-logic-icon{color:var(--qds-accent);font-size:1rem;flex-shrink:0;margin-top:1px;}
.qds-na-logic-title{font-weight:600;color:var(--qds-primary);font-size:0.85rem;}
.qds-na-logic-desc{font-size:0.8rem;color:var(--qds-text-muted);margin-top:2px;}
.qds-na-verdict{background:rgba(0,255,213,0.04);border-left:3px solid var(--qds-primary);
  border-radius:0 8px 8px 0;padding:14px 18px;margin-bottom:14px;
  font-style:italic;color:var(--qds-text-light);font-size:0.9rem;line-height:1.6;}
.qds-na-rec-item{display:flex;align-items:flex-start;gap:8px;
  font-size:0.85rem;color:var(--qds-text-light);margin-bottom:6px;}
.qds-na-rec-icon{color:var(--qds-success);flex-shrink:0;}
</style>
"""


def get_qds_css():
    """Return the lightweight QDS CSS for the Neural Analysis page."""
    return _QDS_NA_CSS


def get_qds_confidence_bar_html(label, percentage, tier_icon=""):
    """
    Render a horizontal confidence bar for a single prop.

    Args:
        label (str):      Player + prop description, e.g. "LeBron James — Over 24.5 Pts"
        percentage (float): Confidence percentage 0-100.
        tier_icon (str):  Optional emoji prefix, e.g. "💎".

    Returns:
        str: HTML string.
    """
    pct = max(0.0, min(100.0, float(percentage)))
    # Color by confidence tier
    if pct >= 80:
        color = "#00ffd5"
    elif pct >= 65:
        color = "#ffcc00"
    elif pct >= 50:
        color = "#00b4ff"
    else:
        color = "#a0b4d0"

    safe_label = _html.escape(str(label))
    safe_icon  = _html.escape(str(tier_icon)) if tier_icon else ""
    return (
        f'<div class="qds-na-conf-bar-wrap">'
        f'<div class="qds-na-conf-bar-label">'
        f'<span>{safe_icon} {safe_label}</span>'
        f'<span style="color:{color};font-weight:700;">{pct:.0f}%</span>'
        f'</div>'
        f'<div class="qds-na-conf-bar-track">'
        f'<div class="qds-na-conf-bar-fill" style="width:{pct}%;background:{color};"></div>'
        f'</div>'
        f'</div>'
    )


def get_qds_metrics_grid_html(metrics_list):
    """
    Render a 4-card metrics grid.

    Args:
        metrics_list (list[dict]): Each dict has keys:
            "label" (str), "value" (str|float), "icon" (str, optional)

    Returns:
        str: HTML string.
    """
    cards_html = ""
    for m in metrics_list:
        icon  = _html.escape(str(m.get("icon", "")))
        label = _html.escape(str(m.get("label", "")))
        value = _html.escape(str(m.get("value", "—")))
        cards_html += (
            f'<div class="qds-na-metric-card">'
            f'<div class="qds-na-metric-label">{icon} {label}</div>'
            f'<div class="qds-na-metric-value">{value}</div>'
            f'</div>'
        )
    return f'<div class="qds-na-metrics-grid">{cards_html}</div>'


def get_qds_prop_card_html(
    player_name,
    team,
    prop_text,
    score,
    tier,
    metrics,
    bonus_factors,
    player_id=None,
):
    """
    Render a full QDS prop card for a single analysis result.

    Args:
        player_name (str): Player's full name.
        team (str):        Team abbreviation, e.g. "LAL".
        prop_text (str):   Prop description, e.g. "💣 Over 24.5 Points".
        score (float):     Confidence score 0-100 (displayed as X.X / 10).
        tier (str):        "Platinum", "Gold", "Silver", or "Bronze".
        metrics (list[dict]): Metrics for the 4-card grid (see get_qds_metrics_grid_html).
        bonus_factors (list[str]): Short bonus factor strings.
        player_id (str|None): NBA player ID for headshot CDN URL.

    Returns:
        str: Self-contained HTML card string.
    """
    # ── Tier config ───────────────────────────────────────────────
    _TIER_CFG = {
        "Platinum": {
            "badge_text": "⚡ QUANTUM PICK",
            "badge_bg":   "#00ffd5",
            "badge_fg":   "#0a101f",
            "border":     "#00ffd5",
            "icon":       "💎",
        },
        "Gold": {
            "badge_text": "🔒 STRONG PICK",
            "badge_bg":   "#ffcc00",
            "badge_fg":   "#0a101f",
            "border":     "#ffcc00",
            "icon":       "🔒",
        },
        "Silver": {
            "badge_text": "✓ SAFE PICK",
            "badge_bg":   "#00b4ff",
            "badge_fg":   "#0a101f",
            "border":     "#00b4ff",
            "icon":       "✓",
        },
        "Bronze": {
            "badge_text": "★ PICK",
            "badge_bg":   "#a0b4d0",
            "badge_fg":   "#0a101f",
            "border":     "#a0b4d0",
            "icon":       "⭐",
        },
    }
    cfg = _TIER_CFG.get(tier, _TIER_CFG["Bronze"])

    # ── Player headshot ───────────────────────────────────────────
    if player_id:
        headshot_url = f"https://cdn.nba.com/headshots/nba/latest/1040x760/{player_id}.png"
    else:
        headshot_url = ""

    fallback_svg = (
        "data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' "
        "width='60' height='60' viewBox='0 0 60 60'%3E"
        "%3Ccircle cx='30' cy='30' r='30' fill='%23141a2d'/%3E"
        "%3Ccircle cx='30' cy='22' r='10' fill='%23a0b4d0'/%3E"
        "%3Cellipse cx='30' cy='50' rx='16' ry='12' fill='%23a0b4d0'/%3E"
        "%3C/svg%3E"
    )
    if headshot_url:
        img_html = (
            f'<img src="{headshot_url}" '
            f'onerror="this.onerror=null;this.src=\'{fallback_svg}\'" '
            f'style="width:60px;height:60px;border-radius:50%;object-fit:cover;'
            f'border:2px solid {cfg["border"]};flex-shrink:0;" alt="{_html.escape(player_name)}">'
        )
    else:
        img_html = (
            f'<img src="{fallback_svg}" '
            f'style="width:60px;height:60px;border-radius:50%;object-fit:cover;'
            f'border:2px solid {cfg["border"]};flex-shrink:0;" alt="{_html.escape(player_name)}">'
        )

    # ── Team badge ────────────────────────────────────────────────
    team_primary, _ = get_team_colors(team)
    team_badge_html = (
        f'<span class="qds-na-team-badge" '
        f'style="background:{team_primary};color:#fff;">'
        f'{_html.escape(str(team))}</span>'
    )

    # ── Safe / Neural Score ───────────────────────────────────────
    safe_score = round(min(10.0, float(score) / 10.0), 1)
    safe_score_str = f"{safe_score:.1f}"

    # ── Confidence bar ────────────────────────────────────────────
    conf_bar = get_qds_confidence_bar_html(
        f"{prop_text}", score, cfg["icon"]
    )

    # ── Metrics grid ─────────────────────────────────────────────
    metrics_html = get_qds_metrics_grid_html(metrics) if metrics else ""

    # ── Bonus factors ─────────────────────────────────────────────
    bonus_html = ""
    for factor in (bonus_factors or []):
        safe_factor = _html.escape(str(factor))
        bonus_html += (
            f'<div class="qds-na-bonus-item">'
            f'<span class="qds-na-bonus-icon">✓</span>'
            f'<span>{safe_factor}</span>'
            f'</div>'
        )

    # ── Build card ────────────────────────────────────────────────
    safe_player = _html.escape(str(player_name))
    safe_prop   = _html.escape(str(prop_text))
    badge_text  = _html.escape(str(cfg["badge_text"]))
    border_color = cfg["border"]
    badge_bg     = cfg["badge_bg"]
    badge_fg     = cfg["badge_fg"]

    return (
        f'<div class="qds-na-card" style="border-top-color:{border_color};">'
        # Badge
        f'<div style="display:flex;justify-content:flex-end;margin-bottom:8px;">'
        f'<span class="qds-na-badge" style="background:{badge_bg};color:{badge_fg};">'
        f'{badge_text}</span>'
        f'</div>'
        # Player row
        f'<div style="display:flex;align-items:center;gap:12px;margin-bottom:12px;">'
        f'{img_html}'
        f'<div style="flex:1;">'
        f'<div class="qds-na-player-name">{safe_player}{team_badge_html}</div>'
        f'<div class="qds-na-prop-desc">{safe_prop}</div>'
        f'</div>'
        # Score
        f'<div style="text-align:center;flex-shrink:0;">'
        f'<div style="font-size:0.65rem;color:var(--qds-text-muted);'
        f'text-transform:uppercase;letter-spacing:0.5px;">SAFE Score™</div>'
        f'<div class="qds-na-score">{safe_score_str}<span style="font-size:0.85rem;'
        f'color:var(--qds-text-muted);">/10</span></div>'
        f'</div>'
        f'</div>'
        # Confidence bar
        f'{conf_bar}'
        # Metrics grid
        f'{metrics_html}'
        # Bonus factors
        + (
            f'<div style="margin-top:10px;">'
            f'<div style="font-size:0.75rem;color:var(--qds-text-muted);'
            f'text-transform:uppercase;letter-spacing:0.5px;margin-bottom:6px;">'
            f'Bonus Factors</div>'
            f'{bonus_html}'
            f'</div>'
            if bonus_html else ""
        )
        + f'</div>'
    )


def get_qds_matchup_header_html(away_team, home_team, game_info=""):
    """
    Render a matchup header banner with team logos, names, and game info.

    Args:
        away_team (str): Away team abbreviation, e.g. "BOS".
        home_team (str): Home team abbreviation, e.g. "LAL".
        game_info (str): Additional text, e.g. date + tip-off time.

    Returns:
        str: HTML string.
    """
    ESPN_NBA = "https://a.espncdn.com/i/teamlogos/nba/500"
    away_logo = f"{ESPN_NBA}/{away_team.lower()}.png"
    home_logo = f"{ESPN_NBA}/{home_team.lower()}.png"
    away_color, _ = get_team_colors(away_team)
    home_color, _ = get_team_colors(home_team)

    safe_away      = _html.escape(str(away_team))
    safe_home      = _html.escape(str(home_team))
    safe_game_info = _html.escape(str(game_info))
    fallback       = "https://cdn.nba.com/logos/leagues/logo-nba.svg"

    return (
        f'<div class="qds-na-header">'
        f'<div style="font-size:0.72rem;color:var(--qds-text-muted);'
        f'text-transform:uppercase;letter-spacing:1px;margin-bottom:10px;">'
        f'SmartBetPro Quantum Matrix Engine 5.6 — Tonight\'s Game</div>'
        f'<div class="qds-na-matchup">'
        # Away team
        f'<div class="qds-na-team-block">'
        f'<img src="{away_logo}" onerror="this.src=\'{fallback}\'" '
        f'class="qds-na-team-logo" alt="{safe_away}">'
        f'<div class="qds-na-team-abbrev" style="color:{away_color};">{safe_away}</div>'
        f'</div>'
        # VS
        f'<div class="qds-na-vs">VS</div>'
        # Home team
        f'<div class="qds-na-team-block">'
        f'<img src="{home_logo}" onerror="this.src=\'{fallback}\'" '
        f'class="qds-na-team-logo" alt="{safe_home}">'
        f'<div class="qds-na-team-abbrev" style="color:{home_color};">{safe_home}</div>'
        f'</div>'
        f'</div>'
        + (
            f'<div style="margin-top:10px;font-size:0.8rem;color:var(--qds-text-muted);">'
            f'{safe_game_info}</div>'
            if safe_game_info else ""
        )
        + f'</div>'
    )


def get_qds_team_card_html(team_name, team_abbrev, record, stats, key_players, team_color):
    """
    Render a QDS-styled team breakdown card.

    Args:
        team_name (str):   Full team name.
        team_abbrev (str): Team abbreviation.
        record (str):      Win-loss record, e.g. "42-30".
        stats (list[dict]): List of {"label": str, "value": str} dicts.
        key_players (list[str]): Player names to highlight.
        team_color (str):  CSS color hex for the left border.

    Returns:
        str: HTML string.
    """
    safe_name   = _html.escape(str(team_name))
    safe_abbrev = _html.escape(str(team_abbrev))
    safe_record = _html.escape(str(record))
    safe_color  = _html.escape(str(team_color))

    # Stats rows
    stats_html = ""
    for s in (stats or []):
        lbl = _html.escape(str(s.get("label", "")))
        val = _html.escape(str(s.get("value", "")))
        stats_html += (
            f'<div style="display:flex;justify-content:space-between;'
            f'padding:4px 0;border-bottom:1px solid rgba(255,255,255,0.05);">'
            f'<span style="color:var(--qds-text-muted);font-size:0.8rem;">{lbl}</span>'
            f'<span style="font-weight:600;color:var(--qds-text-light);font-size:0.8rem;">{val}</span>'
            f'</div>'
        )

    # Key players badges
    players_html = ""
    for p in (key_players or []):
        safe_p = _html.escape(str(p))
        players_html += (
            f'<span style="background:rgba(0,255,213,0.1);color:var(--qds-primary);'
            f'padding:2px 8px;border-radius:4px;font-size:0.75rem;margin:2px;'
            f'display:inline-block;">{safe_p}</span>'
        )

    return (
        f'<div style="background:var(--qds-card);border-radius:10px;padding:16px;'
        f'border-left:4px solid {safe_color};margin-bottom:12px;">'
        f'<div style="display:flex;align-items:center;gap:10px;margin-bottom:12px;">'
        f'<div style="width:8px;height:8px;border-radius:50%;'
        f'background:{safe_color};"></div>'
        f'<div style="font-family:\'Orbitron\',sans-serif;font-weight:700;'
        f'font-size:0.9rem;color:var(--qds-text-light);">'
        f'{safe_name} &nbsp;<span style="color:{safe_color};">({safe_abbrev})</span></div>'
        f'<span style="margin-left:auto;background:rgba(0,0,0,0.3);padding:2px 8px;'
        f'border-radius:4px;font-size:0.75rem;color:var(--qds-text-muted);">'
        f'{safe_record}</span>'
        f'</div>'
        + (f'<div style="margin-bottom:10px;">{stats_html}</div>' if stats_html else "")
        + (
            f'<div style="margin-top:10px;">'
            f'<div style="font-size:0.7rem;color:var(--qds-text-muted);'
            f'text-transform:uppercase;margin-bottom:5px;">Key Players</div>'
            f'{players_html}'
            f'</div>'
            if players_html else ""
        )
        + f'</div>'
    )


def get_qds_strategy_table_html(entries):
    """
    Render the Entry Strategy Matrix table.

    Args:
        entries (list[dict]): Each dict has keys:
            "combo_type" (str), "picks" (str|list), "safe_avg" (float|str),
            "strategy" (str)

    Returns:
        str: HTML string with a styled table.
    """
    if not entries:
        return '<p style="color:var(--qds-text-muted);font-size:0.85rem;">No entry combinations available yet.</p>'

    rows_html = ""
    for entry in entries:
        combo     = _html.escape(str(entry.get("combo_type", "")))
        picks_raw = entry.get("picks", [])
        if isinstance(picks_raw, list):
            picks = ", ".join(_html.escape(str(p)) for p in picks_raw)
        else:
            picks = _html.escape(str(picks_raw))
        safe_avg  = _html.escape(str(entry.get("safe_avg", "")))
        strategy  = _html.escape(str(entry.get("strategy", "")))
        rows_html += (
            f'<tr>'
            f'<td><span style="color:var(--qds-primary);font-weight:600;">{combo}</span></td>'
            f'<td>{picks}</td>'
            f'<td style="text-align:center;">{safe_avg}</td>'
            f'<td style="color:var(--qds-text-muted);">{strategy}</td>'
            f'</tr>'
        )

    return (
        f'<table class="qds-na-strategy-table">'
        f'<thead><tr>'
        f'<th>Combo Type</th>'
        f'<th>Picks</th>'
        f'<th>SAFE Avg</th>'
        f'<th>Strategy</th>'
        f'</tr></thead>'
        f'<tbody>{rows_html}</tbody>'
        f'</table>'
    )


def get_qds_framework_logic_html():
    """
    Return the Framework Logic section HTML explaining the model pipeline.

    Returns:
        str: HTML string.
    """
    items = [
        ("🧮", "Monte Carlo Simulation",
         "We run 2,000+ simulated game outcomes per player, drawing from a "
         "normal distribution centered on the adjusted projection."),
        ("📐", "Projection Engine",
         "Season averages are adjusted for matchup defense ratings, home/away "
         "splits, rest days, pace, and blowout risk."),
        ("⚡", "Directional Forces",
         "Structural signals — pace edge, usage upticks, teammate absences, "
         "target-share shifts — are scored and aggregated."),
        ("🛡️", "Edge Detection",
         "We compare the model's implied probability to the book's implied "
         "probability to detect mispriced lines."),
        ("🔒", "Confidence Scoring",
         "A composite 0-100 confidence score is computed from simulation "
         "probability, edge, form, and force alignment."),
        ("⚠️", "Trap-Line & Sharpness Filters",
         "Lines set suspiciously close to the season average are penalised. "
         "Round-number traps are flagged for avoidance."),
        ("🕸️", "Layer 5 Injury Data",
         "Real-time injury status from NBA.com, RotoWire, and ESPN overrides "
         "stale nba_api designations for the most accurate availability picture."),
    ]

    rows_html = ""
    for icon, title, desc in items:
        safe_icon  = _html.escape(str(icon))
        safe_title = _html.escape(str(title))
        safe_desc  = _html.escape(str(desc))
        rows_html += (
            f'<div class="qds-na-logic-item">'
            f'<span class="qds-na-logic-icon">{safe_icon}</span>'
            f'<div>'
            f'<div class="qds-na-logic-title">{safe_title}</div>'
            f'<div class="qds-na-logic-desc">{safe_desc}</div>'
            f'</div>'
            f'</div>'
        )

    return (
        f'<div style="padding:6px 0;">{rows_html}</div>'
    )


def get_qds_final_verdict_html(summary_text, recommendations):
    """
    Render the Final Verdict section with italic summary and rec steps.

    Args:
        summary_text (str):       One or two sentence italic summary.
        recommendations (list[str]): Actionable checkmark steps.

    Returns:
        str: HTML string.
    """
    safe_summary = _html.escape(str(summary_text))

    recs_html = ""
    for rec in (recommendations or []):
        safe_rec = _html.escape(str(rec))
        recs_html += (
            f'<div class="qds-na-rec-item">'
            f'<span class="qds-na-rec-icon">✓</span>'
            f'<span>{safe_rec}</span>'
            f'</div>'
        )

    return (
        f'<div class="qds-na-verdict">"{safe_summary}"</div>'
        + (
            f'<div style="margin-top:12px;">'
            f'<div style="font-size:0.75rem;color:var(--qds-text-muted);'
            f'text-transform:uppercase;letter-spacing:0.5px;margin-bottom:8px;">'
            f'Recommended Play Strategy</div>'
            f'{recs_html}'
            f'</div>'
            if recs_html else ""
        )
    )

# ============================================================
# END SECTION: QDS Neural Analysis HTML Generators
# ============================================================


# ============================================================
# SECTION: Bet Tracker Card CSS & HTML Generators
# ============================================================

_BET_CARD_CSS = """
<style>
/* ─── Bet Card Base ───────────────────────────────────────── */
.bet-card {
    background: linear-gradient(135deg, #0d1117 0%, #0f1923 100%);
    border: 1px solid rgba(0,240,255,0.18);
    border-radius: 14px;
    padding: 18px 22px;
    margin-bottom: 14px;
    box-shadow: 0 2px 18px rgba(0,0,0,0.45);
    transition: transform 0.18s ease, box-shadow 0.18s ease;
    border-left: 4px solid rgba(0,240,255,0.35);
}
.bet-card:hover {
    transform: translateY(-2px);
    box-shadow: 0 6px 28px rgba(0,240,255,0.12);
}
.bet-card-win  {
    border-color: rgba(0,255,157,0.45);
    border-left: 4px solid #00ff9d;
    box-shadow: 0 0 20px rgba(0,255,157,0.14);
}
.bet-card-loss {
    border-color: rgba(239,68,68,0.45);
    border-left: 4px solid #ff4444;
    box-shadow: 0 0 20px rgba(239,68,68,0.14);
}
.bet-card-push {
    border-color: rgba(160,180,210,0.35);
    border-left: 4px solid #b0bec5;
}
.bet-card-pending {
    border-color: rgba(255,200,0,0.30);
    border-left: 4px solid #ffcc00;
    animation: betCardPulse 2.8s ease-in-out infinite;
}
@keyframes betCardPulse {
    0%,100% { box-shadow: 0 0 6px rgba(255,200,0,0.08); }
    50%      { box-shadow: 0 0 22px rgba(255,200,0,0.28); }
}

/* ─── Tier-specific card glows ────────────────────────────── */
.bet-card-tier-platinum { box-shadow: 0 0 22px rgba(0,240,255,0.18), 0 2px 18px rgba(0,0,0,0.45); }
.bet-card-tier-gold     { box-shadow: 0 0 22px rgba(255,170,0,0.18), 0 2px 18px rgba(0,0,0,0.45); }
.bet-card-tier-silver   { box-shadow: 0 0 14px rgba(192,208,232,0.14), 0 2px 18px rgba(0,0,0,0.45); }
.bet-card-tier-bronze   { box-shadow: 0 0 14px rgba(255,124,58,0.14), 0 2px 18px rgba(0,0,0,0.45); }

/* ─── Card Header ─────────────────────────────────────────── */
.bet-card-player {
    font-size: 1.1rem;
    font-weight: 800;
    font-family: 'Orbitron', sans-serif;
    color: #e8f0ff;
    letter-spacing: 0.04em;
}
.bet-card-team {
    font-size: 0.8rem;
    color: #8a9bb8;
    margin-left: 8px;
}
.bet-card-divider {
    height: 1px;
    background: rgba(0,240,255,0.10);
    margin: 10px 0;
}

/* ─── Direction ───────────────────────────────────────────── */
.direction-over {
    color: #00ff9d;
    font-weight: 900;
    font-size: 1.0rem;
    text-shadow: 0 0 8px rgba(0,255,157,0.5);
}
.direction-under {
    color: #ff6b6b;
    font-weight: 900;
    font-size: 1.0rem;
    text-shadow: 0 0 8px rgba(255,107,107,0.5);
}

/* ─── Confidence Bar ──────────────────────────────────────── */
.confidence-bar-wrap { margin: 10px 0 6px 0; }
.confidence-bar-track {
    height: 8px;
    background: rgba(255,255,255,0.08);
    border-radius: 4px;
    overflow: hidden;
}
.confidence-bar-fill {
    height: 100%;
    border-radius: 4px;
    transition: width 0.4s ease;
}
.confidence-bar-label {
    display: flex;
    justify-content: space-between;
    font-size: 0.75rem;
    color: #8a9bb8;
    margin-bottom: 3px;
}

/* ─── Platform Badges ─────────────────────────────────────── */
.platform-badge-pp { background: #00c853; color: #fff; padding: 2px 9px; border-radius: 5px; font-size: 0.78rem; font-weight: 700; }
.platform-badge-ud { background: #7c4dff; color: #fff; padding: 2px 9px; border-radius: 5px; font-size: 0.78rem; font-weight: 700; }
.platform-badge-dk { background: #2196f3; color: #fff; padding: 2px 9px; border-radius: 5px; font-size: 0.78rem; font-weight: 700; }

/* ─── Tier Badges (compact) ───────────────────────────────── */
.tier-badge-platinum { color: #00f0ff; font-weight: 800; text-shadow: 0 0 6px rgba(0,240,255,0.5); }
.tier-badge-gold     { color: #ffaa00; font-weight: 800; text-shadow: 0 0 6px rgba(255,170,0,0.5); }
.tier-badge-silver   { color: #c0d0e8; font-weight: 800; }
.tier-badge-bronze   { color: #ff7c3a; font-weight: 800; }
.tier-badge-avoid    { color: #ff4444; font-weight: 800; }

/* ─── Result Badges — larger & more prominent ─────────────── */
.result-win  {
    color: #fff;
    background: linear-gradient(90deg, #00c853, #00ff9d);
    font-weight: 900;
    font-size: 0.88rem;
    padding: 3px 12px;
    border-radius: 20px;
    text-shadow: none;
    box-shadow: 0 0 10px rgba(0,255,157,0.45);
    letter-spacing: 0.05em;
}
.result-loss {
    color: #fff;
    background: linear-gradient(90deg, #c62828, #ff4444);
    font-weight: 900;
    font-size: 0.88rem;
    padding: 3px 12px;
    border-radius: 20px;
    text-shadow: none;
    box-shadow: 0 0 10px rgba(255,68,68,0.45);
    letter-spacing: 0.05em;
}
.result-push {
    color: #0d1117;
    background: #b0bec5;
    font-weight: 800;
    font-size: 0.88rem;
    padding: 3px 12px;
    border-radius: 20px;
}
.result-pending {
    color: #0d1117;
    background: linear-gradient(90deg, #ff8f00, #ffcc00);
    font-weight: 800;
    font-size: 0.88rem;
    padding: 3px 12px;
    border-radius: 20px;
    animation: resultPulse 1.8s ease-in-out infinite;
}
@keyframes resultPulse {
    0%,100% { opacity: 1; box-shadow: 0 0 6px rgba(255,200,0,0.3); }
    50%      { opacity: 0.80; box-shadow: 0 0 14px rgba(255,200,0,0.7); }
}

/* ─── Projected vs Actual Comparison ─────────────────────── */
.proj-vs-actual {
    display: flex;
    gap: 12px;
    align-items: center;
    margin-top: 6px;
    font-size: 0.82rem;
}
.proj-label { color: #8a9bb8; }
.proj-value { color: #e8f0ff; font-weight: 700; }
.actual-hit { color: #00ff9d; font-weight: 700; }
.actual-miss { color: #ff6b6b; font-weight: 700; }
.actual-close { color: #ffcc00; font-weight: 700; }

/* ─── Live Status ─────────────────────────────────────────── */
.live-status-winning { color: #00ff9d; font-weight: 700; }
.live-status-losing  { color: #ff4444; font-weight: 700; }
.live-status-pending { color: #ffcc00; font-weight: 700; }
.live-status-final   { color: #8a9bb8; font-weight: 600; }
.live-status-not-started { color: #5a6880; font-weight: 600; }

/* ─── Summary Cards Row ───────────────────────────────────── */
.summary-card {
    background: linear-gradient(135deg, #0d1117, #0f1923);
    border: 1px solid rgba(0,240,255,0.18);
    border-radius: 12px;
    padding: 16px;
    text-align: center;
}
.summary-card-value {
    font-size: 1.8rem;
    font-weight: 900;
    font-family: 'Orbitron', sans-serif;
    color: #00f0ff;
    text-shadow: 0 0 12px rgba(0,240,255,0.4);
}
.summary-card-label {
    font-size: 0.75rem;
    color: #8a9bb8;
    text-transform: uppercase;
    letter-spacing: 0.06em;
    margin-top: 4px;
}

/* ─── Filter Pills ────────────────────────────────────────── */
.filter-pill {
    display: inline-block;
    padding: 5px 14px;
    border-radius: 20px;
    font-size: 0.82rem;
    font-weight: 700;
    cursor: pointer;
    border: 1px solid rgba(0,240,255,0.25);
    background: rgba(0,240,255,0.05);
    color: #8a9bb8;
    margin: 3px 4px;
    transition: all 0.18s ease;
}
.filter-pill:hover, .filter-pill-active {
    background: rgba(0,240,255,0.15);
    color: #00f0ff;
    border-color: rgba(0,240,255,0.5);
}

/* ─── Day Timeline Cards ──────────────────────────────────── */
.day-card-green {
    border-left: 4px solid #00ff9d;
    background: rgba(0,255,157,0.04);
    border-radius: 10px;
    padding: 12px 16px;
    margin-bottom: 10px;
}
.day-card-red {
    border-left: 4px solid #ff4444;
    background: rgba(255,68,68,0.04);
    border-radius: 10px;
    padding: 12px 16px;
    margin-bottom: 10px;
}
.day-card-yellow {
    border-left: 4px solid #ffcc00;
    background: rgba(255,200,0,0.04);
    border-radius: 10px;
    padding: 12px 16px;
    margin-bottom: 10px;
}
</style>
"""


def get_bet_card_css():
    """Return CSS for bet tracker cards."""
    return _BET_CARD_CSS


def get_bet_card_html(bet, show_live_status=False):
    """
    Render a single bet as a styled HTML card.

    Args:
        bet (dict): Bet record from the database.
        show_live_status (bool): Whether to show live game status.

    Returns:
        str: HTML string.
    """
    import html as _h

    player   = _h.escape(str(bet.get("player_name") or "Unknown Player"))
    team     = _h.escape(str(bet.get("team") or ""))
    stat     = _h.escape(str(bet.get("stat_type") or "").replace("_", " ").title())
    line     = bet.get("prop_line") or bet.get("line") or 0
    direction = str(bet.get("direction") or "OVER").upper()
    projected = bet.get("projected_value") or bet.get("projected") or ""
    edge_pct = bet.get("edge_percentage") or bet.get("edge") or 0
    confidence = float(bet.get("confidence_score") or 0)
    tier     = str(bet.get("tier") or "Bronze")
    platform = str(bet.get("platform") or "")
    result   = str(bet.get("result") or "").upper()
    actual   = bet.get("actual_value")
    bet_date = _h.escape(str(bet.get("bet_date") or ""))

    # Direction styling
    dir_class = "direction-over" if direction == "OVER" else "direction-under"
    dir_arrow = "↑" if direction == "OVER" else "↓"

    # Platform left-border color
    plat_lower = platform.lower()
    if "fanduel" in plat_lower or "fd" in plat_lower:
        platform_border_color = "#1456c8"
    elif "draftkings" in plat_lower or "dk" in plat_lower:
        platform_border_color = "#2196f3"
    elif "betmgm" in plat_lower or "mgm" in plat_lower:
        platform_border_color = "#c4a930"
    elif "caesars" in plat_lower:
        platform_border_color = "#00a060"
    else:
        platform_border_color = "rgba(0,240,255,0.35)"

    # Card class by result + tier glow
    tier_lower = tier.lower()
    tier_glow_class = f" bet-card-tier-{tier_lower}" if tier_lower in ("platinum", "gold", "silver", "bronze") else ""
    if result == "WIN":
        card_class = f"bet-card bet-card-win{tier_glow_class}"
        result_html = '<span class="result-win">✅ WIN</span>'
    elif result == "LOSS":
        card_class = f"bet-card bet-card-loss{tier_glow_class}"
        result_html = '<span class="result-loss">❌ LOSS</span>'
    elif result == "PUSH":
        card_class = f"bet-card bet-card-push{tier_glow_class}"
        result_html = '<span class="result-push">🔄 PUSH</span>'
    else:
        card_class = f"bet-card bet-card-pending{tier_glow_class}"
        result_html = '<span class="result-pending">⏳ PENDING</span>'

    # Override platform border color for resolved cards (keep result color as left border)
    if result == "WIN":
        platform_border_color = "#00ff9d"
    elif result == "LOSS":
        platform_border_color = "#ff4444"
    elif result == "PUSH":
        platform_border_color = "#b0bec5"
    # PENDING keeps the platform color

    # Platform badge
    if "prizepicks" in plat_lower:
        plat_html = f'<span class="platform-badge-fd">🟢 PrizePicks</span>'
    elif "underdog" in plat_lower:
        plat_html = f'<span class="platform-badge-dk">🟣 Underdog Fantasy</span>'
    elif "draftkings" in plat_lower or "pick6" in plat_lower or "dk" in plat_lower:
        plat_html = f'<span class="platform-badge-dk">🔵 DraftKings Pick6</span>'
    else:
        safe_plat = _h.escape(platform)
        plat_html = f'<span class="platform-badge">{safe_plat}</span>'

    # Tier badge
    tier_emojis = {"platinum": "💎", "gold": "🥇", "silver": "🥈", "bronze": "🥉", "avoid": "⛔"}
    tier_emoji = tier_emojis.get(tier_lower, "🏅")
    tier_html = f'<span class="tier-badge-{tier_lower}">{tier_emoji} {_h.escape(tier)}</span>'

    # Confidence bar
    conf_pct = max(0.0, min(100.0, confidence))
    if conf_pct >= 80:
        bar_color = "linear-gradient(90deg,#00ffd5,#00f0ff)"
    elif conf_pct >= 65:
        bar_color = "linear-gradient(90deg,#ff9d00,#ffcc00)"
    elif conf_pct >= 50:
        bar_color = "linear-gradient(90deg,#2196f3,#00b4ff)"
    else:
        bar_color = "linear-gradient(90deg,#5a6880,#8a9bb8)"

    conf_bar = (
        f'<div class="confidence-bar-wrap">'
        f'<div class="confidence-bar-label">'
        f'<span>Confidence</span><span style="color:#e8f0ff;font-weight:700;">{conf_pct:.0f}%</span>'
        f'</div>'
        f'<div class="confidence-bar-track">'
        f'<div class="confidence-bar-fill" style="width:{conf_pct}%;background:{bar_color};"></div>'
        f'</div>'
        f'</div>'
    )

    # Projected & edge — and visual projected vs actual comparison
    try:
        proj_float = float(projected) if projected else None
        proj_text = f"📊 Proj: <strong>{proj_float:.1f}</strong>" if proj_float else ""
    except (TypeError, ValueError):
        proj_float = None
        proj_text = ""
    try:
        edge_text = f"· Edge: <strong style='color:#00f0ff;'>+{float(edge_pct):.1f}%</strong>" if edge_pct else ""
    except (TypeError, ValueError):
        edge_text = ""

    # Projected vs Actual comparison (visual indicator)
    actual_html = ""
    if actual is not None and result in ("WIN", "LOSS", "PUSH"):
        try:
            actual_float = float(actual)
            actual_str = f"{actual_float:.1f}"
            if proj_float is not None and abs(proj_float) > 0.1:
                diff = actual_float - proj_float
                diff_pct = abs(diff / proj_float * 100)
                if diff_pct <= 10:
                    # Close to projection — neutral success indicator
                    actual_class = "actual-close"
                    diff_label = f"(±{abs(diff):.1f} — on target)"
                elif diff > 0:
                    # Exceeded projection
                    actual_class = "actual-hit"
                    diff_label = f"(+{diff:.1f} above proj)"
                else:
                    # Below projection
                    actual_class = "actual-miss"
                    diff_label = f"({diff:.1f} below proj)"
                actual_html = (
                    f'<div class="proj-vs-actual">'
                    f'<span class="proj-label">Actual:</span>'
                    f'<span class="{actual_class}">{actual_str}</span>'
                    f'<span style="color:#5a6880;font-size:0.76rem;">{_h.escape(diff_label)}</span>'
                    f'</div>'
                )
            else:
                actual_html = (
                    f'<div style="margin-top:6px;font-size:0.82rem;color:#8a9bb8;">'
                    f'Actual: <strong style="color:#e8f0ff;">{actual_str}</strong>'
                    f'</div>'
                )
        except (TypeError, ValueError):
            pass

    # Live status
    live_html = ""
    if show_live_status:
        live_status = str(bet.get("live_status") or "🕐 Not Started")
        current_val = bet.get("current_value")
        if current_val is not None:
            live_html = (
                f'<div style="margin-top:6px;font-size:0.82rem;">'
                f'Live: <strong>{current_val}</strong> · {_h.escape(live_status)}'
                f'</div>'
            )
        else:
            live_html = f'<div style="margin-top:6px;font-size:0.82rem;">{_h.escape(live_status)}</div>'

    team_display = f'<span class="bet-card-team">· {team}</span>' if team else ""
    date_display = f'<span style="font-size:0.74rem;color:#5a6880;">{bet_date}</span>' if bet_date else ""

    # Bet-type badge — show logo for historical goblin/demon bets
    bet_type = str(bet.get("bet_type") or "").lower()
    bet_type_badge_html = ""
    if bet_type == "goblin" and _os.path.exists(GOBLIN_LOGO_PATH):
        goblin_img = get_logo_img_tag(GOBLIN_LOGO_PATH, width=18, alt="Goblin")
        bet_type_badge_html = (
            f'<span style="display:inline-flex;align-items:center;gap:4px;'
            f'background:rgba(0,255,157,0.08);border:1px solid rgba(0,255,157,0.25);'
            f'border-radius:5px;padding:2px 7px;font-size:0.75rem;color:#00ff9d;">'
            f'{goblin_img} Goblin</span>'
        )
    elif bet_type == "demon" and _os.path.exists(DEMON_LOGO_PATH):
        demon_img = get_logo_img_tag(DEMON_LOGO_PATH, width=18, alt="Demon")
        bet_type_badge_html = (
            f'<span style="display:inline-flex;align-items:center;gap:4px;'
            f'background:rgba(255,94,0,0.08);border:1px solid rgba(255,94,0,0.25);'
            f'border-radius:5px;padding:2px 7px;font-size:0.75rem;color:#ff5e00;">'
            f'{demon_img} Demon</span>'
        )

    return (
        f'<div class="{card_class}" style="border-left-color:{platform_border_color};">'
        f'<div style="display:flex;justify-content:space-between;align-items:flex-start;flex-wrap:wrap;gap:6px;">'
        f'<div>'
        f'<span class="bet-card-player">🏀 {player}</span>{team_display}'
        f'</div>'
        f'<div style="display:flex;flex-direction:column;align-items:flex-end;gap:3px;">'
        f'{result_html}'
        f'{date_display}'
        f'</div>'
        f'</div>'
        f'<div class="bet-card-divider"></div>'
        f'<div style="font-size:0.9rem;color:rgba(255,255,255,0.85);">'
        f'<span class="{dir_class}">{direction} {dir_arrow}</span>'
        f' &nbsp;{stat} &nbsp;·&nbsp; Line: <strong>{line}</strong>'
        f'</div>'
        f'<div style="font-size:0.82rem;color:#8a9bb8;margin-top:4px;">'
        f'{proj_text} {edge_text}'
        f'</div>'
        f'{conf_bar}'
        f'<div style="display:flex;gap:8px;align-items:center;flex-wrap:wrap;margin-top:6px;">'
        f'{plat_html} &nbsp; {tier_html}'
        + (f' &nbsp; {bet_type_badge_html}' if bet_type_badge_html else '')
        + f'</div>'
        f'{actual_html}'
        f'{live_html}'
        f'</div>'
    )


def get_summary_cards_html(total, wins, losses, pushes, pending, win_rate, streak=0, best_platform=""):
    """Render the summary metrics row at the top of the Bet Tracker."""
    import html as _h

    streak_label = (
        f"🔥 W{streak}" if streak > 0
        else f"❄️ L{abs(streak)}" if streak < 0
        else "—"
    )
    streak_color = "#00ff9d" if streak > 0 else "#ff4444" if streak < 0 else "#8a9bb8"
    win_color = "#00ff9d" if win_rate >= 55 else "#ff4444" if win_rate < 45 else "#ffcc00"

    def _card(value, label, color="#00f0ff"):
        v = _h.escape(str(value))
        l = _h.escape(str(label))
        return (
            f'<div class="summary-card" style="flex:1;min-width:130px;">'
            f'<div class="summary-card-value" style="color:{color};">{v}</div>'
            f'<div class="summary-card-label">{l}</div>'
            f'</div>'
        )

    cards = (
        _card(f"{win_rate:.1f}%", "Win Rate", win_color)
        + _card(total, "Total Bets")
        + _card(f"✅{wins} ❌{losses}", "W / L")
        + _card(streak_label, "Streak", streak_color)
        + _card(pending, "Pending", "#ffcc00" if pending > 0 else "#8a9bb8")
    )
    if best_platform:
        cards += _card(_h.escape(best_platform), "Best Platform", "#7c4dff")

    return (
        f'<div style="display:flex;gap:10px;flex-wrap:wrap;margin-bottom:16px;">{cards}</div>'
    )


# ============================================================
# SECTION: Styled Stats Table HTML
# ============================================================

def get_styled_stats_table_html(rows, columns, title=""):
    """
    Render a list of dicts as a dark-glass styled HTML table.

    Args:
        rows (list[dict]):   Data rows — each dict maps column header → value.
        columns (list[str]): Column headers in display order.
        title (str):         Optional table title shown above the table.

    Returns:
        str: Self-contained HTML string safe for ``st.markdown(..., unsafe_allow_html=True)``.
    """
    import html as _h

    _TIER_EMOJI = {
        "platinum": "💎",
        "gold":     "🥇",
        "silver":   "🥈",
        "bronze":   "🥉",
    }

    _BET_TYPE_ICON = {
        "goblin":   get_logo_img_tag(GOBLIN_LOGO_PATH, width=16, alt="Goblin"),
        "demon":    get_logo_img_tag(DEMON_LOGO_PATH,  width=16, alt="Demon"),
        "standard": "",
        "normal":   "",
    }

    def _apply_icon(icon, text):
        """Prepend icon to text.  Returns (html_string, is_html).

        When *icon* is a trusted ``<img>`` tag, the text portion is
        HTML-escaped and the combined value is returned with ``is_html=True``
        so the caller can skip a second escape pass.  Plain-text icons are
        simply concatenated without marking the result as HTML.
        """
        if not icon:
            return text, False
        if "<img" in icon:
            return f"{icon} {_h.escape(text)}", True
        return f"{icon} {text}", False

    def _bet_type_lookup_key(raw_key: str) -> str:
        """Return the normalised key for _BET_TYPE_ICON lookup."""
        key = raw_key.lower()
        if key in _BET_TYPE_ICON:
            return key
        words = key.split()
        return words[-1] if words else ""

    def _win_rate_color(val_str):
        """Return a CSS color based on a win-rate string like '63.0%'."""
        try:
            pct = float(str(val_str).replace("%", "").strip())
            if pct >= 60:
                return "#00ff9d"
            if pct >= 50:
                return "#ffcc00"
            return "#ff4444"
        except (ValueError, TypeError):
            return "#e8f0ff"

    header_cells = "".join(
        f'<th style="padding:8px 14px;text-align:left;color:#00f0ff;'
        f'font-family:Montserrat,sans-serif;font-size:0.82rem;'
        f'text-transform:uppercase;letter-spacing:0.5px;'
        f'border-bottom:1px solid rgba(0,240,255,0.18);">'
        f'{_h.escape(str(c))}</th>'
        for c in columns
    )

    body_rows = []
    for i, row in enumerate(rows):
        row_bg = "rgba(255,255,255,0.03)" if i % 2 == 0 else "transparent"
        cells = []
        for col in columns:
            raw_val = row.get(col, "")
            display_val = str(raw_val)
            is_html = False

            # Tier column — add emoji/icon prefix
            if col.lower() == "tier":
                icon = _TIER_EMOJI.get(display_val.lower(), "")
                display_val, is_html = _apply_icon(icon, display_val)
                cell_color = "#e8f0ff"
            # Bet Type column — add logo icon prefix
            elif col.lower() == "bet type":
                icon = _BET_TYPE_ICON.get(_bet_type_lookup_key(display_val), "")
                display_val, is_html = _apply_icon(icon, display_val)
                cell_color = "#e8f0ff"
            elif "win rate" in col.lower() or "win%" in col.lower():
                cell_color = _win_rate_color(display_val)
            elif col.lower() in ("wins", "w"):
                cell_color = "#00ff9d"
            elif col.lower() in ("losses", "l"):
                cell_color = "#ff4444"
            else:
                cell_color = "rgba(255,255,255,0.85)"

            cell_content = display_val if is_html else _h.escape(display_val)
            cells.append(
                f'<td style="padding:7px 14px;color:{cell_color};'
                f'font-family:Montserrat,sans-serif;font-size:0.88rem;'
                f'border-bottom:1px solid rgba(255,255,255,0.05);">'
                f'{cell_content}</td>'
            )
        body_rows.append(
            f'<tr style="background:{row_bg};">{"".join(cells)}</tr>'
        )

    title_html = (
        f'<div style="color:#00f0ff;font-family:Orbitron,sans-serif;'
        f'font-size:0.95rem;font-weight:700;margin-bottom:8px;">'
        f'{_h.escape(title)}</div>'
        if title else ""
    )

    return (
        f'{title_html}'
        f'<div style="overflow-x:auto;border-radius:10px;'
        f'border:1px solid rgba(0,240,255,0.14);'
        f'background:linear-gradient(135deg,rgba(13,18,40,0.97),rgba(11,18,35,0.99));'
        f'box-shadow:0 0 18px rgba(0,240,255,0.07);">'
        f'<table style="width:100%;border-collapse:collapse;">'
        f'<thead><tr>{header_cells}</tr></thead>'
        f'<tbody>{"".join(body_rows)}</tbody>'
        f'</table></div>'
    )

# ============================================================
# END SECTION: Styled Stats Table HTML
# ============================================================

# ============================================================
# END SECTION: Bet Tracker Card CSS & HTML Generators
# ============================================================


# ============================================================
# SECTION: Player Intelligence CSS & HTML Helpers
# Provides CSS classes and HTML generators for the player
# intelligence strip, form dots, matchup grade badges, and
# availability badges used in Neural Analysis cards and the
# Prop Scanner Quick Analysis panel.
# ============================================================

_PLAYER_INTEL_CSS = """
<style>
/* ─── Availability Badges ─────────────────────────────── */
.avail-badge {
    display: inline-block;
    padding: 2px 8px;
    border-radius: 6px;
    font-size: 0.72rem;
    font-weight: 700;
    letter-spacing: 0.03em;
    white-space: nowrap;
}
.avail-active   { background: rgba(0,255,128,0.15); color: #00ff90; border: 1px solid rgba(0,255,128,0.35); }
.avail-gtd      { background: rgba(255,200,0,0.15);  color: #ffc800; border: 1px solid rgba(255,200,0,0.35); }
.avail-doubtful { background: rgba(255,120,0,0.15);  color: #ff8800; border: 1px solid rgba(255,120,0,0.35); }
.avail-out      { background: rgba(255,60,60,0.15);  color: #ff4444; border: 1px solid rgba(255,60,60,0.35); }

/* ─── Form Dots ───────────────────────────────────────── */
.form-dots-row {
    display: inline-flex;
    gap: 4px;
    align-items: center;
    flex-wrap: nowrap;
}
.form-dot {
    width: 14px;
    height: 14px;
    border-radius: 50%;
    display: inline-block;
    position: relative;
    flex-shrink: 0;
    cursor: default;
}
.form-dot-hit  { background: #00d084; box-shadow: 0 0 5px rgba(0,208,132,0.55); }
.form-dot-miss { background: #ff4d4d; box-shadow: 0 0 5px rgba(255,77,77,0.45); }
.form-dot-na   { background: #3a4560; }
.form-label-hot     { color: #ff7b2e; font-weight: 700; font-size: 0.78rem; }
.form-label-cold    { color: #5bc8f5; font-weight: 700; font-size: 0.78rem; }
.form-label-neutral { color: #8a9bb8; font-weight: 600; font-size: 0.78rem; }

/* ─── Matchup Grade Badges ────────────────────────────── */
.grade-badge {
    display: inline-block;
    width: 28px;
    height: 28px;
    line-height: 28px;
    text-align: center;
    border-radius: 6px;
    font-size: 0.9rem;
    font-weight: 800;
    letter-spacing: 0;
}
.grade-a  { background: rgba(0,255,128,0.18); color: #00e57a; border: 1px solid rgba(0,255,128,0.40); }
.grade-b  { background: rgba(0,200,255,0.14); color: #00c8ff; border: 1px solid rgba(0,200,255,0.35); }
.grade-c  { background: rgba(255,200,0,0.13); color: #e6b800; border: 1px solid rgba(255,200,0,0.30); }
.grade-d  { background: rgba(255,60,60,0.14); color: #ff5050; border: 1px solid rgba(255,60,60,0.32); }
.grade-na { background: rgba(80,90,120,0.20); color: #8a9bb8; border: 1px solid rgba(80,90,120,0.25); }

/* ─── Value Assessment Classes ────────────────────────── */
.val-great   { color: #00e57a; font-weight: 700; }
.val-good    { color: #00c8ff; font-weight: 600; }
.val-neutral { color: #8a9bb8; }

/* ─── Player Intel Strip ──────────────────────────────── */
.intel-strip {
    background: rgba(13,20,45,0.72);
    border: 1px solid rgba(0,240,255,0.10);
    border-radius: 8px;
    padding: 6px 10px;
    display: flex;
    gap: 12px;
    align-items: center;
    flex-wrap: wrap;
    margin-bottom: 6px;
}
.intel-section {
    display: flex;
    align-items: center;
    gap: 5px;
    font-size: 0.76rem;
}
.intel-label {
    color: #5a6e8a;
    font-size: 0.70rem;
    text-transform: uppercase;
    letter-spacing: 0.06em;
    margin-right: 2px;
}

/* ─── Streak Banner ───────────────────────────────────── */
.streak-banner-hot  { background: rgba(255,100,0,0.10); border-left: 3px solid #ff6420;
                      padding: 4px 10px; border-radius: 0 6px 6px 0; font-size: 0.78rem; color: #ffaa60; margin-bottom:4px; }
.streak-banner-cold { background: rgba(0,160,255,0.10); border-left: 3px solid #009fff;
                      padding: 4px 10px; border-radius: 0 6px 6px 0; font-size: 0.78rem; color: #70ceff; margin-bottom:4px; }

/* ─── Quick Analysis Panel (Prop Scanner) ─────────────── */
.qa-row {
    background: rgba(13,20,45,0.55);
    border: 1px solid rgba(0,240,255,0.09);
    border-radius: 8px;
    padding: 8px 12px;
    margin-bottom: 6px;
    display: flex;
    align-items: center;
    gap: 10px;
    flex-wrap: wrap;
}
.qa-player   { font-weight: 700; color: #e8f4ff; font-size: 0.88rem; min-width: 140px; }
.qa-stat     { color: #8a9bb8; font-size: 0.78rem; }
.qa-line     { color: #c0d0e8; font-weight: 600; font-size: 0.85rem; }
.qa-edge     { font-weight: 700; font-size: 0.82rem; }
.qa-edge-pos { color: #00e57a; }
.qa-edge-neg { color: #ff5050; }
.qa-edge-neu { color: #8a9bb8; }
</style>
"""


def get_player_intel_css() -> str:
    """Return CSS for the player intelligence UI components."""
    return _PLAYER_INTEL_CSS


def get_availability_badge_html(badge_label: str, badge_class: str, injury_note: str = "") -> str:
    """Return HTML for an availability / injury status badge.

    *badge_class* should be one of: avail-active, avail-gtd,
    avail-doubtful, avail-out.
    """
    import html as _h
    tooltip = _h.escape(injury_note) if injury_note else ""
    title_attr = f' title="{tooltip}"' if tooltip else ""
    return (
        f'<span class="avail-badge {badge_class}"{title_attr}>'
        f'{_h.escape(badge_label)}</span>'
    )


def get_form_dots_html(form_results: list, window: int = 5, prop_line: float = 0.0) -> str:
    """Return an HTML row of coloured dots representing last-N-game over/under results.

    *form_results* is the ``results`` list from
    ``engine.player_intelligence.get_recent_form_vs_line()``.
    """
    dots = []
    # Most recent game first
    for i, r in enumerate(form_results[:window]):
        css_cls = "form-dot-hit" if r.get("hit") else "form-dot-miss"
        date_str = r.get("date", "")
        val = r.get("value", "?")
        margin = r.get("margin", 0)
        sign = "+" if margin >= 0 else ""
        tooltip = f"{date_str}: {val} ({sign}{margin})"
        import html as _h
        dots.append(
            f'<span class="form-dot {css_cls}" title="{_h.escape(tooltip)}"></span>'
        )
    # Pad with grey dots if fewer games available
    for _ in range(max(0, window - len(form_results))):
        dots.append('<span class="form-dot form-dot-na" title="No data"></span>')

    return f'<span class="form-dots-row">{"".join(dots)}</span>'


def get_matchup_grade_badge_html(grade: str, label: str, css_class: str) -> str:
    """Return an HTML matchup grade badge (A / B / C / D / N/A)."""
    import html as _h
    return (
        f'<span class="grade-badge {css_class}" title="{_h.escape(label)}">'
        f'{_h.escape(grade)}</span>'
    )


def get_intel_strip_html(
    availability_html: str,
    form_html: str,
    hit_rate_pct: float,
    form_label: str,
    grade_html: str,
    edge_pct: float,
    direction: str,
    streak_label: str = "",
) -> str:
    """Return a compact player intelligence strip HTML block.

    Shows availability badge, form dots, hit-rate, matchup grade, and
    edge assessment in a single-row layout for use inside analysis cards.
    """
    form_css = (
        "form-label-hot" if "Hot" in form_label
        else "form-label-cold" if "Cold" in form_label
        else "form-label-neutral"
    )
    hit_pct_str = f"{hit_rate_pct * 100:.0f}%"

    edge_sign = "+" if edge_pct >= 0 else ""
    edge_css = "qa-edge-pos" if edge_pct >= 4 else "qa-edge-neg" if edge_pct <= -4 else "qa-edge-neu"

    streak_html = ""
    if streak_label:
        banner_cls = "streak-banner-hot" if "Over" in streak_label else "streak-banner-cold"
        import html as _h
        streak_html = f'<div class="{banner_cls}">{_h.escape(streak_label)}</div>'

    # Determine direction label from form label
    if "Hot" in form_label:
        _form_dir_label = "Over"
    elif "Cold" in form_label:
        _form_dir_label = "Under"
    else:
        _form_dir_label = "-"

    return f"""
{streak_html}
<div class="intel-strip">
  <div class="intel-section">
    <span class="intel-label">Status</span>{availability_html}
  </div>
  <div class="intel-section">
    <span class="intel-label">L{len(form_html)//20 or 5}</span>
    {form_html}
    <span class="{form_css}">{hit_pct_str} ({_form_dir_label})</span>
  </div>
  <div class="intel-section">
    <span class="intel-label">Matchup</span>{grade_html}
  </div>
  <div class="intel-section">
    <span class="intel-label">Avg Edge</span>
    <span class="qa-edge {edge_css}">{edge_sign}{edge_pct:.1f}% {direction}</span>
  </div>
</div>
"""


# ============================================================
# END SECTION: Player Intelligence CSS & HTML Helpers
# ============================================================


# ============================================================
# SECTION: Quantum Card Matrix — CSS Grid + Glassmorphic Cards
# ============================================================

QUANTUM_CARD_MATRIX_CSS = """
/* ═══════════════════════════════════════════════════════════
   QUANTUM CARD MATRIX — High-Capacity Grid Renderer
   Full Breakdown Cards with Distribution, Forces & Scores
   ═══════════════════════════════════════════════════════════ */
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;600;700&family=JetBrains+Mono:wght@400;600;700&display=swap');

.qcm-grid {
    display: grid;
    grid-template-columns: repeat(auto-fill, minmax(340px, 1fr));
    gap: 16px;
    padding: 8px 0;
    width: 100%;
}

@keyframes qcm-fade-in-up {
    from { opacity: 0; transform: translateY(18px); }
    to   { opacity: 1; transform: translateY(0); }
}

.qcm-card {
    background: rgba(11, 14, 26, 0.88);
    border: 1px solid rgba(255, 255, 255, 0.10);
    border-radius: 14px;
    padding: 18px 20px;
    backdrop-filter: blur(16px);
    -webkit-backdrop-filter: blur(16px);
    box-shadow: 0 4px 24px rgba(0, 0, 0, 0.45), 0 0 16px rgba(0, 240, 255, 0.04);
    transition: border-color 0.25s ease, box-shadow 0.25s ease, transform 0.25s ease;
    animation: qcm-fade-in-up 0.4s ease both;
    font-family: 'Inter', sans-serif;
    color: #e0eeff;
}
.qcm-card:hover {
    border-color: rgba(0, 240, 255, 0.25);
    box-shadow: 0 6px 28px rgba(0, 0, 0, 0.50), 0 0 24px rgba(0, 240, 255, 0.10);
    transform: translateY(-2px);
}

.qcm-card-header {
    display: flex;
    justify-content: space-between;
    align-items: center;
    margin-bottom: 10px;
}
.qcm-player-name {
    font-size: 1.0rem;
    font-weight: 700;
    color: #ffffff;
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
    max-width: 70%;
}
.qcm-tier-badge {
    font-size: 0.68rem;
    font-weight: 700;
    padding: 2px 8px;
    border-radius: 6px;
    text-transform: uppercase;
    letter-spacing: 0.06em;
    font-family: 'JetBrains Mono', monospace;
}
.qcm-tier-platinum { background: rgba(0, 240, 255, 0.15); color: #00f0ff; border: 1px solid rgba(0, 240, 255, 0.30); }
.qcm-tier-gold     { background: rgba(255, 215, 0, 0.15); color: #FFD700; border: 1px solid rgba(255, 215, 0, 0.30); }
.qcm-tier-silver   { background: rgba(192, 192, 192, 0.15); color: #C0C0C0; border: 1px solid rgba(192, 192, 192, 0.30); }
.qcm-tier-bronze   { background: rgba(205, 127, 50, 0.15); color: #CD7F32; border: 1px solid rgba(205, 127, 50, 0.30); }

.qcm-stat-type {
    font-size: 0.78rem;
    color: #94A3B8;
    text-transform: uppercase;
    letter-spacing: 0.08em;
    margin-bottom: 12px;
    font-family: 'JetBrains Mono', monospace;
}
.qcm-stat-type .qcm-team {
    color: #64748b;
    font-size: 0.72rem;
}
.qcm-stat-type .qcm-platform {
    color: #475569;
    font-size: 0.68rem;
}

.qcm-true-line-row {
    display: flex;
    align-items: center;
    justify-content: space-between;
    padding: 10px 12px;
    margin-bottom: 10px;
    background: rgba(0, 240, 255, 0.06);
    border: 1px solid rgba(0, 240, 255, 0.15);
    border-radius: 8px;
}
.qcm-true-line-label {
    font-size: 0.72rem;
    color: #94A3B8;
    text-transform: uppercase;
    letter-spacing: 0.08em;
    font-family: 'JetBrains Mono', monospace;
}
.qcm-true-line-value {
    font-size: 1.3rem;
    font-weight: 700;
    color: #00f0ff;
    font-family: 'JetBrains Mono', monospace;
    font-variant-numeric: tabular-nums;
}

.qcm-prediction {
    font-size: 0.80rem;
    padding: 8px 12px;
    margin-bottom: 10px;
    border-radius: 8px;
    font-family: 'JetBrains Mono', monospace;
    font-weight: 600;
    letter-spacing: 0.02em;
}
.qcm-prediction-neutral {
    background: rgba(148, 163, 184, 0.08);
    border: 1px solid rgba(148, 163, 184, 0.15);
    color: #94A3B8;
}

.qcm-metrics {
    display: flex;
    gap: 8px;
    flex-wrap: wrap;
    margin-bottom: 10px;
}
.qcm-metric {
    flex: 1;
    min-width: 60px;
    text-align: center;
    padding: 6px 4px;
    background: rgba(15, 23, 42, 0.50);
    border-radius: 6px;
    border: 1px solid rgba(255, 255, 255, 0.05);
}
.qcm-metric-val {
    font-size: 0.9rem;
    font-weight: 700;
    color: #ffffff;
    font-family: 'JetBrains Mono', monospace;
    font-variant-numeric: tabular-nums;
}
.qcm-metric-lbl {
    font-size: 0.62rem;
    color: #64748b;
    text-transform: uppercase;
    letter-spacing: 0.06em;
    margin-top: 2px;
}

/* ── Distribution Percentile Row ─────────────────────────── */
.qcm-dist-row {
    display: flex;
    gap: 4px;
    margin-bottom: 10px;
}
.qcm-dist-cell {
    flex: 1;
    text-align: center;
    padding: 5px 2px;
    background: rgba(15, 23, 42, 0.60);
    border-radius: 5px;
    border: 1px solid rgba(255, 255, 255, 0.04);
}
.qcm-dist-val {
    font-size: 0.78rem;
    font-weight: 700;
    color: #c0d0e8;
    font-family: 'JetBrains Mono', monospace;
    font-variant-numeric: tabular-nums;
}
.qcm-dist-lbl {
    font-size: 0.56rem;
    color: #64748b;
    text-transform: uppercase;
    letter-spacing: 0.06em;
    margin-top: 1px;
}
.qcm-dist-median .qcm-dist-val { color: #00f0ff; }
.qcm-dist-proj .qcm-dist-val { color: #ff5e00; }

/* ── Forces Columns ──────────────────────────────────────── */
.qcm-forces {
    display: flex;
    gap: 6px;
    margin-bottom: 10px;
}
.qcm-forces-col {
    flex: 1;
    padding: 8px 10px;
    border-radius: 6px;
    font-size: 0.72rem;
    font-family: 'JetBrains Mono', monospace;
    min-height: 40px;
}
.qcm-forces-over {
    background: rgba(0, 240, 255, 0.04);
    border: 1px solid rgba(0, 240, 255, 0.12);
}
.qcm-forces-under {
    background: rgba(255, 94, 0, 0.04);
    border: 1px solid rgba(255, 94, 0, 0.12);
}
.qcm-forces-label {
    font-weight: 700;
    font-size: 0.64rem;
    text-transform: uppercase;
    letter-spacing: 0.06em;
    margin-bottom: 4px;
    color: #94A3B8;
}
.qcm-forces-over .qcm-forces-label { color: #00f0ff; }
.qcm-forces-under .qcm-forces-label { color: #ff5e00; }

.qcm-force-item {
    color: #c0d0e8;
    margin-bottom: 2px;
    font-size: 0.68rem;
    line-height: 1.4;
}
.qcm-force-none {
    color: #475569;
    font-size: 0.68rem;
    font-style: italic;
}

/* ── Score Breakdown Bars ────────────────────────────────── */
.qcm-breakdown {
    margin-top: 2px;
}
.qcm-breakdown-row {
    display: flex;
    align-items: center;
    gap: 6px;
    margin-bottom: 5px;
    font-family: 'JetBrains Mono', monospace;
}
.qcm-breakdown-label {
    font-size: 0.62rem;
    color: #94A3B8;
    width: 65px;
    flex-shrink: 0;
    text-align: right;
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
}
.qcm-breakdown-score {
    font-size: 0.62rem;
    color: #ff5e00;
    font-weight: 600;
    width: 24px;
    flex-shrink: 0;
    text-align: right;
}
.qcm-breakdown-track {
    flex: 1;
    height: 4px;
    background: rgba(26, 32, 53, 0.80);
    border-radius: 2px;
    overflow: hidden;
}
.qcm-breakdown-fill {
    height: 4px;
    border-radius: 2px;
    transition: width 0.3s ease;
}

/* ── Player Identity Row (headshot + name + SAFE Score) ──── */
.qcm-identity {
    display: flex;
    align-items: center;
    gap: 10px;
    margin-bottom: 10px;
}
.qcm-headshot {
    width: 72px;
    height: 72px;
    border-radius: 50%;
    object-fit: cover;
    flex-shrink: 0;
    border: 2px solid rgba(0, 240, 255, 0.30);
}
.qcm-headshot-gold { border-color: #FFD700; }
.qcm-headshot-platinum { border-color: #00f0ff; }
.qcm-headshot-silver { border-color: #C0C0C0; }
.qcm-headshot-bronze { border-color: #CD7F32; }
.qcm-identity-info {
    flex: 1;
    min-width: 0;
}
.qcm-identity-name {
    font-size: 1.0rem;
    font-weight: 700;
    color: #ffffff;
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
}
.qcm-team-badge {
    display: inline-block;
    padding: 1px 6px;
    border-radius: 4px;
    font-size: 0.64rem;
    font-weight: 700;
    margin-left: 6px;
    vertical-align: middle;
    color: #fff;
}
.qcm-identity-prop {
    font-size: 0.82rem;
    font-weight: 600;
    color: #00f0ff;
    margin-top: 2px;
}
.qcm-safe-score {
    text-align: center;
    flex-shrink: 0;
    padding: 4px 8px;
}
.qcm-safe-score-label {
    font-size: 0.55rem;
    color: #64748b;
    text-transform: uppercase;
    letter-spacing: 0.5px;
    font-family: 'JetBrains Mono', monospace;
}
.qcm-safe-score-value {
    font-size: 1.3rem;
    font-weight: 700;
    color: #00f0ff;
    font-family: 'JetBrains Mono', monospace;
    text-shadow: 0 0 8px rgba(0,240,255,0.35);
    line-height: 1.1;
}
.qcm-safe-score-value span {
    font-size: 0.72rem;
    color: #64748b;
}

/* ── Confidence Bar ─────────────────────────────────────── */
.qcm-conf-bar-wrap {
    margin: 4px 0 10px;
}
.qcm-conf-bar-header {
    display: flex;
    justify-content: space-between;
    font-size: 0.68rem;
    color: #64748b;
    margin-bottom: 3px;
    font-family: 'JetBrains Mono', monospace;
}
.qcm-conf-bar-pct {
    font-weight: 700;
}
.qcm-conf-bar-track {
    height: 6px;
    background: rgba(255, 255, 255, 0.06);
    border-radius: 3px;
    overflow: hidden;
}
.qcm-conf-bar-fill {
    height: 100%;
    border-radius: 3px;
    transition: width 0.5s ease;
}

/* ── Context Metrics Grid (Situational / Matchup / Form / Edge) ── */
.qcm-context-grid {
    display: grid;
    grid-template-columns: repeat(2, 1fr);
    gap: 6px;
    margin-bottom: 10px;
}
.qcm-context-card {
    background: rgba(10, 16, 31, 0.70);
    border-radius: 6px;
    padding: 7px 8px;
    border: 1px solid rgba(0, 240, 255, 0.08);
}
.qcm-context-label {
    font-size: 0.58rem;
    color: #64748b;
    text-transform: uppercase;
    letter-spacing: 0.5px;
    margin-bottom: 2px;
}
.qcm-context-value {
    font-size: 0.72rem;
    font-weight: 600;
    color: #e0eeff;
    font-family: 'JetBrains Mono', monospace;
}

/* ── Bonus Factors ──────────────────────────────────────── */
.qcm-bonus {
    margin-top: 6px;
    padding-top: 6px;
    border-top: 1px solid rgba(255, 255, 255, 0.04);
}
.qcm-bonus-title {
    font-size: 0.58rem;
    color: #64748b;
    text-transform: uppercase;
    letter-spacing: 0.5px;
    margin-bottom: 4px;
}
.qcm-bonus-item {
    display: flex;
    align-items: flex-start;
    gap: 5px;
    font-size: 0.68rem;
    color: #c0d0e8;
    margin-bottom: 2px;
    line-height: 1.35;
}
.qcm-bonus-icon {
    color: #00ff88;
    flex-shrink: 0;
    margin-top: 1px;
}

/* ═══════════════════════════════════════════════════════════
   HORIZONTAL CARD — Best Single Bets wide layout
   ═══════════════════════════════════════════════════════════ */
.qcm-h-card {
    background: linear-gradient(135deg, rgba(11, 14, 26, 0.95), rgba(15, 22, 40, 0.90));
    border: 1px solid rgba(0, 240, 255, 0.18);
    border-radius: 14px;
    padding: 18px 22px;
    backdrop-filter: blur(16px);
    -webkit-backdrop-filter: blur(16px);
    box-shadow: 0 4px 24px rgba(0, 0, 0, 0.45),
                0 0 20px rgba(0, 240, 255, 0.06),
                inset 0 1px 0 rgba(255, 255, 255, 0.04);
    margin-bottom: 14px;
    font-family: 'Inter', sans-serif;
    color: #e0eeff;
    border-left: 4px solid var(--h-card-accent, #00f0ff);
    animation: qcm-fade-in-up 0.4s ease both;
    position: relative;
    overflow: hidden;
}
.qcm-h-card::before {
    content: '';
    position: absolute;
    top: 0;
    left: 0;
    width: 100%;
    height: 100%;
    background: linear-gradient(135deg,
        rgba(0, 240, 255, 0.03) 0%,
        transparent 40%,
        transparent 60%,
        rgba(0, 240, 255, 0.02) 100%);
    pointer-events: none;
}
.qcm-h-card:hover {
    border-color: rgba(0, 240, 255, 0.35);
    box-shadow: 0 6px 32px rgba(0, 0, 0, 0.55),
                0 0 28px rgba(0, 240, 255, 0.12),
                inset 0 1px 0 rgba(255, 255, 255, 0.06);
    transform: translateY(-1px);
    transition: all 0.25s ease;
}
/* Tier-specific accent overrides */
.qcm-h-card[data-tier="Platinum"] { --h-card-accent: #c800ff; border-left-color: #c800ff; }
.qcm-h-card[data-tier="Gold"]     { --h-card-accent: #ffd700; border-left-color: #ffd700; }
.qcm-h-card[data-tier="Silver"]   { --h-card-accent: #b0c0d8; border-left-color: #b0c0d8; }
.qcm-h-card[data-tier="Bronze"]   { --h-card-accent: #cd7f32; border-left-color: #cd7f32; }

/* Top section: identity + metrics side by side */
.qcm-h-top {
    display: flex;
    gap: 16px;
    align-items: flex-start;
    margin-bottom: 10px;
}
.qcm-h-left {
    flex: 0 0 auto;
    display: flex;
    align-items: center;
    gap: 10px;
    min-width: 200px;
}
.qcm-h-center {
    flex: 1;
    min-width: 0;
}
.qcm-h-right {
    flex: 0 0 auto;
    text-align: right;
    display: flex;
    flex-direction: column;
    align-items: flex-end;
    gap: 4px;
}

/* Horizontal metrics strip */
.qcm-h-metrics-strip {
    display: flex;
    gap: 6px;
    flex-wrap: wrap;
}
.qcm-h-metric {
    text-align: center;
    padding: 5px 10px;
    background: linear-gradient(135deg, rgba(15, 23, 42, 0.65), rgba(20, 28, 50, 0.50));
    border-radius: 6px;
    border: 1px solid rgba(0, 240, 255, 0.08);
    min-width: 50px;
    transition: border-color 0.2s ease;
}
.qcm-h-metric:hover {
    border-color: rgba(0, 240, 255, 0.22);
}

/* Probability pill badge */
.qcm-prob-pill {
    padding: 2px 8px;
    border-radius: 4px;
    font-size: 0.68rem;
    font-weight: 700;
    font-family: 'JetBrains Mono', monospace;
    color: #0a0f1a;
}

/* Horizontal bottom section: 3-column layout */
.qcm-h-bottom {
    display: flex;
    gap: 12px;
}
.qcm-h-col {
    flex: 1;
    min-width: 0;
}
.qcm-h-col-narrow {
    flex: 0 0 200px;
}

/* Responsive stacking */
@media (max-width: 900px) {
    .qcm-h-top {
        flex-direction: column;
    }
    .qcm-h-bottom {
        flex-direction: column;
    }
    .qcm-h-col-narrow {
        flex: 1;
    }
}
@media (max-width: 640px) {
    .qcm-grid {
        grid-template-columns: 1fr;
    }
    .qcm-forces {
        flex-direction: column;
    }
}
"""


def get_quantum_card_matrix_css():
    """Return the Quantum Card Matrix CSS for injection via st.markdown."""
    return f"<style>{QUANTUM_CARD_MATRIX_CSS}</style>"

# ============================================================
# END SECTION: Quantum Card Matrix CSS
# ============================================================


# ============================================================
# SECTION: Unified Expandable Player Card CSS
# PURPOSE: Combines the trading card header with all prop
#          analysis cards into one expandable <details> element.
# ============================================================

UNIFIED_PLAYER_CARD_CSS = """
/* ═══════════════════════════════════════════════════════════
   UNIFIED PLAYER CARD — Expandable per-player card
   Combines identity header with grouped prop analysis cards
   ═══════════════════════════════════════════════════════════ */
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;600;700&family=JetBrains+Mono:wght@400;600;700&display=swap');

.upc-grid {
    display: flex;
    flex-direction: column;
    gap: 14px;
    padding: 8px 0;
    width: 100%;
}

/* ── Expandable wrapper (<details>) ─────────────────────── */
.upc-card {
    background: rgba(11, 14, 26, 0.92);
    border: 1px solid rgba(255, 255, 255, 0.10);
    border-radius: 14px;
    backdrop-filter: blur(16px);
    -webkit-backdrop-filter: blur(16px);
    box-shadow: 0 4px 24px rgba(0, 0, 0, 0.45), 0 0 16px rgba(0, 240, 255, 0.04);
    transition: border-color 0.25s ease, box-shadow 0.25s ease;
    font-family: 'Inter', sans-serif;
    color: #e0eeff;
    overflow: hidden;
}
.upc-card[open] {
    border-color: rgba(0, 240, 255, 0.22);
    box-shadow: 0 6px 28px rgba(0, 0, 0, 0.50), 0 0 24px rgba(0, 240, 255, 0.10);
}

/* ── Summary (always-visible header) ────────────────────── */
.upc-card > summary {
    display: flex;
    align-items: center;
    gap: 14px;
    padding: 16px 20px;
    cursor: pointer;
    list-style: none;
    user-select: none;
    transition: background 0.18s ease;
}
.upc-card > summary::-webkit-details-marker { display: none; }
.upc-card > summary::marker { display: none; content: ''; }
.upc-card > summary:hover {
    background: rgba(0, 240, 255, 0.04);
}

/* Headshot */
.upc-headshot {
    width: 80px;
    height: 80px;
    border-radius: 50%;
    border: 2px solid rgba(0, 198, 255, 0.35);
    object-fit: cover;
    flex-shrink: 0;
}

/* Identity block */
.upc-identity {
    flex: 1;
    min-width: 0;
}
.upc-player-name {
    font-size: 1.05rem;
    font-weight: 700;
    color: #ffffff;
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
}
.upc-team-badge {
    display: inline-block;
    padding: 1px 6px;
    border-radius: 4px;
    font-size: 0.64rem;
    font-weight: 700;
    margin-left: 6px;
    vertical-align: middle;
    color: #fff;
}
.upc-subtitle {
    font-family: 'JetBrains Mono', monospace;
    font-size: 0.72rem;
    color: #94A3B8;
    margin-top: 2px;
}

/* Stat pills row */
.upc-stats {
    display: flex;
    gap: 6px;
    margin-top: 4px;
    flex-wrap: wrap;
}
.upc-stat-pill {
    font-family: 'JetBrains Mono', monospace;
    font-size: 0.66rem;
    font-variant-numeric: tabular-nums;
    color: #00C6FF;
    background: rgba(0, 198, 255, 0.08);
    border: 1px solid rgba(0, 198, 255, 0.18);
    border-radius: 6px;
    padding: 2px 7px;
}

/* Right-side summary info */
.upc-summary-right {
    display: flex;
    align-items: center;
    gap: 12px;
    flex-shrink: 0;
}
.upc-prop-count {
    font-family: 'JetBrains Mono', monospace;
    font-size: 0.72rem;
    color: #00f0ff;
    background: rgba(0, 240, 255, 0.08);
    border: 1px solid rgba(0, 240, 255, 0.20);
    border-radius: 6px;
    padding: 4px 10px;
    font-weight: 600;
}
.upc-chevron {
    font-size: 1.1rem;
    color: #64748b;
    transition: transform 0.25s ease;
    flex-shrink: 0;
}
.upc-card[open] .upc-chevron {
    transform: rotate(180deg);
    color: #00f0ff;
}

/* ── Expanded body ──────────────────────────────────────── */
.upc-body {
    padding: 0 20px 18px;
    border-top: 1px solid rgba(255, 255, 255, 0.06);
}
.upc-body .qcm-grid {
    padding-top: 14px;
}

/* ── Joseph M Smith avatar row inside expanded card ────── */
.upc-joseph-row {
    display: flex;
    align-items: center;
    gap: 10px;
    margin-top: 14px;
    padding: 10px 14px;
    background: linear-gradient(135deg, rgba(255, 94, 0, 0.08), rgba(255, 158, 0, 0.04));
    border: 1px solid rgba(255, 94, 0, 0.25);
    border-radius: 10px;
    cursor: pointer;
    transition: border-color 0.2s ease, box-shadow 0.2s ease;
}
.upc-joseph-row:hover {
    border-color: rgba(255, 94, 0, 0.45);
    box-shadow: 0 0 12px rgba(255, 94, 0, 0.12);
}
.upc-joseph-avatar {
    width: 36px;
    height: 36px;
    border-radius: 50%;
    border: 2px solid #ff5e00;
    object-fit: cover;
    flex-shrink: 0;
}
.upc-joseph-label {
    color: #ff9e00;
    font-size: 0.82rem;
    font-weight: 600;
    font-family: 'Inter', sans-serif;
    letter-spacing: 0.02em;
}

/* ── Joseph M Smith response panel (toggled on click) ──── */
.upc-joseph-response {
    margin-top: 10px;
    padding: 14px 16px;
    background: linear-gradient(135deg, rgba(255, 94, 0, 0.06), rgba(15, 23, 42, 0.9));
    border: 1px solid rgba(255, 94, 0, 0.3);
    border-radius: 10px;
    animation: josephFadeIn 0.3s ease-out;
}
@keyframes josephFadeIn {
    from { opacity: 0; transform: translateY(-6px); }
    to   { opacity: 1; transform: translateY(0); }
}
.upc-joseph-resp-header {
    display: flex;
    align-items: center;
    gap: 10px;
    margin-bottom: 10px;
}
.upc-joseph-resp-avatar {
    width: 44px;
    height: 44px;
    border-radius: 50%;
    border: 2px solid #ff5e00;
    object-fit: cover;
    flex-shrink: 0;
    box-shadow: 0 0 10px rgba(255, 94, 0, 0.3);
}
.upc-joseph-resp-title {
    display: flex;
    flex-direction: column;
}
.upc-joseph-resp-name {
    color: #ff9e00;
    font-size: 0.88rem;
    font-weight: 700;
    font-family: 'Orbitron', monospace, sans-serif;
    letter-spacing: 0.5px;
}
.upc-joseph-resp-role {
    color: #64748b;
    font-size: 0.7rem;
    font-weight: 500;
}
.upc-joseph-resp-lock {
    color: #facc15;
    font-size: 0.92rem;
    font-weight: 800;
    font-family: 'Orbitron', monospace, sans-serif;
    letter-spacing: 0.8px;
    margin-bottom: 8px;
    text-shadow: 0 0 8px rgba(250, 204, 21, 0.25);
}
.upc-joseph-resp-rant {
    color: #e2e8f0;
    font-size: 0.84rem;
    line-height: 1.65;
    font-family: 'Montserrat', 'Inter', sans-serif;
}

/* ── Responsive ─────────────────────────────────────────── */
@media (max-width: 640px) {
    .upc-card > summary {
        flex-wrap: wrap;
        gap: 10px;
        padding: 12px 14px;
    }
    .upc-headshot {
        width: 60px;
        height: 60px;
    }
    .upc-summary-right {
        width: 100%;
        justify-content: flex-end;
    }
}
"""


def get_unified_player_card_css():
    """Return the Unified Player Card CSS for injection via st.markdown."""
    return f"<style>{UNIFIED_PLAYER_CARD_CSS}</style>"


# ============================================================
# END SECTION: Unified Expandable Player Card CSS
# ============================================================


# ============================================================
# SECTION: Glassmorphic Dark Theme — Trading Card & Modal CSS
# PURPOSE: Obsidian/Deep Space backgrounds, neon accents,
#          Inter + JetBrains Mono typography, and glassmorphic
#          card/modal styles for the Player Spotlight system.
# ============================================================

GLASSMORPHIC_CARD_CSS = """
/* ── Glassmorphic Dark-Theme Variables ───────────────────── */
:root {
  --gm-bg-deep: #070A13;
  --gm-bg-card: rgba(15, 23, 42, 0.6);
  --gm-border: rgba(255, 255, 255, 0.1);
  --gm-accent-blue: #00C6FF;
  --gm-accent-red: #FF0055;
  --gm-text-primary: #E2E8F0;
  --gm-text-muted: #94A3B8;
  --gm-font-body: 'Inter', sans-serif;
  --gm-font-mono: 'JetBrains Mono', monospace;
}

/* ── Google-Font import for Inter ────────────────────────── */
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;600;700&display=swap');

/* ── Trading-Card Grid ───────────────────────────────────── */
.gm-card-grid {
  display: grid;
  grid-template-columns: repeat(auto-fill, minmax(230px, 1fr));
  gap: 18px;
  padding: 12px 0;
}

/* ── Individual Trading Card ─────────────────────────────── */
.gm-player-card {
  background: var(--gm-bg-card);
  backdrop-filter: blur(10px);
  -webkit-backdrop-filter: blur(10px);
  border: 1px solid var(--gm-border);
  border-radius: 12px;
  padding: 18px 16px;
  cursor: pointer;
  transition: transform 0.18s ease, box-shadow 0.18s ease;
  position: relative;
  overflow: hidden;
}
.gm-player-card:hover {
  transform: translateY(-4px);
  box-shadow: 0 0 18px rgba(0, 198, 255, 0.25);
  border-color: var(--gm-accent-blue);
}

/* ── Card headshot ───────────────────────────────────────── */
.gm-card-headshot {
  width: 72px;
  height: 72px;
  border-radius: 50%;
  border: 2px solid var(--gm-accent-blue);
  object-fit: cover;
  margin: 0 auto 10px;
  display: block;
}

/* ── Card player name ────────────────────────────────────── */
.gm-card-name {
  font-family: var(--gm-font-body);
  font-size: 1rem;
  font-weight: 700;
  color: var(--gm-text-primary);
  text-align: center;
  margin-bottom: 2px;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}

/* ── Card subtitle (Position · Team · Opponent) ──────────── */
.gm-card-sub {
  font-family: var(--gm-font-mono);
  font-size: 0.72rem;
  color: var(--gm-text-muted);
  text-align: center;
  margin-bottom: 10px;
}

/* ── Mini stat pills row ─────────────────────────────────── */
.gm-card-stats {
  display: flex;
  justify-content: center;
  gap: 8px;
  margin-bottom: 10px;
}
.gm-stat-pill {
  font-family: var(--gm-font-mono);
  font-size: 0.68rem;
  font-variant-numeric: tabular-nums;
  color: var(--gm-accent-blue);
  background: rgba(0, 198, 255, 0.08);
  border: 1px solid rgba(0, 198, 255, 0.18);
  border-radius: 6px;
  padding: 2px 8px;
}

/* ── Prop count badge ────────────────────────────────────── */
.gm-card-prop-count {
  font-family: var(--gm-font-mono);
  font-size: 0.68rem;
  color: var(--gm-text-muted);
  text-align: center;
}

/* ── Modal / Dialog overrides ────────────────────────────── */
div[data-testid="stDialog"] > div {
  background: rgba(15, 23, 42, 0.92) !important;
  backdrop-filter: blur(14px) !important;
  -webkit-backdrop-filter: blur(14px) !important;
  border: 1px solid rgba(255, 255, 255, 0.1) !important;
  border-radius: 12px !important;
}

/* ── Modal Vitals Row ────────────────────────────────────── */
.gm-modal-vitals {
  display: flex;
  gap: 20px;
  align-items: flex-start;
  margin-bottom: 20px;
}
.gm-modal-headshot {
  width: 120px;
  height: 120px;
  border-radius: 50%;
  border: 3px solid var(--gm-accent-blue);
  object-fit: cover;
  flex-shrink: 0;
}
.gm-modal-info h2 {
  font-family: var(--gm-font-body);
  color: var(--gm-text-primary);
  margin: 0 0 4px;
}
.gm-modal-info p {
  font-family: var(--gm-font-mono);
  color: var(--gm-text-muted);
  font-size: 0.82rem;
  margin: 0;
}

/* ── Season-Stats Metric Bar ─────────────────────────────── */
.gm-season-bar {
  display: flex;
  gap: 14px;
  margin: 14px 0 20px;
  flex-wrap: wrap;
}
.gm-season-metric {
  text-align: center;
  min-width: 64px;
}
.gm-season-metric .val {
  font-family: var(--gm-font-mono);
  font-variant-numeric: tabular-nums;
  font-size: 1.4rem;
  font-weight: 700;
  color: var(--gm-accent-blue);
}
.gm-season-metric .lbl {
  font-family: var(--gm-font-body);
  font-size: 0.68rem;
  color: var(--gm-text-muted);
  text-transform: uppercase;
  letter-spacing: 0.04em;
}

/* ── Market Grid (bet rows) ──────────────────────────────── */
.gm-market-grid {
  display: grid;
  grid-template-columns: repeat(auto-fill, minmax(220px, 1fr));
  gap: 10px;
  margin-bottom: 18px;
}
.gm-market-cell {
  background: rgba(15, 23, 42, 0.45);
  border: 1px solid var(--gm-border);
  border-radius: 8px;
  padding: 10px 12px;
  font-family: var(--gm-font-mono);
  font-size: 0.78rem;
  color: var(--gm-text-primary);
}
.gm-market-cell .stat-label {
  font-weight: 700;
  color: var(--gm-accent-blue);
  margin-bottom: 4px;
}
.gm-market-cell .edge-pos {
  color: #4ADE80;
}
.gm-market-cell .edge-neg {
  color: var(--gm-accent-red);
}

/* ── Ask Joseph CTA button ───────────────────────────────── */
.gm-ask-joseph-btn button {
  width: 100%;
  background: linear-gradient(135deg, #FF5E00 0%, #FF0055 100%) !important;
  color: #fff !important;
  font-family: 'Orbitron', sans-serif !important;
  font-size: 1rem !important;
  font-weight: 700 !important;
  border: none !important;
  border-radius: 8px !important;
  padding: 12px 0 !important;
  box-shadow: 0 0 20px rgba(255, 94, 0, 0.35);
  transition: box-shadow 0.2s ease;
}
.gm-ask-joseph-btn button:hover {
  box-shadow: 0 0 28px rgba(255, 0, 85, 0.55);
}

/* ── Joseph Broadcast Container ──────────────────────────── */
.gm-joseph-response {
  background: rgba(255, 94, 0, 0.06);
  border: 1px solid rgba(255, 94, 0, 0.30);
  border-radius: 10px;
  padding: 16px;
  margin-top: 14px;
}
.gm-joseph-response .gm-joseph-avatar {
  width: 48px;
  height: 48px;
  border-radius: 50%;
  border: 2px solid #FF5E00;
  object-fit: cover;
  float: left;
  margin-right: 12px;
}
.gm-joseph-response .gm-joseph-lock {
  font-family: 'Orbitron', sans-serif;
  font-size: 0.92rem;
  font-weight: 700;
  color: #FF5E00;
  margin-bottom: 6px;
}
.gm-joseph-response .gm-joseph-rant {
  font-family: var(--gm-font-body);
  font-size: 0.85rem;
  color: var(--gm-text-primary);
  line-height: 1.6;
}
"""


def get_glassmorphic_card_css():
    """Return the Glassmorphic Trading-Card CSS for injection."""
    return f"<style>{GLASSMORPHIC_CARD_CSS}</style>"


def get_player_trading_card_html(
    player_name: str,
    headshot_url: str = "",
    position: str = "N/A",
    team: str = "N/A",
    opponent: str = "TBD",
    season_stats: dict | None = None,
    prop_count: int = 0,
) -> str:
    """Build an HTML Trading Card for one player.

    Parameters
    ----------
    player_name : str
        Display name.
    headshot_url : str
        URL to headshot image.
    position, team, opponent : str
        Player metadata.
    season_stats : dict | None
        ``{"ppg": float, "rpg": float, "apg": float, "avg_minutes": float}``
    prop_count : int
        Number of available props for badge.

    Returns
    -------
    str
        HTML string for one trading card.
    """
    stats = season_stats or {}
    safe_name = _html.escape(str(player_name))
    safe_pos = _html.escape(str(position))
    safe_team = _html.escape(str(team))
    safe_opp = _html.escape(str(opponent))
    safe_url = _html.escape(str(headshot_url))

    ppg = stats.get("ppg", 0.0)
    rpg = stats.get("rpg", 0.0)
    apg = stats.get("apg", 0.0)

    return (
        f'<div class="gm-player-card">'
        f'<img class="gm-card-headshot" src="{safe_url}" alt="{safe_name}" '
        f'onerror="this.src=\'https://cdn.nba.com/headshots/nba/latest/1040x760/fallback.png\'">'
        f'<div class="gm-card-name">{safe_name}</div>'
        f'<div class="gm-card-sub">{safe_pos} · {safe_team} vs {safe_opp}</div>'
        f'<div class="gm-card-stats">'
        f'<span class="gm-stat-pill">{ppg} PPG</span>'
        f'<span class="gm-stat-pill">{rpg} RPG</span>'
        f'<span class="gm-stat-pill">{apg} APG</span>'
        f'</div>'
        f'<div class="gm-card-prop-count">{prop_count} prop{"s" if prop_count != 1 else ""} available</div>'
        f'</div>'
    )


# ============================================================
# END SECTION: Glassmorphic Dark Theme
# ============================================================
