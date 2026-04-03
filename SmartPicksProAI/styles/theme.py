# ============================================================
# FILE: styles/theme.py
# PURPOSE: All CSS/HTML generators for the Smart Pick Pro AI UI.
#          Provides the "Quantum Edge" dark theme matching
#          SmartAI-NBA — glassmorphism cards, neon cyan/green
#          glow, Orbitron headings, JetBrains Mono data.
# ============================================================

import os
import base64

# ── Asset loader ────────────────────────────────────────────

_ASSETS_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "assets")


def _load_image_b64(filename: str) -> str:
    """Return base64-encoded string for an asset image."""
    path = os.path.join(_ASSETS_DIR, filename)
    if not os.path.isfile(path):
        return ""
    try:
        with open(path, "rb") as f:
            return base64.b64encode(f.read()).decode()
    except Exception:
        return ""


# ── Global CSS ──────────────────────────────────────────────

def get_global_css() -> str:
    """Return full <style> block for the Quantum Edge dark theme."""
    return """
<style>
/* ── Fonts ────────────────────────────────────────────────── */
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800;900&family=Orbitron:wght@400;700;800;900&family=JetBrains+Mono:wght@400;500;600;700&display=swap');

/* ── Keyframes ────────────────────────────────────────────── */
@keyframes borderGlow {
    0%, 100% { box-shadow: 0 0 12px rgba(0,240,255,0.15), 0 4px 24px rgba(0,240,255,0.07); }
    50%       { box-shadow: 0 0 28px rgba(0,240,255,0.35), 0 4px 30px rgba(0,240,255,0.15); }
}
@keyframes pulse-platinum {
    0%, 100% { box-shadow: 0 0 10px rgba(0,240,255,0.30); }
    50%       { box-shadow: 0 0 24px rgba(0,240,255,0.60); }
}
@keyframes pulse-gold {
    0%, 100% { box-shadow: 0 0 10px rgba(255,94,0,0.35); }
    50%       { box-shadow: 0 0 24px rgba(255,94,0,0.65); }
}
@keyframes headerShimmer {
    0%   { background-position: 0% 50%; }
    50%  { background-position: 100% 50%; }
    100% { background-position: 0% 50%; }
}
@keyframes fadeInUp {
    from { opacity: 0; transform: translateY(10px); }
    to   { opacity: 1; transform: translateY(0); }
}

/* ── Chrome Obliteration ──────────────────────────────────── */
#MainMenu { visibility: hidden !important; }
header[data-testid="stHeader"] { display: none !important; }
footer { display: none !important; }
.stDeployButton { display: none !important; }
.block-container { padding-top: 1rem !important; max-width: 1400px; }

/* ── Base / Body ──────────────────────────────────────────── */
html, body, [class*="css"] {
    font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
    font-size: 16px;
    color: #c8d8f0;
    background-color: #070A13;
}
.stApp {
    background-color: #070A13;
    background-image:
        radial-gradient(ellipse at 20% 20%, rgba(0,240,255,0.04) 0%, transparent 50%),
        radial-gradient(ellipse at 80% 80%, rgba(200,0,255,0.03) 0%, transparent 50%),
        radial-gradient(ellipse at center, #0d1220 0%, #070A13 100%);
}

/* ── Typography ───────────────────────────────────────────── */
h1, h2, h3, h4, h5, h6 {
    color: #00f0ff !important;
    font-family: 'Orbitron', sans-serif !important;
    letter-spacing: 0.05em;
}
h1 {
    background: linear-gradient(135deg, #00f0ff, #00ff9d);
    -webkit-background-clip: text;
    -webkit-text-fill-color: transparent;
    font-weight: 800;
}
h4, h5, h6 { color: #94A3B8 !important; }

/* ── Sidebar ──────────────────────────────────────────────── */
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
[data-testid="stSidebar"] a { color: #c0d0e8 !important; }

/* ── Metric cards (glassmorphic) ──────────────────────────── */
[data-testid="stMetric"] {
    background: rgba(15,23,42,0.55);
    border: 1px solid rgba(255,255,255,0.05);
    border-radius: 14px;
    padding: 18px 20px;
    backdrop-filter: blur(12px);
    box-shadow: 0 0 20px rgba(0,240,255,0.04), 0 4px 20px rgba(0,0,0,0.30);
    transition: border-color 0.25s ease, box-shadow 0.25s ease, transform 0.25s ease;
}
[data-testid="stMetric"]:hover {
    border-color: rgba(0,240,255,0.20);
    box-shadow: 0 0 28px rgba(0,240,255,0.10), 0 6px 24px rgba(0,0,0,0.40);
    transform: translateY(-3px);
}
[data-testid="stMetricValue"] {
    color: rgba(255,255,255,0.95) !important;
    font-size: 1.4rem !important;
    font-family: 'JetBrains Mono', monospace !important;
}
[data-testid="stMetricLabel"] {
    color: #94A3B8 !important;
    font-size: 0.82rem !important;
    text-transform: uppercase;
    letter-spacing: 0.08em;
}

/* ── Glass cards ──────────────────────────────────────────── */
.glass-card {
    background: rgba(15,23,42,0.50);
    backdrop-filter: blur(24px);
    border: 1px solid rgba(255,255,255,0.06);
    border-radius: 16px;
    padding: 1.2rem 1.4rem;
    margin-bottom: 0.8rem;
    box-shadow: 0 0 40px rgba(0,240,255,0.06), 0 8px 32px rgba(0,0,0,0.45);
    transition: all 0.25s ease;
}
.glass-card:hover {
    border-color: rgba(0,240,255,0.25);
    box-shadow: 0 0 50px rgba(0,240,255,0.12);
    transform: translateY(-2px);
}

/* ── Game tiles ───────────────────────────────────────────── */
.game-tile {
    background: rgba(15,23,42,0.55);
    border: 1px solid rgba(0,240,255,0.08);
    border-radius: 16px;
    padding: 1.4rem;
    text-align: center;
    transition: all 0.3s ease;
    position: relative;
    overflow: hidden;
}
.game-tile::before {
    content: '';
    position: absolute;
    top: 0; left: 0; right: 0; height: 2px;
    background: linear-gradient(90deg, #00f0ff, #00ff9d, #FFD700, #c800ff, #00f0ff);
    background-size: 200% 100%;
    animation: headerShimmer 4s ease infinite;
    opacity: 0;
    transition: opacity 0.3s ease;
}
.game-tile:hover::before { opacity: 1; }
.game-tile:hover {
    border-color: rgba(0,240,255,0.30);
    box-shadow: 0 12px 40px rgba(0,240,255,0.08);
    transform: translateY(-3px);
}
.game-tile .teams { font-size: 1.15rem; font-weight: 700; color: #fff; }
.game-tile .vs { color: #00f0ff; margin: 0 0.4rem; }
.game-tile .score { font-size: 1.5rem; font-weight: 800; color: #00f0ff; margin: 0.4rem 0; }
.game-tile .meta { font-size: 0.7rem; color: rgba(255,255,255,0.35); margin-top: 0.5rem; }

/* ── Buttons ──────────────────────────────────────────────── */
button[kind="primary"] {
    background: linear-gradient(135deg, #00ffd5, #00b4ff) !important;
    color: #070A13 !important;
    border: none !important;
    font-weight: 700 !important;
    box-shadow: 0 0 16px rgba(0,255,213,0.30) !important;
}
button[kind="primary"]:hover {
    transform: translateY(-2px) !important;
    box-shadow: 0 0 28px rgba(0,255,213,0.50), 0 6px 20px rgba(0,0,0,0.4) !important;
}
.stButton > button {
    background: rgba(0,240,255,0.08);
    color: #c8d8f0;
    border: 1px solid rgba(0,240,255,0.20);
    border-radius: 10px;
    font-weight: 600;
    transition: all 0.25s ease;
}
.stButton > button:hover {
    background: rgba(0,240,255,0.15);
    border-color: rgba(0,240,255,0.40);
    box-shadow: 0 6px 24px rgba(0,240,255,0.12);
    transform: translateY(-1px);
}

/* ── Tabs ─────────────────────────────────────────────────── */
.stTabs [data-baseweb="tab-list"] {
    gap: 4px; background: rgba(15,23,42,0.3); border-radius: 12px; padding: 4px;
}
.stTabs [data-baseweb="tab"] {
    background: transparent; border-radius: 8px; color: rgba(255,255,255,0.45);
    font-weight: 500; padding: 0.5rem 1.2rem; transition: all 0.2s ease;
}
.stTabs [data-baseweb="tab"]:hover {
    color: rgba(255,255,255,0.7); background: rgba(0,240,255,0.06);
}
.stTabs [aria-selected="true"] {
    background: rgba(0,240,255,0.12) !important;
    color: #00f0ff !important;
    font-weight: 600;
    border-bottom: 2px solid #00f0ff;
}

/* ── Data tables ──────────────────────────────────────────── */
.stDataFrame { font-size: 0.82rem; }
.stDataFrame [data-testid="stDataFrameResizable"] {
    border: 1px solid rgba(0,240,255,0.08); border-radius: 12px; overflow: hidden;
}
[data-testid="stDataFrame"] td {
    font-family: 'JetBrains Mono', monospace !important;
    color: #e0eeff !important;
}
[data-testid="stDataFrame"] th {
    font-size: 0.75rem !important;
    color: #94A3B8 !important;
    text-transform: uppercase !important;
    letter-spacing: 1px !important;
    background: rgba(7,10,19,0.90) !important;
}

/* ── Section headers ──────────────────────────────────────── */
.section-hdr {
    font-size: 0.78rem; font-weight: 700;
    color: rgba(0,240,255,0.65);
    text-transform: uppercase; letter-spacing: 0.12em;
    margin: 1.5rem 0 0.6rem 0; padding-bottom: 0.4rem;
    border-bottom: 1px solid rgba(0,240,255,0.10);
    font-family: 'Orbitron', sans-serif;
}

/* ── Empty state ──────────────────────────────────────────── */
.empty-state {
    text-align: center; color: rgba(255,255,255,0.3);
    padding: 3rem; font-style: italic; font-size: 0.9rem;
}

/* ── Divider ──────────────────────────────────────────────── */
hr { border-color: rgba(0,240,255,0.08) !important; }

/* ── Scrollbar ────────────────────────────────────────────── */
::-webkit-scrollbar { width: 4px; height: 4px; }
::-webkit-scrollbar-track { background: rgba(7,10,19,0.9); }
::-webkit-scrollbar-thumb { background: rgba(0,240,255,0.30); border-radius: 2px; }
::-webkit-scrollbar-thumb:hover { background: rgba(0,240,255,0.55); }

/* ── Expanders ────────────────────────────────────────────── */
.stExpander {
    background: rgba(13,18,32,0.80) !important;
    border: 1px solid rgba(0,240,255,0.12) !important;
    border-radius: 12px !important;
}

/* ── Tier badges ──────────────────────────────────────────── */
.tier-platinum {
    background: linear-gradient(135deg, rgba(0,240,255,0.12), rgba(0,255,157,0.08));
    border: 1px solid rgba(0,240,255,0.35); color: #00f0ff;
    padding: 4px 12px; border-radius: 8px; font-weight: 700;
    font-size: 0.78rem; text-transform: uppercase;
    animation: pulse-platinum 2.5s ease-in-out infinite;
}
.tier-gold {
    background: linear-gradient(135deg, rgba(255,94,0,0.12), rgba(255,215,0,0.08));
    border: 1px solid rgba(255,94,0,0.35); color: #ff5e00;
    padding: 4px 12px; border-radius: 8px; font-weight: 700;
    font-size: 0.78rem; text-transform: uppercase;
    animation: pulse-gold 2.5s ease-in-out infinite;
}
.tier-silver {
    background: rgba(148,163,184,0.10); border: 1px solid rgba(148,163,184,0.25);
    color: #94A3B8; padding: 4px 12px; border-radius: 8px;
    font-weight: 700; font-size: 0.78rem; text-transform: uppercase;
}
.tier-bronze {
    background: rgba(180,130,70,0.10); border: 1px solid rgba(180,130,70,0.25);
    color: #B48246; padding: 4px 12px; border-radius: 8px;
    font-weight: 700; font-size: 0.78rem; text-transform: uppercase;
}

/* ── Hero HUD ─────────────────────────────────────────────── */
.hero-hud {
    background: rgba(15,23,42,0.50);
    backdrop-filter: blur(24px);
    border: 1px solid rgba(255,255,255,0.06);
    border-radius: 16px; padding: 32px 40px; margin-bottom: 24px;
    box-shadow: 0 0 40px rgba(0,240,255,0.06), 0 8px 32px rgba(0,0,0,0.45);
    position: relative; overflow: hidden;
    display: flex; align-items: center; gap: 28px;
}
.hero-hud::before {
    content: ''; position: absolute;
    top: 0; left: 0; right: 0; height: 2px;
    background: linear-gradient(90deg, #00f0ff, #00ff9d, #FFD700, #c800ff, #00f0ff);
    background-size: 200% 100%;
    animation: headerShimmer 4s ease infinite;
}
.hero-hud-text { flex: 1; min-width: 0; }
.hero-tagline {
    font-size: clamp(1.2rem, 2.5vw, 1.8rem); font-weight: 800;
    font-family: 'Orbitron', sans-serif;
    background: linear-gradient(135deg, #00f0ff, #00ff9d);
    -webkit-background-clip: text; -webkit-text-fill-color: transparent;
    letter-spacing: 0.04em; margin: 0; line-height: 1.3;
}
.hero-subtext {
    font-size: clamp(0.72rem, 1.1vw, 0.85rem); color: #94A3B8;
    font-family: 'JetBrains Mono', monospace; letter-spacing: 0.05em; margin-top: 8px;
}

/* ── Bet result badges ────────────────────────────────────── */
.badge-win { color: #00ff9d; background: rgba(0,255,157,0.10); border: 1px solid rgba(0,255,157,0.30); padding: 2px 10px; border-radius: 6px; font-weight: 700; font-size: 0.8rem; }
.badge-loss { color: #ff4444; background: rgba(255,68,68,0.10); border: 1px solid rgba(255,68,68,0.30); padding: 2px 10px; border-radius: 6px; font-weight: 700; font-size: 0.8rem; }
.badge-push { color: #f59e0b; background: rgba(245,158,11,0.10); border: 1px solid rgba(245,158,11,0.30); padding: 2px 10px; border-radius: 6px; font-weight: 700; font-size: 0.8rem; }
.badge-pending { color: #94A3B8; background: rgba(148,163,184,0.10); border: 1px solid rgba(148,163,184,0.20); padding: 2px 10px; border-radius: 6px; font-weight: 700; font-size: 0.8rem; }

/* ── Alerts ───────────────────────────────────────────────── */
.stAlert {
    background: rgba(15,23,42,0.90) !important;
    border-radius: 8px !important; border: none !important;
    color: #e0eeff !important;
}

/* ── Sidebar Engine Label ─────────────────────────────────── */
.sidebar-engine-label {
    text-align: center; font-size: 0.68rem;
    font-family: 'JetBrains Mono', monospace; font-weight: 700;
    color: rgba(0,240,255,0.70); letter-spacing: 0.08em;
    text-shadow: 0 0 8px rgba(0,240,255,0.5);
    margin-top: 12px;
}
</style>
"""


# ── HTML component generators ───────────────────────────────

def get_hero_banner_html() -> str:
    """Joseph M Smith hero banner for top of home page."""
    b64 = _load_image_b64("Joseph_M_Smith_Hero_Banner.png")
    if not b64:
        return ""
    return f"""
    <div style="text-align:center; margin-bottom:1.5rem;">
        <img src="data:image/png;base64,{b64}"
             style="max-width:100%; border-radius:16px;
                    border:1px solid rgba(0,240,255,0.10);
                    box-shadow: 0 0 30px rgba(0,240,255,0.06);"
             alt="Joseph M Smith — Smart Pick Pro">
    </div>"""


def get_sidebar_avatar_html() -> str:
    """Joseph M Smith avatar for sidebar."""
    b64 = _load_image_b64("Joseph_M_Smith_Avatar.png")
    if not b64:
        return ""
    return f"""
    <div style="text-align:center; margin-bottom:0.5rem;">
        <img src="data:image/png;base64,{b64}"
             style="width:72px; height:72px; border-radius:50%;
                    border:2px solid rgba(0,240,255,0.35);
                    box-shadow: 0 0 16px rgba(0,240,255,0.15);"
             alt="Joseph M Smith">
        <div style="font-size:0.7rem; color:#94A3B8; margin-top:4px;
                    font-family:'JetBrains Mono',monospace; letter-spacing:0.04em;">
            Joseph M Smith</div>
    </div>"""


def get_sidebar_brand_html() -> str:
    """Smart Pick Pro logo + tagline for sidebar top."""
    logo_b64 = _load_image_b64("Smart_Pick_Pro_Logo.png")
    logo_html = ""
    if logo_b64:
        logo_html = f"""
        <img src="data:image/png;base64,{logo_b64}"
             style="max-width:160px; margin-bottom:6px;"
             alt="Smart Pick Pro">"""

    return f"""
    <div style="text-align:center; padding: 0.5rem 0 0.5rem 0;">
        {logo_html}
        <div style="
            font-size:1.1rem; font-weight:800; letter-spacing:0.05em;
            font-family: 'Orbitron', sans-serif;
            background: linear-gradient(135deg, #00f0ff, #00ff9d);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
        ">SMART PICK PRO AI</div>
        <div style="font-size:0.65rem; color:rgba(148,163,184,0.6);
                    letter-spacing:0.15em; text-transform:uppercase;
                    margin-top:0.2rem;
                    font-family:'JetBrains Mono',monospace;">
            Quantum AI • Prop Intelligence
        </div>
    </div>"""


def get_summary_cards_html(
    total_bets: int,
    wins: int,
    losses: int,
    pushes: int,
    pending: int,
    win_rate: float,
    roi: float = 0.0,
) -> str:
    """Return HTML for bet-tracker summary cards."""
    return f"""
    <div style="display:flex; gap:12px; flex-wrap:wrap; margin-bottom:16px;">
        <div class="glass-card" style="flex:1; min-width:110px; text-align:center;">
            <div style="font-size:0.7rem; color:#94A3B8; text-transform:uppercase; letter-spacing:0.1em;">Total</div>
            <div style="font-size:1.4rem; font-weight:800; color:#00f0ff; font-family:'JetBrains Mono',monospace;">{total_bets}</div>
        </div>
        <div class="glass-card" style="flex:1; min-width:110px; text-align:center;">
            <div style="font-size:0.7rem; color:#94A3B8; text-transform:uppercase;">Wins</div>
            <div style="font-size:1.4rem; font-weight:800; color:#00ff9d; font-family:'JetBrains Mono',monospace;">{wins}</div>
        </div>
        <div class="glass-card" style="flex:1; min-width:110px; text-align:center;">
            <div style="font-size:0.7rem; color:#94A3B8; text-transform:uppercase;">Losses</div>
            <div style="font-size:1.4rem; font-weight:800; color:#ff4444; font-family:'JetBrains Mono',monospace;">{losses}</div>
        </div>
        <div class="glass-card" style="flex:1; min-width:110px; text-align:center;">
            <div style="font-size:0.7rem; color:#94A3B8; text-transform:uppercase;">Win Rate</div>
            <div style="font-size:1.4rem; font-weight:800; color:#00f0ff; font-family:'JetBrains Mono',monospace;">{win_rate:.1f}%</div>
        </div>
        <div class="glass-card" style="flex:1; min-width:110px; text-align:center;">
            <div style="font-size:0.7rem; color:#94A3B8; text-transform:uppercase;">Pending</div>
            <div style="font-size:1.4rem; font-weight:800; color:#f59e0b; font-family:'JetBrains Mono',monospace;">{pending}</div>
        </div>
    </div>"""


def get_tier_badge_html(tier: str) -> str:
    """Return a styled tier badge."""
    tier_lower = (tier or "bronze").lower()
    emoji_map = {"platinum": "💎", "gold": "🥇", "silver": "🥈", "bronze": "🥉"}
    emoji = emoji_map.get(tier_lower, "🥉")
    return f'<span class="tier-{tier_lower}">{emoji} {tier}</span>'


def get_logo_html(max_width: int = 200) -> str:
    """Return Smart Pick Pro logo <img> tag."""
    b64 = _load_image_b64("Smart_Pick_Pro_Logo.png")
    if not b64:
        return ""
    return f'<img src="data:image/png;base64,{b64}" style="max-width:{max_width}px;" alt="Smart Pick Pro">'
