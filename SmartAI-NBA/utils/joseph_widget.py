# ============================================================
# FILE: utils/joseph_widget.py
# PURPOSE: Joseph's global sidebar widget — clickable avatar with
#          ambient commentary that appears on EVERY page. Also
#          provides inline commentary injection function for
#          result-generating pages.
# CONNECTS TO: engine/joseph_brain.py, pages/helpers/joseph_live_desk.py,
#              engine/joseph_bets.py, utils/auth.py
# ============================================================

import html as _html
import logging

try:
    import streamlit as st
except ImportError:  # pragma: no cover – unit-test environments
    st = None

try:
    from utils.logger import get_logger
    _logger = get_logger(__name__)
except ImportError:
    _logger = logging.getLogger(__name__)

# ── Engine imports (safe) ────────────────────────────────────
try:
    from pages.helpers.joseph_live_desk import get_joseph_avatar_b64
    _AVATAR_AVAILABLE = True
except ImportError:
    _AVATAR_AVAILABLE = False

    def get_joseph_avatar_b64() -> str:
        return ""

try:
    from engine.joseph_brain import (
        joseph_ambient_line,
        joseph_get_ambient_context,
        joseph_commentary,
    )
    _BRAIN_AVAILABLE = True
except ImportError:
    _BRAIN_AVAILABLE = False

    def joseph_ambient_line(context, **kwargs):
        return ""

    def joseph_get_ambient_context(session_state):
        return ("idle", {})

    def joseph_commentary(results, context_type):
        return ""

import time as _time


def _joseph_typing_generator(text: str):
    """Yield words one at a time with a short delay to simulate typing.

    Designed to be passed to ``st.write_stream()`` so Joseph's words
    appear as if he is actively speaking in real-time.
    """
    words = text.split(" ")
    for i, word in enumerate(words):
        yield word + (" " if i < len(words) - 1 else "")
        _time.sleep(0.03)


try:
    from engine.joseph_bets import joseph_get_track_record
    _BETS_AVAILABLE = True
except ImportError:
    _BETS_AVAILABLE = False

    def joseph_get_track_record():
        return {
            "total": 0, "wins": 0, "losses": 0, "pending": 0,
            "win_rate": 0.0, "roi_estimate": 0.0,
        }

try:
    from utils.auth import is_premium_user
    _AUTH_AVAILABLE = True
except ImportError:
    _AUTH_AVAILABLE = False

    def is_premium_user():
        return True


# ════════════════════════════════════════════════════════════════
# CSS — injected once per session via _inject_widget_css()
# ════════════════════════════════════════════════════════════════

_WIDGET_CSS = """<style>
/* ── Joseph Sidebar Container ───────────────────────────────── */
.joseph-sidebar-container{
    background:rgba(7,10,19,0.90);
    border:1px solid rgba(255,94,0,0.35);
    border-radius:14px;
    padding:14px;
    margin-top:12px;
    text-align:center;
    backdrop-filter:blur(12px);
    -webkit-backdrop-filter:blur(12px);
    position:relative;overflow:hidden;
    box-shadow:0 0 24px rgba(255,94,0,0.06);
}
/* Top broadcast bar */
.joseph-sidebar-container::before{
    content:'';position:absolute;top:0;left:0;right:0;height:3px;
    background:linear-gradient(90deg,#ff5e00,#ff9e00,#ff5e00);
    background-size:200% 100%;
    animation:josephWidgetShimmer 3s linear infinite;
}
/* Bottom broadcast bar */
.joseph-sidebar-container::after{
    content:'';position:absolute;bottom:0;left:0;right:0;height:2px;
    background:linear-gradient(90deg,#ff9e00,#ff5e00,#ff9e00);
    background-size:200% 100%;
    animation:josephWidgetShimmer 3s linear infinite reverse;
}
@keyframes josephWidgetShimmer{
    0%{background-position:-200% 0}100%{background-position:200% 0}
}
/* ── Joseph Sidebar Avatar ──────────────────────────────────── */
.joseph-sidebar-avatar{
    width:56px;height:56px;border-radius:50%;
    border:2px solid #ff5e00;
    box-shadow:0 0 14px rgba(255,94,0,0.45);
    transition:transform 0.2s ease,box-shadow 0.2s ease;
    animation:josephSidebarGlow 3s ease-in-out infinite;
}
@keyframes josephSidebarGlow{
    0%,100%{box-shadow:0 0 14px rgba(255,94,0,0.45)}
    50%{box-shadow:0 0 20px rgba(255,94,0,0.65)}
}
.joseph-sidebar-avatar:hover{
    transform:scale(1.1);
    box-shadow:0 0 22px rgba(255,94,0,0.65);
}
/* ── Sidebar Name Label ─────────────────────────────────────── */
.joseph-sidebar-name{
    font-family:'Orbitron',sans-serif;
    color:#ff5e00;font-size:0.78rem;font-weight:700;
    margin-top:6px;letter-spacing:0.4px;
    text-shadow:0 0 8px rgba(255,94,0,0.15);
}
/* ── LIVE Analysis Badge ────────────────────────────────────── */
.joseph-live-badge{
    display:inline-flex;align-items:center;gap:4px;
    font-family:'Orbitron',sans-serif;font-size:0.6rem;
    color:#ff2020;font-weight:700;letter-spacing:0.8px;
    margin-top:4px;
}
/* ── Ambient Text ───────────────────────────────────────────── */
.joseph-ambient-text{
    color:#ff9d4d;font-size:0.76rem;font-style:italic;
    font-family:'Montserrat',sans-serif;
    margin-top:6px;line-height:1.35;min-height:2.5em;
}
/* ── Track Record Numbers ───────────────────────────────────── */
.joseph-track-record{
    font-family:'JetBrains Mono',monospace;
    font-variant-numeric:tabular-nums;
    font-size:0.7rem;color:#94a3b8;margin-top:8px;
}
/* ── Pulse Dot ──────────────────────────────────────────────── */
.joseph-pulse-dot{
    display:inline-block;width:6px;height:6px;
    border-radius:50%;background:#ff5e00;
    animation:josephPulse 1.5s ease-in-out infinite;
    margin-right:4px;vertical-align:middle;
    box-shadow:0 0 4px rgba(255,94,0,0.5);
}
@keyframes josephPulse{
    0%,100%{opacity:0.4;transform:scale(0.8);}
    50%{opacity:1;transform:scale(1.2);}
}
/* ── Typing Indicator — 3 bouncing dots ─────────────────────── */
.joseph-widget-typing{display:inline-flex;gap:4px;align-items:center;padding:4px 0}
.joseph-widget-typing span{
    display:inline-block;width:5px;height:5px;
    background:#ff5e00;border-radius:50%;
    animation:josephWidgetBounce 1.2s ease-in-out infinite;
}
.joseph-widget-typing span:nth-child(2){animation-delay:0.15s}
.joseph-widget-typing span:nth-child(3){animation-delay:0.3s}
@keyframes josephWidgetBounce{
    0%,80%,100%{transform:translateY(0)}
    40%{transform:translateY(-6px)}
}
/* ── Inline Commentary Card ─────────────────────────────────── */
.joseph-inline-card{
    background:rgba(7,10,19,0.90);
    border:1px solid rgba(255,94,0,0.25);
    border-left:3px solid rgba(255,94,0,0.5);
    border-radius:10px;
    padding:14px 16px;margin:12px 0;
    backdrop-filter:blur(12px);
    -webkit-backdrop-filter:blur(12px);
    transition:all 0.2s ease;
}
.joseph-inline-card:hover{
    border-color:rgba(255,94,0,0.4);
    box-shadow:0 2px 16px rgba(255,94,0,0.08);
}
.joseph-inline-avatar{
    width:36px;height:36px;border-radius:50%;
    border:2px solid #ff5e00;
    vertical-align:middle;margin-right:8px;
    box-shadow:0 0 6px rgba(255,94,0,0.25);
}
.joseph-inline-label{
    color:#ff5e00;font-weight:700;font-size:0.85rem;
    font-family:'Orbitron',sans-serif;
    text-shadow:0 0 6px rgba(255,94,0,0.12);
}
.joseph-inline-text{
    color:#c0d0e8;font-size:0.84rem;margin-top:8px;line-height:1.5;
    font-family:'Montserrat',sans-serif;
}
/* ── Verdict Accents ────────────────────────────────────────── */
.joseph-widget-verdict-smash{color:#ff4444;font-weight:700;text-shadow:0 0 6px rgba(255,68,68,0.2);}
.joseph-widget-verdict-lean{color:#00ff9d;font-weight:700;text-shadow:0 0 6px rgba(0,255,157,0.15);}
.joseph-widget-verdict-fade{color:#ffc800;font-weight:700;text-shadow:0 0 6px rgba(255,200,0,0.15);}
/* ── Ask Joseph Popover ─────────────────────────────────────── */
.joseph-popover-container{
    background:rgba(7,10,19,0.92);
    backdrop-filter:blur(16px);
    -webkit-backdrop-filter:blur(16px);
    border:1px solid rgba(255,94,0,0.3);
    border-radius:14px;
    padding:20px;position:relative;overflow:hidden;
    box-shadow:0 0 30px rgba(255,94,0,0.08);
}
.joseph-popover-container::before{
    content:'';position:absolute;top:0;left:0;right:0;height:3px;
    background:linear-gradient(90deg,#ff5e00,#ff9e00,#ff5e00);
    background-size:200% 100%;
    animation:josephWidgetShimmer 3s linear infinite;
}
.joseph-popover-container::after{
    content:'';position:absolute;bottom:0;left:0;right:0;height:2px;
    background:linear-gradient(90deg,#ff9e00,#ff5e00,#ff9e00);
    background-size:200% 100%;
    animation:josephWidgetShimmer 3s linear infinite reverse;
}
.joseph-popover-avatar{
    width:64px;height:64px;border-radius:50%;
    border:2px solid #ff5e00;object-fit:cover;
    box-shadow:0 0 14px rgba(255,94,0,0.4),0 0 28px rgba(255,94,0,0.15);
    transition:transform 0.2s ease,box-shadow 0.2s ease;
    animation:josephPopoverGlow 3s ease-in-out infinite;
}
@keyframes josephPopoverGlow{
    0%,100%{box-shadow:0 0 14px rgba(255,94,0,0.4),0 0 28px rgba(255,94,0,0.15)}
    50%{box-shadow:0 0 20px rgba(255,94,0,0.6),0 0 36px rgba(255,94,0,0.2)}
}
.joseph-popover-avatar:hover{
    transform:scale(1.08);
    box-shadow:0 0 22px rgba(255,94,0,0.6);
}
.joseph-popover-title{
    font-family:'Orbitron',sans-serif;
    color:#ff5e00;font-size:1.05rem;font-weight:700;
    letter-spacing:0.5px;
    text-shadow:0 0 10px rgba(255,94,0,0.2);
}
.joseph-popover-subtitle{
    color:#94a3b8;font-size:0.82rem;
    font-family:'Montserrat',sans-serif;
}
.joseph-popover-body{
    color:#e2e8f0;font-size:0.88rem;line-height:1.6;
    font-family:'Montserrat',sans-serif;
    margin-top:12px;
}
.joseph-popover-body strong{color:#ff9e00}
.joseph-popover-stat{
    font-family:'JetBrains Mono',monospace;
    font-variant-numeric:tabular-nums;
    color:#00f0ff;font-size:0.82rem;
}
/* ── Floating Bottom-Right Widget ───────────────────────────── */
.joseph-floating-widget{
    position:fixed;bottom:24px;right:24px;z-index:999999;
    display:flex;align-items:center;gap:12px;
    background:rgba(7,10,19,0.94);
    border:1px solid rgba(255,94,0,0.4);
    border-radius:16px;
    padding:10px 18px 10px 10px;
    backdrop-filter:blur(16px);
    -webkit-backdrop-filter:blur(16px);
    box-shadow:0 4px 24px rgba(0,0,0,0.5),0 0 20px rgba(255,94,0,0.1);
    cursor:default;
    transition:box-shadow 0.2s ease;
    max-width:380px;
    overflow:hidden;
}
.joseph-floating-widget::before{
    content:'';position:absolute;top:0;left:0;right:0;height:2px;
    background:linear-gradient(90deg,#ff5e00,#ff9e00,#ff5e00);
    background-size:200% 100%;
    animation:josephWidgetShimmer 3s linear infinite;
}
.joseph-floating-widget::after{
    content:'';position:absolute;bottom:0;left:0;right:0;height:2px;
    background:linear-gradient(90deg,#ff9e00,#ff5e00,#ff9e00);
    background-size:200% 100%;
    animation:josephWidgetShimmer 3s linear infinite reverse;
}
.joseph-floating-widget:hover{
    box-shadow:0 4px 32px rgba(0,0,0,0.6),0 0 28px rgba(255,94,0,0.2);
}
.joseph-floating-avatar{
    width:115px;height:115px;border-radius:50%;flex-shrink:0;
    border:2px solid #ff5e00;object-fit:cover;
    box-shadow:0 0 12px rgba(255,94,0,0.45);
    animation:josephFloatingGlow 3s ease-in-out infinite;
}
@keyframes josephFloatingGlow{
    0%,100%{box-shadow:0 0 12px rgba(255,94,0,0.45)}
    50%{box-shadow:0 0 18px rgba(255,94,0,0.65)}
}
.joseph-floating-avatar:hover{
    transform:scale(1.08);
    box-shadow:0 0 20px rgba(255,94,0,0.65);
    transition:transform 0.2s ease,box-shadow 0.2s ease;
}
.joseph-floating-info{
    display:flex;flex-direction:column;gap:2px;
    min-width:0;
}
.joseph-floating-name{
    font-family:'Orbitron',sans-serif;
    color:#ff5e00;font-size:0.72rem;font-weight:700;
    letter-spacing:0.4px;white-space:nowrap;
    text-shadow:0 0 8px rgba(255,94,0,0.15);
}
.joseph-floating-ambient{
    color:#ff9d4d;font-size:0.68rem;font-style:italic;
    font-family:'Montserrat',sans-serif;
    line-height:1.3;
    display:-webkit-box;-webkit-line-clamp:2;-webkit-box-orient:vertical;
    overflow:hidden;text-overflow:ellipsis;
    transition:opacity 0.6s ease;
}
</style>"""


# ════════════════════════════════════════════════════════════════
# _inject_widget_css — inject once per Streamlit session
# ════════════════════════════════════════════════════════════════

def _inject_widget_css() -> None:
    """Inject the Joseph widget CSS into the page.

    Uses ``st.markdown`` with ``unsafe_allow_html=True``.
    Re-injected on every page render so the CSS survives
    Streamlit page navigation (each page re-renders fully).
    """
    if st is None:
        return
    try:
        st.markdown(_WIDGET_CSS, unsafe_allow_html=True)
    except Exception as exc:
        _logger.debug("_inject_widget_css skipped: %s", exc)


# ════════════════════════════════════════════════════════════════
# render_joseph_sidebar_widget — sidebar avatar + ambient line
# ════════════════════════════════════════════════════════════════

def render_joseph_sidebar_widget() -> None:
    """Render Joseph's sidebar widget in ``st.sidebar``.

    The widget shows:

    * Joseph's 56 px avatar with hover glow
    * A pulsing "LIVE" dot
    * A rotating ambient commentary line from
      :func:`engine.joseph_brain.joseph_ambient_line`
    * A mini track-record badge (wins / losses / ROI) when data
      is available

    Call this function once from every page's layout code so
    Joseph appears globally.
    """
    if st is None:
        return

    try:
        _inject_widget_css()
    except Exception:
        pass

    try:
        # ── Avatar image ──────────────────────────────────────
        avatar_b64 = ""
        try:
            avatar_b64 = get_joseph_avatar_b64()
        except Exception:
            pass

        if avatar_b64:
            avatar_html = (
                f'<img src="data:image/png;base64,{avatar_b64}" '
                f'class="joseph-sidebar-avatar" '
                f'alt="Joseph M. Smith" />'
            )
        else:
            avatar_html = (
                '<div class="joseph-sidebar-avatar" '
                'style="display:flex;align-items:center;justify-content:center;'
                'background:#1a1a2e;font-size:1.4rem;">🎙️</div>'
            )

        # ── Ambient commentary ────────────────────────────────
        ambient_text = ""
        try:
            session_dict = dict(st.session_state) if hasattr(st, "session_state") else {}
            context_key, ctx_kwargs = joseph_get_ambient_context(session_dict)
            ambient_text = joseph_ambient_line(context_key, **ctx_kwargs)
        except Exception as exc:
            _logger.debug("Ambient line failed: %s", exc)

        if not ambient_text:
            ambient_text = "Joseph M. Smith is ALWAYS watching the board…"

        escaped_ambient = _html.escape(ambient_text)

        # ── Track-record mini badge ───────────────────────────
        track_html = ""
        try:
            record = joseph_get_track_record()
            total = record.get("total", 0)
            if total > 0:
                wins = record.get("wins", 0)
                losses = record.get("losses", 0)
                roi = record.get("roi_estimate", 0.0)
                roi_sign = "+" if roi >= 0 else ""
                track_html = (
                    f'<div class="joseph-track-record">'
                    f'📊 {wins}W-{losses}L '
                    f'<span style="color:#00ff9d;">{roi_sign}{roi:.1f}% ROI</span>'
                    f'</div>'
                )
        except Exception:
            pass

        # ── Compose sidebar HTML ──────────────────────────────
        with st.sidebar:
            st.markdown(
                f'<div class="joseph-sidebar-container">'
                f'{avatar_html}'
                f'<div class="joseph-ambient-text">'
                f'<span class="joseph-pulse-dot"></span> '
                f'{escaped_ambient}'
                f'</div>'
                f'{track_html}'
                f'</div>',
                unsafe_allow_html=True,
            )
    except Exception as exc:
        _logger.debug("render_joseph_sidebar_widget failed: %s", exc)


# ════════════════════════════════════════════════════════════════
# render_joseph_floating_widget — fixed bottom-right floating card
# ════════════════════════════════════════════════════════════════

def render_joseph_floating_widget() -> None:
    """Render Joseph as a floating widget pinned to the bottom-right.

    The widget uses ``position:fixed`` so it stays visible regardless
    of scroll position.  It shows:

    * Joseph's **48 px** avatar with animated glow ring
    * A pulsing LIVE dot + his name
    * A rotating ambient commentary line

    Call this function once per page (typically from
    :func:`utils.components.render_global_settings`) to give Joseph
    persistent, always-visible presence.
    """
    if st is None:
        return

    try:
        _inject_widget_css()
    except Exception:
        pass

    try:
        # ── Avatar image ──────────────────────────────────────
        avatar_b64 = ""
        try:
            avatar_b64 = get_joseph_avatar_b64()
        except Exception:
            pass

        if avatar_b64:
            avatar_html = (
                f'<img src="data:image/png;base64,{avatar_b64}" '
                f'class="joseph-floating-avatar" '
                f'alt="Joseph M. Smith" />'
            )
        else:
            avatar_html = (
                '<div class="joseph-floating-avatar" '
                'style="display:flex;align-items:center;justify-content:center;'
                'background:#1a1a2e;font-size:1.2rem;">🎙️</div>'
            )

        # ── Ambient commentary (multiple for 60-second rotation) ─
        _ROTATION_COUNT = 10
        ambient_lines: list[str] = []
        try:
            session_dict = dict(st.session_state) if hasattr(st, "session_state") else {}
            context_key, ctx_kwargs = joseph_get_ambient_context(session_dict)
            seen: set[str] = set()
            # Try up to 3× the target count to handle duplicates from
            # the anti-repetition logic in joseph_ambient_line.
            for _ in range(_ROTATION_COUNT * 3):
                line = joseph_ambient_line(context_key, **ctx_kwargs)
                if line and line not in seen:
                    seen.add(line)
                    ambient_lines.append(line)
                if len(ambient_lines) >= _ROTATION_COUNT:
                    break
        except Exception as exc:
            _logger.debug("Floating ambient line failed: %s", exc)

        if not ambient_lines:
            ambient_lines = [
                "Joseph M. Smith is ALWAYS watching the board…",
            ]

        import json as _json
        first_msg = _html.escape(ambient_lines[0])
        # JSON-encode the array for safe embedding in a <script> tag
        msgs_json = _json.dumps(ambient_lines)

        rotation_js = ""
        if len(ambient_lines) > 1:
            rotation_js = (
                "<script>"
                "(function(){"
                "var el=document.getElementById('joseph-floating-ambient-text');"
                "if(!el)return;"
                f"var msgs={msgs_json};"
                "var idx=0;"
                "setInterval(function(){"
                "el.style.opacity='0';"
                "setTimeout(function(){"
                "idx=(idx+1)%msgs.length;"
                "el.textContent=msgs[idx];"
                "el.style.opacity='1';"
                "},600);"
                "},60000);"
                "})();"
                "</script>"
            )

        # ── Render floating HTML (main page, NOT sidebar) ─────
        st.markdown(
            f'<div class="joseph-floating-widget">'
            f'{avatar_html}'
            f'<div class="joseph-floating-info">'
            f'<div class="joseph-floating-name">'
            f'<span class="joseph-pulse-dot"></span> Joseph M. Smith'
            f'</div>'
            f'<div class="joseph-floating-ambient" '
            f'id="joseph-floating-ambient-text">'
            f'{first_msg}'
            f'</div>'
            f'</div>'
            f'</div>'
            f'{rotation_js}',
            unsafe_allow_html=True,
        )
    except Exception as exc:
        _logger.debug("render_joseph_floating_widget failed: %s", exc)


# ════════════════════════════════════════════════════════════════
# inject_joseph_inline_commentary — inline card for result pages
# ════════════════════════════════════════════════════════════════

def inject_joseph_inline_commentary(
    results: list,
    context_type: str = "analysis_results",
) -> None:
    """Inject an inline Joseph commentary card into the page.

    Designed for result-generating pages (Neural Analysis, Entry
    Builder, etc.) where Joseph reacts to the output.

    Parameters
    ----------
    results : list[dict]
        Analysis result dicts that Joseph will comment on.
    context_type : str
        Context type key for :func:`engine.joseph_brain.joseph_commentary`
        (e.g. ``"analysis_results"``, ``"entry_built"``).
    """
    if st is None:
        return
    if not results:
        return

    try:
        _inject_widget_css()
    except Exception:
        pass

    try:
        # ── Commentary text ───────────────────────────────────
        commentary = ""
        try:
            commentary = joseph_commentary(results, context_type)
        except Exception as exc:
            _logger.debug("joseph_commentary failed: %s", exc)

        if not commentary:
            return

        escaped_commentary = _html.escape(commentary)

        # ── Avatar for inline card ────────────────────────────
        avatar_b64 = ""
        try:
            avatar_b64 = get_joseph_avatar_b64()
        except Exception:
            pass

        if avatar_b64:
            inline_avatar = (
                f'<img src="data:image/png;base64,{avatar_b64}" '
                f'class="joseph-inline-avatar" alt="Joseph" />'
            )
        else:
            inline_avatar = (
                '<span class="joseph-inline-avatar" '
                'style="display:inline-flex;align-items:center;'
                'justify-content:center;background:#1a1a2e;'
                'font-size:0.9rem;">🎙️</span>'
            )

        # ── Verdict accent (check top result) ────────────────
        verdict_class = ""
        top = results[0] if results else {}
        verdict = str(top.get("verdict", top.get("joseph_verdict", ""))).upper()
        if verdict == "SMASH":
            verdict_class = " joseph-widget-verdict-smash"
        elif verdict == "LEAN":
            verdict_class = " joseph-widget-verdict-lean"
        elif verdict in ("FADE", "STAY_AWAY"):
            verdict_class = " joseph-widget-verdict-fade"

        # ── Render inline card with typing effect ─────────────
        # Render the card header (avatar + name) as styled HTML,
        # then use st.write_stream() with a word-by-word generator
        # so Joseph's commentary feels like real-time speech.
        st.markdown(
            f'<div class="joseph-inline-card">'
            f'<div>'
            f'{inline_avatar}'
            f'<span class="joseph-inline-label">Joseph M. Smith</span>'
            f'</div>'
            f'</div>',
            unsafe_allow_html=True,
        )
        # Stream the commentary word by word (typing effect)
        _stream_key = f"_joseph_streamed_{context_type}_{hash(commentary)}"
        if not st.session_state.get(_stream_key, False):
            st.write_stream(_joseph_typing_generator(commentary))
            st.session_state[_stream_key] = True
        else:
            # Already streamed in this session — show instantly
            st.markdown(
                f'<div class="joseph-inline-text{verdict_class}">'
                f'{escaped_commentary}'
                f'</div>',
                unsafe_allow_html=True,
            )
    except Exception as exc:
        _logger.debug("inject_joseph_inline_commentary failed: %s", exc)


# ════════════════════════════════════════════════════════════════
# render_joseph_ask_popover — Ask Joseph popover with 64px avatar
# ════════════════════════════════════════════════════════════════

def render_joseph_ask_popover(
    results: list | None = None,
    context_type: str = "analysis_results",
) -> None:
    """Render an *Ask Joseph* popover button with a 64 px avatar header.

    The popover shows:

    * Joseph's **64 px** avatar with orange glow ring (or 🎙️ fallback)
    * A broadcast-style header with his name
    * A reactive commentary line based on ``results`` (or an ambient
      line when no results are supplied)

    Call from any page to give users a quick-access Joseph surface
    beyond the sidebar widget.

    Parameters
    ----------
    results : list[dict] | None
        Optional analysis results for Joseph to react to.
    context_type : str
        Context type key passed to :func:`engine.joseph_brain.joseph_commentary`.
    """
    if st is None:
        return

    try:
        _inject_widget_css()
    except Exception:
        pass

    try:
        # ── Avatar image ──────────────────────────────────────
        avatar_b64 = ""
        try:
            avatar_b64 = get_joseph_avatar_b64()
        except Exception:
            pass

        if avatar_b64:
            avatar_html = (
                f'<img src="data:image/png;base64,{avatar_b64}" '
                f'class="joseph-popover-avatar" '
                f'alt="Joseph M. Smith" />'
            )
        else:
            avatar_html = (
                '<div class="joseph-popover-avatar" '
                'style="display:flex;align-items:center;justify-content:center;'
                'background:#1a1a2e;font-size:1.6rem;">🎙️</div>'
            )

        # ── Commentary / ambient text ─────────────────────────
        commentary = ""
        try:
            if results:
                commentary = joseph_commentary(results, context_type)
            else:
                session_dict = (
                    dict(st.session_state)
                    if hasattr(st, "session_state")
                    else {}
                )
                ctx_key, ctx_kwargs = joseph_get_ambient_context(session_dict)
                commentary = joseph_ambient_line(ctx_key, **ctx_kwargs)
        except Exception as exc:
            _logger.debug("Popover commentary failed: %s", exc)

        if not commentary:
            commentary = "Joseph M. Smith is ready. What's on the board tonight?"

        escaped_commentary = _html.escape(commentary)

        # ── Render popover ────────────────────────────────────
        with st.popover("🎙️ Ask Joseph"):
            st.markdown(
                f'<div class="joseph-popover-container">'
                f'<div style="display:flex;align-items:center;gap:14px;'
                f'margin-bottom:12px;">'
                f'{avatar_html}'
                f'<div>'
                f'<div class="joseph-popover-title">Joseph M. Smith</div>'
                f'<div class="joseph-popover-subtitle">'
                f'<span class="joseph-pulse-dot"></span> LIVE — NBA Analyst'
                f'</div>'
                f'</div></div>'
                f'<div class="joseph-popover-body">{escaped_commentary}</div>'
                f'</div>',
                unsafe_allow_html=True,
            )
    except Exception as exc:
        _logger.debug("render_joseph_ask_popover failed: %s", exc)
