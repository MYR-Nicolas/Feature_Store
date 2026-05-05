"""
Shared styles and utilities — BTC Feature Store
"""
import json
import os
import streamlit as st
from datetime import datetime, timezone

GLOBAL_CSS = """
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800&family=JetBrains+Mono:wght@400;500&display=swap');

.stApp {
    background:
        radial-gradient(circle at top left,  rgba(59,130,246,0.08), transparent 28%),
        radial-gradient(circle at top right, rgba(99,102,241,0.08), transparent 24%),
        linear-gradient(180deg, #f8fbff 0%, #f6f8fc 45%, #f8fafc 100%);
}
.main .block-container { max-width: 1340px; padding-top: 1.4rem; padding-bottom: 3rem; }
h1, h2, h3 { color: #0f172a; letter-spacing: -0.02em; font-family: 'Inter', sans-serif; }

section[data-testid="stSidebar"] {
    background: linear-gradient(180deg, #f8fbff 0%, #f4f7fb 100%);
    border-right: 1px solid rgba(226,232,240,0.9);
}
section[data-testid="stSidebar"] * { color: #1e293b !important; }
section[data-testid="stSidebar"] .stButton > button {
    background: linear-gradient(135deg, #1e3a8a, #2563eb) !important;
    color: white !important;
    border: none !important;
    font-weight: 600 !important;
    border-radius: 10px !important;
}

[data-testid="stMetric"] {
    background: rgba(255,255,255,0.94);
    border: 1px solid rgba(226,232,240,0.95);
    border-radius: 16px;
    padding: 0.8rem 0.9rem;
    box-shadow: 0 8px 20px rgba(15,23,42,0.05);
}
div[data-testid="stExpander"] {
    border-radius: 14px; overflow: hidden;
    border: 1px solid rgba(191,219,254,0.9);
}
[data-testid="stImage"] img {
    border-radius: 18px;
    border: 1px solid rgba(226,232,240,0.9);
    box-shadow: 0 10px 28px rgba(15,23,42,0.08);
}

.hero-box {
    background: linear-gradient(135deg, #0f172a 0%, #1e3a8a 55%, #2563eb 100%);
    color: white; border-radius: 24px;
    padding: 1.6rem 1.6rem 1.4rem 1.6rem;
    box-shadow: 0 18px 45px rgba(30,58,138,0.22);
    margin-bottom: 1.2rem;
}
.section-box {
    background: rgba(255,255,255,0.90);
    border: 1px solid rgba(226,232,240,0.95);
    border-radius: 18px; padding: 1rem 1.1rem;
    box-shadow: 0 8px 24px rgba(15,23,42,0.05);
    margin-bottom: 1rem;
}

.badge {
    display: inline-block; padding: 0.35rem 0.75rem; border-radius: 999px;
    margin: 0.15rem 0.2rem 0.15rem 0; font-size: 0.82rem; font-weight: 600;
    background: #eef2ff; color: #3730a3; border: 1px solid #c7d2fe;
}
.badge-success { background:#ecfdf5; color:#065f46; border:1px solid #a7f3d0; }
.badge-error   { background:#fef2f2; color:#991b1b; border:1px solid #fecaca; }
.badge-warning { background:#fff7ed; color:#9a3412; border:1px solid #fdba74; }
.badge-info    { background:#eff6ff; color:#1d4ed8; border:1px solid #bfdbfe; }

.stale-banner {
    display:flex; align-items:center; gap:0.75rem;
    background:#fff7ed; border:1px solid #fdba74; border-radius:12px;
    padding:0.75rem 1.1rem; font-size:0.85rem; color:#9a3412;
    margin-bottom:1.2rem; font-weight:500;
}

.kpi-grid {
    display:grid; grid-template-columns:repeat(auto-fit,minmax(160px,1fr));
    gap:1px; background:rgba(226,232,240,0.9);
    border:1px solid rgba(226,232,240,0.9); border-radius:18px;
    overflow:hidden; margin-bottom:1.5rem;
    box-shadow:0 8px 24px rgba(15,23,42,0.05);
}
.kpi-cell { background:rgba(255,255,255,0.96); padding:1.3rem 1.5rem; }
.kpi-label {
    font-size:0.68rem; font-weight:700; letter-spacing:0.09em;
    text-transform:uppercase; color:#94a3b8; margin-bottom:0.5rem;
}
.kpi-value {
    font-size:2rem; font-weight:800; color:#0f172a;
    letter-spacing:-0.04em; line-height:1; font-family:'Inter',sans-serif;
}
.kpi-value.c-green { color:#059669; }
.kpi-value.c-red   { color:#dc2626; }
.kpi-value.c-amber { color:#d97706; }
.kpi-value.c-blue  { color:#2563eb; }
.kpi-sub { font-size:0.72rem; color:#94a3b8; margin-top:0.3rem; }

.sec-label {
    font-size:0.68rem; font-weight:700; letter-spacing:0.10em;
    text-transform:uppercase; color:#94a3b8;
    margin:1.8rem 0 0.9rem 0; padding-bottom:0.5rem;
    border-bottom:1px solid rgba(226,232,240,0.9);
}

.detail-table { width:100%; border-collapse:collapse; font-size:0.85rem; }
.detail-table td {
    padding:0.65rem 0; border-bottom:1px solid rgba(241,245,249,0.9);
    color:#374151; vertical-align:top;
}
.detail-table td:first-child {
    color:#94a3b8; width:38%; font-size:0.72rem; font-weight:600;
    text-transform:uppercase; letter-spacing:0.06em;
}
.detail-table tr:last-child td { border-bottom:none; }

.model-row-wrap {
    border:1px solid rgba(226,232,240,0.9); border-radius:14px;
    overflow:hidden; background:rgba(255,255,255,0.96);
    margin-bottom:1rem; box-shadow:0 4px 12px rgba(15,23,42,0.04);
}
.model-row {
    display:flex; align-items:center; justify-content:space-between;
    padding:0.8rem 1.1rem; border-bottom:1px solid rgba(241,245,249,0.9); gap:1rem;
}
.model-row:last-child { border-bottom:none; }
.model-name { font-family:'JetBrains Mono',monospace; font-size:0.82rem; font-weight:500; color:#1e293b; }
.model-type-tag { font-size:0.65rem; font-weight:700; letter-spacing:0.08em; text-transform:uppercase; color:#94a3b8; margin-bottom:0.15rem; }
.model-time { font-size:0.75rem; color:#94a3b8; white-space:nowrap; font-family:'JetBrains Mono',monospace; }

.check-row { display:flex; align-items:flex-start; gap:0.85rem; padding:0.85rem 0; border-bottom:1px solid rgba(241,245,249,0.9); }
.check-row:last-child { border-bottom:none; }
.check-text { font-size:0.85rem; color:#374151; }
.check-text strong { color:#111827; font-weight:600; }
.check-sub { font-size:0.75rem; color:#94a3b8; margin-top:0.15rem; }

.prog-wrap { background:rgba(226,232,240,0.9); border-radius:4px; height:6px; overflow:hidden; margin:0.5rem 0; }
.prog-fill { height:100%; border-radius:4px; transition:width 0.5s ease; }

.ts-chip {
    display:inline-flex; align-items:center; gap:0.4rem;
    background:rgba(238,242,255,0.8); border:1px solid rgba(199,210,254,0.8);
    border-radius:6px; padding:0.2rem 0.65rem; font-size:0.72rem; color:#3730a3;
    font-family:'JetBrains Mono',monospace;
}

.stats-table { width:100%; border-collapse:collapse; font-size:0.82rem; margin-top:2.5rem; }
.stats-table td { padding:0.65rem 0; border-bottom:1px solid rgba(241,245,249,0.9); color:#374151; vertical-align:middle; }
.stats-table td:first-child { color:#94a3b8; font-size:0.7rem; font-weight:600; text-transform:uppercase; letter-spacing:0.07em; width:55%; }
.stats-table tr:last-child td { border-bottom:none; }
</style>
"""


def inject_css() -> None:
    st.markdown(GLOBAL_CSS, unsafe_allow_html=True)


def hero(eyebrow: str, title: str, subtitle: str) -> None:
    st.markdown(
        '<div class="hero-box">'
        '<div style="font-size:0.8rem;font-weight:800;letter-spacing:0.10em;text-transform:uppercase;opacity:0.82;margin-bottom:0.4rem;">' + eyebrow + '</div>'
        '<div style="font-size:2.15rem;font-weight:800;line-height:1.08;margin-bottom:0.55rem;">' + title + '</div>'
        '<div style="font-size:1rem;line-height:1.72;opacity:0.92;max-width:980px;">' + subtitle + '</div>'
        '</div>',
        unsafe_allow_html=True,
    )


def section_banner(index: str, title: str, description: str) -> None:
    st.markdown(
        '<div class="section-box">'
        '<div style="font-size:0.8rem;font-weight:800;text-transform:uppercase;letter-spacing:0.08em;color:#2563eb;margin-bottom:0.25rem;">Section ' + index + '</div>'
        '<div style="font-size:1.35rem;font-weight:800;color:#0f172a;margin-bottom:0.35rem;">' + title + '</div>'
        '<div style="color:#475569;line-height:1.7;">' + description + '</div>'
        '</div>',
        unsafe_allow_html=True,
    )


def fmt_ts(ts) -> str:

    if ts is None:
        return "—"
    if isinstance(ts, (int, float)):
        try:
            return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
        except Exception:
            return "—"
    if isinstance(ts, str):
        try:
            ts = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        except Exception:
            return ts
    try:
        return ts.strftime("%Y-%m-%d %H:%M UTC")
    except Exception:
        return str(ts)


def fmt_num(n, decimals: int = 0) -> str:
    if n is None:
        return "—"
    try:
        if decimals == 0:
            return f"{int(n):,}"
        return f"{float(n):,.{decimals}f}"
    except Exception:
        return str(n)


def badge_html(status: str) -> str:
    s = (status or "").lower()
    css = {"success": "badge-success", "error": "badge-error", "fail": "badge-error",
           "failed": "badge-error", "warn": "badge-warning", "warning": "badge-warning"}.get(s, "badge-info")
    return '<span class="badge ' + css + '">' + s.upper() + '</span>'


def sidebar_header(project_id: str = "") -> None:
    st.markdown(
        '<div style="font-size:0.7rem;font-weight:800;letter-spacing:0.12em;'
        'text-transform:uppercase;color:#2563eb;margin-bottom:1rem;">BTC Feature Store</div>',
        unsafe_allow_html=True,
    )
    if project_id:
        st.markdown(
            '<div style="margin-top:0.5rem;font-size:0.72rem;color:#64748b;">'
            'Projet : <code>' + project_id + '</code></div>',
            unsafe_allow_html=True,
        )
    else:
        st.markdown(
            '<div style="margin-top:0.5rem;font-size:0.72rem;color:#dc2626;">'
            '⚠ GCP_PROJECT_ID non défini</div>',
            unsafe_allow_html=True,
        )

#================
# GCS Cache fetch
#================

CACHE_DIR = os.path.dirname(os.path.abspath(__file__))


def _local_cache_path() -> str:
    return os.path.join(CACHE_DIR, ".cache_gcs_latest.json")


def _save_local(data: dict) -> None:
    with open(_local_cache_path(), "w", encoding="utf-8") as f:
        json.dump(data, f, default=str, indent=2)


def _load_local() -> dict | None:
    p = _local_cache_path()
    if os.path.exists(p):
        try:
            with open(p, encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return None
    return None


def fetch_gcs_cache(force: bool = False) -> tuple[dict | None, str]:


    gcs_url = st.secrets.get(
        "GCS_CACHE_URL",
        os.getenv("GCS_CACHE_URL", ""),
    )

    if not gcs_url:
        data = _load_local()
        return data, "local"

    if force or ("_gcs_cache" not in st.session_state):
        try:
            import requests
            r = requests.get(gcs_url, timeout=10)
            r.raise_for_status()
            data = r.json()
            _save_local(data)
            st.session_state["_gcs_cache"] = data
            st.session_state["_gcs_source"] = "live"
        except Exception as exc:
            data = _load_local()
            st.session_state["_gcs_cache"] = data
            st.session_state["_gcs_source"] = "stale"
            st.session_state["_gcs_error"] = str(exc)

    return st.session_state.get("_gcs_cache"), st.session_state.get("_gcs_source", "stale")
