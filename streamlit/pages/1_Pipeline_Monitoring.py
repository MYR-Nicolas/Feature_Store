"""Page 1 — Pipeline Monitoring"""
import json
import os
import sys
from datetime import datetime, timezone
from _style import get_bq_client
import pandas as pd
import plotly.graph_objects as go

import streamlit as st

sys.path.insert(0, os.path.dirname(__file__))
from _style import inject_css, hero, section_banner, fmt_ts, fmt_num, badge_html, sidebar_header

st.set_page_config(page_title="Pipeline Monitoring · BTC Feature Store", layout="wide")
inject_css()

CACHE_FILE = os.path.join(os.path.dirname(__file__), ".cache_pipeline.json")
PROJECT_ID = os.getenv("GCP_PROJECT_ID", "")


def _save(data: dict) -> None:
    with open(CACHE_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, default=str, indent=2)


def _load() -> dict | None:
    if os.path.exists(CACHE_FILE):
        try:
            with open(CACHE_FILE, encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return None
    return None


def _fetch(project_id: str) -> dict | None:
    try:
        from google.cloud import bigquery
        client = get_bq_client()
        row = next(iter(client.query(
            "SELECT * FROM `" + project_id + ".monitoring.pipeline_runs` ORDER BY started_at DESC LIMIT 1"
        ).result()), None)
        if row is None:
            return None
        data = dict(row)
        hist = list(client.query(
            "SELECT run_id, status, started_at, duration_seconds, rows_extracted, rows_loaded "
            "FROM `" + project_id + ".monitoring.pipeline_runs` ORDER BY started_at DESC LIMIT 20"
        ).result())
        data["history"] = [dict(h) for h in hist]
        data["_fetched_at"] = datetime.now(timezone.utc).isoformat()
        return data
    except Exception as exc:
        return {"_error": str(exc)}


DEMO = {
    "run_id": "a8f3c1d2-4e91-4b2a-9c07-b3e8f2a10c45",
    "pipeline_name": "btc-pipeline",
    "status": "success",
    "started_at": "2025-04-28T08:00:00+00:00",
    "ended_at": "2025-04-28T08:04:32+00:00",
    "duration_seconds": 272.0,
    "rows_extracted": 10080,
    "rows_loaded": 10080,
    "error_message": None,
    "_demo": True,
    "_fetched_at": "2025-04-28T08:04:32+00:00",
    "history": [
        {"run_id": f"run-{i:03d}", "status": "success" if i % 6 != 0 else "failed",
         "started_at": f"2025-04-{28 - i:02d}T08:00:00+00:00",
         "duration_seconds": 245 + i * 8, "rows_extracted": 10080, "rows_loaded": 10080}
        for i in range(14)
    ],
}

#========
# Sidebar
#========

with st.sidebar:
    sidebar_header(PROJECT_ID)
    refresh_btn = st.button("↻  Rafraîchir", use_container_width=True)

#==========
# Load data
#==========

data: dict | None = None
is_stale = False
is_demo = False
error_msg = ""

if refresh_btn or ("_pipeline_loaded" not in st.session_state):
    st.session_state["_pipeline_loaded"] = True
    if PROJECT_ID:
        with st.spinner("Connexion à BigQuery…"):
            result = _fetch(PROJECT_ID)
        if result and "_error" not in result:
            _save(result)
            data = result
        else:
            error_msg = (result or {}).get("_error", "")
            data = _load()
            is_stale = True
    else:
        data = _load()
        is_stale = bool(data)

if data is None:
    data = DEMO
    is_demo = True

#=======
# Header
#=======

fetched_at = fmt_ts(data.get("_fetched_at"))

hero(
    eyebrow="Monitoring · Pipeline ELT",
    title="Pipeline Runs",
    subtitle="Exécutions du pipeline BTC OHLCV 1m — monitoring.pipeline_runs · "
             "Dernière sync : " + fetched_at,
)

if is_demo:
    st.markdown(
        '<div class="stale-banner">⚡ Données de démonstration — définissez <code>GCP_PROJECT_ID</code> et cliquez sur Rafraîchir.</div>',
        unsafe_allow_html=True,
    )
elif is_stale:
    msg_part = (" — Erreur : " + error_msg[:120]) if error_msg else ""
    st.markdown(
        '<div class="stale-banner">⚠ Connexion GCP indisponible — affichage des dernières données connues.' + msg_part + '</div>',
        unsafe_allow_html=True,
    )

#================
# Section 01 KPIs
#================

section_banner("01", "Résumé de la dernière run", "Statut, performance et intégrité du chargement.")

status = (data.get("status") or "unknown").lower()
status_color = {"success": "c-green", "failed": "c-red", "running": "c-amber"}.get(status, "")
dur = data.get("duration_seconds")
rows_e = data.get("rows_extracted")
rows_l = data.get("rows_loaded")
history = data.get("history", [])
total_runs = len(history)
success_runs = sum(1 for h in history if (h.get("status") or "").lower() == "success")
success_rate = round(success_runs / total_runs * 100) if total_runs else 0
rate_color = "c-green" if success_rate >= 90 else "c-amber"
dur_str = f"{dur:.1f}" if dur else "—"

st.markdown(
    "<div class='kpi-grid'>"
    "<div class='kpi-cell'><div class='kpi-label'>Statut</div>"
    "<div class='kpi-value " + status_color + "'>" + status.upper() + "</div></div>"
    "<div class='kpi-cell'><div class='kpi-label'>Durée</div>"
    "<div class='kpi-value'>" + dur_str + "<span style='font-size:1rem;font-weight:500;color:#94a3b8;'> s</span></div></div>"
    "<div class='kpi-cell'><div class='kpi-label'>Lignes extraites</div>"
    "<div class='kpi-value c-blue'>" + fmt_num(rows_e) + "</div></div>"
    "<div class='kpi-cell'><div class='kpi-label'>Lignes chargées</div>"
    "<div class='kpi-value'>" + fmt_num(rows_l) + "</div></div>"
    "<div class='kpi-cell'><div class='kpi-label'>Taux de succès</div>"
    "<div class='kpi-value " + rate_color + "'>" + str(success_rate) + "<span style='font-size:1rem;font-weight:500;color:#94a3b8;'>%</span></div>"
    "<div class='kpi-sub'>" + str(total_runs) + " runs analysées</div></div>"
    "</div>",
    unsafe_allow_html=True,
)

#===============================
# Section 02 Details + Integrity
#===============================

section_banner("02", "Détails & intégrité", "Métadonnées de la run et taux d'intégrité du chargement.")

col_left, col_right = st.columns(2)

with col_left:
    st.markdown(
        "<div style='"
        "background:rgba(255,255,255,0.96);"
        "border:1px solid rgba(226,232,240,0.9);"
        "border-radius:16px;"
        "padding:1.4rem 1.6rem;"
        "box-shadow:0 4px 16px rgba(15,23,42,0.06);"
        "'>"
        "<div style='font-size:0.68rem;font-weight:700;letter-spacing:0.10em;text-transform:uppercase;"
        "color:#94a3b8;margin-bottom:1.1rem;padding-bottom:0.6rem;border-bottom:1px solid rgba(226,232,240,0.8);'>"
        "Métadonnées</div>"
        "<table style='width:100%;border-collapse:collapse;font-size:0.85rem;'>"
        "<tr><td style='padding:0.65rem 0;border-bottom:1px solid rgba(241,245,249,0.9);color:#94a3b8;font-size:0.72rem;font-weight:600;text-transform:uppercase;letter-spacing:0.06em;width:38%;'>Run ID</td>"
        "<td style='padding:0.65rem 0;border-bottom:1px solid rgba(241,245,249,0.9);color:#374151;text-align:center;'><code style='font-size:0.78rem;background:rgba(238,242,255,0.6);padding:0.15rem 0.5rem;border-radius:4px;'>" + (data.get("run_id") or "—") + "</code></td></tr>"
        "<tr><td style='padding:0.65rem 0;border-bottom:1px solid rgba(241,245,249,0.9);color:#94a3b8;font-size:0.72rem;font-weight:600;text-transform:uppercase;letter-spacing:0.06em;'>Pipeline</td>"
        "<td style='padding:0.65rem 0;border-bottom:1px solid rgba(241,245,249,0.9);color:#374151;font-weight:600;text-align:center;'>" + (data.get("pipeline_name") or "—") + "</td></tr>"
        "<tr><td style='padding:0.65rem 0;border-bottom:1px solid rgba(241,245,249,0.9);color:#94a3b8;font-size:0.72rem;font-weight:600;text-transform:uppercase;letter-spacing:0.06em;'>Démarré à</td>"
        "<td style='padding:0.65rem 0;border-bottom:1px solid rgba(241,245,249,0.9);color:#374151;text-align:center;'>" + fmt_ts(data.get("started_at")) + "</td></tr>"
        "<tr><td style='padding:0.65rem 0;color:#94a3b8;font-size:0.72rem;font-weight:600;text-transform:uppercase;letter-spacing:0.06em;'>Terminé à</td>"
        "<td style='padding:0.65rem 0;color:#374151;text-align:center;'>" + fmt_ts(data.get("ended_at")) + "</td></tr>"
        "</table></div>",
        unsafe_allow_html=True,
    )

with col_right:
    err = data.get("error_message")
    if err:
        st.error(err[:600])
    else:
        match_pct = (rows_l / rows_e * 100) if rows_e and rows_l else 100.0
        fill_color = "#059669" if match_pct >= 99 else ("#d97706" if match_pct >= 95 else "#dc2626")
        bg_color   = "rgba(236,253,245,0.7)" if match_pct >= 99 else ("rgba(255,251,235,0.7)" if match_pct >= 95 else "rgba(254,242,242,0.7)")
        st.markdown(
            "<div style='"
            "background:" + bg_color + ";"
            "border:1px solid rgba(226,232,240,0.9);"
            "border-radius:16px;"
            "padding:1.4rem 1.6rem;"
            "box-shadow:0 4px 16px rgba(15,23,42,0.06);"
            "height:100%;"
            "'>"
            "<div style='font-size:0.68rem;font-weight:700;letter-spacing:0.10em;text-transform:uppercase;"
            "color:#94a3b8;margin-bottom:1.1rem;padding-bottom:0.6rem;border-bottom:1px solid rgba(226,232,240,0.8);'>"
            "Intégrité du chargement</div>"

            "<div style='text-align:center;margin-bottom:1.2rem;'>"
            "<div style='font-size:3rem;font-weight:800;color:" + fill_color + ";letter-spacing:-0.04em;line-height:1;'>"
            + f"{match_pct:.1f}" + "<span style='font-size:1.4rem;font-weight:600;'>%</span></div>"
            "<div style='font-size:0.78rem;color:#64748b;margin-top:0.3rem;'>Extraction → Chargement</div>"
            "</div>"

            "<div style='background:rgba(226,232,240,0.5);border-radius:6px;height:8px;overflow:hidden;margin-bottom:0.8rem;'>"
            "<div style='height:100%;border-radius:6px;background:" + fill_color + ";width:" + f"{min(match_pct,100):.1f}%" + ";'></div></div>"

            "<div style='display:flex;justify-content:space-between;font-size:0.8rem;'>"
            "<div style='text-align:center;'>"
            "<div style='font-size:1.1rem;font-weight:700;color:#0f172a;'>" + fmt_num(rows_e) + "</div>"
            "<div style='font-size:0.7rem;color:#94a3b8;text-transform:uppercase;letter-spacing:0.06em;margin-top:0.1rem;'>Extraites</div>"
            "</div>"
            "<div style='text-align:center;'>"
            "<div style='font-size:1.1rem;font-weight:700;color:#0f172a;'>" + fmt_num(rows_l) + "</div>"
            "<div style='font-size:0.7rem;color:#94a3b8;text-transform:uppercase;letter-spacing:0.06em;margin-top:0.1rem;'>Chargées</div>"
            "</div>"
            "</div>"
            "</div>",
            unsafe_allow_html=True,
        )

#===================
# Section 03 History
#===================

if history:

    section_banner("03", "Historique des runs", "Évolution de la durée d'exécution sur les 20 dernières runs.")
    df = pd.DataFrame(history)
    df["started_at"] = pd.to_datetime(df["started_at"], utc=True)
    df = df.sort_values("started_at")

    avg_dur = df["duration_seconds"].mean()
    max_dur = df["duration_seconds"].max()
    failed  = total_runs - success_runs

    col_chart, col_stat = st.columns([3, 1])
    with col_chart:
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=df["started_at"],
            y=df["duration_seconds"],
            mode="lines+markers",
            line=dict(color="#2563eb", width=2.5, shape="spline"),
            marker=dict(size=6, color="#2563eb", line=dict(color="#ffffff", width=1.5)),
            fill="tozeroy",
            fillcolor="rgba(37,99,235,0.07)",
            hovertemplate="<b>%{x|%Y-%m-%d}</b><br>Durée : %{y:.1f} s<extra></extra>",
        ))
        fig.update_layout(
            plot_bgcolor="#ffffff",
            paper_bgcolor="#ffffff",
            margin=dict(l=10, r=20, t=10, b=10),
            height=210,
            xaxis=dict(
                title=dict(text="Date d'exécution", font=dict(size=11, color="#94a3b8")),
                showgrid=True, gridcolor="rgba(226,232,240,0.8)",
                zeroline=False, showline=False,
                tickfont=dict(size=10, color="#94a3b8", family="Inter, sans-serif"),
                tickformat="%d %b",
            ),
            yaxis=dict(
                title=dict(text="Durée (secondes)", font=dict(size=11, color="#94a3b8")),
                showgrid=True, gridcolor="rgba(226,232,240,0.8)",
                zeroline=False, showline=False,
                tickfont=dict(size=10, color="#94a3b8", family="Inter, sans-serif"),
            ),
            font=dict(family="Inter, sans-serif"),
            showlegend=False,
        )

        st.markdown(
            "<div style='font-size:0.68rem;font-weight:700;letter-spacing:0.10em;"
            "text-transform:uppercase;color:#94a3b8;margin-bottom:0.5rem;'>"
            "Durée d'exécution par run — 20 dernières exécutions</div>"
            "<style>div[data-testid='stPlotlyChart']{background:#ffffff;"
            "border:1px solid #e8eaed;border-radius:14px;overflow:hidden;padding:0.4rem;}</style>",
            unsafe_allow_html=True,
        )
        st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})

    with col_stat:
        st.markdown(
            "<div style='background:rgba(255,255,255,0.96);border:1px solid rgba(226,232,240,0.9);"
            "border-radius:16px;padding:1.4rem 1.6rem;box-shadow:0 4px 16px rgba(15,23,42,0.06);height:100%;'>"

            "<div style='font-size:0.68rem;font-weight:700;letter-spacing:0.10em;text-transform:uppercase;"
            "color:#94a3b8;margin-bottom:1rem;padding-bottom:0.6rem;border-bottom:1px solid rgba(226,232,240,0.8);'>"
            "Statistiques</div>"

            "<div style='display:flex;flex-direction:column;gap:0.9rem;'>"

            "<div style='text-align:center;padding:0.6rem;background:rgba(238,242,255,0.5);border-radius:10px;'>"
            "<div style='font-size:0.65rem;font-weight:700;text-transform:uppercase;letter-spacing:0.08em;color:#94a3b8;margin-bottom:0.2rem;'>Durée moy.</div>"
            "<div style='font-size:1.3rem;font-weight:800;color:#0f172a;'>" + f"{avg_dur:.1f}" + "<span style='font-size:0.8rem;color:#94a3b8;font-weight:500;'> s</span></div>"
            "</div>"

            "<div style='text-align:center;padding:0.6rem;background:rgba(238,242,255,0.5);border-radius:10px;'>"
            "<div style='font-size:0.65rem;font-weight:700;text-transform:uppercase;letter-spacing:0.08em;color:#94a3b8;margin-bottom:0.2rem;'>Durée max</div>"
            "<div style='font-size:1.3rem;font-weight:800;color:#0f172a;'>" + f"{max_dur:.1f}" + "<span style='font-size:0.8rem;color:#94a3b8;font-weight:500;'> s</span></div>"
            "</div>"

            "<div style='display:flex;gap:0.6rem;'>"
            "<div style='flex:1;text-align:center;padding:0.6rem;background:rgba(236,253,245,0.7);border-radius:10px;'>"
            "<div style='font-size:0.65rem;font-weight:700;text-transform:uppercase;letter-spacing:0.08em;color:#059669;margin-bottom:0.2rem;'>Succès</div>"
            "<div style='font-size:1.3rem;font-weight:800;color:#059669;'>" + str(success_runs) + "</div>"
            "</div>"
            "<div style='flex:1;text-align:center;padding:0.6rem;background:rgba(254,242,242,0.7);border-radius:10px;'>"
            "<div style='font-size:0.65rem;font-weight:700;text-transform:uppercase;letter-spacing:0.08em;color:#dc2626;margin-bottom:0.2rem;'>Échecs</div>"
            "<div style='font-size:1.3rem;font-weight:800;color:#dc2626;'>" + str(failed) + "</div>"
            "</div>"
            "</div>"

            "</div></div>",
            unsafe_allow_html=True,
        )
