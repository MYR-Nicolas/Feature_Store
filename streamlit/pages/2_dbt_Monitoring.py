"""Page 2 — dbt Monitoring"""
import json
import os
import sys
from datetime import datetime, timezone
from _style import get_bq_client

import streamlit as st

sys.path.insert(0, os.path.dirname(__file__))
from _style import inject_css, hero, section_banner, fmt_ts, fmt_num, badge_html, sidebar_header

st.set_page_config(page_title="dbt Monitoring · BTC Feature Store", layout="wide")
inject_css()

CACHE_FILE = os.path.join(os.path.dirname(__file__), ".cache_dbt.json")
PROJECT_ID = os.getenv("GCP_PROJECT_ID", "")


def _save(data: list) -> None:
    with open(CACHE_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, default=str, indent=2)


def _load() -> list | None:
    if os.path.exists(CACHE_FILE):
        try:
            with open(CACHE_FILE, encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return None
    return None


def _fetch(project_id: str) -> list | dict:
    try:
        from google.cloud import bigquery
        client = get_bq_client()
        run_row = next(iter(client.query(
            "SELECT run_id FROM `" + project_id + ".monitoring.dbt_results` ORDER BY created_at DESC LIMIT 1"
        ).result()), None)
        if run_row is None:
            return []
        run_id = run_row["run_id"]
        rows = list(client.query(
            "SELECT * FROM `" + project_id + ".monitoring.dbt_results` WHERE run_id = '" + run_id + "' ORDER BY resource_type, model_name"
        ).result())
        result = [dict(r) for r in rows]
        result.append({"_meta": True, "_fetched_at": datetime.now(timezone.utc).isoformat()})
        return result
    except Exception as exc:
        return {"_error": str(exc)}


DEMO = [
    {"run_id": "demo-run-001", "resource_type": r[0], "model_name": r[1],
     "status": r[2], "execution_time": r[3], "message": r[4],
     "created_at": "2025-04-28T08:04:32+00:00"}
    for r in [
        ("model", "btc_ohlcv_1m_silver",      "success", 1.23,  "OK"),
        ("model", "int_btc_base_returns",      "success", 2.41,  "OK"),
        ("model", "int_btc_features",          "success", 4.87,  "OK"),
        ("model", "int_btc_ichimoku",          "success", 3.02,  "OK"),
        ("model", "features",                  "success", 12.54, "OK"),
        ("model", "labels",                    "success", 8.31,  "OK"),
        ("model", "feature_versions",          "success", 0.98,  "OK"),
        ("test",  "not_null_features_ts",      "success", 0.45,  "1 of 1 record"),
        ("test",  "unique_features_ts",        "success", 0.38,  "1 of 1 record"),
        ("test",  "accepted_values_tenkan",    "success", 0.29,  "1 of 1 record"),
        ("test",  "not_null_labels_ts",        "success", 0.31,  "1 of 1 record"),
        ("test",  "unique_labels_ts",          "success", 0.27,  "1 of 1 record"),
        ("test",  "not_null_feature_versions", "success", 0.22,  "1 of 1 record"),
    ]
] + [{"_meta": True, "_demo": True, "_fetched_at": "2025-04-28T08:04:32+00:00"}]

#========
# Sidebar
#========

with st.sidebar:
    sidebar_header(PROJECT_ID)
    refresh_btn = st.button("↻  Rafraîchir", use_container_width=True)
    st.divider()
    st.markdown('<div style="font-size:0.7rem;font-weight:700;color:#64748b;text-transform:uppercase;letter-spacing:0.08em;margin-bottom:0.5rem;">Filtres</div>', unsafe_allow_html=True)
    filter_type   = st.multiselect("Type",   ["model", "test", "seed", "snapshot"], default=["model", "test"])
    filter_status = st.multiselect("Statut", ["success", "error", "warn", "skip"],  default=["success", "error", "warn"])

#==========
# Load data
#==========

rows: list | None = None
is_stale = False
is_demo = False
error_msg = ""

if refresh_btn or ("_dbt_loaded" not in st.session_state):
    st.session_state["_dbt_loaded"] = True
    if PROJECT_ID:
        with st.spinner("Connexion à BigQuery…"):
            result = _fetch(PROJECT_ID)
        if isinstance(result, list):
            _save(result)
            rows = result
        else:
            error_msg = result.get("_error", "")
            rows = _load()
            is_stale = True
    else:
        rows = _load()
        is_stale = bool(rows)

if rows is None:
    rows = DEMO
    is_demo = True

meta       = next((r for r in rows if r.get("_meta")), {})
clean_rows = [r for r in rows if not r.get("_meta")]
fetched_at = fmt_ts(meta.get("_fetched_at"))
is_demo    = is_demo or meta.get("_demo", False)

#=======
# Header
#=======

hero(
    eyebrow="Monitoring · Transformations dbt",
    title="dbt Run Results",
    subtitle="Statuts des modèles et tests dbt — monitoring.dbt_results · Dernière sync : " + fetched_at,
)

if is_demo:
    st.markdown('<div class="stale-banner">⚡ Données de démonstration — définissez <code>GCP_PROJECT_ID</code> et cliquez sur Rafraîchir.</div>', unsafe_allow_html=True)
elif is_stale:
    msg_part = (" — Erreur : " + error_msg[:120]) if error_msg else ""
    st.markdown('<div class="stale-banner">⚠ Connexion GCP indisponible — affichage des dernières données connues.' + msg_part + '</div>', unsafe_allow_html=True)

#================
# Section 01 KPIs
#================

total     = len(clean_rows)
success_n = sum(1 for r in clean_rows if (r.get("status") or "").lower() == "success")
error_n   = sum(1 for r in clean_rows if (r.get("status") or "").lower() in ("error", "failed"))
model_n   = sum(1 for r in clean_rows if r.get("resource_type") == "model")
test_n    = sum(1 for r in clean_rows if r.get("resource_type") == "test")
total_t   = sum(r.get("execution_time") or 0 for r in clean_rows)
run_id    = (clean_rows[0].get("run_id") or "—") if clean_rows else "—"
created_at = fmt_ts((clean_rows[0].get("created_at")) if clean_rows else None)

section_banner("01", "Résumé de la run", "Vue d'ensemble des modèles et tests exécutés.")

st.markdown(
    "<div class='kpi-grid'>"
    "<div class='kpi-cell'><div class='kpi-label'>Total</div><div class='kpi-value'>" + str(total) + "</div></div>"
    "<div class='kpi-cell'><div class='kpi-label'>Succès</div><div class='kpi-value c-green'>" + str(success_n) + "</div></div>"
    "<div class='kpi-cell'><div class='kpi-label'>Erreurs</div><div class='kpi-value " + ("c-red" if error_n > 0 else "c-green") + "'>" + str(error_n) + "</div></div>"
    "<div class='kpi-cell'><div class='kpi-label'>Modèles</div><div class='kpi-value c-blue'>" + str(model_n) + "</div></div>"
    "<div class='kpi-cell'><div class='kpi-label'>Tests</div><div class='kpi-value'>" + str(test_n) + "</div></div>"
    "<div class='kpi-cell'><div class='kpi-label'>Temps total</div><div class='kpi-value'>" + f"{total_t:.1f}" + "<span style='font-size:1rem;font-weight:500;color:#94a3b8;'> s</span></div></div>"
    "</div>",
    unsafe_allow_html=True,
)
st.markdown(
    '<div style="font-size:0.75rem;color:#94a3b8;margin-bottom:1.5rem;">Run ID : <code>' + run_id + '</code> · Exécuté à ' + created_at + '</div>',
    unsafe_allow_html=True,
)

#===================
# Section 02 Results
#===================

filtered = [r for r in clean_rows
            if (r.get("resource_type") or "").lower() in filter_type
            and (r.get("status") or "").lower() in filter_status]

section_banner("02", "Résultats par ressource", "Modèles et tests filtrés par type et statut.")


def _build_row(row: dict) -> str:
    name  = row.get("model_name") or "—"
    rtype = row.get("resource_type") or "—"
    s     = row.get("status") or "unknown"
    t     = row.get("execution_time")
    msg   = row.get("message") or ""
    t_str = f"{t:.2f} s" if t is not None else "—"
    msg_html = ('<div style="font-size:0.72rem;color:#94a3b8;margin-top:0.15rem;">' + msg + '</div>') if msg and rtype == "test" else ""
    return (
        '<div class="model-row">'
        '<div><div class="model-type-tag">' + rtype + '</div>'
        '<div class="model-name">' + name + '</div>' + msg_html + '</div>'
        '<div style="display:flex;align-items:center;gap:1rem;">'
        '<span class="model-time">' + t_str + '</span>' + badge_html(s) + '</div>'
        '</div>'
    )


def render_group(items: list, title: str) -> None:
    if not items:
        return
    st.markdown('<div class="sec-label">' + title + " (" + str(len(items)) + ")</div>", unsafe_allow_html=True)
    rows_html = "".join(_build_row(r) for r in sorted(items, key=lambda x: x.get("model_name") or ""))
    st.markdown('<div class="model-row-wrap">' + rows_html + '</div>', unsafe_allow_html=True)


if not filtered:
    st.info("Aucun résultat correspondant aux filtres.")
else:
    models = [r for r in filtered if r.get("resource_type") == "model"]
    tests  = [r for r in filtered if r.get("resource_type") == "test"]
    others = [r for r in filtered if r.get("resource_type") not in ("model", "test")]
    col_m, col_t = st.columns(2)
    with col_m:
        render_group(models, "Modèles")
        render_group(others, "Autres")
    with col_t:
        render_group(tests, "Tests")

#===========================
# Section 03 Exec time chart
#===========================

model_rows_with_time = [r for r in clean_rows if r.get("resource_type") == "model" and r.get("execution_time")]
if model_rows_with_time:
    import plotly.graph_objects as go

    section_banner("03", "Temps d'exécution", "Durée de matérialisation par modèle dbt.")

    sorted_rows = sorted(model_rows_with_time, key=lambda r: r["execution_time"], reverse=False)
    names  = [r.get("model_name") or "—" for r in sorted_rows]
    values = [r["execution_time"] for r in sorted_rows]

    fig = go.Figure(go.Bar(
        x=values,
        y=names,
        orientation="h",
        marker=dict(
            color=values,
            colorscale=[[0, "#bfdbfe"], [1, "#1d4ed8"]],
            line=dict(width=0),
        ),
        text=[f"{v:.2f} s" for v in values],
        textposition="outside",
        textfont=dict(size=11, color="#374151", family="Inter, sans-serif"),
        hovertemplate="<b>%{y}</b><br>%{x:.2f} s<extra></extra>",
        cliponaxis=False,
    ))

    fig.update_layout(
        plot_bgcolor="#ffffff",
        paper_bgcolor="#ffffff",
        margin=dict(l=0, r=70, t=8, b=8),
        height=max(240, len(names) * 44),
        xaxis=dict(
            showgrid=True, gridcolor="rgba(226,232,240,0.8)",
            zeroline=False, showline=False,
            tickfont=dict(size=11, color="#94a3b8", family="Inter, sans-serif"),
        ),
        yaxis=dict(
            showgrid=False, showline=False,
            tickfont=dict(size=12, color="#1e293b", family="JetBrains Mono, monospace"),
        ),
        font=dict(family="Inter, sans-serif"),
    )

    st.markdown("""
    <style>
    div[data-testid="stPlotlyChart"] {
        background: #ffffff;
        border: 1px solid #e8eaed;
        border-radius: 14px;
        overflow: hidden;
        padding: 0.5rem;
    }
    </style>
    <div style="font-size:0.68rem;font-weight:700;letter-spacing:0.10em;text-transform:uppercase;
    color:#94a3b8;margin-bottom:0.6rem;">
    Durée par modèle (secondes)</div>
    """, unsafe_allow_html=True)

    st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})