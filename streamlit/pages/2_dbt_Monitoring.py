"""Page 2 — dbt Monitoring — lit depuis GCS latest.json"""
import os
import sys
import plotly.graph_objects as go

import streamlit as st

sys.path.insert(0, os.path.dirname(__file__))
from _style import inject_css, hero, section_banner, fmt_ts, fmt_num, badge_html, sidebar_header, fetch_gcs_cache

st.set_page_config(page_title="dbt Monitoring · BTC Feature Store", layout="wide")
inject_css()

DEMO_ROWS = [
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
]

#========
# Sidebar
#========

with st.sidebar:
    sidebar_header()
    refresh_btn = st.button("↻  Rafraîchir", use_container_width=True)
    st.divider()
    st.markdown('<div style="font-size:0.7rem;font-weight:700;color:#64748b;text-transform:uppercase;letter-spacing:0.08em;margin-bottom:0.5rem;">Filtres</div>', unsafe_allow_html=True)
    filter_type   = st.multiselect("Type",   ["model", "test", "seed", "snapshot"], default=["model", "test"])
    filter_status = st.multiselect("Statut", ["success", "error", "warn", "skip"],  default=["success", "error", "warn"])

#==========
# Load data
#==========

payload, source = fetch_gcs_cache(force=refresh_btn)

is_demo = False
is_stale = False

if payload is None:
    clean_rows = DEMO_ROWS
    fetched_at = "—"
    is_demo = True
else:
    dbt_data   = payload.get("dbt", {})
    clean_rows = dbt_data.get("rows", [])
    fetched_at = fmt_ts(dbt_data.get("_fetched_at") or payload.get("_exported_at"))
    is_stale   = (source == "stale")

#========
# Header
#========

hero(
    eyebrow="Monitoring · Transformations dbt",
    title="dbt Run Results",
    subtitle="Statuts des modèles et tests dbt — monitoring.dbt_results · Dernière sync : " + fetched_at,
)

if is_demo:
    st.markdown('<div class="stale-banner">⚡ Données de démonstration — configurez <code>GCS_CACHE_URL</code> dans vos secrets et lancez le pipeline.</div>', unsafe_allow_html=True)
elif is_stale:
    err = st.session_state.get("_gcs_error", "")
    msg = (" — " + err[:100]) if err else ""
    st.markdown('<div class="stale-banner">⚠ Cache GCS indisponible — affichage des dernières données connues.' + msg + '</div>', unsafe_allow_html=True)

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
created_at = fmt_ts(clean_rows[0].get("created_at") if clean_rows else None)

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

    section_banner("03", "Temps d'exécution", "Durée de matérialisation par modèle dbt.")

    sorted_rows = sorted(model_rows_with_time, key=lambda r: r["execution_time"], reverse=False)
    names  = [r.get("model_name") or "—" for r in sorted_rows]
    values = [r["execution_time"] for r in sorted_rows]

    fig = go.Figure(go.Bar(
        x=values, y=names, orientation="h",
        marker=dict(color=values, colorscale=[[0, "#bfdbfe"], [1, "#1d4ed8"]], line=dict(width=0)),
        text=[f"{v:.2f} s" for v in values],
        textposition="outside",
        textfont=dict(size=11, color="#374151", family="Inter, sans-serif"),
        hovertemplate="<b>%{y}</b><br>%{x:.2f} s<extra></extra>",
        cliponaxis=False,
    ))
    fig.update_layout(
        plot_bgcolor="#ffffff", paper_bgcolor="#ffffff",
        margin=dict(l=0, r=70, t=8, b=8),
        height=max(240, len(names) * 44),
        xaxis=dict(showgrid=True, gridcolor="rgba(226,232,240,0.8)", zeroline=False, showline=False,
                   tickfont=dict(size=11, color="#94a3b8", family="Inter, sans-serif")),
        yaxis=dict(showgrid=False, showline=False,
                   tickfont=dict(size=12, color="#1e293b", family="JetBrains Mono, monospace")),
        font=dict(family="Inter, sans-serif"),
    )

    st.markdown(
        "<div style='font-size:0.68rem;font-weight:700;letter-spacing:0.10em;text-transform:uppercase;color:#94a3b8;margin-bottom:0.5rem;'>Durée par modèle (secondes)</div>"
        "<style>div[data-testid='stPlotlyChart']{background:#ffffff;border:1px solid #e8eaed;border-radius:14px;overflow:hidden;padding:0.4rem;}</style>",
        unsafe_allow_html=True,
    )
    st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})
