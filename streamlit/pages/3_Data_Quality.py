"""Page 3 — Data Quality — reads from GCS latest.json"""
import os
import sys

import streamlit as st

sys.path.insert(0, os.path.dirname(__file__))
from utils._style import inject_css, hero, section_banner, fmt_ts, fmt_num, sidebar_header, fetch_gcs_cache

st.set_page_config(page_title="Data Quality - BTC Feature Store", layout="wide")
inject_css()

DEMO_METRICS = {
    "row_count": 10080.0, "column_count": 6.0,
    "null_count": 0.0, "duplicate_count": 0.0,
    "min_timestamp": 1745107200.0, "max_timestamp": 1745712000.0,
}

#========
# Sidebar
#========
with st.sidebar:
    sidebar_header()
    refresh_btn = st.button("↻  Refresh", use_container_width=True)
    st.divider()
    st.markdown('<div style="font-size:0.7rem;font-weight:700;color:#64748b;text-transform:uppercase;letter-spacing:0.08em;margin-bottom:0.75rem;">Alert Thresholds</div>', unsafe_allow_html=True)
    null_warn   = st.number_input("Nulls — warn (≥)",       min_value=0, value=5,  step=1)
    null_danger = st.number_input("Nulls — danger (≥)",     min_value=0, value=50, step=1)
    dup_warn    = st.number_input("Duplicates — warn (≥)",  min_value=0, value=1,  step=1)
    dup_danger  = st.number_input("Duplicates — danger (≥)", min_value=0, value=10, step=1)

#==========
# Load data
#==========

payload, source = fetch_gcs_cache(force=refresh_btn)

is_demo = False
is_stale = False

if payload is None:
    metrics    = DEMO_METRICS
    run_id     = "demo-run-001"
    created_at = "2025-04-28T08:04:32+00:00"
    fetched_at = "—"
    is_demo    = True
else:
    dq_data    = payload.get("quality", {})
    metrics    = dq_data.get("metrics", {})
    run_id     = dq_data.get("run_id", "—")
    created_at = fmt_ts(dq_data.get("created_at"))
    fetched_at = fmt_ts(dq_data.get("_fetched_at") or payload.get("_exported_at"))
    is_stale   = (source == "stale")

#===============
# Unpack metrics
#===============

row_count  = int(metrics.get("row_count", 0))
col_count  = int(metrics.get("column_count", 0))
null_count = int(metrics.get("null_count", 0))
dup_count  = int(metrics.get("duplicate_count", 0))
min_ts     = metrics.get("min_timestamp")
max_ts     = metrics.get("max_timestamp")
span_hours = (max_ts - min_ts) / 3600 if min_ts and max_ts else None
coverage_pct = min(100.0, (row_count / (span_hours * 60)) * 100) if span_hours and span_hours > 0 else None


def _color(v, w, d):
    return "c-red" if v >= d else ("c-amber" if v >= w else "c-green")

#========
# Header
#========
hero(
    eyebrow="Monitoring · Data Quality",
    title="Data Quality",
    subtitle="BTC OHLCV 1m structural validations - monitoring.data_quality - Last sync: " + fetched_at,
)

if is_demo:
    st.markdown('<div class="stale-banner">⚡ Demo data - configure <code>GCS_CACHE_URL</code> in your secrets and run the pipeline.</div>', unsafe_allow_html=True)
elif is_stale:
    err = st.session_state.get("_gcs_error", "")
    msg = (" — " + err[:100]) if err else ""
    st.markdown('<div class="stale-banner">⚠ GCS cache unavailable — displaying last known data.' + msg + '</div>', unsafe_allow_html=True)

#================
# Section 01 KPIs
#================

section_banner("01", "Structural Metrics", "Volume, schema, nulls, duplicates and temporal completeness.")

null_col = _color(null_count, null_warn, null_danger)
dup_col  = _color(dup_count, dup_warn, dup_danger)
cov_val  = f"{coverage_pct:.1f}" if coverage_pct is not None else "—"
cov_col  = ("c-green" if (coverage_pct or 0) >= 95 else "c-amber" if (coverage_pct or 0) >= 80 else "c-red") if coverage_pct is not None else ""

st.markdown(
    "<div class='kpi-grid'>"
    "<div class='kpi-cell'><div class='kpi-label'>Rows</div><div class='kpi-value c-blue'>" + fmt_num(row_count) + "</div><div class='kpi-sub'>in batch</div></div>"
    "<div class='kpi-cell'><div class='kpi-label'>Columns</div><div class='kpi-value'>" + str(col_count) + "</div><div class='kpi-sub'>OHLCV schema</div></div>"
    "<div class='kpi-cell'><div class='kpi-label'>Null Values</div><div class='kpi-value " + null_col + "'>" + fmt_num(null_count) + "</div><div class='kpi-sub'>across all columns</div></div>"
    "<div class='kpi-cell'><div class='kpi-label'>Duplicates</div><div class='kpi-value " + dup_col + "'>" + fmt_num(dup_count) + "</div><div class='kpi-sub'>duplicate rows</div></div>"
    "<div class='kpi-cell'><div class='kpi-label'>1m Coverage</div><div class='kpi-value " + cov_col + "'>" + cov_val + "<span style='font-size:1rem;font-weight:500;color:#94a3b8;'>%</span></div><div class='kpi-sub'>completeness</div></div>"
    "</div>",
    unsafe_allow_html=True,
)

#=============================
# Section 02 Temporal coverage
#=============================
section_banner("02", "Temporal Coverage", "Range and completeness of the 1-minute OHLCV series.")

col_ts1, col_ts2, col_ts3 = st.columns(3)
col_ts1.metric("Min Timestamp", fmt_ts(min_ts) if min_ts else "—")
col_ts2.metric("Max Timestamp", fmt_ts(max_ts) if max_ts else "—")
col_ts3.metric("Range", f"{span_hours:.0f} h · {span_hours/24:.1f} d" if span_hours else "—")

if coverage_pct is not None:
    fill_color = "#059669" if coverage_pct >= 95 else ("#d97706" if coverage_pct >= 80 else "#dc2626")
    st.markdown(
        "<div style='margin:0.75rem 0 0.25rem;'>"
        "<div style='display:flex;justify-content:space-between;font-size:0.82rem;color:#64748b;margin-bottom:0.4rem;'>"
        "<span>1m series completeness</span>"
        "<strong style='color:" + fill_color + ";'>" + f"{coverage_pct:.1f}%" + "</strong></div>"
        "<div class='prog-wrap' style='height:8px;'><div class='prog-fill' style='width:" + f"{min(coverage_pct,100):.1f}%" + ";background:" + fill_color + ";'></div></div>"
        "<div style='display:flex;justify-content:space-between;font-size:0.72rem;color:#94a3b8;margin-top:0.3rem;'>"
        "<span>" + fmt_num(row_count) + " rows present</span>"
        "<span>" + (fmt_num(int(span_hours * 60)) if span_hours else "—") + " expected</span></div></div>",
        unsafe_allow_html=True,
    )

#==========================
# Section 03 Quality checks
#==========================

section_banner("03", "Quality Checks", "Automated validation of structural criteria for the batch.")


def _check_row_html(is_ok, is_warn_ok, label, detail):
    if is_ok:
        icon, color = "✓", "#059669"
    elif is_warn_ok:
        icon, color = "△", "#d97706"
    else:
        icon, color = "✕", "#dc2626"
    return (
        '<div class="check-row">'
        '<div style="color:' + color + ';font-weight:800;min-width:1.2rem;text-align:center;font-size:0.95rem;margin-top:0.05rem;">' + icon + '</div>'
        '<div><div class="check-text"><strong>' + label + '</strong></div>'
        '<div class="check-sub">' + detail + '</div></div>'
        '</div>'
    )


checks = [
    (null_count == 0,        null_count < null_warn,  "Null Values",   (fmt_num(null_count) + " null(s) detected") if null_count > 0 else "No null values — OK"),
    (dup_count == 0,         dup_count < dup_warn,    "Duplicates",    (fmt_num(dup_count) + " duplicate(s) detected") if dup_count > 0 else "No duplicates — OK"),
    (row_count > 1000,       row_count > 100,         "Volume",        fmt_num(row_count) + " rows — " + ("sufficient" if row_count > 1000 else "low volume")),
    (col_count >= 5,         col_count >= 4,          "Schema",        str(col_count) + " columns — " + ("complete OHLCV" if col_count >= 5 else "missing columns")),
    ((coverage_pct or 0) >= 95, (coverage_pct or 0) >= 80, "1m Coverage", cov_val + "% — " + ("continuous series" if (coverage_pct or 0) >= 95 else "potential gaps")),
]

checks_html = "".join(_check_row_html(*c) for c in checks)
st.markdown(
    '<div style="border:1px solid rgba(226,232,240,0.9);border-radius:14px;padding:0.25rem 1.1rem;'
    'background:rgba(255,255,255,0.96);box-shadow:0 4px 12px rgba(15,23,42,0.04);">'
    + checks_html + '</div>',
    unsafe_allow_html=True,
)
st.markdown(
    '<div style="font-size:0.75rem;color:#94a3b8;margin-top:1rem;">Run ID: <code>' + run_id + '</code> · Computed at ' + created_at + '</div>',
    unsafe_allow_html=True,
)

