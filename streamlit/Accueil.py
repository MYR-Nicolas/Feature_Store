"""
Home — BTC Feature Store
Homepage: project overview + structured specifications.
"""

import streamlit as st
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__)))
from utils._style import GLOBAL_CSS

st.set_page_config(
  page_title="BTC Feature Store",
  page_icon="",
  layout="wide",
)

st.markdown(GLOBAL_CSS, unsafe_allow_html=True)

#=====
# HERO
#=====

st.markdown("""
<div class="hero-box">
 <div style="display:flex; align-items:center; gap:0.75rem; margin-bottom:0.6rem;">
  <span style="font-size:2rem;"></span>
  <div>
   <div style="font-size:1.6rem; font-weight:800; letter-spacing:-0.03em; line-height:1.1;">
    BTC Feature Store
   </div>
   <div style="font-size:0.85rem; opacity:0.75; margin-top:0.2rem; font-weight:400;">
    Data Lakehouse - GCP - Production-grade ML pipeline
   </div>
  </div>
 </div>
 <p style="font-size:0.95rem; opacity:0.9; line-height:1.65; margin:0 0 1rem 0;">
  A structured feature store built on BTC/USD 1 minute OHLCV data - from raw API ingestion
  to a versioned feature table ready for ML model training and backtesting.
 </p>
 <div>
  <span class="badge"style="background:rgba(255,255,255,0.12); color:white; border-color:rgba(255,255,255,0.3);">GCP Cloud Run</span>
  <span class="badge"style="background:rgba(255,255,255,0.12); color:white; border-color:rgba(255,255,255,0.3);">BigQuery</span>
  <span class="badge"style="background:rgba(255,255,255,0.12); color:white; border-color:rgba(255,255,255,0.3);">dbt</span>
  <span class="badge"style="background:rgba(255,255,255,0.12); color:white; border-color:rgba(255,255,255,0.3);">Docker</span>
  <span class="badge"style="background:rgba(255,255,255,0.12); color:white; border-color:rgba(255,255,255,0.3);">GitHub Actions CI/CD</span>
  <span class="badge"style="background:rgba(255,255,255,0.12); color:white; border-color:rgba(255,255,255,0.3);">Workload Identity Federation</span>
  <span class="badge"style="background:rgba(255,255,255,0.12); color:white; border-color:rgba(255,255,255,0.3);">Cloud Scheduler</span>
 </div>
</div>
""", unsafe_allow_html=True)

#==========
# KPI CARDS
#==========

st.markdown("""
<div class="kpi-grid">
 <div class="kpi-cell">
  <div class="kpi-label">Granularity</div>
  <div class="kpi-value">1 min</div>
 </div>
 <div class="kpi-cell">
  <div class="kpi-label">Asset</div>
  <div class="kpi-value"style="font-size:1.5rem;">BTC/USD</div>
 </div>
 <div class="kpi-cell">
  <div class="kpi-label">Features (8 families)</div>
  <div class="kpi-value c-blue">30</div>
 </div>
 <div class="kpi-cell">
  <div class="kpi-label">Ingestion frequency</div>
  <div class="kpi-value"style="font-size:1.4rem;">Weekly</div>
 </div>
 <div class="kpi-cell">
  <div class="kpi-label">API sources</div>
  <div class="kpi-value c-blue">3</div>
 </div>
 <div class="kpi-cell">
  <div class="kpi-label">Market</div>
  <div class="kpi-value"style="font-size:1.3rem;">24/7</div>
 </div>
</div>
""", unsafe_allow_html=True)

#=============
# ARCHITECTURE
#=============

st.markdown("### Pipeline Architecture")

def _pipeline_node(label, subtitle, bg, border, color):
  return f"""<div style="display:flex; align-items:center; gap:0.6rem; margin-bottom:0.1rem;">
 <div style="background:{bg}; border:1px solid {border}; border-radius:10px;
       padding:0.35rem 0.65rem; font-size:0.82rem; font-weight:600;
       color:{color}; min-width:130px; text-align:center; white-space:nowrap;">
  {label}
 </div>
 <div style="color:#94a3b8; font-size:0.79rem;">{subtitle}</div>
</div>"""

_arrow = '<div style="color:#cbd5e1; padding-left:60px; font-size:0.85rem; line-height:1;">↓</div>'

left_nodes = [
  ("External APIs", "Binance - Coinbase - CoinAPI",       "#eff6ff", "#bfdbfe", "#1d4ed8"),
  ("ELT Python",   "Extract - validate - load Parquet → GCS",  "#f0fdf4", "#a7f3d0", "#065f46"),
  ("BigLake Table", "External Table BQ → GCS (no data copy)",  "#fefce8", "#fde68a", "#92400e"),
  ("dbt (BigQuery)", "Silver → Intermediate → Marts",       "#fdf4ff", "#e9d5ff", "#7c3aed"),
  ("Feature Store", "mart.features - labels - versions",     "#fff7ed", "#fdba74", "#9a3412"),
]

right_nodes = [
  ("GitHub Actions",  "Push → test → build → deploy",  "#eff6ff", "#bfdbfe", "#1d4ed8"),
  ("Artifact Registry", "Versioned Docker image",     "#f0fdf4", "#a7f3d0", "#065f46"),
  ("Cloud Run Job",   "btc-pipeline - serverless",    "#fff7ed", "#fdba74", "#9a3412"),
  ("Cloud Scheduler",  "Automated weekly trigger",    "#fdf4ff", "#e9d5ff", "#7c3aed"),
]

def _build_pipeline_html(title, nodes):
  inner = _arrow.join(_pipeline_node(*n) for n in nodes)
  return f"""
<div style="background:rgba(255,255,255,0.90); border:1px solid rgba(226,232,240,0.95);
      border-radius:18px; padding:1.1rem 1.2rem;
      box-shadow:0 8px 24px rgba(15,23,42,0.05);">
 <div style="font-size:0.68rem; font-weight:700; text-transform:uppercase;
       letter-spacing:0.1em; color:#94a3b8; margin-bottom:0.9rem;">{title}</div>
 {inner}
</div>"""

col_a, col_b, col_c = st.columns([1, 0.08, 1])
with col_a:
  st.markdown(_build_pipeline_html("Ingestion &amp; Transform — Data Lakehouse", left_nodes),
        unsafe_allow_html=True)
with col_c:
  st.markdown(_build_pipeline_html("Infra &amp; CI/CD", right_nodes),
        unsafe_allow_html=True)


#===============
# SPECIFICATIONS
#===============

st.markdown("### Project Specifications")

with st.expander("Objective & scope"):
  st.markdown("""
Build a **structured and reproducible feature store** dedicated to Bitcoin (BTC),
designed to standardize feature engineering for training, evaluation, and backtesting
of ML/DL models applied to crypto time series.

**Scope**
- Asset: BTC/USD
- Granularity: 1 minute
- Coverage: historical data + weekly incremental ingestion
- Target use cases: supervised training, backtesting, batch inference
""")

with st.expander("Business requirements"):
  st.markdown("The feature store must capture:")
  col1, col2 = st.columns(2)
  with col1:
    st.markdown("""
**Price dynamics**
- Multi-horizon log-returns (1m / 5m / 15m / 60m)
- Momentum, acceleration, reversals

**Risk & uncertainty**
- Rolling volatility (60m, 1d)
- Market regime adaptation
""")
  with col2:
    st.markdown("""
**Market activity**
- Raw volume, log-volume, moving average
- Liquidity and engagement proxy

**Trend structure**
- Ichimoku indicators (balance, congestion, directional regimes)
- Explicit crossover signals
""")

with st.expander("Business constraints"):
  st.markdown("""
| Constraint | Detail |
|---|---|
| 24/7 market | No closing session — strict temporal continuity |
| High volatility | Frequent regime changes |
| Rolling windows | Based solely on temporal order (no calendar date) |
| Heterogeneous liquidity | Use of `log(volume)` + moving average to smooth |
| Reproducibility | Every transformation must be versioned and replayable |
""")

with st.expander("Technical constraints"):
  col1, col2 = st.columns(2)
  with col1:
    st.markdown("""
**Performance & architecture**
- Materialized feature table
- Temporal index for fast retrieval
- Compatible with batch processing and ML training
- dbt transformation versioning
""")
  with col2:
    st.markdown("""
**Data leakage prevention**
- Strictly backward-looking time windows
- No future information used
- Strict feature/target temporal alignment
- Compatibility with realistic backtesting
""")

with st.expander("Data sources & storage — Data Lakehouse architecture"):

  st.markdown("""
<div style="background:linear-gradient(135deg,#eff6ff,#f5f3ff); border:1px solid #c7d2fe;
      border-radius:14px; padding:0.9rem 1.1rem; margin-bottom:1rem;">
 <div style="font-size:0.72rem; font-weight:700; text-transform:uppercase; letter-spacing:0.09em;
       color:#4338ca; margin-bottom:0.4rem;">Data Lakehouse Pattern</div>
 <div style="font-size:0.88rem; color:#1e1b4b; line-height:1.6;">
  This project implements a <strong>Data Lakehouse</strong> pattern: raw Parquet files are stored on
  <strong>GCS (lake)</strong> and exposed directly in BigQuery via a <strong>BigLake external table</strong>,
  with no copy or additional ETL step. dbt then operates on this source to build the
  Silver → Intermediate → Marts layers - combining lake flexibility with warehouse analytics power.
 </div>
</div>
""", unsafe_allow_html=True)

  col1, col2 = st.columns(2)
  with col1:
    st.markdown("""
**API sources (extraction)**
- Binance
- Coinbase
- CoinAPI

Format: JSON - OHLCV - 1 min - BTC/USD 
Frequency: weekly

---

**Bronze layer — GCS (Lake)** 
Date partitioned Parquet files written by the ELT Python job.

| Field | Type |
|---|---|
| `open_time` / `close_time` | TIMESTAMP |
| `open` / `high` / `low` / `close` | FLOAT64 |
| `volume` | FLOAT64 |
| `symbol` / `interval_value` / `source` | STRING |
| `extracted_at` | TIMESTAMP |

""")
  with col2:
    st.markdown("""
**BigLake external table - Lake → Warehouse bridge**

`EXTERNAL TABLE` in BigQuery connected to the GCS bucket via a **BigLake connection**. 
Direct Parquet read without ingestion into BQ - zero data duplication.

---

**dbt layers (BigQuery — Warehouse)**

| Layer | Table | Content |
|---|---|---|
| Silver | `btc_ohlcv_1m_silver` | Cleaned, deduplicated OHLCV |
| Intermediate | `int_btc_base_returns` | Computed log-returns |
| Intermediate | `int_btc_ichimoku` | Ichimoku indicators |
| Intermediate | `int_btc_features` | Full feature assembly (view) |
| Marts | `mart.features` | Final feature store (incremental) |
| Marts | `mart.labels` | Aligned `future_logret` target |
| Marts | `mart.feature_versions` | Schema versioning |
""")

with st.expander("Feature list (8 families - 30 features)"):
  import pandas as pd
  features = [
    {
      "Family": "Returns",
      "Features": "logret_1m - logret_5m - logret_15m - logret_60m",
      "N": 4,
      "Role": "Multi-horizon price dynamics — momentum, acceleration, reversals",
    },
    {
      "Family": "Return Lags",
      "Features": "lag_logret_1m_1 - lag_logret_1m_5 - lag_logret_1m_15 - lag_logret_1m_60",
      "N": 4,
      "Role": "Temporal memory — stabilizes learning in tabular models",
    },
    {
      "Family": "Volatility",
      "Features": "vol_60m - vol_1d - vol_annualized - vol_ratio",
      "N": 4,
      "Role": "Risk regimes: rolling std, annualized (x√525,600), short/long-term ratio",
    },
    {
      "Family": "Volume",
      "Features": "log_volume - vol_ma_60m - vol_zscore",
      "N": 3,
      "Role": "Market activity intensity — filters moves in low-liquidity conditions",
    },
    {
      "Family": "Ichimoku",
      "Features": "tenkan - kijun - span_a - span_b",
      "N": 4,
      "Role": "Trend structure and Ichimoku equilibrium zones (raw values)",
    },
    {
      "Family": "Ichimoku Derivatives",
      "Features": "tenkan_dist - kijun_dist - span_a_dist - span_b_dist - cloud_thickness",
      "N": 5,
      "Role": "Normalized distance to price — measures tension against key levels",
    },
    {
      "Family": "Binary signals",
      "Features": "tenkan_sup_kijun - price_sup_cloud - price_sub_cloud - lag_price_sup_cloud_5",
      "N": 4,
      "Role": "Explicit regime signals for non-linear models (0/1)",
    },
    {
      "Family": "Target",
      "Features": "future_logret",
      "N": 1,
      "Role": "Temporally shifted target — leak-free alignment (supervised learning)",
    },
  ]
  df_feat = pd.DataFrame(features)
  st.dataframe(
    df_feat,
    width='stretch',
    hide_index=True,
    column_config={
      "N": st.column_config.NumberColumn("N", width="small"),
    },
  )
  total = sum(f["N"] for f in features)
  st.caption(f"Total: **{total} features** materialized in `mart.features` — versioned via `mart.feature_versions`")

with st.expander("References"):
  st.markdown("""
- **Ichimoku Analyses & Strategies** — *Charles G. Koonitz*, 2022 
 *How to Detect Market Trends for Stocks, Cryptocurrency, and Forex by Combining Technical Analysis and the Ichimoku Cloud (2nd ed.)*
""")


# ─── FOOTER ──────────────────────────────────────────────────────────────────

st.markdown("---")
st.markdown("""
<div style="text-align:center; font-size:0.78rem; color:#94a3b8; padding: 0.5rem 0 1rem;">
 BTC Feature Store - GCP Data Lakehouse - 
 <a href="https://github.com/MYR-Nicolas/Feature_Store"style="color:#3b82f6; text-decoration:none;">
  GitHub ↗
 </a>
</div>
""", unsafe_allow_html=True)