# BTC Feature Store — GCP Data Lakehouse

> Weekly ELT pipeline extracting BTC/USD OHLCV data, transforming it into ML-ready features via dbt on BigQuery, and deploying automatically to Cloud Run via GitHub Actions.

[![CI/CD](https://img.shields.io/badge/CI%2FCD-GitHub%20Actions-2088FF?logo=github-actions&logoColor=white)](https://github.com/MYR-Nicolas/Feature_Store/actions)
[![Cloud](https://img.shields.io/badge/Cloud-GCP-4285F4?logo=google-cloud&logoColor=white)](https://cloud.google.com/)
[![dbt](https://img.shields.io/badge/Transform-dbt%20BigQuery-FF694B?logo=dbt&logoColor=white)](https://www.getdbt.com/)
[![Python](https://img.shields.io/badge/Python-3.13-3776AB?logo=python&logoColor=white)](https://www.python.org/)
[![Docker](https://img.shields.io/badge/Container-Docker-2496ED?logo=docker&logoColor=white)](https://www.docker.com/)
[![Live Demo](https://img.shields.io/badge/Demo-Streamlit-FF4B4B?logo=streamlit&logoColor=white)](https://featurestore-ddtx8txqfrtjrnlozeybu5.streamlit.app/Pipeline_Monitoring)

---

## Overview

This project implements a production-grade **feature store** for Bitcoin price data, built on Google Cloud Platform. It demonstrates a complete MLOps data lakehouse pattern: raw market data is ingested weekly from multiple APIs, stored in GCS as Parquet, transformed into ML-ready features in BigQuery via dbt, and monitored through a Streamlit dashboard.

The pipeline runs automatically every week via **Cloud Scheduler → Cloud Run Job**, deployed on every push to `main` through a GitHub Actions CI/CD workflow using **Workload Identity Federation** (no long-lived service account keys).

---

## Architecture

```
┌──────────────────────────────────────────────────────────────────┐
│                        GitHub Actions CI/CD                      │
│  push to main → pytest → Docker build → Artifact Registry        │
│               → Cloud Run Job update → Cloud Scheduler trigger   │
└──────────────────────────┬───────────────────────────────────────┘
                           │ weekly
                           ▼
┌─────────────────────────────────────────┐
│              ELT Pipeline               │
│                                         │
│  Extract         Load          Transform│
│  ┌──────────┐   ┌─────────┐   ┌──────┐  │
│  │ Binance  │   │   GCS   │   │ dbt  │  │
│  │ Coinbase │──▶│ Parquet │──▶│  +   │  │
│  │ CoinAPI  │   │  files  │   │  BQ  │  │
│  └──────────┘   └─────────┘   └──────┘  │
│  (fallback chain)                       │
└────────────────────────┬────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────┐
│            BigQuery Feature Store       │
│                                         │
│  bronze/  → silver/  → feature_store/   │
│  raw OHLCV  cleaned    features         │
│                        labels           │
│                        feature_versions │
└────────────────────────┬────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────┐
│         Streamlit Monitoring Dashboard  │
│ Pipeline runs · dbt results · DQ metrics│
└─────────────────────────────────────────┘
```

---

## Features

### ELT Pipeline
- **Multi-source extraction** with automatic fallback: Binance → Coinbase → CoinAPI
- **HTTP retry logic** with configurable attempts and backoff
- **OHLCV validation** — completeness ratio check, duplicate detection, OHLC consistency
- **Weekly windowing** — always extracts the previous complete UTC week (Mon 00:00 → Mon 00:00)
- **GCS storage** — Parquet files partitioned by week under a configurable prefix

### dbt Transformations (BigQuery)
Three-layer medallion architecture — silver → intermediate → marts:

| Layer | Model | Description |
|---|---|---|
| Silver | `btc_ohlcv_1m_silver` | Cleaned OHLCV from GCS via BigLake external table |
| Intermediate | `int_btc_base_returns` | Log-returns at 1m / 5m / 15m / 60m |
| Intermediate | `int_btc_ichimoku` | Ichimoku indicators (Tenkan, Kijun, Span A & B) |
| Intermediate | `int_btc_features` | Full feature set — volatility, volume, signals, lags |
| Marts | `features` | Incremental feature store table (partitioned by day) |
| Marts | `labels` | Forward log-return targets for ML training |
| Marts | `feature_versions` | Version tracking for reproducible model training |

**Engineered features include:**
- Log-returns: `logret_1m`, `logret_5m`, `logret_15m`, `logret_60m`
- Lagged returns: `lag_logret_1m_1/5/15/60`
- Volatility: `vol_60m`, `vol_1d`, `vol_annualized`, `vol_ratio`
- Volume: `log_volume`, `vol_ma_60m`, `vol_zscore`
- Ichimoku: `tenkan`, `kijun`, `span_a`, `span_b`, `cloud_thickness`
- Distance features: `tenkan_dist`, `kijun_dist`, `span_a_dist`, `span_b_dist`
- Binary signals: `tenkan_sup_kijun`, `price_sup_cloud`, `price_sub_cloud`

### CI/CD (GitHub Actions)
- **Workload Identity Federation** — keyless GCP authentication, no stored secrets
- **Automated test gate** — `pytest` must pass before any deployment
- **Cloud Build** — Docker image built and pushed to Artifact Registry
- **Cloud Run Job** — updated atomically on every successful deploy
- **Concurrency control** — `cancel-in-progress: true` prevents race conditions
- **Path filtering** — workflow only triggers on relevant file changes

### Monitoring (Streamlit)
Live dashboard with three pages:
- **Pipeline Monitoring** — run history, status, duration, row counts
- **dbt Monitoring** — model execution times, test results per run
- **Data Quality** — completeness, null rates, OHLC consistency metrics per batch

---

## Tech Stack

| Layer | Technology |
|---|---|
| Language | Python 3.13 |
| Cloud | GCP — GCS, BigQuery, Cloud Run, Artifact Registry, Cloud Scheduler |
| Transform | dbt-bigquery 1.11 |
| Containerization | Docker |
| CI/CD | GitHub Actions + Workload Identity Federation |
| Data Sources | Binance API, Coinbase API, CoinAPI |
| Monitoring | Streamlit 1.56 |
| Testing | pytest + pytest-mock |

---

## Project Structure

```
Feature_Store/
├── ELT/
│   ├── pipeline.py          # Main orchestrator (extract → load → dbt → monitor)
│   ├── extract.py           # Multi-source extraction with fallback + validation
│   ├── load.py              # GCS Parquet upload
│   ├── config.py            # Pydantic settings
│   ├── monitoring.py        # Pipeline run logging to BigQuery
│   ├── dbt_monitoring.py    # dbt results logging to BigQuery
│   ├── monitoring_data_quality.py  # Data quality metrics
│   └── export_metrics.py   # Monitoring cache export to GCS
├── dbt/
│   ├── models/
│   │   ├── silver/          # Cleaned OHLCV layer
│   │   ├── intermediate/    # Returns, Ichimoku, full features
│   │   └── marts/
│   │       └── feature_store/   # features, labels, feature_versions
│   └── macros/              # Reusable SQL macros (returns, ichimoku, signals…)
├── streamlit/
│   ├── Accueil.py
│   └── pages/
│       ├── 1_Pipeline_Monitoring.py
│       ├── 2_DBT_Monitoring.py
│       └── 3_Data_Quality.py
├── tests/                   # pytest test suite
├── biglake_bq/              # Shell scripts for BigLake + BigQuery setup
├── Dockerfile
├── requirements.txt
└── .github/workflows/
    └── deploy.yml           # CI/CD pipeline
```

---

## Local Setup

### Prerequisites
- Python 3.13
- Docker
- GCP project with BigQuery, GCS, Cloud Run, Artifact Registry enabled
- dbt-bigquery (`pip install dbt-bigquery`)

### 1. Clone & install

```bash
git clone https://github.com/MYR-Nicolas/Feature_Store.git
cd Feature_Store
pip install -r requirements.txt
pip install dbt-bigquery
```

### 2. Configure environment

Create a `.env` file at the project root:

```env
GCP_PROJECT_ID=your-project-id
GCP_LOCATION=us-central1
GCS_BUCKET_NAME=your-bucket
GCS_URI=gs://your-bucket
GCS_PREFIX=btc/ohlcv
GCS_BUCKET_NAME_CACHE=your-cache-bucket
BQ_CONNECTION_ID=your-bq-connection
BQ_DATASET=your-dataset
BQ_TABLE_NAME=btc_raw
COINAPI_KEY=your-coinapi-key        # optional, third fallback
```

### 3. Run the pipeline locally

```bash
# Run ELT pipeline
python -m ELT.pipeline

# Run dbt only
cd dbt
dbt debug --profiles-dir .
dbt run --profiles-dir .
dbt test --profiles-dir .
```

### 4. Run with Docker

```bash
docker build -t btc-pipeline .
docker run --env-file .env btc-pipeline
```

### 5. Run tests

```bash
pytest tests/ -v
```

### 6. Launch monitoring dashboard

```bash
python -m streamlit run streamlit/Accueil.py
```

---

## GCP Infrastructure Setup

The `biglake_bq/` directory contains shell scripts to provision the required GCP resources:

```bash
# Create BigQuery dataset
bash biglake_bq/create_dataset.sh

# Create BigLake connection
bash biglake_bq/create_conn_bq.sh

# Create external table on GCS Parquet files
bash biglake_bq/create_table_biglake.sh

# Grant IAM access
bash biglake_bq/grant_access.sh
```

---

## CI/CD Workflow

On every push to `main` that touches `ELT/`, `dbt/`, `streamlit/`, `Dockerfile`, or `requirements.txt`:

1. **Test job** — checks out code, installs dependencies, runs `pytest`
2. **Deploy job** (only if tests pass):
   - Authenticates to GCP via Workload Identity Federation
   - Builds Docker image with `gcloud builds submit`
   - Pushes to Artifact Registry with the commit SHA as tag
   - Updates the Cloud Run Job with the new image and environment variables

No service account key is ever stored in GitHub secrets — authentication is handled entirely through OIDC tokens.

---

## Live Demo

The monitoring dashboard is deployed on Streamlit Cloud:

**[featurestore-ddtx8txqfrtjrnlozeybu5.streamlit.app/Pipeline_Monitoring](https://featurestore-ddtx8txqfrtjrnlozeybu5.streamlit.app/Pipeline_Monitoring)**

---

## Author

**Nicolas Mayeur** — Data Engineer · MLOps · GCP  
[GitHub](https://github.com/MYR-Nicolas) · [Portfolio](https://featurestore-ddtx8txqfrtjrnlozeybu5.streamlit.app/)
