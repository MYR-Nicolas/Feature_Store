import streamlit as st
import pandas as pd

st.set_page_config(
    page_title="Feature Store",
    layout="wide",
)

st.title("FEATURE STORE")
st.subheader("Project Objective")

st.markdown(
"""
Build a structured and reproducible feature store dedicated to Bitcoin (BTC),
aimed at standardizing feature generation for training,
evaluation, and backtesting of machine learning and deep learning models
applied to crypto-financial time series.
"""
)

sections = [
    "Business Requirements",
    "Business Constraints",
    "Technical Constraints",
    "Data Sources",
    "Feature List",
    "References",
]
choice = st.sidebar.radio("Table of Contents", sections)

if choice == "Business Requirements":
    st.subheader("Key Bitcoin Dynamics to Capture")
    st.markdown(
        """
The feature store must capture:

- **Price dynamics** through **multi-horizon logarithmic returns** (momentum, acceleration, reversals)
- **Risk & uncertainty** via **rolling volatility measures**
- **Market activity intensity** using **trading volume** (liquidity / engagement proxy)
- **Trend structure** using **Ichimoku indicators** (equilibrium, congestion, directional regimes)
"""
    )

elif choice == "Business Constraints":
    st.subheader("Business Constraints")
    st.markdown(
        """
- Cryptocurrency market operates **continuously (24/7, no closing sessions)**
- Rolling windows must rely **strictly on temporal ordering**
- **High volatility** and **frequent regime shifts**
- Use of **logarithmic returns** and **multi-horizon rolling volatility**
- **Heterogeneous and time-varying liquidity**
- Use of **log(volume)** and **moving averages** to smooth activity
"""
    )

elif choice == "Technical Constraints":
    st.subheader("Technical Constraints")

    col1, col2 = st.columns(2)

    with col1:
        st.markdown("### Performance & Architecture")
        st.markdown(
        """
    - **Materialized feature table** to avoid recomputation
    - Time-based indexing for fast retrieval
    - Pipeline compatible with batch processing and ML training
    - Feature reproducibility
    - Transformation versioning
    """
    )   

    with col2:
        st.markdown("### Data Leakage Prevention")
        st.markdown(
        """
    - Systematic use of **lag features**
    - Strictly backward-looking time windows
    - No use of future information
    - Compatibility with realistic backtesting
    - Strict feature/target alignment
    """
    )

elif choice == "Data Sources":
    st.subheader("Data Sources & Storage")

    st.markdown(
    """
### Primary Source
Market data is sourced from a centralized PostgreSQL database acting as a data warehouse.

**Database**
- PostgreSQL
- Schema: `market_data`
- Table: `btc_ohlcv`
- Granularity: 1 minute
- Asset: BTC/USDT

**Structure**
- timestamp (PK)
- open
- high
- low
- close
- volume

### Feature Store
Features are materialized in a dedicated table:

- Schema: `feature_store`
- Table: `btc_feature_store`
- Index: timestamp
- Usage: ML training / backtesting / inference

### Pipeline
API → ingestion → PostgreSQL → feature engineering → feature store
"""
    )

elif choice == "Feature List":
    st.subheader("Feature List")

    features = [
    {
        "Family": "Returns",
        "Features": "logret_1m / 5m / 15m / 60m",
        "Role": "Capture price dynamics across multiple time horizons for ML and DL models."
    },
    {
        "Family": "Return Lags",
        "Features": "lag_logret_n",
        "Role": "Introduce temporal memory into tabular models and stabilize learning."
    },
    {
        "Family": "Volatility",
        "Features": "vol_60m, vol_1d",
        "Role": "Enable models to adapt predictions to market volatility regimes."
    },
    {
        "Family": "Volume",
        "Features": "volume, log_volume, vol_ma",
        "Role": "Measure market activity intensity and filter low-liquidity movements."
    },
    {
        "Family": "Ichimoku",
        "Features": "tenkan, kijun, span_a, span_b",
        "Role": "Capture trend structure and market equilibrium zones."
    },
    {
        "Family": "Derived",
        "Features": "tenkan_dist, kijun_dist, cloud_width",
        "Role": "Normalize trend-related information to improve model generalization."
    },
    {
        "Family": "Signals",
        "Features": "tenkan_cross, price_vs_cloud",
        "Role": "Provide explicit regime signals to support non-linear learning."
    },
    {
        "Family": "Target",
        "Features": "future_logret",
        "Role": "Time-aligned target variable for supervised learning."
    },
]

    df = pd.DataFrame(features, columns=["Family", "Features", "Role"])
    st.table(df)

elif choice == "References":
    st.subheader("References")
    st.markdown(
        """
- **Ichimoku Analyses & Strategies**: How to Detect Market Trends for Stocks, Cryptocurrency, and Forex by Combining Technical Analysis and the Ichimoku Cloud (Second Edition) — *Paperback, November 8, 2022*, Charles G. Koonitz
"""
    )