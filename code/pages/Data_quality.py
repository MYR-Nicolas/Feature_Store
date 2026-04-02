import streamlit as st
from pathlib import Path

st.title("Data Quality Control")
st.markdown(
"""
Data quality checks are limited to structural validations:
data types, duplicates, and missing values.

No outlier detection is performed at this stage.  
In the context of financial time series (Bitcoin), extreme values
are an inherent part of the market signal and must be preserved
to ensure consistency of the data used for model training,
backtesting, and inference in machine learning workflows.
"""
)

sql_path = Path("Data_quality.sql")
query = sql_path.read_text(encoding="utf-8")

st.code(query, language="sql")