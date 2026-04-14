import streamlit as st
from pathlib import Path

st.title("Feature Store Implementation")

sql_path = Path("Feature_store.sql")
query = sql_path.read_text(encoding="utf-8")

st.code(query, language="sql")