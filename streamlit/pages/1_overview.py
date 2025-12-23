# streamlit/pages/1_overview.py
import streamlit as st

st.header("Project Overview")

st.markdown("""
### Architecture Summary
- **Data Source**: MIMIC-IV (ICU, Hosp, ED modules)
- **Streaming**: Apache Kafka
- **Processing**: Apache Spark
- **Data Lake**: MinIO (Bronze / Silver)
- **Warehouse**: PostgreSQL
- **Visualization**: Streamlit

This page will later include:
- Data freshness
- Row counts
- Pipeline health
""")
