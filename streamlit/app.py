# streamlit/app.py
import streamlit as st

st.set_page_config(
    page_title="MIMIC-IV Analytics",
    layout="wide",
)

st.title("MIMIC-IV Big Data Analytics Dashboard")
st.markdown("""
This dashboard provides analytical views over the **MIMIC-IV** data warehouse,
populated via a Kafka → Spark → MinIO → PostgreSQL pipeline.
""")

st.sidebar.success("Select a page from the sidebar")
