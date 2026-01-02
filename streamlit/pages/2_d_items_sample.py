# streamlit/pages/2_d_items.py
import streamlit as st
from components.db_connection import read_table

st.header("ICU d_items Dictionary")

# Load data
query = """
SELECT
    itemid,
    label,
    category,
    unitname,
    lownormalvalue,
    highnormalvalue
FROM dim_d_items
ORDER BY itemid
"""
df = read_table(query)

# Metrics
st.metric("Total Items", len(df))

# Filters
categories = sorted(df["category"].dropna().unique())
selected_category = st.selectbox(
    "Filter by category",
    options=["All"] + categories
)

if selected_category != "All":
    df = df[df["category"] == selected_category]

# Display
st.dataframe(
    df,
    use_container_width=True,
    height=600
)
