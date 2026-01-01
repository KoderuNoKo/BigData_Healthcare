import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import logging  


# Nho reimport lai queries.py va database.py
from queries import ICU_STAYS_BY_GENDER, TOP10CODE
from database import (
    get_database_connection,
    run_query,
    run_cached_query, 

    test_connection,
    get_database_stats
) 


def trend():
    """Module 2: Clinical Events and Vital Signs Monitoring"""
    st.header("📈 Trend analysis")
    st.info("Trend Analysis")
    col1, col2 = st.columns(2) 
    with col1:
      st.subheader("🩺 Top 10 ICD Codes by ICU Stays")

      try:
          df = run_query(TOP10CODE)
          if not df.empty:
              fig = px.bar(
                  df,
                  x="icd_code",
                  y="stay_count",
                  labels={
                      "icd_code": "ICD Code",
                      "stay_count": "Number of ICU Stays"
                  },
                  title="Top 10 Diagnoses in ICU (by Stay Count)",
                  text="stay_count",
                  hover_data=["long_title"]
              )

              fig.update_layout(
                  height=400,
                  xaxis=dict(
                      type="category",
                      categoryorder="total descending"
                  ),
                  yaxis_title="Number of ICU Stays"
              )

              fig.update_traces(
                  textposition="outside"
              )

              st.plotly_chart(fig, use_container_width=True)

          else:
              st.warning("No ICD diagnosis data available")

      except Exception as e:
          st.error(f"Failed to load ICD overview: {e}") 
    with col2:
      st.subheader("🚻 ICU Stays by Gender")

      try:
          df = run_query(ICU_STAYS_BY_GENDER)

          if not df.empty:
              fig = px.pie(
                  df,
                  values="stay_count",
                  names="gender",
                  title="Gender Distribution of ICU Stays",
                  color="gender",
                  color_discrete_map={
                      "M": "#1f77b4",
                      "F": "#e377c2"
                  }
              )

              fig.update_traces(
                  textposition="inside",
                  textinfo="percent+label"
              )

              fig.update_layout(
                  height=400,
                  legend_title="Gender"
              )

              st.plotly_chart(fig, use_container_width=True)

          else:
              st.warning("No gender data available")

      except Exception as e:
          st.error(f"Failed to load gender distribution: {e}")
    # Placeholder for:
    # - Vital signs trends
    # - Abnormal values alerts
    # - Chart events timeline
    # - Item frequency analysis
    # - Warning indicators dashboard
