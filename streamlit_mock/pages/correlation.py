import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import logging  


# Nho reimport lai queries.py va database.py
from queries import ED, HEART_SPO2
from database import (
    get_database_connection,
    run_query,
    run_cached_query, 

    test_connection,
    get_database_stats
) 


def correlation():
    """Module 3: Emergency Department Operations"""
    st.header("🚨 Correlation")
    st.info("🚧 Module 3: Correlation")
    col1, col2 = st.columns(2)  
    with col1:  
        st.subheader("🚑 SPO2 vs Heart Rate in ICU")
        try:
          df = run_query(HEART_SPO2)  

          if not df.empty:
              fig = px.scatter(
                  df,
                  x="hr_avg",
                  y="spo2_avg",
                  trendline="ols",   # 👈 linear regression
                  labels={
                      "hr_avg": "Average Heart Rate (bpm)",
                      "spo2_avg": "Average SpO₂ (%)"
                  },
                  title="Heart Rate vs SpO₂ (Hourly Aggregated, Same ICU Stay)",
                  opacity=0.6
              )

              fig.update_layout(
                  height=450
              )

              st.plotly_chart(fig, use_container_width=True)

          else:
              st.warning("No aligned HR–SpO₂ data available")

        except Exception as e:
            st.error(f"Failed to plot HR vs SpO₂: {e}") 
    with col2 : 
        st.subheader("🚑 Triage Acuity vs Heart Rate in emergency department")

        df = run_query(ED)

        df = df.dropna(subset=["acuity", "heartrate"])

        fig = px.scatter(
            df,
            x="acuity",
            y="heartrate",
            trendline="ols",
            labels={
                "acuity": "Triage Acuity (Higher = More Severe)",
                "heartrate": "Heart Rate (bpm)"
            },
            title="Correlation Between Triage Acuity and Heart Rate",
            opacity=0.5
        )

        st.plotly_chart(fig, use_container_width=True)
    # Placeholder for:
    # - ED admission trends
    # - Triage acuity distribution
    # - Chief complaints analysis
    # - Arrival transport modes
    # - Disposition outcomes
    # - ED length of stay