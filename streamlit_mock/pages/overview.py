import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import logging  
from queries import ICU_STAYS_BY_YEAR 
from database import (
    get_database_connection,
    run_query,
    run_cached_query, 

    test_connection,
    get_database_stats
)

def overview():
    """Module 1: Overview of ICU stay data"""
    st.header("👥 Overview of the Data")
    st.info("🚧 Module 1: Overview")

    col1, col2 = st.columns(2)

    # ==============================
    # FIGURE 1: ICU stays by intime
    # ==============================
    with col1:
        st.subheader("📅 ICU Stays by Admission Date")

        try:
            df = run_query(ICU_STAYS_BY_YEAR)

            if not df.empty:
                fig = px.bar(
                    df,
                    x="year",
                    y="stay_count",
                    labels={
                        "year": "ICU Admission Year",
                        "stay_count": "Number of ICU Stays"
                    },
                    title="Yearly ICU Admissions",
                    text="stay_count"
                )

                fig.update_layout(
                    height=400,
                    xaxis=dict(
                        type="category",              # 👈 force discrete axis
                        categoryorder="category ascending"
                    ),
                    yaxis_title="Number of ICU Stays"
                )

                fig.update_traces(
                    textposition="outside"
                )
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.warning("No ICU stay data available")

        except Exception as e:
            st.error(f"Failed to load ICU stay overview: {e}")

    # ==============================
    # FIGURE 2: PLACEHOLDER
    # ==============================
    with col2:
      st.subheader("🏥 ICU Stays by  Care Unit")

      try:
          unit_df = run_query(FIRST_CAREUNIT)
          print(unit_df)
          if not unit_df.empty:
              fig = px.bar(
                  unit_df,
                  x="first_careunit",
                  y="stay_count",
                  labels={
                      "first_careunit": "Care Unit",
                      "stay_count": "Number of ICU Stays"
                  },
                  title="ICU Stays by  Care Unit",
                  text="stay_count"
              )

              fig.update_layout(
                  height=400,
                  xaxis=dict(
                      type="category",
                      categoryorder="total descending"
                  ),
                  yaxis_title="Number of ICU Stays"
              )

              fig.update_traces(textposition="outside")

              st.plotly_chart(fig, use_container_width=True)
          else:
              st.warning("No care unit data available")

      except Exception as e:
          st.error(f"Failed to load care unit overview: {e}") 