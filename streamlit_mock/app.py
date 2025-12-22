"""
app.py
Healthcare Data Warehouse Dashboard
Main application with 6 module placeholders
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import logging 
from queries import ICU_STAYS_BY_YEAR, FIRST_CAREUNIT , TOP10CODE , ICU_STAYS_BY_GENDER , HEART_SPO2 , ED , SEPSIS, SEPSIS_COUNT

# Import database modules
from database import (
    get_database_connection,
    run_query,
    run_cached_query, 
    
    test_connection,
    get_database_stats
)
from schema import DatabaseSchema, seed_sample_data # , drop_all_tables

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Page configuration
st.set_page_config(
    page_title="Healthcare Data Warehouse",
    page_icon="🏥",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
    <style>
    .stTabs [data-baseweb="tab-list"] button [data-testid="stMarkdownContainer"] p {
        font-size:16px;
    }
    </style>
    """, unsafe_allow_html=True)


def init_session_state():
    """Initialize session state variables"""
    if 'db_initialized' not in st.session_state:
        st.session_state.db_initialized = False
    if 'current_module' not in st.session_state:
        st.session_state.current_module = "Dashboard"


def sidebar_config():
    """Configure sidebar with database controls and navigation"""
    with st.sidebar:
        st.header("🏥 Healthcare DW Control")
        
        # Connection status
        col1, col2 = st.columns(2)
        with col1:
            if test_connection():
                st.success("✅ Connected")
            else:
                st.error("❌ Disconnected")
        
        with col2:
            if st.button("🔄 Refresh"):
                st.rerun()
        
        st.markdown("---")
        
        # Database management
        st.subheader("🗄️ Database Management")
        
        col1, col2 = st.columns(2)
        with col1:
            if st.button("📦 Initialize Schema"):
                with st.spinner("Creating tables..."):
                    schema = DatabaseSchema()
                    if schema.initialize_database():
                        st.success("Schema created!")
                        st.session_state.db_initialized = True
                    else:
                        st.error("Failed to create schema")
        
        with col2:
            if st.button("🌱 Seed Data"):
                with st.spinner("Seeding data..."):
                    if seed_sample_data():
                        st.success("Data seeded!")
                        st.cache_data.clear()
                    else:
                        st.error("Failed to seed data")

        st.markdown("---")
        st.subheader("⚠️ Danger Zone")

        if st.button("🗑️ Drop All Tables", type="secondary"):
            with st.spinner("Dropping all tables..."):
                try: 
                    schema = DatabaseSchema()
                    schema.drop_all_tables()
                    st.success("All tables dropped successfully!")
                    st.session_state.db_initialized = False
                    st.cache_data.clear()
                except Exception as e:
                    st.error(f"Failed to drop tables: {e}")
        
        # Database info
        st.markdown("---")
        st.subheader("📊 Database Info")
        try:
            stats = get_database_stats()
            st.metric("Database", "mimic_dw")
            st.metric("Size", stats.get('database_size', 'N/A'))
            st.metric("Tables", stats.get('table_count', 0))
        except Exception as e:
            st.error(f"Could not fetch stats: {e}")
        
        return True


# ============================================
# MODULE 1: overview
# ============================================
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
      st.subheader("🏥 ICU Stays by Last Care Unit")

      try:
          unit_df = run_query(FIRST_CAREUNIT)
          print(unit_df)
          if not unit_df.empty:
              fig = px.bar(
                  unit_df,
                  x="first_careunit",
                  y="stay_count",
                  labels={
                      "first_careunit": "First Care Unit",
                      "stay_count": "Number of ICU Stays"
                  },
                  title="ICU Stays by Last Care Unit",
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

# ============================================
# MODULE 2: trend
# ============================================
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


# ============================================
# MODULE 3: Emergency Department Analytics
# ============================================
def correlation():
    """Module 3: Emergency Department Operations"""
    st.header("🚨 Correlation")
    st.info("🚧 Module 3: Correlation")
    col1, col2 = st.columns(2)  
    with col1:  
        st.subheader("🚑 SPO2 vs Heart Rate in ICU")
        try:
          df = run_query(HEART_SPO2)  # name this query as you like

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


# ============================================
# MODULE 4: ICU Performance Dashboard
# ============================================
def sepsis():
    """Module 4: Intensive Care Unit Analytics"""
    st.header("🏥 Sepsis vs Non-Sepsis")
    # st.info("🚧 Module 4: ICU occupancy, length of stay, care unit transfers, outcomes - Coming Soon")
    col1, col2 = st.columns(2)
    # df = run_query(SEPSIS_COUNT)
    df = run_query(SEPSIS_COUNT)

    with col1:
        st.subheader("🦠 Sepsis vs Non-Sepsis Patients")

        fig = px.bar(
            df,
            x="sepsis_status",
            y="patient_count",
            text="patient_count",
            labels={
                "sepsis_status": "Patient Group",
                "patient_count": "Number of Patients"
            },
            title="Sepsis vs Non-Sepsis ICU Patients"
        )

        fig.update_traces(textposition="outside")
        fig.update_layout(
            yaxis_title="Number of Patients",
            xaxis_title=""
        )

        st.plotly_chart(fig, use_container_width=True)
"""
sepsis_module.py
Enhanced Sepsis Analysis Module for the Healthcare Dashboard
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from database import run_query, run_cached_query
from queries import (
    SEPSIS_COUNT,
    SEPSIS_VITAL_SIGNS_AVG,
    SEPSIS_VITAL_SIGNS_COMPARISON,
    SEPSIS_ABNORMAL_VITALS
)

def sepsis_analysis():
    """Enhanced Sepsis vs Non-Sepsis Analysis with Vital Signs"""
    st.header("🏥 Sepsis vs Non-Sepsis Analysis")
    
    # First row: Count and basic vital signs
    col1, col2 = st.columns(2)
    
    # Get sepsis count
    df_count = run_cached_query(SEPSIS_COUNT)
    
    with col1:
        st.subheader("🦠 Patient Distribution")
        
        if not df_count.empty:
            # Create bar chart for patient count
            fig = px.bar(
                df_count,
                x="sepsis_status",
                y="patient_count",
                text="patient_count",
                color="sepsis_status",
                color_discrete_map={"Sepsis": "#e74c3c", "Non-Sepsis": "#3498db"},
                labels={
                    "sepsis_status": "Patient Group",
                    "patient_count": "Number of Patients"
                },
                title="Sepsis vs Non-Sepsis ICU Patients"
            )
            
            fig.update_traces(textposition="outside", texttemplate='%{text}')
            fig.update_layout(
                yaxis_title="Number of Patients",
                xaxis_title="",
                showlegend=False,
                height=400
            )
            
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No sepsis data available")
    
    # Get vital signs comparison
    df_vitals = run_cached_query(SEPSIS_VITAL_SIGNS_COMPARISON)
    
    with col2:
        st.subheader("📊 Average Vital Signs Comparison")
        
        if not df_vitals.empty:
            # Create a comparison table
            vitals_comparison = df_vitals[['sepsis_status', 'avg_temperature', 'avg_heartrate', 
                                          'avg_resprate', 'avg_o2sat', 'avg_sbp', 'avg_dbp']]
            
            # Transpose for better display
            vitals_comparison = vitals_comparison.set_index('sepsis_status').T
            vitals_comparison.index = ['Temperature (°C)', 'Heart Rate (bpm)', 'Resp Rate (breaths/min)',
                                      'O2 Sat (%)', 'Systolic BP (mmHg)', 'Diastolic BP (mmHg)']
            
            # Display as styled dataframe
            st.dataframe(
                vitals_comparison.style.format("{:.1f}").background_gradient(axis=1, cmap='RdYlBu_r'),
                use_container_width=True
            )
        else:
            st.info("No vital signs data available")
    
    # Second row: Detailed vital signs visualization
    st.markdown("---")
    st.subheader("🔬 Detailed Vital Signs Analysis")
    
    if not df_vitals.empty:
        # Create subplot figure for vital signs
        fig = make_subplots(
            rows=2, cols=3,
            subplot_titles=(
                'Temperature', 'Heart Rate', 'Respiratory Rate',
                'O2 Saturation', 'Systolic BP', 'Diastolic BP'
            ),
            specs=[[{'type': 'bar'}, {'type': 'bar'}, {'type': 'bar'}],
                   [{'type': 'bar'}, {'type': 'bar'}, {'type': 'bar'}]]
        )
        
        # Prepare data for plotting
        sepsis_data = df_vitals[df_vitals['sepsis_status'] == 'Sepsis'].iloc[0] if len(df_vitals[df_vitals['sepsis_status'] == 'Sepsis']) > 0 else None
        nonsepsis_data = df_vitals[df_vitals['sepsis_status'] == 'Non-Sepsis'].iloc[0] if len(df_vitals[df_vitals['sepsis_status'] == 'Non-Sepsis']) > 0 else None
        
        vital_metrics = [
            ('avg_temperature', 1, 1, 'Temperature (°C)'),
            ('avg_heartrate', 1, 2, 'Heart Rate (bpm)'),
            ('avg_resprate', 1, 3, 'Resp Rate'),
            ('avg_o2sat', 2, 1, 'O2 Sat (%)'),
            ('avg_sbp', 2, 2, 'Systolic BP'),
            ('avg_dbp', 2, 3, 'Diastolic BP')
        ]
        
        for metric, row, col, label in vital_metrics:
            values = []
            categories = []
            colors = []
            
            if sepsis_data is not None:
                values.append(sepsis_data[metric])
                categories.append('Sepsis')
                colors.append('#e74c3c')
            
            if nonsepsis_data is not None:
                values.append(nonsepsis_data[metric])
                categories.append('Non-Sepsis')
                colors.append('#3498db')
            
            fig.add_trace(
                go.Bar(
                    x=categories,
                    y=values,
                    marker_color=colors,
                    text=[f'{v:.1f}' for v in values],
                    textposition='outside',
                    showlegend=False
                ),
                row=row, col=col
            )
        
        fig.update_layout(height=600, title_text="Vital Signs Comparison: Sepsis vs Non-Sepsis")
        st.plotly_chart(fig, use_container_width=True)
    
    # Third row: Abnormal vital signs analysis
    st.markdown("---")
    st.subheader("⚠️ Abnormal Vital Signs Frequency")
    
    df_abnormal = run_cached_query(SEPSIS_ABNORMAL_VITALS)
    
    if not df_abnormal.empty:
        col1, col2 = st.columns(2)
        
        with col1:
            # Create radar chart for abnormal vitals percentage
            categories = ['Abnormal Temp', 'Tachycardia', 'Tachypnea', 'Low O2', 'Hypotension']
            
            fig = go.Figure()
            
            for _, row in df_abnormal.iterrows():
                fig.add_trace(go.Scatterpolar(
                    r=[row['abnormal_temp_pct'], row['tachycardia_pct'], 
                       row['tachypnea_pct'], row['low_o2_pct'], row['hypotension_pct']],
                    theta=categories,
                    fill='toself',
                    name=row['sepsis_status'],
                    marker_color='#e74c3c' if row['sepsis_status'] == 'Sepsis' else '#3498db'
                ))
            
            fig.update_layout(
                polar=dict(
                    radialaxis=dict(
                        visible=True,
                        range=[0, 100]
                    )),
                showlegend=True,
                title="Abnormal Vital Signs Prevalence (%)"
            )
            
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            # Create summary metrics
            st.markdown("### 📈 Key Findings")
            
            for _, row in df_abnormal.iterrows():
                status = row['sepsis_status']
                color = "#e74c3c" if status == "Sepsis" else "#3498db"
                
                st.markdown(f"**<span style='color:{color}'>{status} Patients</span>**", unsafe_allow_html=True)
                
                col_a, col_b = st.columns(2)
                with col_a:
                    st.metric("Fever/Hypothermia", f"{row['abnormal_temp_pct']:.1f}%")
                    st.metric("Tachycardia", f"{row['tachycardia_pct']:.1f}%")
                    st.metric("Tachypnea", f"{row['tachypnea_pct']:.1f}%")
                
                with col_b:
                    st.metric("Low O2 Sat", f"{row['low_o2_pct']:.1f}%")
                    st.metric("Hypotension", f"{row['hypotension_pct']:.1f}%")
                    st.metric("Total Patients", f"{row['total_patients']}")
                
                st.markdown("---")
    
    # Additional insights
    with st.expander("📖 Clinical Insights"):
        st.markdown("""
        ### Sepsis Vital Signs Patterns
        
        **Expected differences in sepsis patients:**
        - **Temperature**: Often >38°C (fever) or <36°C (hypothermia)
        - **Heart Rate**: Typically >90 bpm (tachycardia)
        - **Respiratory Rate**: Usually >20 breaths/min (tachypnea)
        - **Blood Pressure**: May show hypotension (SBP <90 mmHg)
        - **O2 Saturation**: Often reduced (<95%)
        
        **SIRS Criteria (2+ required):**
        1. Temperature >38°C or <36°C
        2. Heart rate >90 bpm
        3. Respiratory rate >20 or PaCO2 <32 mmHg
        4. WBC >12,000 or <4,000 or >10% bands
        
        **qSOFA Score (2+ suggests sepsis):**
        1. Respiratory rate ≥22/min
        2. Altered mentation
        3. Systolic blood pressure ≤100 mmHg
        """)
   
    # Placeholder for:
    # - ICU occupancy rates
    # - Length of stay analysis
    # - Care unit distribution
    # - Patient outcomes
    # - Readmission rates
    # - Resource utilization


# ============================================
# MODULE 5: Microbiology & Lab Results
# ============================================
def microbiology_lab_module():
    """Module 5: Microbiology and Laboratory Analytics"""
    st.header("🔬 Microbiology & Lab Results")
    st.info("🚧 Module 5: Culture results, antibiotic resistance, organism tracking, lab trends - Coming Soon")
    
    # Placeholder for:
    # - Positive culture rates
    # - Common organisms
    # - Antibiotic resistance patterns
    # - Specimen types distribution
    # - Test turnaround times
    # - Lab value trends


# ============================================
# MODULE 6: Diagnosis & ICD Analytics
# ============================================
def diagnosis_analytics_module():
    """Module 6: Diagnosis Patterns and ICD Analysis"""
    st.header("🏷️ Diagnosis & ICD Analytics")
    st.info("🚧 Module 6: Diagnosis patterns, ICD code analysis, comorbidity tracking - Coming Soon")
    
    # Placeholder for:
    # - Top diagnoses
    # - ICD code distribution
    # - Comorbidity patterns
    # - Diagnosis trends over time
    # - Primary vs secondary diagnoses
    # - Disease category breakdown


def main():
    """Main application"""
    init_session_state()
    
    # Sidebar
    sidebar_config()
    
    # Main content
    st.title("🏥 Healthcare Data Warehouse Dashboard")
    st.markdown("MIMIC-style clinical data analytics platform")
    
    # Create tabs for each module
    tabs = st.tabs([
        "👥 Data Overview",
        "📈 Trend analysis",
        "🚨 Correlation analysis",
        "🏥 Sepsis Stats",
        "🔬 Microbiology/Lab",
        "🏷️ Diagnosis/ICD"
    ])
    
    # Module 1: Patient Analytics
    with tabs[0]:
        # patient_analytics_module()
        overview()
    
    # Module 2: Trend analysis
    with tabs[1]:
        # clinical_events_module() 
        trend()
    
    # Module 3: Emergency Department
    with tabs[2]: 
        correlation()
        # emergency_dept_module()
    
    # Module 4: ICU Performance
    with tabs[3]: 
        sepsis_analysis()
        # sepsis()
    
    # Module 5: Microbiology & Lab
    with tabs[4]:
        microbiology_lab_module()
    
    # Module 6: Diagnosis Analytics
    with tabs[5]:
        diagnosis_analytics_module()


if __name__ == "__main__":
    main()
