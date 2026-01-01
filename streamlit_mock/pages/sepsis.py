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

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import logging  


# Nho reimport lai queries.py va database.py
from queries import ICU_STAYS_BY_YEAR 
from database import (
    get_database_connection,
    run_query,
    run_cached_query, 

    test_connection,
    get_database_stats
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