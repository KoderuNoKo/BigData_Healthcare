import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px

# Cấu hình trang
st.set_page_config(page_title="ICU Early Warning System", page_icon="⚠️", layout="wide")

# 1. CẤU HÌNH NGƯỠNG CẢNH BÁO (THRESHOLDS)
# Giả lập bảng dim_d_items
THRESHOLDS = {
    'heart_rate': {'min': 60, 'max': 100, 'unit': 'bpm', 'label': 'Heart Rate'},
    'spo2': {'min': 90, 'max': 100, 'unit': '%', 'label': 'SpO2'},
    'sbp': {'min': 90, 'max': 140, 'unit': 'mmHg', 'label': 'Systolic BP'},
    'resp_rate': {'min': 12, 'max': 20, 'unit': 'insp/min', 'label': 'Resp Rate'},
    'temperature': {'min': 36.0, 'max': 38.0, 'unit': '°C', 'label': 'Temperature'} # Lưu ý check đơn vị F hay C trong data của bạn
}

# 2. HÀM LOAD DATA (Đọc từ file CSV đã xử lý)
@st.cache_data
def load_warehouse_data():
    # ⚠️ ĐỔI ĐƯỜNG DẪN TỚI FILE CỦA BẠN
    # File này là file output từ data.py (nhẹ hơn file gốc 40GB)
    path_vitals = 'C:/Users/admin/Downloads/chartevents_vitals.csv'
    
    # Load vitals
    df = pd.read_csv(path_vitals)
    
    # Lấy mẫu 10,000 dòng mới nhất (hoặc ngẫu nhiên) để demo cho nhanh
    # Vì file vitals cũng khá nặng (2GB)
    if len(df) > 30000:
        df = df.sample(30000)
        
    return df

try:
    with st.spinner(" Connecting to Data Warehouse ..."):
        raw_df = load_warehouse_data()
        st.toast("Data loaded successfully!", icon="✅")
except Exception as e:
    st.error(f"❌ Error loading data: {e}")
    st.stop()

# 3. XỬ LÝ LOGIC CẢNH BÁO
def process_warnings(df):
    warnings = []
    
    for _, row in df.iterrows():
        vital_type = row['vital']
        value = row['valuenum']
        
        if vital_type in THRESHOLDS:
            config = THRESHOLDS[vital_type]
            
            status = 'NORMAL'
            if value > config['max']:
                status = 'HIGH'
            elif value < config['min']:
                status = 'LOW'
            
            if status != 'NORMAL':
                warnings.append({
                    'stay_id': int(row['stay_id']),
                    'hours_since_admit': row['hours'],
                    'vital_label': config['label'],
                    'value': value,
                    'unit': config['unit'],
                    'threshold_min': config['min'],
                    'threshold_max': config['max'],
                    'status': status
                })
                
    return pd.DataFrame(warnings)

# Chạy xử lý
df_warnings = process_warnings(raw_df)

# 4. GIAO DIỆN DASHBOARD
st.title("⚠️ ICU Early Warning Monitor")
st.markdown("**Real-time Vital Signs Monitoring & Alert System**")
st.markdown("---")

# --- Sidebar Filters ---
with st.sidebar:
    st.header("🔍 Filter Alerts")
    filter_status = st.multiselect("Alert Level", ['HIGH', 'LOW'], default=['HIGH', 'LOW'])
    filter_vital = st.multiselect("Vital Sign", [v['label'] for v in THRESHOLDS.values()], 
                                  default=[v['label'] for v in THRESHOLDS.values()])

    if st.button("Refresh Data"):
        st.cache_data.clear()
        st.rerun()

# --- Lọc dữ liệu hiển thị ---
if not df_warnings.empty:
    filtered_df = df_warnings[
        (df_warnings['status'].isin(filter_status)) &
        (df_warnings['vital_label'].isin(filter_vital))
    ]
else:
    filtered_df = pd.DataFrame()

# --- KPI Metrics ---
col1, col2, col3, col4 = st.columns(4)
with col1:
    st.metric("Active Alerts", len(filtered_df), delta="Live")
with col2:
    n_critical = len(filtered_df[filtered_df['vital_label'] == 'SpO2'])
    st.metric("Critical Hypoxia (SpO2)", n_critical, delta_color="inverse")
with col3:
    n_tachy = len(filtered_df[(filtered_df['vital_label'] == 'Heart Rate') & (filtered_df['status'] == 'HIGH')])
    st.metric("Tachycardia Events", n_tachy, delta_color="inverse")
with col4:
    n_patients = filtered_df['stay_id'].nunique()
    st.metric("Patients Affected", n_patients)

# --- Charts Layout ---
col_left, col_right = st.columns([2, 1])

with col_left:
    st.subheader("Alert Distribution by Vital Sign")
    if not filtered_df.empty:
        # --- BƯỚC 1: Tính tổng số lượng trước khi vẽ ---
        # Gom nhóm theo 'vital_label' và 'status', sau đó đếm số dòng
        chart_data = filtered_df.groupby(['vital_label', 'status']).size().reset_index(name='Total Alerts')
        
        # --- BƯỚC 2: Vẽ biểu đồ từ dữ liệu đã gom nhóm ---
        fig_bar = px.bar(
            chart_data, 
            x='vital_label', 
            y='Total Alerts',  # Trục Y bây giờ là tổng số lượng
            color='status', 
            color_discrete_map={'HIGH': '#ff4b4b', 'LOW': '#ffa421'},
            barmode='group',
            text='Total Alerts' # (Tùy chọn) Hiển thị số trên cột
        )
        
        # Tùy chỉnh tooltip cho đẹp hơn
        fig_bar.update_traces(
            textposition='outside',
            hovertemplate="<b>%{x}</b><br>Status: %{fullData.name}<br>Count: %{y}"
        )
        
        st.plotly_chart(fig_bar, use_container_width=True)

with col_right:
    st.subheader("Severity Ratio")
    if not filtered_df.empty:
        fig_pie = px.pie(filtered_df, names='status', hole=0.4,
                         color='status',
                         color_discrete_map={'HIGH': '#ff4b4b', 'LOW': '#ffa421'})
        st.plotly_chart(fig_pie, use_container_width=True)

# --- Detailed Table ---
st.subheader("📋 Patient Alert Details")

def color_survived(val):
    color = '#ffcccc' if val == 'HIGH' or val == 'LOW' else ''
    return f'background-color: {color}'

if not filtered_df.empty:
    # Format bảng cho đẹp
    display_df = filtered_df.sort_values(['status', 'value'], ascending=False).head(10000)
    
    st.dataframe(
        display_df.style.applymap(color_survived, subset=['status']),
        column_config={
            "stay_id": "ICU Stay ID",
            "hours_since_admit": st.column_config.NumberColumn("Hours from Admit", format="%.1f h"),
            "vital_label": "Vital Sign",
            "value": st.column_config.NumberColumn("Measured Value", format="%.2f"),
            "status": "Alert Type",
            "threshold_min": "Min Limit",
            "threshold_max": "Max Limit"
        },
        use_container_width=True,
        hide_index=True
    )
else:
    st.success("✅ No critical alerts found in current data stream.")