import streamlit as st
import pandas as pd
import numpy as np
from xgboost import XGBClassifier

# 1. CẤU HÌNH TRANG
st.set_page_config(page_title="ICU Predictive Analytics", page_icon="🏥", layout="wide")

st.title(" ICU Clinical Decision Support System")
st.markdown("Hệ thống hỗ trợ ra quyết định lâm sàng dựa trên AI (MIMIC-IV Data)")

# 2. LOAD MODEL (Dùng Cache để không phải load lại mỗi lần bấm)
@st.cache_resource
def load_models():
    models = {}
    
    # A. Load LoS Model
    try:
        model_los = XGBClassifier()
        model_los.load_model("xgboost_icu_stay.json")
        models['los'] = model_los
    except Exception as e:
        models['los'] = None
        print(f"Lỗi load LoS: {e}")

    # B. Load Mortality Model
    try:
        model_mort = XGBClassifier()
        model_mort.load_model("xgboost_mortality.json")
        models['mortality'] = model_mort
    except Exception as e:
        models['mortality'] = None
        print(f"Lỗi load Mortality: {e}")
        
    # C. Load Readmission Model (MỚI)
    try:
        model_readm = XGBClassifier()
        model_readm.load_model("xgboost_readmission.json")
        models['readmission'] = model_readm
    except Exception as e:
        models['readmission'] = None
        print(f"Lỗi load Readmission: {e}")
        
    return models

# Load models
loaded_models = load_models()
available_models = [k for k, v in loaded_models.items() if v is not None]

if not available_models:
    st.error("❌ Không tìm thấy file model (.json) nào. Hãy đảm bảo bạn đã copy đủ 3 file vào thư mục!")
    st.stop()
else:
    st.success(f"System Ready! Loaded modules: {', '.join(available_models)}")

# 3. GIAO DIỆN NHẬP LIỆU (SIDEBAR)
st.sidebar.header("Patient Vitals & History")

def user_input_features():
    # --- A. Thông tin hành chính ---
    st.sidebar.subheader("Demographics")
    age = st.sidebar.number_input("Age", 18, 100, 65)
    gender = st.sidebar.selectbox("Gender", ["Male", "Female"])
    is_emergency = st.sidebar.checkbox("Emergency Admission?", value=True)
    
    # --- B. Chỉ số sinh tồn (Vitals) ---
    st.sidebar.subheader("Vital Signs (First 24h)")
    heart_rate = st.sidebar.slider("Heart Rate (BPM)", 40, 200, 85)
    sbp = st.sidebar.slider("Systolic BP (mmHg)", 50, 250, 110)
    spo2 = st.sidebar.slider("SpO2 (%)", 50, 100, 98)
    resp_rate = st.sidebar.slider("Resp Rate (bpm)", 10, 60, 20)
    temperature = st.sidebar.slider("Temperature (°C)", 35.0, 41.0, 37.0)
    
    # --- C. Lịch sử bệnh ---
    st.sidebar.subheader("Medical History & Flags")
    dx_count = st.sidebar.number_input("Total Diagnoses Count", 1, 50, 5)
    
    col_s1, col_s2 = st.sidebar.columns(2)
    with col_s1:
        has_sepsis = st.checkbox("Sepsis?", value=False)
        has_hf = st.checkbox("Heart Failure?", value=False)
    with col_s2:
        has_pneu = st.checkbox("Pneumonia?", value=False)
        has_resp_fail = st.checkbox("Resp. Failure?", value=False)

    # --- D. TỔNG HỢP DATA ---
    data = {
        'age': age,
        'is_male': 1 if gender == "Male" else 0,
        'is_emergency': int(is_emergency),
        'dx_count': dx_count,
        'has_sepsis': int(has_sepsis),
        'has_heart_failure': int(has_hf),
        'has_pneumonia': int(has_pneu),
        'has_resp_failure': int(has_resp_fail),
        
        # Vitals Mean
        'mean_heart_rate': heart_rate,
        'mean_sbp': sbp,
        'mean_spo2': spo2,
        'mean_resp_rate': resp_rate,
        'mean_temperature': temperature,
        
        # Vitals Min (Giả lập biên độ dao động)
        'min_heart_rate': heart_rate - 10,
        'min_sbp': sbp - 15,
        'min_spo2': spo2 - 3,
        'min_resp_rate': resp_rate - 4,
        'min_temperature': temperature - 0.5,
        
        # Vitals Max
        'max_heart_rate': heart_rate + 15,
        'max_sbp': sbp + 15,
        'max_spo2': 100 if spo2 + 2 > 100 else spo2 + 2,
        'max_resp_rate': resp_rate + 5,
        'max_temperature': temperature + 1.0,
    }
    
    # Shock Index
    data['shock_index'] = data['mean_heart_rate'] / data['mean_sbp'] if data['mean_sbp'] > 0 else 0
    
    return pd.DataFrame(data, index=[0])

input_df = user_input_features()

# 4. HIỂN THỊ INPUT
with st.expander("🔎 Review Patient Input Data"):
    st.dataframe(input_df)

# 5. NÚT CHẠY DỰ BÁO
if st.button("RUN PREDICTION ANALYSIS", type="primary"):
    
    col1, col2, col3 = st.columns(3)
    
    # --- MODULE 1: MORTALITY PREDICTION ---
    with col1:
        st.subheader("Mortality Risk")
        model = loaded_models['mortality']
        if model:
            feats = model.get_booster().feature_names
            input_ready = input_df.copy()
            for f in feats: 
                if f not in input_ready.columns: input_ready[f] = 0
            
            prob = model.predict_proba(input_ready[feats])[:, 1][0]
            
            if prob > 0.5:
                st.error(f"HIGH RISK: {prob:.1%}")
            elif prob > 0.2:
                st.warning(f"MEDIUM RISK: {prob:.1%}")
            else:
                st.success(f"LOW RISK: {prob:.1%}")
        else:
            st.info("⚠️ File 'xgboost_mortality.json' not found")

    # --- MODULE 2: READMISSION RISK (MỚI) ---
    with col2:
        st.subheader("30-Day Readmission")
        model = loaded_models['readmission']
        if model:
            feats = model.get_booster().feature_names
            input_ready = input_df.copy()
            # Model Readmission có feature lạ (age_group, los_category...) 
            # -> Tự động điền 0 để không bị lỗi
            for f in feats: 
                if f not in input_ready.columns: input_ready[f] = 0
            
            prob = model.predict_proba(input_ready[feats])[:, 1][0]
            
            if prob > 0.5:
                st.error(f"HIGH RISK: {prob:.1%}")
                st.markdown("⚠️ **Action:** Schedule follow-up < 1 week.")
            else:
                st.success(f"LOW RISK: {prob:.1%}")
                st.markdown("✅ Standard discharge plan.")
        else:
            st.info("⚠️ File 'xgboost_readmission.json' not found")

    # --- MODULE 3: ICU LENGTH OF STAY (LoS) ---
    with col3:
        st.subheader("ICU Length of Stay")
        model = loaded_models['los']
        if model:
            feats = model.get_booster().feature_names
            input_ready = input_df.copy()
            for f in feats: 
                if f not in input_ready.columns: input_ready[f] = 0
            
            pred_idx = model.predict(input_ready[feats])[0]
            prob_arr = model.predict_proba(input_ready[feats])[0]
            
            # Mapping: 0=Long, 1=Medium, 2=Short (Theo LabelEncoder A-Z)
            labels = {
                0: ("LONG STAY (>7 days)", "inverse"),
                1: ("MEDIUM (3-7 days)", "warning"),
                2: ("SHORT (<3 days)", "success")
            }
            
            label_text, color_style = labels.get(pred_idx, ("Unknown", "info"))
            confidence = prob_arr[pred_idx]
            
            if color_style == "inverse":
                st.error(f"{label_text}")
                st.caption(f"Confidence: {confidence:.1%}")
            elif color_style == "warning":
                st.warning(f"{label_text}")
                st.caption(f"Confidence: {confidence:.1%}")
            else:
                st.success(f"{label_text}")
                st.caption(f"Confidence: {confidence:.1%}")
        else:
            st.info("⚠️ File 'xgboost_icu_stay.json' not found")

    # 6. GIẢI THÍCH (EXPLAINABILITY)
    st.markdown("---")
    st.subheader("Analysis Insights")
    
    reasons = []
    if input_df['dx_count'][0] > 10:
        reasons.append(f"- **Multimorbidity:** Patient has {input_df['dx_count'][0]} diagnoses (High burden).")
    if input_df['has_sepsis'][0] == 1:
        reasons.append("- **Sepsis Detected:** Major risk factor for Mortality & Long Stay.")
    if input_df['shock_index'][0] > 0.9:
        reasons.append(f"- **Hemodynamic Instability:** Shock Index = {input_df['shock_index'][0]:.2f} (> 0.9).")
    if input_df['is_emergency'][0] == 1:
        reasons.append("- **Emergency Admission:** Associated with acute severity.")
        
    if reasons:
        for r in reasons: st.write(r)
    else:
        st.write("Patient vitals are relatively stable. No critical flags detected.")