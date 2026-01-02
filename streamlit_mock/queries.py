"""
queries.py
Centralized SQL queries for the application
"""

# ============================================
# Dashboard Queries
# ============================================ 

# Table 0 
ICU_STAYS = """ 
SELECT * 
FROM fact_icustays AS fis
LEFT JOIN dim_date AS dd ON fis.intime_date = dd.date_key ; 
""" 


ICU_STAYS_BY_YEAR = """
SELECT
    dd.year AS year,
    COUNT(DISTINCT fis.stay_id) AS stay_count
FROM fact_icustays fis
LEFT JOIN dim_date dd
    ON fis.intime_date = dd.date_key
GROUP BY dd.year
ORDER BY dd.year;
""" 

FIRST_CAREUNIT  = """SELECT
    first_careunit,
    COUNT(DISTINCT stay_id) AS stay_count
FROM fact_icustays
WHERE first_careunit IS NOT NULL
GROUP BY first_careunit
ORDER BY stay_count DESC; 
""" 


# Module 2  
TOP10CODE = """
SELECT
    d.icd_code,
    d.long_title,
    COUNT(DISTINCT f.stay_id) AS stay_count
FROM fact_icustays f
JOIN dim_diagnoses_icd d
    ON f.diagnoses_icd_key = d.diagnoses_icd_key
GROUP BY d.icd_code, d.long_title
ORDER BY stay_count DESC
LIMIT 10; """

ICU_STAYS_BY_GENDER = """
SELECT
    p.gender,
    COUNT(DISTINCT f.stay_id) AS stay_count
FROM fact_icustays f
JOIN dim_patients p
    ON f.subject_id = p.subject_id
WHERE p.gender IS NOT NULL
GROUP BY p.gender;
"""
# Table 1 

# Module 3 

HEART_SPO2 = """
WITH hr AS (
    SELECT
        fce.stay_id,
        dd.full_date AS chart_date,
        dt.hour AS hour_of_day,
        AVG(fce.valuenum) AS hr_avg
    FROM fact_chartevents fce
    JOIN dim_time dt
        ON fce.charttime = dt.time_key
    JOIN dim_date dd
        ON fce.chartdate = dd.date_key
    WHERE fce.itemid = 220045  -- Heart Rate
    GROUP BY fce.stay_id, dd.full_date, dt.hour
),
spo2 AS (
    SELECT
        fce.stay_id,
        dd.full_date AS chart_date,
        dt.hour AS hour_of_day,
        AVG(fce.valuenum) AS spo2_avg
    FROM fact_chartevents fce
    JOIN dim_time dt
        ON fce.charttime = dt.time_key
    JOIN dim_date dd
        ON fce.chartdate = dd.date_key
    WHERE fce.itemid = 220277  -- SpO2
    GROUP BY fce.stay_id, dd.full_date, dt.hour
)
SELECT
    hr.stay_id,
    hr.chart_date,
    hr.hour_of_day,
    hr.hr_avg,
    spo2.spo2_avg
FROM hr
JOIN spo2
    ON hr.stay_id = spo2.stay_id
   AND hr.chart_date = spo2.chart_date
   AND hr.hour_of_day = spo2.hour_of_day
ORDER BY hr.stay_id, hr.chart_date, hr.hour_of_day;
"""


ED = """
SELECT
    fes.stay_id,
    fes.subject_id AS ed_subject_id,
    fes.gender,
    fes.race,
    fes.arrival_transport,
    fes.disposition,

    dt.subject_id AS triage_subject_id,
    dt.temperature,
    dt.heartrate,
    dt.resprate,
    dt.o2sat,
    dt.sbp,
    dt.dbp,
    dt.acuity
FROM fact_edstays fes
LEFT JOIN dim_triage dt
    ON fes.stay_id = dt.stay_id;
""" 

SEPSIS = """
WITH icu_sepsis AS (
    SELECT
        fis.stay_id,
        fis.subject_id,
        fis.los,
        fis.first_careunit,
        fis.last_careunit,
        CASE
            WHEN LOWER(ddi.long_title) LIKE '%sepsis%' THEN 1
            ELSE 0
        END AS sepsis_flag
    FROM fact_icustays fis
    LEFT JOIN dim_diagnoses_icd ddi
        ON fis.diagnoses_icd_key = ddi.diagnoses_icd_key
),
micro_flag AS (
    SELECT DISTINCT
        subject_id,
        1 AS has_micro_event
    FROM fact_microbiologyevents
)
SELECT
    i.*,
    COALESCE(m.has_micro_event, 0) AS has_micro_event
FROM icu_sepsis i
LEFT JOIN micro_flag m
    ON i.subject_id = m.subject_id;
 
"""
SEPSIS_COUNT = """
SELECT
    CASE
        WHEN LOWER(ddi.long_title) LIKE '%sepsis%' THEN 'Sepsis'
        ELSE 'Non-Sepsis'
    END AS sepsis_status,
    COUNT(DISTINCT fis.subject_id) AS patient_count
FROM fact_icustays fis
LEFT JOIN dim_diagnoses_icd ddi
    ON fis.diagnoses_icd_key = ddi.diagnoses_icd_key
GROUP BY sepsis_status
ORDER BY sepsis_status;
"""

"""
queries.py
SQL queries for healthcare analytics
"""

# ============================================
# Sepsis Analysis Queries
# ============================================

SEPSIS_COUNT = """
SELECT
    CASE
        WHEN LOWER(ddi.long_title) LIKE '%sepsis%' THEN 'Sepsis'
        ELSE 'Non-Sepsis'
    END AS sepsis_status,
    COUNT(DISTINCT fis.subject_id) AS patient_count
FROM fact_icustays fis
LEFT JOIN dim_diagnoses_icd ddi
    ON fis.diagnoses_icd_key = ddi.diagnoses_icd_key
GROUP BY sepsis_status
ORDER BY sepsis_status;
"""

SEPSIS_VITAL_SIGNS_AVG = """
SELECT
    CASE
        WHEN LOWER(ddi.long_title) LIKE '%sepsis%' THEN 'Sepsis'
        ELSE 'Non-Sepsis'
    END AS sepsis_status,
    COUNT(DISTINCT fis.subject_id) AS patient_count,
    -- Temperature stats
    AVG(dt.temperature) AS avg_temperature,
    MIN(dt.temperature) AS min_temperature,
    MAX(dt.temperature) AS max_temperature,
    -- Heart rate stats
    AVG(dt.heartrate) AS avg_heartrate,
    MIN(dt.heartrate) AS min_heartrate,
    MAX(dt.heartrate) AS max_heartrate,
    -- Respiratory rate stats
    AVG(dt.resprate) AS avg_resprate,
    MIN(dt.resprate) AS min_resprate,
    MAX(dt.resprate) AS max_resprate,
    -- O2 saturation stats
    AVG(dt.o2sat) AS avg_o2sat,
    MIN(dt.o2sat) AS min_o2sat,
    MAX(dt.o2sat) AS max_o2sat,
    -- Blood pressure stats
    AVG(dt.sbp) AS avg_sbp,
    AVG(dt.dbp) AS avg_dbp,
    -- Acuity score
    AVG(dt.acuity) AS avg_acuity
FROM fact_icustays fis
LEFT JOIN dim_diagnoses_icd ddi
    ON fis.diagnoses_icd_key = ddi.diagnoses_icd_key
LEFT JOIN dim_triage dt
    ON fis.stay_id = dt.stay_id
WHERE dt.temperature IS NOT NULL  -- Ensure we have triage data
GROUP BY sepsis_status
ORDER BY sepsis_status;
"""

SEPSIS_VITAL_SIGNS_COMPARISON = """
WITH sepsis_patients AS (
    SELECT
        fis.subject_id,
        fis.stay_id,
        CASE
            WHEN LOWER(ddi.long_title) LIKE '%sepsis%' THEN 'Sepsis'
            ELSE 'Non-Sepsis'
        END AS sepsis_status,
        dt.temperature,
        dt.heartrate,
        dt.resprate,
        dt.o2sat,
        dt.sbp,
        dt.dbp,
        dt.acuity
    FROM fact_icustays fis
    LEFT JOIN dim_diagnoses_icd ddi
        ON fis.diagnoses_icd_key = ddi.diagnoses_icd_key
    INNER JOIN dim_triage dt
        ON fis.stay_id = dt.stay_id
)
SELECT
    sepsis_status,
    COUNT(DISTINCT subject_id) AS patient_count,
    -- Rounded averages for display
    ROUND(AVG(temperature), 1) AS avg_temperature,
    ROUND(AVG(heartrate), 0) AS avg_heartrate,
    ROUND(AVG(resprate), 0) AS avg_resprate,
    ROUND(AVG(o2sat), 1) AS avg_o2sat,
    ROUND(AVG(sbp), 0) AS avg_sbp,
    ROUND(AVG(dbp), 0) AS avg_dbp,
    ROUND(AVG(acuity), 1) AS avg_acuity,
    -- Standard deviations
    ROUND(STDDEV(temperature), 2) AS std_temperature,
    ROUND(STDDEV(heartrate), 1) AS std_heartrate,
    ROUND(STDDEV(resprate), 1) AS std_resprate,
    ROUND(STDDEV(o2sat), 2) AS std_o2sat
FROM sepsis_patients
GROUP BY sepsis_status
ORDER BY sepsis_status;
"""

SEPSIS_CHIEF_COMPLAINTS = """
SELECT
    CASE
        WHEN LOWER(ddi.long_title) LIKE '%sepsis%' THEN 'Sepsis'
        ELSE 'Non-Sepsis'
    END AS sepsis_status,
    dt.chiefcomplaint,
    COUNT(*) as complaint_count
FROM fact_icustays fis
LEFT JOIN dim_diagnoses_icd ddi
    ON fis.diagnoses_icd_key = ddi.diagnoses_icd_key
INNER JOIN dim_triage dt
    ON fis.stay_id = dt.stay_id
WHERE dt.chiefcomplaint IS NOT NULL
GROUP BY sepsis_status, dt.chiefcomplaint
ORDER BY sepsis_status, complaint_count DESC;
"""

SEPSIS_ABNORMAL_VITALS = """
-- Identify patients with abnormal vital signs by sepsis status
SELECT
    CASE
        WHEN LOWER(ddi.long_title) LIKE '%sepsis%' THEN 'Sepsis'
        ELSE 'Non-Sepsis'
    END AS sepsis_status,
    -- Count abnormal vital signs
    COUNT(CASE WHEN dt.temperature > 38.0 OR dt.temperature < 36.0 THEN 1 END) AS abnormal_temp_count,
    COUNT(CASE WHEN dt.heartrate > 100 THEN 1 END) AS tachycardia_count,
    COUNT(CASE WHEN dt.resprate > 20 THEN 1 END) AS tachypnea_count,
    COUNT(CASE WHEN dt.o2sat < 95 THEN 1 END) AS low_o2_count,
    COUNT(CASE WHEN dt.sbp < 90 THEN 1 END) AS hypotension_count,
    -- Calculate percentages
    ROUND(100.0 * COUNT(CASE WHEN dt.temperature > 38.0 OR dt.temperature < 36.0 THEN 1 END) / COUNT(*), 1) AS abnormal_temp_pct,
    ROUND(100.0 * COUNT(CASE WHEN dt.heartrate > 100 THEN 1 END) / COUNT(*), 1) AS tachycardia_pct,
    ROUND(100.0 * COUNT(CASE WHEN dt.resprate > 20 THEN 1 END) / COUNT(*), 1) AS tachypnea_pct,
    ROUND(100.0 * COUNT(CASE WHEN dt.o2sat < 95 THEN 1 END) / COUNT(*), 1) AS low_o2_pct,
    ROUND(100.0 * COUNT(CASE WHEN dt.sbp < 90 THEN 1 END) / COUNT(*), 1) AS hypotension_pct,
    COUNT(*) AS total_patients
FROM fact_icustays fis
LEFT JOIN dim_diagnoses_icd ddi
    ON fis.diagnoses_icd_key = ddi.diagnoses_icd_key
INNER JOIN dim_triage dt
    ON fis.stay_id = dt.stay_id
GROUP BY sepsis_status
ORDER BY sepsis_status;
"""

# ============================================
# Other Module Queries (placeholders)
# ============================================

PATIENT_DEMOGRAPHICS = """
SELECT 
    gender,
    COUNT(*) as patient_count,
    AVG(anchor_age) as avg_age,
    MIN(anchor_age) as min_age,
    MAX(anchor_age) as max_age
FROM dim_patients
GROUP BY gender
ORDER BY gender;
"""

ICU_LOS_STATS = """
SELECT 
    last_careunit,
    COUNT(*) as admission_count,
    AVG(los) as avg_los,
    MIN(los) as min_los,
    MAX(los) as max_los,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY los) as median_los
FROM fact_icustays
GROUP BY last_careunit
ORDER BY admission_count DESC;
"""