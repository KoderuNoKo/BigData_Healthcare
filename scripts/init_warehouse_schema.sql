-- ========================================
-- Warehouse Schema Initialization Script
-- For MIMIC-IV Analytics
-- ========================================

-- Drop tables if they exist (for clean re-initialization)
DROP TABLE IF EXISTS fact_chartevents CASCADE;
DROP TABLE IF EXISTS dim_d_items CASCADE;
DROP TABLE IF EXISTS dim_time CASCADE;
DROP TABLE IF EXISTS dim_date CASCADE;
DROP TABLE IF EXISTS dim_patients CASCADE;

-- ========================================
-- DIMENSION TABLES
-- ========================================

-- Dimension: d_items (chart items catalog)
CREATE TABLE dim_d_items (
    itemid INT PRIMARY KEY,
    label VARCHAR(255),
    abbreviation VARCHAR(100),
    category VARCHAR(100),
    unitname VARCHAR(100),
    param_type VARCHAR(50),
    lownormalvalue FLOAT,
    highnormalvalue FLOAT
);

-- Dimension: Date (grain: day)
CREATE TABLE dim_date (
    date_key INT PRIMARY KEY,
    full_date DATE NOT NULL,
    day INT,
    week INT,
    month INT,
    quarter INT,
    year INT
);

-- Dimension: Time (grain: second)
CREATE TABLE dim_time (
    time_key INT PRIMARY KEY,
    time_value TIME NOT NULL,
    hour INT,
    minute INT,
    second INT
);

-- Dimension: Patients (simplified for chartevents)
CREATE TABLE dim_patients (
    subject_id INT PRIMARY KEY,
    gender VARCHAR(10),
    anchor_age INT,
    anchor_year INT,
    anchor_year_group VARCHAR(20),
    dod DATE
);

CREATE TABLE dim_diagnoses_icd (
    diagnoses_icd_key SERIAL PRIMARY KEY,
    seq_num INT,
    icd_code VARCHAR(20),
    icd_version INT,
    long_title VARCHAR(255)
);

CREATE TABLE dim_triage (
    stay_id INT PRIMARY KEY,
    subject_id INT,
    temperature NUMERIC,
    heartrate NUMERIC,
    resprate NUMERIC,
    o2sat NUMERIC,
    sbp NUMERIC,
    dbp NUMERIC,
    pain TEXT,
    acuity NUMERIC,
    chiefcomplaint VARCHAR(255)
);

CREATE TABLE dim_admission (
    hadm_id INT PRIMARY KEY,
    subject_id INT,
    admittime INT REFERENCES dim_time(time_key),
    admitdate INT REFERENCES dim_date(date_key),
    dischtime INT REFERENCES dim_time(time_key),
    dischdate INT REFERENCES dim_date(date_key),
    deathtime INT REFERENCES dim_time(time_key),
    deathdate INT REFERENCES dim_date(date_key),
    admission_type VARCHAR(50),
    admit_provider_id VARCHAR(50),
    admission_location VARCHAR(100),
    discharge_location VARCHAR(100),
    insurance VARCHAR(50),
    language VARCHAR(50),
    marital_status VARCHAR(50),
    race VARCHAR(100),
    edregtime INT REFERENCES dim_time(time_key),
    edregdate INT REFERENCES dim_date(date_key),
    edouttime INT REFERENCES dim_time(time_key),
    edoutdate INT REFERENCES dim_date(date_key),
    hospital_expire_flag INT
);





-- ========================================
-- FACT TABLE
-- ========================================

-- Fact: Chartevents (transactional fact table)
CREATE TABLE fact_chartevents (
    chartevents_key SERIAL PRIMARY KEY,
    subject_id INT REFERENCES dim_patients(subject_id),
    hadm_id INT,
    stay_id INT,
    caregiver_id INT,
    chartdate INT REFERENCES dim_date(date_key),
    charttime INT REFERENCES dim_time(time_key),
    itemid INT REFERENCES dim_d_items(itemid),
    value VARCHAR(255),
    valuenum DOUBLE PRECISION,
    valueuom VARCHAR(50),
    warning INT
);

-- Factless: icustays
CREATE TABLE fact_icustays (
    stay_id INT PRIMARY KEY,
    subject_id INT REFERENCES dim_patients(subject_id),
    hadm_id INT,
    first_careunit VARCHAR(50),
    last_careunit VARCHAR(50),
    intime_time INT REFERENCES dim_time(time_key),
    intime_date INT REFERENCES dim_date(date_key),
    outtime_time INT REFERENCES dim_time(time_key),
    outtime_date INT REFERENCES dim_date(date_key),
    los DOUBLE PRECISION,
    diagnoses_icd_key INT REFERENCES dim_diagnoses_icd(diagnoses_icd_key)
);

-- Fact: Microbiology Events
CREATE TABLE fact_microbiologyevents (
    mbe_key SERIAL PRIMARY KEY,
    microevent_id INT,
    subject_id INT REFERENCES dim_patients(subject_id),
    hadm_id INT,
    micro_specimen_id INT,
    order_provider_id VARCHAR(50),
    chartdate INT REFERENCES dim_date(date_key),
    charttime INT REFERENCES dim_time(time_key),
    spec_itemid INT,
    spec_type_desc VARCHAR(255),
    test_seq INT,
    test_itemid INT,
    test_name VARCHAR(255),
    org_itemid INT,
    isolate_num INT,
    quantity VARCHAR(50),
    ab_itemid INT,
    ab_name VARCHAR(100),
    dilution_text VARCHAR(50),
    dilution_comparison VARCHAR(10),
    interpretation VARCHAR(50),
    comments TEXT
);

-- Factless: ED Stays (fact-less)
CREATE TABLE fact_edstays (
    edstays_key SERIAL PRIMARY KEY,
    subject_id INT REFERENCES dim_patients(subject_id),
    hadm_id INT,
    stay_id INT,
    intdate INT REFERENCES dim_date(date_key),
    inttime INT REFERENCES dim_time(time_key),
    outdate INT REFERENCES dim_date(date_key),
    outtime INT REFERENCES dim_time(time_key),
    gender VARCHAR(10),
    race VARCHAR(100),
    arrival_transport VARCHAR(50),
    disposition VARCHAR(50),
    triage INT REFERENCES dim_triage(stay_id),
    stay_duration INT
);



-- ========================================
-- INDEXES for Query Performance
-- ========================================

-- Indexes on fact table foreign keys
CREATE INDEX idx_fact_chartevents_subject ON fact_chartevents(subject_id);
CREATE INDEX idx_fact_chartevents_item ON fact_chartevents(itemid);
CREATE INDEX idx_fact_chartevents_chartdate ON fact_chartevents(chartdate);
CREATE INDEX idx_fact_chartevents_charttime ON fact_chartevents(charttime);
CREATE INDEX idx_fact_chartevents_stay ON fact_chartevents(stay_id);
CREATE INDEX idx_fact_icustays_subject ON fact_icustays(subject_id);
CREATE INDEX idx_fact_icustays_dates ON fact_icustays(intime_date, outtime_date);

CREATE INDEX idx_fact_micro_subject ON fact_microbiologyevents(subject_id);
CREATE INDEX idx_fact_micro_chartdate ON fact_microbiologyevents(chartdate);

CREATE INDEX idx_fact_edstays_subject ON fact_edstays(subject_id);
CREATE INDEX idx_fact_edstays_dates ON fact_edstays(intdate, outdate);

-- Composite index for common queries (patient + time + item)
CREATE INDEX idx_fact_chartevents_composite 
    ON fact_chartevents(subject_id, chartdate, itemid);

-- Index on valuenum for analytical queries
CREATE INDEX idx_fact_chartevents_valuenum ON fact_chartevents(valuenum) 
    WHERE valuenum IS NOT NULL;

-- ========================================
-- PRE-POPULATE DIMENSION TABLES
-- ========================================

-- Pre-populate dim_date (01/01/2100 - 31/12/2299)
-- Generate ~73,000 days (200 years)
INSERT INTO dim_date (date_key, full_date, day, week, month, quarter, year)
SELECT
    TO_CHAR(date_val, 'YYYYMMDD')::INT as date_key,
    date_val as full_date,
    EXTRACT(DAY FROM date_val)::INT as day,
    EXTRACT(WEEK FROM date_val)::INT as week,
    EXTRACT(MONTH FROM date_val)::INT as month,
    EXTRACT(QUARTER FROM date_val)::INT as quarter,
    EXTRACT(YEAR FROM date_val)::INT as year
FROM generate_series(
    '2100-01-01'::DATE,
    '2299-12-31'::DATE,
    '1 day'::INTERVAL
) as date_val;

-- Pre-populate dim_time (grain: second, 86,400 entries)
-- time_key: 0-86399 (seconds since midnight)
INSERT INTO dim_time (time_key, time_value, hour, minute, second)
SELECT
    sec as time_key,
    (sec || ' seconds')::INTERVAL::TIME as time_value,
    (sec / 3600)::INT as hour,
    ((sec % 3600) / 60)::INT as minute,
    (sec % 60)::INT as second
FROM generate_series(0, 86399) as sec;

-- ========================================
-- VERIFICATION
-- ========================================

-- Check record counts
SELECT 'dim_date' as table_name, COUNT(*) as record_count FROM dim_date
UNION ALL
SELECT 'dim_time', COUNT(*) FROM dim_time
ORDER BY table_name;

-- Sample dim_date
SELECT * FROM dim_date ORDER BY date_key LIMIT 5;

-- Sample dim_time
SELECT * FROM dim_time ORDER BY time_key LIMIT 5;

-- ========================================
-- COMMENTS for Documentation
-- ========================================

COMMENT ON TABLE fact_chartevents IS 'Transaction fact table storing ICU chart events from MIMIC-IV';
COMMENT ON TABLE dim_d_items IS 'Dimension table for chart item definitions';
COMMENT ON TABLE dim_date IS 'Date dimension with calendar attributes (2100-2299, day grain)';
COMMENT ON TABLE dim_time IS 'Time dimension with second grain (86,400 entries)';
COMMENT ON TABLE dim_patients IS 'Patient demographic dimension';
COMMENT ON TABLE dim_diagnoses_icd IS 'ICD diagnosis dimension (ICD-9 / ICD-10), decoupled from admissions';
COMMENT ON TABLE dim_triage IS 'ED triage vital signs and acuity assessment';
COMMENT ON TABLE dim_admission IS 'Hospital admission administrative and demographic attributes';
COMMENT ON TABLE fact_icustays IS 'Fact-less table representing ICU stay intervals and care units';
COMMENT ON TABLE fact_microbiologyevents IS 'Transactional fact table for microbiology test results and antibiotic sensitivity';
COMMENT ON TABLE fact_edstays IS 'Fact-less table representing emergency department stay intervals and disposition';

COMMENT ON COLUMN fact_chartevents.chartevents_key IS 'Surrogate key (auto-generated)';
COMMENT ON COLUMN fact_chartevents.valuenum IS 'Numeric value of the measurement';
COMMENT ON COLUMN dim_time.time_key IS 'Seconds since midnight (0-86399)';
COMMENT ON COLUMN dim_date.date_key IS 'Date in YYYYMMDD format';