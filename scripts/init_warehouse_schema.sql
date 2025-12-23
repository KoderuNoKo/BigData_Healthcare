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

-- Dimension: Time (grain: second, no FK to date)
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

-- ========================================
-- INDEXES for Query Performance
-- ========================================

-- Indexes on fact table foreign keys
CREATE INDEX idx_fact_chartevents_subject ON fact_chartevents(subject_id);
CREATE INDEX idx_fact_chartevents_item ON fact_chartevents(itemid);
CREATE INDEX idx_fact_chartevents_chartdate ON fact_chartevents(chartdate);
CREATE INDEX idx_fact_chartevents_charttime ON fact_chartevents(charttime);
CREATE INDEX idx_fact_chartevents_stay ON fact_chartevents(stay_id);

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

COMMENT ON COLUMN fact_chartevents.chartevents_key IS 'Surrogate key (auto-generated)';
COMMENT ON COLUMN fact_chartevents.valuenum IS 'Numeric value of the measurement';
COMMENT ON COLUMN dim_time.time_key IS 'Seconds since midnight (0-86399)';
COMMENT ON COLUMN dim_date.date_key IS 'Date in YYYYMMDD format';