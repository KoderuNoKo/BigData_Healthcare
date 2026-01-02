"""
schema.py
Healthcare data warehouse star schema (MIMIC-style)
"""

import logging
from database import get_database_connection, transaction

logger = logging.getLogger(__name__)


class Schema:
    """Healthcare star-schema tables"""

    # Dimension Tables
    DIM_DATE = """
        CREATE TABLE IF NOT EXISTS dim_date (
            date_key INT PRIMARY KEY,
            full_date DATE,
            day INT,
            week INT,
            month INT,
            quarter INT,
            year INT
        )
    """

    DIM_TIME = """
        CREATE TABLE IF NOT EXISTS dim_time (
            time_key INT PRIMARY KEY,
            timestamp TIMESTAMP,
            date_key INT,
            hour INT,
            minute INT,
            second INT
        )
    """

    DIM_PATIENTS = """
        CREATE TABLE IF NOT EXISTS dim_patients (
            subject_id INT PRIMARY KEY,
            gender VARCHAR(10),
            abbreviation VARCHAR(10),
            anchor_age INT,
            anchor_year INT,
            anchor_year_group VARCHAR(20),
            dod DATE
        )
    """

    DIM_ITEMS = """
        CREATE TABLE IF NOT EXISTS dim_items (
            itemid INT PRIMARY KEY,
            label VARCHAR(200),
            abbreviation VARCHAR(50),
            linksto VARCHAR(50),
            category VARCHAR(100),
            unitname VARCHAR(50),
            param_type VARCHAR(50),
            low_normal_value FLOAT,
            high_normal_value FLOAT
        )
    """

    DIM_DIAGNOSES_ICD = """
        CREATE TABLE IF NOT EXISTS dim_diagnoses_icd (
            diagnoses_icd_key INT PRIMARY KEY,
            icd_code VARCHAR(20),
            icd_version INT,
            long_title VARCHAR(255)
        )
    """

    DIM_TRIAGE = """
        CREATE TABLE IF NOT EXISTS dim_triage (
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
        )
    """

    # Fact Tables
    FACT_ICUSTAYS = """
        CREATE TABLE IF NOT EXISTS fact_icustays (
            stay_key INT PRIMARY KEY,
            stay_id INT,
            subject_id INT,
            intime_date INT,
            outtime_date INT,
            intime_time INT,
            outtime_time INT, 
            first_careunit VARCHAR(20), 
            last_careunit VARCHAR(20),
            los DOUBLE PRECISION,
            diagnoses_icd_key INT
        )
    """

    FACT_EDSTAYS = """
        CREATE TABLE IF NOT EXISTS fact_edstays (
            edstay_key INT PRIMARY KEY,
            subject_id INT,
            hadm_id INT,
            stay_id INT,
            intime INT,
            outtime INT,
            gender VARCHAR(10),
            race VARCHAR(50),
            arrival_transport VARCHAR(50),
            disposition VARCHAR(50)
        )
    """

    FACT_CHARTEVENTS = """
        CREATE TABLE IF NOT EXISTS fact_chartevents (
            chartevents_key INT PRIMARY KEY,
            subject_id INT,
            hadm_id INT,
            stay_id INT,
            chartdate INT,
            charttime INT,
            storetime INT,
            itemid INT,
            value VARCHAR(255),
            valuenum DOUBLE PRECISION,
            valueuom VARCHAR(50),
            warning INT
        )
    """

    FACT_MICROBIOLOGYEVENTS = """
        CREATE TABLE IF NOT EXISTS fact_microbiologyevents (
            micro_key INT PRIMARY KEY,
            microevent_id INT,
            subject_id INT,
            hadm_id INT,
            micro_specimen_id INT,
            order_provider_id VARCHAR(10),
            chartdate INT,
            charttime INT,
            spec_itemid INT,
            spec_type_desc VARCHAR(100),
            test_seq INT,
            storedate INT,
            storetime INT,
            test_itemid INT,
            test_name VARCHAR(100),
            org_itemid INT,
            org_name VARCHAR(100),
            isolate_num INT,
            quantity VARCHAR(50),
            ab_itemid INT,
            ab_name VARCHAR(50),
            dilution_text VARCHAR(50),
            dilution_comparison VARCHAR(20),
            dilution_value DOUBLE PRECISION,
            interpretation VARCHAR(50),
            comments TEXT
        )
    """


class DatabaseSchema:
    """Schema management"""
    
    def __init__(self):
        self.db = get_database_connection()
        self.tables = [
            # Create dimension tables first
            Schema.DIM_DATE,
            Schema.DIM_TIME,
            Schema.DIM_PATIENTS,
            Schema.DIM_ITEMS,
            Schema.DIM_DIAGNOSES_ICD,
            Schema.DIM_TRIAGE,
            # Then fact tables
            Schema.FACT_ICUSTAYS,
            Schema.FACT_EDSTAYS,
            Schema.FACT_CHARTEVENTS,
            Schema.FACT_MICROBIOLOGYEVENTS,
        ]
    
    def create_tables(self):
        """Create all tables"""
        logger.info("Creating database tables...")
        
        with transaction() as conn:
            cursor = conn.cursor()
            for table_sql in self.tables:
                try:
                    cursor.execute(table_sql)
                    logger.info("Table created or already exists")
                except Exception as e:
                    logger.error(f"Error creating table: {e}")
                    raise
            cursor.close()
        
        logger.info("All tables created successfully")
        return True
    
    def initialize_database(self):
        """Initialize database schema"""
        logger.info("Initializing database schema...")
        
        try:
            self.create_tables()
            logger.info("Database schema initialized successfully")
            return True
        except Exception as e:
            logger.error(f"Failed to initialize database: {e}")
            return False
    
    def drop_all_tables(self):
        """Drop all tables (DANGER!)"""
        logger.warning("Dropping all tables...")
        
        with transaction() as conn:
            cursor = conn.cursor()
            
            # Drop in reverse order (facts before dimensions)
            tables_to_drop = [
                'fact_microbiologyevents',
                'fact_chartevents', 
                'fact_edstays',
                'fact_icustays',
                'dim_triage',
                'dim_diagnoses_icd',
                'dim_items',
                'dim_patients',
                'dim_time',
                'dim_date'
            ]
            
            for table in tables_to_drop:
                cursor.execute(f"DROP TABLE IF EXISTS {table} CASCADE")
                logger.info(f"Dropped table: {table}")
            
            cursor.close()
        
        logger.info("All tables dropped")
    
    def get_schema_info(self):
        """Get schema information"""
        query = """
            SELECT 
                t.table_name,
                COUNT(c.column_name) as column_count,
                pg_size_pretty(pg_relation_size(quote_ident(t.table_name)::regclass)) as table_size
            FROM information_schema.tables t
            LEFT JOIN information_schema.columns c 
                ON t.table_name = c.table_name
            WHERE t.table_schema = 'public'
                AND t.table_type = 'BASE TABLE'
            GROUP BY t.table_name
            ORDER BY t.table_name
        """
        
        return self.db.fetch_dataframe(query)


def seed_sample_data():
    """Seed sample healthcare data"""
    logger.info("Seeding sample data...")
    
    db = get_database_connection()
    
    try:
        # Sample date dimension
        dates = [
            (20240101, '2024-01-01', 1, 1, 1, 1, 2024),
            (20240102, '2024-01-02', 2, 1, 1, 1, 2024),
            (20240103, '2024-01-03', 3, 1, 1, 1, 2024),
        ]
        db.execute_many(
            "INSERT INTO dim_date (date_key, full_date, day, week, month, quarter, year) VALUES (%s, %s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING",
            dates
        )
        
        # Sample time dimension
        times = [
            (1, '2024-01-01 08:00:00', 20240101, 8, 0, 0),
            (2, '2024-01-01 12:00:00', 20240101, 12, 0, 0),
            (3, '2024-01-01 16:00:00', 20240101, 16, 0, 0),
        ]
        db.execute_many(
            "INSERT INTO dim_time (time_key, timestamp, date_key, hour, minute, second) VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING",
            times
        )
        
        # Sample patients
        patients = [
            (1001, 'M', 'M', 45, 2024, '2020-2025', None),
            (1002, 'F', 'F', 32, 2024, '2020-2025', None),
            (1003, 'M', 'M', 67, 2024, '2020-2025', None),
        ]
        db.execute_many(
            "INSERT INTO dim_patients (subject_id, gender, abbreviation, anchor_age, anchor_year, anchor_year_group, dod) VALUES (%s, %s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING",
            patients
        )
        
        # Sample items
        items = [
            (220045, 'Heart Rate', 'HR', 'chartevents', 'Routine Vital Signs', 'bpm', 'Numeric', 60, 100),
            (220210, 'Respiratory Rate', 'RR', 'chartevents', 'Routine Vital Signs', 'insp/min', 'Numeric', 12, 20),
            (223761, 'Temperature', 'Temp', 'chartevents', 'Routine Vital Signs', 'C', 'Numeric', 36.5, 37.5),
        ]
        db.execute_many(
            "INSERT INTO dim_items (itemid, label, abbreviation, linksto, category, unitname, param_type, low_normal_value, high_normal_value) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING",
            items
        )
        
        # Sample diagnoses
        diagnoses = [
            (1, 'I10', 9, 'sepsis'),
            (2, 'E11.9', 10, 'Type 2 diabetes mellitus without complications'),
            (3, 'J44.0', 10, 'Chronic obstructive pulmonary disease with acute lower respiratory infection'),
        ]
        db.execute_many(
            "INSERT INTO dim_diagnoses_icd (diagnoses_icd_key, icd_code, icd_version, long_title) VALUES (%s, %s, %s, %s) ON CONFLICT DO NOTHING",
            diagnoses
        )
        
        # Sample triage
        triage = [
            (2001, 1001, 37.2, 78, 16, 98, 120, 80, 'None', 3, 'Chest pain'),
            (2002, 1002, 36.8, 82, 18, 97, 115, 75, 'Mild', 2, 'Shortness of breath'),
            (2003, 1003, 38.1, 92, 20, 95, 130, 85, 'Moderate', 4, 'Abdominal pain'),
        ]
        db.execute_many(
            "INSERT INTO dim_triage (stay_id, subject_id, temperature, heartrate, resprate, o2sat, sbp, dbp, pain, acuity, chiefcomplaint) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING",
            triage
        )
        
        # Sample ICU stays
        icustays = [
            (3001, 2001, 1001, 20240101, 20240103, 1, 3,"CC", 'MICU', 48.5, 1),
            (3002, 2002, 1002, 20240102, 20240102, 2, 3,"CC", 'CCU', 12.0, 2),
        ] 
            #         stay_key INT PRIMARY KEY,
            # stay_id INT,
            # subject_id INT,
            # intime_date INT,
            # outtime_date INT,
            # intime_time INT,
            # outtime_time INT, 
            # first_careunit VARCHAR(20), 
            # last_careunit VARCHAR(20),
            # los DOUBLE PRECISION,
            # diagnoses_icd_key INT
        db.execute_many(
        """
        INSERT INTO fact_icustays (
            stay_key,
            stay_id,
            subject_id,
            intime_date,
            outtime_date,
            intime_time,
            outtime_time,
            first_careunit,
            last_careunit,
            los,
            diagnoses_icd_key
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT DO NOTHING
        """,
        icustays
    )
        
        # Sample ED stays
        edstays = [
            (4001, 1001, 5001, 2001, 1, 2, 'M', 'WHITE', 'AMBULANCE', 'ADMITTED'),
            (4002, 1002, 5002, 2002, 2, 3, 'F', 'ASIAN', 'WALK-IN', 'DISCHARGED'),
        ]
        db.execute_many(
            "INSERT INTO fact_edstays (edstay_key, subject_id, hadm_id, stay_id, intime, outtime, gender, race, arrival_transport, disposition) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING",
            edstays
        )
        
        logger.info("Sample data seeded successfully")
        return True
        
    except Exception as e:
        logger.error(f"Error seeding data: {e}")
        return False


if __name__ == "__main__":
    schema = DatabaseSchema()
    if schema.initialize_database():
        print("Database schema initialized successfully")
        if seed_sample_data():
            print("Sample data seeded successfully")
    else:
        print("Failed to initialize database schema")