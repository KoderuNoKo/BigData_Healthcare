from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from .topics_schema import *

POSTGRES_CONFIG = {
    "url": "jdbc:postgresql://postgres:5432/mimic_dw",
    "user": "dev",
    "password": "devpassword",
    "driver": "org.postgresql.Driver"
}

# ---- Define Transformation Function ----
def chartevents_transform_to_silver(df):
    """
    Apply all cleaning and transformation logic.
    This function is applied to each micro-batch.
    """
    # Remove duplicates within the micro-batch
    df_deduplicated = df.dropDuplicates([
        "subject_id",
        "hadm_id",
        "stay_id",
        "charttime", 
        "itemid"
    ])
    
    # Filter out records with missing critical fields
    df_clean = df_deduplicated.filter(
        F.col("subject_id").isNotNull() &
        F.col("stay_id").isNotNull() &
        F.col("itemid").isNotNull() &
        F.col("charttime").isNotNull()
    )
    
    # Data type enforcement
    df_typed = df_clean.withColumn(
        "valuenum",
        F.col("valuenum").cast(DoubleType())
    ).withColumn(
        "warning",
        F.col("warning").cast(IntegerType())
    )
    
    # Standardization
    df_standardized = df_typed.withColumn(  # timestamp
        "charttime",
        F.to_timestamp(F.col("charttime"))
    )
    df_standardized = df_standardized.withColumn(   # string trimming
        "value",
        F.when(F.trim(F.col("value")) == "", None)
         .otherwise(F.trim(F.col("value")))
    ).withColumn(
        "valueuom",
        F.when(F.trim(F.col("valueuom")) == "", None)
         .otherwise(F.trim(F.col("valueuom")))
    )
    
    df_silver = df_standardized.withColumn(
        "processed_timestamp",
        F.current_timestamp()
    )
    
    return df_silver

def microbiologyevents_transform_to_silver(df):
    return (
        df.dropDuplicates(["microevent_id"])
          .filter(F.col("microevent_id").isNotNull())
          .withColumn("charttime", F.col("charttime").cast("timestamp"))
          .withColumn("processed_timestamp", F.current_timestamp())
    )
    
def chartevents_warehouse_select(df):
    return df.select(
        F.col("subject_id").cast(IntegerType()),
        F.col("hadm_id").cast(IntegerType()),
        F.col("stay_id").cast(IntegerType()),
        F.col("caregiver_id").cast(IntegerType()),
        F.col("charttime_date"),
        F.col("charttime_time"),
        F.col("itemid").cast(IntegerType()),
        F.col("value"),
        F.col("valuenum"),
        F.col("valueuom"),
        F.col("warning").cast(IntegerType())
    )
    
def microbiologyevents_warehouse_select(df):
    return df.select(
        F.col("microevent_id").cast(IntegerType()),
        F.col("subject_id").cast(IntegerType()),
        F.col("hadm_id").cast(IntegerType()),
        F.col("micro_specimen_id").cast(IntegerType()),
        F.col("order_provider_id"),
        F.col("charttime_date"),
        F.col("charttime_time"),
        F.col("spec_itemid").cast(IntegerType()),
        F.col("spec_type_desc"),
        F.col("test_seq").cast(IntegerType()),
        F.col("test_itemid").cast(IntegerType()),
        F.col("test_name"),
        F.col("org_itemid").cast(IntegerType()),
        F.col("isolate_num").cast(IntegerType()),
        F.col("quantity"),
        F.col("ab_itemid").cast(IntegerType()),
        F.col("ab_name"),
        F.col("dilution_text"),
        F.col("dilution_comparison"),
        F.col("interpretation"),
        F.col("comments")
    )

POSTGRES_CONFIG = {
    "url": "jdbc:postgresql://postgres:5432/mimic_dw",
    "user": "dev",
    "password": "devpassword",
    "driver": "org.postgresql.Driver"
}

FACT_TABLES = {
    "icu_chartevents": {
        # lake config (bronze)
        "schema": chartevents_schema,
        "bronze_path": "s3a://mimic-bronze/chartevents/",
        "checkpoint_bronze": "s3a://mimic-bronze/checkpoint/chartevents/",
        
        # lake config (silver)
        "silver_path": "s3a://mimic-silver/chartevents/",
        "watermark_path": "s3a://mimic-silver/watermarks/icu_chartevents_last_processed.txt",
        "checkpoint_silver": "s3a://mimic-silver/checkpoint/chartevents/",
        "transformation_func": chartevents_transform_to_silver,
        
        # warehouse config 
        "table_name": "fact_chartevents",
        "warehouse_select": chartevents_warehouse_select,
        "timestamp_col": "charttime",
        'timestamp_mappings': [
            {
                'source': 'charttime',
                'date_col': 'charttime_date',
                'time_col': 'charttime_time'
            },
        ],
        "required_fks": ["subject_id", "itemid", "charttime_date", "charttime_time"],
        "sampling_method": "none",
        "sampling_rate": 0.1
        
    },
    "hosp_microbiologyevents": {
        # lake config (bronze)
        "schema": microbiologyevents_schema,
        "bronze_path": "s3a://mimic-bronze/microbiologyevents/",
        "checkpoint_bronze": "s3a://mimic-bronze/checkpoint/microbiologyevents/",
        
        # lake config (silver)
        "silver_path": "s3a://mimic-silver/microbiologyevents/",
        "watermark_path": "s3a://mimic-silver/watermarks/fact_microbiologyevents_last_processed.txt",
        "checkpoint_silver": "s3a://mimic-silver/checkpoint/microbiologyevents/",
        "transformation_func": microbiologyevents_transform_to_silver,
        
        # warehouse config
        "table_name": "fact_microbiologyevents",
        "warehouse_select": microbiologyevents_warehouse_select,
        "timestamp_col": "charttime",
        'timestamp_mappings': [
            {
                'source': 'charttime',
                'date_col': 'charttime_date',
                'time_col': 'charttime_time'
            },
        ],
        "required_fks": ["subject_id", "charttime_date", "charttime_time"],
        "sampling_method": "none",
        "sampling_rate": 0.1
    }
}


SCD_TABLES = [

    # =========================
    # DIMENSIONS
    # =========================
    {
        'name': 'dim_d_items',
        'local_path': '/app/data/d_items.csv',
        'columns': [
            'itemid', 'label', 'abbreviation', 'category',
            'unitname', 'param_type',
            'lownormalvalue', 'highnormalvalue'
        ],
        'primary_key': ['itemid'],
        'schema': d_items_schema
    },
    {
        'name': 'dim_patients',
        'local_path': '/app/data/patients.csv',
        'columns': [
            'subject_id', 'gender', 'anchor_age',
            'anchor_year', 'anchor_year_group', 'dod'
        ],
        'primary_key': ['subject_id'],
        'schema': patient_schema
    },
    {
        'name': 'dim_diagnoses_icd',
        'local_path': '/app/data/diagnoses_icd.csv',
        'local_path_d': '/app/data/d_icd_diagnoses.csv',
        'columns': [
            'seq_num',
            'icd_code',
            'icd_version',
            'long_title'
        ],
        'primary_key': ['icd_code', 'icd_version'],
        'schema': diagnoses_icd_schema,
        'schema_d': d_icd_diagnoses_schema
    },
    {
        'name': 'dim_triage',
        'local_path': '/app/data/triage.csv',
        'columns': [
            'stay_id', 'subject_id', 'temperature',
            'heartrate', 'resprate', 'o2sat',
            'sbp', 'dbp', 'pain', 'acuity',
            'chiefcomplaint'
        ],
        'primary_key': ['stay_id'],
        'schema': triage_schema
    },
    {
        'name': 'dim_admission',
        'local_path': '/app/data/admissions.csv',
        'columns': [
            'hadm_id', 'subject_id',
            'admittime_time', 'admittime_date',
            'dischtime_time', 'dischtime_date',
            'deathtime_time', 'deathtime_date',
            'admission_type', 'admit_provider_id',
            'admission_location', 'discharge_location',
            'insurance', 'language', 'marital_status',
            'race',
            'edregtime_time', 'edregtime_date',
            'edouttime_time', 'edouttime_date',
            'hospital_expire_flag'
        ],
        'primary_key': ['hadm_id'],
        'schema': admissions_schema,
        'timestamp_mappings': [
            {
                'source': 'admittime',
                'date_col': 'admittime_date',
                'time_col': 'admittime_time'
            },
            {
                'source': 'dischtime',
                'date_col': 'dischtime_date',
                'time_col': 'dischtime_time'
            },
            {
                'source': 'deathtime',
                'date_col': 'deathtime_date',
                'time_col': 'deathtime_time',
            },            {
                'source': 'edregtime',
                'date_col': 'edregtime_date',
                'time_col': 'edregtime_time',
            },
            {
                'source': 'edouttime',
                'date_col': 'edouttime_date',
                'time_col': 'edouttime_time',
            },
        ],
    },

    # =========================
    # FACT-LESS FACTS
    # =========================
    {
        'name': 'fact_icustays',
        'local_path': '/app/data/icustays.csv',
        'columns': [
            'stay_id', 'subject_id', 'hadm_id',
            'first_careunit', 'last_careunit',
            'intime_time', 'intime_date',
            'outtime_time', 'outtime_date',
            'los',
        ],
        'primary_key': ['stay_id'],
        'schema': icustays_schema,
        'timestamp_mappings': [
            {
                'source': 'intime',
                'date_col': 'intime_date',
                'time_col': 'intime_time'
            },
            {
                'source': 'outtime',
                'date_col': 'outtime_date',
                'time_col': 'outtime_time'
            }
        ],
    },
    {
        'name': 'fact_edstays',
        'local_path': '/app/data/edstays.csv',
        'columns': [
            'subject_id', 'hadm_id', 'stay_id',
            'intime_date', 'intime_time',
            'outtime_date', 'outtime_time',
            'gender', 'race',
            'arrival_transport', 'disposition',
        ],
        'primary_key': ['stay_id'],
        'schema': edstays_schema,
        'timestamp_mappings': [
            {
                'source': 'intime',
                'date_col': 'intime_date',
                'time_col': 'intime_time'
            },
            {
                'source': 'outtime',
                'date_col': 'outtime_date',
                'time_col': 'outtime_time'
            }
        ],
    }
]

def build_spark(app_name):
    return (
        SparkSession.builder
            .appName(app_name)
            .master("local[*]")
            .config("spark.driver.memory", "4g")
            .config("spark.executor.memory", "4g")
            .config("spark.executor.cores", "4")
            # kafka
            .config("spark.jars.packages",
                    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1")            # s3a / minio
            .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
            .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
            .config("spark.hadoop.fs.s3a.secret.key", "minioadmin123")
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
            .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                    "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
            # postgresql
            .config("spark.jars", "/opt/spark/jars/postgresql-42.6.0.jar")
            .getOrCreate()
    )

