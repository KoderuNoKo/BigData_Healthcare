"""
Job 3: Batch Load Dimension Tables (SCDs)
Loads slowly changing dimensions from source to warehouse
Note: dim_date and dim_time are pre-populated by init_warehouse_schema.sql
This job loads: dim_d_items, dim_patients
"""

# from pyspark.sql import functions as F

from modules.common import build_spark, POSTGRES_CONFIG
from modules.readers import read_local, read_bigquery
from modules.writers import write_to_postgres
from modules.topics_schema import (
    admissions_schema,
    edstays_schema,
    diagnoses_icd_schema,
    d_icd_diagnoses_schema,
    d_items_schema,
    icustays_schema,
    patient_schema,
    edstays_schema,
    triage_schema
)

from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType

spark = build_spark("LoadSCDs")

def apply_timestamp_mappings(df, mappings):
    """helper function, translating timestamp to FKs to dim_date and dim_time"""
    for m in mappings:
        src = m['source']
        date_col = m['date_col']
        time_col = m['time_col']

        # date_key: YYYYMMDD
        df = df.withColumn(
            date_col,
            F.when(
                F.col(src).isNotNull(),
                F.date_format(F.col(src), "yyyyMMdd").cast(IntegerType())
            )
        )

        # time_key: seconds since midnight
        df = df.withColumn(
            time_col,
            F.when(
                F.col(src).isNotNull(),
                (
                    F.hour(F.col(src)) * 3600 +
                    F.minute(F.col(src)) * 60 +
                    F.second(F.col(src))
                ).cast(IntegerType())
            )
        )

        # drop original timestamp
        df = df.drop(src)

    return df


print("\n" + "="*60)
print("JOB 3: Batch Load Dimension Tables")
print("="*60)

# Define tables to load
TABLES = [

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

# =============================================
# MAIN PROCESSING LOOP
# =============================================

loaded_tables = {}

for table in TABLES:
    # big query
    # columns_str = ', '.join(dim['columns'])
    # query = f"""
    #     SELECT {columns_str}
    #     FROM `physionet-data.{dim['dataset']}.{dim['source_table']}`
    # """
    
    # df = read_bigquery(
    #     spark,
    #     query=query,
    #     project="bigdata-healthcare"
    # )
    
    # local CSV
    if table['name'] == 'dim_diagnoses_icd':
        df_diag = read_local(
            spark,
            path=table['local_path'],
            fmt="csv",
            schema=table['schema']
        ).select(
            'seq_num',
            'icd_code',
            'icd_version'
        )

        df_dict = read_local(
            spark,
            path=table['local_path_d'],
            fmt="csv",
            schema=table['schema_d']
        ).select(
            'icd_code',
            'icd_version',
            'long_title'
        )

        # Join diagnoses with dictionary
        df = (
            df_diag
            .join(
                df_dict,
                on=['icd_code', 'icd_version'],
                how='inner'
            )
            # Enforce dimension grain
            .groupBy('icd_code', 'icd_version', 'long_title')
            .agg(
                F.min('seq_num').alias('seq_num')
            )
            .select(*table['columns'])
        )
    else:
        df = read_local(
            spark,
            path=table['local_path'],
            fmt="csv",
            schema=table['schema']
        )
        # transform original timstamp to warehouse time (dim_date and dim_time)
        if 'timestamp_mappings' in table:
            print("  Applying timestamp decomposition...")
            df = apply_timestamp_mappings(df, table['timestamp_mappings'])
            
        df = df.select(*table['columns'])
    
    print(f"OK! Data source: Local CSV")
    print(f"  Path: {table['local_path']}")
    
    # remove duplicates
    initial_count = df.count()
    df = df.dropDuplicates(table['primary_key'])
    
    # remove null primary key
    df = df.dropna(subset=table['primary_key'])
    
    final_count = df.count()

    print(f"\nData Quality:")
    print(f"  Initial records: {initial_count:,}")
    print(f"  Final records: {final_count:,}")
    
    # Show sample
    print(f"\nSample data (first 3 rows):")
    df.show(3, truncate=True)
    
    # Write to warehouse
    print(f"\nWriting to warehouse table: {table['name']}")
    try:
        write_to_postgres(
            df=df,
            table_name=table['name'],
            pg_config=POSTGRES_CONFIG,
            mode="append"
        )
        print(f"OK! {table['name']} loaded")
        loaded_tables[table['name']] = final_count
        
    except Exception as e:
        print(f"ERROR! Failed to write to warehouse: {str(e)[:200]}")
        print(f"  Skipping {table['name']}...")
        continue

# =============================================
# SUMMARY
# =============================================
print("\n" + "="*60)
print("OK! JOB 3 COMPLETE: Dimension Loading Summary")
print("="*60)

if loaded_tables:
    for table_name, count in loaded_tables.items():
        print(f"  {table_name:20s}: {count:>10,} records")
else:
    print("  WARNING! No tables were loaded")

spark.stop()