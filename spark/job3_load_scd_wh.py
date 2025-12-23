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
from modules.topics_schema import d_items_schema, patient_schema

spark = build_spark("LoadSCDs")

print("\n" + "="*60)
print("JOB 3: Batch Load Dimension Tables")
print("="*60)

# Define tables to load
DIMENSIONS = [
    {
        'name': 'dim_d_items',
        'source_table': 'd_items',
        'dataset': 'mimiciv_icu',
        'local_path': '/app/data/d_items.csv',
        'columns': [
            'itemid',
            'label',
            'abbreviation',
            'category',
            'unitname',
            'param_type',
            'lownormalvalue',
            'highnormalvalue'
        ],
        'primary_key': 'itemid',
        'schema': d_items_schema
    },
    {
        'name': 'dim_patients',
        'source_table': 'patients',
        'dataset': 'mimiciv_hosp',
        'local_path': '/app/data/patients.csv',
        'columns': [
            'subject_id',
            'gender',
            'anchor_age',
            'anchor_year',
            'anchor_year_group',
            'dod'
        ],
        'primary_key': 'subject_id',
        'schema': patient_schema
    }
]

# =============================================
# MAIN PROCESSING LOOP
# =============================================

loaded_tables = {}

for dim in DIMENSIONS:
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
    df = read_local(
        spark,
        path=dim['local_path'],
        fmt="csv",
        schema=dim['schema']
    ).select(*dim['columns'])
    
    print(f"OK! Data source: Local CSV")
    print(f"  Path: {dim['local_path']}")
    
    # remove duplicates
    initial_count = df.count()
    df = df.dropDuplicates([dim['primary_key']])
    final_count = df.count()
    duplicates_removed = initial_count - final_count
    
    # remove null primary key
    df = df.dropna(subset=[dim['primary_key']])
    
    print(f"\nData Quality:")
    print(f"  Initial records: {initial_count:,}")
    print(f"  Duplicates removed: {duplicates_removed:,}")
    print(f"  Final records: {final_count:,}")
    
    # Show sample
    print(f"\nSample data (first 3 rows):")
    df.show(3, truncate=True)
    
    # Write to warehouse
    print(f"\nWriting to warehouse table: {dim['name']}")
    try:
        write_to_postgres(
            df=df,
            table_name=dim['name'],
            pg_config=POSTGRES_CONFIG,
            mode="append"
        )
        print(f"OK! {dim['name']} loaded successfully")
        loaded_tables[dim['name']] = final_count
        
    except Exception as e:
        print(f"ERROR! Failed to write to warehouse: {str(e)[:200]}")
        print(f"  Skipping {dim['name']}...")
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
    print("  WARNING! No tables were loaded successfully")

print("\nPre-populated dimensions:")
print(f"  {'dim_date':20s}: ~73,000 records (2100-2299)")
print(f"  {'dim_time':20s}:  86,400 records (second grain)")

print("="*60)

# Verification query suggestion
print("\nTo verify in PostgreSQL:")
print("  SELECT 'dim_d_items' as table_name, COUNT(*) FROM dim_d_items")
print("  UNION ALL")
print("  SELECT 'dim_patients', COUNT(*) FROM dim_patients;")

spark.stop()