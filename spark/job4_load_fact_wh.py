"""
Job 4: Incremental Batch Load Fact Tables with Sampling
Reads only NEW data from Silver tier since last run, applies sampling, loads to warehouse
Uses watermark file to track processing progress
"""

from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType
from pyspark.sql.window import Window
from datetime import datetime

from modules.common import build_spark, POSTGRES_CONFIG
from modules.writers import write_to_postgres

spark = build_spark("LoadFactChartevents")

print("\n" + "="*60)
print("JOB 4: Incremental Batch Load Fact Table")
print("="*60)

# =============================================
# CONFIGURATION
# =============================================
SILVER_PATH = "s3a://mimic-silver/chartevents/"
WATERMARK_PATH = "s3a://mimic-silver/.watermarks/fact_chartevents_last_processed.txt"
SAMPLE_RATE = 0.10  # 10% sample - adjust as needed
SAMPLING_METHOD = "stratified_time"  # or "random", "systematic"

print(f"\nConfiguration:")
print(f"  Silver path: {SILVER_PATH}")
print(f"  Watermark path: {WATERMARK_PATH}")
print(f"  Sampling method: {SAMPLING_METHOD}")
print(f"  Sample rate: {SAMPLE_RATE * 100}%")

# =============================================
# 1. GET LAST PROCESSING WATERMARK
# =============================================
print("\n=== [1/6] Reading Processing Watermark ===")

def get_last_watermark():
    """Read the last processed_timestamp from watermark file"""
    try:
        watermark_df = spark.read.text(WATERMARK_PATH)
        last_timestamp = watermark_df.first()[0]
        print(f"Last processed: {last_timestamp}")
        return last_timestamp
    except Exception as e:
        print(f"No watermark found (first run)")
        # First run - use a very old date to process all data
        return "2000-01-01 00:00:00"

def save_watermark(timestamp):
    """Save current processing timestamp to watermark file"""
    watermark_df = spark.createDataFrame(
        [(timestamp,)], ["timestamp"]
    )
    watermark_df.write.mode("overwrite").text(WATERMARK_PATH)
    print(f"Watermark updated: {timestamp}")

last_processed = get_last_watermark()
current_run_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

print(f"  Processing window: {last_processed} -> {current_run_time}")

# =============================================
# 2. READ ONLY NEW DATA FROM SILVER
# =============================================
print("\n=== [2/6] Reading NEW data from Silver ===")

# Read all silver data first
df_silver_all = spark.read.parquet(SILVER_PATH)
total_silver_records = df_silver_all.count()
print(f"  Total records in silver: {total_silver_records:,}")

# Filter to only NEW records based on processed_timestamp
# This assumes Job 2 (bronze→silver) adds processed_timestamp column
df_new = df_silver_all.filter(
    F.col("processed_timestamp") > last_processed
)

new_records = df_new.count()
print(f"New records since last run: {new_records:,}")

if new_records == 0:
    print("\nNo new data to process. Exiting.")
    save_watermark(current_run_time)
    spark.stop()
    exit(0)

# Show sample of new data
print("\nSample of new data:")
df_new.show(3, truncate=False)

# =============================================
# 3. APPLY SAMPLING STRATEGY
# =============================================
print(f"\n=== [3/6] Applying {SAMPLING_METHOD.upper()} Sampling ===")

if SAMPLING_METHOD == "stratified_time":
    # Stratified sampling by date to ensure temporal coverage
    df_with_partition = df_new.withColumn(
        "sample_partition",
        F.date_format(F.col("charttime"), "yyyy-MM-dd")
    )
    
    # Get all unique dates in this batch
    unique_dates = [row[0] for row in 
                   df_with_partition.select("sample_partition")
                   .distinct().collect()]
    
    # Create fractions dict (same sample rate for all dates)
    fractions = {date: SAMPLE_RATE for date in unique_dates}
    
    print(f"  Partitions (unique dates): {len(unique_dates)}")
    print(f"  Date range: {min(unique_dates)} to {max(unique_dates)}")
    print(f"  Sample rate per partition: {SAMPLE_RATE * 100}%")
    
    # Sample within each partition
    df_sampled = df_with_partition.sampleBy(
        col="sample_partition",
        fractions=fractions,
        seed=42  # For reproducibility
    ).drop("sample_partition")
    
elif SAMPLING_METHOD == "random":
    # Simple random sampling
    df_sampled = df_new.sample(
        withReplacement=False,
        fraction=SAMPLE_RATE,
        seed=42
    )
    
elif SAMPLING_METHOD == "systematic":
    # Systematic sampling (every Nth record)
    n = int(1 / SAMPLE_RATE)
    df_sampled = df_new.withColumn(
        "row_num",
        F.row_number().over(Window.orderBy(F.monotonically_increasing_id()))
    ).filter(F.col("row_num") % n == 0).drop("row_num")
else:
    raise ValueError(f"Unknown sampling method: {SAMPLING_METHOD}")

sampled_records = df_sampled.count()
actual_rate = (sampled_records / new_records) * 100 if new_records > 0 else 0

print(f"OK! Sampled records: {sampled_records:,}")
print(f"  Actual sample rate: {actual_rate:.2f}%")
print(f"  Records discarded: {new_records - sampled_records:,}")

# =============================================
# 4. TRANSFORM TO WAREHOUSE SCHEMA
# =============================================
print("\n=== [4/6] Transforming to Warehouse Schema ===")

# Step 4a: Create date_key from chartdate (YYYYMMDD format)
print("  - Creating date_key (YYYYMMDD)...")
df_with_datekey = df_sampled.withColumn(
    "date_key", 
    F.when(
        F.col("charttime").isNotNull(),
        F.date_format(F.col("charttime"), "yyyyMMdd").cast(IntegerType())
    )
)

# Step 4b: Create time_key from charttime (seconds since midnight)
print("  - Creating time_key (seconds since midnight)...")
df_with_timekey = df_with_datekey.withColumn(
    "time_key",
    F.when(
        F.col("charttime").isNotNull(),
        (F.hour(F.col("charttime")) * 3600 +
         F.minute(F.col("charttime")) * 60 +
         F.second(F.col("charttime"))).cast(IntegerType())
    )
)

# Step 4c: Select and map columns to warehouse schema
print("  - Mapping columns to fact table schema...")
df_warehouse = df_with_timekey.select(
    # Don't include chartevents_key - PostgreSQL SERIAL handles this
    F.col("subject_id").cast(IntegerType()).alias("subject_id"),
    F.col("hadm_id").cast(IntegerType()).alias("hadm_id"),
    F.col("stay_id").cast(IntegerType()).alias("stay_id"),
    F.col("caregiver_id").cast(IntegerType()).alias("caregiver_id"),
    F.col("date_key").alias("chartdate"),     # FK to dim_date
    F.col("time_key").alias("charttime"),     # FK to dim_time
    F.col("itemid").cast(IntegerType()).alias("itemid"),
    F.col("value").alias("value"),
    F.col("valuenum").alias("valuenum"),
    F.col("valueuom").alias("valueuom"),
    F.col("warning").cast(IntegerType()).alias("warning")
)

print(" Schema transformation complete")

# =============================================
# 5. DATA QUALITY CHECKS
# =============================================
print("\n=== [5/6] Data Quality Checks ===")

# Check for nulls in critical FK fields
null_checks = {
    "subject_id": df_warehouse.filter(F.col("subject_id").isNull()).count(),
    "itemid": df_warehouse.filter(F.col("itemid").isNull()).count(),
    "chartdate": df_warehouse.filter(F.col("chartdate").isNull()).count(),
    "charttime": df_warehouse.filter(F.col("charttime").isNull()).count()
}

print("\nNull counts in FK fields:")
any_nulls = False
for field, null_count in null_checks.items():
    status = "OK" if null_count == 0 else "ERROR"
    print(f"  {status} {field}: {null_count:,} nulls")
    if null_count > 0:
        any_nulls = True

# Filter out records with null FKs (would violate constraints)
if any_nulls:
    print("\nWARNING! Removing records with null foreign keys...")
    df_clean = df_warehouse.filter(
        F.col("subject_id").isNotNull() &
        F.col("itemid").isNotNull() &
        F.col("chartdate").isNotNull() &
        F.col("charttime").isNotNull()
    )
else:
    df_clean = df_warehouse

final_records = df_clean.count()
dropped_records = sampled_records - final_records

print(f"\nOK! Quality checks complete")
print(f"  Records after QC: {final_records:,}")
print(f"  Records dropped: {dropped_records:,}")

if final_records == 0:
    print("\nWARNING! No valid records to load after quality checks. Exiting.")
    save_watermark(current_run_time)
    spark.stop()
    exit(0)

# =============================================
# 6. WRITE TO WAREHOUSE
# =============================================
print("\n=== [6/6] Loading to Warehouse ===")

print("\nSample of final data:")
df_clean.show(5, truncate=False)

print(f"\nWriting {final_records:,} records to fact_chartevents...")

try:
    write_to_postgres(
        df=df_clean,
        table_name="fact_chartevents",
        pg_config=POSTGRES_CONFIG,
        mode="append"  # Always append for incremental loads
    )
    print("OK! Data loaded successfully")
    
    # Update watermark ONLY after successful write
    save_watermark(current_run_time)
    
except Exception as e:
    print(f"ERROR Failed to load data: {e}")
    print("WARNING! Watermark NOT updated - will retry this batch next run")
    spark.stop()
    exit(1)

# =============================================
# SUMMARY
# =============================================
print("\n" + "="*60)
print("OK! JOB 4 COMPLETE: Incremental load successful")
print("="*60)
print(f"  Processing window:      {last_processed}")
print(f"                          → {current_run_time}")
print(f"  New records in silver:  {new_records:>12,} records")
print(f"  After sampling:         {sampled_records:>12,} records ({actual_rate:.1f}%)")
print(f"  After quality checks:   {final_records:>12,} records")
print(f"  Loaded to warehouse:    {final_records:>12,} records")
print(f"  Watermark updated:      {current_run_time}")
print("="*60)
print("\nNext run will process records after: " + current_run_time)
print("="*60)

spark.stop()