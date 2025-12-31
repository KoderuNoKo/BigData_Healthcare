"""
Job 4: Incremental Batch Load Fact Tables with Sampling
Reads only NEW data from Silver tier since last run, applies sampling, loads to warehouse
Uses watermark file to track processing progress
"""

from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType
from pyspark.sql.window import Window
from datetime import datetime

from modules.common import build_spark, POSTGRES_CONFIG, FACT_TABLES
from modules.writers import write_to_postgres

spark = build_spark("LoadFacts")

def decompose_timestamp(df, src, date_col, time_col):
    return (
        df.withColumn(
            date_col,
            F.date_format(F.col(src), "yyyyMMdd").cast(IntegerType())
        )
        .withColumn(
            time_col,
            (
                F.hour(F.col(src)) * 3600 +
                F.minute(F.col(src)) * 60 +
                F.second(F.col(src))
            ).cast(IntegerType())
        )
    )

def get_last_watermark(path):
    """Read the last processed_timestamp from watermark file"""
    try:
        watermark_df = spark.read.text(path)
        last_timestamp = watermark_df.first()[0]
        print(f"Last processed: {last_timestamp}")
        return last_timestamp
    except Exception as e:
        print(f"No watermark found (first run)")
        # First run - use a very old date to process all data
        return "2000-01-01 00:00:00"

def save_watermark(timestamp, path):
    """Save current processing timestamp to watermark file"""
    watermark_df = spark.createDataFrame(
        [(timestamp,)], ["timestamp"]
    )
    watermark_df.write.mode("overwrite").text(path)
    print(f"Watermark updated: {timestamp}")

for fact_name, cfg in FACT_TABLES.items():
    last_processed = get_last_watermark(cfg["watermark_path"])
    current_run_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    print(f"INFO - [{fact_name}] -  Loading...")
    print(f"  Processing window: {last_processed} -> {current_run_time}")

    # ----- read silver with watermark -----
    df_silver_all = spark.read.parquet(cfg["silver_path"])
    total_silver_records = df_silver_all.count()
    df_new = df_silver_all.filter(
        F.col("processed_timestamp") > last_processed
    )
    new_records = df_new.count()
    if new_records == 0:
        print(f"\nWARNING - {fact_name} - No new data to process. Exiting.")
        save_watermark(current_run_time, cfg["watermark_path"])
        continue
    print(f"\nDEBUG - [{fact_name}] - Sample of new data:")
    df_new.show(3, truncate=False)
    
    # ----- apply sampling -----
    SAMPLING_METHOD = cfg["sampling_method"]
    SAMPLE_RATE = cfg["sampling_rate"]
    if SAMPLING_METHOD == "stratified_time":
        # Stratified sampling by date to ensure temporal coverage
        df_with_partition = df_new.withColumn(
            "sample_partition",
            F.date_format(F.col("charttime"), "yyyy-MM-dd")
        )
        # Get all unique dates in this batch
        fractions = {
            r[0]: SAMPLE_RATE
            for r in df_with_partition.select("sample_partition").distinct().collect()
        }
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
        print(f"INFO - [{fact_name}] - Skipped sampling!")
        df_sampled = df_new
    sampled_records = df_sampled.count()
    actual_rate = (sampled_records / new_records) * 100 if new_records > 0 else 0
    print(f"INFO - [{fact_name}] - OK! Sampled records: {sampled_records:,}")
    print(f"  Actual sample rate: {actual_rate:.2f}%")
    print(f"  Records discarded: {new_records - sampled_records:,}")
    
    # ---- create time and date key for warehouse schema ----
    timestamp_cols = cfg["timestamp_mappings"]
    for ts in timestamp_cols:
        df_ts = decompose_timestamp(
            df_sampled,
            ts["source"],
            ts["date_col"],
            ts["time_col"]
        )
    df_wh = cfg["transformation_func"](df_ts)
    df_warehouse = cfg["warehouse_select"](df_wh)
    print(f"INFO - [{fact_name}] - OK! Schema transformation complete")

    # ---- FK quality check ----
    condition = None
    for fk in cfg["required_fks"]:
        expr = F.col(fk).isNotNull()
        condition = expr if condition is None else condition & expr
    df_clean = df_warehouse.filter(condition)

    wh_count = df_warehouse.count()
    clean_count = df_clean.count()
    if wh_count - clean_count != 0:
        print(f"WARNING - [{fact_name}] -  Null required field, dropped {wh_count - clean_count} rows")
        
    final_records = df_clean.count()
    dropped_records = sampled_records - final_records
    print(f"\nINFO - [{fact_name}] - OK! FK quality checks complete")
    
    # ---- write to warehouse ----
    print(f"\nINFO - [{fact_name}] - Writing {final_records:,} records to {cfg['table_name']}...")
    write_to_postgres(
        df=df_clean,
        table_name=cfg['table_name'],
        pg_config=POSTGRES_CONFIG,
        mode="append"
    )
    print(f"OK! {fact_name} loaded")
    # save watermark + summary work
    save_watermark(current_run_time, cfg["watermark_path"])
    print(
        f"INFO - [{fact_name}] - Load summary"
        f"  Processed window:       {last_processed} -> {current_run_time}",
        f"  Loaded (from lake)      {new_records} new records",
        f"  Wrote (to warehouse)    {final_records} records",
        f"  Current watermark:      {current_run_time}",
        "-----------------------------------------------------------------",
        sep="\n"
    )
    
spark.stop()