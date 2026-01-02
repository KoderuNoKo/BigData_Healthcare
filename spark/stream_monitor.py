from pyspark.sql import functions as F
from modules.common import build_spark
from modules.topics_schema import chartevents_schema

spark = build_spark("BronzeCharteventsMonitor")

# Read Bronze as STREAM
bronze_stream_df = (
    spark.readStream
         .schema(chartevents_schema)
         .format("parquet")
         .load("s3a://mimic-bronze/chartevents/")
)

# 1) Print schema ONCE
print("\n=== Bronze Chartevents Schema ===")
bronze_stream_df.printSchema()

# 2) Monitoring logic per batch
def monitor_batch(batch_df, batch_id):
    print("\n" + "=" * 60)
    print(f"Batch ID: {batch_id}")

    # Row count in this batch
    batch_count = batch_df.count()
    print(f"Rows in this batch: {batch_count}")
    if batch_count > 0:
        print("Sample rows:")
        batch_df.orderBy(F.col("charttime").desc()) \
                .show(3, truncate=False)
    else:
        print("No data in this batch")
        
    # cumulative row count
    total_rows = (
        spark.read
            .parquet("s3a://mimic-bronze/chartevents/")
            .count()
    )
    print(f"Total rows in Bronze so far: {total_rows}")


# 3) Start streaming monitor
query = (
    bronze_stream_df
        .writeStream
        .foreachBatch(monitor_batch)
        .outputMode("append")
        .option(
            "checkpointLocation",
            "s3a://mimic-bronze/checkpoints/monitor_chartevents"
        )
        .trigger(processingTime="30 seconds")  # periodic
        .start()
)

query.awaitTermination()
