from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DoubleType

from modules.common import build_spark

spark = build_spark("StreamBronzeToSilver")

# ---- Configuration ----
from modules.common import FACT_TABLES

queries = []

for topic, cfg in FACT_TABLES.items():
    # ---- Read from Bronze (STREAMING) ----
    print("Setting up streaming read from Bronze layer...")
    df_bronze = (
        spark.readStream
            .format("parquet")
            .schema(cfg["schema"])
            .option("maxFilesPerTrigger", 10)  # Process 10 files at a time
            .load(cfg['bronze_path'])
    )

    transform_to_silver = cfg["transformation_func"]
    df_silver = transform_to_silver(df_bronze)

    # ---- Write to Silver (STREAMING) 
    print(f"Starting streaming write to Silver layer: {cfg['silver_path']}")
    query = (
        df_silver
            .writeStream
            .format("parquet")
            .option("path", cfg['silver_path'])
            .option("checkpointLocation", cfg['checkpoint_silver'])
            .outputMode("append")
            .trigger(processingTime="1 minutes")  # process every minute
            .start()
    )
    
    queries.append(query)

    print("Streaming Bronze -> Silver job started")
    print(f"  - Processing interval: 2 minutes")
    print(f"  - Max files per trigger: 10")
    print(f"  - Checkpoint: {cfg['checkpoint_silver']}")

# Monitor the stream
spark.streams.awaitAnyTermination()
