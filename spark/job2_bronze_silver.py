from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DoubleType

from modules.common import build_spark
from modules.topics_schema import chartevents_schema

spark = build_spark("StreamBronzeToSilver")

# ---- Configuration ----
BRONZE_PATH = "s3a://mimic-bronze/chartevents/"
SILVER_PATH = "s3a://mimic-silver/chartevents/"
CHECKPOINT_PATH = "s3a://mimic-silver/checkpoint/chartevents/"

# ---- Read from Bronze (STREAMING) ----
print("Setting up streaming read from Bronze layer...")
df_bronze = (
    spark.readStream
        .format("parquet")
        .schema(chartevents_schema)
        .option("maxFilesPerTrigger", 10)  # Process 10 files at a time
        .load(BRONZE_PATH)
)

# ---- Define Transformation Function ----
def transform_to_silver(df):
    """
    Apply all cleaning and transformation logic.
    This function is applied to each micro-batch.
    """
    # Remove duplicates within the micro-batch
    df_deduplicated = df.dropDuplicates([
        "subject_id", 
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

df_silver = transform_to_silver(df_bronze)

# ---- Write to Silver (STREAMING) 
print(f"Starting streaming write to Silver layer: {SILVER_PATH}")
query = (
    df_silver
        .writeStream
        .format("parquet")
        .option("path", SILVER_PATH)
        .option("checkpointLocation", CHECKPOINT_PATH)
        .outputMode("append")
        .trigger(processingTime="2 minutes")  # Process every 2 minutes
        .start()
)

print("Streaming Bronze -> Silver job started")
print(f"  - Processing interval: 2 minutes")
print(f"  - Max files per trigger: 10")
print(f"  - Checkpoint: {CHECKPOINT_PATH}")

# Monitor the stream
query.awaitTermination()