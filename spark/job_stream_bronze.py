from pyspark.sql import functions as F
from modules.common import build_spark
from modules.topics_schema import chartevents_schema

spark = build_spark("StreamCharteventsToBronze")

KAFKA_SERVER = "cp-kafka:9092"
TOPIC = "icu_chartevents"

# ---- Read from Kafka ----
df_raw = (
    spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_SERVER)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "latest")
        .load()
)

# ---- Parse JSON payload ----
df_parsed = (
    df_raw
        .select(
            F.from_json(
                F.col("value").cast("string"),
                chartevents_schema
            ).alias("data"),
            # add kafka metadata (not sure necessary) 
            # F.col("topic"),
            # F.col("partition"),
            # F.col("offset"),
            # F.col("timestamp").alias("kafka_timestamp")
        )
        .select("data.*", 
            # "topic", "partition", "offset", "kafka_timestamp"
        )
)

# ---- Add ingestion metadata (not sure necessary) ----
# df_bronze = (
#     df_parsed
#         .withColumn("ingest_ts", F.current_timestamp())
# )

# ---- Write to MinIO Bronze ----
query = (
    df_parsed
        .writeStream
        .format("parquet")
        .option("path", "s3a://mimic-bronze/chartevents/")
        .option("checkpointLocation", "s3a://mimic-bronze/checkpoint/chartevents/")
        .outputMode("append")
        .trigger(processingTime="1 minute")
        .start()
)

query.awaitTermination()
