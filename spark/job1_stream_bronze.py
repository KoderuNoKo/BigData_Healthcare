from pyspark.sql import functions as F
<<<<<<< HEAD
from modules.common import build_spark, FACT_TABLES

spark = build_spark("StreamToBronze")

KAFKA_SERVER = "cp-kafka:9092"

queries = []

for topic, cfg in FACT_TABLES.items():
    df_raw = (
        spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_SERVER)
            .option("subscribe", topic)
            .option("startingOffsets", "latest")
            .load()
    )

    df_parsed = (
        df_raw
            .select(
                F.from_json(
                    F.col("value").cast("string"),
                    cfg["schema"]
                ).alias("data")
            )
            .select("data.*")
            .withColumn("ingest_ts", F.current_timestamp())
    )

    query = (
        df_parsed
            .writeStream
            .format("parquet")
            .option("path", cfg["bronze_path"])
            .option("checkpointLocation", cfg["checkpoint_bronze"])
            .option("startingOffsets", "earliest")
            .outputMode("append")
            .start()
    )

    queries.append(query)

spark.streams.awaitAnyTermination()
=======
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
>>>>>>> 22b70e8fe9c8765ef4d864170f09bd9aa70f22d1
