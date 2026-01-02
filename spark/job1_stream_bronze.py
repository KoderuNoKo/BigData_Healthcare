from pyspark.sql import functions as F
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
