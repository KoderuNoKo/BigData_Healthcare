from pyspark.sql import functions as F
from pyspark.sql.types import *
from modules.common import build_spark
from modules.topics_schema import *

spark = build_spark("StreamCharteventsBronze")

KAFKA_SERVER = "cp-kafka:9092"

# ---- Read from Kafka ----
df_raw = (
    spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_SERVER)
        .option("subscribe", "icu_chartevents")
        .option("startingOffsets", "latest")
        .load()
)

# Kafka data assumed to be JSON in the "value" field
df_parsed = df_raw.select(
    F.from_json(
        F.col("value").cast("string"), 
        schema=chartevents_schema
    ).alias("data")
).select("data.*")

(
    df_parsed
        .writeStream
        .format("parquet")
        .option("path", "s3a://mimic-bronze/chartevents/")
        .option("checkpointLocation", "s3a://mimic-bronze/checkpoint/chartevents/")
        .outputMode("append")
        .start()
        .awaitTermination()
)
