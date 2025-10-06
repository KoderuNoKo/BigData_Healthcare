from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

TOPIC = "icu_chartevents"
BOOTSTRAP = "cp-kafka:9092"

spark = SparkSession.builder \
    .appName("ICUCharteventsConsumer") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")
# 1️⃣ Define the schema of your Kafka JSON message
schema = StructType([
    StructField("subject_id", IntegerType()),
    StructField("hadm_id", IntegerType()),
    StructField("stay_id", IntegerType()),
    StructField("caregiver_id", IntegerType()),
    StructField("charttime", StringType()),
    StructField("storetime", StringType()),
    StructField("itemid", IntegerType()),
    StructField("value", StringType()),
    StructField("valuenum", DoubleType()),
    StructField("valueuom", StringType()),
    StructField("warning", StringType())
])

# 2️⃣ Read from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", BOOTSTRAP) \
    .option("subscribe", TOPIC) \
    .option("startingOffsets", "latest") \
    .load()

# 3️⃣ Parse the JSON value field
parsed = df.select(from_json(col("value").cast("string"), schema).alias("data")) \
            .select("data.*")

# 4️⃣ Print stream to console
query = parsed.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .start()

query.awaitTermination()