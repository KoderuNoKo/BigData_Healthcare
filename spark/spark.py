from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

TOPIC = ["icu_chartevents", "hosp_labevents"]

BOOTSTRAP = "cp-kafka:9092"

spark = SparkSession.builder \
    .appName("ICUCharteventsConsumer") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")
# Define the schema of your Kafka JSON message
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

# Read from Kafka
df_chartevents = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", BOOTSTRAP) \
    .option("subscribe", TOPIC[0]) \
    .option("startingOffsets", "latest") \
    .load()
    
parsed_chartevents = df_chartevents.select(from_json(col("value").cast("string"), schema).alias("data")) \
            .select("data.*")
    
df_labevents = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", BOOTSTRAP) \
    .option("subscribe", TOPIC[1]) \
    .option("startingOffsets", "latest") \
    .load()
    
parsed_labevents = df_labevents.select(from_json(col("value").cast("string"), schema).alias("data")) \
            .select("data.*")

# Print stream to console
query = parsed_labevents.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .start()

query.awaitTermination()