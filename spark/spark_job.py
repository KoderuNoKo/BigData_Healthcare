# spark_job.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, from_json, to_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

from modules.kafka_io import KafkaIO
from modules.sampler import DataSampler

KAFKA_BOOTSTRAP_SERVERS = 'cp-kafka:9092'

spark = (SparkSession.builder
    .appName("SparkProcessor")
    .master("local[*]")
    .config("spark.driver.memory", "4g")
    .config("spark.executor.memory", "4g")
    .config("spark.executor.cores", "4")
    # Add Kafka package for reading from Kafka
    .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1")
    .getOrCreate()
)

chartevents_schema = StructType([
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

d_items_schema = StructType([
    StructField("itemid", IntegerType()),
    StructField("label", StringType()),
    StructField("abbreviation", StringType()),
    StructField("linksto", StringType()),
    StructField("category", StringType()),
    StructField("unitname", StringType()),
    StructField("param_type", StringType()),
    StructField("lownormalvalue", DoubleType()),
    StructField("highnormalvalue", DoubleType()),
])

kafka_io = KafkaIO(spark, KAFKA_BOOTSTRAP_SERVERS)
sampler = DataSampler()


from pyspark.sql.functions import from_json, col, expr

df_chartevents_raw = (spark.readStream
    .format('kafka')
    .option('kafka.bootstrap.servers', KAFKA_BOOTSTRAP_SERVERS)
    .option('subscribe', 'icu_chartevents')
    .option('startingOffsets', 'earliest')
    .option("maxOffsetsPerTrigger", "50")
    .load()
)

df_d_items_raw = (spark.read
    .format('kafka')
    .option('kafka.bootstrap.servers', KAFKA_BOOTSTRAP_SERVERS)
    .option('subscribe', 'icu_d_items')
    .option('startingOffsets', 'earliest')
    .option('endingOffsets', 'latest')
    .load()
)

df_chartevents = (df_chartevents_raw
    .select(from_json(col('value').cast('string'), chartevents_schema).alias('data'))
    .select('data.*')
)

df_d_items = (df_d_items_raw
    .select(from_json(col('value').cast('string'), d_items_schema).alias('data'))
    .select('data.*')
)
df_d_items.cache()

df_joined = df_chartevents.join(df_d_items, on='itemid', how='left')

df_processed = df_joined.withColumn("is_critical", 
    expr("valuenum < lownormalvalue OR valuenum > highnormalvalue")
)

sampled_df = sampler.conditional_sample(
    df_processed, 
    condition_col="is_critical", 
    pass_rate=0.10
)

query = sampled_df.writeStream \
    .format("console") \
    .outputMode("append") \
    .option("checkpointLocation", '/home/dev/app/spark/checkpoints') \
    .trigger(processingTime="10 second") \
    .start()

query.awaitTermination()