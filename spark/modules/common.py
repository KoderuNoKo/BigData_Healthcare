from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from .topics_schema import *

POSTGRES_CONFIG = {
    "url": "jdbc:postgresql://postgres:5432/mimic_dw",
    "user": "dev",
    "password": "devpassword",
    "driver": "org.postgresql.Driver"
}

# ---- Define Transformation Function ----
def chartevents_transform_to_silver(df):
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

def microbiologyevents_transform_to_silver(df):
    return (
        df.dropDuplicates(["micro_specimen_id"])
          .filter(F.col("micro_specimen_id").isNotNull())
          .withColumn("charttime", F.col("charttime").cast("timestamp"))
    )

POSTGRES_CONFIG = {
    "url": "jdbc:postgresql://postgres:5432/mimic_dw",
    "user": "dev",
    "password": "devpassword",
    "driver": "org.postgresql.Driver"
}

FACT_TABLES = {
    "icu_chartevents": {
        "bronze_path": "s3a://mimic-bronze/chartevents/",
        "silver_path": "s3a://mimic-silver/chartevents/",
        "checkpoint_bronze": "s3a://mimic-bronze/checkpoint/chartevents/",
        "checkpoint_silver": "s3a://mimic-silver/checkpoint/chartevents/",
        "schema": chartevents_schema,
        "transformation_func": chartevents_transform_to_silver
    },
    "hosp_microbiologyevents": {
        "bronze_path": "s3a://mimic-bronze/microbiologyevents/",
        "silver_path": "s3a://mimic-silver/microbiologyevents/",
        "checkpoint_bronze": "s3a://mimic-bronze/checkpoint/microbiologyevents/",
        "checkpoint_silver": "s3a://mimic-silver/checkpoint/microbiologyevents/",
        "schema": microbiologyevents_schema,
        "transformation_func": microbiologyevents_transform_to_silver
    }
}

def build_spark(app_name):
    return (
        SparkSession.builder
            .appName(app_name)
            .master("local[*]")
            .config("spark.driver.memory", "4g")
            .config("spark.executor.memory", "4g")
            .config("spark.executor.cores", "4")
            # kafka
            .config("spark.jars.packages",
                    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1")            # s3a / minio
            .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
            .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
            .config("spark.hadoop.fs.s3a.secret.key", "minioadmin123")
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
            .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                    "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
            # postgresql
            .config("spark.jars", "/opt/spark/jars/postgresql-42.6.0.jar")
            .getOrCreate()
    )

