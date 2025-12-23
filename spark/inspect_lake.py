from pyspark.sql import functions as F
from modules.topics_schema import *
from modules.common import build_spark

spark = build_spark("ReadBronze")

bronze_df = spark.read.parquet("s3a://mimic-silver/chartevents/")

# testing, just printout what's read
print(f"Current number of rows:{bronze_df.count()}")
bronze_df.show()