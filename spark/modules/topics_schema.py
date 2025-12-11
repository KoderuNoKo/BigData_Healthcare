"""
Define the schema for each topics, translated from MIMIC-IV documentation at https://mimic.mit.edu/docs/iv/modules/
"""
from pyspark.sql.types import *

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