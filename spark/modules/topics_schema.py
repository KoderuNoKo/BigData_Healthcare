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

patient_schema = StructType([
    StructField("subject_id", IntegerType(), nullable=False),
    StructField("gender", StringType(), nullable=False),
    StructField("anchor_age", IntegerType(), nullable=False),
    StructField("anchor_year", IntegerType(), nullable=False),
    StructField("anchor_year_group", StringType(), nullable=False),
    StructField("dod", TimestampType()),
])

icu_stays_schema = StructType([
    StructField("subject_id", IntegerType()),
    StructField("hadm_id", IntegerType()),
    StructField("stay_id", IntegerType()),
    StructField("first_careunit", StringType()),
    StructField("last_careunit", StringType()),
    StructField("inttime", TimestampType()),
    StructField("outtime", TimestampType()),
    StructField("los", DoubleType()),
])

microbiologyevents_schema = StructType([
    StructField("microevent_id", IntegerType(), False),
    StructField("subject_id", IntegerType(), False),
    StructField("hadm_id", IntegerType(), True),
    StructField("micro_specimen_id", IntegerType(), False),
    StructField("order_provider_id", StringType(), True),
    StructField("chartdate", TimestampType(), False),
    StructField("charttime", TimestampType(), True),
    StructField("spec_itemid", IntegerType(), False),
    StructField("spec_type_desc", StringType(), False),
    StructField("test_seq", IntegerType(), False),
    StructField("storedate", TimestampType(), True),
    StructField("storetime", TimestampType(), True),
    StructField("test_itemid", IntegerType(), True),
    StructField("test_name", StringType(), True),
    StructField("org_itemid", IntegerType(), True),
    StructField("org_name", StringType(), True),
    StructField("isolate_num", IntegerType(), True),
    StructField("quantity", StringType(), True),
    StructField("ab_itemid", IntegerType(), True),
    StructField("ab_name", StringType(), True),
    StructField("dilution_text", StringType(), True),
    StructField("dilution_comparison", StringType(), True),
    StructField("dilution_value", DoubleType(), True),
    StructField("interpretation", StringType(), True),
    StructField("comments", StringType(), True)
])