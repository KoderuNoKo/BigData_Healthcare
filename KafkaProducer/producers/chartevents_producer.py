import pandas as pd
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

from .base_producer import BaseProducer

class CharteventsProducer(BaseProducer):
    def __init__(self, name, config, kafka_config, checkpoint_dir = "./checkpoints"):
        super().__init__(name, config, kafka_config, checkpoint_dir)
        
    def parse_row(self, row):
        return {
            "subject_id": int(row["subject_id"]),
            "hadm_id": int(row["hadm_id"]) if not pd.isna(row["hadm_id"]) else None,
            "stay_id": int(row["stay_id"]) if not pd.isna(row["stay_id"]) else None,
            "caregiver_id": int(row["caregiver_id"]) if not pd.isna(row["caregiver_id"]) else None,
            "charttime": pd.to_datetime(row["charttime"]).isoformat(),
            "storetime": pd.to_datetime(row["storetime"]).isoformat(),
            "itemid": int(row["itemid"]),
            "value": row["value"],
            "valuenum": None if pd.isna(row["valuenum"]) else float(row["valuenum"]),
            "valuenum": None if pd.isna(row["valuenum"]) else float(row["valuenum"]),
            "valueuom": row["valueuom"],
            "warning": row["warning"]
        }
        
    def get_partition_key(self, message):
        partition_key = self.config.get('partition_key', None)
        return str(message[partition_key]) if partition_key else None
    
    def validate_row(self, row):
        return True
    
    def get_schema(self):
        return StructType([
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