import pandas as pd
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

from .base_producer import BaseProducer

class DItemsProducer(BaseProducer):
    def __init__(self, name, config, kafka_config, checkpoint_dir = "/home/dev/app/KafkaProducer/checkpoints"):
        super().__init__(name, config, kafka_config, checkpoint_dir)
        
    def parse_row(self, row):
        return {
            "itemid": int(row["itemid"]) if not pd.isna(row["itemid"]) else None,
            "label": row["label"] if not pd.isna(row["label"]) else None,
            "abbreviation": row["abbreviation"] if not pd.isna(row["abbreviation"]) else None,
            "linksto": row["linksto"] if not pd.isna(row["linksto"]) else None,
            "category": row["category"] if not pd.isna(row["category"]) else None,
            "unitname": row["unitname"] if not pd.isna(row["unitname"]) else None,
            "param_type": row["param_type"] if not pd.isna(row["param_type"]) else None,
            "lownormalvalue": float(row["lownormalvalue"]) if not pd.isna(row["lownormalvalue"]) else None,
            "highnormalvalue": float(row["highnormalvalue"]) if not pd.isna(row["highnormalvalue"]) else None,
        }
        
    def get_partition_key(self, message):
        partition_key = self.config.get('partition_key', None)
        return str(message[partition_key]) if partition_key else None
    
    def validate_row(self, row):
        return True
    
    def get_schema(self):
        return StructType([
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