import pandas as pd

from .base_producer import BaseProducer

class LabeventsProducer(BaseProducer):
    def __init__(self, name, config, kafka_config, checkpoint_dir = "./checkpoints"):
        super().__init__(name, config, kafka_config, checkpoint_dir)
        
    def parse_row(self, row):
        return {
            "labevent_id": int(row["labevent_id"]),
            "subject_id": int(row["subject_id"]),
            "hadm_id": int(row["hadm_id"]) if not pd.isna(row["hadm_id"]) else None,
            "specimen_id": int(row["specimen_id"]),
            "itemid": int(row["itemid"]),
            "order_provider_id": str(row["order_provider_id"]) if not pd.isna(row["order_provider_id"]) else None,
            "charttime": pd.to_datetime(row["charttime"]).isoformat(),
            "storetime": pd.to_datetime(row["storetime"]).isoformat(),
            "value": str(row["value"]) if pd.isna(row["value"]) else None,
            "valuenum": None if pd.isna(row["valuenum"]) else float(row["valuenum"]),
            "valueuom": row["valueuom"] if pd.isna(row["valueuom"]) else None,
            "ref_range_lower": None if pd.isna(row["ref_range_lower"]) else float(row["ref_range_lower"]),
            "ref_range_upper": None if pd.isna(row["ref_range_upper"]) else float(row["ref_range_upper"]),
            "flag": None if pd.isna(row["flag"]) else row["flag"],
            "priority": None if pd.isna(row["priority"]) else row["priority"],
            "comments": None if pd.isna(row["comments"]) else row["comments"],
        }
        
    def get_partition_key(self, message):
        partition_key = self.config.get('partition_key', None)
        return str(message[partition_key]) if partition_key else None
    
    def validate_row(self, row):
        return True