import pandas as pd
from datetime import datetime
from .base_producer import BaseProducer

class MicrobiologyeventsProducer(BaseProducer):
    def __init__(self, name, config, kafka_config, checkpoint_dir="./checkpoints"):
        super().__init__(name, config, kafka_config, checkpoint_dir)
        
    def parse_row(self, row):
        """Parse a row from the microbiologyevents table into a dictionary."""
        return {
            "microevent_id": int(row["microevent_id"]),
            "subject_id": int(row["subject_id"]),
            "hadm_id": int(row["hadm_id"]) if not pd.isna(row["hadm_id"]) else None,
            "micro_specimen_id": int(row["micro_specimen_id"]),
            "order_provider_id": str(row["order_provider_id"]) if not pd.isna(row["order_provider_id"]) else None,
            "chartdate": self._parse_timestamp(row["chartdate"]),
            "charttime": self._parse_timestamp(row["charttime"]) if not pd.isna(row["charttime"]) else None,
            "spec_itemid": int(row["spec_itemid"]),
            "spec_type_desc": str(row["spec_type_desc"]),
            "test_seq": int(row["test_seq"]),
            "storedate": self._parse_timestamp(row["storedate"]) if not pd.isna(row["storedate"]) else None,
            "storetime": self._parse_timestamp(row["storetime"]) if not pd.isna(row["storetime"]) else None,
            "test_itemid": int(row["test_itemid"]) if not pd.isna(row["test_itemid"]) else None,
            "test_name": str(row["test_name"]) if not pd.isna(row["test_name"]) else None,
            "org_itemid": int(row["org_itemid"]) if not pd.isna(row["org_itemid"]) else None,
            "org_name": str(row["org_name"]) if not pd.isna(row["org_name"]) else None,
            "isolate_num": int(row["isolate_num"]) if not pd.isna(row["isolate_num"]) else None,
            "quantity": str(row["quantity"]) if not pd.isna(row["quantity"]) else None,
            "ab_itemid": int(row["ab_itemid"]) if not pd.isna(row["ab_itemid"]) else None,
            "ab_name": str(row["ab_name"]) if not pd.isna(row["ab_name"]) else None,
            "dilution_text": str(row["dilution_text"]) if not pd.isna(row["dilution_text"]) else None,
            "dilution_comparison": str(row["dilution_comparison"]) if not pd.isna(row["dilution_comparison"]) else None,
            "dilution_value": float(row["dilution_value"]) if not pd.isna(row["dilution_value"]) else None,
            "interpretation": str(row["interpretation"]) if not pd.isna(row["interpretation"]) else None,
            "comments": str(row["comments"]) if not pd.isna(row["comments"]) else None
        }
    
    def _parse_timestamp(self, value):
        """Helper method to parse timestamp values consistently."""
        if pd.isna(value):
            return None
        try:
            # Handle different timestamp formats
            if isinstance(value, (datetime, pd.Timestamp)):
                return value.isoformat()
            elif isinstance(value, str):
                return pd.to_datetime(value).isoformat()
            else:
                return str(value)
        except Exception:
            return str(value) if not pd.isna(value) else None
        
    def get_partition_key(self, message):
        partition_key = self.config.get('partition_key', None)
        return str(message[partition_key]) if partition_key else None
    
    def validate_row(self, row):
        """Validate a parsed microbiology event row."""
        return True