#!/usr/bin/env python3
import json
import time
import pandas as pd
from confluent_kafka import Producer
import argparse

BOOTSTRAP = "cp-kafka:9092"
TOPIC = "icu_chartevents"

dir = ""
filename = "/home/dev/app/data/mimic-iv-3.1/icu/chartevents.csv"
path_data = f"{dir}/{filename}"

def delivery_report(err, msg):
    if err:
        print(f"Delivery failed: {err}")

def row_to_message(row):
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
        "valueuom": row["valueuom"],
        "warning": row["warning"]
    }

def main(args):
    df = pd.read_csv(path_data, nrows=args.limit)
    producer = Producer({'bootstrap.servers': BOOTSTRAP})

    try:
        while True:
            for _, row in df.iterrows():
                msg = row_to_message(row)
                key = str(msg["subject_id"])
                producer.produce(TOPIC, key=key, value=json.dumps(msg), callback=delivery_report)
                producer.poll(0)
                time.sleep(1/args.rate)  # simulate streaming delay
            producer.flush()
            print("=== Completed one full cycle through CSV, restarting... ===")
            
            if not args.loop:
                # if loop is disabled, just run once
                break
    except KeyboardInterrupt:
        print("\nStopped by user. Flushing pending messages...")
        producer.flush()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, help="Batch size to read from data source: <limit> rows/batch", default=1000)
    parser.add_argument("--rate", type=int, help="Message sending rate: <rate> msgs/sec", default=20)
    parser.add_argument("--loop", action="store_true", help="Keeping sending data, one batch after another")
    args = parser.parse_args()
    main(args)