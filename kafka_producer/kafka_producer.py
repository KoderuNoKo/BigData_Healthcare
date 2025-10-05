#!/usr/bin/env python3
import json, time, pandas as pd
from confluent_kafka import Producer
import argparse

BOOTSTRAP = "cp-kafka:9092"
TOPIC = "icu_chartevents"

def delivery_report(err, msg):
    if err:
        print(f"Delivery failed: {err}")

def row_to_message(row):
    msg = {
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
    return msg


def main(args):
    df = pd.read_csv("small_file.csv", nrows=args.limit)  # test subset
    producer = Producer({'bootstrap.servers': BOOTSTRAP})

    try:
        while True:  # infinite loop
            for _, row in df.iterrows():
                msg = row_to_message(row)
                key = str(msg["subject_id"])
                producer.produce(TOPIC, key=key, value=json.dumps(msg), callback=delivery_report)
                producer.poll(0)
                time.sleep(0.5)  # simulate streaming delay
            producer.flush()
            print("=== Completed one full cycle through CSV, restarting... ===")
    except KeyboardInterrupt:
        print("\nStopped by user. Flushing pending messages...")
        producer.flush()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=1000) 
    main(parser.parse_args())

