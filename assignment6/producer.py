import json
import time
import pandas as pd

from confluent_kafka import Producer

from config import *

# -----------------------------
# LOAD DATA
# -----------------------------

df = pd.read_parquet("september_first_two_days.parquet")

df = df.sort_values("tpep_pickup_datetime")

# -----------------------------
# JSON SERIALIZATION
# -----------------------------


def row_to_json(row):

    d = row.to_dict()

    for k, v in d.items():

        if hasattr(v, "isoformat"):
            d[k] = v.isoformat()

        elif isinstance(v, float) and pd.isna(v):
            d[k] = None

    return json.dumps(d)


# -----------------------------
# PRODUCER
# -----------------------------

producer = Producer({"bootstrap.servers": BOOTSTRAP})

print("Streaming taxi events...")

for i, (_, row) in enumerate(df.iterrows()):

    producer.produce(TOPIC_RAW, key=str(row["PULocationID"]), value=row_to_json(row))

    if i % 5000 == 0:
        producer.poll(0)

        print(f"Sent {i}/{len(df)}")

    time.sleep(0.002)

producer.flush()

print("Streaming complete.")
