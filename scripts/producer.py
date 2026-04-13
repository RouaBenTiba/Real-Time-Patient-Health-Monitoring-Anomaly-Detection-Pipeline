import json
import time
import pandas as pd
from kafka import KafkaProducer

# ---------------- KAFKA ----------------
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# ---------------- LOAD DATA ----------------
df = pd.read_csv("data_lake/raw/heart_stream_data.csv")

print("🚀 Streaming started...")

# ---------------- STREAM LOOP ----------------
for _, row in df.iterrows():

    event = row.to_dict()

    producer.send("heart-stream", event)

    print("Sent:", event)

    time.sleep(1)   # 🔥 1 event per second