import json
import time
import pandas as pd
import random
from kafka import KafkaProducer

# ---------------- KAFKA ----------------
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# ---------------- LOAD DATA ----------------
df = pd.read_csv("data/heart_stream_data.csv")

# 🔥 shuffle once (random order)
df = df.sample(frac=1).reset_index(drop=True)

print("🚀 Streaming started ")
# ---------------- STREAM LOOP ----------------
for _, row in df.iterrows():

    event = row.to_dict()

    producer.send("heart-stream", event)
    producer.flush()

    print("📤 Sent:", event)

    time.sleep(9)