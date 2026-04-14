import json
from kafka import KafkaConsumer
import csv
import os

# ---------------- KAFKA ----------------
consumer = KafkaConsumer(
    'heart-stream',
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    auto_offset_reset='latest',
    group_id='heart-group'
)

print("👂 Listening heart-stream...")

# ---------------- FILE ----------------
file_name = "heart_stream_output.csv"
file_exists = os.path.isfile(file_name)

file = open(file_name, mode='a', newline='')
writer = csv.DictWriter(file, fieldnames=[
    "id_patient",
    "timestamp",
    "heart_rate",
    "temperature"
])

# header only once
if not file_exists:
    writer.writeheader()

# ---------------- STREAM LOOP ----------------
for msg in consumer:
    data = msg.value

    print("Received:", data)

    writer.writerow({
        "id_patient": data.get("id_patient"),
        "timestamp": data.get("timestamp"),
        "heart_rate": data.get("heart_rate"),
        "temperature": data.get("temperature")
    })

    file.flush()