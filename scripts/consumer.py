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

# ======================================================
# ⏳ WAIT FOR PRODUCER
# ======================================================
print("⏳ Waiting for producer to start...")

while True:
    msg_pack = consumer.poll(timeout_ms=3000)

    if msg_pack:
        print("🚀 Producer detected, streaming started...")
        break
    else:
        print("...still waiting producer")

# ======================================================
# 📁 FILE INIT
# ======================================================
output_file = "data/heart_stream_output.csv"
file_exists = os.path.isfile(output_file)

out_file = open(output_file, mode='a', newline='')
writer = None

# ======================================================
# 🚀 STREAM LOOP
# ======================================================
for msg in consumer:
    data = msg.value

    print("📥 Received:", data)

    # 🔥 create header dynamically
    if writer is None:
        writer = csv.DictWriter(out_file, fieldnames=data.keys())
        if not file_exists:
            writer.writeheader()

    writer.writerow(data)
    out_file.flush()