import json
import time
from kafka import KafkaConsumer
import csv
import os
import psycopg2

# ======================================================
# KAFKA CONSUMER
# ======================================================
consumer = KafkaConsumer(
    'heart-stream',
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    auto_offset_reset='latest',
    group_id='heart-group'
)

print("⏳ Waiting for producer to start...")

# ======================================================
# WAIT UNTIL FIRST MESSAGE ARRIVES
# ======================================================
while True:
    msg_pack = consumer.poll(timeout_ms=3000)

    if msg_pack:
        print("🚀 Producer detected → streaming started")
        break
    else:
        print("...still waiting producer")

# ======================================================
# CSV OUTPUT
# ======================================================
output_file = "data/heart_stream_output.csv"
file_exists = os.path.isfile(output_file)

out_file = open(output_file, mode='a', newline='')
writer = csv.DictWriter(out_file, fieldnames=[
    "id_patient", "timestamp", "age", "sex", "cp",
    "trestbps", "chol", "fbs", "restecg", "thalch",
    "exang", "oldpeak", "slope", "ca", "thal", "num", "heart_rate"
])

if not file_exists:
    writer.writeheader()

# ======================================================
# POSTGRES
# ======================================================
conn = psycopg2.connect(
    dbname="health_dw",
    user="postgres",
    password="test",
    host="localhost",
    port="5433"
)

cursor = conn.cursor()

cursor.execute("""
CREATE TABLE IF NOT EXISTS heart_stream (
    id SERIAL PRIMARY KEY,
    id_patient INT,
    timestamp TEXT,
    age INT,
    sex TEXT,
    cp TEXT,
    trestbps FLOAT,
    chol FLOAT,
    fbs BOOLEAN,
    restecg TEXT,
    thalch FLOAT,
    exang BOOLEAN,
    oldpeak FLOAT,
    slope TEXT,
    ca FLOAT,
    thal TEXT,
    num INT,
    heart_rate FLOAT
);
""")

conn.commit()

# ======================================================
# STREAM PROCESSING
# ======================================================
for msg in consumer:
    data = msg.value

    # ---------------- CSV ----------------
    writer.writerow({
        "id_patient": data.get("id_patient"),
        "timestamp": data.get("timestamp"),
        "age": data.get("age"),
        "sex": data.get("sex"),
        "cp": data.get("cp"),
        "trestbps": data.get("trestbps"),
        "chol": data.get("chol"),
        "fbs": data.get("fbs"),
        "restecg": data.get("restecg"),
        "thalch": data.get("thalch"),
        "exang": data.get("exang"),
        "oldpeak": data.get("oldpeak"),
        "slope": data.get("slope"),
        "ca": data.get("ca"),
        "thal": data.get("thal"),
        "num": data.get("num"),
        "heart_rate": data.get("heart_rate")
    })

    out_file.flush()

    # ---------------- DATA WAREHOUSE ----------------
    cursor.execute("""
        INSERT INTO heart_stream (
            id_patient, timestamp, age, sex, cp,
            trestbps, chol, fbs, restecg, thalch,
            exang, oldpeak, slope, ca, thal, num, heart_rate
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                %s,%s,%s,%s,%s,%s,%s)
    """, (
        data.get("id_patient"),
        data.get("timestamp"),
        data.get("age"),
        data.get("sex"),
        data.get("cp"),
        data.get("trestbps"),
        data.get("chol"),
        data.get("fbs"),
        data.get("restecg"),
        data.get("thalch"),
        data.get("exang"),
        data.get("oldpeak"),
        data.get("slope"),
        data.get("ca"),
        data.get("thal"),
        data.get("num"),
        data.get("heart_rate")
    ))

    conn.commit()

    print("📥 Stored:", data.get("id_patient"))