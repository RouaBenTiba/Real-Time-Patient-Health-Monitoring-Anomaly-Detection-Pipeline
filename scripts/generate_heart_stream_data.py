import pandas as pd
import random
from datetime import datetime, timedelta

# ---------------- CONFIG ----------------
NUM_ROWS = 1000

data = []

start_time = datetime.now()

# ---------------- GENERATE DATA ----------------
for i in range(NUM_ROWS):

    event_time = start_time + timedelta(seconds=i)

    row = {
        "id_patient": random.randint(1, 100),
        "timestamp": event_time.strftime("%Y-%m-%d %H:%M:%S"),
        "heart_rate": random.randint(50, 180),
        "temperature": round(random.uniform(35.0, 40.5), 2)
    }

    data.append(row)

df = pd.DataFrame(data)

df.to_csv("data_lake/raw/heart_stream_data.csv", index=False)

print("✅ Dataset generated: heart_stream_data.csv")