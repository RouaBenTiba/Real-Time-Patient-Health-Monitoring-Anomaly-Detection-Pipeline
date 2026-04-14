import pandas as pd
import random
from datetime import datetime, timedelta

# ---------------- CONFIG ----------------
NUM_PATIENTS = 200
MEASURES_PER_PATIENT = 10
ANOMALY_RATE = 0.5

data = []
start_time = datetime.now()

# ---------------- POSSIBLE VALUES ----------------
sex_values = ["Male", "Female"]
cp_values = ["typical angina", "atypical angina", "non-anginal", "asymptomatic"]
restecg_values = ["normal", "lv hypertrophy"]
slope_values = ["upsloping", "flat", "downsloping"]
thal_values = ["normal", "fixed defect", "reversable defect"]

# ---------------- GENERATE DATA ----------------
for patient_id in range(1, NUM_PATIENTS + 1):

    # patient static info
    age = random.randint(30, 80)
    sex = random.choice(sex_values)
    cp = random.choice(cp_values)
    fbs = random.choice([True, False])
    restecg = random.choice(restecg_values)
    exang = random.choice([True, False])
    slope = random.choice(slope_values)
    thal = random.choice(thal_values)

    for t in range(MEASURES_PER_PATIENT):

        event_time = start_time + timedelta(seconds=len(data))
        is_anomaly = random.random() < ANOMALY_RATE

        # ---------------- NORMAL ----------------
        if not is_anomaly:
            trestbps = random.uniform(110, 140)
            chol = random.uniform(150, 240)
            thalch = random.uniform(120, 180)
            oldpeak = random.uniform(0.0, 1.5)
            heart_rate = random.randint(60, 100)
            ca = random.randint(0, 1)
            num = 0

        # ---------------- ANOMALY ----------------
        else:
            trestbps = random.uniform(150, 200)
            chol = random.uniform(250, 400)
            thalch = random.uniform(80, 120)
            oldpeak = random.uniform(2.0, 6.0)
            heart_rate = random.randint(120, 200)
            ca = random.randint(2, 4)
            num = random.randint(1, 4)

        data.append({
            "id_patient": patient_id,
            "timestamp": event_time.strftime("%Y-%m-%d %H:%M:%S"),

            "age": age,
            "sex": sex,
            "cp": cp,
            "trestbps": round(trestbps, 2),
            "chol": round(chol, 2),
            "fbs": fbs,
            "restecg": restecg,
            "thalch": round(thalch, 2),
            "exang": exang,
            "oldpeak": round(oldpeak, 2),
            "slope": slope,
            "ca": ca,
            "thal": thal,
            "num": num,

            # 🔥 NEW FEATURE
            "heart_rate": heart_rate
        })

# ---------------- SAVE ----------------
df = pd.DataFrame(data)

df.to_csv("data/heart_stream_data.csv", index=False)

print("✅ Dataset streaming indépendant généré")
print("👥 Patients:", NUM_PATIENTS)
print("📊 Lignes:", len(df))