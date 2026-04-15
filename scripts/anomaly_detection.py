import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from datetime import datetime

# ----------------------------
# 1. CONFIG POSTGRES
# ----------------------------
DB_USER = "postgres"
DB_PASSWORD = "test"
DB_HOST = "localhost"
DB_PORT = "5433"
DB_NAME = "health_dw"

engine = create_engine(
    f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)

import pandas as pd
from sqlalchemy import create_engine, text
from sklearn.ensemble import IsolationForest

# ----------------------------
# 1. CONFIG POSTGRES
# ----------------------------
engine = create_engine("postgresql://postgres:test@localhost:5433/health_dw")

# ----------------------------
# 2. CREATION TABLE ANOMALIES
# ----------------------------
create_table_query = """
CREATE TABLE IF NOT EXISTS anomalies (
    id SERIAL PRIMARY KEY,
    patient_id INT,
    timestamp TIMESTAMP,
    anomaly_score FLOAT,
    is_anomaly BOOLEAN,
    reason TEXT
);
"""

with engine.connect() as conn:
    conn.execute(text(create_table_query))
    conn.commit()

print("✅ Table anomalies prête")

# ----------------------------
# 2. LOAD HISTORICAL DATA (baseline)
# ----------------------------
def load_baseline():
    query = "SELECT * FROM heart_disease"
    df = pd.read_sql(query, engine)
    return df

# ----------------------------
# 3. CALCUL STATISTIQUES
# ----------------------------
def compute_stats(df):
    stats = {}
    for col in ["trestbps", "chol", "thalch", "oldpeak"]:
        stats[col] = {
            "mean": df[col].mean(),
            "std": df[col].std()
        }
    return stats

# ----------------------------
# 4. ANOMALY SCORE
# ----------------------------
def compute_anomaly_score(row, stats):

    score = 0
    reasons = []

    # -------- RULE BASED --------
    if row["trestbps"] > 140:
        score += 1
        reasons.append("high_blood_pressure")

    if row["chol"] > 240:
        score += 1
        reasons.append("high_cholesterol")

    if row["thalch"] < 100:
        score += 1
        reasons.append("low_heart_capacity")

    if row["oldpeak"] > 2:
        score += 1
        reasons.append("abnormal_ecg")

    if row["ca"] > 0:
        score += 1
        reasons.append("vessel_blockage")

    # -------- Z-SCORE --------
    for col in ["trestbps", "chol", "thalch", "oldpeak"]:
        if stats[col]["std"] > 0:
            z = (row[col] - stats[col]["mean"]) / stats[col]["std"]
            if abs(z) > 2.5:
                score += 1
                reasons.append(f"{col}_outlier")

    return score, ",".join(reasons)

# ----------------------------
# 5. DETECTION FUNCTION
# ----------------------------
def detect_anomalies(stream_df, stats):

    results = []

    for _, row in stream_df.iterrows():
        score, reasons = compute_anomaly_score(row, stats)

        is_anomaly = score >= 3

        results.append({
            "patient_id": row["id"],
            "timestamp": datetime.now(),
            "anomaly_score": score,
            "is_anomaly": is_anomaly,
            "reason": reasons
        })

    return pd.DataFrame(results)

# ----------------------------
# 6. SAVE TO POSTGRES
# ----------------------------
def save_anomalies(df):
    df = df[df["is_anomaly"] == True]  # garde seulement les anomalies

    if df.empty:
        print("ℹ️ Aucune anomalie détectée, rien à enregistrer.")
        return

    df.to_sql(
        "anomalies",
        engine,
        if_exists="append",
        index=False
    )
    print("✅ Anomalies sauvegardées dans PostgreSQL")

# ----------------------------
# 7. MAIN PIPELINE
# ----------------------------
if __name__ == "__main__":

    print("🚀 Starting anomaly detection pipeline...")

    # 1. Load baseline
    baseline_df = load_baseline()

    # 2. Compute stats
    stats = compute_stats(baseline_df)

    # 3. SIMULATION STREAMING (tu peux remplacer par Kafka plus tard)
    stream_df = baseline_df.sample(5).copy()

    # Inject anomaly artificielle
    stream_df.loc[0, "chol"] = 400
    stream_df.loc[1, "trestbps"] = 180

    print("📡 Streaming data:")
    print(stream_df[["id", "chol", "trestbps"]])

    # 4. Detect anomalies
    anomalies_df = detect_anomalies(stream_df, stats)

    print("🚨 Detected anomalies:")
    print(anomalies_df)

    # 5. Save results
    save_anomalies(anomalies_df)