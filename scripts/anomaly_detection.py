import pandas as pd
from sqlalchemy import create_engine, text

DB_USER = "postgres"
DB_PASSWORD = "test"
DB_HOST = "postgres"
DB_PORT = "5432"
DB_NAME = "health_dw"

engine = create_engine(
    f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)

# ----------------------------
def create_anomaly_table():
    query = """
    CREATE TABLE IF NOT EXISTS anomalies (
        id SERIAL PRIMARY KEY,
        patient_id INT,
        timestamp TIMESTAMP,
        anomaly_score FLOAT,
        is_anomaly BOOLEAN,
        reason TEXT
    );
    """
    with engine.begin() as conn:
        conn.execute(text(query))

# ----------------------------
def load_stream():
    return pd.read_sql("SELECT * FROM heart_stream", engine)

def load_baseline():
    try:
        return pd.read_sql("SELECT * FROM heart_disease", engine)
    except Exception:
        print("⚠️ heart_disease not ready yet")
        return pd.DataFrame()

# ----------------------------
def compute_stats(df):
    return {
        col: {
            "mean": df[col].mean(),
            "std": df[col].std()
        }
        for col in ["trestbps", "chol", "thalch", "oldpeak"]
    }

# ----------------------------
def compute_anomaly_score(row, stats):

    score = 0
    reasons = []

    rules = {
        "trestbps": (140, "high_bp"),
        "chol": (240, "high_chol"),
        "thalch": (100, "low_hr"),
        "oldpeak": (2, "ecg_abnormal")
    }

    for col, (threshold, reason) in rules.items():
        if col in row and row[col] is not None:
            if (col in ["thalch"] and row[col] < threshold) or (col not in ["thalch"] and row[col] > threshold):
                score += 1
                reasons.append(reason)

    # Z-score
    for col in stats:
        if stats[col]["std"] > 0:
            z = (row[col] - stats[col]["mean"]) / stats[col]["std"]
            if abs(z) > 2.5:
                score += 1
                reasons.append(f"{col}_outlier")

    return score, ",".join(reasons)

# ----------------------------
def detect_anomalies(stream_df, baseline_df):

    stats = compute_stats(baseline_df)

    results = []

    for _, row in stream_df.iterrows():

        score, reasons = compute_anomaly_score(row, stats)

        results.append({
            "patient_id": row.get("id_patient"),
            "timestamp": row.get("timestamp"),
            "anomaly_score": score,
            "is_anomaly": score >= 3,
            "reason": reasons
        })

    return pd.DataFrame(results)

# ----------------------------
def save_anomalies(df):

    df = df[df["is_anomaly"] == True]

    if df.empty:
        print("ℹ️ no anomalies")
        return

    df.to_sql("anomalies", engine, if_exists="append", index=False)

    print("✅ saved")