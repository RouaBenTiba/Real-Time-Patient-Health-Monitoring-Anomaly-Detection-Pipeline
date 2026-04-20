from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys

sys.path.append("/opt/airflow")

from scripts.anomaly_detection import (
    create_anomaly_table,
    load_stream,
    load_baseline,
    detect_anomalies,
    save_anomalies
)

def anomaly_pipeline():

    create_anomaly_table()

    stream_df = load_stream()
    baseline_df = load_baseline()

    if stream_df.empty:
        print("⚠️ stream empty")
        return

    anomalies = detect_anomalies(stream_df, baseline_df)

    print(f"🚨 anomalies: {len(anomalies)}")

    save_anomalies(anomalies)

with DAG(
    dag_id="health_anomaly_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False
) as dag:

    PythonOperator(
        task_id="run_anomaly",
        python_callable=anomaly_pipeline
    )