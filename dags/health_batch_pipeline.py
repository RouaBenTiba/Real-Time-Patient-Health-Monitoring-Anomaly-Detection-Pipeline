from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import subprocess

from scripts.extract_csv import create_bucket, extract_heart, extract_diabetes

# ----------------------------
# TASKS
# ----------------------------
def extract_task():
    create_bucket()
    extract_heart()
    extract_diabetes()

def transform_task():
    subprocess.run(["python", "/opt/airflow/scripts/spark_transformation.py"])

def load_task():
    subprocess.run(["python", "/opt/airflow/scripts/load_to_datawarhouse.py"])

# ----------------------------
# DAG
# ----------------------------
with DAG(
    dag_id="health_batch_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False
) as dag:

    extract = PythonOperator(
        task_id="extract",
        python_callable=extract_task
    )

    transform = PythonOperator(
        task_id="transform",
        python_callable=transform_task
    )

    load = PythonOperator(
        task_id="load",
        python_callable=load_task
    )

    extract >> transform >> load