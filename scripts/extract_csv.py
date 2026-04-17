import pandas as pd
import boto3
from pathlib import Path
from datetime import datetime
import os

# ---------------------------
# CONFIG MINIO
# ---------------------------
s3 = boto3.client(
    "s3",
    endpoint_url="http://localhost:9000",
    aws_access_key_id="minioadmin",
    aws_secret_access_key="minioadmin"
)

bucket_name = "health-raw"

# dossier local
LOCAL_DIR = "/opt/airflow/data_lake/raw"

def ensure_directory():
    os.makedirs(LOCAL_DIR, exist_ok=True)

# ---------------------------
# CREATE BUCKET
# ---------------------------
def create_bucket():
    try:
        s3.head_bucket(Bucket=bucket_name)
        print("⚠️ Bucket déjà existant")
    except:
        s3.create_bucket(Bucket=bucket_name)
        print("✅ Bucket créé")

# ---------------------------
# UPLOAD TO MINIO
# ---------------------------
def upload_to_minio(file_path):
    s3.upload_file(file_path, bucket_name, Path(file_path).name)
    print(f"☁️ Upload MinIO: {file_path}")

# ---------------------------
# SAVE LOCAL + METADATA
# ---------------------------
def save_local(df, filename):
    path = os.path.join(LOCAL_DIR, filename)
    df.to_csv(path, index=False)
    print(f"💾 Sauvegardé localement: {path}")
    return path

# ---------------------------
# DATASET 1 : DIABETES
# ---------------------------
def extract_diabetes():
    ensure_directory()
    import kagglehub

    path = kagglehub.dataset_download(
        "jamaltariqcheema/pima-indians-diabetes-dataset"
    )

    print("📥 Diabetes dataset path:", path)

    files = os.listdir(path)
    csv_file = [f for f in files if f.endswith(".csv")][0]

    df = pd.read_csv(f"{path}/{csv_file}")
    df["source"] = "diabetes_kaggle"
    df["ingest_date"] = datetime.now()

    file_path = save_local(df, "diabetes.csv")
    upload_to_minio(file_path)

    return df

# ---------------------------
# DATASET 2 : HEART
# ---------------------------
def extract_heart():
    import kagglehub

    path = kagglehub.dataset_download(
        "redwankarimsony/heart-disease-data"
    )

    print("📥 Heart dataset path:", path)

    files = os.listdir(path)
    csv_file = [f for f in files if f.endswith(".csv")][0]

    df = pd.read_csv(f"{path}/{csv_file}")
    df["source"] = "heart_kaggle"
    df["ingest_date"] = datetime.now()

    file_path = save_local(df, "heart_disease.csv")
    upload_to_minio(file_path)

    return df

# ---------------------------
# MAIN
# ---------------------------
if __name__ == "__main__":
    create_bucket()

    df_diabetes = extract_diabetes()
    df_heart = extract_heart()

    print("🎉 Ingestion terminée (Local + MinIO)")