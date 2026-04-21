import os
from sqlalchemy import create_engine, text

from pyspark.sql import SparkSession
import pyspark.sql.functions as F


from scripts.spark_transformation import (
    heart_dataset_transformation,
    diabetes_dataset_transformation
)


# ----------------------------
# 1. SPARK SESSION
# ----------------------------
spark = SparkSession.builder \
    .appName("HealthDW") \
    .config("spark.sql.session.timeZone", "UTC") \
    .getOrCreate()

# ----------------------------
# 2. POSTGRES CONFIG (DOCKER)
# ----------------------------
DB_USER = "postgres"
DB_PASSWORD = "test"

DB_HOST = "postgres"   # IMPORTANT Docker service name
DB_PORT = "5432"

DB_NAME = "health_dw"

engine = create_engine(
    f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)

# ----------------------------
# 3. SPARK → PANDAS SAFE
# ----------------------------
def spark_to_pandas(df):

    if "ingest_date" in df.columns:
        df = df.withColumn(
            "ingest_date",
            F.to_timestamp("ingest_date")
        )

    for c in df.columns:
        if "timestamp" in c or "date" in c:
            df = df.withColumn(c, F.col(c).cast("timestamp"))

    return df.toPandas()

# ----------------------------
# 4. LOAD FUNCTION (WITH DEBUG)
# ----------------------------
def load_to_postgres(pdf, table_name):

    print(f"\n🚀 Loading {table_name} into PostgreSQL...")

    print(f"📦 Rows to insert: {len(pdf)}")

    pdf.to_sql(
        table_name,
        engine,
        if_exists="append",
        index=False
    )

    # 🔥 VERIFY INSERT
    with engine.connect() as conn:
        count = conn.execute(
            text(f"SELECT COUNT(*) FROM {table_name}")
        ).fetchone()[0]

        print(f"📊 Rows in DB after insert ({table_name}): {count}")

    print(f"✅ {table_name} loaded successfully!\n")

# ----------------------------
# 5. MAIN PIPELINE
# ----------------------------
if __name__ == "__main__":

    print("🚀 Starting Data Warehouse Pipeline\n")

    # ----------------------------
    # TRANSFORMATION
    # ----------------------------
    heart_df = heart_dataset_transformation()
    diabetes_df = diabetes_dataset_transformation()

    # ----------------------------
    # CONVERSION
    # ----------------------------
    heart_pdf = spark_to_pandas(heart_df)
    diabetes_pdf = spark_to_pandas(diabetes_df)

    print("📌 Heart size:", len(heart_pdf))
    print("📌 Diabetes size:", len(diabetes_pdf))

    # ----------------------------
    # LOAD
    # ----------------------------
    load_to_postgres(heart_pdf, "heart_disease")
    load_to_postgres(diabetes_pdf, "diabetes")

    print("🎉 PIPELINE DONE")