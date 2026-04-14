import os
import pandas as pd
from sqlalchemy import create_engine

from pyspark.sql import SparkSession
import pyspark.sql.functions as F

from spark_transformation import heart_dataset_transformation
from spark_transformation import diabetes_dataset_transformation

# ----------------------------
# 1. SPARK SESSION (FIX TIMEZONE)
# ----------------------------
spark = SparkSession.builder \
    .appName("HealthDW") \
    .config("spark.sql.session.timeZone", "UTC") \
    .getOrCreate()

# ----------------------------
# 2. POSTGRES CONFIG
# ----------------------------
DB_USER = "postgres"
DB_PASSWORD = "test"
DB_HOST = "localhost"
DB_PORT = "5433"
DB_NAME = "health_dw"

engine = create_engine(
    f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)

# ----------------------------
# 3. SAFE SPARK → PANDAS
# ----------------------------
def spark_to_pandas(df):

    # 🔥 FIX TIME COLUMN WITHOUT DELETING IT
    if "ingest_date" in df.columns:
        df = df.withColumn(
            "ingest_date",
            F.to_timestamp("ingest_date")  # convert to Spark timestamp standard
        )

    # 🔥 CAST ALL OTHER DATE/TIME COLUMNS SAFELY
    for c in df.columns:
        if "timestamp" in c or "date" in c:
            df = df.withColumn(c, F.col(c).cast("timestamp"))

    return df.toPandas()

# ----------------------------
# 4. LOAD FUNCTION
# ----------------------------
def load_to_postgres(pdf, table_name):
    print(f"🚀 Loading {table_name} into PostgreSQL...")

    pdf.to_sql(
        table_name,
        engine,
        if_exists="append",
        index=False
    )

    print(f"✅ {table_name} loaded successfully!")

# ----------------------------
# 5. MAIN PIPELINE
# ----------------------------
if __name__ == "__main__":

    print("🚀 Starting Data Warehouse Pipeline")

    # Spark transformation
    heart_df = heart_dataset_transformation()
    diabetes_df = diabetes_dataset_transformation()

    # 🔥 KEEP TIME, JUST FIX IT
    heart_df = heart_df.withColumn(
        "ingest_date",
        F.to_timestamp("ingest_date")
    )

    diabetes_df = diabetes_df.withColumn(
        "ingest_date",
        F.to_timestamp("ingest_date")
    )

    # Convert safely
    heart_pdf = spark_to_pandas(heart_df)
    diabetes_pdf = spark_to_pandas(diabetes_df)

    # Load
    load_to_postgres(heart_pdf, "heart_disease")
    load_to_postgres(diabetes_pdf, "diabetes")

    print("🎉 DONE")