from pyspark.sql import SparkSession
import boto3
import os
from pyspark.sql.functions import col, sum, when

from pyspark.sql.functions import col, lower, trim, when
from pyspark.sql.types import IntegerType, DoubleType

def download_from_minio(bucket_name, file_name, local_path):
    s3 = boto3.client(
        "s3",
        endpoint_url="http://localhost:9000",
        aws_access_key_id="minioadmin",
        aws_secret_access_key="minioadmin"
    )
    s3.download_file(bucket_name, file_name, local_path)
    
    print("✅ Fichier téléchargé depuis MinIO")


# ----------------------------
# 1. Spark Session (LOCAL ONLY)
# ----------------------------
spark = SparkSession.builder \
    .appName("HealthPipeline") \
    .getOrCreate()



bucket_name = "health-raw"
heart_file_name = "heart_disease.csv"
heart_local_path = "data/heart_disease.csv"

diabetes_file_name = "diabetes.csv"
diabetes_local_path = "data/diabetes.csv"


# ----------------------------
# 4. Spark lit le fichier local
# ----------------------------
def heart_dataset_transformation():
    if not os.path.exists(heart_local_path):
        download_from_minio(bucket_name, heart_file_name, heart_local_path)

    df = spark.read.csv(
        heart_local_path,
        header=True,
        inferSchema=True
    )

    df.show(5)
    df.printSchema()

    print("valeurs nulles :\n")
   

    df.select([
        sum(when(col(c).isNull(), 1).otherwise(0)).alias(c)
        for c in df.columns
    ]).show()

    df=clean_heart_data(df)

    return df




def diabetes_dataset_transformation():
    if not os.path.exists(diabetes_local_path):
        download_from_minio(bucket_name, diabetes_file_name, diabetes_local_path)

    df = spark.read.csv(
        diabetes_local_path,
        header=True,
        inferSchema=True
    )

    df.show(5)
    df.printSchema()
    print("valeurs nulles :\n")
   

    df.select([
        sum(when(col(c).isNull(), 1).otherwise(0)).alias(c)
        for c in df.columns
    ]).show()

    return df




def clean_heart_data(df):

    # ----------------------------
    # 0. COPIE DATAFRAME
    # ----------------------------
    df_clean = df

    # ----------------------------
    # 1. TAILLE AVANT
    # ----------------------------
    print(" Taille AVANT nettoyage :", df_clean.count())

    # ----------------------------
    # 2. DROP NULLS
    # ----------------------------
    df_clean = df_clean.dropna(subset=["age", "trestbps", "chol", "thalch"])

    # ----------------------------
    # 3. NORMALISATION TEXTE
    # ----------------------------
    df_clean = df_clean.withColumn("sex", lower(trim(col("sex"))))
    df_clean = df_clean.withColumn("cp", lower(trim(col("cp"))))
    df_clean = df_clean.withColumn("restecg", lower(trim(col("restecg"))))
    df_clean = df_clean.withColumn("thal", lower(trim(col("thal"))))
    df_clean = df_clean.withColumn("slope", lower(trim(col("slope"))))

    # ----------------------------
    # 4. ENCODING
    # ----------------------------
    df_clean = df_clean.withColumn("sex",
        when(col("sex") == "male", 1).otherwise(0)
    )

    df_clean = df_clean.withColumn("cp",
        when(col("cp") == "typical angina", 0)
        .when(col("cp") == "atypical angina", 1)
        .when(col("cp") == "non-anginal pain", 2)
        .otherwise(3)
    )

    df_clean = df_clean.withColumn("fbs",
        when(col("fbs") == True, 1).otherwise(0)
    )

    df_clean = df_clean.withColumn("exang",
        when(col("exang") == True, 1).otherwise(0)
    )

    df_clean = df_clean.withColumn("slope",
        when(col("slope") == "upsloping", 0)
        .when(col("slope") == "flat", 1)
        .otherwise(2)
    )

    df_clean = df_clean.withColumn("thal",
        when(col("thal") == "normal", 1)
        .when(col("thal") == "fixed defect", 2)
        .otherwise(3)
    )

    # ----------------------------
    # 5. CAST TYPES
    # ----------------------------
    df_clean = df_clean \
        .withColumn("age", col("age").cast(IntegerType())) \
        .withColumn("trestbps", col("trestbps").cast(DoubleType())) \
        .withColumn("chol", col("chol").cast(DoubleType())) \
        .withColumn("thalch", col("thalch").cast(DoubleType())) \
        .withColumn("oldpeak", col("oldpeak").cast(DoubleType()))

    # ----------------------------
    # 6. FEATURE ENGINEERING
    # ----------------------------
    df_clean = df_clean.withColumn("high_cholesterol",
        when(col("chol") > 240, 1).otherwise(0)
    )

    df_clean = df_clean.withColumn("high_bp",
        when(col("trestbps") > 140, 1).otherwise(0)
    )

    df_clean = df_clean.withColumn("risk_score",
        col("age")*0.2 +
        col("chol")*0.01 +
        col("trestbps")*0.01 +
        col("oldpeak")*0.5
    )

    # ----------------------------
    # 7. TAILLE APRÈS
    # ----------------------------
    print("Taille APRÈS nettoyage :", df_clean.count())
    print(" Lignes supprimées :", df.count() - df_clean.count())
    return df_clean





print(" Transformation des datasets avec Spark\n")
print("=====================================\n")
print(" Dataset Heart Disease")
heart_dataset_transformation()
print("=====================================\n")
# print(" Dataset Diabetes")
# diabetes_dataset_transformation()