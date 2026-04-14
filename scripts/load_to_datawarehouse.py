import os
import pandas as pd
from sqlalchemy import create_engine

from spark_transformation import heart_dataset_transformation
from spark_transformation import diabetes_dataset_transformation

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

heart_table_name = "heart_disease"
diabetes_table_name = "diabetes"

# ----------------------------
# 2. CONVERSION SPARK -> PANDAS
# ----------------------------
def spark_to_pandas(df):
    return df.toPandas()

# ----------------------------
# 3. LOAD FUNCTION
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
# 4. MAIN
# ----------------------------
if __name__ == "__main__":

    heart_df = heart_dataset_transformation() #saprk dataframe
    diabetes_df = diabetes_dataset_transformation() #spark dataframe

    heart_pdf = spark_to_pandas(heart_df)#pandas dataframe
    diabetes_pdf = spark_to_pandas(diabetes_df)

    load_to_postgres(heart_pdf, heart_table_name)
    load_to_postgres(diabetes_pdf, diabetes_table_name)