FROM apache/airflow:2.9.0

USER root

# ✅ Installer Java 17
RUN apt-get update && \
    apt-get install -y openjdk-17-jdk && \
    apt-get clean

# ✅ JAVA HOME
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# 🔥 IMPORTANT : repasser à airflow AVANT pip
USER airflow

# ✅ Installer pyspark proprement
RUN pip install --no-cache-dir pyspark kagglehub boto3 pandas sqlalchemy psycopg2-binary