# Real-Time Patient Health Monitoring & Anomaly Detection Pipeline

A data engineering project that combines batch ETL processing and real-time data streaming to process patient health data, store it in PostgreSQL, and detect potential anomalies.

## Overview

This project implements a data pipeline for healthcare data using Apache Airflow, Apache Spark, Apache Kafka, and PostgreSQL.

It includes three main workflows:

- **Batch ETL Pipeline:** Extracts patient data from CSV files, transforms it using Spark, and loads it into PostgreSQL.
- **Real-Time Streaming Pipeline:** Consumes patient health events from Kafka and stores them in CSV files and PostgreSQL.
- **Anomaly Detection Pipeline:** Analyzes patient measurements and identifies potentially abnormal values using thresholds and statistical Z-scores.

## Architecture

```text
                  CSV Files
                      |
                      v
              Apache Airflow
                      |
                      v
                 Apache Spark
             (Transform Data)
                      |
                      v
                 PostgreSQL
                  (Data Warehouse)


              Real-Time Data
                      |
                      v
                 Apache Kafka
                 heart-stream
                      |
                      v
                Kafka Consumer
                   /       \
                  v         v
             CSV Output  PostgreSQL
                            |
                            v
                   Anomaly Detection
                     (Airflow DAG)
                            |
                            v
                       Anomalies
```

## Technologies Used

| Technology | Purpose |
|---|---|
| Python | Pipeline implementation and data processing |
| Apache Airflow | Workflow orchestration and scheduling |
| Apache Spark | Data transformation |
| Apache Kafka | Real-time data streaming |
| PostgreSQL | Data warehouse and persistent storage |
| Pandas | Data manipulation and analysis |
| SQLAlchemy | Database connectivity |
| Docker Compose | Containerized environment |
| CSV | Raw data storage and exchange |

## Project Features

### 1. Batch ETL Pipeline

The batch pipeline processes healthcare datasets, including heart disease and diabetes data.

Workflow:

1. Extract raw data from CSV files.
2. Transform and prepare the data using Apache Spark.
3. Load the processed data into PostgreSQL.
4. Orchestrate the pipeline with Apache Airflow.

The target tables include:

- `heart_disease`
- `diabetes`

### 2. Real-Time Data Streaming

Apache Kafka handles incoming patient health events through the `heart-stream` topic.

The consumer:

1. Reads events from Kafka.
2. Processes incoming JSON messages.
3. Saves streaming data to a CSV output file.
4. Inserts patient measurements into the PostgreSQL `heart_stream` table.

### 3. Anomaly Detection

The anomaly detection workflow uses Apache Airflow to analyze streaming measurements against baseline health data.

The monitored features include:

- `trestbps` — resting blood pressure
- `chol` — cholesterol
- `thalch` — maximum heart rate
- `oldpeak` — ST depression

The detection process uses predefined thresholds and statistical Z-scores to flag potentially unusual values.

Detected records are stored in the `anomalies` table for further analysis.

**Note:** This project is for educational and data engineering purposes. It does not provide medical diagnoses or replace professional medical advice.

## Project Structure

```text
Real-Time-Patient-Health-Monitoring-Anomaly-Detection-Pipeline/
│
├── dags/
│   ├── health_batch_pipeline.py
│   └── health_anomaly_pipeline.py
│
├── scripts/
│   ├── extract_csv.py
│   ├── spark_transformation.py
│   ├── load_to_datawarehouse.py
│   └── anomaly_detection.py
│
├── data_lake/
│   └── raw/
│       ├── heart_disease.csv
│       └── diabetes.csv
│
├── data/
│   └── heart_stream_output.csv
│
├── docker-compose.yml
└── README.md
```

*The directory structure above is indicative. Adapt it to match the actual files in your repository.*

## Prerequisites

Before running the project, install:

- Docker
- Docker Compose
- Git

Make sure Docker is running before starting the services.

## Installation and Setup

### 1. Clone the repository

Replace the URL below with your actual GitHub repository URL.

```bash
git clone <YOUR_GITHUB_REPOSITORY_URL>
cd Real-Time-Patient-Health-Monitoring-Anomaly-Detection-Pipeline
```

### 2. Start the services

```bash
docker compose up -d
```

Check the running containers:

```bash
docker compose ps
```

### 3. Access Airflow

Open the Airflow web interface using the address and credentials configured in your `docker-compose.yml`.

The default web address is commonly:

```text
http://localhost:8080
```

Activate the relevant DAGs in the Airflow interface to run the pipelines.

### 4. Verify the database

Connect to PostgreSQL using the credentials and port configured in your Docker Compose file.

For example:

```bash
docker exec -it <postgres_container_name> psql -U postgres -d health_dw
```

List the database tables:

```sql
\dt
```

Check the batch data:

```sql
SELECT * FROM heart_disease LIMIT 10;
SELECT * FROM diabetes LIMIT 10;
```

Check the streaming data and detected anomalies:

```sql
SELECT * FROM heart_stream LIMIT 10;
SELECT * FROM anomalies LIMIT 10;
```

Replace the container name, database name, and credentials if your configuration differs.

## Expected Results

After successful execution, the pipeline should:

- Process healthcare CSV datasets.
- Store transformed data in PostgreSQL.
- Consume real-time events from Kafka.
- Persist streaming measurements.
- Identify potentially anomalous measurements.
- Store detected anomalies for further analysis.

The actual results depend on the input datasets, Kafka messages, database configuration, and Airflow DAG execution.


## Author

**Roua Ben Tiba**

GitHub: [RouaBenTiba](https://github.com/RouaBenTiba)

---

*Built as a data engineering project exploring batch processing, real-time streaming, workflow orchestration, and anomaly detection.*
