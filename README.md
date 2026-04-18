# Emergency Response Coordination System

> An end-to-end data engineering pipeline that processes emergency incident data in real time —  
> from raw 911 dispatch alerts through Kafka streaming, Spark processing, Delta Lake storage, and Airflow orchestration.

---

## What This Project Does

This project simulates a production-grade emergency response data platform. It ingests 911 incident data, cleans and transforms it, stores it in a relational database, streams live alerts through Apache Kafka, processes large volumes with Apache Spark, and orchestrates everything automatically using Apache Airflow DAGs. Data is persisted in a Delta Lakehouse for ACID compliance and time-travel queries.

**Pipeline overview:**

```
Raw CSV Data
    │
    ▼
Python ETL (clean, normalize, validate)
    │
    ▼
SQLite Database (structured query layer)
    │
    ├──► Apache Kafka (real-time streaming)
    │
    ▼
Apache Spark (large-scale aggregations)
    │
    ▼
Delta Lake (versioned, ACID-compliant storage)
    │
    ▼
Apache Airflow DAG (automated orchestration)
    │
    ▼
Final Report (CSV + dashboard summary)
```

---

## Technologies Used

| Phase | Technology | Purpose |
|-------|------------|---------|
| Ingestion | Python, Pandas | Read, clean, and normalize CSV data |
| Database | SQLite, SQL | Store and query incidents relationally |
| Data Quality | Great Expectations | Schema validation, anomaly detection |
| Big Data | Apache Spark (PySpark) | Process 1M+ records at scale |
| Streaming | Apache Kafka | Real-time emergency alert pipeline |
| Orchestration | Apache Airflow | Schedule, monitor, and automate DAGs |
| Storage | Delta Lake | Versioned, ACID-compliant data lakehouse |
| Cloud | Google Colab | Cloud compute environment for notebooks |

---

## Project Structure

```
emergency_response_system/
├── dags/
│   ├── emergency_etl_dag.py      # Nightly ETL pipeline (Airflow DAG)
│   └── final_pipeline.py         # Capstone: end-to-end orchestration DAG
├── pipeline/
│   ├── producer.py               # Kafka producer (911 dispatch simulator)
│   ├── consumer.py               # Kafka consumer
│   ├── spark_streaming.py        # Spark Structured Streaming
│   └── upload_to_cloud.py        # Cloud (MinIO/S3) upload utility
├── scripts/
│   ├── transformations.py        # ETL transformation functions
│   ├── data_quality.py           # Automated data validation (Task 29)
│   ├── delta_lakehouse.py        # Delta Lake read/write/time-travel
│   ├── generate_incidents.py     # Synthetic data generator
│   ├── clean_data.py             # Data cleaning pipeline
│   ├── star_schema.py            # Star schema warehouse setup
│   └── final_integration_test.py # End-to-end integration test
├── notebooks/                    # Google Colab notebooks (one per phase)
├── reports/
│   └── final_report.csv          # Instructor summary report
├── docker-compose.yml            # Docker services (Kafka, Zookeeper)
├── docker-compose-airflow.yml    # Airflow stack
├── docker-compose-minio.yml      # MinIO cloud storage stack
├── .gitignore
└── README.md
```

---

## How to Run

### Prerequisites
- Python 3.10+
- Java 11+ (required for PySpark)
- Docker Desktop (for Kafka and Airflow services)

### 1. Clone the repository
```bash
git clone https://github.com/Preetishbansal/emergency_response_system.git
cd emergency_response_system
```

### 2. Set up the Python environment
```bash
python -m venv venv
venv\Scripts\activate          # Windows
pip install pandas pyspark delta-spark kafka-python great-expectations apache-airflow
```

### 3. Start Kafka and Airflow (Docker)
```bash
docker-compose up -d           # starts Kafka + Zookeeper
docker-compose -f docker-compose-airflow.yml up -d  # starts Airflow
```

### 4. Run the full integration test locally
```bash
python scripts/final_integration_test.py
```

### 5. Access Airflow UI
Open [http://localhost:8080](http://localhost:8080) and trigger:
- `emergency_nightly_etl` — nightly ETL with data quality checks
- `emergency_response_system` — full end-to-end capstone pipeline

### 6. Google Colab (Cloud)
1. Open [colab.research.google.com](https://colab.research.google.com)
2. Upload notebooks from the `/notebooks` folder
3. Mount Google Drive: `drive.mount('/content/drive')`
4. Run notebooks in order: `01_setup → 02_etl → 03_spark → 04_kafka → 05_airflow → 06_final`

---

## Sample Output

After running the final pipeline, the dashboard report looks like this:

```
=== EMERGENCY RESPONSE SYSTEM — FINAL REPORT ===
    zone  total_incidents  avg_response_mins  critical_count
    West               27                0.1               0
   North               19                0.1               0
   South               18                0.1               0
    East               18                0.2               0
Downtown               18                0.1               0
```

Full report saved to `reports/final_report.csv`.

---

## Key Features

- **Automated Data Quality**: Validates schema, checks for nulls, detects Z-score anomalies in response times — pipeline halts automatically if quality fails.
- **ACID Compliance**: Delta Lake ensures every write is transactional, with rollback capability.
- **Time Travel**: Query historical snapshots of incident data using Delta's versioning.
- **Real-Time Streaming**: Kafka producer simulates live 911 dispatch and sends alerts to consumers in under 1 second per event.
- **Scalable Analytics**: PySpark processes the full dataset in seconds, compared to minutes with pure pandas.

---

## What I Learned

### Key takeaways from each phase:

- **Linux & Python**: Automating file operations with scripts saves hours of manual work in real-world data pipelines.
- **SQL & Warehousing**: Designing a star schema made analytical queries significantly faster and easier to maintain than flat-table approaches.
- **Spark**: Processing 1M+ rows with PySpark was dramatically faster than pandas alone — distributed computing is essential at scale.
- **Kafka**: Real-time streaming is fundamentally different from batch processing. You process each event the moment it arrives, enabling sub-second alerting.
- **Airflow**: Orchestration is what transforms a collection of scripts into a reliable, self-healing production system. DAGs provide visibility, retries, and scheduling out-of-the-box.
- **Delta Lake**: Time travel solved the problem of "what did our data look like before that bad update?" — an invaluable feature for production data engineering.
- **Data Quality**: Automated validation with Great Expectations catches data corruption before it propagates downstream, which is far cheaper than fixing corrupted reports.

### Biggest challenge:
Configuring Apache Spark on Windows — `NativeIO` and Hadoop DLL errors required careful environment setup with `winutils.exe` binaries and environment variable management via Python itself.

### What I would add with more time:
- A live web dashboard using **Streamlit** showing real-time incident maps
- **Automated alerting** via email/SMS when response times exceed 10 minutes
- **Machine learning** (scikit-learn) to predict incident severity from zone and time-of-day patterns
- A **Kubernetes** deployment to make the pipeline truly production-scale

---

## Author

Preetish Bansal  
Data Engineering Capstone Project