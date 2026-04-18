from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd, sqlite3

default_args = {
    'owner': 'you',
    'retries': 3,                          # retry 3 times on failure
    'retry_delay': timedelta(minutes=5),   # wait 5 min between retries
    'email_on_failure': False,
}

with DAG(
    dag_id='emergency_nightly_etl',
    default_args=default_args,
    description='Nightly ETL for emergency incident data',
    schedule='0 0 * * *',   # run at midnight every day
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:

    def extract_task():
        df = pd.read_csv('/data/raw/incidents.csv')
        df.to_parquet('/data/processed/raw_extract.parquet')
        print(f"Extracted {len(df)} rows")

    def validate_task():
        """Task 29 — Automated Data Quality Checks"""
        import pandas as pd
        from data_quality import run_quality_checks
        
        df = pd.read_parquet('/data/processed/raw_extract.parquet')
        is_valid = run_quality_checks(df, name="Nightly ETL Ingest")
        
        if not is_valid:
            raise ValueError("Data quality checks failed! Stopping pipeline.")
        print("Data quality checks passed.")

    def transform_task():
        df = pd.read_parquet('/data/processed/raw_extract.parquet')
        df.dropna(inplace=True)
        df['severity'] = df['severity'].str.lower()
        df.to_parquet('/data/processed/transformed.parquet')
        print(f"Transformed: {len(df)} clean rows")

    def load_task():
        df = pd.read_parquet('/data/processed/transformed.parquet')
        conn = sqlite3.connect('/data/emergency.db')
        df.to_sql('incidents', conn, if_exists='append', index=False)
        conn.close()
        print("Loaded to database!")

    def save_to_lakehouse():
        """Task 28 — Write transformed data to Delta Lakehouse for ACID
        transactions, schema enforcement, and time-travel queries."""
        from pyspark.sql import SparkSession
        from delta import configure_spark_with_delta_pip
        from delta.tables import DeltaTable

        DELTA_PATH    = '/data/delta/incidents'
        PARQUET_INPUT = '/data/processed/transformed.parquet'

        builder = (
            SparkSession.builder
            .appName("DeltaLakehouse_ETL")
            .master("local[*]")
            .config("spark.sql.extensions",
                    "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog",
                    "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        )
        spark = configure_spark_with_delta_pip(builder).getOrCreate()
        spark.sparkContext.setLogLevel("WARN")

        # Read the transformed Parquet produced by transform_task
        df = spark.read.parquet(PARQUET_INPUT)
        row_count = df.count()
        print(f"[Lakehouse] Writing {row_count} rows to Delta table …")

        # Append or overwrite — "append" keeps history, enabling time-travel
        df.write.format("delta").mode("append").save(DELTA_PATH)
        print(f"[Lakehouse] ✅ Delta table updated at: {DELTA_PATH}")

        # Log the latest version from the Delta history
        delta_table = DeltaTable.forPath(spark, DELTA_PATH)
        history = delta_table.history(1).collect()
        print(f"[Lakehouse] Latest version: {history[0]['version']}  "
              f"operation: {history[0]['operation']}")

        spark.stop()

    t1 = PythonOperator(task_id='extract',           python_callable=extract_task)
    t_val = PythonOperator(task_id='validate',        python_callable=validate_task)
    t2 = PythonOperator(task_id='transform',         python_callable=transform_task)
    t3 = PythonOperator(task_id='load',              python_callable=load_task)
    t4 = PythonOperator(task_id='save_to_lakehouse', python_callable=save_to_lakehouse)

    t1 >> t_val >> t2 >> t3 >> t4   # chain: extract → validate → transform → load → lakehouse
