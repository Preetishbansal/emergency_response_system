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

    t1 = PythonOperator(task_id='extract', python_callable=extract_task)
    t2 = PythonOperator(task_id='transform', python_callable=transform_task)
    t3 = PythonOperator(task_id='load', python_callable=load_task)

    t1 >> t2 >> t3   # t1 runs first, then t2, then t3
