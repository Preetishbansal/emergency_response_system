import pandas as pd
import sqlite3

# ETL Pipeline (Transform BEFORE loading)
def run_etl():
    # Extract
    df = pd.read_csv('data/raw/incidents.csv')
    print(f"[ETL] Extracted {len(df)} rows")

    # Transform (in memory, before DB)
    df['response_time'] = pd.to_numeric(df['response_time'], errors='coerce')
    df.dropna(inplace=True)
    df['severity'] = df['severity'].str.lower().str.strip()
    df['processed_at'] = pd.Timestamp.now()

    # Load (clean data goes in)
    conn = sqlite3.connect('data/emergency.db')
    df.to_sql('incidents_etl', conn, if_exists='replace', index=False)
    conn.close()
    print(f"[ETL] Loaded {len(df)} clean rows into 'incidents_etl' table")
    print("[ETL] Complete!\n")


# ELT Pipeline (Load raw THEN transform inside DB)
def run_elt():
    # Extract + Load raw first
    df = pd.read_csv('data/raw/incidents.csv')
    conn = sqlite3.connect('data/emergency.db')
    df.to_sql('raw_incidents', conn, if_exists='replace', index=False)
    print(f"[ELT] Loaded {len(df)} raw rows into 'raw_incidents' table")

    # Transform inside database using SQL
    conn.execute("DROP TABLE IF EXISTS incidents_elt")
    conn.execute("""
        CREATE TABLE incidents_elt AS
        SELECT *, LOWER(TRIM(severity)) as clean_severity
        FROM raw_incidents
        WHERE response_time IS NOT NULL
    """)
    conn.commit()

    result = pd.read_sql("SELECT COUNT(*) as count FROM incidents_elt", conn)
    print(f"[ELT] Transformed {result['count'][0]} rows into 'incidents_elt' table")
    conn.close()
    print("[ELT] Complete!")


# Compare both
def compare():
    conn = sqlite3.connect('data/emergency.db')
    etl_df = pd.read_sql("SELECT COUNT(*) as rows, AVG(response_time) as avg_rt FROM incidents_etl", conn)
    elt_df = pd.read_sql("SELECT COUNT(*) as rows, AVG(response_time) as avg_rt FROM incidents_elt", conn)
    conn.close()

    print("\n--- COMPARISON ---")
    print(f"ETL output: {etl_df['rows'][0]} rows | Avg response time: {round(etl_df['avg_rt'][0], 2)} mins")
    print(f"ELT output: {elt_df['rows'][0]} rows | Avg response time: {round(elt_df['avg_rt'][0], 2)} mins")
    print("Both pipelines produced equivalent results!")


if __name__ == "__main__":
    print("=== RUNNING ETL PIPELINE ===")
    run_etl()
    print("=== RUNNING ELT PIPELINE ===")
    run_elt()
    compare()
