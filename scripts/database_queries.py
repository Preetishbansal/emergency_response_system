import sqlite3
import pandas as pd

# Create database and table
conn = sqlite3.connect('data/emergency.db')

# Create tables
conn.execute("""
CREATE TABLE IF NOT EXISTS incidents (
    incident_id    TEXT PRIMARY KEY,
    timestamp      DATETIME,
    zone           TEXT,
    severity       TEXT,
    response_time  REAL,
    unit_id        TEXT,
    hospital_id    TEXT
)
""")

# Load data from CSV into database
df = pd.read_csv('data/processed/incidents_clean.csv')
df.to_sql('incidents', conn, if_exists='replace', index=False)

# Basic SQL queries (Task 6)
print("--- TASK 6: Critical Incidents by Zone ---")
result = pd.read_sql("""
    SELECT zone, COUNT(*) as total, AVG(response_time) as avg_time
    FROM incidents
    WHERE LOWER(severity) = 'critical'
    GROUP BY zone
    ORDER BY avg_time ASC
""", conn)
print(result)
print("\n")

# Window function (Task 7) - rank zones by response time
print("--- TASK 7: Ranking Zones by Average Speed ---")
result2 = pd.read_sql("""
    SELECT zone, avg_time,
           RANK() OVER (ORDER BY avg_time) as speed_rank
    FROM (
        SELECT zone, AVG(response_time) as avg_time
        FROM incidents GROUP BY zone
    )
""", conn)
print(result2)

conn.close()
