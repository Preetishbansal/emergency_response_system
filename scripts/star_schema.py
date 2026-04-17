import sqlite3

# Create star schema in your database
conn = sqlite3.connect('data/emergency_warehouse.db')

# FACT table - the main events
conn.execute("""
CREATE TABLE IF NOT EXISTS fact_incidents (
    incident_id TEXT PRIMARY KEY,
    time_id INTEGER,
    location_id INTEGER,
    unit_id INTEGER,
    severity_id INTEGER,
    response_time_seconds REAL,
    outcome TEXT
)""")

# DIMENSION tables - the context
conn.execute("""
CREATE TABLE IF NOT EXISTS dim_time (
    time_id INTEGER PRIMARY KEY,
    date DATE, hour INTEGER, day_of_week TEXT, is_weekend INTEGER
)""")

conn.execute("""
CREATE TABLE IF NOT EXISTS dim_location (
    location_id INTEGER PRIMARY KEY,
    zone TEXT, district TEXT, city TEXT, lat REAL, lon REAL
)""")

conn.execute("""
CREATE TABLE IF NOT EXISTS dim_unit (
    unit_id INTEGER PRIMARY KEY,
    unit_name TEXT, unit_type TEXT, base_station TEXT
)""")

print("Star schema created!")
conn.close()
