"""
Task 20 - Kafka Consumer: Hospital / Unit Alert Receiver
Reads real-time emergency alerts from Kafka, processes them, and logs
critical incidents to the SQLite database used in database_queries.py.
"""

import json
import sqlite3
import os
from datetime import datetime
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
import time

# ── Configuration ──────────────────────────────────────────────────────────────
KAFKA_BROKER  = 'localhost:9092'
TOPIC_NAME    = 'emergency-alerts'
GROUP_ID      = 'hospital-group'          # consumer group (Task 20)
DB_PATH       = 'data/emergency.db'
LOG_PATH      = 'data/raw/streaming.log'  # same log used by emergency_server.py

# Severity weights for display
SEVERITY_ICONS = {
    'low':      '🟢',
    'medium':   '🟡',
    'high':     '🟠',
    'critical': '🔴',
}


# ── DB setup ───────────────────────────────────────────────────────────────────
def setup_database():
    """
    Ensure the incidents table exists and has all required columns.
    If the DB was created by an older script (e.g. database_queries.py) that
    didn't include unit_id / hospital_id, we add those columns automatically.
    """
    os.makedirs('data', exist_ok=True)
    conn = sqlite3.connect(DB_PATH)

    # Create table with full schema if it doesn't exist yet
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
    conn.commit()

    # ── Auto-migrate: add any missing columns to an existing table ─────────────
    existing_cols = {row[1] for row in conn.execute("PRAGMA table_info(incidents)")}
    migrations = {
        'unit_id':    'ALTER TABLE incidents ADD COLUMN unit_id    TEXT',
        'hospital_id':'ALTER TABLE incidents ADD COLUMN hospital_id TEXT',
    }
    for col, sql in migrations.items():
        if col not in existing_cols:
            conn.execute(sql)
            print(f"  🔧 DB migrated: added column '{col}' to incidents table")
    conn.commit()

    return conn


def save_to_db(conn, alert: dict):
    """Persist a received alert into SQLite (upsert — safe to re-run)."""
    try:
        conn.execute("""
            INSERT OR REPLACE INTO incidents
                (incident_id, timestamp, zone, severity, response_time, unit_id, hospital_id)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            alert.get('incident_id'),
            alert.get('timestamp'),
            alert.get('zone'),
            alert.get('severity'),
            alert.get('response_time'),
            alert.get('unit_id'),
            alert.get('hospital_id'),
        ))
        conn.commit()
    except sqlite3.Error as e:
        print(f"  ⚠️  DB error: {e}")

def log_to_file(alert: dict):
    """Append received alert to the streaming log (same file as emergency_server.py)."""
    os.makedirs('data/raw', exist_ok=True)
    with open(LOG_PATH, 'a') as f:
        f.write(json.dumps(alert) + '\n')

# ── Connect to Kafka ───────────────────────────────────────────────────────────
def create_consumer():
    """Create and return a KafkaConsumer with retry logic."""
    for attempt in range(1, 6):
        try:
            consumer = KafkaConsumer(
                TOPIC_NAME,
                bootstrap_servers=KAFKA_BROKER,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                group_id=GROUP_ID,           # enables offset tracking (Task 20)
                auto_offset_reset='latest',  # start from newest messages
                enable_auto_commit=True,
            )
            print(f"✅ Consumer connected | topic={TOPIC_NAME} | group={GROUP_ID}")
            return consumer
        except NoBrokersAvailable:
            print(f"⏳ Kafka not ready (attempt {attempt}/5). Retrying in 3s...")
            time.sleep(3)
    raise RuntimeError("❌ Could not connect to Kafka. Is Docker running?")

# ── Stats tracker ──────────────────────────────────────────────────────────────
class AlertStats:
    def __init__(self):
        self.total = 0
        self.by_severity = {'low': 0, 'medium': 0, 'high': 0, 'critical': 0}
        self.by_zone     = {}
        self.by_type     = {}

    def record(self, alert: dict):
        self.total += 1
        sev  = alert.get('severity', 'unknown')
        zone = alert.get('zone', 'unknown')
        itype = alert.get('incident_type', 'unknown')

        self.by_severity[sev] = self.by_severity.get(sev, 0) + 1
        self.by_zone[zone]    = self.by_zone.get(zone, 0) + 1
        self.by_type[itype]   = self.by_type.get(itype, 0) + 1

    def print_summary(self):
        print("\n" + "─" * 65)
        print(f"  📊 SESSION SUMMARY  ({self.total} alerts received)")
        print("─" * 65)
        print("  By Severity :", {k: v for k, v in self.by_severity.items() if v})
        print("  By Zone     :", self.by_zone)
        print("  By Type     :", self.by_type)
        print("─" * 65)

# ── Main loop ──────────────────────────────────────────────────────────────────
def main():
    print("=" * 65)
    print("🏥 Hospital Alert Consumer — Phase 4, Task 20")
    print(f"   Broker : {KAFKA_BROKER}")
    print(f"   Topic  : {TOPIC_NAME}")
    print(f"   Group  : {GROUP_ID}")
    print(f"   DB     : {DB_PATH}")
    print("   Waiting for live alerts… (Ctrl+C to stop)")
    print("=" * 65)

    conn     = setup_database()
    consumer = create_consumer()
    stats    = AlertStats()

    try:
        for message in consumer:
            alert = message.value
            stats.record(alert)

            icon = SEVERITY_ICONS.get(alert.get('severity', ''), '⚪')
            print(
                f"  {icon} [{alert.get('incident_id')}] "
                f"{alert.get('incident_type','?'):8s} | "
                f"Zone: {alert.get('zone','?'):7s} | "
                f"Severity: {alert.get('severity','?'):8s} | "
                f"Unit: {alert.get('unit_id','?')} | "
                f"Partition: {message.partition} | "
                f"Offset: {message.offset}"
            )

            # Save ALL alerts to DB and log file
            save_to_db(conn, alert)
            log_to_file(alert)

            # Extra action for critical alerts
            if alert.get('severity') == 'critical':
                print(
                    f"    🚨 CRITICAL ALERT — Dispatching {alert.get('unit_id')} "
                    f"to {alert.get('zone')} Zone → {alert.get('hospital_id')}"
                )

    except KeyboardInterrupt:
        print("\n🛑 Consumer stopped.")
        stats.print_summary()
    finally:
        consumer.close()
        conn.close()
        print("Consumer and DB connection closed.")

if __name__ == '__main__':
    main()
