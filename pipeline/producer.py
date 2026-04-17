"""
Task 19 - Kafka Producer: 911 Dispatch System
Simulates a dispatch center sending real-time emergency alerts to Kafka.
Matches the alert schema already used in emergency_client.py + database_queries.py.
"""

import json
import time
import random
import uuid
from datetime import datetime
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

# ── Configuration ──────────────────────────────────────────────────────────────
KAFKA_BROKER   = 'localhost:9092'
TOPIC_NAME     = 'emergency-alerts'
ALERT_INTERVAL = 1   # seconds between alerts

# Data matching the project's existing schema (zone, severity, incident_type)
INCIDENT_TYPES = ['Fire', 'Medical', 'Police', 'Accident', 'Hazmat']
ZONES          = ['North', 'South', 'East', 'West', 'Central']
SEVERITIES     = ['low', 'medium', 'high', 'critical']
UNIT_IDS       = ['UNIT-101', 'UNIT-202', 'UNIT-303', 'UNIT-404', 'UNIT-505']
HOSPITAL_IDS   = ['HOSP-A', 'HOSP-B', 'HOSP-C']

# ── Connect to Kafka ───────────────────────────────────────────────────────────
def create_producer():
    """Create and return a KafkaProducer with retry logic."""
    for attempt in range(1, 6):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8'),
                retries=3,
            )
            print(f"✅ Connected to Kafka broker at {KAFKA_BROKER}")
            return producer
        except NoBrokersAvailable:
            print(f"⏳ Kafka not ready yet (attempt {attempt}/5). Retrying in 3s...")
            time.sleep(3)
    raise RuntimeError("❌ Could not connect to Kafka after 5 attempts. Is Docker running?")

# ── Generate an alert matching the project schema ──────────────────────────────
def generate_alert() -> dict:
    severity = random.choice(SEVERITIES)
    zone     = random.choice(ZONES)
    itype    = random.choice(INCIDENT_TYPES)

    return {
        'incident_id':   str(uuid.uuid4())[:8].upper(),
        'timestamp':     datetime.now().isoformat(),
        'incident_type': itype,
        'zone':          zone,
        'severity':      severity,
        'response_time': round(random.uniform(120, 3500), 2),   # seconds
        'unit_id':       random.choice(UNIT_IDS),
        'hospital_id':   random.choice(HOSPITAL_IDS),
        'priority':      'Critical' if severity == 'critical' else 'Standard',
    }

# ── Delivery callback ──────────────────────────────────────────────────────────
def on_send_success(record_metadata, alert):
    print(
        f"  ✉️  Sent  [{alert['incident_id']}] "
        f"{alert['incident_type']:8s} | Zone: {alert['zone']:7s} | "
        f"Severity: {alert['severity']:8s} "
        f"→ topic={record_metadata.topic} "
        f"partition={record_metadata.partition} "
        f"offset={record_metadata.offset}"
    )

def on_send_error(exc):
    print(f"  ❌ Failed to send message: {exc}")

# ── Main loop ──────────────────────────────────────────────────────────────────
def main():
    print("=" * 65)
    print("🚨 Emergency Dispatch Producer — Phase 4, Task 19")
    print(f"   Broker : {KAFKA_BROKER}")
    print(f"   Topic  : {TOPIC_NAME}")
    print(f"   Rate   : 1 alert / {ALERT_INTERVAL}s   (Ctrl+C to stop)")
    print("=" * 65)

    producer = create_producer()
    sent = 0

    try:
        while True:
            alert = generate_alert()

            # Partition by incident_type so consumers can filter by type
            producer.send(
                TOPIC_NAME,
                key=alert['incident_type'],
                value=alert
            ).add_callback(
                lambda meta, a=alert: on_send_success(meta, a)
            ).add_errback(on_send_error)

            producer.flush()   # ensure message is sent before sleeping
            sent += 1
            time.sleep(ALERT_INTERVAL)

    except KeyboardInterrupt:
        print(f"\n🛑 Stopped after sending {sent} alerts.")
    finally:
        producer.close()
        print("Producer closed.")

if __name__ == '__main__':
    main()
