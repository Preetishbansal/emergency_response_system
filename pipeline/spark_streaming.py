"""
Task 21 - Real-time Processing with Spark Structured Streaming
Reads the live Kafka 'emergency-alerts' topic, parses JSON,
detects severity anomalies, and counts active incidents per zone
in 30-second sliding windows.

Run with:
    python pipeline/spark_streaming.py

Prerequisites:
    - Docker Compose running (Kafka on localhost:9092)
    - producer.py sending alerts in another terminal
    - pip install pyspark kafka-python
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType
)

# ── Spark Session with Kafka connector ────────────────────────────────────────
spark = (
    SparkSession.builder
    .appName("EmergencyStreaming")
    .master("local[2]")
    # Downloads the Kafka connector JAR automatically (first run takes ~1 min)
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0"
    )
    .config("spark.sql.shuffle.partitions", "2")      # keep it lightweight locally
    .config("spark.ui.showConsoleProgress", "false")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

print("=" * 65)
print("⚡ Emergency Spark Streaming — Phase 4, Task 21")
print("   Reading from Kafka topic: emergency-alerts")
print("   Detecting anomalies + counting incidents per zone / 30s")
print("=" * 65)

# ── Schema matching producer.py alert structure ───────────────────────────────
alert_schema = StructType([
    StructField("incident_id",   StringType(),  True),
    StructField("timestamp",     StringType(),  True),   # ISO string from producer
    StructField("incident_type", StringType(),  True),
    StructField("zone",          StringType(),  True),
    StructField("severity",      StringType(),  True),
    StructField("response_time", DoubleType(),  True),
    StructField("unit_id",       StringType(),  True),
    StructField("hospital_id",   StringType(),  True),
    StructField("priority",      StringType(),  True),
])

# ── Read raw bytes from Kafka ─────────────────────────────────────────────────
raw_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "emergency-alerts")
    .option("startingOffsets", "latest")
    .load()
)

# ── Parse JSON value + cast timestamp ─────────────────────────────────────────
alerts = (
    raw_stream
    .select(
        F.from_json(F.col("value").cast("string"), alert_schema).alias("data"),
        F.col("timestamp").alias("kafka_timestamp")          # Kafka ingest time
    )
    .select(
        "data.*",
        "kafka_timestamp",
        # Convert ISO timestamp string → proper event_time for windowing
        F.to_timestamp(F.col("timestamp")).alias("event_time")
    )
)

# ── QUERY 1: Incident count per zone in 30-second tumbling windows ─────────────
print("\n[Q1] Starting zone-activity window (30s tumbling)…")

zone_counts = (
    alerts
    .withWatermark("event_time", "1 minute")       # tolerate up to 1 min late data
    .groupBy(
        F.window(F.col("event_time"), "30 seconds"),
        F.col("zone")
    )
    .agg(
        F.count("incident_id").alias("incident_count"),
        F.countDistinct("severity").alias("severity_variety")
    )
    .select(
        F.col("window.start").alias("window_start"),
        F.col("window.end").alias("window_end"),
        F.col("zone"),
        F.col("incident_count"),
        F.col("severity_variety")
    )
)

query_zones = (
    zone_counts.writeStream
    .outputMode("update")
    .format("console")
    .option("truncate", False)
    .option("numRows", 20)
    .trigger(processingTime="15 seconds")
    .queryName("zone_activity")
    .start()
)

# ── QUERY 2: Anomaly Detection — flag critical + high-response-time incidents ──
print("[Q2] Starting anomaly detector…")

anomalies = (
    alerts
    .filter(
        (F.col("severity") == "critical") |
        (F.col("response_time") > 2400)       # >40 minutes is an anomaly
    )
    .select(
        F.col("incident_id"),
        F.col("incident_type"),
        F.col("zone"),
        F.col("severity"),
        F.round(F.col("response_time") / 60, 1).alias("response_mins"),
        F.col("unit_id"),
        F.col("hospital_id"),
        F.col("event_time"),
        # Reason label
        F.when(F.col("severity") == "critical", "⚠️  CRITICAL SEVERITY")
         .when(F.col("response_time") > 2400,   "⏱️  SLOW RESPONSE >40min")
         .otherwise("MULTIPLE FLAGS")
         .alias("anomaly_reason")
    )
)

query_anomalies = (
    anomalies.writeStream
    .outputMode("append")
    .format("console")
    .option("truncate", False)
    .option("numRows", 10)
    .trigger(processingTime="10 seconds")
    .queryName("anomaly_detector")
    .start()
)

# ── QUERY 3: Live severity breakdown across all zones ─────────────────────────
print("[Q3] Starting severity breakdown stream…\n")

severity_breakdown = (
    alerts
    .withWatermark("event_time", "1 minute")
    .groupBy(
        F.window(F.col("event_time"), "30 seconds"),
        F.col("severity")
    )
    .agg(
        F.count("incident_id").alias("count"),
        F.avg("response_time").alias("avg_response_secs")
    )
    .select(
        F.col("window.start").alias("window_start"),
        F.col("severity"),
        F.col("count"),
        F.round(F.col("avg_response_secs") / 60, 2).alias("avg_response_mins")
    )
    .orderBy("severity")
)

query_severity = (
    severity_breakdown.writeStream
    .outputMode("update")
    .format("console")
    .option("truncate", False)
    .trigger(processingTime="20 seconds")
    .queryName("severity_breakdown")
    .start()
)

print("✅ All 3 streaming queries started. Consuming from Kafka…")
print("   Press Ctrl+C to stop.\n")

# ── Wait for all queries to finish (runs until Ctrl+C) ────────────────────────
spark.streams.awaitAnyTermination()
