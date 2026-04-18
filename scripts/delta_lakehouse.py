# -*- coding: utf-8 -*-
"""
Task 28 -- Delta Lake / Lakehouse
===================================
Adds ACID transactions, schema enforcement, and time-travel to the
emergency-response data lake using Delta Lake on top of Apache Spark.

Usage:
    pip install delta-spark pyspark
    python scripts/delta_lakehouse.py
"""

import os
import sys

# ── Windows: set HADOOP_HOME so Spark can find winutils.exe ─────────
# winutils.exe and hadoop.dll must exist in %HADOOP_HOME%\bin on Windows.
# Download from: https://github.com/cdarlint/winutils (use hadoop-3.3.5)
# and place both files at  C:\hadoop\bin\
if sys.platform.startswith("win"):
    _hadoop_home = r"C:\hadoop"
    os.environ["HADOOP_HOME"] = _hadoop_home
    os.environ["hadoop.home.dir"] = _hadoop_home
    # Prepend hadoop bin to PATH so JVM subprocess finds hadoop.dll
    _hadoop_bin = os.path.join(_hadoop_home, "bin")
    os.environ["PATH"] = _hadoop_bin + os.pathsep + os.environ.get("PATH", "")
    # Load hadoop.dll eagerly into this process; the JVM inherits its handle
    import ctypes
    _dll_path = os.path.join(_hadoop_bin, "hadoop.dll")
    if os.path.exists(_dll_path):
        ctypes.WinDLL(_dll_path)

from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable

# ── paths ───────────────────────────────────────────────────────────
BASE_DIR   = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
INPUT_CSV  = os.path.join(BASE_DIR, "data", "processed", "incidents_clean.csv")
DELTA_PATH = os.path.join(BASE_DIR, "data", "delta", "incidents")

# ── 1. Create Spark session with Delta Lake support ─────────────────
print("=" * 60)
print("  DELTA LAKE / LAKEHOUSE - Emergency Response System")
print("=" * 60)

builder = (
    SparkSession.builder
    .appName("DeltaLake")
    .master("local[*]")
    .config("spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    # Windows fix: disable native library loading to avoid NativeIO issues
    .config("spark.hadoop.io.native.lib.available", "false")
    # Use RawLocalFileSystem
    .config("spark.hadoop.fs.file.impl",
            "org.apache.hadoop.fs.RawLocalFileSystem")
    # Point local temp dir to a project folder to avoid C:\Users temp issues
    .config("spark.local.dir", os.path.join(BASE_DIR, "data", "spark_temp"))
)

# Ensure temp dir exists
os.makedirs(os.path.join(BASE_DIR, "data", "spark_temp"), exist_ok=True)

spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# ── Windows: Py4J Hack to bypass NativeIO access0 check ─────────────
if sys.platform.startswith("win"):
    try:
        # Access the JVM bridge
        sc = spark.sparkContext
        jvm = sc._gateway.jvm
        
        # Get the NativeIO$Windows class
        native_io_class = jvm.org.apache.hadoop.io.nativeio.NativeIO.Windows
        
        # Use reflection to find the 'skipstatuscheck' field if it exists (Hadoop 3.x)
        # and set it to True. This avoids the call to access0() which fails on Windows.
        try:
            field = native_io_class.getClass().getDeclaredField("skipstatuscheck")
            field.setAccessible(True)
            field.set(None, True)
            print("[OK] Bypassed Hadoop NativeIO check via reflection.")
        except:
            # For some Hadoop versions the field name might differ or not exist
            print("[WARN] Could not find skipstatuscheck field, skipping reflection hack.")
    except Exception as e:
        print(f"[WARN] Failed to apply NativeIO hack: {e}")

print("\n[OK] Spark session created with Delta Lake support.\n")

# ── 2. Ingest CSV -> Delta table ────────────────────────────────────
print("-" * 60)
print("Step 1 >> Writing incidents CSV as a Delta table ...")
print("-" * 60)

df = spark.read.csv(INPUT_CSV, header=True, inferSchema=True)
print(f"   Rows read from CSV : {df.count()}")
print(f"   Columns            : {df.columns}\n")

df.write.format("delta").mode("overwrite").save(DELTA_PATH)
print(f"   [OK] Delta table written to: {DELTA_PATH}\n")

# Show a sample of the freshly-written table (version 0)
print("   Sample (version 0 - original data):")
spark.read.format("delta").load(DELTA_PATH).show(5, truncate=False)

# ── 3. ACID Update -- normalise severity values ─────────────────────
print("-" * 60)
print("Step 2 >> Updating severity values (ACID transaction) ...")
print("-" * 60)

delta_table = DeltaTable.forPath(spark, DELTA_PATH)

# Standardise severity to lowercase (e.g., "Critical" -> "critical")
for original in ["Critical", "CRITICAL", "High", "HIGH", "Medium", "MEDIUM", "Low", "LOW"]:
    delta_table.update(
        condition=f"severity == '{original}'",
        set={"severity": f"'{original.lower()}'"}
    )

print("   [OK] Severity values normalised to lowercase.\n")
print("   Sample (version 1, after update):")
spark.read.format("delta").load(DELTA_PATH).show(5, truncate=False)

# ── 4. TIME TRAVEL -- compare before vs. after ─────────────────────
print("-" * 60)
print("Step 3 >> TIME TRAVEL -- reading version 0 (before update) ...")
print("-" * 60)

df_v0 = (
    spark.read.format("delta")
    .option("versionAsOf", 0)
    .load(DELTA_PATH)
)

print("\n   [TIME TRAVEL] Version 0 (original, before update):")
df_v0.select("incident_id", "severity", "incident_type", "zone").show(5, truncate=False)

df_latest = spark.read.format("delta").load(DELTA_PATH)
print("   [CURRENT] Latest version (after update):")
df_latest.select("incident_id", "severity", "incident_type", "zone").show(5, truncate=False)

# ── 5. Delta table history (audit log) ─────────────────────────────
print("-" * 60)
print("Step 4 >> Delta table history (audit log) ...")
print("-" * 60)
delta_table.history().select(
    "version", "timestamp", "operation", "operationMetrics"
).show(truncate=False)

# ── 6. Schema ──────────────────────────────────────────────────────
print("-" * 60)
print("Step 5 >> Schema of Delta table ...")
print("-" * 60)
spark.read.format("delta").load(DELTA_PATH).printSchema()

# ── cleanup ────────────────────────────────────────────────────────
spark.stop()
print("\n[DONE] Delta Lakehouse demo complete.")
print("=" * 60)
