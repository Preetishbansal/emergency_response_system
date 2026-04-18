from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
import os
import sys

# Ensure Windows finds the Hadoop winutils binaries we added previously
os.environ['HADOOP_HOME'] = os.path.join(os.getcwd(), 'hadoop')
os.environ['PATH'] += os.pathsep + os.path.join(os.getcwd(), 'hadoop', 'bin')

# Build Spark Session configured with Delta
builder = SparkSession.builder \
    .appName("DeltaLake") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

print("=> Reading Clean Incidents CSV")
# Write data as Delta table
df = spark.read.csv('data/processed/incidents_clean.csv', header=True, inferSchema=True)

print("=> Writing data as Delta table...")
df.write.format("delta").mode("overwrite").save("data/delta/incidents")

print("=> Updating records using Delta Lake time-travel capabilities...")
# Update some records (Delta handles this cleanly)
from delta.tables import DeltaTable
delta_table = DeltaTable.forPath(spark, "data/delta/incidents")
delta_table.update(
    condition="severity == 'CRITICAL'",
    set={"severity": "'critical'"}
)

print("=> TIME TRAVEL: Viewing data BEFORE the update! (version 0)")
# TIME TRAVEL — see data as it was before the update!
spark.read.format("delta") \
    .option("versionAsOf", 0) \
    .load("data/delta/incidents") \
    .show(5)   # shows original data before update
