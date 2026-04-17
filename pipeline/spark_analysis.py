from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Create a Spark session (the entry point to Spark)
spark = SparkSession.builder \
    .appName("EmergencyResponseAnalysis") \
    .master("local[*]") \
    .config("spark.ui.showConsoleProgress", "false") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Read CSV (Spark can handle millions of rows)
df = spark.read.csv('data/raw/incidents_large.csv', header=True, inferSchema=True)
print(f"\nTotal rows: {df.count():,}")

# Task 15: DataFrame transformations
print("\n--- TASK 15: Zone Analysis ---")
result = df \
    .filter(F.col('response_time') < 3600) \
    .withColumn('response_mins', F.col('response_time') / 60) \
    .groupBy('zone_id') \
    .agg(
        F.count('incident_id').alias('total_incidents'),
        F.avg('response_mins').alias('avg_response_mins'),
        F.max('response_mins').alias('max_response_mins')
    ) \
    .orderBy('avg_response_mins')

result.show(20)

# Task 16: Spark SQL - same query but SQL syntax
print("--- TASK 16: Spark SQL ---")
df.createOrReplaceTempView("incidents")
spark.sql("""
    SELECT zone_id, COUNT(*) as total, ROUND(AVG(response_time/60), 2) as avg_mins
    FROM incidents
    WHERE response_time < 3600
    GROUP BY zone_id ORDER BY avg_mins
""").show()

print("Tasks 14-16 complete!")
spark.stop()
