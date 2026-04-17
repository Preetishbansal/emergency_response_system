from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import broadcast
import time

spark = SparkSession.builder \
    .appName("SparkOptimization") \
    .master("local[*]") \
    .config("spark.ui.showConsoleProgress", "false") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

df = spark.read.csv('data/raw/incidents_large.csv', header=True, inferSchema=True)

# 1. Partition data (split by zone for parallel processing)
print("--- OPTIMIZATION 1: Repartitioning by zone ---")
df_partitioned = df.repartition('zone_id')
print(f"Partitions after repartition: {df_partitioned.rdd.getNumPartitions()}")

# 2. Cache hot datasets (avoid re-reading from disk)
print("\n--- OPTIMIZATION 2: Caching critical incidents ---")
df_critical = df.filter(F.col('severity') == 'critical').cache()

# First action — reads from disk + fills cache
start = time.time()
count = df_critical.count()
print(f"Without cache (first run): {time.time() - start:.2f}s  |  Critical rows: {count:,}")

# Second action — served from memory
start = time.time()
df_critical.count()
print(f"With cache (second run):   {time.time() - start:.2f}s  (much faster!)")

# 3. Execution plan
print("\n--- OPTIMIZATION 3: Execution plan for group-by ---")
df.groupBy('zone_id').count().explain()

# 4. Benchmark: cached vs non-cached
print("\n--- BENCHMARK: avg response time ---")
start = time.time()
avg = df_critical.agg(F.avg('response_time')).collect()
print(f"Cached avg query: {time.time()-start:.2f}s | Avg: {round(avg[0][0], 2)} secs")

print("\nTask 17 complete!")
df_critical.unpersist()
spark.stop()
