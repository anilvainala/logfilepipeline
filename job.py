# job.py

from pyspark.sql import SparkSession

# Create Spark session with Hive support
spark = SparkSession.builder \
    .appName("LogPipeline") \
    .enableHiveSupport() \
    .getOrCreate()

print("=== Spark Session Created ===")

# Read file from HDFS
df = spark.read.text("hdfs://localhost:9000/data/logs/access.log")

# Debug checks
print("=== Checking Data ===")
print("Row Count:", df.count())

df.show(truncate=False)

# Create database if not exists
spark.sql("CREATE DATABASE IF NOT EXISTS logsdb")

# Write to Hive table
df.write.mode("overwrite").saveAsTable("logsdb.processed_logs")

print("=== Data written to Hive table logsdb.processed_logs ===")

# Stop Spark
spark.stop()