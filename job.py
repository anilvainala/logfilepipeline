# job.py
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Pipeline") \
    .enableHiveSupport() \
    .getOrCreate()

df = spark.read.text("hdfs://localhost:9000/data/logs/access.log")

# create DB
spark.sql("CREATE DATABASE IF NOT EXISTS logsdb")

# write table
df.write.mode("overwrite").saveAsTable("logsdb.processed_logs")

spark.stop()