from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_extract, concat_ws

spark = SparkSession.builder \
    .appName("LogPipeline") \
    .enableHiveSupport() \
    .getOrCreate()

# Read log file from HDFS
df = spark.read.text("hdfs://localhost:9000/data/logs/access.log")

# Extract fields
structured_df = df.select(
    concat_ws(
        " ",
        regexp_extract("value", r"^(\d{4}-\d{2}-\d{2})", 1),
        regexp_extract("value", r"^\d{4}-\d{2}-\d{2}\s+(\d{2}:\d{2}:\d{2})", 1)
    ).alias("timestamp"),

    regexp_extract(
        "value",
        r"\d{2}:\d{2}:\d{2}\s+(\w+)",
        1
    ).alias("level"),

    regexp_extract(
        "value",
        r"user=(\w+)",
        1
    ).alias("user"),

    regexp_extract(
        "value",
        r"action=([\w_]+)",
        1
    ).alias("action")
)

# Create database
spark.sql("CREATE DATABASE IF NOT EXISTS logsdb")

# Save as Hive table
structured_df.write.mode("overwrite") \
    .saveAsTable("logsdb.processed_logs_structured")

spark.stop()