# job.py
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Pipeline").enableHiveSupport().getOrCreate()

df = spark.read.text("hdfs:///data/logs/access.log")
df.write.mode("overwrite").saveAsTable("logsdb.processed_logs")

spark.stop()