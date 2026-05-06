# job.py
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Pipeline").enableHiveSupport().getOrCreate()

##df = spark.read.text("hdfs:///data/logs/access.log")
df = spark.read.text("hdfs://localhost:9000/data/logs/access.log")
df.show(truncate=False)
df.write.mode("overwrite").saveAsTable("logsdb.processed_logs")

spark.stop()