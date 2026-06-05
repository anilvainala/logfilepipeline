from pyspark.sql import SparkSession

# Create Spark Session with Hive support
spark = SparkSession.builder \
    .appName("SalesPipeline") \
    .enableHiveSupport() \
    .getOrCreate()

# Read CSV from HDFS
df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("hdfs://localhost:9000/data/sales/5M_Sales_Records.csv")

# Check schema
df.printSchema()

# Create database if not exists
spark.sql("CREATE DATABASE IF NOT EXISTS salesdb")

# Write into Hive table
df.write \
    .mode("overwrite") \
    .saveAsTable("salesdb.sales_5m")

print("Successfully loaded data into Hive table salesdb.sales_5m")

spark.stop()