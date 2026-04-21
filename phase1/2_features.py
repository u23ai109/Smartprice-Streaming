from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import lag, avg

spark = SparkSession.builder \
    .appName("M5_Feature_Engineering") \
    .getOrCreate()

# Load long data
df = spark.read.parquet("hdfs:///project/m5_long")

print(">>> Loaded long format data")

# Window definition
window = Window.partitionBy("item_id", "store_id").orderBy("day")

# Lag features
df = df.withColumn("lag_7", lag("sales", 7).over(window))
df = df.withColumn("lag_14", lag("sales", 14).over(window))

print(">>> Lag features created")

# Rolling average
df = df.withColumn(
    "rolling_avg_7",
    avg("sales").over(window.rowsBetween(-7, 0))
)

print(">>> Rolling average created")

# Drop nulls
df = df.dropna()

print(">>> Nulls removed")

# Save features
df.write.mode("overwrite").parquet("hdfs:///project/m5_features")

print(">>> Features saved to HDFS")

spark.stop()
