from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, from_unixtime
from pyspark.sql.types import StructType, StringType, IntegerType, DoubleType

from pyspark.ml.regression import GBTRegressionModel
from pyspark.ml.feature import VectorAssembler

# --------------------------------------------------
# Spark Session
# --------------------------------------------------
spark = SparkSession.builder \
    .appName("SmartPrice_Final_Stable") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# --------------------------------------------------
# Load ML Model (GBT)
# --------------------------------------------------
model = GBTRegressionModel.load("hdfs:///project/m5_model")

# --------------------------------------------------
# Schema
# --------------------------------------------------
schema = StructType() \
    .add("item_id", StringType()) \
    .add("price", IntegerType()) \
    .add("quantity", IntegerType()) \
    .add("timestamp", DoubleType())

# --------------------------------------------------
# Kafka Source
# --------------------------------------------------
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "sales_stream") \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load()

# --------------------------------------------------
# Parse JSON
# --------------------------------------------------
parsed_df = df.selectExpr("CAST(value AS STRING) as json") \
    .select(from_json(col("json"), schema).alias("data")) \
    .select("data.*")

# --------------------------------------------------
# Timestamp + Watermark
# --------------------------------------------------
final_df = parsed_df \
    .withColumn("event_time", from_unixtime(col("timestamp")).cast("timestamp")) \
    .withWatermark("event_time", "20 seconds")

# --------------------------------------------------
# Window Aggregation
# --------------------------------------------------
agg_df = final_df.groupBy(
    window(col("event_time"), "10 seconds"),
    col("item_id")
).sum("quantity") \
 .withColumnRenamed("sum(quantity)", "total_quantity")

# --------------------------------------------------
# Feature Engineering (SAFE VERSION)
# --------------------------------------------------
feature_df = agg_df \
    .withColumn("lag_7", col("total_quantity")) \
    .withColumn("lag_14", col("total_quantity")) \
    .withColumn("rolling_avg_7", col("total_quantity"))

# --------------------------------------------------
# Assemble Features (REQUIRED for your model)
# --------------------------------------------------
assembler = VectorAssembler(
    inputCols=["lag_7", "lag_14", "rolling_avg_7"],
    outputCol="features"
)

model_input = assembler.transform(feature_df)

# --------------------------------------------------
# Apply ML Model
# --------------------------------------------------
prediction_df = model.transform(model_input)

# --------------------------------------------------
# Final Output
# --------------------------------------------------
output_df = prediction_df.select(
    "item_id",
    "window",
    "total_quantity",
    "lag_7",
    "lag_14",
    "rolling_avg_7",
    col("prediction").alias("predicted_demand")
)

# --------------------------------------------------
# Write to Console
# --------------------------------------------------
console_query = output_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .start()

# --------------------------------------------------
# Write to HDFS
# --------------------------------------------------
file_query = output_df.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", "/project/streaming/predictions") \
    .option("checkpointLocation", "/project/streaming/checkpoints_final") \
    .start()

# --------------------------------------------------
# Start Streaming
# --------------------------------------------------
spark.streams.awaitAnyTermination()
