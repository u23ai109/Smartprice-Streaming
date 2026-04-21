from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("M5_Long_Format") \
    .getOrCreate()

# Load dataset from HDFS
df = spark.read.csv(
    "hdfs:///project/m5/sales_train_validation.csv",
    header=True,
    inferSchema=True
)

print(">>> Dataset Loaded")

# Extract day columns
day_cols = [c for c in df.columns if c.startswith("d_")]

print(f">>> Total day columns: {len(day_cols)}")

# Convert wide → long
stack_expr = "stack({0}, {1}) as (day, sales)".format(
    len(day_cols),
    ", ".join([f"'{c}', {c}" for c in day_cols])
)

long_df = df.selectExpr(
    "item_id", "dept_id", "cat_id",
    "store_id", "state_id",
    stack_expr
)

print(">>> Conversion done")

# Save as Parquet (optimized)
long_df.write.mode("overwrite") \
    .partitionBy("state_id") \
    .parquet("hdfs:///project/m5_long")

print(">>> Saved to HDFS: /project/m5_long")

spark.stop()
