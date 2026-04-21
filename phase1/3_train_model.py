from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import GBTRegressor

spark = SparkSession.builder \
    .appName("M5_Model_Training") \
    .getOrCreate()

# Load feature data
df = spark.read.parquet("hdfs:///project/m5_features")

print(">>> Features loaded")

# Assemble features
assembler = VectorAssembler(
    inputCols=["lag_7", "lag_14", "rolling_avg_7"],
    outputCol="features"
)

data = assembler.transform(df)

print(">>> Features vectorized")

# Train-test split
train, test = data.randomSplit([0.8, 0.2])

print(">>> Data split done")

# Train GBT model
model = GBTRegressor(
    featuresCol="features",
    labelCol="sales",
    maxIter=20
)

model = model.fit(train)

print(">>> Model trained")

# Save model
model.write().overwrite().save("hdfs:///project/m5_model")

print(">>> Model saved to HDFS")

spark.stop()
