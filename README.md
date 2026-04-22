🚀 SmartPrice: Real-Time Demand Prediction using Kafka + Spark + ML

📌 Overview

SmartPrice is a real-time data engineering + machine learning pipeline that predicts product demand dynamically using streaming data.

The system simulates live sales data, processes it using Apache Spark Structured Streaming, applies feature engineering (lag-based), and uses a trained ML model to generate real-time demand predictions.

---

🏗️ Architecture

Kafka → Spark Streaming → Feature Engineering → ML Model → Predictions (Console + HDFS)

---

⚙️ Tech Stack

- Apache Kafka (Data Streaming)
- Apache Spark (Structured Streaming + MLlib)
- Hadoop HDFS (Storage)
- Python (PySpark)
- M5 Forecasting Dataset (Kaggle)

---

📊 Dataset

This project uses the M5 Forecasting Dataset.

🔗 Download from Kaggle:
https://www.kaggle.com/competitions/m5-forecasting-accuracy/data

After downloading:

hdfs dfs -put m5 /project/m5

---

🧠 Model

- Model Used: Gradient Boosted Trees (GBTRegressor)
- Trained on:
  - lag_7
  - lag_14
  - rolling_avg_7

Model is stored in HDFS:

hdfs:///project/m5_model

---

📂 Project Structure

tree project_smartprice/
      │
      ├── phase1/        # Batch training (model training)
      ├── phase2/        # Kafka + Spark streaming
      ├── phase3/        # Feature engineering
      ├── phase4/        # Model integration + predictions
      │
      └── README.md

---

🔄 Workflow

1. Start Kafka

bin/zookeeper-server-start.sh config/zookeeper.properties

bin/kafka-server-start.sh config/server.properties

---

2. Create Topic

bin/kafka-topics.sh --create \
--topic sales_stream \
--bootstrap-server localhost:9092 \
--partitions 1 \
--replication-factor 1

---

3. Run Producer (Simulated Streaming Data)

python3 producer_bot.py

---

4. Run Spark Streaming + Model

spark-submit \
--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
spark_stream.py

---

📈 Output

Console Output:

- item_id
- total_quantity
- lag features
- predicted_demand

HDFS Output:

/project/streaming/predictions/

---

🧩 Features Implemented

- Real-time Kafka streaming
- JSON parsing in Spark
- Window-based aggregation
- Stateful lag feature computation (lag_7, lag_14)
- Rolling average calculation
- ML model integration (GBTRegressor)
- Streaming predictions stored in HDFS

---

⚠️ Notes

- Dataset and model are NOT included in repo due to size
- Ensure Kafka and Spark versions are compatible
- Clear checkpoints before rerunning streaming jobs if needed

---

🧹 Cleanup Commands

hdfs dfs -rm -r /project/streaming/predictions
hdfs dfs -rm -r /project/streaming/checkpoints_stateful

---

🎯 Result

Successfully built an end-to-end real-time ML pipeline that:

- Simulates live data
- Processes streaming data
- Applies feature engineering
- Predicts demand dynamically

---

👨‍💻 Author

Teja Swaroop

Sandeep

Raj Kumar

---

⭐ If you like this project, give it a star!
