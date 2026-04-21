from kafka import KafkaProducer
import json
import time
import random

# --------------------------------------------------
# Kafka Producer Setup
# --------------------------------------------------
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

topic = "sales_stream"

# --------------------------------------------------
# Sample data pool
# --------------------------------------------------
items = ["ITEM_1", "ITEM_2", "ITEM_3", "ITEM_4"]

# --------------------------------------------------
# Streaming loop
# --------------------------------------------------
while True:
    data = {
        "item_id": random.choice(items),
        "price": random.randint(50, 500),
        "quantity": random.randint(1, 5),

        # IMPORTANT: epoch timestamp (seconds)
        "timestamp": time.time()
    }

    # Send to Kafka
    producer.send(topic, value=data)

    print(f"Sent: {data}")

    # Control speed (important for testing)
    time.sleep(1)
