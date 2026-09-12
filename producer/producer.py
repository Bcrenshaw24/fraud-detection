import json 
import os 
import random 
import time 
from datetime import datetime, timezone 
from confluent_kafka import Producer 
from faker import Faker 

fake = Faker()

KAFKA_SERVER = os.getenv("KAFKA_SERVER", "kafka:29092")
TOPIC_NAME = "financial_transactions"

producer_config = {"bootstrap.servers": KAFKA_SERVER}
producer = Producer(producer_config)

print(f"Connecting to Kafka at {KAFKA_SERVER}...")

USER_POOL = [f"usr_{i:04d}" for i in range(1, 101)]

def generate_transaction(): 
    return { 
        "transaction_id": f"tx_{fake.uuid4()[:8]}", 
        "user_id": random.choice(USER_POOL), 
        "amount": round(random.uniform(5.0, 1500.0), 2),
        "currency": "USD",
        "merchant_category": random.choice(["retail", "grocery", "electronics", "travel", "entertainment"]),
        "latitude": float(fake.latitude()),
        "longitude": float(fake.longitude()),
        "timestamp": datetime.now(timezone.utc).isoformat()
    }
def delivery_report(err, msg): 
    if err is not None: 
        print(f"Delivery failed for record {msg.key()}: {err}")

if __name__ == "__main__": 
    time.sleep(10)
    print("Starting transaction producer stream...")

    while True: 
        tx = generate_transaction()
        producer.produce( 
            topic=TOPIC_NAME, 
            key=tx["user_id"], 
            value=json.dumps(tx), 
            callback=delivery_report
        )
        producer.poll(0)
        time.sleep(0.2) 

