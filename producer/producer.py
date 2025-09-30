# producer/producer.py
import time
import json
import random
import os
from kafka import KafkaProducer
from dotenv import load_dotenv

load_dotenv()

KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'localhost:9092')
TOPIC = os.getenv('INPUT_TOPIC', 'tweets')

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    linger_ms=10
)

SAMPLES = [
    "I love this product! It's amazing.",
    "Worst service ever. Totally disappointed.",
    "I feel okay about this. Nothing special.",
    "Absolutely fantastic — exceeded my expectations!",
    "This is awful, I want a refund.",
    "Meh. Could be better.",
    "Incredible performance, will buy again.",
    "Not worth the price.",
    "Superb quality and fast delivery.",
    "Terrible experience with customer support."
]

def send_message(i):
    payload = {
        "id": f"msg-{int(time.time()*1000)}-{i}",
        "text": random.choice(SAMPLES),
        "lang": "en",
        "ts": time.time()
    }
    producer.send(TOPIC, payload)
    producer.flush()

if __name__ == "__main__":
    i = 0
    print(f"Producing to {KAFKA_BROKER} topic {TOPIC}")
    try:
        while True:
            send_message(i)
            i += 1
            # control rate (messages per second). adjust for load test.
            time.sleep(0.05)  # ~20 msgs/sec
    except KeyboardInterrupt:
        print("Producer stopped.")
