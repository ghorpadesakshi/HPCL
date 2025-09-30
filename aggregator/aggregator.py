# aggregator/aggregator.py
import os
import time
import threading
import json
from collections import deque, Counter
from fastapi import FastAPI
from kafka import KafkaConsumer
from dotenv import load_dotenv

load_dotenv()
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'kafka:9092')
RESULTS_TOPIC = os.getenv('RESULTS_TOPIC', 'sentiment_results')

app = FastAPI(title="Aggregator")

# Rolling store for last N results
ROLLING_WINDOW = 1000
lock = threading.Lock()
recent = deque(maxlen=ROLLING_WINDOW)
label_counts = Counter()

def consume_loop():
    consumer = KafkaConsumer(
        RESULTS_TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='aggregator-group'
    )
    print("Aggregator consumer started")
    for msg in consumer:
        data = msg.value
        label = data.get('label', 'UNK')
        score = data.get('score', 0.0)
        ts = data.get('proc_ts', time.time())
        with lock:
            recent.append({"label": label, "score": score, "ts": ts})
            label_counts[label] += 1
            # trim counter if deque removed elements:
            if len(recent) < ROLLING_WINDOW:
                pass

# start consumer thread
t = threading.Thread(target=consume_loop, daemon=True)
t.start()

@app.get("/metrics")
def metrics():
    with lock:
        counts = dict(label_counts)
        avg_score = sum(x['score'] for x in recent)/len(recent) if recent else 0.0
        total = len(recent)
    return {"total": total, "counts": counts, "avg_score": avg_score, "window": ROLLING_WINDOW}
