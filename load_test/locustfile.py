# load_test/locustfile.py
from locust import TaskSet, task, between, HttpUser
import os, json, time, random
from kafka import KafkaProducer
from dotenv import load_dotenv

load_dotenv()
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'localhost:9092')
TOPIC = os.getenv('INPUT_TOPIC', 'tweets')

producer = KafkaProducer(bootstrap_servers=KAFKA_BROKER,
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

SAMPLES = ["I love this!", "I hate this!", "Not bad", "Awesome", "Terrible", "Okay"]

class ProducerTasks(TaskSet):
    @task
    def send_kafka(self):
        payload = {"id": f"lt-{int(time.time()*1000)}", "text": random.choice(SAMPLES), "ts": time.time()}
        producer.send(TOPIC, payload)
        producer.flush()

class KafkaUser(HttpUser):
    tasks = [ProducerTasks]
    wait_time = between(0.01, 0.5)
