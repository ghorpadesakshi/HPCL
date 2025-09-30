from kafka import KafkaProducer
import time

TOPIC = "test-topic"
BROKER = "localhost:9092"

producer = KafkaProducer(bootstrap_servers=[BROKER])

messages = [
    "I love coding",
    "This project is really hard",
    "Today is a great day",
    "I hate bugs in my code",
    "Machine learning is awesome",
    "This is neutral"
]

print("Sending messages...")

for msg in messages:
    producer.send(TOPIC, value=msg.encode("utf-8"))
    print(f"Sent: {msg}")
    time.sleep(1)  # small delay so consumers process one by one

producer.flush()
print("All messages sent successfully!")
