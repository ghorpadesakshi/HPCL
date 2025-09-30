from kafka import KafkaConsumer
from textblob import TextBlob
from concurrent.futures import ThreadPoolExecutor
import threading

TOPIC = 'test-topic'
BROKER = 'localhost:9092'
NUM_CONSUMERS = 3  # number of parallel consumers
LOG_FILE = 'sentiment_log.txt'
lock = threading.Lock()  # to avoid concurrent write issues

def process_message(message):
    text = message.value.decode('utf-8')
    sentiment = TextBlob(text).sentiment.polarity
    if sentiment > 0:
        label = 'Positive'
    elif sentiment < 0:
        label = 'Negative'
    else:
        label = 'Neutral'
    
    output = f"Message: {text} | Sentiment: {label}"
    print(output)
    
    # Write to log file safely
    with lock:
        with open(LOG_FILE, 'a', encoding='utf-8') as f:
            f.write(output + '\n')

def consume():
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=[BROKER],
        auto_offset_reset='earliest',
        group_id='sentiment-group'
    )
    for message in consumer:
        process_message(message)

if __name__ == "__main__":
    print(f"Starting {NUM_CONSUMERS} parallel consumers...")
    with ThreadPoolExecutor(max_workers=NUM_CONSUMERS) as executor:
        for _ in range(NUM_CONSUMERS):
            executor.submit(consume)
