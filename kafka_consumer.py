from kafka import KafkaConsumer
from textblob import TextBlob

# Connect to Kafka
consumer = KafkaConsumer(
    'test-topic',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    group_id='sentiment-group'
)

print("Waiting for messages...")

for message in consumer:
    text = message.value.decode('utf-8')
    sentiment = TextBlob(text).sentiment.polarity
    if sentiment > 0:
        label = 'Positive'
    elif sentiment < 0:
        label = 'Negative'
    else:
        label = 'Neutral'
    print(f"Message: {text} | Sentiment: {label}")
