import json
import time
import random
from kafka import KafkaProducer
import tweepy

# ======================
# 🔹 CONFIG
# ======================
TOPIC = "tweets"
BROKER = "localhost:9092"

# Twitter API credentials (replace with your keys)
BEARER_TOKEN = "AAAAAAAAAAAAAAAAAAAAADH64QEAAAAAIuhbXx8vZshISHUou2W2PboeEq0%3DFAOcAhoKjQcYGI2CRUyts8d2yuq5tcUfF6TiLuMv1LfhoIJhW3"

# Sample words for synthetic tweets
positive_words = ["love", "happy", "great", "awesome", "fantastic", "amazing", "superb", "excited"]
negative_words = ["hate", "angry", "bad", "terrible", "worst", "sad", "upset", "awful"]
neutral_words = ["today", "weather", "update", "meeting", "news", "event", "random", "info"]

# Kafka producer
producer = KafkaProducer(
    bootstrap_servers=[BROKER],
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# ======================
# 🔹 Synthetic Tweet Generator
# ======================
def generate_synthetic_tweet():
    sentiment = random.choice(["Positive", "Negative", "Neutral"])
    if sentiment == "Positive":
        word = random.choice(positive_words)
        text = f"I really {word} this new update!"
    elif sentiment == "Negative":
        word = random.choice(negative_words)
        text = f"This feels so {word}, not impressed at all."
    else:
        word = random.choice(neutral_words)
        text = f"Just another {word} happening right now."
    return {"text": text, "synthetic": True}


# ======================
# 🔹 Twitter API Fetcher
# ======================
def fetch_real_tweets(client, query="AI -is:retweet lang:en", max_results=10):
    try:
        tweets = client.search_recent_tweets(query=query, max_results=max_results)
        if not tweets.data:
            return []
        return [{"text": t.text, "synthetic": False} for t in tweets.data]
    except Exception as e:
        print(f"⚠️ Twitter API error: {e}")
        return None


# ======================
# 🔹 Main Producer Loop
# ======================
def main():
    client = None
    if BEARER_TOKEN != "YOUR_TWITTER_BEARER_TOKEN":
        try:
            client = tweepy.Client(bearer_token=BEARER_TOKEN)
            print("✅ Connected to Twitter API")
        except Exception as e:
            print(f"⚠️ Could not connect to Twitter API, switching to synthetic. Error: {e}")

    try:
        while True:
            tweets = []
            if client:  # Try real tweets first
                tweets = fetch_real_tweets(client)

            if tweets is None or len(tweets) == 0:  # Fallback if no real tweets
                tweet = generate_synthetic_tweet()
                tweets = [tweet]
                print(f"➡️ Sent synthetic tweet: {tweet['text']}")
            else:
                for t in tweets:
                    print(f"🐦 Sent real tweet: {t['text'][:50]}...")

            # Send to Kafka
            for t in tweets:
                producer.send(TOPIC, t)

            time.sleep(5)  # wait before next fetch

    except KeyboardInterrupt:
        print("\n🛑 Stopped by user. Closing Kafka producer...")
    finally:
        producer.close()


if __name__ == "__main__":
    main()
