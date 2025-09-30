# HPCL - Parallel Sentiment Analysis Dashboard

Real-time Twitter Sentiment Dashboard using Kafka, Zookeeper, NLP, and Streamlit. Tweets are streamed, analyzed, and classified as Positive, Negative, or Neutral. Results are displayed on an interactive dashboard with dynamic charts, enabling scalable and fault-tolerant sentiment tracking.

## Quick Start (Docker Compose)

1. Build & run:
   ```bash
   docker-compose up --build
   Check running containers:
   ```

docker ps

Start the Twitter producer (streams tweets into Kafka):

python twitter_kafka_producer.py

Start the parallel Kafka consumer (processes tweets in real time):

python kafka_parallel_consumer.py

Launch the Streamlit dashboard (view real-time sentiment analysis):

python -m streamlit run dashboard.py

```

```
