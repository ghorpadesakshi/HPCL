import streamlit as st
from kafka import KafkaConsumer
from textblob import TextBlob
import json
import threading
import time
import pandas as pd
import matplotlib.pyplot as plt

# Kafka topic and broker
TOPIC = 'tweets'
BROKER = 'localhost:9092'

st.set_page_config(page_title="Real-Time Twitter Sentiment Dashboard", layout="wide")
st.title("📊 Real-Time Twitter Sentiment Dashboard")

# Shared messages list
messages = []

# Kafka consumer
def consume_tweets():
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=[BROKER],
        auto_offset_reset='earliest',
        group_id='sentiment-dashboard'
    )
    for msg in consumer:
        data = json.loads(msg.value.decode('utf-8'))
        text = data.get("text", "")
        sentiment_score = TextBlob(text).sentiment.polarity
        if sentiment_score > 0:
            label = 'Positive'
        elif sentiment_score < 0:
            label = 'Negative'
        else:
            label = 'Neutral'
        messages.append({
            "text": text,
            "sentiment": label,
            "score": sentiment_score,
            "time": time.time()
        })

# Start Kafka consumer in background thread
threading.Thread(target=consume_tweets, daemon=True).start()

# Streamlit UI refresh loop
placeholder = st.empty()
refresh_rate = 2  # seconds

while True:
    if messages:
        with placeholder.container():
            df = pd.DataFrame(messages)

            # --- KPI Cards Row ---
            col1, col2, col3 = st.columns(3)
            col1.metric("😊 Positive", df[df['sentiment']=="Positive"].shape[0])
            col2.metric("😐 Neutral", df[df['sentiment']=="Neutral"].shape[0])
            col3.metric("☹️ Negative", df[df['sentiment']=="Negative"].shape[0])

            # --- Charts Row: each in its own card ---
            chart_col1, chart_col2, chart_col3 = st.columns(3)

            # Gender-like Pie Chart (example: sentiment share)
            with chart_col1:
                st.subheader("Sentiment Share")
                sentiment_counts = df['sentiment'].value_counts()
                fig1, ax1 = plt.subplots()
                ax1.pie(sentiment_counts, labels=sentiment_counts.index,
                        autopct='%1.1f%%', startangle=90)
                ax1.axis("equal")
                st.pyplot(fig1)
                plt.close(fig1)

            # Geography-like Horizontal Bar (example: counts)
            with chart_col2:
                st.subheader("Sentiment Distribution")
                st.bar_chart(sentiment_counts)

            # Age-like Stacked Bar (example: trend over last 50 tweets)
            with chart_col3:
                st.subheader("Recent Sentiments")
                recent_df = df.tail(50)
                trend_counts = recent_df['sentiment'].value_counts()
                st.bar_chart(trend_counts)

            # --- Latest Tweets Table Below ---
            st.subheader("Latest Tweets")
            st.table(df[['text', 'sentiment', 'score']].tail(10).iloc[::-1])

    time.sleep(refresh_rate)
