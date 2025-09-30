# processor/spark_app.py
import os
import json
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, udf
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
import requests
from dotenv import load_dotenv

load_dotenv()

KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'kafka:9092')
INPUT_TOPIC = os.getenv('INPUT_TOPIC', 'tweets')
RESULTS_TOPIC = os.getenv('RESULTS_TOPIC', 'sentiment_results')
INFERENCE_URL = os.getenv('INFERENCE_URL', 'http://inference:8000/predict_batch')

spark = SparkSession.builder \
    .appName("SentimentProcessor") \
    .getOrCreate()

# Schema of the incoming JSON
schema = StructType([
    StructField("id", StringType(), True),
    StructField("text", StringType(), True),
    StructField("lang", StringType(), True),
    StructField("ts", DoubleType(), True)
])

# Read from Kafka topic
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", INPUT_TOPIC) \
    .option("startingOffsets", "latest") \
    .load()

# Convert value (binary) to string and parse JSON
raw = df.selectExpr("CAST(value AS STRING) as json_str")
parsed = raw.select(from_json(col("json_str"), schema).alias("data")).select("data.*")

# Collect batches of messages using foreachBatch
def process_batch(batch_df, batch_id):
    rows = batch_df.select("id", "text", "ts").collect()
    if not rows:
        return
    # prepare batched request
    texts = [r['text'] for r in rows]
    ids = [r['id'] for r in rows]
    resp = requests.post(INFERENCE_URL, json={"texts": texts}, timeout=30)
    if resp.status_code != 200:
        print("Inference error", resp.status_code, resp.text)
        return
    results = resp.json().get("results", [])
    # publish results back to Kafka
    from kafka import KafkaProducer
    producer = KafkaProducer(bootstrap_servers=KAFKA_BROKER,
                             value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    for idx, res in enumerate(results):
        out = {
            "id": ids[idx],
            "text": texts[idx],
            "label": res.get("label"),
            "score": res.get("score"),
            "proc_ts": time.time()
        }
        producer.send(RESULTS_TOPIC, out)
    producer.flush()
    print(f"Processed batch {batch_id}: {len(rows)} messages")

# Start streaming with foreachBatch
query = parsed.writeStream \
    .foreachBatch(process_batch) \
    .option("checkpointLocation", "/tmp/spark-checkpoint") \
    .start()

query.awaitTermination()
