#!/bin/bash
set -e

KAFKA=kafka:9092
INPUT_TOPIC=tweets
RESULTS_TOPIC=sentiment_results

# retry until kafka is up
for i in {1..30}; do
  kafka-topics.sh --bootstrap-server $KAFKA --list && break || sleep 1
done

kafka-topics.sh --bootstrap-server $KAFKA --create --topic $INPUT_TOPIC --partitions 3 --replication-factor 1 || true
kafka-topics.sh --bootstrap-server $KAFKA --create --topic $RESULTS_TOPIC --partitions 3 --replication-factor 1 || true

echo "Topics ensured: $INPUT_TOPIC, $RESULTS_TOPIC"
