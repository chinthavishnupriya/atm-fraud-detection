#!/usr/bin/env bash
set -e

KAFKA_HOME="${KAFKA_HOME:-/opt/kafka}"
BOOTSTRAP_SERVER="${BOOTSTRAP_SERVER:-localhost:9092}"
TOPIC="bank-transactions"
PARTITIONS=3
REPLICATION_FACTOR=1

if ! "$KAFKA_HOME/bin/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP_SERVER" --list | grep -qx "$TOPIC"; then
  echo "Creating Kafka topic: $TOPIC"
  "$KAFKA_HOME/bin/kafka-topics.sh" \
    --bootstrap-server "$BOOTSTRAP_SERVER" \
    --create \
    --topic "$TOPIC" \
    --partitions "$PARTITIONS" \
    --replication-factor "$REPLICATION_FACTOR"
else
  echo "Kafka topic $TOPIC already exists."
fi

echo
echo "Kafka topic configuration:"
"$KAFKA_HOME/bin/kafka-topics.sh" \
  --bootstrap-server "$BOOTSTRAP_SERVER" \
  --describe \
  --topic "$TOPIC"
