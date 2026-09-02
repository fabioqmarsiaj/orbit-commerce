#!/bin/bash
# Creates all orbit-commerce Kafka topics. Runs once via a short-lived
# "kafka-init" container that shares the apache/kafka image and exits after
# topic creation completes (see docker-compose.yml).
set -e

BOOTSTRAP_SERVER="${KAFKA_BOOTSTRAP_SERVER:-broker:19092}"
PARTITIONS="${TOPIC_PARTITIONS:-3}"
REPLICATION_FACTOR="${TOPIC_REPLICATION_FACTOR:-1}"

TOPICS=(
  "order.events"
  "inventory.commands"
  "inventory.events"
  "payment.commands"
  "payment.events"
  "shipping.commands"
  "shipping.events"
  "user-activity.events"
)

echo "Waiting for broker at ${BOOTSTRAP_SERVER}..."
until /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server "${BOOTSTRAP_SERVER}" > /dev/null 2>&1; do
  echo "Broker not ready yet, retrying in 2s..."
  sleep 2
done

for topic in "${TOPICS[@]}"; do
  echo "Creating topic: ${topic} (partitions=${PARTITIONS}, replication-factor=${REPLICATION_FACTOR})"
  /opt/kafka/bin/kafka-topics.sh --bootstrap-server "${BOOTSTRAP_SERVER}" \
    --create --if-not-exists \
    --topic "${topic}" \
    --partitions "${PARTITIONS}" \
    --replication-factor "${REPLICATION_FACTOR}"
done

echo "All orbit-commerce topics created:"
/opt/kafka/bin/kafka-topics.sh --bootstrap-server "${BOOTSTRAP_SERVER}" --list
