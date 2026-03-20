#!/bin/bash
set -e

echo "Waiting for Kafka brokers to be ready..."
sleep 10

# Create commit-log topic on primary with 60s retention for testing
echo "Creating commit-log topic on primary..."
/opt/kafka/bin/kafka-topics.sh --create --topic commit-log \
  --partitions 1 --replication-factor 1 \
  --config log.retention.ms=60000 \
  --bootstrap-server primary-kafka:9092

# Create primary.commit-log topic on standby (MM2 can auto-create, but being explicit is safer)
echo "Creating primary.commit-log topic on standby..."
/opt/kafka/bin/kafka-topics.sh --create --topic primary.commit-log \
  --partitions 1 --replication-factor 1 \
  --bootstrap-server standby-kafka:9093

echo "Topics created successfully."
