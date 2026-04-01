#!/bin/bash

set -e

LOG_FILE="./logs/mm2_test.log"
mkdir -p ./logs
rm -f "$LOG_FILE"

log() {
echo "$1" | tee -a "$LOG_FILE"
}

capture_logs() {
log "📡 [EVENT] Capturing MM2 logs..."
docker logs mm2 --since 20s >> "$LOG_FILE" 2>&1
}

log "=== 🚀 MM2 Test Script (On-demand Producer) ==="

# ----------------------------

# Detect docker network

# ----------------------------

NETWORK=$(docker inspect primary-kafka \
  --format='{{range $k, $v := .NetworkSettings.Networks}}{{$k}}{{end}}')

log "🌐 [EVENT] Using Docker network: $NETWORK"

# ----------------------------

# Wait for Kafka brokers

# ----------------------------

log "⏳ [EVENT] Waiting for primary Kafka..."

until docker exec primary-kafka sh -c "/opt/kafka/bin/kafka-topics.sh --bootstrap-server primary-kafka:9092 --list" >/dev/null 2>&1; do
sleep 3
done

log "✅ [EVENT] Primary Kafka is ready"

log "⏳ [EVENT] Waiting for standby Kafka..."

until docker exec standby-kafka sh -c "/opt/kafka/bin/kafka-topics.sh --bootstrap-server standby-kafka:9092 --list" >/dev/null 2>&1; do
sleep 3
done

log "✅ [EVENT] Standby Kafka is ready"

# ----------------------------

# Wait for MM2

# ----------------------------

log "⏳ [EVENT] Waiting for MirrorMaker2..."

until docker logs mm2 2>&1 | grep -q "Herder started"; do
sleep 3
done

log "✅ [EVENT] MirrorMaker2 is running"

# ----------------------------

# Create topic

# ----------------------------

log "📌 [EVENT] Creating topic: commit-log"

docker exec primary-kafka sh -c "/opt/kafka/bin/kafka-topics.sh 
--bootstrap-server primary-kafka:9092 
--create --if-not-exists 
--topic commit-log 
--partitions 1 
--replication-factor 1" >> "$LOG_FILE" 2>&1

# IMPORTANT: allow MM2 to detect topic

log "⏳ [EVENT] Waiting for MM2 topic discovery..."
sleep 10

# ----------------------------

# Producer

# ----------------------------

run_producer() {
COUNT=$1
log "📤 [EVENT] Producing $COUNT messages..."

docker run --rm --network $NETWORK 
-e BOOTSTRAP_SERVERS=primary-kafka:9092 
commit-log-producer 
--count $COUNT >> "$LOG_FILE" 2>&1
}

# =========================================================

# Scenario 1: Normal Replication

# =========================================================

log ""
log "==============================="
log "✅ Scenario 1: Normal Replication"
log "==============================="

run_producer 20

log "⏳ [EVENT] Waiting for topic replication in standby..."

until docker exec standby-kafka sh -c 
"/opt/kafka/bin/kafka-topics.sh --bootstrap-server standby-kafka:9092 --list | grep primary.commit-log" >/dev/null 2>&1
do
sleep 2
done

log "📥 [EVENT] Consuming from standby..."

COUNT=$(docker exec standby-kafka sh -c "/opt/kafka/bin/kafka-console-consumer.sh 
--bootstrap-server standby-kafka:9092 
--topic primary.commit-log 
--from-beginning 
--timeout-ms 5000 2>/dev/null | wc -l")

log "🔍 [EVENT] Records replicated: $COUNT"

if [ "$COUNT" -ge 20 ]; then
log "✅ TEST 1 PASSED"
else
log "❌ TEST 1 FAILED"
fi

# =========================================================

# Scenario 2: Log Truncation

# =========================================================

log ""
log "==============================="
log "🔥 Scenario 2: Log Truncation"
log "==============================="

log "⏸️ [EVENT] Pausing MM2..."
docker pause mm2 >> "$LOG_FILE" 2>&1

log "⚙️ [EVENT] Setting aggressive retention (10 sec)..."
docker exec primary-kafka sh -c "/opt/kafka/bin/kafka-configs.sh 
--bootstrap-server primary-kafka:9092 
--entity-type topics 
--entity-name commit-log 
--alter --add-config retention.ms=10000" >> "$LOG_FILE" 2>&1

run_producer 500
sleep 10

run_producer 500

log "⏳ [EVENT] Waiting for truncation..."
sleep 20

log "▶️ [EVENT] Resuming MM2..."
docker unpause mm2 >> "$LOG_FILE" 2>&1

sleep 10

capture_logs

log "📄 [EVENT] Checking truncation detection..."

if grep -q "LOG TRUNCATION DETECTED" "$LOG_FILE"; then
log "✅ TEST 2 PASSED"
else
log "❌ TEST 2 FAILED"
fi

# =========================================================

# Scenario 3: Topic Reset

# =========================================================

log ""
log "==============================="
log "🔥 Scenario 3: Topic Reset"
log "==============================="

log "⏸️ [EVENT] Pausing MM2..."
docker pause mm2 >> "$LOG_FILE" 2>&1

run_producer 500
sleep 5

log "🗑️ [EVENT] Deleting topic..."
docker exec primary-kafka sh -c "/opt/kafka/bin/kafka-topics.sh 
--bootstrap-server primary-kafka:9092 
--delete --topic commit-log" >> "$LOG_FILE" 2>&1

sleep 10

log "♻️ [EVENT] Recreating topic..."
docker exec primary-kafka sh -c "/opt/kafka/bin/kafka-topics.sh 
--bootstrap-server primary-kafka:9092 
--create 
--topic commit-log 
--partitions 1 
--replication-factor 1" >> "$LOG_FILE" 2>&1

sleep 5

log "▶️ [EVENT] Resuming MM2..."
docker unpause mm2 >> "$LOG_FILE" 2>&1

run_producer 500
sleep 10

capture_logs

log "📄 [EVENT] Checking topic reset detection..."

if grep -q "TOPIC RESET DETECTED" "$LOG_FILE"; then
log "✅ TEST 3 PASSED"
else
log "❌ TEST 3 FAILED"
fi

log "📄 [EVENT] Checking recovery..."

if grep -q "Recovery successful" "$LOG_FILE"; then
log "✅ RECOVERY PASSED"
else
log "❌ RECOVERY FAILED"
tail -n 50 "$LOG_FILE"
exit 1
fi

log ""
log "🎯 ALL TESTS COMPLETED"
