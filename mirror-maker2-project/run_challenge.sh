#!/bin/bash

set -e

LOG_FILE="./logs/mm2_test.log"
mkdir -p ./logs
rm -f "$LOG_FILE"

log() {
  echo -e "$1" | tee -a "$LOG_FILE"
}

capture_logs() {
  log "\n📡 [EVENT] Capturing MM2 logs (last 20s)..."
  docker logs mm2 --since 20s >> "$LOG_FILE" 2>&1
}

print_result() {
  if [ "$1" = "PASS" ]; then
    log "✅ RESULT: $2 PASSED"
  else
    log "❌ RESULT: $2 FAILED"
    tail -n 40 "$LOG_FILE"
  fi
}

log "=== 🚀 MM2 Test Script (Enhanced Validation) ==="

# ----------------------------
# Detect docker network
# ----------------------------
NETWORK=$(docker inspect primary-kafka \
  --format='{{range $k, $v := .NetworkSettings.Networks}}{{$k}}{{end}}')

log "🌐 Using Docker network: $NETWORK"

# ----------------------------
# Wait for Kafka
# ----------------------------
log "⏳ Waiting for primary Kafka..."
until docker exec primary-kafka sh -c "/opt/kafka/bin/kafka-topics.sh --bootstrap-server primary-kafka:9092 --list" >/dev/null 2>&1; do sleep 3; done
log "✅ Primary Kafka ready"

log "⏳ Waiting for standby Kafka..."
until docker exec standby-kafka sh -c "/opt/kafka/bin/kafka-topics.sh --bootstrap-server standby-kafka:9092 --list" >/dev/null 2>&1; do sleep 3; done
log "✅ Standby Kafka ready"

log "⏳ Waiting for MM2..."
until docker logs mm2 2>&1 | grep -q "Herder started"; do sleep 3; done
log "✅ MM2 running"

# ----------------------------
# Create topic
# ----------------------------
log "📌 Creating topic: commit-log"
docker exec primary-kafka sh -c "/opt/kafka/bin/kafka-topics.sh \
--bootstrap-server primary-kafka:9092 \
--create --if-not-exists \
--topic commit-log \
--partitions 1 \
--replication-factor 1" >> "$LOG_FILE" 2>&1

sleep 10

# ----------------------------
# Producer
# ----------------------------
run_producer() {
  COUNT=$1
  log "📤 Producing $COUNT messages..."
  docker run --rm --network $NETWORK \
    -e BOOTSTRAP_SERVERS=primary-kafka:9092 \
    commit-log-producer:latest \
    --count $COUNT >> "$LOG_FILE" 2>&1
}

# =========================================================
# Scenario 1
# =========================================================
log "\n==============================="
log "✅ Scenario 1: Normal Replication"
log "==============================="

run_producer 1000

until docker exec standby-kafka sh -c \
"/opt/kafka/bin/kafka-topics.sh --bootstrap-server standby-kafka:9092 --list | grep primary.commit-log" >/dev/null 2>&1
do sleep 2; done

COUNT=$(docker exec standby-kafka sh -c "/opt/kafka/bin/kafka-console-consumer.sh \
--bootstrap-server standby-kafka:9092 \
--topic primary.commit-log \
--from-beginning \
--timeout-ms 5000 2>/dev/null | wc -l")

log "🔍 Replicated records: $COUNT"

if [ "$COUNT" -ge 1000 ]; then
  print_result "PASS" "TEST 1"
else
  print_result "FAIL" "TEST 1"
fi

# =========================================================
# Scenario 2
# =========================================================
log "\n==============================="
log "🔥 Scenario 2: Log Truncation"
log "==============================="

log "⏸️ [EVENT] Pausing MM2..."
docker pause mm2

log "⚙️ Applying retention (5s)..."
docker exec primary-kafka sh -c "/opt/kafka/bin/kafka-configs.sh \
--bootstrap-server primary-kafka:9092 \
--entity-type topics \
--entity-name commit-log \
--alter --add-config retention.ms=5000,segment.ms=2000" >> "$LOG_FILE" 2>&1

run_producer 1000
sleep 5
run_producer 1000

log "⏳ Waiting for truncation..."
sleep 30

log "▶️ [EVENT] Resuming MM2..." 
docker unpause mm2
sleep 10
capture_logs


# 🔥 Detection
if grep -q "\[TRUNCATION DETECTED - OFFSET_OUT_OF_RANGE\]" "$LOG_FILE"; then
  log "🔎 Truncation log detected"
  Print_result "PASS" "TEST 2 - Detection"
else
  print_result "FAIL" "TEST 2 - Detection"
fi

# 🔥 Fail-fast validation
if grep -q "\[FAIL-FAST\]" "$LOG_FILE"; then
  log "🔎 Fail-fast log detected"
  FAILFAST="PASS"
  print_result "PASS" "TEST 2 - Fail-Fast Detection"
else
  FAILFAST="FAIL"
  log "🔎 No fail-fast log detected"
  print_result "FAIL" "TEST 2 - Fail-Fast Detection"
fi


# =========================================================
# Scenario 3
# =========================================================
log "\n==============================="
log "🔥 Scenario 3: Topic Reset"
log "==============================="

log "⏸️ [EVENT] Pausing MM2..."
docker pause mm2

run_producer 300
sleep 5

log "🗑️ Deleting topic..."
docker exec primary-kafka sh -c "/opt/kafka/bin/kafka-topics.sh \
--bootstrap-server primary-kafka:9092 \
--delete --topic commit-log" >> "$LOG_FILE" 2>&1

sleep 15

log "♻️ Recreating topic..."
docker exec primary-kafka sh -c "/opt/kafka/bin/kafka-topics.sh \
--bootstrap-server primary-kafka:9092 \
--create \
--topic commit-log \
--partitions 1 \
--replication-factor 1" >> "$LOG_FILE" 2>&1

sleep 10

log "▶️ [EVENT] Resuming MM2..."
docker unpause mm2

run_producer 300
sleep 10
capture_logs

# 🔥 Detection
if grep -q "\[TOPIC RESET DETECTED\]" "$LOG_FILE"; then
  log "🔎 Reset log detected"
  RESET="PASS"
else
  RESET="FAIL"
fi

# 🔥 Recovery validation (data should exist)
RECOVERY_COUNT=$(docker exec standby-kafka sh -c "/opt/kafka/bin/kafka-console-consumer.sh \
--bootstrap-server standby-kafka:9092 \
--topic primary.commit-log \
--from-beginning \
--timeout-ms 5000 2>/dev/null | wc -l")

log "🔍 Records after reset: $RECOVERY_COUNT"

if [ "$RECOVERY_COUNT" -gt 0 ]; then
  RECOVERY="PASS"
else
  RECOVERY="FAIL"
fi

if [ "$RESET" = "PASS" ] && [ "$RECOVERY" = "PASS" ]; then
  print_result "PASS" "TEST 3"
else
  print_result "FAIL" "TEST 3"
fi

log "\n🎯 ALL TESTS COMPLETED"







