#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMPOSE_FILE="$SCRIPT_DIR/docker-compose.yml"
MM2_CONTAINER="mm2-replicator"
PRIMARY_CONTAINER="mm2-primary"
STANDBY_CONTAINER="mm2-standby"
SOURCE_TOPIC="commit-log"
TARGET_TOPIC="primary.commit-log"

compose() {
  docker compose -f "$COMPOSE_FILE" "$@"
}

reset_environment() {
  echo
  echo "==> Resetting challenge environment"
  compose down -v --remove-orphans >/dev/null 2>&1 || true
  compose up -d --build
  wait_for_container "$MM2_CONTAINER" 120
  sleep 5
}

wait_for_container() {
  local container="$1"
  local timeout="$2"
  local deadline=$((SECONDS + timeout))
  while (( SECONDS < deadline )); do
    if [[ "$(docker inspect -f '{{.State.Running}}' "$container" 2>/dev/null || true)" == "true" ]]; then
      return 0
    fi
    sleep 2
  done
  echo "Timed out waiting for container $container" >&2
  return 1
}

produce() {
  local count="$1"
  python3 "$SCRIPT_DIR/producer.py" --count "$count"
}

end_offset() {
  local container="$1"
  local bootstrap="$2"
  local topic="$3"
  docker exec "$container" /opt/kafka/bin/kafka-get-offsets.sh \
    --bootstrap-server "$bootstrap" --topic "$topic" 2>/dev/null \
    | awk -F: '{sum += $3} END {print sum + 0}'
}

beginning_offset() {
  docker exec "$PRIMARY_CONTAINER" /opt/kafka/bin/kafka-get-offsets.sh \
    --bootstrap-server primary:19092 --topic "$SOURCE_TOPIC" --time -2 2>/dev/null \
    | awk -F: 'NR == 1 {print $3 + 0}'
}

wait_for_target_offset() {
  local expected="$1"
  local timeout="${2:-60}"
  local deadline=$((SECONDS + timeout))
  while (( SECONDS < deadline )); do
    local current
    current="$(end_offset "$STANDBY_CONTAINER" standby:19092 "$TARGET_TOPIC")"
    if (( current >= expected )); then
      echo "Target end offset is $current (expected >= $expected)."
      return 0
    fi
    sleep 2
  done
  echo "Timed out waiting for $TARGET_TOPIC to reach offset $expected" >&2
  compose logs --no-color mm2 | tail -n 100 >&2 || true
  return 1
}

wait_for_source_beginning_above() {
  local minimum="$1"
  local timeout="${2:-90}"
  local deadline=$((SECONDS + timeout))
  while (( SECONDS < deadline )); do
    local current
    current="$(beginning_offset)"
    if (( current > minimum )); then
      echo "Source beginning offset advanced to $current (required > $minimum)."
      return 0
    fi
    sleep 2
  done
  echo "Retention did not advance the source beginning offset above $minimum in time." >&2
  docker logs "$PRIMARY_CONTAINER" 2>&1 | tail -n 100 >&2 || true
  return 1
}

wait_for_mm2_log() {
  local pattern="$1"
  local timeout="${2:-45}"
  local deadline=$((SECONDS + timeout))
  while (( SECONDS < deadline )); do
    if docker logs "$MM2_CONTAINER" 2>&1 | grep -Fq "$pattern"; then
      docker logs "$MM2_CONTAINER" 2>&1 | grep -F "$pattern" | tail -n 3
      return 0
    fi
    sleep 2
  done
  echo "Did not find MM2 log marker: $pattern" >&2
  docker logs "$MM2_CONTAINER" 2>&1 | tail -n 100 >&2 || true
  return 1
}

scenario_normal_replication() {
  echo
  echo "================ Scenario 1: normal replication ================"
  reset_environment
  produce 10
  wait_for_target_offset 10 60
  echo "PASS: 10 events replicated from commit-log to primary.commit-log."
}

scenario_retention_data_loss() {
  echo
  echo "================ Scenario 2: retention / data loss ================"
  reset_environment
  produce 5
  wait_for_target_offset 5 60

  echo "Pausing MirrorMaker 2 and producing records that it cannot consume..."
  docker pause "$MM2_CONTAINER" >/dev/null
  produce 5
  sleep 2
  # Force a new segment so the preceding unreplicated segment can age out deterministically.
  produce 1

  echo "Waiting for the 60-second retention window to expire and log start to advance..."
  wait_for_source_beginning_above 5 100

  docker unpause "$MM2_CONTAINER" >/dev/null
  wait_for_mm2_log "Source data loss detected" 45
  echo "PASS: retention-driven offset loss was detected and the MM2 task failed fast."
}

scenario_topic_reset_recovery() {
  echo
  echo "================ Scenario 3: topic reset recovery ================"
  reset_environment
  produce 5
  wait_for_target_offset 5 60

  echo "Deleting and recreating the source topic while MM2 remains running..."
  docker exec "$PRIMARY_CONTAINER" /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server primary:19092 --delete --topic "$SOURCE_TOPIC"

  local deadline=$((SECONDS + 30))
  while (( SECONDS < deadline )); do
    if ! docker exec "$PRIMARY_CONTAINER" /opt/kafka/bin/kafka-topics.sh \
      --bootstrap-server primary:19092 --describe --topic "$SOURCE_TOPIC" >/dev/null 2>&1; then
      break
    fi
    sleep 1
  done

  docker exec "$PRIMARY_CONTAINER" /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server primary:19092 --create --topic "$SOURCE_TOPIC" \
    --partitions 1 --replication-factor 1 \
    --config retention.ms=60000 --config segment.ms=1000

  produce 3
  wait_for_mm2_log "SOURCE_TOPIC_RESET_RECOVERED" 45
  wait_for_target_offset 8 60
  echo "PASS: topic reset was detected, MM2 recovered automatically, and replication resumed without an MM2 restart."
}

cleanup() {
  echo
  echo "Challenge complete. Containers are left running for log inspection."
  echo "Run: docker compose -f '$COMPOSE_FILE' down -v"
}

trap cleanup EXIT

command -v docker >/dev/null || { echo "docker is required" >&2; exit 1; }
command -v python3 >/dev/null || { echo "python3 is required" >&2; exit 1; }
docker compose version >/dev/null

scenario_normal_replication
scenario_retention_data_loss
scenario_topic_reset_recovery

echo
echo "All three scenarios completed successfully."
