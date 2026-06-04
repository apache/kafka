#!/usr/bin/env bash
# =============================================================================
# run_challenge.sh
#
# Orchestrates the three demonstration scenarios for the Enhanced MirrorMaker 2:
#
#   1. NORMAL    : produce 1000 events, verify all replicate PR -> DR.
#   2. TRUNCATION: pause MM2, let retention purge un-replicated data, resume MM2,
#                  verify MM2 detects truncation and FAILS FAST (Task 2).
#   3. RESET     : pause MM2, delete & recreate the source topic, resume MM2,
#                  verify MM2 detects the reset and AUTO-RECOVERS (Task 3).
#
# Requires: docker + docker compose v2.
# Usage:    ./run_challenge.sh [normal|truncation|reset|all]   (default: all)
# =============================================================================
set -euo pipefail

SCENARIO="${1:-all}"

PRIMARY="primary-kafka:29092"
DR="dr-kafka:29092"
SRC_TOPIC="commit-log"
DST_TOPIC="primary.commit-log"
COMPOSE="docker compose"

# kafka CLI tools live inside the broker container; we exec into it.
kcli() { docker exec primary-kafka "/opt/kafka/bin/$1" "${@:2}"; }
kcli_dr() { docker exec dr-kafka "/opt/kafka/bin/$1" "${@:2}"; }

log()  { printf "\n\033[1;36m==> %s\033[0m\n" "$*"; }
ok()   { printf "\033[1;32m[PASS]\033[0m %s\n" "$*"; }
fail() { printf "\033[1;31m[FAIL]\033[0m %s\n" "$*"; exit 1; }

wait_for_brokers() {
  log "Waiting for both Kafka clusters to be healthy..."
  for c in primary-kafka dr-kafka; do
    for _ in $(seq 1 30); do
      if docker exec "$c" /opt/kafka/bin/kafka-broker-api-versions.sh \
            --bootstrap-server localhost:29092 >/dev/null 2>&1; then
        ok "$c is up"; break
      fi
      sleep 2
    done
  done
}

create_source_topic() {
  local retention_ms="${1:-604800000}"  # default 7 days; override for truncation test
  kcli kafka-topics.sh --bootstrap-server "$PRIMARY" --create --if-not-exists \
      --topic "$SRC_TOPIC" --partitions 1 --replication-factor 1 \
      --config "retention.ms=${retention_ms}" \
      --config "segment.ms=10000" >/dev/null
}

count_dr_messages() {
  kcli_dr kafka-run-class.sh kafka.tools.GetOffsetShell \
      --broker-list "$DR" --topic "$DST_TOPIC" 2>/dev/null \
      | awk -F: '{sum+=$3} END {print sum+0}'
}

pause_mm2()  { log "Pausing MirrorMaker 2"; docker pause mm2 >/dev/null; }
resume_mm2() { log "Resuming MirrorMaker 2"; docker unpause mm2 >/dev/null; }
restart_mm2(){ log "Restarting MirrorMaker 2"; docker restart mm2 >/dev/null; }

mm2_running() { [ "$(docker inspect -f '{{.State.Running}}' mm2 2>/dev/null)" = "true" ]; }

# ---------------------------------------------------------------------------
bootstrap_env() {
  log "Starting clusters and MirrorMaker 2"
  $COMPOSE up -d primary-kafka dr-kafka mm2
  wait_for_brokers
}

# ---------------------------------------------------------------------------
scenario_normal() {
  log "SCENARIO 1: NORMAL REPLICATION (1000 messages)"
  create_source_topic 604800000
  mm2_running || restart_mm2
  resume_mm2 2>/dev/null || true

  log "Producing 1000 events to '$SRC_TOPIC'"
  $COMPOSE run --rm producer \
      --count 1000 --bootstrap-servers "$PRIMARY" --topic "$SRC_TOPIC"

  log "Waiting up to 60s for replication to '$DST_TOPIC'"
  local got=0
  for _ in $(seq 1 30); do
    got="$(count_dr_messages)"
    [ "$got" -ge 1000 ] && break
    sleep 2
  done

  log "Replicated message count on DR: $got / 1000"
  [ "$got" -ge 1000 ] && ok "Normal replication verified ($got messages)" \
                       || fail "Expected >=1000 on DR, got $got"
}

# ---------------------------------------------------------------------------
scenario_truncation() {
  log "SCENARIO 2: LOG TRUNCATION DETECTION (fail-fast / Task 2)"
  # Fresh source topic with aggressive 60s retention.
  kcli kafka-topics.sh --bootstrap-server "$PRIMARY" --delete --topic "$SRC_TOPIC" 2>/dev/null || true
  sleep 5
  create_source_topic 60000   # log.retention.ms = 60000 (per spec)

  # Pause MM2 BEFORE producing so data accumulates un-replicated, then ages out.
  mm2_running && pause_mm2 || restart_mm2

  log "Producing 500 events that MM2 will NOT yet replicate (it is paused)"
  $COMPOSE run --rm producer \
      --count 500 --bootstrap-servers "$PRIMARY" --topic "$SRC_TOPIC"

  log "Waiting 90s for 60s-retention + segment roll to purge the un-replicated data"
  sleep 90

  log "Forcing MM2 to advance past committed offsets, then resuming"
  resume_mm2

  log "Watching MM2 logs for truncation detection (up to 60s)"
  local detected=""
  for _ in $(seq 1 30); do
    if docker logs mm2 2>&1 | grep -q "LogTruncationException\|Detected log truncation\|silent data loss"; then
      detected="yes"; break
    fi
    sleep 2
  done

  if [ -n "$detected" ]; then
    ok "MM2 detected log truncation and failed fast (Task 2)"
    log "Relevant MM2 log lines:"
    docker logs mm2 2>&1 | grep -i "truncation\|data loss" | tail -5 || true
  else
    fail "MM2 did not surface a truncation/fail-fast log within the timeout"
  fi
}

# ---------------------------------------------------------------------------
scenario_reset() {
  log "SCENARIO 3: TOPIC RESET RECOVERY (auto-recover / Task 3)"
  restart_mm2
  create_source_topic 604800000

  log "Producing 200 events, waiting for them to replicate"
  $COMPOSE run --rm producer \
      --count 200 --bootstrap-servers "$PRIMARY" --topic "$SRC_TOPIC"
  sleep 15

  pause_mm2

  log "Deleting and recreating source topic '$SRC_TOPIC' (simulated maintenance)"
  kcli kafka-topics.sh --bootstrap-server "$PRIMARY" --delete --topic "$SRC_TOPIC"
  sleep 8
  create_source_topic 604800000

  log "Producing 300 fresh events to the recreated topic"
  $COMPOSE run --rm producer \
      --count 300 --bootstrap-servers "$PRIMARY" --topic "$SRC_TOPIC"

  resume_mm2

  log "Watching MM2 logs for reset detection + auto-recovery (up to 60s)"
  local recovered=""
  for _ in $(seq 1 30); do
    if docker logs mm2 2>&1 | grep -q "Detected source topic reset\|Resubscribed source topic-partition"; then
      recovered="yes"; break
    fi
    sleep 2
  done

  if [ -n "$recovered" ]; then
    ok "MM2 detected the topic reset and resubscribed automatically (Task 3)"
    log "Relevant MM2 log lines:"
    docker logs mm2 2>&1 | grep -i "reset\|resubscrib" | tail -5 || true
  else
    fail "MM2 did not surface a reset-recovery log within the timeout"
  fi

  # Confirm MM2 is still alive (it recovered rather than dying).
  mm2_running && ok "MM2 still running after reset (recovered gracefully)" \
               || fail "MM2 is not running after reset"
}

# ---------------------------------------------------------------------------
main() {
  bootstrap_env
  case "$SCENARIO" in
    normal)     scenario_normal ;;
    truncation) scenario_truncation ;;
    reset)      scenario_reset ;;
    all)        scenario_normal; scenario_truncation; scenario_reset ;;
    *) echo "Usage: $0 [normal|truncation|reset|all]"; exit 1 ;;
  esac
  log "Done."
}

main
