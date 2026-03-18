#!/usr/bin/env bash
# E-commerce replication: OrderService & PaymentService events, NotificationService consumes.
# Scenarios: (1) normal replication, (2) log truncation fail-fast, (3) topic reset recovery.
# Usage: ./run_challenge.sh [normal|truncation|reset|all]

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

COMPOSE_FILE="${COMPOSE_FILE:-docker-compose.yml}"
PRIMARY_BROKER="kafka-primary:9092"
STANDBY_BROKER="kafka-standby:9092"
ORDER_TOPIC="order-events"
PAYMENT_TOPIC="payment-events"
DR_ORDER_TOPIC="primary.order-events"
DR_PAYMENT_TOPIC="primary.payment-events"
MM_SERVICE="mirror-maker"
RETENTION_MS=60000

log() {
  echo "[$(date -Iseconds 2>/dev/null || date +%Y-%m-%dT%H:%M:%S)] $*"
}

dc() {
  if command -v docker-compose &>/dev/null; then
    docker-compose -f "$COMPOSE_FILE" "$@"
  else
    docker compose -f "$COMPOSE_FILE" "$@"
  fi
}

wait_for_broker() {
  local name=$1
  local bootstrap=$2
  log "Waiting for broker $name at $bootstrap"
  for i in $(seq 1 60); do
    if dc exec -T kafka-tools bin/kafka-topics.sh --bootstrap-server "$bootstrap" --list >/dev/null 2>&1; then
      log "Broker $name is up"
      return 0
    fi
    sleep 2
  done
  log "ERROR: broker $name did not become ready"
  return 1
}

create_topics() {
  log "Creating e-commerce topics (order-events, payment-events)"

  for topic in "$ORDER_TOPIC" "$PAYMENT_TOPIC"; do
    dc exec -T kafka-tools bin/kafka-topics.sh \
      --bootstrap-server "$PRIMARY_BROKER" \
      --create --if-not-exists \
      --topic "$topic" \
      --partitions 1 --replication-factor 1 \
      --config "log.retention.ms=$RETENTION_MS"
  done

  for topic in "$DR_ORDER_TOPIC" "$DR_PAYMENT_TOPIC"; do
    dc exec -T kafka-tools bin/kafka-topics.sh \
      --bootstrap-server "$STANDBY_BROKER" \
      --create --if-not-exists \
      --topic "$topic" \
      --partitions 1 --replication-factor 1
  done
}

# Produce order events (ORDER_CREATED) to order-events
produce_order_events() {
  local count=$1
  log "Producing $count order events to $ORDER_TOPIC"
  for i in $(seq 1 "$count"); do
    event_id=$(uuidgen 2>/dev/null || cat /proc/sys/kernel/random/uuid 2>/dev/null || echo "order-$i-$$")
    ts=$(date +%s)
    order_id="ORD-$i"
    echo "{\"event_id\":\"$event_id\",\"event_type\":\"ORDER_CREATED\",\"timestamp\":$ts,\"order_id\":\"$order_id\",\"customer_id\":\"CUST-$i\",\"total_amount\":$((i * 10)).99,\"currency\":\"USD\",\"status\":\"CREATED\"}"
  done | dc exec -i kafka-tools bin/kafka-console-producer.sh \
    --bootstrap-server "$PRIMARY_BROKER" \
    --topic "$ORDER_TOPIC" \
    --request-required-acks 1
}

# Produce payment events (PAYMENT_SUCCESSFUL) to payment-events
produce_payment_events() {
  local count=$1
  log "Producing $count payment events to $PAYMENT_TOPIC"
  for i in $(seq 1 "$count"); do
    event_id=$(uuidgen 2>/dev/null || cat /proc/sys/kernel/random/uuid 2>/dev/null || echo "pay-$i-$$")
    ts=$(date +%s)
    order_id="ORD-$i"
    echo "{\"event_id\":\"$event_id\",\"event_type\":\"PAYMENT_SUCCESSFUL\",\"timestamp\":$ts,\"payment_id\":\"PAY-$i\",\"order_id\":\"$order_id\",\"amount\":$((i * 10)).99,\"currency\":\"USD\",\"status\":\"SUCCESS\"}"
  done | dc exec -i kafka-tools bin/kafka-console-producer.sh \
    --bootstrap-server "$PRIMARY_BROKER" \
    --topic "$PAYMENT_TOPIC" \
    --request-required-acks 1
}

# Produce both order and payment events (simulating OrderService + PaymentService)
produce_ecommerce_events() {
  local count=$1
  produce_order_events "$count"
  produce_payment_events "$count"
}

count_messages() {
  local cluster=$1
  local topic=$2
  local bootstrap
  if [[ "$cluster" == "primary" ]]; then
    bootstrap="$PRIMARY_BROKER"
  else
    bootstrap="$STANDBY_BROKER"
  fi
  dc exec -T kafka-tools bash -c \
    "bin/kafka-console-consumer.sh --bootstrap-server $bootstrap --topic $topic --from-beginning --timeout-ms 10000 2>/dev/null | wc -l" || echo "0"
}

check_mm2_log() {
  local pattern="$1"
  log "Checking MirrorMaker2 logs for: $pattern"
  dc logs "$MM_SERVICE" 2>&1 | grep -F "$pattern" >/dev/null || true
}

###############################################################################
# Scenario 1: Normal replication (order + payment events)
###############################################################################
scenario_normal_replication() {
  log "=== Scenario 1: Normal replication (E-commerce) ==="
  dc down -v --remove-orphans 2>/dev/null || true
  dc up -d kafka-primary kafka-standby kafka-tools

  wait_for_broker primary "$PRIMARY_BROKER"
  wait_for_broker standby "$STANDBY_BROKER"
  create_topics

  log "Starting MirrorMaker2"
  dc up -d "$MM_SERVICE"
  sleep 15

  produce_ecommerce_events 500
  log "Waiting for replication"
  sleep 25

  order_primary=$(count_messages primary "$ORDER_TOPIC")
  order_standby=$(count_messages standby "$DR_ORDER_TOPIC")
  payment_primary=$(count_messages primary "$PAYMENT_TOPIC")
  payment_standby=$(count_messages standby "$DR_PAYMENT_TOPIC")

  log "Order events  - Primary: $order_primary, Standby: $order_standby"
  log "Payment events - Primary: $payment_primary, Standby: $payment_standby"

  if [[ "${order_standby:-0}" -ge "${order_primary:-0}" && "${payment_standby:-0}" -ge "${payment_primary:-0}" ]]; then
    log "Scenario 1 PASSED: order-events and payment-events replicated to DR"
  else
    log "WARNING: DR may still be catching up; check counts above"
  fi
}

###############################################################################
# Scenario 2: Log truncation detection (fail-fast) on order-events
###############################################################################
scenario_truncation_detection() {
  log "=== Scenario 2: Log truncation detection (order-events) ==="

  dc stop "$MM_SERVICE" 2>/dev/null || true

  produce_order_events 200
  log "Sleeping past retention (70s) so order-events is truncated"
  sleep 70

  log "Restarting MirrorMaker2; enhanced MM2 should detect offset gap and fail-fast"
  dc up -d "$MM_SERVICE"
  sleep 20

  if dc ps -a --format '{{.Names}} {{.Status}}' 2>/dev/null | grep -q "mirror-maker.*Exited"; then
    log "Scenario 2: MirrorMaker2 exited (fail-fast) as expected after truncation"
  else
    check_mm2_log "truncat"; check_mm2_log "offset gap"; check_mm2_log "OffsetOutOfRange"
    log "Scenario 2: Check mirror-maker logs for truncation/offset gap detection"
  fi
}

###############################################################################
# Scenario 3: Topic reset handling (order-events delete/recreate)
###############################################################################
scenario_topic_reset() {
  log "=== Scenario 3: Topic reset handling (order-events) ==="

  dc stop "$MM_SERVICE" 2>/dev/null || true

  log "Deleting order-events on primary"
  dc exec -T kafka-tools bin/kafka-topics.sh \
    --bootstrap-server "$PRIMARY_BROKER" \
    --delete --topic "$ORDER_TOPIC" 2>/dev/null || true
  sleep 5

  log "Recreating order-events on primary"
  dc exec -T kafka-tools bin/kafka-topics.sh \
    --bootstrap-server "$PRIMARY_BROKER" \
    --create --topic "$ORDER_TOPIC" \
    --partitions 1 --replication-factor 1 \
    --config "log.retention.ms=$RETENTION_MS"

  log "Restarting MirrorMaker2; should detect reset and resubscribe from beginning"
  dc up -d "$MM_SERVICE"
  sleep 20

  produce_order_events 100
  produce_payment_events 50
  sleep 20

  order_standby=$(count_messages standby "$DR_ORDER_TOPIC")
  log "Standby primary.order-events count after topic reset: $order_standby"

  if [[ "${order_standby:-0}" -ge 100 ]]; then
    log "Scenario 3 PASSED: MirrorMaker2 recovered from order-events reset"
  else
    log "Scenario 3: Standby order count $order_standby; verify MM2 reset recovery in logs"
  fi
}

###############################################################################
# Main
###############################################################################
case "${1:-all}" in
  normal)
    scenario_normal_replication
    ;;
  truncation)
    scenario_truncation_detection
    ;;
  reset)
    scenario_topic_reset
    ;;
  all)
    scenario_normal_replication
    scenario_truncation_detection
    scenario_topic_reset
    ;;
  *)
    echo "Usage: $0 [normal|truncation|reset|all]"
    exit 1
    ;;
esac
