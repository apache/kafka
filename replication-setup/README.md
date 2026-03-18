# Kafka Data Replication – E-commerce Event Streaming

This folder runs the **Kafka Data Replication** project using a real-life **e-commerce** example: **OrderService** and **PaymentService** produce events; **NotificationService** consumes them; **MirrorMaker 2** replicates event topics to the DR cluster.

## E-commerce design

### Topics (Primary cluster)

| Topic           | Producer          | Event types              | Description                    |
|----------------|-------------------|--------------------------|--------------------------------|
| `order-events` | OrderService      | ORDER_CREATED, ORDER_CANCELLED | Order lifecycle events         |
| `payment-events` | PaymentService  | PAYMENT_SUCCESSFUL, PAYMENT_FAILED | Payment outcome events        |

Both topics: **1 partition**, **1 replica**, **60s retention** (for truncation testing).

### Flow

1. **OrderService** produces to `order-events` (e.g. ORDER_CREATED, ORDER_CANCELLED).
2. **PaymentService** produces to `payment-events` (e.g. PAYMENT_SUCCESSFUL, PAYMENT_FAILED).
3. **NotificationService** consumes from both topics and sends notifications (in this setup it logs each event).
4. **MirrorMaker 2** replicates `order-events` and `payment-events` to the standby cluster as `primary.order-events` and `primary.payment-events`, so a DR NotificationService can consume the same stream after failover.

### Event schemas

See **[docs/EVENT_SCHEMA.md](docs/EVENT_SCHEMA.md)** for JSON examples: order events (ORDER_CREATED, ORDER_CANCELLED) and payment events (PAYMENT_SUCCESSFUL, PAYMENT_FAILED).

---

## Repository & images

- **Kafka fork**: This code lives in a fork of [Apache Kafka](https://github.com/apache/kafka) (v4.0.0) with MirrorMaker 2 fault-tolerance changes.
- **Docker images**: Set `KAFKA_IMAGE` for your enhanced Kafka/MirrorMaker 2 image (default: `apache/kafka:4.0.0`).

---

## Prerequisites

- Docker and Docker Compose
- Bash (e.g. Git Bash on Windows) for `run_challenge.sh`

---

## Quick start

```bash
cd replication-setup
export KAFKA_IMAGE=your-dockerhub-user/kafka-mm2-enhanced:latest   # optional
docker compose up -d kafka-primary kafka-standby kafka-tools
# Then create topics and start MM2, or run the challenge script (see below).
```

---

## Running the challenge script

`run_challenge.sh` runs three scenarios: normal replication, log truncation (fail-fast), and topic reset recovery.

```bash
chmod +x run_challenge.sh
./run_challenge.sh all
```

### Scenario 1: Normal replication

- Brings up Primary and Standby, creates `order-events` and `payment-events` on both (with 60s retention on primary).
- Starts MirrorMaker 2, produces 500 order events and 500 payment events to the primary, then checks that `primary.order-events` and `primary.payment-events` on standby have at least as many messages.

### Scenario 2: Log truncation (fail-fast)

- Stops MirrorMaker 2, produces 200 order events, waits 70s for retention to truncate `order-events`.
- Restarts MirrorMaker 2; the **enhanced** MirrorMaker 2 should detect the offset gap, log an error, and **fail fast**. The script checks that the mirror-maker container has exited or that logs show truncation/offset-gap messages.

### Scenario 3: Topic reset

- Stops MirrorMaker 2, deletes and recreates `order-events` on the primary.
- Restarts MirrorMaker 2; it should detect the topic reset and **resubscribe from the beginning**.
- Produces 100 order events and 50 payment events and verifies replication to the standby.

Run a single scenario:

```bash
./run_challenge.sh normal
./run_challenge.sh truncation
./run_challenge.sh reset
```

---

## Configuration

- **MirrorMaker 2**: `config/mm2.properties` replicates `order-events` and `payment-events` from primary to standby.
- **NotificationService**: In `docker-compose.yml`, a container consumes from `order-events` and `payment-events` on the primary and logs each message (simulating notifications). Start it with `docker compose up -d notification-service` after the brokers and topics are up.

---

## Log analysis

```bash
# MirrorMaker 2 (truncation and topic reset messages)
docker compose logs mirror-maker

# Topic details
docker compose exec kafka-tools bin/kafka-topics.sh \
  --bootstrap-server kafka-primary:9092 --describe --topic order-events
docker compose exec kafka-tools bin/kafka-topics.sh \
  --bootstrap-server kafka-standby:9092 --describe --topic primary.order-events

# Consume replicated order events from DR
docker compose exec -T kafka-tools bin/kafka-console-consumer.sh \
  --bootstrap-server kafka-standby:9092 --topic primary.order-events --from-beginning --timeout-ms 10000
```

**What to look for**

- **Truncation**: MirrorMaker 2 logs about offset gap / log truncation and container exit (fail-fast).
- **Topic reset**: Logs about topic reset / resubscribing from beginning and messages flowing again.

---

## Design rationale

- **E-commerce domains**: Order and payment events are separate topics so each service owns its stream; NotificationService consumes both for a unified view.
- **Single partition**: Keeps ordering per topic and simplifies the demo; retention is 60s to trigger truncation in scenario 2.
- **DR replication**: MirrorMaker 2 replicates both topics so a standby NotificationService can run against the DR cluster with the same event stream.

---

## AI usage

If you use AI tools for this project, document in your main README or report: which tools, which prompts, and how they helped (e.g. e-commerce event design, replication-setup layout, or script logic). Ensure you understand and can explain every line of the delivered code and config.
