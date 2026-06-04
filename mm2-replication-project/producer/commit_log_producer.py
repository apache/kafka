#!/usr/bin/env python3
"""
Commit Log Producer (Task 1)

A small CLI application that generates JSON commit-log events and produces them
to the primary cluster's `commit-log` topic. Each event matches the schema
defined in the project specification:

    {
      "event_id": "<uuid4>",
      "timestamp": <unix epoch seconds>,
      "op_type": "INSERT" | "UPDATE" | "DELETE",
      "key": "doc:<hex>",
      "value": { "status": "<status>" }
    }

The message key is set to the event's `key` field so that all events for the
same document land on the same partition (relevant once partition counts grow
beyond 1; harmless with the single-partition test setup).

Usage:
    python commit_log_producer.py --count 1000 \
        --bootstrap-servers localhost:9092 \
        --topic commit-log
"""

import argparse
import json
import random
import sys
import time
import uuid

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

OP_TYPES = ["INSERT", "UPDATE", "DELETE"]
STATUSES = ["active", "archived", "pending", "deleted", "draft"]


def build_event() -> dict:
    """Construct a single commit-log event matching the required schema."""
    return {
        "event_id": str(uuid.uuid4()),
        "timestamp": int(time.time()),
        "op_type": random.choice(OP_TYPES),
        "key": "doc:%04x" % random.randint(0, 0xFFFF),
        "value": {"status": random.choice(STATUSES)},
    }


def create_producer(bootstrap_servers: str, retries: int = 30) -> KafkaProducer:
    """Create a KafkaProducer, retrying while the broker comes online."""
    last_err = None
    for attempt in range(1, retries + 1):
        try:
            return KafkaProducer(
                bootstrap_servers=bootstrap_servers.split(","),
                # acks=all -> wait for the leader to persist before counting a
                # send as successful. Important for a "write-ahead log".
                acks="all",
                retries=5,
                linger_ms=5,
                key_serializer=lambda k: k.encode("utf-8") if k is not None else None,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            )
        except NoBrokersAvailable as err:
            last_err = err
            print(
                "[producer] broker not available yet (attempt %d/%d), retrying in 2s..."
                % (attempt, retries),
                file=sys.stderr,
            )
            time.sleep(2)
    raise SystemExit("[producer] could not connect to Kafka: %s" % last_err)


def main() -> int:
    parser = argparse.ArgumentParser(description="Commit Log Producer")
    parser.add_argument(
        "--count",
        type=int,
        required=True,
        help="Number of messages to produce, then exit.",
    )
    parser.add_argument(
        "--bootstrap-servers",
        default="localhost:9092",
        help="Comma-separated Kafka bootstrap servers (default: localhost:9092).",
    )
    parser.add_argument(
        "--topic",
        default="commit-log",
        help="Destination topic (default: commit-log).",
    )
    args = parser.parse_args()

    if args.count < 0:
        raise SystemExit("[producer] --count must be non-negative")

    producer = create_producer(args.bootstrap_servers)
    print(
        "[producer] producing %d event(s) to topic '%s' on %s"
        % (args.count, args.topic, args.bootstrap_servers)
    )

    sent = 0
    for _ in range(args.count):
        event = build_event()
        producer.send(args.topic, key=event["key"], value=event)
        sent += 1
        if sent % 100 == 0:
            print("[producer] queued %d/%d" % (sent, args.count))

    # Block until every buffered record has been delivered (or failed).
    producer.flush()
    producer.close()
    print("[producer] done. delivered %d event(s) to '%s'." % (sent, args.topic))
    return 0


if __name__ == "__main__":
    sys.exit(main())
