# Kafka client examples

This module contains some Kafka client examples.

Start a local single-broker Kafka cluster with a plain listener configured on port 9092.

## Producer and consumer demo

This demo uses automatic offset commits.

- Run `examples/bin/java-producer-consumer-demo.sh 10000` to asynchronously send 10k records to `my-topic` and consume them.
- Run `examples/bin/java-producer-consumer-demo.sh 10000 sync` to send and consume the records synchronously.

## Consumer-aware rebalance listener demo

This demo shows manual offset management with `RebalanceListener` and the callback-scoped `RebalanceConsumer`, which is valid only during a callback.

Create the two-partition topic used by the demo:

```bash
bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic rebalance-listener-demo --partitions 2
```

Run the demo in two terminals, starting the second instance after the first reports its assignment:

```bash
examples/bin/java-rebalance-listener-demo.sh
```

The default eager rebalance may revoke all partitions before reassignment. The listener commits on revocation,
demonstrates an explicit seek on assignment, and does not commit lost partitions. Stop either consumer with `Ctrl+C` to trigger another rebalance.

## Exactly-once demo

Run `examples/bin/exactly-once-demo.sh 6 3 10000` to create input-topic and output-topic with 6 partitions each, start 3 transactional application instances, and process 10k records.
