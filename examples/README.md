# Kafka client examples

This module contains some Kafka client examples.

Start a local single-broker Kafka cluster with a plain listener configured on port 9092.

## Producer and consumer demo

This demo uses automatic offset commits.

- Run `examples/bin/java-producer-consumer-demo.sh 10000` to asynchronously send 10k records to `my-topic` and consume them.
- Run `examples/bin/java-producer-consumer-demo.sh 10000 sync` to synchronously send 10k records to `my-topic` and consume them.

## Rebalance listener demo

This demo shows manual offset management with `RebalanceListener` and the callback-scoped `RebalanceConsumer`, which is valid only during a callback.

Create the two-partition topic used by the demo:

```bash
bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic rebalance-listener-demo --partitions 2
```

Run the demo in two terminals, starting the second instance after the first reports its assignment:

```bash
examples/bin/java-rebalance-listener-demo.sh
```

Stop either consumer with `Ctrl+C` to observe another rebalance.

To observe `onPartitionsLost` on Linux/macOS, ensure both consumers are running, then suspend one with `Ctrl+Z` after it has received partitions.
Wait for its session to time out and the other consumer to take over its partitions, then run `fg` in the original terminal to resume it.
The callback logs the lost partitions without committing offsets, since the consumer no longer owns them.

The demo uses `classic` by default. To compare callback behavior with the `consumer` protocol, stop both instances and restart them with the `consumer` argument.

## Exactly-once demo

This demo uses Kafka transactions to atomically commit output records and consumed offsets, providing exactly-once processing between Kafka topics.

Run `examples/bin/exactly-once-demo.sh 6 3 10000` to create input-topic and output-topic with 6 partitions each, start 3 transactional application instances, and process 10k records.
