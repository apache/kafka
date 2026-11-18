# Kafka client examples

This module contains some Kafka client examples.

1. Start a local single-broker Kafka cluster with a plain listener configured on port 9092.
2. Run `examples/bin/java-producer-consumer-demo.sh 10000` to asynchronously send 10k records to `my-topic` and consume them.
3. Run `examples/bin/java-producer-consumer-demo.sh 10000 sync` to synchronously send 10k records to `my-topic` and consume them.
4. Run `examples/bin/exactly-once-demo.sh 6 3 10000` to create input-topic and output-topic with 6 partitions each,
   start 3 transactional application instances and process 10k records.
