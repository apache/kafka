# Kafka client examples

This module contains some Kafka client examples.

1. Start a Kafka 2.5+ local cluster with a plain listener configured on port 9092.
2. Run `examples/bin/java-producer-consumer-demo.sh` to send 10k messages to topic1 using an asynchronous producer and consume them.
3. Run `examples/bin/java-producer-consumer-demo.sh sync` to send 10k messages to topic1 using a synchronous producer and consume them.
4. Run `examples/bin/exactly-once-demo.sh 6 3 10000` to create input-topic and output-topic with 6 partitions each, 
   send 10k messages to the input topic, and process them with 3 transactional application instances (read-process-write).
