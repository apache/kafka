Hadoop to Kafka Bridge
======================

What's new?
-----------

* Kafka 0.8 support
  * No more ZK-based load balancing (backwards incompatible change)
* Semantic partitioning is now supported in KafkaOutputFormat. Just specify a
  key in the output committer of your job. The Pig StoreFunc doesn't support
  semantic partitioning.
* Config parameters are now the same as the Kafka producer, just prepended with
  kafka.output (e.g., kafka.output.max.message.size). This is a backwards
  incompatible change.

What is it?
-----------

The Hadoop to Kafka bridge is a way to publish data from Hadoop to Kafka. There
are two possible mechanisms, varying from easy to difficult: writing a Pig
script and writing messages in Avro format, or rolling your own job using the
Kafka `OutputFormat`. 

Note that there are no write-once semantics: any client of the data must handle
messages in an idempotent manner. That is, because of node failures and
Hadoop's failure recovery, it's possible that the same message is published
multiple times in the same push.

How do I use it?
----------------

With this bridge, Kafka topics are URIs and are specified as URIs of the form
`kafka://<kafka-server>/<kafka-topic>` to connect to a specific Kafka broker.

### Pig ###

Pig bridge writes data in binary Avro format with one message created per input
row. To push data via Kafka, store to the Kafka URI using `AvroKafkaStorage`
with the Avro schema as its first argument. You'll need to register the
appropriate Kafka JARs. Here is what an example Pig script looks like:

    REGISTER hadoop-producer_2.8.0-0.8.0.jar;
    REGISTER avro-1.4.0.jar;
    REGISTER piggybank.jar;
    REGISTER kafka-0.8.0.jar;
    REGISTER jackson-core-asl-1.5.5.jar;
    REGISTER jackson-mapper-asl-1.5.5.jar;
    REGISTER scala-library.jar;

    member_info = LOAD 'member_info.tsv' AS (member_id : int, name : chararray);
    names = FOREACH member_info GENERATE name;
    STORE member_info INTO 'kafka://my-kafka:9092/member_info' USING kafka.bridge.AvroKafkaStorage('"string"');

That's it! The Pig StoreFunc makes use of AvroStorage in Piggybank to convert
from Pig's data model to the specified Avro schema.

Further, multi-store is possible with KafkaStorage, so you can easily write to
multiple topics and brokers in the same job:

    SPLIT member_info INTO early_adopters IF member_id < 1000, others IF member_id >= 1000;
    STORE early_adopters INTO 'kafka://my-broker:9092/early_adopters' USING AvroKafkaStorage('$schema');
    STORE others INTO 'kafka://my-broker2:9092/others' USING AvroKafkaStorage('$schema');

### KafkaOutputFormat ###

KafkaOutputFormat is a Hadoop OutputFormat for publishing data via Kafka. It
uses the newer 0.20 mapreduce APIs and simply pushes bytes (i.e.,
BytesWritable). This is a lower-level method of publishing data, as it allows
you to precisely control output.

Included is an example that publishes some input text line-by-line to a topic.
With KafkaOutputFormat, the key can be a null, where it is ignored by the
producer (random partitioning), or any object for semantic partitioning of the
stream (with an appropriate Kafka partitioner set). Speculative execution is
turned off by the OutputFormat.

What can I tune?
----------------

* kafka.output.queue.bytes: Bytes to queue in memory before pushing to the Kafka
  producer (i.e., the batch size). Default is 1,000,000 (1 million) bytes.

Any of Kafka's producer parameters can be changed by prefixing them with
"kafka.output" in one's job configuration. For example, to change the
compression codec, one would add the "kafka.output.compression.codec" parameter
(e.g., "SET kafka.output.compression.codec 0" in one's Pig script for no
compression). 

For easier debugging, the above values as well as the Kafka broker information
(kafka.metadata.broker.list), the topic (kafka.output.topic), and the schema
(kafka.output.schema) are injected into the job's configuration. By default,
the Hadoop producer uses Kafka's sync producer as asynchronous operation
doesn't make sense in the batch Hadoop case.

