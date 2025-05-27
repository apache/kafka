---
title: Configuring a Streams Application
description: 
weight: 2
tags: ['kafka', 'docs']
aliases: 
keywords: 
type: docs
---

# Configuring a Streams Application

Kafka and Kafka Streams configuration options must be configured before using Streams. You can configure Kafka Streams by specifying parameters in a `java.util.Properties` instance.

  1. Create a `java.util.Properties` instance.

  2. Set the parameters. For example:
    
        import java.util.Properties;
    import org.apache.kafka.streams.StreamsConfig;
    
    Properties settings = new Properties();
    // Set a few key parameters
    settings.put(StreamsConfig.APPLICATION_ID_CONFIG, "my-first-streams-application");
    settings.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka-broker1:9092");
    // Any further settings
    settings.put(... , ...);




# Configuration parameter reference

This section contains the most common Streams configuration parameters. For a full reference, see the [Streams](/40/javadoc/org/apache/kafka/streams/StreamsConfig.html) Javadocs.

  * Required configuration parameters
    * application.id
    * bootstrap.servers
  * Recommended configuration parameters for resiliency
    * acks
    * replication.factor
    * min.insync.replicas
    * num.standby.replicas
  * Optional configuration parameters
    * acceptable.recovery.lag
    * default.deserialization.exception.handler (deprecated since 4.0)
    * default.key.serde
    * default.production.exception.handler (deprecated since 4.0)
    * default.timestamp.extractor
    * default.value.serde
    * deserialization.exception.handler
    * enable.metrics.push
    * log.summary.interval.ms
    * max.task.idle.ms
    * max.warmup.replicas
    * num.standby.replicas
    * num.stream.threads
    * probing.rebalance.interval.ms
    * processing.exception.handler
    * processing.guarantee
    * processor.wrapper.class
    * production.exception.handler
    * rack.aware.assignment.non_overlap_cost
    * rack.aware.assignment.strategy
    * rack.aware.assignment.tags
    * rack.aware.assignment.traffic_cost
    * replication.factor
    * rocksdb.config.setter
    * state.dir
    * task.assignor.class
    * topology.optimization
    * windowed.inner.class.serde
  * Kafka consumers and producer configuration parameters
    * Naming
    * Default Values
    * Parameters controlled by Kafka Streams
    * enable.auto.commit



# Required configuration parameters

Here are the required Streams configuration parameters.

Parameter Name | Importance | Description | Default Value  
---|---|---|---  
application.id | Required | An identifier for the stream processing application. Must be unique within the Kafka cluster. | None  
bootstrap.servers | Required | A list of host/port pairs to use for establishing the initial connection to the Kafka cluster. | None  
  
## application.id

> (Required) The application ID. Each stream processing application must have a unique ID. The same ID must be given to all instances of the application. It is recommended to use only alphanumeric characters, `.` (dot), `-` (hyphen), and `_` (underscore). Examples: `"hello_world"`, `"hello_world-v1.0.0"`
> 
> This ID is used in the following places to isolate resources used by the application from others:
> 
>   * As the default Kafka consumer and producer `client.id` prefix
>   * As the Kafka consumer `group.id` for coordination
>   * As the name of the subdirectory in the state directory (cf. `state.dir`)
>   * As the prefix of internal Kafka topic names
> 

> 
> Tip:
>     When an application is updated, the `application.id` should be changed unless you want to reuse the existing data in internal topics and state stores. For example, you could embed the version information within `application.id`, as `my-app-v1.0.0` and `my-app-v1.0.2`.

## bootstrap.servers

> (Required) The Kafka bootstrap servers. This is the same [setting](/40/documentation.html#producerconfigs) that is used by the underlying producer and consumer clients to connect to the Kafka cluster. Example: `"kafka-broker1:9092,kafka-broker2:9092"`.

# Recommended configuration parameters for resiliency

There are several Kafka and Kafka Streams configuration options that need to be configured explicitly for resiliency in face of broker failures:

Parameter Name | Corresponding Client | Default value | Consider setting to  
---|---|---|---  
acks | Producer (for version <=2.8) | `acks="1")` | `acks="all"`  
replication.factor (for broker version 2.3 or older) | Streams | `-1` | `3` (broker 2.4+: ensure broker config `default.replication.factor=3`)  
min.insync.replicas | Broker | `1` | `2`  
num.standby.replicas | Streams | `0` | `1`  
  
Increasing the replication factor to 3 ensures that the internal Kafka Streams topic can tolerate up to 2 broker failures. The tradeoff from moving to the default values to the recommended ones is that some performance and more storage space (3x with the replication factor of 3) are sacrificed for more resiliency.

## acks

> The number of acknowledgments that the leader must have received before considering a request complete. This controls the durability of records that are sent. The possible values are:
> 
>   * `acks="0"` The producer does not wait for acknowledgment from the server and the record is immediately added to the socket buffer and considered sent. No guarantee can be made that the server has received the record in this case, and the producer won't generally know of any failures. The offset returned for each record will always be set to `-1`.
>   * `acks="1"` The leader writes the record to its local log and responds without waiting for full acknowledgement from all followers. If the leader immediately fails after acknowledging the record, but before the followers have replicated it, then the record will be lost.
>   * `acks="all"` (default since 3.0 release) The leader waits for the full set of in-sync replicas to acknowledge the record. This guarantees that the record will not be lost if there is at least one in-sync replica alive. This is the strongest available guarantee.
> 

> 
> For more information, see the [Kafka Producer documentation](https://kafka.apache.org/#producerconfigs).

## replication.factor

> See the description here.

## min.insync.replicas

The minimum number of in-sync replicas available for replication if the producer is configured with `acks="all"` (see [topic configs](/40/#topicconfigs_min.insync.replicas)). 

## num.standby.replicas

> See the description here.
    
    
    Properties streamsSettings = new Properties();
    // for broker version 2.3 or older
    //streamsSettings.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 3);
    // for version 2.8 or older
    //streamsSettings.put(StreamsConfig.producerPrefix(ProducerConfig.ACKS_CONFIG), "all");
    streamsSettings.put(StreamsConfig.topicPrefix(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG), 2);
    streamsSettings.put(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, 1);

# Optional configuration parameters

Here are the optional [Streams](/40/javadoc/org/apache/kafka/streams/StreamsConfig.html) javadocs, sorted by level of importance:

>   * High: These are parameters with a default value which is most likely not a good fit for production use. It's highly recommended to revisit these parameters for production usage.
>   * Medium: The default values of these parameters should work for production for many cases, but it's not uncommon that they are changed, for example to tune performance.
>   * Low: It should rarely be necessary to change the value for these parameters. It's only recommended to change them if there is a very specific issue you want to address.
> 


Parameter Name | Importance | Description | Default Value  
---|---|---|---  
acceptable.recovery.lag | Medium | The maximum acceptable lag (number of offsets to catch up) for an instance to be considered caught-up and ready for the active task. | `10000`  
application.server | Low | A host:port pair pointing to an embedded user defined endpoint that can be used for discovering the locations of state stores within a single Kafka Streams application. The value of this must be different for each instance of the application. | the empty string  
buffered.records.per.partition | Low | The maximum number of records to buffer per partition. | `1000`  
statestore.cache.max.bytes | Medium | Maximum number of memory bytes to be used for record caches across all threads. | `10485760`  
cache.max.bytes.buffering (Deprecated. Use statestore.cache.max.bytes instead.) | Medium | Maximum number of memory bytes to be used for record caches across all threads. | `10485760`  
client.id | Medium | An ID string to pass to the server when making requests. (This setting is passed to the consumer/producer clients used internally by Kafka Streams.) | the empty string  
commit.interval.ms | Low | The frequency in milliseconds with which to save the position (offsets in source topics) of tasks. | `30000` (30 seconds)  
default.deserialization.exception.handler (Deprecated. Use deserialization.exception.handler instead.) | Medium | Exception handling class that implements the `DeserializationExceptionHandler` interface. | `LogAndContinueExceptionHandler`  
default.key.serde | Medium | Default serializer/deserializer class for record keys, implements the `Serde` interface. Must be set by the user or all serdes must be passed in explicitly (see also default.value.serde). | `null`  
default.production.exception.handler (Deprecated. Use production.exception.handler instead.) | Medium | Exception handling class that implements the `ProductionExceptionHandler` interface. | `DefaultProductionExceptionHandler`  
default.timestamp.extractor | Medium | Timestamp extractor class that implements the `TimestampExtractor` interface. See Timestamp Extractor | `FailOnInvalidTimestamp`  
default.value.serde | Medium | Default serializer/deserializer class for record values, implements the `Serde` interface. Must be set by the user or all serdes must be passed in explicitly (see also default.key.serde). | `null`  
default.dsl.store | Low |  [DEPRECATED] The default state store type used by DSL operators. Deprecated in favor of `dsl.store.suppliers.class` | `"ROCKS_DB"`  
deserialization.exception.handler | Medium | Exception handling class that implements the `DeserializationExceptionHandler` interface. | `LogAndContinueExceptionHandler`  
dsl.store.suppliers.class | Low |  Defines a default state store implementation to be used by any stateful DSL operator that has not explicitly configured the store implementation type. Must implement the `org.apache.kafka.streams.state.DslStoreSuppliers` interface.  | `BuiltInDslStoreSuppliers.RocksDBDslStoreSuppliers`  
log.summary.interval.ms | Low | The output interval in milliseconds for logging summary information (disabled if negative). | `120000` (2 minutes)  
enable.metrics.push | Low | Whether to enable pushing of client metrics to the cluster, if the cluster has a client metrics subscription which matches this client. | `true`  
max.task.idle.ms | Medium |  This config controls whether joins and merges may produce out-of-order results. The config value is the maximum amount of time in milliseconds a stream task will stay idle when it is fully caught up on some (but not all) input partitions to wait for producers to send additional records and avoid potential out-of-order record processing across multiple input streams. The default (zero) does not wait for producers to send more records, but it does wait to fetch data that is already present on the brokers. This default means that for records that are already present on the brokers, Streams will process them in timestamp order. Set to -1 to disable idling entirely and process any locally available data, even though doing so may produce out-of-order processing.  | `0`  
max.warmup.replicas | Medium | The maximum number of warmup replicas (extra standbys beyond the configured num.standbys) that can be assigned at once. | `2`  
metric.reporters | Low | A list of classes to use as metrics reporters. | the empty list  
metrics.num.samples | Low | The number of samples maintained to compute metrics. | `2`  
metrics.recording.level | Low | The highest recording level for metrics. | `INFO`  
metrics.sample.window.ms | Low | The window of time in milliseconds a metrics sample is computed over. | `30000` (30 seconds)  
num.standby.replicas | High | The number of standby replicas for each task. | `0`  
num.stream.threads | Medium | The number of threads to execute stream processing. | `1`  
probing.rebalance.interval.ms | Low | The maximum time in milliseconds to wait before triggering a rebalance to probe for warmup replicas that have sufficiently caught up. | `600000` (10 minutes)  
processing.exception.handler | Medium | Exception handling class that implements the `ProcessingExceptionHandler` interface. | `LogAndFailProcessingExceptionHandler`  
processing.guarantee | Medium | The processing mode. Can be either `"at_least_once"` or `"exactly_once_v2"` (for EOS version 2, requires broker version 2.5+). See Processing Guarantee.. | `"at_least_once"`  
processor.wrapper.class | Medium | A class or class name implementing the `ProcessorWrapper` interface. Must be passed in when creating the topology, and will not be applied unless passed in to the appropriate constructor as a TopologyConfig. You should use the `StreamsBuilder#new(TopologyConfig)` constructor for DSL applications, and the `Topology#new(TopologyConfig)` constructor for PAPI applications.  
production.exception.handler | Medium | Exception handling class that implements the `ProductionExceptionHandler` interface. | `DefaultProductionExceptionHandler`  
poll.ms | Low | The amount of time in milliseconds to block waiting for input. | `100`  
rack.aware.assignment.strategy | Low | The strategy used for rack aware assignment. Acceptable value are `"none"` (default), `"min_traffic"`, and `"balance_suttopology"`. See Rack Aware Assignment Strategy. | `"none"`  
List of tag keys used to distribute standby replicas across Kafka Streams clients. When configured, Kafka Streams will make a best-effort to distribute the standby tasks over clients with different tag values. See Rack Aware Assignment Tags. the empty list  
rack.aware.assignment.non_overlap_cost | Low | Cost associated with moving tasks from existing assignment. See Rack Aware Assignment Non-Overlap-Cost. | `null`  
rack.aware.assignment.non_overlap_cost | Low | Cost associated with cross rack traffic. See Rack Aware Assignment Traffic-Cost. | `null`  
replication.factor | Medium | The replication factor for changelog topics and repartition topics created by the application. The default of `-1` (meaning: use broker default replication factor) requires broker version 2.4 or newer. | `-1`  
retry.backoff.ms | Low | The amount of time in milliseconds, before a request is retried. | `100`  
rocksdb.config.setter | Medium | The RocksDB configuration. | `null`  
state.cleanup.delay.ms | Low | The amount of time in milliseconds to wait before deleting state when a partition has migrated. | `600000` (10 minutes)  
state.dir | High | Directory location for state stores. | `/${java.io.tmpdir}/kafka-streams`  
task.assignor.class | Medium | A task assignor class or class name implementing the `TaskAssignor` interface. | The high-availability task assignor.  
task.timeout.ms | Medium | The maximum amount of time in milliseconds a task might stall due to internal errors and retries until an error is raised. For a timeout of `0 ms`, a task would raise an error for the first internal error. For any timeout larger than `0 ms`, a task will retry at least once before an error is raised. | `300000` (5 minutes)  
topology.optimization | Medium | A configuration telling Kafka Streams if it should optimize the topology and what optimizations to apply. Acceptable values are: `StreamsConfig.NO_OPTIMIZATION` (`none`), `StreamsConfig.OPTIMIZE` (`all`) or a comma separated list of specific optimizations: `StreamsConfig.REUSE_KTABLE_SOURCE_TOPICS` (`reuse.ktable.source.topics`), `StreamsConfig.MERGE_REPARTITION_TOPICS` (`merge.repartition.topics`), `StreamsConfig.SINGLE_STORE_SELF_JOIN` (`single.store.self.join`).  | `"NO_OPTIMIZATION"`  
upgrade.from | Medium | The version you are upgrading from during a rolling upgrade. See Upgrade From | `null`  
windowstore.changelog.additional.retention.ms | Low | Added to a windows maintainMs to ensure data is not deleted from the log prematurely. Allows for clock drift. | `86400000` (1 day)  
window.size.ms | Low | Sets window size for the deserializer in order to calculate window end times. | `null`  
  
## acceptable.recovery.lag

> The maximum acceptable lag (total number of offsets to catch up from the changelog) for an instance to be considered caught-up and able to receive an active task. Streams will only assign stateful active tasks to instances whose state stores are within the acceptable recovery lag, if any exist, and assign warmup replicas to restore state in the background for instances that are not yet caught up. Should correspond to a recovery time of well under a minute for a given workload. Must be at least 0. 
> 
> Note: if you set this to `Long.MAX_VALUE` it effectively disables the warmup replicas and task high availability, allowing Streams to immediately produce a balanced assignment and migrate tasks to a new instance without first warming them up. 

## deserialization.exception.handler (deprecated: default.deserialization.exception.handler)

> The deserialization exception handler allows you to manage record exceptions that fail to deserialize. This can be caused by corrupt data, incorrect serialization logic, or unhandled record types. The implemented exception handler needs to return a `FAIL` or `CONTINUE` depending on the record and the exception thrown. Returning `FAIL` will signal that Streams should shut down and `CONTINUE` will signal that Streams should ignore the issue and continue processing. The following library built-in exception handlers are available:
> 
>   * [LogAndContinueExceptionHandler](/40/javadoc/org/apache/kafka/streams/errors/LogAndContinueExceptionHandler.html): This handler logs the deserialization exception and then signals the processing pipeline to continue processing more records. This log-and-skip strategy allows Kafka Streams to make progress instead of failing if there are records that fail to deserialize.
>   * [LogAndFailExceptionHandler](/40/javadoc/org/apache/kafka/streams/errors/LogAndFailExceptionHandler.html). This handler logs the deserialization exception and then signals the processing pipeline to stop processing more records.
> 

> 
> You can also provide your own customized exception handler besides the library provided ones to meet your needs. For example, you can choose to forward corrupt records into a quarantine topic (think: a "dead letter queue") for further processing. To do this, use the Producer API to write a corrupted record directly to the quarantine topic. To be more concrete, you can create a separate `KafkaProducer` object outside the Streams client, and pass in this object as well as the dead letter queue topic name into the `Properties` map, which then can be retrieved from the `configure` function call. The drawback of this approach is that "manual" writes are side effects that are invisible to the Kafka Streams runtime library, so they do not benefit from the end-to-end processing guarantees of the Streams API:
>     
>     
>     public class SendToDeadLetterQueueExceptionHandler implements DeserializationExceptionHandler {
>         KafkaProducer<byte[], byte[]> dlqProducer;
>         String dlqTopic;
>     
>         @Override
>         public DeserializationHandlerResponse handle(final ErrorHandlerContext context,
>                                                      final ConsumerRecord<byte[], byte[]> record,
>                                                      final Exception exception) {
>     
>             log.warn("Exception caught during Deserialization, sending to the dead queue topic; " +
>                 "taskId: {}, topic: {}, partition: {}, offset: {}",
>                 context.taskId(), record.topic(), record.partition(), record.offset(),
>                 exception);
>     
>             dlqProducer.send(new ProducerRecord<>(dlqTopic, record.timestamp(), record.key(), record.value(), record.headers())).get();
>     
>             return DeserializationHandlerResponse.CONTINUE;
>         }
>     
>         @Override
>         public void configure(final Map<String, ?> configs) {
>             dlqProducer = .. // get a producer from the configs map
>             dlqTopic = .. // get the topic name from the configs map
>         }
>     }

## production.exception.handler (deprecated: default.production.exception.handler)

> The production exception handler allows you to manage exceptions triggered when trying to interact with a broker such as attempting to produce a record that is too large. By default, Kafka provides and uses the [DefaultProductionExceptionHandler](/40/javadoc/org/apache/kafka/streams/errors/DefaultProductionExceptionHandler.html) that always fails when these exceptions occur.
> 
> An exception handler can return `FAIL`, `CONTINUE`, or `RETRY` depending on the record and the exception thrown. Returning `FAIL` will signal that Streams should shut down. `CONTINUE` will signal that Streams should ignore the issue and continue processing. For `RetriableException` the handler may return `RETRY` to tell the runtime to retry sending the failed record (**Note:** If `RETRY` is returned for a non-`RetriableException` it will be treated as `FAIL`.) If you want to provide an exception handler that always ignores records that are too large, you could implement something like the following:
>     
>     
>     import java.util.Properties;
>     import org.apache.kafka.streams.StreamsConfig;
>     import org.apache.kafka.common.errors.RecordTooLargeException;
>     import org.apache.kafka.streams.errors.ProductionExceptionHandler;
>     import org.apache.kafka.streams.errors.ProductionExceptionHandler.ProductionExceptionHandlerResponse;
>     
>     public class IgnoreRecordTooLargeHandler implements ProductionExceptionHandler {
>         public void configure(Map<String, Object> config) {}
>     
>         public ProductionExceptionHandlerResponse handle(final ErrorHandlerContext context,
>                                                          final ProducerRecord<byte[], byte[]> record,
>                                                          final Exception exception) {
>             if (exception instanceof RecordTooLargeException) {
>                 return ProductionExceptionHandlerResponse.CONTINUE;
>             } else {
>                 return ProductionExceptionHandlerResponse.FAIL;
>             }
>         }
>     }
>     
>     Properties settings = new Properties();
>     
>     // other various kafka streams settings, e.g. bootstrap servers, application id, etc
>     
>     settings.put(StreamsConfig.PRODUCTION_EXCEPTION_HANDLER_CLASS_CONFIG,
>                  IgnoreRecordTooLargeHandler.class);

## default.timestamp.extractor

> A timestamp extractor pulls a timestamp from an instance of [ConsumerRecord](/40/javadoc/org/apache/kafka/clients/consumer/ConsumerRecord.html). Timestamps are used to control the progress of streams.
> 
> The default extractor is [FailOnInvalidTimestamp](/40/javadoc/org/apache/kafka/streams/processor/FailOnInvalidTimestamp.html). This extractor retrieves built-in timestamps that are automatically embedded into Kafka messages by the Kafka producer client since [Kafka version 0.10](https://cwiki.apache.org/confluence/display/KAFKA/KIP-32+-+Add+timestamps+to+Kafka+message). Depending on the setting of Kafka's server-side `log.message.timestamp.type` broker and `message.timestamp.type` topic parameters, this extractor provides you with:
> 
>   * **event-time** processing semantics if `log.message.timestamp.type` is set to `CreateTime` aka "producer time" (which is the default). This represents the time when a Kafka producer sent the original message. If you use Kafka's official producer client, the timestamp represents milliseconds since the epoch.
>   * **ingestion-time** processing semantics if `log.message.timestamp.type` is set to `LogAppendTime` aka "broker time". This represents the time when the Kafka broker received the original message, in milliseconds since the epoch.
> 

> 
> The `FailOnInvalidTimestamp` extractor throws an exception if a record contains an invalid (i.e. negative) built-in timestamp, because Kafka Streams would not process this record but silently drop it. Invalid built-in timestamps can occur for various reasons: if for example, you consume a topic that is written to by pre-0.10 Kafka producer clients or by third-party producer clients that don't support the new Kafka 0.10 message format yet; another situation where this may happen is after upgrading your Kafka cluster from `0.9` to `0.10`, where all the data that was generated with `0.9` does not include the `0.10` message timestamps.
> 
> If you have data with invalid timestamps and want to process it, then there are two alternative extractors available. Both work on built-in timestamps, but handle invalid timestamps differently.
> 
>   * [LogAndSkipOnInvalidTimestamp](/40/javadoc/org/apache/kafka/streams/processor/LogAndSkipOnInvalidTimestamp.html): This extractor logs a warn message and returns the invalid timestamp to Kafka Streams, which will not process but silently drop the record. This log-and-skip strategy allows Kafka Streams to make progress instead of failing if there are records with an invalid built-in timestamp in your input data.
>   * [UsePartitionTimeOnInvalidTimestamp](/40/javadoc/org/apache/kafka/streams/processor/UsePartitionTimeOnInvalidTimestamp.html). This extractor returns the record's built-in timestamp if it is valid (i.e. not negative). If the record does not have a valid built-in timestamps, the extractor returns the previously extracted valid timestamp from a record of the same topic partition as the current record as a timestamp estimation. In case that no timestamp can be estimated, it throws an exception.
> 

> 
> Another built-in extractor is [WallclockTimestampExtractor](/40/javadoc/org/apache/kafka/streams/processor/WallclockTimestampExtractor.html). This extractor does not actually "extract" a timestamp from the consumed record but rather returns the current time in milliseconds from the system clock (think: `System.currentTimeMillis()`), which effectively means Streams will operate on the basis of the so-called **processing-time** of events.
> 
> You can also provide your own timestamp extractors, for instance to retrieve timestamps embedded in the payload of messages. If you cannot extract a valid timestamp, you can either throw an exception, return a negative timestamp, or estimate a timestamp. Returning a negative timestamp will result in data loss - the corresponding record will not be processed but silently dropped. If you want to estimate a new timestamp, you can use the value provided via `previousTimestamp` (i.e., a Kafka Streams timestamp estimation). Here is an example of a custom `TimestampExtractor` implementation:
>     
>     
>     import org.apache.kafka.clients.consumer.ConsumerRecord;
>     import org.apache.kafka.streams.processor.TimestampExtractor;
>     
>     // Extracts the embedded timestamp of a record (giving you "event-time" semantics).
>     public class MyEventTimeExtractor implements TimestampExtractor {
>     
>       @Override
>       public long extract(final ConsumerRecord<Object, Object> record, final long previousTimestamp) {
>         // `Foo` is your own custom class, which we assume has a method that returns
>         // the embedded timestamp (milliseconds since midnight, January 1, 1970 UTC).
>         long timestamp = -1;
>         final Foo myPojo = (Foo) record.value();
>         if (myPojo != null) {
>           timestamp = myPojo.getTimestampInMillis();
>         }
>         if (timestamp < 0) {
>           // Invalid timestamp!  Attempt to estimate a new timestamp,
>           // otherwise fall back to wall-clock time (processing-time).
>           if (previousTimestamp >= 0) {
>             return previousTimestamp;
>           } else {
>             return System.currentTimeMillis();
>           }
>         }
>       }
>     
>     }
> 
> You would then define the custom timestamp extractor in your Streams configuration as follows:
>     
>     
>     import java.util.Properties;
>     import org.apache.kafka.streams.StreamsConfig;
>     
>     Properties streamsConfiguration = new Properties();
>     streamsConfiguration.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, MyEventTimeExtractor.class);

## default.key.serde

> The default Serializer/Deserializer class for record keys, null unless set by user. Serialization and deserialization in Kafka Streams happens whenever data needs to be materialized, for example:
> 
>   * Whenever data is read from or written to a _Kafka topic_ (e.g., via the `StreamsBuilder#stream()` and `KStream#to()` methods).
>   * Whenever data is read from or written to a _state store_.
> 

> 
> This is discussed in more detail in [Data types and serialization](datatypes.html#streams-developer-guide-serdes).

## default.value.serde

> The default Serializer/Deserializer class for record values, null unless set by user. Serialization and deserialization in Kafka Streams happens whenever data needs to be materialized, for example:
> 
>   * Whenever data is read from or written to a _Kafka topic_ (e.g., via the `StreamsBuilder#stream()` and `KStream#to()` methods).
>   * Whenever data is read from or written to a _state store_.
> 

> 
> This is discussed in more detail in [Data types and serialization](datatypes.html#streams-developer-guide-serdes).

## rack.aware.assignment.non_overlap_cost

> This configuration sets the cost of moving a task from the original assignment computed either by `StickyTaskAssignor` or `HighAvailabilityTaskAssignor`. Together with `rack.aware.assignment.traffic_cost`, they control whether the optimizer favors minimizing cross rack traffic or minimizing the movement of tasks in the existing assignment. If this config is set to a larger value than `rack.aware.assignment.traffic_cost`, the optimizer will try to maintain the existing assignment computed by the task assignor. Note that the optimizer takes the ratio of these two configs into consideration of favoring maintaining existing assignment or minimizing traffic cost. For example, setting `rack.aware.assignment.non_overlap_cost` to 10 and `rack.aware.assignment.traffic_cost` to 1 is more likely to maintain existing assignment than setting `rack.aware.assignment.non_overlap_cost` to 100 and `rack.aware.assignment.traffic_cost` to 50. 
> 
> The default value is null which means default `non_overlap_cost` in different assignors will be used. In `StickyTaskAssignor`, it has a default value of 10 and `rack.aware.assignment.traffic_cost` has a default value of 1, which means maintaining stickiness is preferred in `StickyTaskAssignor`. In `HighAvailabilityTaskAssignor`, it has a default value of 1 and `rack.aware.assignment.traffic_cost` has a default value of 10, which means minimizing cross rack traffic is preferred in `HighAvailabilityTaskAssignor`. 

## rack.aware.assignment.strategy

> This configuration sets the strategy Kafka Streams uses for rack aware task assignment so that cross traffic from broker to client can be reduced. This config will only take effect when `broker.rack` is set on the brokers and `client.rack` is set on Kafka Streams side. There are two settings for this config: 
> 
>   * `none`. This is the default value which means rack aware task assignment will be disabled.
>   * `min_traffic`. This settings means that the rack aware task assigner will compute an assignment which tries to minimize cross rack traffic.
>   * `balance_subtopology`. This settings means that the rack aware task assigner will compute an assignment which will try to balance tasks from same subtopology to different clients and minimize cross rack traffic on top of that.
> 

> 
> This config can be used together with rack.aware.assignment.non_overlap_cost and rack.aware.assignment.traffic_cost to balance reducing cross rack traffic and maintaining the existing assignment. 

## rack.aware.assignment.tags

> This configuration sets a list of tag keys used to distribute standby replicas across Kafka Streams clients. When configured, Kafka Streams will make a best-effort to distribute the standby tasks over clients with different tag values. 
> 
> Tags for the Kafka Streams clients can be set via `client.tag.` prefix. Example: 
>     
>     
>     Client-1                                   | Client-2
>     _______________________________________________________________________
>     client.tag.zone: eu-central-1a             | client.tag.zone: eu-central-1b
>     client.tag.cluster: k8s-cluster1           | client.tag.cluster: k8s-cluster1
>     rack.aware.assignment.tags: zone,cluster   | rack.aware.assignment.tags: zone,cluster
>     
>     
>     Client-3                                   | Client-4
>     _______________________________________________________________________
>     client.tag.zone: eu-central-1a             | client.tag.zone: eu-central-1b
>     client.tag.cluster: k8s-cluster2           | client.tag.cluster: k8s-cluster2
>     rack.aware.assignment.tags: zone,cluster   | rack.aware.assignment.tags: zone,cluster
> 
> In the above example, we have four Kafka Streams clients across two zones (`eu-central-1a`, `eu-central-1b`) and across two clusters (`k8s-cluster1`, `k8s-cluster2`). For an active task located on `Client-1`, Kafka Streams will allocate a standby task on `Client-4`, since `Client-4` has a different `zone` and a different `cluster` than `Client-1`. 

## rack.aware.assignment.traffic_cost

> This configuration sets the cost of cross rack traffic. Together with `rack.aware.assignment.non_overlap_cost`, they control whether the optimizer favors minimizing cross rack traffic or minimizing the movement of tasks in the existing assignment. If this config is set to a larger value than `rack.aware.assignment.non_overlap_cost`, the optimizer will try to compute an assignment which minimize the cross rack traffic. Note that the optimizer takes the ratio of these two configs into consideration of favoring maintaining existing assignment or minimizing traffic cost. For example, setting `rack.aware.assignment.traffic_cost` to 10 and `rack.aware.assignment.non_overlap_cost` to 1 is more likely to minimize cross rack traffic than setting `rack.aware.assignment.traffic_cost` to 100 and `rack.aware.assignment.non_overlap_cost` to 50. 
> 
> The default value is null which means default traffic cost in different assignors will be used. In `StickyTaskAssignor`, it has a default value of 1 and `rack.aware.assignment.non_overlap_cost` has a default value of 10. In `HighAvailabilityTaskAssignor`, it has a default value of 10 and `rack.aware.assignment.non_overlap_cost` has a default value of 1. 

## log.summary.interval.ms

> This configuration controls the output interval for summary information. If greater or equal to 0, the summary log will be output according to the set time interval; If less than 0, summary output is disabled. 

## enable.metrics.push

> Kafka Streams metrics can be pushed to the brokers similar to client metrics. Additionally, Kafka Streams allows to enable/disable metric pushing for each embedded client individually. However, pushing Kafka Streams metrics requires that `enable.metric.push` is enabled on the main-consumer and admin client. 

## max.task.idle.ms

> This configuration controls how long Streams will wait to fetch data in order to provide in-order processing semantics. 
> 
> When processing a task that has multiple input partitions (as in a join or merge), Streams needs to choose which partition to process the next record from. When all input partitions have locally buffered data, Streams picks the partition whose next record has the lowest timestamp. This has the desirable effect of collating the input partitions in timestamp order, which is generally what you want in a streaming join or merge. However, when Streams does not have any data buffered locally for one of the partitions, it does not know whether the next record for that partition will have a lower or higher timestamp than the remaining partitions' records. 
> 
> There are two cases to consider: either there is data in that partition on the broker that Streams has not fetched yet, or Streams is fully caught up with that partition on the broker, and the producers simply haven't produced any new records since Streams polled the last batch. 
> 
> The default value of `0` causes Streams to delay processing a task when it detects that it has no locally buffered data for a partition, but there is data available on the brokers. Specifically, when there is an empty partition in the local buffer, but Streams has a non-zero lag for that partition. However, as soon as Streams catches up to the broker, it will continue processing, even if there is no data in one of the partitions. That is, it will not wait for new data to be _produced_. This default is designed to sacrifice some throughput in exchange for intuitively correct join semantics. 
> 
> Any config value greater than zero indicates the number of _extra_ milliseconds that Streams will wait if it has a caught-up but empty partition. In other words, this is the amount of time to wait for new data to be produced to the input partitions to ensure in-order processing of data in the event of a slow producer. 
> 
> The config value of `-1` indicates that Streams will never wait to buffer empty partitions before choosing the next record by timestamp, which achieves maximum throughput at the expense of introducing out-of-order processing. 

## max.warmup.replicas

> The maximum number of warmup replicas (extra standbys beyond the configured `num.standbys`) that can be assigned at once for the purpose of keeping the task available on one instance while it is warming up on another instance it has been reassigned to. Used to throttle how much extra broker traffic and cluster state can be used for high availability. Increasing this will allow Streams to warm up more tasks at once, speeding up the time for the reassigned warmups to restore sufficient state for them to be transitioned to active tasks. Must be at least 1. 
> 
> Note that one warmup replica corresponds to one [Stream Task](/40/streams/architecture#streams_architecture_tasks). Furthermore, note that each warmup task can only be promoted to an active task during a rebalance (normally during a so-called probing rebalance, which occur at a frequency specified by the `probing.rebalance.interval.ms` config). This means that the maximum rate at which active tasks can be migrated from one Kafka Streams instance to another instance can be determined by (`max.warmup.replicas` / `probing.rebalance.interval.ms`). 

## num.standby.replicas

> The number of standby replicas. Standby replicas are shadow copies of local state stores. Kafka Streams attempts to create the specified number of replicas per store and keep them up to date as long as there are enough instances running. Standby replicas are used to minimize the latency of task failover. A task that was previously running on a failed instance is preferred to restart on an instance that has standby replicas so that the local state store restoration process from its changelog can be minimized. Details about how Kafka Streams makes use of the standby replicas to minimize the cost of resuming tasks on failover can be found in the [State](../architecture.html#streams_architecture_state) section. 
> 
> Recommendation:
>     Increase the number of standbys to 1 to get instant fail-over, i.e., high-availability. Increasing the number of standbys requires more client-side storage space. For example, with 1 standby, 2x space is required.
> 
> Note:
>     If you enable n standby tasks, you need to provision n+1 `KafkaStreams` instances.

## num.stream.threads

> This specifies the number of stream threads in an instance of the Kafka Streams application. The stream processing code runs in these thread. For more information about Kafka Streams threading model, see [Threading Model](../architecture.html#streams_architecture_threads).

## probing.rebalance.interval.ms

> The maximum time to wait before triggering a rebalance to probe for warmup replicas that have restored enough to be considered caught up. Streams will only assign stateful active tasks to instances that are caught up and within the acceptable.recovery.lag, if any exist. Probing rebalances are used to query the latest total lag of warmup replicas and transition them to active tasks if ready. They will continue to be triggered as long as there are warmup tasks, and until the assignment is balanced. Must be at least 1 minute. 

## processing.exception.handler

> The processing exception handler allows you to manage exceptions triggered during the processing of a record. The implemented exception handler needs to return a `FAIL` or `CONTINUE` depending on the record and the exception thrown. Returning `FAIL` will signal that Streams should shut down and `CONTINUE` will signal that Streams should ignore the issue and continue processing. The following library built-in exception handlers are available:
> 
>   * [LogAndContinueProcessingExceptionHandler](/40/javadoc/org/apache/kafka/streams/errors/LogAndContinueProcessingExceptionHandler.html): This handler logs the processing exception and then signals the processing pipeline to continue processing more records. This log-and-skip strategy allows Kafka Streams to make progress instead of failing if there are records that fail to be processed.
>   * [LogAndFailProcessingExceptionHandler](/40/javadoc/org/apache/kafka/streams/errors/LogAndFailProcessingExceptionHandler.html). This handler logs the processing exception and then signals the processing pipeline to stop processing more records.
> 

> 
> You can also provide your own customized exception handler besides the library provided ones to meet your needs. For example, you can choose to forward corrupt records into a quarantine topic (think: a "dead letter queue") for further processing. To do this, use the Producer API to write a corrupted record directly to the quarantine topic. To be more concrete, you can create a separate `KafkaProducer` object outside the Streams client, and pass in this object as well as the dead letter queue topic name into the `Properties` map, which then can be retrieved from the `configure` function call. The drawback of this approach is that "manual" writes are side effects that are invisible to the Kafka Streams runtime library, so they do not benefit from the end-to-end processing guarantees of the Streams API:
>     
>     
>     public class SendToDeadLetterQueueExceptionHandler implements ProcessingExceptionHandler {
>         KafkaProducer<byte[], byte[]> dlqProducer;
>         String dlqTopic;
>     
>         @Override
>         public ProcessingHandlerResponse handle(final ErrorHandlerContext context,
>                                                 final Record record,
>                                                 final Exception exception) {
>     
>             log.warn("Exception caught during message processing, sending to the dead queue topic; " +
>                 "processor node: {}, taskId: {}, source topic: {}, source partition: {}, source offset: {}",
>                 context.processorNodeId(), context.taskId(), context.topic(), context.partition(), context.offset(),
>                 exception);
>     
>             dlqProducer.send(new ProducerRecord<>(dlqTopic, null, record.timestamp(), (byte[]) record.key(), (byte[]) record.value(), record.headers()));
>     
>             return ProcessingHandlerResponse.CONTINUE;
>         }
>     
>         @Override
>         public void configure(final Map<String, ?> configs) {
>             dlqProducer = .. // get a producer from the configs map
>             dlqTopic = .. // get the topic name from the configs map
>         }
>     }

## processing.guarantee

> The processing guarantee that should be used. Possible values are `"at_least_once"` (default) and `"exactly_once_v2"` (for EOS version 2). Deprecated config options are `"exactly_once"` (for EOS alpha), and `"exactly_once_beta"` (for EOS version 2). Using `"exactly_once_v2"` (or the deprecated `"exactly_once_beta"`) requires broker version 2.5 or newer, while using the deprecated `"exactly_once"` requires broker version 0.11.0 or newer. Note that if exactly-once processing is enabled, the default for parameter `commit.interval.ms` changes to 100ms. Additionally, consumers are configured with `isolation.level="read_committed"` and producers are configured with `enable.idempotence=true` per default. Note that by default exactly-once processing requires a cluster of at least three brokers what is the recommended setting for production. For development, you can change this configuration by adjusting broker setting `transaction.state.log.replication.factor` and `transaction.state.log.min.isr` to the number of brokers you want to use. For more details see [Processing Guarantees](../core-concepts#streams_processing_guarantee). 
> 
> Recommendation:
>     While it is technically possible to use EOS with any replication factor, using a replication factor lower than 3 effectively voids EOS. Thus it is strongly recommended to use a replication factor of 3 (together with `min.in.sync.replicas=2`). This recommendation applies to all topics (i.e. `__transaction_state`, `__consumer_offsets`, Kafka Streams internal topics, and user topics).

## processor.wrapper.class

> A class or class name implementing the `ProcessorWrapper` interface. This feature allows you to wrap any of the processors in the compiled topology, including both custom processor implementations and those created by Streams for DSL operators. This can be useful for logging or tracing implementations since it allows access to the otherwise-hidden processor context for DSL operators, and also allows for injecting additional debugging information to an entire application topology with just a single config. 
> 
> IMPORTANT: This MUST be passed in when creating the topology, and will not be applied unless passed in to the appropriate topology-building constructor. You should use the `StreamsBuilder#new(TopologyConfig)` constructor for DSL applications, and the `Topology#new(TopologyConfig)` constructor for PAPI applications. 

## replication.factor

> This specifies the replication factor of internal topics that Kafka Streams creates when local states are used or a stream is repartitioned for aggregation. Replication is important for fault tolerance. Without replication even a single broker failure may prevent progress of the stream processing application. It is recommended to use a similar replication factor as source topics.
> 
> Recommendation:
>     Increase the replication factor to 3 to ensure that the internal Kafka Streams topic can tolerate up to 2 broker failures. Note that you will require more storage space as well (3x with the replication factor of 3).

## rocksdb.config.setter

> The RocksDB configuration. Kafka Streams uses RocksDB as the default storage engine for persistent stores. To change the default configuration for RocksDB, you can implement `RocksDBConfigSetter` and provide your custom class via [rocksdb.config.setter](/40/javadoc/org/apache/kafka/streams/state/RocksDBConfigSetter.html).
> 
> Here is an example that adjusts the memory size consumed by RocksDB.
>     
>     
>     public static class CustomRocksDBConfig implements RocksDBConfigSetter {
>         // This object should be a member variable so it can be closed in RocksDBConfigSetter#close.
>         private org.rocksdb.Cache cache = new org.rocksdb.LRUCache(16 * 1024L * 1024L);
>     
>         @Override
>         public void setConfig(final String storeName, final Options options, final Map<String, Object> configs) {
>             // See #1 below.
>             BlockBasedTableConfig tableConfig = (BlockBasedTableConfig) options.tableFormatConfig();
>             tableConfig.setBlockCache(cache);
>             // See #2 below.
>             tableConfig.setBlockSize(16 * 1024L);
>             // See #3 below.
>             tableConfig.setCacheIndexAndFilterBlocks(true);
>             options.setTableFormatConfig(tableConfig);
>             // See #4 below.
>             options.setMaxWriteBufferNumber(2);
>         }
>     
>         @Override
>         public void close(final String storeName, final Options options) {
>             // See #5 below.
>             cache.close();
>         }
>     }
>     
>     Properties streamsSettings = new Properties();
>     streamsConfig.put(StreamsConfig.ROCKSDB_CONFIG_SETTER_CLASS_CONFIG, CustomRocksDBConfig.class);
> 
> Notes for example:
>     
> 
>   1. `BlockBasedTableConfig tableConfig = (BlockBasedTableConfig) options.tableFormatConfig();` Get a reference to the existing table config rather than create a new one, so you don't accidentally overwrite defaults such as the `BloomFilter`, which is an important optimization. 
>   2. `tableConfig.setBlockSize(16 * 1024L);` Modify the default [block size](https://github.com/apache/kafka/blob/2.3/streams/src/main/java/org/apache/kafka/streams/state/internals/RocksDBStore.java#L79) per these instructions from the [RocksDB GitHub](https://github.com/facebook/rocksdb/wiki/Memory-usage-in-RocksDB#indexes-and-filter-blocks).
>   3. `tableConfig.setCacheIndexAndFilterBlocks(true);` Do not let the index and filter blocks grow unbounded. For more information, see the [RocksDB GitHub](https://github.com/facebook/rocksdb/wiki/Block-Cache#caching-index-and-filter-blocks).
>   4. `options.setMaxWriteBufferNumber(2);` See the advanced options in the [RocksDB GitHub](https://github.com/facebook/rocksdb/blob/8dee8cad9ee6b70fd6e1a5989a8156650a70c04f/include/rocksdb/advanced_options.h#L103).
>   5. `cache.close();` To avoid memory leaks, you must close any objects you constructed that extend org.rocksdb.RocksObject. See [RocksJava docs](https://github.com/facebook/rocksdb/wiki/RocksJava-Basics#memory-management) for more details.
> 

 
 #### state.dir

> The state directory. Kafka Streams persists local states under the state directory. Each application has a subdirectory on its hosting machine that is located under the state directory. The name of the subdirectory is the application ID. The state stores associated with the application are created under this subdirectory. When running multiple instances of the same application on a single machine, this path must be unique for each such instance.
 
 #### task.assignor.class

> A task assignor class or class name implementing the `org.apache.kafka.streams.processor.assignment.TaskAssignor` interface. Defaults to the high-availability task assignor. One possible alternative implementation provided in Apache Kafka is the `org.apache.kafka.streams.processor.assignment.assignors.StickyTaskAssignor`, which was the default task assignor before KIP-441 and minimizes task movement at the cost of stateful task availability. Alternative implementations of the task assignment algorithm can be plugged into the application by implementing a custom `TaskAssignor` and setting this config to the name of the custom task assignor class. 
 
 #### topology.optimization

> A configuration telling Kafka Streams if it should optimize the topology and what optimizations to apply. Acceptable values are: `StreamsConfig.NO_OPTIMIZATION` (`none`), `StreamsConfig.OPTIMIZE` (`all`) or a comma separated list of specific optimizations: `StreamsConfig.REUSE_KTABLE_SOURCE_TOPICS` (`reuse.ktable.source.topics`), `StreamsConfig.MERGE_REPARTITION_TOPICS` (`merge.repartition.topics`), `StreamsConfig.SINGLE_STORE_SELF_JOIN` (`single.store.self.join`). 

We recommend listing specific optimizations in the config for production code so that the structure of your topology will not change unexpectedly during upgrades of the Streams library. 

These optimizations include moving/reducing repartition topics and reusing the source topic as the changelog for source KTables. These optimizations will save on network traffic and storage in Kafka without changing the semantics of your applications. Enabling them is recommended. 

Note that as of 2.3, you need to do two things to enable optimizations. In addition to setting this config to `StreamsConfig.OPTIMIZE`, you'll need to pass in your configuration properties when building your topology by using the overloaded `StreamsBuilder.build(Properties)` method. For example `KafkaStreams myStream = new KafkaStreams(streamsBuilder.build(properties), properties)`. 
 
 #### windowed.inner.class.serde

> Serde for the inner class of a windowed record. Must implement the org.apache.kafka.common.serialization.Serde interface. 

Note that this config is only used by plain consumer/producer clients that set a windowed de/serializer type via configs. For Kafka Streams applications that deal with windowed types, you must pass in the inner serde type when you instantiate the windowed serde object for your topology. 
 
 #### upgrade.from

> The version you are upgrading from. It is important to set this config when performing a rolling upgrade to certain versions, as described in the upgrade guide. You should set this config to the appropriate version before bouncing your instances and upgrading them to the newer version. Once everyone is on the newer version, you should remove this config and do a second rolling bounce. It is only necessary to set this config and follow the two-bounce upgrade path when upgrading from below version 2.0, or when upgrading to 2.4+ from any version lower than 2.4. 
 
 ### Kafka consumers, producer and admin client configuration parameters
 
 You can specify parameters for the Kafka [consumers](/40/javadoc/org/apache/kafka/clients/consumer/package-summary.html), [producers](/40/javadoc/org/apache/kafka/clients/producer/package-summary.html), and [admin client](/40/javadoc/org/apache/kafka/kafka/clients/admin/package-summary.html) that are used internally. The consumer, producer and admin client settings are defined by specifying parameters in a `StreamsConfig` instance.
 
 In this example, the Kafka [consumer session timeout](/40/javadoc/org/apache/kafka/clients/consumer/ConsumerConfig.html#SESSION_TIMEOUT_MS_CONFIG) is configured to be 60000 milliseconds in the Streams settings:
     
     
     Properties streamsSettings = new Properties();
     // Example of a "normal" setting for Kafka Streams
     streamsSettings.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka-broker-01:9092");
     // Customize the Kafka consumer settings of your Streams application
     streamsSettings.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 60000);
 
 #### Naming
 
 Some consumer, producer and admin client configuration parameters use the same parameter name, and Kafka Streams library itself also uses some parameters that share the same name with its embedded client. For example, `send.buffer.bytes` and `receive.buffer.bytes` are used to configure TCP buffers; `request.timeout.ms` and `retry.backoff.ms` control retries for client request. You can avoid duplicate names by prefix parameter names with `consumer.`, `producer.`, or `admin.` (e.g., `consumer.send.buffer.bytes` and `producer.send.buffer.bytes`).
     
     
     Properties streamsSettings = new Properties();
     // same value for consumer, producer, and admin client
     streamsSettings.put("PARAMETER_NAME", "value");
     // different values for consumer and producer
     streamsSettings.put("consumer.PARAMETER_NAME", "consumer-value");
     streamsSettings.put("producer.PARAMETER_NAME", "producer-value");
     streamsSettings.put("admin.PARAMETER_NAME", "admin-value");
     // alternatively, you can use
     streamsSettings.put(StreamsConfig.consumerPrefix("PARAMETER_NAME"), "consumer-value");
     streamsSettings.put(StreamsConfig.producerPrefix("PARAMETER_NAME"), "producer-value");
     streamsSettings.put(StreamsConfig.adminClientPrefix("PARAMETER_NAME"), "admin-value");
 
 You could further separate consumer configuration by adding different prefixes:
 
   * `main.consumer.` for main consumer which is the default consumer of stream source.
   * `restore.consumer.` for restore consumer which is in charge of state store recovery.
   * `global.consumer.` for global consumer which is used in global KTable construction.
 

 
 For example, if you only want to set restore consumer config without touching other consumers' settings, you could simply use `restore.consumer.` to set the config.
     
     
     Properties streamsSettings = new Properties();
     // same config value for all consumer types
     streamsSettings.put("consumer.PARAMETER_NAME", "general-consumer-value");
     // set a different restore consumer config. This would make restore consumer take restore-consumer-value,
     // while main consumer and global consumer stay with general-consumer-value
     streamsSettings.put("restore.consumer.PARAMETER_NAME", "restore-consumer-value");
     // alternatively, you can use
     streamsSettings.put(StreamsConfig.restoreConsumerPrefix("PARAMETER_NAME"), "restore-consumer-value");
 
 Same applied to `main.consumer.` and `main.consumer.`, if you only want to specify one consumer type config.
 
 Additionally, to configure the internal repartition/changelog topics, you could use the `topic.` prefix, followed by any of the standard topic configs.
     
     
     Properties streamsSettings = new Properties();
     // Override default for both changelog and repartition topics
     streamsSettings.put("topic.PARAMETER_NAME", "topic-value");
     // alternatively, you can use
     streamsSettings.put(StreamsConfig.topicPrefix("PARAMETER_NAME"), "topic-value");
 
 #### Default Values
 
 Kafka Streams uses different default values for some of the underlying client configs, which are summarized below. For detailed descriptions of these configs, see [Producer Configs](/40/documentation.html#producerconfigs) and [Consumer Configs](/40/documentation.html#consumerconfigs).
 
 Parameter Name | Corresponding Client | Streams Default  
 ---|---|---  
 auto.offset.reset | Consumer | `earliest`  
 linger.ms | Producer | `100`  
 max.poll.records | Consumer | `1000`  
 client.id | - | `<application.id>-<random-UUID>`  
   
 If EOS is enabled, other parameters have the following default values.
 
 Parameter Name | Corresponding Client | Streams Default  
 ---|---|---  
 transaction.timeout.ms | Producer | `10000`  
 delivery.timeout.ms | Producer | `Integer.MAX_VALUE`  
   
 ### Parameters controlled by Kafka Streams
 
 Some parameters are not configurable by the user. If you supply a value that is different from the default value, your value is ignored. Below is a list of some of these parameters.
 
 Parameter Name | Corresponding Client | Streams Default  
 ---|---|---  
 allow.auto.create.topics | Consumer | `false`  
 group.id | Consumer | `application.id`  
 enable.auto.commit | Consumer | `false`  
 partition.assignment.strategy | Consumer | `StreamsPartitionAssignor`  
   
 If EOS is enabled, other parameters are set with the following values.
 
 Parameter Name | Corresponding Client | Streams Default  
 ---|---|---  
 isolation.level | Consumer | `READ_COMMITTED`  
 enable.idempotence | Producer | `true`  
   
 ### client.id
 
 Kafka Streams uses the `client.id` parameter to compute derived client IDs for internal clients. If you don't set `client.id`, Kafka Streams sets it to `<application.id>-<random-UUID>`.
 
 This value will be used to derive the client IDs of the following internal clients.
 
 Client | client.id  
 ---|---  
 Consumer | `<client.id>-StreamThread-<threadIdx>-consumer`  
 Restore consumer | `<client.id>-StreamThread-<threadIdx>-restore-consumer`  
 Global consumer | `<client.id>-global-consumer`  
 Producer |  **For Non-EOS and EOS v2:**` <client.id>-StreamThread-<threadIdx>-producer`  
 **For EOS v1:**` <client.id>-StreamThread-<threadIdx>-<taskId>-producer`  
 Admin | `<client.id>-admin`  
   
 #### enable.auto.commit

> The consumer auto commit. To guarantee at-least-once processing semantics and turn off auto commits, Kafka Streams overrides this consumer config value to `false`. Consumers will only commit explicitly via _commitSync_ calls when the Kafka Streams library or a user decides to commit the current processing state.
 
 [Previous](/40/streams/developer-guide/write-streams) [Next](/40/streams/developer-guide/dsl-api)
 
   * [Documentation](/documentation)
   * [Kafka Streams](/streams)
   * [Developer Guide](/streams/developer-guide/)
 

