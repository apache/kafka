---
title: Processor API
description: 
weight: 4
tags: ['kafka', 'docs']
aliases: 
keywords: 
type: docs
---

<!--
 Licensed to the Apache Software Foundation (ASF) under one or more
 contributor license agreements.  See the NOTICE file distributed with
 this work for additional information regarding copyright ownership.
 The ASF licenses this file to You under the Apache License, Version 2.0
 (the "License"); you may not use this file except in compliance with
 the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
-->


The Processor API allows developers to define and connect custom processors and to interact with state stores. With the Processor API, you can define arbitrary stream processors that process one received record at a time, and connect these processors with their associated state stores to compose the processor topology that represents a customized processing logic.




# Overview

The Processor API can be used to implement both **stateless** as well as **stateful** operations, where the latter is achieved through the use of state stores.

**Tip**

**Combining the DSL and the Processor API:** You can combine the convenience of the DSL with the power and flexibility of the Processor API as described in the section [Applying processors and transformers (Processor API integration)](dsl-api.html#streams-developer-guide-dsl-process).

For a complete list of available API functionality, see the [Streams](/39/javadoc/org/apache/kafka/streams/package-summary.html) API docs.

# Defining a Stream Processor

A [stream processor](../core-concepts.html#streams_processor_node) is a node in the processor topology that represents a single processing step. With the Processor API, you can define arbitrary stream processors that processes one received record at a time, and connect these processors with their associated state stores to compose the processor topology.

You can define a customized stream processor by implementing the `Processor` interface, which provides the `process()` API method. The `process()` method is called on each of the received records.

The `Processor` interface also has an `init()` method, which is called by the Kafka Streams library during task construction phase. Processor instances should perform any required initialization in this method. The `init()` method passes in a `ProcessorContext` instance, which provides access to the metadata of the currently processed record, including its source Kafka topic and partition, its corresponding message offset, and further such information. You can also use this context instance to schedule a punctuation function (via `ProcessorContext#schedule()`), to forward a new record as a key-value pair to the downstream processors (via `ProcessorContext#forward()`), and to commit the current processing progress (via `ProcessorContext#commit()`). Any resources you set up in `init()` can be cleaned up in the `close()` method. Note that Kafka Streams may re-use a single `Processor` object by calling `init()` on it again after `close()`.

The `Processor` interface takes two sets of generic parameters: `KIn, VIn, KOut, VOut`. These define the input and output types that the processor implementation can handle. `KIn` and `VIn` define the key and value types that will be passed to `process()`. Likewise, `KOut` and `VOut` define the forwarded key and value types that `ProcessorContext#forward()` will accept. If your processor does not forward any records at all (or if it only forwards `null` keys or values), a best practice is to set the output generic type argument to `Void`. If it needs to forward multiple types that don't share a common superclass, you will have to set the output generic type argument to `Object`. 

Both the `Processor#process()` and the `ProcessorContext#forward()` methods handle records in the form of the `Record<K, V>` data class. This class gives you access to the main components of a Kafka record: the key, value, timestamp and headers. When forwarding records, you can use the constructor to create a new `Record` from scratch, or you can use the convenience builder methods to replace one of the `Record`'s properties and copy over the rest. For example, `inputRecord.withValue(newValue)` would copy the key, timestamp, and headers from `inputRecord` while setting the output record's value to `newValue`. Note that this does not mutate `inputRecord`, but instead creates a shallow copy. Beware that this is only a shallow copy, so if you plan to mutate the key, value, or headers elsewhere in the program, you will want to create a deep copy of those fields yourself. 

In addition to handling incoming records via `Processor#process()`, you have the option to schedule periodic invocation (called "punctuation") in your processor's `init()` method by calling `ProcessorContext#schedule()` and passing it a `Punctuator`. The `PunctuationType` determines what notion of time is used for the punctuation scheduling: either [stream-time](../core-concepts.html#streams_time) or wall-clock-time (by default, stream-time is configured to represent event-time via `TimestampExtractor`). When stream-time is used, `punctuate()` is triggered purely by data because stream-time is determined (and advanced forward) by the timestamps derived from the input data. When there is no new input data arriving, stream-time is not advanced and thus `punctuate()` is not called.

For example, if you schedule a `Punctuator` function every 10 seconds based on `PunctuationType.STREAM_TIME` and if you process a stream of 60 records with consecutive timestamps from 1 (first record) to 60 seconds (last record), then `punctuate()` would be called 6 times. This happens regardless of the time required to actually process those records. `punctuate()` would be called 6 times regardless of whether processing these 60 records takes a second, a minute, or an hour.

When wall-clock-time (i.e. `PunctuationType.WALL_CLOCK_TIME`) is used, `punctuate()` is triggered purely by the wall-clock time. Reusing the example above, if the `Punctuator` function is scheduled based on `PunctuationType.WALL_CLOCK_TIME`, and if these 60 records were processed within 20 seconds, `punctuate()` is called 2 times (one time every 10 seconds). If these 60 records were processed within 5 seconds, then no `punctuate()` is called at all. Note that you can schedule multiple `Punctuator` callbacks with different `PunctuationType` types within the same processor by calling `ProcessorContext#schedule()` multiple times inside `init()` method.

**Attention**

Stream-time is only advanced when Streams processes records. If there are no records to process, or if Streams is waiting for new records due to the [Task Idling](/documentation/#streamsconfigs_max.task.idle.ms) configuration, then the stream time will not advance and `punctuate()` will not be triggered if `PunctuationType.STREAM_TIME` was specified. This behavior is independent of the configured timestamp extractor, i.e., using `WallclockTimestampExtractor` does not enable wall-clock triggering of `punctuate()`.

**Example**

The following example `Processor` defines a simple word-count algorithm and the following actions are performed:

  * In the `init()` method, schedule the punctuation every 1000 time units (the time unit is normally milliseconds, which in this example would translate to punctuation every 1 second) and retrieve the local state store by its name "Counts".
  * In the `process()` method, upon each received record, split the value string into words, and update their counts into the state store (we will talk about this later in this section).
  * In the `punctuate()` method, iterate the local state store and send the aggregated counts to the downstream processor (we will talk about downstream processors later in this section), and commit the current stream state.


    
    
    public class WordCountProcessor implements Processor<String, String, String, String> {
        private KeyValueStore<String, Integer> kvStore;
    
        @Override
        public void init(final ProcessorContext<String, String> context) {
            context.schedule(Duration.ofSeconds(1), PunctuationType.STREAM_TIME, timestamp -> {
                try (final KeyValueIterator<String, Integer> iter = kvStore.all()) {
                    while (iter.hasNext()) {
                        final KeyValue<String, Integer> entry = iter.next();
                        context.forward(new Record<>(entry.key, entry.value.toString(), timestamp));
                    }
                }
            });
            kvStore = context.getStateStore("Counts");
        }
    
        @Override
        public void process(final Record<String, String> record) {
            final String[] words = record.value().toLowerCase(Locale.getDefault()).split("\W+");
    
            for (final String word : words) {
                final Integer oldValue = kvStore.get(word);
    
                if (oldValue == null) {
                    kvStore.put(word, 1);
                } else {
                    kvStore.put(word, oldValue + 1);
                }
            }
        }
    
        @Override
        public void close() {
            // close any resources managed by this processor
            // Note: Do not close any StateStores as these are managed by the library
        }
    }

**Note**

**Stateful processing with state stores:** The `WordCountProcessor` defined above can access the currently received record in its `process()` method, and it can leverage state stores to maintain processing states to, for example, remember recently arrived records for stateful processing needs like aggregations and joins. For more information, see the state stores documentation.

# Unit Testing Processors

Kafka Streams comes with a `test-utils` module to help you write unit tests for your processors [here](testing.html#unit-testing-processors). 

# State Stores

To implement a **stateful** `Processor` or `Transformer`, you must provide one or more state stores to the processor or transformer (_stateless_ processors or transformers do not need state stores). State stores can be used to remember recently received input records, to track rolling aggregates, to de-duplicate input records, and more. Another feature of state stores is that they can be [interactively queried](interactive-queries.html#streams-developer-guide-interactive-queries) from other applications, such as a NodeJS-based dashboard or a microservice implemented in Scala or Go.

The available state store types in Kafka Streams have fault tolerance enabled by default.

## Defining and creating a State Store

You can either use one of the available store types or implement your own custom store type. It's common practice to leverage an existing store type via the `Stores` factory.

Note that, when using Kafka Streams, you normally don't create or instantiate state stores directly in your code. Rather, you define state stores indirectly by creating a so-called `StoreBuilder`. This builder is used by Kafka Streams as a factory to instantiate the actual state stores locally in application instances when and where needed.

The following store types are available out of the box.  
  
<table>  
<tr>  
<th>

Store Type
</th>  
<th>

Storage Engine
</th>  
<th>

Fault-tolerant?
</th>  
<th>

Description
</th> </tr>  
<tr>  
<td>

Persistent `KeyValueStore<K, V>`
</td>  
<td>

RocksDB
</td>  
<td>

Yes (enabled by default)
</td>  
<td>



  * **The recommended store type for most use cases.**
  * Stores its data on local disk.
  * Storage capacity: managed local state can be larger than the memory (heap space) of an application instance, but must fit into the available local disk space.
  * RocksDB settings can be fine-tuned, see [RocksDB configuration](config-streams.html#streams-developer-guide-rocksdb-config).
  * Available [store variants](/39/javadoc/org/apache/kafka/streams/state/Stores.html#persistentKeyValueStore\(java.lang.String\)): timestamped key-value store, versioned key-value store, time window key-value store, session window key-value store.
  * Use [persistentTimestampedKeyValueStore](/39/javadoc/org/apache/kafka/streams/state/Stores.html#persistentTimestampedKeyValueStore\(java.lang.String\)) when you need a persistent key-(value/timestamp) store that supports put/get/delete and range queries.
  * Use [persistentVersionedKeyValueStore](/39/javadoc/org/apache/kafka/streams/state/Stores.html#persistentVersionedKeyValueStore\(java.lang.String,java.time.Duration\)) when you need a persistent, versioned key-(value/timestamp) store that supports put/get/delete and timestamped get operations.
  * Use [persistentWindowStore](/39/javadoc/org/apache/kafka/streams/state/Stores.html#persistentWindowStore\(java.lang.String,java.time.Duration,java.time.Duration,boolean\)) or [persistentTimestampedWindowStore](/39/javadoc/org/apache/kafka/streams/state/Stores.html#persistentTimestampedWindowStore\(java.lang.String,java.time.Duration,java.time.Duration,boolean\)) when you need a persistent timeWindowedKey-value or timeWindowedKey-(value/timestamp) store, respectively.
  * Use [persistentSessionStore](/39/javadoc/org/apache/kafka/streams/state/Stores.html#persistentSessionStore\(java.lang.String,java.time.Duration\)) when you need a persistent sessionWindowedKey-value store.


    
    
    // Creating a persistent key-value store:
    // here, we create a `KeyValueStore<String, Long>` named "persistent-counts".
    import org.apache.kafka.streams.state.StoreBuilder;
    import org.apache.kafka.streams.state.Stores;
    
    // Using a `KeyValueStoreBuilder` to build a `KeyValueStore`.
    StoreBuilder<KeyValueStore<String, Long>> countStoreSupplier =
      Stores.keyValueStoreBuilder(
        Stores.persistentKeyValueStore("persistent-counts"),
        Serdes.String(),
        Serdes.Long());
    KeyValueStore<String, Long> countStore = countStoreSupplier.build();


</td> </tr>  
<tr>  
<td>

In-memory `KeyValueStore<K, V>`
</td>  
<td>

-
</td>  
<td>

Yes (enabled by default)
</td>  
<td>



  * Stores its data in memory.
  * Storage capacity: managed local state must fit into memory (heap space) of an application instance.
  * Useful when application instances run in an environment where local disk space is either not available or local disk space is wiped in-between app instance restarts.
  * Available [store variants](/39/javadoc/org/apache/kafka/streams/state/Stores.html#inMemoryKeyValueStore-java.lang.String-): time window key-value store, session window key-value store.
  * Use [TimestampedKeyValueStore](/39/javadoc/org/apache/kafka/streams/state/TimestampedKeyValueStore.html) when you need a key-(value/timestamp) store that supports put/get/delete and range queries.
  * Use [TimestampedWindowStore](/39/javadoc/org/apache/kafka/streams/state/TimestampedWindowStore.html) when you need to store windowedKey-(value/timestamp) pairs.
  * There is no built-in in-memory, versioned key-value store at this time.


    
    
    // Creating an in-memory key-value store:
    // here, we create a `KeyValueStore<String, Long>` named "inmemory-counts".
    import org.apache.kafka.streams.state.StoreBuilder;
    import org.apache.kafka.streams.state.Stores;
    
    // Using a `KeyValueStoreBuilder` to build a `KeyValueStore`.
    StoreBuilder<KeyValueStore<String, Long>> countStoreSupplier =
      Stores.keyValueStoreBuilder(
        Stores.inMemoryKeyValueStore("inmemory-counts"),
        Serdes.String(),
        Serdes.Long());
    KeyValueStore<String, Long> countStore = countStoreSupplier.build();


</td> </tr> </table>

## Fault-tolerant State Stores

To make state stores fault-tolerant and to allow for state store migration without data loss, a state store can be continuously backed up to a Kafka topic behind the scenes. For example, to migrate a stateful stream task from one machine to another when [elastically adding or removing capacity from your application](running-app.html#streams-developer-guide-execution-scaling). This topic is sometimes referred to as the state store's associated _changelog topic_ , or its _changelog_. For example, if you experience machine failure, the state store and the application's state can be fully restored from its changelog. You can enable or disable this backup feature for a state store.

Fault-tolerant state stores are backed by a [compacted](https://kafka.apache.org/documentation.html#compaction) changelog topic. The purpose of compacting this topic is to prevent the topic from growing indefinitely, to reduce the storage consumed in the associated Kafka cluster, and to minimize recovery time if a state store needs to be restored from its changelog topic.

Fault-tolerant windowed state stores are backed by a topic that uses both compaction and deletion. Because of the structure of the message keys that are being sent to the changelog topics, this combination of deletion and compaction is required for the changelog topics of window stores. For window stores, the message keys are composite keys that include the "normal" key and window timestamps. For these types of composite keys it would not be sufficient to only enable compaction to prevent a changelog topic from growing out of bounds. With deletion enabled, old windows that have expired will be cleaned up by Kafka's log cleaner as the log segments expire. The default retention setting is `Windows#maintainMs()` \+ 1 day. You can override this setting by specifying `StreamsConfig.WINDOW_STORE_CHANGE_LOG_ADDITIONAL_RETENTION_MS_CONFIG` in the `StreamsConfig`.

When you open an `Iterator` from a state store you must call `close()` on the iterator when you are done working with it to reclaim resources; or you can use the iterator from within a try-with-resources statement. If you do not close an iterator, you may encounter an OOM error.

## Enable or Disable Fault Tolerance of State Stores (Store Changelogs)

You can enable or disable fault tolerance for a state store by enabling or disabling the change logging of the store through `enableLogging()` and `disableLogging()`. You can also fine-tune the associated topic's configuration if needed.

Example for disabling fault-tolerance:
    
    
    import org.apache.kafka.streams.state.StoreBuilder;
    import org.apache.kafka.streams.state.Stores;
    
    StoreBuilder<KeyValueStore<String, Long>> countStoreSupplier = Stores.keyValueStoreBuilder(
      Stores.persistentKeyValueStore("Counts"),
        Serdes.String(),
        Serdes.Long())
      .withLoggingDisabled(); // disable backing up the store to a changelog topic

Attention

If the changelog is disabled then the attached state store is no longer fault tolerant and it can't have any [standby replicas](config-streams.html#streams-developer-guide-standby-replicas).

Here is an example for enabling fault tolerance, with additional changelog-topic configuration: You can add any log config from [kafka.log.LogConfig](https://github.com/apache/kafka/blob/trunk/core/src/main/scala/kafka/log/LogConfig.scala). Unrecognized configs will be ignored.
    
    
    import org.apache.kafka.streams.state.StoreBuilder;
    import org.apache.kafka.streams.state.Stores;
    
    Map<String, String> changelogConfig = new HashMap();
    // override min.insync.replicas
    changelogConfig.put(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "1")
    
    StoreBuilder<KeyValueStore<String, Long>> countStoreSupplier = Stores.keyValueStoreBuilder(
      Stores.persistentKeyValueStore("Counts"),
        Serdes.String(),
        Serdes.Long())
      .withLoggingEnabled(changelogConfig); // enable changelogging, with custom changelog settings

## Timestamped State Stores

KTables always store timestamps by default. A timestamped state store improves stream processing semantics and enables handling out-of-order data in source KTables, detecting out-of-order joins and aggregations, and getting the timestamp of the latest update in an Interactive Query.

You can query timestamped state stores both with and without a timestamp.

**Upgrade note:** All users upgrade with a single rolling bounce per instance. 

  * For Processor API users, nothing changes in existing applications, and you have the option of using the timestamped stores.
  * For DSL operators, store data is upgraded lazily in the background.
  * No upgrade happens if you provide a custom XxxBytesStoreSupplier, but you can opt-in by implementing the [TimestampedBytesStore](/39/javadoc/org/apache/kafka/streams/state/TimestampedBytesStore.html) interface. In this case, the old format is retained, and Streams uses a proxy store that removes/adds timestamps on read/write.



## Versioned Key-Value State Stores

Versioned key-value state stores are available since Kafka Streams 3.5. Rather than storing a single record version (value and timestamp) per key, versioned state stores may store multiple record versions per key. This allows versioned state stores to support timestamped retrieval operations to return the latest record (per key) as of a specified timestamp.

You can create a persistent, versioned state store by passing a [VersionedBytesStoreSupplier](/39/javadoc/org/apache/kafka/streams/state/Stores.html#persistentVersionedKeyValueStore\(java.lang.String,java.time.Duration\)) to the [versionedKeyValueStoreBuilder](/39/javadoc/org/apache/kafka/streams/state/Stores.html#versionedKeyValueStoreBuilder\(java.lang.String,java.time.Duration\)), or by implementing your own [VersionedKeyValueStore](/39/javadoc/org/apache/kafka/streams/state/VersionedKeyValueStore.html).

Each versioned store has an associated, fixed-duration _history retention_ parameter which specifies long old record versions should be kept for. In particular, a versioned store guarantees to return accurate results for timestamped retrieval operations where the timestamp being queried is within history retention of the current observed stream time.

History retention also doubles as its _grace period_ , which determines how far back in time out-of-order writes to the store will be accepted. A versioned store will not accept writes (inserts, updates, or deletions) if the timestamp associated with the write is older than the current observed stream time by more than the grace period. Stream time in this context is tracked per-partition, rather than per-key, which means it's important that grace period (i.e., history retention) be set high enough to accommodate a record with one key arriving out-of-order relative to a record for another key.

Because the memory footprint of versioned key-value stores is higher than that of non-versioned key-value stores, you may want to adjust your [RocksDB memory settings](memory-mgmt.html#streams-developer-guide-memory-management-rocksdb) accordingly. Benchmarking your application with versioned stores is also advised as performance is expected to be worse than when using non-versioned stores.

Versioned stores do not support caching or interactive queries at this time. Also, window stores and global tables may not be versioned.

**Upgrade note:** Versioned state stores are opt-in only; no automatic upgrades from non-versioned to versioned stores will take place. 

Upgrades are supported from persistent, non-versioned key-value stores to persistent, versioned key-value stores as long as the original store has the same changelog topic format as the versioned store being upgraded to. Both persistent [key-value stores](/39/javadoc/org/apache/kafka/streams/state/Stores.html#persistentKeyValueStore\(java.lang.String\)) and [timestamped key-value stores](/39/javadoc/org/apache/kafka/streams/state/Stores.html#persistentTimestampedKeyValueStore\(java.lang.String\)) share the same changelog topic format as [persistent versioned key-value stores](/39/javadoc/org/apache/kafka/streams/state/Stores.html#persistentVersionedKeyValueStore\(java.lang.String,java.time.Duration\)), and therefore both are eligible for upgrades.

If you wish to upgrade an application using persistent, non-versioned key-value stores to use persistent, versioned key-value stores instead, you can perform the following procedure:

  * Stop all application instances, and [clear any local state directories](app-reset-tool.html#streams-developer-guide-reset-local-environment) for the store(s) being upgraded.
  * Update your application code to use versioned stores where desired.
  * Update your changelog topic configs, for the relevant state stores, to set the value of `min.compaction.lag.ms` to be at least your desired history retention. History retention plus one day is recommended as buffer for the use of broker wall clock time during compaction.
  * Restart your application instances and allow time for the versioned stores to rebuild state from changelog.



## ReadOnly State Stores

A read-only state store materialized the data from its input topic. It also uses the input topic for fault-tolerance, and thus does not have an additional changelog topic (the input topic is re-used as changelog). Thus, the input topic should be configured with [log compaction](https://kafka.apache.org/documentation.html#compaction). Note that no other processor should modify the content of the state store, and the only writer should be the associated "state update processor"; other processors may read the content of the read-only store.

**note:** beware of the partitioning requirements when using read-only state stores for lookups during processing. You might want to make sure the original changelog topic is co-partitioned with the processors reading the read-only statestore.

## Implementing Custom State Stores

You can use the built-in state store types or implement your own. The primary interface to implement for the store is `org.apache.kafka.streams.processor.StateStore`. Kafka Streams also has a few extended interfaces such as `KeyValueStore` and `VersionedKeyValueStore`.

Note that your customized `org.apache.kafka.streams.processor.StateStore` implementation also needs to provide the logic on how to restore the state via the `org.apache.kafka.streams.processor.StateRestoreCallback` or `org.apache.kafka.streams.processor.BatchingStateRestoreCallback` interface. Details on how to instantiate these interfaces can be found in the [javadocs](/39/javadoc/org/apache/kafka/streams/processor/StateStore.html).

You also need to provide a "builder" for the store by implementing the `org.apache.kafka.streams.state.StoreBuilder` interface, which Kafka Streams uses to create instances of your store.

# Accessing Processor Context

As we have mentioned in the Defining a Stream Processor section, a `ProcessorContext` control the processing workflow, such as scheduling a punctuation function, and committing the current processed state.

This object can also be used to access the metadata related with the application like `applicationId`, `taskId`, and `stateDir`, and also record related metadata as `topic`, `partition`, `offset`, `timestamp` and `headers`.

Here is an example implementation of how to add a new header to the record:
    
    
    public void process(String key, String value) {
    
        // add a header to the elements
        context().headers().add.("key", "value");
    }

# Connecting Processors and State Stores

Now that a processor (WordCountProcessor) and the state stores have been defined, you can construct the processor topology by connecting these processors and state stores together by using the `Topology` instance. In addition, you can add source processors with the specified Kafka topics to generate input data streams into the topology, and sink processors with the specified Kafka topics to generate output data streams out of the topology.

Here is an example implementation:
    
    
    Topology builder = new Topology();
    // add the source processor node that takes Kafka topic "source-topic" as input
    builder.addSource("Source", "source-topic")
        // add the WordCountProcessor node which takes the source processor as its upstream processor
        .addProcessor("Process", () -> new WordCountProcessor(), "Source")
        // add the count store associated with the WordCountProcessor processor
        .addStateStore(countStoreBuilder, "Process")
        // add the sink processor node that takes Kafka topic "sink-topic" as output
        // and the WordCountProcessor node as its upstream processor
        .addSink("Sink", "sink-topic", "Process");

Here is a quick explanation of this example:

  * A source processor node named `"Source"` is added to the topology using the `addSource` method, with one Kafka topic `"source-topic"` fed to it.
  * A processor node named `"Process"` with the pre-defined `WordCountProcessor` logic is then added as the downstream processor of the `"Source"` node using the `addProcessor` method.
  * A predefined persistent key-value state store is created and associated with the `"Process"` node, using `countStoreBuilder`.
  * A sink processor node is then added to complete the topology using the `addSink` method, taking the `"Process"` node as its upstream processor and writing to a separate `"sink-topic"` Kafka topic (note that users can also use another overloaded variant of `addSink` to dynamically determine the Kafka topic to write to for each received record from the upstream processor).



In some cases, it may be more convenient to add and connect a state store at the same time as you add the processor to the topology. This can be done by implementing `ConnectedStoreProvider#stores()` on the `ProcessorSupplier` instead of calling `Topology#addStateStore()`, like this: 
    
    
    Topology builder = new Topology();
    // add the source processor node that takes Kafka "source-topic" as input
    builder.addSource("Source", "source-topic")
        // add the WordCountProcessor node which takes the source processor as its upstream processor.
        // the ProcessorSupplier provides the count store associated with the WordCountProcessor
        .addProcessor("Process", new ProcessorSupplier<String, String, String, String>() {
            public Processor<String, String, String, String> get() {
                return new WordCountProcessor();
            }
    
            public Set<StoreBuilder<?>> stores() {
                final StoreBuilder<KeyValueStore<String, Long>> countsStoreBuilder =
                    Stores
                        .keyValueStoreBuilder(
                            Stores.persistentKeyValueStore("Counts"),
                            Serdes.String(),
                            Serdes.Long()
                        );
                return Collections.singleton(countsStoreBuilder);
            }
        }, "Source")
        // add the sink processor node that takes Kafka topic "sink-topic" as output
        // and the WordCountProcessor node as its upstream processor
        .addSink("Sink", "sink-topic", "Process");

This allows for a processor to "own" state stores, effectively encapsulating their usage from the user wiring the topology. Multiple processors that share a state store may provide the same store with this technique, as long as the `StoreBuilder` is the same `instance`.

In these topologies, the `"Process"` stream processor node is considered a downstream processor of the `"Source"` node, and an upstream processor of the `"Sink"` node. As a result, whenever the `"Source"` node forwards a newly fetched record from Kafka to its downstream `"Process"` node, the `WordCountProcessor#process()` method is triggered to process the record and update the associated state store. Whenever `context#forward()` is called in the `WordCountProcessor#punctuate()` method, the aggregate key-value pair will be sent via the `"Sink"` processor node to the Kafka topic `"sink-topic"`. Note that in the `WordCountProcessor` implementation, you must refer to the same store name `"Counts"` when accessing the key-value store, otherwise an exception will be thrown at runtime, indicating that the state store cannot be found. If the state store is not associated with the processor in the `Topology` code, accessing it in the processor's `init()` method will also throw an exception at runtime, indicating the state store is not accessible from this processor.

Note that the `Topology#addProcessor` function takes a `ProcessorSupplier` as argument, and that the supplier pattern requires that a new `Processor` instance is returned each time `ProcessorSupplier#get()` is called. Creating a single `Processor` object and returning the same object reference in `ProcessorSupplier#get()` would be a violation of the supplier pattern and leads to runtime exceptions. So remember not to provide a singleton `Processor` instance to `Topology`. The `ProcessorSupplier` should always generate a new instance each time `ProcessorSupplier#get()` gets called.

Now that you have fully defined your processor topology in your application, you can proceed to [running the Kafka Streams application](running-app.html#streams-developer-guide-execution).

  * [Documentation](/documentation)
  * [Kafka Streams](/documentation/streams)
  * [Developer Guide](/documentation/streams/developer-guide/)


