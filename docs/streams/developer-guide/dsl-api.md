---
title: Streams DSL
description: 
weight: 3
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


The Kafka Streams DSL (Domain Specific Language) is built on top of the Streams Processor API. It is the recommended for most users, especially beginners. Most data processing operations can be expressed in just a few lines of DSL code.

      * Aggregating
      * Joining
        * Join co-partitioning requirements
        * KStream-KStream Join
        * KTable-KTable Equi-Join
        * KTable-KTable Foreign-Key Join
        * KStream-KTable Join
        * KStream-GlobalKTable Join
      * Windowing
        * Hopping time windows
        * Tumbling time windows
        * Sliding time windows
        * Session Windows
        * Window Final Results
    * Applying processors and transformers (Processor API integration)
  * Naming Operators in a Streams DSL application
  * Controlling KTable update rate
  * Using timestamp-based semantics for table processors
  * Writing streams back to Kafka
  * Testing a Streams application
  * Kafka Streams DSL for Scala
    * Sample Usage
    * Implicit Serdes
    * User-Defined Serdes



# Overview

In comparison to the [Processor API](processor-api.html#streams-developer-guide-processor-api), only the DSL supports:

  * Built-in abstractions for [streams and tables](../core-concepts.html#streams_concepts_duality) in the form of KStream, KTable, and GlobalKTable. Having first-class support for streams and tables is crucial because, in practice, most use cases require not just either streams or databases/tables, but a combination of both. For example, if your use case is to create a customer 360-degree view that is updated in real-time, what your application will be doing is transforming many input _streams_ of customer-related events into an output _table_ that contains a continuously updated 360-degree view of your customers.
  * Declarative, functional programming style with stateless transformations (e.g. `map` and `filter`) as well as stateful transformations such as aggregations (e.g. `count` and `reduce`), joins (e.g. `leftJoin`), and windowing (e.g. session windows).



With the DSL, you can define [processor topologies](../core-concepts.html#streams_topology) (i.e., the logical processing plan) in your application. The steps to accomplish this are:

  1. Specify one or more input streams that are read from Kafka topics.
  2. Compose transformations on these streams.
  3. Write the resulting output streams back to Kafka topics, or expose the processing results of your application directly to other applications through [interactive queries](interactive-queries.html#streams-developer-guide-interactive-queries) (e.g., via a REST API).



After the application is run, the defined processor topologies are continuously executed (i.e., the processing plan is put into action). A step-by-step guide for writing a stream processing application using the DSL is provided below.

For a complete list of available API functionality, see also the [Streams](/37/javadoc/org/apache/kafka/streams/package-summary.html) API docs.

### KStream

Only the **Kafka Streams DSL** has the notion of a `KStream`. 

A **KStream** is an abstraction of a **record stream** , where each data record represents a self-contained datum in the unbounded data set. Using the table analogy, data records in a record stream are always interpreted as an "INSERT" \-- think: adding more entries to an append-only ledger -- because no record replaces an existing row with the same key. Examples are a credit card transaction, a page view event, or a server log entry. 

To illustrate, let's imagine the following two data records are being sent to the stream: 

("alice", 1) --> ("alice", 3)

If your stream processing application were to sum the values per user, it would return `4` for `alice`. Why? Because the second data record would not be considered an update of the previous record. Compare this behavior of KStream to `KTable` below, which would return `3` for `alice`. 

### KTable

Only the **Kafka Streams DSL** has the notion of a `KTable`. 

A **KTable** is an abstraction of a **changelog stream** , where each data record represents an update. More precisely, the value in a data record is interpreted as an "UPDATE" of the last value for the same record key, if any (if a corresponding key doesn't exist yet, the update will be considered an INSERT). Using the table analogy, a data record in a changelog stream is interpreted as an UPSERT aka INSERT/UPDATE because any existing row with the same key is overwritten. Also, `null` values are interpreted in a special way: a record with a `null` value represents a "DELETE" or tombstone for the record's key. 

To illustrate, let's imagine the following two data records are being sent to the stream: 

("alice", 1) --> ("alice", 3) 

If your stream processing application were to sum the values per user, it would return `3` for `alice`. Why? Because the second data record would be considered an update of the previous record. 

**Effects of Kafka's log compaction:** Another way of thinking about KStream and KTable is as follows: If you were to store a KTable into a Kafka topic, you'd probably want to enable Kafka's [log compaction](http://kafka.apache.org/documentation.html#compaction) feature, e.g. to save storage space. 

However, it would not be safe to enable log compaction in the case of a KStream because, as soon as log compaction would begin purging older data records of the same key, it would break the semantics of the data. To pick up the illustration example again, you'd suddenly get a `3` for `alice` instead of a `4` because log compaction would have removed the `("alice", 1)` data record. Hence log compaction is perfectly safe for a KTable (changelog stream) but it is a mistake for a KStream (record stream). 

We have already seen an example of a changelog stream in the section [streams and tables](../core-concepts.html#streams_concepts_duality). Another example are change data capture (CDC) records in the changelog of a relational database, representing which row in a database table was inserted, updated, or deleted. 

KTable also provides an ability to look up _current_ values of data records by keys. This table-lookup functionality is available through **join operations** (see also **Joining** in the Developer Guide) as well as through **Interactive Queries**. 

### GlobalKTable

Only the **Kafka Streams DSL** has the notion of a **GlobalKTable**.

Like a **KTable** , a **GlobalKTable** is an abstraction of a **changelog stream** , where each data record represents an update. 

A GlobalKTable differs from a KTable in the data that they are being populated with, i.e. which data from the underlying Kafka topic is being read into the respective table. Slightly simplified, imagine you have an input topic with 5 partitions. In your application, you want to read this topic into a table. Also, you want to run your application across 5 application instances for **maximum parallelism**. 

  * If you read the input topic into a **KTable** , then the "local" KTable instance of each application instance will be populated with data **from only 1 partition** of the topic's 5 partitions. 
  * If you read the input topic into a **GlobalKTable** , then the local GlobalKTable instance of each application instance will be populated with data **from all partitions of the topic**. 



GlobalKTable provides the ability to look up _current_ values of data records by keys. This table-lookup functionality is available through `join operations`. Note that a GlobalKTable has **no** notion of time in contrast to a KTable. 

Benefits of global tables:

  * More convenient and/or efficient **joins** : Notably, global tables allow you to perform star joins, they support "foreign-key" lookups (i.e., you can lookup data in the table not just by record key, but also by data in the record values), and they are more efficient when chaining multiple joins. Also, when joining against a global table, the input data does not need to be **co-partitioned**. 
  * Can be used to "broadcast" information to all the running instances of your application. 



Downsides of global tables:

  * Increased local storage consumption compared to the (partitioned) KTable because the entire topic is tracked.
  * Increased network and Kafka broker load compared to the (partitioned) KTable because the entire topic is read.



# Creating source streams from Kafka

You can easily read data from Kafka topics into your application. The following operations are supported.  
  
<table>  
<tr>  
<th>

Reading from Kafka
</th>  
<th>

Description
</th> </tr>  
<tr>  
<td>



**Stream**

  * _input topics_ -> KStream


</td>  
<td>



Creates a KStream from the specified Kafka input topics and interprets the data as a record stream. A `KStream` represents a _partitioned_ record stream. [(details)](/37/javadoc/org/apache/kafka/streams/StreamsBuilder.html#stream\(java.lang.String\))

In the case of a KStream, the local KStream instance of every application instance will be populated with data from only **a subset** of the partitions of the input topic. Collectively, across all application instances, all input topic partitions are read and processed.
    
    
    import org.apache.kafka.common.serialization.Serdes;
    import org.apache.kafka.streams.StreamsBuilder;
    import org.apache.kafka.streams.kstream.KStream;
    
    StreamsBuilder builder = new StreamsBuilder();
    
    KStream<String, Long> wordCounts = builder.stream(
        "word-counts-input-topic", /* input topic */
        Consumed.with(
          Serdes.String(), /* key serde */
          Serdes.Long()   /* value serde */
        );

If you do not specify Serdes explicitly, the default Serdes from the [configuration](config-streams.html#streams-developer-guide-configuration) are used.

You **must specify Serdes explicitly** if the key or value types of the records in the Kafka input topics do not match the configured default Serdes. For information about configuring default Serdes, available Serdes, and implementing your own custom Serdes see [Data Types and Serialization](datatypes.html#streams-developer-guide-serdes).

Several variants of `stream` exist. For example, you can specify a regex pattern for input topics to read from (note that all matching topics will be part of the same input topic group, and the work will not be parallelized for different topics if subscribed to in this way).


</td> </tr>  
<tr>  
<td>



**Table**

  * _input topic_ -> KTable


</td>  
<td>



Reads the specified Kafka input topic into a KTable. The topic is interpreted as a changelog stream, where records with the same key are interpreted as UPSERT aka INSERT/UPDATE (when the record value is not `null`) or as DELETE (when the value is `null`) for that key. [(details)](/37/javadoc/org/apache/kafka/streams/StreamsBuilder.html#table-java.lang.String\(java.lang.String\))

In the case of a KTable, the local KTable instance of every application instance will be populated with data from only **a subset** of the partitions of the input topic. Collectively, across all application instances, all input topic partitions are read and processed.

You must provide a name for the table (more precisely, for the internal [state store](../architecture.html#streams_architecture_state) that backs the table). This is required for supporting [interactive queries](interactive-queries.html#streams-developer-guide-interactive-queries) against the table. When a name is not provided the table will not be queryable and an internal name will be provided for the state store.

If you do not specify Serdes explicitly, the default Serdes from the [configuration](config-streams.html#streams-developer-guide-configuration) are used.

You **must specify Serdes explicitly** if the key or value types of the records in the Kafka input topics do not match the configured default Serdes. For information about configuring default Serdes, available Serdes, and implementing your own custom Serdes see [Data Types and Serialization](datatypes.html#streams-developer-guide-serdes).

Several variants of `table` exist, for example to specify the `auto.offset.reset` policy to be used when reading from the input topic.


</td> </tr>  
<tr>  
<td>



**Global Table**

  * _input topic_ -> GlobalKTable


</td>  
<td>



Reads the specified Kafka input topic into a GlobalKTable. The topic is interpreted as a changelog stream, where records with the same key are interpreted as UPSERT aka INSERT/UPDATE (when the record value is not `null`) or as DELETE (when the value is `null`) for that key. [(details)](/37/javadoc/org/apache/kafka/streams/StreamsBuilder.html#globalTable-java.lang.String\(java.lang.String\))

In the case of a GlobalKTable, the local GlobalKTable instance of every application instance will be populated with data from **all** the partitions of the input topic.

You must provide a name for the table (more precisely, for the internal [state store](../architecture.html#streams_architecture_state) that backs the table). This is required for supporting [interactive queries](interactive-queries.html#streams-developer-guide-interactive-queries) against the table. When a name is not provided the table will not be queryable and an internal name will be provided for the state store.
    
    
    import org.apache.kafka.common.serialization.Serdes;
    import org.apache.kafka.streams.StreamsBuilder;
    import org.apache.kafka.streams.kstream.GlobalKTable;
    
    StreamsBuilder builder = new StreamsBuilder();
    
    GlobalKTable<String, Long> wordCounts = builder.globalTable(
        "word-counts-input-topic",
        Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as(
          "word-counts-global-store" /* table/store name */)
          .withKeySerde(Serdes.String()) /* key serde */
          .withValueSerde(Serdes.Long()) /* value serde */
        );

You **must specify Serdes explicitly** if the key or value types of the records in the Kafka input topics do not match the configured default Serdes. For information about configuring default Serdes, available Serdes, and implementing your own custom Serdes see [Data Types and Serialization](datatypes.html#streams-developer-guide-serdes).

Several variants of `globalTable` exist to e.g. specify explicit Serdes.


</td> </tr> </table>

# Transform a stream

The KStream and KTable interfaces support a variety of transformation operations. Each of these operations can be translated into one or more connected processors into the underlying processor topology. Since KStream and KTable are strongly typed, all of these transformation operations are defined as generic functions where users could specify the input and output data types.

Some KStream transformations may generate one or more KStream objects, for example: \- `filter` and `map` on a KStream will generate another KStream \- `split` on KStream can generate multiple KStreams

Some others may generate a KTable object, for example an aggregation of a KStream also yields a KTable. This allows Kafka Streams to continuously update the computed value upon arrivals of [out-of-order records](../core-concepts.html#streams_concepts_aggregations) after it has already been produced to the downstream transformation operators.

All KTable transformation operations can only generate another KTable. However, the Kafka Streams DSL does provide a special function that converts a KTable representation into a KStream. All of these transformation methods can be chained together to compose a complex processor topology.

These transformation operations are described in the following subsections:

  * Stateless transformations
  * Stateful transformations



## Stateless transformations

Stateless transformations do not require state for processing and they do not require a state store associated with the stream processor. Kafka 0.11.0 and later allows you to materialize the result from a stateless `KTable` transformation. This allows the result to be queried through [interactive queries](interactive-queries.html#streams-developer-guide-interactive-queries). To materialize a `KTable`, each of the below stateless operations [can be augmented](interactive-queries.html#streams-developer-guide-interactive-queries-local-key-value-stores) with an optional `queryableStoreName` argument.  
  
<table>  
<tr>  
<th>

Transformation
</th>  
<th>

Description
</th> </tr>  
<tr>  
<td>



**Branch**

  * KStream -> BranchedKStream


</td>  
<td>



Branch (or split) a `KStream` based on the supplied predicates into one or more `KStream` instances. ([details](/37/javadoc/org/apache/kafka/streams/kstream/KStream.html#split\(\)))

Predicates are evaluated in order. A record is placed to one and only one output stream on the first match: if the n-th predicate evaluates to true, the record is placed to n-th stream. If a record does not match any predicates, it will be routed to the default branch, or dropped if no default branch is created.

Branching is useful, for example, to route records to different downstream topics.
    
    
    KStream<String, Long> stream = ...;
    Map<String, KStream<String, Long>> branches =
        stream.split(Named.as("Branch-"))
            .branch((key, value) -> key.startsWith("A"),  /* first predicate  */
                 Branched.as("A"))
            .branch((key, value) -> key.startsWith("B"),  /* second predicate */
                 Branched.as("B"))
            .defaultBranch(Branched.as("C"))              /* default branch */
    );
    
    // KStream branches.get("Branch-A") contains all records whose keys start with "A"
    // KStream branches.get("Branch-B") contains all records whose keys start with "B"
    // KStream branches.get("Branch-C") contains all other records
    
    // Java 7 example: cf. `filter` for how to create `Predicate` instances


</td> </tr>  
<tr>  
<td>



**Filter**

  * KStream -> KStream
  * KTable -> KTable


</td>  
<td>



Evaluates a boolean function for each element and retains those for which the function returns true. ([KStream details](/37/javadoc/org/apache/kafka/streams/kstream/KStream.html#filter-org.apache.kafka.streams.kstream.Predicate-), [KTable details](/37/javadoc/org/apache/kafka/streams/kstream/KTable.html#filter-org.apache.kafka.streams.kstream.Predicate-))
    
    
    KStream<String, Long> stream = ...;
    
    // A filter that selects (keeps) only positive numbers
    // Java 8+ example, using lambda expressions
    KStream<String, Long> onlyPositives = stream.filter((key, value) -> value > 0);
    
    // Java 7 example
    KStream<String, Long> onlyPositives = stream.filter(
        new Predicate<String, Long>() {
          @Override
          public boolean test(String key, Long value) {
            return value > 0;
          }
        });


</td> </tr>  
<tr>  
<td>



**Inverse Filter**

  * KStream -> KStream
  * KTable -> KTable


</td>  
<td>



Evaluates a boolean function for each element and drops those for which the function returns true. ([KStream details](/37/javadoc/org/apache/kafka/streams/kstream/KStream.html#filterNot-org.apache.kafka.streams.kstream.Predicate-), [KTable details](/37/javadoc/org/apache/kafka/streams/kstream/KTable.html#filterNot-org.apache.kafka.streams.kstream.Predicate-))
    
    
    KStream<String, Long> stream = ...;
    
    // An inverse filter that discards any negative numbers or zero
    // Java 8+ example, using lambda expressions
    KStream<String, Long> onlyPositives = stream.filterNot((key, value) -> value <= 0);
    
    // Java 7 example
    KStream<String, Long> onlyPositives = stream.filterNot(
        new Predicate<String, Long>() {
          @Override
          public boolean test(String key, Long value) {
            return value <= 0;
          }
        });


</td> </tr>  
<tr>  
<td>



**FlatMap**

  * KStream -> KStream


</td>  
<td>



Takes one record and produces zero, one, or more records. You can modify the record keys and values, including their types. ([details](/37/javadoc/org/apache/kafka/streams/kstream/KStream.html#flatMap-org.apache.kafka.streams.kstream.KeyValueMapper-))

**Marks the stream for data re-partitioning:** Applying a grouping or a join after `flatMap` will result in re-partitioning of the records. If possible use `flatMapValues` instead, which will not cause data re-partitioning.
    
    
    KStream<Long, String> stream = ...;
    KStream<String, Integer> transformed = stream.flatMap(
         // Here, we generate two output records for each input record.
         // We also change the key and value types.
         // Example: (345L, "Hello") -> ("HELLO", 1000), ("hello", 9000)
        (key, value) -> {
          List<KeyValue<String, Integer>> result = new LinkedList<>();
          result.add(KeyValue.pair(value.toUpperCase(), 1000));
          result.add(KeyValue.pair(value.toLowerCase(), 9000));
          return result;
        }
      );
    
    // Java 7 example: cf. `map` for how to create `KeyValueMapper` instances


</td> </tr>  
<tr>  
<td>



**FlatMapValues**

  * KStream -> KStream


</td>  
<td>



Takes one record and produces zero, one, or more records, while retaining the key of the original record. You can modify the record values and the value type. ([details](/37/javadoc/org/apache/kafka/streams/kstream/KStream.html#flatMapValues-org.apache.kafka.streams.kstream.ValueMapper-))

`flatMapValues` is preferable to `flatMap` because it will not cause data re-partitioning. However, you cannot modify the key or key type like `flatMap` does.
    
    
    // Split a sentence into words.
    KStream<byte[], String> sentences = ...;
    KStream<byte[], String> words = sentences.flatMapValues(value -> Arrays.asList(value.split("\s+")));
    
    // Java 7 example: cf. `mapValues` for how to create `ValueMapper` instances


</td> </tr>  
<tr>  
<td>



**Foreach**

  * KStream -> void
  * KStream -> void
  * KTable -> void


</td>  
<td>



**Terminal operation.** Performs a stateless action on each record. ([details](/37/javadoc/org/apache/kafka/streams/kstream/KStream.html#foreach-org.apache.kafka.streams.kstream.ForeachAction-))

You would use `foreach` to cause _side effects_ based on the input data (similar to `peek`) and then _stop_ _further processing_ of the input data (unlike `peek`, which is not a terminal operation).

**Note on processing guarantees:** Any side effects of an action (such as writing to external systems) are not trackable by Kafka, which means they will typically not benefit from Kafka's processing guarantees.
    
    
    KStream<String, Long> stream = ...;
    
    // Print the contents of the KStream to the local console.
    // Java 8+ example, using lambda expressions
    stream.foreach((key, value) -> System.out.println(key + " => " + value));
    
    // Java 7 example
    stream.foreach(
        new ForeachAction<String, Long>() {
          @Override
          public void apply(String key, Long value) {
            System.out.println(key + " => " + value);
          }
        });


</td> </tr>  
<tr>  
<td>



**GroupByKey**

  * KStream -> KGroupedStream


</td>  
<td>



Groups the records by the existing key. ([details](/37/javadoc/org/apache/kafka/streams/kstream/KStream.html#groupByKey--))

Grouping is a prerequisite for aggregating a stream or a table and ensures that data is properly partitioned ("keyed") for subsequent operations.

**When to set explicit Serdes:** Variants of `groupByKey` exist to override the configured default Serdes of your application, which **you** **must do** if the key and/or value types of the resulting `KGroupedStream` do not match the configured default Serdes.

**Note**

**Grouping vs. Windowing:** A related operation is windowing, which lets you control how to "sub-group" the grouped records _of the same key_ into so-called _windows_ for stateful operations such as windowed aggregations or windowed joins.

**Causes data re-partitioning if and only if the stream was marked for re-partitioning.** `groupByKey` is preferable to `groupBy` because it re-partitions data only if the stream was already marked for re-partitioning. However, `groupByKey` does not allow you to modify the key or key type like `groupBy` does.
    
    
    KStream<byte[], String> stream = ...;
    
    // Group by the existing key, using the application's configured
    // default serdes for keys and values.
    KGroupedStream<byte[], String> groupedStream = stream.groupByKey();
    
    // When the key and/or value types do not match the configured
    // default serdes, we must explicitly specify serdes.
    KGroupedStream<byte[], String> groupedStream = stream.groupByKey(
        Grouped.with(
          Serdes.ByteArray(), /* key */
          Serdes.String())     /* value */
      );


</td> </tr>  
<tr>  
<td>



**GroupBy**

  * KStream -> KGroupedStream
  * KTable -> KGroupedTable


</td>  
<td>



Groups the records by a _new_ key, which may be of a different key type. When grouping a table, you may also specify a new value and value type. `groupBy` is a shorthand for `selectKey(...).groupByKey()`. ([KStream details](/37/javadoc/org/apache/kafka/streams/kstream/KStream.html#groupBy-org.apache.kafka.streams.kstream.KeyValueMapper-), [KTable details](/37/javadoc/org/apache/kafka/streams/kstream/KTable.html#groupBy-org.apache.kafka.streams.kstream.KeyValueMapper-))

Grouping is a prerequisite for aggregating a stream or a table and ensures that data is properly partitioned ("keyed") for subsequent operations.

**When to set explicit Serdes:** Variants of `groupBy` exist to override the configured default Serdes of your application, which **you must** **do** if the key and/or value types of the resulting `KGroupedStream` or `KGroupedTable` do not match the configured default Serdes.

**Note**

**Grouping vs. Windowing:** A related operation is windowing, which lets you control how to "sub-group" the grouped records _of the same key_ into so-called _windows_ for stateful operations such as windowed aggregations or windowed joins.

**Always causes data re-partitioning:** `groupBy` always causes data re-partitioning. If possible use `groupByKey` instead, which will re-partition data only if required.
    
    
    KStream<byte[], String> stream = ...;
    KTable<byte[], String> table = ...;
    
    // Java 8+ examples, using lambda expressions
    
    // Group the stream by a new key and key type
    KGroupedStream<String, String> groupedStream = stream.groupBy(
        (key, value) -> value,
        Grouped.with(
          Serdes.String(), /* key (note: type was modified) */
          Serdes.String())  /* value */
      );
    
    // Group the table by a new key and key type, and also modify the value and value type.
    KGroupedTable<String, Integer> groupedTable = table.groupBy(
        (key, value) -> KeyValue.pair(value, value.length()),
        Grouped.with(
          Serdes.String(), /* key (note: type was modified) */
          Serdes.Integer()) /* value (note: type was modified) */
      );
    
    
    // Java 7 examples
    
    // Group the stream by a new key and key type
    KGroupedStream<String, String> groupedStream = stream.groupBy(
        new KeyValueMapper<byte[], String, String>>() {
          @Override
          public String apply(byte[] key, String value) {
            return value;
          }
        },
        Grouped.with(
          Serdes.String(), /* key (note: type was modified) */
          Serdes.String())  /* value */
      );
    
    // Group the table by a new key and key type, and also modify the value and value type.
    KGroupedTable<String, Integer> groupedTable = table.groupBy(
        new KeyValueMapper<byte[], String, KeyValue<String, Integer>>() {
          @Override
          public KeyValue<String, Integer> apply(byte[] key, String value) {
            return KeyValue.pair(value, value.length());
          }
        },
        Grouped.with(
          Serdes.String(), /* key (note: type was modified) */
          Serdes.Integer()) /* value (note: type was modified) */
      );


</td> </tr>  
<tr>  
<td>



**Cogroup**

  * KGroupedStream -> CogroupedKStream
  * CogroupedKStream -> CogroupedKStream


</td>  
<td>



Cogrouping allows to aggregate multiple input streams in a single operation. The different (already grouped) input streams must have the same key type and may have different values types. [KGroupedStream#cogroup()](/37/javadoc/org/apache/kafka/streams/kstream/KGroupedStream.html#cogroup) creates a new cogrouped stream with a single input stream, while [CogroupedKStream#cogroup()](/37/javadoc/org/apache/kafka/streams/kstream/CogroupedKStream.html#cogroup) adds a grouped stream to an existing cogrouped stream. A `CogroupedKStream` may be [windowed](/37/javadoc/org/apache/kafka/streams/kstream/CogroupedKStream.html#windowedBy) before it is [aggregated](/37/javadoc/org/apache/kafka/streams/kstream/CogroupedKStream.html#aggregate). 

Cogroup does not cause a repartition as it has the prerequisite that the input streams are grouped. In the process of creating these groups they will have already been repartitioned if the stream was already marked for repartitioning.
    
    
    KStream<byte[], String> stream = ...;
                            KStream<byte[], String> stream2 = ...;
    
    // Group by the existing key, using the application's configured
    // default serdes for keys and values.
    KGroupedStream<byte[], String> groupedStream = stream.groupByKey();
    KGroupedStream<byte[], String> groupedStream2 = stream2.groupByKey();
    CogroupedKStream<byte[], String> cogroupedStream = groupedStream.cogroup(aggregator1).cogroup(groupedStream2, aggregator2);
    
    KTable<byte[], String> table = cogroupedStream.aggregate(initializer);
    
    KTable<byte[], String> table2 = cogroupedStream.windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMillis(500))).aggregate(initializer);


</td> </tr>  
<tr>  
<td>



**Map**

  * KStream -> KStream


</td>  
<td>



Takes one record and produces one record. You can modify the record key and value, including their types. ([details](/37/javadoc/org/apache/kafka/streams/kstream/KStream.html#map-org.apache.kafka.streams.kstream.KeyValueMapper-))

**Marks the stream for data re-partitioning:** Applying a grouping or a join after `map` will result in re-partitioning of the records. If possible use `mapValues` instead, which will not cause data re-partitioning.
    
    
    KStream<byte[], String> stream = ...;
    
    // Java 8+ example, using lambda expressions
    // Note how we change the key and the key type (similar to `selectKey`)
    // as well as the value and the value type.
    KStream<String, Integer> transformed = stream.map(
        (key, value) -> KeyValue.pair(value.toLowerCase(), value.length()));
    
    // Java 7 example
    KStream<String, Integer> transformed = stream.map(
        new KeyValueMapper<byte[], String, KeyValue<String, Integer>>() {
          @Override
          public KeyValue<String, Integer> apply(byte[] key, String value) {
            return new KeyValue<>(value.toLowerCase(), value.length());
          }
        });


</td> </tr>  
<tr>  
<td>



**Map (values only)**

  * KStream -> KStream
  * KTable -> KTable


</td>  
<td>



Takes one record and produces one record, while retaining the key of the original record. You can modify the record value and the value type. ([KStream details](/37/javadoc/org/apache/kafka/streams/kstream/KStream.html#mapValues-org.apache.kafka.streams.kstream.ValueMapper-), [KTable details](/37/javadoc/org/apache/kafka/streams/kstream/KTable.html#mapValues-org.apache.kafka.streams.kstream.ValueMapper-))

`mapValues` is preferable to `map` because it will not cause data re-partitioning. However, it does not allow you to modify the key or key type like `map` does.
    
    
    KStream<byte[], String> stream = ...;
    
    // Java 8+ example, using lambda expressions
    KStream<byte[], String> uppercased = stream.mapValues(value -> value.toUpperCase());
    
    // Java 7 example
    KStream<byte[], String> uppercased = stream.mapValues(
        new ValueMapper<String>() {
          @Override
          public String apply(String s) {
            return s.toUpperCase();
          }
        });


</td> </tr>  
<tr>  
<td>



**Merge**

  * KStream -> KStream


</td>  
<td>



Merges records of two streams into one larger stream. ([details](/37/javadoc/org/apache/kafka/streams/kstream/KStream.html#merge-org.apache.kafka.streams.kstream.KStream-)) 

There is no ordering guarantee between records from different streams in the merged stream. Relative order is preserved within each input stream though (ie, records within the same input stream are processed in order)
    
    
    KStream<byte[], String> stream1 = ...;
    
    KStream<byte[], String> stream2 = ...;
    
    KStream<byte[], String> merged = stream1.merge(stream2);


</td> </tr>  
<tr>  
<td>



**Peek**

  * KStream -> KStream


</td>  
<td>



Performs a stateless action on each record, and returns an unchanged stream. ([details](/37/javadoc/org/apache/kafka/streams/kstream/KStream.html#peek-org.apache.kafka.streams.kstream.ForeachAction-))

You would use `peek` to cause _side effects_ based on the input data (similar to `foreach`) and _continue_ _processing_ the input data (unlike `foreach`, which is a terminal operation). `peek` returns the input stream as-is; if you need to modify the input stream, use `map` or `mapValues` instead.

`peek` is helpful for use cases such as logging or tracking metrics or for debugging and troubleshooting.

**Note on processing guarantees:** Any side effects of an action (such as writing to external systems) are not trackable by Kafka, which means they will typically not benefit from Kafka's processing guarantees.
    
    
    KStream<byte[], String> stream = ...;
    
    // Java 8+ example, using lambda expressions
    KStream<byte[], String> unmodifiedStream = stream.peek(
        (key, value) -> System.out.println("key=" + key + ", value=" + value));
    
    // Java 7 example
    KStream<byte[], String> unmodifiedStream = stream.peek(
        new ForeachAction<byte[], String>() {
          @Override
          public void apply(byte[] key, String value) {
            System.out.println("key=" + key + ", value=" + value);
          }
        });


</td> </tr>  
<tr>  
<td>



**Print**

  * KStream -> void


</td>  
<td>



**Terminal operation.** Prints the records to `System.out`. See Javadocs for serde and `toString()` caveats. ([details](/37/javadoc/org/apache/kafka/streams/kstream/KStream.html#print--))

Calling `print()` is the same as calling `foreach((key, value) -> System.out.println(key + ", " + value))`

`print` is mainly for debugging/testing purposes, and it will try to flush on each record print. Hence it **should not** be used for production usage if performance requirements are concerned.
    
    
    KStream<byte[], String> stream = ...;
    // print to sysout
    stream.print();
    
    // print to file with a custom label
    stream.print(Printed.toFile("streams.out").withLabel("streams"));


</td> </tr>  
<tr>  
<td>



**SelectKey**

  * KStream -> KStream


</td>  
<td>



Assigns a new key - possibly of a new key type - to each record. ([details](/37/javadoc/org/apache/kafka/streams/kstream/KStream.html#selectKey-org.apache.kafka.streams.kstream.KeyValueMapper-))

Calling `selectKey(mapper)` is the same as calling `map((key, value) -> mapper(key, value), value)`.

**Marks the stream for data re-partitioning:** Applying a grouping or a join after `selectKey` will result in re-partitioning of the records.
    
    
    KStream<byte[], String> stream = ...;
    
    // Derive a new record key from the record's value.  Note how the key type changes, too.
    // Java 8+ example, using lambda expressions
    KStream<String, String> rekeyed = stream.selectKey((key, value) -> value.split(" ")[0])
    
    // Java 7 example
    KStream<String, String> rekeyed = stream.selectKey(
        new KeyValueMapper<byte[], String, String>() {
          @Override
          public String apply(byte[] key, String value) {
            return value.split(" ")[0];
          }
        });


</td> </tr>  
<tr>  
<td>



**Table to Stream**

  * KTable -> KStream


</td>  
<td>



Get the changelog stream of this table. ([details](/37/javadoc/org/apache/kafka/streams/kstream/KTable.html#toStream--))
    
    
    KTable<byte[], String> table = ...;
    
    // Also, a variant of `toStream` exists that allows you
    // to select a new key for the resulting stream.
    KStream<byte[], String> stream = table.toStream();


</td> </tr>  
<tr>  
<td>



**Stream to Table**

  * KStream -> KTable


</td>  
<td>



Convert an event stream into a table, or say a changelog stream. ([details](/37/javadoc/org/apache/kafka/streams/kstream/KStream.html#toTable--))
    
    
    KStream<byte[], String> stream = ...;
    
    KTable<byte[], String> table = stream.toTable();


</td> </tr>  
<tr>  
<td>



**Repartition**

  * KStream -> KStream


</td>  
<td>



Manually trigger repartitioning of the stream with desired number of partitions. ([details](/37/javadoc/org/apache/kafka/streams/kstream/KStream.html#repartition--))

`repartition()` is similar to `through()` however Kafka Streams will manage the topic for you. Generated topic is treated as internal topic, as a result data will be purged automatically as any other internal repartition topic. In addition, you can specify the desired number of partitions, which allows to easily scale in/out downstream sub-topologies. `repartition()` operation always triggers repartitioning of the stream, as a result it can be used with embedded Processor API methods (like `transform()` et al.) that do not trigger auto repartitioning when key changing operation is performed beforehand. 
    
    
    KStream<byte[], String> stream = ... ;
    KStream<byte[], String> repartitionedStream = stream.repartition(Repartitioned.numberOfPartitions(10));


</td> </tr> </table>

## Stateful transformations

Stateful transformations depend on state for processing inputs and producing outputs and require a [state store](../architecture.html#streams_architecture_state) associated with the stream processor. For example, in aggregating operations, a windowing state store is used to collect the latest aggregation results per window. In join operations, a windowing state store is used to collect all of the records received so far within the defined window boundary.

**Note:** Following store types are used regardless of the possibly specified type (via the parameter `materialized`): 

  * non-windowed aggregations and non-windowed KTables use [TimestampedKeyValueStore](/37/javadoc/org/apache/kafka/streams/state/TimestampedKeyValueStore.html)s or [VersionedKeyValueStore](/37/javadoc/org/apache/kafka/streams/state/VersionedKeyValueStore.html)s, depending on whether the parameter `materialized` is versioned
  * time-windowed aggregations and KStream-KStream joins use [TimestampedWindowStore](/37/javadoc/org/apache/kafka/streams/state/TimestampedWindowStore.html)s
  * session windowed aggregations use [SessionStore](/37/javadoc/org/apache/kafka/streams/state/SessionStore.html)s (there is no timestamped session store as of now)



Note, that state stores are fault-tolerant. In case of failure, Kafka Streams guarantees to fully restore all state stores prior to resuming the processing. See [Fault Tolerance](../architecture.html#streams_architecture_recovery) for further information.

Available stateful transformations in the DSL include:

  * Aggregating
  * Joining
  * Windowing (as part of aggregations and joins)
  * Applying custom processors and transformers, which may be stateful, for Processor API integration



The following diagram shows their relationships:

![](/37/images/streams-stateful_operations.png)

Stateful transformations in the DSL.

Here is an example of a stateful application: the WordCount algorithm.

WordCount example in Java 8+, using lambda expressions:
    
    
    // Assume the record values represent lines of text.  For the sake of this example, you can ignore
    // whatever may be stored in the record keys.
    KStream<String, String> textLines = ...;
    
    KStream<String, Long> wordCounts = textLines
        // Split each text line, by whitespace, into words.  The text lines are the record
        // values, i.e. you can ignore whatever data is in the record keys and thus invoke
        // `flatMapValues` instead of the more generic `flatMap`.
        .flatMapValues(value -> Arrays.asList(value.toLowerCase().split("\W+")))
        // Group the stream by word to ensure the key of the record is the word.
        .groupBy((key, word) -> word)
        // Count the occurrences of each word (record key).
        //
        // This will change the stream type from `KGroupedStream<String, String>` to
        // `KTable<String, Long>` (word -> count).
        .count()
        // Convert the `KTable<String, Long>` into a `KStream<String, Long>`.
        .toStream();

WordCount example in Java 7:
    
    
    // Code below is equivalent to the previous Java 8+ example above.
    KStream<String, String> textLines = ...;
    
    KStream<String, Long> wordCounts = textLines
        .flatMapValues(new ValueMapper<String, Iterable<String>>() {
            @Override
            public Iterable<String> apply(String value) {
                return Arrays.asList(value.toLowerCase().split("\W+"));
            }
        })
        .groupBy(new KeyValueMapper<String, String, String>>() {
            @Override
            public String apply(String key, String word) {
                return word;
            }
        })
        .count()
        .toStream();

### Aggregating

After records are grouped by key via `groupByKey` or `groupBy` - and thus represented as either a `KGroupedStream` or a `KGroupedTable`, they can be aggregated via an operation such as `reduce`. Aggregations are key-based operations, which means that they always operate over records (notably record values) of the same key. You can perform aggregations on windowed or non-windowed data.  
  
<table>  
<tr>  
<th>

Transformation
</th>  
<th>

Description
</th> </tr>  
<tr>  
<td>



**Aggregate**

  * KGroupedStream -> KTable
  * KGroupedTable -> KTable


</td>  
<td>



**Rolling aggregation.** Aggregates the values of (non-windowed) records by the grouped key or cogrouped. Aggregating is a generalization of `reduce` and allows, for example, the aggregate value to have a different type than the input values. ([KGroupedStream details](/37/javadoc/org/apache/kafka/streams/kstream/KGroupedStream.html), [KGroupedTable details](/37/javadoc/org/apache/kafka/streams/kstream/KGroupedTable.html) [KGroupedTable details](/37/javadoc/org/apache/kafka/streams/kstream/CogroupedKStream.html))

When aggregating a _grouped stream_ , you must provide an initializer (e.g., `aggValue = 0`) and an "adder" aggregator (e.g., `aggValue + curValue`). When aggregating a _grouped table_ , you must additionally provide a "subtractor" aggregator (think: `aggValue - oldValue`).

When aggregating a _cogrouped stream_ , the actual aggregators are provided for each input stream in the prior `cogroup()`calls, and thus you only need to provide an initializer (e.g., `aggValue = 0`) 

Several variants of `aggregate` exist, see Javadocs for details.
    
    
    KGroupedStream<byte[], String> groupedStream = ...;
    KGroupedTable<byte[], String> groupedTable = ...;
    
    // Java 8+ examples, using lambda expressions
    
    // Aggregating a KGroupedStream (note how the value type changes from String to Long)
    KTable<byte[], Long> aggregatedStream = groupedStream.aggregate(
        () -> 0L, /* initializer */
        (aggKey, newValue, aggValue) -> aggValue + newValue.length(), /* adder */
        Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("aggregated-stream-store") /* state store name */
            .withValueSerde(Serdes.Long()); /* serde for aggregate value */
    
    // Aggregating a KGroupedTable (note how the value type changes from String to Long)
    KTable<byte[], Long> aggregatedTable = groupedTable.aggregate(
        () -> 0L, /* initializer */
        (aggKey, newValue, aggValue) -> aggValue + newValue.length(), /* adder */
        (aggKey, oldValue, aggValue) -> aggValue - oldValue.length(), /* subtractor */
        Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("aggregated-table-store") /* state store name */
    	.withValueSerde(Serdes.Long()) /* serde for aggregate value */
    
    
    // Java 7 examples
    
    // Aggregating a KGroupedStream (note how the value type changes from String to Long)
    KTable<byte[], Long> aggregatedStream = groupedStream.aggregate(
        new Initializer<Long>() { /* initializer */
          @Override
          public Long apply() {
            return 0L;
          }
        },
        new Aggregator<byte[], String, Long>() { /* adder */
          @Override
          public Long apply(byte[] aggKey, String newValue, Long aggValue) {
            return aggValue + newValue.length();
          }
        },
        Materialized.as("aggregated-stream-store")
            .withValueSerde(Serdes.Long());
    
    // Aggregating a KGroupedTable (note how the value type changes from String to Long)
    KTable<byte[], Long> aggregatedTable = groupedTable.aggregate(
        new Initializer<Long>() { /* initializer */
          @Override
          public Long apply() {
            return 0L;
          }
        },
        new Aggregator<byte[], String, Long>() { /* adder */
          @Override
          public Long apply(byte[] aggKey, String newValue, Long aggValue) {
            return aggValue + newValue.length();
          }
        },
        new Aggregator<byte[], String, Long>() { /* subtractor */
          @Override
          public Long apply(byte[] aggKey, String oldValue, Long aggValue) {
            return aggValue - oldValue.length();
          }
        },
        Materialized.as("aggregated-stream-store")
            .withValueSerde(Serdes.Long());

Detailed behavior of `KGroupedStream`:

  * Input records with `null` keys are ignored.
  * When a record key is received for the first time, the initializer is called (and called before the adder).
  * Whenever a record with a non-`null` value is received, the adder is called.



Detailed behavior of `KGroupedTable`:

  * Input records with `null` keys are ignored.
  * When a record key is received for the first time, the initializer is called (and called before the adder and subtractor). Note that, in contrast to `KGroupedStream`, over time the initializer may be called more than once for a key as a result of having received input tombstone records for that key (see below).
  * When the first non-`null` value is received for a key (e.g., INSERT), then only the adder is called.
  * When subsequent non-`null` values are received for a key (e.g., UPDATE), then (1) the subtractor is called with the old value as stored in the table and (2) the adder is called with the new value of the input record that was just received. The subtractor is guaranteed to be called before the adder if the extracted grouping key of the old and new value is the same. The detection of this case depends on the correct implementation of the equals() method of the extracted key type. Otherwise, the order of execution for the subtractor and adder is not defined.
  * When a tombstone record - i.e. a record with a `null` value - is received for a key (e.g., DELETE), then only the subtractor is called. Note that, whenever the subtractor returns a `null` value itself, then the corresponding key is removed from the resulting `KTable`. If that happens, any next input record for that key will trigger the initializer again.



See the example at the bottom of this section for a visualization of the aggregation semantics.


</td> </tr>  
<tr>  
<td>



**Aggregate (windowed)**

  * KGroupedStream -> KTable


</td>  
<td>



**Windowed aggregation.** Aggregates the values of records, per window, by the grouped key. Aggregating is a generalization of `reduce` and allows, for example, the aggregate value to have a different type than the input values. ([TimeWindowedKStream details](/37/javadoc/org/apache/kafka/streams/kstream/TimeWindowedKStream.html), [SessionWindowedKStream details](/37/javadoc/org/apache/kafka/streams/kstream/SessionWindowedKStream.html))

You must provide an initializer (e.g., `aggValue = 0`), "adder" aggregator (e.g., `aggValue + curValue`), and a window. When windowing based on sessions, you must additionally provide a "session merger" aggregator (e.g., `mergedAggValue = leftAggValue + rightAggValue`).

The windowed `aggregate` turns a `TimeWindowedKStream<K, V>` or `SessionWindowedKStream<K, V>` into a windowed `KTable<Windowed<K>, V>`.

Several variants of `aggregate` exist, see Javadocs for details.
    
    
    import java.time.Duration;
    KGroupedStream<String, Long> groupedStream = ...;
    
    // Java 8+ examples, using lambda expressions
    
    // Aggregating with time-based windowing (here: with 5-minute tumbling windows)
    KTable<Windowed<String>, Long> timeWindowedAggregatedStream = groupedStream.windowedBy(Duration.ofMinutes(5))
        .aggregate(
            () -> 0L, /* initializer */
            (aggKey, newValue, aggValue) -> aggValue + newValue, /* adder */
            Materialized.<String, Long, WindowStore<Bytes, byte[]>>as("time-windowed-aggregated-stream-store") /* state store name */
            .withValueSerde(Serdes.Long())); /* serde for aggregate value */
    
    // Aggregating with time-based windowing (here: with 5-minute sliding windows and 30-minute grace period)
    KTable<Windowed<String>, Long> timeWindowedAggregatedStream = groupedStream.windowedBy(SlidingWindows.ofTimeDifferenceAndGrace(Duration.ofMinutes(5), Duration.ofMinutes(30)))
        .aggregate(
            () -> 0L, /* initializer */
            (aggKey, newValue, aggValue) -> aggValue + newValue, /* adder */
            Materialized.<String, Long, WindowStore<Bytes, byte[]>>as("time-windowed-aggregated-stream-store") /* state store name */
            .withValueSerde(Serdes.Long())); /* serde for aggregate value */
    
    // Aggregating with session-based windowing (here: with an inactivity gap of 5 minutes)
    KTable<Windowed<String>, Long> sessionizedAggregatedStream = groupedStream.windowedBy(SessionWindows.ofInactivityGapWithNoGrace(Duration.ofMinutes(5)).
        aggregate(
        	() -> 0L, /* initializer */
        	(aggKey, newValue, aggValue) -> aggValue + newValue, /* adder */
            (aggKey, leftAggValue, rightAggValue) -> leftAggValue + rightAggValue, /* session merger */
            Materialized.<String, Long, SessionStore<Bytes, byte[]>>as("sessionized-aggregated-stream-store") /* state store name */
            .withValueSerde(Serdes.Long())); /* serde for aggregate value */
    
    // Java 7 examples
    
    // Aggregating with time-based windowing (here: with 5-minute tumbling windows)
    KTable<Windowed<String>, Long> timeWindowedAggregatedStream = groupedStream.windowedBy(Duration.ofMinutes(5))
        .aggregate(
            new Initializer<Long>() { /* initializer */
                @Override
                public Long apply() {
                    return 0L;
                }
            },
            new Aggregator<String, Long, Long>() { /* adder */
                @Override
                public Long apply(String aggKey, Long newValue, Long aggValue) {
                    return aggValue + newValue;
                }
            },
            Materialized.<String, Long, WindowStore<Bytes, byte[]>>as("time-windowed-aggregated-stream-store")
              .withValueSerde(Serdes.Long()));
    
    // Aggregating with session-based windowing (here: with an inactivity gap of 5 minutes)
    KTable<Windowed<String>, Long> sessionizedAggregatedStream = groupedStream.windowedBy(SessionWindows.ofInactivityGapWithNoGrace(Duration.ofMinutes(5)).
        aggregate(
            new Initializer<Long>() { /* initializer */
                @Override
                public Long apply() {
                    return 0L;
                }
            },
            new Aggregator<String, Long, Long>() { /* adder */
                @Override
                public Long apply(String aggKey, Long newValue, Long aggValue) {
                    return aggValue + newValue;
                }
            },
            new Merger<String, Long>() { /* session merger */
                @Override
                public Long apply(String aggKey, Long leftAggValue, Long rightAggValue) {
                    return rightAggValue + leftAggValue;
                }
            },
            Materialized.<String, Long, SessionStore<Bytes, byte[]>>as("sessionized-aggregated-stream-store")
              .withValueSerde(Serdes.Long()));

Detailed behavior:

  * The windowed aggregate behaves similar to the rolling aggregate described above. The additional twist is that the behavior applies _per window_.
  * Input records with `null` keys are ignored in general.
  * When a record key is received for the first time for a given window, the initializer is called (and called before the adder).
  * Whenever a record with a non-`null` value is received for a given window, the adder is called.
  * When using session windows: the session merger is called whenever two sessions are being merged.



See the example at the bottom of this section for a visualization of the aggregation semantics.


</td> </tr>  
<tr>  
<td>



**Count**

  * KGroupedStream -> KTable
  * KGroupedTable -> KTable


</td>  
<td>



**Rolling aggregation.** Counts the number of records by the grouped key. ([KGroupedStream details](/37/javadoc/org/apache/kafka/streams/kstream/KGroupedStream.html), [KGroupedTable details](/37/javadoc/org/apache/kafka/streams/kstream/KGroupedTable.html))

Several variants of `count` exist, see Javadocs for details.
    
    
    KGroupedStream<String, Long> groupedStream = ...;
    KGroupedTable<String, Long> groupedTable = ...;
    
    // Counting a KGroupedStream
    KTable<String, Long> aggregatedStream = groupedStream.count();
    
    // Counting a KGroupedTable
    KTable<String, Long> aggregatedTable = groupedTable.count();

Detailed behavior for `KGroupedStream`:

  * Input records with `null` keys or values are ignored.



Detailed behavior for `KGroupedTable`:

  * Input records with `null` keys are ignored. Records with `null` values are not ignored but interpreted as "tombstones" for the corresponding key, which indicate the deletion of the key from the table.


</td> </tr>  
<tr>  
<td>



**Count (windowed)**

  * KGroupedStream -> KTable


</td>  
<td>



**Windowed aggregation.** Counts the number of records, per window, by the grouped key. ([TimeWindowedKStream details](/37/javadoc/org/apache/kafka/streams/kstream/TimeWindowedKStream.html), [SessionWindowedKStream details](/37/javadoc/org/apache/kafka/streams/kstream/SessionWindowedKStream.html))

The windowed `count` turns a `TimeWindowedKStream<K, V>` or `SessionWindowedKStream<K, V>` into a windowed `KTable<Windowed<K>, V>`.

Several variants of `count` exist, see Javadocs for details.
    
    
    import java.time.Duration;
    KGroupedStream<String, Long> groupedStream = ...;
    
    // Counting a KGroupedStream with time-based windowing (here: with 5-minute tumbling windows)
    KTable<Windowed<String>, Long> aggregatedStream = groupedStream.windowedBy(
        TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(5))) /* time-based window */
        .count();
    
    // Counting a KGroupedStream with time-based windowing (here: with 5-minute sliding windows and 30-minute grace period)
    KTable<Windowed<String>, Long> aggregatedStream = groupedStream.windowedBy(
        SlidingWindows.ofTimeDifferenceAndGrace(Duration.ofMinutes(5), Duration.ofMinutes(30))) /* time-based window */
        .count();
    
    // Counting a KGroupedStream with session-based windowing (here: with 5-minute inactivity gaps)
    KTable<Windowed<String>, Long> aggregatedStream = groupedStream.windowedBy(
        SessionWindows.ofInactivityGapWithNoGrace(Duration.ofMinutes(5))) /* session window */
        .count();

Detailed behavior:

  * Input records with `null` keys or values are ignored.


</td> </tr>  
<tr>  
<td>



**Reduce**

  * KGroupedStream -> KTable
  * KGroupedTable -> KTable


</td>  
<td>



**Rolling aggregation.** Combines the values of (non-windowed) records by the grouped key. The current record value is combined with the last reduced value, and a new reduced value is returned. The result value type cannot be changed, unlike `aggregate`. ([KGroupedStream details](/37/javadoc/org/apache/kafka/streams/kstream/KGroupedStream.html), [KGroupedTable details](/37/javadoc/org/apache/kafka/streams/kstream/KGroupedTable.html))

When reducing a _grouped stream_ , you must provide an "adder" reducer (e.g., `aggValue + curValue`). When reducing a _grouped table_ , you must additionally provide a "subtractor" reducer (e.g., `aggValue - oldValue`).

Several variants of `reduce` exist, see Javadocs for details.
    
    
    KGroupedStream<String, Long> groupedStream = ...;
    KGroupedTable<String, Long> groupedTable = ...;
    
    // Java 8+ examples, using lambda expressions
    
    // Reducing a KGroupedStream
    KTable<String, Long> aggregatedStream = groupedStream.reduce(
        (aggValue, newValue) -> aggValue + newValue /* adder */);
    
    // Reducing a KGroupedTable
    KTable<String, Long> aggregatedTable = groupedTable.reduce(
        (aggValue, newValue) -> aggValue + newValue, /* adder */
        (aggValue, oldValue) -> aggValue - oldValue /* subtractor */);
    
    
    // Java 7 examples
    
    // Reducing a KGroupedStream
    KTable<String, Long> aggregatedStream = groupedStream.reduce(
        new Reducer<Long>() { /* adder */
          @Override
          public Long apply(Long aggValue, Long newValue) {
            return aggValue + newValue;
          }
        });
    
    // Reducing a KGroupedTable
    KTable<String, Long> aggregatedTable = groupedTable.reduce(
        new Reducer<Long>() { /* adder */
          @Override
          public Long apply(Long aggValue, Long newValue) {
            return aggValue + newValue;
          }
        },
        new Reducer<Long>() { /* subtractor */
          @Override
          public Long apply(Long aggValue, Long oldValue) {
            return aggValue - oldValue;
          }
        });

Detailed behavior for `KGroupedStream`:

  * Input records with `null` keys are ignored in general.
  * When a record key is received for the first time, then the value of that record is used as the initial aggregate value.
  * Whenever a record with a non-`null` value is received, the adder is called.



Detailed behavior for `KGroupedTable`:

  * Input records with `null` keys are ignored in general.
  * When a record key is received for the first time, then the value of that record is used as the initial aggregate value. Note that, in contrast to `KGroupedStream`, over time this initialization step may happen more than once for a key as a result of having received input tombstone records for that key (see below).
  * When the first non-`null` value is received for a key (e.g., INSERT), then only the adder is called.
  * When subsequent non-`null` values are received for a key (e.g., UPDATE), then (1) the subtractor is called with the old value as stored in the table and (2) the adder is called with the new value of the input record that was just received. The subtractor is guaranteed be called before the adder if the extracted grouping key of the old and new value is the same. The detection of this case depends on the correct implementation of the equals() method of the extracted key type. Otherwise, the order of execution for the subtractor and adder is not defined.
  * When a tombstone record - i.e. a record with a `null` value - is received for a key (e.g., DELETE), then only the subtractor is called. Note that, whenever the subtractor returns a `null` value itself, then the corresponding key is removed from the resulting `KTable`. If that happens, any next input record for that key will re-initialize its aggregate value.



See the example at the bottom of this section for a visualization of the aggregation semantics.


</td> </tr>  
<tr>  
<td>



**Reduce (windowed)**

  * KGroupedStream -> KTable


</td>  
<td>



**Windowed aggregation.** Combines the values of records, per window, by the grouped key. The current record value is combined with the last reduced value, and a new reduced value is returned. Records with `null` key or value are ignored. The result value type cannot be changed, unlike `aggregate`. ([TimeWindowedKStream details](/37/javadoc/org/apache/kafka/streams/kstream/TimeWindowedKStream.html), [SessionWindowedKStream details](/37/javadoc/org/apache/kafka/streams/kstream/SessionWindowedKStream.html))

The windowed `reduce` turns a turns a `TimeWindowedKStream<K, V>` or a `SessionWindowedKStream<K, V>` into a windowed `KTable<Windowed<K>, V>`.

Several variants of `reduce` exist, see Javadocs for details.
    
    
    import java.time.Duration;
    KGroupedStream<String, Long> groupedStream = ...;
    
    // Java 8+ examples, using lambda expressions
    
    // Aggregating with time-based windowing (here: with 5-minute tumbling windows)
    KTable<Windowed<String>, Long> timeWindowedAggregatedStream = groupedStream.windowedBy(
      TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(5)) /* time-based window */)
      .reduce(
        (aggValue, newValue) -> aggValue + newValue /* adder */
      );
    
    // Aggregating with time-based windowing (here: with 5-minute sliding windows and 30-minute grace)
    KTable<Windowed<String>, Long> timeWindowedAggregatedStream = groupedStream.windowedBy(
      SlidingWindows.ofTimeDifferenceAndGrace(Duration.ofMinutes(5), Duration.ofMinutes(30))) /* time-based window */)
      .reduce(
        (aggValue, newValue) -> aggValue + newValue /* adder */
      );
    
    // Aggregating with session-based windowing (here: with an inactivity gap of 5 minutes)
    KTable<Windowed<String>, Long> sessionzedAggregatedStream = groupedStream.windowedBy(
      SessionWindows.ofInactivityGapWithNoGrace(Duration.ofMinutes(5))) /* session window */
      .reduce(
        (aggValue, newValue) -> aggValue + newValue /* adder */
      );
    
    
    // Java 7 examples
    
    // Aggregating with time-based windowing (here: with 5-minute tumbling windows)
    KTable<Windowed<String>, Long> timeWindowedAggregatedStream = groupedStream.windowedBy(
      TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(5)) /* time-based window */)
      .reduce(
        new Reducer<Long>() { /* adder */
          @Override
          public Long apply(Long aggValue, Long newValue) {
            return aggValue + newValue;
          }
        });
    
    // Aggregating with session-based windowing (here: with an inactivity gap of 5 minutes)
    KTable<Windowed<String>, Long> timeWindowedAggregatedStream = groupedStream.windowedBy(
      SessionWindows.ofInactivityGapWithNoGrace(Duration.ofMinutes(5))) /* session window */
      .reduce(
        new Reducer<Long>() { /* adder */
          @Override
          public Long apply(Long aggValue, Long newValue) {
            return aggValue + newValue;
          }
        });

Detailed behavior:

  * The windowed reduce behaves similar to the rolling reduce described above. The additional twist is that the behavior applies _per window_.
  * Input records with `null` keys are ignored in general.
  * When a record key is received for the first time for a given window, then the value of that record is used as the initial aggregate value.
  * Whenever a record with a non-`null` value is received for a given window, the adder is called.



See the example at the bottom of this section for a visualization of the aggregation semantics.


</td> </tr> </table>

**Example of semantics for stream aggregations:** A `KGroupedStream` -> `KTable` example is shown below. The streams and the table are initially empty. Bold font is used in the column for "KTable `aggregated`" to highlight changed state. An entry such as `(hello, 1)` denotes a record with key `hello` and value `1`. To improve the readability of the semantics table you can assume that all records are processed in timestamp order.
    
    
    // Key: word, value: count
    KStream<String, Integer> wordCounts = ...;
    
    KGroupedStream<String, Integer> groupedStream = wordCounts
        .groupByKey(Grouped.with(Serdes.String(), Serdes.Integer()));
    
    KTable<String, Integer> aggregated = groupedStream.aggregate(
        () -> 0, /* initializer */
        (aggKey, newValue, aggValue) -> aggValue + newValue, /* adder */
        Materialized.<String, Long, KeyValueStore<Bytes, byte[]>as("aggregated-stream-store" /* state store name */)
          .withKeySerde(Serdes.String()) /* key serde */
          .withValueSerde(Serdes.Integer()); /* serde for aggregate value */

**Note**

**Impact of record caches** : For illustration purposes, the column "KTable `aggregated`" below shows the table's state changes over time in a very granular way. In practice, you would observe state changes in such a granular way only when [record caches](memory-mgmt.html#streams-developer-guide-memory-management-record-cache) are disabled (default: enabled). When record caches are enabled, what might happen for example is that the output results of the rows with timestamps 4 and 5 would be [compacted](memory-mgmt.html#streams-developer-guide-memory-management-record-cache), and there would only be a single state update for the key `kafka` in the KTable (here: from `(kafka 1)` directly to `(kafka, 3)`. Typically, you should only disable record caches for testing or debugging purposes - under normal circumstances it is better to leave record caches enabled.  
  
<table>  
<tr>  
<th>

 
</th>  
<th>

KStream `wordCounts`
</th>  
<th>

KGroupedStream `groupedStream`
</th>  
<th>

KTable `aggregated`
</th> </tr>  
<tr>  
<th>

Timestamp
</th>  
<th>

Input record
</th>  
<th>

Grouping
</th>  
<th>

Initializer
</th>  
<th>

Adder
</th>  
<th>

State
</th> </tr>  
<tr>  
<td>

1
</td>  
<td>

(hello, 1)
</td>  
<td>

(hello, 1)
</td>  
<td>

0 (for hello)
</td>  
<td>

(hello, 0 + 1)
</td>  
<td>



**(hello, 1)**


</td> </tr>  
<tr>  
<td>

2
</td>  
<td>

(kafka, 1)
</td>  
<td>

(kafka, 1)
</td>  
<td>

0 (for kafka)
</td>  
<td>

(kafka, 0 + 1)
</td>  
<td>



(hello, 1)

**(kafka, 1)**


</td> </tr>  
<tr>  
<td>

3
</td>  
<td>

(streams, 1)
</td>  
<td>

(streams, 1)
</td>  
<td>

0 (for streams)
</td>  
<td>

(streams, 0 + 1)
</td>  
<td>



(hello, 1)

(kafka, 1)

**(streams, 1)**


</td> </tr>  
<tr>  
<td>

4
</td>  
<td>

(kafka, 1)
</td>  
<td>

(kafka, 1)
</td>  
<td>

 
</td>  
<td>

(kafka, 1 + 1)
</td>  
<td>



(hello, 1)

(kafka, **2**)

(streams, 1)


</td> </tr>  
<tr>  
<td>

5
</td>  
<td>

(kafka, 1)
</td>  
<td>

(kafka, 1)
</td>  
<td>

 
</td>  
<td>

(kafka, 2 + 1)
</td>  
<td>



(hello, 1)

(kafka, **3**)

(streams, 1)


</td> </tr>  
<tr>  
<td>

6
</td>  
<td>

(streams, 1)
</td>  
<td>

(streams, 1)
</td>  
<td>

 
</td>  
<td>

(streams, 1 + 1)
</td>  
<td>



(hello, 1)

(kafka, 3)

(streams, **2**)


</td> </tr> </table>

**Example of semantics for table aggregations:** A `KGroupedTable` -> `KTable` example is shown below. The tables are initially empty. Bold font is used in the column for "KTable `aggregated`" to highlight changed state. An entry such as `(hello, 1)` denotes a record with key `hello` and value `1`. To improve the readability of the semantics table you can assume that all records are processed in timestamp order.
    
    
    // Key: username, value: user region (abbreviated to "E" for "Europe", "A" for "Asia")
    KTable<String, String> userProfiles = ...;
    
    // Re-group `userProfiles`.  Don't read too much into what the grouping does:
    // its prime purpose in this example is to show the *effects* of the grouping
    // in the subsequent aggregation.
    KGroupedTable<String, Integer> groupedTable = userProfiles
        .groupBy((user, region) -> KeyValue.pair(region, user.length()), Serdes.String(), Serdes.Integer());
    
    KTable<String, Integer> aggregated = groupedTable.aggregate(
        () -> 0, /* initializer */
        (aggKey, newValue, aggValue) -> aggValue + newValue, /* adder */
        (aggKey, oldValue, aggValue) -> aggValue - oldValue, /* subtractor */
        Materialized.<String, Long, KeyValueStore<Bytes, byte[]>as("aggregated-table-store" /* state store name */)
          .withKeySerde(Serdes.String()) /* key serde */
          .withValueSerde(Serdes.Integer()); /* serde for aggregate value */

**Note**

**Impact of record caches** : For illustration purposes, the column "KTable `aggregated`" below shows the table's state changes over time in a very granular way. In practice, you would observe state changes in such a granular way only when [record caches](memory-mgmt.html#streams-developer-guide-memory-management-record-cache) are disabled (default: enabled). When record caches are enabled, what might happen for example is that the output results of the rows with timestamps 4 and 5 would be [compacted](memory-mgmt.html#streams-developer-guide-memory-management-record-cache), and there would only be a single state update for the key `kafka` in the KTable (here: from `(kafka 1)` directly to `(kafka, 3)`. Typically, you should only disable record caches for testing or debugging purposes - under normal circumstances it is better to leave record caches enabled.  
  
<table>  
<tr>  
<th>

 
</th>  
<th>

KTable `userProfiles`
</th>  
<th>

KGroupedTable `groupedTable`
</th>  
<th>

KTable `aggregated`
</th> </tr>  
<tr>  
<th>

Timestamp
</th>  
<th>

Input record
</th>  
<th>

Interpreted as
</th>  
<th>

Grouping
</th>  
<th>

Initializer
</th>  
<th>

Adder
</th>  
<th>

Subtractor
</th>  
<th>

State
</th> </tr>  
<tr>  
<td>

1
</td>  
<td>

(alice, E)
</td>  
<td>

INSERT alice
</td>  
<td>

(E, 5)
</td>  
<td>

0 (for E)
</td>  
<td>

(E, 0 + 5)
</td>  
<td>

 
</td>  
<td>



**(E, 5)**


</td> </tr>  
<tr>  
<td>

2
</td>  
<td>

(bob, A)
</td>  
<td>

INSERT bob
</td>  
<td>

(A, 3)
</td>  
<td>

0 (for A)
</td>  
<td>

(A, 0 + 3)
</td>  
<td>

 
</td>  
<td>



**(A, 3)**

(E, 5)


</td> </tr>  
<tr>  
<td>

3
</td>  
<td>

(charlie, A)
</td>  
<td>

INSERT charlie
</td>  
<td>

(A, 7)
</td>  
<td>

 
</td>  
<td>

(A, 3 + 7)
</td>  
<td>

 
</td>  
<td>



(A, **10**)

(E, 5)


</td> </tr>  
<tr>  
<td>

4
</td>  
<td>

(alice, A)
</td>  
<td>

UPDATE alice
</td>  
<td>

(A, 5)
</td>  
<td>

 
</td>  
<td>

(A, 10 + 5)
</td>  
<td>

(E, 5 - 5)
</td>  
<td>



(A, **15**)

(E, **0**)


</td> </tr>  
<tr>  
<td>

5
</td>  
<td>

(charlie, null)
</td>  
<td>

DELETE charlie
</td>  
<td>

(null, 7)
</td>  
<td>

 
</td>  
<td>

 
</td>  
<td>

(A, 15 - 7)
</td>  
<td>



(A, **8**)

(E, 0)


</td> </tr>  
<tr>  
<td>

6
</td>  
<td>

(null, E)
</td>  
<td>

_ignored_
</td>  
<td>

 
</td>  
<td>

 
</td>  
<td>

 
</td>  
<td>

 
</td>  
<td>



(A, 8)

(E, 0)


</td> </tr>  
<tr>  
<td>

7
</td>  
<td>

(bob, E)
</td>  
<td>

UPDATE bob
</td>  
<td>

(E, 3)
</td>  
<td>

 
</td>  
<td>

(E, 0 + 3)
</td>  
<td>

(A, 8 - 3)
</td>  
<td>



(A, **5**)

(E, **3**)


</td> </tr> </table>

### Joining

Streams and tables can also be joined. Many stream processing applications in practice are coded as streaming joins. For example, applications backing an online shop might need to access multiple, updating database tables (e.g. sales prices, inventory, customer information) in order to enrich a new data record (e.g. customer transaction) with context information. That is, scenarios where you need to perform table lookups at very large scale and with a low processing latency. Here, a popular pattern is to make the information in the databases available in Kafka through so-called _change data capture_ in combination with [Kafka's Connect API](../../#connect), and then implementing applications that leverage the Streams API to perform very fast and efficient local joins of such tables and streams, rather than requiring the application to make a query to a remote database over the network for each record. In this example, the KTable concept in Kafka Streams would enable you to track the latest state (e.g., snapshot) of each table in a local state store, thus greatly reducing the processing latency as well as reducing the load of the remote databases when doing such streaming joins.

The following join operations are supported, see also the diagram in the overview section of Stateful Transformations. Depending on the operands, joins are either windowed joins or non-windowed joins.  
  
<table>  
<tr>  
<th>

Join operands
</th>  
<th>

Type
</th>  
<th>

(INNER) JOIN
</th>  
<th>

LEFT JOIN
</th>  
<th>

OUTER JOIN
</th> </tr>  
<tr>  
<td>

KStream-to-KStream
</td>  
<td>

Windowed
</td>  
<td>

Supported
</td>  
<td>

Supported
</td>  
<td>

Supported
</td> </tr>  
<tr>  
<td>

KTable-to-KTable
</td>  
<td>

Non-windowed
</td>  
<td>

Supported
</td>  
<td>

Supported
</td>  
<td>

Supported
</td> </tr>  
<tr>  
<td>

KTable-to-KTable Foreign-Key Join
</td>  
<td>

Non-windowed
</td>  
<td>

Supported
</td>  
<td>

Supported
</td>  
<td>

Not Supported
</td> </tr>  
<tr>  
<td>

KStream-to-KTable
</td>  
<td>

Non-windowed
</td>  
<td>

Supported
</td>  
<td>

Supported
</td>  
<td>

Not Supported
</td> </tr>  
<tr>  
<td>

KStream-to-GlobalKTable
</td>  
<td>

Non-windowed
</td>  
<td>

Supported
</td>  
<td>

Supported
</td>  
<td>

Not Supported
</td> </tr>  
<tr>  
<td>

KTable-to-GlobalKTable
</td>  
<td>

N/A
</td>  
<td>

Not Supported
</td>  
<td>

Not Supported
</td>  
<td>

Not Supported
</td> </tr> </table>

Each case is explained in more detail in the subsequent sections.

#### Join co-partitioning requirements

For equi-joins, input data must be co-partitioned when joining. This ensures that input records with the same key from both sides of the join, are delivered to the same stream task during processing. **It is your responsibility to ensure data co-partitioning when joining**. Co-partitioning is not required when performing KTable-KTable Foreign-Key joins and Global KTable joins. 

The requirements for data co-partitioning are:

  * The input topics of the join (left side and right side) must have the **same number of partitions**.
  * All applications that _write_ to the input topics must have the **same partitioning strategy** so that records with the same key are delivered to same partition number. In other words, the keyspace of the input data must be distributed across partitions in the same manner. This means that, for example, applications that use Kafka's [Java Producer API](../../#producerapi) must use the same partitioner (cf. the producer setting `"partitioner.class"` aka `ProducerConfig.PARTITIONER_CLASS_CONFIG`), and applications that use the Kafka's Streams API must use the same `StreamPartitioner` for operations such as `KStream#to()`. The good news is that, if you happen to use the default partitioner-related settings across all applications, you do not need to worry about the partitioning strategy.



Why is data co-partitioning required? Because KStream-KStream, KTable-KTable, and KStream-KTable joins are performed based on the keys of records (e.g., `leftRecord.key == rightRecord.key`), it is required that the input streams/tables of a join are co-partitioned by key.

There are two exceptions where co-partitioning is not required. For KStream-GlobalKTable joins joins, co-partitioning is not required because _all_ partitions of the `GlobalKTable`'s underlying changelog stream are made available to each `KafkaStreams` instance. That is, each instance has a full copy of the changelog stream. Further, a `KeyValueMapper` allows for non-key based joins from the `KStream` to the `GlobalKTable`. KTable-KTable Foreign-Key joins also do not require co-partitioning. Kafka Streams internally ensures co-partitioning for Foreign-Key joins. 

**Note**

**Kafka Streams partly verifies the co-partitioning requirement:** During the partition assignment step, i.e. at runtime, Kafka Streams verifies whether the number of partitions for both sides of a join are the same. If they are not, a `TopologyBuilderException` (runtime exception) is being thrown. Note that Kafka Streams cannot verify whether the partitioning strategy matches between the input streams/tables of a join - it is up to the user to ensure that this is the case.

**Ensuring data co-partitioning:** If the inputs of a join are not co-partitioned yet, you must ensure this manually. You may follow a procedure such as outlined below. It is recommended to repartition the topic with fewer partitions to match the larger partition number of avoid bottlenecks. Technically it would also be possible to repartition the topic with more partitions to the smaller partition number. For stream-table joins, it's recommended to repartition the KStream because repartitioning a KTable might result in a second state store. For table-table joins, you might also consider to size of the KTables and repartition the smaller KTable.

  1. Identify the input KStream/KTable in the join whose underlying Kafka topic has the smaller number of partitions. Let's call this stream/table "SMALLER", and the other side of the join "LARGER". To learn about the number of partitions of a Kafka topic you can use, for example, the CLI tool `bin/kafka-topics` with the `--describe` option.

  2. Within your application, re-partition the data of "SMALLER". You must ensure that, when repartitioning the data with `repartition`, the same partitioner is used as for "LARGER".

>      * If "SMALLER" is a KStream: `KStream#repartition(Repartitioned.numberOfPartitions(...))`.
>      * If "SMALLER" is a KTable: `KTable#toStream#repartition(Repartitioned.numberOfPartitions(...).toTable())`.

  3. Within your application, perform the join between "LARGER" and the new stream/table.




#### KStream-KStream Join

KStream-KStream joins are always windowed joins, because otherwise the size of the internal state store used to perform the join - e.g., a sliding window or "buffer" - would grow indefinitely. For stream-stream joins it's important to highlight that a new input record on one side will produce a join output _for each_ matching record on the other side, and there can be _multiple_ such matching records in a given join window (cf. the row with timestamp 15 in the join semantics table below, for example).

Join output records are effectively created as follows, leveraging the user-supplied `ValueJoiner`:
    
    
    KeyValue<K, LV> leftRecord = ...;
    KeyValue<K, RV> rightRecord = ...;
    ValueJoiner<LV, RV, JV> joiner = ...;
    
    KeyValue<K, JV> joinOutputRecord = KeyValue.pair(
        leftRecord.key, /* by definition, leftRecord.key == rightRecord.key */
        joiner.apply(leftRecord.value, rightRecord.value)
      );  
  
<table>  
<tr>  
<th>

Transformation
</th>  
<th>

Description
</th> </tr>  
<tr>  
<td>



**Inner Join (windowed)**

  * (KStream, KStream) -> KStream


</td>  
<td>



Performs an INNER JOIN of this stream with another stream. Even though this operation is windowed, the joined stream will be of type `KStream<K, ...>` rather than `KStream<Windowed<K>, ...>`. [(details)](/37/javadoc/org/apache/kafka/streams/kstream/KStream.html#join-org.apache.kafka.streams.kstream.KStream-org.apache.kafka.streams.kstream.ValueJoiner-org.apache.kafka.streams.kstream.JoinWindows-)

**Data must be co-partitioned** : The input data for both sides must be co-partitioned.

**Causes data re-partitioning of a stream if and only if the stream was marked for re-partitioning (if both are marked, both are re-partitioned).**

Several variants of `join` exists, see the Javadocs for details.
    
    
    import java.time.Duration;
    KStream<String, Long> left = ...;
    KStream<String, Double> right = ...;
    
    // Java 8+ example, using lambda expressions
    KStream<String, String> joined = left.join(right,
        (leftValue, rightValue) -> "left=" + leftValue + ", right=" + rightValue, /* ValueJoiner */
        JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofMinutes(5)),
        Joined.with(
          Serdes.String(), /* key */
          Serdes.Long(),   /* left value */
          Serdes.Double())  /* right value */
      );
    
    // Java 7 example
    KStream<String, String> joined = left.join(right,
        new ValueJoiner<Long, Double, String>() {
          @Override
          public String apply(Long leftValue, Double rightValue) {
            return "left=" + leftValue + ", right=" + rightValue;
          }
        },
        JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofMinutes(5)),
        Joined.with(
          Serdes.String(), /* key */
          Serdes.Long(),   /* left value */
          Serdes.Double())  /* right value */
      );

Detailed behavior:

  * The join is _key-based_ , i.e. with the join predicate `leftRecord.key == rightRecord.key`, and _window-based_ , i.e. two input records are joined if and only if their timestamps are "close" to each other as defined by the user-supplied `JoinWindows`, i.e. the window defines an additional join predicate over the record timestamps.

  * The join will be triggered under the conditions listed below whenever new input is received. When it is triggered, the user-supplied `ValueJoiner` will be called to produce join output records.

>     * Input records with a `null` key or a `null` value are ignored and do not trigger the join.




See the semantics overview at the bottom of this section for a detailed description.


</td> </tr>  
<tr>  
<td>



**Left Join (windowed)**

  * (KStream, KStream) -> KStream


</td>  
<td>



Performs a LEFT JOIN of this stream with another stream. Even though this operation is windowed, the joined stream will be of type `KStream<K, ...>` rather than `KStream<Windowed<K>, ...>`. [(details)](/37/javadoc/org/apache/kafka/streams/kstream/KStream.html#leftJoin-org.apache.kafka.streams.kstream.KStream-org.apache.kafka.streams.kstream.ValueJoiner-org.apache.kafka.streams.kstream.JoinWindows-)

**Data must be co-partitioned** : The input data for both sides must be co-partitioned.

**Causes data re-partitioning of a stream if and only if the stream was marked for re-partitioning (if both are marked, both are re-partitioned).**

Several variants of `leftJoin` exists, see the Javadocs for details.
    
    
    import java.time.Duration;
    KStream<String, Long> left = ...;
    KStream<String, Double> right = ...;
    
    // Java 8+ example, using lambda expressions
    KStream<String, String> joined = left.leftJoin(right,
        (leftValue, rightValue) -> "left=" + leftValue + ", right=" + rightValue, /* ValueJoiner */
        JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofMinutes(5)),
        Joined.with(
          Serdes.String(), /* key */
          Serdes.Long(),   /* left value */
          Serdes.Double())  /* right value */
      );
    
    // Java 7 example
    KStream<String, String> joined = left.leftJoin(right,
        new ValueJoiner<Long, Double, String>() {
          @Override
          public String apply(Long leftValue, Double rightValue) {
            return "left=" + leftValue + ", right=" + rightValue;
          }
        },
        JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofMinutes(5)),
        Joined.with(
          Serdes.String(), /* key */
          Serdes.Long(),   /* left value */
          Serdes.Double())  /* right value */
      );

Detailed behavior:

  * The join is _key-based_ , i.e. with the join predicate `leftRecord.key == rightRecord.key`, and _window-based_ , i.e. two input records are joined if and only if their timestamps are "close" to each other as defined by the user-supplied `JoinWindows`, i.e. the window defines an additional join predicate over the record timestamps.

  * The join will be triggered under the conditions listed below whenever new input is received. When it is triggered, the user-supplied `ValueJoiner` will be called to produce join output records.

>     * Input records with a `null` value are ignored and do not trigger the join.

  * For each input record on the left side that does not have any match on the right side, the `ValueJoiner` will be called with `ValueJoiner#apply(leftRecord.value, null)`; this explains the row with timestamp=60 and timestampe=80 in the table below, which lists `[E, null]` and `[F, null]`in the LEFT JOIN column. Note that these left results are emitted after the specified grace period passed. **Caution:** using the deprecated `JoinWindows.of(...).grace(...)` API might result in eagerly emitted spurious left results.




See the semantics overview at the bottom of this section for a detailed description.


</td> </tr>  
<tr>  
<td>



**Outer Join (windowed)**

  * (KStream, KStream) -> KStream


</td>  
<td>



Performs an OUTER JOIN of this stream with another stream. Even though this operation is windowed, the joined stream will be of type `KStream<K, ...>` rather than `KStream<Windowed<K>, ...>`. [(details)](/37/javadoc/org/apache/kafka/streams/kstream/KStream.html#outerJoin-org.apache.kafka.streams.kstream.KStream-org.apache.kafka.streams.kstream.ValueJoiner-org.apache.kafka.streams.kstream.JoinWindows-)

**Data must be co-partitioned** : The input data for both sides must be co-partitioned.

**Causes data re-partitioning of a stream if and only if the stream was marked for re-partitioning (if both are marked, both are re-partitioned).**

Several variants of `outerJoin` exists, see the Javadocs for details.
    
    
    import java.time.Duration;
    KStream<String, Long> left = ...;
    KStream<String, Double> right = ...;
    
    // Java 8+ example, using lambda expressions
    KStream<String, String> joined = left.outerJoin(right,
        (leftValue, rightValue) -> "left=" + leftValue + ", right=" + rightValue, /* ValueJoiner */
        JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofMinutes(5)),
        Joined.with(
          Serdes.String(), /* key */
          Serdes.Long(),   /* left value */
          Serdes.Double())  /* right value */
      );
    
    // Java 7 example
    KStream<String, String> joined = left.outerJoin(right,
        new ValueJoiner<Long, Double, String>() {
          @Override
          public String apply(Long leftValue, Double rightValue) {
            return "left=" + leftValue + ", right=" + rightValue;
          }
        },
        JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofMinutes(5)),
        Joined.with(
          Serdes.String(), /* key */
          Serdes.Long(),   /* left value */
          Serdes.Double())  /* right value */
      );

Detailed behavior:

  * The join is _key-based_ , i.e. with the join predicate `leftRecord.key == rightRecord.key`, and _window-based_ , i.e. two input records are joined if and only if their timestamps are "close" to each other as defined by the user-supplied `JoinWindows`, i.e. the window defines an additional join predicate over the record timestamps.

  * The join will be triggered under the conditions listed below whenever new input is received. When it is triggered, the user-supplied `ValueJoiner` will be called to produce join output records.

>     * Input records with a `null` value are ignored and do not trigger the join.

  * For each input record on one side that does not have any match on the other side, the `ValueJoiner` will be called with `ValueJoiner#apply(leftRecord.value, null)` or `ValueJoiner#apply(null, rightRecord.value)`, respectively; this explains the row with timestamp=60, timestamp=80, and timestamp=100 in the table below, which lists `[E, null]`, `[F, null]`, and `[null, f]` in the OUTER JOIN column. Note that these left and right results are emitted after the specified grace period passed. **Caution:** using the deprecated `JoinWindows.of(...).grace(...)` API might result in eagerly emitted spurious left/right results.




See the semantics overview at the bottom of this section for a detailed description.


</td> </tr> </table>

**Semantics of stream-stream joins:** The semantics of the various stream-stream join variants are explained below. To improve the readability of the table, assume that (1) all records have the same key (and thus the key in the table is omitted), and (2) all records are processed in timestamp order. We assume a join window size of 10 seconds with a grace period of 5 seconds.

**Note:** If you use the old and now deprecated API to specify the grace period, i.e., `JoinWindows.of(...).grace(...)`, left/outer join results are emitted eagerly, and the observed result might differ from the result shown below.

The columns INNER JOIN, LEFT JOIN, and OUTER JOIN denote what is passed as arguments to the user-supplied [ValueJoiner](/37/javadoc/org/apache/kafka/streams/kstream/ValueJoiner.html) for the `join`, `leftJoin`, and `outerJoin` methods, respectively, whenever a new input record is received on either side of the join. An empty table cell denotes that the `ValueJoiner` is not called at all.  
  
<table>  
<tr>  
<th>

Timestamp
</th>  
<th>

Left (KStream)
</th>  
<th>

Right (KStream)
</th>  
<th>

(INNER) JOIN
</th>  
<th>

LEFT JOIN
</th>  
<th>

OUTER JOIN
</th> </tr>  
<tr>  
<td>

1
</td>  
<td>

null
</td>  
<td>

 
</td>  
<td>

 
</td>  
<td>

 
</td>  
<td>

 
</td> </tr>  
<tr>  
<td>

2
</td>  
<td>

 
</td>  
<td>

null
</td>  
<td>

 
</td>  
<td>

 
</td>  
<td>

 
</td> </tr>  
<tr>  
<td>

3
</td>  
<td>

A
</td>  
<td>

 
</td>  
<td>

 
</td>  
<td>


</td>  
<td>


</td> </tr>  
<tr>  
<td>

4
</td>  
<td>

 
</td>  
<td>

a
</td>  
<td>

[A, a]
</td>  
<td>

[A, a]
</td>  
<td>

[A, a]
</td> </tr>  
<tr>  
<td>

5
</td>  
<td>

B
</td>  
<td>

 
</td>  
<td>

[B, a]
</td>  
<td>

[B, a]
</td>  
<td>

[B, a]
</td> </tr>  
<tr>  
<td>

6
</td>  
<td>

 
</td>  
<td>

b
</td>  
<td>

[A, b], [B, b]
</td>  
<td>

[A, b], [B, b]
</td>  
<td>

[A, b], [B, b]
</td> </tr>  
<tr>  
<td>

7
</td>  
<td>

null
</td>  
<td>

 
</td>  
<td>

 
</td>  
<td>

 
</td>  
<td>

 
</td> </tr>  
<tr>  
<td>

8
</td>  
<td>

 
</td>  
<td>

null
</td>  
<td>

 
</td>  
<td>

 
</td>  
<td>

 
</td> </tr>  
<tr>  
<td>

9
</td>  
<td>

C
</td>  
<td>

 
</td>  
<td>

[C, a], [C, b]
</td>  
<td>

[C, a], [C, b]
</td>  
<td>

[C, a], [C, b]
</td> </tr>  
<tr>  
<td>

10
</td>  
<td>

 
</td>  
<td>

c
</td>  
<td>

[A, c], [B, c], [C, c]
</td>  
<td>

[A, c], [B, c], [C, c]
</td>  
<td>

[A, c], [B, c], [C, c]
</td> </tr>  
<tr>  
<td>

11
</td>  
<td>

 
</td>  
<td>

null
</td>  
<td>

 
</td>  
<td>

 
</td>  
<td>

 
</td> </tr>  
<tr>  
<td>

12
</td>  
<td>

null
</td>  
<td>

 
</td>  
<td>

 
</td>  
<td>

 
</td>  
<td>

 
</td> </tr>  
<tr>  
<td>

13
</td>  
<td>

 
</td>  
<td>

null
</td>  
<td>

 
</td>  
<td>

 
</td>  
<td>

 
</td> </tr>  
<tr>  
<td>

14
</td>  
<td>

 
</td>  
<td>

d
</td>  
<td>

[A, d], [B, d], [C, d]
</td>  
<td>

[A, d], [B, d], [C, d]
</td>  
<td>

[A, d], [B, d], [C, d]
</td> </tr>  
<tr>  
<td>

15
</td>  
<td>

D
</td>  
<td>

 
</td>  
<td>

[D, a], [D, b], [D, c], [D, d]
</td>  
<td>

[D, a], [D, b], [D, c], [D, d]
</td>  
<td>

[D, a], [D, b], [D, c], [D, d]
</td> </tr>  
<tr>  
<td>

... 
</td>  
<td>


</td>  
<td>


</td>  
<td>


</td>  
<td>


</td>  
<td>


</td> </tr>  
<tr>  
<td>

40
</td>  
<td>

E
</td>  
<td>


</td>  
<td>


</td>  
<td>


</td>  
<td>


</td> </tr>  
<tr>  
<td>

... 
</td>  
<td>


</td>  
<td>


</td>  
<td>


</td>  
<td>


</td>  
<td>


</td> </tr>  
<tr>  
<td>

60
</td>  
<td>

F
</td>  
<td>


</td>  
<td>


</td>  
<td>

[E, null]
</td>  
<td>

[E, null]
</td> </tr>  
<tr>  
<td>

... 
</td>  
<td>


</td>  
<td>


</td>  
<td>


</td>  
<td>


</td>  
<td>


</td> </tr>  
<tr>  
<td>

80
</td>  
<td>


</td>  
<td>

f
</td>  
<td>


</td>  
<td>

[F, null]
</td>  
<td>

[F, null]
</td> </tr>  
<tr>  
<td>

... 
</td>  
<td>


</td>  
<td>


</td>  
<td>


</td>  
<td>


</td>  
<td>


</td> </tr>  
<tr>  
<td>

100
</td>  
<td>

G
</td>  
<td>


</td>  
<td>


</td>  
<td>


</td>  
<td>

[null, f]
</td> </tr> </table>

#### KTable-KTable Equi-Join

KTable-KTable equi-joins are always _non-windowed_ joins. They are designed to be consistent with their counterparts in relational databases. The changelog streams of both KTables are materialized into local state stores to represent the latest snapshot of their table duals. The join result is a new KTable that represents the changelog stream of the join operation.

Join output records are effectively created as follows, leveraging the user-supplied `ValueJoiner`:
    
    
    KeyValue<K, LV> leftRecord = ...;
    KeyValue<K, RV> rightRecord = ...;
    ValueJoiner<LV, RV, JV> joiner = ...;
    
    KeyValue<K, JV> joinOutputRecord = KeyValue.pair(
        leftRecord.key, /* by definition, leftRecord.key == rightRecord.key */
        joiner.apply(leftRecord.value, rightRecord.value)
      );  
  
<table>  
<tr>  
<th>

Transformation
</th>  
<th>

Description
</th> </tr>  
<tr>  
<td>



**Inner Join**

  * (KTable, KTable) -> KTable


</td>  
<td>



Performs an INNER JOIN of this table with another table. The result is an ever-updating KTable that represents the "current" result of the join. [(details)](/37/javadoc/org/apache/kafka/streams/kstream/KTable.html#join-org.apache.kafka.streams.kstream.KTable-org.apache.kafka.streams.kstream.ValueJoiner-)

**Data must be co-partitioned** : The input data for both sides must be co-partitioned.
    
    
    KTable<String, Long> left = ...;
    KTable<String, Double> right = ...;
    
    // Java 8+ example, using lambda expressions
    KTable<String, String> joined = left.join(right,
        (leftValue, rightValue) -> "left=" + leftValue + ", right=" + rightValue /* ValueJoiner */
      );
    
    // Java 7 example
    KTable<String, String> joined = left.join(right,
        new ValueJoiner<Long, Double, String>() {
          @Override
          public String apply(Long leftValue, Double rightValue) {
            return "left=" + leftValue + ", right=" + rightValue;
          }
        });

Detailed behavior:

  * The join is _key-based_ , i.e. with the join predicate `leftRecord.key == rightRecord.key`.

  * The join will be triggered under the conditions listed below whenever new input is received. When it is triggered, the user-supplied `ValueJoiner` will be called to produce join output records.

>     * Input records with a `null` key are ignored and do not trigger the join.
>     * Input records with a `null` value are interpreted as _tombstones_ for the corresponding key, which indicate the deletion of the key from the table. Tombstones do not trigger the join. When an input tombstone is received, then an output tombstone is forwarded directly to the join result KTable if required (i.e. only if the corresponding key actually exists already in the join result KTable).
>     * When joining versioned tables, out-of-order input records, i.e., those for which another record from the same table, with the same key and a larger timestamp, has already been processed, are ignored and do not trigger the join.




See the semantics overview at the bottom of this section for a detailed description.


</td> </tr>  
<tr>  
<td>



**Left Join**

  * (KTable, KTable) -> KTable


</td>  
<td>



Performs a LEFT JOIN of this table with another table. [(details)](/37/javadoc/org/apache/kafka/streams/kstream/KTable.html#leftJoin-org.apache.kafka.streams.kstream.KTable-org.apache.kafka.streams.kstream.ValueJoiner-)

**Data must be co-partitioned** : The input data for both sides must be co-partitioned.
    
    
    KTable<String, Long> left = ...;
    KTable<String, Double> right = ...;
    
    // Java 8+ example, using lambda expressions
    KTable<String, String> joined = left.leftJoin(right,
        (leftValue, rightValue) -> "left=" + leftValue + ", right=" + rightValue /* ValueJoiner */
      );
    
    // Java 7 example
    KTable<String, String> joined = left.leftJoin(right,
        new ValueJoiner<Long, Double, String>() {
          @Override
          public String apply(Long leftValue, Double rightValue) {
            return "left=" + leftValue + ", right=" + rightValue;
          }
        });

Detailed behavior:

  * The join is _key-based_ , i.e. with the join predicate `leftRecord.key == rightRecord.key`.

  * The join will be triggered under the conditions listed below whenever new input is received. When it is triggered, the user-supplied `ValueJoiner` will be called to produce join output records.

>     * Input records with a `null` key are ignored and do not trigger the join.
>     * Input records with a `null` value are interpreted as _tombstones_ for the corresponding key, which indicate the deletion of the key from the table. Right-tombstones trigger the join, but left-tombstones don't: when an input tombstone is received, an output tombstone is forwarded directly to the join result KTable if required (i.e. only if the corresponding key actually exists already in the join result KTable).
>     * When joining versioned tables, out-of-order input records, i.e., those for which another record from the same table, with the same key and a larger timestamp, has already been processed, are ignored and do not trigger the join.

  * For each input record on the left side that does not have any match on the right side, the `ValueJoiner` will be called with `ValueJoiner#apply(leftRecord.value, null)`; this explains the row with timestamp=3 in the table below, which lists `[A, null]` in the LEFT JOIN column.




See the semantics overview at the bottom of this section for a detailed description.


</td> </tr>  
<tr>  
<td>



**Outer Join**

  * (KTable, KTable) -> KTable


</td>  
<td>



Performs an OUTER JOIN of this table with another table. [(details)](/37/javadoc/org/apache/kafka/streams/kstream/KTable.html#outerJoin-org.apache.kafka.streams.kstream.KTable-org.apache.kafka.streams.kstream.ValueJoiner-)

**Data must be co-partitioned** : The input data for both sides must be co-partitioned.
    
    
    KTable<String, Long> left = ...;
    KTable<String, Double> right = ...;
    
    // Java 8+ example, using lambda expressions
    KTable<String, String> joined = left.outerJoin(right,
        (leftValue, rightValue) -> "left=" + leftValue + ", right=" + rightValue /* ValueJoiner */
      );
    
    // Java 7 example
    KTable<String, String> joined = left.outerJoin(right,
        new ValueJoiner<Long, Double, String>() {
          @Override
          public String apply(Long leftValue, Double rightValue) {
            return "left=" + leftValue + ", right=" + rightValue;
          }
        });

Detailed behavior:

  * The join is _key-based_ , i.e. with the join predicate `leftRecord.key == rightRecord.key`.

  * The join will be triggered under the conditions listed below whenever new input is received. When it is triggered, the user-supplied `ValueJoiner` will be called to produce join output records.

>     * Input records with a `null` key are ignored and do not trigger the join.
>     * Input records with a `null` value are interpreted as _tombstones_ for the corresponding key, which indicate the deletion of the key from the table. Tombstones may trigger joins, depending on the content in the left and right tables. When an input tombstone is received, an output tombstone is forwarded directly to the join result KTable if required (i.e. only if the corresponding key actually exists already in the join result KTable).
>     * When joining versioned tables, out-of-order input records, i.e., those for which another record from the same table, with the same key and a larger timestamp, has already been processed, are ignored and do not trigger the join.

  * For each input record on one side that does not have any match on the other side, the `ValueJoiner` will be called with `ValueJoiner#apply(leftRecord.value, null)` or `ValueJoiner#apply(null, rightRecord.value)`, respectively; this explains the rows with timestamp=3 and timestamp=7 in the table below, which list `[A, null]` and `[null, b]`, respectively, in the OUTER JOIN column.




See the semantics overview at the bottom of this section for a detailed description.


</td> </tr> </table>

**Semantics of table-table equi-joins:** The semantics of the various table-table equi-join variants are explained below. To improve the readability of the table, you can assume that (1) all records have the same key (and thus the key in the table is omitted) and that (2) all records are processed in timestamp order. The columns INNER JOIN, LEFT JOIN, and OUTER JOIN denote what is passed as arguments to the user-supplied [ValueJoiner](/37/javadoc/org/apache/kafka/streams/kstream/ValueJoiner.html) for the `join`, `leftJoin`, and `outerJoin` methods, respectively, whenever a new input record is received on either side of the join. An empty table cell denotes that the `ValueJoiner` is not called at all.  
  
<table>  
<tr>  
<th>

Timestamp
</th>  
<th>

Left (KTable)
</th>  
<th>

Right (KTable)
</th>  
<th>

(INNER) JOIN
</th>  
<th>

LEFT JOIN
</th>  
<th>

OUTER JOIN
</th> </tr>  
<tr>  
<td>

1
</td>  
<td>

null
</td>  
<td>

 
</td>  
<td>

 
</td>  
<td>

 
</td>  
<td>

 
</td> </tr>  
<tr>  
<td>

2
</td>  
<td>

 
</td>  
<td>

null
</td>  
<td>

 
</td>  
<td>

 
</td>  
<td>

 
</td> </tr>  
<tr>  
<td>

3
</td>  
<td>

A
</td>  
<td>

 
</td>  
<td>

 
</td>  
<td>

[A, null]
</td>  
<td>

[A, null]
</td> </tr>  
<tr>  
<td>

4
</td>  
<td>

 
</td>  
<td>

a
</td>  
<td>

[A, a]
</td>  
<td>

[A, a]
</td>  
<td>

[A, a]
</td> </tr>  
<tr>  
<td>

5
</td>  
<td>

B
</td>  
<td>

 
</td>  
<td>

[B, a]
</td>  
<td>

[B, a]
</td>  
<td>

[B, a]
</td> </tr>  
<tr>  
<td>

6
</td>  
<td>

 
</td>  
<td>

b
</td>  
<td>

[B, b]
</td>  
<td>

[B, b]
</td>  
<td>

[B, b]
</td> </tr>  
<tr>  
<td>

7
</td>  
<td>

null
</td>  
<td>

 
</td>  
<td>

null
</td>  
<td>

null
</td>  
<td>

[null, b]
</td> </tr>  
<tr>  
<td>

8
</td>  
<td>

 
</td>  
<td>

null
</td>  
<td>

 
</td>  
<td>

 
</td>  
<td>

null
</td> </tr>  
<tr>  
<td>

9
</td>  
<td>

C
</td>  
<td>

 
</td>  
<td>

 
</td>  
<td>

[C, null]
</td>  
<td>

[C, null]
</td> </tr>  
<tr>  
<td>

10
</td>  
<td>

 
</td>  
<td>

c
</td>  
<td>

[C, c]
</td>  
<td>

[C, c]
</td>  
<td>

[C, c]
</td> </tr>  
<tr>  
<td>

11
</td>  
<td>

 
</td>  
<td>

null
</td>  
<td>

null
</td>  
<td>

[C, null]
</td>  
<td>

[C, null]
</td> </tr>  
<tr>  
<td>

12
</td>  
<td>

null
</td>  
<td>

 
</td>  
<td>

 
</td>  
<td>

null
</td>  
<td>

null
</td> </tr>  
<tr>  
<td>

13
</td>  
<td>

 
</td>  
<td>

null
</td>  
<td>

 
</td>  
<td>

 
</td>  
<td>

 
</td> </tr>  
<tr>  
<td>

14
</td>  
<td>

 
</td>  
<td>

d
</td>  
<td>

 
</td>  
<td>

 
</td>  
<td>

[null, d]
</td> </tr>  
<tr>  
<td>

15
</td>  
<td>

D
</td>  
<td>

 
</td>  
<td>

[D, d]
</td>  
<td>

[D, d]
</td>  
<td>

[D, d]
</td> </tr> </table>

#### KTable-KTable Foreign-Key Join

KTable-KTable foreign-key joins are always _non-windowed_ joins. Foreign-key joins are analogous to joins in SQL. As a rough example: 

` SELECT ... FROM {this KTable} JOIN {other KTable} ON {other.key} = {result of foreignKeyExtractor(this.value)} ... `

The output of the operation is a new KTable containing the join result. 

The changelog streams of both KTables are materialized into local state stores to represent the latest snapshot of their table duals. A foreign-key extractor function is applied to the left record, with a new intermediate record created and is used to lookup and join with the corresponding primary key on the right hand side table. The result is a new KTable that represents the changelog stream of the join operation.

The left KTable can have multiple records which map to the same key on the right KTable. An update to a single left KTable entry may result in a single output event, provided the corresponding key exists in the right KTable. Consequently, a single update to a right KTable entry will result in an update for each record in the left KTable that has the same foreign key.  
  
  
  
<table>  
<tr>  
<th>

Transformation
</th>  
<th>

Description
</th> </tr>  
<tr>  
<td>



**Inner Join**

  * (KTable, KTable) -> KTable


</td>  
<td>



Performs a foreign-key INNER JOIN of this table with another table. The result is an ever-updating KTable that represents the "current" result of the join. [(details)](/%7B%7Bversion%7D%7D/javadoc/org/apache/kafka/streams/kstream/KTable.html#join-org.apache.kafka.streams.kstream.KTable-org.apache.kafka.streams.kstream.ValueJoiner-)
    
    
    KTable<String, Long> left = ...;
                    KTable<Long, Double> right = ...;
    //This foreignKeyExtractor simply uses the left-value to map to the right-key.
    Function<Long, Long> foreignKeyExtractor = (x) -> x;
    
    // Java 8+ example, using lambda expressions
                    KTable<String, String> joined = left.join(right, foreignKeyExtractor,
                        (leftValue, rightValue) -> "left=" + leftValue + ", right=" + rightValue /* ValueJoiner */
                      );

Detailed behavior:

  * The join is _key-based_ , i.e. with the join predicate: 
        
        foreignKeyExtractor.apply(leftRecord.value) == rightRecord.key

  * The join will be triggered under the conditions listed below whenever new input is received. When it is triggered, the user-supplied `ValueJoiner` will be called to produce join output records.

>     * Records for which the `foreignKeyExtractor` produces `null` are ignored and do not trigger a join. If you want to join with `null` foreign keys, use a suitable sentinel value to do so (i.e. `"NULL"` for a String field, or `-1` for an auto-incrementing integer field). 
>     * Input records with a `null` value are interpreted as _tombstones_ for the corresponding key, which indicate the deletion of the key from the table. Tombstones do not trigger the join. When an input tombstone is received, then an output tombstone is forwarded directly to the join result KTable if required (i.e. only if the corresponding key actually exists already in the join result KTable).
>     * When joining versioned tables, out-of-order input records, i.e., those for which another record from the same table, with the same key and a larger timestamp, has already been processed, are ignored and do not trigger the join.




See the semantics overview at the bottom of this section for a detailed description.


</td> </tr>  
<tr>  
<td>



**Left Join**

  * (KTable, KTable) -> KTable


</td>  
<td>



Performs a foreign-key LEFT JOIN of this table with another table. [(details)](/%7B%7Bversion%7D%7D/javadoc/org/apache/kafka/streams/kstream/KTable.html#leftJoin-org.apache.kafka.streams.kstream.KTable-org.apache.kafka.streams.kstream.ValueJoiner-)
    
    
    KTable<String, Long> left = ...;
                    KTable<Long, Double> right = ...;
    //This foreignKeyExtractor simply uses the left-value to map to the right-key.
    Function<Long, Long> foreignKeyExtractor = (x) -> x;
    
    // Java 8+ example, using lambda expressions
                    KTable<String, String> joined = left.leftJoin(right, foreignKeyExtractor,
                        (leftValue, rightValue) -> "left=" + leftValue + ", right=" + rightValue /* ValueJoiner */
                      );

Detailed behavior:

  * The join is _key-based_ , i.e. with the join predicate: 
        
        foreignKeyExtractor.apply(leftRecord.value) == rightRecord.key

  * The join will be triggered under the conditions listed below whenever new input is received. When it is triggered, the user-supplied `ValueJoiner` will be called to produce join output records.

>     * Input records with a `null` value are interpreted as _tombstones_ for the corresponding key, which indicate the deletion of the key from the table. Right-tombstones trigger the join, but left-tombstones don't: when an input tombstone is received, then an output tombstone is forwarded directly to the join result KTable if required (i.e. only if the corresponding key actually exists already in the join result KTable).
>     * When joining versioned tables, out-of-order input records, i.e., those for which another record from the same table, with the same key and a larger timestamp, has already been processed, are ignored and do not trigger the join.

  * For each input record on the left side that does not have any match on the right side, the `ValueJoiner` will be called with `ValueJoiner#apply(leftRecord.value, null)`; this explains the row with timestamp=7 & 8 in the table below, which lists `(q,10,null) and (r,10,null)` in the LEFT JOIN column.




See the semantics overview at the bottom of this section for a detailed description.


</td> </tr> </table>

**Semantics of table-table foreign-key joins:** The semantics of the table-table foreign-key INNER and LEFT JOIN variants are demonstrated below. The key is shown alongside the value for each record. Records are processed in incrementing offset order. The columns INNER JOIN and LEFT JOIN denote what is passed as arguments to the user-supplied [ValueJoiner](/%7B%7Bversion%7D%7D/javadoc/org/apache/kafka/streams/kstream/ValueJoiner.html) for the `join` and `leftJoin` methods, respectively, whenever a new input record is received on either side of the join. An empty table cell denotes that the `ValueJoiner` is not called at all. For the purpose of this example, `Function foreignKeyExtractor` simply uses the left-value as the output.   
  
<table>  
<tr>  
<th>

Record Offset
</th>  
<th>

Left KTable (K, extracted-FK)
</th>  
<th>

Right KTable (FK, VR)
</th>  
<th>

(INNER) JOIN
</th>  
<th>

LEFT JOIN
</th> </tr>  
<tr>  
<td>

1
</td>  
<td>

(k,1)
</td>  
<td>

 (1,foo)
</td>  
<td>

(k,1,foo)   

</td>  
<td>

(k,1,foo)
</td> </tr>  
<tr>  
<td>

2
</td>  
<td>

(k,2)  
</td>  
<td>

  

</td>  
<td>

(k,null)
</td>  
<td>

(k,2,null)   

</td> </tr>  
<tr>  
<td>

3
</td>  
<td>

(k,3)  

</td>  
<td>

 
</td>  
<td>

(k,null)
</td>  
<td>

(k,3,null)  

</td> </tr>  
<tr>  
<td>

4
</td>  
<td>

 
</td>  
<td>

(3,bar)  

</td>  
<td>

(k,3,bar)  

</td>  
<td>

(k,3,bar)  

</td> </tr>  
<tr>  
<td>

5
</td>  
<td>

(k,null)  

</td>  
<td>

 
</td>  
<td>

(k,null)  

</td>  
<td>

(k,null,null)  
</td> </tr>  
<tr>  
<td>

6
</td>  
<td>

(k,1)
</td>  
<td>

  

</td>  
<td>

(k,1,foo)  

</td>  
<td>

(k,1,foo)  

</td> </tr>  
<tr>  
<td>

7
</td>  
<td>

(q,10)  

</td>  
<td>

 
</td>  
<td>

  

</td>  
<td>

(q,10,null) 
</td> </tr>  
<tr>  
<td>

8
</td>  
<td>

(r,10)
</td>  
<td>

  

</td>  
<td>

 
</td>  
<td>

(r,10,null)
</td> </tr>  
<tr>  
<td>

9
</td>  
<td>

  

</td>  
<td>

(10,baz)
</td>  
<td>

(q,10,baz), (r,10,baz)
</td>  
<td>

(q,10,baz), (r,10,baz)
</td> </tr> </table>

#### KStream-KTable Join

KStream-KTable joins are always _non-windowed_ joins. They allow you to perform _table lookups_ against a KTable (changelog stream) upon receiving a new record from the KStream (record stream). An example use case would be to enrich a stream of user activities (KStream) with the latest user profile information (KTable).

Join output records are effectively created as follows, leveraging the user-supplied `ValueJoiner`:
    
    
    KeyValue<K, LV> leftRecord = ...;
    KeyValue<K, RV> rightRecord = ...;
    ValueJoiner<LV, RV, JV> joiner = ...;
    
    KeyValue<K, JV> joinOutputRecord = KeyValue.pair(
        leftRecord.key, /* by definition, leftRecord.key == rightRecord.key */
        joiner.apply(leftRecord.value, rightRecord.value)
      );  
  
<table>  
<tr>  
<th>

Transformation
</th>  
<th>

Description
</th> </tr>  
<tr>  
<td>



**Inner Join**

  * (KStream, KTable) -> KStream


</td>  
<td>



Performs an INNER JOIN of this stream with the table, effectively doing a table lookup. [(details)](/37/javadoc/org/apache/kafka/streams/kstream/KStream.html#join-org.apache.kafka.streams.kstream.KTable-org.apache.kafka.streams.kstream.ValueJoiner-)

**Data must be co-partitioned** : The input data for both sides must be co-partitioned.

**Causes data re-partitioning of the stream if and only if the stream was marked for re-partitioning.**

Several variants of `join` exists, see the Javadocs for details.
    
    
    KStream<String, Long> left = ...;
    KTable<String, Double> right = ...;
    
    // Java 8+ example, using lambda expressions
    KStream<String, String> joined = left.join(right,
        (leftValue, rightValue) -> "left=" + leftValue + ", right=" + rightValue, /* ValueJoiner */
        Joined.keySerde(Serdes.String()) /* key */
          .withValueSerde(Serdes.Long()) /* left value */
          .withGracePeriod(Duration.ZERO) /* grace period */
      );
    
    // Java 7 example
    KStream<String, String> joined = left.join(right,
        new ValueJoiner<Long, Double, String>() {
          @Override
          public String apply(Long leftValue, Double rightValue) {
            return "left=" + leftValue + ", right=" + rightValue;
          }
        },
        Joined.keySerde(Serdes.String()) /* key */
          .withValueSerde(Serdes.Long()) /* left value */
          .withGracePeriod(Duration.ZERO) /* grace period */
      );

Detailed behavior:

  * The join is _key-based_ , i.e. with the join predicate `leftRecord.key == rightRecord.key`.

  * The join will be triggered under the conditions listed below whenever new input is received. When it is triggered, the user-supplied `ValueJoiner` will be called to produce join output records.

>     * Only input records for the left side (stream) trigger the join. Input records for the right side (table) update only the internal right-side join state.
>     * Input records for the stream with a `null` key or a `null` value are ignored and do not trigger the join.
>     * Input records for the table with a `null` value are interpreted as _tombstones_ for the corresponding key, which indicate the deletion of the key from the table. Tombstones do not trigger the join.

  * When the table is versioned, the table record to join with is determined by performing a timestamped lookup, i.e., the table record which is joined will be the latest-by-timestamp record with timestamp less than or equal to the stream record timestamp. If the stream record timestamp is older than the table's history retention, then the record is dropped.
  * To use the grace period, the table needs to be versioned. This will cause the stream to buffer for the specified grace period before trying to find a matching record with the right timestamp in the table. The case where the grace period would be used for is if a record in the table has a timestamp less than or equal to the stream record timestamp but arrives after the stream record. If the table record arrives within the grace period the join will still occur. If the table record does not arrive before the grace period the join will continue as normal. 



See the semantics overview at the bottom of this section for a detailed description.


</td> </tr>  
<tr>  
<td>



**Left Join**

  * (KStream, KTable) -> KStream


</td>  
<td>



Performs a LEFT JOIN of this stream with the table, effectively doing a table lookup. [(details)](/37/javadoc/org/apache/kafka/streams/kstream/KStream.html#leftJoin-org.apache.kafka.streams.kstream.KTable-org.apache.kafka.streams.kstream.ValueJoiner-)

**Data must be co-partitioned** : The input data for both sides must be co-partitioned.

**Causes data re-partitioning of the stream if and only if the stream was marked for re-partitioning.**

Several variants of `leftJoin` exists, see the Javadocs for details.
    
    
    KStream<String, Long> left = ...;
    KTable<String, Double> right = ...;
    
    // Java 8+ example, using lambda expressions
    KStream<String, String> joined = left.leftJoin(right,
        (leftValue, rightValue) -> "left=" + leftValue + ", right=" + rightValue, /* ValueJoiner */
        Joined.keySerde(Serdes.String()) /* key */
          .withValueSerde(Serdes.Long()) /* left value */
          .withGracePeriod(Duration.ZERO) /* grace period */
      );
    
    // Java 7 example
    KStream<String, String> joined = left.leftJoin(right,
        new ValueJoiner<Long, Double, String>() {
          @Override
          public String apply(Long leftValue, Double rightValue) {
            return "left=" + leftValue + ", right=" + rightValue;
          }
        },
        Joined.keySerde(Serdes.String()) /* key */
          .withValueSerde(Serdes.Long()) /* left value */
          .withGracePeriod(Duration.ZERO) /* grace period */
      );

Detailed behavior:

  * The join is _key-based_ , i.e. with the join predicate `leftRecord.key == rightRecord.key`.

  * The join will be triggered under the conditions listed below whenever new input is received. When it is triggered, the user-supplied `ValueJoiner` will be called to produce join output records.

>     * Only input records for the left side (stream) trigger the join. Input records for the right side (table) update only the internal right-side join state.
>     * Input records for the stream with a `null` value are ignored and do not trigger the join.
>     * Input records for the table with a `null` value are interpreted as _tombstones_ for the corresponding key, which indicate the deletion of the key from the table. Tombstones do not trigger the join.

  * For each input record on the left side that does not have any match on the right side, the `ValueJoiner` will be called with `ValueJoiner#apply(leftRecord.value, null)`; this explains the row with timestamp=3 in the table below, which lists `[A, null]` in the LEFT JOIN column.

  * When the table is versioned, the table record to join with is determined by performing a timestamped lookup, i.e., the table record which is joined will be the latest-by-timestamp record with timestamp less than or equal to the stream record timestamp. If the stream record timestamp is older than the table's history retention, then the record that is joined will be `null`.
  * To use the grace period, the table needs to be versioned. This will cause the stream to buffer for the specified grace period before trying to find a matching record with the right timestamp in the table. The case where the grace period would be used for is if a record in the table has a timestamp less than or equal to the stream record timestamp but arrives after the stream record. If the table record arrives within the grace period the join will still occur. If the table record does not arrive before the grace period the join will continue as normal. 



See the semantics overview at the bottom of this section for a detailed description.


</td> </tr> </table>

**Semantics of stream-table joins:** The semantics of the various stream-table join variants are explained below. To improve the readability of the table we assume that (1) all records have the same key (and thus we omit the key in the table) and that (2) all records are processed in timestamp order. The columns INNER JOIN and LEFT JOIN denote what is passed as arguments to the user-supplied [ValueJoiner](/37/javadoc/org/apache/kafka/streams/kstream/ValueJoiner.html) for the `join` and `leftJoin` methods, respectively, whenever a new input record is received on either side of the join. An empty table cell denotes that the `ValueJoiner` is not called at all.  
  
<table>  
<tr>  
<th>

Timestamp
</th>  
<th>

Left (KStream)
</th>  
<th>

Right (KTable)
</th>  
<th>

(INNER) JOIN
</th>  
<th>

LEFT JOIN
</th> </tr>  
<tr>  
<td>

1
</td>  
<td>

null
</td>  
<td>

 
</td>  
<td>

 
</td>  
<td>

 
</td> </tr>  
<tr>  
<td>

2
</td>  
<td>

 
</td>  
<td>

null
</td>  
<td>

 
</td>  
<td>

 
</td> </tr>  
<tr>  
<td>

3
</td>  
<td>

A
</td>  
<td>

 
</td>  
<td>

 
</td>  
<td>

[A, null]
</td> </tr>  
<tr>  
<td>

4
</td>  
<td>

 
</td>  
<td>

a
</td>  
<td>

 
</td>  
<td>

 
</td> </tr>  
<tr>  
<td>

5
</td>  
<td>

B
</td>  
<td>

 
</td>  
<td>

[B, a]
</td>  
<td>

[B, a]
</td> </tr>  
<tr>  
<td>

6
</td>  
<td>

 
</td>  
<td>

b
</td>  
<td>

 
</td>  
<td>

 
</td> </tr>  
<tr>  
<td>

7
</td>  
<td>

null
</td>  
<td>

 
</td>  
<td>

 
</td>  
<td>

 
</td> </tr>  
<tr>  
<td>

8
</td>  
<td>

 
</td>  
<td>

null
</td>  
<td>

 
</td>  
<td>

 
</td> </tr>  
<tr>  
<td>

9
</td>  
<td>

C
</td>  
<td>

 
</td>  
<td>

 
</td>  
<td>

[C, null]
</td> </tr>  
<tr>  
<td>

10
</td>  
<td>

 
</td>  
<td>

c
</td>  
<td>

 
</td>  
<td>

 
</td> </tr>  
<tr>  
<td>

11
</td>  
<td>

 
</td>  
<td>

null
</td>  
<td>

 
</td>  
<td>

 
</td> </tr>  
<tr>  
<td>

12
</td>  
<td>

null
</td>  
<td>

 
</td>  
<td>

 
</td>  
<td>

 
</td> </tr>  
<tr>  
<td>

13
</td>  
<td>

 
</td>  
<td>

null
</td>  
<td>

 
</td>  
<td>

 
</td> </tr>  
<tr>  
<td>

14
</td>  
<td>

 
</td>  
<td>

d
</td>  
<td>

 
</td>  
<td>

 
</td> </tr>  
<tr>  
<td>

15
</td>  
<td>

D
</td>  
<td>

 
</td>  
<td>

[D, d]
</td>  
<td>

[D, d]
</td> </tr> </table>

#### KStream-GlobalKTable Join

KStream-GlobalKTable joins are always _non-windowed_ joins. They allow you to perform _table lookups_ against a GlobalKTable (entire changelog stream) upon receiving a new record from the KStream (record stream). An example use case would be "star queries" or "star joins", where you would enrich a stream of user activities (KStream) with the latest user profile information (GlobalKTable) and further context information (further GlobalKTables). However, because GlobalKTables have no notion of time, a KStream-GlobalKTable join is not a temporal join, and there is no event-time synchronization between updates to a GlobalKTable and processing of KStream records.

At a high-level, KStream-GlobalKTable joins are very similar to KStream-KTable joins. However, global tables provide you with much more flexibility at the some expense when compared to partitioned tables:

  * They do not require data co-partitioning.
  * They allow for efficient "star joins"; i.e., joining a large-scale "facts" stream against "dimension" tables
  * They allow for joining against foreign keys; i.e., you can lookup data in the table not just by the keys of records in the stream, but also by data in the record values.
  * They make many use cases feasible where you must work on heavily skewed data and thus suffer from hot partitions.
  * They are often more efficient than their partitioned KTable counterpart when you need to perform multiple joins in succession.



Join output records are effectively created as follows, leveraging the user-supplied `ValueJoiner`:
    
    
    KeyValue<K, LV> leftRecord = ...;
    KeyValue<K, RV> rightRecord = ...;
    ValueJoiner<LV, RV, JV> joiner = ...;
    
    KeyValue<K, JV> joinOutputRecord = KeyValue.pair(
        leftRecord.key, /* by definition, leftRecord.key == rightRecord.key */
        joiner.apply(leftRecord.value, rightRecord.value)
      );  
  
<table>  
<tr>  
<th>

Transformation
</th>  
<th>

Description
</th> </tr>  
<tr>  
<td>



**Inner Join**

  * (KStream, GlobalKTable) -> KStream


</td>  
<td>



Performs an INNER JOIN of this stream with the global table, effectively doing a table lookup. [(details)](/37/javadoc/org/apache/kafka/streams/kstream/KStream.html#join-org.apache.kafka.streams.kstream.GlobalKTable-org.apache.kafka.streams.kstream.KeyValueMapper-org.apache.kafka.streams.kstream.ValueJoiner-)

The `GlobalKTable` is fully bootstrapped upon (re)start of a `KafkaStreams` instance, which means the table is fully populated with all the data in the underlying topic that is available at the time of the startup. The actual data processing begins only once the bootstrapping has completed.

**Causes data re-partitioning of the stream if and only if the stream was marked for re-partitioning.**
    
    
    KStream<String, Long> left = ...;
    GlobalKTable<Integer, Double> right = ...;
    
    // Java 8+ example, using lambda expressions
    KStream<String, String> joined = left.join(right,
        (leftKey, leftValue) -> leftKey.length(), /* derive a (potentially) new key by which to lookup against the table */
        (leftValue, rightValue) -> "left=" + leftValue + ", right=" + rightValue /* ValueJoiner */
      );
    
    // Java 7 example
    KStream<String, String> joined = left.join(right,
        new KeyValueMapper<String, Long, Integer>() { /* derive a (potentially) new key by which to lookup against the table */
          @Override
          public Integer apply(String key, Long value) {
            return key.length();
          }
        },
        new ValueJoiner<Long, Double, String>() {
          @Override
          public String apply(Long leftValue, Double rightValue) {
            return "left=" + leftValue + ", right=" + rightValue;
          }
        });

Detailed behavior:

  * The join is indirectly _key-based_ , i.e. with the join predicate `KeyValueMapper#apply(leftRecord.key, leftRecord.value) == rightRecord.key`.

  * The join will be triggered under the conditions listed below whenever new input is received. When it is triggered, the user-supplied `ValueJoiner` will be called to produce join output records.

>     * Only input records for the left side (stream) trigger the join. Input records for the right side (table) update only the internal right-side join state.
>     * Input records for the stream with a `null` key or a `null` value are ignored and do not trigger the join.
>     * Input records for the table with a `null` value are interpreted as _tombstones_ , which indicate the deletion of a record key from the table. Tombstones do not trigger the join.



</td> </tr>  
<tr>  
<td>



**Left Join**

  * (KStream, GlobalKTable) -> KStream


</td>  
<td>



Performs a LEFT JOIN of this stream with the global table, effectively doing a table lookup. [(details)](/37/javadoc/org/apache/kafka/streams/kstream/KStream.html#leftJoin-org.apache.kafka.streams.kstream.GlobalKTable-org.apache.kafka.streams.kstream.KeyValueMapper-org.apache.kafka.streams.kstream.ValueJoiner-)

The `GlobalKTable` is fully bootstrapped upon (re)start of a `KafkaStreams` instance, which means the table is fully populated with all the data in the underlying topic that is available at the time of the startup. The actual data processing begins only once the bootstrapping has completed.

**Causes data re-partitioning of the stream if and only if the stream was marked for re-partitioning.**
    
    
    KStream<String, Long> left = ...;
    GlobalKTable<Integer, Double> right = ...;
    
    // Java 8+ example, using lambda expressions
    KStream<String, String> joined = left.leftJoin(right,
        (leftKey, leftValue) -> leftKey.length(), /* derive a (potentially) new key by which to lookup against the table */
        (leftValue, rightValue) -> "left=" + leftValue + ", right=" + rightValue /* ValueJoiner */
      );
    
    // Java 7 example
    KStream<String, String> joined = left.leftJoin(right,
        new KeyValueMapper<String, Long, Integer>() { /* derive a (potentially) new key by which to lookup against the table */
          @Override
          public Integer apply(String key, Long value) {
            return key.length();
          }
        },
        new ValueJoiner<Long, Double, String>() {
          @Override
          public String apply(Long leftValue, Double rightValue) {
            return "left=" + leftValue + ", right=" + rightValue;
          }
        });

Detailed behavior:

  * The join is indirectly _key-based_ , i.e. with the join predicate `KeyValueMapper#apply(leftRecord.key, leftRecord.value) == rightRecord.key`.

  * The join will be triggered under the conditions listed below whenever new input is received. When it is triggered, the user-supplied `ValueJoiner` will be called to produce join output records.

>     * Only input records for the left side (stream) trigger the join. Input records for the right side (table) update only the internal right-side join state.
>     * Input records for the stream with a `null` value are ignored and do not trigger the join.
>     * Input records for the table with a `null` value are interpreted as _tombstones_ , which indicate the deletion of a record key from the table. Tombstones do not trigger the join.

  * For each input record on the left side that does not have any match on the right side, the `ValueJoiner` will be called with `ValueJoiner#apply(leftRecord.value, null)`.



</td> </tr> </table>

**Semantics of stream-global-table joins:** The join semantics are different to KStream-KTable joins because it's not a temporal join. Another difference is that, for KStream-GlobalKTable joins, the left input record is first "mapped" with a user-supplied `KeyValueMapper` into the table's keyspace prior to the table lookup.

### Windowing

Windowing lets you control how to group records that have the same key for stateful operations such as aggregations or joins into so-called windows. Windows are tracked per record key.

**Note**

A related operation is grouping, which groups all records that have the same key to ensure that data is properly partitioned ("keyed") for subsequent operations. Once grouped, windowing allows you to further sub-group the records of a key.

For example, in join operations, a windowing state store is used to store all the records received so far within the defined window boundary. In aggregating operations, a windowing state store is used to store the latest aggregation results per window. Old records in the state store are purged after the specified [window retention period](../core-concepts.html#streams_concepts_windowing). Kafka Streams guarantees to keep a window for at least this specified time; the default value is one day and can be changed via `Materialized#withRetention()`.

The DSL supports the following types of windows:  
  
<table>  
<tr>  
<th>

Window name
</th>  
<th>

Behavior
</th>  
<th>

Short description
</th> </tr>  
<tr>  
<td>

Hopping time window
</td>  
<td>

Time-based
</td>  
<td>

Fixed-size, overlapping windows
</td> </tr>  
<tr>  
<td>

Tumbling time window
</td>  
<td>

Time-based
</td>  
<td>

Fixed-size, non-overlapping, gap-less windows
</td> </tr>  
<tr>  
<td>

Sliding time window
</td>  
<td>

Time-based
</td>  
<td>

Fixed-size, overlapping windows that work on differences between record timestamps
</td> </tr>  
<tr>  
<td>

Session window
</td>  
<td>

Session-based
</td>  
<td>

Dynamically-sized, non-overlapping, data-driven windows
</td> </tr> </table>

#### Hopping time windows

Hopping time windows are windows based on time intervals. They model fixed-sized, (possibly) overlapping windows. A hopping window is defined by two properties: the window's _size_ and its _advance interval_ (aka "hop"). The advance interval specifies by how much a window moves forward relative to the previous one. For example, you can configure a hopping window with a size 5 minutes and an advance interval of 1 minute. Since hopping windows can overlap - and in general they do - a data record may belong to more than one such windows.

**Note**

**Hopping windows vs. sliding windows:** Hopping windows are sometimes called "sliding windows" in other stream processing tools. Kafka Streams follows the terminology in academic literature, where the semantics of sliding windows are different to those of hopping windows.

The following code defines a hopping window with a size of 5 minutes and an advance interval of 1 minute:
    
    
    import java.time.Duration;
    import org.apache.kafka.streams.kstream.TimeWindows;
    
    // A hopping time window with a size of 5 minutes and an advance interval of 1 minute.
    // The window's name -- the string parameter -- is used to e.g. name the backing state store.
    Duration windowSize = Duration.ofMinutes(5);
    Duration advance = Duration.ofMinutes(1);
    TimeWindows.ofSizeWithNoGrace(windowSize).advanceBy(advance);

![](/37/images/streams-time-windows-hopping.png)

This diagram shows windowing a stream of data records with hopping windows. In this diagram the time numbers represent minutes; e.g. t=5 means "at the five-minute mark". In reality, the unit of time in Kafka Streams is milliseconds, which means the time numbers would need to be multiplied with 60 * 1,000 to convert from minutes to milliseconds (e.g. t=5 would become t=300,000).

Hopping time windows are _aligned to the epoch_ , with the lower interval bound being inclusive and the upper bound being exclusive. "Aligned to the epoch" means that the first window starts at timestamp zero. For example, hopping windows with a size of 5000ms and an advance interval ("hop") of 3000ms have predictable window boundaries `[0;5000),[3000;8000),...` -- and **not** `[1000;6000),[4000;9000),...` or even something "random" like `[1452;6452),[4452;9452),...`.

Unlike non-windowed aggregates that we have seen previously, windowed aggregates return a _windowed KTable_ whose keys type is `Windowed<K>`. This is to differentiate aggregate values with the same key from different windows. The corresponding window instance and the embedded key can be retrieved as `Windowed#window()` and `Windowed#key()`, respectively.

#### Tumbling time windows

Tumbling time windows are a special case of hopping time windows and, like the latter, are windows based on time intervals. They model fixed-size, non-overlapping, gap-less windows. A tumbling window is defined by a single property: the window's _size_. A tumbling window is a hopping window whose window size is equal to its advance interval. Since tumbling windows never overlap, a data record will belong to one and only one window.

![](/37/images/streams-time-windows-tumbling.png)

This diagram shows windowing a stream of data records with tumbling windows. Windows do not overlap because, by definition, the advance interval is identical to the window size. In this diagram the time numbers represent minutes; e.g. t=5 means "at the five-minute mark". In reality, the unit of time in Kafka Streams is milliseconds, which means the time numbers would need to be multiplied with 60 * 1,000 to convert from minutes to milliseconds (e.g. t=5 would become t=300,000).

Tumbling time windows are _aligned to the epoch_ , with the lower interval bound being inclusive and the upper bound being exclusive. "Aligned to the epoch" means that the first window starts at timestamp zero. For example, tumbling windows with a size of 5000ms have predictable window boundaries `[0;5000),[5000;10000),...` -- and **not** `[1000;6000),[6000;11000),...` or even something "random" like `[1452;6452),[6452;11452),...`.

The following code defines a tumbling window with a size of 5 minutes:
    
    
    import java.time.Duration;
    import org.apache.kafka.streams.kstream.TimeWindows;
    
    // A tumbling time window with a size of 5 minutes (and, by definition, an implicit
    // advance interval of 5 minutes), and grace period of 1 minute.
    Duration windowSize = Duration.ofMinutes(5);
    Duration gracePeriod = Duration.ofMinutes(1);
    TimeWindows.ofSizeAndGrace(windowSize, gracePeriod);
    
    // The above is equivalent to the following code:
    TimeWindows.ofSizeAndGrace(windowSize, gracePeriod).advanceBy(windowSize);

#### Sliding time windows

Sliding windows are actually quite different from hopping and tumbling windows. In Kafka Streams, sliding windows are used for join operations, specified by using the `JoinWindows` class, and windowed aggregations, specified by using the `SlidingWindows` class.

A sliding window models a fixed-size window that slides continuously over the time axis. In this model, two data records are said to be included in the same window if (in the case of symmetric windows) the difference of their timestamps is within the window size. As a sliding window moves along the time axis, records may fall into multiple snapshots of the sliding window, but each unique combination of records appears only in one sliding window snapshot.

The following code defines a sliding window with a time difference of 10 minutes and a grace period of 30 minutes:
    
    
    import org.apache.kafka.streams.kstream.SlidingWindows;
    
    // A sliding time window with a time difference of 10 minutes and grace period of 30 minutes
    Duration timeDifference = Duration.ofMinutes(10);
    Duration gracePeriod = Duration.ofMinutes(30);
    SlidingWindows.ofTimeDifferenceAndGrace(timeDifference, gracePeriod);

![](/37/images/streams-sliding-windows.png)

This diagram shows windowing a stream of data records with sliding windows. The overlap of the sliding window snapshots varies depending on the record times. In this diagram, the time numbers represent milliseconds. For example, t=5 means "at the five millisecond mark".

Sliding windows are aligned to the data record timestamps, not to the epoch. In contrast to hopping and tumbling windows, the lower and upper window time interval bounds of sliding windows are both inclusive.

#### Session Windows

Session windows are used to aggregate key-based events into so-called _sessions_ , the process of which is referred to as _sessionization_. Sessions represent a **period of activity** separated by a defined **gap of inactivity** (or "idleness"). Any events processed that fall within the inactivity gap of any existing sessions are merged into the existing sessions. If an event falls outside of the session gap, then a new session will be created.

Session windows are different from the other window types in that:

  * all windows are tracked independently across keys - e.g. windows of different keys typically have different start and end times
  * their window sizes sizes vary - even windows for the same key typically have different sizes



The prime area of application for session windows is **user behavior analysis**. Session-based analyses can range from simple metrics (e.g. count of user visits on a news website or social platform) to more complex metrics (e.g. customer conversion funnel and event flows).

The following code defines a session window with an inactivity gap of 5 minutes:
    
    
    import java.time.Duration;
    import org.apache.kafka.streams.kstream.SessionWindows;
    
    // A session window with an inactivity gap of 5 minutes.
    SessionWindows.ofInactivityGapWithNoGrace(Duration.ofMinutes(5));

Given the previous session window example, here's what would happen on an input stream of six records. When the first three records arrive (upper part of in the diagram below), we'd have three sessions (see lower part) after having processed those records: two for the green record key, with one session starting and ending at the 0-minute mark (only due to the illustration it looks as if the session goes from 0 to 1), and another starting and ending at the 6-minute mark; and one session for the blue record key, starting and ending at the 2-minute mark.

![](/37/images/streams-session-windows-01.png)

Detected sessions after having received three input records: two records for the green record key at t=0 and t=6, and one record for the blue record key at t=2. In this diagram the time numbers represent minutes; e.g. t=5 means "at the five-minute mark". In reality, the unit of time in Kafka Streams is milliseconds, which means the time numbers would need to be multiplied with 60 * 1,000 to convert from minutes to milliseconds (e.g. t=5 would become t=300,000).

If we then receive three additional records (including two out-of-order records), what would happen is that the two existing sessions for the green record key will be merged into a single session starting at time 0 and ending at time 6, consisting of a total of three records. The existing session for the blue record key will be extended to end at time 5, consisting of a total of two records. And, finally, there will be a new session for the blue key starting and ending at time 11.

![](/37/images/streams-session-windows-02.png)

Detected sessions after having received six input records. Note the two out-of-order data records at t=4 (green) and t=5 (blue), which lead to a merge of sessions and an extension of a session, respectively.

#### Window Final Results

In Kafka Streams, windowed computations update their results continuously. As new data arrives for a window, freshly computed results are emitted downstream. For many applications, this is ideal, since fresh results are always available. and Kafka Streams is designed to make programming continuous computations seamless. However, some applications need to take action **only** on the final result of a windowed computation. Common examples of this are sending alerts or delivering results to a system that doesn't support updates. 

Suppose that you have an hourly windowed count of events per user. If you want to send an alert when a user has _less than_ three events in an hour, you have a real challenge. All users would match this condition at first, until they accrue enough events, so you cannot simply send an alert when someone matches the condition; you have to wait until you know you won't see any more events for a particular window and _then_ send the alert. 

Kafka Streams offers a clean way to define this logic: after defining your windowed computation, you can suppress the intermediate results, emitting the final count for each user when the window is **closed**. 

For example:
    
    
    KGroupedStream<UserId, Event> grouped = ...;
    grouped
        .windowedBy(TimeWindows.ofSizeAndGrace(Duration.ofHours(1), Duration.ofMinutes(10)))
        .count()
        .suppress(Suppressed.untilWindowCloses(unbounded()))
        .filter((windowedUserId, count) -> count < 3)
        .toStream()
        .foreach((windowedUserId, count) -> sendAlert(windowedUserId.window(), windowedUserId.key(), count));

The key parts of this program are: 

`ofSizeAndGrace(Duration.ofHours(1), Duration.ofMinutes(10))`
    The specified grace period of 10 minutes (i.e., the `Duration.ofMinutes(10)` argument) allows us to bound the lateness of events the window will accept. For example, the 09:00 to 10:00 window will accept out-of-order records until 10:10, at which point, the window is **closed**. 
`.suppress(Suppressed.untilWindowCloses(...))`
    This configures the suppression operator to emit nothing for a window until it closes, and then emit the final result. For example, if user `U` gets 10 events between 09:00 and 10:10, the `filter` downstream of the suppression will get no events for the windowed key `U@09:00-10:00` until 10:10, and then it will get exactly one with the value `10`. This is the final result of the windowed count. 
`unbounded()`
     This configures the buffer used for storing events until their windows close. Production code is able to put a cap on the amount of memory to use for the buffer, but this simple example creates a buffer with no upper bound. 

One thing to note is that suppression is just like any other Kafka Streams operator, so you can build a topology with two branches emerging from the `count`, one suppressed, and one not, or even multiple differently configured suppressions. This allows you to apply suppressions where they are needed and otherwise rely on the default continuous update behavior. 

For more detailed information, see the JavaDoc on the `Suppressed` config object and [KIP-328](https://cwiki.apache.org/confluence/x/sQU0BQ "KIP-328"). 

## Applying processors and transformers (Processor API integration)

Beyond the aforementioned stateless and stateful transformations, you may also leverage the [Processor API](processor-api.html#streams-developer-guide-processor-api) from the DSL. There are a number of scenarios where this may be helpful:

  * **Customization:** You need to implement special, customized logic that is not or not yet available in the DSL.
  * **Combining ease-of-use with full flexibility where it 's needed:** Even though you generally prefer to use the expressiveness of the DSL, there are certain steps in your processing that require more flexibility and tinkering than the DSL provides. For example, only the Processor API provides access to a record's metadata such as its topic, partition, and offset information. However, you don't want to switch completely to the Processor API just because of that.
  * **Migrating from other tools:** You are migrating from other stream processing technologies that provide an imperative API, and migrating some of your legacy code to the Processor API was faster and/or easier than to migrate completely to the DSL right away.

  
<table>  
<tr>  
<th>

Transformation
</th>  
<th>

Description
</th> </tr>  
<tr>  
<td>



**Process**

  * KStream -> void


</td>  
<td>



**Terminal operation.** Applies a `Processor` to each record. `process()` allows you to leverage the [Processor API](processor-api.html#streams-developer-guide-processor-api) from the DSL. ([details](/37/javadoc/org/apache/kafka/streams/kstream/KStream.html#process-org.apache.kafka.streams.processor.ProcessorSupplier-java.lang.String...-))

This is essentially equivalent to adding the `Processor` via `Topology#addProcessor()` to your [processor topology](../core-concepts.html#streams_topology).

An example is available in the [javadocs](/37/javadoc/org/apache/kafka/streams/kstream/KStream.html#process-org.apache.kafka.streams.processor.ProcessorSupplier-java.lang.String...-).


</td> </tr>  
<tr>  
<td>



**Transform**

  * KStream -> KStream


</td>  
<td>



Applies a `Transformer` to each record. `transform()` allows you to leverage the [Processor API](processor-api.html#streams-developer-guide-processor-api) from the DSL. ([details](/37/javadoc/org/apache/kafka/streams/kstream/KStream.html#transform-org.apache.kafka.streams.kstream.TransformerSupplier-java.lang.String...-))

Each input record is transformed into zero, one, or more output records (similar to the stateless `flatMap`). The `Transformer` must return `null` for zero output. You can modify the record's key and value, including their types.

**Marks the stream for data re-partitioning:** Applying a grouping or a join after `transform` will result in re-partitioning of the records. If possible use `transformValues` instead, which will not cause data re-partitioning.

`transform` is essentially equivalent to adding the `Transformer` via `Topology#addProcessor()` to your [processor topology](../core-concepts.html#streams_topology).

An example is available in the [javadocs](/37/javadoc/org/apache/kafka/streams/kstream/KStream.html#transform-org.apache.kafka.streams.kstream.TransformerSupplier-java.lang.String...-). 


</td> </tr>  
<tr>  
<td>



**Transform (values only)**

  * KStream -> KStream
  * KTable -> KTable


</td>  
<td>



Applies a `ValueTransformer` to each record, while retaining the key of the original record. `transformValues()` allows you to leverage the [Processor API](processor-api.html#streams-developer-guide-processor-api) from the DSL. ([details](/37/javadoc/org/apache/kafka/streams/kstream/KStream.html#transformValues-org.apache.kafka.streams.kstream.ValueTransformerSupplier-java.lang.String...-))

Each input record is transformed into exactly one output record (zero output records or multiple output records are not possible). The `ValueTransformer` may return `null` as the new value for a record.

`transformValues` is preferable to `transform` because it will not cause data re-partitioning.

`transformValues` is essentially equivalent to adding the `ValueTransformer` via `Topology#addProcessor()` to your [processor topology](../core-concepts.html#streams_topology).

An example is available in the [javadocs](/37/javadoc/org/apache/kafka/streams/kstream/KStream.html#transformValues-org.apache.kafka.streams.kstream.ValueTransformerSupplier-java.lang.String...-).


</td> </tr> </table>

**CAUTION:** If you are using "merge repartition topics" optimization, it is not recommended to use `KStream#processValues` to avoid compatibility issues for future upgrades to newer versions of Kafka Streams. For more details, see the [migration guide](/40/documentation/streams/developer-guide/dsl-api.htm#transformers-removal-and-migration-to-processors) in the Kafka Streams 4.0 docs.

The following example shows how to leverage, via the `KStream#process()` method, a custom `Processor` that sends an email notification whenever a page view count reaches a predefined threshold.

First, we need to implement a custom stream processor, `PopularPageEmailAlert`, that implements the `Processor` interface:
    
    
    // A processor that sends an alert message about a popular page to a configurable email address
    public class PopularPageEmailAlert implements Processor<PageId, Long, Void, Void> {
    
      private final String emailAddress;
      private ProcessorContext<Void, Void> context;
    
      public PopularPageEmailAlert(String emailAddress) {
        this.emailAddress = emailAddress;
      }
    
      @Override
      public void init(ProcessorContext<Void, Void> context) {
        this.context = context;
    
        // Here you would perform any additional initializations such as setting up an email client.
      }
    
      @Override
      void process(Record<PageId, Long> record) {
        // Here you would format and send the alert email.
        //
        // In this specific example, you would be able to include
        // information about the page's ID and its view count
      }
    
      @Override
      void close() {
        // Any code for clean up would go here, for example tearing down the email client and anything
        // else you created in the init() method
        // This processor instance will not be used again after this call.
      }
    
    }

**Tip**

Even though we do not demonstrate it in this example, a stream processor can access any available state stores by calling `ProcessorContext#getStateStore()`. State stores are only available if they have been connected to the processor, or if they are global stores. While global stores do not need to be connected explicitly, they only allow for read-only access. There are two ways to connect state stores to a processor: 

  * By passing the name of a store that has already been added via `Topology#addStateStore()` to the corresponding `KStream#process()` method call.
  * Implementing `ConnectedStoreProvider#stores()` on the `ProcessorSupplier` passed to `KStream#process()`. In this case there is no need to call `StreamsBuilder#addStateStore()` beforehand, the store will be automatically added for you. You can also implement `ConnectedStoreProvider#stores()` on the `Value*` or `*WithKey` supplier variants, or `TransformerSupplier` or any of its variants. 



Then we can leverage the `PopularPageEmailAlert` processor in the DSL via `KStream#process`.
    
    
    KStream<String, GenericRecord> pageViews = ...;
    
    // Send an email notification when the view count of a page reaches one thousand.
    pageViews.groupByKey()
             .count()
             .filter((PageId pageId, Long viewCount) -> viewCount == 1000)
             // PopularPageEmailAlert is your custom processor that implements the
             // `Processor` interface, see further down below.
             .process(() -> new PopularPageEmailAlert("alerts@yourcompany.com"));

Naming Operators in a Streams DSL application Kafka Streams allows you to [name processors](dsl-topology-naming.html) created via the Streams DSL 

# Controlling KTable emit rate

A KTable is logically a continuously updated table. These updates make their way to downstream operators whenever new data is available, ensuring that the whole computation is as fresh as possible. Logically speaking, most programs describe a series of transformations, and the update rate is not a factor in the program behavior. In these cases, the rate of update is more of a performance concern. Operators are able to optimize both the network traffic (to the Kafka brokers) and the disk traffic (to the local state stores) by adjusting commit interval and batch size configurations. 

However, some applications need to take other actions, such as calling out to external systems, and therefore need to exercise some control over the rate of invocations, for example of `KStream#foreach`. 

Rather than achieving this as a side-effect of the [KTable record cache](memory-mgmt.html#streams-developer-guide-memory-management-record-cache), you can directly impose a rate limit via the `KTable#suppress` operator. 

For example: 
    
    
    KGroupedTable<String, String> groupedTable = ...;
    groupedTable
        .count()
        .suppress(untilTimeLimit(ofMinutes(5), maxBytes(1_000_000L).emitEarlyWhenFull()))
        .toStream()
        .foreach((key, count) -> updateCountsDatabase(key, count));

This configuration ensures that `updateCountsDatabase` gets events for each `key` no more than once every 5 minutes. Note that the latest state for each key has to be buffered in memory for that 5-minute period. You have the option to control the maximum amount of memory to use for this buffer (in this case, 1MB). There is also an option to impose a limit in terms of number of records (or to leave both limits unspecified). 

Additionally, it is possible to choose what happens if the buffer fills up. This example takes a relaxed approach and just emits the oldest records before their 5-minute time limit to bring the buffer back down to size. Alternatively, you can choose to stop processing and shut the application down. This may seem extreme, but it gives you a guarantee that the 5-minute time limit will be absolutely enforced. After the application shuts down, you could allocate more memory for the buffer and resume processing. Emitting early is preferable for most applications. 

For more detailed information, see the JavaDoc on the `Suppressed` config object and [KIP-328](https://cwiki.apache.org/confluence/x/sQU0BQ "KIP-328"). 

# Using timestamp-based semantics for table processors

By default, tables in Kafka Streams use offset-based semantics. When multiple records arrive for the same key, the one with the largest record offset is considered the latest record for the key, and is the record that appears in aggregation and join results computed on the table. This is true even in the event of [out-of-order data](/37/documentation/streams/core-concepts.html#streams_out_of_ordering). The record with the largest offset is considered to be the latest record for the key, even if this record does not have the largest timestamp.

An alternative to offset-based semantics is timestamp-based semantics. With timestamp-based semantics, the record with the largest timestamp is considered the latest record, even if there is another record with a larger offset (and smaller timestamp). If there is no out-of-order data (per key), then offset-based semantics and timestamp-based semantics are equivalent; the difference only appears when there is out-of-order data.

Starting with Kafka Streams 3.5, Kafka Streams supports timestamp-based semantics through the use of [versioned state stores](/37/documentation/streams/developer-guide/processor-api.html#versioned-state-stores). When a table is materialized with a versioned state store, it is a versioned table and will result in different processor semantics in the presence of out-of-order data.

  * When performing a stream-table join, stream-side records will join with the latest-by-timestamp table record which has a timestamp less than or equal to the stream record's timestamp. This is in contrast to joining a stream to an unversioned table, in which case the latest-by-offset table record will be joined, even if the stream-side record is out-of-order and has a lower timestamp.
  * Aggregations computed on the table will include the latest-by-timestamp record for each key, instead of the latest-by-offset record. Out-of-order updates (per key) will not trigger a new aggregation result. This is true for `count` and `reduce` operations as well, in addition to `aggregate` operations.
  * Table joins will use the latest-by-timestamp record for each key, instead of the latest-by-offset record. Out-of-order updates (per key) will not trigger a new join result. This is true for both primary-key table-table joins and also foreign-key table-table joins. If a versioned table is joined with an unversioned table, the result will be the join of the latest-by-timestamp record from the versioned table with the latest-by-offset record from the unversioned table.
  * Table filter operations will no longer suppress consecutive tombstones, so users may observe more `null` records downstream of the filter than compared to when filtering an unversioned table. This is done in order to preserve a complete version history downstream, in the event of out-of-order data.
  * `suppress` operations are not allowed on versioned tables, as this would collapse the version history and lead to undefined behavior.



Once a table is materialized with a versioned store, downstream tables are also considered versioned until any of the following occurs:

  * A downstream table is explicitly materialized, either with an unversioned store supplier or with no store supplier (all stores are unversioned by default, including the default store supplier)
  * Any stateful transformation occurs, including aggregations and joins
  * A table is converted to a stream and back.



The results of certain processors should not be materialized with versioned stores, as these processors do not produce a complete older version history, and therefore materialization as a versioned table would lead to unpredictable results:

  * Aggregate processors, for both table and stream aggregations. This includes `aggregate`, `count` and `reduce` operations.
  * Table-table join processors, including both primary-key and foreign-key joins.



For more on versioned stores and how to start using them in your application, see [here](/37/documentation/streams/developer-guide/processor-api.html#versioned-state-stores).

# Writing streams back to Kafka

Any streams and tables may be (continuously) written back to a Kafka topic. As we will describe in more detail below, the output data might be re-partitioned on its way to Kafka, depending on the situation.  
  
<table>  
<tr>  
<th>

Writing to Kafka
</th>  
<th>

Description
</th> </tr>  
<tr>  
<td>



**To**

  * KStream -> void


</td>  
<td>



**Terminal operation.** Write the records to Kafka topic(s). ([KStream details](/37/javadoc/org/apache/kafka/streams/kstream/KStream.html#to\(java.lang.String\)))

When to provide serdes explicitly:

  * If you do not specify Serdes explicitly, the default Serdes from the [configuration](config-streams.html#streams-developer-guide-configuration) are used.
  * You **must specify Serdes explicitly** via the `Produced` class if the key and/or value types of the `KStream` do not match the configured default Serdes.
  * See [Data Types and Serialization](datatypes.html#streams-developer-guide-serdes) for information about configuring default Serdes, available Serdes, and implementing your own custom Serdes.



A variant of `to` exists that enables you to specify how the data is produced by using a `Produced` instance to specify, for example, a `StreamPartitioner` that gives you control over how output records are distributed across the partitions of the output topic.

Another variant of `to` exists that enables you to dynamically choose which topic to send to for each record via a `TopicNameExtractor` instance.
    
    
    KStream<String, Long> stream = ...;
    
    // Write the stream to the output topic, using the configured default key
    // and value serdes.
    stream.to("my-stream-output-topic");
    
    // Write the stream to the output topic, using explicit key and value serdes,
    // (thus overriding the defaults in the config properties).
    stream.to("my-stream-output-topic", Produced.with(Serdes.String(), Serdes.Long());

**Causes data re-partitioning if any of the following conditions is true:**

  1. If the output topic has a different number of partitions than the stream/table.
  2. If the `KStream` was marked for re-partitioning.
  3. If you provide a custom `StreamPartitioner` to explicitly control how to distribute the output records across the partitions of the output topic.
  4. If the key of an output record is `null`.


</td> </tr> </table>

**Note**

**When you want to write to systems other than Kafka:** Besides writing the data back to Kafka, you can also apply a custom processor as a stream sink at the end of the processing to, for example, write to external databases. First, doing so is not a recommended pattern - we strongly suggest to use the [Kafka Connect API](../../connect/index.html#kafka-connect) instead. However, if you do use such a sink processor, please be aware that it is now your responsibility to guarantee message delivery semantics when talking to such external systems (e.g., to retry on delivery failure or to prevent message duplication).

# Testing a Streams application

Kafka Streams comes with a `test-utils` module to help you test your application [here](testing.html). 

# Kafka Streams DSL for Scala

The Kafka Streams DSL Java APIs are based on the Builder design pattern, which allows users to incrementally build the target functionality using lower level compositional fluent APIs. These APIs can be called from Scala, but there are several issues:

  1. **Additional type annotations** \- The Java APIs use Java generics in a way that are not fully compatible with the type inferencer of the Scala compiler. Hence the user has to add type annotations to the Scala code, which seems rather non-idiomatic in Scala.
  2. **Verbosity** \- In some cases the Java APIs appear too verbose compared to idiomatic Scala.
  3. **Type Unsafety** \- The Java APIs offer some options where the compile time type safety is sometimes subverted and can result in runtime errors. This stems from the fact that the Serdes defined as part of config are not type checked during compile time. Hence any missing Serdes can result in runtime errors.



The Kafka Streams DSL for Scala library is a wrapper over the existing Java APIs for Kafka Streams DSL that addresses the concerns raised above. It does not attempt to provide idiomatic Scala APIs that one would implement in a Scala library developed from scratch. The intention is to make the Java APIs more usable in Scala through better type inferencing, enhanced expressiveness, and lesser boilerplates. 

The library wraps Java Stream DSL APIs in Scala thereby providing:

  1. Better type inference in Scala.
  2. Less boilerplate in application code.
  3. The usual builder-style composition that developers get with the original Java API.
  4. Implicit serializers and de-serializers leading to better abstraction and less verbosity.
  5. Better type safety during compile time.



All functionality provided by Kafka Streams DSL for Scala are under the root package name of `org.apache.kafka.streams.scala`.

Many of the public facing types from the Java API are wrapped. The following Scala abstractions are available to the user:

  * `org.apache.kafka.streams.scala.StreamsBuilder`
  * `org.apache.kafka.streams.scala.kstream.KStream`
  * `org.apache.kafka.streams.scala.kstream.KTable`
  * `org.apache.kafka.streams.scala.kstream.KGroupedStream`
  * `org.apache.kafka.streams.scala.kstream.KGroupedTable`
  * `org.apache.kafka.streams.scala.kstream.SessionWindowedKStream`
  * `org.apache.kafka.streams.scala.kstream.TimeWindowedKStream`



The library also has several utility abstractions and modules that the user needs to use for proper semantics.

  * `org.apache.kafka.streams.scala.ImplicitConversions`: Module that brings into scope the implicit conversions between the Scala and Java classes.
  * `org.apache.kafka.streams.scala.serialization.Serdes`: Module that contains all primitive Serdes that can be imported as implicits and a helper to create custom Serdes.



The library is cross-built with Scala 2.12 and 2.13. To reference the library compiled against Scala 2.13 include the following in your maven `pom.xml` add the following:
    
    
    <dependency>
      <groupId>org.apache.kafka</groupId>
      <artifactId>kafka-streams-scala_2.13</artifactId>
      <version>3.7.2</version>
    </dependency>

To use the library compiled against Scala 2.12 replace the `artifactId` with `kafka-streams-scala_2.12`.

When using SBT then you can reference the correct library using the following:
    
    
    libraryDependencies += "org.apache.kafka" %% "kafka-streams-scala" % "3.7.2"

## Sample Usage

The library works by wrapping the original Java abstractions of Kafka Streams within a Scala wrapper object and then using implicit conversions between them. All the Scala abstractions are named identically as the corresponding Java abstraction, but they reside in a different package of the library e.g. the Scala class `org.apache.kafka.streams.scala.StreamsBuilder` is a wrapper around `org.apache.kafka.streams.StreamsBuilder`, `org.apache.kafka.streams.scala.kstream.KStream` is a wrapper around `org.apache.kafka.streams.kstream.KStream`, and so on.

Here's an example of the classic WordCount program that uses the Scala `StreamsBuilder` that builds an instance of `KStream` which is a wrapper around Java `KStream`. Then we reify to a table and get a `KTable`, which, again is a wrapper around Java `KTable`.

The net result is that the following code is structured just like using the Java API, but with Scala and with far fewer type annotations compared to using the Java API directly from Scala. The difference in type annotation usage is more obvious when given an example. Below is an example WordCount implementation that will be used to demonstrate the differences between the Scala and Java API.
    
    
    import java.time.Duration
    import java.util.Properties
    
    import org.apache.kafka.streams.kstream.Materialized
    import org.apache.kafka.streams.scala.ImplicitConversions._
    import org.apache.kafka.streams.scala._
    import org.apache.kafka.streams.scala.kstream._
    import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}
    
    object WordCountApplication extends App {
      import Serdes._
    
      val props: Properties = {
        val p = new Properties()
        p.put(StreamsConfig.APPLICATION_ID_CONFIG, "wordcount-application")
        p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka-broker1:9092")
        p
      }
    
      val builder: StreamsBuilder = new StreamsBuilder
      val textLines: KStream[String, String] = builder.stream[String, String]("TextLinesTopic")
      val wordCounts: KTable[String, Long] = textLines
        .flatMapValues(textLine => textLine.toLowerCase.split("\W+"))
        .groupBy((_, word) => word)
        .count(Materialized.as("counts-store"))
      wordCounts.toStream.to("WordsWithCountsTopic")
    
      val streams: KafkaStreams = new KafkaStreams(builder.build(), props)
      streams.start()
    
      sys.ShutdownHookThread {
         streams.close(Duration.ofSeconds(10))
      }
    }

In the above code snippet, we don't have to provide any Serdes, `Grouped`, `Produced`, `Consumed` or `Joined` explicitly. They will also not be dependent on any Serdes specified in the config. **In fact all Serdes specified in the config will be ignored by the Scala APIs**. All Serdes and `Grouped`, `Produced`, `Consumed` or `Joined` will be handled through implicit Serdes as discussed later in the Implicit Serdes section. The complete independence from configuration based Serdes is what makes this library completely typesafe. Any missing instances of Serdes, `Grouped`, `Produced`, `Consumed` or `Joined` will be flagged as a compile time error.

## Implicit Serdes

One of the common complaints of Scala users with the Java API has been the repetitive usage of the Serdes in API invocations. Many of the APIs need to take the Serdes through abstractions like `Grouped`, `Produced`, `Repartitioned`, `Consumed` or `Joined`. And the user has to supply them every time through the with function of these classes.

The library uses the power of [Scala implicit parameters](https://docs.scala-lang.org/tour/implicit-parameters.html) to alleviate this concern. As a user you can provide implicit Serdes or implicit values of `Grouped`, `Produced`, `Repartitioned`, `Consumed` or `Joined` once and make your code less verbose. In fact you can just have the implicit Serdes in scope and the library will make the instances of `Grouped`, `Produced`, `Consumed` or `Joined` available in scope.

The library also bundles all implicit Serdes of the commonly used primitive types in a Scala module - so just import the module vals and have all Serdes in scope. A similar strategy of modular implicits can be adopted for any user-defined Serdes as well (User-defined Serdes are discussed in the next section).

Here's an example:
    
    
    // DefaultSerdes brings into scope implicit Serdes (mostly for primitives)
    // that will set up all Grouped, Produced, Consumed and Joined instances.
    // So all APIs below that accept Grouped, Produced, Consumed or Joined will
    // get these instances automatically
    import Serdes._
    
    val builder = new StreamsBuilder()
    
    val userClicksStream: KStream[String, Long] = builder.stream(userClicksTopic)
    
    val userRegionsTable: KTable[String, String] = builder.table(userRegionsTopic)
    
    // The following code fragment does not have a single instance of Grouped,
    // Produced, Consumed or Joined supplied explicitly.
    // All of them are taken care of by the implicit Serdes imported by DefaultSerdes
    val clicksPerRegion: KTable[String, Long] =
      userClicksStream
        .leftJoin(userRegionsTable)((clicks, region) => (if (region == null) "UNKNOWN" else region, clicks))
        .map((_, regionWithClicks) => regionWithClicks)
        .groupByKey
        .reduce(_ + _)
    
    clicksPerRegion.toStream.to(outputTopic)

Quite a few things are going on in the above code snippet that may warrant a few lines of elaboration:

  1. The code snippet does not depend on any config defined Serdes. In fact any Serdes defined as part of the config will be ignored.
  2. All Serdes are picked up from the implicits in scope. And `import Serdes._` brings all necessary Serdes in scope.
  3. This is an example of compile time type safety that we don't have in the Java APIs.
  4. The code looks less verbose and more focused towards the actual transformation that it does on the data stream.



## User-Defined Serdes

When the default primitive Serdes are not enough and we need to define custom Serdes, the usage is exactly the same as above. Just define the implicit Serdes and start building the stream transformation. Here's an example with `AvroSerde`:
    
    
    // domain object as a case class
    case class UserClicks(clicks: Long)
    
    // An implicit Serde implementation for the values we want to
    // serialize as avro
    implicit val userClicksSerde: Serde[UserClicks] = new AvroSerde
    
    // Primitive Serdes
    import Serdes._
    
    // And then business as usual ..
    
    val userClicksStream: KStream[String, UserClicks] = builder.stream(userClicksTopic)
    
    val userRegionsTable: KTable[String, String] = builder.table(userRegionsTopic)
    
    // Compute the total per region by summing the individual click counts per region.
    val clicksPerRegion: KTable[String, Long] =
     userClicksStream
    
       // Join the stream against the table.
       .leftJoin(userRegionsTable)((clicks, region) => (if (region == null) "UNKNOWN" else region, clicks.clicks))
    
       // Change the stream from <user> -> <region, clicks> to <region> -> <clicks>
       .map((_, regionWithClicks) => regionWithClicks)
    
       // Compute the total per region by summing the individual click counts per region.
       .groupByKey
       .reduce(_ + _)
    
    // Write the (continuously updating) results to the output topic.
    clicksPerRegion.toStream.to(outputTopic)

A complete example of user-defined Serdes can be found in a test class within the library.

  * [Documentation](/documentation)
  * [Kafka Streams](/documentation/streams)
  * [Developer Guide](/documentation/streams/developer-guide/)


