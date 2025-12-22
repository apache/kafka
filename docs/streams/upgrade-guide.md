---
title: Upgrade Guide
description: 
weight: 6
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


# Upgrade Guide and API Changes

Upgrading from any older version to 4.3.0 is possible: if upgrading from 3.4 or below, you will need to do two rolling bounces, where during the first rolling bounce phase you set the config `upgrade.from="older version"` (possible values are `"2.4" - "3.4"`) and during the second you remove it. This is required to safely handle 2 changes. The first is a change in foreign-key join serialization format. The second is a change in the serialization format for an internal repartition topic. For more details, please refer to [KIP-904](https://cwiki.apache.org/confluence/x/P5VbDg): 

  * prepare your application instances for a rolling bounce and make sure that config `upgrade.from` is set to the version from which it is being upgrade.
  * bounce each instance of your application once 
  * prepare your newly deployed 4.3.0 application instances for a second round of rolling bounces; make sure to remove the value for config `upgrade.from`
  * bounce each instance of your application once more to complete the upgrade 



As an alternative, an offline upgrade is also possible. Upgrading from any versions as old as 0.11.0.x to 4.3.0 in offline mode require the following steps: 

  * stop all old (e.g., 0.11.0.x) application instances 
  * update your code and swap old code and jar file with new code and new jar file 
  * restart all new (4.3.0) application instances 



For a table that shows Streams API compatibility with Kafka broker versions, see Broker Compatibility.

## Notable compatibility changes in past releases

Starting in version 4.0.0, Kafka Streams will only be compatible when running against brokers on version 2.1 or higher. Additionally, exactly-once semantics (EOS) will require brokers to be at least version 2.5. 

Downgrading from 3.5.x or newer version to 3.4.x or older version needs special attention: Since 3.5.0 release, Kafka Streams uses a new serialization format for repartition topics. This means that older versions of Kafka Streams would not be able to recognize the bytes written by newer versions, and hence it is harder to downgrade Kafka Streams with version 3.5.0 or newer to older versions in-flight. For more details, please refer to [KIP-904](https://cwiki.apache.org/confluence/x/P5VbDg). For a downgrade, first switch the config from `"upgrade.from"` to the version you are downgrading to. This disables writing of the new serialization format in your application. It's important to wait in this state long enough to make sure that the application has finished processing any "in-flight" messages written into the repartition topics in the new serialization format. Afterwards, you can downgrade your application to a pre-3.5.x version. 

Downgrading from 3.0.x or newer version to 2.8.x or older version needs special attention: Since 3.0.0 release, Kafka Streams uses a newer RocksDB version whose on-disk format changed. This means that old versioned RocksDB would not be able to recognize the bytes written by that newer versioned RocksDB, and hence it is harder to downgrade Kafka Streams with version 3.0.0 or newer to older versions in-flight. Users need to wipe out the local RocksDB state stores written by the new versioned Kafka Streams before swapping in the older versioned Kafka Streams bytecode, which would then restore the state stores with the old on-disk format from the changelogs. 

Kafka Streams does not support running multiple instances of the same application as different processes on the same physical state directory. Starting in 2.8.0 (as well as 2.7.1 and 2.6.2), this restriction will be enforced. If you wish to run more than one instance of Kafka Streams, you must configure them with different values for `state.dir`. 

Starting in Kafka Streams 2.6.x, a new processing mode is available, named EOS version 2. This can be configured by setting `"processing.guarantee"` to `"exactly_once_v2"` for application versions 3.0+, or setting it to `"exactly_once_beta"` for versions between 2.6 and 2.8. To use this new feature, your brokers must be on version 2.5.x or newer. If you want to upgrade your EOS application from an older version and enable this feature in version 3.0+, you first need to upgrade your application to version 3.0.x, staying on `"exactly_once"`, and then do second round of rolling bounces to switch to `"exactly_once_v2"`. If you are upgrading an EOS application from an older (pre-2.6) version to a version between 2.6 and 2.8, follow these same steps but with the config `"exactly_once_beta"` instead. No special steps are required to upgrade an application using `"exactly_once_beta"` from version 2.6+ to 3.0 or higher: you can just change the config from `"exactly_once_beta"` to `"exactly_once_v2"` during the rolling upgrade. For a downgrade, do the reverse: first switch the config from `"exactly_once_v2"` to `"exactly_once"` to disable the feature in your 2.6.x application. Afterward, you can downgrade your application to a pre-2.6.x version. 

Since 2.6.0 release, Kafka Streams depends on a RocksDB version that requires MacOS 10.14 or higher.

## Streams API changes in 4.2.0

Kafka Streams now supports Dead Letter Queue (DLQ). A new config `errors.deadletterqueue.topic.name` allows to specify the name of the DLQ topic. When set and `DefaultProductionExceptionHandler` is used, records that cause exceptions will be forwarded to the DLQ topic. If a custom exception handler is used, it is up to the custom handler to build DLQ records to send, hence, depending on the implementation, the `errors.deadletterqueue.topic.name` configuration may be ignored. `org.apache.kafka.streams.errors.ProductionExceptionHandler$ProductionExceptionHandlerResponse` is deprecated and replaced by `org.apache.kafka.streams.errors.ProductionExceptionHandler$Response`. Methods `handle` and `handleSerializationException` in `org.apache.kafka.streams.errors.ProductionExceptionHandler` are deprecated and replaced by `handleError` and `handleSerializationError` respectively in order to use the new `Response` class. More details can be found in [KIP-1034](https://cwiki.apache.org/confluence/x/HwviEQ). 

We introduce a new `org.apache.kafka.streams.CloseOptions` class which replaces the existing `org.apache.kafka.streams.KafkaStreams$CloseOptions`. The latter is deprecated and will be removed in the next major release. `CloseOptions` class allows to specify close timeout and group membership operation - whether the consumer needs to leave the group or remain in the group. More details can be found in [KIP-1153](https://cwiki.apache.org/confluence/x/QAq9F). 

Kafka Streams now allows to enable state store directories created by Kafka Streams to have write access for the OS group, via the newly added config `allow.os.group.write.access`. More details can be found in [KIP-1230](https://cwiki.apache.org/confluence/x/jgl3Fw). 

`org.apache.kafka.streams.errors.BrokerNotFoundException` was deprecated and will be removed in the next major release. More details can be found in [KIP-1195](https://cwiki.apache.org/confluence/x/8AxJFg). 

Kafka Streams now provides rebalance listener metrics for "streams" groups to monitor the latency of rebalance callbacks. The following metrics are available at the thread level: `tasks-revoked-latency-avg`, `tasks-revoked-latency-max`, `tasks-assigned-latency-avg`, `tasks-assigned-latency-max`, `tasks-lost-latency-avg`, and `tasks-lost-latency-max`. Note that these metrics are only populated when the Streams Rebalance Protocol (KIP-1071) is enabled. Users migrating from the consumer rebalance listener metrics should update their monitoring dashboards and alerts to use these new streams-specific metrics. More details can be found in [KIP-1216](https://cwiki.apache.org/confluence/x/ywnxFg). 

The `application-id` tag is now available for the Kafka Streams client state metric (`client-state`). More details can be found in [KIP-1221](https://cwiki.apache.org/confluence/x/jQobFw). 

## Streams API changes in 4.1.0

**Note:** Kafka Streams 4.1.0 contains a critical memory leak bug ([KAFKA-19748](https://issues.apache.org/jira/browse/KAFKA-19748)) that affects users of range scans and certain DSL operators (session windows, sliding windows, stream-stream joins, foreign-key joins). Users running Kafka Streams should consider upgrading directly to 4.1.1, which includes the fix for it.

### Early Access of the Streams Rebalance Protocol

The Streams Rebalance Protocol is a broker-driven rebalancing system designed specifically for Kafka Streams applications. Following the pattern of KIP-848, which moved rebalance coordination of plain consumers from clients to brokers, KIP-1071 extends this model to Kafka Streams workloads. Instead of clients computing new assignments on the client during rebalance events involving all members of the group, assignments are computed continuously on the broker. Instead of using a consumer group, the streams application registers as a streams group with the broker, which manages and exposes all metadata required for coordination of the streams application instances. 

This Early Access release covers a subset of the functionality detailed in [KIP-1071](https://cwiki.apache.org/confluence/display/KAFKA/KIP-1071%3A+Streams+Rebalance+Protocol). Do not use the new protocol in production. The API is subject to change in future releases. 

**What's Included in Early Access**

  * **Core Streams Group Rebalance Protocol:** The `group.protocol=streams` configuration enables the dedicated streams rebalance protocol. This separates streams groups from consumer groups and provides a streams-specific group membership lifecycle and metadata management on the broker.
  * **Sticky Task Assignor:** A basic task assignment strategy that minimizes task movement during rebalances is included.
  * **Interactive Query Support:** IQ operations are compatible with the new streams protocol.
  * **New Admin RPC:** The `StreamsGroupDescribe` RPC provides streams-specific metadata separate from consumer group information, with corresponding access via the `Admin` client.
  * **CLI Integration:** You can list, describe, and delete streams groups via the `kafka-streams-groups.sh` script.



**What's Not Included in Early Access**

  * **Static Membership:** Setting a client `instance.id` will be rejected.
  * **Topology Updates:** If a topology is changed significantly (e.g., by adding new source topics or changing the number of sub-topologies), a new streams group must be created.
  * **High Availability Assignor:** Only the sticky assignor is supported.
  * **Regular Expressions:** Pattern-based topic subscription is not supported.
  * **Reset Operations:** CLI offset reset operations are not supported.
  * **Protocol Migration:** Group migration is not available between the classic and new streams protocols.



**Why Use the Streams Rebalance Protocol?**

  * **Broker-Driven Coordination:** Centralizes task assignment logic on brokers instead of the client. This provides consistent, authoritative task assignment decisions from a single coordination point and reduces the potential for split-brain scenarios. 
  * **Faster, More Stable Rebalances:** Reduces rebalance duration and impact by removing the global synchronization point. This minimizes application downtime during membership changes or failures. 
  * **Better Observability:** Provides dedicated metrics and admin interfaces that separate streams from consumer groups, leading to clearer troubleshooting with broker-side observability. 



Enabling the protocol requires the brokers and clients are running Apache Kafka 4.1. It should be enabled only on new clusters for testing purposes. Set `unstable.feature.versions.enable=true` for controllers and brokers, and set `unstable.api.versions.enable=true` on the brokers as well. In your Kafka Streams application configuration, set `group.protocol=streams`. 

When `unstable.api.versions.enable=true` is set when the kafka storage is first created, and no explicit metadata version is set, the feature will be enabled by default. In other configurations (e.g. if the cluster already existed, or a metadata version was hardcoded), you may have to enable it explicitly. First, check the current feature level by running `kafka-features.sh --bootstrap-server localhost:9092 describe`. If `streams.version` shows `FinalizedVersionLevel` is 1, no action is needed. Otherwise, upgrade by running `kafka-features.sh --bootstrap-server localhost:9092 upgrade --feature streams.version=1`. After the upgrade, verify the change by running `kafka-features.sh --bootstrap-server localhost:9092 describe`. 

Migration between the classic consumer group protocol and the Streams Rebalance Protocol is not supported in either direction. An application using this protocol must use a new `application.id` that has not been used by any application on the classic protocol. Furthermore, this ID must not be in use as a `group.id` by any consumer ("classic" or "consumer") nor share-group application. It is also possible to delete a previous consumer group using `kafka-consumer-groups.sh` before starting the application with the new protocol, which will however also delete all offsets for that group. 

To operate the new streams groups, explore the options of `kafka-streams-groups.sh` to list, describe, and delete streams groups. In the new protocol, `streams.session.timeout.ms`, `streams.heartbeat.interval.ms` and `streams.num.standby.replicas` are group-level configurations, which are ignored when they are set on the client side. Use the `kafka-configs.sh` tool to set these configurations, for example: `kafka-configs.sh --bootstrap-server localhost:9092 --alter --entity-type groups --entity-name wordcount --add-config streams.num.standby.replicas=1`. 

Please provide feedback on this feature via the [Kafka mailing lists](https://kafka.apache.org/contact) or by filing [JIRA issues](https://kafka.apache.org/contributing). 

### Other changes

The introduction of [KIP-1111](https://cwiki.apache.org/confluence/x/4Y_MEw) enables you to enforce explicit naming for all internal resources of the topology, including internal topics (e.g., changelog and repartition topics) and their associated state stores. This ensures that every internal resource is named before the Kafka Streams application is deployed, which is essential for upgrading your topology. You can enable this feature via `StreamsConfig` using the `StreamsConfig#ENSURE_EXPLICIT_INTERNAL_RESOURCE_NAMING_CONFIG` parameter. When set to `true`, the application will refuse to start if any internal resource has an auto-generated name. 

## Streams API changes in 4.0.0

In this release, eos-v1 (Exactly Once Semantics version 1) is no longer supported. To use eos-v2, brokers must be running version 2.5 or later. Additionally, all deprecated methods, classes, APIs, and config parameters up to and including AK 3.5 release have been removed. A few important ones are listed below. The full list can be found in [KAFKA-12822](https://issues.apache.org/jira/browse/KAFKA-12822). 

  * [Old processor APIs](https://issues.apache.org/jira/browse/KAFKA-12829)
  * [KStream#through() in both Java and Scala](https://issues.apache.org/jira/browse/KAFKA-12823)
  * ["transformer" methods and classes in both Java and Scala](https://issues.apache.org/jira/browse/KAFKA-16339)
    * migrating from `KStreams#transformValues()` to `KStreams.processValues()` might not be safe due to [KAFKA-19668](https://issues.apache.org/jira/browse/KAFKA-19668). Please refer to the [migration guide](/43/documentation/streams/developer-guide/dsl-api.html#transformers-removal-and-migration-to-processors) for more details. 
  * [kstream.KStream#branch in both Java and Scala](https://issues.apache.org/jira/browse/KAFKA-12824)
  * [builder methods for Time/Session/Join/SlidingWindows](https://issues.apache.org/jira/browse/KAFKA-16332)
  * [KafkaStreams#setUncaughtExceptionHandler()](https://issues.apache.org/jira/browse/KAFKA-12827)



In this release the `ClientInstanceIds` instance stores the global consumer`Uuid` for the [KIP-714](https://cwiki.apache.org/confluence/x/2xRRCg#KIP714:Clientmetricsandobservability-Clientidentificationandtheclientinstanceid) id with a key of global stream-thread name appended with `"-global-consumer"` where before it was only the global stream-thread name. 

In this release two configs `default.deserialization.exception.handler` and `default.production.exception.handler` are deprecated, as they don't have any overwrites, which is described in [KIP-1056](https://cwiki.apache.org/confluence/x/Y41yEg) You can refer to new configs via `deserialization.exception.handler` and `production.exception.handler`. 

In previous release, a new version of the Processor API was introduced and the old Processor API was incrementally replaced and deprecated. [KIP-1070](https://cwiki.apache.org/confluence/x/sxCTEg) follow this path by deprecating `MockProcessorContext`, `Transformer`, `TransformerSupplier`, `ValueTransformer`, and `ValueTransformerSupplier`. 

Previously, the `ProductionExceptionHandler` was not invoked on a (retriable) `TimeoutException`. With Kafka Streams 4.0, the handler is called, and the default handler would return `RETRY` to not change existing behavior. However, a custom handler can now decide to break the infinite retry loop by returning either `CONTINUE` or `FAIL` ([KIP-1065](https://cwiki.apache.org/confluence/x/LQ6TEg)). 

In this release, Kafka Streams metrics can be collected broker side via the KIP-714 broker-plugin. For more detailed information, refer to [KIP-1076](https://cwiki.apache.org/confluence/x/XA-OEg) document please. 

[KIP-1077](https://cwiki.apache.org/confluence/x/eA-OEg) deprecates the `ForeachProcessor` class. This change is aimed at improving the organization and clarity of the Kafka Streams API by ensuring that internal classes are not exposed in public packages. 

[KIP-1078](https://cwiki.apache.org/confluence/x/hg-OEg) deprecates the leaking getter methods in the `Joined` helper class. These methods are deprecated without a replacement for future removal, as they don't add any value to Kafka Streams users. 

To ensures better encapsulation and organization of configuration documentation within Kafka Streams, [KIP-1085](https://cwiki.apache.org/confluence/x/hYz9Eg) deprecate certain public doc description variables that are only used within the `StreamsConfig` or `TopologyConfig` classes. Additionally, the unused variable `DUMMY_THREAD_INDEX` will also be deprecated. 

Due to the removal of the already deprecated `#through` method in Kafka Streams, the `intermediateTopicsOption` of `StreamsResetter` tool in Apache Kafka is not needed any more and therefore is deprecated ([KIP-1087](https://cwiki.apache.org/confluence/x/Vo39Eg)). 

Since string metrics cannot be collected on the broker side (KIP-714), [KIP-1091](https://cwiki.apache.org/confluence/x/IgstEw) introduces numeric counterparts to allow proper broker-side metric collection for Kafka Streams applications. These metrics will be available at the `INFO` recording level, and a thread-level metric with a String value will be available for users leveraging Java Management Extensions (`JMX`). 

In order to reduce storage overhead and improve API usability, a new method in the Java and Scala APIs that accepts a BiFunction for foreign key extraction is introduced by [KIP-1104](https://cwiki.apache.org/confluence/x/gIuMEw). KIP-1104 allows foreign key extraction from both the key and value in KTable joins in Apache Kafka. Previously, foreign key joins in KTables only allowed extraction from the value, which led to data duplication and potential inconsistencies. This enhancement introduces a new method in the Java and Scala APIs that accepts a BiFunction for foreign key extraction, enabling more intuitive and efficient joins. The existing methods will be deprecated but not removed, ensuring backward compatibility. This change aims to reduce storage overhead and improve API usability. 

With introduction of [KIP-1106](https://cwiki.apache.org/confluence/x/NIyMEw), the existing `Topology.AutoOffsetReset` is deprecated and replaced with a new class `org.apache.kafka.streams.AutoOffsetReset` to capture the reset strategies. New methods will be added to the `org.apache.kafka.streams.Topology` and `org.apache.kafka.streams.kstream.Consumed` classes to support the new reset strategy. These changes aim to provide more flexibility and efficiency in managing offsets, especially in scenarios involving long-term storage and infinite retention. 

You can now configure your topology with a `ProcessorWrapper`, which allows you to access and optionally wrap/replace any processor in the topology by injecting an alternative `ProcessorSupplier` in its place. This can be used to peek records and access the processor context even for DSL operators, for example to implement a logging or tracing framework, or to aid in testing or debugging scenarios. You must implement the `ProcessorWrapper` interface and then pass the class or class name into the configs via the new `StreamsConfig#PROCESSOR_WRAPPER_CLASS_CONFIG` config. NOTE: this config is applied during the topology building phase, and therefore will not take effect unless the config is passed in when creating the StreamsBuilder (DSL) or Topology(PAPI) objects. You MUST use the StreamsBuilder/Topology constructor overload that accepts a TopologyConfig parameter for the `StreamsConfig#PROCESSOR_WRAPPER_CLASS_CONFIG` to be picked up. See [KIP-1112](https://cwiki.apache.org/confluence/x/TZCMEw) for more details. 

Upgraded RocksDB dependency to version 9.7.3 (from 7.9.2). This upgrade incorporates various improvements and optimizations within RocksDB. However, it also introduces some API changes. The `org.rocksdb.AccessHint` class, along with its associated methods, has been removed. Several methods related to compressed block cache configuration in the `BlockBasedTableConfig` class have been removed, including `blockCacheCompressedNumShardBits`, `blockCacheCompressedSize`, and their corresponding setters. These functionalities are now consolidated under the `cache` option, and developers should configure their compressed block cache using the `setCache` method instead. The `NO_FILE_CLOSES` field has been removed from the `org.rocksdb.TickerTypeenum` as a result the `number-open-files` metrics does not work as expected. Metric `number-open-files` returns constant -1 from now on until it will officially be removed. The `org.rocksdb.Options.setLogger()` method now accepts a `LoggerInterface` as a parameter instead of the previous `Logger`. Some data types used in RocksDB's Java API have been modified. These changes, along with the removed class, field, and new methods, are primarily relevant to users implementing custom RocksDB configurations. These changes are expected to be largely transparent to most Kafka Streams users. However, those employing advanced RocksDB customizations within their Streams applications, particularly through the `rocksdb.config.setter`, are advised to consult the detailed RocksDB 9.7.3 changelog to ensure a smooth transition and adapt their configurations as needed. Specifically, users leveraging the removed `AccessHint` class, the removed methods from the `BlockBasedTableConfig` class, the `NO_FILE_CLOSES` field from `TickerType`, or relying on the previous signature of `setLogger()` will need to update their implementations. 

## Streams API changes in 3.9.0

The introduction of [KIP-1033](https://cwiki.apache.org/confluence/x/xQniEQ) enables you to provide a processing exception handler to manage exceptions during the processing of a record rather than throwing the exception all the way out of your streams application. You can provide the configs via the `StreamsConfig` as `StreamsConfig#PROCESSING_EXCEPTION_HANDLER_CLASS_CONFIG`. The specified handler must implement the `org.apache.kafka.streams.errors.ProcessingExceptionHandler` interface. 

Kafka Streams now allows to customize the logging interval of stream-thread runtime summary, via the newly added config `log.summary.interval.ms`. By default, the summary is logged every 2 minutes. More details can be found in [KIP-1049](https://cwiki.apache.org/confluence/x/fwpeEg). 

## Streams API changes in 3.8.0

Kafka Streams now supports customizable task assignment strategies via the `task.assignor.class` configuration. The configuration can be set to the fully qualified class name of a custom task assignor implementation that has to extend the new `org.apache.kafka.streams.processor.assignment.TaskAssignor` interface. The new configuration also allows users to bring back the behavior of the old task assignor `StickyTaskAssignor` that was used before the introduction of the `HighAvailabilityTaskAssignor`. If no custom task assignor is configured, the default task assignor `HighAvailabilityTaskAssignor` is used. If you were using the `internal.task.assignor.class` config, you should switch to using the new `task.assignor.class` config instead, as the internal config will be removed in a future release. If you were previously plugging in the `StickyTaskAssignor` via the legacy `internal.task.assignor.class` config, you will need to make sure that you are importing the new `org.apache.kafka.streams.processor.assignment.StickTaskAssignor` when you switch over to the new `task.assignor.class` config, which is a version of the `StickyTaskAssignor` that implements the new public `TaskAssignor` interface. For more details, see the public interface section of [KIP-924](https://cwiki.apache.org/confluence/x/PxU0Dw). 

The Processor API now support so-called read-only state stores, added via [KIP-813](https://cwiki.apache.org/confluence/x/q53kCw). These stores don't have a dedicated changelog topic, but use their source topic for fault-tolerance, similar to `KTables` with source-topic optimization enabled. 

To improve detection of leaked state store iterators, we added new store-level metrics to track the number and age of open iterators. The new metrics are `num-open-iterators`, `iterator-duration-avg`, `iterator-duration-max` and `oldest-iterator-open-since-ms`. These metrics are available for all state stores, including RocksDB, in-memory, and custom stores. More details can be found in [KIP-989](https://cwiki.apache.org/confluence/x/9KCzDw). 

## Streams API changes in 3.7.0

We added a new method to `KafkaStreams`, namely `KafkaStreams#setStandbyUpdateListener()` in [KIP-988](https://cwiki.apache.org/confluence/x/yqCzDw), in which users can provide their customized implementation of the newly added `StandbyUpdateListener` interface to continuously monitor changes to standby tasks. 

IQv2 supports `RangeQuery` that allows to specify unbounded, bounded, or half-open key-ranges, which return data in unordered (byte[]-lexicographical) order (per partition). [KIP-985](https://cwiki.apache.org/confluence/x/eKCzDw) extends this functionality by adding `.withDescendingKeys()` and `.withAscendingKeys()`to allow user to receive data in descending or ascending order. 

[KIP-992](https://cwiki.apache.org/confluence/x/TYxEE) adds two new query types, namely `TimestampedKeyQuery` and `TimestampedRangeQuery`. Both should be used to query a timestamped key-value store, to retrieve a `ValueAndTimestamp` result. The existing `KeyQuery` and `RangeQuery` are changed to always return the value only for timestamped key-value stores. 

IQv2 adds support for `MultiVersionedKeyQuery` (introduced in [KIP-968](https://cwiki.apache.org/confluence/x/WpSzDw)) that allows retrieving a set of records from a versioned state store for a given key and a specified time range. Users have to use `fromTime(Instant)` and/or `toTime(Instant)` to specify a half or a complete time range. 

IQv2 adds support for `VersionedKeyQuery` (introduced in [KIP-960](https://cwiki.apache.org/confluence/x/qo_zDw)) that allows retrieving a single record from a versioned state store based on its key and timestamp. Users have to use the `asOf(Instant)` method to define a query that returns the record's version for the specified timestamp. To be more precise, the key query returns the record with the greatest timestamp `<= Instant`. 

The non-null key requirements for Kafka Streams join operators were relaxed as part of [KIP-962](https://cwiki.apache.org/confluence/x/f5CzDw). The behavior of the following operators changed. 

  * left join KStream-KStream: no longer drop left records with null-key and call ValueJoiner with 'null' for right value.
  * outer join KStream-KStream: no longer drop left/right records with null-key and call ValueJoiner with 'null' for right/left value.
  * left-foreign-key join KTable-KTable: no longer drop left records with null-foreign-key returned by the ForeignKeyExtractor and call ValueJoiner with 'null' for right value.
  * left join KStream-KTable: no longer drop left records with null-key and call ValueJoiner with 'null' for right value.
  * left join KStream-GlobalTable: no longer drop records when KeyValueMapper returns 'null' and call ValueJoiner with 'null' for right value.

Stream-DSL users who want to keep the current behavior can prepend a .filter() operator to the aforementioned operators and filter accordingly. The following snippets illustrate how to keep the old behavior. 
    
    
        
                //left join KStream-KStream
                leftStream
                .filter((key, value) -> key != null)
                .leftJoin(rightStream, (leftValue, rightValue) -> join(leftValue, rightValue), windows);
    
                //outer join KStream-KStream
                rightStream
                .filter((key, value) -> key != null);
                leftStream
                .filter((key, value) -> key != null)
                .outerJoin(rightStream, (leftValue, rightValue) -> join(leftValue, rightValue), windows);
    
                //left-foreign-key join KTable-KTable
                Function&ltString;, String> foreignKeyExtractor = leftValue -> ...
                leftTable
                .filter((key, value) -> foreignKeyExtractor.apply(value) != null)
                .leftJoin(rightTable, foreignKeyExtractor, (leftValue, rightValue) -> join(leftValue, rightValue), Named.as("left-foreign-key-table-join"));
    
                //left join KStream-KTable
                leftStream
                .filter((key, value) -> key != null)
                .leftJoin(kTable, (k, leftValue, rightValue) -> join(leftValue, rightValue));
    
                //left join KStream-GlobalTable
                KeyValueMapper&ltString;, String, String> keyValueMapper = (key, value) -> ...;
                leftStream
                .filter((key, value) -> keyValueMapper.apply(key,value) != null)
                .leftJoin(globalTable, keyValueMapper, (leftValue, rightValue) -> join(leftValue, rightValue));
        
        

The `default.dsl.store` config was deprecated in favor of the new `dsl.store.suppliers.class` config to allow for custom state store implementations to be configured as the default. If you currently specify `default.dsl.store=ROCKS_DB` or `default.dsl.store=IN_MEMORY` replace those configurations with `dsl.store.suppliers.class=BuiltInDslStoreSuppliers.RocksDBDslStoreSuppliers.class` and `dsl.stores.suppliers.class=BuiltInDslStoreSuppliers.InMemoryDslStoreSuppliers.class` respectively 

A new configuration option `balance_subtopology` for `rack.aware.assignment.strategy` was introduced in 3.7 release. For more information, including how it can be enabled and further configured, see the [**Kafka Streams Developer Guide**](/43/documentation/streams/developer-guide/config-streams.html#rack-aware-assignment-strategy). 

## Streams API changes in 3.6.0

Rack aware task assignment was introduced in [KIP-925](https://cwiki.apache.org/confluence/x/CQ40Dw). Rack aware task assignment can be enabled for `StickyTaskAssignor` or `HighAvailabilityTaskAssignor` to compute task assignments which can minimize cross rack traffic under certain conditions. For more information, including how it can be enabled and further configured, see the [**Kafka Streams Developer Guide**](/43/documentation/streams/developer-guide/config-streams.html#rack-aware-assignment-strategy). 

IQv2 supports a `RangeQuery` that allows to specify unbounded, bounded, or half-open key-ranges. Users have to use `withUpperBound(K)`, `withLowerBound(K)`, or `withNoBounds()` to specify half-open or unbounded ranges, but cannot use `withRange(K lower, K upper)` for the same. [KIP-941](https://cwiki.apache.org/confluence/x/_Rk0Dw) closes this gap by allowing to pass in `null` as upper and lower bound (with semantics "no bound") to simplify the usage of the `RangeQuery` class. 

KStreams-to-KTable joins now have an option for adding a grace period. The grace period is enabled on the `Joined` object using with `withGracePeriod()` method. This change was introduced in [KIP-923](https://cwiki.apache.org/confluence/x/lAs0Dw). To use the grace period option in the Stream-Table join the table must be [versioned](/43/documentation/streams/developer-guide/dsl-api.html#versioned-state-stores). For more information, including how it can be enabled and further configured, see the [**Kafka Streams Developer Guide**](/43/documentation/streams/developer-guide/config-streams.html#rack-aware-assignment-strategy). 

## Streams API changes in 3.5.0

A new state store type, versioned key-value stores, was introduced in [KIP-889](https://cwiki.apache.org/confluence/x/AIwODg) and [KIP-914](https://cwiki.apache.org/confluence/x/QorFDg). Rather than storing a single record version (value and timestamp) per key, versioned state stores may store multiple record versions per key. This allows versioned state stores to support timestamped retrieval operations to return the latest record (per key) as of a specified timestamp. For more information, including how to upgrade from a non-versioned key-value store to a versioned store in an existing application, see the [Developer Guide](/43/documentation/streams/developer-guide/dsl-api.html#versioned-state-stores). Versioned key-value stores are opt-in only; existing applications will not be affected upon upgrading to 3.5 without explicit code changes. 

In addition to KIP-899, [KIP-914](https://cwiki.apache.org/confluence/x/QorFDg) updates DSL processing semantics if a user opts-in to use the new versioned key-value stores. Using the new versioned key-value stores, DSL processing are able to handle out-of-order data better: For example, late record may be dropped and stream-table joins do a timestamped based lookup into the table. Table aggregations and primary/foreign-key table-table joins are also improved. Note: versioned key-value stores are not supported for global-KTable and don't work with `suppress()`. 

[KIP-904](https://cwiki.apache.org/confluence/x/P5VbDg) improves the implementation of KTable aggregations. In general, an input KTable update triggers a result refinent for two rows; however, prior to KIP-904, if both refinements happen to the same result row, two independent updates to the same row are applied, resulting in spurious itermediate results. KIP-904 allows us to detect this case, and to only apply a single update avoiding spurious intermediate results. 

Error handling is improved via [KIP-399](https://cwiki.apache.org/confluence/x/R4nQBQ). The existing `ProductionExceptionHandler` now also covers serialization errors. 

We added a new Serde type `Boolean` in [KIP-907](https://cwiki.apache.org/confluence/x/pZpbDg)

[KIP-884](https://cwiki.apache.org/confluence/x/AZfGDQ) adds a new config `default.client.supplier` that allows to use a custom `KafkaClientSupplier` without any code changes. 

## Streams API changes in 3.4.0

[KIP-770](https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=186878390) deprecates config `cache.max.bytes.buffering` in favor of the newly introduced config `statestore.cache.max.bytes`. To improve monitoring, two new metrics `input-buffer-bytes-total` and `cache-size-bytes-total` were added at the DEBUG level. Note, that the KIP is only partially implemented in the 3.4.0 release, and config `input.buffer.max.bytes` is not available yet. 

[KIP-873](https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=211883356) enables you to multicast result records to multiple partition of downstream sink topics and adds functionality for choosing to drop result records without sending. The `Integer StreamPartitioner.partition()` method is deprecated and replaced by the newly added `Optiona≶Set<Integer>>StreamPartitioner.partitions()` method, which enables returning a set of partitions to send the record to. 

[KIP-862](https://cwiki.apache.org/confluence/x/WSf1D) adds a DSL optimization for stream-stream self-joins. The optimization is enabled via a new option `single.store.self.join` which can be set via existing config `topology.optimization`. If enabled, the DSL will use a different join processor implementation that uses a single RocksDB store instead of two, to avoid unnecessary data duplication for the self-join case. 

[KIP-865](https://cwiki.apache.org/confluence/x/UY9rDQ) updates the Kafka Streams application reset tool’s server parameter name to conform to the other Kafka tooling by deprecating the `--bootstrap-servers` parameter and introducing a new `--bootstrap-server` parameter in its place. 

## Streams API changes in 3.3.0

Kafka Streams does not send a "leave group" request when an instance is closed. This behavior implies that a rebalance is delayed until `max.poll.interval.ms` passed. [KIP-812](https://cwiki.apache.org/confluence/x/KZvkCw) introduces `KafkaStreams.close(CloseOptions)` overload, which allows forcing an instance to leave the group immediately. Note: Due to internal limitations, `CloseOptions` only works for static consumer groups at this point (cf. [KAFKA-16514](https://issues.apache.org/jira/browse/KAFKA-16514) for more details and a fix in some future release). 

[KIP-820](https://cwiki.apache.org/confluence/x/yKbkCw) adapts the PAPI type-safety improvement of KIP-478 into the DSL. The existing methods `KStream.transform`, `KStream.flatTransform`, `KStream.transformValues`, and `KStream.flatTransformValues` as well as all overloads of `void KStream.process` are deprecated in favor of the newly added methods 

  * `KStream<KOut,VOut> KStream.process(ProcessorSupplier, ...)`
  * `KStream<K,VOut> KStream.processValues(FixedKeyProcessorSupplier, ...)`

Both new methods have multiple overloads and return a `KStream` instead of `void` as the deprecated `process()` methods did. In addition, `FixedKeyProcessor`, `FixedKeyRecord`, `FixedKeyProcessorContext`, and `ContextualFixedKeyProcessor` are introduced to guard against disallowed key modification inside `processValues()`. Furthermore, `ProcessingContext` is added for a better interface hierarchy. **CAUTION:** The newly added `KStream.processValues()` method introduced a regression bug ([KAFKA-19668](https://issues.apache.org/jira/browse/KAFKA-19668)). If you have "merge repartition topics" optimization enabled, it is not safe to migrate from `transformValues()` to `processValues()` in 3.3.0 release. The bug is only fixed with Kafka Streams 4.0.1, 4.1.1, and 4.2.0. For more details, please refer to the [migration guide](/43/documentation/streams/developer-guide/dsl-api.html#transformers-removal-and-migration-to-processors). 

Emitting a windowed aggregation result only after a window is closed is currently supported via the `suppress()` operator. However, `suppress()` uses an in-memory implementation and does not support RocksDB. To close this gap, [KIP-825](https://cwiki.apache.org/confluence/x/n7fkCw) introduces "emit strategies", which are built into the aggregation operator directly to use the already existing RocksDB store. `TimeWindowedKStream.emitStrategy(EmitStrategy)` and `SessionWindowedKStream.emitStrategy(EmitStrategy)` allow picking between "emit on window update" (default) and "emit on window close" strategies. Additionally, a few new emit metrics are added, as well as a necessary new method, `SessionStore.findSessions(long, long)`. 

[KIP-834](https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=211882832) allows pausing and resuming a Kafka Streams instance. Pausing implies that processing input records and executing punctuations will be skipped; Kafka Streams will continue to poll to maintain its group membership and may commit offsets. In addition to the new methods `KafkaStreams.pause()` and `KafkaStreams.resume()`, it is also supported to check if an instance is paused via the `KafkaStreams.isPaused()` method. 

To improve monitoring of Kafka Streams applications, [KIP-846](https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=211886093) adds four new metrics `bytes-consumed-total`, `records-consumed-total`, `bytes-produced-total`, and `records-produced-total` within a new **topic level** scope. The metrics are collected at INFO level for source and sink nodes, respectively. 

## Streams API changes in 3.2.0

RocksDB offers many metrics which are critical to monitor and tune its performance. Kafka Streams started to make RocksDB metrics accessible like any other Kafka metric via [KIP-471](https://cwiki.apache.org/confluence/x/A5LiBg) in 2.4.0 release. However, the KIP was only partially implemented, and is now completed with the 3.2.0 release. For a full list of available RocksDB metrics, please consult the [monitoring documentation](/43/documentation/#kafka_streams_client_monitoring). 

Kafka Streams ships with RocksDB and in-memory store implementations and users can pick which one to use. However, for the DSL, the choice is a per-operator one, making it cumbersome to switch from the default RocksDB store to in-memory store for all operators, especially for larger topologies. [KIP-591](https://cwiki.apache.org/confluence/x/eCvcC) adds a new config `default.dsl.store` that enables setting the default store for all DSL operators globally. Note that it is required to pass `TopologyConfig` to the `StreamsBuilder` constructor to make use of this new config. 

For multi-AZ deployments, it is desired to assign StandbyTasks to a KafkaStreams instance running in a different AZ than the corresponding active StreamTask. [KIP-708](https://cwiki.apache.org/confluence/x/UQ5RCg) enables configuring Kafka Streams instances with a rack-aware StandbyTask assignment strategy, by using the new added configs `rack.aware.assignment.tags` and corresponding `client.tag.<myTag>`. 

[KIP-791](https://cwiki.apache.org/confluence/x/I5BnCw) adds a new method `Optional<RecordMetadata> StateStoreContext.recordMetadata()` to expose record metadata. This helps for example to provide read-your-writes consistency guarantees in interactive queries. 

[Interactive Queries](/documentation/streams/developer-guide/interactive-queries.html) allow users to tap into the operational state of Kafka Streams processor nodes. The existing API is tightly coupled with the actual state store interfaces and thus the internal implementation of state store. To break up this tight coupling and allow for building more advanced IQ features, [KIP-796](https://cwiki.apache.org/confluence/x/34xnCw) introduces a completely new IQv2 API, via `StateQueryRequest` and `StateQueryResult` classes, as well as `Query` and `QueryResult` interfaces (plus additional helper classes). In addition, multiple built-in query types were added: `KeyQuery` for key lookups and `RangeQuery` (via [KIP-805](https://cwiki.apache.org/confluence/x/85OqCw)) for key-range queries on key-value stores, as well as `WindowKeyQuery` and `WindowRangeQuery` (via [KIP-806](https://cwiki.apache.org/confluence/x/LJaqCw)) for key and range lookup into windowed stores. 

The Kafka Streams DSL may insert so-called repartition topics for certain DSL operators to ensure correct partitioning of data. These topics are configured with infinite retention time, and Kafka Streams purges old data explicitly via "delete record" requests, when commiting input topic offsets. [KIP-811](https://cwiki.apache.org/confluence/x/JY-kCw) adds a new config `repartition.purge.interval.ms` allowing you to configure the purge interval independently of the commit interval. 

## Streams API changes in 3.1.0

The semantics of left/outer stream-stream join got improved via [KIP-633](https://cwiki.apache.org/confluence/x/Ho2NCg). Previously, left-/outer stream-stream join might have emitted so-call spurious left/outer results, due to an eager-emit strategy. The implementation was changed to emit left/outer join result records only after the join window is closed. The old API to specify the join window, i.e., `JoinWindows.of()` that enables the eager-emit strategy, was deprecated in favor of a `JoinWindows.ofTimeDifferenceAndGrace()` and `JoinWindows.ofTimeDifferencWithNoGrace()`. The new semantics are only enabled if you use the new join window builders.  
Additionally, KIP-633 makes setting a grace period also mandatory for windowed aggregations, i.e., for `TimeWindows` (hopping/tumbling), `SessionWindows`, and `SlidingWindows`. The corresponding builder methods `.of(...)` were deprecated in favor of the new `.ofTimeDifferenceAndGrace()` and `.ofTimeDifferencWithNoGrace()` methods. 

[KIP-761](https://cwiki.apache.org/confluence/x/vAUBCw) adds new metrics that allow to track blocking times on the underlying consumer and producer clients. Check out the section on [Kafka Streams metrics](/documentation/#kafka_streams_monitoring) for more details. 

[Interactive Queries](/documentation/streams/developer-guide/interactive-queries.html) were improved via [KIP-763](https://cwiki.apache.org/confluence/x/jAoBCw) [KIP-766](https://cwiki.apache.org/confluence/x/tIIjCw). Range queries now accept `null` as lower/upper key-range bound to indicate an open-ended lower/upper bound. 

Foreign-key table-table joins now support custom partitioners via [KIP-775](https://cwiki.apache.org/confluence/x/-QhACw). Previously, if an input table was partitioned by a non-default partitioner, joining records might fail. With KIP-775 you now can pass a custom `StreamPartitioner` into the join using the newly added `TableJoined` object. 

## Streams API changes in 3.0.0

We improved the semantics of [task idling (`max.task.idle.ms`)](/documentation/streams/developer-guide/config-streams.html#max-task-idle-ms). Now Streams provides stronger in-order join and merge processing semantics. Streams's new default pauses processing on tasks with multiple input partitions when one of the partitions has no data buffered locally but has a non-zero lag. In other words, Streams will wait to fetch records that are already available on the broker. This results in improved join semantics, since it allows Streams to interleave the two input partitions in timestamp order instead of just processing whichever partition happens to be buffered. There is an option to disable this new behavior, and there is also an option to make Streams wait even longer for new records to be _produced_ to the input partitions, which you can use to get stronger time semantics when you know some of your producers may be slow. See the [config reference](/documentation/streams/developer-guide/config-streams.html#max-task-idle-ms) for more information, and [KIP-695](https://cwiki.apache.org/confluence/x/JSXZCQ) for the larger context of this change. 

Interactive Queries may throw new exceptions for different errors: 

  * `UnknownStateStoreException`: If the specified store name does not exist in the topology, an `UnknownStateStoreException` will be thrown instead of the former `InvalidStateStoreException`.
  * `StreamsNotStartedException`: If Streams state is `CREATED`, a `StreamsNotStartedException` will be thrown.
  * `InvalidStateStorePartitionException`: If the specified partition does not exist, a `InvalidStateStorePartitionException` will be thrown.



See [KIP-216](https://cwiki.apache.org/confluence/x/0JpzB) for more information. 

We deprecated the StreamsConfig `processing.guarantee` configuration value `"exactly_once"` (for EOS version 1) in favor of the improved EOS version 2, formerly configured via `"exactly_once_beta`. To avoid confusion about the term "beta" in the config name and highlight the production-readiness of EOS version 2, we have also renamed "eos-beta" to "eos-v2" and deprecated the configuration value `"exactly_once_beta"`, replacing it with a new configuration value `"exactly_once_v2"` Users of exactly-once semantics should plan to migrate to the eos-v2 config and prepare for the removal of the deprecated configs in 4.0 or after at least a year from the release of 3.0, whichever comes last. Note that eos-v2 requires broker version 2.5 or higher, like eos-beta, so users should begin to upgrade their kafka cluster if necessary. See [KIP-732](https://cwiki.apache.org/confluence/x/zJONCg) for more details. 

We removed the default implementation of `RocksDBConfigSetter#close()`. 

We dropped the default 24 hours grace period for windowed operations such as Window or Session aggregates, or stream-stream joins. This period determines how long after a window ends any out-of-order records will still be processed. Records coming in after the grace period has elapsed are considered late and will be dropped. But in operators such as suppression, a large grace period has the drawback of incurring an equally large output latency. The current API made it all too easy to miss the grace period config completely, leading you to wonder why your application seems to produce no output -- it actually is, but not for 24 hours. 

To prevent accidentally or unknowingly falling back to the default 24hr grace period, we deprecated all of the existing static constructors for the `Windows` classes (such as `TimeWindows#of`). These are replaced by new static constructors of two flavors: `#ofSizeAndGrace` and `#ofSizeWithNoGrace` (these are for the `TimeWindows` class; analogous APIs exist for the `JoinWindows`, `SessionWindows`, and SlidingWindows classes). With these new APIs you are forced to set the grace period explicitly, or else consciously choose to opt out by selecting the `WithNoGrace` flavor which sets it to 0 for situations where you really don't care about the grace period, for example during testing or when playing around with Kafka Streams for the first time. Note that using the new APIs for the `JoinWindows` class will also enable a fix for spurious left/outer join results, as described in the following paragraph. For more details on the grace period and new static constructors, see [KIP-633](https://cwiki.apache.org/confluence/x/Ho2NCg)

Additionally, in older versions Kafka Streams emitted stream-stream left/outer join results eagerly. This behavior may lead to spurious left/outer join result records. In this release, we changed the behavior to avoid spurious results and left/outer join result are only emitted after the join window is closed, i.e., after the grace period elapsed. To maintain backward compatibility, the old API `JoinWindows#of(timeDifference)` preserves the old eager-emit behavior and only the new APIs `JoinWindows#ofTimeDifferenceAndGrace()` and `JoinsWindows#ofTimeDifferenceNoGrace` enable the new behavior. Check out [KAFKA-10847](https://issues.apache.org/jira/browse/KAFKA-10847) for more information. 

The public `topicGroupId` and `partition` fields on TaskId have been deprecated and replaced with getters. Please migrate to using the new `TaskId.subtopology()` (which replaces `topicGroupId`) and `TaskId.partition()` APIs instead. Also, the `TaskId#readFrom` and `TaskId#writeTo` methods have been deprecated and will be removed, as they were never intended for public use. We have also deprecated the `org.apache.kafka.streams.processor.TaskMetadata` class and introduced a new interface `org.apache.kafka.streams.TaskMetadata` to be used instead. This change was introduced to better reflect the fact that `TaskMetadata` was not meant to be instantiated outside of Kafka codebase. Please note that the new `TaskMetadata` offers APIs that better represent the task id as an actual `TaskId` object instead of a String. Please migrate to the new `org.apache.kafka.streams.TaskMetadata` which offers these better methods, for example, by using the new `ThreadMetadata#activeTasks` and `ThreadMetadata#standbyTasks`. `org.apache.kafka.streams.processor.ThreadMetadata` class is also now deprecated and the newly introduced interface `org.apache.kafka.streams.ThreadMetadata` is to be used instead. In this new `ThreadMetadata` interface, any reference to the deprecated `TaskMetadata` is replaced by the new interface. Finally, also `org.apache.kafka.streams.state.StreamsMetadata` has been deprecated. Please migrate to the new `org.apache.kafka.streams.StreamsMetadata`. We have deprecated several methods under `org.apache.kafka.streams.KafkaStreams` that returned the aforementioned deprecated classes: 

  * Users of `KafkaStreams#allMetadata` are meant to migrate to the new `KafkaStreams#metadataForAllStreamsClients`.
  * Users of `KafkaStreams#allMetadataForStore(String)` are meant to migrate to the new `KafkaStreams#streamsMetadataForStore(String)`.
  * Users of `KafkaStreams#localThreadsMetadata` are meant to migrate to the new `KafkaStreams#metadataForLocalThreads`.



See [KIP-740](https://cwiki.apache.org/confluence/x/vYTOCg) and [KIP-744](https://cwiki.apache.org/confluence/x/XIrOCg) for more details.

We removed the following deprecated APIs: 

  * `--zookeeper` flag of the application reset tool: deprecated in Kafka 1.0.0 ([KIP-198](https://cwiki.apache.org/confluence/x/6J1jB)).
  * `--execute` flag of the application reset tool: deprecated in Kafka 1.1.0 ([KIP-171](https://cwiki.apache.org/confluence/x/ApI7B)).
  * `StreamsBuilder#addGlobalStore` (one overload): deprecated in Kafka 1.1.0 ([KIP-233](https://cwiki.apache.org/confluence/x/vKpzB)).
  * `ProcessorContext#forward` (some overloads): deprecated in Kafka 2.0.0 ([KIP-251](https://cwiki.apache.org/confluence/x/Ih6HB)).
  * `WindowBytesStoreSupplier#segments`: deprecated in Kafka 2.1.0 ([KIP-319](https://cwiki.apache.org/confluence/x/mQU0BQ)).
  * `segments, until, maintainMs` on `TimeWindows`, `JoinWindows`, and `SessionWindows`: deprecated in Kafka 2.1.0 ([KIP-328](https://cwiki.apache.org/confluence/x/sQU0BQ)).
  * Overloaded `JoinWindows#of, before, after`, `SessionWindows#with`, `TimeWindows#of, advanceBy`, `UnlimitedWindows#startOn` and `KafkaStreams#close` with `long` typed parameters: deprecated in Kafka 2.1.0 ([KIP-358](https://cwiki.apache.org/confluence/x/IBNPBQ)).
  * Overloaded `KStream#groupBy, groupByKey` and `KTable#groupBy` with `Serialized` parameter: deprecated in Kafka 2.1.0 ([KIP-372](https://cwiki.apache.org/confluence/x/mgJ1BQ)).
  * `Joined#named, name`: deprecated in Kafka 2.3.0 ([KIP-307](https://cwiki.apache.org/confluence/x/xikYBQ)).
  * `TopologyTestDriver#pipeInput, readOutput`, `OutputVerifier` and `ConsumerRecordFactory` classes ([KIP-470](https://cwiki.apache.org/confluence/x/tI-iBg)).
  * `KafkaClientSupplier#getAdminClient`: deprecated in Kafka 2.4.0 ([KIP-476](https://cwiki.apache.org/confluence/x/V9XiBg)).
  * Overloaded `KStream#join, leftJoin, outerJoin` with `KStream` and `Joined` parameters: deprecated in Kafka 2.4.0 ([KIP-479](https://cwiki.apache.org/confluence/x/EBEgBw)).
  * `WindowStore#put(K key, V value)`: deprecated in Kafka 2.4.0 ([KIP-474](https://cwiki.apache.org/confluence/x/kcviBg)).
  * `UsePreviousTimeOnInvalidTimestamp`: deprecated in Kafka 2.5.0 as renamed to `UsePartitionTimeOnInvalidTimestamp` ([KIP-530](https://cwiki.apache.org/confluence/x/BxXABw)).
  * Overloaded `KafkaStreams#metadataForKey`: deprecated in Kafka 2.5.0 ([KIP-535](https://cwiki.apache.org/confluence/x/Xg-jBw)).
  * Overloaded `KafkaStreams#store`: deprecated in Kafka 2.5.0 ([KIP-562](https://cwiki.apache.org/confluence/x/QYyvC)).



The following dependencies were removed from Kafka Streams: 

  * Connect-json: As of Kafka Streams no longer has a compile time dependency on "connect:json" module ([KAFKA-5146](https://issues.apache.org/jira/browse/KAFKA-5146)). Projects that were relying on this transitive dependency will have to explicitly declare it.



The default value for configuration parameter `replication.factor` was changed to `-1` (meaning: use broker default replication factor). The `replication.factor` value of `-1` requires broker version 2.4 or newer. 

The new serde type was introduced `ListSerde`: 

  * Added class `ListSerde` to (de)serialize `List`-based objects 
  * Introduced `ListSerializer` and `ListDeserializer` to power the new functionality 



## Streams API changes in 2.8.0

We extended `StreamJoined` to include the options `withLoggingEnabled()` and `withLoggingDisabled()` in [KIP-689](https://cwiki.apache.org/confluence/x/DyrZCQ). 

We added two new methods to `KafkaStreams`, namely `KafkaStreams#addStreamThread()` and `KafkaStreams#removeStreamThread()` in [KIP-663](https://cwiki.apache.org/confluence/x/FDd4CQ). These methods have enabled adding and removing StreamThreads to a running KafkaStreams client. 

We deprecated `KafkaStreams#setUncaughtExceptionHandler(final Thread.UncaughtExceptionHandler uncaughtExceptionHandler)` in favor of `KafkaStreams#setUncaughtExceptionHandler(final StreamsUncaughtExceptionHandler streamsUncaughtExceptionHandler)` in [KIP-671](https://cwiki.apache.org/confluence/x/lkN4CQ). The default handler will close the Kafka Streams client and the client will transit to state ERROR. If you implement a custom handler, the new interface allows you to return a `StreamThreadExceptionResponse`, which will determine how the application will respond to a stream thread failure. 

Changes in [KIP-663](https://cwiki.apache.org/confluence/x/FDd4CQ) necessitated the KafkaStreams client state machine to update, which was done in [KIP-696](https://cwiki.apache.org/confluence/x/lCvZCQ). The ERROR state is now terminal with PENDING_ERROR being a transitional state where the resources are closing. The ERROR state indicates that there is something wrong and the Kafka Streams client should not be blindly restarted without classifying the error that caused the thread to fail. If the error is of a type that you would like to retry, you should have the `StreamsUncaughtExceptionHandler` return `REPLACE_THREAD`. When all stream threads are dead there is no automatic transition to ERROR as a new stream thread can be added. 

The `TimeWindowedDeserializer` constructor `TimeWindowedDeserializer(final Deserializer inner)` was deprecated to encourage users to properly set their window size through `TimeWindowedDeserializer(final Deserializer inner, Long windowSize)`. An additional streams config, `window.size.ms`, was added for users that cannot set the window size through the constructor, such as when using the console consumer. [KIP-659](https://cwiki.apache.org/confluence/x/aDR4CQ) has more details. 

To simplify testing, two new constructors that don't require a `Properties` parameter have been added to the `TopologyTestDriver` class. If `Properties` are passed into the constructor, it is no longer required to set mandatory configuration parameters (cf. [KIP-680](https://cwiki.apache.org/confluence/x/MB3ZCQ)). 

We added the `prefixScan()` method to interface `ReadOnlyKeyValueStore`. The new `prefixScan()` allows fetching all values whose keys start with a given prefix. See [KIP-614](https://cwiki.apache.org/confluence/x/qhkRCQ) for more details. 

Kafka Streams is now handling `TimeoutException` thrown by the consumer, producer, and admin client. If a timeout occurs on a task, Kafka Streams moves to the next task and retries to make progress on the failed task in the next iteration. To bound how long Kafka Streams retries a task, you can set `task.timeout.ms` (default is 5 minutes). If a task does not make progress within the specified task timeout, which is tracked on a per-task basis, Kafka Streams throws a `TimeoutException` (cf. [KIP-572](https://cwiki.apache.org/confluence/x/5ArcC)). 

We changed the default value of `default.key.serde` and `default.value.serde` to be `null` instead of `ByteArraySerde`. Users will now see a `ConfigException` if their serdes are not correctly configured through those configs or passed in explicitly. See [KIP-741](https://cwiki.apache.org/confluence/x/bIbOCg) for more details. 

## Streams API changes in 2.7.0

In `KeyQueryMetadata` we deprecated `getActiveHost()`, `getStandbyHosts()` as well as `getPartition()` and replaced them with `activeHost()`, `standbyHosts()` and `partition()` respectively. `KeyQueryMetadata` was introduced in Kafka Streams 2.5 release with getter methods having prefix `get`. The intend of this change is to bring the method names to Kafka custom to not use the `get` prefix for getter methods. The old methods are deprecated and is not effected. (Cf. [KIP-648](https://cwiki.apache.org/confluence/x/vyd4CQ).) 

The `StreamsConfig` variable for configuration parameter `"topology.optimization"` is renamed from `TOPOLOGY_OPTIMIZATION` to `TOPOLOGY_OPTIMIZATION_CONFIG`. The old variable is deprecated. Note, that the parameter name itself is not affected. (Cf. [KIP-626](https://cwiki.apache.org/confluence/x/gBB4CQ).) 

The configuration parameter `retries` is deprecated in favor of the new parameter `task.timeout.ms`. Kafka Streams' runtime ignores `retries` if set, however, it would still forward the parameter to its internal clients. 

We added `SlidingWindows` as an option for `windowedBy()` windowed aggregations as described in [KIP-450](https://cwiki.apache.org/confluence/x/nAqZBg). Sliding windows are fixed-time and data-aligned windows that allow for flexible and efficient windowed aggregations. 

The end-to-end latency metrics introduced in 2.6 have been expanded to include store-level metrics. The new store-level metrics are recorded at the TRACE level, a new metrics recording level. Enabling TRACE level metrics will automatically turn on all higher levels, ie INFO and DEBUG. See [KIP-613](https://cwiki.apache.org/confluence/x/gBkRCQ) for more information. 

## Streams API changes in 2.6.0

We added a new processing mode, EOS version 2, that improves application scalability using exactly-once guarantees (via [KIP-447](https://cwiki.apache.org/confluence/x/vhYlBg)). You can enable this new feature by setting the configuration parameter `processing.guarantee` to the new value `"exactly_once_beta"`. Note that you need brokers with version 2.5 or newer to use this feature. 

For more highly available stateful applications, we've modified the task assignment algorithm to delay the movement of stateful active tasks to instances that aren't yet caught up with that task's state. Instead, to migrate a task from one instance to another (eg when scaling out), Streams will assign a warmup replica to the target instance so it can begin restoring the state while the active task stays available on an instance that already had the task. The instances warming up tasks will communicate their progress to the group so that, once ready, Streams can move active tasks to their new owners in the background. Check out [KIP-441](https://cwiki.apache.org/confluence/x/0i4lBg) for full details, including several new configs for control over this new feature. 

New end-to-end latency metrics have been added. These task-level metrics will be logged at the INFO level and report the min and max end-to-end latency of a record at the beginning/source node(s) and end/terminal node(s) of a task. See [KIP-613](https://cwiki.apache.org/confluence/x/gBkRCQ) for more information. 

As of 2.6.0 Kafka Streams deprecates `KStream.through()` in favor of the new `KStream.repartition()` operator (as per [KIP-221](https://cwiki.apache.org/confluence/x/i55zB)). `KStream.repartition()` is similar to `KStream.through()`, however Kafka Streams will manage the topic for you. If you need to write into and read back from a topic that you manage, you can fall back to use `KStream.to()` in combination with `StreamsBuilder#stream()`. Please refer to the [developer guide](/43/documentation/streams/developer-guide/dsl-api.html) for more details about `KStream.repartition()`. 

The usability of `StateStore`s within the Processor API is improved: `ProcessorSupplier` and `TransformerSupplier` now extend `ConnectedStoreProvider` as per [KIP-401](https://cwiki.apache.org/confluence/x/XI3QBQ), enabling a user to provide `StateStore`s with alongside Processor/Transformer logic so that they are automatically added and connected to the processor. 

We added a `--force` option in StreamsResetter to force remove left-over members on broker side when long session time out was configured as per [KIP-571](https://cwiki.apache.org/confluence/x/8I7JC). 

We added `Suppressed.withLoggingDisabled()` and `Suppressed.withLoggingEnabled(config)` methods to allow disabling or configuring of the changelog topic and allows for configuration of the changelog topic as per [KIP-446](https://cwiki.apache.org/confluence/x/RBiGBg). 

## Streams API changes in 2.5.0

We add a new `cogroup()` operator (via [KIP-150](https://cwiki.apache.org/confluence/x/YxcjB)) that allows to aggregate multiple streams in a single operation. Cogrouped streams can also be windowed before they are aggregated. Please refer to the [developer guide](/43/documentation/streams/developer-guide/dsl-api.html) for more details. 

We added a new `KStream.toTable()` API to translate an input event stream into a changelog stream as per [KIP-523](https://cwiki.apache.org/confluence/x/IBKrBw). 

We added a new Serde type `Void` in [KIP-527](https://cwiki.apache.org/confluence/x/3QvABw) to represent null keys or null values from input topic. 

Deprecated `UsePreviousTimeOnInvalidTimestamp` and replaced it with `UsePartitionTimeOnInvalidTimeStamp` as per [KIP-530](https://cwiki.apache.org/confluence/x/BxXABw). 

Deprecated `KafkaStreams.store(String, QueryableStoreType)` and replaced it with `KafkaStreams.store(StoreQueryParameters)` to allow querying for a store with variety of parameters, including querying a specific task and stale stores, as per [KIP-562](https://cwiki.apache.org/confluence/x/QYyvC) and [KIP-535](https://cwiki.apache.org/confluence/x/Xg-jBw) respectively. 

## Streams API changes in 2.4.0

As of 2.4.0 Kafka Streams offers a KTable-KTable foreign-key join (as per [KIP-213](https://cwiki.apache.org/confluence/x/pJlzB)). This joiner allows for records to be joined between two KTables with different keys. Both [INNER and LEFT foreign-key joins](/43/documentation/streams/developer-guide/dsl-api.html#ktable-ktable-fk-join) are supported. 

In the 2.4 release, you now can name all operators in a Kafka Streams DSL topology via [KIP-307](https://cwiki.apache.org/confluence/x/xikYBQ). Giving your operators meaningful names makes it easier to understand the topology description (`Topology#describe()#toString()`) and understand the full context of what your Kafka Streams application is doing.   
There are new overloads on most `KStream` and `KTable` methods that accept a `Named` object. Typically you'll provide a name for the DSL operation by using `Named.as("my operator name")`. Naming of repartition topics for aggregation operations will still use `Grouped` and join operations will use either `Joined` or the new `StreamJoined` object. 

Before the 2.4.0 version of Kafka Streams, users of the DSL could not name the state stores involved in a stream-stream join. If users changed their topology and added a operator before the join, the internal names of the state stores would shift, requiring an application reset when redeploying. In the 2.4.0 release, Kafka Streams adds the `StreamJoined` class, which gives users the ability to name the join processor, repartition topic(s) (if a repartition is required), and the state stores involved in the join. Also, by naming the state stores, the changelog topics backing the state stores are named as well. It's important to note that naming the stores **will not** make them queryable via Interactive Queries.   
Another feature delivered by `StreamJoined` is that you can now configure the type of state store used in the join. You can elect to use in-memory stores or custom state stores for a stream-stream join. Note that the provided stores will not be available for querying via Interactive Queries. With the addition of `StreamJoined`, stream-stream join operations using `Joined` have been deprecated. Please switch over to stream-stream join methods using the new overloaded methods. You can get more details from [KIP-479](https://cwiki.apache.org/confluence/x/EBEgBw). 

With the introduction of incremental cooperative rebalancing, Streams no longer requires all tasks be revoked at the beginning of a rebalance. Instead, at the completion of the rebalance only those tasks which are to be migrated to another consumer for overall load balance will need to be closed and revoked. This changes the semantics of the `StateListener` a bit, as it will not necessarily transition to `REBALANCING` at the beginning of a rebalance anymore. Note that this means IQ will now be available at all times except during state restoration, including while a rebalance is in progress. If restoration is occurring when a rebalance begins, we will continue to actively restore the state stores and/or process standby tasks during a cooperative rebalance. Note that with this new rebalancing protocol, you may sometimes see a rebalance be followed by a second short rebalance that ensures all tasks are safely distributed. For details on please see [KIP-429](https://cwiki.apache.org/confluence/x/vAclBg). 

The 2.4.0 release contains newly added and reworked metrics. [KIP-444](https://cwiki.apache.org/confluence/x/CiiGBg) adds new _client level_ (i.e., `KafkaStreams` instance level) metrics to the existing thread-level, task-level, and processor-/state-store-level metrics. For a full list of available client level metrics, see the [KafkaStreams monitoring](/43/documentation/#kafka_streams_client_monitoring) section in the operations guide.   
Furthermore, RocksDB metrics are exposed via [KIP-471](https://cwiki.apache.org/confluence/x/A5LiBg). For a full list of available RocksDB metrics, see the [RocksDB monitoring](/43/documentation/#kafka_streams_rocksdb_monitoring) section in the operations guide. 

Kafka Streams `test-utils` got improved via [KIP-470](https://cwiki.apache.org/confluence/x/tI-iBg) to simplify the process of using `TopologyTestDriver` to test your application code. We deprecated `ConsumerRecordFactory`, `TopologyTestDriver#pipeInput()`, `OutputVerifier`, as well as `TopologyTestDriver#readOutput()` and replace them with `TestInputTopic` and `TestOutputTopic`, respectively. We also introduced a new class `TestRecord` that simplifies assertion code. For full details see the [Testing section](/43/documentation/streams/developer-guide/testing.html) in the developer guide. 

In 2.4.0, we deprecated `WindowStore#put(K key, V value)` that should never be used. Instead the existing `WindowStore#put(K key, V value, long windowStartTimestamp)` should be used ([KIP-474](https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=115526545)). 

Furthermore, the `PartitionGrouper` interface and its corresponding configuration parameter `partition.grouper` were deprecated ([KIP-528](https://cwiki.apache.org/confluence/x/BwzABw)) and will be removed in the next major release ([KAFKA-7785](https://issues.apache.org/jira/browse/KAFKA-7785). Hence, this feature won't be supported in the future any longer and you need to updated your code accordingly. If you use a custom `PartitionGrouper` and stop to use it, the created tasks might change. Hence, you will need to reset your application to upgrade it. 

For Streams API changes in version older than 2.4.x, please check [3.9 upgrade document](/39/documentation/streams/upgrade-guide).

## Streams API broker compatibility

The following table shows which versions of the Kafka Streams API are compatible with various Kafka broker versions. For Kafka Stream version older than 2.4.x, please check [3.9 upgrade document](/39/documentation/streams/upgrade-guide).  
  
<table>  
<tr>  
<th>


</th>  
<th>

Kafka Broker (columns)
</th> </tr>  
<tr>  
<td>

Kafka Streams API (rows)
</td>  
<td>

2.4.x and  
2.5.x and  
2.6.x and  
2.7.x and  
2.8.x and  
3.0.x and  
3.1.x and  
3.2.x and  
3.3.x and  
3.4.x and  
3.5.x and  
3.6.x and  
3.7.x and  
3.8.x and  
3.9.x and  
4.0.x
</td>  
<td>

4.1.x
</td> </tr>  
<tr>  
<td>

2.4.x and  
2.5.x
</td>  
<td>

compatible
</td>  
<td>

compatible
</td> </tr>  
<tr>  
<td>

2.6.x and  
2.7.x and  
2.8.x and  
3.0.x and  
3.1.x and  
3.2.x and  
3.3.x and  
3.4.x and  
3.5.x and  
3.6.x and  
3.7.x and  
3.8.x and  
3.9.x and  
4.0.x and  
4.1.x
</td>  
<td>

compatible; enabling exactly-once v2 requires broker version 2.5.x or higher
</td>  
<td>

compatible
</td> </tr> </table>

[Previous](/43/documentation/streams/developer-guide/app-reset-tool) Next

  * [Documentation](/documentation)
  * [Kafka Streams](/documentation/streams)


