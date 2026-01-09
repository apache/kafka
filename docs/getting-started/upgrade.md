---
title: Upgrading
description: 
weight: 5
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


## Upgrading to 4.3.0

### Upgrading Servers to 4.3.0 from any version 3.3.x through 4.2.0

### Notable changes in 4.3.0

  * Support dynamically changing configs for dynamic quorum controllers. Previously only brokers and static quorum controllers were supported. For further details, please refer to [KAFKA-18928](https://issues.apache.org/jira/browse/KAFKA-18928). 
  * Two new configs have been introduced: `group.coordinator.cached.buffer.max.bytes` and `share.coordinator.cached.buffer.max.bytes`. They allow the respective coordinators to set the maximum buffer size retained for reuse. For further details, please refer to [KIP-1196](https://cwiki.apache.org/confluence/x/hA5JFg). 
  * The new config have been introduced: `remote.log.metadata.topic.min.isr` with 2 as default value. You can correct the min.insync.replicas for the existed __remote_log_metadata topic via kafka-configs.sh if needed. For further details, please refer to [KIP-1235](https://cwiki.apache.org/confluence/x/yommFw).


## Upgrading to 4.2.0

### Upgrading Servers to 4.2.0 from any version 3.3.x through 4.1.x

### Notable changes in 4.2.0

  * The `--max-partition-memory-bytes` option in `kafka-console-producer` is deprecated and will be removed in Kafka 5.0. Please use `--batch-size` instead. 
  * Queues for Kafka ([KIP-932](https://cwiki.apache.org/confluence/x/4hA0Dw)) is production-ready in Apache Kafka 4.2. This feature introduces a new kind of group called share groups, as an alternative to consumer groups. Consumers in a share group cooperatively consume records from topics, without assigning each partition to just one consumer. Share groups also introduce per-record acknowledgement and counting of delivery attempts. Use share groups in cases where records are processed one at a time, rather than as part of an ordered stream. 
  * The `org.apache.kafka.common.header.internals.RecordHeader` class has been updated to be read thread-safe. See [KIP-1205](https://cwiki.apache.org/confluence/x/nYmhFg) for details. In other words, each individual `Header` object within a `ConsumerRecord`'s `headers` can now be safely read from multiple threads concurrently. 
  * The `org.apache.kafka.disallowed.login.modules` config was deprecated. Please use the `org.apache.kafka.allowed.login.modules` instead. 
  * The `remote.log.manager.thread.pool.size` config was deprecated. Please use the `remote.log.manager.follower.thread.pool.size` instead. 
  * The `KafkaPrincipalBuilder` now extends `KafkaPrincipalSerde`. Force developer to implement `KafkaPrincipalSerde` interface for custom `KafkaPrincipalBuilder`. For further details, please refer to [KIP-1157](https://cwiki.apache.org/confluence/x/1gq9F). 
  * The behavior of `org.apache.kafka.streams.KafkaStreams#removeStreamThread` has been changed. The consumer has no longer remove once `removeStreamThread` finished. Instead, consumer would be kicked off from the group after `org.apache.kafka.streams.processor.internals.StreamThread` completes its `run` function. 
  * The support for MX4J library, enabled through `kafka_mx4jenable` system property, was deprecated and will be removed in Kafka 5.0. 
  * The `PARTITIONER_ADPATIVE_PARTITIONING_ENABLE_CONFIG` in `ProducerConfig` was deprecated and will be removed in Kafka 5.0. Please use the `PARTITIONER_ADAPTIVE_PARTITIONING_ENABLE_CONFIG` instead. 
  * The `ConsumerPerformance` command line tool has a new `--include` option that is an alternative to the `--topic` option. This new option allows to pass a regular expression specifying a list of topics to include for consumption, which is useful to test consumer performance across multiple topics or dynamically matching topic sets. 
  * The consistency of argument names for the command-line tools has been improved ([KIP-1147](https://cwiki.apache.org/confluence/x/DguWF)). The deprecated options will be removed in Kafka 5.0. 
    * In `kafka-producer-perf-test.sh`, `--bootstrap-server` and `--reporting-interval` option are added. 
    * In `kafka-console-consumer.sh` and `kafka-console-share-consumer.sh`, the option `--property` which is used to specify the properties for the formatter is deprecated in favor of `--formatter-property`. 
    * In `kafka-console-producer.sh`, the option `--property` which is used to specify properties in the form key=value to the message reader is deprecated in favor of `--reader-property`. 
    * In `kafka-consumer-perf-test.sh` and `kafka-share-consumer-perf-test.sh`, the option `--messages` is deprecated in favor of `--num-records` to bring all performance testing tools in line. 
    * The option `--command-property` is used for all command-line tools which accept configuration properties directly on the command line. The tools affected are: 
      * `kafka-console-consumer.sh`, `kafka-console-producer.sh` and `kafka-console-share-consumer.sh` (`--consumer-property` and `--producer-property` are deprecated in favor of `--command-property`) 
      * `kafka-producer-perf-test.sh` (`--producer-props` is deprecated in favor of `--command-property`) 
      * `kafka-consumer-perf-test.sh` and `kafka-share-consumer-perf-test.sh` gain the `--command-property` option to bring all performance testing tools in line 
    * The option `--command-config` is used for all command-line tools which accept a file of configuration properties. The tools affected are: 
      * `kafka-cluster.sh` (`--config` is deprecated in favor of `--command-config`) 
      * `kafka-console-consumer.sh`, `kafka-console-producer.sh` and `kafka-console-share-consumer.sh` (`--consumer.config` and `--producer.config` are deprecated in favor of `--command-config`) 
      * `kafka-consumer-perf-test.sh`, `kafka-producer-perf-test.sh` and `kafka-share-consumer-perf-test.sh` (`--consumer.config` and `--producer.config` are deprecated in favor of `--command-config`) 
      * `kafka-verifiable-consumer.sh` and `kafka-verifiable-producer.sh` (`--consumer.config` and `--producer.config` are deprecated in favor of `--command-config`) 
      * `kafka-leader-election.sh` (`--admin.config` is deprecated in favor of `--command-config`) 
      * `kafka-streams-application-reset.sh` (`--config-file` is deprecated in favor of `--command-config`) 
  * The `num.replica.fetchers` config has a new lower bound of 1. 
  * Improvements have been made to the validation rules and default values of LIST-type configurations ([KIP-1161](https://cwiki.apache.org/confluence/x/HArXF)). 
    * LIST-type configurations now enforce stricter validation: 
      * Null values are no longer accepted for most LIST-type configurations. Exceptions apply only to configurations that have null as their default value, as users cannot explicitly assign null values in configuration files or through the API.
      * Most LIST-type configurations no longer accept duplicate entries, except in cases where duplicates are explicitly supported. For backward compatibility, if users configure duplicate entries when they are not accepted, duplicate entries will be ignored and a warning will be logged.
      * For certain configurations, an empty list causes the system to malfunction. Therefore, empty lists are no longer allowed for those configurations.
    * Several configurations have been reclassified from STRING-type to LIST-type to better reflect their intended use as comma-separated values. 
    * Default values for certain configurations have been adjusted to ensure better consistency with related settings. 
    * `cleanup.policy` now supports empty values, which means infinite retention. This is equivalent to setting `retention.ms=-1` and `retention.bytes=-1`   
If `cleanup.policy` is empty and `remote.storage.enable` is set to true, the local log segments will be cleaned based on the values of `log.local.retention.bytes` and `log.local.retention.ms`.   
If `cleanup.policy` is empty and `remote.storage.enable` is set to false, local log segments will not be deleted automatically. However, records can still be deleted explicitly through `deleteRecords` API calls, which will advance the log start offset and remove the corresponding log segments. 
  * The `controller.quorum.auto.join.enable` has been added to `QuorumConfig`, enabling KRaft controllers to automatically join the cluster's voter set, and defaults to false. For further details, please refer to [KIP-853](https://cwiki.apache.org/confluence/x/nyH1D). 
  * The AppInfo metrics will deprecate the following metric names, which will be removed in Kafka 5.0: 
    * `[name=start-time-ms, group=app-info, description=Metric indicating start-time-ms, tags={}]`
    * `[name=commit-id, group=app-info, description=Metric indicating commit-id, tags={}]`
    * `[name=version, group=app-info, description=Metric indicating version, tags={}]`
In addition, the `client-id` will be added to the tags of these metrics. The new metric names will be: 
    * `[name=start-time-ms, group=app-info, description=Metric indicating start-time-ms, tags={client-id=...}]`
    * `[name=commit-id, group=app-info, description=Metric indicating commit-id, tags={client-id=...}]`
    * `[name=version, group=app-info, description=Metric indicating version, tags={client-id=...}]`
For further details, please refer to [KIP-1120](https://cwiki.apache.org/confluence/x/3gn0Ew). 
  * The metrics `org.apache.kafka.server:type=AssignmentsManager.QueuedReplicaToDirAssignments`, `org.apache.kafka.storage.internals.log:type=RemoteStorageThreadPool.RemoteLogReaderTaskQueueSize`, and `org.apache.kafka.storage.internals.log:type=RemoteStorageThreadPool.RemoteLogReaderAvgIdlePercent` have been deprecated and will be removed in Kafka 5.0. As replacements, the following metrics have been introduced, which report the same information: `kafka.server:type=AssignmentsManager.QueuedReplicaToDirAssignments`, `kafka.log.remote:type=RemoteStorageThreadPool.RemoteLogReaderTaskQueueSize`, and `kafka.log.remote:type=RemoteStorageThreadPool.RemoteLogReaderAvgIdlePercent`. For further details, please refer to [KIP-1100](https://cwiki.apache.org/confluence/x/6oqMEw). 
  * A new metric `AvgIdleRatio` has been added to the `ControllerEventManager` and `MetadataLoader` groups. These metrics measure the average idle ratio of their respective event queue threads, providing visibility into how much time each component spends waiting for events versus processing them. The metric value ranges from 0.0 (always busy) to 1.0 (always idle). 
  * Deprecated `org.apache.kafka.streams.KafkaStreams$CloseOptions` and its related methods, such as `KafkaStreams#close(org.apache.kafka.streams.KafkaStreams$CloseOptions)`. As a replacement, please use `org.apache.kafka.streams.CloseOptions` and `KafkaStreams#close(org.apache.kafka.streams.CloseOptions)`. For further details, please refer to [KIP-1153](https://cwiki.apache.org/confluence/x/QAq9F). 
  * A new implementation of `ConnectorClientConfigOverridePolicy`, `AllowlistConnectorClientConfigOverridePolicy`, has been added. This enables specifying the configurations that connectors can override via `connector.client.config.override.allowlist`. From Kafka 5.0.0, this will be the default [connector.client.config.override.policy](documentation/#connectconfigs_connector.client.config.override.policy) policy. The `PrincipalConnectorClientConfigOverridePolicy` policy is now deprecated and will be removed in Kafka 5.0.0. For further details, please refer to [KIP-1188](https://cwiki.apache.org/confluence/x/2IkvFg). 
  * It is now possible to specify the start time for a Kafka Streams punctuation, instead of relying on the non-deterministic time when you register it. For further details, please refer to [KIP-1146](https://cwiki.apache.org/confluence/x/9QqWF). 
  * Added an optional `--node-id` flag to the `FeatureCommand` command. It specifies the node to describe. If not provided, an arbitrary node is used. 



## Upgrading to 4.1.0

**Note:** Kafka Streams 4.1.0 contains a critical memory leak bug ([KAFKA-19748](https://issues.apache.org/jira/browse/KAFKA-19748)) that affects users of range scans and certain DSL operators (session windows, sliding windows, stream-stream joins, foreign-key joins). Users running Kafka Streams should consider upgrading directly to 4.1.1, which includes the fix for it.

### Upgrading Servers to 4.1.0 from any version 3.3.x through 4.0.x

### Notable changes in 4.1.0

  * Apache Kafka 4.1 ships with a preview of Queues for Kafka ([KIP-932](https://cwiki.apache.org/confluence/x/4hA0Dw)). This feature introduces a new kind of group called share groups, as an alternative to consumer groups. Consumers in a share group cooperatively consume records from topics, without assigning each partition to just one consumer. Share groups also introduce per-record acknowledgement and counting of delivery attempts. Use share groups in cases where records are processed one at a time, rather than as part of an ordered stream. To enable share groups, use the `kafka-features.sh` tool to upgrade to `share.version=1`. For more information, please read the [ release notes](https://cwiki.apache.org/confluence/x/CIq3FQ). 
  * **Common**
    * The logger class name for LogCleaner has been updated from `kafka.log.LogCleaner` to `org.apache.kafka.storage.internals.log.LogCleaner` in the log4j2.yaml configuration file. Added loggers for `org.apache.kafka.storage.internals.log.LogCleaner$CleanerThread` and `org.apache.kafka.storage.internals.log.Cleaner` classes to CleanerAppender. 
    * The filename for rotated `state-change.log` files has been updated from `stage-change.log.[date]` to `state-change.log.[date]` in the log4j2.yaml configuration file. 
  * **Broker**
    * The configuration `log.cleaner.enable` is deprecated. Users should no longer set it to `false` to prepare for future removal. After the removal, `log.cleaner.threads` will also have a lower bound of 1. For further details, please refer to [KIP-1148](https://cwiki.apache.org/confluence/x/XAyWF). 
    * The KIP-966 part 1: Eligible Leader Replicas(ELR) will be enabled by default on the new clusters. After the ELR feature enabled, the previously set `min.insync.replicas` value at the broker-level config will be removed. Please set at the cluster-level if necessary. For further details, please refer to [here](/43/documentation.html#eligible_leader_replicas). 
  * **Producer**
    * The `flush` method now detects potential deadlocks and prohibits its use inside a callback. This change prevents unintended blocking behavior, which was a known risk in earlier versions. 
  * **Command**
    * The `force` option of `ConfigCommand` has been removed, as it has been non-operational since version 0.10.1.0. 
  * **Admin**
    * The `listConsumerGroups()` and `listConsumerGroups(ListConsumerGroupsOptions)` methods in `Admin` are deprecated, and will be removed in the next major version. Use `Admin.listGroups(ListGroupsOptions.forConsumerGroups())` instead. 
  * **Kafka Streams**
    * The `window.size.ms` and `window.inner.serde.class` in `StreamsConfig` are deprecated. Use the corresponding string constants defined in `TimeWindowedSerializer`, `TimeWindowedDeserializer`, `SessionWindowedSerializer` and `SessionWindowedDeserializer` instead. 



## Upgrading to 4.0.1

### Upgrading Clients to 4.0.1

**For a rolling upgrade:**

  1. Upgrade the clients one at a time: shut down the client, update the code, and restart it.
  2. Clients (including Streams and Connect) must be on version 2.1 or higher before upgrading to 4.0. Many deprecated APIs were removed in Kafka 4.0. For more information about the compatibility, please refer to the [compatibility matrix](/43/documentation/compatibility.html) or [KIP-1124](https://cwiki.apache.org/confluence/x/y4kgF).



### Upgrading Servers to 4.0.1 from any version 3.3.x through 3.9.x

Note: Apache Kafka 4.0 only supports KRaft mode - ZooKeeper mode has been removed. As such, **broker upgrades to 4.0.0 (and higher) require KRaft mode and the software and metadata versions must be at least 3.3.x** (the first version when KRaft mode was deemed production ready). For clusters in KRaft mode with versions older than 3.3.x, we recommend upgrading to 3.9.x before upgrading to 4.0.x. Clusters in ZooKeeper mode have to be [migrated to KRaft mode](/43/documentation.html#kraft_zk_migration) before they can be upgraded to 4.0.x. 

**For a rolling upgrade:**

  1. Upgrade the brokers one at a time: shut down the broker, update the code, and restart it. Once you have done so, the brokers will be running the latest version and you can verify that the cluster's behavior and performance meet expectations. 
  2. Once the cluster's behavior and performance have been verified, finalize the upgrade by running ` bin/kafka-features.sh --bootstrap-server localhost:9092 upgrade --release-version 4.0 `
  3. Note that cluster metadata downgrade is not supported in this version since it has metadata changes. Every [MetadataVersion](https://github.com/apache/kafka/blob/trunk/server-common/src/main/java/org/apache/kafka/server/common/MetadataVersion.java) has a boolean parameter that indicates if there are metadata changes (i.e. `IBP_4_0_IV1(23, "4.0", "IV1", true)` means this version has metadata changes). Given your current and target versions, a downgrade is only possible if there are no metadata changes in the versions between.



### Notable changes in 4.0.1

  * The filename for rotated `state-change.log` files has been updated from `stage-change.log.[date]` to `state-change.log.[date]` in the log4j2.yaml configuration file. See [KAFKA-19576](https://issues.apache.org/jira/browse/KAFKA-19576) for details. 
  * Kafka Streams include a critical fix to upgrade from `KStreams#transformValues()` (remove with 4.0.0 release) to `KStreams#processValues()`. For more details, see the [migration guide](/43/documentation/streams/developer-guide/dsl-api.html#transformers-removal-and-migration-to-processors). 



### Notable changes in 4.0.0

  * Old protocol API versions have been removed. Users should ensure brokers are version 2.1 or higher before upgrading Java clients (including Connect and Kafka Streams which use the clients internally) to 4.0. Similarly, users should ensure their Java clients (including Connect and Kafka Streams) version is 2.1 or higher before upgrading brokers to 4.0. Finally, care also needs to be taken when it comes to kafka clients that are not part of Apache Kafka, please see [KIP-896](https://cwiki.apache.org/confluence/x/K5sODg) for the details. 
  * Apache Kafka 4.0 only supports KRaft mode - ZooKeeper mode has been removed. About version upgrade, check [Upgrading to 4.0.1 from any version 3.3.x through 3.9.x](/43/documentation.html#upgrade_4_0_1) for more info. 
  * Apache Kafka 4.0 ships with a brand-new group coordinator implementation (See [here](https://cwiki.apache.org/confluence/x/HhD1D)). Functionally speaking, it implements all the same APIs. There are reasonable defaults, but the behavior of the new group coordinator can be tuned by setting the configurations with prefix `group.coordinator`. 
  * The Next Generation of the Consumer Rebalance Protocol ([KIP-848](https://cwiki.apache.org/confluence/x/HhD1D)) is now Generally Available (GA) in Apache Kafka 4.0. The protocol is automatically enabled on the server when the upgrade to 4.0 is finalized. Note that once the new protocol is used by consumer groups, the cluster can only be downgraded to version 3.4.1 or newer. For more information check [here](/43/documentation.html#consumer_rebalance_protocol). 
  * Transactions Server-Side Defense ([KIP-890](https://cwiki.apache.org/confluence/x/B40ODg)) brings a strengthened transactional protocol to Apache Kafka 4.0. The new and improved transactional protocol is enabled when the upgrade to 4.0 is finalized. When using 4.0 producer clients, the producer epoch is bumped on every transaction to ensure every transaction includes the intended messages and duplicates are not written as part of the next transaction. Downgrading the protocol is safe. For more information check [here](/43/documentation.html#transaction_protocol). 
  * Eligible Leader Replicas ([KIP-966 Part 1](https://cwiki.apache.org/confluence/x/mpOzDw)) enhances the replication protocol for the Apache Kafka 4.0. Now the KRaft controller keeps track of the data partition replicas that are not included in ISR but are safe to be elected as leader without data loss. Such replicas are stored in the partition metadata as the `Eligible Leader Replicas`(ELR). For more information check [here](/43/documentation.html#eligible_leader_replicas). 
  * Since Apache Kafka 4.0.0, we have added a system property (`org.apache.kafka.sasl.oauthbearer.allowed.urls`) to set the allowed URLs as SASL OAUTHBEARER token or jwks endpoints. By default, the value is an empty list. Users should explicitly set the allowed list if necessary. 
  * A number of deprecated classes, methods, configurations and tools have been removed. 
    * **Common**
      * The `metrics.jmx.blacklist` and `metrics.jmx.whitelist` configurations were removed from the `org.apache.kafka.common.metrics.JmxReporter` Please use `metrics.jmx.exclude` and `metrics.jmx.include` respectively instead. 
      * The `auto.include.jmx.reporter` configuration was removed. The `metric.reporters` configuration is now set to `org.apache.kafka.common.metrics.JmxReporter` by default. 
      * The constructor `org.apache.kafka.common.metrics.JmxReporter` with string argument was removed. See [KIP-606](https://cwiki.apache.org/confluence/x/SxIRCQ) for details. 
      * The `bufferpool-wait-time-total`, `io-waittime-total`, and `iotime-total` metrics were removed. Please use `bufferpool-wait-time-ns-total`, `io-wait-time-ns-total`, and `io-time-ns-total` metrics as replacements, respectively. 
      * The `kafka.common.requests.DescribeLogDirsResponse.LogDirInfo` class was removed. Please use the `kafka.clients.admin.DescribeLogDirsResult.descriptions()` class and `kafka.clients.admin.DescribeLogDirsResult.allDescriptions()` instead. 
      * The `kafka.common.requests.DescribeLogDirsResponse.ReplicaInfo` class was removed. Please use the `kafka.clients.admin.DescribeLogDirsResult.descriptions()` class and `kafka.clients.admin.DescribeLogDirsResult.allDescriptions()` instead. 
      * The `org.apache.kafka.common.security.oauthbearer.secured.OAuthBearerLoginCallbackHandler` class was removed. Please use the `org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginCallbackHandler` class instead. 
      * The `org.apache.kafka.common.security.oauthbearer.secured.OAuthBearerValidatorCallbackHandler` class was removed. Please use the `org.apache.kafka.common.security.oauthbearer.OAuthBearerValidatorCallbackHandler` class instead. 
      * The `org.apache.kafka.common.errors.NotLeaderForPartitionException` class was removed. The `org.apache.kafka.common.errors.NotLeaderOrFollowerException` is returned if a request could not be processed because the broker is not the leader or follower for a topic partition. 
      * The `org.apache.kafka.clients.producer.internals.DefaultPartitioner` and `org.apache.kafka.clients.producer.UniformStickyPartitioner` classes were removed. 
      * The `log.message.format.version` and `message.format.version` configs were removed. 
      * The function `onNewBatch` in `org.apache.kafka.clients.producer.Partitioner` class was removed. 
      * The default properties files for KRaft mode are no longer stored in the separate `config/kraft` directory since Zookeeper has been removed. These files have been consolidated with other configuration files. Now all configuration files are in `config` directory. 
      * The valid format for `--bootstrap-server` only supports comma-separated value, such as `host1:port1,host2:port2,...`. Providing other formats, like space-separated bootstrap servers (e.g., `host1:port1 host2:port2 host3:port3`), will result in an exception, even though this was allowed in Apache Kafka versions prior to 4.0. 
    * **Broker**
      * The `delegation.token.master.key` configuration was removed. Please use `delegation.token.secret.key` instead. 
      * The `offsets.commit.required.acks` configuration was removed. See [KIP-1041](https://cwiki.apache.org/confluence/x/9YobEg) for details. 
      * The `log.message.timestamp.difference.max.ms` configuration was removed. Please use `log.message.timestamp.before.max.ms` and `log.message.timestamp.after.max.ms` instead. See [KIP-937](https://cwiki.apache.org/confluence/x/thQ0Dw) for details. 
      * The `remote.log.manager.copier.thread.pool.size` configuration default value was changed to 10 from -1. Values of -1 are no longer valid; a minimum value of 1 or higher is required. See [KIP-1030](https://cwiki.apache.org/confluence/x/FAqpEQ)
      * The `remote.log.manager.expiration.thread.pool.size` configuration default value was changed to 10 from -1. Values of -1 are no longer valid; a minimum value of 1 or higher is required. See [KIP-1030](https://cwiki.apache.org/confluence/x/FAqpEQ)
      * The `remote.log.manager.thread.pool.size` configuration default value was changed to 2 from 10. See [KIP-1030](https://cwiki.apache.org/confluence/x/FAqpEQ)
      * The minimum `segment.bytes/log.segment.bytes` has changed from 14 bytes to 1MB. See [KIP-1030](https://cwiki.apache.org/confluence/x/FAqpEQ)
    * **MirrorMaker**
      * The original MirrorMaker (MM1) and related classes were removed. Please use the Connect-based MirrorMaker (MM2), as described in the [Geo-Replication section.](/43/documentation/#georeplication). 
      * The `use.incremental.alter.configs` configuration was removed from `MirrorSourceConnector`. The modified behavior is now identical to the previous `required` configuration, therefore users should ensure that brokers in the target cluster are at least running 2.3.0. 
      * The `add.source.alias.to.metrics` configuration was removed from `MirrorSourceConnector`. The source cluster alias is now always added to the metrics. 
      * The `config.properties.blacklist` was removed from the `org.apache.kafka.connect.mirror.MirrorSourceConfig` Please use `config.properties.exclude` instead. 
      * The `topics.blacklist` was removed from the `org.apache.kafka.connect.mirror.MirrorSourceConfig` Please use `topics.exclude` instead. 
      * The `groups.blacklist` was removed from the `org.apache.kafka.connect.mirror.MirrorSourceConfig` Please use `groups.exclude` instead. 
    * **Tools**
      * The `kafka.common.MessageReader` class was removed. Please use the [`org.apache.kafka.tools.api.RecordReader`](/{version}/javadoc/org/apache/kafka/tools/api/RecordReader.html) interface to build custom readers for the `kafka-console-producer` tool. 
      * The `kafka.tools.DefaultMessageFormatter` class was removed. Please use the `org.apache.kafka.tools.consumer.DefaultMessageFormatter` class instead. 
      * The `kafka.tools.LoggingMessageFormatter` class was removed. Please use the `org.apache.kafka.tools.consumer.LoggingMessageFormatter` class instead. 
      * The `kafka.tools.NoOpMessageFormatter` class was removed. Please use the `org.apache.kafka.tools.consumer.NoOpMessageFormatter` class instead. 
      * The `--whitelist` option was removed from the `kafka-console-consumer` command line tool. Please use `--include` instead. 
      * Redirections from the old tools packages have been removed: `kafka.admin.FeatureCommand`, `kafka.tools.ClusterTool`, `kafka.tools.EndToEndLatency`, `kafka.tools.StateChangeLogMerger`, `kafka.tools.StreamsResetter`, `kafka.tools.JmxTool`. 
      * The `--authorizer`, `--authorizer-properties`, and `--zk-tls-config-file` options were removed from the `kafka-acls` command line tool. Please use `--bootstrap-server` or `--bootstrap-controller` instead. 
      * The `kafka.serializer.Decoder` trait was removed, please use the [`org.apache.kafka.tools.api.Decoder`](/{version}/javadoc/org/apache/kafka/tools/api/Decoder.html) interface to build custom decoders for the `kafka-dump-log` tool. 
      * The `kafka.coordinator.group.OffsetsMessageFormatter` class was removed. Please use the `org.apache.kafka.tools.consumer.OffsetsMessageFormatter` class instead. 
      * The `kafka.coordinator.group.GroupMetadataMessageFormatter` class was removed. Please use the `org.apache.kafka.tools.consumer.GroupMetadataMessageFormatter` class instead. 
      * The `kafka.coordinator.transaction.TransactionLogMessageFormatter` class was removed. Please use the `org.apache.kafka.tools.consumer.TransactionLogMessageFormatter` class instead. 
      * The `--topic-white-list` option was removed from the `kafka-replica-verification` command line tool. Please use `--topics-include` instead. 
      * The `--broker-list` option was removed from the `kafka-verifiable-consumer` command line tool. Please use `--bootstrap-server` instead. 
      * `kafka-configs.sh` now uses incrementalAlterConfigs API to alter broker configurations instead of the deprecated alterConfigs API, and it will fall directly if the broker doesn't support incrementalAlterConfigs API, which means the broker version is prior to 2.3.x. See [KIP-1011](https://cwiki.apache.org/confluence/x/wIn5E) for more details. 
      * The `kafka.admin.ZkSecurityMigrator` tool was removed. 
    * **Connect**
      * The `whitelist` and `blacklist` configurations were removed from the `org.apache.kafka.connect.transforms.ReplaceField` transformation. Please use `include` and `exclude` respectively instead. 
      * The `onPartitionsRevoked(Collection<TopicPartition>)` and `onPartitionsAssigned(Collection<TopicPartition>)` methods were removed from `SinkTask`. 
      * The `commitRecord(SourceRecord)` method was removed from `SourceTask`. 
    * **Consumer**
      * The `poll(long)` method was removed from the consumer. Please use `poll(Duration)` instead. Note that there is a difference in behavior between the two methods. The `poll(Duration)` method does not block beyond the timeout awaiting partition assignment, whereas the earlier `poll(long)` method used to wait beyond the timeout. 
      * The `committed(TopicPartition)` and `committed(TopicPartition, Duration)` methods were removed from the consumer. Please use `committed(Set<TopicPartition>)` and `committed(Set<TopicPartition>, Duration)` instead. 
      * The `setException(KafkaException)` method was removed from the `org.apache.kafka.clients.consumer.MockConsumer`. Please use `setPollException(KafkaException)` instead. 
    * **Producer**
      * The `enable.idempotence` configuration will no longer automatically fall back when the `max.in.flight.requests.per.connection` value exceeds 5. 
      * The deprecated `sendOffsetsToTransaction(Map<TopicPartition, OffsetAndMetadata>, String)` method has been removed from the Producer API. 
      * The default `linger.ms` changed from 0 to 5 in Apache Kafka 4.0 as the efficiency gains from larger batches typically result in similar or lower producer latency despite the increased linger. 
    * **Admin client**
      * The `alterConfigs` method was removed from the `org.apache.kafka.clients.admin.Admin`. Please use `incrementalAlterConfigs` instead. 
      * The `org.apache.kafka.common.ConsumerGroupState` enumeration and related methods have been deprecated. Please use `GroupState` instead which applies to all types of group. 
      * The `Admin.describeConsumerGroups` method used to return a `ConsumerGroupDescription` in state `DEAD` if the group ID was not found. In Apache Kafka 4.0, the `GroupIdNotFoundException` is thrown instead as part of the support for new types of group. 
      * The `org.apache.kafka.clients.admin.DeleteTopicsResult.values()` method was removed. Please use `org.apache.kafka.clients.admin.DeleteTopicsResult.topicNameValues()` instead. 
      * The `org.apache.kafka.clients.admin.TopicListing.TopicListing(String, boolean)` method was removed. Please use `org.apache.kafka.clients.admin.TopicListing.TopicListing(String, Uuid, boolean)` instead. 
      * The `org.apache.kafka.clients.admin.ListConsumerGroupOffsetsOptions.topicPartitions(List<TopicPartition>)` method was removed. Please use `org.apache.kafka.clients.admin.Admin.listConsumerGroupOffsets(Map<String, ListConsumerGroupOffsetsSpec>, ListConsumerGroupOffsetsOptions)` instead. 
      * The deprecated `dryRun` methods were removed from the `org.apache.kafka.clients.admin.UpdateFeaturesOptions`. Please use `validateOnly` instead. 
      * The constructor `org.apache.kafka.clients.admin.FeatureUpdate` with short and boolean arguments was removed. Please use the constructor that accepts short and the specified `UpgradeType` enum instead. 
      * The `allowDowngrade` method was removed from the `org.apache.kafka.clients.admin.FeatureUpdate`. 
      * The `org.apache.kafka.clients.admin.DescribeTopicsResult.DescribeTopicsResult(Map<String, KafkaFuture<TopicDescription>>)` method was removed. Please use `org.apache.kafka.clients.admin.DescribeTopicsResult.DescribeTopicsResult(Map<Uuid, KafkaFuture<TopicDescription>>, Map<String, KafkaFuture<TopicDescription>>)` instead. 
      * The `values()` method was removed from the `org.apache.kafka.clients.admin.DescribeTopicsResult`. Please use `topicNameValues()` instead. 
      * The `all()` method was removed from the `org.apache.kafka.clients.admin.DescribeTopicsResult`. Please use `allTopicNames()` instead. 
    * **Kafka Streams**
      * All public APIs, deprecated in Apache Kafka 3.6 or an earlier release, have been removed, with the exception of `JoinWindows.of()` and `JoinWindows#grace()`. See [KAFKA-17531](https://issues.apache.org/jira/browse/KAFKA-17531) for details. 
      * The most important changes are highlighted in the [Kafka Streams upgrade guide](/43/documentation/streams/upgrade-guide.html#streams_api_changes_400). 
      * For a full list of changes, see [KAFKA-12822](https://issues.apache.org/jira/browse/KAFKA-12822). 
      * If you are using `KStream#transformValues()` which was removed with Apache Kafka 4.0.0 release, and you need to rewrite your program to use `KStreams#processValues()` instead, pay close attention to the [migration guide](/43/documentation/streams/developer-guide/dsl-api.html#transformers-removal-and-migration-to-processors). 
  * Other changes: 
    * The minimum Java version required by clients and Kafka Streams applications has been increased from Java 8 to Java 11 while brokers, connect and tools now require Java 17. See [KIP-750](https://cwiki.apache.org/confluence/x/P4vOCg) and [KIP-1013](https://cwiki.apache.org/confluence/x/Bov5E) for more details. 
    * Java 23 support has been added in Apache Kafka 4.0. 
    * Scala 2.12 support has been removed in Apache Kafka 4.0. See [KIP-751](https://cwiki.apache.org/confluence/x/OovOCg) for more details 
    * Logging framework has been migrated from Log4j to Log4j2. Users can use the log4j-transform-cli tool to automatically convert their existing Log4j configuration files to Log4j2 format. See [log4j-transform-cli](https://logging.staged.apache.org/log4j/transform/cli.html#log4j-transform-cli) for more details. Log4j2 provides limited compatibility for Log4j configurations. See [Use Log4j 1 to Log4j 2 bridge](https://logging.apache.org/log4j/2.x/migrate-from-log4j1.html#ConfigurationCompatibility) for more information, 
    * KafkaLog4jAppender has been removed, users should migrate to the log4j2 appender See [KafkaAppender](https://logging.apache.org/log4j/2.x/manual/appenders.html#KafkaAppender) for more details 
    * The `--delete-config` option in the `kafka-topics` command line tool has been deprecated. 
    * For implementors of RemoteLogMetadataManager (RLMM), a new API `nextSegmentWithTxnIndex` is introduced in RLMM to allow the implementation to return the next segment metadata with a transaction index. This API is used when the consumers are enabled with isolation level as READ_COMMITTED. See [KIP-1058](https://cwiki.apache.org/confluence/x/BwuTEg) for more details. 
    * The criteria for identifying internal topics in ReplicationPolicy and DefaultReplicationPolicy have been updated to enable the replication of topics that appear to be internal but aren't truly internal to Kafka and Mirror Maker 2. See [KIP-1074](https://cwiki.apache.org/confluence/x/jA3OEg) for more details. 
    * [KIP-714](https://cwiki.apache.org/confluence/x/2xRRCg) is now enabled for Kafka Streams via [KIP-1076](https://cwiki.apache.org/confluence/x/XA-OEg). This allows to not only collect the metric of the internally used clients of a Kafka Streams application via a broker-side plugin, but also to collect the [metrics](/43/documentation/#kafka_streams_monitoring) of the Kafka Streams runtime itself. 
    * The default value of `num.recovery.threads.per.data.dir` has been changed from 1 to 2. The impact of this is faster recovery post unclean shutdown at the expense of extra IO cycles. See [KIP-1030](https://cwiki.apache.org/confluence/x/FAqpEQ)
    * The default value of `message.timestamp.after.max.ms` has been changed from Long.Max to 1 hour. The impact of this messages with a timestamp of more than 1 hour in the future will be rejected when message.timestamp.type=CreateTime is set. See [KIP-1030](https://cwiki.apache.org/confluence/x/FAqpEQ)
    * Introduced in [KIP-890](https://cwiki.apache.org/confluence/x/B40ODg), the `TransactionAbortableException` enhances error handling within transactional operations by clearly indicating scenarios where transactions should be aborted due to errors. It is important for applications to properly manage both `TimeoutException` and `TransactionAbortableException` when working with transaction producers.
      * **TimeoutException:** This exception indicates that a transactional operation has timed out. Given the risk of message duplication that can arise from retrying operations after a timeout (potentially violating exactly-once semantics), applications should treat timeouts as reasons to abort the ongoing transaction.
      * **TransactionAbortableException:** Specifically introduced to signal errors that should lead to transaction abortion, ensuring this exception is properly handled is critical for maintaining the integrity of transactional processing.
      * To ensure seamless operation and compatibility with future Kafka versions, developers are encouraged to update their error-handling logic to treat both exceptions as triggers for aborting transactions. This approach is pivotal for preserving exactly-once semantics.
      * See [KIP-890](https://cwiki.apache.org/confluence/x/B40ODg) and [KIP-1050](https://cwiki.apache.org/confluence/x/8ItyEg) for more details 
    * The filename for rotated `state-change.log` files incorrectly rotates to `stage-change.log.[date]` (changing state to stage). This issue is corrected in 4.0.1. See [KAFKA-19576](https://issues.apache.org/jira/browse/KAFKA-19576) for details. 



## Upgrading to 3.9.0 and older versions

See [Upgrading From Previous Versions](/39/documentation/#upgrade) in the 3.9 documentation.
