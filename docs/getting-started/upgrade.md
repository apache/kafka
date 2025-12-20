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


## Upgrading to 3.7.2 from any version 0.8.x through 3.6.x

### Notable changes in 3.7.2

  * In case you run your Kafka clusters with no execution permission for the `/tmp` partition, Kafka will not work properly. It might either refuse to start or fail when producing and consuming messages. This is due to the compression libraries `zstd-jni` and `snappy`. To remediate this problem you need to pass the following JVM flags to Kafka `ZstdTempFolder` and `org.xerial.snappy.tempdir` pointing to a directory with execution permissions. For example, this could be done via the `KAFKA_OPTS` environment variable like follows: `export KAFKA_OPTS="-DZstdTempFolder=/opt/kafka/tmp -Dorg.xerial.snappy.tempdir=/opt/kafka/tmp"`. 



## Upgrading to 3.7.0 from any version 0.8.x through 3.6.x

### Upgrading ZooKeeper-based clusters

**If you are upgrading from a version prior to 2.1.x, please see the note in step 5 below about the change to the schema used to store consumer offsets. Once you have changed the inter.broker.protocol.version to the latest version, it will not be possible to downgrade to a version prior to 2.1.**

**For a rolling upgrade:**

  1. Update server.properties on all brokers and add the following properties. CURRENT_KAFKA_VERSION refers to the version you are upgrading from. CURRENT_MESSAGE_FORMAT_VERSION refers to the message format version currently in use. If you have previously overridden the message format version, you should keep its current value. Alternatively, if you are upgrading from a version prior to 0.11.0.x, then CURRENT_MESSAGE_FORMAT_VERSION should be set to match CURRENT_KAFKA_VERSION. 
     * inter.broker.protocol.version=CURRENT_KAFKA_VERSION (e.g. `3.6`, `3.5`, etc.)
     * log.message.format.version=CURRENT_MESSAGE_FORMAT_VERSION (See potential performance impact following the upgrade for the details on what this configuration does.)
If you are upgrading from version 0.11.0.x or above, and you have not overridden the message format, then you only need to override the inter-broker protocol version. 
     * inter.broker.protocol.version=CURRENT_KAFKA_VERSION (e.g. `3.6`, `3.5`, etc.)
  2. Upgrade the brokers one at a time: shut down the broker, update the code, and restart it. Once you have done so, the brokers will be running the latest version and you can verify that the cluster's behavior and performance meets expectations. It is still possible to downgrade at this point if there are any problems. 
  3. Once the cluster's behavior and performance has been verified, bump the protocol version by editing `inter.broker.protocol.version` and setting it to `3.7`. 
  4. Restart the brokers one by one for the new protocol version to take effect. Once the brokers begin using the latest protocol version, it will no longer be possible to downgrade the cluster to an older version. 
  5. If you have overridden the message format version as instructed above, then you need to do one more rolling restart to upgrade it to its latest version. Once all (or most) consumers have been upgraded to 0.11.0 or later, change log.message.format.version to 3.7 on each broker and restart them one by one. Note that the older Scala clients, which are no longer maintained, do not support the message format introduced in 0.11, so to avoid conversion costs (or to take advantage of exactly once semantics), the newer Java clients must be used. 



### Upgrading KRaft-based clusters

**If you are upgrading from a version prior to 3.3.0, please see the note in step 3 below. Once you have changed the metadata.version to the latest version, it will not be possible to downgrade to a version prior to 3.3-IV0.**

**For a rolling upgrade:**

  1. Upgrade the brokers one at a time: shut down the broker, update the code, and restart it. Once you have done so, the brokers will be running the latest version and you can verify that the cluster's behavior and performance meets expectations. 
  2. Once the cluster's behavior and performance has been verified, bump the metadata.version by running ` ./bin/kafka-features.sh upgrade --metadata 3.7 `
  3. Note that cluster metadata downgrade is not supported in this version since it has metadata changes. Every [MetadataVersion](https://github.com/apache/kafka/blob/trunk/server-common/src/main/java/org/apache/kafka/server/common/MetadataVersion.java) after 3.2.x has a boolean parameter that indicates if there are metadata changes (i.e. `IBP_3_3_IV3(7, "3.3", "IV3", true)` means this version has metadata changes). Given your current and target versions, a downgrade is only possible if there are no metadata changes in the versions between.



### Notable changes in 3.7.2

  * The consumer gets an important bug-fix for KIP-714. See [KAFKA-17731](https://issues.apache.org/jira/browse/KAFKA-17731) for more details. 
  * Kafka Streams, using EOSv2, might commit incorrect offsets potentially leading to data loss in repartition topics. See [KAFKA-17635](https://issues.apache.org/jira/browse/KAFKA-17635) for more details. 
  * Kafka Streams might pause(), but never resume() partitions. See [KAFKA-17299](https://issues.apache.org/jira/browse/KAFKA-17299) for more details. 



### Notable changes in 3.7.1

  * MirrorMaker 2 can now emit checkpoints for offsets mirrored before the start of the Checkpoint task for improved offset translation. This requires MirrorMaker 2 to have READ authorization to the Checkpoint topic. If READ is not authorized, checkpointing is limited to offsets mirrorred after the start of the task. See [KAFKA-15905](https://issues.apache.org/jira/browse/KAFKA-15905) for more details. 
  * JBOD support in KRaft was introduced from Metadata Version (MV) 3.7-IV2. Configuring Brokers with multiple log directories can lead to indefinite unavailability. Brokers will now detect this situation and log an error. See [KAFKA-16606](https://issues.apache.org/jira/browse/KAFKA-16606) for more details. 



### Notable changes in 3.7.0

  * Java 11 support for the broker and tools has been deprecated and will be removed in Apache Kafka 4.0. This complements the previous deprecation of Java 8 for all components. Please refer to [KIP-1013](https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=284789510) for more details. 
  * Client APIs released prior to Apache Kafka 2.1 are now marked deprecated in 3.7 and will be removed in Apache Kafka 4.0. See [KIP-896](https://cwiki.apache.org/confluence/display/KAFKA/KIP-896%3A+Remove+old+client+protocol+API+versions+in+Kafka+4.0) for details and RPC versions that are now deprecated. 
  * Early access of the new simplified Consumer Rebalance Protocol is available, and it is not recommended for use in production environments. You are encouraged to test it and provide feedback! For more information about the early access feature, please check [KIP-848](https://cwiki.apache.org/confluence/display/KAFKA/KIP-848%3A+The+Next+Generation+of+the+Consumer+Rebalance+Protocol) and the [Early Access Release Notes](https://cwiki.apache.org/confluence/display/KAFKA/The+Next+Generation+of+the+Consumer+Rebalance+Protocol+%28KIP-848%29+-+Early+Access+Release+Notes). 
  * More metrics related to Tiered Storage have been introduced. They should improve the operational experience of running Tiered Storage in production. For more detailed information, please refer to [KIP-963](https://cwiki.apache.org/confluence/display/KAFKA/KIP-963%3A+Additional+metrics+in+Tiered+Storage). 
  * Kafka Streams ships multiple KIPs for IQv2 support. See the [Kafka Streams upgrade section](/37/documentation/streams/upgrade-guide#streams_api_changes_370) for more details. 
  * All the notable changes are present in the [blog post announcing the 3.7.0 release.](https://kafka.apache.org/blog#apache_kafka_370_release_announcement)



## Upgrading to 3.6.2 from any version 0.8.x through 3.5.x

### Upgrading ZooKeeper-based clusters

**If you are upgrading from a version prior to 2.1.x, please see the note in step 5 below about the change to the schema used to store consumer offsets. Once you have changed the inter.broker.protocol.version to the latest version, it will not be possible to downgrade to a version prior to 2.1.**

**For a rolling upgrade:**

  1. Update server.properties on all brokers and add the following properties. CURRENT_KAFKA_VERSION refers to the version you are upgrading from. CURRENT_MESSAGE_FORMAT_VERSION refers to the message format version currently in use. If you have previously overridden the message format version, you should keep its current value. Alternatively, if you are upgrading from a version prior to 0.11.0.x, then CURRENT_MESSAGE_FORMAT_VERSION should be set to match CURRENT_KAFKA_VERSION. 
     * inter.broker.protocol.version=CURRENT_KAFKA_VERSION (e.g. `3.5`, `3.4`, etc.)
     * log.message.format.version=CURRENT_MESSAGE_FORMAT_VERSION (See potential performance impact following the upgrade for the details on what this configuration does.)
If you are upgrading from version 0.11.0.x or above, and you have not overridden the message format, then you only need to override the inter-broker protocol version. 
     * inter.broker.protocol.version=CURRENT_KAFKA_VERSION (e.g. `3.5`, `3.4`, etc.)
  2. Upgrade the brokers one at a time: shut down the broker, update the code, and restart it. Once you have done so, the brokers will be running the latest version and you can verify that the cluster's behavior and performance meets expectations. It is still possible to downgrade at this point if there are any problems. 
  3. Once the cluster's behavior and performance has been verified, bump the protocol version by editing `inter.broker.protocol.version` and setting it to `3.6`. 
  4. Restart the brokers one by one for the new protocol version to take effect. Once the brokers begin using the latest protocol version, it will no longer be possible to downgrade the cluster to an older version. 
  5. If you have overridden the message format version as instructed above, then you need to do one more rolling restart to upgrade it to its latest version. Once all (or most) consumers have been upgraded to 0.11.0 or later, change log.message.format.version to 3.6 on each broker and restart them one by one. Note that the older Scala clients, which are no longer maintained, do not support the message format introduced in 0.11, so to avoid conversion costs (or to take advantage of exactly once semantics), the newer Java clients must be used. 



### Upgrading KRaft-based clusters

**If you are upgrading from a version prior to 3.3.0, please see the note in step 3 below. Once you have changed the metadata.version to the latest version, it will not be possible to downgrade to a version prior to 3.3-IV0.**

**For a rolling upgrade:**

  1. Upgrade the brokers one at a time: shut down the broker, update the code, and restart it. Once you have done so, the brokers will be running the latest version and you can verify that the cluster's behavior and performance meets expectations. 
  2. Once the cluster's behavior and performance has been verified, bump the metadata.version by running ` ./bin/kafka-features.sh upgrade --metadata 3.6 `
  3. Note that cluster metadata downgrade is not supported in this version since it has metadata changes. Every [MetadataVersion](https://github.com/apache/kafka/blob/trunk/server-common/src/main/java/org/apache/kafka/server/common/MetadataVersion.java) after 3.2.x has a boolean parameter that indicates if there are metadata changes (i.e. `IBP_3_3_IV3(7, "3.3", "IV3", true)` means this version has metadata changes). Given your current and target versions, a downgrade is only possible if there are no metadata changes in the versions between.



### Notable changes in 3.6.0

  * Apache Kafka now supports having both an IPv4 and an IPv6 listener on the same port. This change only applies to non advertised listeners (advertised listeners already have this feature)
  * The Apache Zookeeper dependency has been upgraded to 3.8.1 due to 3.6 reaching end-of-life. To bring both your Kafka and Zookeeper clusters to the latest versions: 
    * **>=2.4** Kafka clusters can be updated directly. Zookeeper clusters which are running binaries bundled with Kafka versions 2.4 or above can be updated directly.
    * **< 2.4** Kafka clusters first need to be updated to a version greater than 2.4 and smaller than 3.6. Zookeeper clusters which are running binaries bundled with Kafka versions below 2.4 need to be updated to any binaries bundled with Kafka versions greater than 2.4 and smaller than 3.6. You can then follow the first bullet-point.
For more detailed information please refer to the Compatibility, Deprecation, and Migration Plan section in [KIP-902](https://cwiki.apache.org/confluence/display/KAFKA/KIP-902%3A+Upgrade+Zookeeper+to+3.8.1). 
  * The configuration `log.message.timestamp.difference.max.ms` is deprecated. Two new configurations, `log.message.timestamp.before.max.ms` and `log.message.timestamp.after.max.ms`, have been added. For more detailed information, please refer to [KIP-937](https://cwiki.apache.org/confluence/display/KAFKA/KIP-937%3A+Improve+Message+Timestamp+Validation). 
  * Kafka Streams has introduced a new task assignor, `RackAwareTaskAssignor`, for computing task assignments which can minimize cross rack traffic under certain conditions. It works with existing `StickyTaskAssignor` and `HighAvailabilityTaskAssignor`. See [KIP-925](https://cwiki.apache.org/confluence/display/KAFKA/KIP-925%3A+Rack+aware+task+assignment+in+Kafka+Streams) and [**Kafka Streams Developer Guide**](/37/documentation/streams/developer-guide/config-streams.html#rack-aware-assignment-strategy) for more details. 
  * To account for a break in compatibility introduced in version 3.1.0, MirrorMaker 2 has added a new `replication.policy.internal.topic.separator.enabled` property. If upgrading from 3.0.x or earlier, it may be necessary to set this property to `false`; see the property's documentation for more details.
  * Early access of tiered storage feature is available, and it is not recommended for use in production environments. Welcome to test it and provide any feedback to us. For more information about the early access tiered storage feature, please check [KIP-405](https://cwiki.apache.org/confluence/display/KAFKA/KIP-405%3A+Kafka+Tiered+Storage) and [Tiered Storage Early Access Release Note](https://cwiki.apache.org/confluence/display/KAFKA/Kafka+Tiered+Storage+Early+Access+Release+Notes). 
  * Transaction partition verification ([KIP-890](https://cwiki.apache.org/confluence/display/KAFKA/KIP-890%3A+Transactions+Server-Side+Defense)) has been added to data partitions to prevent hanging transactions. This feature is enabled by default and can be disabled by setting `transaction.partition.verification.enable` to false. The configuration can also be updated dynamically and is applied to the broker. Workloads running on version 3.6.0 with compression can experience InvalidRecordExceptions and UnknownServerExceptions. Upgrading to 3.6.1 or newer or disabling the feature fixes the issue. 
  * The `ByteBufferDeserializer` ([KIP-863](https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=225152035)) was changed to avoid unnecessary deep-copies of data. This changes the behavior of the deserializer as user code cannot make any assumptions about buffer position, limit, capacity any longer. 



## Upgrading to 3.5.2 from any version 0.8.x through 3.4.x

All upgrade steps remain same as upgrading to 3.5.0

### Notable changes in 3.5.2

  * When migrating producer ID blocks from ZK to KRaft, there could be duplicate producer IDs being given to transactional or idempotent producers. This can cause long term problems since the producer IDs are persisted and reused for a long time. See [KAFKA-15552](https://issues.apache.org/jira/browse/KAFKA-15552) for more details. 
  * In 3.5.0 and 3.5.1, there could be an issue that the empty ISR is returned from controller after AlterPartition request during rolling upgrade. This issue will impact the availability of the topic partition. See [KAFKA-15353](https://issues.apache.org/jira/browse/KAFKA-15353) for more details. 



## Upgrading to 3.5.1 from any version 0.8.x through 3.4.x

All upgrade steps remain same as upgrading to 3.5.0

### Notable changes in 3.5.1

  * Upgraded the dependency, snappy-java, to a version which is not vulnerable to [CVE-2023-34455.](https://nvd.nist.gov/vuln/detail/CVE-2023-34455) You can find more information about the CVE at [Kafka CVE list.](/community/cve-list/#CVE-2023-34455)
  * Fixed a regression introduced in 3.3.0, which caused `security.protocol` configuration values to be restricted to upper case only. After the fix, `security.protocol` values are case insensitive. See [KAFKA-15053](https://issues.apache.org/jira/browse/KAFKA-15053) for details. 



## Upgrading to 3.5.0 from any version 0.8.x through 3.4.x

### Upgrading ZooKeeper-based clusters

**If you are upgrading from a version prior to 2.1.x, please see the note in step 5 below about the change to the schema used to store consumer offsets. Once you have changed the inter.broker.protocol.version to the latest version, it will not be possible to downgrade to a version prior to 2.1.**

**For a rolling upgrade:**

  1. Update server.properties on all brokers and add the following properties. CURRENT_KAFKA_VERSION refers to the version you are upgrading from. CURRENT_MESSAGE_FORMAT_VERSION refers to the message format version currently in use. If you have previously overridden the message format version, you should keep its current value. Alternatively, if you are upgrading from a version prior to 0.11.0.x, then CURRENT_MESSAGE_FORMAT_VERSION should be set to match CURRENT_KAFKA_VERSION. 
     * inter.broker.protocol.version=CURRENT_KAFKA_VERSION (e.g. `3.4`, `3.3`, etc.)
     * log.message.format.version=CURRENT_MESSAGE_FORMAT_VERSION (See potential performance impact following the upgrade for the details on what this configuration does.)
If you are upgrading from version 0.11.0.x or above, and you have not overridden the message format, then you only need to override the inter-broker protocol version. 
     * inter.broker.protocol.version=CURRENT_KAFKA_VERSION (e.g. `3.4`, `3.3`, etc.)
  2. Upgrade the brokers one at a time: shut down the broker, update the code, and restart it. Once you have done so, the brokers will be running the latest version and you can verify that the cluster's behavior and performance meets expectations. It is still possible to downgrade at this point if there are any problems. 
  3. Once the cluster's behavior and performance has been verified, bump the protocol version by editing `inter.broker.protocol.version` and setting it to `3.5`. 
  4. Restart the brokers one by one for the new protocol version to take effect. Once the brokers begin using the latest protocol version, it will no longer be possible to downgrade the cluster to an older version. 
  5. If you have overridden the message format version as instructed above, then you need to do one more rolling restart to upgrade it to its latest version. Once all (or most) consumers have been upgraded to 0.11.0 or later, change log.message.format.version to 3.5 on each broker and restart them one by one. Note that the older Scala clients, which are no longer maintained, do not support the message format introduced in 0.11, so to avoid conversion costs (or to take advantage of exactly once semantics), the newer Java clients must be used. 



### Upgrading KRaft-based clusters

**If you are upgrading from a version prior to 3.3.0, please see the note in step 3 below. Once you have changed the metadata.version to the latest version, it will not be possible to downgrade to a version prior to 3.3-IV0.**

**For a rolling upgrade:**

  1. Upgrade the brokers one at a time: shut down the broker, update the code, and restart it. Once you have done so, the brokers will be running the latest version and you can verify that the cluster's behavior and performance meets expectations. 
  2. Once the cluster's behavior and performance has been verified, bump the metadata.version by running ` ./bin/kafka-features.sh upgrade --metadata 3.5 `
  3. Note that cluster metadata downgrade is not supported in this version since it has metadata changes. Every [MetadataVersion](https://github.com/apache/kafka/blob/trunk/server-common/src/main/java/org/apache/kafka/server/common/MetadataVersion.java) after 3.2.x has a boolean parameter that indicates if there are metadata changes (i.e. `IBP_3_3_IV3(7, "3.3", "IV3", true)` means this version has metadata changes). Given your current and target versions, a downgrade is only possible if there are no metadata changes in the versions between.



### Notable changes in 3.5.0

  * Kafka Streams has introduced a new state store type, versioned key-value stores, for storing multiple record versions per key, thereby enabling timestamped retrieval operations to return the latest record (per key) as of a specified timestamp. See [KIP-889](https://cwiki.apache.org/confluence/display/KAFKA/KIP-889%3A+Versioned+State+Stores) and [KIP-914](https://cwiki.apache.org/confluence/display/KAFKA/KIP-914%3A+DSL+Processor+Semantics+for+Versioned+Stores) for more details. If the new store typed is used in the DSL, improved processing semantics are applied as described in [KIP-914](https://cwiki.apache.org/confluence/display/KAFKA/KIP-914%3A+DSL+Processor+Semantics+for+Versioned+Stores). 
  * KTable aggregation semantics got further improved via [KIP-904](https://cwiki.apache.org/confluence/display/KAFKA/KIP-904%3A+Kafka+Streams+-+Guarantee+subtractor+is+called+before+adder+if+key+has+not+changed), now avoiding spurious intermediate results. 
  * Kafka Streams' `ProductionExceptionHandler` is improved via [KIP-399](https://cwiki.apache.org/confluence/display/KAFKA/KIP-399%3A+Extend+ProductionExceptionHandler+to+cover+serialization+exceptions), now also covering serialization errors. 
  * MirrorMaker now uses incrementalAlterConfigs API by default to synchronize topic configurations instead of the deprecated alterConfigs API. A new settings called `use.incremental.alter.configs` is introduced to allow users to control which API to use. This new setting is marked deprecated and will be removed in the next major release when incrementalAlterConfigs API is always used. See [KIP-894](https://cwiki.apache.org/confluence/display/KAFKA/KIP-894%3A+Use+incrementalAlterConfigs+API+for+syncing+topic+configurations) for more details. 
  * The JmxTool, EndToEndLatency, StreamsResetter, ConsumerPerformance and ClusterTool have been migrated to the tools module. The 'kafka.tools' package is deprecated and will change to 'org.apache.kafka.tools' in the next major release. See [KAFKA-14525](https://issues.apache.org/jira/browse/KAFKA-14525) for more details. 



## Upgrading to 3.4.0 from any version 0.8.x through 3.3.x

**If you are upgrading from a version prior to 2.1.x, please see the note below about the change to the schema used to store consumer offsets. Once you have changed the inter.broker.protocol.version to the latest version, it will not be possible to downgrade to a version prior to 2.1.**

**For a rolling upgrade:**

  1. Update server.properties on all brokers and add the following properties. CURRENT_KAFKA_VERSION refers to the version you are upgrading from. CURRENT_MESSAGE_FORMAT_VERSION refers to the message format version currently in use. If you have previously overridden the message format version, you should keep its current value. Alternatively, if you are upgrading from a version prior to 0.11.0.x, then CURRENT_MESSAGE_FORMAT_VERSION should be set to match CURRENT_KAFKA_VERSION. 
     * inter.broker.protocol.version=CURRENT_KAFKA_VERSION (e.g. `3.3`, `3.2`, etc.)
     * log.message.format.version=CURRENT_MESSAGE_FORMAT_VERSION (See potential performance impact following the upgrade for the details on what this configuration does.)
If you are upgrading from version 0.11.0.x or above, and you have not overridden the message format, then you only need to override the inter-broker protocol version. 
     * inter.broker.protocol.version=CURRENT_KAFKA_VERSION (e.g. `3.3`, `3.2`, etc.)
  2. Upgrade the brokers one at a time: shut down the broker, update the code, and restart it. Once you have done so, the brokers will be running the latest version and you can verify that the cluster's behavior and performance meets expectations. It is still possible to downgrade at this point if there are any problems. 
  3. Once the cluster's behavior and performance has been verified, bump the protocol version by editing `inter.broker.protocol.version` and setting it to `3.4`. 
  4. Restart the brokers one by one for the new protocol version to take effect. Once the brokers begin using the latest protocol version, it will no longer be possible to downgrade the cluster to an older version. 
  5. If you have overridden the message format version as instructed above, then you need to do one more rolling restart to upgrade it to its latest version. Once all (or most) consumers have been upgraded to 0.11.0 or later, change log.message.format.version to 3.4 on each broker and restart them one by one. Note that the older Scala clients, which are no longer maintained, do not support the message format introduced in 0.11, so to avoid conversion costs (or to take advantage of exactly once semantics), the newer Java clients must be used. 



## Upgrading a KRaft-based cluster to 3.4.0 from any version 3.0.x through 3.3.x

**If you are upgrading from a version prior to 3.3.0, please see the note below. Once you have changed the metadata.version to the latest version, it will not be possible to downgrade to a version prior to 3.3-IV0.**

**For a rolling upgrade:**

  1. Upgrade the brokers one at a time: shut down the broker, update the code, and restart it. Once you have done so, the brokers will be running the latest version and you can verify that the cluster's behavior and performance meets expectations. 
  2. Once the cluster's behavior and performance has been verified, bump the metadata.version by running ` ./bin/kafka-features.sh upgrade --metadata 3.4 `
  3. Note that cluster metadata downgrade is not supported in this version since it has metadata changes. Every [MetadataVersion](https://github.com/apache/kafka/blob/trunk/server-common/src/main/java/org/apache/kafka/server/common/MetadataVersion.java) after 3.2.x has a boolean parameter that indicates if there are metadata changes (i.e. `IBP_3_3_IV3(7, "3.3", "IV3", true)` means this version has metadata changes). Given your current and target versions, a downgrade is only possible if there are no metadata changes in the versions between.



### Notable changes in 3.4.0

  * Since Apache Kafka 3.4.0, we have added a system property ("org.apache.kafka.disallowed.login.modules") to disable the problematic login modules usage in SASL JAAS configuration. Also by default "com.sun.security.auth.module.JndiLoginModule" is disabled from Apache Kafka 3.4.0. 



## Upgrading to 3.3.1 from any version 0.8.x through 3.2.x

**If you are upgrading from a version prior to 2.1.x, please see the note below about the change to the schema used to store consumer offsets. Once you have changed the inter.broker.protocol.version to the latest version, it will not be possible to downgrade to a version prior to 2.1.**

**For a rolling upgrade:**

  1. Update server.properties on all brokers and add the following properties. CURRENT_KAFKA_VERSION refers to the version you are upgrading from. CURRENT_MESSAGE_FORMAT_VERSION refers to the message format version currently in use. If you have previously overridden the message format version, you should keep its current value. Alternatively, if you are upgrading from a version prior to 0.11.0.x, then CURRENT_MESSAGE_FORMAT_VERSION should be set to match CURRENT_KAFKA_VERSION. 
     * inter.broker.protocol.version=CURRENT_KAFKA_VERSION (e.g. `3.2`, `3.1`, etc.)
     * log.message.format.version=CURRENT_MESSAGE_FORMAT_VERSION (See potential performance impact following the upgrade for the details on what this configuration does.)
If you are upgrading from version 0.11.0.x or above, and you have not overridden the message format, then you only need to override the inter-broker protocol version. 
     * inter.broker.protocol.version=CURRENT_KAFKA_VERSION (e.g. `3.2`, `3.1`, etc.)
  2. Upgrade the brokers one at a time: shut down the broker, update the code, and restart it. Once you have done so, the brokers will be running the latest version and you can verify that the cluster's behavior and performance meets expectations. It is still possible to downgrade at this point if there are any problems. 
  3. Once the cluster's behavior and performance has been verified, bump the protocol version by editing `inter.broker.protocol.version` and setting it to `3.3`. 
  4. Restart the brokers one by one for the new protocol version to take effect. Once the brokers begin using the latest protocol version, it will no longer be possible to downgrade the cluster to an older version. 
  5. If you have overridden the message format version as instructed above, then you need to do one more rolling restart to upgrade it to its latest version. Once all (or most) consumers have been upgraded to 0.11.0 or later, change log.message.format.version to 3.3 on each broker and restart them one by one. Note that the older Scala clients, which are no longer maintained, do not support the message format introduced in 0.11, so to avoid conversion costs (or to take advantage of exactly once semantics), the newer Java clients must be used. 



## Upgrading a KRaft-based cluster to 3.3.1 from any version 3.0.x through 3.2.x

**If you are upgrading from a version prior to 3.3.1, please see the note below. Once you have changed the metadata.version to the latest version, it will not be possible to downgrade to a version prior to 3.3-IV0.**

**For a rolling upgrade:**

  1. Upgrade the brokers one at a time: shut down the broker, update the code, and restart it. Once you have done so, the brokers will be running the latest version and you can verify that the cluster's behavior and performance meets expectations. 
  2. Once the cluster's behavior and performance has been verified, bump the metadata.version by running ` ./bin/kafka-features.sh upgrade --metadata 3.3 `
  3. Note that cluster metadata downgrade is not supported in this version since it has metadata changes. Every [MetadataVersion](https://github.com/apache/kafka/blob/trunk/server-common/src/main/java/org/apache/kafka/server/common/MetadataVersion.java) after 3.2.x has a boolean parameter that indicates if there are metadata changes (i.e. `IBP_3_3_IV3(7, "3.3", "IV3", true)` means this version has metadata changes). Given your current and target versions, a downgrade is only possible if there are no metadata changes in the versions between.



### Notable changes in 3.3.1

  * KRaft mode is production ready for new clusters. See [KIP-833](https://cwiki.apache.org/confluence/display/KAFKA/KIP-833%3A+Mark+KRaft+as+Production+Ready) for more details (including limitations). 
  * The partitioner used by default for records with no keys has been improved to avoid pathological behavior when one or more brokers are slow. The new logic may affect the batching behavior, which can be tuned using the `batch.size` and/or `linger.ms` configuration settings. The previous behavior can be restored by setting `partitioner.class=org.apache.kafka.clients.producer.internals.DefaultPartitioner`. See [KIP-794](https://cwiki.apache.org/confluence/display/KAFKA/KIP-794%3A+Strictly+Uniform+Sticky+Partitioner) for more details. 
  * There is now a slightly different upgrade process for KRaft clusters than for ZK-based clusters, as described above.
  * Introduced a new API `addMetricIfAbsent` to `Metrics` which would create a new Metric if not existing or return the same metric if already registered. Note that this behaviour is different from `addMetric` API which throws an `IllegalArgumentException` when trying to create an already existing metric. (See [KIP-843](https://cwiki.apache.org/confluence/display/KAFKA/KIP-843%3A+Adding+addMetricIfAbsent+method+to+Metrics) for more details). 



## Upgrading to 3.2.0 from any version 0.8.x through 3.1.x

**If you are upgrading from a version prior to 2.1.x, please see the note below about the change to the schema used to store consumer offsets. Once you have changed the inter.broker.protocol.version to the latest version, it will not be possible to downgrade to a version prior to 2.1.**

**For a rolling upgrade:**

  1. Update server.properties on all brokers and add the following properties. CURRENT_KAFKA_VERSION refers to the version you are upgrading from. CURRENT_MESSAGE_FORMAT_VERSION refers to the message format version currently in use. If you have previously overridden the message format version, you should keep its current value. Alternatively, if you are upgrading from a version prior to 0.11.0.x, then CURRENT_MESSAGE_FORMAT_VERSION should be set to match CURRENT_KAFKA_VERSION. 
     * inter.broker.protocol.version=CURRENT_KAFKA_VERSION (e.g. `3.1`, `3.0`, etc.)
     * log.message.format.version=CURRENT_MESSAGE_FORMAT_VERSION (See potential performance impact following the upgrade for the details on what this configuration does.)
If you are upgrading from version 0.11.0.x or above, and you have not overridden the message format, then you only need to override the inter-broker protocol version. 
     * inter.broker.protocol.version=CURRENT_KAFKA_VERSION (e.g. `3.1`, `3.0`, etc.)
  2. Upgrade the brokers one at a time: shut down the broker, update the code, and restart it. Once you have done so, the brokers will be running the latest version and you can verify that the cluster's behavior and performance meets expectations. It is still possible to downgrade at this point if there are any problems. 
  3. Once the cluster's behavior and performance has been verified, bump the protocol version by editing `inter.broker.protocol.version` and setting it to `3.2`. 
  4. Restart the brokers one by one for the new protocol version to take effect. Once the brokers begin using the latest protocol version, it will no longer be possible to downgrade the cluster to an older version. 
  5. If you have overridden the message format version as instructed above, then you need to do one more rolling restart to upgrade it to its latest version. Once all (or most) consumers have been upgraded to 0.11.0 or later, change log.message.format.version to 3.2 on each broker and restart them one by one. Note that the older Scala clients, which are no longer maintained, do not support the message format introduced in 0.11, so to avoid conversion costs (or to take advantage of exactly once semantics), the newer Java clients must be used. 



### Notable changes in 3.2.0

  * Idempotence for the producer is enabled by default if no conflicting configurations are set. When producing to brokers older than 2.8.0, the `IDEMPOTENT_WRITE` permission is required. Check the compatibility section of [KIP-679](https://cwiki.apache.org/confluence/display/KAFKA/KIP-679%3A+Producer+will+enable+the+strongest+delivery+guarantee+by+default#KIP679:Producerwillenablethestrongestdeliveryguaranteebydefault-Compatibility,Deprecation,andMigrationPlan) for details. In 3.0.0 and 3.1.0, a bug prevented this default from being applied, which meant that idempotence remained disabled unless the user had explicitly set `enable.idempotence` to true (See [KAFKA-13598](https://issues.apache.org/jira/browse/KAFKA-13598) for more details). This issue was fixed and the default is properly applied in 3.0.1, 3.1.1, and 3.2.0.
  * A notable exception is Connect that by default disables idempotent behavior for all of its producers in order to uniformly support using a wide range of Kafka broker versions. Users can change this behavior to enable idempotence for some or all producers via Connect worker and/or connector configuration. Connect may enable idempotent producers by default in a future major release.
  * Kafka has replaced log4j with reload4j due to security concerns. This only affects modules that specify a logging backend (`connect-runtime` and `kafka-tools` are two such examples). A number of modules, including `kafka-clients`, leave it to the application to specify the logging backend. More information can be found at [reload4j](https://reload4j.qos.ch). Projects that depend on the affected modules from the Kafka project should use [slf4j-log4j12 version 1.7.35 or above](https://www.slf4j.org/manual.html#swapping) or slf4j-reload4j to avoid [possible compatibility issues originating from the logging framework](https://www.slf4j.org/codes.html#no_tlm).
  * The example connectors, `FileStreamSourceConnector` and `FileStreamSinkConnector`, have been removed from the default classpath. To use them in Kafka Connect standalone or distributed mode they need to be explicitly added, for example `CLASSPATH=./libs/connect-file-3.2.0.jar ./bin/connect-distributed.sh`.



## Upgrading to 3.1.0 from any version 0.8.x through 3.0.x

**If you are upgrading from a version prior to 2.1.x, please see the note below about the change to the schema used to store consumer offsets. Once you have changed the inter.broker.protocol.version to the latest version, it will not be possible to downgrade to a version prior to 2.1.**

**For a rolling upgrade:**

  1. Update server.properties on all brokers and add the following properties. CURRENT_KAFKA_VERSION refers to the version you are upgrading from. CURRENT_MESSAGE_FORMAT_VERSION refers to the message format version currently in use. If you have previously overridden the message format version, you should keep its current value. Alternatively, if you are upgrading from a version prior to 0.11.0.x, then CURRENT_MESSAGE_FORMAT_VERSION should be set to match CURRENT_KAFKA_VERSION. 
     * inter.broker.protocol.version=CURRENT_KAFKA_VERSION (e.g. `3.0`, `2.8`, etc.)
     * log.message.format.version=CURRENT_MESSAGE_FORMAT_VERSION (See potential performance impact following the upgrade for the details on what this configuration does.)
If you are upgrading from version 0.11.0.x or above, and you have not overridden the message format, then you only need to override the inter-broker protocol version. 
     * inter.broker.protocol.version=CURRENT_KAFKA_VERSION (e.g. `3.0`, `2.8`, etc.)
  2. Upgrade the brokers one at a time: shut down the broker, update the code, and restart it. Once you have done so, the brokers will be running the latest version and you can verify that the cluster's behavior and performance meets expectations. It is still possible to downgrade at this point if there are any problems. 
  3. Once the cluster's behavior and performance has been verified, bump the protocol version by editing `inter.broker.protocol.version` and setting it to `3.1`. 
  4. Restart the brokers one by one for the new protocol version to take effect. Once the brokers begin using the latest protocol version, it will no longer be possible to downgrade the cluster to an older version. 
  5. If you have overridden the message format version as instructed above, then you need to do one more rolling restart to upgrade it to its latest version. Once all (or most) consumers have been upgraded to 0.11.0 or later, change log.message.format.version to 3.1 on each broker and restart them one by one. Note that the older Scala clients, which are no longer maintained, do not support the message format introduced in 0.11, so to avoid conversion costs (or to take advantage of exactly once semantics), the newer Java clients must be used. 



### Notable changes in 3.1.1

  * Idempotence for the producer is enabled by default if no conflicting configurations are set. When producing to brokers older than 2.8.0, the `IDEMPOTENT_WRITE` permission is required. Check the compatibility section of [KIP-679](https://cwiki.apache.org/confluence/display/KAFKA/KIP-679%3A+Producer+will+enable+the+strongest+delivery+guarantee+by+default#KIP679:Producerwillenablethestrongestdeliveryguaranteebydefault-Compatibility,Deprecation,andMigrationPlan) for details. A bug prevented the producer idempotence default from being applied which meant that it remained disabled unless the user had explicitly set `enable.idempotence` to true. See [KAFKA-13598](https://issues.apache.org/jira/browse/KAFKA-13598) for more details. This issue was fixed and the default is properly applied.
  * A notable exception is Connect that by default disables idempotent behavior for all of its producers in order to uniformly support using a wide range of Kafka broker versions. Users can change this behavior to enable idempotence for some or all producers via Connect worker and/or connector configuration. Connect may enable idempotent producers by default in a future major release.
  * Kafka has replaced log4j with reload4j due to security concerns. This only affects modules that specify a logging backend (`connect-runtime` and `kafka-tools` are two such examples). A number of modules, including `kafka-clients`, leave it to the application to specify the logging backend. More information can be found at [reload4j](https://reload4j.qos.ch). Projects that depend on the affected modules from the Kafka project should use [slf4j-log4j12 version 1.7.35 or above](https://www.slf4j.org/manual.html#swapping) or slf4j-reload4j to avoid [possible compatibility issues originating from the logging framework](https://www.slf4j.org/codes.html#no_tlm).



### Notable changes in 3.1.0

  * Apache Kafka supports Java 17.
  * The following metrics have been deprecated: `bufferpool-wait-time-total`, `io-waittime-total`, and `iotime-total`. Please use `bufferpool-wait-time-ns-total`, `io-wait-time-ns-total`, and `io-time-ns-total` instead. See [KIP-773](https://cwiki.apache.org/confluence/display/KAFKA/KIP-773%3A+Differentiate+consistently+metric+latency+measured+in+millis+and+nanos) for more details.
  * IBP 3.1 introduces topic IDs to FetchRequest as a part of [KIP-516](https://cwiki.apache.org/confluence/display/KAFKA/KIP-516%3A+Topic+Identifiers).



## Upgrading to 3.0.1 from any version 0.8.x through 2.8.x

**If you are upgrading from a version prior to 2.1.x, please see the note below about the change to the schema used to store consumer offsets. Once you have changed the inter.broker.protocol.version to the latest version, it will not be possible to downgrade to a version prior to 2.1.**

**For a rolling upgrade:**

  1. Update server.properties on all brokers and add the following properties. CURRENT_KAFKA_VERSION refers to the version you are upgrading from. CURRENT_MESSAGE_FORMAT_VERSION refers to the message format version currently in use. If you have previously overridden the message format version, you should keep its current value. Alternatively, if you are upgrading from a version prior to 0.11.0.x, then CURRENT_MESSAGE_FORMAT_VERSION should be set to match CURRENT_KAFKA_VERSION. 
     * inter.broker.protocol.version=CURRENT_KAFKA_VERSION (e.g. `2.8`, `2.7`, etc.)
     * log.message.format.version=CURRENT_MESSAGE_FORMAT_VERSION (See potential performance impact following the upgrade for the details on what this configuration does.)
If you are upgrading from version 0.11.0.x or above, and you have not overridden the message format, then you only need to override the inter-broker protocol version. 
     * inter.broker.protocol.version=CURRENT_KAFKA_VERSION (e.g. `2.8`, `2.7`, etc.)
  2. Upgrade the brokers one at a time: shut down the broker, update the code, and restart it. Once you have done so, the brokers will be running the latest version and you can verify that the cluster's behavior and performance meets expectations. It is still possible to downgrade at this point if there are any problems. 
  3. Once the cluster's behavior and performance has been verified, bump the protocol version by editing `inter.broker.protocol.version` and setting it to `3.0`. 
  4. Restart the brokers one by one for the new protocol version to take effect. Once the brokers begin using the latest protocol version, it will no longer be possible to downgrade the cluster to an older version. 
  5. If you have overridden the message format version as instructed above, then you need to do one more rolling restart to upgrade it to its latest version. Once all (or most) consumers have been upgraded to 0.11.0 or later, change log.message.format.version to 3.0 on each broker and restart them one by one. Note that the older Scala clients, which are no longer maintained, do not support the message format introduced in 0.11, so to avoid conversion costs (or to take advantage of exactly once semantics), the newer Java clients must be used. 



### Notable changes in 3.0.1

  * Idempotence for the producer is enabled by default if no conflicting configurations are set. When producing to brokers older than 2.8.0, the `IDEMPOTENT_WRITE` permission is required. Check the compatibility section of [KIP-679](https://cwiki.apache.org/confluence/display/KAFKA/KIP-679%3A+Producer+will+enable+the+strongest+delivery+guarantee+by+default#KIP679:Producerwillenablethestrongestdeliveryguaranteebydefault-Compatibility,Deprecation,andMigrationPlan) for details. A bug prevented the producer idempotence default from being applied which meant that it remained disabled unless the user had explicitly set `enable.idempotence` to true. See [KAFKA-13598](https://issues.apache.org/jira/browse/KAFKA-13598) for more details. This issue was fixed and the default is properly applied.



### Notable changes in 3.0.0

  * The producer has stronger delivery guarantees by default: `idempotence` is enabled and `acks` is set to `all` instead of `1`. See [KIP-679](https://cwiki.apache.org/confluence/display/KAFKA/KIP-679%3A+Producer+will+enable+the+strongest+delivery+guarantee+by+default) for details. In 3.0.0 and 3.1.0, a bug prevented the idempotence default from being applied which meant that it remained disabled unless the user had explicitly set `enable.idempotence` to true. Note that the bug did not affect the `acks=all` change. See [KAFKA-13598](https://issues.apache.org/jira/browse/KAFKA-13598) for more details. This issue was fixed and the default is properly applied in 3.0.1, 3.1.1, and 3.2.0.
  * Java 8 and Scala 2.12 support have been deprecated since Apache Kafka 3.0 and will be removed in Apache Kafka 4.0. See [KIP-750](https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=181308223) and [KIP-751](https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=181308218) for more details.
  * ZooKeeper has been upgraded to version 3.6.3.
  * A preview of KRaft mode is available, though upgrading to it from the 2.8 Early Access release is not possible. See the KRaft section for details.
  * The release tarball no longer includes test, sources, javadoc and test sources jars. These are still published to the Maven Central repository. 
  * A number of implementation dependency jars are [now available in the runtime classpath instead of compile and runtime classpaths](https://github.com/apache/kafka/pull/10203). Compilation errors after the upgrade can be fixed by adding the missing dependency jar(s) explicitly or updating the application not to use internal classes.
  * The default value for the consumer configuration `session.timeout.ms` was increased from 10s to 45s. See [KIP-735](https://cwiki.apache.org/confluence/display/KAFKA/KIP-735%3A+Increase+default+consumer+session+timeout) for more details.
  * The broker configuration `log.message.format.version` and topic configuration `message.format.version` have been deprecated. The value of both configurations is always assumed to be `3.0` if `inter.broker.protocol.version` is `3.0` or higher. If `log.message.format.version` or `message.format.version` are set, we recommend clearing them at the same time as the `inter.broker.protocol.version` upgrade to 3.0. This will avoid potential compatibility issues if the `inter.broker.protocol.version` is downgraded. See [KIP-724](https://cwiki.apache.org/confluence/display/KAFKA/KIP-724%3A+Drop+support+for+message+formats+v0+and+v1) for more details.
  * The Streams API removed all deprecated APIs that were deprecated in version 2.5.0 or earlier. For a complete list of removed APIs compare the detailed Kafka Streams upgrade notes.
  * Kafka Streams no longer has a compile time dependency on "connect:json" module ([KAFKA-5146](https://issues.apache.org/jira/browse/KAFKA-5146)). Projects that were relying on this transitive dependency will have to explicitly declare it.
  * Custom principal builder implementations specified through `principal.builder.class` must now implement the `KafkaPrincipalSerde` interface to allow for forwarding between brokers. See [KIP-590](https://cwiki.apache.org/confluence/display/KAFKA/KIP-590%3A+Redirect+Zookeeper+Mutation+Protocols+to+The+Controller) for more details about the usage of KafkaPrincipalSerde.
  * A number of deprecated classes, methods and tools have been removed from the `clients`, `connect`, `core` and `tools` modules:
    * The Scala `Authorizer`, `SimpleAclAuthorizer` and related classes have been removed. Please use the Java `Authorizer` and `AclAuthorizer` instead.
    * The `Metric#value()` method was removed ([KAFKA-12573](https://issues.apache.org/jira/browse/KAFKA-12573)).
    * The `Sum` and `Total` classes were removed ([KAFKA-12584](https://issues.apache.org/jira/browse/KAFKA-12584)). Please use `WindowedSum` and `CumulativeSum` instead.
    * The `Count` and `SampledTotal` classes were removed. Please use `WindowedCount` and `WindowedSum` respectively instead.
    * The `PrincipalBuilder`, `DefaultPrincipalBuilder` and `ResourceFilter` classes were removed. 
    * Various constants and constructors were removed from `SslConfigs`, `SaslConfigs`, `AclBinding` and `AclBindingFilter`.
    * The `Admin.electedPreferredLeaders()` methods were removed. Please use `Admin.electLeaders` instead.
    * The `kafka-preferred-replica-election` command line tool was removed. Please use `kafka-leader-election` instead.
    * The `--zookeeper` option was removed from the `kafka-topics` and `kafka-reassign-partitions` command line tools. Please use `--bootstrap-server` instead.
    * In the `kafka-configs` command line tool, the `--zookeeper` option is only supported for updating SCRAM Credentials configuration and describing/updating dynamic broker configs when brokers are not running. Please use `--bootstrap-server` for other configuration operations.
    * The `ConfigEntry` constructor was removed ([KAFKA-12577](https://issues.apache.org/jira/browse/KAFKA-12577)). Please use the remaining public constructor instead.
    * The config value `default` for the client config `client.dns.lookup` has been removed. In the unlikely event that you set this config explicitly, we recommend leaving the config unset (`use_all_dns_ips` is used by default).
    * The `ExtendedDeserializer` and `ExtendedSerializer` classes have been removed. Please use `Deserializer` and `Serializer` instead.
    * The `close(long, TimeUnit)` method was removed from the producer, consumer and admin client. Please use `close(Duration)`.
    * The `ConsumerConfig.addDeserializerToConfig` and `ProducerConfig.addSerializerToConfig` methods were removed. These methods were not intended to be public API and there is no replacement.
    * The `NoOffsetForPartitionException.partition()` method was removed. Please use `partitions()` instead.
    * The default `partition.assignment.strategy` is changed to "[RangeAssignor, CooperativeStickyAssignor]", which will use the RangeAssignor by default, but allows upgrading to the CooperativeStickyAssignor with just a single rolling bounce that removes the RangeAssignor from the list. Please check the client upgrade path guide [here](https://cwiki.apache.org/confluence/display/KAFKA/KIP-429:+Kafka+Consumer+Incremental+Rebalance+Protocol#KIP429:KafkaConsumerIncrementalRebalanceProtocol-Consumer) for more detail.
    * The Scala `kafka.common.MessageFormatter` was removed. Please use the Java `org.apache.kafka.common.MessageFormatter`.
    * The `MessageFormatter.init(Properties)` method was removed. Please use `configure(Map)` instead.
    * The `checksum()` method has been removed from `ConsumerRecord` and `RecordMetadata`. The message format v2, which has been the default since 0.11, moved the checksum from the record to the record batch. As such, these methods don't make sense and no replacements exist.
    * The `ChecksumMessageFormatter` class was removed. It is not part of the public API, but it may have been used with `kafka-console-consumer.sh`. It reported the checksum of each record, which has not been supported since message format v2.
    * The `org.apache.kafka.clients.consumer.internals.PartitionAssignor` class has been removed. Please use `org.apache.kafka.clients.consumer.ConsumerPartitionAssignor` instead.
    * The `quota.producer.default` and `quota.consumer.default` configurations were removed ([KAFKA-12591](https://issues.apache.org/jira/browse/KAFKA-12591)). Dynamic quota defaults must be used instead.
    * The `port` and `host.name` configurations were removed. Please use `listeners` instead.
    * The `advertised.port` and `advertised.host.name` configurations were removed. Please use `advertised.listeners` instead.
    * The deprecated worker configurations `rest.host.name` and `rest.port` were removed ([KAFKA-12482](https://issues.apache.org/jira/browse/KAFKA-12482)) from the Kafka Connect worker configuration. Please use `listeners` instead.
  * The `Producer#sendOffsetsToTransaction(Map offsets, String consumerGroupId)` method has been deprecated. Please use `Producer#sendOffsetsToTransaction(Map offsets, ConsumerGroupMetadata metadata)` instead, where the `ConsumerGroupMetadata` can be retrieved via `KafkaConsumer#groupMetadata()` for stronger semantics. Note that the full set of consumer group metadata is only understood by brokers or version 2.5 or higher, so you must upgrade your kafka cluster to get the stronger semantics. Otherwise, you can just pass in `new ConsumerGroupMetadata(consumerGroupId)` to work with older brokers. See [KIP-732](https://cwiki.apache.org/confluence/x/zJONCg) for more details. 
  * The Connect `internal.key.converter` and `internal.value.converter` properties have been completely [removed](https://cwiki.apache.org/confluence/x/2YDOCg). The use of these Connect worker properties has been deprecated since version 2.0.0. Workers are now hardcoded to use the JSON converter with `schemas.enable` set to `false`. If your cluster has been using a different internal key or value converter, you can follow the migration steps outlined in [KIP-738](https://cwiki.apache.org/confluence/x/2YDOCg) to safely upgrade your Connect cluster to 3.0. 
  * The Connect-based MirrorMaker (MM2) includes changes to support `IdentityReplicationPolicy`, enabling replication without renaming topics. The existing `DefaultReplicationPolicy` is still used by default, but identity replication can be enabled via the `replication.policy` configuration property. This is especially useful for users migrating from the older MirrorMaker (MM1), or for use-cases with simple one-way replication topologies where topic renaming is undesirable. Note that `IdentityReplicationPolicy`, unlike `DefaultReplicationPolicy`, cannot prevent replication cycles based on topic names, so take care to avoid cycles when constructing your replication topology. 
  * The original MirrorMaker (MM1) and related classes have been deprecated. Please use the Connect-based MirrorMaker (MM2), as described in the [Geo-Replication section](/37/documentation/#georeplication). 



## Upgrading to 2.8.1 from any version 0.8.x through 2.7.x

**If you are upgrading from a version prior to 2.1.x, please see the note below about the change to the schema used to store consumer offsets. Once you have changed the inter.broker.protocol.version to the latest version, it will not be possible to downgrade to a version prior to 2.1.**

**For a rolling upgrade:**

  1. Update server.properties on all brokers and add the following properties. CURRENT_KAFKA_VERSION refers to the version you are upgrading from. CURRENT_MESSAGE_FORMAT_VERSION refers to the message format version currently in use. If you have previously overridden the message format version, you should keep its current value. Alternatively, if you are upgrading from a version prior to 0.11.0.x, then CURRENT_MESSAGE_FORMAT_VERSION should be set to match CURRENT_KAFKA_VERSION. 
     * inter.broker.protocol.version=CURRENT_KAFKA_VERSION (e.g. `2.7`, `2.6`, etc.)
     * log.message.format.version=CURRENT_MESSAGE_FORMAT_VERSION (See potential performance impact following the upgrade for the details on what this configuration does.)
If you are upgrading from version 0.11.0.x or above, and you have not overridden the message format, then you only need to override the inter-broker protocol version. 
     * inter.broker.protocol.version=CURRENT_KAFKA_VERSION (e.g. `2.7`, `2.6`, etc.)
  2. Upgrade the brokers one at a time: shut down the broker, update the code, and restart it. Once you have done so, the brokers will be running the latest version and you can verify that the cluster's behavior and performance meets expectations. It is still possible to downgrade at this point if there are any problems. 
  3. Once the cluster's behavior and performance has been verified, bump the protocol version by editing `inter.broker.protocol.version` and setting it to `2.8`. 
  4. Restart the brokers one by one for the new protocol version to take effect. Once the brokers begin using the latest protocol version, it will no longer be possible to downgrade the cluster to an older version. 
  5. If you have overridden the message format version as instructed above, then you need to do one more rolling restart to upgrade it to its latest version. Once all (or most) consumers have been upgraded to 0.11.0 or later, change log.message.format.version to 2.8 on each broker and restart them one by one. Note that the older Scala clients, which are no longer maintained, do not support the message format introduced in 0.11, so to avoid conversion costs (or to take advantage of exactly once semantics), the newer Java clients must be used. 



### Notable changes in 2.8.0

  * The 2.8.0 release added a new method to the Authorizer Interface introduced in [KIP-679](https://cwiki.apache.org/confluence/display/KAFKA/KIP-679%3A+Producer+will+enable+the+strongest+delivery+guarantee+by+default). The motivation is to unblock our future plan to enable the strongest message delivery guarantee by default. Custom authorizer should consider providing a more efficient implementation that supports audit logging and any custom configs or access rules. 
  * IBP 2.8 introduces topic IDs to topics as a part of [KIP-516](https://cwiki.apache.org/confluence/display/KAFKA/KIP-516%3A+Topic+Identifiers). When using ZooKeeper, this information is stored in the TopicZNode. If the cluster is downgraded to a previous IBP or version, future topics will not get topic IDs and it is not guaranteed that topics will retain their topic IDs in ZooKeeper. This means that upon upgrading again, some topics or all topics will be assigned new IDs. 
  * Kafka Streams introduce a type-safe `split()` operator as a substitution for deprecated `KStream#branch()` method (cf. [KIP-418](https://cwiki.apache.org/confluence/display/KAFKA/KIP-418%3A+A+method-chaining+way+to+branch+KStream)). 



## Upgrading to 2.7.0 from any version 0.8.x through 2.6.x

**If you are upgrading from a version prior to 2.1.x, please see the note below about the change to the schema used to store consumer offsets. Once you have changed the inter.broker.protocol.version to the latest version, it will not be possible to downgrade to a version prior to 2.1.**

**For a rolling upgrade:**

  1. Update server.properties on all brokers and add the following properties. CURRENT_KAFKA_VERSION refers to the version you are upgrading from. CURRENT_MESSAGE_FORMAT_VERSION refers to the message format version currently in use. If you have previously overridden the message format version, you should keep its current value. Alternatively, if you are upgrading from a version prior to 0.11.0.x, then CURRENT_MESSAGE_FORMAT_VERSION should be set to match CURRENT_KAFKA_VERSION. 
     * inter.broker.protocol.version=CURRENT_KAFKA_VERSION (e.g. `2.6`, `2.5`, etc.)
     * log.message.format.version=CURRENT_MESSAGE_FORMAT_VERSION (See potential performance impact following the upgrade for the details on what this configuration does.)
If you are upgrading from version 0.11.0.x or above, and you have not overridden the message format, then you only need to override the inter-broker protocol version. 
     * inter.broker.protocol.version=CURRENT_KAFKA_VERSION (e.g. `2.6`, `2.5`, etc.)
  2. Upgrade the brokers one at a time: shut down the broker, update the code, and restart it. Once you have done so, the brokers will be running the latest version and you can verify that the cluster's behavior and performance meets expectations. It is still possible to downgrade at this point if there are any problems. 
  3. Once the cluster's behavior and performance has been verified, bump the protocol version by editing `inter.broker.protocol.version` and setting it to `2.7`. 
  4. Restart the brokers one by one for the new protocol version to take effect. Once the brokers begin using the latest protocol version, it will no longer be possible to downgrade the cluster to an older version. 
  5. If you have overridden the message format version as instructed above, then you need to do one more rolling restart to upgrade it to its latest version. Once all (or most) consumers have been upgraded to 0.11.0 or later, change log.message.format.version to 2.7 on each broker and restart them one by one. Note that the older Scala clients, which are no longer maintained, do not support the message format introduced in 0.11, so to avoid conversion costs (or to take advantage of exactly once semantics), the newer Java clients must be used. 



### Notable changes in 2.7.0

  * The 2.7.0 release includes the core Raft implementation specified in [KIP-595](https://cwiki.apache.org/confluence/display/KAFKA/KIP-595%3A+A+Raft+Protocol+for+the+Metadata+Quorum). There is a separate "raft" module containing most of the logic. Until integration with the controller is complete, there is a standalone server that users can use for testing the performance of the Raft implementation. See the README.md in the raft module for details 
  * KIP-651 [adds support](https://cwiki.apache.org/confluence/display/KAFKA/KIP-651+-+Support+PEM+format+for+SSL+certificates+and+private+key) for using PEM files for key and trust stores. 
  * KIP-612 [adds support](https://cwiki.apache.org/confluence/display/KAFKA/KIP-612%3A+Ability+to+Limit+Connection+Creation+Rate+on+Brokers) for enforcing broker-wide and per-listener connection create rates. The 2.7.0 release contains the first part of KIP-612 with dynamic configuration coming in the 2.8.0 release. 
  * The ability to throttle topic and partition creations or topics deletions to prevent a cluster from being harmed via [KIP-599](https://cwiki.apache.org/confluence/display/KAFKA/KIP-599%3A+Throttle+Create+Topic%2C+Create+Partition+and+Delete+Topic+Operations)
  * When new features become available in Kafka there are two main issues: 
    1. How do Kafka clients become aware of broker capabilities?
    2. How does the broker decide which features to enable?
[KIP-584](https://cwiki.apache.org/confluence/display/KAFKA/KIP-584%3A+Versioning+scheme+for+features) provides a flexible and operationally easy solution for client discovery, feature gating and rolling upgrades using a single restart. 
  * The ability to print record offsets and headers with the `ConsoleConsumer` is now possible via [KIP-431](https://cwiki.apache.org/confluence/display/KAFKA/KIP-431%3A+Support+of+printing+additional+ConsumerRecord+fields+in+DefaultMessageFormatter)
  * The addition of [KIP-554](https://cwiki.apache.org/confluence/display/KAFKA/KIP-554%3A+Add+Broker-side+SCRAM+Config+API) continues progress towards the goal of Zookeeper removal from Kafka. The addition of KIP-554 means you don't have to connect directly to ZooKeeper anymore for managing SCRAM credentials. 
  * Altering non-reconfigurable configs of existent listeners causes `InvalidRequestException`. By contrast, the previous (unintended) behavior would have caused the updated configuration to be persisted, but it wouldn't take effect until the broker was restarted. See [KAFKA-10479](https://github.com/apache/kafka/pull/9284) for more discussion. See `DynamicBrokerConfig.DynamicSecurityConfigs` and `SocketServer.ListenerReconfigurableConfigs` for the supported reconfigurable configs of existent listeners. 
  * Kafka Streams adds support for [Sliding Windows Aggregations](https://cwiki.apache.org/confluence/display/KAFKA/KIP-450%3A+Sliding+Window+Aggregations+in+the+DSL) in the KStreams DSL. 
  * Reverse iteration over state stores enabling more efficient most recent update searches with [KIP-617](https://cwiki.apache.org/confluence/display/KAFKA/KIP-617%3A+Allow+Kafka+Streams+State+Stores+to+be+iterated+backwards)
  * End-to-End latency metrics in Kafka Steams see [KIP-613](https://cwiki.apache.org/confluence/display/KAFKA/KIP-613%3A+Add+end-to-end+latency+metrics+to+Streams) for more details 
  * Kafka Streams added metrics reporting default RocksDB properties with [KIP-607](https://cwiki.apache.org/confluence/display/KAFKA/KIP-607%3A+Add+Metrics+to+Kafka+Streams+to+Report+Properties+of+RocksDB)
  * Better Scala implicit Serdes support from [KIP-616](https://cwiki.apache.org/confluence/display/KAFKA/KIP-616%3A+Rename+implicit+Serdes+instances+in+kafka-streams-scala)



## Upgrading to 2.6.0 from any version 0.8.x through 2.5.x

**If you are upgrading from a version prior to 2.1.x, please see the note below about the change to the schema used to store consumer offsets. Once you have changed the inter.broker.protocol.version to the latest version, it will not be possible to downgrade to a version prior to 2.1.**

**For a rolling upgrade:**

  1. Update server.properties on all brokers and add the following properties. CURRENT_KAFKA_VERSION refers to the version you are upgrading from. CURRENT_MESSAGE_FORMAT_VERSION refers to the message format version currently in use. If you have previously overridden the message format version, you should keep its current value. Alternatively, if you are upgrading from a version prior to 0.11.0.x, then CURRENT_MESSAGE_FORMAT_VERSION should be set to match CURRENT_KAFKA_VERSION. 
     * inter.broker.protocol.version=CURRENT_KAFKA_VERSION (e.g. `2.5`, `2.4`, etc.)
     * log.message.format.version=CURRENT_MESSAGE_FORMAT_VERSION (See potential performance impact following the upgrade for the details on what this configuration does.)
If you are upgrading from version 0.11.0.x or above, and you have not overridden the message format, then you only need to override the inter-broker protocol version. 
     * inter.broker.protocol.version=CURRENT_KAFKA_VERSION (e.g. `2.5`, `2.4`, etc.)
  2. Upgrade the brokers one at a time: shut down the broker, update the code, and restart it. Once you have done so, the brokers will be running the latest version and you can verify that the cluster's behavior and performance meets expectations. It is still possible to downgrade at this point if there are any problems. 
  3. Once the cluster's behavior and performance has been verified, bump the protocol version by editing `inter.broker.protocol.version` and setting it to `2.6`. 
  4. Restart the brokers one by one for the new protocol version to take effect. Once the brokers begin using the latest protocol version, it will no longer be possible to downgrade the cluster to an older version. 
  5. If you have overridden the message format version as instructed above, then you need to do one more rolling restart to upgrade it to its latest version. Once all (or most) consumers have been upgraded to 0.11.0 or later, change log.message.format.version to 2.6 on each broker and restart them one by one. Note that the older Scala clients, which are no longer maintained, do not support the message format introduced in 0.11, so to avoid conversion costs (or to take advantage of exactly once semantics), the newer Java clients must be used. 



### Notable changes in 2.6.0

  * Kafka Streams adds a new processing mode (requires broker 2.5 or newer) that improves application scalability using exactly-once guarantees (cf. [KIP-447](https://cwiki.apache.org/confluence/display/KAFKA/KIP-447%3A+Producer+scalability+for+exactly+once+semantics)) 
  * TLSv1.3 has been enabled by default for Java 11 or newer. The client and server will negotiate TLSv1.3 if both support it and fallback to TLSv1.2 otherwise. See [KIP-573](https://cwiki.apache.org/confluence/display/KAFKA/KIP-573%3A+Enable+TLSv1.3+by+default) for more details. 
  * The default value for the `client.dns.lookup` configuration has been changed from `default` to `use_all_dns_ips`. If a hostname resolves to multiple IP addresses, clients and brokers will now attempt to connect to each IP in sequence until the connection is successfully established. See [KIP-602](https://cwiki.apache.org/confluence/display/KAFKA/KIP-602%3A+Change+default+value+for+client.dns.lookup) for more details. 
  * `NotLeaderForPartitionException` has been deprecated and replaced with `NotLeaderOrFollowerException`. Fetch requests and other requests intended only for the leader or follower return NOT_LEADER_OR_FOLLOWER(6) instead of REPLICA_NOT_AVAILABLE(9) if the broker is not a replica, ensuring that this transient error during reassignments is handled by all clients as a retriable exception. 



## Upgrading to 2.5.0 from any version 0.8.x through 2.4.x

**If you are upgrading from a version prior to 2.1.x, please see the note below about the change to the schema used to store consumer offsets. Once you have changed the inter.broker.protocol.version to the latest version, it will not be possible to downgrade to a version prior to 2.1.**

**For a rolling upgrade:**

  1. Update server.properties on all brokers and add the following properties. CURRENT_KAFKA_VERSION refers to the version you are upgrading from. CURRENT_MESSAGE_FORMAT_VERSION refers to the message format version currently in use. If you have previously overridden the message format version, you should keep its current value. Alternatively, if you are upgrading from a version prior to 0.11.0.x, then CURRENT_MESSAGE_FORMAT_VERSION should be set to match CURRENT_KAFKA_VERSION. 
     * inter.broker.protocol.version=CURRENT_KAFKA_VERSION (e.g. `2.4`, `2.3`, etc.)
     * log.message.format.version=CURRENT_MESSAGE_FORMAT_VERSION (See potential performance impact following the upgrade for the details on what this configuration does.)
If you are upgrading from version 0.11.0.x or above, and you have not overridden the message format, then you only need to override the inter-broker protocol version. 
     * inter.broker.protocol.version=CURRENT_KAFKA_VERSION (e.g. `2.4`, `2.3`, etc.)
  2. Upgrade the brokers one at a time: shut down the broker, update the code, and restart it. Once you have done so, the brokers will be running the latest version and you can verify that the cluster's behavior and performance meets expectations. It is still possible to downgrade at this point if there are any problems. 
  3. Once the cluster's behavior and performance has been verified, bump the protocol version by editing `inter.broker.protocol.version` and setting it to `2.5`. 
  4. Restart the brokers one by one for the new protocol version to take effect. Once the brokers begin using the latest protocol version, it will no longer be possible to downgrade the cluster to an older version. 
  5. If you have overridden the message format version as instructed above, then you need to do one more rolling restart to upgrade it to its latest version. Once all (or most) consumers have been upgraded to 0.11.0 or later, change log.message.format.version to 2.5 on each broker and restart them one by one. Note that the older Scala clients, which are no longer maintained, do not support the message format introduced in 0.11, so to avoid conversion costs (or to take advantage of exactly once semantics), the newer Java clients must be used. 
  6. There are several notable changes to the reassignment tool `kafka-reassign-partitions.sh` following the completion of [KIP-455](https://cwiki.apache.org/confluence/display/KAFKA/KIP-455%3A+Create+an+Administrative+API+for+Replica+Reassignment). This tool now requires the `--additional` flag to be provided when changing the throttle of an active reassignment. Reassignment cancellation is now possible using the `--cancel` command. Finally, reassignment with `--zookeeper` has been deprecated in favor of `--bootstrap-server`. See the KIP for more detail. 



### Notable changes in 2.5.0

  * When `RebalanceProtocol#COOPERATIVE` is used, `Consumer#poll` can still return data while it is in the middle of a rebalance for those partitions still owned by the consumer; in addition `Consumer#commitSync` now may throw a non-fatal `RebalanceInProgressException` to notify users of such an event, in order to distinguish from the fatal `CommitFailedException` and allow users to complete the ongoing rebalance and then reattempt committing offsets for those still-owned partitions.
  * For improved resiliency in typical network environments, the default value of `zookeeper.session.timeout.ms` has been increased from 6s to 18s and `replica.lag.time.max.ms` from 10s to 30s.
  * New DSL operator `cogroup()` has been added for aggregating multiple streams together at once.
  * Added a new `KStream.toTable()` API to translate an input event stream into a KTable.
  * Added a new Serde type `Void` to represent null keys or null values from input topic.
  * Deprecated `UsePreviousTimeOnInvalidTimestamp` and replaced it with `UsePartitionTimeOnInvalidTimeStamp`.
  * Improved exactly-once semantics by adding a pending offset fencing mechanism and stronger transactional commit consistency check, which greatly simplifies the implementation of a scalable exactly-once application. We also added a new exactly-once semantics code example under [examples](https://github.com/apache/kafka/tree/2.5/examples) folder. Check out [KIP-447](https://cwiki.apache.org/confluence/display/KAFKA/KIP-447%3A+Producer+scalability+for+exactly+once+semantics) for the full details.
  * Added a new public api `KafkaStreams.queryMetadataForKey(String, K, Serializer) to get detailed information on the key being queried. It provides information about the partition number where the key resides in addition to hosts containing the active and standby partitions for the key.`
  * Provided support to query stale stores (for high availability) and the stores belonging to a specific partition by deprecating `KafkaStreams.store(String, QueryableStoreType)` and replacing it with `KafkaStreams.store(StoreQueryParameters)`.
  * Added a new public api to access lag information for stores local to an instance with `KafkaStreams.allLocalStorePartitionLags()`.
  * Scala 2.11 is no longer supported. See [KIP-531](https://cwiki.apache.org/confluence/display/KAFKA/KIP-531%3A+Drop+support+for+Scala+2.11+in+Kafka+2.5) for details.
  * All Scala classes from the package `kafka.security.auth` have been deprecated. See [KIP-504](https://cwiki.apache.org/confluence/display/KAFKA/KIP-504+-+Add+new+Java+Authorizer+Interface) for details of the new Java authorizer API added in 2.4.0. Note that `kafka.security.auth.Authorizer` and `kafka.security.auth.SimpleAclAuthorizer` were deprecated in 2.4.0. 
  * TLSv1 and TLSv1.1 have been disabled by default since these have known security vulnerabilities. Only TLSv1.2 is now enabled by default. You can continue to use TLSv1 and TLSv1.1 by explicitly enabling these in the configuration options `ssl.protocol` and `ssl.enabled.protocols`. 
  * ZooKeeper has been upgraded to 3.5.7, and a ZooKeeper upgrade from 3.4.X to 3.5.7 can fail if there are no snapshot files in the 3.4 data directory. This usually happens in test upgrades where ZooKeeper 3.5.7 is trying to load an existing 3.4 data dir in which no snapshot file has been created. For more details about the issue please refer to [ZOOKEEPER-3056](https://issues.apache.org/jira/browse/ZOOKEEPER-3056). A fix is given in [ZOOKEEPER-3056](https://issues.apache.org/jira/browse/ZOOKEEPER-3056), which is to set `snapshot.trust.empty=true` config in `zookeeper.properties` before the upgrade. 
  * ZooKeeper version 3.5.7 supports TLS-encrypted connectivity to ZooKeeper both with or without client certificates, and additional Kafka configurations are available to take advantage of this. See [KIP-515](https://cwiki.apache.org/confluence/display/KAFKA/KIP-515%3A+Enable+ZK+client+to+use+the+new+TLS+supported+authentication) for details. 



## Upgrading from 0.8.x, 0.9.x, 0.10.0.x, 0.10.1.x, 0.10.2.x, 0.11.0.x, 1.0.x, 1.1.x, 2.0.x or 2.1.x or 2.2.x or 2.3.x to 2.4.0

**If you are upgrading from a version prior to 2.1.x, please see the note below about the change to the schema used to store consumer offsets. Once you have changed the inter.broker.protocol.version to the latest version, it will not be possible to downgrade to a version prior to 2.1.**

**For a rolling upgrade:**

  1. Update server.properties on all brokers and add the following properties. CURRENT_KAFKA_VERSION refers to the version you are upgrading from. CURRENT_MESSAGE_FORMAT_VERSION refers to the message format version currently in use. If you have previously overridden the message format version, you should keep its current value. Alternatively, if you are upgrading from a version prior to 0.11.0.x, then CURRENT_MESSAGE_FORMAT_VERSION should be set to match CURRENT_KAFKA_VERSION. 
     * inter.broker.protocol.version=CURRENT_KAFKA_VERSION (e.g. 0.10.0, 0.11.0, 1.0, 2.0, 2.2).
     * log.message.format.version=CURRENT_MESSAGE_FORMAT_VERSION (See potential performance impact following the upgrade for the details on what this configuration does.)
If you are upgrading from version 0.11.0.x or above, and you have not overridden the message format, then you only need to override the inter-broker protocol version. 
     * inter.broker.protocol.version=CURRENT_KAFKA_VERSION (0.11.0, 1.0, 1.1, 2.0, 2.1, 2.2, 2.3).
  2. Upgrade the brokers one at a time: shut down the broker, update the code, and restart it. Once you have done so, the brokers will be running the latest version and you can verify that the cluster's behavior and performance meets expectations. It is still possible to downgrade at this point if there are any problems. 
  3. Once the cluster's behavior and performance has been verified, bump the protocol version by editing `inter.broker.protocol.version` and setting it to 2.4. 
  4. Restart the brokers one by one for the new protocol version to take effect. Once the brokers begin using the latest protocol version, it will no longer be possible to downgrade the cluster to an older version. 
  5. If you have overridden the message format version as instructed above, then you need to do one more rolling restart to upgrade it to its latest version. Once all (or most) consumers have been upgraded to 0.11.0 or later, change log.message.format.version to 2.4 on each broker and restart them one by one. Note that the older Scala clients, which are no longer maintained, do not support the message format introduced in 0.11, so to avoid conversion costs (or to take advantage of exactly once semantics), the newer Java clients must be used. 



**Additional Upgrade Notes:**

  1. ZooKeeper has been upgraded to 3.5.6. ZooKeeper upgrade from 3.4.X to 3.5.6 can fail if there are no snapshot files in 3.4 data directory. This usually happens in test upgrades where ZooKeeper 3.5.6 is trying to load an existing 3.4 data dir in which no snapshot file has been created. For more details about the issue please refer to [ZOOKEEPER-3056](https://issues.apache.org/jira/browse/ZOOKEEPER-3056). A fix is given in [ZOOKEEPER-3056](https://issues.apache.org/jira/browse/ZOOKEEPER-3056), which is to set `snapshot.trust.empty=true` config in `zookeeper.properties` before the upgrade. But we have observed data loss in standalone cluster upgrades when using `snapshot.trust.empty=true` config. For more details about the issue please refer to [ZOOKEEPER-3644](https://issues.apache.org/jira/browse/ZOOKEEPER-3644). So we recommend the safe workaround of copying empty [snapshot](https://issues.apache.org/jira/secure/attachment/12928686/snapshot.0) file to the 3.4 data directory, if there are no snapshot files in 3.4 data directory. For more details about the workaround please refer to [ZooKeeper Upgrade FAQ](https://cwiki.apache.org/confluence/display/ZOOKEEPER/Upgrade+FAQ). 
  2. An embedded Jetty based [AdminServer](https://zookeeper.apache.org/doc/r3.5.6/zookeeperAdmin.html#sc_adminserver) added in ZooKeeper 3.5. AdminServer is enabled by default in ZooKeeper and is started on port 8080. AdminServer is disabled by default in the ZooKeeper config (`zookeeper.properties`) provided by the Apache Kafka distribution. Make sure to update your local `zookeeper.properties` file with `admin.enableServer=false` if you wish to disable the AdminServer. Please refer [AdminServer config](https://zookeeper.apache.org/doc/r3.5.6/zookeeperAdmin.html#sc_adminserver) to configure the AdminServer. 



### Notable changes in 2.4.0

  * A new Admin API has been added for partition reassignments. Due to changing the way Kafka propagates reassignment information, it is possible to lose reassignment state in failure edge cases while upgrading to the new version. It is not recommended to start reassignments while upgrading.
  * ZooKeeper has been upgraded from 3.4.14 to 3.5.6. TLS and dynamic reconfiguration are supported by the new version.
  * The `bin/kafka-preferred-replica-election.sh` command line tool has been deprecated. It has been replaced by `bin/kafka-leader-election.sh`.
  * The methods `electPreferredLeaders` in the Java `AdminClient` class have been deprecated in favor of the methods `electLeaders`.
  * Scala code leveraging the `NewTopic(String, int, short)` constructor with literal values will need to explicitly call `toShort` on the second literal.
  * The argument in the constructor `GroupAuthorizationException(String)` is now used to specify an exception message. Previously it referred to the group that failed authorization. This was done for consistency with other exception types and to avoid potential misuse. The constructor `TopicAuthorizationException(String)` which was previously used for a single unauthorized topic was changed similarly. 
  * The internal `PartitionAssignor` interface has been deprecated and replaced with a new `ConsumerPartitionAssignor` in the public API. Some methods/signatures are slightly different between the two interfaces. Users implementing a custom PartitionAssignor should migrate to the new interface as soon as possible. 
  * The `DefaultPartitioner` now uses a sticky partitioning strategy. This means that records for specific topic with null keys and no assigned partition will be sent to the same partition until the batch is ready to be sent. When a new batch is created, a new partition is chosen. This decreases latency to produce, but it may result in uneven distribution of records across partitions in edge cases. Generally users will not be impacted, but this difference may be noticeable in tests and other situations producing records for a very short amount of time. 
  * The blocking `KafkaConsumer#committed` methods have been extended to allow a list of partitions as input parameters rather than a single partition. It enables fewer request/response iterations between clients and brokers fetching for the committed offsets for the consumer group. The old overloaded functions are deprecated and we would recommend users to make their code changes to leverage the new methods (details can be found in [KIP-520](https://cwiki.apache.org/confluence/display/KAFKA/KIP-520%3A+Add+overloaded+Consumer%23committed+for+batching+partitions)). 
  * We've introduced a new `INVALID_RECORD` error in the produce response to distinguish from the `CORRUPT_MESSAGE` error. To be more concrete, previously when a batch of records was sent as part of a single request to the broker and one or more of the records failed the validation due to various causes (mismatch magic bytes, crc checksum errors, null key for log compacted topics, etc), the whole batch would be rejected with the same and misleading `CORRUPT_MESSAGE`, and the caller of the producer client would see the corresponding exception from either the future object of `RecordMetadata` returned from the `send` call as well as in the `Callback#onCompletion(RecordMetadata metadata, Exception exception)` Now with the new error code and improved error messages of the exception, producer callers would be better informed about the root cause why their sent records were failed. 
  * We are introducing incremental cooperative rebalancing to the clients' group protocol, which allows consumers to keep all of their assigned partitions during a rebalance and at the end revoke only those which must be migrated to another consumer for overall cluster balance. The `ConsumerCoordinator` will choose the latest `RebalanceProtocol` that is commonly supported by all of the consumer's supported assignors. You can use the new built-in `CooperativeStickyAssignor` or plug in your own custom cooperative assignor. To do so you must implement the `ConsumerPartitionAssignor` interface and include `RebalanceProtocol.COOPERATIVE` in the list returned by `ConsumerPartitionAssignor#supportedProtocols`. Your custom assignor can then leverage the `ownedPartitions` field in each consumer's `Subscription` to give partitions back to their previous owners whenever possible. Note that when a partition is to be reassigned to another consumer, it _must_ be removed from the new assignment until it has been revoked from its original owner. Any consumer that has to revoke a partition will trigger a followup rebalance to allow the revoked partition to safely be assigned to its new owner. See the [ConsumerPartitionAssignor RebalanceProtocol javadocs](/https://kafka.apache.org/24/javadoc/index.html?org/apache/kafka/clients/consumer/ConsumerPartitionAssignor.RebalanceProtocol.html) for more information.   
To upgrade from the old (eager) protocol, which always revokes all partitions before rebalancing, to cooperative rebalancing, you must follow a specific upgrade path to get all clients on the same `ConsumerPartitionAssignor` that supports the cooperative protocol. This can be done with two rolling bounces, using the `CooperativeStickyAssignor` for the example: during the first one, add "cooperative-sticky" to the list of supported assignors for each member (without removing the previous assignor -- note that if previously using the default, you must include that explicitly as well). You then bounce and/or upgrade it. Once the entire group is on 2.4+ and all members have the "cooperative-sticky" among their supported assignors, remove the other assignor(s) and perform a second rolling bounce so that by the end all members support only the cooperative protocol. For further details on the cooperative rebalancing protocol and upgrade path, see [KIP-429](https://cwiki.apache.org/confluence/x/vAclBg). 
  * There are some behavioral changes to the `ConsumerRebalanceListener`, as well as a new API. Exceptions thrown during any of the listener's three callbacks will no longer be swallowed, and will instead be re-thrown all the way up to the `Consumer.poll()` call. The `onPartitionsLost` method has been added to allow users to react to abnormal circumstances where a consumer may have lost ownership of its partitions (such as a missed rebalance) and cannot commit offsets. By default, this will simply call the existing `onPartitionsRevoked` API to align with previous behavior. Note however that `onPartitionsLost` will not be called when the set of lost partitions is empty. This means that no callback will be invoked at the beginning of the first rebalance of a new consumer joining the group.   
The semantics of the `ConsumerRebalanceListener's` callbacks are further changed when following the cooperative rebalancing protocol described above. In addition to `onPartitionsLost`, `onPartitionsRevoked` will also never be called when the set of revoked partitions is empty. The callback will generally be invoked only at the end of a rebalance, and only on the set of partitions that are being moved to another consumer. The `onPartitionsAssigned` callback will however always be called, even with an empty set of partitions, as a way to notify users of a rebalance event (this is true for both cooperative and eager). For details on the new callback semantics, see the [ConsumerRebalanceListener javadocs](/https://kafka.apache.org/24/javadoc/index.html?org/apache/kafka/clients/consumer/ConsumerRebalanceListener.html). 
  * The Scala trait `kafka.security.auth.Authorizer` has been deprecated and replaced with a new Java API `org.apache.kafka.server.authorizer.Authorizer`. The authorizer implementation class `kafka.security.auth.SimpleAclAuthorizer` has also been deprecated and replaced with a new implementation `kafka.security.authorizer.AclAuthorizer`. `AclAuthorizer` uses features supported by the new API to improve authorization logging and is compatible with `SimpleAclAuthorizer`. For more details, see [KIP-504](https://cwiki.apache.org/confluence/display/KAFKA/KIP-504+-+Add+new+Java+Authorizer+Interface). 



## Upgrading from 0.8.x, 0.9.x, 0.10.0.x, 0.10.1.x, 0.10.2.x, 0.11.0.x, 1.0.x, 1.1.x, 2.0.x or 2.1.x or 2.2.x to 2.3.0

**If you are upgrading from a version prior to 2.1.x, please see the note below about the change to the schema used to store consumer offsets. Once you have changed the inter.broker.protocol.version to the latest version, it will not be possible to downgrade to a version prior to 2.1.**

**For a rolling upgrade:**

  1. Update server.properties on all brokers and add the following properties. CURRENT_KAFKA_VERSION refers to the version you are upgrading from. CURRENT_MESSAGE_FORMAT_VERSION refers to the message format version currently in use. If you have previously overridden the message format version, you should keep its current value. Alternatively, if you are upgrading from a version prior to 0.11.0.x, then CURRENT_MESSAGE_FORMAT_VERSION should be set to match CURRENT_KAFKA_VERSION. 
     * inter.broker.protocol.version=CURRENT_KAFKA_VERSION (e.g. 0.8.2, 0.9.0, 0.10.0, 0.10.1, 0.10.2, 0.11.0, 1.0, 1.1).
     * log.message.format.version=CURRENT_MESSAGE_FORMAT_VERSION (See potential performance impact following the upgrade for the details on what this configuration does.)
If you are upgrading from 0.11.0.x, 1.0.x, 1.1.x, 2.0.x, or 2.1.x, and you have not overridden the message format, then you only need to override the inter-broker protocol version. 
     * inter.broker.protocol.version=CURRENT_KAFKA_VERSION (0.11.0, 1.0, 1.1, 2.0, 2.1, 2.2).
  2. Upgrade the brokers one at a time: shut down the broker, update the code, and restart it. Once you have done so, the brokers will be running the latest version and you can verify that the cluster's behavior and performance meets expectations. It is still possible to downgrade at this point if there are any problems. 
  3. Once the cluster's behavior and performance has been verified, bump the protocol version by editing `inter.broker.protocol.version` and setting it to 2.3. 
  4. Restart the brokers one by one for the new protocol version to take effect. Once the brokers begin using the latest protocol version, it will no longer be possible to downgrade the cluster to an older version. 
  5. If you have overridden the message format version as instructed above, then you need to do one more rolling restart to upgrade it to its latest version. Once all (or most) consumers have been upgraded to 0.11.0 or later, change log.message.format.version to 2.3 on each broker and restart them one by one. Note that the older Scala clients, which are no longer maintained, do not support the message format introduced in 0.11, so to avoid conversion costs (or to take advantage of exactly once semantics), the newer Java clients must be used. 



### Notable changes in 2.3.0

  * We are introducing a new rebalancing protocol for Kafka Connect based on [incremental cooperative rebalancing](https://cwiki.apache.org/confluence/display/KAFKA/KIP-415%3A+Incremental+Cooperative+Rebalancing+in+Kafka+Connect). The new protocol does not require stopping all the tasks during a rebalancing phase between Connect workers. Instead, only the tasks that need to be exchanged between workers are stopped and they are started in a follow up rebalance. The new Connect protocol is enabled by default beginning with 2.3.0. For more details on how it works and how to enable the old behavior of eager rebalancing, checkout [incremental cooperative rebalancing design](/37/documentation/#connect_administration). 
  * We are introducing static membership towards consumer user. This feature reduces unnecessary rebalances during normal application upgrades or rolling bounces. For more details on how to use it, checkout [static membership design](/37/documentation/#static_membership). 
  * Kafka Streams DSL switches its used store types. While this change is mainly transparent to users, there are some corner cases that may require code changes. See the [Kafka Streams upgrade section](/37/documentation/streams/upgrade-guide#streams_api_changes_230) for more details. 
  * Kafka Streams 2.3.0 requires 0.11 message format or higher and does not work with older message format.



## Upgrading from 0.8.x, 0.9.x, 0.10.0.x, 0.10.1.x, 0.10.2.x, 0.11.0.x, 1.0.x, 1.1.x, 2.0.x or 2.1.x to 2.2.0

**If you are upgrading from a version prior to 2.1.x, please see the note below about the change to the schema used to store consumer offsets. Once you have changed the inter.broker.protocol.version to the latest version, it will not be possible to downgrade to a version prior to 2.1.**

**For a rolling upgrade:**

  1. Update server.properties on all brokers and add the following properties. CURRENT_KAFKA_VERSION refers to the version you are upgrading from. CURRENT_MESSAGE_FORMAT_VERSION refers to the message format version currently in use. If you have previously overridden the message format version, you should keep its current value. Alternatively, if you are upgrading from a version prior to 0.11.0.x, then CURRENT_MESSAGE_FORMAT_VERSION should be set to match CURRENT_KAFKA_VERSION. 
     * inter.broker.protocol.version=CURRENT_KAFKA_VERSION (e.g. 0.8.2, 0.9.0, 0.10.0, 0.10.1, 0.10.2, 0.11.0, 1.0, 1.1).
     * log.message.format.version=CURRENT_MESSAGE_FORMAT_VERSION (See potential performance impact following the upgrade for the details on what this configuration does.)
If you are upgrading from 0.11.0.x, 1.0.x, 1.1.x, or 2.0.x and you have not overridden the message format, then you only need to override the inter-broker protocol version. 
     * inter.broker.protocol.version=CURRENT_KAFKA_VERSION (0.11.0, 1.0, 1.1, 2.0).
  2. Upgrade the brokers one at a time: shut down the broker, update the code, and restart it. Once you have done so, the brokers will be running the latest version and you can verify that the cluster's behavior and performance meets expectations. It is still possible to downgrade at this point if there are any problems. 
  3. Once the cluster's behavior and performance has been verified, bump the protocol version by editing `inter.broker.protocol.version` and setting it to 2.2. 
  4. Restart the brokers one by one for the new protocol version to take effect. Once the brokers begin using the latest protocol version, it will no longer be possible to downgrade the cluster to an older version. 
  5. If you have overridden the message format version as instructed above, then you need to do one more rolling restart to upgrade it to its latest version. Once all (or most) consumers have been upgraded to 0.11.0 or later, change log.message.format.version to 2.2 on each broker and restart them one by one. Note that the older Scala clients, which are no longer maintained, do not support the message format introduced in 0.11, so to avoid conversion costs (or to take advantage of exactly once semantics), the newer Java clients must be used. 



### Notable changes in 2.2.1

  * Kafka Streams 2.2.1 requires 0.11 message format or higher and does not work with older message format.



### Notable changes in 2.2.0

  * The default consumer group id has been changed from the empty string (`""`) to `null`. Consumers who use the new default group id will not be able to subscribe to topics, and fetch or commit offsets. The empty string as consumer group id is deprecated but will be supported until a future major release. Old clients that rely on the empty string group id will now have to explicitly provide it as part of their consumer config. For more information see [KIP-289](https://cwiki.apache.org/confluence/display/KAFKA/KIP-289%3A+Improve+the+default+group+id+behavior+in+KafkaConsumer).
  * The `bin/kafka-topics.sh` command line tool is now able to connect directly to brokers with `--bootstrap-server` instead of zookeeper. The old `--zookeeper` option is still available for now. Please read [KIP-377](https://cwiki.apache.org/confluence/display/KAFKA/KIP-377%3A+TopicCommand+to+use+AdminClient) for more information.
  * Kafka Streams depends on a newer version of RocksDBs that requires MacOS 10.13 or higher.



## Upgrading from 0.8.x, 0.9.x, 0.10.0.x, 0.10.1.x, 0.10.2.x, 0.11.0.x, 1.0.x, 1.1.x, or 2.0.0 to 2.1.0

**Note that 2.1.x contains a change to the internal schema used to store consumer offsets. Once the upgrade is complete, it will not be possible to downgrade to previous versions. See the rolling upgrade notes below for more detail.**

**For a rolling upgrade:**

  1. Update server.properties on all brokers and add the following properties. CURRENT_KAFKA_VERSION refers to the version you are upgrading from. CURRENT_MESSAGE_FORMAT_VERSION refers to the message format version currently in use. If you have previously overridden the message format version, you should keep its current value. Alternatively, if you are upgrading from a version prior to 0.11.0.x, then CURRENT_MESSAGE_FORMAT_VERSION should be set to match CURRENT_KAFKA_VERSION. 
     * inter.broker.protocol.version=CURRENT_KAFKA_VERSION (e.g. 0.8.2, 0.9.0, 0.10.0, 0.10.1, 0.10.2, 0.11.0, 1.0, 1.1).
     * log.message.format.version=CURRENT_MESSAGE_FORMAT_VERSION (See potential performance impact following the upgrade for the details on what this configuration does.)
If you are upgrading from 0.11.0.x, 1.0.x, 1.1.x, or 2.0.x and you have not overridden the message format, then you only need to override the inter-broker protocol version. 
     * inter.broker.protocol.version=CURRENT_KAFKA_VERSION (0.11.0, 1.0, 1.1, 2.0).
  2. Upgrade the brokers one at a time: shut down the broker, update the code, and restart it. Once you have done so, the brokers will be running the latest version and you can verify that the cluster's behavior and performance meets expectations. It is still possible to downgrade at this point if there are any problems. 
  3. Once the cluster's behavior and performance has been verified, bump the protocol version by editing `inter.broker.protocol.version` and setting it to 2.1. 
  4. Restart the brokers one by one for the new protocol version to take effect. Once the brokers begin using the latest protocol version, it will no longer be possible to downgrade the cluster to an older version. 
  5. If you have overridden the message format version as instructed above, then you need to do one more rolling restart to upgrade it to its latest version. Once all (or most) consumers have been upgraded to 0.11.0 or later, change log.message.format.version to 2.1 on each broker and restart them one by one. Note that the older Scala clients, which are no longer maintained, do not support the message format introduced in 0.11, so to avoid conversion costs (or to take advantage of exactly once semantics), the newer Java clients must be used. 



**Additional Upgrade Notes:**

  1. Offset expiration semantics has slightly changed in this version. According to the new semantics, offsets of partitions in a group will not be removed while the group is subscribed to the corresponding topic and is still active (has active consumers). If group becomes empty all its offsets will be removed after default offset retention period (or the one set by broker) has passed (unless the group becomes active again). Offsets associated with standalone (simple) consumers, that do not use Kafka group management, will be removed after default offset retention period (or the one set by broker) has passed since their last commit.
  2. The default for console consumer's `enable.auto.commit` property when no `group.id` is provided is now set to `false`. This is to avoid polluting the consumer coordinator cache as the auto-generated group is not likely to be used by other consumers.
  3. The default value for the producer's `retries` config was changed to `Integer.MAX_VALUE`, as we introduced `delivery.timeout.ms` in [KIP-91](https://cwiki.apache.org/confluence/display/KAFKA/KIP-91+Provide+Intuitive+User+Timeouts+in+The+Producer), which sets an upper bound on the total time between sending a record and receiving acknowledgement from the broker. By default, the delivery timeout is set to 2 minutes.
  4. By default, MirrorMaker now overrides `delivery.timeout.ms` to `Integer.MAX_VALUE` when configuring the producer. If you have overridden the value of `retries` in order to fail faster, you will instead need to override `delivery.timeout.ms`.
  5. The `ListGroup` API now expects, as a recommended alternative, `Describe Group` access to the groups a user should be able to list. Even though the old `Describe Cluster` access is still supported for backward compatibility, using it for this API is not advised.
  6. [KIP-336](https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=87298242) deprecates the ExtendedSerializer and ExtendedDeserializer interfaces and propagates the usage of Serializer and Deserializer. ExtendedSerializer and ExtendedDeserializer were introduced with [KIP-82](https://cwiki.apache.org/confluence/display/KAFKA/KIP-82+-+Add+Record+Headers) to provide record headers for serializers and deserializers in a Java 7 compatible fashion. Now we consolidated these interfaces as Java 7 support has been dropped since.



### Notable changes in 2.1.0

  * Jetty has been upgraded to 9.4.12, which excludes TLS_RSA_* ciphers by default because they do not support forward secrecy, see https://github.com/eclipse/jetty.project/issues/2807 for more information.
  * Unclean leader election is automatically enabled by the controller when `unclean.leader.election.enable` config is dynamically updated by using per-topic config override.
  * The `AdminClient` has added a method `AdminClient#metrics()`. Now any application using the `AdminClient` can gain more information and insight by viewing the metrics captured from the `AdminClient`. For more information see [KIP-324](https://cwiki.apache.org/confluence/display/KAFKA/KIP-324%3A+Add+method+to+get+metrics%28%29+in+AdminClient)
  * Kafka now supports Zstandard compression from [KIP-110](https://cwiki.apache.org/confluence/display/KAFKA/KIP-110%3A+Add+Codec+for+ZStandard+Compression). You must upgrade the broker as well as clients to make use of it. Consumers prior to 2.1.0 will not be able to read from topics which use Zstandard compression, so you should not enable it for a topic until all downstream consumers are upgraded. See the KIP for more detail. 



## Upgrading from 0.8.x, 0.9.x, 0.10.0.x, 0.10.1.x, 0.10.2.x, 0.11.0.x, 1.0.x, or 1.1.x to 2.0.0

Kafka 2.0.0 introduces wire protocol changes. By following the recommended rolling upgrade plan below, you guarantee no downtime during the upgrade. However, please review the notable changes in 2.0.0 before upgrading. 

**For a rolling upgrade:**

  1. Update server.properties on all brokers and add the following properties. CURRENT_KAFKA_VERSION refers to the version you are upgrading from. CURRENT_MESSAGE_FORMAT_VERSION refers to the message format version currently in use. If you have previously overridden the message format version, you should keep its current value. Alternatively, if you are upgrading from a version prior to 0.11.0.x, then CURRENT_MESSAGE_FORMAT_VERSION should be set to match CURRENT_KAFKA_VERSION. 
     * inter.broker.protocol.version=CURRENT_KAFKA_VERSION (e.g. 0.8.2, 0.9.0, 0.10.0, 0.10.1, 0.10.2, 0.11.0, 1.0, 1.1).
     * log.message.format.version=CURRENT_MESSAGE_FORMAT_VERSION (See potential performance impact following the upgrade for the details on what this configuration does.)
If you are upgrading from 0.11.0.x, 1.0.x, or 1.1.x and you have not overridden the message format, then you only need to override the inter-broker protocol format. 
     * inter.broker.protocol.version=CURRENT_KAFKA_VERSION (0.11.0, 1.0, 1.1).
  2. Upgrade the brokers one at a time: shut down the broker, update the code, and restart it. 
  3. Once the entire cluster is upgraded, bump the protocol version by editing `inter.broker.protocol.version` and setting it to 2.0. 
  4. Restart the brokers one by one for the new protocol version to take effect.
  5. If you have overridden the message format version as instructed above, then you need to do one more rolling restart to upgrade it to its latest version. Once all (or most) consumers have been upgraded to 0.11.0 or later, change log.message.format.version to 2.0 on each broker and restart them one by one. Note that the older Scala consumer does not support the new message format introduced in 0.11, so to avoid the performance cost of down-conversion (or to take advantage of exactly once semantics), the newer Java consumer must be used.



**Additional Upgrade Notes:**

  1. If you are willing to accept downtime, you can simply take all the brokers down, update the code and start them back up. They will start with the new protocol by default.
  2. Bumping the protocol version and restarting can be done any time after the brokers are upgraded. It does not have to be immediately after. Similarly for the message format version.
  3. If you are using Java8 method references in your Kafka Streams code you might need to update your code to resolve method ambiguities. Hot-swapping the jar-file only might not work.
  4. ACLs should not be added to prefixed resources, (added in [KIP-290](https://cwiki.apache.org/confluence/display/KAFKA/KIP-290%3A+Support+for+Prefixed+ACLs)), until all brokers in the cluster have been updated. 

**NOTE:** any prefixed ACLs added to a cluster, even after the cluster is fully upgraded, will be ignored should the cluster be downgraded again. 



### Notable changes in 2.0.0

  * [KIP-186](https://cwiki.apache.org/confluence/x/oYtjB) increases the default offset retention time from 1 day to 7 days. This makes it less likely to "lose" offsets in an application that commits infrequently. It also increases the active set of offsets and therefore can increase memory usage on the broker. Note that the console consumer currently enables offset commit by default and can be the source of a large number of offsets which this change will now preserve for 7 days instead of 1. You can preserve the existing behavior by setting the broker config `offsets.retention.minutes` to 1440.
  * Support for Java 7 has been dropped, Java 8 is now the minimum version required.
  * The default value for `ssl.endpoint.identification.algorithm` was changed to `https`, which performs hostname verification (man-in-the-middle attacks are possible otherwise). Set `ssl.endpoint.identification.algorithm` to an empty string to restore the previous behaviour. 
  * [KAFKA-5674](https://issues.apache.org/jira/browse/KAFKA-5674) extends the lower interval of `max.connections.per.ip` minimum to zero and therefore allows IP-based filtering of inbound connections.
  * [KIP-272](https://cwiki.apache.org/confluence/display/KAFKA/KIP-272%3A+Add+API+version+tag+to+broker%27s+RequestsPerSec+metric) added API version tag to the metric `kafka.network:type=RequestMetrics,name=RequestsPerSec,request={Produce|FetchConsumer|FetchFollower|...}`. This metric now becomes `kafka.network:type=RequestMetrics,name=RequestsPerSec,request={Produce|FetchConsumer|FetchFollower|...},version={0|1|2|3|...}`. This will impact JMX monitoring tools that do not automatically aggregate. To get the total count for a specific request type, the tool needs to be updated to aggregate across different versions. 
  * [KIP-225](https://cwiki.apache.org/confluence/x/uaBzB) changed the metric "records.lag" to use tags for topic and partition. The original version with the name format "{topic}-{partition}.records-lag" has been removed.
  * The Scala consumers, which have been deprecated since 0.11.0.0, have been removed. The Java consumer has been the recommended option since 0.10.0.0. Note that the Scala consumers in 1.1.0 (and older) will continue to work even if the brokers are upgraded to 2.0.0.
  * The Scala producers, which have been deprecated since 0.10.0.0, have been removed. The Java producer has been the recommended option since 0.9.0.0. Note that the behaviour of the default partitioner in the Java producer differs from the default partitioner in the Scala producers. Users migrating should consider configuring a custom partitioner that retains the previous behaviour. Note that the Scala producers in 1.1.0 (and older) will continue to work even if the brokers are upgraded to 2.0.0.
  * MirrorMaker and ConsoleConsumer no longer support the Scala consumer, they always use the Java consumer.
  * The ConsoleProducer no longer supports the Scala producer, it always uses the Java producer.
  * A number of deprecated tools that rely on the Scala clients have been removed: ReplayLogProducer, SimpleConsumerPerformance, SimpleConsumerShell, ExportZkOffsets, ImportZkOffsets, UpdateOffsetsInZK, VerifyConsumerRebalance.
  * The deprecated kafka.tools.ProducerPerformance has been removed, please use org.apache.kafka.tools.ProducerPerformance.
  * New Kafka Streams configuration parameter `upgrade.from` added that allows rolling bounce upgrade from older version. 
  * [KIP-284](https://cwiki.apache.org/confluence/x/DVyHB) changed the retention time for Kafka Streams repartition topics by setting its default value to `Long.MAX_VALUE`.
  * Updated `ProcessorStateManager` APIs in Kafka Streams for registering state stores to the processor topology. For more details please read the Streams [Upgrade Guide](/37/documentation/streams/upgrade-guide#streams_api_changes_200).
  * In earlier releases, Connect's worker configuration required the `internal.key.converter` and `internal.value.converter` properties. In 2.0, these are [no longer required](https://cwiki.apache.org/confluence/x/AZQ7B) and default to the JSON converter. You may safely remove these properties from your Connect standalone and distributed worker configurations:  
`internal.key.converter=org.apache.kafka.connect.json.JsonConverter` `internal.key.converter.schemas.enable=false` `internal.value.converter=org.apache.kafka.connect.json.JsonConverter` `internal.value.converter.schemas.enable=false`
  * [KIP-266](https://cwiki.apache.org/confluence/x/5kiHB) adds a new consumer configuration `default.api.timeout.ms` to specify the default timeout to use for `KafkaConsumer` APIs that could block. The KIP also adds overloads for such blocking APIs to support specifying a specific timeout to use for each of them instead of using the default timeout set by `default.api.timeout.ms`. In particular, a new `poll(Duration)` API has been added which does not block for dynamic partition assignment. The old `poll(long)` API has been deprecated and will be removed in a future version. Overloads have also been added for other `KafkaConsumer` methods like `partitionsFor`, `listTopics`, `offsetsForTimes`, `beginningOffsets`, `endOffsets` and `close` that take in a `Duration`.
  * Also as part of KIP-266, the default value of `request.timeout.ms` has been changed to 30 seconds. The previous value was a little higher than 5 minutes to account for maximum time that a rebalance would take. Now we treat the JoinGroup request in the rebalance as a special case and use a value derived from `max.poll.interval.ms` for the request timeout. All other request types use the timeout defined by `request.timeout.ms`
  * The internal method `kafka.admin.AdminClient.deleteRecordsBefore` has been removed. Users are encouraged to migrate to `org.apache.kafka.clients.admin.AdminClient.deleteRecords`.
  * The AclCommand tool `--producer` convenience option uses the [KIP-277](https://cwiki.apache.org/confluence/display/KAFKA/KIP-277+-+Fine+Grained+ACL+for+CreateTopics+API) finer grained ACL on the given topic. 
  * [KIP-176](https://cwiki.apache.org/confluence/display/KAFKA/KIP-176%3A+Remove+deprecated+new-consumer+option+for+tools) removes the `--new-consumer` option for all consumer based tools. This option is redundant since the new consumer is automatically used if --bootstrap-server is defined. 
  * [KIP-290](https://cwiki.apache.org/confluence/display/KAFKA/KIP-290%3A+Support+for+Prefixed+ACLs) adds the ability to define ACLs on prefixed resources, e.g. any topic starting with 'foo'.
  * [KIP-283](https://cwiki.apache.org/confluence/display/KAFKA/KIP-283%3A+Efficient+Memory+Usage+for+Down-Conversion) improves message down-conversion handling on Kafka broker, which has typically been a memory-intensive operation. The KIP adds a mechanism by which the operation becomes less memory intensive by down-converting chunks of partition data at a time which helps put an upper bound on memory consumption. With this improvement, there is a change in `FetchResponse` protocol behavior where the broker could send an oversized message batch towards the end of the response with an invalid offset. Such oversized messages must be ignored by consumer clients, as is done by `KafkaConsumer`. 

KIP-283 also adds new topic and broker configurations `message.downconversion.enable` and `log.message.downconversion.enable` respectively to control whether down-conversion is enabled. When disabled, broker does not perform any down-conversion and instead sends an `UNSUPPORTED_VERSION` error to the client.

  * Dynamic broker configuration options can be stored in ZooKeeper using kafka-configs.sh before brokers are started. This option can be used to avoid storing clear passwords in server.properties as all password configs may be stored encrypted in ZooKeeper.
  * ZooKeeper hosts are now re-resolved if connection attempt fails. But if your ZooKeeper host names resolve to multiple addresses and some of them are not reachable, then you may need to increase the connection timeout `zookeeper.connection.timeout.ms`.



### New Protocol Versions

  * [KIP-279](https://cwiki.apache.org/confluence/display/KAFKA/KIP-279%3A+Fix+log+divergence+between+leader+and+follower+after+fast+leader+fail+over): OffsetsForLeaderEpochResponse v1 introduces a partition-level `leader_epoch` field. 
  * [KIP-219](https://cwiki.apache.org/confluence/display/KAFKA/KIP-219+-+Improve+quota+communication): Bump up the protocol versions of non-cluster action requests and responses that are throttled on quota violation.
  * [KIP-290](https://cwiki.apache.org/confluence/display/KAFKA/KIP-290%3A+Support+for+Prefixed+ACLs): Bump up the protocol versions ACL create, describe and delete requests and responses.



### Upgrading a 1.1 Kafka Streams Application

  * Upgrading your Streams application from 1.1 to 2.0 does not require a broker upgrade. A Kafka Streams 2.0 application can connect to 2.0, 1.1, 1.0, 0.11.0, 0.10.2 and 0.10.1 brokers (it is not possible to connect to 0.10.0 brokers though). 
  * Note that in 2.0 we have removed the public APIs that are deprecated prior to 1.0; users leveraging on those deprecated APIs need to make code changes accordingly. See [Streams API changes in 2.0.0](/37/documentation/streams/upgrade-guide#streams_api_changes_200) for more details. 



## Upgrading from 0.8.x, 0.9.x, 0.10.0.x, 0.10.1.x, 0.10.2.x, 0.11.0.x, or 1.0.x to 1.1.x

Kafka 1.1.0 introduces wire protocol changes. By following the recommended rolling upgrade plan below, you guarantee no downtime during the upgrade. However, please review the notable changes in 1.1.0 before upgrading. 

**For a rolling upgrade:**

  1. Update server.properties on all brokers and add the following properties. CURRENT_KAFKA_VERSION refers to the version you are upgrading from. CURRENT_MESSAGE_FORMAT_VERSION refers to the message format version currently in use. If you have previously overridden the message format version, you should keep its current value. Alternatively, if you are upgrading from a version prior to 0.11.0.x, then CURRENT_MESSAGE_FORMAT_VERSION should be set to match CURRENT_KAFKA_VERSION. 
     * inter.broker.protocol.version=CURRENT_KAFKA_VERSION (e.g. 0.8.2, 0.9.0, 0.10.0, 0.10.1, 0.10.2, 0.11.0, 1.0).
     * log.message.format.version=CURRENT_MESSAGE_FORMAT_VERSION (See potential performance impact following the upgrade for the details on what this configuration does.)
If you are upgrading from 0.11.0.x or 1.0.x and you have not overridden the message format, then you only need to override the inter-broker protocol format. 
     * inter.broker.protocol.version=CURRENT_KAFKA_VERSION (0.11.0 or 1.0).
  2. Upgrade the brokers one at a time: shut down the broker, update the code, and restart it. 
  3. Once the entire cluster is upgraded, bump the protocol version by editing `inter.broker.protocol.version` and setting it to 1.1. 
  4. Restart the brokers one by one for the new protocol version to take effect. 
  5. If you have overridden the message format version as instructed above, then you need to do one more rolling restart to upgrade it to its latest version. Once all (or most) consumers have been upgraded to 0.11.0 or later, change log.message.format.version to 1.1 on each broker and restart them one by one. Note that the older Scala consumer does not support the new message format introduced in 0.11, so to avoid the performance cost of down-conversion (or to take advantage of exactly once semantics), the newer Java consumer must be used.



**Additional Upgrade Notes:**

  1. If you are willing to accept downtime, you can simply take all the brokers down, update the code and start them back up. They will start with the new protocol by default.
  2. Bumping the protocol version and restarting can be done any time after the brokers are upgraded. It does not have to be immediately after. Similarly for the message format version.
  3. If you are using Java8 method references in your Kafka Streams code you might need to update your code to resolve method ambiguties. Hot-swapping the jar-file only might not work.



### Notable changes in 1.1.1

  * New Kafka Streams configuration parameter `upgrade.from` added that allows rolling bounce upgrade from version 0.10.0.x 
  * See the [**Kafka Streams upgrade guide**](/37/documentation/streams/upgrade-guide.html) for details about this new config. 


### Notable changes in 1.1.0

  * The kafka artifact in Maven no longer depends on log4j or slf4j-log4j12. Similarly to the kafka-clients artifact, users can now choose the logging back-end by including the appropriate slf4j module (slf4j-log4j12, logback, etc.). The release tarball still includes log4j and slf4j-log4j12.
  * [KIP-225](https://cwiki.apache.org/confluence/x/uaBzB) changed the metric "records.lag" to use tags for topic and partition. The original version with the name format "{topic}-{partition}.records-lag" is deprecated and will be removed in 2.0.0.
  * Kafka Streams is more robust against broker communication errors. Instead of stopping the Kafka Streams client with a fatal exception, Kafka Streams tries to self-heal and reconnect to the cluster. Using the new `AdminClient` you have better control of how often Kafka Streams retries and can [configure](/37/documentation/streams/developer-guide/config-streams) fine-grained timeouts (instead of hard coded retries as in older version).
  * Kafka Streams rebalance time was reduced further making Kafka Streams more responsive.
  * Kafka Connect now supports message headers in both sink and source connectors, and to manipulate them via simple message transforms. Connectors must be changed to explicitly use them. A new `HeaderConverter` is introduced to control how headers are (de)serialized, and the new "SimpleHeaderConverter" is used by default to use string representations of values.
  * kafka.tools.DumpLogSegments now automatically sets deep-iteration option if print-data-log is enabled explicitly or implicitly due to any of the other options like decoder.



### New Protocol Versions

  * [KIP-226](https://cwiki.apache.org/confluence/display/KAFKA/KIP-226+-+Dynamic+Broker+Configuration) introduced DescribeConfigs Request/Response v1.
  * [KIP-227](https://cwiki.apache.org/confluence/display/KAFKA/KIP-227%3A+Introduce+Incremental+FetchRequests+to+Increase+Partition+Scalability) introduced Fetch Request/Response v7.



### Upgrading a 1.0 Kafka Streams Application

  * Upgrading your Streams application from 1.0 to 1.1 does not require a broker upgrade. A Kafka Streams 1.1 application can connect to 1.0, 0.11.0, 0.10.2 and 0.10.1 brokers (it is not possible to connect to 0.10.0 brokers though). 
  * See [Streams API changes in 1.1.0](/37/documentation/streams/upgrade-guide#streams_api_changes_110) for more details. 



## Upgrading from 0.8.x, 0.9.x, 0.10.0.x, 0.10.1.x, 0.10.2.x or 0.11.0.x to 1.0.0

Kafka 1.0.0 introduces wire protocol changes. By following the recommended rolling upgrade plan below, you guarantee no downtime during the upgrade. However, please review the notable changes in 1.0.0 before upgrading. 

**For a rolling upgrade:**

  1. Update server.properties on all brokers and add the following properties. CURRENT_KAFKA_VERSION refers to the version you are upgrading from. CURRENT_MESSAGE_FORMAT_VERSION refers to the message format version currently in use. If you have previously overridden the message format version, you should keep its current value. Alternatively, if you are upgrading from a version prior to 0.11.0.x, then CURRENT_MESSAGE_FORMAT_VERSION should be set to match CURRENT_KAFKA_VERSION. 
     * inter.broker.protocol.version=CURRENT_KAFKA_VERSION (e.g. 0.8.2, 0.9.0, 0.10.0, 0.10.1, 0.10.2, 0.11.0).
     * log.message.format.version=CURRENT_MESSAGE_FORMAT_VERSION (See potential performance impact following the upgrade for the details on what this configuration does.)
If you are upgrading from 0.11.0.x and you have not overridden the message format, you must set both the message format version and the inter-broker protocol version to 0.11.0. 
     * inter.broker.protocol.version=0.11.0
     * log.message.format.version=0.11.0
  2. Upgrade the brokers one at a time: shut down the broker, update the code, and restart it. 
  3. Once the entire cluster is upgraded, bump the protocol version by editing `inter.broker.protocol.version` and setting it to 1.0. 
  4. Restart the brokers one by one for the new protocol version to take effect. 
  5. If you have overridden the message format version as instructed above, then you need to do one more rolling restart to upgrade it to its latest version. Once all (or most) consumers have been upgraded to 0.11.0 or later, change log.message.format.version to 1.0 on each broker and restart them one by one. If you are upgrading from 0.11.0 and log.message.format.version is set to 0.11.0, you can update the config and skip the rolling restart. Note that the older Scala consumer does not support the new message format introduced in 0.11, so to avoid the performance cost of down-conversion (or to take advantage of exactly once semantics), the newer Java consumer must be used.



**Additional Upgrade Notes:**

  1. If you are willing to accept downtime, you can simply take all the brokers down, update the code and start them back up. They will start with the new protocol by default.
  2. Bumping the protocol version and restarting can be done any time after the brokers are upgraded. It does not have to be immediately after. Similarly for the message format version.



### Notable changes in 1.0.2

  * New Kafka Streams configuration parameter `upgrade.from` added that allows rolling bounce upgrade from version 0.10.0.x 
  * See the [**Kafka Streams upgrade guide**](/37/documentation/streams/upgrade-guide.html) for details about this new config. 


### Notable changes in 1.0.1

  * Restored binary compatibility of AdminClient's Options classes (e.g. CreateTopicsOptions, DeleteTopicsOptions, etc.) with 0.11.0.x. Binary (but not source) compatibility had been broken inadvertently in 1.0.0.



### Notable changes in 1.0.0

  * Topic deletion is now enabled by default, since the functionality is now stable. Users who wish to to retain the previous behavior should set the broker config `delete.topic.enable` to `false`. Keep in mind that topic deletion removes data and the operation is not reversible (i.e. there is no "undelete" operation)
  * For topics that support timestamp search if no offset can be found for a partition, that partition is now included in the search result with a null offset value. Previously, the partition was not included in the map. This change was made to make the search behavior consistent with the case of topics not supporting timestamp search. 
  * If the `inter.broker.protocol.version` is 1.0 or later, a broker will now stay online to serve replicas on live log directories even if there are offline log directories. A log directory may become offline due to IOException caused by hardware failure. Users need to monitor the per-broker metric `offlineLogDirectoryCount` to check whether there is offline log directory. 
  * Added KafkaStorageException which is a retriable exception. KafkaStorageException will be converted to NotLeaderForPartitionException in the response if the version of the client's FetchRequest or ProducerRequest does not support KafkaStorageException. 
  * -XX:+DisableExplicitGC was replaced by -XX:+ExplicitGCInvokesConcurrent in the default JVM settings. This helps avoid out of memory exceptions during allocation of native memory by direct buffers in some cases.
  * The overridden `handleError` method implementations have been removed from the following deprecated classes in the `kafka.api` package: `FetchRequest`, `GroupCoordinatorRequest`, `OffsetCommitRequest`, `OffsetFetchRequest`, `OffsetRequest`, `ProducerRequest`, and `TopicMetadataRequest`. This was only intended for use on the broker, but it is no longer in use and the implementations have not been maintained. A stub implementation has been retained for binary compatibility.
  * The Java clients and tools now accept any string as a client-id.
  * The deprecated tool `kafka-consumer-offset-checker.sh` has been removed. Use `kafka-consumer-groups.sh` to get consumer group details.
  * SimpleAclAuthorizer now logs access denials to the authorizer log by default.
  * Authentication failures are now reported to clients as one of the subclasses of `AuthenticationException`. No retries will be performed if a client connection fails authentication.
  * Custom `SaslServer` implementations may throw `SaslAuthenticationException` to provide an error message to return to clients indicating the reason for authentication failure. Implementors should take care not to include any security-critical information in the exception message that should not be leaked to unauthenticated clients.
  * The `app-info` mbean registered with JMX to provide version and commit id will be deprecated and replaced with metrics providing these attributes.
  * Kafka metrics may now contain non-numeric values. `org.apache.kafka.common.Metric#value()` has been deprecated and will return `0.0` in such cases to minimise the probability of breaking users who read the value of every client metric (via a `MetricsReporter` implementation or by calling the `metrics()` method). `org.apache.kafka.common.Metric#metricValue()` can be used to retrieve numeric and non-numeric metric values.
  * Every Kafka rate metric now has a corresponding cumulative count metric with the suffix `-total` to simplify downstream processing. For example, `records-consumed-rate` has a corresponding metric named `records-consumed-total`.
  * Mx4j will only be enabled if the system property `kafka_mx4jenable` is set to `true`. Due to a logic inversion bug, it was previously enabled by default and disabled if `kafka_mx4jenable` was set to `true`.
  * The package `org.apache.kafka.common.security.auth` in the clients jar has been made public and added to the javadocs. Internal classes which had previously been located in this package have been moved elsewhere.
  * When using an Authorizer and a user doesn't have required permissions on a topic, the broker will return TOPIC_AUTHORIZATION_FAILED errors to requests irrespective of topic existence on broker. If the user have required permissions and the topic doesn't exists, then the UNKNOWN_TOPIC_OR_PARTITION error code will be returned. 
  * config/consumer.properties file updated to use new consumer config properties.



### New Protocol Versions

  * [KIP-112](https://cwiki.apache.org/confluence/display/KAFKA/KIP-112%3A+Handle+disk+failure+for+JBOD): LeaderAndIsrRequest v1 introduces a partition-level `is_new` field. 
  * [KIP-112](https://cwiki.apache.org/confluence/display/KAFKA/KIP-112%3A+Handle+disk+failure+for+JBOD): UpdateMetadataRequest v4 introduces a partition-level `offline_replicas` field. 
  * [KIP-112](https://cwiki.apache.org/confluence/display/KAFKA/KIP-112%3A+Handle+disk+failure+for+JBOD): MetadataResponse v5 introduces a partition-level `offline_replicas` field. 
  * [KIP-112](https://cwiki.apache.org/confluence/display/KAFKA/KIP-112%3A+Handle+disk+failure+for+JBOD): ProduceResponse v4 introduces error code for KafkaStorageException. 
  * [KIP-112](https://cwiki.apache.org/confluence/display/KAFKA/KIP-112%3A+Handle+disk+failure+for+JBOD): FetchResponse v6 introduces error code for KafkaStorageException. 
  * [KIP-152](https://cwiki.apache.org/confluence/display/KAFKA/KIP-152+-+Improve+diagnostics+for+SASL+authentication+failures): SaslAuthenticate request has been added to enable reporting of authentication failures. This request will be used if the SaslHandshake request version is greater than 0. 



### Upgrading a 0.11.0 Kafka Streams Application

  * Upgrading your Streams application from 0.11.0 to 1.0 does not require a broker upgrade. A Kafka Streams 1.0 application can connect to 0.11.0, 0.10.2 and 0.10.1 brokers (it is not possible to connect to 0.10.0 brokers though). However, Kafka Streams 1.0 requires 0.10 message format or newer and does not work with older message formats. 
  * If you are monitoring on streams metrics, you will need make some changes to the metrics names in your reporting and monitoring code, because the metrics sensor hierarchy was changed. 
  * There are a few public APIs including `ProcessorContext#schedule()`, `Processor#punctuate()` and `KStreamBuilder`, `TopologyBuilder` are being deprecated by new APIs. We recommend making corresponding code changes, which should be very minor since the new APIs look quite similar, when you upgrade. 
  * See [Streams API changes in 1.0.0](/37/documentation/streams/upgrade-guide#streams_api_changes_100) for more details. 



### Upgrading a 0.10.2 Kafka Streams Application

  * Upgrading your Streams application from 0.10.2 to 1.0 does not require a broker upgrade. A Kafka Streams 1.0 application can connect to 1.0, 0.11.0, 0.10.2 and 0.10.1 brokers (it is not possible to connect to 0.10.0 brokers though). 
  * If you are monitoring on streams metrics, you will need make some changes to the metrics names in your reporting and monitoring code, because the metrics sensor hierarchy was changed. 
  * There are a few public APIs including `ProcessorContext#schedule()`, `Processor#punctuate()` and `KStreamBuilder`, `TopologyBuilder` are being deprecated by new APIs. We recommend making corresponding code changes, which should be very minor since the new APIs look quite similar, when you upgrade. 
  * If you specify customized `key.serde`, `value.serde` and `timestamp.extractor` in configs, it is recommended to use their replaced configure parameter as these configs are deprecated. 
  * See [Streams API changes in 0.11.0](/37/documentation/streams/upgrade-guide#streams_api_changes_0110) for more details. 



### Upgrading a 0.10.1 Kafka Streams Application

  * Upgrading your Streams application from 0.10.1 to 1.0 does not require a broker upgrade. A Kafka Streams 1.0 application can connect to 1.0, 0.11.0, 0.10.2 and 0.10.1 brokers (it is not possible to connect to 0.10.0 brokers though). 
  * You need to recompile your code. Just swapping the Kafka Streams library jar file will not work and will break your application. 
  * If you are monitoring on streams metrics, you will need make some changes to the metrics names in your reporting and monitoring code, because the metrics sensor hierarchy was changed. 
  * There are a few public APIs including `ProcessorContext#schedule()`, `Processor#punctuate()` and `KStreamBuilder`, `TopologyBuilder` are being deprecated by new APIs. We recommend making corresponding code changes, which should be very minor since the new APIs look quite similar, when you upgrade. 
  * If you specify customized `key.serde`, `value.serde` and `timestamp.extractor` in configs, it is recommended to use their replaced configure parameter as these configs are deprecated. 
  * If you use a custom (i.e., user implemented) timestamp extractor, you will need to update this code, because the `TimestampExtractor` interface was changed. 
  * If you register custom metrics, you will need to update this code, because the `StreamsMetric` interface was changed. 
  * See [Streams API changes in 1.0.0](/37/documentation/streams/upgrade-guide#streams_api_changes_100), [Streams API changes in 0.11.0](/37/documentation/streams/upgrade-guide#streams_api_changes_0110) and [Streams API changes in 0.10.2](/37/documentation/streams/upgrade-guide#streams_api_changes_0102) for more details. 



### Upgrading a 0.10.0 Kafka Streams Application

  * Upgrading your Streams application from 0.10.0 to 1.0 does require a broker upgrade because a Kafka Streams 1.0 application can only connect to 0.1, 0.11.0, 0.10.2, or 0.10.1 brokers. 
  * There are couple of API changes, that are not backward compatible (cf. [Streams API changes in 1.0.0](/37/documentation/streams/upgrade-guide#streams_api_changes_100), [Streams API changes in 0.11.0](/37/documentation/streams#streams_api_changes_0110), [Streams API changes in 0.10.2](/37/documentation/streams#streams_api_changes_0102), and [Streams API changes in 0.10.1](/37/documentation/streams#streams_api_changes_0101) for more details). Thus, you need to update and recompile your code. Just swapping the Kafka Streams library jar file will not work and will break your application. 
  * Upgrading from 0.10.0.x to 1.0.2 requires two rolling bounces with config `upgrade.from="0.10.0"` set for first upgrade phase (cf. [KIP-268](https://cwiki.apache.org/confluence/display/KAFKA/KIP-268%3A+Simplify+Kafka+Streams+Rebalance+Metadata+Upgrade)). As an alternative, an offline upgrade is also possible. 
    * prepare your application instances for a rolling bounce and make sure that config `upgrade.from` is set to `"0.10.0"` for new version 0.11.0.3 
    * bounce each instance of your application once 
    * prepare your newly deployed 1.0.2 application instances for a second round of rolling bounces; make sure to remove the value for config `upgrade.from`
    * bounce each instance of your application once more to complete the upgrade 
  * Upgrading from 0.10.0.x to 1.0.0 or 1.0.1 requires an offline upgrade (rolling bounce upgrade is not supported) 
    * stop all old (0.10.0.x) application instances 
    * update your code and swap old code and jar file with new code and new jar file 
    * restart all new (1.0.0 or 1.0.1) application instances 



## Upgrading from 0.8.x, 0.9.x, 0.10.0.x, 0.10.1.x or 0.10.2.x to 0.11.0.0

Kafka 0.11.0.0 introduces a new message format version as well as wire protocol changes. By following the recommended rolling upgrade plan below, you guarantee no downtime during the upgrade. However, please review the notable changes in 0.11.0.0 before upgrading. 

Starting with version 0.10.2, Java clients (producer and consumer) have acquired the ability to communicate with older brokers. Version 0.11.0 clients can talk to version 0.10.0 or newer brokers. However, if your brokers are older than 0.10.0, you must upgrade all the brokers in the Kafka cluster before upgrading your clients. Version 0.11.0 brokers support 0.8.x and newer clients. 

**For a rolling upgrade:**

  1. Update server.properties on all brokers and add the following properties. CURRENT_KAFKA_VERSION refers to the version you are upgrading from. CURRENT_MESSAGE_FORMAT_VERSION refers to the current message format version currently in use. If you have not overridden the message format previously, then CURRENT_MESSAGE_FORMAT_VERSION should be set to match CURRENT_KAFKA_VERSION. 
     * inter.broker.protocol.version=CURRENT_KAFKA_VERSION (e.g. 0.8.2, 0.9.0, 0.10.0, 0.10.1 or 0.10.2).
     * log.message.format.version=CURRENT_MESSAGE_FORMAT_VERSION (See potential performance impact following the upgrade for the details on what this configuration does.)
  2. Upgrade the brokers one at a time: shut down the broker, update the code, and restart it. 
  3. Once the entire cluster is upgraded, bump the protocol version by editing `inter.broker.protocol.version` and setting it to 0.11.0, but do not change `log.message.format.version` yet. 
  4. Restart the brokers one by one for the new protocol version to take effect. 
  5. Once all (or most) consumers have been upgraded to 0.11.0 or later, then change log.message.format.version to 0.11.0 on each broker and restart them one by one. Note that the older Scala consumer does not support the new message format, so to avoid the performance cost of down-conversion (or to take advantage of exactly once semantics), the new Java consumer must be used.



**Additional Upgrade Notes:**

  1. If you are willing to accept downtime, you can simply take all the brokers down, update the code and start them back up. They will start with the new protocol by default.
  2. Bumping the protocol version and restarting can be done any time after the brokers are upgraded. It does not have to be immediately after. Similarly for the message format version.
  3. It is also possible to enable the 0.11.0 message format on individual topics using the topic admin tool (`bin/kafka-topics.sh`) prior to updating the global setting `log.message.format.version`.
  4. If you are upgrading from a version prior to 0.10.0, it is NOT necessary to first update the message format to 0.10.0 before you switch to 0.11.0.



### Upgrading a 0.10.2 Kafka Streams Application

  * Upgrading your Streams application from 0.10.2 to 0.11.0 does not require a broker upgrade. A Kafka Streams 0.11.0 application can connect to 0.11.0, 0.10.2 and 0.10.1 brokers (it is not possible to connect to 0.10.0 brokers though). 
  * If you specify customized `key.serde`, `value.serde` and `timestamp.extractor` in configs, it is recommended to use their replaced configure parameter as these configs are deprecated. 
  * See [Streams API changes in 0.11.0](/37/documentation/streams/upgrade-guide#streams_api_changes_0110) for more details. 



### Upgrading a 0.10.1 Kafka Streams Application

  * Upgrading your Streams application from 0.10.1 to 0.11.0 does not require a broker upgrade. A Kafka Streams 0.11.0 application can connect to 0.11.0, 0.10.2 and 0.10.1 brokers (it is not possible to connect to 0.10.0 brokers though). 
  * You need to recompile your code. Just swapping the Kafka Streams library jar file will not work and will break your application. 
  * If you specify customized `key.serde`, `value.serde` and `timestamp.extractor` in configs, it is recommended to use their replaced configure parameter as these configs are deprecated. 
  * If you use a custom (i.e., user implemented) timestamp extractor, you will need to update this code, because the `TimestampExtractor` interface was changed. 
  * If you register custom metrics, you will need to update this code, because the `StreamsMetric` interface was changed. 
  * See [Streams API changes in 0.11.0](/37/documentation/streams/upgrade-guide#streams_api_changes_0110) and [Streams API changes in 0.10.2](/37/documentation/streams/upgrade-guide#streams_api_changes_0102) for more details. 



### Upgrading a 0.10.0 Kafka Streams Application

  * Upgrading your Streams application from 0.10.0 to 0.11.0 does require a broker upgrade because a Kafka Streams 0.11.0 application can only connect to 0.11.0, 0.10.2, or 0.10.1 brokers. 
  * There are couple of API changes, that are not backward compatible (cf. [Streams API changes in 0.11.0](/37/documentation/streams#streams_api_changes_0110), [Streams API changes in 0.10.2](/37/documentation/streams#streams_api_changes_0102), and [Streams API changes in 0.10.1](/37/documentation/streams#streams_api_changes_0101) for more details). Thus, you need to update and recompile your code. Just swapping the Kafka Streams library jar file will not work and will break your application. 
  * Upgrading from 0.10.0.x to 0.11.0.3 requires two rolling bounces with config `upgrade.from="0.10.0"` set for first upgrade phase (cf. [KIP-268](https://cwiki.apache.org/confluence/display/KAFKA/KIP-268%3A+Simplify+Kafka+Streams+Rebalance+Metadata+Upgrade)). As an alternative, an offline upgrade is also possible. 
    * prepare your application instances for a rolling bounce and make sure that config `upgrade.from` is set to `"0.10.0"` for new version 0.11.0.3 
    * bounce each instance of your application once 
    * prepare your newly deployed 0.11.0.3 application instances for a second round of rolling bounces; make sure to remove the value for config `upgrade.from`
    * bounce each instance of your application once more to complete the upgrade 
  * Upgrading from 0.10.0.x to 0.11.0.0, 0.11.0.1, or 0.11.0.2 requires an offline upgrade (rolling bounce upgrade is not supported) 
    * stop all old (0.10.0.x) application instances 
    * update your code and swap old code and jar file with new code and new jar file 
    * restart all new (0.11.0.0 , 0.11.0.1, or 0.11.0.2) application instances 



### Notable changes in 0.11.0.3

  * New Kafka Streams configuration parameter `upgrade.from` added that allows rolling bounce upgrade from version 0.10.0.x 
  * See the [**Kafka Streams upgrade guide**](/37/documentation/streams/upgrade-guide.html) for details about this new config. 


### Notable changes in 0.11.0.0

  * Unclean leader election is now disabled by default. The new default favors durability over availability. Users who wish to to retain the previous behavior should set the broker config `unclean.leader.election.enable` to `true`.
  * Producer configs `block.on.buffer.full`, `metadata.fetch.timeout.ms` and `timeout.ms` have been removed. They were initially deprecated in Kafka 0.9.0.0.
  * The `offsets.topic.replication.factor` broker config is now enforced upon auto topic creation. Internal auto topic creation will fail with a GROUP_COORDINATOR_NOT_AVAILABLE error until the cluster size meets this replication factor requirement.
  * When compressing data with snappy, the producer and broker will use the compression scheme's default block size (2 x 32 KB) instead of 1 KB in order to improve the compression ratio. There have been reports of data compressed with the smaller block size being 50% larger than when compressed with the larger block size. For the snappy case, a producer with 5000 partitions will require an additional 315 MB of JVM heap.
  * Similarly, when compressing data with gzip, the producer and broker will use 8 KB instead of 1 KB as the buffer size. The default for gzip is excessively low (512 bytes). 
  * The broker configuration `max.message.bytes` now applies to the total size of a batch of messages. Previously the setting applied to batches of compressed messages, or to non-compressed messages individually. A message batch may consist of only a single message, so in most cases, the limitation on the size of individual messages is only reduced by the overhead of the batch format. However, there are some subtle implications for message format conversion (see below for more detail). Note also that while previously the broker would ensure that at least one message is returned in each fetch request (regardless of the total and partition-level fetch sizes), the same behavior now applies to one message batch.
  * GC log rotation is enabled by default, see KAFKA-3754 for details.
  * Deprecated constructors of RecordMetadata, MetricName and Cluster classes have been removed.
  * Added user headers support through a new Headers interface providing user headers read and write access.
  * ProducerRecord and ConsumerRecord expose the new Headers API via `Headers headers()` method call.
  * ExtendedSerializer and ExtendedDeserializer interfaces are introduced to support serialization and deserialization for headers. Headers will be ignored if the configured serializer and deserializer are not the above classes.
  * A new config, `group.initial.rebalance.delay.ms`, was introduced. This config specifies the time, in milliseconds, that the `GroupCoordinator` will delay the initial consumer rebalance. The rebalance will be further delayed by the value of `group.initial.rebalance.delay.ms` as new members join the group, up to a maximum of `max.poll.interval.ms`. The default value for this is 3 seconds. During development and testing it might be desirable to set this to 0 in order to not delay test execution time. 
  * `org.apache.kafka.common.Cluster#partitionsForTopic`, `partitionsForNode` and `availablePartitionsForTopic` methods will return an empty list instead of `null` (which is considered a bad practice) in case the metadata for the required topic does not exist. 
  * Streams API configuration parameters `timestamp.extractor`, `key.serde`, and `value.serde` were deprecated and replaced by `default.timestamp.extractor`, `default.key.serde`, and `default.value.serde`, respectively. 
  * For offset commit failures in the Java consumer's `commitAsync` APIs, we no longer expose the underlying cause when instances of `RetriableCommitFailedException` are passed to the commit callback. See [KAFKA-5052](https://issues.apache.org/jira/browse/KAFKA-5052) for more detail. 



### New Protocol Versions

  * [KIP-107](https://cwiki.apache.org/confluence/display/KAFKA/KIP-107%3A+Add+purgeDataBefore\(\)+API+in+AdminClient): FetchRequest v5 introduces a partition-level `log_start_offset` field. 
  * [KIP-107](https://cwiki.apache.org/confluence/display/KAFKA/KIP-107%3A+Add+purgeDataBefore\(\)+API+in+AdminClient): FetchResponse v5 introduces a partition-level `log_start_offset` field. 
  * [KIP-82](https://cwiki.apache.org/confluence/display/KAFKA/KIP-82+-+Add+Record+Headers): ProduceRequest v3 introduces an array of `header` in the message protocol, containing `key` field and `value` field.
  * [KIP-82](https://cwiki.apache.org/confluence/display/KAFKA/KIP-82+-+Add+Record+Headers): FetchResponse v5 introduces an array of `header` in the message protocol, containing `key` field and `value` field.



### Notes on Exactly Once Semantics

Kafka 0.11.0 includes support for idempotent and transactional capabilities in the producer. Idempotent delivery ensures that messages are delivered exactly once to a particular topic partition during the lifetime of a single producer. Transactional delivery allows producers to send data to multiple partitions such that either all messages are successfully delivered, or none of them are. Together, these capabilities enable "exactly once semantics" in Kafka. More details on these features are available in the user guide, but below we add a few specific notes on enabling them in an upgraded cluster. Note that enabling EoS is not required and there is no impact on the broker's behavior if unused.

  1. Only the new Java producer and consumer support exactly once semantics.
  2. These features depend crucially on the 0.11.0 message format. Attempting to use them on an older format will result in unsupported version errors.
  3. Transaction state is stored in a new internal topic `__transaction_state`. This topic is not created until the the first attempt to use a transactional request API. Similar to the consumer offsets topic, there are several settings to control the topic's configuration. For example, `transaction.state.log.min.isr` controls the minimum ISR for this topic. See the configuration section in the user guide for a full list of options.
  4. For secure clusters, the transactional APIs require new ACLs which can be turned on with the `bin/kafka-acls.sh`. tool.
  5. EoS in Kafka introduces new request APIs and modifies several existing ones. See [KIP-98](https://cwiki.apache.org/confluence/display/KAFKA/KIP-98+-+Exactly+Once+Delivery+and+Transactional+Messaging#KIP-98-ExactlyOnceDeliveryandTransactionalMessaging-RPCProtocolSummary) for the full details



### Notes on the new message format in 0.11.0

The 0.11.0 message format includes several major enhancements in order to support better delivery semantics for the producer (see [KIP-98](https://cwiki.apache.org/confluence/display/KAFKA/KIP-98+-+Exactly+Once+Delivery+and+Transactional+Messaging)) and improved replication fault tolerance (see [KIP-101](https://cwiki.apache.org/confluence/display/KAFKA/KIP-101+-+Alter+Replication+Protocol+to+use+Leader+Epoch+rather+than+High+Watermark+for+Truncation)). Although the new format contains more information to make these improvements possible, we have made the batch format much more efficient. As long as the number of messages per batch is more than 2, you can expect lower overall overhead. For smaller batches, however, there may be a small performance impact. See [here](bit.ly/kafka-eos-perf) for the results of our initial performance analysis of the new message format. You can also find more detail on the message format in the [KIP-98](https://cwiki.apache.org/confluence/display/KAFKA/KIP-98+-+Exactly+Once+Delivery+and+Transactional+Messaging#KIP-98-ExactlyOnceDeliveryandTransactionalMessaging-MessageFormat) proposal. 

One of the notable differences in the new message format is that even uncompressed messages are stored together as a single batch. This has a few implications for the broker configuration `max.message.bytes`, which limits the size of a single batch. First, if an older client produces messages to a topic partition using the old format, and the messages are individually smaller than `max.message.bytes`, the broker may still reject them after they are merged into a single batch during the up-conversion process. Generally this can happen when the aggregate size of the individual messages is larger than `max.message.bytes`. There is a similar effect for older consumers reading messages down-converted from the new format: if the fetch size is not set at least as large as `max.message.bytes`, the consumer may not be able to make progress even if the individual uncompressed messages are smaller than the configured fetch size. This behavior does not impact the Java client for 0.10.1.0 and later since it uses an updated fetch protocol which ensures that at least one message can be returned even if it exceeds the fetch size. To get around these problems, you should ensure 1) that the producer's batch size is not set larger than `max.message.bytes`, and 2) that the consumer's fetch size is set at least as large as `max.message.bytes`. 

Most of the discussion on the performance impact of upgrading to the 0.10.0 message format remains pertinent to the 0.11.0 upgrade. This mainly affects clusters that are not secured with TLS since "zero-copy" transfer is already not possible in that case. In order to avoid the cost of down-conversion, you should ensure that consumer applications are upgraded to the latest 0.11.0 client. Significantly, since the old consumer has been deprecated in 0.11.0.0, it does not support the new message format. You must upgrade to use the new consumer to use the new message format without the cost of down-conversion. Note that 0.11.0 consumers support backwards compatibility with 0.10.0 brokers and upward, so it is possible to upgrade the clients first before the brokers. 

## Upgrading from 0.8.x, 0.9.x, 0.10.0.x or 0.10.1.x to 0.10.2.0

0.10.2.0 has wire protocol changes. By following the recommended rolling upgrade plan below, you guarantee no downtime during the upgrade. However, please review the notable changes in 0.10.2.0 before upgrading. 

Starting with version 0.10.2, Java clients (producer and consumer) have acquired the ability to communicate with older brokers. Version 0.10.2 clients can talk to version 0.10.0 or newer brokers. However, if your brokers are older than 0.10.0, you must upgrade all the brokers in the Kafka cluster before upgrading your clients. Version 0.10.2 brokers support 0.8.x and newer clients. 

**For a rolling upgrade:**

  1. Update server.properties file on all brokers and add the following properties: 
     * inter.broker.protocol.version=CURRENT_KAFKA_VERSION (e.g. 0.8.2, 0.9.0, 0.10.0 or 0.10.1).
     * log.message.format.version=CURRENT_KAFKA_VERSION (See potential performance impact following the upgrade for the details on what this configuration does.) 
  2. Upgrade the brokers one at a time: shut down the broker, update the code, and restart it. 
  3. Once the entire cluster is upgraded, bump the protocol version by editing inter.broker.protocol.version and setting it to 0.10.2. 
  4. If your previous message format is 0.10.0, change log.message.format.version to 0.10.2 (this is a no-op as the message format is the same for 0.10.0, 0.10.1 and 0.10.2). If your previous message format version is lower than 0.10.0, do not change log.message.format.version yet - this parameter should only change once all consumers have been upgraded to 0.10.0.0 or later.
  5. Restart the brokers one by one for the new protocol version to take effect. 
  6. If log.message.format.version is still lower than 0.10.0 at this point, wait until all consumers have been upgraded to 0.10.0 or later, then change log.message.format.version to 0.10.2 on each broker and restart them one by one. 



**Note:** If you are willing to accept downtime, you can simply take all the brokers down, update the code and start all of them. They will start with the new protocol by default. 

**Note:** Bumping the protocol version and restarting can be done any time after the brokers were upgraded. It does not have to be immediately after. 

### Upgrading a 0.10.1 Kafka Streams Application

  * Upgrading your Streams application from 0.10.1 to 0.10.2 does not require a broker upgrade. A Kafka Streams 0.10.2 application can connect to 0.10.2 and 0.10.1 brokers (it is not possible to connect to 0.10.0 brokers though). 
  * You need to recompile your code. Just swapping the Kafka Streams library jar file will not work and will break your application. 
  * If you use a custom (i.e., user implemented) timestamp extractor, you will need to update this code, because the `TimestampExtractor` interface was changed. 
  * If you register custom metrics, you will need to update this code, because the `StreamsMetric` interface was changed. 
  * See [Streams API changes in 0.10.2](/37/documentation/streams/upgrade-guide#streams_api_changes_0102) for more details. 



### Upgrading a 0.10.0 Kafka Streams Application

  * Upgrading your Streams application from 0.10.0 to 0.10.2 does require a broker upgrade because a Kafka Streams 0.10.2 application can only connect to 0.10.2 or 0.10.1 brokers. 
  * There are couple of API changes, that are not backward compatible (cf. [Streams API changes in 0.10.2](/37/documentation/streams#streams_api_changes_0102) for more details). Thus, you need to update and recompile your code. Just swapping the Kafka Streams library jar file will not work and will break your application. 
  * Upgrading from 0.10.0.x to 0.10.2.2 requires two rolling bounces with config `upgrade.from="0.10.0"` set for first upgrade phase (cf. [KIP-268](https://cwiki.apache.org/confluence/display/KAFKA/KIP-268%3A+Simplify+Kafka+Streams+Rebalance+Metadata+Upgrade)). As an alternative, an offline upgrade is also possible. 
    * prepare your application instances for a rolling bounce and make sure that config `upgrade.from` is set to `"0.10.0"` for new version 0.10.2.2 
    * bounce each instance of your application once 
    * prepare your newly deployed 0.10.2.2 application instances for a second round of rolling bounces; make sure to remove the value for config `upgrade.from`
    * bounce each instance of your application once more to complete the upgrade 
  * Upgrading from 0.10.0.x to 0.10.2.0 or 0.10.2.1 requires an offline upgrade (rolling bounce upgrade is not supported) 
    * stop all old (0.10.0.x) application instances 
    * update your code and swap old code and jar file with new code and new jar file 
    * restart all new (0.10.2.0 or 0.10.2.1) application instances 



### Notable changes in 0.10.2.2

  * New configuration parameter `upgrade.from` added that allows rolling bounce upgrade from version 0.10.0.x 



### Notable changes in 0.10.2.1

  * The default values for two configurations of the StreamsConfig class were changed to improve the resiliency of Kafka Streams applications. The internal Kafka Streams producer `retries` default value was changed from 0 to 10. The internal Kafka Streams consumer `max.poll.interval.ms` default value was changed from 300000 to `Integer.MAX_VALUE`. 



### Notable changes in 0.10.2.0

  * The Java clients (producer and consumer) have acquired the ability to communicate with older brokers. Version 0.10.2 clients can talk to version 0.10.0 or newer brokers. Note that some features are not available or are limited when older brokers are used. 
  * Several methods on the Java consumer may now throw `InterruptException` if the calling thread is interrupted. Please refer to the `KafkaConsumer` Javadoc for a more in-depth explanation of this change.
  * Java consumer now shuts down gracefully. By default, the consumer waits up to 30 seconds to complete pending requests. A new close API with timeout has been added to `KafkaConsumer` to control the maximum wait time.
  * Multiple regular expressions separated by commas can be passed to MirrorMaker with the new Java consumer via the --whitelist option. This makes the behaviour consistent with MirrorMaker when used the old Scala consumer.
  * Upgrading your Streams application from 0.10.1 to 0.10.2 does not require a broker upgrade. A Kafka Streams 0.10.2 application can connect to 0.10.2 and 0.10.1 brokers (it is not possible to connect to 0.10.0 brokers though).
  * The Zookeeper dependency was removed from the Streams API. The Streams API now uses the Kafka protocol to manage internal topics instead of modifying Zookeeper directly. This eliminates the need for privileges to access Zookeeper directly and "StreamsConfig.ZOOKEEPER_CONFIG" should not be set in the Streams app any more. If the Kafka cluster is secured, Streams apps must have the required security privileges to create new topics.
  * Several new fields including "security.protocol", "connections.max.idle.ms", "retry.backoff.ms", "reconnect.backoff.ms" and "request.timeout.ms" were added to StreamsConfig class. User should pay attention to the default values and set these if needed. For more details please refer to [3.5 Kafka Streams Configs](/37/documentation/#streamsconfigs).



### New Protocol Versions

  * [KIP-88](https://cwiki.apache.org/confluence/display/KAFKA/KIP-88%3A+OffsetFetch+Protocol+Update): OffsetFetchRequest v2 supports retrieval of offsets for all topics if the `topics` array is set to `null`. 
  * [KIP-88](https://cwiki.apache.org/confluence/display/KAFKA/KIP-88%3A+OffsetFetch+Protocol+Update): OffsetFetchResponse v2 introduces a top-level `error_code` field. 
  * [KIP-103](https://cwiki.apache.org/confluence/display/KAFKA/KIP-103%3A+Separation+of+Internal+and+External+traffic): UpdateMetadataRequest v3 introduces a `listener_name` field to the elements of the `end_points` array. 
  * [KIP-108](https://cwiki.apache.org/confluence/display/KAFKA/KIP-108%3A+Create+Topic+Policy): CreateTopicsRequest v1 introduces a `validate_only` field. 
  * [KIP-108](https://cwiki.apache.org/confluence/display/KAFKA/KIP-108%3A+Create+Topic+Policy): CreateTopicsResponse v1 introduces an `error_message` field to the elements of the `topic_errors` array. 



## Upgrading from 0.8.x, 0.9.x or 0.10.0.X to 0.10.1.0

0.10.1.0 has wire protocol changes. By following the recommended rolling upgrade plan below, you guarantee no downtime during the upgrade. However, please notice the Potential breaking changes in 0.10.1.0 before upgrade.   
Note: Because new protocols are introduced, it is important to upgrade your Kafka clusters before upgrading your clients (i.e. 0.10.1.x clients only support 0.10.1.x or later brokers while 0.10.1.x brokers also support older clients). 

**For a rolling upgrade:**

  1. Update server.properties file on all brokers and add the following properties: 
     * inter.broker.protocol.version=CURRENT_KAFKA_VERSION (e.g. 0.8.2.0, 0.9.0.0 or 0.10.0.0).
     * log.message.format.version=CURRENT_KAFKA_VERSION (See potential performance impact following the upgrade for the details on what this configuration does.) 
  2. Upgrade the brokers one at a time: shut down the broker, update the code, and restart it. 
  3. Once the entire cluster is upgraded, bump the protocol version by editing inter.broker.protocol.version and setting it to 0.10.1.0. 
  4. If your previous message format is 0.10.0, change log.message.format.version to 0.10.1 (this is a no-op as the message format is the same for both 0.10.0 and 0.10.1). If your previous message format version is lower than 0.10.0, do not change log.message.format.version yet - this parameter should only change once all consumers have been upgraded to 0.10.0.0 or later.
  5. Restart the brokers one by one for the new protocol version to take effect. 
  6. If log.message.format.version is still lower than 0.10.0 at this point, wait until all consumers have been upgraded to 0.10.0 or later, then change log.message.format.version to 0.10.1 on each broker and restart them one by one. 



**Note:** If you are willing to accept downtime, you can simply take all the brokers down, update the code and start all of them. They will start with the new protocol by default. 

**Note:** Bumping the protocol version and restarting can be done any time after the brokers were upgraded. It does not have to be immediately after. 

### Notable changes in 0.10.1.2

  * New configuration parameter `upgrade.from` added that allows rolling bounce upgrade from version 0.10.0.x 



### Potential breaking changes in 0.10.1.0

  * The log retention time is no longer based on last modified time of the log segments. Instead it will be based on the largest timestamp of the messages in a log segment.
  * The log rolling time is no longer depending on log segment create time. Instead it is now based on the timestamp in the messages. More specifically. if the timestamp of the first message in the segment is T, the log will be rolled out when a new message has a timestamp greater than or equal to T + log.roll.ms 
  * The open file handlers of 0.10.0 will increase by ~33% because of the addition of time index files for each segment.
  * The time index and offset index share the same index size configuration. Since each time index entry is 1.5x the size of offset index entry. User may need to increase log.index.size.max.bytes to avoid potential frequent log rolling. 
  * Due to the increased number of index files, on some brokers with large amount the log segments (e.g. >15K), the log loading process during the broker startup could be longer. Based on our experiment, setting the num.recovery.threads.per.data.dir to one may reduce the log loading time. 



### Upgrading a 0.10.0 Kafka Streams Application

  * Upgrading your Streams application from 0.10.0 to 0.10.1 does require a broker upgrade because a Kafka Streams 0.10.1 application can only connect to 0.10.1 brokers. 
  * There are couple of API changes, that are not backward compatible (cf. [Streams API changes in 0.10.1](/37/documentation/streams/upgrade-guide#streams_api_changes_0101) for more details). Thus, you need to update and recompile your code. Just swapping the Kafka Streams library jar file will not work and will break your application. 
  * Upgrading from 0.10.0.x to 0.10.1.2 requires two rolling bounces with config `upgrade.from="0.10.0"` set for first upgrade phase (cf. [KIP-268](https://cwiki.apache.org/confluence/display/KAFKA/KIP-268%3A+Simplify+Kafka+Streams+Rebalance+Metadata+Upgrade)). As an alternative, an offline upgrade is also possible. 
    * prepare your application instances for a rolling bounce and make sure that config `upgrade.from` is set to `"0.10.0"` for new version 0.10.1.2 
    * bounce each instance of your application once 
    * prepare your newly deployed 0.10.1.2 application instances for a second round of rolling bounces; make sure to remove the value for config `upgrade.from`
    * bounce each instance of your application once more to complete the upgrade 
  * Upgrading from 0.10.0.x to 0.10.1.0 or 0.10.1.1 requires an offline upgrade (rolling bounce upgrade is not supported) 
    * stop all old (0.10.0.x) application instances 
    * update your code and swap old code and jar file with new code and new jar file 
    * restart all new (0.10.1.0 or 0.10.1.1) application instances 



### Notable changes in 0.10.1.0

  * The new Java consumer is no longer in beta and we recommend it for all new development. The old Scala consumers are still supported, but they will be deprecated in the next release and will be removed in a future major release. 
  * The `--new-consumer`/`--new.consumer` switch is no longer required to use tools like MirrorMaker and the Console Consumer with the new consumer; one simply needs to pass a Kafka broker to connect to instead of the ZooKeeper ensemble. In addition, usage of the Console Consumer with the old consumer has been deprecated and it will be removed in a future major release. 
  * Kafka clusters can now be uniquely identified by a cluster id. It will be automatically generated when a broker is upgraded to 0.10.1.0. The cluster id is available via the kafka.server:type=KafkaServer,name=ClusterId metric and it is part of the Metadata response. Serializers, client interceptors and metric reporters can receive the cluster id by implementing the ClusterResourceListener interface. 
  * The BrokerState "RunningAsController" (value 4) has been removed. Due to a bug, a broker would only be in this state briefly before transitioning out of it and hence the impact of the removal should be minimal. The recommended way to detect if a given broker is the controller is via the kafka.controller:type=KafkaController,name=ActiveControllerCount metric. 
  * The new Java Consumer now allows users to search offsets by timestamp on partitions. 
  * The new Java Consumer now supports heartbeating from a background thread. There is a new configuration `max.poll.interval.ms` which controls the maximum time between poll invocations before the consumer will proactively leave the group (5 minutes by default). The value of the configuration `request.timeout.ms` (default to 30 seconds) must always be smaller than `max.poll.interval.ms`(default to 5 minutes), since that is the maximum time that a JoinGroup request can block on the server while the consumer is rebalance. Finally, the default value of `session.timeout.ms` has been adjusted down to 10 seconds, and the default value of `max.poll.records` has been changed to 500.
  * When using an Authorizer and a user doesn't have **Describe** authorization on a topic, the broker will no longer return TOPIC_AUTHORIZATION_FAILED errors to requests since this leaks topic names. Instead, the UNKNOWN_TOPIC_OR_PARTITION error code will be returned. This may cause unexpected timeouts or delays when using the producer and consumer since Kafka clients will typically retry automatically on unknown topic errors. You should consult the client logs if you suspect this could be happening.
  * Fetch responses have a size limit by default (50 MB for consumers and 10 MB for replication). The existing per partition limits also apply (1 MB for consumers and replication). Note that neither of these limits is an absolute maximum as explained in the next point. 
  * Consumers and replicas can make progress if a message larger than the response/partition size limit is found. More concretely, if the first message in the first non-empty partition of the fetch is larger than either or both limits, the message will still be returned. 
  * Overloaded constructors were added to `kafka.api.FetchRequest` and `kafka.javaapi.FetchRequest` to allow the caller to specify the order of the partitions (since order is significant in v3). The previously existing constructors were deprecated and the partitions are shuffled before the request is sent to avoid starvation issues. 



### New Protocol Versions

  * ListOffsetRequest v1 supports accurate offset search based on timestamps. 
  * MetadataResponse v2 introduces a new field: "cluster_id". 
  * FetchRequest v3 supports limiting the response size (in addition to the existing per partition limit), it returns messages bigger than the limits if required to make progress and the order of partitions in the request is now significant. 
  * JoinGroup v1 introduces a new field: "rebalance_timeout". 



## Upgrading from 0.8.x or 0.9.x to 0.10.0.0

0.10.0.0 has potential breaking changes (please review before upgrading) and possible  performance impact following the upgrade. By following the recommended rolling upgrade plan below, you guarantee no downtime and no performance impact during and following the upgrade.   
Note: Because new protocols are introduced, it is important to upgrade your Kafka clusters before upgrading your clients. 

**Notes to clients with version 0.9.0.0:** Due to a bug introduced in 0.9.0.0, clients that depend on ZooKeeper (old Scala high-level Consumer and MirrorMaker if used with the old consumer) will not work with 0.10.0.x brokers. Therefore, 0.9.0.0 clients should be upgraded to 0.9.0.1 **before** brokers are upgraded to 0.10.0.x. This step is not necessary for 0.8.X or 0.9.0.1 clients. 

**For a rolling upgrade:**

  1. Update server.properties file on all brokers and add the following properties: 
     * inter.broker.protocol.version=CURRENT_KAFKA_VERSION (e.g. 0.8.2 or 0.9.0.0).
     * log.message.format.version=CURRENT_KAFKA_VERSION (See potential performance impact following the upgrade for the details on what this configuration does.) 
  2. Upgrade the brokers. This can be done a broker at a time by simply bringing it down, updating the code, and restarting it. 
  3. Once the entire cluster is upgraded, bump the protocol version by editing inter.broker.protocol.version and setting it to 0.10.0.0. NOTE: You shouldn't touch log.message.format.version yet - this parameter should only change once all consumers have been upgraded to 0.10.0.0 
  4. Restart the brokers one by one for the new protocol version to take effect. 
  5. Once all consumers have been upgraded to 0.10.0, change log.message.format.version to 0.10.0 on each broker and restart them one by one. 



**Note:** If you are willing to accept downtime, you can simply take all the brokers down, update the code and start all of them. They will start with the new protocol by default. 

**Note:** Bumping the protocol version and restarting can be done any time after the brokers were upgraded. It does not have to be immediately after. 

### Potential performance impact following upgrade to 0.10.0.0

The message format in 0.10.0 includes a new timestamp field and uses relative offsets for compressed messages. The on disk message format can be configured through log.message.format.version in the server.properties file. The default on-disk message format is 0.10.0. If a consumer client is on a version before 0.10.0.0, it only understands message formats before 0.10.0. In this case, the broker is able to convert messages from the 0.10.0 format to an earlier format before sending the response to the consumer on an older version. However, the broker can't use zero-copy transfer in this case. Reports from the Kafka community on the performance impact have shown CPU utilization going from 20% before to 100% after an upgrade, which forced an immediate upgrade of all clients to bring performance back to normal. To avoid such message conversion before consumers are upgraded to 0.10.0.0, one can set log.message.format.version to 0.8.2 or 0.9.0 when upgrading the broker to 0.10.0.0. This way, the broker can still use zero-copy transfer to send the data to the old consumers. Once consumers are upgraded, one can change the message format to 0.10.0 on the broker and enjoy the new message format that includes new timestamp and improved compression. The conversion is supported to ensure compatibility and can be useful to support a few apps that have not updated to newer clients yet, but is impractical to support all consumer traffic on even an overprovisioned cluster. Therefore, it is critical to avoid the message conversion as much as possible when brokers have been upgraded but the majority of clients have not. 

For clients that are upgraded to 0.10.0.0, there is no performance impact. 

**Note:** By setting the message format version, one certifies that all existing messages are on or below that message format version. Otherwise consumers before 0.10.0.0 might break. In particular, after the message format is set to 0.10.0, one should not change it back to an earlier format as it may break consumers on versions before 0.10.0.0. 

**Note:** Due to the additional timestamp introduced in each message, producers sending small messages may see a message throughput degradation because of the increased overhead. Likewise, replication now transmits an additional 8 bytes per message. If you're running close to the network capacity of your cluster, it's possible that you'll overwhelm the network cards and see failures and performance issues due to the overload. 

**Note:** If you have enabled compression on producers, you may notice reduced producer throughput and/or lower compression rate on the broker in some cases. When receiving compressed messages, 0.10.0 brokers avoid recompressing the messages, which in general reduces the latency and improves the throughput. In certain cases, however, this may reduce the batching size on the producer, which could lead to worse throughput. If this happens, users can tune linger.ms and batch.size of the producer for better throughput. In addition, the producer buffer used for compressing messages with snappy is smaller than the one used by the broker, which may have a negative impact on the compression ratio for the messages on disk. We intend to make this configurable in a future Kafka release. 

### Potential breaking changes in 0.10.0.0

  * Starting from Kafka 0.10.0.0, the message format version in Kafka is represented as the Kafka version. For example, message format 0.9.0 refers to the highest message version supported by Kafka 0.9.0. 
  * Message format 0.10.0 has been introduced and it is used by default. It includes a timestamp field in the messages and relative offsets are used for compressed messages. 
  * ProduceRequest/Response v2 has been introduced and it is used by default to support message format 0.10.0 
  * FetchRequest/Response v2 has been introduced and it is used by default to support message format 0.10.0 
  * MessageFormatter interface was changed from `def writeTo(key: Array[Byte], value: Array[Byte], output: PrintStream)` to `def writeTo(consumerRecord: ConsumerRecord[Array[Byte], Array[Byte]], output: PrintStream)`
  * MessageReader interface was changed from `def readMessage(): KeyedMessage[Array[Byte], Array[Byte]]` to `def readMessage(): ProducerRecord[Array[Byte], Array[Byte]]`
  * MessageFormatter's package was changed from `kafka.tools` to `kafka.common`
  * MessageReader's package was changed from `kafka.tools` to `kafka.common`
  * MirrorMakerMessageHandler no longer exposes the `handle(record: MessageAndMetadata[Array[Byte], Array[Byte]])` method as it was never called. 
  * The 0.7 KafkaMigrationTool is no longer packaged with Kafka. If you need to migrate from 0.7 to 0.10.0, please migrate to 0.8 first and then follow the documented upgrade process to upgrade from 0.8 to 0.10.0. 
  * The new consumer has standardized its APIs to accept `java.util.Collection` as the sequence type for method parameters. Existing code may have to be updated to work with the 0.10.0 client library. 
  * LZ4-compressed message handling was changed to use an interoperable framing specification (LZ4f v1.5.1). To maintain compatibility with old clients, this change only applies to Message format 0.10.0 and later. Clients that Produce/Fetch LZ4-compressed messages using v0/v1 (Message format 0.9.0) should continue to use the 0.9.0 framing implementation. Clients that use Produce/Fetch protocols v2 or later should use interoperable LZ4f framing. A list of interoperable LZ4 libraries is available at <https://www.lz4.org/> 


### Notable changes in 0.10.0.0

  * Starting from Kafka 0.10.0.0, a new client library named **Kafka Streams** is available for stream processing on data stored in Kafka topics. This new client library only works with 0.10.x and upward versioned brokers due to message format changes mentioned above. For more information please read [Streams documentation](/37/documentation/streams).
  * The default value of the configuration parameter `receive.buffer.bytes` is now 64K for the new consumer.
  * The new consumer now exposes the configuration parameter `exclude.internal.topics` to restrict internal topics (such as the consumer offsets topic) from accidentally being included in regular expression subscriptions. By default, it is enabled.
  * The old Scala producer has been deprecated. Users should migrate their code to the Java producer included in the kafka-clients JAR as soon as possible. 
  * The new consumer API has been marked stable. 



## Upgrading from 0.8.0, 0.8.1.X, or 0.8.2.X to 0.9.0.0

0.9.0.0 has potential breaking changes (please review before upgrading) and an inter-broker protocol change from previous versions. This means that upgraded brokers and clients may not be compatible with older versions. It is important that you upgrade your Kafka cluster before upgrading your clients. If you are using MirrorMaker downstream clusters should be upgraded first as well. 

**For a rolling upgrade:**

  1. Update server.properties file on all brokers and add the following property: inter.broker.protocol.version=0.8.2.X 
  2. Upgrade the brokers. This can be done a broker at a time by simply bringing it down, updating the code, and restarting it. 
  3. Once the entire cluster is upgraded, bump the protocol version by editing inter.broker.protocol.version and setting it to 0.9.0.0.
  4. Restart the brokers one by one for the new protocol version to take effect 



**Note:** If you are willing to accept downtime, you can simply take all the brokers down, update the code and start all of them. They will start with the new protocol by default. 

**Note:** Bumping the protocol version and restarting can be done any time after the brokers were upgraded. It does not have to be immediately after. 

### Potential breaking changes in 0.9.0.0

  * Java 1.6 is no longer supported. 
  * Scala 2.9 is no longer supported. 
  * Broker IDs above 1000 are now reserved by default to automatically assigned broker IDs. If your cluster has existing broker IDs above that threshold make sure to increase the reserved.broker.max.id broker configuration property accordingly. 
  * Configuration parameter replica.lag.max.messages was removed. Partition leaders will no longer consider the number of lagging messages when deciding which replicas are in sync. 
  * Configuration parameter replica.lag.time.max.ms now refers not just to the time passed since last fetch request from replica, but also to time since the replica last caught up. Replicas that are still fetching messages from leaders but did not catch up to the latest messages in replica.lag.time.max.ms will be considered out of sync. 
  * Compacted topics no longer accept messages without key and an exception is thrown by the producer if this is attempted. In 0.8.x, a message without key would cause the log compaction thread to subsequently complain and quit (and stop compacting all compacted topics). 
  * MirrorMaker no longer supports multiple target clusters. As a result it will only accept a single --consumer.config parameter. To mirror multiple source clusters, you will need at least one MirrorMaker instance per source cluster, each with its own consumer configuration. 
  * Tools packaged under _org.apache.kafka.clients.tools.*_ have been moved to _org.apache.kafka.tools.*_. All included scripts will still function as usual, only custom code directly importing these classes will be affected. 
  * The default Kafka JVM performance options (KAFKA_JVM_PERFORMANCE_OPTS) have been changed in kafka-run-class.sh. 
  * The kafka-topics.sh script (kafka.admin.TopicCommand) now exits with non-zero exit code on failure. 
  * The kafka-topics.sh script (kafka.admin.TopicCommand) will now print a warning when topic names risk metric collisions due to the use of a '.' or '_' in the topic name, and error in the case of an actual collision. 
  * The kafka-console-producer.sh script (kafka.tools.ConsoleProducer) will use the Java producer instead of the old Scala producer be default, and users have to specify 'old-producer' to use the old producer. 
  * By default, all command line tools will print all logging messages to stderr instead of stdout. 



### Notable changes in 0.9.0.1

  * The new broker id generation feature can be disabled by setting broker.id.generation.enable to false. 
  * Configuration parameter log.cleaner.enable is now true by default. This means topics with a cleanup.policy=compact will now be compacted by default, and 128 MB of heap will be allocated to the cleaner process via log.cleaner.dedupe.buffer.size. You may want to review log.cleaner.dedupe.buffer.size and the other log.cleaner configuration values based on your usage of compacted topics. 
  * Default value of configuration parameter fetch.min.bytes for the new consumer is now 1 by default. 



### Deprecations in 0.9.0.0

  * Altering topic configuration from the kafka-topics.sh script (kafka.admin.TopicCommand) has been deprecated. Going forward, please use the kafka-configs.sh script (kafka.admin.ConfigCommand) for this functionality. 
  * The kafka-consumer-offset-checker.sh (kafka.tools.ConsumerOffsetChecker) has been deprecated. Going forward, please use kafka-consumer-groups.sh (kafka.admin.ConsumerGroupCommand) for this functionality. 
  * The kafka.tools.ProducerPerformance class has been deprecated. Going forward, please use org.apache.kafka.tools.ProducerPerformance for this functionality (kafka-producer-perf-test.sh will also be changed to use the new class). 
  * The producer config block.on.buffer.full has been deprecated and will be removed in future release. Currently its default value has been changed to false. The KafkaProducer will no longer throw BufferExhaustedException but instead will use max.block.ms value to block, after which it will throw a TimeoutException. If block.on.buffer.full property is set to true explicitly, it will set the max.block.ms to Long.MAX_VALUE and metadata.fetch.timeout.ms will not be honoured



## Upgrading from 0.8.1 to 0.8.2

0.8.2 is fully compatible with 0.8.1. The upgrade can be done one broker at a time by simply bringing it down, updating the code, and restarting it. 

## Upgrading from 0.8.0 to 0.8.1

0.8.1 is fully compatible with 0.8. The upgrade can be done one broker at a time by simply bringing it down, updating the code, and restarting it. 

## Upgrading from 0.7

Release 0.7 is incompatible with newer releases. Major changes were made to the API, ZooKeeper data structures, and protocol, and configuration in order to add replication (Which was missing in 0.7). The upgrade from 0.7 to later versions requires a [special tool](https://cwiki.apache.org/confluence/display/KAFKA/Migrating+from+0.7+to+0.8) for migration. This migration can be done without downtime. 
