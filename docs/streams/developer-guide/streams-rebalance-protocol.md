---
title: Streams Rebalance Protocol
description:
weight: 14
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

The Streams Rebalance Protocol is a broker-driven rebalancing system designed specifically for Kafka Streams applications. Following the pattern of KIP-848, which moved rebalance coordination of plain consumers from clients to brokers, KIP-1071 extends this model to Kafka Streams workloads.

# Overview

Instead of clients computing new assignments on the client during rebalance events involving all members of the group, assignments are computed continuously on the broker. Instead of using a consumer group, the streams application registers as a **streams group** with the broker, which manages and exposes all metadata required for coordination of the streams application instances.

This approach brings Kafka Streams coordination in line with the modern broker-driven rebalance model introduced for consumers in KIP-848, providing a dedicated group type with streams-specific semantics and metadata management.

# What's Supported in This Version

The following features are available in the current release:

* **Core Streams Group Rebalance Protocol**: The `group.protocol=streams` configuration enables the dedicated streams rebalance protocol. This separates streams groups from consumer groups and provides a streams-specific group membership lifecycle and metadata management on the broker.

* **Sticky Task Assignor**: A basic task assignment strategy that minimizes task movement during rebalances is included.

* **Interactive Query Support**: IQ operations are compatible with the new streams protocol.

* **New Admin RPC**: The StreamsGroupDescribe RPC provides streams-specific metadata separate from consumer group information, with corresponding access via the [`Admin`](/{version}/javadoc/org/apache/kafka/clients/admin/Admin.html) interface.

* **CLI Integration**: You can list, describe, and delete streams groups via the [bin/kafka-streams-groups.sh](/{version}/streams/developer-guide/kafka-streams-group-sh/) script.

* **Offline Migration**: After shutting down all members and waiting for their `session.timeout.ms` to expire (or forcing an explicit group leave), a classic group can be converted to a streams group and a streams group can be converted to a classic group. The only broker-side group data that will be preserved are the committed offsets. Internal topics (changelog and repartition topics) will continue to exist as regular Kafka topics.

# What's Not Supported in This Version

The following features are not yet available and should be avoided when using the new protocol:

* **Static Membership**: Setting a client `instance.id` will be rejected.

* **Topology Updates**: If a topology is changed significantly (e.g., by adding new source topics or changing the number of subtopologies), a new streams group must be created.

* **High Availability Assignor**: Only the sticky assignor is supported. This implies that "warmup tasks" and rack aware assignment are not supported yet.

* **Regular Expressions**: Pattern-based topic subscription is not supported.

* **Online Migration**: Group migration while the application is running is not available between the classic and new streams protocol.

* **Custom Client Supplier**: Using a custom `KafkaClientSupplier` will only allow so provide restore/global consumer, producer, and admin client. It's not possible to provide the "main" consumer when "streams" groups are enabled. 

# Why Use the Streams Rebalance Protocol?

The Streams Rebalance Protocol offers several key advantages over the classic client-driven protocol:

* **Broker-Driven Coordination**: Centralizes task assignment logic on brokers instead of the client. This provides consistent, authoritative task assignment decisions from a single coordination point, and reduces the potential for split-brain scenarios.

* **Faster, More Stable Rebalances**: Reduces rebalance duration and impact by removing the global synchronization point. This minimizes application downtime during membership changes or failures.

* **Better Observability**: Provides dedicated metrics and admin interfaces that separate streams from consumer groups, leading to clearer troubleshooting with broker-side observability. See the [streams groups metrics](/{version}/operations/monitoring#group-coordinator-monitoring) documentation for details.

# Enabling the Protocol

The Streams Rebalance Protocol is enabled by default on new clusters starting with Apache Kafka 4.2. Both brokers and clients must be running Apache Kafka 4.2 or later to use this protocol.

## Broker Configuration

The protocol is enabled by default on new Apache Kafka 4.2 clusters. To enable the feature on existing clusters (after upgrading to 4.2) or to explicitly control it:

Enable the feature:
```
bin/kafka-features.sh --bootstrap-server localhost:9092 upgrade --feature streams.version=1
```

Disable the feature:
```
bin/kafka-features.sh --bootstrap-server localhost:9092 downgrade --feature streams.version=0
```

## Client Configuration

In your Kafka Streams application configuration, set:
```
group.protocol=streams
```

# Configuration

## Broker Configuration

The following broker configurations control the behavior of streams groups. For complete details, see the [broker configuration](/{version}/configuration/broker-configs) documentation.

* [`group.coordinator.rebalance.protocols`](/{version}/configuration/broker-configs#brokerconfigs_group.coordinator.rebalance.protocols): The list of enabled rebalance protocols. `"streams"` is included in the list of protocols to enable streams groups.
* [`group.streams.session.timeout.ms`](/{version}/configuration/broker-configs#brokerconfigs_group.streams.session.timeout.ms): The default timeout for all streams group (if not specifically overwritten for a specific streams group) to detect client failures when using the streams group protocol.
* [`group.streams.min.session.timeout.ms`](/{version}/configuration/broker-configs#brokerconfigs_group.streams.min.session.timeout.ms): The minimum session timeout.
* [`group.streams.max.session.timeout.ms`](/{version}/configuration/broker-configs#brokerconfigs_group.streams.max.session.timeout.ms): The maximum session timeout.
* [`group.streams.heartbeat.interval.ms`](/{version}/configuration/broker-configs#brokerconfigs_group.streams.heartbeat.interval.ms): The default heartbeat interval given to the members.
* [`group.streams.min.heartbeat.interval.ms`](/{version}/configuration/broker-configs#brokerconfigs_group.streams.min.heartbeat.interval.ms): The minimum heartbeat interval.
* [`group.streams.max.heartbeat.interval.ms`](/{version}/configuration/broker-configs#brokerconfigs_group.streams.max.heartbeat.interval.ms): The maximum heartbeat interval.
* [`group.streams.max.size`](/{version}/configuration/broker-configs#brokerconfigs_group.streams.max.size): The maximum number of streams clients that a single streams group can accommodate.
* [`group.streams.num.standby.replicas`](/{version}/configuration/broker-configs#brokerconfigs_group.streams.num.standby.replicas): The default number of standby replicas for each task.
* [`group.streams.max.standby.replicas`](/{version}/configuration/broker-configs#brokerconfigs_group.streams.max.standby.replicas): Maximum for dynamic configurations of the standby replica configuration.
* [`group.streams.initial.rebalance.delay.ms`](/{version}/configuration/broker-configs#brokerconfigs_group.streams.initial.rebalance.delay.ms): The first rebalance of a new (ie, previously empty) group is delayed by this amount to allow more members to join the group.

## Group Configuration

Configurations for the resource type `GROUP` are available in `DescribeConfigs` and `IncrementalAlterConfigs` to override the default broker configurations dynamically for specific groups. These can be set using the [`Admin`](/{version}/javadoc/org/apache/kafka/clients/admin/Admin.html) Java interface or the `bin/kafka-configs.sh` utility.

For complete details, see the [group configuration](/{version}/configuration/group-configs) documentation.

The following group-level configurations are available for streams groups:

* [`streams.session.timeout.ms`](/{version}/configuration/group-configs#groupconfigs_streams.session.timeout.ms): The timeout to detect client failures when using the streams group protocol.
* [`streams.heartbeat.interval.ms`](/{version}/configuration/group-configs#groupconfigs_streams.heartbeat.interval.ms): The heartbeat interval given to the members.
* [`streams.num.standby.replicas`](/{version}/configuration/group-configs#groupconfigs_streams.num.standby.replicas): The number of standby replicas for each task.
* [`streams.initial.rebalance.delay.ms`](/{version}/configuration/group-configs#groupconfigs_streams.initial.rebalance.delay.ms): The first rebalance of a group is delayed by this amount to allow more members to join the group.

### Example: Setting Group-Level Configuration
```
bin/kafka-configs.sh --bootstrap-server localhost:9092 \
  --alter --entity-type groups --entity-name wordcount \
  --add-config streams.num.standby.replicas=1
```

**Note:** In the streams rebalance protocol, `session.timeout.ms`, `heartbeat.interval.ms` and `num.standby.replicas` are group-level configurations, which are ignored when they are set on the client side. Use the `bin/kafka-configs.sh` tool to set these configurations as shown above.

## Streams Configuration

For complete details on all Kafka Streams configurations, see the [streams configuration](/{version}/configuration/kafka-streams-configs) documentation.

The following client configuration enables the streams rebalance protocol:

* [`group.protocol`](/{version}/configuration/kafka-streams-configs#streamsconfigs_group.protocol): A flag which indicates if the streams rebalance protocol should be used. Set to `streams` to enable (default is `classic`).

### Ignored Configurations

The following configurations are ignored when the streams rebalance protocol is enabled:
* [`acceptable.recovery.lag`](/{version}/configuration/kafka-streams-configs#streamsconfigs_acceptable.recovery.lag)
* [`max.warmup.replicas`](/{version}/configuration/kafka-streams-configs#streamsconfigs_max.warmup.replicas)
* [`num.standby.replicas`](/{version}/configuration/kafka-streams-configs#streamsconfigs_num.standby.replicas) (use group-level configuration instead)
* [`probing.rebalance.interval.ms`](/{version}/configuration/kafka-streams-configs#streamsconfigs_probing.rebalance.interval.ms)
* [`rack.aware.assignment.tags`](/{version}/configuration/kafka-streams-configs#streamsconfigs_rack.aware.assignment.tags)
* [`rack.aware.assignment.strategy`](/{version}/configuration/kafka-streams-configs#streamsconfigs_rack.aware.assignment.strategy)
* [`rack.aware.assignment.traffic_cost`](/{version}/configuration/kafka-streams-configs#streamsconfigs_rack.aware.assignment.traffic_cost)
* [`rack.aware.assignment.non_overlap_cost`](/{version}/configuration/kafka-streams-configs#streamsconfigs_rack.aware.assignment.non_overlap_cost)
* [`task.assignor.class`](/{version}/configuration/kafka-streams-configs#streamsconfigs_task.assignor.class)
* [`session.timeout.ms`](/{version}/configuration/kafka-streams-configs#streamsconfigs_session.timeout.ms) (use group-level configuration instead)
* [`heartbeat.interval.ms`](/{version}/configuration/kafka-streams-configs#streamsconfigs_heartbeat.interval.ms) (use group-level configuration instead)

# Administration

## Admin API

Use the "streams groups" methods of the [`Admin`](/{version}/javadoc/org/apache/kafka/clients/admin/Admin.html) interface to manage streams groups programmatically. These APIs are mostly backed by the same implementations as the consumer group API.

The main differences from consumer group APIs are:

* The `describeStreamsGroups` uses the DescribeStreamsGroup RPC and contains different information than consumer groups.
* A streams group has an extra state - `NOT_READY` - and no legacy states from the classic protocol.
* `removeMembersFromConsumerGroup` will not have a corresponding API in this version, as it uses the LeaveGroup RPC for classic consumer groups, which is not available for KIP-848-style groups.

## kafka-streams-groups.sh

A new tool called `bin/kafka-streams-groups.sh` is added for working with streams groups. It replaces `bin/kafka-streams-application-reset.sh` for streams groups and can be used to list, describe, and delete streams groups. See the [kafka-streams-groups.sh documentation](/{version}/streams/developer-guide/kafka-streams-group-sh/) for detailed usage information.

# Architecture and How It Works

## Streams Groups

The protocol introduces the concept of a **streams group** in parallel to a consumer group. Streams clients use a dedicated heartbeat RPC, `StreamsGroupHeartbeat`, to join a group, leave a group, and update the group coordinator about its currently owned tasks and its client-specific metadata.

The group coordinator manages a streams group similarly to a consumer group, continuously updating the group member metadata via heartbeat responses and running assignment logic when changes are detected. A new group type called `streams` is introduced to the group coordinator, with new record key and value types for group metadata, topology metadata, and group member metadata. These records are persisted in the `__consumer_offsets` topic.

A group can either be a streams group, a share group, or a consumer group, defined by the first heartbeat request using the corresponding GroupId.

## Topology Configuration and Validation

To assign tasks among streams clients, the group coordinator uses topology metadata that is initialized when a member joins the group and persisted in the consumer offsets topic.

Whenever a member joins the streams group, the first heartbeat request contains metadata of the topology. The metadata describes the topology as a set of subtopologies, each identified by a unique string identifier and containing metadata relevant for creation of internal topics and assignment.

### Topology Validation and NOT_READY State

During the handling of the streams group heartbeat, the group coordinator may detect that source/sink or internal topics required by the topology do not exist or differ in their configuration from what is required for the topology to execute successfully. This triggers a "topology configuration" process, in which the group coordinator performs the following steps:

* Check that all configured source topics exist.
* Check that "copartition groups" are satisfied - that is, all source topics that are supposed to be copartitioned are indeed copartitioned.
* Derive the required number of partitions for all internal topics from the source topic configuration.
* Check that all internal topics exist with the right configuration.

If any source topics or internal topics are missing, the group enters a state `NOT_READY`. In `NOT_READY`, all heartbeats will be handled as usual (so they typically should not fail), but in the heartbeat response, the status will indicate which kind of problem exists. All members will get an empty assignment when the group is in `NOT_READY` state.

## Centralized Assignment Configuration

Core assignment options are configured centrally on the broker, without relying on each client's configuration. This allows tuning a streams group without redeploying the streams application. The core assignment option introduced on the broker-side is `num.standby.replicas`. This can be configured both globally on the broker and dynamically for specific streams groups through the `IncrementalAlterConfigs` and `DescribeConfigs` RPCs.

The last used assignment configuration is stored in the group metadata on the broker. This way, if an assignment configuration is dynamically changed, reassignment can be triggered immediately.

# Monitoring and Metrics

The existing group metrics are extended to differentiate between streams groups and consumer groups and account for streams group states. For complete details, see the [streams groups metrics](/{version}/operations/monitoring#group-coordinator-monitoring) documentation.

## Group Count by Protocol

Number of groups based on type of protocol, where the list of protocols is extended by the `protocol=streams` variation:
```
kafka.server:type=group-coordinator-metrics,name=group-count,protocol={consumer|classic|streams}
```

## Streams Group Count by State

Number of streams groups based on state:
```
kafka.server:type=group-coordinator-metrics,name=streams-group-count,state={empty|not_ready|assigning|reconciling|stable|dead}
```

## Streams Group Rebalances

Streams group rebalances sensor:
```
kafka.server:type=group-coordinator-metrics,name=streams-group-rebalance-rate

kafka.server:type=group-coordinator-metrics,name=streams-group-rebalance-count
```

# Migration from Classic Protocol

Currently, only offline migration is supported. To migrate a Kafka Streams application from the classic protocol to the streams rebalance protocol:

1. Shut down all application instances.
2. Wait for the `session.timeout.ms` to expire so the group becomes empty (or force an explicit group leave).
3. Update the application configuration to set `group.protocol=streams`.
4. Restart the application instances.

The only broker-side group data that will be preserved are the committed offsets. All other group metadata will be recreated when the application starts with the new protocol. Internal topics (changelog and repartition topics) will continue to exist as regular Kafka topics.

Similarly, you can convert a streams group back to a classic group by following the same process but setting `group.protocol=classic`.

**Warning:** Online migration (migrating while the application is running) is not available in this version. Plan for a maintenance window when migrating between protocols.
