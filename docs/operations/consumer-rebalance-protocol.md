---
title: Consumer Rebalance Protocol
description: Kafka consumer rebalance protocol behavior, migration paths, and limitations.
weight: 10
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


## Overview

Starting from Apache Kafka 4.0, the Next Generation of the Consumer Rebalance Protocol ([KIP-848](https://cwiki.apache.org/confluence/x/HhD1D)) is Generally Available (GA), ready for production workloads. 
It improves the scalability of consumer groups while simplifying consumers. It also decreases rebalance times, thanks to its fully incremental design, which no longer relies on a global synchronization barrier.

There are now two types of consumer groups, named depending upon whether the consumers are using the new "consumer" group protocol or the earlier "classic" group protocol.

## Server

The new consumer protocol is automatically enabled on the server since Apache Kafka 4.0. Enabling and disabling the protocol on the server side is controlled by the `group.version` feature flag.

The consumer heartbeat interval and the session timeout are controlled by the server now with the following configs:

  * `group.consumer.heartbeat.interval.ms`
  * `group.consumer.session.timeout.ms`



The assignment strategy is also controlled by the server. The `group.consumer.assignors` configuration can be used to specify the list of available assignors for `Consumer` groups. 
* `uniform` and `range` assignors are provided by default
* `uniform` is the default one (first assignor in the list of `group.consumer.assignors`) unless the Consumer selects a different one (via the client config `group.remote.assignor`)
* it is possible to implement custom assignment strategies on the server side, by implementing the `ConsumerGroupPartitionAssignor` interface and specifying the full class name in the `group.consumer.assignors` configuration.

### Migrating from Client-side Assignors

The following table shows the mapping from client-side assignors to the new server-side assignors:

| Client-side Assignor      | Server-side Assignor |
|---------------------------|----------------------|
| RangeAssignor             | range                |
| CooperativeStickyAssignor | uniform              |
| StickyAssignor            | uniform              |
| RoundRobinAssignor        | uniform              |


## Consumer

Since Apache Kafka 4.0, the Consumer fully supports the new Consumer rebalance protocol. However, the protocol is not enabled by default. The `group.protocol` configuration must be set to `consumer` to enable it. When enabled, the new consumer protocol is used alongside an improved threading model.

The `subscribe(SubscriptionPattern)` and `subscribe(SubscriptionPattern, ConsumerRebalanceListener)` methods have been added to subscribe to a regular expression with the new Consumer rebalance protocol. With these methods, the regular expression uses the RE2J format and is now evaluated on the server side.

New metrics have been added to the Consumer when using the new rebalance protocol, mainly providing visibility over the improved threading model. See [New Consumer Metrics](https://cwiki.apache.org/confluence/x/lQ_TEg).

When the new rebalance protocol is enabled, the following configurations and APIs are no longer usable:

  * `heartbeat.interval.ms`
  * `session.timeout.ms`
  * `partition.assignment.strategy`
  * `enforceRebalance(String)` and `enforceRebalance()`



## Upgrade & Downgrade

### Offline

Consumer groups are automatically converted from `Classic` to `Consumer` and vice versa when they are empty. Hence, it is possible to change the protocol used by the group by shutting down all the consumers and bringing them back up with the `group.protocol=consumer` configuration. The downside is that it requires taking the consumer group down.

### Online

Consumer groups can be upgraded without downtime by rolling out the consumer with the `group.protocol=consumer` configuration. When the first consumer using the new Consumer rebalance protocol joins the group, the group is converted from `Classic` to `Consumer`, and the Classic rebalance protocol is interoperated to work with the new Consumer rebalance protocol. This is only possible when the classic group uses an assignor that does not embed custom metadata.

Consumer groups can be downgraded using the opposite process. In this case, the group is converted from `Consumer` to `Classic` when the last consumer using the new Consumer rebalance protocol leaves the group.

## Evolution Timeline

The evolution timeline of the Consumer rebalance protocol is described in [KIP-1274](https://cwiki.apache.org/confluence/display/KAFKA/KIP-1274%3A+Deprecate+and+remove+support+for+Classic+rebalance+protocol+in+KafkaConsumer), and expected to be as follows:

  * **Apache Kafka 3.7**: Early Access
  * **Apache Kafka 4.0**: GA (production-ready).
  * **Apache Kafka 5.0**: `KafkaConsumer` defaults to `Consumer` protocol, while still supporting `Classic` 
  * **Apache Kafka 6.0**: `KafkaConsumer` only supports `Consumer` as rebalance protocol, while the broker still supports `Classic` for backward compatibility.

## Limitations

  * Client-side assignors are not supported and not in scope at the moment. Use [KAFKA-18327](https://issues.apache.org/jira/browse/KAFKA-18327) to provide feedback if you have custom assignment strategies that may not be covered.
  * Rack-aware assignment strategies are not fully supported yet (work is in progress, see [KAFKA-19387](https://issues.apache.org/jira/browse/KAFKA-19387)).


