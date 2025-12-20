---
title: Consumer Rebalance Protocol
description: Consumer Rebalance Protocol
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

Starting from Apache Kafka 4.0, the Next Generation of the Consumer Rebalance Protocol ([KIP-848](https://cwiki.apache.org/confluence/display/KAFKA/KIP-848%3A+The+Next+Generation+of+the+Consumer+Rebalance+Protocol)) is Generally Available (GA). It improves the scalability of consumer groups while simplifying consumers. It also decreases rebalance times, thanks to its fully incremental design, which no longer relies on a global synchronization barrier.

Consumer Groups using the new protocol are now referred to as `Consumer` groups, while groups using the old protocol are referred to as `Classic` groups. Note that Classic groups can still be used to form consumer groups using the old protocol.

## Server

The new consumer protocol is automatically enabled on the server since Apache Kafka 4.0. Enabling and disabling the protocol is controlled by the `group.version` feature flag.

The consumer heartbeat interval and the session timeout are controlled by the server now with the following configs:

  * `group.consumer.heartbeat.interval.ms`
  * `group.consumer.session.timeout.ms`



The assignment strategy is also controlled by the server. The `group.consumer.assignors` configuration can be used to specify the list of available assignors for `Consumer` groups. By default, the `uniform` assignor and the `range` assignor are configured. The first assignor in the list is used by default unless the Consumer selects a different one. It is also possible to implement custom assignment strategies on the server side by implementing the `ConsumerGroupPartitionAssignor` interface and specifying the full class name in the configuration.

## Consumer

Since Apache Kafka 4.0, the Consumer supports the new consumer rebalance protocol. However, the protocol is not enabled by default. The `group.protocol` configuration must be set to `consumer` to enable it. When enabled, the new consumer protocol is used alongside an improved threading model.

The `group.remote.assignor` configuration is introduced as an optional configuration to overwrite the default assignment strategy configured on the server side.

The `subscribe(SubscriptionPattern)` and `subscribe(SubscriptionPattern, ConsumerRebalanceListener)` methods have been added to subscribe to a regular expression with the new consumer rebalance protocol. With these methods, the regular expression uses the RE2J format and is now evaluated on the server side.

New metrics have been added to the Consumer when using the new rebalance protocol, mainly providing visibility over the improved threading model. See [New Consumer Metrics](https://cwiki.apache.org/confluence/display/KAFKA/KIP-1068%3A+New+metrics+for+the+new+KafkaConsumer).

When the new rebalance protocol is enabled, the following configurations and APIs are no longer usable:

  * `heartbeat.interval.ms`
  * `session.timeout.ms`
  * `partition.assignment.strategy`
  * `enforceRebalance(String)` and `enforceRebalance()`



## Upgrade & Downgrade

### Offline

Consumer groups are automatically converted from `Classic` to `Consumer` and vice versa when they are empty. Hence, it is possible to change the protocol used by the group by shutting down all the consumers and bringing them back up with the `group.protocol=consumer` configuration. The downside is that it requires taking the consumer group down.

### Online

Consumer groups can be upgraded without downtime by rolling out the consumer with the `group.protocol=consumer` configuration. When the first consumer using the new consumer rebalance protocol joins the group, the group is converted from `Classic` to `Consumer`, and the classic rebalance protocol is interoperated to work with the new consumer rebalance protocol. This is only possible when the classic group uses an assignor that does not embed custom metadata.

Consumer groups can be downgraded using the opposite process. In this case, the group is converted from `Consumer` to `Classic` when the last consumer using the new consumer rebalance protocol leaves the group.

## Limitations

While the new consumer rebalance protocol works for most use cases, it is still important to be aware of the following limitations:

  * Client-side assignors are not supported. (see [KAFKA-18327](https://issues.apache.org/jira/browse/KAFKA-18327))
  * Rack-aware assignment strategies are not fully supported. (see [KAFKA-17747](https://issues.apache.org/jira/browse/KAFKA-17747))


