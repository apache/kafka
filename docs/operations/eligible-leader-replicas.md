---
title: Eligible Leader Replicas
description: Eligible Leader Replicas
weight: 12
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

Starting from Apache Kafka 4.0, Eligible Leader Replicas ([KIP-966 Part 1](https://cwiki.apache.org/confluence/x/mpOzDw)) is available for the users to an improvement to Kafka replication (ELR is enabled by default on new clusters starting 4.1). As the "strict min ISR" rule has been generally applied, which means the high watermark for the data partition can't advance if the size of the ISR is smaller than the min ISR(`min.insync.replicas`), it makes some replicas that are not in the ISR safe to become the leader. The KRaft controller stores such replicas in the PartitionRecord field called `Eligible Leader Replicas`. During the leader election, the controller will select the leaders with the following order:

  * If ISR is not empty, select one of them.
  * If ELR is not empty, select one that is not fenced.
  * Select the last known leader if it is unfenced. This is a similar behavior prior to the 4.0 when all the replicas are offline.



## Upgrade & Downgrade

The ELR is not enabled by default for 4.0. To enable the new protocol on the server, set `eligible.leader.replicas.version=1`. After that the upgrade, the KRaft controller will start tracking the ELR. 

Downgrades are safe to perform by setting `eligible.leader.replicas.version=0`.

## Tool

The ELR fields can be checked through the API DescribeTopicPartitions. The admin client can fetch the ELR info by describing the topics.

Note that when the ELR feature is enabled:

  * The cluster-level `min.insync.replicas` config will be added if there is not any. The value is the same as the static config in the active controller.
  * The removal of `min.insync.replicas` config at the cluster-level is not allowed.
  * If the cluster-level `min.insync.replicas` is updated, even if the value is unchanged, all the ELR state will be cleaned.
  * The previously set `min.insync.replicas` value at the broker-level config will be removed. Please set at the cluster-level if necessary.
  * The alteration of `min.insync.replicas` config at the broker-level is not allowed.
  * If `min.insync.replicas` is updated for a topic, the ELR state will be cleaned.


