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

Starting from Apache Kafka 4.0, Eligible Leader Replicas ([KIP-966 Part 1](https://cwiki.apache.org/confluence/display/KAFKA/KIP-966%3A+Eligible+Leader+Replicas)) is available for the users to an improvement to Kafka replication. As the "strict min ISR" rule has been generally applied, which means the high watermark for the data partition can't advance if the size of the ISR is smaller than the min ISR(`min.insync.replicas`), it makes some replicas that are not in the ISR safe to become the leader. The KRaft controller stores such replicas in the PartitionRecord field called `Eligible Leader Replicas`. During the leader election, the controller will select the leaders with the following order:

  * If ISR is not empty, select one of them.
  * If ELR is not empty, select one that is not fenced.
  * Select the last known leader if it is unfenced. This is a similar behavior prior to the 4.0 when all the replicas are offline.



## Upgrade & Downgrade

The ELR is not enabled by default for 4.0. To enable the new protocol on the server, set `eligible.leader.replicas.version=1`. After that the upgrade, the KRaft controller will start tracking the ELR. 

Downgrades are safe to perform by setting `eligible.leader.replicas.version=0`.

## Tool

The ELR fields can be checked through the API DescribeTopicPartitions. The admin client can fetch the ELR info by describing the topics. Also note that, if `min.insync.replicas` is updated for a topic, the ELR field will be cleaned. If cluster default min ISR is updated, all the ELR fields will be cleaned.
