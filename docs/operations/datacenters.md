---
title: Datacenters
description: Kafka deployment patterns for multiple datacenters.
weight: 2
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


Some deployments will need to manage a data pipeline that spans multiple datacenters. Our recommended approach to this is to deploy a local Kafka cluster in each datacenter, with application instances in each datacenter interacting only with their local cluster and mirroring data between clusters (see the documentation on Geo-Replication for how to do this). 

This deployment pattern allows datacenters to act as independent entities and allows us to manage and tune inter-datacenter replication centrally. This allows each facility to stand alone and operate even if the inter-datacenter links are unavailable: when this occurs the mirroring falls behind until the link is restored at which time it catches up. 

For applications that need a global view of all data you can use mirroring to provide clusters which have aggregate data mirrored from the local clusters in _all_ datacenters. These aggregate clusters are used for reads by applications that require the full data set. 

This is not the only possible deployment pattern. It is possible to read from or write to a remote Kafka cluster over the WAN, though obviously this will add whatever latency is required to get the cluster. 

Kafka naturally batches data in both the producer and consumer so it can achieve high-throughput even over a high-latency connection. By default Kafka does not set the TCP socket buffer sizes, and the operating system sizes them. Over a high-latency connection, the buffers must be large enough for the [bandwidth-delay product](https://en.wikipedia.org/wiki/Bandwidth-delay_product) of the link. To allow this, it may be necessary to increase the operating system limits (on Linux, the maximum values in `net.ipv4.tcp_wmem` and `net.ipv4.tcp_rmem`). You can also set fixed sizes with `send.buffer.bytes` and `receive.buffer.bytes` on the producer and consumer, and `socket.send.buffer.bytes` and `socket.receive.buffer.bytes` on the broker; on Linux, fixed sizes are limited by `net.core.wmem_max` and `net.core.rmem_max`. 

It is generally _not_ advisable to run a _single_ Kafka cluster that spans multiple datacenters over a high-latency link. This will incur very high replication latency for Kafka writes, and Kafka will not remain available in all locations if the network between locations is unavailable. 
