---
title: Java Version
description: Java Version
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


Java 8, Java 11, and Java 17 are supported. 

Note that Java 8 support project-wide has been deprecated since Apache Kafka 3.0 and Java 11 support for the broker and tools has been deprecated since Apache Kafka 3.7. Both will be removed in Apache Kafka 4.0. 

Java 11 and later versions perform significantly better if TLS is enabled, so they are highly recommended (they also include a number of other performance improvements: G1GC, CRC32C, Compact Strings, Thread-Local Handshakes and more). 

From a security perspective, we recommend the latest released patch version as older freely available versions have disclosed security vulnerabilities. 

Typical arguments for running Kafka with OpenJDK-based Java implementations (including Oracle JDK) are: 
    
    
    -Xmx6g -Xms6g -XX:MetaspaceSize=96m -XX:+UseG1GC
    -XX:MaxGCPauseMillis=20 -XX:InitiatingHeapOccupancyPercent=35 -XX:G1HeapRegionSize=16M
    -XX:MinMetaspaceFreeRatio=50 -XX:MaxMetaspaceFreeRatio=80 -XX:+ExplicitGCInvokesConcurrent

For reference, here are the stats for one of LinkedIn's busiest clusters (at peak) that uses said Java arguments: 

  * 60 brokers
  * 50k partitions (replication factor 2)
  * 800k messages/sec in
  * 300 MB/sec inbound, 1 GB/sec+ outbound

All of the brokers in that cluster have a 90% GC pause time of about 21ms with less than 1 young GC per second. 
