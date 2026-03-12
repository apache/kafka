---
title: Java Version
description: Java Version
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


Java 17, Java 21, and Java 25 are fully supported while Java 11 is supported for a subset of modules (clients, streams and related). Support for versions newer than the most recent LTS version are best-effort and the project typically only tests with the most recent non LTS version. 

We generally recommend running Apache Kafka with the most recent LTS release (Java 25 at the time of writing) for performance, efficiency and support reasons. From a security perspective, we recommend the latest released patch version as older versions typically have disclosed security vulnerabilities. 

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
