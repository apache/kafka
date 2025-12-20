---
title: Managing Streams Application Topics
description: 
weight: 11
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


A Kafka Streams application continuously reads from Kafka topics, processes the read data, and then writes the processing results back into Kafka topics. The application may also auto-create other Kafka topics in the Kafka brokers, for example state store changelogs topics. This section describes the differences these topic types and how to manage the topics and your applications.

Kafka Streams distinguishes between user topics and internal topics.

# User topics

User topics exist externally to an application and are read from or written to by the application, including:

Input topics
    Topics that are specified via source processors in the application's topology; e.g. via `StreamsBuilder#stream()`, `StreamsBuilder#table()` and `Topology#addSource()`.
Output topics
    Topics that are specified via sink processors in the application's topology; e.g. via `KStream#to()`, `KTable.to()` and `Topology#addSink()`.
Intermediate topics
    Topics that are both input and output topics of the application's topology.

User topics must be created and manually managed ahead of time (e.g., via the [topic tools](../../kafka/post-deployment.html#kafka-operations-admin)). If user topics are shared among multiple applications for reading and writing, the application users must coordinate topic management. If user topics are centrally managed, then application users then would not need to manage topics themselves but simply obtain access to them.

Note

You should not use the auto-create topic feature on the brokers to create user topics, because:

  * Auto-creation of topics may be disabled in your Kafka cluster.
  * Auto-creation automatically applies the default topic settings such as the replicaton factor. These default settings might not be what you want for certain output topics (e.g., `auto.create.topics.enable=true` in the [Kafka broker configuration](http://kafka.apache.org/0100/documentation.html#brokerconfigs)).



# Internal topics

Internal topics are used internally by the Kafka Streams application while executing, for example the changelog topics for state stores. These topics are created by the application and are only used by that stream application.

If security is enabled on the Kafka brokers, you must grant the underlying clients admin permissions so that they can create internal topics set. For more information, see [Streams Security](security.html#streams-developer-guide-security).

Note

The internal topics follow the naming convention `<application.id>-<operatorName>-<suffix>`, but this convention is not guaranteed for future releases.

The following settings apply to the default configuration for internal topics:

  * For all internal topics, `message.timestamp.type` is set to `CreateTime`.
  * For internal repartition topics, the compaction policy is `delete` and the retention time is `-1` (infinite).
  * For internal changelog topics for key-value stores, the compaction policy is `compact`.
  * For internal changelog topics for windowed key-value stores, the compaction policy is `delete,compact`. The retention time is set to 24 hours plus your setting for the windowed store.
  * For internal changelog topics for versioned state stores, the cleanup policy is `compact`, and `min.compaction.lag.ms` is set to 24 hours plus the store's historyRetentionMs` value.



  * [Documentation](/documentation)
  * [Kafka Streams](/documentation/streams)
  * [Developer Guide](/documentation/streams/developer-guide/)


