---
title: Kafka Streams Security Model
description: Apache Kafka Streams Security Model
weight: 10
tags: ['kafka', 'docs', 'security']
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


This page extends the [Apache Kafka security model](../security-model) to Kafka Streams. From the cluster's perspective a Streams application is an ordinary Kafka client and inherits the broker's authentication, authorization, and transport-encryption model through the same `ssl.*` and `sasl.*` client configs.

## Things You Need To Know

- **Streams is a client library, not a service.** It runs inside your application's process, so its security boundary is the application's; there is no separate Streams network surface to secure.
- **A Streams application authenticates as a single principal.** That principal needs ACLs covering its source topics, its internal repartition/changelog topics (typically `<application.id>-*`), and the consumer group `<application.id>`.
- **Local state lives on disk.** State stores and their changelogs are only as protected as the host filesystem and the broker's at-rest story; Kafka does not encrypt them.

## Known Non-Findings

In line with the [core model's classification](../security-model), the following are not, on their own, security vulnerabilities:

- **Application-level issues in a Streams topology.** Streams runs inside the user's application, so bugs in user-supplied processors, state handling, or topology code fall within the application's trust boundary, not the broker's.
- **Local state being readable on the host.** State stores and changelogs on disk are protected by the host filesystem, not by Kafka; reading them requires local access that is outside the model.
