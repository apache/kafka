---
title: Overview
description: Overview
weight: 1
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


Kafka Connect is a tool for scalably and reliably streaming data between Apache Kafka and other systems. It makes it simple to quickly define _connectors_ that move large collections of data into and out of Kafka. Kafka Connect can ingest entire databases or collect metrics from all your application servers into Kafka topics, making the data available for stream processing with low latency. An export job can deliver data from Kafka topics into secondary storage and query systems or into batch systems for offline analysis.

Kafka Connect features include:

  * **A common framework for Kafka connectors** \- Kafka Connect standardizes integration of other data systems with Kafka, simplifying connector development, deployment, and management
  * **Distributed and standalone modes** \- scale up to a large, centrally managed service supporting an entire organization or scale down to development, testing, and small production deployments
  * **REST interface** \- submit and manage connectors to your Kafka Connect cluster via an easy to use REST API
  * **Automatic offset management** \- with just a little information from connectors, Kafka Connect can manage the offset commit process automatically so connector developers do not need to worry about this error prone part of connector development
  * **Distributed and scalable by default** \- Kafka Connect builds on the existing group management protocol. More workers can be added to scale up a Kafka Connect cluster.
  * **Streaming/batch integration** \- leveraging Kafka's existing capabilities, Kafka Connect is an ideal solution for bridging streaming and batch data systems


