---
title: Introduction
description: 
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


## What is event streaming?

Event streaming is the digital equivalent of the human body's central nervous system. It is the technological foundation for the 'always-on' world where businesses are increasingly software-defined and automated, and where the user of software is more software. 

Technically speaking, event streaming is the practice of capturing data in real-time from event sources like databases, sensors, mobile devices, cloud services, and software applications in the form of streams of events; storing these event streams durably for later retrieval; manipulating, processing, and reacting to the event streams in real-time as well as retrospectively; and routing the event streams to different destination technologies as needed. Event streaming thus ensures a continuous flow and interpretation of data so that the right information is at the right place, at the right time. 

## What can I use event streaming for?

Event streaming is applied to a [wide variety of use cases](/powered-by) across a plethora of industries and organizations. Its many examples include: 

  * To process payments and financial transactions in real-time, such as in stock exchanges, banks, and insurances. 
  * To track and monitor cars, trucks, fleets, and shipments in real-time, such as in logistics and the automotive industry. 
  * To continuously capture and analyze sensor data from IoT devices or other equipment, such as in factories and wind parks. 
  * To collect and immediately react to customer interactions and orders, such as in retail, the hotel and travel industry, and mobile applications. 
  * To monitor patients in hospital care and predict changes in condition to ensure timely treatment in emergencies. 
  * To connect, store, and make available data produced by different divisions of a company. 
  * To serve as the foundation for data platforms, event-driven architectures, and microservices. 



## Apache Kafka® is an event streaming platform. What does that mean?

Kafka combines three key capabilities so you can implement [your use cases](/powered-by) for event streaming end-to-end with a single battle-tested solution: 

  1. To **publish** (write) and **subscribe to** (read) streams of events, including continuous import/export of your data from other systems. 
  2. To **store** streams of events durably and reliably for as long as you want. 
  3. To **process** streams of events as they occur or retrospectively. 



And all this functionality is provided in a distributed, highly scalable, elastic, fault-tolerant, and secure manner. Kafka can be deployed on bare-metal hardware, virtual machines, and containers, and on-premises as well as in the cloud. You can choose between self-managing your Kafka environments and using fully managed services offered by a variety of vendors. 

## How does Kafka work in a nutshell?

Kafka is a distributed system consisting of **servers** and **clients** that communicate via a high-performance [TCP network protocol](/protocol.html). It can be deployed on bare-metal hardware, virtual machines, and containers in on-premise as well as cloud environments. 

**Servers** : Kafka is run as a cluster of one or more servers that can span multiple datacenters or cloud regions. Some of these servers form the storage layer, called the brokers. Other servers run [Kafka Connect](/documentation/#connect) to continuously import and export data as event streams to integrate Kafka with your existing systems such as relational databases as well as other Kafka clusters. To let you implement mission-critical use cases, a Kafka cluster is highly scalable and fault-tolerant: if any of its servers fails, the other servers will take over their work to ensure continuous operations without any data loss. 

**Clients** : They allow you to write distributed applications and microservices that read, write, and process streams of events in parallel, at scale, and in a fault-tolerant manner even in the case of network problems or machine failures. Kafka ships with some such clients included, which are augmented by [dozens of clients](https://cwiki.apache.org/confluence/x/3gDVAQ) provided by the Kafka community: clients are available for Java and Scala including the higher-level [Kafka Streams](/documentation/streams/) library, for Go, Python, C/C++, and many other programming languages as well as REST APIs. 

## Main Concepts and Terminology

An **event** records the fact that "something happened" in the world or in your business. It is also called record or message in the documentation. When you read or write data to Kafka, you do this in the form of events. Conceptually, an event has a key, value, timestamp, and optional metadata headers. Here's an example event: 

  * Event key: "Alice" 
  * Event value: "Made a payment of $200 to Bob" 
  * Event timestamp: "Jun. 25, 2020 at 2:06 p.m." 



**Producers** are those client applications that publish (write) events to Kafka, and **consumers** are those that subscribe to (read and process) these events. In Kafka, producers and consumers are fully decoupled and agnostic of each other, which is a key design element to achieve the high scalability that Kafka is known for. For example, producers never need to wait for consumers. Kafka provides various [guarantees](/documentation/#semantics) such as the ability to process events exactly-once. 

Events are organized and durably stored in **topics**. Very simplified, a topic is similar to a folder in a filesystem, and the events are the files in that folder. An example topic name could be "payments". Topics in Kafka are always multi-producer and multi-subscriber: a topic can have zero, one, or many producers that write events to it, as well as zero, one, or many consumers that subscribe to these events. Events in a topic can be read as often as needed-unlike traditional messaging systems, events are not deleted after consumption. Instead, you define for how long Kafka should retain your events through a per-topic configuration setting, after which old events will be discarded. Kafka's performance is effectively constant with respect to data size, so storing data for a long time is perfectly fine. 

Topics are **partitioned** , meaning a topic is spread over a number of "buckets" located on different Kafka brokers. This distributed placement of your data is very important for scalability because it allows client applications to both read and write the data from/to many brokers at the same time. When a new event is published to a topic, it is actually appended to one of the topic's partitions. Events with the same event key (e.g., a customer or vehicle ID) are written to the same partition, and Kafka [guarantees](/documentation/#semantics) that any consumer of a given topic-partition will always read that partition's events in exactly the same order as they were written. 

![](/images/streams-and-tables-p1_p4.png) Figure: This example topic has four partitions P1-P4. Two different producer clients are publishing, independently from each other, new events to the topic by writing events over the network to the topic's partitions. Events with the same key (denoted by their color in the figure) are written to the same partition. Note that both producers can write to the same partition if appropriate. 

To make your data fault-tolerant and highly-available, every topic can be **replicated** , even across geo-regions or datacenters, so that there are always multiple brokers that have a copy of the data just in case things go wrong, you want to do maintenance on the brokers, and so on. A common production setting is a replication factor of 3, i.e., there will always be three copies of your data. This replication is performed at the level of topic-partitions. 

This primer should be sufficient for an introduction. The [Design](/documentation/#design) section of the documentation explains Kafka's various concepts in full detail, if you are interested. 

## Kafka APIs

In addition to command line tooling for management and administration tasks, Kafka has five core APIs for Java and Scala: 

  * The [Admin API](/documentation.html#adminapi) to manage and inspect topics, brokers, and other Kafka objects. 
  * The [Producer API](/documentation.html#producerapi) to publish (write) a stream of events to one or more Kafka topics. 
  * The [Consumer API](/documentation.html#consumerapi) to subscribe to (read) one or more topics and to process the stream of events produced to them. 
  * The [Kafka Streams API](/documentation/streams) to implement stream processing applications and microservices. It provides higher-level functions to process event streams, including transformations, stateful operations like aggregations and joins, windowing, processing based on event-time, and more. Input is read from one or more topics in order to generate output to one or more topics, effectively transforming the input streams to output streams. 
  * The [Kafka Connect API](/documentation.html#connect) to build and run reusable data import/export connectors that consume (read) or produce (write) streams of events from and to external systems and applications so they can integrate with Kafka. For example, a connector to a relational database like PostgreSQL might capture every change to a set of tables. However, in practice, you typically don't need to implement your own connectors because the Kafka community already provides hundreds of ready-to-use connectors. 



## Where to go from here

  * To get hands-on experience with Kafka, follow the [Quickstart](/quickstart). 
  * To understand Kafka in more detail, read the [Documentation](/documentation/). You also have your choice of [Kafka books and academic papers](/books-and-papers). 
  * Browse through the [Use Cases](/powered-by) to learn how other users in our world-wide community are getting value out of Kafka. 
  * Join a [local Kafka meetup group](/events) and [watch talks from Kafka Summit](https://kafka-summit.org/past-events/), the main conference of the Kafka community. 


