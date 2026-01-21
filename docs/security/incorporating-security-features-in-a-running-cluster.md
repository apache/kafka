---
title: Incorporating Security Features in a Running Cluster
description: Incorporating Security Features in a Running Cluster
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


You can secure a running cluster via one or more of the supported protocols discussed previously. This is done in phases: 

  * Incrementally bounce the cluster nodes to open additional secured port(s).
  * Restart clients using the secured rather than PLAINTEXT port (assuming you are securing the client-broker connection).
  * Incrementally bounce the cluster again to enable broker-to-broker security (if this is required)
  * A final incremental bounce to close the PLAINTEXT port.



The specific steps for configuring SSL and SASL are described in sections 7.3 and 7.4. Follow these steps to enable security for your desired protocol(s). 

The security implementation lets you configure different protocols for both broker-client and broker-broker communication. These must be enabled in separate bounces. A PLAINTEXT port must be left open throughout so brokers and/or clients can continue to communicate. 

When performing an incremental bounce stop the brokers cleanly via a SIGTERM. It's also good practice to wait for restarted replicas to return to the ISR list before moving onto the next node. 

As an example, say we wish to encrypt both broker-client and broker-broker communication with SSL. In the first incremental bounce, an SSL port is opened on each node: 
    
    
    listeners=PLAINTEXT://broker1:9091,SSL://broker1:9092

We then restart the clients, changing their config to point at the newly opened, secured port: 
    
    
    bootstrap.servers = [broker1:9092,...]
    security.protocol = SSL
    ...etc

In the second incremental server bounce we instruct Kafka to use SSL as the broker-broker protocol (which will use the same SSL port): 
    
    
    listeners=PLAINTEXT://broker1:9091,SSL://broker1:9092
    security.inter.broker.protocol=SSL

In the final bounce we secure the cluster by closing the PLAINTEXT port: 
    
    
    listeners=SSL://broker1:9092
    security.inter.broker.protocol=SSL

Alternatively we might choose to open multiple ports so that different protocols can be used for broker-broker and broker-client communication. Say we wished to use SSL encryption throughout (i.e. for broker-broker and broker-client communication) but we'd like to add SASL authentication to the broker-client connection also. We would achieve this by opening two additional ports during the first bounce: 
    
    
    listeners=PLAINTEXT://broker1:9091,SSL://broker1:9092,SASL_SSL://broker1:9093

We would then restart the clients, changing their config to point at the newly opened, SASL & SSL secured port: 
    
    
    bootstrap.servers = [broker1:9093,...]
    security.protocol = SASL_SSL
    ...etc

The second server bounce would switch the cluster to use encrypted broker-broker communication via the SSL port we previously opened on port 9092: 
    
    
    listeners=PLAINTEXT://broker1:9091,SSL://broker1:9092,SASL_SSL://broker1:9093
    security.inter.broker.protocol=SSL

The final bounce secures the cluster by closing the PLAINTEXT port. 
    
    
    listeners=SSL://broker1:9092,SASL_SSL://broker1:9093
    security.inter.broker.protocol=SSL
