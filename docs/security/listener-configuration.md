---
title: Listener Configuration
description: Listener Configuration
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


In order to secure a Kafka cluster, it is necessary to secure the channels that are used to communicate with the servers. Each server must define the set of listeners that are used to receive requests from clients as well as other servers. Each listener may be configured to authenticate clients using various mechanisms and to ensure traffic between the server and the client is encrypted. This section provides a primer for the configuration of listeners.

Kafka servers support listening for connections on multiple ports. This is configured through the `listeners` property in the server configuration, which accepts a comma-separated list of the listeners to enable. At least one listener must be defined on each server. The format of each listener defined in `listeners` is given below:
    
    
    {LISTENER_NAME}://{hostname}:{port}

The `LISTENER_NAME` is usually a descriptive name which defines the purpose of the listener. For example, many configurations use a separate listener for client traffic, so they might refer to the corresponding listener as `CLIENT` in the configuration:
    
    
    listeners=CLIENT://localhost:9092

The security protocol of each listener is defined in a separate configuration: `listener.security.protocol.map`. The value is a comma-separated list of each listener mapped to its security protocol. For example, the follow value configuration specifies that the `CLIENT` listener will use SSL while the `BROKER` listener will use plaintext.
    
    
    listener.security.protocol.map=CLIENT:SSL,BROKER:PLAINTEXT

Possible options (case-insensitive) for the security protocol are given below:

  1. PLAINTEXT
  2. SSL
  3. SASL_PLAINTEXT
  4. SASL_SSL



The plaintext protocol provides no security and does not require any additional configuration. In the following sections, this document covers how to configure the remaining protocols.

If each required listener uses a separate security protocol, it is also possible to use the security protocol name as the listener name in `listeners`. Using the example above, we could skip the definition of the `CLIENT` and `BROKER` listeners using the following definition:
    
    
    listeners=SSL://localhost:9092,PLAINTEXT://localhost:9093

However, we recommend users to provide explicit names for the listeners since it makes the intended usage of each listener clearer.

Among the listeners in this list, it is possible to declare the listener to be used for inter-broker communication by setting the `inter.broker.listener.name` configuration to the name of the listener. The primary purpose of the inter-broker listener is partition replication. If not defined, then the inter-broker listener is determined by the security protocol defined by `security.inter.broker.protocol`, which defaults to `PLAINTEXT`.

For legacy clusters which rely on Zookeeper to store cluster metadata, it is possible to declare a separate listener to be used for metadata propagation from the active controller to the brokers. This is defined by `control.plane.listener.name`. The active controller will use this listener when it needs to push metadata updates to the brokers in the cluster. The benefit of using a control plane listener is that it uses a separate processing thread, which makes it less likely for application traffic to impede timely propagation of metadata changes (such as partition leader and ISR updates). Note that the default value is null, which means that the controller will use the same listener defined by `inter.broker.listener`

In a KRaft cluster, a broker is any server which has the `broker` role enabled in `process.roles` and a controller is any server which has the `controller` role enabled. Listener configuration depends on the role. The listener defined by `inter.broker.listener.name` is used exclusively for requests between brokers. Controllers, on the other hand, must use separate listener which is defined by the `controller.listener.names` configuration. This cannot be set to the same value as the inter-broker listener.

Controllers receive requests both from other controllers and from brokers. For this reason, even if a server does not have the `controller` role enabled (i.e. it is just a broker), it must still define the controller listener along with any security properties that are needed to configure it. For example, we might use the following configuration on a standalone broker:
    
    
    process.roles=broker
    listeners=BROKER://localhost:9092
    inter.broker.listener.name=BROKER
    controller.quorum.bootstrap.servers=localhost:9093
    controller.listener.names=CONTROLLER
    listener.security.protocol.map=BROKER:SASL_SSL,CONTROLLER:SASL_SSL

The controller listener is still configured in this example to use the `SASL_SSL` security protocol, but it is not included in `listeners` since the broker does not expose the controller listener itself. The port that will be used in this case comes from the `controller.quorum.voters` configuration, which defines the complete list of controllers.

For KRaft servers which have both the broker and controller role enabled, the configuration is similar. The only difference is that the controller listener must be included in `listeners`:
    
    
    process.roles=broker,controller
    listeners=BROKER://localhost:9092,CONTROLLER://localhost:9093
    inter.broker.listener.name=BROKER
    controller.quorum.bootstrap.servers=localhost:9093
    controller.listener.names=CONTROLLER
    listener.security.protocol.map=BROKER:SASL_SSL,CONTROLLER:SASL_SSL

It is a requirement that the host and port defined in `controller.quorum.bootstrap.servers` is routed to the exposed controller listeners. For example, here the `CONTROLLER` listener is bound to localhost:9093. The connection string defined by `controller.quorum.bootstrap.servers` must then also use localhost:9093, as it does here.

The controller will accept requests on all listeners defined by `controller.listener.names`. Typically there would be just one controller listener, but it is possible to have more. For example, this provides a way to change the active listener from one port or security protocol to another through a roll of the cluster (one roll to expose the new listener, and one roll to remove the old listener). When multiple controller listeners are defined, the first one in the list will be used for outbound requests.

It is conventional in Kafka to use a separate listener for clients. This allows the inter-cluster listeners to be isolated at the network level. In the case of the controller listener in KRaft, the listener should be isolated since clients do not work with it anyway. Clients are expected to connect to any other listener configured on a broker. Any requests that are bound for the controller will be forwarded as described below

In the following section, this document covers how to enable SSL on a listener for encryption as well as authentication. The subsequent section will then cover additional authentication mechanisms using SASL.
