---
title: ZooKeeper Encryption
description: ZooKeeper Encryption
weight: 8
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


ZooKeeper connections that use mutual TLS are encrypted. Beginning with ZooKeeper version 3.5.7 (the version shipped with Kafka version 2.5) ZooKeeper supports a sever-side config `ssl.clientAuth` (case-insensitively: `want`/`need`/`none` are the valid options, the default is `need`), and setting this value to `none` in ZooKeeper allows clients to connect via a TLS-encrypted connection without presenting their own certificate. Here is a sample (partial) Kafka Broker configuration for connecting to ZooKeeper with just TLS encryption. These configurations are described above in Broker Configs. 
    
    
    # connect to the ZooKeeper port configured for TLS
    zookeeper.connect=zk1:2182,zk2:2182,zk3:2182
    # required to use TLS to ZooKeeper (default is false)
    zookeeper.ssl.client.enable=true
    # required to use TLS to ZooKeeper
    zookeeper.clientCnxnSocket=org.apache.zookeeper.ClientCnxnSocketNetty
    # define trust stores to use TLS to ZooKeeper; ignored unless zookeeper.ssl.client.enable=true
    # no need to set keystore information assuming ssl.clientAuth=none on ZooKeeper
    zookeeper.ssl.truststore.location=/path/to/kafka/truststore.jks
    zookeeper.ssl.truststore.password=kafka-ts-passwd
    # tell broker to create ACLs on znodes (if using SASL authentication, otherwise do not set this)
    zookeeper.set.acl=true
