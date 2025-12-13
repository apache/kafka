---
title: ZooKeeper Encryption
description: ZooKeeper Encryption
weight: 8
tags: ['kafka', 'docs']
aliases: 
keywords: 
type: docs
---

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
