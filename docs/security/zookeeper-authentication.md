---
title: ZooKeeper Authentication
description: ZooKeeper Authentication
weight: 7
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


ZooKeeper supports mutual TLS (mTLS) authentication beginning with the 3.5.x versions. Kafka supports authenticating to ZooKeeper with SASL and mTLS -- either individually or both together -- beginning with version 2.5. See [KIP-515: Enable ZK client to use the new TLS supported authentication](https://cwiki.apache.org/confluence/display/KAFKA/KIP-515%3A+Enable+ZK+client+to+use+the+new+TLS+supported+authentication) for more details. 

When using mTLS alone, every broker and any CLI tools (such as the ZooKeeper Security Migration Tool) should identify itself with the same Distinguished Name (DN) because it is the DN that is ACL'ed. This can be changed as described below, but it involves writing and deploying a custom ZooKeeper authentication provider. Generally each certificate should have the same DN but a different Subject Alternative Name (SAN) so that hostname verification of the brokers and any CLI tools by ZooKeeper will succeed. 

When using SASL authentication to ZooKeeper together with mTLS, both the SASL identity and either the DN that created the znode (i.e. the creating broker's certificate) or the DN of the Security Migration Tool (if migration was performed after the znode was created) will be ACL'ed, and all brokers and CLI tools will be authorized even if they all use different DNs because they will all use the same ACL'ed SASL identity. It is only when using mTLS authentication alone that all the DNs must match (and SANs become critical -- again, in the absence of writing and deploying a custom ZooKeeper authentication provider as described below). 

Use the broker properties file to set TLS configs for brokers as described below. 

Use the `--zk-tls-config-file <file>` option to set TLS configs in the Zookeeper Security Migration Tool. The `kafka-acls.sh` and `kafka-configs.sh` CLI tools also support the `--zk-tls-config-file <file>` option. 

Use the `-zk-tls-config-file <file>` option (note the single-dash rather than double-dash) to set TLS configs for the `zookeeper-shell.sh` CLI tool. 

## New clusters

### ZooKeeper SASL Authentication

To enable ZooKeeper SASL authentication on brokers, there are two necessary steps: 

  1. Create a JAAS login file and set the appropriate system property to point to it as described above
  2. Set the configuration property `zookeeper.set.acl` in each broker to true

The metadata stored in ZooKeeper for the Kafka cluster is world-readable, but can only be modified by the brokers. The rationale behind this decision is that the data stored in ZooKeeper is not sensitive, but inappropriate manipulation of that data can cause cluster disruption. We also recommend limiting the access to ZooKeeper via network segmentation (only brokers and some admin tools need access to ZooKeeper). 

### ZooKeeper Mutual TLS Authentication

ZooKeeper mTLS authentication can be enabled with or without SASL authentication. As mentioned above, when using mTLS alone, every broker and any CLI tools (such as the ZooKeeper Security Migration Tool) must generally identify itself with the same Distinguished Name (DN) because it is the DN that is ACL'ed, which means each certificate should have an appropriate Subject Alternative Name (SAN) so that hostname verification of the brokers and any CLI tool by ZooKeeper will succeed. 

It is possible to use something other than the DN for the identity of mTLS clients by writing a class that extends `org.apache.zookeeper.server.auth.X509AuthenticationProvider` and overrides the method `protected String getClientId(X509Certificate clientCert)`. Choose a scheme name and set `authProvider.[scheme]` in ZooKeeper to be the fully-qualified class name of the custom implementation; then set `ssl.authProvider=[scheme]` to use it. 

Here is a sample (partial) ZooKeeper configuration for enabling TLS authentication. These configurations are described in the [ZooKeeper Admin Guide](https://zookeeper.apache.org/doc/r3.5.7/zookeeperAdmin.html#sc_authOptions). 
    
    
    secureClientPort=2182
    serverCnxnFactory=org.apache.zookeeper.server.NettyServerCnxnFactory
    authProvider.x509=org.apache.zookeeper.server.auth.X509AuthenticationProvider
    ssl.keyStore.location=/path/to/zk/keystore.jks
    ssl.keyStore.password=zk-ks-passwd
    ssl.trustStore.location=/path/to/zk/truststore.jks
    ssl.trustStore.password=zk-ts-passwd

**IMPORTANT** : ZooKeeper does not support setting the key password in the ZooKeeper server keystore to a value different from the keystore password itself. Be sure to set the key password to be the same as the keystore password. 

Here is a sample (partial) Kafka Broker configuration for connecting to ZooKeeper with mTLS authentication. These configurations are described above in Broker Configs. 
    
    
    # connect to the ZooKeeper port configured for TLS
    zookeeper.connect=zk1:2182,zk2:2182,zk3:2182
    # required to use TLS to ZooKeeper (default is false)
    zookeeper.ssl.client.enable=true
    # required to use TLS to ZooKeeper
    zookeeper.clientCnxnSocket=org.apache.zookeeper.ClientCnxnSocketNetty
    # define key/trust stores to use TLS to ZooKeeper; ignored unless zookeeper.ssl.client.enable=true
    zookeeper.ssl.keystore.location=/path/to/kafka/keystore.jks
    zookeeper.ssl.keystore.password=kafka-ks-passwd
    zookeeper.ssl.truststore.location=/path/to/kafka/truststore.jks
    zookeeper.ssl.truststore.password=kafka-ts-passwd
    # tell broker to create ACLs on znodes
    zookeeper.set.acl=true

**IMPORTANT** : ZooKeeper does not support setting the key password in the ZooKeeper client (i.e. broker) keystore to a value different from the keystore password itself. Be sure to set the key password to be the same as the keystore password. 

## Migrating clusters

If you are running a version of Kafka that does not support security or simply with security disabled, and you want to make the cluster secure, then you need to execute the following steps to enable ZooKeeper authentication with minimal disruption to your operations: 

  1. Enable SASL and/or mTLS authentication on ZooKeeper. If enabling mTLS, you would now have both a non-TLS port and a TLS port, like this: 
         
         clientPort=2181
         secureClientPort=2182
         serverCnxnFactory=org.apache.zookeeper.server.NettyServerCnxnFactory
         authProvider.x509=org.apache.zookeeper.server.auth.X509AuthenticationProvider
         ssl.keyStore.location=/path/to/zk/keystore.jks
         ssl.keyStore.password=zk-ks-passwd
         ssl.trustStore.location=/path/to/zk/truststore.jks
         ssl.trustStore.password=zk-ts-passwd

  2. Perform a rolling restart of brokers setting the JAAS login file and/or defining ZooKeeper mutual TLS configurations (including connecting to the TLS-enabled ZooKeeper port) as required, which enables brokers to authenticate to ZooKeeper. At the end of the rolling restart, brokers are able to manipulate znodes with strict ACLs, but they will not create znodes with those ACLs
  3. If you enabled mTLS, disable the non-TLS port in ZooKeeper
  4. Perform a second rolling restart of brokers, this time setting the configuration parameter `zookeeper.set.acl` to true, which enables the use of secure ACLs when creating znodes
  5. Execute the ZkSecurityMigrator tool. To execute the tool, there is this script: `bin/zookeeper-security-migration.sh` with `zookeeper.acl` set to secure. This tool traverses the corresponding sub-trees changing the ACLs of the znodes. Use the `--zk-tls-config-file <file>` option if you enable mTLS.



It is also possible to turn off authentication in a secure cluster. To do it, follow these steps:

  1. Perform a rolling restart of brokers setting the JAAS login file and/or defining ZooKeeper mutual TLS configurations, which enables brokers to authenticate, but setting `zookeeper.set.acl` to false. At the end of the rolling restart, brokers stop creating znodes with secure ACLs, but are still able to authenticate and manipulate all znodes
  2. Execute the ZkSecurityMigrator tool. To execute the tool, run this script `bin/zookeeper-security-migration.sh` with `zookeeper.acl` set to unsecure. This tool traverses the corresponding sub-trees changing the ACLs of the znodes. Use the `--zk-tls-config-file <file>` option if you need to set TLS configuration.
  3. If you are disabling mTLS, enable the non-TLS port in ZooKeeper
  4. Perform a second rolling restart of brokers, this time omitting the system property that sets the JAAS login file and/or removing ZooKeeper mutual TLS configuration (including connecting to the non-TLS-enabled ZooKeeper port) as required
  5. If you are disabling mTLS, disable the TLS port in ZooKeeper

Here is an example of how to run the migration tool: 
    
    
    $ bin/zookeeper-security-migration.sh --zookeeper.acl=secure --zookeeper.connect=localhost:2181

Run this to see the full list of parameters:
    
    
    $ bin/zookeeper-security-migration.sh --help

## Migrating the ZooKeeper ensemble

It is also necessary to enable SASL and/or mTLS authentication on the ZooKeeper ensemble. To do it, we need to perform a rolling restart of the server and set a few properties. See above for mTLS information. Please refer to the ZooKeeper documentation for more detail: 

  1. [Apache ZooKeeper documentation](https://zookeeper.apache.org/doc/r3.5.7/zookeeperProgrammers.html#sc_ZooKeeperAccessControl)
  2. [Apache ZooKeeper wiki](https://cwiki.apache.org/confluence/display/ZOOKEEPER/Zookeeper+and+SASL)



## ZooKeeper Quorum Mutual TLS Authentication

It is possible to enable mTLS authentication between the ZooKeeper servers themselves. Please refer to the [ZooKeeper documentation](https://zookeeper.apache.org/doc/r3.5.7/zookeeperAdmin.html#Quorum+TLS) for more detail. 
