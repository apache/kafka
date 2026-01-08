---
title: KRaft vs ZooKeeper
description: 
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


# Differences Between KRaft mode and ZooKeeper mode

# Removed ZooKeeper Features

This section documents differences in behavior between KRaft mode and ZooKeeper mode. Specifically, several configurations, metrics and features have changed or are no longer required in KRaft mode. To migrate an existing cluster from ZooKeeper mode to KRaft mode, please refer to the [ZooKeeper to KRaft Migration](/39/operations/kraft/#zookeeper-to-kraft-migration) section. 

## Configurations

  * Removed password encoder-related configurations. These configurations were used in ZooKeeper mode to define the key and backup key for encrypting sensitive data (e.g., passwords), specify the algorithm and key generation method for password encryption (e.g., AES, RSA), and control the key length and encryption strength. 

    * `password.encoder.secret`
    * `password.encoder.old.secret`
    * `password.encoder.keyfactory.algorithm`
    * `password.encoder.cipher.algorithm`
    * `password.encoder.key.length`
    * `password.encoder.iterations`

In KRaft mode, Kafka stores sensitive data in records, and the data is not encrypted in Kafka. 

  * Removed `control.plane.listener.name`. Kafka relies on ZooKeeper to manage metadata, but some internal operations (e.g., communication between controllers (a.k.a., broker controller) and brokers) still require Kafka’s internal control plane for coordination. 

In KRaft mode, Kafka eliminates its dependency on ZooKeeper, and the control plane functionality is fully integrated into Kafka itself. The process roles are clearly separated: brokers handle data-related requests, while the controllers (a.k.a., quorum controller) manages metadata-related requests. The controllers use the Raft protocol for internal communication, which operates differently from the ZooKeeper model. Use the following parameters to configure the control plane listener: 

    * `controller.listener.names`
    * `listeners`
    * `listener.security.protocol.map`
  * Removed graceful broker shutdowns-related configurations. These configurations were used in ZooKeeper mode to define the maximum number of retries and the retry backoff time for controlled shutdowns. It can reduce the risk of unplanned leader changes and data inconsistencies. 

    * `controlled.shutdown.max.retries`
    * `controlled.shutdown.retry.backoff.ms`

In KRaft mode, Kafka uses the Raft protocol to manage metadata. The broker shutdown process differs from ZooKeeper mode as it is managed by the quorum-based controller. The shutdown process is more reliable and efficient due to automated leader transfers and metadata updates handled by the controller. 

  * Removed the broker id generation-related configurations. These configurations were used in ZooKeeper mode to specify the broker id auto generation and control the broker id generation process. 

    * `reserved.broker.max.id`
    * `broker.id.generation.enable`

Kafka uses the node id in KRaft mode to identify servers. 

    * `node.id`
  * Removed broker protocol version-related configurations. These configurations were used in ZooKeeper mode to define communication protocol version between brokers. In KRaft mode, Kafka uses `metadata.version` to control the feature level of the cluster, which can be managed using `bin/kafka-features.sh`. 

    * `inter.broker.protocol.version`
  * Removed dynamic configurations which relied on ZooKeeper. In KRaft mode, to change these configurations, you need to restart the broker/controller. 

    * `advertised.listeners`
  * Removed the leader imbalance configuration used only in ZooKeeper. `leader.imbalance.per.broker.percentage` was used to limit the preferred leader election frequency in ZooKeeper. 

    * `leader.imbalance.per.broker.percentage`
  * Removed ZooKeeper related configurations. 

    * `zookeeper.connect`
    * `zookeeper.session.timeout.ms`
    * `zookeeper.connection.timeout.ms`
    * `zookeeper.set.acl`
    * `zookeeper.max.in.flight.requests`
    * `zookeeper.ssl.client.enable`
    * `zookeeper.clientCnxnSocket`
    * `zookeeper.ssl.keystore.location`
    * `zookeeper.ssl.keystore.password`
    * `zookeeper.ssl.keystore.type`
    * `zookeeper.ssl.truststore.location`
    * `zookeeper.ssl.truststore.password`
    * `zookeeper.ssl.truststore.type`
    * `zookeeper.ssl.protocol`
    * `zookeeper.ssl.enabled.protocols`
    * `zookeeper.ssl.cipher.suites`
    * `zookeeper.ssl.endpoint.identification.algorithm`
    * `zookeeper.ssl.crl.enable`
    * `zookeeper.ssl.ocsp.enable`



## Dynamic Log Levels

  * The dynamic log levels feature allows you to change the log4j settings of a running broker or controller process without restarting it. The command-line syntax for setting dynamic log levels on brokers has not changed in KRaft mode. Here is an example of setting the log level on a broker:  

        
        ./bin/kafka-configs.sh --bootstrap-server localhost:9092 \
            --entity-type broker-loggers \
            --entity-name 1 \
            --alter \
            --add-config org.apache.kafka.raft.KafkaNetworkChannel=TRACE
                        

  * When setting dynamic log levels on the controllers, the `--bootstrap-controller` flag must be used. Here is an example of setting the log level ona controller:  

        
        ./bin/kafka-configs.sh --bootstrap-controller localhost:9093 \
            --entity-type broker-loggers \
            --entity-name 1 \
            --alter \
            --add-config org.apache.kafka.raft.KafkaNetworkChannel=TRACE
                        

  
Note that the entity-type must be specified as `broker-loggers`, even though we are changing a controller's log level rather than a broker's log level. 

  * When changing the log level of a combined node, which has both broker and controller roles, either --bootstrap-servers or --bootstrap-controllers may be used. Combined nodes have only a single set of log levels; there are not different log levels for the broker and controller parts of the process. 




## Dynamic Controller Configurations

  * Some Kafka configurations can be changed dynamically, without restarting the process. The command-line syntax for setting dynamic log levels on brokers has not changed in KRaft mode. Here is an example of setting the number of IO threads on a broker:  

        
        ./bin/kafka-configs.sh --bootstrap-server localhost:9092 \
            --entity-type brokers \
            --entity-name 1 \
            --alter \
            --add-config num.io.threads=5
                        

  * Controllers will apply all applicable cluster-level dynamic configurations. For example, the following command-line will change the `max.connections` setting on all of the brokers and all of the controllers in the cluster:  

        
        ./bin/kafka-configs.sh --bootstrap-server localhost:9092 \
            --entity-type brokers \
            --entity-default \
            --alter \
            --add-config max.connections=10000
                        

  
It is not currently possible to apply a dynamic configuration on only a single controller. 




# Metrics

  * Removed the following metrics related to ZooKeeper. `ControlPlaneNetworkProcessorAvgIdlePercent` is to monitor the average fraction of time the network processors are idle. The other `ControlPlaneExpiredConnectionsKilledCount` is to monitor the total number of connections disconnected, across all processors. 

    * `ControlPlaneNetworkProcessorAvgIdlePercent`
    * `ControlPlaneExpiredConnectionsKilledCount`

In KRaft mode, Kafka also provides metrics to monitor the network processors and expired connections. Use the following metrics to monitor the network processors and expired connections: 

    * `NetworkProcessorAvgIdlePercent`
    * `ExpiredConnectionsKilledCount`
  * Removed the metrics which are only used in ZooKeeper mode. 

    * `kafka.controller:type=ControllerChannelManager,name=QueueSize`
    * `kafka.controller:type=ControllerChannelManager,name=RequestRateAndQueueTimeMs`
    * `kafka.controller:type=ControllerEventManager,name=EventQueueSize`
    * `kafka.controller:type=ControllerEventManager,name=EventQueueTimeMs`
    * `kafka.controller:type=ControllerStats,name=AutoLeaderBalanceRateAndTimeMs`
    * `kafka.controller:type=ControllerStats,name=ControlledShutdownRateAndTimeMs`
    * `kafka.controller:type=ControllerStats,name=ControllerChangeRateAndTimeMs`
    * `kafka.controller:type=ControllerStats,name=ControllerShutdownRateAndTimeMs`
    * `kafka.controller:type=ControllerStats,name=IdleRateAndTimeMs`
    * `kafka.controller:type=ControllerStats,name=IsrChangeRateAndTimeMs`
    * `kafka.controller:type=ControllerStats,name=LeaderAndIsrResponseReceivedRateAndTimeMs`
    * `kafka.controller:type=ControllerStats,name=LeaderElectionRateAndTimeMs`
    * `kafka.controller:type=ControllerStats,name=ListPartitionReassignmentRateAndTimeMs`
    * `kafka.controller:type=ControllerStats,name=LogDirChangeRateAndTimeMs`
    * `kafka.controller:type=ControllerStats,name=ManualLeaderBalanceRateAndTimeMs`
    * `kafka.controller:type=KafkaController,name=MigratingZkBrokerCount`
    * `kafka.controller:type=ControllerStats,name=PartitionReassignmentRateAndTimeMs`
    * `kafka.controller:type=ControllerStats,name=TopicChangeRateAndTimeMs`
    * `kafka.controller:type=ControllerStats,name=TopicDeletionRateAndTimeMs`
    * `kafka.controller:type=KafkaController,name=TopicsIneligibleToDeleteCount`
    * `kafka.controller:type=ControllerStats,name=TopicUncleanLeaderElectionEnableRateAndTimeMs`
    * `kafka.controller:type=ControllerStats,name=UncleanLeaderElectionEnableRateAndTimeMs`
    * `kafka.controller:type=ControllerStats,name=UncleanLeaderElectionsPerSec`
    * `kafka.controller:type=ControllerStats,name=UpdateFeaturesRateAndTimeMs`
    * `kafka.controller:type=ControllerStats,name=UpdateMetadataResponseReceivedRateAndTimeMs`
    * `kafka.controller:type=KafkaController,name=ActiveBrokerCount`
    * `kafka.controller:type=KafkaController,name=ActiveControllerCount`
    * `kafka.controller:type=KafkaController,name=ControllerState`
    * `kafka.controller:type=KafkaController,name=FencedBrokerCount`
    * `kafka.controller:type=KafkaController,name=GlobalPartitionCount`
    * `kafka.controller:type=KafkaController,name=GlobalTopicCount`
    * `kafka.controller:type=KafkaController,name=OfflinePartitionsCount`
    * `kafka.controller:type=KafkaController,name=PreferredReplicaImbalanceCount`
    * `kafka.controller:type=KafkaController,name=ReplicasIneligibleToDeleteCount`
    * `kafka.controller:type=KafkaController,name=ReplicasToDeleteCount`
    * `kafka.controller:type=KafkaController,name=TopicsToDeleteCount`
    * `kafka.controller:type=KafkaController,name=ZkMigrationState`
    * `kafka.server:type=DelayedOperationPurgatory,name=PurgatorySize,delayedOperation=ElectLeader`
    * `kafka.server:type=DelayedOperationPurgatory,name=PurgatorySize,delayedOperation=topic`
    * `kafka.server:type=DelayedOperationPurgatory,name=NumDelayedOperations,delayedOperation=ElectLeader`
    * `kafka.server:type=DelayedOperationPurgatory,name=NumDelayedOperations,delayedOperation=topic`
    * `kafka.server:type=SessionExpireListener,name=SessionState`
    * `kafka.server:type=SessionExpireListener,name=ZooKeeperAuthFailuresPerSec`
    * `kafka.server:type=SessionExpireListener,name=ZooKeeperDisconnectsPerSec`
    * `kafka.server:type=SessionExpireListener,name=ZooKeeperExpiresPerSec`
    * `kafka.server:type=SessionExpireListener,name=ZooKeeperReadOnlyConnectsPerSec`
    * `kafka.server:type=SessionExpireListener,name=ZooKeeperSaslAuthenticationsPerSec`
    * `kafka.server:type=SessionExpireListener,name=ZooKeeperSyncConnectsPerSec`
    * `kafka.server:type=ZooKeeperClientMetrics,name=ZooKeeperRequestLatencyMs`



# Behavioral Change Reference

This document catalogs the functional and operational differences between ZooKeeper mode and KRaft mode. 

  * **Configuration Value Size Limitation** : KRaft mode restricts configuration values to a maximum size of `Short.MAX_VALUE`, which prevents using the append operation to create larger configuration values. 
  * **Policy Class Deployment** : In KRaft mode, the `CreateTopicPolicy` and `AlterConfigPolicy` plugins run on the controller instead of the broker. This requires users to deploy the policy class JAR files on the controller and configure the parameters (`create.topic.policy.class.name` and `alter.config.policy.class.name`) on the controller. 

Note: If migrating from ZooKeeper mode, ensure policy JARs are moved from brokers to controllers.

  * **Custom implementations of`KafkaPrincipalBuilder`**: In KRaft mode, custom implementations of `KafkaPrincipalBuilder` must also implement `KafkaPrincipalSerde`; otherwise brokers will not be able to forward requests to the controller. 


