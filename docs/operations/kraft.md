---
title: KRaft
description: KRaft
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


## Configuration

### Process Roles

In KRaft mode each Kafka server can be configured as a controller, a broker, or both using the `process.roles` property. This property can have the following values:

  * If `process.roles` is set to `broker`, the server acts as a broker.
  * If `process.roles` is set to `controller`, the server acts as a controller.
  * If `process.roles` is set to `broker,controller`, the server acts as both a broker and a controller.



Kafka servers that act as both brokers and controllers are referred to as "combined" servers. Combined servers are simpler to operate for small use cases like a development environment. The key disadvantage is that the controller will be less isolated from the rest of the system. For example, it is not possible to roll or scale the controllers separately from the brokers in combined mode. Combined mode is not recommended in critical deployment environments.

### Controllers

In KRaft mode, specific Kafka servers are selected to be controllers. The servers selected to be controllers will participate in the metadata quorum. Each controller is either an active or a hot standby for the current active controller.

A Kafka admin will typically select 3 or 5 servers for this role, depending on factors like cost and the number of concurrent failures your system should withstand without availability impact. A majority of the controllers must be alive in order to maintain availability. With 3 controllers, the cluster can tolerate 1 controller failure; with 5 controllers, the cluster can tolerate 2 controller failures.

All of the servers in a Kafka cluster discover the active controller using the `controller.quorum.bootstrap.servers` property. All the controllers should be enumerated in this property. Each controller is identified with their `host` and `port` information. For example:
    
    
    controller.quorum.bootstrap.servers=host1:port1,host2:port2,host3:port3

If a Kafka cluster has 3 controllers named controller1, controller2 and controller3, then controller1 may have the following configuration:
    
    
    process.roles=controller
    node.id=1
    listeners=CONTROLLER://controller1.example.com:9093
    controller.quorum.bootstrap.servers=controller1.example.com:9093,controller2.example.com:9093,controller3.example.com:9093
    controller.listener.names=CONTROLLER

Every broker and controller must set the `controller.quorum.bootstrap.servers` property. 

## Provisioning Nodes

The `bin/kafka-storage.sh random-uuid` command can be used to generate a cluster ID for your new cluster. This cluster ID must be used when formatting each server in the cluster with the `bin/kafka-storage.sh format` command. 

This is different from how Kafka has operated in the past. Previously, Kafka would format blank storage directories automatically, and also generate a new cluster ID automatically. One reason for the change is that auto-formatting can sometimes obscure an error condition. This is particularly important for the metadata log maintained by the controller and broker servers. If a majority of the controllers were able to start with an empty log directory, a leader might be able to be elected with missing committed data.

### Bootstrap a Standalone Controller

The recommended method for creating a new KRaft controller cluster is to bootstrap it with one voter and dynamically add the rest of the controllers. Bootstrapping the first controller can be done with the following CLI command: 
    
    
    $ bin/kafka-storage.sh format --cluster-id <CLUSTER_ID> --standalone --config config/controller.properties

This command will 1) create a meta.properties file in metadata.log.dir with a randomly generated directory.id, 2) create a snapshot at 00000000000000000000-0000000000.checkpoint with the necessary control records (KRaftVersionRecord and VotersRecord) to make this Kafka node the only voter for the quorum. 

### Bootstrap with Multiple Controllers

The KRaft cluster metadata partition can also be bootstrapped with more than one voter. This can be done by using the --initial-controllers flag: 
    
    
    CLUSTER_ID="$(bin/kafka-storage.sh random-uuid)"
    CONTROLLER_0_UUID="$(bin/kafka-storage.sh random-uuid)"
    CONTROLLER_1_UUID="$(bin/kafka-storage.sh random-uuid)"
    CONTROLLER_2_UUID="$(bin/kafka-storage.sh random-uuid)"
    
    # In each controller execute
    bin/kafka-storage.sh format --cluster-id ${CLUSTER_ID} \
                         --initial-controllers "0@controller-0:1234:${CONTROLLER_0_UUID},1@controller-1:1234:${CONTROLLER_1_UUID},2@controller-2:1234:${CONTROLLER_2_UUID}" \
                         --config config/controller.properties

This command is similar to the standalone version but the snapshot at 00000000000000000000-0000000000.checkpoint will instead contain a VotersRecord that includes information for all of the controllers specified in --initial-controllers. It is important that the value of this flag is the same in all of the controllers with the same cluster id. In the replica description 0@controller-0:1234:3Db5QLSqSZieL3rJBUUegA, 0 is the replica id, 3Db5QLSqSZieL3rJBUUegA is the replica directory id, controller-0 is the replica's host and 1234 is the replica's port. 

### Formatting Brokers and New Controllers

When provisioning new broker and controller nodes that we want to add to an existing Kafka cluster, use the `kafka-storage.sh format` command with the --no-initial-controllers flag. 
    
    
    $ bin/kafka-storage.sh format --cluster-id <CLUSTER_ID> --config config/server.properties --no-initial-controllers

## Controller membership changes

### Static versus Dynamic KRaft Quorums

There are two ways to run KRaft: the old way using static controller quorums, and the new way using KIP-853 dynamic controller quorums.

When using a static quorum, the configuration file for each broker and controller must specify the IDs, hostnames, and ports of all controllers in `controller.quorum.voters`.

In contrast, when using a dynamic quorum, you should set `controller.quorum.bootstrap.servers` instead. This configuration key need not contain all the controllers, but it should contain as many as possible so that all the servers can locate the quorum. In other words, its function is much like the `bootstrap.servers` configuration used by Kafka clients.

If you are not sure whether you are using static or dynamic quorums, you can determine this by running something like the following:
    
    
      $ bin/kafka-features.sh --bootstrap-controller localhost:9093 describe
    

If the `kraft.version` field is level 0 or absent, you are using a static quorum. If it is 1 or above, you are using a dynamic quorum. For example, here is an example of a static quorum:
    
    
    Feature: kraft.version  SupportedMinVersion: 0  SupportedMaxVersion: 1  FinalizedVersionLevel: 0 Epoch: 5
    Feature: metadata.version       SupportedMinVersion: 3.3-IV3    SupportedMaxVersion: 3.9-IV0 FinalizedVersionLevel: 3.9-IV0  Epoch: 5
    

Here is another example of a static quorum:
    
    
    Feature: metadata.version       SupportedMinVersion: 3.3-IV3    SupportedMaxVersion: 3.8-IV0 FinalizedVersionLevel: 3.8-IV0  Epoch: 5
    

Here is an example of a dynamic quorum:
    
    
    Feature: kraft.version  SupportedMinVersion: 0  SupportedMaxVersion: 1  FinalizedVersionLevel: 1 Epoch: 5
    Feature: metadata.version       SupportedMinVersion: 3.3-IV3    SupportedMaxVersion: 3.9-IV0 FinalizedVersionLevel: 3.9-IV0  Epoch: 5
    

The static versus dynamic nature of the quorum is determined at the time of formatting. Specifically, the quorum will be formatted as dynamic if `controller.quorum.voters` is **not** present, and if the software version is Apache Kafka 3.9 or newer. If you have followed the instructions earlier in this document, you will get a dynamic quorum.

If you would like the formatting process to fail if a dynamic quorum cannot be achieved, format your controllers using the `--feature kraft.version=1`. (Note that you should not supply this flag when formatting brokers -- only when formatting controllers.)
    
    
      $ bin/kafka-storage.sh format -t KAFKA_CLUSTER_ID --feature kraft.version=1 -c controller_static.properties
      Cannot set kraft.version to 1 unless KIP-853 configuration is present. Try removing the --feature flag for kraft.version.
    

Note: Currently it is **not** possible to convert clusters using a static controller quorum to use a dynamic controller quorum. This function will be supported in the future release. 

### Add New Controller

If a dynamic controller cluster already exists, it can be expanded by first provisioning a new controller using the kafka-storage.sh tool and starting the controller. After starting the controller, the replication to the new controller can be monitored using the `bin/kafka-metadata-quorum.sh describe --replication` command. Once the new controller has caught up to the active controller, it can be added to the cluster using the `bin/kafka-metadata-quorum.sh add-controller` command. When using broker endpoints use the --bootstrap-server flag: 
    
    
    $ bin/kafka-metadata-quorum.sh --command-config config/controller.properties --bootstrap-server localhost:9092 add-controller

When using controller endpoints use the --bootstrap-controller flag: 
    
    
    $ bin/kafka-metadata-quorum.sh --command-config config/controller.properties --bootstrap-controller localhost:9093 add-controller

### Remove Controller

If the dynamic controller cluster already exists, it can be shrunk using the `bin/kafka-metadata-quorum.sh remove-controller` command. Until KIP-996: Pre-vote has been implemented and released, it is recommended to shutdown the controller that will be removed before running the remove-controller command. When using broker endpoints use the --bootstrap-server flag: 
    
    
    $ bin/kafka-metadata-quorum.sh --bootstrap-server localhost:9092 remove-controller --controller-id <id> --controller-directory-id <directory-id>

When using controller endpoints use the --bootstrap-controller flag: 
    
    
    $ bin/kafka-metadata-quorum.sh --bootstrap-controller localhost:9092 remove-controller --controller-id <id> --controller-directory-id <directory-id>

## Debugging

### Metadata Quorum Tool

The kafka-metadata-quorum.sh tool can be used to describe the runtime state of the cluster metadata partition. For example, the following command displays a summary of the metadata quorum:
    
    
    $ bin/kafka-metadata-quorum.sh --bootstrap-server localhost:9092 describe --status
    ClusterId:              fMCL8kv1SWm87L_Md-I2hg
    LeaderId:               3002
    LeaderEpoch:            2
    HighWatermark:          10
    MaxFollowerLag:         0
    MaxFollowerLagTimeMs:   -1
    CurrentVoters:          [{"id": 3000, "directoryId": "ILZ5MPTeRWakmJu99uBJCA", "endpoints": ["CONTROLLER://localhost:9093"]},
                             {"id": 3001, "directoryId": "b-DwmhtOheTqZzPoh52kfA", "endpoints": ["CONTROLLER://localhost:9094"]},
                             {"id": 3002, "directoryId": "g42deArWBTRM5A1yuVpMCg", "endpoints": ["CONTROLLER://localhost:9095"]}]
    CurrentObservers:       [{"id": 0, "directoryId": "3Db5QLSqSZieL3rJBUUegA"},
                             {"id": 1, "directoryId": "UegA3Db5QLSqSZieL3rJBU"},
                             {"id": 2, "directoryId": "L3rJBUUegA3Db5QLSqSZie"}]

### Dump Log Tool

The kafka-dump-log.sh tool can be used to debug the log segments and snapshots for the cluster metadata directory. The tool will scan the provided files and decode the metadata records. For example, this command decodes and prints the records in the first log segment:
    
    
    $ bin/kafka-dump-log.sh --cluster-metadata-decoder --files metadata_log_dir/__cluster_metadata-0/00000000000000000000.log

This command decodes and prints the records in the a cluster metadata snapshot:
    
    
    $ bin/kafka-dump-log.sh --cluster-metadata-decoder --files metadata_log_dir/__cluster_metadata-0/00000000000000000100-0000000001.checkpoint

### Metadata Shell

The kafka-metadata-shell.sh tool can be used to interactively inspect the state of the cluster metadata partition:
    
    
    $ bin/kafka-metadata-shell.sh --snapshot metadata_log_dir/__cluster_metadata-0/00000000000000000000.checkpoint
    >> ls /
    brokers  local  metadataQuorum  topicIds  topics
    >> ls /topics
    foo
    >> cat /topics/foo/0/data
    {
      "partitionId" : 0,
      "topicId" : "5zoAlv-xEh9xRANKXt1Lbg",
      "replicas" : [ 1 ],
      "isr" : [ 1 ],
      "removingReplicas" : null,
      "addingReplicas" : null,
      "leader" : 1,
      "leaderEpoch" : 0,
      "partitionEpoch" : 0
    }
    >> exit

## Deploying Considerations

  * Kafka server's `process.role` should be set to either `broker` or `controller` but not both. Combined mode can be used in development environments, but it should be avoided in critical deployment environments.
  * For redundancy, a Kafka cluster should use 3 or more controllers, depending on factors like cost and the number of concurrent failures your system should withstand without availability impact. For the KRaft controller cluster to withstand `N` concurrent failures the controller cluster must include `2N + 1` controllers.
  * The Kafka controllers store all the metadata for the cluster in memory and on disk. We believe that for a typical Kafka cluster 5GB of main memory and 5GB of disk space on the metadata log director is sufficient.



## ZooKeeper to KRaft Migration

In order to migrate from ZooKeeper to KRaft you need to use a bridge release. The last bridge release is Kafka 3.9. See the [ZooKeeper to KRaft Migration steps](/39/operations/kraft/#zookeeper-to-kraft-migration) in the 3.9 documentation.
