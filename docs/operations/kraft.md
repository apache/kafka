---
title: KRaft
description: KRaft
weight: 10
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
  * If `process.roles` is not set at all, it is assumed to be in ZooKeeper mode.



Kafka servers that act as both brokers and controllers are referred to as "combined" servers. Combined servers are simpler to operate for small use cases like a development environment. The key disadvantage is that the controller will be less isolated from the rest of the system. For example, it is not possible to roll or scale the controllers separately from the brokers in combined mode. Combined mode is not recommended in critical deployment environments.

### Controllers

In KRaft mode, specific Kafka servers are selected to be controllers (unlike the ZooKeeper-based mode, where any server can become the Controller). The servers selected to be controllers will participate in the metadata quorum. Each controller is either an active or a hot standby for the current active controller.

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

The `kafka-storage.sh random-uuid` command can be used to generate a cluster ID for your new cluster. This cluster ID must be used when formatting each server in the cluster with the `kafka-storage.sh format` command. 

This is different from how Kafka has operated in the past. Previously, Kafka would format blank storage directories automatically, and also generate a new cluster ID automatically. One reason for the change is that auto-formatting can sometimes obscure an error condition. This is particularly important for the metadata log maintained by the controller and broker servers. If a majority of the controllers were able to start with an empty log directory, a leader might be able to be elected with missing committed data.

### Bootstrap a Standalone Controller

The recommended method for creating a new KRaft controller cluster is to bootstrap it with one voter and dynamically add the rest of the controllers. Bootstrapping the first controller can be done with the following CLI command: 
    
    
    $ bin/kafka-storage.sh format --cluster-id <cluster-id> --standalone --config ./config/kraft/controller.properties

This command will 1) create a meta.properties file in metadata.log.dir with a randomly generated directory.id, 2) create a snapshot at 00000000000000000000-0000000000.checkpoint with the necessary control records (KRaftVersionRecord and VotersRecord) to make this Kafka node the only voter for the quorum. 

### Bootstrap with Multiple Controllers

The KRaft cluster metadata partition can also be bootstrapped with more than one voter. This can be done by using the --initial-controllers flag: 
    
    
    cluster-id=$(bin/kafka-storage.sh random-uuid)
    controller-0-uuid=$(bin/kafka-storage.sh random-uuid)
    controller-1-uuid=$(bin/kafka-storage.sh random-uuid)
    controller-2-uuid=$(bin/kafka-storage.sh random-uuid)
    
    # In each controller execute
    bin/kafka-storage.sh format --cluster-id ${cluster-id} \
                         --initial-controllers "0@controller-0:1234:${controller-0-uuid},1@controller-1:1234:${controller-1-uuid},2@controller-2:1234:${controller-2-uuid}" \
                         --config config/kraft/controller.properties

This command is similar to the standalone version but the snapshot at 00000000000000000000-0000000000.checkpoint will instead contain a VotersRecord that includes information for all of the controllers specified in --initial-controllers. It is important that the value of this flag is the same in all of the controllers with the same cluster id. In the replica description 0@controller-0:1234:3Db5QLSqSZieL3rJBUUegA, 0 is the replica id, 3Db5QLSqSZieL3rJBUUegA is the replica directory id, controller-0 is the replica's host and 1234 is the replica's port. 

### Formatting Brokers and New Controllers

When provisioning new broker and controller nodes that we want to add to an existing Kafka cluster, use the `kafka-storage.sh format` command with the --no-initial-controllers flag. 
    
    
    $ bin/kafka-storage.sh format --cluster-id <cluster-id> --config config/kraft/server.properties --no-initial-controllers

## Controller membership changes

### Static versus Dynamic KRaft Quorums

There are two ways to run KRaft: the old way using static controller quorums, and the new way using KIP-853 dynamic controller quorums.

When using a static quorum, the configuration file for each broker and controller must specify the IDs, hostnames, and ports of all controllers in `controller.quorum.voters`.

In contrast, when using a dynamic quorum, you should set `controller.quorum.bootstrap.servers` instead. This configuration key need not contain all the controllers, but it should contain as many as possible so that all the servers can locate the quorum. In other words, its function is much like the `bootstrap.servers` configuration used by Kafka clients.

If you are not sure whether you are using static or dynamic quorums, you can determine this by running something like the following:
    
    
      $ bin/kafka-features.sh --bootstrap-controller localhost:9093 describe
    

If the `kraft.version` field is level 0 or absent, you are using a static quorum. If it is 1 or above, you are using a dynamic quorum. For example, here is an example of a static quorum:
    
    
    Feature: kraft.version  SupportedMinVersion: 0  SupportedMaxVersion: 1  FinalizedVersionLevel: 0 Epoch: 5
    Feature: metadata.version       SupportedMinVersion: 3.0-IV1    SupportedMaxVersion: 3.9-IV0 FinalizedVersionLevel: 3.9-IV0  Epoch: 5
    

Here is another example of a static quorum:
    
    
    Feature: metadata.version       SupportedMinVersion: 3.0-IV1    SupportedMaxVersion: 3.8-IV0 FinalizedVersionLevel: 3.8-IV0  Epoch: 5
    

Here is an example of a dynamic quorum:
    
    
    Feature: kraft.version  SupportedMinVersion: 0  SupportedMaxVersion: 1  FinalizedVersionLevel: 1 Epoch: 5
    Feature: metadata.version       SupportedMinVersion: 3.0-IV1    SupportedMaxVersion: 3.9-IV0 FinalizedVersionLevel: 3.9-IV0  Epoch: 5
    

The static versus dynamic nature of the quorum is determined at the time of formatting. Specifically, the quorum will be formatted as dynamic if `controller.quorum.voters` is **not** present, and if the software version is Apache Kafka 3.9 or newer. If you have followed the instructions earlier in this document, you will get a dynamic quorum.

If you would like the formatting process to fail if a dynamic quorum cannot be achieved, format your controllers using the `--feature kraft.version=1`. (Note that you should not supply this flag when formatting brokers -- only when formatting controllers.)
    
    
      $ bin/kafka-storage.sh format -t KAFKA_CLUSTER_ID --feature kraft.version=1 -c config/kraft/controller.properties
      Cannot set kraft.version to 1 unless KIP-853 configuration is present. Try removing the --feature flag for kraft.version.
    

Note: Currently it is **not** possible to convert clusters using a static controller quorum to use a dynamic controller quorum. This function will be supported in the future release. 

### Add New Controller

If a dynamic controller cluster already exists, it can be expanded by first provisioning a new controller using the kafka-storage.sh tool and starting the controller. After starting the controller, the replication to the new controller can be monitored using the `kafka-metadata-quorum describe --replication` command. Once the new controller has caught up to the active controller, it can be added to the cluster using the `kafka-metadata-quorum add-controller` command. When using broker endpoints use the --bootstrap-server flag: 
    
    
    $ bin/kafka-metadata-quorum.sh --command-config config/kraft/controller.properties --bootstrap-server localhost:9092 add-controller

When using controller endpoints use the --bootstrap-controller flag: 
    
    
    $ bin/kafka-metadata-quorum.sh --command-config config/kraft/controller.properties --bootstrap-controller localhost:9092 add-controller

### Remove Controller

If the dynamic controller cluster already exists, it can be shrunk using the `bin/kafka-metadata-quorum.sh remove-controller` command. Until KIP-996: Pre-vote has been implemented and released, it is recommended to shutdown the controller that will be removed before running the remove-controller command. When using broker endpoints use the --bootstrap-server flag: 
    
    
    $ bin/kafka-metadata-quorum --bootstrap-server localhost:9092 remove-controller --controller-id <id> --controller-directory-id <directory-id>

When using controller endpoints use the --bootstrap-controller flag: 
    
    
    $ bin/kafka-metadata-quorum --bootstrap-controller localhost:9092 remove-controller --controller-id <id> --controller-directory-id <directory-id>

## Debugging

### Metadata Quorum Tool

The `kafka-metadata-quorum` tool can be used to describe the runtime state of the cluster metadata partition. For example, the following command displays a summary of the metadata quorum:
    
    
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

The `kafka-dump-log` tool can be used to debug the log segments and snapshots for the cluster metadata directory. The tool will scan the provided files and decode the metadata records. For example, this command decodes and prints the records in the first log segment:
    
    
    $ bin/kafka-dump-log.sh --cluster-metadata-decoder --files metadata_log_dir/__cluster_metadata-0/00000000000000000000.log

This command decodes and prints the records in the a cluster metadata snapshot:
    
    
    $ bin/kafka-dump-log.sh --cluster-metadata-decoder --files metadata_log_dir/__cluster_metadata-0/00000000000000000100-0000000001.checkpoint

### Metadata Shell

The `kafka-metadata-shell` tool can be used to interactively inspect the state of the cluster metadata partition:
    
    
    $ bin/kafka-metadata-shell.sh --snapshot metadata_log_dir/__cluster_metadata-0/00000000000000000000.log
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



## Missing Features

The following features are not fully implemented in KRaft mode:

  * Supporting JBOD configurations with multiple storage directories. Note that an Early Access release is supported in 3.7 as per [KIP-858](https://cwiki.apache.org/confluence/display/KAFKA/KIP-858%3A+Handle+JBOD+broker+disk+failure+in+KRaft). Note that it is not yet recommended for use in production environments. Please refer to the [release notes](https://cwiki.apache.org/confluence/display/KAFKA/Kafka+JBOD+in+KRaft+Early+Access+Release+Notes) to help us test it and provide feedback at [KAFKA-16061](https://issues.apache.org/jira/browse/KAFKA-16061).
  * Modifying certain dynamic configurations on the standalone KRaft controller



## ZooKeeper to KRaft Migration

### Terminology

  * Brokers that are in **ZK mode** store their metadata in Apache ZooKepeer. This is the old mode of handling metadata.
  * Brokers that are in **KRaft mode** store their metadata in a KRaft quorum. This is the new and improved mode of handling metadata.
  * **Migration** is the process of moving cluster metadata from ZooKeeper into a KRaft quorum.



### Migration Phases

In general, the migration process passes through several phases. 

  * In the **initial phase** , all the brokers are in ZK mode, and there is a ZK-based controller.
  * During the **initial metadata load** , a KRaft quorum loads the metadata from ZooKeeper,
  * In **hybrid phase** , some brokers are in ZK mode, but there is a KRaft controller.
  * In **dual-write phase** , all brokers are KRaft, but the KRaft controller is continuing to write to ZK.
  * When the migration has been **finalized** , we no longer write metadata to ZooKeeper.



### Limitations

  * While a cluster is being migrated from ZK mode to KRaft mode, we do not support changing the _metadata version_ (also known as the _inter.broker.protocol.version_.) Please do not attempt to do this during a migration, or you may break the cluster.
  * After the migration has been finalized, it is not possible to revert back to ZooKeeper mode.
  * During the migration, if a ZK broker is running with multiple log directories, any directory failure will cause the broker to shutdown. Brokers with broken log directories will only be able to migrate to KRaft once the directories are repaired. For further details refer to [KAFKA-16431](https://issues.apache.org/jira/browse/KAFKA-16431). 
  * As noted above, some features are not fully implemented in KRaft mode. If you are using one of those features, you will not be able to migrate to KRaft yet.
  * There is a known inconsistency between ZK and KRaft modes in the arguments passed to an `AlterConfigPolicy`, when operations of type `SUBTRACT`, `DELETE` or `APPEND` are processed. This has been addressed with a compatibility flag in version 3.9.2. For further details see [KIP-1252](https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=399279475).



### Preparing for migration

Before beginning the migration, the Kafka brokers must be upgraded to software version 3.9.1 and have the "inter.broker.protocol.version" configuration set to "3.9". 

It is recommended to enable TRACE level logging for the migration components while the migration is active. This can be done by adding the following log4j configuration to each KRaft controller's "log4j.properties" file. 
    
    
    log4j.logger.org.apache.kafka.metadata.migration=TRACE

It is generally useful to enable DEBUG logging on the KRaft controllers and the ZK brokers during the migration. 

### Provisioning the KRaft controller quorum

Two things are needed before the migration can begin. First, the brokers must be configured to support the migration and second, a KRaft controller quorum must be deployed. The KRaft controllers should be provisioned with the same cluster ID as the existing Kafka cluster. This can be found by examining one of the "meta.properties" files in the data directories of the brokers, or by running the following command. 
    
    
    $ bin/zookeeper-shell.sh localhost:2181 get /cluster/id

The KRaft controller quorum should also be provisioned with the latest `metadata.version`. This is done automatically when you format the node with the `kafka-storage.sh` tool. For further instructions on KRaft deployment, please refer to the above documentation. 

In addition to the standard KRaft configuration, the KRaft controllers will need to enable support for the migration as well as provide ZooKeeper connection configuration. 

Here is a sample config for a KRaft controller that is ready for migration: 
    
    
    # Sample KRaft cluster controller.properties listening on 9093
    process.roles=controller
    node.id=3000
    controller.quorum.bootstrap.servers=localhost:9093
    controller.listener.names=CONTROLLER
    listeners=CONTROLLER://:9093
    
    # Enable the migration
    zookeeper.metadata.migration.enable=true
    
    # ZooKeeper client configuration
    zookeeper.connect=localhost:2181
    
    # The inter broker listener in brokers to allow KRaft controller send RPCs to brokers
    inter.broker.listener.name=PLAINTEXT
    
    # Other configs ...

Follow these steps to format and start a new standalone controller: 
    
    
    # Save the previously retrieved cluster ID from ZooKeeper in a variable called zk-cluster-id
    $ bin/kafka-storage.sh format --standalone --cluster-id=<zk-cluster-id> -c config/kraft/controller.properties
    $ bin/kafka-server-start.sh config/kraft/controller.properties

Note: The migration can stall if the ZooKeeper Security Migration Tool was previously executed (see [KAFKA-19480](https://issues.apache.org/jira/browse/KAFKA-19480) for more details). As a workaround, the malformed "/migration" node can be removed from ZooKeeper by running `delete /migration` with the `zookeeper-shell.sh` CLI tool.

_Note: The KRaft cluster`node.id` values must be different from any existing ZK broker `broker.id`. In KRaft-mode, the brokers and controllers share the same Node ID namespace._

### Enter Migration Mode on the Brokers

Once the KRaft controller quorum has been started, the brokers will need to be reconfigured and restarted. Brokers may be restarted in a rolling fashion to avoid impacting cluster availability. Each broker requires the following configuration to communicate with the KRaft controllers and to enable the migration. 

  * broker.id: Ensure `broker.id` is set to a non-negative integer even if `broker.id.generation.enable` is enabled (default is enabled). Additionally, ensure `broker.id` does not exceed `reserved.broker.max.id` to avoid failure.
  * controller.quorum.bootstrap.servers
  * controller.listener.names
  * The controller.listener.name should also be added to listener.security.property.map
  * zookeeper.metadata.migration.enable



Here is a sample config for a broker that is ready for migration:
    
    
    # Sample ZK broker server.properties listening on 9092
    broker.id=0
    listeners=PLAINTEXT://:9092
    advertised.listeners=PLAINTEXT://localhost:9092
    listener.security.protocol.map=PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT
    
    # Set the IBP
    inter.broker.protocol.version=3.9
    
    # Enable the migration
    zookeeper.metadata.migration.enable=true
    
    # ZooKeeper client configuration
    zookeeper.connect=localhost:2181
    
    # KRaft controller quorum configuration
    controller.quorum.bootstrap.servers=localhost:9093
    controller.listener.names=CONTROLLER

_Note: Once the final ZK broker has been restarted with the necessary configuration, the migration will automatically begin._ When the migration is complete, an INFO level log can be observed on the active controller: 
    
    
    Completed migration of metadata from Zookeeper to KRaft

### Migrating brokers to KRaft

Once the KRaft controller completes the metadata migration, the brokers will still be running in ZooKeeper mode. While the KRaft controller is in migration mode, it will continue sending controller RPCs to the ZooKeeper mode brokers. This includes RPCs like UpdateMetadata and LeaderAndIsr. 

To migrate the brokers to KRaft, they simply need to be reconfigured as KRaft brokers and restarted. Using the above broker configuration as an example, we would replace the `broker.id` with `node.id` and add `process.roles=broker`. It is important that the broker maintain the same Broker/Node ID when it is restarted. The zookeeper configurations should be removed at this point. Finally, if you have set `control.plane.listener.name`. please remove it before restarting in KRaft mode. 

If your broker has authorization configured via the `authorizer.class.name` property using `kafka.security.authorizer.AclAuthorizer`, this is also the time to change it to use `org.apache.kafka.metadata.authorizer.StandardAuthorizer` instead. 
    
    
    # Sample KRaft broker server.properties listening on 9092
    process.roles=broker
    node.id=0
    listeners=PLAINTEXT://:9092
    advertised.listeners=PLAINTEXT://localhost:9092
    listener.security.protocol.map=PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT
    
    # Don't set the IBP, KRaft uses "metadata.version" feature flag
    # inter.broker.protocol.version=3.9
    
    # Remove the migration enabled flag
    # zookeeper.metadata.migration.enable=true
    
    # Remove ZooKeeper client configuration
    # zookeeper.connect=localhost:2181
    
    # Keep the KRaft controller quorum configuration
    controller.quorum.bootstrap.servers=localhost:9093
    controller.listener.names=CONTROLLER

Each broker is restarted with a KRaft configuration until the entire cluster is running in KRaft mode. 

### Finalizing the migration

Once all brokers have been restarted in KRaft mode, the last step to finalize the migration is to take the KRaft controllers out of migration mode. This is done by removing the "zookeeper.metadata.migration.enable" property from each of their configs and restarting them one at a time. 

Once the migration has been finalized, you can safely deprovision your ZooKeeper cluster, assuming you are not using it for anything else. After this point, it is no longer possible to revert to ZooKeeper mode. 
    
    
    # Sample KRaft cluster controller.properties listening on 9093
    process.roles=controller
    node.id=3000
    controller.quorum.bootstrap.servers=localhost:9093
    controller.listener.names=CONTROLLER
    listeners=CONTROLLER://:9093
    
    # Disable the migration
    # zookeeper.metadata.migration.enable=true
    
    # Remove ZooKeeper client configuration
    # zookeeper.connect=localhost:2181
    
    # Other configs ...

### Reverting to ZooKeeper mode During the Migration

While the cluster is still in migration mode, it is possible to revert to ZooKeeper mode. The process to follow depends on how far the migration has progressed. In order to find out how to revert, select the **final** migration step that you have **completed** in this table. 

Note that the directions given here assume that each step was fully completed, and they were done in order. So, for example, we assume that if "Enter Migration Mode on the Brokers" was completed, "Provisioning the KRaft controller quorum" was also fully completed previously. 

If you did not fully complete any step, back out whatever you have done and then follow revert directions for the last fully completed step.   
  
<table>  
<tr>  
<th>

Final Migration Section Completed
</th>  
<th>

Directions for Reverting
</th>  
<th>

Notes
</th> </tr>  
<tr>  
<td>

Preparing for migration
</td>  
<td>

The preparation section does not involve leaving ZooKeeper mode. So there is nothing to do in the case of a revert. 
</td>  
<td>


</td> </tr>  
<tr>  
<td>

Provisioning the KRaft controller quorum
</td>  
<td>



  * Deprovision the KRaft controller quorum. 
  * Then you are done. 


</td>  
<td>


</td> </tr>  
<tr>  
<td>

Enter Migration Mode on the brokers
</td>  
<td>



  * Deprovision the KRaft controller quorum. 
  * Using `zookeeper-shell.sh`, run `delete /controller` so that one of the brokers can become the new old-style controller. Additionally, run `get /migration` followed by `delete /migration` to clear the migration state from ZooKeeper. This will allow you to re-attempt the migration in the future. The data read from "/migration" can be useful for debugging. 
  * On each broker, remove the `zookeeper.metadata.migration.enable`, `controller.listener.names`, and `controller.quorum.bootstrap.servers` configurations, and replace `node.id` with `broker.id`. Then perform a rolling restart of all brokers. 
  * Then you are done. 


</td>  
<td>

It is important to perform the `zookeeper-shell.sh` step **quickly** , to minimize the amount of time that the cluster lacks a controller. Until the ` /controller` znode is deleted, you can also ignore any errors in the broker log about failing to connect to the Kraft controller. Those error logs should disappear after second roll to pure zookeeper mode. 
</td> </tr>  
<tr>  
<td>

Migrating brokers to KRaft
</td>  
<td>



  * On each broker, remove the `process.roles` configuration, replace the `node.id` with `broker.id` and restore the `zookeeper.connect` configuration to its previous value. If your cluster requires other ZooKeeper configurations for brokers, such as `zookeeper.ssl.protocol`, re-add those configurations as well. Then perform a rolling restart of all brokers. 
  * Deprovision the KRaft controller quorum. 
  * Using `zookeeper-shell.sh`, run `delete /controller` so that one of the brokers can become the new old-style controller. Additionally, run `get /migration` followed by `delete /migration` to clear the migration state from ZooKeeper. This will allow you to re-attempt the migration in the future. The data read from "/migration" can be useful for debugging. 
  * On each broker, remove the `zookeeper.metadata.migration.enable`, `controller.listener.names`, and `controller.quorum.bootstrap.servers` configurations. Then perform a second rolling restart of all brokers. 
  * Then you are done. 


</td>  
<td>



  * It is important to perform the `zookeeper-shell.sh` step **quickly** , to minimize the amount of time that the cluster lacks a controller. Until the ` /controller` znode is deleted, you can also ignore any errors in the broker log about failing to connect to the Kraft controller. Those error logs should disappear after second roll to pure zookeeper mode. 
  * Make sure that on the first cluster roll, `zookeeper.metadata.migration.enable` remains set to `true`. **Do not set it to false until the second cluster roll.**


</td> </tr>  
<tr>  
<td>

Finalizing the migration
</td>  
<td>

If you have finalized the ZK migration, then you cannot revert. 
</td>  
<td>

Some users prefer to wait for a week or two before finalizing the migration. While this requires you to keep the ZooKeeper cluster running for a while longer, it may be helpful in validating KRaft mode in your cluster. 
</td> </tr> </table>
