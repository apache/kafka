---
title: Geo-Replication (Cross-Cluster Data Mirroring)
description: Geo-Replication (Cross-Cluster Data Mirroring)
weight: 3
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


## Geo-Replication Overview

Kafka administrators can define data flows that cross the boundaries of individual Kafka clusters, data centers, or geo-regions. Such event streaming setups are often needed for organizational, technical, or legal requirements. Common scenarios include: 

  * Geo-replication
  * Disaster recovery
  * Feeding edge clusters into a central, aggregate cluster
  * Physical isolation of clusters (such as production vs. testing)
  * Cloud migration or hybrid cloud deployments
  * Legal and compliance requirements



Administrators can set up such inter-cluster data flows with Kafka's MirrorMaker (version 2), a tool to replicate data between different Kafka environments in a streaming manner. MirrorMaker is built on top of the Kafka Connect framework and supports features such as: 

  * Replicates topics (data plus configurations)
  * Replicates consumer groups including offsets to migrate applications between clusters
  * Replicates ACLs
  * Preserves partitioning
  * Automatically detects new topics and partitions
  * Provides a wide range of metrics, such as end-to-end replication latency across multiple data centers/clusters
  * Fault-tolerant and horizontally scalable operations



_Note: Geo-replication with MirrorMaker replicates data across Kafka clusters. This inter-cluster replication is different from Kafka'sintra-cluster replication, which replicates data within the same Kafka cluster._

## What Are Replication Flows

With MirrorMaker, Kafka administrators can replicate topics, topic configurations, consumer groups and their offsets, and ACLs from one or more source Kafka clusters to one or more target Kafka clusters, i.e., across cluster environments. In a nutshell, MirrorMaker uses Connectors to consume from source clusters and produce to target clusters. 

These directional flows from source to target clusters are called replication flows. They are defined with the format `{source_cluster}->{target_cluster}` in the MirrorMaker configuration file as described later. Administrators can create complex replication topologies based on these flows. 

Here are some example patterns: 

  * Active/Active high availability deployments: `A->B, B->A`
  * Active/Passive or Active/Standby high availability deployments: `A->B`
  * Aggregation (e.g., from many clusters to one): `A->K, B->K, C->K`
  * Fan-out (e.g., from one to many clusters): `K->A, K->B, K->C`
  * Forwarding: `A->B, B->C, C->D`



By default, a flow replicates all topics and consumer groups (except excluded ones). However, each replication flow can be configured independently. For instance, you can define that only specific topics or consumer groups are replicated from the source cluster to the target cluster. 

Here is a first example on how to configure data replication from a `primary` cluster to a `secondary` cluster (an active/passive setup): 
    
    
    # Basic settings
    clusters = primary, secondary
    primary.bootstrap.servers = broker3-primary:9092
    secondary.bootstrap.servers = broker5-secondary:9092
    
    # Define replication flows
    primary->secondary.enabled = true
    primary->secondary.topics = foobar-topic, quux-.*

## Configuring Geo-Replication

The following sections describe how to configure and run a dedicated MirrorMaker cluster. If you want to run MirrorMaker within an existing Kafka Connect cluster or other supported deployment setups, please refer to [KIP-382: MirrorMaker 2.0](https://cwiki.apache.org/confluence/display/KAFKA/KIP-382%3A+MirrorMaker+2.0) and be aware that the names of configuration settings may vary between deployment modes. 

Beyond what's covered in the following sections, further examples and information on configuration settings are available at: 

  * [MirrorMakerConfig](https://github.com/apache/kafka/blob/trunk/connect/mirror/src/main/java/org/apache/kafka/connect/mirror/MirrorMakerConfig.java), [MirrorConnectorConfig](https://github.com/apache/kafka/blob/trunk/connect/mirror/src/main/java/org/apache/kafka/connect/mirror/MirrorConnectorConfig.java)
  * [DefaultTopicFilter](https://github.com/apache/kafka/blob/trunk/connect/mirror/src/main/java/org/apache/kafka/connect/mirror/DefaultTopicFilter.java) for topics, [DefaultGroupFilter](https://github.com/apache/kafka/blob/trunk/connect/mirror/src/main/java/org/apache/kafka/connect/mirror/DefaultGroupFilter.java) for consumer groups
  * Example configuration settings in [connect-mirror-maker.properties](https://github.com/apache/kafka/blob/trunk/config/connect-mirror-maker.properties), [KIP-382: MirrorMaker 2.0](https://cwiki.apache.org/confluence/display/KAFKA/KIP-382%3A+MirrorMaker+2.0)



### Configuration File Syntax

The MirrorMaker configuration file is typically named `connect-mirror-maker.properties`. You can configure a variety of components in this file: 

  * MirrorMaker settings: global settings including cluster definitions (aliases), plus custom settings per replication flow
  * Kafka Connect and connector settings
  * Kafka producer, consumer, and admin client settings



Example: Define MirrorMaker settings (explained in more detail later). 
    
    
    # Global settings
    clusters = us-west, us-east   # defines cluster aliases
    us-west.bootstrap.servers = broker3-west:9092
    us-east.bootstrap.servers = broker5-east:9092
    
    topics = .*   # all topics to be replicated by default
    
    # Specific replication flow settings (here: flow from us-west to us-east)
    us-west->us-east.enabled = true
    us-west->us.east.topics = foo.*, bar.*  # override the default above

MirrorMaker is based on the Kafka Connect framework. Any Kafka Connect, source connector, and sink connector settings as described in the documentation chapter on Kafka Connect can be used directly in the MirrorMaker configuration, without having to change or prefix the name of the configuration setting. 

Example: Define custom Kafka Connect settings to be used by MirrorMaker. 
    
    
    # Setting Kafka Connect defaults for MirrorMaker
    tasks.max = 5

Most of the default Kafka Connect settings work well for MirrorMaker out-of-the-box, with the exception of `tasks.max`. In order to evenly distribute the workload across more than one MirrorMaker process, it is recommended to set `tasks.max` to at least `2` (preferably higher) depending on the available hardware resources and the total number of topic-partitions to be replicated. 

You can further customize MirrorMaker's Kafka Connect settings _per source or target cluster_ (more precisely, you can specify Kafka Connect worker-level configuration settings "per connector"). Use the format of `{cluster}.{config_name}` in the MirrorMaker configuration file. 

Example: Define custom connector settings for the `us-west` cluster. 
    
    
    # us-west custom settings
    us-west.offset.storage.topic = my-mirrormaker-offsets

MirrorMaker internally uses the Kafka producer, consumer, and admin clients. Custom settings for these clients are often needed. To override the defaults, use the following format in the MirrorMaker configuration file: 

  * `{source}.consumer.{consumer_config_name}`
  * `{target}.producer.{producer_config_name}`
  * `{source_or_target}.admin.{admin_config_name}`



Example: Define custom producer, consumer, admin client settings. 
    
    
    # us-west cluster (from which to consume)
    us-west.consumer.isolation.level = read_committed
    us-west.admin.bootstrap.servers = broker57-primary:9092
    
    # us-east cluster (to which to produce)
    us-east.producer.compression.type = gzip
    us-east.producer.buffer.memory = 32768
    us-east.admin.bootstrap.servers = broker8-secondary:9092

### Exactly once

Exactly-once semantics are supported for dedicated MirrorMaker clusters as of version 3.5.0.

For new MirrorMaker clusters, set the `exactly.once.source.support` property to enabled for all targeted Kafka clusters that should be written to with exactly-once semantics. For example, to enable exactly-once for writes to cluster `us-east`, the following configuration can be used: 
    
    
    us-east.exactly.once.source.support = enabled

For existing MirrorMaker clusters, a two-step upgrade is necessary. Instead of immediately setting the `exactly.once.source.support` property to enabled, first set it to `preparing` on all nodes in the cluster. Once this is complete, it can be set to `enabled` on all nodes in the cluster, in a second round of restarts. 

In either case, it is also necessary to enable intra-cluster communication between the MirrorMaker nodes, as described in [KIP-710](https://cwiki.apache.org/confluence/display/KAFKA/KIP-710%3A+Full+support+for+distributed+mode+in+dedicated+MirrorMaker+2.0+clusters). To do this, the `dedicated.mode.enable.internal.rest` property must be set to `true`. In addition, many of the REST-related [configuration properties available for Kafka Connect](https://kafka.apache.org/documentation/#connectconfigs) can be specified the MirrorMaker config. For example, to enable intra-cluster communication in MirrorMaker cluster with each node listening on port 8080 of their local machine, the following should be added to the MirrorMaker config file: 
    
    
    dedicated.mode.enable.internal.rest = true
    listeners = http://localhost:8080

**Note that, if intra-cluster communication is enabled in production environments, it is highly recommended to secure the REST servers brought up by each MirrorMaker node. See the[configuration properties for Kafka Connect](https://kafka.apache.org/documentation/#connectconfigs) for information on how this can be accomplished. **

It is also recommended to filter records from aborted transactions out from replicated data when running MirrorMaker. To do this, ensure that the consumer used to read from source clusters is configured with `isolation.level` set to `read_committed`. If replicating data from cluster `us-west`, this can be done for all replication flows that read from that cluster by adding the following to the MirrorMaker config file: 
    
    
    us-west.consumer.isolation.level = read_committed

As a final note, under the hood, MirrorMaker uses Kafka Connect source connectors to replicate data. For more information on exactly-once support for these kinds of connectors, see the [relevant docs page](https://kafka.apache.org/documentation/#connect_exactlyoncesource). 

### Creating and Enabling Replication Flows

To define a replication flow, you must first define the respective source and target Kafka clusters in the MirrorMaker configuration file. 

  * `clusters` (required): comma-separated list of Kafka cluster "aliases"
  * `{clusterAlias}.bootstrap.servers` (required): connection information for the specific cluster; comma-separated list of "bootstrap" Kafka brokers 


Example: Define two cluster aliases `primary` and `secondary`, including their connection information. 
    
    
    clusters = primary, secondary
    primary.bootstrap.servers = broker10-primary:9092,broker-11-primary:9092
    secondary.bootstrap.servers = broker5-secondary:9092,broker6-secondary:9092

Secondly, you must explicitly enable individual replication flows with `{source}->{target}.enabled = true` as needed. Remember that flows are directional: if you need two-way (bidirectional) replication, you must enable flows in both directions. 
    
    
    # Enable replication from primary to secondary
    primary->secondary.enabled = true

By default, a replication flow will replicate all but a few special topics and consumer groups from the source cluster to the target cluster, and automatically detect any newly created topics and groups. The names of replicated topics in the target cluster will be prefixed with the name of the source cluster (see section further below). For example, the topic `foo` in the source cluster `us-west` would be replicated to a topic named `us-west.foo` in the target cluster `us-east`. 

The subsequent sections explain how to customize this basic setup according to your needs. 

### Configuring Replication Flows

The configuration of a replication flow is a combination of top-level default settings (e.g., `topics`), on top of which flow-specific settings, if any, are applied (e.g., `us-west->us-east.topics`). To change the top-level defaults, add the respective top-level setting to the MirrorMaker configuration file. To override the defaults for a specific replication flow only, use the syntax format `{source}->{target}.{config.name}`. 

The most important settings are: 

  * `topics`: list of topics or a regular expression that defines which topics in the source cluster to replicate (default: `topics = .*`) 
  * `topics.exclude`: list of topics or a regular expression to subsequently exclude topics that were matched by the `topics` setting (default: `topics.exclude = .*[\-\.]internal, .*\.replica, __.*`) 
  * `groups`: list of topics or regular expression that defines which consumer groups in the source cluster to replicate (default: `groups = .*`) 
  * `groups.exclude`: list of topics or a regular expression to subsequently exclude consumer groups that were matched by the `groups` setting (default: `groups.exclude = console-consumer-.*, connect-.*, __.*`) 
  * `{source}->{target}.enable`: set to `true` to enable the replication flow (default: `false`) 


Example: 
    
    
    # Custom top-level defaults that apply to all replication flows
    topics = .*
    groups = consumer-group1, consumer-group2
    
    # Don't forget to enable a flow!
    us-west->us-east.enabled = true
    
    # Custom settings for specific replication flows
    us-west->us-east.topics = foo.*
    us-west->us-east.groups = bar.*
    us-west->us-east.emit.heartbeats = false

Additional configuration settings are supported which can be left with their default values in most cases. See [MirrorMaker Configs](/documentation/#mirrormakerconfigs). 

### Securing Replication Flows

MirrorMaker supports the same security settings as Kafka Connect, so please refer to the linked section for further information. 

Example: Encrypt communication between MirrorMaker and the `us-east` cluster. 
    
    
    us-east.security.protocol=SSL
    us-east.ssl.truststore.location=/path/to/truststore.jks
    us-east.ssl.truststore.password=my-secret-password
    us-east.ssl.keystore.location=/path/to/keystore.jks
    us-east.ssl.keystore.password=my-secret-password
    us-east.ssl.key.password=my-secret-password

### Custom Naming of Replicated Topics in Target Clusters

Replicated topics in a target cluster—sometimes called _remote_ topics—are renamed according to a replication policy. MirrorMaker uses this policy to ensure that events (aka records, messages) from different clusters are not written to the same topic-partition. By default as per [DefaultReplicationPolicy](https://github.com/apache/kafka/blob/trunk/connect/mirror-client/src/main/java/org/apache/kafka/connect/mirror/DefaultReplicationPolicy.java), the names of replicated topics in the target clusters have the format `{source}.{source_topic_name}`: 
    
    
    us-west         us-east
    =========       =================
                    bar-topic
    foo-topic  -->  us-west.foo-topic

You can customize the separator (default: `.`) with the `replication.policy.separator` setting: 
    
    
    # Defining a custom separator
    us-west->us-east.replication.policy.separator = _

If you need further control over how replicated topics are named, you can implement a custom `ReplicationPolicy` and override `replication.policy.class` (default is `DefaultReplicationPolicy`) in the MirrorMaker configuration. 

### Preventing Configuration Conflicts

MirrorMaker processes share configuration via their target Kafka clusters. This behavior may cause conflicts when configurations differ among MirrorMaker processes that operate against the same target cluster. 

For example, the following two MirrorMaker processes would be racy: 
    
    
    # Configuration of process 1
    A->B.enabled = true
    A->B.topics = foo
    
    # Configuration of process 2
    A->B.enabled = true
    A->B.topics = bar

In this case, the two processes will share configuration via cluster `B`, which causes a conflict. Depending on which of the two processes is the elected "leader", the result will be that either the topic `foo` or the topic `bar` is replicated, but not both. 

It is therefore important to keep the MirrorMaker configuration consistent across replication flows to the same target cluster. This can be achieved, for example, through automation tooling or by using a single, shared MirrorMaker configuration file for your entire organization. 

### Best Practice: Consume from Remote, Produce to Local

To minimize latency ("producer lag"), it is recommended to locate MirrorMaker processes as close as possible to their target clusters, i.e., the clusters that it produces data to. That's because Kafka producers typically struggle more with unreliable or high-latency network connections than Kafka consumers. 
    
    
    First DC          Second DC
    ==========        =========================
    primary --------- MirrorMaker --> secondary
    (remote)                           (local)

To run such a "consume from remote, produce to local" setup, run the MirrorMaker processes close to and preferably in the same location as the target clusters, and explicitly set these "local" clusters in the `--clusters` command line parameter (blank-separated list of cluster aliases): 
    
    
    # Run in secondary's data center, reading from the remote `primary` cluster
    $ bin/connect-mirror-maker.sh connect-mirror-maker.properties --clusters secondary

The `--clusters secondary` tells the MirrorMaker process that the given cluster(s) are nearby, and prevents it from replicating data or sending configuration to clusters at other, remote locations. 

### Example: Active/Passive High Availability Deployment

The following example shows the basic settings to replicate topics from a primary to a secondary Kafka environment, but not from the secondary back to the primary. Please be aware that most production setups will need further configuration, such as security settings. 
    
    
    # Unidirectional flow (one-way) from primary to secondary cluster
    primary.bootstrap.servers = broker1-primary:9092
    secondary.bootstrap.servers = broker2-secondary:9092
    
    primary->secondary.enabled = true
    secondary->primary.enabled = false
    
    primary->secondary.topics = foo.*  # only replicate some topics

### Example: Active/Active High Availability Deployment

The following example shows the basic settings to replicate topics between two clusters in both ways. Please be aware that most production setups will need further configuration, such as security settings. 
    
    
    # Bidirectional flow (two-way) between us-west and us-east clusters
    clusters = us-west, us-east
    us-west.bootstrap.servers = broker1-west:9092,broker2-west:9092
    Us-east.bootstrap.servers = broker3-east:9092,broker4-east:9092
    
    us-west->us-east.enabled = true
    us-east->us-west.enabled = true

_Note on preventing replication "loops" (where topics will be originally replicated from A to B, then the replicated topics will be replicated yet again from B to A, and so forth)_ : As long as you define the above flows in the same MirrorMaker configuration file, you do not need to explicitly add `topics.exclude` settings to prevent replication loops between the two clusters. 

### Example: Multi-Cluster Geo-Replication

Let's put all the information from the previous sections together in a larger example. Imagine there are three data centers (west, east, north), with two Kafka clusters in each data center (e.g., `west-1`, `west-2`). The example in this section shows how to configure MirrorMaker (1) for Active/Active replication within each data center, as well as (2) for Cross Data Center Replication (XDCR). 

First, define the source and target clusters along with their replication flows in the configuration: 
    
    
    # Basic settings
    clusters: west-1, west-2, east-1, east-2, north-1, north-2
    west-1.bootstrap.servers = ...
    west-2.bootstrap.servers = ...
    east-1.bootstrap.servers = ...
    east-2.bootstrap.servers = ...
    north-1.bootstrap.servers = ...
    north-2.bootstrap.servers = ...
    
    # Replication flows for Active/Active in West DC
    west-1->west-2.enabled = true
    west-2->west-1.enabled = true
    
    # Replication flows for Active/Active in East DC
    east-1->east-2.enabled = true
    east-2->east-1.enabled = true
    
    # Replication flows for Active/Active in North DC
    north-1->north-2.enabled = true
    north-2->north-1.enabled = true
    
    # Replication flows for XDCR via west-1, east-1, north-1
    west-1->east-1.enabled  = true
    west-1->north-1.enabled = true
    east-1->west-1.enabled  = true
    east-1->north-1.enabled = true
    north-1->west-1.enabled = true
    north-1->east-1.enabled = true

Then, in each data center, launch one or more MirrorMaker as follows: 
    
    
    # In West DC:
    $ bin/connect-mirror-maker.sh connect-mirror-maker.properties --clusters west-1 west-2
    
    # In East DC:
    $ bin/connect-mirror-maker.sh connect-mirror-maker.properties --clusters east-1 east-2
    
    # In North DC:
    $ bin/connect-mirror-maker.sh connect-mirror-maker.properties --clusters north-1 north-2

With this configuration, records produced to any cluster will be replicated within the data center, as well as across to other data centers. By providing the `--clusters` parameter, we ensure that each MirrorMaker process produces data to nearby clusters only. 

_Note:_ The `--clusters` parameter is, technically, not required here. MirrorMaker will work fine without it. However, throughput may suffer from "producer lag" between data centers, and you may incur unnecessary data transfer costs. 

## Starting Geo-Replication

You can run as few or as many MirrorMaker processes (think: nodes, servers) as needed. Because MirrorMaker is based on Kafka Connect, MirrorMaker processes that are configured to replicate the same Kafka clusters run in a distributed setup: They will find each other, share configuration (see section below), load balance their work, and so on. If, for example, you want to increase the throughput of replication flows, one option is to run additional MirrorMaker processes in parallel. 

To start a MirrorMaker process, run the command: 
    
    
    $ bin/connect-mirror-maker.sh connect-mirror-maker.properties

After startup, it may take a few minutes until a MirrorMaker process first begins to replicate data. 

Optionally, as described previously, you can set the parameter `--clusters` to ensure that the MirrorMaker process produces data to nearby clusters only. 
    
    
    # Note: The cluster alias us-west must be defined in the configuration file
    $ bin/connect-mirror-maker.sh connect-mirror-maker.properties \
        --clusters us-west
    

_Note when testing replication of consumer groups:_ By default, MirrorMaker does not replicate consumer groups created by the kafka-console-consumer.sh tool, which you might use to test your MirrorMaker setup on the command line. If you do want to replicate these consumer groups as well, set the `groups.exclude` configuration accordingly (default: `groups.exclude = console-consumer-.*, connect-.*, __.*`). Remember to update the configuration again once you completed your testing. 

## Stopping Geo-Replication

You can stop a running MirrorMaker process by sending a SIGTERM signal with the command: 
    
    
    $ kill <MirrorMaker pid>

## Applying Configuration Changes

To make configuration changes take effect, the MirrorMaker process(es) must be restarted. 

## Monitoring Geo-Replication

It is recommended to monitor MirrorMaker processes to ensure all defined replication flows are up and running correctly. MirrorMaker is built on the Connect framework and inherits all of Connect's metrics, such `source-record-poll-rate`. In addition, MirrorMaker produces its own metrics under the `kafka.connect.mirror` metric group. Metrics are tagged with the following properties: 

  * `source`: alias of source cluster (e.g., `primary`)
  * `target`: alias of target cluster (e.g., `secondary`)
  * `topic`: replicated topic on target cluster
  * `partition`: partition being replicated



Metrics are tracked for each replicated topic. The source cluster can be inferred from the topic name. For example, replicating `topic1` from `primary->secondary` will yield metrics like: 

  * `target=secondary`
  * `topic=primary.topic1`
  * `partition=1` 


The following metrics are emitted: 
    
    
    # MBean: kafka.connect.mirror:type=MirrorSourceConnector,target=([-.w]+),topic=([-.w]+),partition=([0-9]+)
    record-count            # number of records replicated source -> target
    record-age-ms           # age of records when they are replicated
    record-age-ms-min
    record-age-ms-max
    record-age-ms-avg
    replication-latency-ms  # time it takes records to propagate source->target
    replication-latency-ms-min
    replication-latency-ms-max
    replication-latency-ms-avg
    byte-rate               # average number of bytes/sec in replicated records
    
    # MBean: kafka.connect.mirror:type=MirrorCheckpointConnector,source=([-.w]+),target=([-.w]+)
    
    checkpoint-latency-ms   # time it takes to replicate consumer offsets
    checkpoint-latency-ms-min
    checkpoint-latency-ms-max
    checkpoint-latency-ms-avg

These metrics do not differentiate between created-at and log-append timestamps. 
