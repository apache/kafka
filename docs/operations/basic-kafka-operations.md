---
title: Basic Kafka Operations
description: Basic Kafka Operations
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


This section will review the most common operations you will perform on your Kafka cluster. All of the tools reviewed in this section are available under the `bin/` directory of the Kafka distribution and each tool will print details on all possible commandline options if it is run with no arguments. 

## Adding and removing topics

You have the option of either adding topics manually or having them be created automatically when data is first published to a non-existent topic. If topics are auto-created then you may want to tune the default topic configurations used for auto-created topics. 

Topics are added and modified using the topic tool: 
    
    
    $ bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic my_topic_name \
        --partitions 20 --replication-factor 3 --config x=y

The replication factor controls how many servers will replicate each message that is written. If you have a replication factor of 3 then up to 2 servers can fail before you will lose access to your data. We recommend you use a replication factor of 2 or 3 so that you can transparently bounce machines without interrupting data consumption. 

The partition count controls how many logs the topic will be sharded into. There are several impacts of the partition count. First each partition must fit entirely on a single server. So if you have 20 partitions the full data set (and read and write load) will be handled by no more than 20 servers (not counting replicas). Finally the partition count impacts the maximum parallelism of your consumers. This is discussed in greater detail in the concepts section. 

Each sharded partition log is placed into its own folder under the Kafka log directory. The name of such folders consists of the topic name, appended by a dash (-) and the partition id. Since a typical folder name can not be over 255 characters long, there will be a limitation on the length of topic names. We assume the number of partitions will not ever be above 100,000. Therefore, topic names cannot be longer than 249 characters. This leaves just enough room in the folder name for a dash and a potentially 5 digit long partition id. 

The configurations added on the command line override the default settings the server has for things like the length of time data should be retained. The complete set of per-topic configurations is documented here. 

## Modifying topics

You can change the configuration or partitioning of a topic using the same topic tool. 

To add partitions you can do 
    
    
    $ bin/kafka-topics.sh --bootstrap-server localhost:9092 --alter --topic my_topic_name \
        --partitions 40

Be aware that one use case for partitions is to semantically partition data, and adding partitions doesn't change the partitioning of existing data so this may disturb consumers if they rely on that partition. That is if data is partitioned by `hash(key) % number_of_partitions` then this partitioning will potentially be shuffled by adding partitions but Kafka will not attempt to automatically redistribute data in any way. 

To add configs: 
    
    
    $ bin/kafka-configs.sh --bootstrap-server localhost:9092 --entity-type topics --entity-name my_topic_name --alter --add-config x=y

To remove a config: 
    
    
    $ bin/kafka-configs.sh --bootstrap-server localhost:9092 --entity-type topics --entity-name my_topic_name --alter --delete-config x

And finally deleting a topic: 
    
    
    $ bin/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic my_topic_name

Kafka does not currently support reducing the number of partitions for a topic. 

Instructions for changing the replication factor of a topic can be found here. 

## Graceful shutdown

The Kafka cluster will automatically detect any broker shutdown or failure and elect new leaders for the partitions on that machine. This will occur whether a server fails or it is brought down intentionally for maintenance or configuration changes. For the latter cases Kafka supports a more graceful mechanism for stopping a server than just killing it. When a server is stopped gracefully it has two optimizations it will take advantage of: 

  1. It will sync all its logs to disk to avoid needing to do any log recovery when it restarts (i.e. validating the checksum for all messages in the tail of the log). Log recovery takes time so this speeds up intentional restarts. 
  2. It will migrate any partitions the server is the leader for to other replicas prior to shutting down. This will make the leadership transfer faster and minimize the time each partition is unavailable to a few milliseconds. 
Syncing the logs will happen automatically whenever the server is stopped other than by a hard kill, but the controlled leadership migration requires using a special setting: 
    
    
    controlled.shutdown.enable=true

Note that controlled shutdown will only succeed if _all_ the partitions hosted on the broker have replicas (i.e. the replication factor is greater than 1 _and_ at least one of these replicas is alive). This is generally what you want since shutting down the last replica would make that topic partition unavailable. 

## Balancing leadership

Whenever a broker stops or crashes, leadership for that broker's partitions transfers to other replicas. When the broker is restarted it will only be a follower for all its partitions, meaning it will not be used for client reads and writes. 

To avoid this imbalance, Kafka has a notion of preferred replicas. If the list of replicas for a partition is 1,5,9 then node 1 is preferred as the leader to either node 5 or 9 because it is earlier in the replica list. By default the Kafka cluster will try to restore leadership to the preferred replicas. This behaviour is configured with: 
    
    
    auto.leader.rebalance.enable=true

You can also set this to false, but you will then need to manually restore leadership to the restored replicas by running the command: 
    
    
    $ bin/kafka-leader-election.sh --bootstrap-server localhost:9092 --election-type preferred --all-topic-partitions

## Balancing Replicas Across Racks

The rack awareness feature spreads replicas of the same partition across different racks. This extends the guarantees Kafka provides for broker-failure to cover rack-failure, limiting the risk of data loss should all the brokers on a rack fail at once. The feature can also be applied to other broker groupings such as availability zones in EC2. 

You can specify that a broker belongs to a particular rack by adding a property to the broker config: 
    
    
    broker.rack=my-rack-id

When a topic is created, modified or replicas are redistributed, the rack constraint will be honoured, ensuring replicas span as many racks as they can (a partition will span min(#racks, replication-factor) different racks). 

The algorithm used to assign replicas to brokers ensures that the number of leaders per broker will be constant, regardless of how brokers are distributed across racks. This ensures balanced throughput. 

However if racks are assigned different numbers of brokers, the assignment of replicas will not be even. Racks with fewer brokers will get more replicas, meaning they will use more storage and put more resources into replication. Hence it is sensible to configure an equal number of brokers per rack. 

## Mirroring data between clusters & Geo-replication

Kafka administrators can define data flows that cross the boundaries of individual Kafka clusters, data centers, or geographical regions. Please refer to the section on Geo-Replication for further information. 

## Checking consumer position

Sometimes it's useful to see the position of your consumers. We have a tool that will show the position of all consumers in a consumer group as well as how far behind the end of the log they are. To run this tool on a consumer group named _my-group_ consuming a topic named _my-topic_ would look like this: 
    
    
    $ bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group my-group
    TOPIC                          PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG        CONSUMER-ID                                       HOST                           CLIENT-ID
    my-topic                       0          2               4               2          consumer-1-029af89c-873c-4751-a720-cefd41a669d6   /127.0.0.1                     consumer-1
    my-topic                       1          2               3               1          consumer-1-029af89c-873c-4751-a720-cefd41a669d6   /127.0.0.1                     consumer-1
    my-topic                       2          2               3               1          consumer-2-42c1abd4-e3b2-425d-a8bb-e1ea49b29bb2   /127.0.0.1                     consumer-2

## Managing Consumer Groups

With the ConsumerGroupCommand tool, we can list, describe, or delete the consumer groups. The consumer group can be deleted manually, or automatically when the last committed offset for that group expires. Manual deletion works only if the group does not have any active members. For example, to list all consumer groups across all topics: 
    
    
    $ bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --list
    test-consumer-group

To view offsets, as mentioned earlier, we "describe" the consumer group like this: 
    
    
    $ bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group my-group
    TOPIC           PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG             CONSUMER-ID                                    HOST            CLIENT-ID
    topic3          0          241019          395308          154289          consumer2-e76ea8c3-5d30-4299-9005-47eb41f3d3c4 /127.0.0.1      consumer2
    topic2          1          520678          803288          282610          consumer2-e76ea8c3-5d30-4299-9005-47eb41f3d3c4 /127.0.0.1      consumer2
    topic3          1          241018          398817          157799          consumer2-e76ea8c3-5d30-4299-9005-47eb41f3d3c4 /127.0.0.1      consumer2
    topic1          0          854144          855809          1665            consumer1-3fc8d6f1-581a-4472-bdf3-3515b4aee8c1 /127.0.0.1      consumer1
    topic2          0          460537          803290          342753          consumer1-3fc8d6f1-581a-4472-bdf3-3515b4aee8c1 /127.0.0.1      consumer1
    topic3          2          243655          398812          155157          consumer4-117fe4d3-c6c1-4178-8ee9-eb4a3954bee0 /127.0.0.1      consumer4

There are a number of additional "describe" options that can be used to provide more detailed information about a consumer group: 

  * \--members: This option provides the list of all active members in the consumer group. 
        
        $ bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group my-group --members
        CONSUMER-ID                                    HOST            CLIENT-ID       #PARTITIONS
        consumer1-3fc8d6f1-581a-4472-bdf3-3515b4aee8c1 /127.0.0.1      consumer1       2
        consumer4-117fe4d3-c6c1-4178-8ee9-eb4a3954bee0 /127.0.0.1      consumer4       1
        consumer2-e76ea8c3-5d30-4299-9005-47eb41f3d3c4 /127.0.0.1      consumer2       3
        consumer3-ecea43e4-1f01-479f-8349-f9130b75d8ee /127.0.0.1      consumer3       0

  * \--members --verbose: On top of the information reported by the "--members" options above, this option also provides the partitions assigned to each member. 
        
        $ bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group my-group --members --verbose
        CONSUMER-ID                                    HOST            CLIENT-ID       #PARTITIONS     ASSIGNMENT
        consumer1-3fc8d6f1-581a-4472-bdf3-3515b4aee8c1 /127.0.0.1      consumer1       2               topic1(0), topic2(0)
        consumer4-117fe4d3-c6c1-4178-8ee9-eb4a3954bee0 /127.0.0.1      consumer4       1               topic3(2)
        consumer2-e76ea8c3-5d30-4299-9005-47eb41f3d3c4 /127.0.0.1      consumer2       3               topic2(1), topic3(0,1)
        consumer3-ecea43e4-1f01-479f-8349-f9130b75d8ee /127.0.0.1      consumer3       0               -

  * \--offsets: This is the default describe option and provides the same output as the "--describe" option.
  * \--state: This option provides useful group-level information. 
        
        $ bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group my-group --state
        COORDINATOR (ID)          ASSIGNMENT-STRATEGY       STATE                #MEMBERS
        localhost:9092 (0)        range                     Stable               4


To manually delete one or multiple consumer groups, the "--delete" option can be used: 
    
    
    $ bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --delete --group my-group --group my-other-group
    Deletion of requested consumer groups ('my-group', 'my-other-group') was successful.

To reset offsets of a consumer group, "--reset-offsets" option can be used. This option supports one consumer group at the time. It requires defining following scopes: --all-topics or --topic. One scope must be selected, unless you use '--from-file' scenario. Also, first make sure that the consumer instances are inactive. See [KIP-122](https://cwiki.apache.org/confluence/display/KAFKA/KIP-122%3A+Add+Reset+Consumer+Group+Offsets+tooling) for more details. 

It has 3 execution options: 

  * (default) to display which offsets to reset. 
  * \--execute : to execute --reset-offsets process. 
  * \--export : to export the results to a CSV format. 



\--reset-offsets also has the following scenarios to choose from (at least one scenario must be selected): 

  * \--to-datetime <String: datetime> : Reset offsets to offsets from datetime. Format: 'YYYY-MM-DDTHH:mm:SS.sss' 
  * \--to-earliest : Reset offsets to earliest offset. 
  * \--to-latest : Reset offsets to latest offset. 
  * \--shift-by <Long: number-of-offsets> : Reset offsets shifting current offset by 'n', where 'n' can be positive or negative. 
  * \--from-file : Reset offsets to values defined in CSV file. 
  * \--to-current : Resets offsets to current offset. 
  * \--by-duration <String: duration> : Reset offsets to offset by duration from current timestamp. Format: 'PnDTnHnMnS' 
  * \--to-offset : Reset offsets to a specific offset. 

Please note, that out of range offsets will be adjusted to available offset end. For example, if offset end is at 10 and offset shift request is of 15, then, offset at 10 will actually be selected. 

For example, to reset offsets of a consumer group to the latest offset: 
    
    
    $ bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --reset-offsets --group consumergroup1 --topic topic1 --to-latest
    TOPIC                          PARTITION  NEW-OFFSET
    topic1                         0          0

## Expanding your cluster

Adding servers to a Kafka cluster is easy, just assign them a unique broker id and start up Kafka on your new servers. However these new servers will not automatically be assigned any data partitions, so unless partitions are moved to them they won't be doing any work until new topics are created. So usually when you add machines to your cluster you will want to migrate some existing data to these machines. 

The process of migrating data is manually initiated but fully automated. Under the covers what happens is that Kafka will add the new server as a follower of the partition it is migrating and allow it to fully replicate the existing data in that partition. When the new server has fully replicated the contents of this partition and joined the in-sync replica one of the existing replicas will delete their partition's data. 

The partition reassignment tool can be used to move partitions across brokers. An ideal partition distribution would ensure even data load and partition sizes across all brokers. The partition reassignment tool does not have the capability to automatically study the data distribution in a Kafka cluster and move partitions around to attain an even load distribution. As such, the admin has to figure out which topics or partitions should be moved around. 

The partition reassignment tool can run in 3 mutually exclusive modes: 

  * \--generate: In this mode, given a list of topics and a list of brokers, the tool generates a candidate reassignment to move all partitions of the specified topics to the new brokers. This option merely provides a convenient way to generate a partition reassignment plan given a list of topics and target brokers.
  * \--execute: In this mode, the tool kicks off the reassignment of partitions based on the user provided reassignment plan. (using the --reassignment-json-file option). This can either be a custom reassignment plan hand crafted by the admin or provided by using the --generate option
  * \--verify: In this mode, the tool verifies the status of the reassignment for all partitions listed during the last --execute. The status can be either of successfully completed, failed or in progress



### Automatically migrating data to new machines

The partition reassignment tool can be used to move some topics off of the current set of brokers to the newly added brokers. This is typically useful while expanding an existing cluster since it is easier to move entire topics to the new set of brokers, than moving one partition at a time. When used to do this, the user should provide a list of topics that should be moved to the new set of brokers and a target list of new brokers. The tool then evenly distributes all partitions for the given list of topics across the new set of brokers. During this move, the replication factor of the topic is kept constant. Effectively the replicas for all partitions for the input list of topics are moved from the old set of brokers to the newly added brokers. 

For instance, the following example will move all partitions for topics foo1,foo2 to the new set of brokers 5,6. At the end of this move, all partitions for topics foo1 and foo2 will _only_ exist on brokers 5,6. 

Since the tool accepts the input list of topics as a json file, you first need to identify the topics you want to move and create the json file as follows: 
    
    
    $ cat topics-to-move.json
    {
      "topics": [
        { "topic": "foo1" },
        { "topic": "foo2" }
      ],
      "version": 1
    }

Once the json file is ready, use the partition reassignment tool to generate a candidate assignment: 
    
    
    $ bin/kafka-reassign-partitions.sh --bootstrap-server localhost:9092 --topics-to-move-json-file topics-to-move.json --broker-list "5,6" --generate
    Current partition replica assignment
    {"version":1,
     "partitions":[{"topic":"foo1","partition":0,"replicas":[2,1],"log_dirs":["any"]},
                   {"topic":"foo1","partition":1,"replicas":[1,3],"log_dirs":["any"]},
                   {"topic":"foo1","partition":2,"replicas":[3,4],"log_dirs":["any"]},
                   {"topic":"foo2","partition":0,"replicas":[4,2],"log_dirs":["any"]},
                   {"topic":"foo2","partition":1,"replicas":[2,1],"log_dirs":["any"]},
                   {"topic":"foo2","partition":2,"replicas":[1,3],"log_dirs":["any"]}]
    }
    
    Proposed partition reassignment configuration
    {"version":1,
     "partitions":[{"topic":"foo1","partition":0,"replicas":[6,5],"log_dirs":["any"]},
                   {"topic":"foo1","partition":1,"replicas":[5,6],"log_dirs":["any"]},
                   {"topic":"foo1","partition":2,"replicas":[6,5],"log_dirs":["any"]},
                   {"topic":"foo2","partition":0,"replicas":[5,6],"log_dirs":["any"]},
                   {"topic":"foo2","partition":1,"replicas":[6,5],"log_dirs":["any"]},
                   {"topic":"foo2","partition":2,"replicas":[5,6],"log_dirs":["any"]}]
    }

The tool generates a candidate assignment that will move all partitions from topics foo1,foo2 to brokers 5,6. Note, however, that at this point, the partition movement has not started, it merely tells you the current assignment and the proposed new assignment. The current assignment should be saved in case you want to rollback to it. The new assignment should be saved in a json file (e.g. expand-cluster-reassignment.json) to be input to the tool with the --execute option as follows: 
    
    
    $ bin/kafka-reassign-partitions.sh --bootstrap-server localhost:9092 --reassignment-json-file expand-cluster-reassignment.json --execute
    Current partition replica assignment
    
    {"version":1,
     "partitions":[{"topic":"foo1","partition":0,"replicas":[2,1],"log_dirs":["any"]},
                   {"topic":"foo1","partition":1,"replicas":[1,3],"log_dirs":["any"]},
                   {"topic":"foo1","partition":2,"replicas":[3,4],"log_dirs":["any"]},
                   {"topic":"foo2","partition":0,"replicas":[4,2],"log_dirs":["any"]},
                   {"topic":"foo2","partition":1,"replicas":[2,1],"log_dirs":["any"]},
                   {"topic":"foo2","partition":2,"replicas":[1,3],"log_dirs":["any"]}]
    }
    
    Save this to use as the --reassignment-json-file option during rollback
    Successfully started partition reassignments for foo1-0,foo1-1,foo1-2,foo2-0,foo2-1,foo2-2

Finally, the --verify option can be used with the tool to check the status of the partition reassignment. Note that the same expand-cluster-reassignment.json (used with the --execute option) should be used with the --verify option: 
    
    
    $ bin/kafka-reassign-partitions.sh --bootstrap-server localhost:9092 --reassignment-json-file expand-cluster-reassignment.json --verify
    Status of partition reassignment:
    Reassignment of partition [foo1,0] is completed
    Reassignment of partition [foo1,1] is still in progress
    Reassignment of partition [foo1,2] is still in progress
    Reassignment of partition [foo2,0] is completed
    Reassignment of partition [foo2,1] is completed
    Reassignment of partition [foo2,2] is completed

### Custom partition assignment and migration

The partition reassignment tool can also be used to selectively move replicas of a partition to a specific set of brokers. When used in this manner, it is assumed that the user knows the reassignment plan and does not require the tool to generate a candidate reassignment, effectively skipping the --generate step and moving straight to the --execute step 

For instance, the following example moves partition 0 of topic foo1 to brokers 5,6 and partition 1 of topic foo2 to brokers 2,3: 

The first step is to hand craft the custom reassignment plan in a json file: 
    
    
    $ cat custom-reassignment.json
    {"version":1,"partitions":[{"topic":"foo1","partition":0,"replicas":[5,6]},{"topic":"foo2","partition":1,"replicas":[2,3]}]}

Then, use the json file with the --execute option to start the reassignment process: 
    
    
    $ bin/kafka-reassign-partitions.sh --bootstrap-server localhost:9092 --reassignment-json-file custom-reassignment.json --execute
    Current partition replica assignment
    
    {"version":1,
     "partitions":[{"topic":"foo1","partition":0,"replicas":[1,2],"log_dirs":["any"]},
                   {"topic":"foo2","partition":1,"replicas":[3,4],"log_dirs":["any"]}]
    }
    
    Save this to use as the --reassignment-json-file option during rollback
    Successfully started partition reassignments for foo1-0,foo2-1

The --verify option can be used with the tool to check the status of the partition reassignment. Note that the same custom-reassignment.json (used with the --execute option) should be used with the --verify option: 
    
    
    $ bin/kafka-reassign-partitions.sh --bootstrap-server localhost:9092 --reassignment-json-file custom-reassignment.json --verify
    Status of partition reassignment:
    Reassignment of partition [foo1,0] is completed
    Reassignment of partition [foo2,1] is completed

## Decommissioning brokers

The partition reassignment tool does not have the ability to automatically generate a reassignment plan for decommissioning brokers yet. As such, the admin has to come up with a reassignment plan to move the replica for all partitions hosted on the broker to be decommissioned, to the rest of the brokers. This can be relatively tedious as the reassignment needs to ensure that all the replicas are not moved from the decommissioned broker to only one other broker. To make this process effortless, we plan to add tooling support for decommissioning brokers in the future. 

## Increasing replication factor

Increasing the replication factor of an existing partition is easy. Just specify the extra replicas in the custom reassignment json file and use it with the --execute option to increase the replication factor of the specified partitions. 

For instance, the following example increases the replication factor of partition 0 of topic foo from 1 to 3. Before increasing the replication factor, the partition's only replica existed on broker 5. As part of increasing the replication factor, we will add more replicas on brokers 6 and 7. 

The first step is to hand craft the custom reassignment plan in a json file: 
    
    
    $ cat increase-replication-factor.json
    {"version":1,
     "partitions":[{"topic":"foo","partition":0,"replicas":[5,6,7]}]}

Then, use the json file with the --execute option to start the reassignment process: 
    
    
    $ bin/kafka-reassign-partitions.sh --bootstrap-server localhost:9092 --reassignment-json-file increase-replication-factor.json --execute
    Current partition replica assignment
    
    {"version":1,
     "partitions":[{"topic":"foo","partition":0,"replicas":[5],"log_dirs":["any"]}]}
    
    Save this to use as the --reassignment-json-file option during rollback
    Successfully started partition reassignment for foo-0

The --verify option can be used with the tool to check the status of the partition reassignment. Note that the same increase-replication-factor.json (used with the --execute option) should be used with the --verify option: 
    
    
    $ bin/kafka-reassign-partitions.sh --bootstrap-server localhost:9092 --reassignment-json-file increase-replication-factor.json --verify
    Status of partition reassignment:
    Reassignment of partition [foo,0] is completed

You can also verify the increase in replication factor with the kafka-topics tool: 
    
    
    $ bin/kafka-topics.sh --bootstrap-server localhost:9092 --topic foo --describe
    Topic:foo	PartitionCount:1	ReplicationFactor:3	Configs:
      Topic: foo	Partition: 0	Leader: 5	Replicas: 5,6,7	Isr: 5,6,7

## Limiting Bandwidth Usage during Data Migration

Kafka lets you apply a throttle to replication traffic, setting an upper bound on the bandwidth used to move replicas from machine to machine. This is useful when rebalancing a cluster, bootstrapping a new broker or adding or removing brokers, as it limits the impact these data-intensive operations will have on users. 

There are two interfaces that can be used to engage a throttle. The simplest, and safest, is to apply a throttle when invoking the kafka-reassign-partitions.sh, but kafka-configs.sh can also be used to view and alter the throttle values directly. 

So for example, if you were to execute a rebalance, with the below command, it would move partitions at no more than 50MB/s. 
    
    
    $ bin/kafka-reassign-partitions.sh --bootstrap-server localhost:9092 --execute --reassignment-json-file bigger-cluster.json --throttle 50000000

When you execute this script you will see the throttle engage: 
    
    
    The inter-broker throttle limit was set to 50000000 B/s
    Successfully started partition reassignment for foo1-0

Should you wish to alter the throttle, during a rebalance, say to increase the throughput so it completes quicker, you can do this by re-running the execute command with the --additional option passing the same reassignment-json-file:
    
    
    $ bin/kafka-reassign-partitions.sh --bootstrap-server localhost:9092 --additional --execute --reassignment-json-file bigger-cluster.json --throttle 700000000
    The inter-broker throttle limit was set to 700000000 B/s

Once the rebalance completes the administrator can check the status of the rebalance using the --verify option. If the rebalance has completed, the throttle will be removed via the --verify command. It is important that administrators remove the throttle in a timely manner once rebalancing completes by running the command with the --verify option. Failure to do so could cause regular replication traffic to be throttled. 

When the --verify option is executed, and the reassignment has completed, the script will confirm that the throttle was removed:
    
    
    $ bin/kafka-reassign-partitions.sh --bootstrap-server localhost:9092 --verify --reassignment-json-file bigger-cluster.json
    Status of partition reassignment:
    Reassignment of partition [my-topic,1] is completed
    Reassignment of partition [my-topic,0] is completed
    
    Clearing broker-level throttles on brokers 1,2,3
    Clearing topic-level throttles on topic my-topic

The administrator can also validate the assigned configs using the kafka-configs.sh. There are two pairs of throttle configuration used to manage the throttling process. First pair refers to the throttle value itself. This is configured, at a broker level, using the dynamic properties: 
    
    
    leader.replication.throttled.rate
    follower.replication.throttled.rate

Then there is the configuration pair of enumerated sets of throttled replicas: 
    
    
    leader.replication.throttled.replicas
    follower.replication.throttled.replicas

Which are configured per topic. 

All four config values are automatically assigned by kafka-reassign-partitions.sh (discussed below).

To view the throttle limit configuration:
    
    
    $ bin/kafka-configs.sh --describe --bootstrap-server localhost:9092 --entity-type brokers
    Configs for brokers '2' are leader.replication.throttled.rate=700000000,follower.replication.throttled.rate=700000000
    Configs for brokers '1' are leader.replication.throttled.rate=700000000,follower.replication.throttled.rate=700000000

This shows the throttle applied to both leader and follower side of the replication protocol. By default both sides are assigned the same throttled throughput value. 

To view the list of throttled replicas:
    
    
    $ bin/kafka-configs.sh --describe --bootstrap-server localhost:9092 --entity-type topics
    Configs for topic 'my-topic' are leader.replication.throttled.replicas=1:102,0:101,
        follower.replication.throttled.replicas=1:101,0:102

Here we see the leader throttle is applied to partition 1 on broker 102 and partition 0 on broker 101. Likewise the follower throttle is applied to partition 1 on broker 101 and partition 0 on broker 102. 

By default kafka-reassign-partitions.sh will apply the leader throttle to all replicas that exist before the rebalance, any one of which might be leader. It will apply the follower throttle to all move destinations. So if there is a partition with replicas on brokers 101,102, being reassigned to 102,103, a leader throttle, for that partition, would be applied to 101,102 and a follower throttle would be applied to 103 only. 

If required, you can also use the --alter switch on kafka-configs.sh to alter the throttle configurations manually. 

### Safe usage of throttled replication

Some care should be taken when using throttled replication. In particular:

_(1) Throttle Removal:_

The throttle should be removed in a timely manner once reassignment completes (by running kafka-reassign-partitions.sh --verify). 

_(2) Ensuring Progress:_

If the throttle is set too low, in comparison to the incoming write rate, it is possible for replication to not make progress. This occurs when:
    
    
    max(BytesInPerSec) > throttle

Where BytesInPerSec is the metric that monitors the write throughput of producers into each broker. 

The administrator can monitor whether replication is making progress, during the rebalance, using the metric:
    
    
    kafka.server:type=FetcherLagMetrics,name=ConsumerLag,clientId=([-.\w]+),topic=([-.\w]+),partition=([0-9]+)

The lag should constantly decrease during replication. If the metric does not decrease the administrator should increase the throttle throughput as described above. 

## Setting quotas

Quotas overrides and defaults may be configured at (user, client-id), user or client-id levels as described here. By default, clients receive an unlimited quota. It is possible to set custom quotas for each (user, client-id), user or client-id group. 

Configure custom quota for (user=user1, client-id=clientA): 
    
    
    $ bin/kafka-configs.sh --bootstrap-server localhost:9092 --alter --add-config 'producer_byte_rate=1024,consumer_byte_rate=2048,request_percentage=200' --entity-type users --entity-name user1 --entity-type clients --entity-name clientA
    Updated config for entity: user-principal 'user1', client-id 'clientA'.

Configure custom quota for user=user1: 
    
    
    $ bin/kafka-configs.sh --bootstrap-server localhost:9092 --alter --add-config 'producer_byte_rate=1024,consumer_byte_rate=2048,request_percentage=200' --entity-type users --entity-name user1
    Updated config for entity: user-principal 'user1'.

Configure custom quota for client-id=clientA: 
    
    
    $ bin/kafka-configs.sh --bootstrap-server localhost:9092 --alter --add-config 'producer_byte_rate=1024,consumer_byte_rate=2048,request_percentage=200' --entity-type clients --entity-name clientA
    Updated config for entity: client-id 'clientA'.

It is possible to set default quotas for each (user, client-id), user or client-id group by specifying _\--entity-default_ option instead of _\--entity-name_. 

Configure default client-id quota for user=userA: 
    
    
    $ bin/kafka-configs.sh --bootstrap-server localhost:9092 --alter --add-config 'producer_byte_rate=1024,consumer_byte_rate=2048,request_percentage=200' --entity-type users --entity-name user1 --entity-type clients --entity-default
    Updated config for entity: user-principal 'user1', default client-id.

Configure default quota for user: 
    
    
    $ bin/kafka-configs.sh --bootstrap-server localhost:9092 --alter --add-config 'producer_byte_rate=1024,consumer_byte_rate=2048,request_percentage=200' --entity-type users --entity-default
    Updated config for entity: default user-principal.

Configure default quota for client-id: 
    
    
    $ bin/kafka-configs.sh --bootstrap-server localhost:9092 --alter --add-config 'producer_byte_rate=1024,consumer_byte_rate=2048,request_percentage=200' --entity-type clients --entity-default
    Updated config for entity: default client-id.

Here's how to describe the quota for a given (user, client-id): 
    
    
    $ bin/kafka-configs.sh --bootstrap-server localhost:9092 --describe --entity-type users --entity-name user1 --entity-type clients --entity-name clientA
    Configs for user-principal 'user1', client-id 'clientA' are producer_byte_rate=1024,consumer_byte_rate=2048,request_percentage=200

Describe quota for a given user: 
    
    
    $ bin/kafka-configs.sh --bootstrap-server localhost:9092 --describe --entity-type users --entity-name user1
    Configs for user-principal 'user1' are producer_byte_rate=1024,consumer_byte_rate=2048,request_percentage=200

Describe quota for a given client-id: 
    
    
    $ bin/kafka-configs.sh --bootstrap-server localhost:9092 --describe --entity-type clients --entity-name clientA
    Configs for client-id 'clientA' are producer_byte_rate=1024,consumer_byte_rate=2048,request_percentage=200

If entity name is not specified, all entities of the specified type are described. For example, describe all users: 
    
    
    $ bin/kafka-configs.sh --bootstrap-server localhost:9092 --describe --entity-type users
    Configs for user-principal 'user1' are producer_byte_rate=1024,consumer_byte_rate=2048,request_percentage=200
    Configs for default user-principal are producer_byte_rate=1024,consumer_byte_rate=2048,request_percentage=200

Similarly for (user, client): 
    
    
    $ bin/kafka-configs.sh --bootstrap-server localhost:9092 --describe --entity-type users --entity-type clients
    Configs for user-principal 'user1', default client-id are producer_byte_rate=1024,consumer_byte_rate=2048,request_percentage=200
    Configs for user-principal 'user1', client-id 'clientA' are producer_byte_rate=1024,consumer_byte_rate=2048,request_percentage=200
