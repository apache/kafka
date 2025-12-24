---
title: Tiered Storage
description: Tiered Storage
weight: 9
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


## Tiered Storage Overview

Kafka data is mostly consumed in a streaming fashion using tail reads. Tail reads leverage OS's page cache to serve the data instead of disk reads. Older data is typically read from the disk for backfill or failure recovery purposes and is infrequent.

In the tiered storage approach, Kafka cluster is configured with two tiers of storage - local and remote. The local tier is the same as the current Kafka that uses the local disks on the Kafka brokers to store the log segments. The new remote tier uses external storage systems, such as HDFS or S3, to store the completed log segments. Please check [KIP-405](https://cwiki.apache.org/confluence/x/KJDQBQ) for more information. 

## Configuration

### Broker Configurations

By default, the Kafka server will not enable the tiered storage feature. `remote.log.storage.system.enable` is the property to control whether to enable tiered storage functionality in a broker or not. Setting it to "true" enables this feature. 

`RemoteStorageManager` is an interface to provide the lifecycle of remote log segments and indexes. Kafka server doesn't provide out-of-the-box implementation of RemoteStorageManager. Users must configure `remote.log.storage.manager.class.name` and `remote.log.storage.manager.class.path` to specify the implementation of RemoteStorageManager. 

`RemoteLogMetadataManager` is an interface to provide the lifecycle of metadata about remote log segments with strongly consistent semantics. By default, Kafka provides an implementation with storage as an internal topic. This implementation can be changed by configuring `remote.log.metadata.manager.class.name` and `remote.log.metadata.manager.class.path`. When adopting the default kafka internal topic based implementation, `remote.log.metadata.manager.listener.name` is a mandatory property to specify which listener the clients created by the default RemoteLogMetadataManager implementation. 

### Topic Configurations

After correctly configuring broker side configurations for tiered storage feature, there are still configurations in topic level needed to be set. `remote.storage.enable` is the switch to determine if a topic wants to use tiered storage or not. By default it is set to false. After enabling `remote.storage.enable` property, the next thing to consider is the log retention. When tiered storage is enabled for a topic, there are 2 additional log retention configurations to set: 

  * `local.retention.ms`
  * `retention.ms`
  * `local.retention.bytes`
  * `retention.bytes`



The configuration prefixed with `local` are to specify the time/size the "local" log file can accept before moving to remote storage, and then get deleted. If unset, The value in `retention.ms` and `retention.bytes` will be used.

## Quick Start Example

Apache Kafka doesn't provide an out-of-the-box RemoteStorageManager implementation. To have a preview of the tiered storage feature, the [LocalTieredStorage](https://github.com/apache/kafka/blob/trunk/storage/src/test/java/org/apache/kafka/server/log/remote/storage/LocalTieredStorage.java) implemented for integration test can be used, which will create a temporary directory in local storage to simulate the remote storage. 

To adopt the `LocalTieredStorage`, the test library needs to be built locally
    
    
    # please checkout to the specific version tag you're using before building it
    # ex: `git checkout 4.3.0`
    $ ./gradlew clean :storage:testJar

After build successfully, there should be a `kafka-storage-x.x.x-test.jar` file under `storage/build/libs`. Next, setting configurations in the broker side to enable tiered storage feature.
    
    
    # Sample KRaft broker server.properties listening on PLAINTEXT://:9092
    remote.log.storage.system.enable=true
    
    # Setting the listener for the clients in RemoteLogMetadataManager to talk to the brokers.
    remote.log.metadata.manager.listener.name=PLAINTEXT
    
    # Please provide the implementation info for remoteStorageManager.
    # This is the mandatory configuration for tiered storage.
    # Here, we use the `LocalTieredStorage` built above.
    remote.log.storage.manager.class.name=org.apache.kafka.server.log.remote.storage.LocalTieredStorage
    remote.log.storage.manager.class.path=/PATH/TO/kafka-storage-4.3.0-test.jar
    
    # These 2 prefix are default values, but customizable
    remote.log.storage.manager.impl.prefix=rsm.config.
    remote.log.metadata.manager.impl.prefix=rlmm.config.
    
    # Configure the directory used for `LocalTieredStorage`
    # Note, please make sure the brokers need to have access to this directory
    rsm.config.dir=/tmp/kafka-remote-storage
    
    # This needs to be changed if number of brokers in the cluster is more than 1
    rlmm.config.remote.log.metadata.topic.replication.factor=1
    
    # Try to speed up the log retention check interval for testing
    log.retention.check.interval.ms=1000

Following quick start guide to start up the kafka environment. Then, create a topic with tiered storage enabled with configs: 
    
    
    # remote.storage.enable=true -> enables tiered storage on the topic
    # local.retention.ms=1000 -> The number of milliseconds to keep the local log segment before it gets deleted.
    # Note that a local log segment is eligible for deletion only after it gets uploaded to remote.
    # retention.ms=3600000 -> when segments exceed this time, the segments in remote storage will be deleted
    # segment.bytes=1048576 -> for test only, to speed up the log segment rolling interval
    # file.delete.delay.ms=10000 -> for test only, to speed up the local-log segment file delete delay
    
    $ bin/kafka-topics.sh --create --topic tieredTopic --bootstrap-server localhost:9092 \
    --config remote.storage.enable=true --config local.retention.ms=1000 --config retention.ms=3600000 \
    --config segment.bytes=1048576 --config file.delete.delay.ms=1000

Try to send messages to the `tieredTopic` topic to roll the log segment:
    
    
    $ bin/kafka-producer-perf-test.sh --bootstrap-server localhost:9092 --topic tieredTopic --num-records 1200 --record-size 1024 --throughput -1

Then, after the active segment is rolled, the old segment should be moved to the remote storage and get deleted. This can be verified by checking the remote log directory configured above. For example: 
    
    
    $ ls /tmp/kafka-remote-storage/kafka-tiered-storage/tieredTopic-0-jF8s79t9SrG_PNqlwv7bAA
    00000000000000000000-knnxbs3FSRyKdPcSAOQC-w.index
    00000000000000000000-knnxbs3FSRyKdPcSAOQC-w.snapshot
    00000000000000000000-knnxbs3FSRyKdPcSAOQC-w.leader_epoch_checkpoint
    00000000000000000000-knnxbs3FSRyKdPcSAOQC-w.timeindex
    00000000000000000000-knnxbs3FSRyKdPcSAOQC-w.log

Lastly, we can try to consume some data from the beginning and print offset number, to make sure it will successfully fetch offset 0 from the remote storage.
    
    
    $ bin/kafka-console-consumer.sh --topic tieredTopic --from-beginning --max-messages 1 --bootstrap-server localhost:9092 --formatter-property print.offset=true

In KRaft mode, you can disable tiered storage at the topic level, to make the remote logs as read-only logs, or completely delete all remote logs.

If you want to let the remote logs become read-only and no more local logs copied to the remote storage, you can set `remote.storage.enable=true,remote.log.copy.disable=true` to the topic.

Note: You also need to set `local.retention.ms` and `local.retention.bytes` to the same value as `retention.ms` and `retention.bytes`, or set to "-2". This is because after disabling remote log copy, the local retention policies will not be applied anymore, and that might confuse users and cause unexpected disk full. 
    
    
    $ bin/kafka-configs.sh --bootstrap-server localhost:9092 \
       --alter --entity-type topics --entity-name tieredTopic \
       --add-config 'remote.storage.enable=true,remote.log.copy.disable=true,local.retention.ms=-2,local.retention.bytes=-2'

If you want to completely disable tiered storage at the topic level with all remote logs deleted, you can set `remote.storage.enable=false,remote.log.delete.on.disable=true` to the topic.
    
    
    $ bin/kafka-configs.sh --bootstrap-server localhost:9092 \
       --alter --entity-type topics --entity-name tieredTopic \
       --add-config 'remote.storage.enable=false,remote.log.delete.on.disable=true'

You can also re-enable tiered storage feature at the topic level. Please note, if you want to disable tiered storage at the cluster level, you should delete the tiered storage enabled topics explicitly. Attempting to disable tiered storage at the cluster level without deleting the topics using tiered storage will result in an exception during startup.
    
    
    $ bin/kafka-topics.sh --delete --topic tieredTopic --bootstrap-server localhost:9092

After topics are deleted, you're safe to set `remote.log.storage.system.enable=false` in the broker configuration.

## Limitations

While the Tiered Storage works for most use cases, it is still important to be aware of the following limitations: 

  * No support for compacted topics
  * Disabling tiered storage on all topics where it is enabled is required before disabling tiered storage at the broker level
  * Admin actions related to tiered storage feature are only supported on clients from version 3.0 onwards
  * No support for log segments missing producer snapshot file. It can happen when topic is created before v2.8.0.



For more information, please check [Kafka Tiered Storage GA Release Notes](https://cwiki.apache.org/confluence/x/9xDOEg). 
