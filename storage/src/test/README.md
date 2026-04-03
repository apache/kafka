# Local Tiered Storage Setup Guide

An implementation of RemoteStorageManager which relies on the local file system to store  offloaded log segments
and associated data. Due to the consistency semantic of POSIX-compliant file systems, this remote storage provides 
strong read-after-write consistency and a segment's data can be accessed once the copy to the storage succeeded.

The local tiered storage keeps a simple structure of directories mimicking that of Apache Kafka. The name of each of the
files under the scope of a log segment (the log file, its indexes, etc.) follows the structure 
/storage-directory/topicId-partition-topicName/segmentId-fileType. The topicId and segmentId are defined in UuidBase64 format.

Given the root directory of the storage, segments and associated files are organized as represented below:
```shell
/ storage-directory  / LWgrMmVrT0a__7a4SasuPA-0-topic / bCqX9U--S-6U8XUM9II25Q-segment
.                                                     . bCqX9U--S-6U8XUM9II25Q-offset_index
.                                                     . bCqX9U--S-6U8XUM9II25Q-time_index
.                                                     . h956soEzTzi9a-NOQ-DvKA-segment
.                                                     . h956soEzTzi9a-NOQ-DvKA-offset_index
.                                                     . h956soEzTzi9a-NOQ-DvKA-segment
.
/ LWgrMmVrT0a__7a4SasuPA-1-topic / o8CQPT86QQmbFmi3xRmiHA-segment
.                                . o8CQPT86QQmbFmi3xRmiHA-offset_index
.                                . o8CQPT86QQmbFmi3xRmiHA-time_index
.
/ DRagLm_PS9Wl8fz1X43zVg-3-btopic / jvj3vhliTGeU90sIosmp_g-segment
.                                 . jvj3vhliTGeU90sIosmp_g-offset_index
.                                 . jvj3vhliTGeU90sIosmp_g-time_index
```

## Local Tiered Storage Configuration
To configure the broker with local file system as remote storage, use the below configs:
```properties
# Remote Storage Manager Config
remote.log.storage.system.enable=true
remote.log.storage.manager.class.name=org.apache.kafka.server.log.remote.storage.LocalTieredStorage
remote.log.storage.manager.class.path=
remote.log.storage.manager.impl.prefix=remote.log.storage.local.
remote.log.storage.local.dir=/tmp/kafka-remote-storage

# Remote Log Metadata Manager Config
remote.log.metadata.manager.class.name=org.apache.kafka.server.log.remote.metadata.storage.TopicBasedRemoteLogMetadataManager
remote.log.metadata.manager.class.path=
remote.log.metadata.manager.listener.name=PLAINTEXT
remote.log.manager.task.interval.ms=30000
remote.log.manager.thread.pool.size=10
remote.log.reader.max.pending.tasks=100
remote.log.metadata.manager.impl.prefix=rlmm.config.
rlmm.config.remote.log.metadata.topic.num.partitions=5
rlmm.config.remote.log.metadata.topic.replication.factor=3
rlmm.config.remote.log.metadata.topic.retention.ms=-1

log.retention.check.interval.ms=1000
log.segment.delete.delay.ms=1000

offsets.topic.num.partitions=5
offsets.topic.replication.factor=3

transaction.state.log.num.partitions=5
transaction.state.log.replication.factor=3
transaction.state.log.min.isr=2
```

## Starting the server
Before starting the server, make sure that `kafka-clients-<version>-test.jar` and `kafka-storage-<version>-test.jar` 
libraries are included in your distribution. The `LocalTieredStorage` implementation is in the test package, 
so we should include the test jars in the classpath before starting the server.

```shell
export INCLUDE_TEST_JARS=true
# start the server
nohup sh kafka-server-start.sh ../config/server.properties &
```

## Test a sample topic with local tiered storage
Run at-least two brokers to simulate the error scenarios. Make sure that you have more than one segment file per 
partition. Only passive segments are uploaded to the remote store.

Topic config to create a log-segment accepting one and only record and ensures the segment deletion to consume the 
records from the tiered storage.

tiered-topic-config.properties:
```properties
index.interval.bytes=1
segment.index.bytes=12
local.retention.bytes=1
remote.storage.enable=true
```

Set the producer linger config to 60 seconds to optimistically generate batches of records with a predetermined size. 
On flush/close, the console producer sends all the records in a single batch.

tiered-producer-config.properties:
```properties
linger.ms=60000
```

Idempotent producer configs:
```properties
linger.ms=60000
enable.idempotence=true
acks=all
client.id=sample-producer
```

### Steps
```shell
# Create a test topic named samsung
sh kafka-topics.sh --bootstrap-server localhost:9092 --topic samsung --replication-factor 2 --partitions 1 --create --config index.interval.bytes=1 --config segment.index.bytes=12 --config local.retention.bytes=1 --config remote.storage.enable=true
# Produce messages to the topic
sh kafka-console-producer.sh --bootstrap-server localhost:9092 --producer.config ../config/tiered-producer-config.properties --topic samsung
# Consume messages from the topic
sh kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic samsung --from-beginning
# NOTE: Produce some more messages to roll the active segment to passive.
```

```shell
# To view the contents of the uploaded log segment
# Rename the <uuid>-segment to its corresponding base offset log filename (No need to match the actual base offset with 
# filename, any offset number can do fine).  The number format should contain 20 digits. e.g. 00000000000000000001.log 
sh kafka-dump-log.sh --files 00000000000000000001.log -print-data-log
# To dump the remote log metadata and view its contents:
sh kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic __remote_log_metadata --from-beginning --formatter org.apache.kafka.server.log.remote.metadata.storage.serialization.RemoteLogMetadataFormatter
```

To change the leader, run kafka-reassign-partitions and kafka-leader-election script:
```shell
sh kafka-reassign-partitions.sh --bootstrap-server localhost:9092 --broker-list 0,1 --topics-to-move-json-file <topics-to-move.json> --generate
sh kafka-reassign-partitions.sh --bootstrap-server localhost:9092 --reassignment-json-file <reassignment.json> --execute
# wait for the preferred leader election to trigger (or) run the leader election script.
sh kafka-leader-election.sh --bootstrap-server localhost:9092 --election-type PREFERRED --path-to-json-file <leader-election.json>
```
```shell
# Delete some records to move the remote log start offset and consume again:
sh kafka-delete-records.sh --bootstrap-server localhost:9092 --offset-json-file <delete_records.json>
```

topics-to-move.json:
```json
{"topics": [{ "topic": "samsung" }, { "topic": "nokia" }], "version": 1}
```

reassignment.json:
```json
{
    "version": 1,
    "partitions": [
        { "topic": "samsung", "partition": 0, "replicas": [0, 1], "log_dirs": ["any", "any"] },
        { "topic": "nokia", "partition": 0, "replicas": [0, 1], "log_dirs": ["any", "any"] }
    ]
}
```

leader-election.json:
```json
{
    "partitions": [
        { "topic": "samsung", "partition": 0 },
        { "topic": "nokia", "partition": 0 }
    ]
}
```

delete_records.json:
```json
{
    "partitions": [ { "topic": "samsung", "partition": 0, "offset": 2 } ],
    "version": 1
}
```

