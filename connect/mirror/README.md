
# MirrorMaker 2.0

MM2 leverages the Connect framework to replicate topics between Kafka
clusters. MM2 includes several new features, including:

 - both topics and consumer groups are replicated
 - topic configuration and ACLs are replicated
 - cross-cluster offsets are synchronized
 - partitioning is preserved

## Replication flows

MM2 replicates topics and consumer groups from upstream source clusters
to downstream target clusters. These directional flows are notated
`A->B`.

It's possible to create complex replication topologies based on these
`source->target` flows, including:

 - *fan-out*, e.g. `K->A, K->B, K->C`
 - *aggregation*, e.g. `A->K, B->K, C->K`
 - *active/active*, e.g. `A->B, B->A`

Each replication flow can be configured independently, e.g. to replicate
specific topics or groups:

    A->B.topics = topic-1, topic-2
    A->B.groups = group-1, group-2

By default, all topics and consumer groups are replicated (except
excluded ones), across all enabled replication flows. Each
replication flow must be explicitly enabled to begin replication:

    A->B.enabled = true
    B->A.enabled = true

## Starting an MM2 process

You can run any number of MM2 processes as needed. Any MM2 processes
which are configured to replicate the same Kafka clusters will find each
other, share configuration, load balance, etc.

To start an MM2 process, first specify Kafka cluster information in a
configuration file as follows:

    # mm2.properties
    clusters = us-west, us-east
    us-west.bootstrap.servers = host1:9092
    us-east.bootstrap.servers = host2:9092

You can list any number of clusters this way.

Optionally, you can override default MirrorMaker properties:

    topics = .*
    groups = group1, group2
    emit.checkpoints.interval.seconds = 10

These will apply to all replication flows. You can also override default
properties for specific clusters or replication flows:

    # configure a specific cluster
    us-west.offset.storage.topic = mm2-offsets

    # configure a specific source->target replication flow
    us-west->us-east.emit.heartbeats = false

Next, enable individual replication flows as follows:

    us-west->us-east.enabled = true     # disabled by default

Finally, launch one or more MirrorMaker processes with the `connect-mirror-maker.sh`
script:

    $ ./bin/connect-mirror-maker.sh mm2.properties

## Multicluster environments

MM2 supports replication between multiple Kafka clusters, whether in the
same data center or across multiple data centers. A single MM2 cluster
can span multiple data centers, but it is recommended to keep MM2's producers
as close as possible to their target clusters. To do so, specify a subset
of clusters for each MM2 node as follows:

    # in west DC:
    $ ./bin/connect-mirror-maker.sh mm2.properties --clusters west-1 west-2

This signals to the node that the given clusters are nearby, and prevents the
node from sending records or configuration to clusters in other data centers.

### Example

Say there are three data centers (west, east, north) with two Kafka
clusters in each data center (west-1, west-2 etc). We can configure MM2
for active/active replication within each data center, as well as cross data
center replication (XDCR) as follows:

    # mm2.properties
    clusters: west-1, west-2, east-1, east-2, north-1, north-2

    west-1.bootstrap.servers = ...
    ---%<---

    # active/active in west
    west-1->west-2.enabled = true
    west-2->west-1.enabled = true

    # active/active in east
    east-1->east-2.enabled = true
    east-2->east-1.enabled = true

    # active/active in north
    north-1->north-2.enabled = true
    north-2->north-1.enabled = true

    # XDCR via west-1, east-1, north-1
    west-1->east-1.enabled = true
    west-1->north-1.enabled = true
    east-1->west-1.enabled = true
    east-1->north-1.enabled = true
    north-1->west-1.enabled = true
    north-1->east-1.enabled = true

Then, launch MM2 in each data center as follows:

    # in west:
    $ ./bin/connect-mirror-maker.sh mm2.properties --clusters west-1 west-2

    # in east:
    $ ./bin/connect-mirror-maker.sh mm2.properties --clusters east-1 east-2

    # in north:
    $ ./bin/connect-mirror-maker.sh mm2.properties --clusters north-1 north-2
    
With this configuration, records produced to any cluster will be replicated
within the data center, as well as across to other data centers. By providing
the `--clusters` parameter, we ensure that each node only produces records to
nearby clusters.

N.B. that the `--clusters` parameter is not technically required here. MM2 will work fine without it; however, throughput may suffer from "producer lag" between
data centers, and you may incur unnecessary data transfer costs.

## Configuration
The following sections target for dedicated MM2 cluster. If running MM2 in a Connect cluster, please refer to [KIP-382: MirrorMaker 2.0](https://cwiki.apache.org/confluence/display/KAFKA/KIP-382%3A+MirrorMaker+2.0) for guidance.
 
### General Kafka Connect Config
All Kafka Connect, Source Connector, Sink Connector configs, as defined in [Kafka official doc](https://kafka.apache.org/documentation/#connectconfigs), can be 
directly used in MM2 configuration without prefix in the configuration name. As the starting point, most of these default configs may work well with the exception of `tasks.max`.

In order to evenly distribute the workload across more than one MM2 instance, it is advised to set `tasks.max` at least to 2 or even larger depending on the hardware resources
and the total number partitions to be replicated.

### Kafka Connect Config for a Specific Connector
If needed, Kafka Connect worker-level configs could be even specified "per connector", which needs to follow the format of `cluster_alias.config_name` in MM2 configuration. For example,
 
    backup.ssl.truststore.location = /usr/lib/jvm/zulu-8-amd64/jre/lib/security/cacerts // SSL cert location
    backup.security.protocol = SSL // if target cluster needs SSL to send message
    
### MM2 Config for a Specific Connector
MM2 itself has many configs to control how it behaves. To override those default values, add the config name by the format of `source_cluster_alias->target_cluster_alias.config_name` in MM2 configuration. For example,
    
    backup->primary.enabled = false // set to false if one-way replication is desired
    primary->backup.topics.blacklist = topics_to_blacklist
    primary->backup.emit.heartbeats.enabled = false
    primary->backup.sync.group.offsets = true 

### Producer / Consumer / Admin Config used by MM2
In many cases, customized values for producer or consumer configurations are needed. In order to override the default values of producer or consumer used by MM2, 
`target_cluster_alias.producer.producer_config_name`, `source_cluster_alias.consumer.consumer_config_name` or `cluster_alias.admin.admin_config_name` are the formats to use in MM2 configuration. For example,

     backup.producer.compression.type = gzip
     backup.producer.buffer.memory = 32768
     primary.consumer.isolation.level = read_committed
     primary.admin.bootstrap.servers = localhost:9092
     
### Shared configuration

MM2 processes share configuration via their target Kafka clusters. 
For example, the following two processes would be racy:

    # process1:
    A->B.enabled = true
    A->B.topics = foo

    # process2:
    A->B.enabled = true
    A->B.topics = bar

In this case, the two processes will share configuration via cluster `B`.
Depending on which processes is elected "leader", the result will be
that either `foo` or `bar` is replicated -- but not both. For this reason,
it is important to keep configuration consistent across flows to the same
target cluster. In most cases, your entire organization should use a single
MM2 configuration file.

## Remote topics

MM2 employs a naming convention to ensure that records from different
clusters are not written to the same partition. By default, replicated
topics are renamed based on "source cluster aliases":

    topic-1 --> source.topic-1

This can be customized by overriding the `replication.policy.separator`
property (default is a period). If you need more control over how
remote topics are defined, you can implement a custom `ReplicationPolicy`
and override `replication.policy.class` (default is
`DefaultReplicationPolicy`).

## Monitoring an MM2 process

MM2 is built on the Connect framework and inherits all of Connect's metrics, e.g.
`source-record-poll-rate`. In addition, MM2 produces its own metrics under the
`kafka.connect.mirror` metric group. Metrics are tagged with the following properties:

    - *target*: alias of target cluster
    - *source*: alias of source cluster
    - *topic*:  remote topic on target cluster 
    - *partition*: partition being replicated

Metrics are tracked for each *remote* topic. The source cluster can be inferred
from the topic name. For example, replicating `topic1` from `A->B` will yield metrics
like:

    - `target=B`
    - `topic=A.topic1`
    - `partition=1`

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

These metrics do not discern between created-at and log-append timestamps.


