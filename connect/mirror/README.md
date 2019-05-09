
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
blacklisted ones), across all enabled replication flows. Each
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
can span multiple data centers, but it is recommended to configure MM2
nodes differently in each data center. For example, if you have 3 data
centers (west, east, north), each with 2 Kafka clusters (primary, backup),
you might configure MM2 as follows:

In the north data center:

    west-primary->north-primary.enabled = true
    east-primary->north-primary.enabled = true
    north-primary->north-backup.enabled = true

In the west data center:

    east-primary->west-primary.enabled = true
    north-primary->west-primary.enabled = true
    west-primary->west-backup.enabled = true

and so on, to ensure that records are only produced to nearby clusters
(otherwise, MM2's producers will need to wait longer for ACKs from
far-away clusters).

If your clusters are all nearby or in the same data center, you can
configure all MM2 nodes uniformly. For example, if you have two nearby
clusters (`primary`, `backup`), you can configure all MM2 nodes as follows:

    primary->backup.enabled
    backup->primary.enabled

## Shared configuration

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
target cluster.

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
    replication-latecny-ms  # time it takes records to propagate source->target
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


